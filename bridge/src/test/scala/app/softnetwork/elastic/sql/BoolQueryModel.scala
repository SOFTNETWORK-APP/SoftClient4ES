package app.softnetwork.elastic.sql

import com.fasterxml.jackson.databind.JsonNode

import scala.jdk.CollectionConverters._

/** How Elasticsearch evaluates the `bool` queries a WHERE emits, over documents whose fields `c1 …
  * cN` are 0 or 1 (`bits`: field `ci` is bit `i - 1`).
  *
  * The one rule that matters here, from the Elasticsearch reference: a `bool` with `should` and no
  * explicit `minimum_should_match` requires ONE `should` clause only when it holds no `filter` and
  * no `must` clause -- next to either, the `should` clauses are optional. Elasticsearch 6 adds one
  * exception: a `bool` evaluated in a FILTER context requires one `should` clause anyway. `es6 =
  * true` applies that exception. `nested` is read with a ONE-child document (its query is evaluated
  * on the same bits); the evaluation context carries through it.
  */
object BoolQueryModel {

  final case class Unmodelled(msg: String) extends Exception(msg)

  def matches(q: JsonNode, bits: Int, es6: Boolean, filterContext: Boolean = false): Boolean = {
    val kinds = q.fieldNames().asScala.toList
    if (kinds.size != 1) throw Unmodelled(s"query node with keys $kinds")
    val body = q.get(kinds.head)
    kinds.head match {
      case "bool" =>
        val known = Set(
          "filter",
          "must",
          "must_not",
          "should",
          "minimum_should_match",
          "boost",
          "adjust_pure_negative"
        )
        body
          .fieldNames()
          .asScala
          .find(k => !known.contains(k))
          .foreach(k => throw Unmodelled(s"bool key $k"))
        def clauses(name: String): List[JsonNode] = Option(body.get(name)) match {
          case Some(n) if n.isArray => n.elements().asScala.toList
          case Some(n)              => List(n)
          case None                 => Nil
        }
        val (filters, musts, nots, shoulds) =
          (clauses("filter"), clauses("must"), clauses("must_not"), clauses("should"))
        val required = Option(body.get("minimum_should_match")).map(_.asText.toInt).getOrElse {
          if (shoulds.isEmpty) 0
          else if (es6 && filterContext) 1
          else if (filters.isEmpty && musts.isEmpty) 1
          else 0
        }
        filters.forall(matches(_, bits, es6, filterContext = true)) &&
        musts.forall(matches(_, bits, es6, filterContext)) &&
        nots.forall(n => !matches(n, bits, es6, filterContext = true)) &&
        shoulds.count(matches(_, bits, es6, filterContext)) >= required
      case "nested" => matches(body.get("query"), bits, es6, filterContext)
      case "term" =>
        val field = body.fieldNames().asScala.toList.head
        val v = Option(body.get(field)).map(n => if (n.isObject) n.get("value") else n).get
        val i = """c(\d+)""".r
          .findFirstMatchIn(field)
          .map(_.group(1).toInt)
          .getOrElse(throw Unmodelled(s"term on $field"))
        val bit = ((bits >> (i - 1)) & 1) == 1
        (if (bit) 1.0 else 0.0) == v.asDouble
      case "match" =>
        // `MATCH (ci) AGAINST ('1')`: read like `ci = 1` -- one term, the field's value
        val field = body.fieldNames().asScala.toList.head
        val v = Option(body.get(field)).map(n => if (n.isObject) n.get("query") else n).get
        val i = """c(\d+)""".r
          .findFirstMatchIn(field)
          .map(_.group(1).toInt)
          .getOrElse(throw Unmodelled(s"match on $field"))
        val bit = ((bits >> (i - 1)) & 1) == 1
        bit == (v.asText == "1")
      case "match_all" => true
      case other       => throw Unmodelled(s"query kind $other")
    }
  }

  /** Every `bool` that holds `should` clauses next to `filter` or `must` clauses without an
    * explicit `minimum_should_match` -- where its `should` clauses are optional.
    */
  def optionalShoulds(q: JsonNode): Int = {
    var count = 0
    def walk(n: JsonNode): Unit =
      if (n.isObject) {
        Option(n.get("bool")).filter(_.isObject).foreach { b =>
          val should = Option(b.get("should")).exists(_.size > 0)
          val required =
            Option(b.get("filter")).exists(_.size > 0) || Option(b.get("must")).exists(_.size > 0)
          if (should && required && b.get("minimum_should_match") == null) count += 1
        }
        n.elements().asScala.foreach(walk)
      } else if (n.isArray) n.elements().asScala.foreach(walk)
    walk(q)
    count
  }
}
