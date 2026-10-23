package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/** Under OR, a negation that cannot fold into its criterion (a MATCH) is ONE of the alternatives: a
  * `bool { must_not }` inside the `should`. It sat in the bool's own `must_not`, beside the
  * `should` clauses, where Elasticsearch requires it of every document: `x = 50 OR NOT MATCH(k)`
  * answered `x = 50 AND NOT MATCH(k)`.
  */
class OrNegationEmissionSpec extends AnyFlatSpec with Matchers {

  implicit val timestamp: Long = 0L

  private val mapper = new ObjectMapper()

  private def queryOf(sql: String): JsonNode = Parser(sql) match {
    case Right(s: SingleSearch) =>
      val request: ElasticSearchRequest = s
      mapper.readTree(request.query).get("query")
    case other => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  /** Every `bool` of the query. */
  private def bools(n: JsonNode): Seq[JsonNode] =
    if (n.isObject)
      Option(n.get("bool")).toSeq ++ n.elements().asScala.toSeq.flatMap(bools)
    else if (n.isArray) n.elements().asScala.toSeq.flatMap(bools)
    else Nil

  "a negated MATCH under OR" should "be one of the should clauses, not a must_not beside them" in {
    Seq(
      "n = 50 OR NOT MATCH(k) AGAINST ('a')",
      "n = 50 OR NOT MATCH(k, g1) AGAINST ('a')",
      "(n = 50 OR NOT MATCH(k) AGAINST ('a')) AND g1 = 'a'"
    ).foreach { condition =>
      val q = queryOf(s"SELECT id FROM t WHERE $condition")
      withClue(s"[$condition] $q ") {
        bools(q).filter(b => b.has("should") && b.has("must_not")) shouldBe empty
        // the OR's own bool: the one whose alternatives test `n`
        val alternatives = bools(q)
          .find(b => b.has("should") && b.get("should").toString.contains(""""term":{"n""""))
          .map(_.get("should").elements().asScala.toSeq)
          .getOrElse(fail("no should clause tests n"))
        alternatives.size shouldBe 2
        // the negated alternative keeps the documents where its column has a value
        alternatives.exists(a =>
          a.toString.contains("must_not") && a.toString.contains("exists")
        ) shouldBe true
      }
    }
  }

  "a negated MATCH under AND" should "stay a must_not of the bool, beside the column's exists" in {
    val q = queryOf("SELECT id FROM t WHERE n = 50 AND NOT MATCH(k) AGAINST ('a')")
    withClue(q) {
      bools(q).exists(_.has("should")) shouldBe false
      q.toString should include(""""must_not":[{"match":{"k":{"query":"a"}}}]""")
    }
  }

  "a negated condition over another UNNEST under OR" should "be one of the alternatives too" in {
    val q = queryOf(
      "SELECT t.id FROM t AS t JOIN UNNEST(t.items) AS i JOIN UNNEST(t.tags) AS g " +
      "WHERE i.sku = 's1' OR NOT g.v = 'x'"
    )
    withClue(q) {
      bools(q).filter(b => b.has("should") && b.has("must_not")) shouldBe empty
      bools(q).exists(b => b.has("should") && b.get("should").size() == 2) shouldBe true
    }
  }
}
