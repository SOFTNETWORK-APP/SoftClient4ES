/*
 * Copyright 2025 SOFTNETWORK
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app.softnetwork.elastic.client

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticResult, ElasticSuccess}
import app.softnetwork.elastic.client.scroll.ScrollConfig
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{
  MultiSearch,
  SQLAggregation,
  SelectStatement,
  SingleSearch
}
import com.fasterxml.jackson.databind.JsonNode
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._

/** An un-aliased dotted column (`SELECT obj.a FROM t`) carries the document's value under its
  * output name in every row core builds from hits, as its aliased form (`obj.a AS k`) does -- and
  * its parent object stops leaking into the row once the reader has read it.
  *
  * No Docker: a [[NopeClientApi]] double answers every request with the canned page of the index it
  * is asked for, parsed as a client parses a page (no projection), so each route -- the one-shot
  * search (sync and async), the scroll / PIT / search_after stream, the one-shot `_msearch` and the
  * per-leg `UNION ALL` -- builds its rows with core's own derivation of the projection. The pages
  * are what Elasticsearch 8.18.3 answers for a `_source` restricted to the column (MEASURED, T0-2):
  * a flat spelling stays flat, a flat `null` beside the nested value is what main's INSERT ...
  * SELECT writes, an array of objects is returned whole.
  */
class NestedPathColumnsSpec
    extends AnyFlatSpec
    with Matchers
    with BeforeAndAfterAll
    with ElasticConversion {

  implicit val system: ActorSystem = ActorSystem("nested-path-columns-spec")

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private def hit(id: String, source: String): String = s"""{"_id":"$id","_source":$source}"""

  private def page(hits: Seq[String]): String =
    s"""{"took":1,"hits":{"total":{"value":${hits.size}},"hits":${hits.mkString("[", ",", "]")}}}"""

  /** `t1`: what Elasticsearch returns for `_source: ["obj.a"]`, by document (D shapes). */
  private val dotted: Seq[(String, String)] = Seq(
    "d1"  -> """{"obj":{"a":1}}""", // D-n
    "d2"  -> """{"obj.a":2}""", // D-f
    "d3"  -> """{"obj.a":3,"obj":{"a":30}}""", // D-b
    "d4"  -> """{"obj.a":null,"obj":{"a":4}}""", // D-z
    "d4n" -> """{"obj":{"a":4}}""", // D-n4
    "d5"  -> """{"obj":[{"a":5},{"a":50}]}""", // D-l
    "d6"  -> """{}""", // D-s (the sibling is filtered out by the includes)
    "d6w" -> """{"obj":{"b":6}}""", // D-s read with the whole `_source` (window base rows)
    "d7"  -> """{}""" // D-m
  )

  /** `t3`: the same for `_source: ["obj.x.y"]`. */
  private val deep: Seq[(String, String)] = Seq(
    "d9"  -> """{"obj":{"x":{"y":9}}}""", // D-x
    "d10" -> """{"obj":{"x.y":10}}""" // D-mx
  )

  /** `t2`: `_source: ["n"]`. */
  private val plain: Seq[(String, String)] = Seq(
    "n1" -> """{"n":-2}""",
    "n2" -> """{"n":10}""",
    "n3" -> """{}"""
  )

  private val pages: Map[String, String] = Map(
    "t1" -> page(dotted.map((hit _).tupled)),
    "t2" -> page(plain.map((hit _).tupled)),
    "t3" -> page(deep.map((hit _).tupled))
  )

  /** Every route ends in this double: a canned page per index, parsed with no projection, exactly
    * as the clients parse their pages (`parseResponseTree(json, fieldAliases, ListMap.empty)`).
    */
  private class PagedClient extends NopeClientApi {
    override protected def logger: Logger = LoggerFactory.getLogger("NestedPathColumnsSpec")
    override def version: ElasticResult[String] = ElasticSuccess("8.18.3")

    /** The projection `search` / `scroll` hand to the row sites, as they derive it. */
    def projection(
      single: SingleSearch
    ): (Seq[String], ListMap[String, String], Map[String, Seq[(String, String)]]) =
      (extractOutputFieldNames(single), single.fieldAliases, single.nestedHitsMappings)

    private def tree(q: ElasticQuery): JsonNode = mapper.readTree(pages(q.indices.head))

    private def stream(
      q: ElasticQuery,
      aliases: ListMap[String, String],
      ctx: ConversionContext
    ): Source[ListMap[String, Any], NotUsed] =
      Source(parseResponseTree(tree(q), aliases, ListMap.empty)(ctx).get.toList)

    override private[client] def pitSearchAfter(
      elasticQuery: ElasticQuery,
      fieldAliases: ListMap[String, String],
      config: ScrollConfig,
      hasSorts: Boolean
    )(implicit
      system: ActorSystem,
      context: ConversionContext
    ): Source[ListMap[String, Any], NotUsed] = stream(elasticQuery, fieldAliases, context)

    override private[client] def searchAfter(
      elasticQuery: ElasticQuery,
      fieldAliases: ListMap[String, String],
      config: ScrollConfig,
      hasSorts: Boolean
    )(implicit
      system: ActorSystem,
      context: ConversionContext
    ): Source[ListMap[String, Any], NotUsed] = stream(elasticQuery, fieldAliases, context)

    override private[client] def scrollClassic(
      elasticQuery: ElasticQuery,
      fieldAliases: ListMap[String, String],
      aggregations: ListMap[String, SQLAggregation],
      config: ScrollConfig
    )(implicit
      system: ActorSystem,
      context: ConversionContext
    ): Source[ListMap[String, Any], NotUsed] = stream(elasticQuery, fieldAliases, context)

    override private[client] def executeSingleSearch(
      elasticQuery: ElasticQuery
    ): ElasticResult[Option[JsonNode]] = ElasticSuccess(Some(tree(elasticQuery)))

    override private[client] def executeSingleSearchAsync(elasticQuery: ElasticQuery)(implicit
      ec: scala.concurrent.ExecutionContext
    ): scala.concurrent.Future[ElasticResult[Option[JsonNode]]] =
      scala.concurrent.Future.successful(ElasticSuccess(Some(tree(elasticQuery))))

    override private[client] def executeMultiSearch(
      elasticQueries: ElasticQueries
    ): ElasticResult[Option[JsonNode]] =
      ElasticSuccess(
        Some(
          mapper.readTree(
            elasticQueries.queries
              .map(q => pages(q.indices.head))
              .mkString(
                """{"responses":[""",
                ",",
                "]}"
              )
          )
        )
      )
  }

  private lazy val client = new PagedClient

  /** The routes a row-shaped statement takes (spec §4 R): P1 sync / async (LIMIT 10 => one-shot
    * search), P2 (no LIMIT => scroll / PIT; ORDER BY => search_after; the stream itself).
    */
  private val routes: Seq[String] = Seq("P1", "P1-async", "P2", "P2-ordered", "P2-stream")

  private def rows(
    sql: String,
    route: String,
    ctx: ConversionContext = NativeContext
  ): Seq[ListMap[String, Any]] = {
    implicit val ec: scala.concurrent.ExecutionContext = system.dispatcher
    def ok(r: ElasticResult[ElasticResponse]): Seq[ListMap[String, Any]] = r match {
      case ElasticSuccess(response) => response.results
      case ElasticFailure(error)    => fail(s"[$route] $sql: ${error.message}")
    }
    route match {
      case "P1" => ok(client.search(SelectStatement(s"$sql LIMIT 10"))(ctx))
      case "P1-async" =>
        ok(Await.result(client.searchAsync(SelectStatement(s"$sql LIMIT 10"))(ec, ctx), 30.seconds))
      case "P2"         => ok(client.search(SelectStatement(sql))(ctx))
      case "P2-ordered" => ok(client.search(SelectStatement(s"$sql ORDER BY id"))(ctx))
      case "P2-stream" =>
        Await.result(
          client.scroll(SelectStatement(sql))(system, ctx).map(_._1).runWith(Sink.seq),
          30.seconds
        )
    }
  }

  private def cls(v: Any): String = if (v == null) "null" else v.getClass.getName

  // ---------------------------------------------------------------------------------------------

  "An un-aliased dotted column" should "carry the value its aliased form carries, on every route" in {
    // D-z is the one exception (A-3): a flat null holds no value in Elasticsearch, so the reader
    // walks to 4, where the alias loop keeps the flat null
    val expected = Seq[Any](1, 2, 3, 4, 4, null, null, null, null)
    routes.foreach { route =>
      val s1 = rows("SELECT obj.a FROM t1", route)
      val c1 = rows("SELECT obj.a AS k FROM t1", route)
      withClue(s"[$route] ") {
        s1.map(_.get("obj.a").orNull) shouldBe expected
        s1.zip(c1).zip(dotted.map(_._1)).foreach { case ((s, c), id) =>
          withClue(s"$id: ") {
            if (id == "d4") c("k") shouldBe (null: Any)
            else {
              s("obj.a") shouldBe c("k")
              cls(s("obj.a")) shouldBe cls(c("k"))
            }
          }
        }
      }
    }
  }

  it should "drop the parent the reader read, and keep every parent that may carry a value" in {
    routes.foreach { route =>
      val s1 = rows("SELECT obj.a FROM t1", route)
      withClue(s"[$route] ") {
        s1.map(_.toList) shouldBe Seq(
          List("obj.a" -> 1),
          List("obj.a" -> 2),
          // a non-null flat spelling won: the walk did not read `obj`, which stays as on main
          List("obj.a" -> 3, "obj" -> Map("a" -> 30)),
          List("obj.a" -> 4),
          List("obj.a" -> 4),
          // an array of objects: the walk met a list, the parent is the value's only carrier
          List("obj.a" -> null, "obj" -> List(Map("a" -> 5), Map("a" -> 50))),
          List("obj.a" -> null),
          // a sibling missing from the document loses nothing: the parent goes
          List("obj.a" -> null),
          List("obj.a" -> null)
        )
      }
    }
  }

  it should "walk a deeper path, and keep the parent of a mixed spelling" in {
    routes.foreach { route =>
      withClue(s"[$route] ") {
        rows("SELECT obj.x.y FROM t3", route).map(_.toList) shouldBe Seq(
          List("obj.x.y" -> 9),
          List("obj.x.y" -> null, "obj" -> Map("x.y" -> 10))
        )
      }
    }
  }

  it should "place itself in SELECT order beside a plain column" in {
    routes.foreach { route =>
      withClue(s"[$route] ") {
        rows("SELECT id, obj.a FROM t1", route).take(4).map(_.keys.toList) shouldBe Seq(
          List("id", "obj.a"),
          List("id", "obj.a"),
          List("id", "obj.a", "obj"),
          List("id", "obj.a")
        )
      }
    }
  }

  it should "keep the parent requested, aliased under it, renamed by an alias or under a star" in {
    val d1 = """{"obj":{"a":1,"b":6},"n":3,"a":7}"""
    def one(sql: String): ListMap[String, Any] = {
      val c = new PagedClient {
        override private[client] def executeSingleSearch(
          q: ElasticQuery
        ): ElasticResult[Option[JsonNode]] =
          ElasticSuccess(Some(mapper.readTree(page(Seq(hit("d1", d1))))))
      }
      c.search(SelectStatement(s"$sql LIMIT 10"))(NativeContext) match {
        case ElasticSuccess(r) => r.results.head
        case ElasticFailure(e) => fail(s"$sql: ${e.message}")
      }
    }
    // S5: `obj` is requested
    one("SELECT obj, obj.a FROM t1").toList shouldBe
    List("obj" -> Map("a" -> 1, "b" -> 6), "obj.a" -> 1, "n" -> 3, "a" -> 7)
    // S4: an aliased source lies under `obj` (A-5)
    val s4 = one("SELECT obj.a, obj.b AS k FROM t1")
    s4("obj.a") shouldBe 1
    s4("k") shouldBe 6
    s4 should contain key "obj"
    // S6: the alias twin reads its alias (A-6), and the parent stays
    val s6 = one("SELECT obj.a, obj.a AS k FROM t1")
    s6("obj.a") shouldBe 1
    s6("k") shouldBe 1
    s6 should contain key "obj"
    // U5: a segment of the column is an alias key -- the rename reaches `obj.a` (A-5)
    one("SELECT a AS z, obj.a FROM t1") should contain key "obj"
    // U3: a star keeps every parent (D5 c)
    val u3 = one("SELECT *, obj.a FROM t1")
    u3("obj.a") shouldBe 1
    u3 should contain key "obj"
    // the plain form drops it
    one("SELECT obj.a FROM t1").toList shouldBe List("obj.a" -> 1, "n" -> 3, "a" -> 7)
  }

  it should "read the value in the entity context, and keep the document's shape there" in {
    val entity = rows("SELECT obj.a FROM t1", "P1", EntityContext)
    entity.map(_.get("obj.a").orNull) shouldBe Seq[Any](1, 2, 3, 4, 4, null, null, null, null)
    // an absent value stays ABSENT under EntityContext (SubqueryResolver's contract)
    entity(5) should not contain key("obj.a")
    entity(6) should not contain key("obj.a")
    entity.head should contain key "obj"
    entity(3) should contain key "obj"
  }

  "A UNION ALL" should "carry an un-aliased dotted leg's values, one-shot and leg by leg" in {
    Seq("" -> "P4 (per leg, scroll)", " LIMIT 10" -> "P3 (one-shot _msearch)").foreach {
      case (limit, route) =>
        withClue(s"[$route] ") {
          val s8 = client.search(
            SelectStatement(s"SELECT obj.a FROM t1$limit UNION ALL SELECT n FROM t2$limit")
          )(NativeContext) match {
            case ElasticSuccess(r) => r.results
            case ElasticFailure(e) => fail(e.message)
          }
          s8.map(_.get("obj.a").orNull) shouldBe Seq[Any](1, 2, 3, 4, 4, null, null, null, null, -2,
            10, null)
          s8.take(2).map(_.keys.toList) shouldBe Seq(List("obj.a"), List("obj.a"))
          val s9 = client.search(
            SelectStatement(s"SELECT n FROM t2$limit UNION ALL SELECT obj.a FROM t1$limit")
          )(NativeContext) match {
            case ElasticSuccess(r) => r.results
            case ElasticFailure(e) => fail(e.message)
          }
          s9.map(_.get("n").orNull) shouldBe Seq[Any](-2, 10, null, 1, 2, 3, 4, 4, null, null, null,
            null)
        }
    }
  }

  it should "convert a dotted leg's value to the result's type in the one rebuild (S10)" in {
    val legs = mapper.readTree(s"""{"responses":[${pages("t1")},${pages("t2")}]}""")
    val toDouble: Any => Any = {
      case null                => null
      case n: java.lang.Number => n.doubleValue()
      case other               => other
    }
    val parsed = parseResponseTree(
      legs,
      ListMap.empty,
      ListMap.empty,
      Seq("obj.a"),
      legProjections = Seq(
        LegProjection(ListMap.empty, Seq("obj.a"), Map.empty, ListMap.empty, Some(Array(toDouble))),
        LegProjection(ListMap.empty, Seq("n"), Map.empty, ListMap.empty, Some(Array(toDouble)))
      )
    )(NativeContext).get
    parsed.map(_.get("obj.a").orNull) shouldBe Seq[Any](1.0, 2.0, 3.0, 4.0, 4.0, null, null, null,
      null, -2.0, 10.0, null)
    parsed.head("obj.a") shouldBe a[java.lang.Double]
  }

  // ---- the plan, decided once per stream (D2, D5, D7) -----------------------------------------

  private def legs(sql: String): Seq[SingleSearch] = Parser(sql) match {
    case Right(single: SingleSearch) => Seq(single)
    case Right(multi: MultiSearch)   => multi.requests
    case other                       => fail(s"$sql: $other")
  }

  /** Per leg: (walked paths, droppable parents, alias twins), or None when nothing qualifies. */
  private def plan(
    sql: String,
    ctx: ConversionContext = NativeContext
  ): Seq[Option[(List[String], List[String], List[String])]] =
    legs(sql).map { leg =>
      val (fields, aliases, nested) = client.projection(leg)
      nestedPathColumns(fields, aliases, nested)(ctx).map(p =>
        (p.paths.toList, p.parents.toList, p.aliasOf.toList.filter(_ ne null))
      )
    }

  private val t = "dotted_cols"

  "The nested-path plan" should "select exactly the un-aliased dotted columns, and the parents it may drop" in {
    def walks(paths: String*)(parents: String*) = Some((paths.toList, parents.toList, Nil))
    val expected: Seq[(String, Seq[Option[(List[String], List[String], List[String])]])] = Seq(
      s"SELECT obj.a FROM $t"     -> Seq(walks("obj.a")("obj")),
      s"SELECT id, obj.a FROM $t" -> Seq(walks("obj.a")("obj")),
      s"SELECT obj.x.y FROM $t"   -> Seq(walks("obj.x.y")("obj")),
      // S4: an aliased source under `obj` keeps it (A-5)
      s"SELECT obj.a, obj.b AS k FROM $t" -> Seq(walks("obj.a")()),
      // S5: `obj` is requested
      s"SELECT obj, obj.a FROM $t" -> Seq(walks("obj.a")()),
      // S6: the alias twin reads `k` (A-6), and keeps `obj`
      s"SELECT obj.a, obj.a AS k FROM $t" -> Seq(Some((List("obj.a"), Nil, List("k")))),
      s"""SELECT "obj.a" FROM $t"""       -> Seq(walks("obj.a")("obj")),
      s"SELECT obj.a FROM $t UNION ALL SELECT n FROM $t"   -> Seq(walks("obj.a")("obj"), None),
      s"SELECT n FROM $t UNION ALL SELECT obj.a FROM $t"   -> Seq(None, walks("obj.a")("obj")),
      s"SELECT obj.a FROM $t UNION ALL SELECT dbl FROM $t" -> Seq(walks("obj.a")("obj"), None),
      // S11 / U4-U6: an output named `obj`, an alias key among the segments; `obj.a` an alias
      s"SELECT n AS obj, obj.a FROM $t"         -> Seq(walks("obj.a")()),
      s"SELECT a AS z, obj.a FROM $t"           -> Seq(walks("obj.a")()),
      s"""SELECT n AS "obj.a", obj.b FROM $t""" -> Seq(walks("obj.b")("obj")),
      // U1 / U2: the qualifier is stripped at parse time; U3: a star keeps every parent
      s"SELECT t.obj.a FROM $t AS t" -> Seq(walks("obj.a")("obj")),
      s"SELECT $t.obj.a FROM $t"     -> Seq(walks("obj.a")("obj")),
      s"SELECT *, obj.a FROM $t"     -> Seq(walks("obj.a")()),
      // C6 and C7b name `obj.a` (site 1 never reads a bucket row; C7b is an S row, D5 d)
      s"SELECT obj.a, COUNT(*) AS c FROM $t GROUP BY obj.a" -> Seq(walks("obj.a")("obj")),
      s"SELECT t.id, t.obj.a, c.name FROM $t t JOIN UNNEST(t.children) AS c" ->
      Seq(walks("obj.a")("obj")),
      s"SELECT id, obj.a, ROW_NUMBER() OVER (PARTITION BY obj.a ORDER BY id) AS rn FROM $t" ->
      Seq(walks("obj.a")("obj"))
    ) ++ controls.map(c => c -> legs(c).map(_ => None))
    expected.foreach { case (sql, want) => withClue(s"[$sql] ") { plan(sql) shouldBe want } }
  }

  /** C1-C5, C7a, C7c, C8: no un-aliased dotted column. */
  private lazy val controls: Seq[String] = Seq(
    s"SELECT obj.a AS k FROM $t",
    s"""SELECT obj.a AS "obj.a" FROM $t""",
    s"SELECT n FROM $t",
    s"SELECT n AS m FROM $t",
    s"SELECT n + 1 FROM $t",
    s"SELECT ABS(obj.a) FROM $t",
    s"SELECT obj.a::BIGINT FROM $t",
    s"SELECT obj.a + 1 FROM $t",
    s"SELECT obj FROM $t",
    s"SELECT t.id, c.name FROM $t t JOIN UNNEST(t.children) AS c",
    s"SELECT t.id, c.addr.city FROM $t t JOIN UNNEST(t.children) AS c",
    s"SELECT * FROM $t",
    s"SELECT obj.a AS k FROM $t UNION ALL SELECT n FROM $t"
  )

  it should "drop no parent in the entity context, and read nothing under DocumentContext" in {
    plan(s"SELECT obj.a FROM $t", EntityContext) shouldBe Seq(Some((List("obj.a"), Nil, Nil)))
    plan(s"SELECT obj.a FROM $t", DocumentContext) shouldBe Seq(None)
  }

  it should "hand back the projector INSTANCE when nothing qualifies (D7)" in {
    val project: ListMap[String, Any] => ListMap[String, Any] = row => row
    (controls.flatMap(legs) ++ legs(s"SELECT obj.a FROM $t")).foreach { leg =>
      val (fields, aliases, nested) = client.projection(leg)
      withClue(s"[${leg.sql}] ") {
        withNestedPaths(fields, aliases, nested)(project)(
          DocumentContext
        ) should be theSameInstanceAs project
      }
    }
    controls.flatMap(legs).foreach { leg =>
      val (fields, aliases, nested) = client.projection(leg)
      withClue(s"[${leg.sql}] ") {
        withNestedPaths(fields, aliases, nested)(project)(
          NativeContext
        ) should be theSameInstanceAs project
        withNestedPaths(fields, aliases, nested)(project)(
          EntityContext
        ) should be theSameInstanceAs project
      }
    }
  }

  it should "keep a parent past the 64th, which the per-row mask cannot track" in {
    val paths = (0 to 64).map(i => s"p$i.a")
    val p = nestedPathColumns(paths, ListMap.empty, Map.empty)(NativeContext).get
    p.parents.length shouldBe 64
    p.parentOf(63) shouldBe 63
    p.parentOf(64) shouldBe -1
    val row =
      ListMap[String, Any](paths.map(_.takeWhile(_ != '.')).map(k => k -> Map("a" -> 1)): _*)
    val read = p(row)
    read.keySet should not contain "p0"
    read("p64") shouldBe Map("a" -> 1)
    read("p64.a") shouldBe 1
  }

  "The reader" should "classify each miss: absent value, list, scalar, dotted key" in {
    val p = nestedPathColumns(Seq("obj.a", "obj.x.y"), ListMap.empty, Map.empty)(NativeContext).get
    def obj(v: Any) = ListMap[String, Any]("obj" -> v)
    // both read: the parent goes
    p(obj(Map("a" -> 1, "x" -> Map("y" -> 2)))).toList shouldBe List("obj.a" -> 1, "obj.x.y" -> 2)
    // `obj.x.y` stops on an absent key in objects without a dotted key: still read
    p(obj(Map("a" -> 1, "x" -> Map("z" -> 2)))).toList shouldBe List("obj.a" -> 1)
    // a list before the leaf: not read, kept
    p(obj(Map("a" -> 1, "x" -> List(Map("y" -> 2))))).keySet should contain("obj")
    // a dotted key in an object on the way: not read, kept
    p(obj(Map("a" -> 1, "x.y" -> 3))).keySet should contain("obj")
    // a scalar parent: nothing is walked, nothing is dropped (D5 b)
    p(obj(5)).toList shouldBe List("obj" -> 5)
    // a non-null flat key wins and the row is left as it is
    val flat = ListMap[String, Any]("obj.a" -> 7, "obj" -> Map("a" -> 1, "x" -> Map("y" -> 2)))
    p(flat).toList shouldBe flat.toList :+ ("obj.x.y" -> 2)
    // a flat null falls through to the walk, which replaces it
    p(ListMap[String, Any]("obj.a" -> null, "obj" -> Map("a" -> 4)))("obj.a") shouldBe 4
  }

  it should "leave every aggregation row of a GROUP BY over the column as the projector builds it" in {
    val grouped = """{
                    |  "took": 1,
                    |  "hits": { "total": { "value": 3 }, "hits": [] },
                    |  "aggregations": {
                    |    "obj.a": {
                    |      "buckets": [
                    |        { "key": 1, "doc_count": 2, "c": { "value": 2 } },
                    |        { "key": 4, "doc_count": 1, "c": { "value": 1 } }
                    |      ]
                    |    }
                    |  }
                    |}""".stripMargin
    parseResponseTree(mapper.readTree(grouped), ListMap.empty, ListMap.empty, Seq("obj.a", "c"))(
      NativeContext
    ).get.map(_.toList) shouldBe Seq(List("obj.a" -> 1, "c" -> 2), List("obj.a" -> 4, "c" -> 1))
  }

  it should "append the row's extras after the requested columns, the parent once dropped" in {
    val whole = page(Seq(hit("d1", """{"id":"d1","n":3,"obj":{"a":1,"b":6}}""")))
    parseResponseTree(mapper.readTree(whole), ListMap.empty, ListMap.empty, Seq("obj.a", "id"))(
      NativeContext
    ).get.map(_.toList) shouldBe Seq(List("obj.a" -> 1, "id" -> "d1", "n" -> 3))
    // the same row under DocumentContext: main's document shape, the null cell included
    parseResponseTree(mapper.readTree(whole), ListMap.empty, ListMap.empty, Seq("obj.a", "id"))(
      DocumentContext
    ).get.map(_.toList) shouldBe Seq(
      List("obj.a" -> null, "id" -> "d1", "n" -> 3, "obj" -> Map("a" -> 1, "b" -> 6))
    )
  }
}
