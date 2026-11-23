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

package app.softnetwork.elastic.arrow

import akka.actor.ActorSystem
import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.client.{ElasticClientApi, ResultSchema, SetOperationValues}
import app.softnetwork.elastic.client.SetOperationValues.{LegSchema, LegSlot}
import app.softnetwork.elastic.client.result.{
  ElasticFailure,
  ElasticSuccess,
  QueryResult,
  QueryRows,
  QueryStream,
  QueryStructured
}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.MultiSearch
import app.softnetwork.elastic.sql.query.MultiSearch.SetOperationType
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/** SCHEMA-18 (R-199) -- a `NULL` literal in a set-operation branch, on Elasticsearch 8, in a
  * DOWNSTREAM package as the relational engine reaches it:
  *
  *   - core's own `UNION ALL` answers the column's type there (DuckDB's), its `describe` the schema
  *     it runs, the NULL branch empty or not -- unchanged by the leg's slot, which core's run path
  *     does not read;
  *   - the engine's leg schema (`SetOperationValues.legSchemas`) gives the NULL leg its column's
  *     type, and reads every canonical cell core gives that leg on its own -- plain, aggregation
  *     and un-aliased -- as NULL, under the row key core gives it.
  */
class JavaClientNullSlotSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val table = "s18_t"

  /** Created through core's DDL: `ARRAY<INT>` is recorded in the mapping's `_meta`. */
  private val ddl: String =
    s"CREATE TABLE $table (id KEYWORD, n INT, l BIGINT, x DOUBLE, f REAL, b BOOLEAN, s KEYWORD, " +
    "d DATE, ts TIMESTAMP, c_es TIMESTAMP OPTIONS (format = 'epoch_second'), " +
    "tsz TIMESTAMP OPTIONS (format = 'yyyy-MM-dd HH:mm z'), bin VARBINARY, " +
    "o STRUCT FIELDS(a INT, e TIMESTAMP OPTIONS (format = 'epoch_second')), g GEO_POINT, " +
    "ai ARRAY<INT>, PRIMARY KEY (id))"

  /** Two documents; the second's `o` has no `a` (A-231). */
  private val docs: Seq[(String, String)] = Seq(
    "r1" -> ("""{"id":"r1","n":3,"l":10000000000,"x":1.5,"f":0.5,"b":true,"s":"a",""" +
    """"d":"2024-01-31","ts":"2024-01-31T10:30:00Z","c_es":1706697000,""" +
    """"tsz":"2024-01-31 10:30 CET","bin":"aGk=","o":{"a":1,"e":1706697000},""" +
    """"g":{"lat":1.0,"lon":2.0},"ai":[1,2]}"""),
    "r2" -> ("""{"id":"r2","n":5,"l":7,"x":2.5,"f":1.25,"b":false,"s":"b",""" +
    """"d":"2024-02-01","ts":"2024-02-01T00:00:00Z","c_es":1706700000,""" +
    """"tsz":"2024-01-31 10:30 UTC","bin":"AAE=","o":{"e":1706700000},""" +
    """"g":[4.0,3.0],"ai":[3,4]}""")
  )

  override def beforeAll(): Unit = {
    super.beforeAll()
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $table"), 60.seconds))
    run(ddl)
    docs.foreach { case (id, source) => client.index(table, id, source) }
    client.refresh(table)
    ()
  }

  override def afterAll(): Unit = {
    Try(client.deleteIndex(table))
    super.afterAll()
  }

  private def run(sql: String): QueryResult =
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(result) => result
      case ElasticFailure(error)  => fail(s"[$sql] ${error.message}")
    }

  /** The consumer shape immune to the next arity change, with the schema. */
  private def rowsAndSchema(
    result: QueryResult
  ): (Seq[ListMap[String, Any]], Option[ResultSchema]) =
    result match {
      case q: QueryRows       => (q.rows, q.schema)
      case q: QueryStructured => (q.response.results, q.schema)
      case q: QueryStream =>
        (Await.result(q.stream.map(_._1).runWith(Sink.seq), 60.seconds), q.schema)
      case other => fail(s"$other")
    }

  private def described(sql: String): Option[ResultSchema] =
    client.describe(sql) match {
      case ElasticSuccess(schema) => schema
      case ElasticFailure(error)  => fail(s"[$sql] describe: ${error.message}")
    }

  /** The leg schemas the engine reads: each branch with the table's mapping attached. */
  private def legs(sql: String): Seq[Option[LegSchema]] = {
    val multiple = Parser(sql) match {
      case Right(m: MultiSearch) =>
        m.copy(requests = m.requests.map(_.update(Some(client.loadSchema(table).get))))
      case other => fail(s"[$sql] $other")
    }
    SetOperationValues
      .legSchemas(multiple.requests, multiple.resolvedOperators, Some("8.18.3"))
      .fold(refusal => fail(s"[$sql] $refusal"), identity)
  }

  /** A type as DuckDB names it. */
  private def render(t: SetOperationType): String =
    t.sqlType match {
      case SQLTypes.Struct =>
        t.fields.map(f => s"${f.name} ${render(f.valueType)}").mkString("STRUCT(", ", ", ")")
      case SQLTypes.Array(elementType) =>
        t.element.map(render).getOrElse(elementType.typeId) + "[]"
      case scalar => scalar.typeId
    }

  /** A value and its class, comparable across runs. */
  private def canonical(v: Any): (String, String) =
    v match {
      case null           => ("NULL", "null")
      case b: Array[Byte] => (b.map(x => f"${x & 0xff}%02x").mkString, "bytes")
      case m: scala.collection.Map[_, _] =>
        (
          m.toSeq.map { case (k, x) => s"$k=${canonical(x)._1}" }.sorted.mkString("{", ",", "}"),
          "map"
        )
      case s: scala.collection.Seq[_] => (s.map(canonical(_)._1).mkString("[", ",", "]"), "list")
      case other                      => (other.toString, other.getClass.getName)
    }

  /** Each sibling: the NULL slot's type (= DuckDB's) and the statement's type on core's run (D4: a
    * routed date is answered as stored, `VARCHAR`).
    */
  private val siblings: Seq[(String, String, String)] = Seq(
    ("n", "INT", "INT"),
    ("l", "BIGINT", "BIGINT"),
    ("x", "DOUBLE", "DOUBLE"),
    ("f", "REAL", "REAL"),
    ("b", "BOOLEAN", "BOOLEAN"),
    ("s", "VARCHAR", "VARCHAR"),
    ("d", "DATE", "DATE"),
    ("ts", "TIMESTAMP", "TIMESTAMP"),
    ("c_es", "TIMESTAMP", "TIMESTAMP"),
    ("tsz", "TIMESTAMP", "VARCHAR"),
    ("bin", "VARBINARY", "VARBINARY"),
    ("o", "STRUCT(a INT, e TIMESTAMP)", "STRUCT"),
    ("o.a", "INT", "INT"),
    ("o.e", "TIMESTAMP", "TIMESTAMP"),
    ("g", "GEO_POINT", "GEO_POINT"),
    ("ai", "INT[]", "ARRAY<INT>")
  )

  private val none = "WHERE id = 'none'"

  "Core's UNION ALL" should "answer DuckDB's type at a NULL literal, empty or not" in {
    val wrong = for {
      (sibling, slotType, statementType) <- siblings
      empty                              <- Seq(false, true)
      nullFirst                          <- Seq(true, false)
      problem <- {
        val nullBranch = s"SELECT NULL AS k FROM $table${if (empty) s" $none" else ""}"
        val other = s"SELECT $sibling AS k FROM $table"
        val sql =
          if (nullFirst) s"$nullBranch UNION ALL $other" else s"$other UNION ALL $nullBranch"
        // the control: the sibling beside an empty copy of itself, on the same path
        val control = s"SELECT $sibling AS k FROM $table $none UNION ALL $other"
        val (rows, schema) = rowsAndSchema(run(sql))
        val (controlRows, _) = rowsAndSchema(run(control))
        val expected = controlRows.map(r => canonical(r.getOrElse("k", null))) ++
          (if (empty) Nil else docs.map(_ => canonical(null)))
        val got = rows.map(r => canonical(r.getOrElse("k", null)))
        val nullLeg = if (nullFirst) 0 else 1
        val slot = legs(sql)(nullLeg).flatMap(_.slots.head)
        val describedSchema = described(sql)
        // `!=` binds looser than `->`: each condition is parenthesised
        Seq[(Boolean, String)](
          (schema.map(_.columns.map(_.sqlType.typeId)) != Some(Seq(statementType))) ->
          s"type ${schema.map(_.columns.map(_.sqlType.typeId))} instead of $statementType",
          (describedSchema != schema)     -> s"describe $describedSchema != run $schema",
          (got.sorted != expected.sorted) -> s"values $got instead of $expected",
          (slot.map(s => render(s.sqlType)) != Some(slotType)) ->
          s"NULL slot ${slot.map(s => render(s.sqlType))} instead of $slotType"
        ).collect { case (true, what) => s"[$sql] $what" }
      }
    } yield problem
    wrong shouldBe empty
  }

  "The engine's leg schema" should "read a NULL leg's every canonical cell as NULL, typed as its column" in {
    // each NULL leg run alone through core: its canonical rows, read by its slot under the row key
    // core gives the item (`outputName`, the planner's key -- the un-aliased item's too)
    val shapes: Seq[(String, String, Boolean)] = Seq(
      (
        s"SELECT NULL AS k FROM $table UNION SELECT n AS k FROM $table",
        "plain",
        true
      ),
      (
        s"SELECT NULL AS k, COUNT(*) AS c FROM $table UNION SELECT n AS k, COUNT(*) AS c " +
        s"FROM $table GROUP BY n",
        "aggregation",
        false
      ),
      (
        s"SELECT NULL FROM $table UNION SELECT n FROM $table",
        "un-aliased",
        true
      )
    )
    shapes.foreach { case (sql, shape, scripted) =>
      withClue(s"[$shape: $sql] ") {
        val leg = Parser(sql) match {
          case Right(m: MultiSearch) => m.requests.head
          case other                 => fail(s"$other")
        }
        val key = leg.select.fieldsWithComputedAliases.head.outputName
        val slot: LegSlot = legs(sql).head.flatMap(_.slots.head).getOrElse(fail("no NULL slot"))
        render(slot.sqlType) shouldBe "INT"
        slot.asStored shouldBe Nil
        val (rows, _) = rowsAndSchema(run(leg.sql))
        rows should not be empty
        rows.foreach { row =>
          // the un-aliased item: core keys its cell by the output name the engine reads it by
          if (shape == "un-aliased") row.keySet should contain(key)
          val cell = row.getOrElse(key, null)
          assert(slot.read(cell) == null, s"$cell read")
          // a row query's NULL item is a script field (Elasticsearch's one-element array, which the
          // slot's reader reads); an aggregation's is not, so its canonical cell must be NULL -- a
          // list there would be written as it comes by the engine
          if (scripted)
            assert(
              cell == null || (cell match {
                case s: scala.collection.Seq[_] => s.size == 1 && s.head == null
                case _                          => false
              }),
              s"$cell"
            )
          else assert(cell == null, s"$cell")
        }
      }
    }
  }
}
