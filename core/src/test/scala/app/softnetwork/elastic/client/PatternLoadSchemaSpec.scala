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

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql.schema.TableAlias
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration._

/** `loadSchema` of an index PATTERN or a comma list answers the merged schema of the indices it
  * matches, on a COLD cache as on a warm one. Before, the single-index path refused it on a cold
  * cache (it is not a concrete index name) and answered the entry `loadPatternSchema` had written
  * on a warm one, so DESCRIBE, SHOW CREATE TABLE or a JOIN leg over `logs-*` answered or failed
  * depending on which statement had run before it.
  */
class PatternLoadSchemaSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("pattern-load-schema-spec")

  override def afterAll(): Unit = {
    Await.result(system.terminate(), 10.seconds)
    super.afterAll()
  }

  private def document(extra: String): String =
    s"""{"aliases":{},"mappings":{"properties":{
       |  "id":{"type":"keyword"},"event_ts":{"type":"date"}$extra}},
       |"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}""".stripMargin

  /** `GET <pattern>` answers every matched index, keyed by its name. */
  private class PatternClient extends NopeClientApi {
    override protected def logger: Logger = LoggerFactory.getLogger(getClass)

    @volatile var gets: List[String] = Nil
    @volatile var created: List[String] = Nil
    @volatile var deleted: List[String] = Nil

    override private[client] def executeVersion(): ElasticResult[String] =
      ElasticResult.success("8.18.3")

    /** `logs_1` exists, so a CTAS into it takes the OR REPLACE route. */
    override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
      ElasticResult.success(index == "logs_1")

    override private[client] def executeCreateIndex(
      index: String,
      settings: String,
      mappings: Option[String],
      aliases: Seq[TableAlias]
    ): ElasticResult[Boolean] = {
      created = created :+ index
      ElasticResult.success(true)
    }

    override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] = {
      deleted = deleted :+ index
      ElasticResult.success(true)
    }

    override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] = {
      gets = gets :+ index
      index match {
        case "logs_*" => ElasticResult.success(Some(s"""{"logs_1":${document("")}}"""))
        case name @ ("other" | "other2") =>
          ElasticResult.success(Some(s"""{"$name":${document("")}}"""))
        case "logs-*" | "logs-1,logs-2" =>
          ElasticResult.success(
            Some(
              s"""{"logs-1":${document("")},"logs-2":${document(
                ""","level":{"type":"keyword"}"""
              )}}"""
            )
          )
        case _ => ElasticResult.success(None)
      }
    }
  }

  "loadSchema of an index pattern or a list" should "answer the merged schema on a cold cache" in {
    Seq("logs-*", "logs-1,logs-2").foreach { source =>
      withClue(s"$source: ") {
        val client = new PatternClient // a fresh client: nothing cached
        client.schemaCacheSize shouldBe 0
        client.loadSchema(source) match {
          case ElasticSuccess(schema) =>
            schema.columns.map(_.name).sorted shouldBe List("event_ts", "id", "level")
          case ElasticFailure(error) => fail(s"refused: ${error.message}")
        }
      }
    }
  }

  it should "be ONE cache entry with the pattern read: one GET, one entry" in {
    val client = new PatternClient
    client.loadSchema("logs-*") shouldBe a[ElasticSuccess[_]]
    client.loadPatternSchema("logs-*") shouldBe a[ElasticSuccess[_]]
    client.gets shouldBe List("logs-*")
    client.schemaCacheSize shouldBe 1
  }

  "CREATE OR REPLACE TABLE … AS SELECT over a pattern that matches the target" should
  "be refused before the target is touched" in {
    // it would delete `logs_1` and refill it from `logs_*` without `logs_1`'s own rows
    val client = new PatternClient
    Await.result(
      client.run("CREATE OR REPLACE TABLE logs_1 AS SELECT id, event_ts FROM logs_*"),
      10.seconds
    ) match {
      case ElasticFailure(error) =>
        error.message shouldBe "CREATE TABLE logs_1 cannot select from itself."
        error.statusCode shouldBe Some(400)
      case ElasticSuccess(other) => fail(s"expected the refusal, got $other")
    }
    client.deleted shouldBe Nil
    client.created shouldBe Nil
  }

  "CREATE TABLE … AS SELECT into a NEW name the pattern matches" should "still run" in {
    val client = new PatternClient
    val result =
      Await.result(
        client.run("CREATE TABLE logs_2 AS SELECT id, event_ts FROM logs_*"),
        10.seconds
      )
    result match {
      case ElasticFailure(error) => error.message should not include "cannot select from itself"
      case ElasticSuccess(_)     =>
    }
    client.created shouldBe List("logs_2")
    client.deleted shouldBe Nil
  }

  private def ctas(client: PatternClient, sql: String): ElasticResult[_] =
    Await.result(client.run(sql), 10.seconds)

  private def refusedAsSelfRead(result: ElasticResult[_]): Boolean = result match {
    case ElasticFailure(error) => error.message.contains("cannot select from itself")
    case _                     => false
  }

  "CREATE OR REPLACE TABLE … AS SELECT over a pattern that EXCLUDES the target" should
  "not be refused: it reads nothing it destroys" in {
    val client = new PatternClient
    val result =
      ctas(client, "CREATE OR REPLACE TABLE logs_1 AS SELECT id, event_ts FROM logs_*,-logs_1")
    withClue(s"$result: ") { refusedAsSelfRead(result) shouldBe false }
    client.deleted shouldBe List("logs_1") // the OR REPLACE route ran
  }

  "CREATE OR REPLACE TABLE … AS SELECT whose WHERE subquery reads the target" should
  "be refused before the target is touched" in {
    // the route would delete `logs_1`, then the subquery would read the emptied index
    Seq(
      "CREATE OR REPLACE TABLE logs_1 AS SELECT id, event_ts FROM other WHERE id IN (SELECT id FROM logs_1)",
      "CREATE OR REPLACE TABLE logs_1 AS SELECT id, event_ts FROM other WHERE id IN (SELECT id FROM logs_*)"
    ).foreach { sql =>
      val client = new PatternClient
      val result = ctas(client, sql)
      withClue(s"[$sql] $result: ") {
        refusedAsSelfRead(result) shouldBe true
        client.deleted shouldBe Nil
        client.created shouldBe Nil
      }
    }
  }

  it should "still run when the subquery reads another index" in {
    val client = new PatternClient
    val result = ctas(
      client,
      "CREATE OR REPLACE TABLE logs_1 AS SELECT id, event_ts FROM other WHERE id IN (SELECT id FROM other2)"
    )
    withClue(s"$result: ") { refusedAsSelfRead(result) shouldBe false }
    client.deleted shouldBe List("logs_1")
  }

  // ------------------------------------------------------------------------------------------
  // SCHEMA-7 (D7) -- the `_field_caps` fallback of a QUERY's schema
  // ------------------------------------------------------------------------------------------

  /** `GET s7_t/_field_caps?fields=*` as Elasticsearch 8.18.3 answered a `read`-only user (trimmed).
    */
  private val capsTable =
    """{"indices":["s7_t"],"fields":{"_id":{"_id":{"type":"_id","metadata_field":true}},""" +
    """"id":{"keyword":{"type":"keyword","metadata_field":false}},""" +
    """"ip":{"ip":{"type":"ip","metadata_field":false}},""" +
    """"d":{"date":{"type":"date","metadata_field":false}}}}"""

  /** `GET s7_p*` with `_field_caps`: `k` a keyword in `s7_p1`, a long in `s7_p2`. */
  private val capsPattern =
    """{"indices":["s7_p1","s7_p2"],"fields":{"k":{""" +
    """"keyword":{"type":"keyword","metadata_field":false,"indices":["s7_p1"]},""" +
    """"long":{"type":"long","metadata_field":false,"indices":["s7_p2"]}}}}"""

  private val capsNone = """{"indices":[],"fields":{}}"""

  private def forbidden(index: String): ElasticResult[Option[String]] =
    ElasticFailure(
      ElasticError(
        message = s"action [indices:admin/get] is unauthorized for user [ro] on indices [$index]",
        statusCode = Some(403),
        index = Some(index)
      )
    )

  /** A user holding only the index `read` privilege: `GET <index | pattern>` answers 403,
    * `_field_caps` 200. Counts both.
    */
  private class ReadOnlyClient extends NopeClientApi {
    override protected def logger: Logger = LoggerFactory.getLogger(getClass)
    val gets = new java.util.concurrent.atomic.AtomicInteger(0)
    val caps = new java.util.concurrent.atomic.AtomicInteger(0)

    override private[client] def executeVersion(): ElasticResult[String] =
      ElasticResult.success("8.18.3")

    override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] = {
      gets.incrementAndGet()
      forbidden(index)
    }

    override private[client] def executeFieldCaps(source: String): ElasticResult[Option[String]] = {
      caps.incrementAndGet()
      source match {
        case "s7_t"  => ElasticResult.success(Some(capsTable))
        case "s7_p*" => ElasticResult.success(Some(capsPattern))
        case _       => ElasticResult.success(Some(capsNone))
      }
    }
  }

  /** [[PatternClient]], counting the `_field_caps` requests it is asked. */
  private class CountingPatternClient extends PatternClient {
    val caps = new java.util.concurrent.atomic.AtomicInteger(0)
    override private[client] def executeFieldCaps(source: String): ElasticResult[Option[String]] = {
      caps.incrementAndGet()
      ElasticResult.success(Some(capsNone))
    }
  }

  private def statement(sql: String): app.softnetwork.elastic.sql.query.SingleSearch =
    app.softnetwork.elastic.sql.parser.Parser(sql) match {
      case Right(ss: app.softnetwork.elastic.sql.query.SingleSearch) => ss
      case other                                                     => fail(s"[$sql] $other")
    }

  "a read-only user's query schema" should "be typed from _field_caps once per TTL, then cost nothing" in {
    Seq("s7_t" -> "id", "s7_p*" -> "k").foreach { case (source, column) =>
      withClue(s"$source: ") {
        val client = new ReadOnlyClient
        client.loadQuerySchema(source) match {
          case ElasticSuccess(schema) =>
            schema.tableType shouldBe app.softnetwork.elastic.sql.schema.TableType.FieldCapabilities
            schema.find(column).map(_.dataType) shouldBe Some(
              app.softnetwork.elastic.sql.`type`.SQLTypes.Keyword
            )
          case ElasticFailure(error) => fail(s"refused: ${error.message}")
        }
        (client.gets.get(), client.caps.get()) shouldBe ((1, 1))
        // every later statement: zero round trips, through the seam every query consumer reads
        client.loadQuerySchema(source) shouldBe a[ElasticSuccess[_]]
        client.resolveWithSchema(statement(s"SELECT $column FROM $source")) shouldBe
        a[ElasticSuccess[_]]
        client.resolveWithSchema(statement(s"SELECT $column FROM $source")) shouldBe
        a[ElasticSuccess[_]]
        (client.gets.get(), client.caps.get()) shouldBe ((1, 1))
        client.schemaCacheSize shouldBe 1
      }
    }
  }

  it should "type its fields as the privileged client would: the ip keeps its mapping type, a pattern merges" in {
    val client = new ReadOnlyClient
    val table = client.loadQuerySchema("s7_t").toOption.getOrElse(fail("no schema"))
    table.find("ip").map(_.mappingType) shouldBe Some("ip")
    table.find("d").flatMap(_.options.get("format")).map(_.value) shouldBe Some(
      app.softnetwork.elastic.sql.query.TemporalLiterals.FieldCapsDateFormat
    )
    table.find("_id") shouldBe None
  }

  it should "remember a pattern that matches no index as the 404 main answered" in {
    val client = new ReadOnlyClient
    client.loadQuerySchema("none_*") match {
      case ElasticFailure(error) => error.statusCode shouldBe Some(404)
      case other                 => fail(s"expected a 404, got $other")
    }
    client.schemaCacheSize shouldBe 0
    // through the seam: the miss is remembered for its TTL, no request is sent again
    client.resolveWithSchema(statement("SELECT id FROM none_*"))
    val (gets, caps) = (client.gets.get(), client.caps.get())
    client.resolveWithSchema(statement("SELECT id FROM none_*"))
    (client.gets.get(), client.caps.get()) shouldBe ((gets, caps))
  }

  it should "keep DDL on the mapping: loadSchema still fails 403, and leaves the entry" in {
    val client = new ReadOnlyClient
    client.loadQuerySchema("s7_t") shouldBe a[ElasticSuccess[_]]
    client.loadSchema("s7_t") match {
      case ElasticFailure(error) => error.statusCode shouldBe Some(403)
      case ElasticSuccess(schema) =>
        fail(s"DDL answered the _field_caps schema (${schema.tableType})")
    }
    client.loadPatternSchema("s7_t,s7_p*") match {
      case ElasticFailure(error) => error.statusCode shouldBe Some(403)
      case other                 => fail(s"expected the 403, got $other")
    }
    (client.gets.get(), client.caps.get()) shouldBe ((3, 1))
    // the query entry is still there
    client.loadQuerySchema("s7_t") shouldBe a[ElasticSuccess[_]]
    (client.gets.get(), client.caps.get()) shouldBe ((3, 1))
    // and invalidation drops it, as it drops a mapping's
    client.invalidateSchema("s7_t")
    client.loadQuerySchema("s7_t") shouldBe a[ElasticSuccess[_]]
    (client.gets.get(), client.caps.get()) shouldBe ((4, 2))
  }

  "a privileged user's query schema" should "cost what it cost on main: one GET, no _field_caps" in {
    val client = new CountingPatternClient
    client.loadQuerySchema("logs-*") match {
      case ElasticSuccess(schema) =>
        schema.columns.map(_.name).sorted shouldBe List("event_ts", "id", "level")
        schema.tableType shouldBe app.softnetwork.elastic.sql.schema.TableType.Regular
      case ElasticFailure(error) => fail(s"refused: ${error.message}")
    }
    client.loadQuerySchema("logs-*") shouldBe a[ElasticSuccess[_]]
    client.loadQuerySchema("other") shouldBe a[ElasticSuccess[_]]
    client.gets shouldBe List("logs-*", "other")
    client.caps.get() shouldBe 0
    // a pattern that matches no index: one `_field_caps` per TTL before its 404 is remembered
    client.resolveWithSchema(statement("SELECT id FROM none_*"))
    client.resolveWithSchema(statement("SELECT id FROM none_*"))
    client.caps.get() shouldBe 1
  }
}
