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
}
