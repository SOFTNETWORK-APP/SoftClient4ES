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
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.CreateTable
import app.softnetwork.elastic.sql.schema.{mapper, Table, TableAlias}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable
import scala.concurrent.duration._

/** ALTER TABLE on the reindex path, Docker-free: what is written to which pipeline, and which
  * pipeline the index is left on. The cluster is a stub that keeps the pipelines it is given and
  * records every pipeline write, every settings update and every migration.
  */
class AlterTablePipelineRewriteSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem("alter-table-pipeline-rewrite")
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(10.seconds))

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private val livePipeline = "t_ddl_default_pipeline"

  private val table: Table = Parser(
    "CREATE TABLE t (id KEYWORD, a INT, b INT, c INT SCRIPT AS (a + b), PRIMARY KEY (id))"
  ) match {
    case Right(ct: CreateTable) => ct.schema.setDefaultPipelineName(livePipeline)
    case other                  => fail(s"$other")
  }

  /** What Elasticsearch answers to `GET /t` for the index elasticsql created for `of`. */
  private def indexJsonOf(of: Table): String = {
    val doc = mapper.createObjectNode()
    doc.set[ObjectNode]("mappings", of.indexMappings.deepCopy())
    doc.set[ObjectNode]("settings", of.indexSettings.deepCopy())
    doc.set[ObjectNode]("aliases", mapper.createObjectNode())
    val root = mapper.createObjectNode()
    root.set[ObjectNode]("t", doc)
    mapper.writeValueAsString(root)
  }
  private val indexJson: String = indexJsonOf(table)

  private val status =
    """{"set":{"field":"status","value":"active","if":"ctx.status == null","ignore_failure":true}}"""
  private val unrenderable = """{"set":{"field":"tags","value":[null,"a"]}}"""
  private val csv =
    """{"csv":{"field":"line","target_fields":["l1","l2"],"separator":";"}}"""
  private val split = """{"split":{"field":"tags","separator":",","ignore_missing":true}}"""
  private val nullEntry = """{"script":{"source":"ctx.n = params.d","params":{"d":null}}}"""

  /** The table's declared pipeline as stored, with `extra` processors appended. */
  private def stored(extra: String*): String = storedOf(table, extra: _*)

  /** `of`'s declared pipeline as elasticsql stored it (its description included), with `extra`. */
  private def storedOf(of: Table, extra: String*): String = {
    val node = mapper.readTree(of.defaultPipeline.json).asInstanceOf[ObjectNode]
    val processors = node.get("processors").asInstanceOf[ArrayNode]
    extra.foreach(p => processors.add(mapper.readTree(p)))
    mapper.writeValueAsString(node)
  }

  /** `json` with the top-level `fields` (name -> JSON) set. */
  private def withFields(json: String, fields: (String, String)*): String = {
    val node = mapper.readTree(json).asInstanceOf[ObjectNode]
    fields.foreach { case (k, v) => node.set[ObjectNode](k, mapper.readTree(v)) }
    mapper.writeValueAsString(node)
  }

  private def field(json: String, name: String): Option[String] =
    Option(mapper.readTree(json).get(name)).map(mapper.writeValueAsString)

  private val pipelineFields = Seq(
    "on_failure" -> """[{"set":{"field":"error","value":"{{ _ingest.on_failure_message }}"}}]""",
    "version"    -> "3",
    "_meta"      -> """{"owner":"ops"}"""
  )

  private def defaultPipelineOf(settings: String): String =
    mapper.readTree(settings).path("index").path("default_pipeline").asText()

  private def processorsOf(json: String): Seq[String] = {
    val processors = mapper.readTree(json).get("processors")
    (0 until processors.size()).map(i => mapper.writeValueAsString(processors.get(i)))
  }

  private class Cluster(
    esVersion: String,
    pipelines: Map[String, String],
    failingWrite: Option[String] = None,
    index: String = indexJson
  ) extends NopeClientApi {
    override protected def logger: Logger = LoggerFactory.getLogger(getClass)

    val live: mutable.Map[String, String] = mutable.Map(pipelines.toSeq: _*)
    val read: mutable.ListBuffer[String] = mutable.ListBuffer()
    val written: mutable.ListBuffer[String] = mutable.ListBuffer()

    /** The `default_pipeline` of every settings update, in order. */
    val restored: mutable.ListBuffer[String] = mutable.ListBuffer()

    /** The `default_pipeline` every migration ran through. */
    val migrated: mutable.ListBuffer[String] = mutable.ListBuffer()

    override private[client] def executeVersion(): ElasticResult[String] =
      ElasticResult.success(esVersion)
    override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
      ElasticResult.success(index == "t")
    override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] =
      ElasticResult.success(if (index == "t") Some(this.index) else None)
    override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] =
      ElasticResult.success(true)
    override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] =
      ElasticResult.success(true)
    override private[client] def executeGetPipeline(name: String): ElasticResult[Option[String]] = {
      read += name
      ElasticResult.success(live.get(name))
    }
    override private[client] def executeCreatePipeline(
      name: String,
      definition: String
    ): ElasticResult[Boolean] = {
      if (failingWrite.contains(name)) throw new IllegalStateException(s"write of $name failed")
      written += name
      live(name) = definition
      ElasticResult.success(true)
    }
    override private[client] def executeDeletePipeline(
      name: String,
      ifExists: Boolean
    ): ElasticResult[Boolean] = {
      live -= name
      ElasticResult.success(true)
    }
    override private[client] def executeUpdateSettings(
      index: String,
      settings: String
    ): ElasticResult[Boolean] = {
      restored += defaultPipelineOf(settings)
      ElasticResult.success(true)
    }
    override private[client] def performMigration(
      index: String,
      tempIndex: String,
      mapping: String,
      settings: String,
      aliases: Seq[TableAlias]
    ): ElasticResult[Boolean] = {
      migrated += defaultPipelineOf(settings)
      ElasticResult.success(true)
    }
  }

  behavior of "ALTER TABLE on the reindex path"

  it should "write the declared change to the pipeline the index is left on when the statement renames it" in {
    val cluster = new Cluster(
      "8.18.3",
      Map(livePipeline -> stored(), "other" -> stored(status))
    )
    val res = cluster
      .run(
        "ALTER TABLE t (SET SETTING default_pipeline = 'other', ADD COLUMN x INT SCRIPT AS (a * 2))"
      )
      .futureValue
    withClue(s"$res ") { res.isSuccess shouldBe true }
    // the controls: the statement read the renamed pipeline, and reindexed
    cluster.read should contain("other")
    cluster.migrated should have size 1
    // whichever pipeline the index is left on, that pipeline computes the new column
    processorsOf(cluster.live(cluster.restored.last)).exists(_.contains("ctx.x = ")) shouldBe true
  }

  it should "restore the pipeline setting when a write after the migration throws" in {
    val cluster = new Cluster(
      "8.18.3",
      Map(livePipeline -> stored(status)),
      failingWrite = Some(livePipeline)
    )
    // the exception propagates (or the statement answers a failure): never a success
    val outcome =
      scala.util.Try(cluster.run("ALTER TABLE t ADD COLUMN x INT SCRIPT AS (a * 2)").futureValue)
    outcome.map(_.isSuccess) should not be scala.util.Success(true)
    outcome.failed.map(_.getMessage).toOption.foreach(_ should include("write of"))
    // the control: the documents were migrated through the temporary pipeline
    cluster.migrated should have size 1
    cluster.migrated.head should startWith("t_tmp_")
    cluster.restored shouldBe Seq(livePipeline)
    // and the temporary pipeline it created is not left behind
    cluster.live.keys.filter(_.startsWith("t_tmp_")) shouldBe empty
  }

  it should "write an undeclared processor the value model cannot render, verbatim, on every major" in {
    Seq("6.8.23", "8.18.3").foreach { esVersion =>
      withClue(s"[$esVersion] ") {
        val cluster = new Cluster(esVersion, Map(livePipeline -> stored(unrenderable)))
        val res = cluster
          .run("ALTER TABLE t ADD COLUMN x INT SCRIPT AS (a * 2)")
          .futureValue
        withClue(s"$res ") { res.isSuccess shouldBe true }
        cluster.written should contain(livePipeline)
        processorsOf(cluster.live(livePipeline)) should contain(unrenderable)
        cluster.restored.last shouldBe livePipeline
      }
    }
  }

  it should "write an undeclared processor exactly as Elasticsearch returned it, on every major" in {
    // ES 6 and 7 strip `separator` (6 also `description`, `ignore_empty_value`) from the
    // processors elasticsql renders; a csv would silently fall back to `,`, a split would be refused
    Seq("6.8.23", "7.17.29", "8.18.3").foreach { esVersion =>
      withClue(s"[$esVersion] ") {
        val cluster = new Cluster(esVersion, Map(livePipeline -> stored(csv, split, nullEntry)))
        val res = cluster.run("ALTER TABLE t ADD COLUMN x INT SCRIPT AS (a * 2)").futureValue
        withClue(s"$res ") { res.isSuccess shouldBe true }
        // the control: the owned change was written
        cluster.written should contain(livePipeline)
        processorsOf(cluster.live(livePipeline)).exists(_.contains("ctx.x = ")) shouldBe true
        // what was sent, as sent
        Seq(csv, split, nullEntry).foreach(p => cluster.live(livePipeline) should include(p))
      }
    }
  }

  it should "keep the pipeline's own on_failure, version and _meta on the reindex path" in {
    val cluster =
      new Cluster("8.18.3", Map(livePipeline -> withFields(stored(status), pipelineFields: _*)))
    val res = cluster.run("ALTER TABLE t ADD COLUMN x INT SCRIPT AS (a * 2)").futureValue
    withClue(s"$res ") { res.isSuccess shouldBe true }
    cluster.migrated should have size 1 // the control: the reindex path
    processorsOf(cluster.live(livePipeline)).exists(_.contains("ctx.x = ")) shouldBe true
    pipelineFields.foreach { case (k, v) =>
      field(cluster.live(livePipeline), k) shouldBe Some(
        mapper.writeValueAsString(mapper.readTree(v))
      )
    }
  }

  it should "keep the pipeline's own on_failure, version and _meta on the in-place path" in {
    // the declared script changed out of band, re-asserted by an ALTER that reindexes nothing
    val drifted = stored(status).replace("(param1 + param2)", "(param1 + param2 + 100)")
    val cluster =
      new Cluster("8.18.3", Map(livePipeline -> withFields(drifted, pipelineFields: _*)))
    val res = cluster.run("ALTER TABLE t SET SETTING refresh_interval = '2s'").futureValue
    withClue(s"$res ") { res.isSuccess shouldBe true }
    cluster.migrated shouldBe empty // the control: the in-place path
    cluster.written should contain(livePipeline)
    cluster.live(livePipeline) should not include "param2 + 100"
    pipelineFields.foreach { case (k, v) =>
      field(cluster.live(livePipeline), k) shouldBe Some(
        mapper.writeValueAsString(mapper.readTree(v))
      )
    }
  }

  it should "rewrite the description elasticsql wrote, whatever the pipeline is named" in {
    // the DDL's default name, and a name given in OPTIONS (default_pipeline = ...)
    val named = table.setDefaultPipelineName("events_enriched")
    Seq(table -> livePipeline, named -> "events_enriched").foreach { case (of, name) =>
      withClue(s"[$name] ") {
        val cluster =
          new Cluster("8.18.3", Map(name -> storedOf(of, status)), index = indexJsonOf(of))
        def description: String = field(cluster.live(name), "description").getOrElse("")
        cluster
          .run("ALTER TABLE t ADD COLUMN x INT SCRIPT AS (a * 2)")
          .futureValue
          .isSuccess shouldBe
        true
        description should include("x INT SCRIPT AS (a * 2)")
        // and again after that rewrite, whose description also names the kept processor
        cluster
          .run("ALTER TABLE t ADD COLUMN y INT SCRIPT AS (a * 3)")
          .futureValue
          .isSuccess shouldBe
        true
        description should include("y INT SCRIPT AS (a * 3)")
      }
    }
  }

  it should "keep a description the user wrote, whatever the pipeline is named" in {
    Seq(table -> livePipeline, table.setDefaultPipelineName("user_pipe") -> "user_pipe").foreach {
      case (of, name) =>
        withClue(s"[$name] ") {
          val described =
            withFields(storedOf(of, status), "description" -> """"my pipeline"""")
          val cluster =
            new Cluster("8.18.3", Map(name -> described), index = indexJsonOf(of))
          val res = cluster.run("ALTER TABLE t ADD COLUMN x INT SCRIPT AS (a * 2)").futureValue
          withClue(s"$res ") { res.isSuccess shouldBe true }
          cluster.written should contain(name) // the control: the pipeline was rewritten
          field(cluster.live(name), "description") shouldBe Some(""""my pipeline"""")
        }
    }
  }

  /** The bytes CREATE PIPELINE and ALTER PIPELINE statements sent before the CREATE handling moved
    * into one helper (`pipeline-statement-bytes.txt`, captured on the same stub from the code
    * before the move), on each major's path.
    */
  it should "send CREATE PIPELINE and ALTER PIPELINE statements exactly as before, on every major" in {
    val create =
      """CREATE OR REPLACE PIPELINE p WITH PROCESSORS (SET (field = "name", if = "ctx.name == null", description = "DEFAULT 'anonymous'", ignore_failure = true, value = "anonymous"), DATE_INDEX_NAME (field = "birthdate", index_name_prefix = "users-", date_formats = ["yyyy-MM"], ignore_failure = true, date_rounding = "M", description = "PARTITION BY birthdate (MONTH)", separator = "-"), SET (field = "_id", description = "PRIMARY KEY (id)", ignore_failure = false, ignore_empty_value = false, value = "{{id}}"))"""
    val alter =
      """ALTER PIPELINE p (ADD PROCESSOR SET (field = "status", if = "ctx.status == null", description = "status DEFAULT 'active'", ignore_failure = true, value = "active"))"""
    val source = scala.io.Source.fromResource("pipeline-statement-bytes.txt")
    val before =
      try source.getLines().map(_.split("\\|", 3)).map(l => (l(0), l(1)) -> l(2)).toMap
      finally source.close()
    Seq("6.8.23", "7.17.29", "8.18.3").foreach { esVersion =>
      withClue(s"[$esVersion] ") {
        val cluster = new Cluster(esVersion, Map.empty)
        cluster.run(create).futureValue.isSuccess shouldBe true
        cluster.live("p") shouldBe before((esVersion, "create"))
        cluster.run(alter).futureValue.isSuccess shouldBe true
        cluster.live("p") shouldBe before((esVersion, "alter"))
      }
    }
  }
}
