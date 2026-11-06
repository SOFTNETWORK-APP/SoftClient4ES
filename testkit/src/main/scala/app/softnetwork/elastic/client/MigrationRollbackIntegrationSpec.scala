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

import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess,
  QueryResult
}
import app.softnetwork.elastic.scalatest.ElasticTestKit
import app.softnetwork.elastic.sql.schema.TableAlias
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/** A failed reindex migration never loses the index, on a real cluster.
  *
  *   - a failure copying to the temporary index (documents the new mapping refuses) leaves the
  *     index as it was, through both callers: ALTER TABLE and `updateMapping`;
  *   - every state a failure after the deletion of the original leaves (staged as the forward steps
  *     leave it) is restored by the rollback, from both backup shapes: what ALTER TABLE backs up
  *     (the loaded table) and what `updateMapping` backs up (`GET` mapping, settings, aliases);
  *   - a restore that fails keeps the temporary index, with every document;
  *   - a restore copies the documents back through no pipeline, then attaches the original's back:
  *     a non-idempotent pipeline does not run twice;
  *   - a copy back failing in the real `updateMapping` forward is restored.
  *
  * Every table carries a plain alias and a filtered one; what is compared is what Elasticsearch
  * answers (`_alias`, `_settings` without the settings it generates, the mapping, the documents),
  * never the message alone: a failure branch reaches a message too.
  */
trait MigrationRollbackIntegrationSpec extends GatewayIntegrationTestKit {
  self: ElasticTestKit =>
  import MigrationRollbackIntegrationSpec.State

  private val json = new ObjectMapper()

  private lazy val api: ElasticClientApi = client.asInstanceOf[ElasticClientApi]

  private val generated = Seq("uuid", "creation_date", "provided_name", "version")

  private val ids = Seq("1", "2", "3")

  private def source(id: String): String = {
    val k = if (id == "3") "b" else "a"
    s"""{"id":"$id","k":"$k","d":"2024-01-0${id}T10:00:00Z"}"""
  }

  private val filter = ListMap[String, Any]("term" -> ListMap("k" -> ListMap("value" -> "a")))

  /** The original definition, plus one field: the temporary index of the staged states. */
  private val newProperties =
    """{"properties":{"id":{"type":"keyword"},"k":{"type":"keyword"},"d":{"type":"date"},"z":{"type":"keyword"}}}"""

  private val formatted =
    """{"properties":{"id":{"type":"keyword"},"k":{"type":"keyword"},"d":{"type":"date","format":"yyyy-MM-dd"}}}"""

  private def run(sql: String): ElasticResult[QueryResult] =
    Await.result(client.run(sql), 120.seconds)

  private def tmps(t: String): Seq[String] =
    api.allMappings(Seq(s"${t}_tmp*")) match {
      case ElasticSuccess(m) => m.keys.toSeq.sorted
      case _                 => Nil
    }

  private def drop(t: String): Unit = {
    tmps(t).foreach(api.deleteIndex)
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $t"), 60.seconds))
    Try(api.deleteIndex(t))
  }

  /** `t` created by elasticsql, three documents, a plain alias and a filtered one. */
  private def table(t: String): Unit = {
    drop(t)
    run(s"CREATE TABLE $t (id KEYWORD, k KEYWORD, d TIMESTAMP, PRIMARY KEY (id))") match {
      case ElasticSuccess(_) =>
      case other             => fail(s"CREATE TABLE $t: $other")
    }
    ids.foreach(id => api.index(t, id, source(id)).get shouldBe true)
    api.addAlias(TableAlias(t, s"${t}_a")).get shouldBe true
    api.addAlias(TableAlias(t, s"${t}_f", filter = filter)).get shouldBe true
    api.refresh(t)
    api.invalidateSchema(t)
  }

  private def propertiesOf(mapping: JsonNode, t: String): JsonNode = {
    var node = mapping
    if (node.has(t)) node = node.get(t)
    if (node.has("mappings")) node = node.get("mappings")
    if (node.has("_doc")) node = node.get("_doc")
    node.path("properties")
  }

  private def state(t: String): State = {
    api.refresh(t)
    api.invalidateSchema(t)
    val settings = json.readTree(api.loadSettings(t).get).asInstanceOf[ObjectNode]
    val uuid = settings.path("uuid").asText()
    generated.foreach(settings.remove)
    val mapping = json.readTree(api.getMapping(t).get)
    val docs = ids.flatMap { id =>
      api.get(id, t) match {
        case ElasticSuccess(Some(src)) => Some(id -> json.readTree(src))
        case _                         => None
      }
    }.toMap
    val count = collectRows(System.nanoTime(), run(s"SELECT id FROM $t")).size
    val aliases = json.readTree(api.executeGetAliases(t).get).path(t).path("aliases")
    State(uuid, docs, count, mapping, propertiesOf(mapping, t), settings, aliases)
  }

  private def expectRestored(t: String, tmp: String, before: State): Unit = {
    val after = state(t)
    after.docs shouldBe before.docs
    after.count shouldBe ids.size
    after.properties shouldBe before.properties
    after.settings shouldBe before.settings
    // the filtered alias keeps its filter
    after.aliases shouldBe before.aliases
    api.indexExists(tmp, pattern = false).get shouldBe false
  }

  // ----------------------------------------------------------------------------------------------
  // a failure before the original is deleted
  // ----------------------------------------------------------------------------------------------

  "a failure copying to the temporary index" should "leave the index unchanged (ALTER TABLE)" in {
    val t = "zr1a"
    table(t)
    val before = state(t)
    run(s"ALTER TABLE $t ALTER COLUMN d SET OPTIONS (format = 'yyyy-MM-dd')") match {
      case ElasticFailure(error) =>
        error.message should endWith(s"Index '$t' is unchanged.")
      case other => fail(s"the ALTER TABLE answered $other")
    }
    state(t) shouldBe before
    tmps(t) shouldBe empty
  }

  it should "leave the index unchanged (updateMapping)" in {
    val t = "zr1u"
    table(t)
    val before = state(t)
    api.updateMapping(t, formatted) match {
      case ElasticFailure(error) =>
        error.message should endWith(s"Index '$t' is unchanged.")
      case other => fail(s"updateMapping answered $other")
    }
    state(t) shouldBe before
    tmps(t) shouldBe empty
  }

  // ----------------------------------------------------------------------------------------------
  // a failure after the original was deleted: the states it leaves, restored
  // ----------------------------------------------------------------------------------------------

  private val states: Seq[(String, MigrationStep, (String, String) => Unit)] = Seq(
    ("re-create, index absent", MigrationStep.RecreateOriginal, (_, _) => ()),
    (
      "re-create, index created empty",
      MigrationStep.RecreateOriginal,
      (t, _) => api.createIndex(t, mappings = Some(newProperties)).get shouldBe true
    ),
    (
      "copy back, index partial",
      MigrationStep.CopyBack,
      (t, _) => {
        api.createIndex(t, mappings = Some(newProperties)).get shouldBe true
        api.index(t, "1", source("1")).get shouldBe true
        api.refresh(t)
      }
    ),
    (
      "open, index complete",
      MigrationStep.OpenOriginal,
      (t, tmp) => {
        api.createIndex(t, mappings = Some(newProperties)).get shouldBe true
        api.reindex(tmp, t, refresh = true).get._1 shouldBe true
      }
    )
  )

  for {
    shape                         <- Seq("ALTER TABLE", "updateMapping")
    ((label, failedAt, stage), i) <- states.zipWithIndex
  } it should s"be restored by the rollback [$shape backup · $label]" in {
    val t = s"zr2${if (shape == "ALTER TABLE") "a" else "u"}$i"
    table(t)
    val before = state(t)
    // the backup, as its caller takes it
    val (backup, tempAliases) =
      if (shape == "ALTER TABLE") {
        val s = api.loadSchema(t).get
        (
          IndexBackup(s.indexMappings.toString, s.indexSettings.toString, s.indexAliases),
          s.indexAliases
        )
      } else
        (IndexBackup(api.getMapping(t).get, api.loadSettings(t).get, api.getAliases(t).get), Nil)
    // the temporary index as step 1 leaves it (the ALTER TABLE one carries the table's aliases),
    // with the documents of step 2
    val tmp = s"${t}_tmp_x"
    if (
      api
        .createIndex(tmp, mappings = Some(newProperties), aliases = tempAliases)
        .toOption
        .contains(true)
    ) ()
    else {
      // a client whose create refuses a filter read back cannot reach this state through ALTER
      // TABLE (its step 1 fails first): the aliases go on with the filter it accepts
      Try(api.deleteIndex(tmp))
      api
        .createIndex(
          tmp,
          mappings = Some(newProperties),
          aliases = tempAliases.map(IndexMigration.plainAlias)
        )
        .get shouldBe true
    }
    api.reindex(t, tmp, refresh = true).get._1 shouldBe true
    api.deleteIndex(t).get shouldBe true
    stage(t, tmp)

    api.rollbackMigration(t, tmp, backup, failedAt) shouldBe RollbackOutcome.Restored(None)
    expectRestored(t, tmp, before)
  }

  // ----------------------------------------------------------------------------------------------
  // a restore that fails
  // ----------------------------------------------------------------------------------------------

  "a restore that fails" should "keep the temporary index and name it" in {
    val t = "zr3"
    drop(t)
    api
      .createIndex(
        t,
        mappings = Some("""{"properties":{"id":{"type":"keyword"},"n":{"type":"integer"}}}""")
      )
      .get shouldBe true
    ids.foreach(id => api.index(t, id, s"""{"id":"$id","n":$id}""").get shouldBe true)
    api.addAlias(TableAlias(t, s"${t}_a")).get shouldBe true
    api.refresh(t)
    val backup = IndexBackup(api.getMapping(t).get, api.loadSettings(t).get, api.getAliases(t).get)
    val tmp = s"${t}_tmp_x"
    api
      .createIndex(
        tmp,
        mappings = Some("""{"properties":{"id":{"type":"keyword"},"n":{"type":"keyword"}}}""")
      )
      .get shouldBe true
    api.reindex(t, tmp, refresh = true).get._1 shouldBe true
    // a document the original mapping refuses: the copy back fails on it
    api.index(tmp, "4", """{"id":"4","n":"x"}""").get shouldBe true
    api.refresh(tmp)
    api.deleteIndex(t).get shouldBe true

    val outcome = api.rollbackMigration(t, tmp, backup, MigrationStep.RecreateOriginal)
    outcome shouldBe a[RollbackOutcome.Kept]
    IndexMigration
      .migrationError(t, tmp, ElasticError("Reindex failed"), outcome)
      .message should endWith(s"its documents are kept in index '$tmp'.")
    api.indexExists(tmp, pattern = false).get shouldBe true
    (ids :+ "4").foreach(id => api.get(id, tmp).get should not be empty)
  }

  // ----------------------------------------------------------------------------------------------
  // a restore runs no pipeline
  // ----------------------------------------------------------------------------------------------

  "a restore" should "copy the documents back through no pipeline, then attach the original one back" in {
    val t = "zx1"
    val pipeline = s"${t}_append_pipeline"
    val mapping = """{"properties":{"id":{"type":"keyword"},"tags":{"type":"keyword"}}}"""
    drop(t)
    Try(api.deletePipeline(pipeline, ifExists = true))
    // not idempotent, and declared by no table: a second run appends a second value
    api
      .createPipeline(pipeline, """{"processors":[{"append":{"field":"tags","value":["x"]}}]}""")
      .get shouldBe true
    def sources(index: String): Map[String, JsonNode] = {
      api.refresh(index)
      ids.flatMap { id =>
        api.get(id, index) match {
          case ElasticSuccess(Some(src)) => Some(id -> json.readTree(src))
          case _                         => None
        }
      }.toMap
    }
    try {
      api
        .createIndex(
          t,
          settings =
            s"""{"index":{"number_of_shards":1,"number_of_replicas":0,"default_pipeline":"$pipeline"}}""",
          mappings = Some(mapping)
        )
        .get shouldBe true
      ids.foreach(id => api.index(t, id, s"""{"id":"$id"}""").get shouldBe true)
      val before = sources(t)
      // the canary: the pipeline ran once on each document
      before.keySet shouldBe ids.toSet
      before.values.foreach(_.path("tags").size() shouldBe 1)
      val backup =
        IndexBackup(api.getMapping(t).get, api.loadSettings(t).get, api.getAliases(t).get)
      // the temporary index as a routed ALTER TABLE leaves it: its pipeline holds only what the
      // table declares (here: none)
      val tmp = s"${t}_tmp_x"
      api.createIndex(tmp, mappings = Some(mapping)).get shouldBe true
      api.reindex(t, tmp, refresh = true).get._1 shouldBe true
      api.deleteIndex(t).get shouldBe true

      api.rollbackMigration(t, tmp, backup, MigrationStep.CopyBack) shouldBe
      RollbackOutcome.Restored(None)
      withClue(s"before=$before ") {
        sources(t) shouldBe before
      }
      api.indexExists(tmp, pattern = false).get shouldBe false
      // the pipeline is attached back: a new document goes through it
      json.readTree(api.loadSettings(t).get).path("default_pipeline").asText() shouldBe pipeline
      api.index(t, "4", """{"id":"4"}""").get shouldBe true
      api.refresh(t)
      api.get("4", t) match {
        case ElasticSuccess(Some(src)) => json.readTree(src).path("tags").size() shouldBe 1
        case other                     => fail(s"document 4: $other")
      }
    } finally {
      drop(t)
      Try(api.deletePipeline(pipeline, ifExists = true))
    }
  }

  // ----------------------------------------------------------------------------------------------
  // a copy back failing in the real forward
  // ----------------------------------------------------------------------------------------------

  "a copy back failing in the updateMapping forward" should "be restored" in {
    val t = "zr4"
    val pipeline = s"${t}_fail_pipeline"
    table(t)
    Try(api.deletePipeline(pipeline, ifExists = true))
    // passes the copy to the temporary index (its target is not `t`), fails the copy back
    api
      .createPipeline(
        pipeline,
        s"""{"processors":[{"fail":{"if":"ctx._index == '$t'","message":"refused on $t"}}]}"""
      )
      .get shouldBe true
    val before = state(t)
    try {
      api.updateMapping(
        t,
        newProperties,
        s"""{"index":{"default_pipeline":"$pipeline"}}"""
      ) match {
        case ElasticFailure(error) =>
          error.message should endWith(s"Index '$t' was restored with its documents.")
        case other => fail(s"updateMapping answered $other")
      }
      expectRestored(t, s"${t}_tmp_none", before)
      tmps(t) shouldBe empty
      state(t).settings.path("default_pipeline").asText() should not be pipeline
    } finally Try(api.deletePipeline(pipeline, ifExists = true))
  }
}

object MigrationRollbackIntegrationSpec {

  /** What Elasticsearch answers for an index. */
  final case class State(
    uuid: String,
    docs: Map[String, JsonNode],
    count: Int,
    mapping: JsonNode,
    properties: JsonNode,
    settings: JsonNode,
    aliases: JsonNode
  )
}
