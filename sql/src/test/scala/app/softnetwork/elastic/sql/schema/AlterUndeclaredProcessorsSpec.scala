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

package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{AlterTable, CreatePipeline, CreateTable}
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/** ALTER TABLE against a live pipeline that holds processors the table does not declare.
  *
  * The live pipeline is built the way Elasticsearch keeps it: the index document elasticsql creates
  * for the table is loaded with `Index(...).schema` exactly as the schema load does, its declared
  * pipeline is stored whole (7.9+) or without any `description` (6.8, which keeps none), and the
  * undeclared processors are added to the stored JSON. Every row runs on both stored forms unless
  * it says otherwise.
  */
class AlterUndeclaredProcessorsSpec extends AnyFlatSpec with Matchers {

  private val Default = IngestPipelineType.Default
  private val Final = IngestPipelineType.Final

  private def created(sql: String): Table = Parser(sql) match {
    case Right(ct: CreateTable) => ct.schema
    case other                  => fail(s"$other")
  }

  private def alter(sql: String): AlterTable = Parser(sql) match {
    case Right(a: AlterTable) => a
    case other                => fail(s"$other")
  }

  private val ddl =
    "CREATE TABLE t (id KEYWORD, a INT, b INT, c INT SCRIPT AS (a + b), PRIMARY KEY (id))"

  /** What Elasticsearch answers to `GET /t` for the index elasticsql created. */
  private val indexJson: String = {
    val table = created(ddl).setDefaultPipelineName("t_ddl_default_pipeline")
    val mappings = table.indexMappings.deepCopy()
    mappings
      .get("properties")
      .asInstanceOf[ObjectNode]
      .set[ObjectNode]("status", mapper.readTree("""{"type":"keyword"}"""))
    val settings = table.indexSettings.deepCopy()
    settings.get("index").asInstanceOf[ObjectNode].put("number_of_shards", "1")
    val doc = mapper.createObjectNode()
    doc.set[ObjectNode]("mappings", mappings)
    doc.set[ObjectNode]("settings", settings)
    doc.set[ObjectNode]("aliases", mapper.createObjectNode())
    val root = mapper.createObjectNode()
    root.set[ObjectNode]("t", doc)
    mapper.writeValueAsString(root)
  }

  private def loaded: Table = Index("t", indexJson).schema

  /** A pipeline as Elasticsearch 6.8 keeps it: no `description`, on the pipeline or a processor. */
  private def es68(json: String): String = {
    val node = mapper.readTree(json).asInstanceOf[ObjectNode]
    node.remove("description")
    node.get("processors").elements().asScala.foreach { p =>
      val props = p.get(p.fieldNames().next()).asInstanceOf[ObjectNode]
      props.remove("description")
      props.remove("separator")
      props.remove("ignore_empty_value")
      ()
    }
    mapper.writeValueAsString(node)
  }

  private def form(on68: Boolean): String = if (on68) "6.8" else "7.9+"

  /** The live pipeline's JSON: what the table declared, stored in the given form, with `leading`
    * processors put first and `extra` ones appended.
    */
  private def live(
    declared: IngestPipeline,
    on68: Boolean,
    extra: Seq[String],
    leading: Seq[String] = Nil
  ): String = {
    val stored =
      mapper.readTree(if (on68) es68(declared.json) else declared.json).asInstanceOf[ObjectNode]
    val processors = stored.get("processors").asInstanceOf[ArrayNode]
    leading.reverse.foreach(p => processors.insert(0, mapper.readTree(p)))
    extra.foreach(p => processors.add(mapper.readTree(p)))
    mapper.writeValueAsString(stored)
  }

  private def typed(name: String, json: String, pipelineType: IngestPipelineType = Default) =
    IngestPipeline(name, json, Some(pipelineType))

  private def rewrite(
    name: String,
    json: String,
    before: IngestPipeline,
    after: IngestPipeline,
    pipelineType: IngestPipelineType = Default
  ): PipelineRewrite =
    typed(name, json, pipelineType)
      .rewrite(before, after, json)

  /** The ALTER of `sql` against the table's own pipeline, stored in the given form. */
  private def altered(
    sql: String,
    on68: Boolean,
    extra: Seq[String],
    leading: Seq[String] = Nil
  ): (String, IngestPipeline, PipelineRewrite) = {
    val table = loaded
    val json = live(table.defaultPipeline, on68, extra, leading)
    val after = table.merge(alter(sql).statements).declaredPipeline(Default)
    (json, after, rewrite(table.defaultPipeline.name, json, table.declaredPipeline(Default), after))
  }

  private def stored(json: String, position: Int): String =
    mapper.writeValueAsString(mapper.readTree(json).get("processors").get(position))

  private def shape(diff: Seq[PipelineDiff]): Seq[String] =
    diff.map(d => s"${d.getClass.getSimpleName}(${d.processor.sql})")

  private def columns(pipeline: IngestPipeline): Seq[String] =
    pipeline.processors.map(p => s"${p.processorType.name}:${p.column}")

  private val status =
    """{"set":{"field":"status","value":"active","if":"ctx.status == null","ignore_failure":true}}"""
  private val taggedStatus =
    """{"set":{"field":"status","value":"active","if":"ctx.status == null","tag":"status_default",""" +
    """"on_failure":[{"set":{"field":"error","value":"{{ _ingest.on_failure_message }}"}}]}}"""
  private val userScript = """{"script":{"source":"ctx.c = 2","lang":"painless"}}"""
  private val setOnComputed = """{"set":{"field":"c","value":0,"if":"ctx.c == null"}}"""
  private val leadingRename =
    """{"rename":{"field":"a_raw","target_field":"a","ignore_missing":true}}"""
  private val unrenderable = """{"set":{"field":"tags","value":[null,"a"]}}"""
  private val storedScript = """{"script":{"id":"tag_document","params":{"tag":"t"}}}"""

  private val AddNote = "ALTER TABLE t ADD COLUMN note KEYWORD"
  private val DropC = "ALTER TABLE t DROP COLUMN c"
  private val SetScript = "ALTER TABLE t ALTER COLUMN c SET SCRIPT AS (a * b)"
  private val AddX = "ALTER TABLE t ADD COLUMN x INT SCRIPT AS (a * 2)"
  private val SetDefault = "ALTER TABLE t ALTER COLUMN a SET DEFAULT 0"
  private val alters = Seq(AddNote, DropC, SetScript, AddX, SetDefault)

  private val generatedSource =
    "ctx.c = (param1 == null || param2 == null) ? null : (param1 + param2)"

  /** The owned changes each ALTER makes, whatever the pipeline holds besides. */
  private def ownedChange(sql: String, diff: Seq[PipelineDiff]): Unit = sql match {
    case AddNote => diff shouldBe empty
    case DropC =>
      diff should have size 1
      diff.head shouldBe a[ProcessorRemoved]
      diff.head.processor.processorType shouldBe IngestProcessorType.Script
      diff.head.processor.properties("source").toString should include(generatedSource)
    case SetScript  => shape(diff) shouldBe Seq("ProcessorChanged(c INT SCRIPT AS (a * b))")
    case AddX       => shape(diff) shouldBe Seq("ProcessorAdded(x INT SCRIPT AS (a * 2))")
    case SetDefault => shape(diff) shouldBe Seq("ProcessorAdded(a SET DEFAULT 0)")
  }

  behavior of "ALTER TABLE over a pipeline holding processors the table does not declare"

  it should "leave the pipeline unchanged on an unrelated ADD COLUMN, keeping the processor verbatim" in {
    Seq(false, true).foreach { on68 =>
      Seq(status, taggedStatus).foreach { undeclared =>
        withClue(s"[${form(on68)} $undeclared] ") {
          val (json, _, rw) = altered(AddNote, on68, Seq(undeclared))
          rw.diff shouldBe empty
          columns(rw.written) shouldBe Seq("script:c", "set:_id", "set:status")
          rw.written.processors.last.json shouldBe stored(json, 2)
          rw.written.processors.last.json shouldBe undeclared
        }
      }
    }
    // the mechanism: the typed model does not render the tagged processor back as stored
    IngestProcessor(Default, mapper.readTree(taggedStatus)).json should not be taggedStatus
  }

  it should "remove only the dropped column's script on DROP COLUMN" in {
    Seq(false, true).foreach { on68 =>
      withClue(s"[${form(on68)}] ") {
        val (json, _, rw) = altered(DropC, on68, Seq(status))
        ownedChange(DropC, rw.diff)
        columns(rw.written) shouldBe Seq("set:_id", "set:status")
        rw.written.processors.last.json shouldBe stored(json, 2)
      }
    }
  }

  it should "change only the computed column's script on SET SCRIPT" in {
    Seq(false, true).foreach { on68 =>
      withClue(s"[${form(on68)}] ") {
        val (json, _, rw) = altered(SetScript, on68, Seq(status))
        ownedChange(SetScript, rw.diff)
        columns(rw.written) shouldBe Seq("script:c", "set:_id", "set:status")
        rw.written.processors.last.json shouldBe stored(json, 2)
      }
    }
  }

  it should "add only the new computed column's script on ADD COLUMN ... SCRIPT AS" in {
    Seq(false, true).foreach { on68 =>
      withClue(s"[${form(on68)}] ") {
        val (json, _, rw) = altered(AddX, on68, Seq(status))
        ownedChange(AddX, rw.diff)
        columns(rw.written) shouldBe Seq("script:c", "script:x", "set:_id", "set:status")
        rw.written.processors.last.json shouldBe stored(json, 2)
      }
    }
  }

  it should "add only the default, in declared order, on SET DEFAULT" in {
    Seq(false, true).foreach { on68 =>
      withClue(s"[${form(on68)}] ") {
        val (json, _, rw) = altered(SetDefault, on68, Seq(status))
        ownedChange(SetDefault, rw.diff)
        columns(rw.written) shouldBe Seq("set:a", "script:c", "set:_id", "set:status")
        rw.written.processors.last.json shouldBe stored(json, 2)
      }
    }
  }

  it should "reach a fixed point beside a user script assigning the computed column" in {
    val sizes = Map(AddNote -> 4, DropC -> 3, SetScript -> 4, AddX -> 5, SetDefault -> 5)
    Seq(false, true).foreach { on68 =>
      alters.foreach { sql =>
        withClue(s"[${form(on68)} $sql] ") {
          val table = loaded
          val name = table.defaultPipeline.name
          val before = table.declaredPipeline(Default)
          val after = table.merge(alter(sql).statements).declaredPipeline(Default)
          var json = live(table.defaultPipeline, on68, Seq(status, userScript))
          // round 1 is the statement; rounds 2 and 3 re-ALTER what round 1 wrote
          val rounds = (1 to 3).map { round =>
            val rw = rewrite(name, json, if (round == 1) before else after, after)
            if (rw.diff.nonEmpty) json = if (on68) es68(rw.written.json) else rw.written.json
            typed(name, json).processors.size
          }
          rounds shouldBe Seq.fill(3)(sizes(sql))
          if (sql == DropC) {
            val kept = typed(name, json).processors
            kept.map(p => s"${p.processorType.name}:${p.column}").take(2) shouldBe
            Seq("set:_id", "set:status")
            kept.map(_.properties.get("source").map(_.toString)) shouldBe
            Seq(None, None, Some("ctx.c = 2"))
          }
        }
      }
    }
  }

  it should "keep an undeclared set on a declared column for every ALTER" in {
    Seq(false, true).foreach { on68 =>
      alters.foreach { sql =>
        withClue(s"[${form(on68)} $sql] ") {
          val (json, _, rw) = altered(sql, on68, Seq(setOnComputed))
          rw.undeclared.map(p => s"${p.processorType.name}:${p.column}") shouldBe Seq("set:c")
          ownedChange(sql, rw.diff)
          rw.written.processors.last.json shouldBe stored(json, 2)
          rw.written.processors.last.json shouldBe setOnComputed
        }
      }
    }
  }

  it should "keep a stored-script processor verbatim for every ALTER" in {
    // a stored script has no `source`: reading the pipeline used to throw a NullPointerException,
    // and the ALTER with it
    Seq(false, true).foreach { on68 =>
      alters.foreach { sql =>
        withClue(s"[${form(on68)} $sql] ") {
          val (json, _, rw) = altered(sql, on68, Seq(storedScript))
          rw.undeclared.map(_.processorType.name) shouldBe Seq("script")
          ownedChange(sql, rw.diff)
          rw.written.processors.last.json shouldBe stored(json, 2)
          rw.written.processors.last.json shouldBe storedScript
        }
      }
    }
    // the mechanism: the processor reads as one no column declares
    IngestProcessor(Default, mapper.readTree(storedScript)) shouldBe a[GenericProcessor]
  }

  it should "keep a leading undeclared processor first, verbatim" in {
    Seq(false, true).foreach { on68 =>
      withClue(s"[${form(on68)}] ") {
        val (json, _, rw) = altered(SetScript, on68, Nil, leading = Seq(leadingRename))
        ownedChange(SetScript, rw.diff)
        rw.written.processors.map(_.processorType.name) shouldBe Seq("rename", "script", "set")
        rw.written.processors.head.json shouldBe stored(json, 0)
        rw.written.processors.head.json shouldBe leadingRename
      }
    }
    // the mechanism: the typed model drops `ignore_missing`
    IngestProcessor(Default, mapper.readTree(leadingRename)).json should not be leadingRename
  }

  it should "leave a final pipeline untouched, since a table declares no final processor" in {
    val json = """{"processors":[{"set":{"field":"tagged","value":true}}]}"""
    Seq(false, true).foreach { on68 =>
      withClue(s"[${form(on68)}] ") {
        val table = loaded
        val stored = if (on68) es68(json) else json
        val after = table.merge(alter(AddNote).statements).declaredPipeline(Final)
        val rw = rewrite("gov", stored, table.declaredPipeline(Final), after, Final)
        rw.diff shouldBe empty
        rw.undeclared.map(_.column) shouldBe Seq("tagged")
        rw.written.processors.map(_.json) shouldBe Seq(
          """{"set":{"field":"tagged","value":true}}"""
        )
      }
    }
  }

  it should "leave a pipeline set outside elasticsql untouched" in {
    val raw =
      """{"o":{"mappings":{"properties":{"x":{"type":"keyword"}}},"settings":{"index":{"default_pipeline":"user_pipe"}}}}"""
    val json =
      """{"processors":[{"lowercase":{"field":"x"}},{"set":{"field":"y","value":1}}]}"""
    Seq(false, true).foreach { on68 =>
      withClue(s"[${form(on68)}] ") {
        val outside = Index("o", raw).schema
        outside.defaultPipelineName shouldBe Some("user_pipe")
        val after =
          outside
            .merge(alter("ALTER TABLE o ADD COLUMN z KEYWORD").statements)
            .declaredPipeline(
              Default
            )
        val stored = if (on68) es68(json) else json
        val rw = rewrite("user_pipe", stored, outside.declaredPipeline(Default), after)
        rw.diff shouldBe empty
        rw.undeclared should have size 2
      }
    }
  }

  it should "change an owned processor edited out of band back to its declaration" in {
    Seq(false, true).foreach { on68 =>
      withClue(s"[${form(on68)}] ") {
        val table = loaded
        val json = live(table.defaultPipeline, on68, Nil)
          .replace("(param1 + param2)", "(param1 + param2 + 100)")
        val after = table.merge(alter(AddNote).statements).declaredPipeline(Default)
        val rw = rewrite(table.defaultPipeline.name, json, table.declaredPipeline(Default), after)
        rw.undeclared shouldBe empty
        shape(rw.diff) shouldBe Seq("ProcessorChanged(c INT SCRIPT AS (a + b))")
      }
    }
  }

  it should "answer exactly the comparison against the declaration when nothing is undeclared" in {
    Seq(false, true).foreach { on68 =>
      alters.foreach { sql =>
        withClue(s"[${form(on68)} $sql] ") {
          val (json, after, rw) = altered(sql, on68, Nil)
          rw.undeclared shouldBe empty
          rw.written shouldBe after
          rw.diff.toSet shouldBe typed(after.name, json).diff(after).toSet
          rw.diff should have size typed(after.name, json).diff(after).size.toLong
        }
      }
    }
  }

  it should "write an undeclared processor the value model cannot render, verbatim and without throwing" in {
    // `Value.apply` throws for a list whose head has no `Values` companion; the pipeline's
    // `description` is rendered from every processor, so a write of this pipeline used to throw
    Seq(false, true).foreach { on68 =>
      withClue(s"[${form(on68)}] ") {
        val (json, _, rw) = altered(SetScript, on68, Seq(unrenderable))
        ownedChange(SetScript, rw.diff)
        val installed = CreatePipeline(
          rw.written.name,
          Default,
          orReplace = true,
          processors = rw.written.processors
        ).ddlPipeline.json
        stored(installed, 2) shouldBe stored(json, 2)
        stored(installed, 2) shouldBe unrenderable
        noException should be thrownBy rw.written.sql
      }
    }
  }

  it should "never render a value the value model cannot represent as a different one" in {
    // SHOW CREATE PIPELINE re-runs as a statement: rendering `[null, "a"]` as text would re-create
    // the processor with a STRING value, so the user-facing renderings stay loud, as before, and
    // only the written pipeline, which is never re-parsed, carries the stored node
    val append = """{"append":{"field":"tags","value":[null,"a"]}}"""
    val json = s"""{"processors":[$append]}"""
    val read = typed("p", json)
    an[IllegalArgumentException] should be thrownBy read.ddl // SHOW CREATE PIPELINE
    an[IllegalArgumentException] should be thrownBy read.describe // DESCRIBE PIPELINE
    val written = IngestPipeline.verbatim("p", json, Default)
    noException should be thrownBy written.json
    stored(written.json, 0) shouldBe append
  }
}
