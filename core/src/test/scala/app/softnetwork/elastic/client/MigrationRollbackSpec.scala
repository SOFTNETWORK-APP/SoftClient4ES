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
import app.softnetwork.elastic.client.IndexCluster._
import app.softnetwork.elastic.client.MigrationStep._
import app.softnetwork.elastic.client.RollbackOutcome._
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql.{ObjectValue, Value}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.CreateTable
import app.softnetwork.elastic.sql.schema.{Table, TableAlias}
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{ArrayNode, ObjectNode}
import org.mockito.Mockito
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.Logger

import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.io.Source
import scala.jdk.CollectionConverters._

/** A failed reindex migration never loses the index, Docker-free, on [[IndexCluster]].
  *
  *   - every cell: two callers (`updateMapping`, ALTER TABLE through `run`) × the step that fails
  *     (and how it fails) × the rollback step that fails too, each asserting I1-I5:
  *     - I1: every document exists in at least one index, also once every queued deletion landed;
  *     - I2: an original never deleted is untouched, and a temporary index left is named;
  *     - I3: a restore gives back the original definition (aliases and their filter included) and
  *       every document, and no temporary index; a failed deletion of the temporary index answers
  *       success with the migrated index;
  *     - I4: the temporary index holding the only copy is never deleted, and named; after a failed
  *       deletion of one copy, the other is never deleted; no alias reads a temporary index kept
  *       beside the index;
  *     - I5: one state sentence, and the status that goes with it;
  *   - the controls: main's rollback, on the same cluster, loses the documents or the index;
  *   - the census: a successful migration sends the requests main sent, in the same order;
  *   - the messages, the alias filters and the mapping a restore creates.
  */
class MigrationRollbackSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem("migration-rollback")
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(10.seconds))

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  // ----------------------------------------------------------------------------------------------
  // fixtures
  // ----------------------------------------------------------------------------------------------

  private val live = "t_ddl_default_pipeline"

  private val filter = """{"term":{"k":{"value":"a"}}}"""

  /** A plain alias and a filtered one, the filter as Elasticsearch hands it back. */
  private val aliases = Seq("t_a" -> "{}", "t_f" -> s"""{"filter":$filter}""")

  private val docs: Seq[(String, String)] =
    (1 to 3).map(i => i.toString -> s"""{"id":"$i","k":"a","d":"2024-01-0$i"}""")

  /** A document the new mapping (`d` as `yyyy-MM-dd`) refuses: the copy to the temporary index
    * fails on it, after the documents before it.
    */
  private val badDoc = "9" -> """{"id":"9","k":"b","d":"2024-01-09T10:00:00Z"}"""

  private val origMapping =
    """{"properties":{"d":{"type":"date"},"id":{"type":"keyword"},"k":{"type":"keyword"}}}"""
  private val newMapping =
    """{"properties":{"d":{"type":"date","format":"yyyy-MM-dd"},"id":{"type":"keyword"},"k":{"type":"keyword"}}}"""
  private val origSettings =
    """{"number_of_shards":"1","number_of_replicas":"0","refresh_interval":"2s","analysis":{"analyzer":{"a":{"type":"custom","tokenizer":"standard"}}}}"""

  private lazy val table: Table =
    Parser("CREATE TABLE t (id KEYWORD, k KEYWORD, d TIMESTAMP, PRIMARY KEY (id))") match {
      case Right(ct: CreateTable) => ct.schema.setDefaultPipelineName(live)
      case other                  => fail(s"$other")
    }

  private val alter = "ALTER TABLE t ALTER COLUMN d SET OPTIONS (format = 'yyyy-MM-dd')"

  sealed trait Caller
  case object ByUpdateMapping extends Caller { override def toString = "updateMapping" }
  case object ByAlter extends Caller { override def toString = "ALTER TABLE" }
  private val callers = Seq(ByUpdateMapping, ByAlter)

  private def clusterFor(caller: Caller, logger: Logger, withBadDoc: Boolean): IndexCluster = {
    val all = if (withBadDoc) docs :+ badDoc else docs
    caller match {
      case ByUpdateMapping =>
        val c = new IndexCluster(log = logger)
        c.stage("t", origMapping, origSettings, aliases, all)
        c
      case ByAlter =>
        val c = new IndexCluster(Map(live -> table.defaultPipeline.json), logger)
        c.stage(
          "t",
          mapper.writeValueAsString(table.indexMappings),
          mapper.writeValueAsString(table.indexSettings.path("index")),
          aliases,
          all
        )
        c
    }
  }

  private def migrate(caller: Caller, c: IndexCluster): ElasticResult[_] = caller match {
    case ByUpdateMapping => c.updateMapping("t", newMapping)
    case ByAlter         => c.run(alter).futureValue
  }

  /** What an index holds, for comparisons. */
  final case class Snap(
    uuid: String,
    properties: JsonNode,
    settings: JsonNode,
    aliases: Map[String, JsonNode],
    docs: Map[String, JsonNode]
  )

  private def withoutGenerated(settings: ObjectNode): JsonNode = {
    val copy = settings.deepCopy()
    refusedKeys.foreach(k => copy.remove(k))
    copy
  }

  private def snap(c: IndexCluster, index: String): Option[Snap] =
    c.indices.get(index).map { idx =>
      Snap(
        idx.uuid,
        idx.mapping.path("properties"),
        withoutGenerated(idx.settings),
        idx.aliases.toMap.map { case (k, v) => k -> (v: JsonNode) },
        idx.docs.toMap.map { case (k, v) => k -> (v: JsonNode) }
      )
    }

  private def tmpOf(c: IndexCluster): Option[String] = c.indices.keys.find(_.contains("_tmp_"))

  /** The log lines a mock logger received, by level. */
  private def lines(logger: Logger, level: String): Seq[String] =
    Mockito
      .mockingDetails(logger)
      .getInvocations
      .asScala
      .toSeq
      .filter(_.getMethod.getName == level)
      .flatMap(_.getArguments.headOption.collect { case s: String => s })

  // ----------------------------------------------------------------------------------------------
  // the cells
  // ----------------------------------------------------------------------------------------------

  private val steps =
    Seq(
      CreateTemp,
      CopyToTemp,
      DeleteOriginal,
      RecreateOriginal,
      CopyBack,
      OpenOriginal,
      DeleteTemp
    )

  private type Rule = PartialFunction[(String, String, Int), Fault]

  /** How the forward fails.
    *
    * @param tempExists
    *   the temporary index exists once the forward stopped
    * @param origPresent
    *   the original index is still there when the rollback looks (a refused deletion, or one that
    *   lands after the probe)
    * @param indexLeft
    *   the forward left an index named `t` that the restore must delete first
    */
  final case class FailAt(
    label: String,
    step: MigrationStep,
    fault: Rule = PartialFunction.empty,
    badDoc: Boolean = false,
    partial: Option[(String, Int)] = None,
    tempExists: Boolean = true,
    origPresent: Boolean = false,
    indexLeft: Boolean = false
  ) {
    def reached(s: MigrationStep): Boolean = steps.indexOf(step) >= steps.indexOf(s)
  }

  private val failAts = Seq(
    FailAt(
      "CreateTemp, refused",
      CreateTemp,
      { case ("create", "tmp", 1) => Refuse },
      tempExists = false
    ),
    FailAt(
      "CreateTemp, created but unacknowledged",
      CreateTemp,
      { case ("create", "tmp", 1) => Unacknowledged }
    ),
    FailAt("CopyToTemp, a document refused", CopyToTemp, badDoc = true),
    FailAt("CopyToTemp, an error", CopyToTemp, { case ("copy", "t>tmp", 1) => ErrorAfterEffect }),
    FailAt("CopyToTemp, thrown", CopyToTemp, { case ("copy", "t>tmp", 1) => Throws }),
    FailAt(
      "DeleteOriginal, refused",
      DeleteOriginal,
      { case ("delete", "t", 1) => Refuse },
      origPresent = true
    ),
    FailAt(
      "DeleteOriginal, took effect",
      DeleteOriginal,
      { case ("delete", "t", 1) => ErrorAfterEffect }
    ),
    FailAt(
      "DeleteOriginal, lands after the probe",
      DeleteOriginal,
      { case ("delete", "t", 1) => Late },
      origPresent = true
    ),
    FailAt("RecreateOriginal, refused", RecreateOriginal, { case ("create", "t", 1) => Refuse }),
    FailAt(
      "RecreateOriginal, created but unacknowledged",
      RecreateOriginal,
      { case ("create", "t", 1) => Unacknowledged },
      indexLeft = true
    ),
    FailAt("RecreateOriginal, thrown", RecreateOriginal, { case ("create", "t", 1) => Throws }),
    FailAt("CopyBack, partial", CopyBack, partial = Some(("tmp>t", 1)), indexLeft = true),
    FailAt(
      "OpenOriginal, refused",
      OpenOriginal,
      { case ("open", "t", 1) => Refuse },
      indexLeft = true
    ),
    FailAt("DeleteTemp, refused", DeleteTemp, { case ("delete", "tmp", 1) => Refuse }),
    FailAt("DeleteTemp, lands after the answer", DeleteTemp, { case ("delete", "tmp", 1) => Late })
  )

  /** A rollback request that fails too. Its number is counted after the forward's. */
  final case class RollbackFault(label: String, rule: (FailAt, IndexCluster) => Rule)

  private def forwardCount(f: FailAt, op: String): Int = op match {
    case "delete" => if (f.reached(DeleteOriginal)) 1 else 0
    case "create" => if (f.reached(RecreateOriginal)) 1 else 0
    case "copy"   => if (f.reached(CopyBack)) 1 else 0
    case "open"   => if (f.reached(OpenOriginal)) 1 else 0
    case _        => 0
  }

  private val noFault = RollbackFault("none", (_, _) => PartialFunction.empty)
  private val deleteIndexFault = RollbackFault(
    "delete index",
    (f, _) => { case ("delete", "t", n) if n == forwardCount(f, "delete") + 1 => Refuse }
  )
  private val createFault = RollbackFault(
    "create",
    (f, _) => { case ("create", "t", n) if n == forwardCount(f, "create") + 1 => Refuse }
  )
  private val copyBackFault = RollbackFault(
    "copy back",
    (f, _) => { case ("copy", "tmp>t", n) if n == forwardCount(f, "copy") + 1 => Refuse }
  )
  private val openFault = RollbackFault(
    "open",
    (f, _) => { case ("open", "t", n) if n == forwardCount(f, "open") + 1 => Refuse }
  )
  // the second alias: the first one is already back on the index
  private val addAliasFault =
    RollbackFault("add alias", (_, _) => { case ("addAlias", "t", 2) => Refuse })
  private val deleteTempFault =
    RollbackFault("delete temp", (_, _) => { case ("delete", "tmp", 1) => Refuse })
  private val removeAliasFault = RollbackFault(
    "remove alias",
    (_, _) => {
      case ("delete", "tmp", 1)      => Refuse
      case ("removeAlias", "tmp", _) => Refuse
    }
  )
  private val probeFault = RollbackFault(
    "probe",
    (_, c) => { case ("exists", "t", _) if c.count("delete", "t") >= 1 => Refuse }
  )
  // the original's pipelines attached back after the copy back (close, put, open): the put fails,
  // the index stays closed
  private val reattachFault = RollbackFault(
    "attach the pipelines back",
    (_, _) => { case ("updateSettings", "t", 1) => Refuse }
  )

  private def faultsFor(f: FailAt, caller: Caller): Seq[RollbackFault] = {
    // only the ALTER TABLE temporary index carries aliases, only the ALTER TABLE original a
    // pipeline
    val alias = if (caller == ByAlter) Seq(removeAliasFault) else Nil
    val reattach = if (caller == ByAlter) Seq(reattachFault) else Nil
    f.step match {
      case CreateTemp if !f.tempExists     => Seq(noFault)
      case CreateTemp | CopyToTemp         => Seq(noFault, deleteTempFault) ++ alias
      case DeleteOriginal if f.origPresent => Seq(noFault, probeFault) ++ alias
      case DeleteTemp                      => Seq(noFault) ++ alias
      case _ =>
        Seq(noFault) ++
          (if (f.step == DeleteOriginal) Seq(probeFault) else Nil) ++
          (if (f.indexLeft) Seq(deleteIndexFault) else Nil) ++
          Seq(createFault, copyBackFault, openFault) ++ reattach ++
          Seq(addAliasFault, deleteTempFault) ++ alias
    }
  }

  private def expectedState(f: FailAt, r: RollbackFault): String = f.step match {
    case DeleteTemp => "success"
    case CreateTemp | CopyToTemp =>
      if (r == deleteTempFault || r == removeAliasFault) "unchanged, temp kept" else "unchanged"
    case DeleteOriginal if f.origPresent =>
      if (r == probeFault) "kept" else "unchanged, temp kept"
    case _ =>
      r match {
        case `noFault`                              => "restored"
        case `deleteTempFault` | `removeAliasFault` => "restored, temp kept"
        case _                                      => "kept"
      }
  }

  private def stateOf(message: String): String =
    if (message.contains("Index 't' is unchanged"))
      if (message.contains("was kept")) "unchanged, temp kept" else "unchanged"
    else if (message.contains("Index 't' was restored with its documents"))
      if (message.contains("was kept")) "restored, temp kept" else "restored"
    else if (message.contains("Index 't' could not be restored")) "kept"
    else if (message.contains("Index 't' was left as it is")) "no temporary copy"
    else s"none in [$message]"

  private def runCell(caller: Caller, f: FailAt, r: RollbackFault): Unit = {
    val logger = Mockito.mock(classOf[Logger])
    val c = clusterFor(caller, logger, f.badDoc)
    val origin = snap(c, "t").get
    val forward = f.fault
    val rollback = r.rule(f, c)
    c.fault = forward orElse rollback
    f.partial.foreach(c.partialCopies += _)

    val result = migrate(caller, c)
    val messageNow = result.error.map(_.message).getOrElse("")
    // a queued deletion lands
    c.landAll()

    val t = snap(c, "t")
    val tmp = tmpOf(c)
    val all = c.indices.values.flatMap(_.docs.keySet).toSet
    val cluePrefix =
      s"[$caller · ${f.label} · rollback fault: ${r.label}] result=$result calls=${c.calls.mkString(" | ")} "

    withClue(cluePrefix) {
      // the failure is the cell's, not one the cluster raised by itself
      c.unplanned shouldBe empty

      // I1
      origin.docs.keySet.subsetOf(all) shouldBe true

      if (f.step == DeleteTemp) {
        // I3 (DeleteTemp): success, the index holds every document under the new definition
        result.isSuccess shouldBe true
        t.map(_.docs.keySet) shouldBe Some(origin.docs.keySet)
        t.get.properties.path("d").path("format").asText() shouldBe "yyyy-MM-dd"
        val warnings = lines(logger, "warn")
        warnings.exists(w =>
          w.contains("could not be deleted") && w.contains("t_tmp_")
        ) shouldBe true
        if (r == removeAliasFault)
          warnings.exists(_.contains("its aliases could not all be removed")) shouldBe true
        else tmp.flatMap(c.indices.get).foreach(_.aliases shouldBe empty)
      } else {
        result.isFailure shouldBe true
        val error = result.error.get
        val state = stateOf(error.message)
        state shouldBe expectedState(f, r)

        // I5: one state sentence, and its status
        error.message.split("Index 't' ", -1).length - 1 shouldBe 1
        error.message should not include "Rollback failed"
        state match {
          case "kept" | "no temporary copy" => error.statusCode shouldBe Some(500)
          case _                            => error.statusCode should not be Some(500)
        }
        if (caller == ByAlter) error.operation shouldBe Some("schema")

        // I2: an original never deleted is untouched
        t.filter(_.uuid == origin.uuid).foreach(_ shouldBe origin)
        if (!f.reached(DeleteOriginal)) t shouldBe Some(origin)

        // I3: a restore gives back the original definition and every document
        if (state.startsWith("restored")) {
          val restored = t.getOrElse(fail("no index after a restore"))
          restored.properties shouldBe origin.properties
          restored.settings shouldBe origin.settings
          restored.aliases shouldBe origin.aliases
          restored.docs shouldBe origin.docs
          if (state == "restored") tmp shouldBe None
        }

        // I4 (a): a temporary index left is named
        tmp.foreach(name => messageNow should include(s"'$name'"))
        if (state == "kept") tmp should not be empty

        // a failed attach-back of the pipelines: the index holds the documents, so the temporary
        // index kept beside it loses its aliases; an ALTER TABLE restore attaches them back through
        // updateSettings (close, put, open), so its refused open fails the attach-back too
        if (r == reattachFault || (r == openFault && caller == ByAlter)) {
          messageNow should include(
            "re-attaching the original pipelines to index 't' (close, update the settings, open) failed, the index may be left closed: "
          )
          t.map(_.docs) shouldBe Some(origin.docs)
          tmp.flatMap(c.indices.get).map(_.aliases.isEmpty) shouldBe Some(true)
        }
      }

      // I4 (b): after a failed deletion of one copy, the other is never deleted
      c.failed.filter(_._2.startsWith("delete:")).foreach { case (position, opRole, _) =>
        val later = c.calls.zipWithIndex.drop(position + 1)
        if (opRole == "delete:tmp") later.map(_._1) should not contain "delete(t)"
        else
          later.find(_._1 == "delete(t_tmp_X)").foreach { case (_, at) =>
            // only once the documents are back in a re-created index
            val copiedBack = c.calls.zipWithIndex.slice(position + 1, at).exists { case (call, i) =>
              call.startsWith("copy(t_tmp_X, t,") && !c.failed.exists(_._1 == i)
            }
            copiedBack shouldBe true
          }
      }

      // I4 (c): no alias reads a temporary index kept beside an index holding every document
      val removeAliasFailed = c.failed.exists(_._2 == "removeAlias:tmp")
      if (removeAliasFailed)
        (messageNow + lines(logger, "warn").mkString) should include(
          "its aliases could not all be removed"
        )
      else
        (tmp.flatMap(c.indices.get), c.indices.get("t")) match {
          case (Some(kept), Some(index)) =>
            // never one alias over both copies
            (kept.aliases.keySet intersect index.aliases.keySet) shouldBe empty
            val keptBeside = f.step == DeleteTemp ||
              Set("unchanged, temp kept", "restored, temp kept").contains(expectedState(f, r))
            if (keptBeside && origin.docs.keySet.subsetOf(index.docs.keySet))
              kept.aliases shouldBe empty
          case _ =>
        }

      // the log lines asserted elsewhere, each ONCE per failed migration
      if (f.step != DeleteTemp) {
        val errors = lines(logger, "error")
        val infos = lines(logger, "info")
        errors.count(_.startsWith("❌ Migration failed for")) shouldBe 1
        infos.count(_.startsWith("Attempting rollback for")) shouldBe 1
        val clean = Set("unchanged", "restored").contains(expectedState(f, r))
        infos.count(_.startsWith("✅ Rollback completed successfully for")) shouldBe (if (clean) 1
                                                                                     else 0)
        infos.count(_.startsWith("✅ Rollback index completed for")) shouldBe (if (clean) 1 else 0)
        errors.count(_.startsWith("❌ Rollback failed for")) shouldBe (if (clean) 0 else 1)
      }
    }
  }

  behavior of "a failed reindex migration"

  for {
    caller <- callers
    f      <- failAts
    r      <- faultsFor(f, caller)
  } it should s"keep every document [$caller · ${f.label} · rollback fault: ${r.label}]" in {
    runCell(caller, f, r)
  }

  it should "keep an index re-created behind a stale schema cache untouched" in {
    // the table is read once (cached), then re-created elsewhere with the same definition: a
    // failure copying to the temporary index leaves the LIVE index as it is
    val logger = Mockito.mock(classOf[Logger])
    val c = clusterFor(ByAlter, logger, withBadDoc = true)
    c.loadSchema("t").isSuccess shouldBe true
    val recreated = snap(c, "t").get
    val idx = c.indices("t")
    c.indices("t") = new c.Idx("uuid-external", idx.mapping, idx.settings, idx.aliases, idx.docs)
    val result = c.run(alter).futureValue
    withClue(s"$result ") {
      c.unplanned shouldBe empty
      stateOf(result.error.get.message) shouldBe "unchanged"
      c.indices("t").uuid shouldBe "uuid-external"
      snap(c, "t").get.copy(uuid = recreated.uuid) shouldBe recreated
      tmpOf(c) shouldBe None
    }
  }

  it should "touch nothing when the temporary index is gone at the restore" in {
    val logger = Mockito.mock(classOf[Logger])
    val c = clusterFor(ByUpdateMapping, logger, withBadDoc = false)
    c.fault = { case ("create", "t", 1) => Refuse }
    // deleted by someone else between the two requests
    c.hook = { case ("create", "t", 1) => tmpOf(c).foreach(c.indices.remove) }
    val result = c.updateMapping("t", newMapping)
    withClue(s"$result calls=${c.calls.mkString(" | ")} ") {
      c.unplanned shouldBe empty
      val error = result.error.get
      stateOf(error.message) shouldBe "no temporary copy"
      error.message should include("no temporary index 't_tmp_")
      error.statusCode shouldBe Some(500)
      val failedAt = c.calls.indexWhere(_.startsWith("create(t,"))
      c.calls
        .drop(failedAt + 1)
        .filter(call => call.startsWith("delete(") || call.startsWith("create(")) shouldBe empty
    }
  }

  /** The requests of a restore that succeeded: its create, then what follows the copy back up to
    * the first alias added back (refreshes and existence checks left out).
    */
  private def restoreRequests(c: IndexCluster): (String, Seq[String]) = {
    val restore = c.calls.drop(c.calls.lastIndexWhere(_.startsWith("create(t,"))).toList
    val copyBack = restore.indexWhere(_.startsWith("copy(t_tmp_X, t,"))
    (
      restore.head,
      restore
        .drop(copyBack + 1)
        .takeWhile(!_.startsWith("addAlias("))
        .filterNot(call => call.startsWith("refresh(") || call.startsWith("exists("))
    )
  }

  it should "copy the documents back through no pipeline, then attach the original's back" in {
    callers.foreach { caller =>
      val c = clusterFor(caller, Mockito.mock(classOf[Logger]), withBadDoc = false)
      val origin = snap(c, "t").get
      c.partialCopies += (("tmp>t", 1))
      val result = migrate(caller, c)
      withClue(s"[$caller] $result calls=${c.calls.mkString(" | ")} ") {
        c.unplanned shouldBe empty
        stateOf(result.error.get.message) shouldBe "restored"
        val (create, afterCopyBack) = restoreRequests(c)
        caller match {
          case ByAlter =>
            // the original's default pipeline, set to `_none` at the create
            create should include(""""default_pipeline":"_none"""")
            create should not include live
            // attached back after the copy back, in place of the open
            afterCopyBack shouldBe Seq(
              "close(t)",
              s"""updateSettings(t, {"index":{"default_pipeline":"$live"}})""",
              "open(t)"
            )
          case ByUpdateMapping =>
            // no pipeline in the original: the restore opens the index, as before
            create should not include "_pipeline"
            afterCopyBack shouldBe Seq("open(t)")
        }
        snap(c, "t").get.settings shouldBe origin.settings
      }
    }
  }

  it should "detach and attach back both pipelines of a raw settings backup" in {
    val c = new IndexCluster(log = Mockito.mock(classOf[Logger]))
    c.stage(
      "t",
      origMapping,
      """{"number_of_shards":"1","default_pipeline":"p_default","final_pipeline":"p_final"}""",
      aliases,
      docs
    )
    val origin = snap(c, "t").get
    c.partialCopies += (("tmp>t", 1))
    val result = c.updateMapping("t", newMapping)
    withClue(s"$result calls=${c.calls.mkString(" | ")} ") {
      c.unplanned shouldBe empty
      stateOf(result.error.get.message) shouldBe "restored"
      val (create, afterCopyBack) = restoreRequests(c)
      create should include(""""default_pipeline":"_none"""")
      create should include(""""final_pipeline":"_none"""")
      afterCopyBack shouldBe Seq(
        "close(t)",
        """updateSettings(t, {"index":{"default_pipeline":"p_default","final_pipeline":"p_final"}})""",
        "open(t)"
      )
      snap(c, "t").get shouldBe origin.copy(uuid = c.indices("t").uuid)
    }
  }

  // ----------------------------------------------------------------------------------------------
  // the controls: main's migration and rollback, on the same cluster
  // ----------------------------------------------------------------------------------------------

  /** Main's `performMigration` + `rollbackMigration` (the updateMapping caller), verbatim but for
    * the receiver.
    */
  private def mainMigration(c: IndexCluster): ElasticResult[Boolean] = {
    val tmp = "t_tmp_0badc0de"
    val (origM, origS, origA) = (for {
      m <- c.getMapping("t")
      s <- c.loadSettings("t")
      a <- c.getAliases("t")
    } yield (m, s, a)).get
    val forward = for {
      _ <- c.createIndex(tmp, c.defaultSettings, Some(newMapping), Nil).filter(_ == true)
      _ <- c.reindex("t", tmp, refresh = true).filter(_._1 == true)
      _ <- c.deleteIndex("t").filter(_ == true)
      _ <- c.createIndex("t", c.defaultSettings, Some(newMapping), Nil).filter(_ == true)
      _ <- c.reindex(tmp, "t", refresh = true).filter(_._1 == true)
      _ <- c.openIndex("t").filter(_ == true)
      _ <- c.deleteIndex(tmp)
    } yield true
    forward match {
      case failure @ ElasticFailure(_) =>
        for {
          tempExists <- c.indexExists(tmp, pattern = false)
          _ <- c.indexExists("t", pattern = false).flatMap {
            case true  => c.deleteIndex("t")
            case false => ElasticResult.success(true)
          }
          _ <- c.createIndex("t", origS, Some(origM), origA).filter(_ == true)
          _ <-
            if (tempExists) c.reindex(tmp, "t", refresh = true).filter(_._1 == true)
            else ElasticResult.success((true, None))
          _ <- c.openIndex("t")
          _ <- if (tempExists) c.deleteIndex(tmp) else ElasticResult.success(true)
        } yield true
        failure
      case success => success
    }
  }

  private val controls: Seq[(Int, FailAt)] = Seq(
    1 -> FailAt("create temp", CreateTemp, { case ("create", "tmp", 1) => Refuse }),
    2 -> FailAt("copy to temp", CopyToTemp, badDoc = true),
    3 -> FailAt("delete original", DeleteOriginal, { case ("delete", "t", 1) => Refuse }),
    4 -> FailAt("re-create", RecreateOriginal, { case ("create", "t", 1) => Refuse }),
    5 -> FailAt("copy back", CopyBack, partial = Some(("tmp>t", 1))),
    6 -> FailAt("open", OpenOriginal, { case ("open", "t", 1) => Refuse }),
    7 -> FailAt("delete temp", DeleteTemp, { case ("delete", "tmp", 1) => ErrorAfterEffect })
  )

  for ((row, f) <- controls)
    it should s"lose the index under main's rollback [control, row $row: ${f.label}]" in {
      val c = clusterFor(ByUpdateMapping, Mockito.mock(classOf[Logger]), f.badDoc)
      val origin = snap(c, "t").get
      c.fault = f.fault
      f.partial.foreach(c.partialCopies += _)
      mainMigration(c).isFailure shouldBe true
      val all = c.indices.values.flatMap(_.docs.keySet).toSet
      withClue(s"calls=${c.calls.mkString(" | ")} ") {
        // the re-create with the generated settings is refused: the index is gone
        c.indices.contains("t") shouldBe false
        // rows 1, 2 and 7 lose documents; rows 3 to 6 leave them only in the temporary index
        if (Set(1, 2, 7).contains(row)) origin.docs.keySet.subsetOf(all) shouldBe false
        else {
          origin.docs.keySet.subsetOf(all) shouldBe true
          tmpOf(c) should not be empty
        }
      }
    }

  // ----------------------------------------------------------------------------------------------
  // census: a successful migration is main's, request for request
  // ----------------------------------------------------------------------------------------------

  private val status =
    """{"set":{"field":"status","value":"active","if":"ctx.status == null","ignore_failure":true}}"""

  private def censusTable(ddl: String): Table = Parser(ddl) match {
    case Right(ct: CreateTable) => ct.schema.setDefaultPipelineName(live)
    case other                  => fail(s"$other")
  }

  private def storedOf(of: Table, extra: String*): String = {
    val node = mapper.readTree(of.defaultPipeline.json).asInstanceOf[ObjectNode]
    val processors = node.get("processors").asInstanceOf[ArrayNode]
    extra.foreach(p => processors.add(mapper.readTree(p)))
    mapper.writeValueAsString(node)
  }

  private def censusCluster(
    of: Table,
    docs: Seq[(String, String)],
    extra: String*
  ): IndexCluster = {
    val c = new IndexCluster(Map(live -> storedOf(of, extra: _*)))
    c.stage(
      "t",
      mapper.writeValueAsString(of.indexMappings),
      mapper.writeValueAsString(of.indexSettings.path("index")),
      aliases,
      docs
    )
    c
  }

  private lazy val mainCensus: Map[String, Seq[String]] = {
    val source = Source.fromResource("migration-census-main.txt")
    try source
      .getLines()
      .toSeq
      .map(_.split("\\|", 3))
      .groupBy(_(0))
      .map { case (shape, ls) => shape -> ls.sortBy(_(1).toInt).map(_(2)) } finally source.close()
  }

  it should "send main's requests when an updateMapping migration succeeds [census]" in {
    val c = new IndexCluster()
    c.stage(
      "t",
      """{"properties":{"id":{"type":"keyword"},"n":{"type":"integer"}}}""",
      """{"number_of_shards":"1","number_of_replicas":"0"}""",
      aliases,
      Seq("1" -> """{"id":"1","n":1}""", "2" -> """{"id":"2","n":2}""")
    )
    c.updateMapping(
      "t",
      """{"properties":{"id":{"type":"keyword"},"n":{"type":"long"}}}"""
    ) shouldBe
    ElasticSuccess(true)
    c.calls.toList shouldBe mainCensus("updateMapping")
  }

  it should "send main's requests when an ALTER TABLE migration succeeds [census]" in {
    val c = censusCluster(
      censusTable("CREATE TABLE t (id KEYWORD, n INT, PRIMARY KEY (id))"),
      Seq("1" -> """{"id":"1","n":1}""", "2" -> """{"id":"2","n":2}""")
    )
    c.run("ALTER TABLE t ALTER COLUMN n SET DATA TYPE BIGINT").futureValue.isSuccess shouldBe true
    c.calls.toList shouldBe mainCensus("alter")
  }

  it should "send main's requests when an ALTER TABLE migration through a routed pipeline succeeds [census]" in {
    val c = censusCluster(
      censusTable(
        "CREATE TABLE t (id KEYWORD, a INT, b INT, c INT SCRIPT AS (a + b), PRIMARY KEY (id))"
      ),
      Seq("1" -> """{"id":"1","a":1,"b":2,"c":3}""", "2" -> """{"id":"2","a":2,"b":2,"c":4}"""),
      status
    )
    c.run("ALTER TABLE t ADD COLUMN x INT SCRIPT AS (a * 2)").futureValue.isSuccess shouldBe true
    c.calls.toList shouldBe mainCensus("routed")
  }

  // ----------------------------------------------------------------------------------------------
  // what the user reads, the alias filters, the mapping a restore creates
  // ----------------------------------------------------------------------------------------------

  behavior of "IndexMigration.migrationError"

  private val cause =
    ElasticError(
      "Reindex failed for index 't_tmp_1'",
      statusCode = Some(400),
      operation = Some("reindex")
    )

  it should "answer the cause plus one state sentence, with the status of the state" in {
    def error(outcome: RollbackOutcome): ElasticError =
      IndexMigration.migrationError("t", "t_tmp_1", cause, outcome)
    val tempError = ElasticError("delete refused.")

    error(
      Unchanged(None)
    ).message shouldBe "Reindex failed for index 't_tmp_1'. Index 't' is unchanged."
    error(Unchanged(Some(tempError))).message shouldBe
    "Reindex failed for index 't_tmp_1'. Index 't' is unchanged; the temporary index 't_tmp_1' was kept: delete refused."
    error(Restored(None)).message shouldBe
    "Reindex failed for index 't_tmp_1'. Index 't' was restored with its documents."
    error(Restored(Some(tempError))).message shouldBe
    "Reindex failed for index 't_tmp_1'. Index 't' was restored with its documents; the temporary index 't_tmp_1' was kept: delete refused."
    error(Kept(ElasticError("create refused"))).message shouldBe
    "Reindex failed for index 't_tmp_1'. Index 't' could not be restored: create refused; its documents are kept in index 't_tmp_1'."
    error(NoTemporaryCopy).message shouldBe
    "Reindex failed for index 't_tmp_1'. Index 't' was left as it is: no temporary index 't_tmp_1' exists."

    Seq(Unchanged(None), Unchanged(Some(tempError)), Restored(None), Restored(Some(tempError)))
      .foreach(o => error(o).statusCode shouldBe Some(400))
    Seq(Kept(tempError), NoTemporaryCopy).foreach(o => error(o).statusCode shouldBe Some(500))
    error(NoTemporaryCopy).operation shouldBe Some("reindex")
  }

  behavior of "IndexMigration.plainAlias"

  it should "render a filter read from an _alias body unchanged, through node and Value(filter).toJson" in {
    val c = new IndexCluster()
    c.stage("t", origMapping, origSettings, aliases, docs)
    val expected = mapper.readTree(filter)
    val read: Seq[TableAlias] = c.getAliases("t").get
    val loaded: Seq[TableAlias] = c.loadSchema("t").get.indexAliases
    Seq(read, loaded).foreach { as =>
      val f = as.find(_.alias == "t_f").get
      // the control: as read, the filter keeps Values below its first key. Not through `f.node`:
      // Jackson walks those Values as beans, through the NULL every statement of the JVM shares,
      // and never ends once a statement has given that NULL an expression
      f.filter.values.exists {
        case below: scala.collection.Map[_, _] => below.values.exists(_.isInstanceOf[Value[_]])
        case _                                 => false
      } shouldBe true
      val plain = IndexMigration.plainAlias(f)
      plain.node.get("filter") shouldBe expected
      Value(plain.filter).asInstanceOf[ObjectValue].toJson shouldBe expected
      val a = as.find(_.alias == "t_a").get
      IndexMigration.plainAlias(a) shouldBe a
    }
    // a filter built from raw values is left as it is
    val raw = TableAlias("t", "r", filter = ListMap("term" -> Map("k" -> "a")))
    IndexMigration.plainAlias(raw) shouldBe raw
  }

  behavior of "IndexMigration.creatableMapping"

  it should "remove the wrapper getMapping answers, and nothing else" in {
    def creatable(mapping: String): String = IndexMigration.creatableMapping("t", mapping)
    val m = """{"properties":{"d":{"type":"date"}},"_meta":{"type":"regular"}}"""
    creatable(s"""{"mappings":$m}""") shouldBe m
    creatable(s"""{"t":{"mappings":$m}}""") shouldBe m
    creatable(m) shouldBe m
    creatable(s"""{"_doc":$m}""") shouldBe s"""{"_doc":$m}"""
    creatable("""{"properties":{}}""") shouldBe """{"properties":{}}"""
    // a bare mapping whose one field is named `mappings` (the REST clients' shape)
    val keywordField = """{"properties":{"mappings":{"type":"keyword"}}}"""
    creatable(keywordField) shouldBe keywordField
    val objectField = """{"properties":{"mappings":{"properties":{"x":{"type":"keyword"}}}}}"""
    creatable(objectField) shouldBe objectField
    // another index's entry is not this index's mapping
    creatable(s"""{"u":{"mappings":$m}}""") shouldBe s"""{"u":{"mappings":$m}}"""
    // an index named `mappings`: both wrapped shapes
    IndexMigration.creatableMapping("mappings", s"""{"mappings":{"mappings":$m}}""") shouldBe m
    IndexMigration.creatableMapping("mappings", s"""{"mappings":$m}""") shouldBe m
  }

  behavior of "IndexMigration.detachPipelines"

  it should "set each pipeline setting carried to _none, and attach the original values back" in {
    // a table's settings, under `index`
    IndexMigration.detachPipelines(
      """{"index":{"number_of_shards":"1","default_pipeline":"p","analysis":{"a":{"b":"c"}}}}"""
    ) shouldBe (
      (
        """{"index":{"number_of_shards":"1","default_pipeline":"_none","analysis":{"a":{"b":"c"}}}}""",
        Some("""{"index":{"default_pipeline":"p"}}""")
      )
    )
    // the raw `index` object GET _settings answers, both settings
    IndexMigration.detachPipelines(
      """{"final_pipeline":"f","number_of_shards":"1","default_pipeline":"p"}"""
    ) shouldBe (
      (
        """{"final_pipeline":"_none","number_of_shards":"1","default_pipeline":"_none"}""",
        Some("""{"index":{"default_pipeline":"p","final_pipeline":"f"}}""")
      )
    )
    // none carried: the settings as they were, nothing to attach back
    val none = """{"index":{"number_of_shards":"1"}}"""
    IndexMigration.detachPipelines(none) shouldBe ((none, None))
    IndexMigration.detachPipelines("[]") shouldBe (("[]", None))
  }
}
