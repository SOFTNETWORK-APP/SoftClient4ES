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

import app.softnetwork.elastic.client.result.ElasticError
import app.softnetwork.elastic.sql.Value
import app.softnetwork.elastic.sql.schema.TableAlias
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode

import scala.collection.immutable.ListMap

/** What a reindex migration backs up before it starts: the definition its rollback restores.
  *
  * @param mapping
  *   the original mapping
  * @param settings
  *   the original settings as JSON, `{"index": {…}}` or the raw `index` object; the settings
  *   Elasticsearch generates are removed when it is restored
  * @param aliases
  *   the original aliases, re-added one by one after the documents are back
  */
private[client] final case class IndexBackup(
  mapping: String,
  settings: String,
  aliases: Seq[TableAlias]
)

/** The steps of a reindex migration, in order. The rollback decides from the step that FAILED. */
private[client] sealed abstract class MigrationStep(val name: String)

private[client] object MigrationStep {

  /** Create the temporary index with the new definition. */
  case object CreateTemp extends MigrationStep("create the temporary index")

  /** Copy the documents to the temporary index. */
  case object CopyToTemp extends MigrationStep("copy to the temporary index")

  /** Delete the original index. */
  case object DeleteOriginal extends MigrationStep("delete the original index")

  /** Re-create the index with the new definition. */
  case object RecreateOriginal extends MigrationStep("re-create the index")

  /** Copy the documents back from the temporary index. */
  case object CopyBack extends MigrationStep("copy back from the temporary index")

  /** Open the re-created index. */
  case object OpenOriginal extends MigrationStep("open the index")

  /** Delete the temporary index. Its failure is not a migration failure: the index already holds
    * every document under the new definition.
    */
  case object DeleteTemp extends MigrationStep("delete the temporary index")
}

/** Where the documents are after the rollback of a failed migration. */
private[client] sealed trait RollbackOutcome

private[client] object RollbackOutcome {

  /** The original index was never deleted and is as it was. `tempLeft` is set when the temporary
    * index was kept: its deletion failed, or the deletion of the original may still take effect.
    */
  final case class Unchanged(tempLeft: Option[ElasticError]) extends RollbackOutcome

  /** The original index was deleted and is back, documents, settings and aliases. `tempLeft` is set
    * when the temporary index could not be deleted afterwards.
    */
  final case class Restored(tempLeft: Option[ElasticError]) extends RollbackOutcome

  /** The restore stopped: the temporary index holds every document and is kept. */
  final case class Kept(reason: ElasticError) extends RollbackOutcome

  /** The original index was deleted and no temporary index exists: nothing was touched. */
  case object NoTemporaryCopy extends RollbackOutcome
}

private[client] object IndexMigration {
  import RollbackOutcome._

  /** The error of a failed migration: the cause, then ONE sentence saying where the documents are.
    * Unchanged and Restored keep the cause's status; any other outcome answers 500.
    */
  def migrationError(
    index: String,
    tempIndex: String,
    cause: ElasticError,
    outcome: RollbackOutcome
  ): ElasticError = {
    def text(e: ElasticError): String = e.message.trim.stripSuffix(".")
    def kept(tempLeft: Option[ElasticError]): String =
      tempLeft.map(e => s"; the temporary index '$tempIndex' was kept: ${text(e)}").getOrElse("")
    val (state, status) = outcome match {
      case Unchanged(tempLeft) =>
        (s"Index '$index' is unchanged${kept(tempLeft)}.", cause.statusCode)
      case Restored(tempLeft) =>
        (s"Index '$index' was restored with its documents${kept(tempLeft)}.", cause.statusCode)
      case Kept(reason) =>
        (
          s"Index '$index' could not be restored: ${text(reason)}; its documents are kept in index '$tempIndex'.",
          Some(500)
        )
      case NoTemporaryCopy =>
        (
          s"Index '$index' was left as it is: no temporary index '$tempIndex' exists.",
          Some(500)
        )
    }
    cause.copy(message = s"${text(cause)}. $state", statusCode = status)
  }

  /** A mapping of `index` as a create request takes it. `getMapping` answers it wrapped, per
    * client: the Java clients as `{"mappings": …}`, Jest as `{"<index>": {"mappings": …}}`, the
    * REST clients as the mapping itself (its `_doc` type, if any, is the create's business). The
    * wrapper is removed; anything else is returned unchanged, so a bare mapping whose one field is
    * named `mappings` is never taken for a wrapper.
    */
  def creatableMapping(index: String, mapping: String): String = {
    def mappingsOf(node: JsonNode): Option[JsonNode] = node match {
      case wrapper: ObjectNode if wrapper.size() == 1 =>
        wrapper.get("mappings") match {
          case inner: ObjectNode => Some(inner)
          case _                 => None
        }
      case _ => None
    }
    scala.util.Try(mapper.readTree(mapping)).toOption match {
      case Some(root: ObjectNode) =>
        // Jest's shape first: an index named `mappings` is answered as {"mappings":{"mappings":…}}
        Option(root.get(index))
          .filter(_ => root.size() == 1)
          .flatMap(mappingsOf)
          .orElse(mappingsOf(root))
          .map(mapper.writeValueAsString)
          .getOrElse(mapping)
      case _ => mapping
    }
  }

  /** The value of a pipeline setting that runs no pipeline (the forward's, too). */
  val NoPipeline: String = "_none"

  /** The pipeline settings a restore detaches while it copies the documents back. */
  val pipelineSettings: Seq[String] = Seq("default_pipeline", "final_pipeline")

  /** The settings a restore creates the index with, and the settings that attach the original
    * pipelines back once the documents are copied back.
    *
    * Each of [[pipelineSettings]] that `settings` carries is set to [[NoPipeline]]: the documents
    * copied back are already as the original held them, and a pipeline would run on them a second
    * time. It is set, not removed, so an index template cannot supply a pipeline. The keys are
    * looked for where `copyableSettings` looks: under `index` when there is one, else at the root;
    * every other key and value is left as it was.
    *
    * @return
    *   the settings to create the index with, and `{"index": {<key>: <original value>, …}}` when
    *   `settings` carries a pipeline setting (`None`: it carries none, nothing to attach back)
    */
  def detachPipelines(settings: String): (String, Option[String]) =
    mapper.readTree(settings) match {
      case root: ObjectNode =>
        val target = root.get("index") match {
          case index: ObjectNode => index
          case _                 => root
        }
        val carried = pipelineSettings.filter(key => target.has(key))
        if (carried.isEmpty) (settings, None)
        else {
          val attach = mapper.createObjectNode()
          val original = attach.putObject("index")
          carried.foreach { key =>
            original.set[JsonNode](key, target.get(key).deepCopy[JsonNode]())
            target.put(key, NoPipeline)
          }
          (mapper.writeValueAsString(root), Some(mapper.writeValueAsString(attach)))
        }
      case _ => (settings, None)
    }

  /** `alias` with a filter Elasticsearch accepts back. A filter read from an index (its `_alias`
    * body, or a loaded table) keeps [[Value]] s below its first key: rendered as they are, they are
    * Jackson beans to a Java client and SQL literals to a REST one. Every nested [[Value]] is
    * unwrapped to its raw value; a raw value is left as it is.
    */
  def plainAlias(alias: TableAlias): TableAlias =
    if (alias.filter.isEmpty) alias
    else alias.copy(filter = alias.filter.map { case (k, v) => k -> plain(v) })

  private def plain(value: Any): Any = value match {
    case v: Value[_] => Value.unwrap(v)
    case m: ListMap[_, _] =>
      m.map { case (k, v) => k -> plain(v) }
    case m: scala.collection.Map[_, _] =>
      ListMap(m.toSeq.map { case (k, v) => k -> plain(v) }: _*)
    case s: Seq[_] => s.map(plain)
    case other     => other
  }
}
