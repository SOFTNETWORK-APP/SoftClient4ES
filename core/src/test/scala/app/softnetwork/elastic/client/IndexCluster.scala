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

import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql.schema.TableAlias
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

/** A cluster kept in memory, document by document, for the migration specs: every index holds its
  * uuid, mapping, settings (the four generated keys included), aliases and documents.
  *
  *   - its create REFUSES a generated setting, as Elasticsearch does, and an index that exists;
  *   - its reindex copies the documents in order and stops at the first one the target mapping does
  *     not accept (an integer field holding a non-integer, a `yyyy-MM-dd` date holding a
  *     timestamp), leaving the documents copied before it;
  *   - its create keeps a `TableAlias` as the ES 8 client sends it, without its filter; its
  *     add-alias records the alias node;
  *   - every request can be faulted: [[IndexCluster.Fault]]; any other exception the cluster raises
  *     is a defect of the cluster, recorded in [[IndexCluster.unplanned]].
  *
  * Requests are named `(op, role, n)`: `op` in create, copy, delete, open, exists, addAlias,
  * removeAlias, updateSettings; `role` is the index name, `tmp` for a temporary index
  * (`<name>_tmp_…`), and `<source role>><target role>` for a copy; `n` counts the requests of that
  * op on that role.
  */
class IndexCluster(
  pipelines: Map[String, String] = Map.empty,
  private val log: Logger = LoggerFactory.getLogger(classOf[IndexCluster])
) extends NopeClientApi {
  import IndexCluster._

  override protected def logger: Logger = log

  final class Idx(
    val uuid: String,
    var mapping: ObjectNode,
    var settings: ObjectNode,
    val aliases: mutable.LinkedHashMap[String, ObjectNode],
    val docs: mutable.LinkedHashMap[String, ObjectNode]
  )

  val indices: mutable.LinkedHashMap[String, Idx] = mutable.LinkedHashMap()
  val live: mutable.Map[String, String] = mutable.Map(pipelines.toSeq: _*)

  /** Every request, in order, with its arguments (temporary names normalised to `_tmp_X`). */
  val calls: mutable.ListBuffer[String] = mutable.ListBuffer()

  /** Every request answered with an error, `false` or a throw: its position in [[calls]], `op:role`
    * and what it answered.
    */
  val failed: mutable.ListBuffer[(Int, String, String)] = mutable.ListBuffer()

  /** Every exception a request raised that no [[IndexCluster.Throws]] fault planned: a defect of
    * the cluster itself, which a migration would otherwise turn into an ordinary failure.
    */
  val unplanned: mutable.ListBuffer[(String, Throwable)] = mutable.ListBuffer()

  /** The copies, by `(role, n)`, that stop after their first document and answer a failure. */
  val partialCopies: mutable.Set[(String, Int)] = mutable.Set()

  /** The fault of a request, by `(op, role, n)`. */
  var fault: PartialFunction[(String, String, Int), Fault] = PartialFunction.empty

  /** Run before a request is answered (an external change between two requests). */
  var hook: PartialFunction[(String, String, Int), Unit] = PartialFunction.empty

  private val counts = mutable.Map[(String, String), Int]()
  private var pending: List[() => Unit] = Nil
  private var uuids = 0

  def count(op: String, role: String): Int = counts.getOrElse((op, role), 0)

  /** Lands every deletion still queued. */
  def landAll(): Unit = {
    val landing = pending
    pending = Nil
    landing.reverse.foreach(_())
  }

  private def call[T](op: String, role: String, desc: String, unacknowledged: => T)(
    effect: => ElasticResult[T]
  ): ElasticResult[T] = {
    calls += normalise(desc)
    val position = calls.size - 1
    val landing = pending
    pending = Nil
    val n = count(op, role) + 1
    counts((op, role)) = n
    try {
      if (hook.isDefinedAt((op, role, n))) hook((op, role, n))
      val answer: ElasticResult[T] = fault.lift((op, role, n)) match {
        case None => effect
        case Some(Refuse) =>
          ElasticFailure(ElasticError(s"$op refused on $role", statusCode = Some(503)))
        case Some(ErrorAfterEffect) =>
          effect match {
            case ElasticSuccess(_) =>
              ElasticFailure(ElasticError(s"$op of $role timed out", statusCode = Some(504)))
            case failure => failure
          }
        case Some(Unacknowledged) =>
          effect match {
            case ElasticSuccess(_) => ElasticSuccess(unacknowledged)
            case failure           => failure
          }
        case Some(Late) =>
          pending ::= (() => { effect; () })
          ElasticFailure(ElasticError(s"$op of $role timed out", statusCode = Some(504)))
        case Some(Throws) =>
          failed += ((position, op + ":" + role, "throw"))
          throw new PlannedThrow(s"$op threw on $role")
      }
      answer match {
        case ElasticFailure(e)          => failed += ((position, op + ":" + role, e.message))
        case ElasticSuccess(false)      => failed += ((position, op + ":" + role, "false"))
        case ElasticSuccess((false, _)) => failed += ((position, op + ":" + role, "false"))
        case _                          =>
      }
      answer
    } catch {
      case NonFatal(e) if !e.isInstanceOf[PlannedThrow] =>
        unplanned += ((normalise(desc), e))
        throw e
    } finally landing.reverse.foreach(_())
  }

  // --------------------------------------------------------------------------------------------
  // staging
  // --------------------------------------------------------------------------------------------

  private def nextUuid(): String = { uuids += 1; s"uuid-$uuids" }

  private def generated(index: String, uuid: String): Seq[(String, JsonNode)] = Seq(
    "uuid"          -> mapper.getNodeFactory.textNode(uuid),
    "creation_date" -> mapper.getNodeFactory.textNode("1700000000000"),
    "provided_name" -> mapper.getNodeFactory.textNode(index),
    "version"       -> mapper.readTree("""{"created":"8521000"}""")
  )

  /** An index as Elasticsearch holds it: `settings` is the `index` object without the generated
    * keys, which are added here; `aliases` maps an alias name to its node as `_alias` answers it.
    */
  def stage(
    index: String,
    mapping: String,
    settings: String,
    aliases: Seq[(String, String)],
    docs: Seq[(String, String)]
  ): Idx = {
    val uuid = nextUuid()
    val s = mapper.readTree(settings).deepCopy[ObjectNode]()
    generated(index, uuid).foreach { case (k, v) => s.set[JsonNode](k, v) }
    val idx = new Idx(
      uuid,
      mapper.readTree(mapping).deepCopy[ObjectNode](),
      s,
      mutable.LinkedHashMap(aliases.map { case (a, node) =>
        a -> mapper.readTree(node).deepCopy[ObjectNode]()
      }: _*),
      mutable.LinkedHashMap(docs.map { case (id, src) =>
        id -> mapper.readTree(src).deepCopy[ObjectNode]()
      }: _*)
    )
    indices(index) = idx
    idx
  }

  // --------------------------------------------------------------------------------------------
  // renders
  // --------------------------------------------------------------------------------------------

  private def aliasesNode(idx: Idx): ObjectNode = {
    val node = mapper.createObjectNode()
    idx.aliases.foreach { case (a, n) => node.set[JsonNode](a, n.deepCopy()) }
    node
  }

  private def indexDocument(index: String, idx: Idx): String = {
    val doc = mapper.createObjectNode()
    doc.set[JsonNode]("aliases", aliasesNode(idx))
    doc.set[JsonNode]("mappings", idx.mapping.deepCopy())
    val settings = mapper.createObjectNode()
    settings.set[JsonNode]("index", idx.settings.deepCopy())
    doc.set[JsonNode]("settings", settings)
    val root = mapper.createObjectNode()
    root.set[JsonNode](index, doc)
    mapper.writeValueAsString(root)
  }

  /** An alias as a create request leaves it, the ES 8 client's render (`executeCreateIndex`):
    * routing and flags, NOT the filter. Never `alias.node` here: a filter read back keeps
    * [[app.softnetwork.elastic.sql.Value]] s below its first key, and Jackson walks them as beans,
    * through the NULL every statement of the JVM shares; once a statement has given that NULL an
    * expression, the walk never ends.
    */
  private def createdAlias(alias: TableAlias): ObjectNode =
    alias.copy(filter = ListMap.empty).node

  private def notFound(index: String): ElasticFailure =
    ElasticFailure(
      ElasticError(s"no such index [$index]", statusCode = Some(404), index = Some(index))
    )

  /** Does `doc` fit `mapping`? An integer field takes an integral number; a `yyyy-MM-dd` date takes
    * that pattern only.
    */
  private def fits(mapping: ObjectNode, doc: ObjectNode): Boolean = {
    val properties = mapping.path("properties")
    doc.properties().asScala.forall { entry =>
      val field = properties.path(entry.getKey)
      val value = entry.getValue
      field.path("type").asText("") match {
        case "integer" | "long" | "short" | "byte" => value.isIntegralNumber || value.isNull
        case "date" if field.path("format").asText("") == "yyyy-MM-dd" =>
          value.isNull || value.asText().matches("""\d{4}-\d{2}-\d{2}""")
        case _ => true
      }
    }
  }

  // --------------------------------------------------------------------------------------------
  // requests
  // --------------------------------------------------------------------------------------------

  override private[client] def executeVersion(): ElasticResult[String] = {
    calls += "version"
    ElasticResult.success("8.18.3")
  }

  override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
    call("exists", roleOf(index), s"exists($index)", false) {
      ElasticResult.success(indices.contains(index))
    }

  override private[client] def executeCreateIndex(
    index: String,
    settings: String,
    mappings: Option[String],
    aliases: Seq[TableAlias]
  ): ElasticResult[Boolean] =
    call(
      "create",
      roleOf(index),
      // the alias names only: a filter read back renders its Values as Jackson beans, whose
      // property order is not stable from one JVM to the next
      s"create($index, $settings, ${mappings.getOrElse("-")}, ${aliases.map(_.alias).mkString("[", ",", "]")})",
      false
    ) {
      if (indices.contains(index))
        ElasticFailure(
          ElasticError(
            s"index [$index] already exists",
            statusCode = Some(400),
            index = Some(index)
          )
        )
      else {
        val root = mapper.readTree(settings).deepCopy[ObjectNode]()
        val flat = root.path("index") match {
          case o: ObjectNode =>
            root.remove("index")
            root.properties().asScala.foreach(e => o.set[JsonNode](e.getKey, e.getValue))
            o
          case _ => root
        }
        refusedKeys.find(k => flat.has(k)) match {
          case Some(k) =>
            ElasticFailure(
              ElasticError(
                s"unknown setting [index.$k] please check that any required plugins are installed, or check the breaking changes documentation for removed settings",
                statusCode = Some(400),
                index = Some(index)
              )
            )
          case None =>
            val uuid = nextUuid()
            generated(index, uuid).foreach { case (k, v) => flat.set[JsonNode](k, v) }
            indices(index) = new Idx(
              uuid,
              mappings
                .map(m => mapper.readTree(m).deepCopy[ObjectNode]())
                .getOrElse(mapper.readTree("""{"properties":{}}""").deepCopy[ObjectNode]()),
              flat,
              mutable.LinkedHashMap(aliases.map(a => a.alias -> createdAlias(a)): _*),
              mutable.LinkedHashMap()
            )
            ElasticResult.success(true)
        }
      }
    }

  override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] = {
    calls += normalise(s"getIndex($index)")
    ElasticResult.success(indices.get(index).map(indexDocument(index, _)))
  }

  override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
    call("delete", roleOf(index), s"delete($index)", false) {
      if (indices.remove(index).isDefined) ElasticResult.success(true) else notFound(index)
    }

  override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] =
    call("open", roleOf(index), s"open($index)", false) {
      if (indices.contains(index)) ElasticResult.success(true) else notFound(index)
    }

  override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] = {
    calls += normalise(s"close($index)")
    if (indices.contains(index)) ElasticResult.success(true) else notFound(index)
  }

  override private[client] def executeReindex(
    sourceIndex: String,
    targetIndex: String,
    refresh: Boolean,
    pipeline: Option[String]
  ): ElasticResult[(Boolean, Option[Long])] =
    call[(Boolean, Option[Long])](
      "copy",
      s"${roleOf(sourceIndex)}>${roleOf(targetIndex)}",
      s"copy($sourceIndex, $targetIndex, $refresh, $pipeline)",
      (false, None)
    ) {
      (indices.get(sourceIndex), indices.get(targetIndex)) match {
        case (Some(source), Some(target))
            if partialCopies.contains(
              (
                s"${roleOf(sourceIndex)}>${roleOf(targetIndex)}",
                count("copy", s"${roleOf(sourceIndex)}>${roleOf(targetIndex)}")
              )
            ) =>
          source.docs.headOption.foreach { case (id, doc) => target.docs(id) = doc.deepCopy() }
          ElasticResult.success((false, None))
        case (Some(source), Some(target)) =>
          val refused = source.docs.find { case (_, doc) => !fits(target.mapping, doc) }
          val copied = source.docs.takeWhile { case (id, _) => !refused.exists(_._1 == id) }
          copied.foreach { case (id, doc) => target.docs(id) = doc.deepCopy() }
          if (refused.isDefined) ElasticResult.success((false, None))
          else ElasticResult.success((true, Some(copied.size.toLong)))
        case (None, _) => notFound(sourceIndex)
        case _         => notFound(targetIndex)
      }
    }

  override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = {
    calls += normalise(s"refresh($index)")
    ElasticResult.success(indices.contains(index))
  }

  override private[client] def executeGetMapping(index: String): ElasticResult[String] = {
    calls += normalise(s"getMapping($index)")
    indices.get(index) match {
      case Some(idx) =>
        // as the Java clients answer it
        val root = mapper.createObjectNode()
        root.set[JsonNode]("mappings", idx.mapping.deepCopy())
        ElasticResult.success(mapper.writeValueAsString(root))
      case None => notFound(index)
    }
  }

  override private[client] def executeSetMapping(
    index: String,
    mapping: String
  ): ElasticResult[Boolean] = {
    calls += normalise(s"setMapping($index, $mapping)")
    ElasticResult.success(indices.contains(index))
  }

  override private[client] def executeLoadSettings(index: String): ElasticResult[String] = {
    calls += normalise(s"loadSettings($index)")
    indices.get(index) match {
      case Some(idx) =>
        val inner = mapper.createObjectNode()
        inner.set[JsonNode]("index", idx.settings.deepCopy())
        val settings = mapper.createObjectNode()
        settings.set[JsonNode]("settings", inner)
        val root = mapper.createObjectNode()
        root.set[JsonNode](index, settings)
        ElasticResult.success(mapper.writeValueAsString(root))
      case None => notFound(index)
    }
  }

  override private[client] def executeUpdateSettings(
    index: String,
    settings: String
  ): ElasticResult[Boolean] =
    call("updateSettings", roleOf(index), s"updateSettings($index, $settings)", false) {
      indices.get(index) match {
        case Some(idx) =>
          val root = mapper.readTree(settings)
          val flat = Option(root.get("index")).getOrElse(root)
          flat.properties().asScala.foreach(e => idx.settings.set[JsonNode](e.getKey, e.getValue))
          ElasticResult.success(true)
        case None => notFound(index)
      }
    }

  override private[client] def executeGetAliases(index: String): ElasticResult[String] = {
    calls += normalise(s"getAliases($index)")
    indices.get(index) match {
      case Some(idx) =>
        val doc = mapper.createObjectNode()
        doc.set[JsonNode]("aliases", aliasesNode(idx))
        val root = mapper.createObjectNode()
        root.set[JsonNode](index, doc)
        ElasticResult.success(mapper.writeValueAsString(root))
      case None => notFound(index)
    }
  }

  override private[client] def executeAddAlias(alias: TableAlias): ElasticResult[Boolean] =
    call(
      "addAlias",
      roleOf(alias.table),
      s"addAlias(${alias.table}, ${alias.alias}, ${alias.node})",
      false
    ) {
      indices.get(alias.table) match {
        case Some(idx) =>
          idx.aliases(alias.alias) = alias.node
          ElasticResult.success(true)
        case None => notFound(alias.table)
      }
    }

  override private[client] def executeRemoveAlias(
    index: String,
    alias: String
  ): ElasticResult[Boolean] =
    call("removeAlias", roleOf(index), s"removeAlias($index, $alias)", false) {
      indices.get(index) match {
        case Some(idx) if idx.aliases.remove(alias).isDefined => ElasticResult.success(true)
        case _ =>
          ElasticFailure(ElasticError(s"aliases [$alias] missing", statusCode = Some(404)))
      }
    }

  override private[client] def executeGetPipeline(name: String): ElasticResult[Option[String]] = {
    calls += normalise(s"getPipeline($name)")
    ElasticResult.success(live.get(name))
  }

  override private[client] def executeCreatePipeline(
    name: String,
    definition: String
  ): ElasticResult[Boolean] = {
    calls += normalise(s"createPipeline($name, $definition)")
    live(name) = definition
    ElasticResult.success(true)
  }

  override private[client] def executeDeletePipeline(
    name: String,
    ifExists: Boolean
  ): ElasticResult[Boolean] = {
    calls += normalise(s"deletePipeline($name, $ifExists)")
    live -= name
    ElasticResult.success(true)
  }
}

object IndexCluster {

  /** How a request goes wrong. */
  sealed trait Fault

  /** It answers an error and changes nothing. */
  case object Refuse extends Fault

  /** It takes effect, then answers an error (a timeout after the master applied it). */
  case object ErrorAfterEffect extends Fault

  /** It takes effect and answers `false`. */
  case object Unacknowledged extends Fault

  /** It answers an error; it takes effect after the NEXT request is answered (a queued deletion a
    * probe cannot see).
    */
  case object Late extends Fault

  /** It throws and changes nothing. */
  case object Throws extends Fault

  /** What a [[Throws]] fault throws. */
  final class PlannedThrow(message: String) extends IllegalStateException(message)

  /** What Elasticsearch refuses in a create: the settings it generates. */
  val refusedKeys: Seq[String] = Seq("uuid", "creation_date", "provided_name", "version")

  def roleOf(index: String): String = if (index.contains("_tmp_")) "tmp" else index

  /** One line per request: temporary names as `_tmp_X`, line breaks folded. */
  def normalise(s: String): String =
    s.replaceAll("_tmp_[0-9a-f]{8,13}", "_tmp_X").replaceAll("\\s*\\n\\s*", " ")
}
