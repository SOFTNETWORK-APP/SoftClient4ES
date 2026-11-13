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

package app.softnetwork.elastic.scalatest

import app.softnetwork.elastic.client.ElasticsearchVersion
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, render}

import java.nio.charset.StandardCharsets

/** What a reset does with the objects of one kind that differ from the boot snapshot. */
sealed trait ResetAction

/** Objects created after the snapshot are deleted. */
case object DeleteNew extends ResetAction

/** Any difference with the snapshot (added or removed) refuses the reset: nothing is deleted. */
case object RefuseChange extends ResetAction

/** The cluster a reset inspects: its version, and whether security and Watcher are on. */
final case class ResetTarget(version: String, security: Boolean, watcher: Boolean)

/** One kind of cluster object a shared-container reset inspects (and, for [[DeleteNew]], deletes).
  *
  * `parse` and `deleteRequests` are pure: the I/O lives in [[SharedElasticContainers]].
  */
trait ResetStep {

  def name: String

  def action: ResetAction

  def applies(t: ResetTarget): Boolean

  /** The GET path that lists the objects of this kind. 200 is parsed, 404 means none. */
  def listPath(t: ResetTarget): String

  /** The objects the list body names, system objects INCLUDED (no filter). */
  def listed(body: JValue, t: ResetTarget): Set[String] = parse(body, t)

  /** The objects the list body names, system objects EXCLUDED: names starting with `.` or holding
    * `@`, objects whose `_meta.managed` is `true`, reserved users and roles.
    */
  def parse(body: JValue, t: ResetTarget): Set[String]

  /** (method, path, body) of the requests that delete these objects; empty for a refused kind. */
  def deleteRequests(names: Seq[String], t: ResetTarget): Seq[(String, String, Option[String])]
}

object ResetStep {

  /** A system object by its name alone: a leading `.` or an `@` (ES's own templates, pipelines,
    * hidden indices).
    */
  def isSystemName(name: String): Boolean = name.startsWith(".") || name.contains("@")

  /** `_meta.managed == true`: an object Elasticsearch installs and re-installs itself. */
  def isManaged(meta: JValue): Boolean = meta \ "managed" match {
    case JBool(true) => true
    case _           => false
  }

  /** The kinds a default reset handles, in deletion order (a policy still used by a pipeline cannot
    * be deleted, nor a component template still used by an index template; a pipeline's script
    * processor can name a stored script).
    */
  lazy val defaults: Seq[ResetStep] = Seq(
    Indices,
    LegacyTemplates,
    IndexTemplates,
    ComponentTemplates,
    Pipelines,
    StoredScripts,
    EnrichPolicies,
    Users,
    Roles,
    ApiKeys,
    Watchers,
    Transforms,
    ClusterSettings,
    Licence
  )

  // ------------------------------------------------------------------------------------------
  // JSON shape helpers: a 200 body whose shape is not the expected one FAILS, so that a wrong
  // per-major endpoint or a changed response can never silently disable a step. A 404 reaches a
  // step as `JNothing` and means none.
  // ------------------------------------------------------------------------------------------

  private[scalatest] def shapeError(step: String, expected: String): Nothing =
    throw new IllegalStateException(s"[es-reset] unexpected $step list: expected $expected")

  private[scalatest] def objectFields(
    step: String,
    v: JValue,
    what: String
  ): List[(String, JValue)] =
    v match {
      case JObject(fields) => fields
      case _               => shapeError(step, s"an object at $what")
    }

  private[scalatest] def arrayElements(step: String, v: JValue, what: String): List[JValue] =
    v match {
      case JArray(elements) => elements
      case _                => shapeError(step, s"an array at $what")
    }

  private[scalatest] def stringOf(v: JValue): Option[String] = v match {
    case JString(s) => Some(s)
    case _          => None
  }

  private[scalatest] def longOf(v: JValue): Option[Long] = v match {
    case JInt(i)     => Some(i.toLong)
    case JLong(l)    => Some(l)
    case JDouble(d)  => Some(d.toLong)
    case JDecimal(d) => Some(d.toLong)
    case JString(s)  => scala.util.Try(s.trim.toLong).toOption
    case _           => None
  }

  private[scalatest] def valueText(v: JValue): String = v match {
    case JString(s) => s
    case JNull      => "null"
    case JNothing   => ""
    case other      => compact(render(other))
  }

  private[scalatest] def reservedMetadata(obj: JValue): Boolean =
    obj \ "metadata" \ "_reserved" match {
      case JBool(true) => true
      case _           => false
    }

  /** A path segment, percent-encoded but for the RFC 3986 unreserved characters. */
  private[scalatest] def segment(name: String): String = {
    val sb = new StringBuilder
    name.getBytes(StandardCharsets.UTF_8).foreach { b =>
      val c = (b & 0xff).toChar
      if (
        (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') ||
        c == '-' || c == '_' || c == '.' || c == '~'
      )
        sb.append(c)
      else sb.append("%%%02X".format(b & 0xff))
    }
    sb.toString
  }

  private def atLeast(t: ResetTarget, major: Int, minor: Int): Boolean =
    ElasticsearchVersion.isAtLeast(t.version, major, minor)

  // ------------------------------------------------------------------------------------------
  // Deletable kinds
  // ------------------------------------------------------------------------------------------

  /** A deletable kind whose list body names objects. */
  abstract class ObjectStep(val name: String) extends ResetStep {

    final val action: ResetAction = DeleteNew

    /** (name, object) of every listed object. */
    protected def objects(body: JValue, t: ResetTarget): Seq[(String, JValue)]

    /** Whether an object is a system one beyond its name (managed, reserved). */
    protected def system(obj: JValue): Boolean = false

    override def listed(body: JValue, t: ResetTarget): Set[String] =
      if (body == JNothing) Set.empty // 404: none
      else objects(body, t).map(_._1).toSet

    override def parse(body: JValue, t: ResetTarget): Set[String] =
      if (body == JNothing) Set.empty // 404: none
      else objects(body, t).collect { case (n, o) if !isSystemName(n) && !system(o) => n }.toSet
  }

  /** Non-dot indices; deleted by name (never a wildcard: `action.destructive_requires_name`). */
  case object Indices extends ObjectStep("indices") {
    def applies(t: ResetTarget): Boolean = true

    /** `expand_wildcards=all` from 7.7 only, so a hidden non-dot index cannot escape. 6.8's
      * `_cat/indices` reads no such parameter and answers 400 to an unconsumed one; 6.x has no
      * hidden indices.
      */
    def listPath(t: ResetTarget): String =
      if (atLeast(t, 7, 7)) "/_cat/indices?format=json&h=index&expand_wildcards=all"
      else "/_cat/indices?format=json&h=index"

    protected def objects(body: JValue, t: ResetTarget): Seq[(String, JValue)] =
      arrayElements(name, body, "the root").map { o =>
        stringOf(o \ "index").getOrElse(shapeError(name, "an index name in each row")) -> o
      }

    def deleteRequests(names: Seq[String], t: ResetTarget): Seq[(String, String, Option[String])] =
      ClusterReset
        .chunks(names.map(segment))
        .map(chunk => ("DELETE", s"/${chunk.mkString(",")}?ignore_unavailable=true", None))
  }

  /** A deletable kind deleted one object per request, at `<prefix>/<name>`. */
  abstract class NamedStep(name: String, deletePrefix: String) extends ObjectStep(name) {
    def deleteRequests(names: Seq[String], t: ResetTarget): Seq[(String, String, Option[String])] =
      names.map(n => ("DELETE", s"$deletePrefix/${segment(n)}", None))
  }

  case object LegacyTemplates extends NamedStep("legacy_templates", "/_template") {
    def applies(t: ResetTarget): Boolean = true
    def listPath(t: ResetTarget): String = "/_template"
    protected def objects(body: JValue, t: ResetTarget): Seq[(String, JValue)] =
      objectFields(name, body, "the root")
    override protected def system(obj: JValue): Boolean = isManaged(obj \ "_meta")
  }

  case object IndexTemplates extends NamedStep("index_templates", "/_index_template") {
    def applies(t: ResetTarget): Boolean =
      ElasticsearchVersion.supportsComposableTemplates(t.version)
    def listPath(t: ResetTarget): String = "/_index_template"
    protected def objects(body: JValue, t: ResetTarget): Seq[(String, JValue)] =
      arrayElements(name, body \ "index_templates", "index_templates").map { o =>
        stringOf(o \ "name").getOrElse(shapeError(name, "a name in each template")) ->
        (o \ "index_template")
      }
    override protected def system(obj: JValue): Boolean = isManaged(obj \ "_meta")
  }

  case object ComponentTemplates extends NamedStep("component_templates", "/_component_template") {
    def applies(t: ResetTarget): Boolean =
      ElasticsearchVersion.supportsComposableTemplates(t.version)
    def listPath(t: ResetTarget): String = "/_component_template"
    protected def objects(body: JValue, t: ResetTarget): Seq[(String, JValue)] =
      arrayElements(name, body \ "component_templates", "component_templates").map { o =>
        stringOf(o \ "name").getOrElse(shapeError(name, "a name in each template")) ->
        (o \ "component_template")
      }
    override protected def system(obj: JValue): Boolean = isManaged(obj \ "_meta")
  }

  /** Ingest pipelines. ES 8 installs built-in pipelines ASYNCHRONOUSLY, possibly after the
    * snapshot: the `.`, `@` and `_meta.managed` filter applies here as for templates.
    */
  case object Pipelines extends NamedStep("pipelines", "/_ingest/pipeline") {
    def applies(t: ResetTarget): Boolean = true
    def listPath(t: ResetTarget): String = "/_ingest/pipeline"
    protected def objects(body: JValue, t: ResetTarget): Seq[(String, JValue)] =
      objectFields(name, body, "the root")
    override protected def system(obj: JValue): Boolean = isManaged(obj \ "_meta")
  }

  /** Stored scripts (and search templates): listed from the cluster state, where an absent key
    * means none.
    */
  case object StoredScripts extends NamedStep("stored_scripts", "/_scripts") {
    def applies(t: ResetTarget): Boolean = true
    def listPath(t: ResetTarget): String =
      "/_cluster/state/metadata?filter_path=metadata.stored_scripts"
    protected def objects(body: JValue, t: ResetTarget): Seq[(String, JValue)] = body match {
      case JObject(_) =>
        body \ "metadata" \ "stored_scripts" match {
          case JNothing => Nil // no stored script: filter_path drops the key
          case scripts  => objectFields(name, scripts, "metadata.stored_scripts")
        }
      case _ => shapeError(name, "an object at the root")
    }
  }

  case object EnrichPolicies extends NamedStep("enrich_policies", "/_enrich/policy") {
    def applies(t: ResetTarget): Boolean = ElasticsearchVersion.supportsEnrich(t.version)
    def listPath(t: ResetTarget): String = "/_enrich/policy"
    protected def objects(body: JValue, t: ResetTarget): Seq[(String, JValue)] =
      arrayElements(name, body \ "policies", "policies").map { p =>
        // {"config": {"<match|geo_match|range>": {"name": …}}}
        objectFields(name, p \ "config", "config").headOption
          .flatMap { case (_, config) => stringOf(config \ "name") }
          .getOrElse(shapeError(name, "config.<type>.name in each policy")) -> p
      }
  }

  /** Users created after the snapshot (secure keys only: the security API refuses every call when
    * security is off, so no such user can exist on an open cluster).
    */
  case object Users extends NamedStep("users", "/_security/user") {
    def applies(t: ResetTarget): Boolean = t.security
    def listPath(t: ResetTarget): String = "/_security/user"
    protected def objects(body: JValue, t: ResetTarget): Seq[(String, JValue)] =
      objectFields(name, body, "the root")
    override protected def system(obj: JValue): Boolean = reservedMetadata(obj)
  }

  case object Roles extends NamedStep("roles", "/_security/role") {
    def applies(t: ResetTarget): Boolean = t.security
    def listPath(t: ResetTarget): String = "/_security/role"
    protected def objects(body: JValue, t: ResetTarget): Seq[(String, JValue)] =
      objectFields(name, body, "the root")
    override protected def system(obj: JValue): Boolean = reservedMetadata(obj)
  }

  /** API keys, compared by id among the VALID ones: an invalidated key stays listed until its
    * retention period ends. "Deleting" one invalidates it.
    */
  case object ApiKeys extends ObjectStep("api_keys") {
    def applies(t: ResetTarget): Boolean = t.security
    def listPath(t: ResetTarget): String = "/_security/api_key"
    protected def objects(body: JValue, t: ResetTarget): Seq[(String, JValue)] =
      arrayElements(name, body \ "api_keys", "api_keys")
        .filterNot(k => (k \ "invalidated") == JBool(true))
        .map(k => stringOf(k \ "id").getOrElse(shapeError(name, "an id in each key")) -> k)
    def deleteRequests(names: Seq[String], t: ResetTarget): Seq[(String, String, Option[String])] =
      if (names.isEmpty) Nil
      else
        Seq(
          (
            "DELETE",
            "/_security/api_key",
            Some(compact(render(JObject("ids" -> JArray(names.map(JString(_)).toList)))))
          )
        )
  }

  // ------------------------------------------------------------------------------------------
  // Refused kinds: a difference with the snapshot refuses the reset
  // ------------------------------------------------------------------------------------------

  abstract class RefusedStep(val name: String) extends ResetStep {
    final val action: ResetAction = RefuseChange

    /** What a 200 body says (a 404 means none). */
    protected def read(body: JValue, t: ResetTarget): Set[String]

    final def parse(body: JValue, t: ResetTarget): Set[String] =
      if (body == JNothing) Set.empty else read(body, t)

    def deleteRequests(names: Seq[String], t: ResetTarget): Seq[(String, String, Option[String])] =
      Nil
  }

  /** Watchers, by their count; inspected only when the key has Watcher on. */
  case object Watchers extends RefusedStep("watchers") {
    def applies(t: ResetTarget): Boolean = t.watcher
    def listPath(t: ResetTarget): String =
      if (atLeast(t, 7, 0)) "/_watcher/stats" else "/_xpack/watcher/stats"
    protected def read(body: JValue, t: ResetTarget): Set[String] = {
      val count = arrayElements(name, body \ "stats", "stats").map { s =>
        longOf(s \ "watch_count").getOrElse(shapeError(name, "a watch_count in each node"))
      }.sum
      Set(s"watch_count=$count")
    }
  }

  case object Transforms extends RefusedStep("transforms") {
    def applies(t: ResetTarget): Boolean = atLeast(t, 7, 5)
    def listPath(t: ResetTarget): String = "/_transform"
    protected def read(body: JValue, t: ResetTarget): Set[String] =
      arrayElements(name, body \ "transforms", "transforms").map { tr =>
        stringOf(tr \ "id").getOrElse(shapeError(name, "an id in each transform"))
      }.toSet
  }

  /** Persistent and transient cluster settings, as `persistent:<key>=<value>`. */
  case object ClusterSettings extends RefusedStep("cluster_settings") {
    def applies(t: ResetTarget): Boolean = true
    def listPath(t: ResetTarget): String = "/_cluster/settings?flat_settings=true"
    protected def read(body: JValue, t: ResetTarget): Set[String] =
      Seq("persistent", "transient").flatMap { scope =>
        objectFields(name, body \ scope, scope).map { case (k, v) => s"$scope:$k=${valueText(v)}" }
      }.toSet
  }

  case object Licence extends RefusedStep("licence") {
    def applies(t: ResetTarget): Boolean = true
    def listPath(t: ResetTarget): String = "/_license"
    protected def read(body: JValue, t: ResetTarget): Set[String] = {
      val licence = body \ "license"
      Seq("type", "status").map { field =>
        s"$field=${stringOf(licence \ field).getOrElse(shapeError(name, s"license.$field"))}"
      }.toSet
    }
  }
}

/** A refused kind: what changed since the snapshot, and the suite after which it was first seen. */
final case class Refusal(step: String, added: Set[String], removed: Set[String], leftBy: String)

/** The deletions, in step order, and the refusals of one reset. */
final case class ResetPlan(deletions: Seq[(String, Seq[String])], refusals: Seq[Refusal])

/** The pure part of a shared-container reset: what differs from the snapshot, what to delete and
  * what to refuse. No I/O.
  */
object ClusterReset {

  /** The longest comma-joined list of index names one DELETE carries. */
  val MaxChunkChars: Int = 3000

  /** Per applicable step, the names that differ from the snapshot: created after it for a deletable
    * kind (system names never count), the symmetric difference for a refused one. A step absent
    * from the snapshot has an empty baseline.
    */
  def differences(
    steps: Seq[ResetStep],
    t: ResetTarget,
    snapshot: Map[String, Set[String]],
    current: Map[String, Set[String]]
  ): Seq[(ResetStep, Set[String], Set[String])] =
    steps.filter(_.applies(t)).map { step =>
      val now = current.getOrElse(step.name, Set.empty[String])
      val atBoot = snapshot.getOrElse(step.name, Set.empty[String])
      step.action match {
        case DeleteNew =>
          (step, (now -- atBoot).filterNot(ResetStep.isSystemName), Set.empty[String])
        case RefuseChange => (step, now -- atBoot, atBoot -- now)
      }
    }

  /** The suite after which each differing object was first seen: an object already known keeps its
    * suite (so a cascade keeps naming the real offender), a new one gets `previous`, an object gone
    * is pruned.
    */
  def updateFirstSeen(
    steps: Seq[ResetStep],
    t: ResetTarget,
    snapshot: Map[String, Set[String]],
    current: Map[String, Set[String]],
    firstSeen: Map[(String, String), String],
    previous: String
  ): Map[(String, String), String] =
    differences(steps, t, snapshot, current).flatMap { case (step, added, removed) =>
      (added ++ removed).toSeq.map { n =>
        val key = (step.name, n)
        key -> firstSeen.getOrElse(key, previous)
      }
    }.toMap

  def plan(
    steps: Seq[ResetStep],
    t: ResetTarget,
    snapshot: Map[String, Set[String]],
    current: Map[String, Set[String]],
    firstSeen: Map[(String, String), String],
    previous: String
  ): ResetPlan = {
    val diffs = differences(steps, t, snapshot, current)
    val deletions = diffs.collect {
      case (step, added, _) if step.action == DeleteNew && added.nonEmpty =>
        step.name -> added.toSeq.sorted
    }
    val refusals = diffs.collect {
      case (step, added, removed)
          if step.action == RefuseChange && (added.nonEmpty || removed.nonEmpty) =>
        val leftBy = (added ++ removed).toSeq.sorted
          .flatMap(n => firstSeen.get((step.name, n)))
          .headOption
          .getOrElse(previous)
        Refusal(step.name, added, removed, leftBy)
    }
    ResetPlan(deletions, refusals)
  }

  /** Greedy chunks whose comma-joined length is at most `maxChars` (a single longer name is a chunk
    * of its own).
    */
  def chunks(names: Seq[String], maxChars: Int = MaxChunkChars): Seq[Seq[String]] = {
    val result = Seq.newBuilder[Seq[String]]
    var current = Vector.empty[String]
    var length = 0
    names.foreach { n =>
      val added = if (current.isEmpty) n.length else length + 1 + n.length
      if (current.nonEmpty && added > maxChars) {
        result += current
        current = Vector(n)
        length = n.length
      } else {
        current = current :+ n
        length = added
      }
    }
    if (current.nonEmpty) result += current
    result.result()
  }

  /** A sorted name list cut at `max` characters. */
  def cut(names: Iterable[String], max: Int = 200): String = {
    val joined = names.toSeq.sorted.mkString(",")
    if (joined.length <= max) joined else joined.take(max) + "…"
  }
}
