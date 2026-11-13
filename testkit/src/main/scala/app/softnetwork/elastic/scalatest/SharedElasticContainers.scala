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

import app.softnetwork.elastic.client.spi.ElasticClientFactory
import org.apache.http.entity.ContentType
import org.apache.http.message.BasicHeader
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.apache.http.{Header, HttpHost}
import org.elasticsearch.client.{Request, ResponseException, RestClient}
import org.json4s._
import org.json4s.jackson.JsonMethods.parse
import org.testcontainers.elasticsearch.ElasticsearchContainer

import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.security.MessageDigest
import java.util.{Base64, Locale}
import scala.util.control.NonFatal

/** The configuration a shared container is keyed by: its image tag and the text of its
  * `elasticsearch.yml`, as rendered by the ONE renderer that also writes the file.
  */
final case class ContainerKey(imageTag: String, yml: String) {

  /** `<tag>/<first 8 hex digits of sha256(yml)>`. */
  def short: String = s"$imageTag/${ContainerKey.sha256(yml).take(8)}"
}

object ContainerKey {
  private[scalatest] def sha256(text: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(text.getBytes(StandardCharsets.UTF_8))
      .map(b => "%02x".format(b & 0xff))
      .mkString
}

/** A reset refused because a refused kind (watchers, transforms, cluster settings, licence) differs
  * from the snapshot. Nothing was deleted.
  */
final class ResetRefusedException(val refusals: Seq[Refusal], message: String)
    extends IllegalStateException(message)

/** One Elasticsearch container per test JVM per [[ContainerKey]], reset before each suite.
  *
  * Enabled by the system property `softclient4es.testkit.shared-container=true` (set in the forked
  * test JVM's `javaOptions`). Without it nothing here is ever reached: [[ElasticDockerTestKit]]
  * starts and stops a container of its own per suite, as it always did.
  *
  *   - [[acquire]] starts the container once, waits for an active licence, takes the snapshot of
  *     every reset step, then registers it. A failed start is never registered: the next suite
  *     tries again.
  *   - [[reset]] (from `ElasticDockerTestKit.start()`, BEFORE each suite) inspects every step,
  *     refuses (deleting nothing) when a refused kind changed, else deletes what was created after
  *     the snapshot, then shuts the cached factory clients down (the next suite has the same URL).
  *   - [[leakReport]] (from `stop()`, after each suite) reports what the suite left. It never
  *     throws.
  *   - A JVM shutdown hook stops every registered container; Ryuk is the backstop.
  *
  * Suites must run SERIALLY in the JVM (sbt's default for a forked test group): a reset deletes the
  * indices another suite would be using.
  *
  * Log lines (stdout): `[es-container]` for every container start, shared or not; `[es-reset]` and
  * `[es-leaks]` for a shared container only. No header, body or password is ever printed.
  */
object SharedElasticContainers {

  val Property: String = "softclient4es.testkit.shared-container"

  /** Read on every call (a `lazy val` would latch it for the whole JVM). */
  def enabled: Boolean = sys.props.get(Property).contains("true")

  private final class Entry(
    val key: ContainerKey,
    val container: ElasticsearchContainer,
    val target: ResetTarget,
    val admin: RestClient,
    val snapshot: Map[String, Set[String]]
  ) {
    @volatile var firstSeen: Map[(String, String), String] = Map.empty
    @volatile var lastSuite: String = "-"
  }

  // Immutable, replaced under the lock: the shutdown hook reads it without locking.
  @volatile private[this] var registry: List[Entry] = Nil

  // Started unshared containers, by identity: counted in `live=` only (never stopped by the hook).
  private[this] val unsharedLive =
    java.util.Collections.newSetFromMap(
      new java.util.IdentityHashMap[ElasticsearchContainer, java.lang.Boolean]()
    )

  @volatile private[this] var hookInstalled = false

  private[this] lazy val module: String = {
    val dir = Paths.get(sys.props.getOrElse("user.dir", ".")).toAbsolutePath.normalize
    val n = dir.getNameCount
    (if (n >= 2) dir.subpath(n - 2, n) else dir).toString.replace('\\', '/')
  }

  private def entryOf(c: ElasticsearchContainer): Option[Entry] = registry.find(_.container eq c)

  /** Whether this container is the shared one of its key (identity, never the property). */
  def isShared(c: ElasticsearchContainer): Boolean = entryOf(c).isDefined

  private[elastic] def targetOf(c: ElasticsearchContainer): Option[ResetTarget] =
    entryOf(c).map(_.target)

  private[elastic] def snapshotOf(c: ElasticsearchContainer): Option[Map[String, Set[String]]] =
    entryOf(c).map(_.snapshot)

  /** The shared container of `key`, started (once) by `create()` when none is registered. */
  def acquire(key: ContainerKey, target: ResetTarget, suite: String, steps: Seq[ResetStep])(
    create: () => ElasticsearchContainer
  ): ElasticsearchContainer = synchronized {
    registry.find(_.key == key) match {
      case Some(entry) => entry.container
      case None =>
        val container = create()
        val bootStart = System.nanoTime()
        try container.start()
        catch {
          case NonFatal(e) =>
            try container.stop()
            catch { case NonFatal(_) => () }
            throw e
        }
        val boot = secondsSince(bootStart)
        var admin: RestClient = null
        try {
          admin = adminClient(container, target)
          val licenceStart = System.nanoTime()
          waitForActiveLicence(admin, target)
          val licence = secondsSince(licenceStart)
          val snapshot = inspect(admin, target, steps)
          val entry = new Entry(key, container, target, admin, snapshot)
          registry = registry :+ entry
          installShutdownHook()
          Console.println(
            s"[es-container] boot=${fmt(boot)}s licence=${fmt(licence)}s key=${key.short} " +
            s"shared=true module=$module suite=$suite id=${idOf(container)} live=$live"
          )
          container
        } catch {
          case NonFatal(e) =>
            if (admin != null) {
              try admin.close()
              catch { case NonFatal(_) => () }
            }
            try container.stop()
            catch { case NonFatal(_) => () }
            throw e
        }
    }
  }

  /** Starts an unshared container exactly as before (`start()`), plus the `[es-container]` line. */
  private[scalatest] def startUnshared(
    c: ElasticsearchContainer,
    key: ContainerKey,
    suite: String
  ): Unit = {
    val bootStart = System.nanoTime()
    c.start()
    val boot = secondsSince(bootStart)
    unsharedLive.synchronized(unsharedLive.add(c))
    Console.println(
      s"[es-container] boot=${fmt(boot)}s licence=- key=${key.short} shared=false " +
      s"module=$module suite=$suite id=${idOf(c)} live=$live"
    )
  }

  /** Stops an unshared container exactly as before (`stop()`). */
  private[scalatest] def stopUnshared(c: ElasticsearchContainer): Unit =
    try c.stop()
    finally unsharedLive.synchronized(unsharedLive.remove(c))

  /** Deletes what was created on this shared container since its snapshot, or throws a
    * [[ResetRefusedException]] (deleting nothing) when a refused kind changed.
    */
  def reset(c: ElasticsearchContainer, suite: String, steps: Seq[ResetStep]): Unit =
    synchronized {
      val entry = entryOf(c).getOrElse(
        throw new IllegalStateException(s"[es-reset] suite=$suite: the container is not shared")
      )
      val t = entry.target
      val start = System.nanoTime()
      val applicable = steps.filter(_.applies(t))
      val current = inspect(entry.admin, t, applicable)
      val previous = entry.lastSuite
      val firstSeen =
        ClusterReset.updateFirstSeen(
          applicable,
          t,
          entry.snapshot,
          current,
          entry.firstSeen,
          previous
        )
      val plan = ClusterReset.plan(applicable, t, entry.snapshot, current, firstSeen, previous)
      entry.firstSeen = firstSeen
      entry.lastSuite = suite
      if (plan.refusals.nonEmpty) {
        val reasons = plan.refusals.map { r =>
          s"${r.step} changed (added=${ClusterReset.cut(r.added)} " +
          s"removed=${ClusterReset.cut(r.removed)}) left by ${r.leftBy}"
        }
        throw new ResetRefusedException(
          plan.refusals,
          s"[es-reset] refused before suite $suite on key ${entry.key.short}: " +
          s"${reasons.mkString("; ")}. Nothing was deleted; a refused kind is never reset."
        )
      }
      val byName = applicable.map(s => s.name -> s).toMap
      plan.deletions.foreach { case (stepName, names) =>
        byName(stepName).deleteRequests(names, t).foreach { case (method, path, body) =>
          send(entry.admin, method, path, body, t)
        }
      }
      // the next suite reaches the same URL: no client cached by the previous one may serve it
      ElasticClientFactory.shutdown()
      val deleted = plan.deletions.map(_._2.size).sum
      val kinds = plan.deletions.map { case (k, names) => s"$k=${ClusterReset.cut(names)}" }
      Console.println(
        s"[es-reset] suite=$suite key=${entry.key.short} took=${fmt(secondsSince(start))}s " +
        s"deleted=$deleted (${kinds.mkString(" ")}) previous=$previous"
      )
    }

  /** Reports what the suite left on this shared container. Never throws. */
  def leakReport(c: ElasticsearchContainer, suite: String, steps: Seq[ResetStep]): Unit = {
    val line =
      try {
        synchronized {
          val entry = entryOf(c).getOrElse(
            throw new IllegalStateException("the container is not shared")
          )
          val t = entry.target
          val applicable = steps.filter(_.applies(t))
          val current = inspect(entry.admin, t, applicable)
          val left = ClusterReset
            .differences(applicable, t, entry.snapshot, current)
            .collect {
              case (step, added, removed) if added.nonEmpty || removed.nonEmpty =>
                step.name -> (added ++ removed)
            }
          val search = get(entry.admin, "/_nodes/stats/indices/search", t)
          val script = get(entry.admin, "/_nodes/stats/script", t)
          def total(stats: JValue, path: String*): String = {
            val values = ResetStep
              .objectFields("nodes", stats \ "nodes", "nodes")
              .map { case (_, node) => path.foldLeft(node)(_ \ _) }
              .flatMap(ResetStep.longOf)
            if (values.isEmpty) "-" else values.sum.toString
          }
          s"[es-leaks] suite=$suite key=${entry.key.short} left=${left.map(_._2.size).sum} " +
          s"(${left.map { case (k, names) => s"$k=${ClusterReset.cut(names)}" }.mkString(" ")}) " +
          s"open_contexts=${total(search, "indices", "search", "open_contexts")} " +
          s"scroll_current=${total(search, "indices", "search", "scroll_current")} " +
          s"script_compilations=${total(script, "script", "compilations")} " +
          s"compilation_limit_triggered=${total(script, "script", "compilation_limit_triggered")}"
        }
      } catch {
        case NonFatal(e) =>
          val message = Option(e.getMessage).getOrElse("").take(200)
          s"[es-leaks] suite=$suite left=? error=${e.getClass.getSimpleName}: $message"
      }
    try Console.println(line)
    catch { case NonFatal(_) => () }
  }

  /** Every applicable step's objects, system objects excluded. */
  private def inspect(
    admin: RestClient,
    t: ResetTarget,
    steps: Seq[ResetStep]
  ): Map[String, Set[String]] =
    steps.filter(_.applies(t)).map(s => s.name -> s.parse(list(admin, s, t), t)).toMap

  /** The list body of one step: 200 is parsed, 404 means none (`JNothing`), any other status fails
    * loud, naming the method, path, status and Elasticsearch version.
    */
  private[elastic] def list(client: RestClient, step: ResetStep, t: ResetTarget): JValue =
    get(client, step.listPath(t), t)

  private def get(client: RestClient, path: String, t: ResetTarget): JValue = {
    val (status, body) =
      try {
        val response = client.performRequest(new Request("GET", path))
        (
          response.getStatusLine.getStatusCode,
          Option(response.getEntity).map(EntityUtils.toString).getOrElse("")
        )
      } catch {
        case e: ResponseException => (e.getResponse.getStatusLine.getStatusCode, "")
      }
    status match {
      case 200 => parse(body)
      case 404 => JNothing
      case other =>
        throw new IllegalStateException(
          s"[es-reset] GET $path answered $other on Elasticsearch ${t.version}"
        )
    }
  }

  private def send(
    client: RestClient,
    method: String,
    path: String,
    body: Option[String],
    t: ResetTarget
  ): Unit = {
    val request = new Request(method, path)
    body.foreach(b => request.setEntity(new NStringEntity(b, ContentType.APPLICATION_JSON)))
    val status =
      try client.performRequest(request).getStatusLine.getStatusCode
      catch { case e: ResponseException => e.getResponse.getStatusLine.getStatusCode }
    // 404: already gone
    if (status >= 300 && status != 404)
      throw new IllegalStateException(
        s"[es-reset] $method $path answered $status on Elasticsearch ${t.version}"
      )
  }

  /** The admin client of a key: anonymous on an open cluster; `elastic` with the container's
    * `ELASTIC_PASSWORD` (Testcontainers sets one for ES >= 8) on a secure one.
    */
  private def adminClient(c: ElasticsearchContainer, t: ResetTarget): RestClient = {
    val builder = RestClient.builder(HttpHost.create(s"http://${c.getHttpHostAddress}"))
    if (t.security) {
      val password = Option(c.getEnvMap.get("ELASTIC_PASSWORD")).getOrElse(
        throw new IllegalStateException(
          "[es-container] security is on but the container declares no ELASTIC_PASSWORD"
        )
      )
      val token = Base64.getEncoder.encodeToString(
        s"elastic:$password".getBytes(StandardCharsets.UTF_8)
      )
      builder.setDefaultHeaders(Array[Header](new BasicHeader("Authorization", s"Basic $token")))
    }
    builder.build()
  }

  /** Waits (once per container) until `GET /_license` answers `"status":"active"`. */
  private def waitForActiveLicence(admin: RestClient, t: ResetTarget): Unit = {
    val deadline = System.nanoTime() + 120L * 1000000000L
    var last = "no answer"
    var active = false
    while (!active && System.nanoTime() < deadline) {
      try {
        val response = admin.performRequest(new Request("GET", "/_license"))
        val body = EntityUtils.toString(response.getEntity).replaceAll("\\s", "")
        active = body.contains("\"status\":\"active\"")
        if (!active) last = "licence not active yet"
      } catch {
        case e: ResponseException =>
          last = s"status ${e.getResponse.getStatusLine.getStatusCode}"
        case NonFatal(e) => last = e.getClass.getSimpleName
      }
      if (!active) Thread.sleep(500)
    }
    if (!active)
      throw new IllegalStateException(
        s"[es-container] no active licence after 120s on Elasticsearch ${t.version} ($last)"
      )
  }

  private def installShutdownHook(): Unit =
    if (!hookInstalled) {
      hookInstalled = true
      sys.addShutdownHook {
        registry.foreach { entry =>
          try entry.admin.close()
          catch { case NonFatal(_) => () }
          try entry.container.stop()
          catch { case NonFatal(_) => () }
        }
      }
      ()
    }

  private def live: Int = registry.size + unsharedLive.synchronized(unsharedLive.size)

  private def idOf(c: ElasticsearchContainer): String =
    Option(c.getContainerId).map(_.take(12)).getOrElse("-")

  private def secondsSince(start: Long): Double = (System.nanoTime() - start) / 1e9

  private def fmt(seconds: Double): String = String.format(Locale.ROOT, "%.2f", Double.box(seconds))
}
