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

import app.softnetwork.elastic.scalatest.{
  DeleteNew,
  ElasticDockerTestKit,
  ResetRefusedException,
  ResetStep,
  ResetTarget,
  SharedElasticContainers
}
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.apache.http.util.EntityUtils
import org.elasticsearch.client.{Request, ResponseException, RestClient}
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

/** The control of the shared-container reset on an OPEN key, one runner per client module.
  *
  *   - one object of each deletable kind the major supports, planted after the snapshot, is gone
  *     after `reset`, while every object the reset must keep (system objects, the snapshot) stays;
  *   - a persistent cluster setting makes `reset` refuse, naming the suite after which it was first
  *     seen, and deleting nothing; once the setting is restored, `reset` passes.
  *
  * It FAILS (never cancels) when its container is not shared: run without the
  * `softclient4es.testkit.shared-container` property, it is filtered out (`*ResetSpec`).
  */
trait SharedContainerResetSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  private def http(
    client: RestClient,
    method: String,
    path: String,
    body: Option[String] = None
  ): (Int, String) = {
    val request = new Request(method, path)
    body.foreach(b => request.setEntity(new NStringEntity(b, ContentType.APPLICATION_JSON)))
    try {
      val response = client.performRequest(request)
      (
        response.getStatusLine.getStatusCode,
        Option(response.getEntity).map(EntityUtils.toString).getOrElse("")
      )
    } catch {
      case e: ResponseException =>
        (
          e.getResponse.getStatusLine.getStatusCode,
          Option(e.getResponse.getEntity).map(EntityUtils.toString).getOrElse("")
        )
    }
  }

  private def ok(call: (Int, String)): Unit =
    withClue(s"set-up call answered ${call._1}: ${call._2.take(300)}: ")(call._1 should be < 300)

  private def target: ResetTarget =
    SharedElasticContainers
      .targetOf(elasticContainer)
      .getOrElse(fail("this suite's container is not shared"))

  /** (step, raw names, names the reset may delete) per applicable deletable step. */
  private def lists(t: ResetTarget): Map[String, (Set[String], Set[String])] =
    resetSteps
      .filter(s => s.action == DeleteNew && s.applies(t))
      .map { s =>
        val body = SharedElasticContainers.list(restClient, s, t)
        s.name -> ((s.listed(body, t), s.parse(body, t)))
      }
      .toMap

  /** `all_templates`, as `ElasticTestKit.beforeAll` creates it: an in-test reset deletes it. */
  private def restoreAllTemplates(): Unit =
    ok(
      http(
        restClient,
        "PUT",
        "/_template/all_templates",
        Some(
          """{"index_patterns":["*"],"settings":{"number_of_shards":1,"number_of_replicas":0}}"""
        )
      )
    )

  private val Index = "ci1_reset_idx"
  private val Source = "ci1_reset_src"
  private val Legacy = "ci1_reset_legacy"
  private val IndexTemplate = "ci1_reset_it"
  private val Component = "ci1_reset_ct"
  private val Script = "ci1_reset_script"
  private val Pipeline = "ci1_reset_pipeline"
  private val EnrichPipeline = "ci1_reset_enrich_pipeline"
  private val Policy = "ci1_reset_policy"

  /** One object per deletable kind the major supports, as (step, name). */
  private def plant(t: ResetTarget): Seq[(String, String)] = {
    val c = restClient
    val v = t.version
    val composable = ResetStep.IndexTemplates.applies(t)
    val enrich = ResetStep.EnrichPolicies.applies(t)
    val idMapping =
      if (ElasticsearchVersion.isEs6(v))
        """{"_doc":{"properties":{"id":{"type":"keyword"},"name":{"type":"keyword"}}}}"""
      else """{"properties":{"id":{"type":"keyword"},"name":{"type":"keyword"}}}"""
    ok(http(c, "PUT", s"/$Index"))
    ok(http(c, "PUT", s"/$Source", Some(s"""{"mappings":$idMapping}""")))
    ok(http(c, "PUT", s"/$Source/_doc/1?refresh=true", Some("""{"id":"1","name":"one"}""")))
    ok(
      http(
        c,
        "PUT",
        s"/_template/$Legacy",
        Some("""{"index_patterns":["ci1_reset_legacy_*"],"settings":{"number_of_shards":1}}""")
      )
    )
    if (composable) {
      ok(
        http(
          c,
          "PUT",
          s"/_component_template/$Component",
          Some("""{"template":{"settings":{"number_of_replicas":0}}}""")
        )
      )
      ok(
        http(
          c,
          "PUT",
          s"/_index_template/$IndexTemplate",
          Some(s"""{"index_patterns":["ci1_reset_it_*"],"composed_of":["$Component"]}""")
        )
      )
    }
    ok(
      http(
        c,
        "PUT",
        s"/_scripts/$Script",
        Some("""{"script":{"lang":"painless","source":"ctx.ci1 = 1"}}""")
      )
    )
    // the pipeline names the stored script: pipelines are deleted before stored scripts
    ok(
      http(
        c,
        "PUT",
        s"/_ingest/pipeline/$Pipeline",
        Some(s"""{"processors":[{"script":{"id":"$Script"}}]}""")
      )
    )
    if (enrich) {
      ok(
        http(
          c,
          "PUT",
          s"/_enrich/policy/$Policy",
          Some(
            s"""{"match":{"indices":"$Source","match_field":"id","enrich_fields":["name"]}}"""
          )
        )
      )
      ok(http(c, "POST", s"/_enrich/policy/$Policy/_execute?wait_for_completion=true"))
      // a policy a pipeline still uses cannot be deleted: pipelines go before enrich policies
      ok(
        http(
          c,
          "PUT",
          s"/_ingest/pipeline/$EnrichPipeline",
          Some(
            s"""{"processors":[{"enrich":{"policy_name":"$Policy","field":"id","target_field":"e"}}]}"""
          )
        )
      )
    }
    Seq(
      "indices"          -> Index,
      "indices"          -> Source,
      "legacy_templates" -> Legacy,
      "pipelines"        -> Pipeline,
      "stored_scripts"   -> Script
    ) ++
    (if (composable)
       Seq("index_templates" -> IndexTemplate, "component_templates" -> Component)
     else Nil) ++
    (if (enrich) Seq("enrich_policies" -> Policy, "pipelines" -> EnrichPipeline) else Nil)
  }

  "The shared-container reset" should "run on a shared container" in {
    SharedElasticContainers.isShared(elasticContainer) shouldBe true
  }

  it should "delete one object of every deletable kind and keep what it must keep" in {
    val t = target
    val snapshot = SharedElasticContainers.snapshotOf(elasticContainer).getOrElse(Map.empty)
    val planted = plant(t)
    val before = lists(t)
    // the steps SEE every planted object (a parse that missed one would pass vacuously below)
    planted.foreach { case (step, name) =>
      withClue(s"$step before the reset: ")(before(step)._1 should contain(name))
    }
    // what the reset must keep: every listed object it may not delete (system objects, snapshot),
    // but for what the planted objects derive (the policy's `.enrich-ci1_reset_policy-*` index)
    val kept = before.map { case (step, (raw, deletable)) =>
      step -> (raw -- (deletable -- snapshot.getOrElse(step, Set.empty[String])))
        .filterNot(_.contains("ci1_reset"))
    }
    withClue("no system object was listed: the keep check below would be vacuous: ") {
      kept.values.flatten should not be empty
    }
    try {
      SharedElasticContainers.reset(elasticContainer, suiteName, resetSteps)
      val after = lists(t)
      planted.foreach { case (step, name) =>
        withClue(s"$step after the reset: ")(after(step)._1 should not contain name)
      }
      kept.foreach { case (step, names) =>
        withClue(s"$step kept by the reset: ")(names.diff(after(step)._1) shouldBe empty)
      }
    } finally restoreAllTemplates()
  }

  it should "refuse a persistent cluster setting, naming the suite, and pass once it is restored" in {
    val setting = "cluster.routing.allocation.node_concurrent_recoveries"
    ok(
      http(
        restClient,
        "PUT",
        "/_cluster/settings",
        Some(s"""{"persistent":{"$setting":"3"}}""")
      )
    )
    val leftIndex = "ci1_reset_left"
    try {
      ok(http(restClient, "PUT", s"/$leftIndex"))
      val refused = intercept[ResetRefusedException] {
        SharedElasticContainers.reset(elasticContainer, suiteName, resetSteps)
      }
      refused.refusals.map(r => (r.step, r.leftBy)) shouldBe Seq("cluster_settings" -> suiteName)
      refused.refusals.head.added.exists(_.contains(setting)) shouldBe true
      refused.getMessage should include(s"left by $suiteName")
      // nothing was deleted
      http(restClient, "HEAD", s"/$leftIndex")._1 shouldBe 200
    } finally {
      ok(
        http(
          restClient,
          "PUT",
          "/_cluster/settings",
          Some(s"""{"persistent":{"$setting":null}}""")
        )
      )
    }
    try {
      noException should be thrownBy SharedElasticContainers.reset(
        elasticContainer,
        suiteName,
        resetSteps
      )
      http(restClient, "HEAD", s"/$leftIndex")._1 shouldBe 404
    } finally restoreAllTemplates()
  }
}
