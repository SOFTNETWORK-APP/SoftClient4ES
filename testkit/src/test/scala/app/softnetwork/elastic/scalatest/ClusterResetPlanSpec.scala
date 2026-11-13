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

import org.json4s.JsonAST.JNothing
import org.json4s.jackson.JsonMethods.parse
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The pure part of the shared-container reset, Docker-free: per kind and per major, what a list
  * body names, what the plan deletes (current - snapshot - system objects, in deletion order), what
  * it refuses and whom it blames.
  */
class ClusterResetPlanSpec extends AnyFlatSpec with Matchers {

  private val es6 = ResetTarget("6.8.23", security = false, watcher = true)
  private val es7 = ResetTarget("7.17.29", security = false, watcher = true)
  private val es8 = ResetTarget("8.18.3", security = false, watcher = true)
  private val es9 = ResetTarget("9.0.3", security = false, watcher = true)
  private val es8Secure = es8.copy(security = true)
  private val all = Seq(es6, es7, es8, es9)

  import ResetStep._

  // ------------------------------------------------------------------------------------------
  // list bodies (inline ES JSON) -> names
  // ------------------------------------------------------------------------------------------

  "indices" should "list every index and keep the non-system ones" in {
    val body = parse(
      """[{"index":"orders"},{"index":".watches"},{"index":".ds-ilm-history-7-2026.11.13-000001"},
        | {"index":"a@b"},{"index":"all_templates_target"}]""".stripMargin
    )
    Indices.listed(body, es8) shouldBe Set(
      "orders",
      ".watches",
      ".ds-ilm-history-7-2026.11.13-000001",
      "a@b",
      "all_templates_target"
    )
    Indices.parse(body, es8) shouldBe Set("orders", "all_templates_target")
    Indices.parse(JNothing, es8) shouldBe empty
  }

  it should "expand wildcards from 7.7 only (6.8 answers 400 to an unconsumed parameter)" in {
    Indices.listPath(es6) shouldBe "/_cat/indices?format=json&h=index"
    Seq(es7, es8, es9).foreach { t =>
      Indices.listPath(t) shouldBe "/_cat/indices?format=json&h=index&expand_wildcards=all"
    }
    Indices.listPath(ResetTarget("7.6.2", security = false, watcher = true)) shouldBe
    "/_cat/indices?format=json&h=index"
  }

  it should "delete by name, in chunks of at most 3,000 characters, ignoring unavailable ones" in {
    val names = (1 to 400).map(i => f"index_with_a_rather_long_name_$i%04d")
    val requests = Indices.deleteRequests(names, es8)
    requests.size should be > 1
    requests.foreach { case (method, path, body) =>
      method shouldBe "DELETE"
      body shouldBe None
      path should endWith("?ignore_unavailable=true")
      path.stripPrefix("/").stripSuffix("?ignore_unavailable=true").length should be <= 3000
    }
    requests
      .flatMap(_._2.stripPrefix("/").stripSuffix("?ignore_unavailable=true").split(","))
      .toSeq shouldBe names
  }

  "chunks" should "keep every chunk within the limit and preserve the order" in {
    ClusterReset.chunks(Seq("aa", "bb", "cc"), maxChars = 5) shouldBe Seq(
      Seq("aa", "bb"),
      Seq("cc")
    )
    ClusterReset.chunks(Seq("abcdef", "g"), maxChars = 5) shouldBe Seq(Seq("abcdef"), Seq("g"))
    ClusterReset.chunks(Nil) shouldBe empty
  }

  "legacy templates" should "drop dot, @ and managed templates" in {
    val body = parse(
      """{"all_templates":{"index_patterns":["*"]},".watch-history-9":{"index_patterns":[".watcher-history-9*"]},
        | "logs@settings":{},"managed_one":{"_meta":{"managed":true}},"mine":{"_meta":{"managed":false}}}""".stripMargin
    )
    LegacyTemplates.listed(body, es6) should have size 5
    LegacyTemplates.parse(body, es6) shouldBe Set("all_templates", "mine")
    LegacyTemplates.deleteRequests(Seq("mine"), es6) shouldBe Seq(
      ("DELETE", "/_template/mine", None)
    )
  }

  "index and component templates" should "apply from 7.8 and drop managed ones" in {
    val index = parse(
      """{"index_templates":[{"name":"logs","index_template":{"_meta":{"managed":true}}},
        | {"name":"ilm-history-7","index_template":{"_meta":{"description":"x","managed":true}}},
        | {"name":"ci1_it","index_template":{"index_patterns":["ci1_*"]}}]}""".stripMargin
    )
    IndexTemplates.listed(index, es8) shouldBe Set("logs", "ilm-history-7", "ci1_it")
    IndexTemplates.parse(index, es8) shouldBe Set("ci1_it")
    val component = parse(
      """{"component_templates":[{"name":"logs@mappings","component_template":{}},
        | {"name":"ci1_ct","component_template":{"template":{}}}]}""".stripMargin
    )
    ComponentTemplates.parse(component, es8) shouldBe Set("ci1_ct")
    Seq(IndexTemplates, ComponentTemplates).foreach { step =>
      step.applies(es6) shouldBe false
      step.applies(ResetTarget("7.7.1", security = false, watcher = true)) shouldBe false
      Seq(es7, es8, es9).foreach(t => step.applies(t) shouldBe true)
    }
  }

  "pipelines" should "drop dot, @ and managed pipelines (ES 8 installs some after the snapshot)" in {
    val body = parse(
      """{"logs@json-pipeline":{},"logs-default-pipeline":{"_meta":{"managed":true}},
        | ".fleet_final_pipeline-1":{},"ci1_pipeline":{"processors":[]}}""".stripMargin
    )
    Pipelines.parse(body, es8) shouldBe Set("ci1_pipeline")
    Pipelines.parse(JNothing, es8) shouldBe empty // 404: no pipeline at all
  }

  "stored scripts" should "read the cluster state, an absent key meaning none" in {
    StoredScripts.listPath(
      es6
    ) shouldBe "/_cluster/state/metadata?filter_path=metadata.stored_scripts"
    StoredScripts.parse(parse("{}"), es8) shouldBe empty
    StoredScripts.parse(
      parse("""{"metadata":{"stored_scripts":{"grouped_tag_document":{"lang":"painless"}}}}"""),
      es8
    ) shouldBe Set("grouped_tag_document")
    StoredScripts.deleteRequests(Seq("grouped_tag_document"), es6) shouldBe
    Seq(("DELETE", "/_scripts/grouped_tag_document", None))
  }

  "enrich policies" should "apply from 7.5 and read the policy name of any type" in {
    val body = parse(
      """{"policies":[{"config":{"match":{"name":"p1","indices":["s"]}}},
        | {"config":{"range":{"name":"p2"}}}]}""".stripMargin
    )
    EnrichPolicies.parse(body, es8) shouldBe Set("p1", "p2")
    EnrichPolicies.applies(es6) shouldBe false
    Seq(es7, es8, es9).foreach(t => EnrichPolicies.applies(t) shouldBe true)
  }

  "users, roles and API keys" should "apply on a secure key only and never name a reserved object" in {
    val users = parse(
      """{"elastic":{"metadata":{"_reserved":true}},"kibana_system":{"metadata":{"_reserved":true}},
        | "s7_ro":{"metadata":{}}}""".stripMargin
    )
    Users.listed(users, es8Secure) shouldBe Set("elastic", "kibana_system", "s7_ro")
    Users.parse(users, es8Secure) shouldBe Set("s7_ro")
    val roles = parse(
      """{"superuser":{"metadata":{"_reserved":true}},"s7_reader":{"metadata":{}}}"""
    )
    Roles.parse(roles, es8Secure) shouldBe Set("s7_reader")
    val keys = parse(
      """{"api_keys":[{"id":"k1","invalidated":false},{"id":"k2","invalidated":true},{"id":"k3"}]}"""
    )
    ApiKeys.parse(keys, es8Secure) shouldBe Set("k1", "k3")
    ApiKeys.deleteRequests(Seq("k1", "k3"), es8Secure) shouldBe
    Seq(("DELETE", "/_security/api_key", Some("""{"ids":["k1","k3"]}""")))
    ApiKeys.deleteRequests(Nil, es8Secure) shouldBe empty
    Seq(Users, Roles, ApiKeys).foreach { step =>
      all.foreach(t => step.applies(t) shouldBe false)
      step.applies(es8Secure) shouldBe true
    }
    Users.deleteRequests(Seq("s7_ro"), es8Secure) shouldBe
    Seq(("DELETE", "/_security/user/s7_ro", None))
  }

  "refused kinds" should "read watchers, transforms, cluster settings and the licence per major" in {
    Watchers.listPath(es6) shouldBe "/_xpack/watcher/stats"
    Seq(es7, es8, es9).foreach(t => Watchers.listPath(t) shouldBe "/_watcher/stats")
    Watchers.parse(parse("""{"stats":[{"watch_count":2},{"watch_count":1}]}"""), es8) shouldBe
    Set("watch_count=3")
    Watchers.applies(es8.copy(watcher = false)) shouldBe false
    Transforms.applies(es6) shouldBe false
    Transforms.parse(parse("""{"count":1,"transforms":[{"id":"t1"}]}"""), es8) shouldBe Set("t1")
    ClusterSettings.listPath(es8) shouldBe "/_cluster/settings?flat_settings=true"
    ClusterSettings.parse(
      parse("""{"persistent":{"a.b":"3"},"transient":{"c":true}}"""),
      es8
    ) shouldBe Set("persistent:a.b=3", "transient:c=true")
    Licence.parse(
      parse("""{"license":{"type":"trial","status":"active","uid":"x"}}"""),
      es6
    ) shouldBe
    Set("type=trial", "status=active")
    Seq(Watchers, Transforms, ClusterSettings, Licence).foreach { step =>
      step.deleteRequests(Seq("x"), es8) shouldBe empty
    }
  }

  "a body of an unexpected shape" should "fail loud, never read as empty" in {
    an[IllegalStateException] should be thrownBy Indices.parse(parse("""{"index":"x"}"""), es8)
    an[IllegalStateException] should be thrownBy IndexTemplates.parse(parse("""{"x":[]}"""), es8)
    an[IllegalStateException] should be thrownBy Watchers.parse(parse("""{"x":1}"""), es8)
    an[IllegalStateException] should be thrownBy Licence.parse(parse("""{"license":{}}"""), es8)
  }

  // ------------------------------------------------------------------------------------------
  // the plan
  // ------------------------------------------------------------------------------------------

  private val snapshot = Map(
    "indices"          -> Set.empty[String],
    "legacy_templates" -> Set("boot_template"),
    "pipelines"        -> Set("boot_pipeline"),
    "cluster_settings" -> Set.empty[String],
    "licence"          -> Set("type=trial", "status=active"),
    "watchers"         -> Set("watch_count=0")
  )

  "plan" should "delete exactly current - snapshot, system names excluded, in deletion order" in {
    val current = Map(
      "indices"          -> Set("orders", "customers", ".tasks"),
      "legacy_templates" -> Set("boot_template", "all_templates"),
      "pipelines"        -> Set("boot_pipeline", "p1"),
      "stored_scripts"   -> Set("s1"), // absent from the snapshot: empty baseline
      "enrich_policies"  -> Set("e1"),
      "cluster_settings" -> Set.empty[String],
      "licence"          -> Set("type=trial", "status=active"),
      "watchers"         -> Set("watch_count=0")
    )
    val plan = ClusterReset.plan(ResetStep.defaults, es8, snapshot, current, Map.empty, "Prev")
    plan.refusals shouldBe empty
    plan.deletions shouldBe Seq(
      "indices"          -> Seq("customers", "orders"),
      "legacy_templates" -> Seq("all_templates"),
      "pipelines"        -> Seq("p1"),
      "stored_scripts"   -> Seq("s1"),
      "enrich_policies"  -> Seq("e1")
    )
    // the order is ResetStep.defaults' order
    val order = ResetStep.defaults.map(_.name)
    val positions = plan.deletions.map { case (step, _) => order.indexOf(step) }
    positions shouldBe positions.sorted
  }

  it should "skip the steps a major does not support" in {
    val current = Map(
      "enrich_policies" -> Set("e1"),
      "index_templates" -> Set("it1"),
      "users"           -> Set("u1")
    )
    ClusterReset
      .plan(ResetStep.defaults, es6, Map.empty, current, Map.empty, "Prev")
      .deletions shouldBe
    empty
    ClusterReset
      .plan(ResetStep.defaults, es8Secure, Map.empty, current, Map.empty, "Prev")
      .deletions shouldBe
    Seq("index_templates" -> Seq("it1"), "enrich_policies" -> Seq("e1"), "users" -> Seq("u1"))
  }

  it should "never delete a reserved user or a built-in role (the parse drops them)" in {
    val users = Users.parse(parse("""{"elastic":{"metadata":{"_reserved":true}}}"""), es8Secure)
    val roles = Roles.parse(parse("""{"superuser":{"metadata":{"_reserved":true}}}"""), es8Secure)
    val plan = ClusterReset.plan(
      ResetStep.defaults,
      es8Secure,
      Map.empty,
      Map("users" -> users, "roles" -> roles),
      Map.empty,
      "Prev"
    )
    plan.deletions shouldBe empty
  }

  it should "refuse a changed refused kind, naming the suite after which it was first seen" in {
    val current = snapshot ++ Map(
      "cluster_settings" -> Set("persistent:a.b=3"),
      "indices"          -> Set("orders")
    )
    val plan = ClusterReset.plan(ResetStep.defaults, es8, snapshot, current, Map.empty, "SuiteA")
    plan.refusals shouldBe Seq(
      Refusal("cluster_settings", Set("persistent:a.b=3"), Set.empty, "SuiteA")
    )
    // a removed object is a change too
    val removed = snapshot ++ Map("licence" -> Set("type=basic", "status=active"))
    ClusterReset
      .plan(ResetStep.defaults, es8, snapshot, removed, Map.empty, "SuiteB")
      .refusals shouldBe
    Seq(Refusal("licence", Set("type=basic"), Set("type=trial"), "SuiteB"))
    // each refused kind
    Seq(
      "watchers"   -> Set("watch_count=1"),
      "transforms" -> Set("t1")
    ).foreach { case (step, names) =>
      ClusterReset
        .plan(ResetStep.defaults, es8, snapshot, snapshot + (step -> names), Map.empty, "SuiteC")
        .refusals
        .map(r => (r.step, r.leftBy)) shouldBe Seq(step -> "SuiteC")
    }
  }

  "firstSeen" should "keep naming the real offender through a cascade, and forget what is gone" in {
    val changed = snapshot + ("cluster_settings" -> Set("persistent:a.b=3"))
    // after SuiteA: first seen
    val seen1 =
      ClusterReset.updateFirstSeen(ResetStep.defaults, es8, snapshot, changed, Map.empty, "SuiteA")
    seen1 shouldBe Map(("cluster_settings", "persistent:a.b=3") -> "SuiteA")
    // the next reset (after the aborted SuiteB) still blames SuiteA
    val seen2 =
      ClusterReset.updateFirstSeen(ResetStep.defaults, es8, snapshot, changed, seen1, "SuiteB")
    seen2 shouldBe seen1
    ClusterReset
      .plan(ResetStep.defaults, es8, snapshot, changed, seen2, "SuiteB")
      .refusals
      .map(_.leftBy) shouldBe
    Seq("SuiteA")
    // restored: pruned
    ClusterReset.updateFirstSeen(
      ResetStep.defaults,
      es8,
      snapshot,
      snapshot,
      seen2,
      "SuiteC"
    ) shouldBe empty
  }

  "cut" should "sort and cut a name list at 200 characters" in {
    ClusterReset.cut(Seq("b", "a")) shouldBe "a,b"
    val long = ClusterReset.cut((1 to 100).map(i => s"name$i"))
    long.length shouldBe 201
    long should endWith("…")
  }

  "ContainerKey" should "be the image tag and the first 8 hex digits of the yml's sha256" in {
    val key = ContainerKey("8.18.3", "cluster.name: docker-cluster\n")
    key.short should fullyMatch regex """8\.18\.3/[0-9a-f]{8}"""
    key.short shouldBe ContainerKey("8.18.3", "cluster.name: docker-cluster\n").short
    key.short should not be ContainerKey("8.18.3", "cluster.name: other\n").short
  }
}
