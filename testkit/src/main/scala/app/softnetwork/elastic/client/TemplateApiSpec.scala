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
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticSuccess}
import app.softnetwork.elastic.scalatest.ElasticTestKit
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import app.softnetwork.persistence.generateUUID
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.ExecutionContextExecutor

trait TemplateApiSpec
    extends AnyFlatSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with BeforeAndAfterEach {
  self: ElasticTestKit =>

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds))

  def client: TemplateApi with VersionApi

  // ==================== HELPER METHODS ====================

  def supportComposableTemplates: Boolean = {
    client.version match {
      case ElasticSuccess(v) => ElasticsearchVersion.supportsComposableTemplates(v)
      case ElasticFailure(error) =>
        log.error(s"❌ Failed to retrieve Elasticsearch version: ${error.message}")
        false
    }
  }

  def esVersion: String = {
    client.version match {
      case ElasticSuccess(v) => v
      case ElasticFailure(_) => "unknown"
    }
  }

  /** Sanitize pattern to create valid alias name
    *
    * Removes invalid characters: [' ','"','*',',','/','<','>','?','\','|']
    *
    * @param pattern
    *   the index pattern (e.g., "test-*")
    * @return
    *   valid alias name (e.g., "test-alias")
    */
  private def sanitizeAliasName(pattern: String): String = {
    pattern
      .replaceAll("[\\s\"*,/<>?\\\\|]", "") // Remove invalid characters
      .replaceAll("-+", "-") // Replace multiple dashes with single dash
      .replaceAll("^-|-$", "") // Remove leading/trailing dashes
      .toLowerCase
  }

  /** Generate composable template JSON
    *
    * @param pattern
    *   the index pattern (e.g., "test-*")
    * @param priority
    *   the template priority (default: 100)
    * @param withAliases
    *   whether to include aliases (default: true)
    * @return
    *   JSON string for composable template
    */
  def composableTemplate(
    pattern: String,
    priority: Int = 100,
    withAliases: Boolean = true
  ): String = {
    val aliasName = sanitizeAliasName(pattern)
    val aliasesJson = if (withAliases) {
      s"""
         |    "aliases": {
         |      "${aliasName}-alias": {}
         |    }
         |""".stripMargin
    } else {
      ""
    }

    s"""
       |{
       |  "index_patterns": ["$pattern"],
       |  "priority": $priority,
       |  "template": {
       |    "settings": {
       |      "number_of_shards": 1,
       |      "number_of_replicas": 0
       |    },
       |    "mappings": {
       |      "properties": {
       |        "timestamp": { "type": "date" },
       |        "message": { "type": "text" }
       |      }
       |    }${if (withAliases) s",\n\t$aliasesJson" else ""}
       |  },
       |  "version": 1,
       |  "_meta": {
       |    "description": "Test template"
       |  }
       |}
       |""".stripMargin
  }

  /** Generate legacy template JSON
    *
    * @param pattern
    *   the index pattern (e.g., "test-*")
    * @param order
    *   the template order (default: 1)
    * @param withAliases
    *   whether to include aliases (default: true)
    * @return
    *   JSON string for legacy template
    */
  def legacyTemplate(
    pattern: String,
    order: Int = 1,
    withAliases: Boolean = true,
    version: Int = 1
  ): String = {
    val aliasName = sanitizeAliasName(pattern)
    val aliasesJson = if (withAliases) {
      s"""
         |  "aliases": {
         |    "${aliasName}-alias": {}
         |  },
         |""".stripMargin
    } else {
      ""
    }

    s"""
       |{
       |  "index_patterns": ["$pattern"],
       |  "order": $order,
       |  "settings": {
       |    "number_of_shards": 1,
       |    "number_of_replicas": 0
       |  },
       |  "mappings": {
       |    "_doc": {
       |      "properties": {
       |        "timestamp": { "type": "date" },
       |        "message": { "type": "text" }
       |      }
       |    }
       |  }${if (withAliases) s",\n\t$aliasesJson" else ""}
       |  "version": $version
       |}
       |""".stripMargin
  }

  def cleanupTemplate(name: String): Unit = {
    client.deleteTemplate(name, ifExists = true)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    // Cleanup all test templates
    client.listTemplates() match {
      case ElasticSuccess(templates) =>
        templates.keys.filter(_.startsWith("test-")).foreach(cleanupTemplate)
      case _ => // Ignore
    }
  }

  override def beforeAll(): Unit = {
    self.beforeAll()
    info(s"Running tests against Elasticsearch $esVersion")
    info(s"Composable templates supported: $supportComposableTemplates")
  }

  // ==================== TEST SUITE ====================

  behavior of "TemplateApi"

  // ========== CREATE TEMPLATE ==========

  it should "create a composable template (ES 7.8+)" in {
    assume(supportComposableTemplates, "Composable templates not supported")

    val template = composableTemplate("test-composable-*")
    val result = client.createTemplate("test-composable", template)

    result shouldBe a[ElasticSuccess[_]]
    result.asInstanceOf[ElasticSuccess[Boolean]].value shouldBe true
  }

  it should "create a legacy template (ES < 7.8)" in {
    assume(!supportComposableTemplates, "Legacy templates only for ES < 7.8")

    val template = legacyTemplate("test-legacy-*")
    val result = client.createTemplate("test-legacy", template)

    result shouldBe a[ElasticSuccess[_]]
    result.asInstanceOf[ElasticSuccess[Boolean]].value shouldBe true
  }

  it should "auto-convert legacy format to composable (ES 7.8+)" in {
    assume(supportComposableTemplates, "Composable templates not supported")

    val legacyFormat = legacyTemplate("test-auto-convert-*")
    val result = client.createTemplate("test-auto-convert", legacyFormat)

    result shouldBe a[ElasticSuccess[_]]

    // Verify it was stored as composable
    client.getTemplate("test-auto-convert") match {
      case ElasticSuccess(Some(json)) =>
        json should include("priority")
        json should include("template")
        json should not include "order"
      case _ => fail("Failed to get template")
    }
  }

  it should "auto-convert composable format to legacy (ES < 7.8)" in {
    assume(!supportComposableTemplates, "Legacy templates only for ES < 7.8")

    val composableFormat = composableTemplate("test-auto-convert-legacy-*")
    val result = client.createTemplate("test-auto-convert-legacy", composableFormat)

    result shouldBe a[ElasticSuccess[_]]

    // Verify it was stored as legacy
    client.getTemplate("test-auto-convert-legacy") match {
      case ElasticSuccess(Some(json)) =>
        json should include("order")
        json should not include "priority"
        json should not include "\"template\":"
      case _ => fail("Failed to get template")
    }
  }

  it should "preserve order field in legacy templates" in {
    assume(!supportComposableTemplates, "Test for legacy templates only")

    // Test with different orders
    val orders = List(0, 1, 5, 10, 100)

    orders.foreach { order =>
      val templateName = s"test-order-$order"
      val template = legacyTemplate(s"test-order-$order-*", order = order)

      // Check the JSON before creation
      template should include(s""""order": $order""")

      // Create the template
      val createResult = client.createTemplate(templateName, template)
      createResult shouldBe a[ElasticSuccess[_]]

      // Retrieve and verify
      client.getTemplate(templateName) match {
        case ElasticSuccess(Some(json)) =>
          log.info(s"Template JSON for order=$order: $json")

          // Parse the JSON to check the order
          val mapper = JacksonConfig.objectMapper
          val root = mapper.readTree(json)

          if (root.has("order")) {
            val actualOrder = root.get("order").asInt()
            actualOrder shouldBe order
          } else {
            fail(s"Template does not contain 'order' field. JSON: $json")
          }

        case other => fail(s"Failed to get template: $other")
      }

      // Cleanup
      client.deleteTemplate(templateName, ifExists = true)
    }
  }

  it should "update an existing template" in {
    val template1 = if (supportComposableTemplates) {
      composableTemplate("test-update-*", priority = 100)
    } else {
      legacyTemplate("test-update-*", order = 1)
    }

    val template2 = if (supportComposableTemplates) {
      composableTemplate("test-update-*", priority = 200)
    } else {
      legacyTemplate("test-update-*", order = 2, version = 2)
    }

    // Create initial
    val createResult = client.createTemplate("test-update", template1)
    createResult shouldBe a[ElasticSuccess[_]]

    // Verify initial state
    client.getTemplate("test-update") match {
      case ElasticSuccess(Some(json)) =>
        info(s"Initial template: $json")
        if (supportComposableTemplates) {
          json should include("\"priority\":100")
        } else {
          json should include("\"order\":1")
          json should include("\"version\":1")
        }
      case other => fail(s"Failed to get initial template: $other")
    }

    // Update
    val updateResult = client.createTemplate("test-update", template2)
    updateResult shouldBe a[ElasticSuccess[_]]

    // Verify update
    client.getTemplate("test-update") match {
      case ElasticSuccess(Some(json)) =>
        info(s"Updated template: $json")

        if (supportComposableTemplates) {
          json should include("\"priority\":200")
        } else {
          // Parse JSON to verify exact values
          val mapper = JacksonConfig.objectMapper
          val root = mapper.readTree(json)

          root.get("order").asInt() shouldBe 2
          root.get("version").asInt() shouldBe 2
        }

      case other => fail(s"Failed to get updated template: $other")
    }

    // Cleanup
    client.deleteTemplate("test-update", ifExists = true)
  }

  it should "reject invalid template name" in {
    val invalidNames = Seq("", " ", "UPPERCASE", "with space", "_underscore", "-dash", "with/slash")

    invalidNames.foreach { name =>
      val result = client.createTemplate(name, composableTemplate("test-*"))
      result shouldBe a[ElasticFailure]
    }
  }

  it should "reject invalid JSON definition" in {
    val invalidJson = "{ invalid json }"
    val result = client.createTemplate("test-invalid-json", invalidJson)
    result shouldBe a[ElasticFailure]
  }

  it should "reject template without index_patterns" in {
    val noPatterns = """{"priority": 1, "template": {}}"""
    val result = client.createTemplate("test-no-patterns", noPatterns)
    result shouldBe a[ElasticFailure]
  }

  // ========== GET TEMPLATE ==========

  it should "get an existing template" in {
    val template = if (supportComposableTemplates) {
      composableTemplate("test-get-*")
    } else {
      legacyTemplate("test-get-*")
    }

    client.createTemplate("test-get", template)

    val result = client.getTemplate("test-get")
    result match {
      case ElasticSuccess(Some(json)) =>
        json should include("test-get-*")
        json should include("number_of_shards")
      case _ => fail("Failed to get template")
    }
  }

  it should "return None for non-existing template" in {
    val result = client.getTemplate("non-existing-template")
    result shouldBe ElasticSuccess(None)
  }

  it should "reject invalid template name in get" in {
    val result = client.getTemplate("")
    result shouldBe a[ElasticFailure]
  }

  // ========== LIST TEMPLATES ==========

  it should "list all templates" in {
    val template1 = if (supportComposableTemplates) {
      composableTemplate("test-list-1-*")
    } else {
      legacyTemplate("test-list-1-*")
    }

    val template2 = if (supportComposableTemplates) {
      composableTemplate("test-list-2-*")
    } else {
      legacyTemplate("test-list-2-*")
    }

    client.createTemplate("test-list-1", template1)
    client.createTemplate("test-list-2", template2)

    val result = client.listTemplates()
    result match {
      case ElasticSuccess(templates) =>
        templates.keys should contain allOf ("test-list-1", "test-list-2")
        templates.values.foreach { json =>
          json should not be empty
        }
      case _ => fail("Failed to list templates")
    }
  }

  it should "return empty map when no templates exist" in {
    // Cleanup all test templates
    client.listTemplates() match {
      case ElasticSuccess(templates) =>
        templates.keys.filter(_.startsWith("test-")).foreach(cleanupTemplate)
      case _ => // Ignore
    }

    val result = client.listTemplates()
    result match {
      case ElasticSuccess(templates) =>
        templates.keys.filter(_.startsWith("test-")) shouldBe empty
      case _ => fail("Failed to list templates")
    }
  }

  // ========== TEMPLATE EXISTS ==========

  it should "return true for existing template" in {
    val template = if (supportComposableTemplates) {
      composableTemplate("test-exists-*")
    } else {
      legacyTemplate("test-exists-*")
    }

    client.createTemplate("test-exists", template)

    val result = client.templateExists("test-exists")
    result shouldBe ElasticSuccess(true)
  }

  it should "return false for non-existing template" in {
    val result = client.templateExists("non-existing-template")
    result shouldBe ElasticSuccess(false)
  }

  it should "reject invalid template name in exists" in {
    val result = client.templateExists("")
    result shouldBe a[ElasticFailure]
  }

  // ========== DELETE TEMPLATE ==========

  it should "delete an existing template" in {
    val template = if (supportComposableTemplates) {
      composableTemplate("test-delete-*")
    } else {
      legacyTemplate("test-delete-*")
    }

    client.createTemplate("test-delete", template)
    client.templateExists("test-delete") shouldBe ElasticSuccess(true)

    val deleteResult = client.deleteTemplate("test-delete")
    deleteResult shouldBe ElasticSuccess(true)

    client.templateExists("test-delete") shouldBe ElasticSuccess(false)
  }

  it should "succeed when deleting non-existing template with ifExists=true" in {
    val result = client.deleteTemplate("non-existing-template", ifExists = true)
    result shouldBe ElasticSuccess(false)
  }

  it should "fail when deleting non-existing template with ifExists=false" in {
    val result = client.deleteTemplate("non-existing-template", ifExists = false)
    // Comportement dépend de l'implémentation, mais devrait échouer ou retourner false
    result match {
      case ElasticSuccess(false) => succeed
      case ElasticFailure(_)     => succeed
      case _                     => fail("Should fail or return false")
    }
  }

  it should "reject invalid template name in delete" in {
    val result = client.deleteTemplate("")
    result shouldBe a[ElasticFailure]
  }

  // ========== EDGE CASES ==========

  it should "handle template with complex mappings" in {
    val complexTemplate =
      s"""
         |{
         |  "index_patterns": ["test-complex-*"],
         |  ${if (supportComposableTemplates) "\"priority\": 100," else "\"order\": 1,"}
         |  ${if (supportComposableTemplates) "\"template\": {" else ""}
         |    "settings": {
         |      "number_of_shards": 2,
         |      "number_of_replicas": 1,
         |      "refresh_interval": "30s"
         |    },
         |    "mappings": {
         |      "properties": {
         |        "timestamp": { "type": "date", "format": "strict_date_optional_time||epoch_millis" },
         |        "message": { "type": "text", "analyzer": "standard" },
         |        "level": { "type": "keyword" },
         |        "tags": { "type": "keyword" },
         |        "metadata": {
         |          "type": "object",
         |          "properties": {
         |            "host": { "type": "keyword" },
         |            "service": { "type": "keyword" }
         |          }
         |        }
         |      }
         |    },
         |    "aliases": {
         |      "logs-all": {},
         |      "logs-recent": {
         |        "filter": {
         |          "range": {
         |            "timestamp": {
         |              "gte": "now-7d"
         |            }
         |          }
         |        }
         |      }
         |    }
         |  ${if (supportComposableTemplates) "}" else ""}
         |}
         |""".stripMargin

    val result = client.createTemplate("test-complex", complexTemplate)
    result shouldBe a[ElasticSuccess[_]]

    // Verify
    client.getTemplate("test-complex") match {
      case ElasticSuccess(Some(json)) =>
        json should include("test-complex-*")
        json should include("metadata")
        json should include("logs-all")
      case _ => fail("Failed to get complex template")
    }
  }

  it should "handle template with multiple index patterns" in {
    val multiPatternTemplate =
      s"""
         |{
         |  "index_patterns": ["test-multi-1-*", "test-multi-2-*", "test-multi-3-*"],
         |  ${if (supportComposableTemplates) "\"priority\": 100," else "\"order\": 1,"}
         |  ${if (supportComposableTemplates) "\"template\": {" else ""}
         |    "settings": {
         |      "number_of_shards": 1
         |    }
         |  ${if (supportComposableTemplates) "}" else ""}
         |}
         |""".stripMargin

    val result = client.createTemplate("test-multi-pattern", multiPatternTemplate)
    result shouldBe a[ElasticSuccess[_]]

    client.getTemplate("test-multi-pattern") match {
      case ElasticSuccess(Some(json)) =>
        json should include("test-multi-1-*")
        json should include("test-multi-2-*")
        json should include("test-multi-3-*")
      case _ => fail("Failed to get multi-pattern template")
    }
  }

  it should "handle template with version and metadata" in {
    val versionedTemplate =
      s"""
         |{
         |  "index_patterns": ["test-versioned-*"],
         |  ${if (supportComposableTemplates) "\"priority\": 100," else "\"order\": 1,"}
         |  "version": 42,
         |  ${if (supportComposableTemplates) "\"_meta\": {" else ""}
         |    ${if (supportComposableTemplates) "\"description\": \"Test versioned template\","
      else ""}
         |    ${if (supportComposableTemplates) "\"author\": \"test-suite\"" else ""}
         |  ${if (supportComposableTemplates) "}," else ""}
         |  ${if (supportComposableTemplates) "\"template\": {" else ""}
         |    "settings": {
         |      "number_of_shards": 1
         |    }
         |  ${if (supportComposableTemplates) "}" else ""}
         |}
         |""".stripMargin

    val result = client.createTemplate("test-versioned", versionedTemplate)
    result shouldBe a[ElasticSuccess[_]]

    client.getTemplate("test-versioned") match {
      case ElasticSuccess(Some(json)) =>
        json should include("\"version\":42")
        if (supportComposableTemplates) {
          json should include("_meta")
        }
      case _ => fail("Failed to get versioned template")
    }
  }

  // ========== CONCURRENT OPERATIONS ==========

  it should "handle concurrent template operations" in {
    import scala.concurrent.Future

    val futures = (1 to 5).map { i =>
      Future {
        val template = if (supportComposableTemplates) {
          composableTemplate(s"test-concurrent-$i-*")
        } else {
          legacyTemplate(s"test-concurrent-$i-*")
        }
        client.createTemplate(s"test-concurrent-$i", template)
      }
    }

    val results = Future.sequence(futures).futureValue
    results.foreach { result =>
      result shouldBe a[ElasticSuccess[_]]
    }

    // Verify all were created
    val listResult = client.listTemplates()
    listResult match {
      case ElasticSuccess(templates) =>
        (1 to 5).foreach { i =>
          templates.keys should contain(s"test-concurrent-$i")
        }
      case _ => fail("Failed to list templates")
    }

    // Cleanup
    (1 to 5).foreach { i =>
      client.deleteTemplate(s"test-concurrent-$i", ifExists = true)
    }
  }

  // ========== PERFORMANCE ==========

  it should "handle bulk template operations efficiently" in {
    val startTime = System.currentTimeMillis()

    // Create 20 templates
    (1 to 20).foreach { i =>
      val template = if (supportComposableTemplates) {
        composableTemplate(s"test-bulk-$i-*")
      } else {
        legacyTemplate(s"test-bulk-$i-*")
      }
      client.createTemplate(s"test-bulk-$i", template)
    }

    // List all
    client.listTemplates() match {
      case ElasticSuccess(templates) =>
        templates.keys.count(_.startsWith("test-bulk-")) should be >= 20
      case _ => fail("Failed to list templates")
    }

    // Delete all
    (1 to 20).foreach { i =>
      client.deleteTemplate(s"test-bulk-$i", ifExists = true)
    }

    val duration = System.currentTimeMillis() - startTime
    log.info(s"Bulk operations completed in ${duration}ms")

    duration should be < 30000L // Should complete in less than 30 seconds
  }

}
