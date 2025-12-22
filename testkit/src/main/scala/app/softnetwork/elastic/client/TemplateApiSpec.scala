package app.softnetwork.elastic.client

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticSuccess}
import app.softnetwork.elastic.scalatest.ElasticTestKit
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

  def supportComposableTemplates: Boolean = {
    // Get Elasticsearch version
    val elasticVersion = {
      client.version match {
        case ElasticSuccess(v) => v
        case ElasticFailure(error) =>
          log.error(s"❌ Failed to retrieve Elasticsearch version: ${error.message}")
          return false
      }
    }
    ElasticsearchVersion.supportsComposableTemplates(elasticVersion)
  }

  "createTemplate" should "create and retrieve template" in {
    val template =
      """
        |{
        |  "index_patterns": ["test-*"],
        |  "priority": 100,
        |  "template": {
        |    "settings": {
        |      "number_of_shards": 1
        |    }
        |  }
        |}
        |""".stripMargin

    // Create
    val createResult = client.createTemplate("test-template", template)
    createResult shouldBe a[ElasticSuccess[_]]

    // Verify existence
    val existsResult = client.templateExists("test-template")
    existsResult shouldBe ElasticSuccess(true)

    // Get
    val getResult = client.getTemplate("test-template")
    getResult match {
      case ElasticSuccess(Some(json)) =>
        json should include("test-*")
        if (supportComposableTemplates)
          json should include("priority")
      case _ => fail("Failed to get template")
    }

    // Cleanup
    client.deleteTemplate("test-template", ifExists = true)
  }

  "createTemplate" should "convert legacy format automatically" in {
    val legacyTemplate =
      """
        |{
        |  "index_patterns": ["legacy-*"],
        |  "order": 1,
        |  "settings": {
        |    "number_of_shards": 1
        |  }
        |}
        |""".stripMargin

    val result = client.createTemplate("legacy-test", legacyTemplate)
    result shouldBe a[ElasticSuccess[_]]

    // Verify conversion
    client.getTemplate("legacy-test") match {
      case ElasticSuccess(Some(json)) if supportComposableTemplates =>
        json should include("priority")
        json should include("template")
      case ElasticSuccess(Some(json)) =>
        json should include("legacy-*")
      case _ => fail("Failed to get template")
    }

    // Cleanup
    client.deleteTemplate("legacy-test", ifExists = true)
  }

  "deleteTemplate" should "handle non-existing template with ifExists=true" in {
    val result = client.deleteTemplate("non-existing", ifExists = true)
    result shouldBe ElasticSuccess(false)
  }

  "listTemplates" should "return all templates" in {
    // Create test templates
    client.createTemplate("list-1", """{"index_patterns":["test1-*"],"priority":1,"template":{}}""")
    client.createTemplate("list-2", """{"index_patterns":["test2-*"],"priority":2,"template":{}}""")

    // List
    val result = client.listTemplates()
    result match {
      case ElasticSuccess(templates) =>
        templates.keys should contain allOf ("list-1", "list-2")
      case _ => fail("Failed to list templates")
    }

    // Cleanup
    client.deleteTemplate("list-1", ifExists = true)
    client.deleteTemplate("list-2", ifExists = true)
  }
}
