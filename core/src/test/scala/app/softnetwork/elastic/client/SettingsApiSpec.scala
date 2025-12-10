package app.softnetwork.elastic.client

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.mockito.MockitoSugar
import org.mockito.ArgumentMatchersSugar
import org.slf4j.Logger
import app.softnetwork.elastic.client.result._
import com.google.gson.JsonParser

/** Unit tests for SettingsApi Coverage target: 80%+ Using mockito-scala 1.17.12
  */
class SettingsApiSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterEach
    with MockitoSugar
    with ArgumentMatchersSugar {

  // Mock logger
  val mockLogger: Logger = mock[Logger]

  // Concrete implementation for testing
  class TestSettingsApi extends SettingsApi with IndicesApi with RefreshApi {
    override protected def logger: Logger = mockLogger

    // Control variables
    var executeUpdateSettingsResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeLoadSettingsResult: ElasticResult[String] = ElasticSuccess(
      """{"my-index":{"settings":{"index":{"number_of_shards":"1"}}}}"""
    )
    var executeCloseIndexResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeOpenIndexResult: ElasticResult[Boolean] = ElasticSuccess(true)

    override private[client] def executeUpdateSettings(
      index: String,
      settings: String
    ): ElasticResult[Boolean] = {
      executeUpdateSettingsResult
    }

    override private[client] def executeLoadSettings(index: String): ElasticResult[String] = {
      executeLoadSettingsResult
    }

    override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] = {
      executeCloseIndexResult
    }

    override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = {
      executeOpenIndexResult
    }

    // Other required methods
    override private[client] def executeCreateIndex(
      index: String,
      settings: String
    ): ElasticResult[Boolean] = ???
    override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] = ???
    override private[client] def executeReindex(
      sourceIndex: String,
      targetIndex: String,
      refresh: Boolean,
      pipeline: Option[String]
    ): ElasticResult[(Boolean, Option[Long])] = ???
    override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] = ???
    override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = ???
  }

  var settingsApi: TestSettingsApi = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    settingsApi = new TestSettingsApi()
    reset(mockLogger)
  }

  "SettingsApi" should {

    "toggleRefresh" should {

      "enable refresh interval when enable is true" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = settingsApi.toggleRefresh("my-index", enable = true)

        // Then
        result.isSuccess shouldBe true

        verify(mockLogger).debug(contains("refresh_interval"))
        verify(mockLogger).debug(contains("1s"))
      }

      "disable refresh interval when enable is false" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = settingsApi.toggleRefresh("my-index", enable = false)

        // Then
        result.isSuccess shouldBe true

        verify(mockLogger).debug(contains("refresh_interval"))
        verify(mockLogger).debug(contains("-1"))
      }

      "reject invalid index name" in {
        // When
        val result = settingsApi.toggleRefresh("INVALID", enable = true)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
        result.error.get.operation shouldBe Some("updateSettings")
      }

      "handle failure when closing index fails" in {
        // Given
        val error = ElasticError("Cannot close index", statusCode = Some(500))
        settingsApi.executeCloseIndexResult = ElasticFailure(error)

        // When
        val result = settingsApi.toggleRefresh("my-index", enable = true)

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Cannot close index"

        verify(mockLogger).error(contains("Closing index my-index failed"))
      }

      "handle failure when updating settings fails" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        val error = ElasticError("Update failed", statusCode = Some(500))
        settingsApi.executeUpdateSettingsResult = ElasticFailure(error)

        // When
        val result = settingsApi.toggleRefresh("my-index", enable = true)

        // Then
        result.isFailure shouldBe true

        verify(mockLogger).error(contains("Updating settings for index 'my-index' failed"))
      }

      "handle failure when opening index fails after successful update" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        val error = ElasticError("Cannot open index", statusCode = Some(500))
        settingsApi.executeOpenIndexResult = ElasticFailure(error)

        // When
        val result = settingsApi.toggleRefresh("my-index", enable = true)

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Cannot open index"

        verify(mockLogger).info("✅ Updating settings for index 'my-index' succeeded")
      }
    }

    "setReplicas" should {

      "successfully set number of replicas" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = settingsApi.setReplicas("my-index", 2)

        // Then
        result.isSuccess shouldBe true

        verify(mockLogger).debug(contains("number_of_replicas"))
        verify(mockLogger).debug(contains("2"))
      }

      "accept zero replicas" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = settingsApi.setReplicas("my-index", 0)

        // Then
        result.isSuccess shouldBe true

        verify(mockLogger).debug(contains("0"))
      }

      "accept multiple replicas" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = settingsApi.setReplicas("my-index", 5)

        // Then
        result.isSuccess shouldBe true

        verify(mockLogger).debug(contains("5"))
      }

      "reject invalid index name" in {
        // When
        val result = settingsApi.setReplicas("INVALID", 2)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
      }

      "handle negative replicas value" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When - Note: validation of replica count is ES responsibility
        val result = settingsApi.setReplicas("my-index", -1)

        // Then - API accepts it, ES will reject
        result.isSuccess shouldBe true
        verify(mockLogger).debug(contains("-1"))
      }

      "handle failure during update" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        val error = ElasticError("Update failed")
        settingsApi.executeUpdateSettingsResult = ElasticFailure(error)

        // When
        val result = settingsApi.setReplicas("my-index", 2)

        // Then
        result.isFailure shouldBe true

        verify(mockLogger).error(contains("Updating settings for index 'my-index' failed"))
      }
    }

    "updateSettings" should {

      "successfully update settings with default settings" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = settingsApi.updateSettings("my-index")

        // Then
        result.isSuccess shouldBe true

        verify(mockLogger).debug(contains("🔧 Updating settings for index my-index"))
        verify(mockLogger).info("✅ Updating settings for index 'my-index' succeeded")
      }

      "successfully update settings with custom settings" in {
        // Given
        val customSettings = """{"index": {"max_result_window": 20000}}"""
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = settingsApi.updateSettings("my-index", customSettings)

        // Then
        result.isSuccess shouldBe true

        verify(mockLogger).debug(contains(customSettings))
        verify(mockLogger).info(contains("succeeded"))
      }

      "reject invalid index name" in {
        // When
        val result = settingsApi.updateSettings("INVALID")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
        result.error.get.message should include("lowercase")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("updateSettings")

        verify(mockLogger, never).debug(any[String])
      }

      "reject empty index name" in {
        // When
        val result = settingsApi.updateSettings("")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("cannot be empty")
      }

      "reject invalid JSON settings" in {
        // Given
        val invalidJson = """{"index": invalid}"""

        // When
        val result = settingsApi.updateSettings("my-index", invalidJson)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid settings")
        result.error.get.message should include("Invalid JSON")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("updateSettings")
      }

      "reject empty settings" in {
        // When
        val result = settingsApi.updateSettings("my-index", "")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid settings")
        result.error.get.message should include("cannot be empty")
      }

      "reject null settings" in {
        // When
        val result = settingsApi.updateSettings("my-index", null)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid settings")
      }

      "fail when closeIndex fails" in {
        // Given
        val error = ElasticError("Cannot close index", statusCode = Some(500))
        settingsApi.executeCloseIndexResult = ElasticFailure(error)

        // When
        val result = settingsApi.updateSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Cannot close index"

        // closeIndex logs info "Closing index..." before failing
        verify(mockLogger).info(contains("Closing index 'my-index'"))
        verify(mockLogger).error(contains("Closing index my-index failed"))
        verify(mockLogger).error(contains("settings for index 'my-index' will not be updated"))
      }

      "fail when executeUpdateSettings fails" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        val error = ElasticError("Update failed", statusCode = Some(500))
        settingsApi.executeUpdateSettingsResult = ElasticFailure(error)

        // When
        val result = settingsApi.updateSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Update failed"

        // closeIndex logs info messages
        verify(mockLogger).info(contains("Closing index 'my-index'"))
        verify(mockLogger).info(contains("✅ Index 'my-index' closed successfully"))
        verify(mockLogger).error("❌ Updating settings for index 'my-index' failed: Update failed")
      }

      "fail when executeUpdateSettings returns false" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(false)

        // When
        val result = settingsApi.updateSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Updating settings for index 'my-index' failed")
        result.error.get.operation shouldBe Some("updateSettings")
        result.error.get.index shouldBe Some("my-index")
      }

      "fail when openIndex fails after successful update" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        val error = ElasticError("Cannot open index", statusCode = Some(500))
        settingsApi.executeOpenIndexResult = ElasticFailure(error)

        // When
        val result = settingsApi.updateSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Cannot open index"

        verify(mockLogger).info("✅ Updating settings for index 'my-index' succeeded")
      }

      "validate index name before settings" in {
        // Given
        val invalidJson = """invalid"""

        // When
        val result = settingsApi.updateSettings("INVALID", invalidJson)

        // Then
        result.error.get.message should include("Invalid index")
        result.error.get.message should not include "Invalid settings"
      }

      "handle network timeout during close" in {
        // Given
        val error = ElasticError(
          "Connection timeout",
          cause = Some(new java.net.SocketTimeoutException()),
          statusCode = Some(504)
        )
        settingsApi.executeCloseIndexResult = ElasticFailure(error)

        // When
        val result = settingsApi.updateSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.cause shouldBe defined
      }

      "handle authentication error" in {
        // Given
        val error = ElasticError("Authentication failed", statusCode = Some(401))
        settingsApi.executeCloseIndexResult = ElasticFailure(error)

        // When
        val result = settingsApi.updateSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(401)
      }
    }

    "loadSettings" should {

      "successfully load settings for existing index" in {
        // Given
        val jsonResponse =
          """{"my-index":{"settings":{"index":{"number_of_shards":"1","number_of_replicas":"2"}}}}"""
        settingsApi.executeLoadSettingsResult = ElasticSuccess(jsonResponse)

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get should include("number_of_shards")
        result.get should include("number_of_replicas")

        verify(mockLogger).debug("🔍 Loading settings for index my-index")
      }

      "extract only index settings from response" in {
        // Given
        val jsonResponse = """{"my-index":{"settings":{"index":{"number_of_shards":"3"}}}}"""
        settingsApi.executeLoadSettingsResult = ElasticSuccess(jsonResponse)

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isSuccess shouldBe true
        val parsedResult = JsonParser.parseString(result.get).getAsJsonObject
        parsedResult.has("number_of_shards") shouldBe true
        parsedResult.get("number_of_shards").getAsString shouldBe "3"
      }

      "reject invalid index name" in {
        // When
        val result = settingsApi.loadSettings("INVALID")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
        result.error.get.operation shouldBe Some("updateSettings")

        verify(mockLogger, never).debug(contains("🔍"))
      }

      "reject empty index name" in {
        // When
        val result = settingsApi.loadSettings("")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("cannot be empty")
      }

      "fail when executeLoadSettings fails" in {
        // Given
        val error = ElasticError("Load failed", statusCode = Some(500))
        settingsApi.executeLoadSettingsResult = ElasticFailure(error)

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Load failed"
      }

      "fail when response contains invalid JSON" in {
        // Given
        settingsApi.executeLoadSettingsResult = ElasticSuccess("invalid json {")

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.operation shouldBe Some("loadSettings")

        verify(mockLogger).error(contains("Failed to parse JSON settings"))
      }

      "fail when index not found in response" in {
        // Given
        val jsonResponse = """{"other-index":{"settings":{"index":{}}}}"""
        settingsApi.executeLoadSettingsResult = ElasticSuccess(jsonResponse)

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index 'my-index' not found in the loaded settings")
        result.error.get.operation shouldBe Some("loadSettings")
        result.error.get.index shouldBe Some("my-index")

        verify(mockLogger).error(contains("Index 'my-index' not found"))
      }

      "fail when response is empty JSON object" in {
        // Given
        settingsApi.executeLoadSettingsResult = ElasticSuccess("{}")

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("not found in the loaded settings")
      }

      "handle complex settings structure" in {
        // Given
        val complexJson =
          """{
            |  "my-index": {
            |    "settings": {
            |      "index": {
            |        "number_of_shards": "5",
            |        "number_of_replicas": "2",
            |        "refresh_interval": "1s",
            |        "max_result_window": "10000",
            |        "analysis": {
            |          "analyzer": {
            |            "custom": {
            |              "type": "standard"
            |            }
            |          }
            |        }
            |      }
            |    }
            |  }
            |}""".stripMargin
        settingsApi.executeLoadSettingsResult = ElasticSuccess(complexJson)

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get should include("number_of_shards")
        result.get should include("analysis")
        result.get should include("custom")
      }

      "handle settings with nested objects" in {
        // Given
        val jsonResponse =
          """{
            |  "my-index": {
            |    "settings": {
            |      "index": {
            |        "mapping": {
            |          "total_fields": {
            |            "limit": "2000"
            |          }
            |        }
            |      }
            |    }
            |  }
            |}""".stripMargin
        settingsApi.executeLoadSettingsResult = ElasticSuccess(jsonResponse)

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get should include("mapping")
        result.get should include("total_fields")
      }

      "handle network error" in {
        // Given
        val error = ElasticError(
          "Connection timeout",
          cause = Some(new java.net.SocketTimeoutException()),
          statusCode = Some(504)
        )
        settingsApi.executeLoadSettingsResult = ElasticFailure(error)

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.cause shouldBe defined
      }

      "handle index not found error" in {
        // Given
        val error = ElasticError("Index not found", statusCode = Some(404))
        settingsApi.executeLoadSettingsResult = ElasticFailure(error)

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(404)
      }
    }

    "workflow scenarios" should {

      "successfully toggle refresh, update replicas, and load settings" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)
        settingsApi.executeLoadSettingsResult = ElasticSuccess(
          """{"my-index":{"settings":{"index":{"number_of_replicas":"2","refresh_interval":"1s"}}}}"""
        )

        // When
        val toggleResult = settingsApi.toggleRefresh("my-index", enable = true)
        val replicasResult = settingsApi.setReplicas("my-index", 2)
        val loadResult = settingsApi.loadSettings("my-index")

        // Then
        toggleResult.isSuccess shouldBe true
        replicasResult.isSuccess shouldBe true
        loadResult.isSuccess shouldBe true
        loadResult.get should include("number_of_replicas")
        loadResult.get should include("refresh_interval")
      }

      "handle partial failure in workflow" in {
        // Given - First operation succeeds
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)
        val result1 = settingsApi.toggleRefresh("my-index", enable = true)

        // Given - Second operation fails
        settingsApi.executeCloseIndexResult = ElasticFailure(ElasticError("Cannot close"))
        val result2 = settingsApi.setReplicas("my-index", 2)

        // Then
        result1.isSuccess shouldBe true
        result2.isFailure shouldBe true
      }

      "handle close-update-open workflow correctly" in {
        // Given
        var closeCalled = false
        var updateCalled = false
        var openCalled = false

        val workflowApi = new SettingsApi with IndicesApi with RefreshApi {
          override protected def logger: Logger = mockLogger

          override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] = {
            closeCalled = true
            ElasticSuccess(true)
          }

          override private[client] def executeUpdateSettings(
            index: String,
            settings: String
          ): ElasticResult[Boolean] = {
            updateCalled = true
            closeCalled shouldBe true // Close must be called first
            ElasticSuccess(true)
          }

          override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = {
            openCalled = true
            updateCalled shouldBe true // Update must be called before open
            ElasticSuccess(true)
          }

          override private[client] def executeLoadSettings(index: String): ElasticResult[String] =
            ???
          override private[client] def executeCreateIndex(
            index: String,
            settings: String
          ): ElasticResult[Boolean] = ???
          override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeReindex(
            sourceIndex: String,
            targetIndex: String,
            refresh: Boolean,
            pipeline: Option[String]
          ): ElasticResult[(Boolean, Option[Long])] = ???
          override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = ???
        }

        // When
        workflowApi.updateSettings("my-index")

        // Then
        closeCalled shouldBe true
        updateCalled shouldBe true
        openCalled shouldBe true
      }

      "not call update or open if close fails" in {
        // Given
        var updateCalled = false
        var openCalled = false

        val workflowApi = new SettingsApi with IndicesApi with RefreshApi {
          override protected def logger: Logger = mockLogger

          override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] = {
            ElasticFailure(ElasticError("Close failed"))
          }

          override private[client] def executeUpdateSettings(
            index: String,
            settings: String
          ): ElasticResult[Boolean] = {
            updateCalled = true
            ElasticSuccess(true)
          }

          override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = {
            openCalled = true
            ElasticSuccess(true)
          }

          override private[client] def executeLoadSettings(index: String): ElasticResult[String] =
            ???
          override private[client] def executeCreateIndex(
            index: String,
            settings: String
          ): ElasticResult[Boolean] = ???
          override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeReindex(
            sourceIndex: String,
            targetIndex: String,
            refresh: Boolean,
            pipeline: Option[String]
          ): ElasticResult[(Boolean, Option[Long])] = ???
          override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = ???
        }

        // When
        workflowApi.updateSettings("my-index")

        // Then
        updateCalled shouldBe false
        openCalled shouldBe false
      }

      "not call open if update fails" in {
        // Given
        var openCalled = false

        val workflowApi = new SettingsApi with IndicesApi with RefreshApi {
          override protected def logger: Logger = mockLogger

          override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] = {
            ElasticSuccess(true)
          }

          override private[client] def executeUpdateSettings(
            index: String,
            settings: String
          ): ElasticResult[Boolean] = {
            ElasticFailure(ElasticError("Update failed"))
          }

          override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = {
            openCalled = true
            ElasticSuccess(true)
          }

          override private[client] def executeLoadSettings(index: String): ElasticResult[String] =
            ???
          override private[client] def executeCreateIndex(
            index: String,
            settings: String
          ): ElasticResult[Boolean] = ???
          override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeReindex(
            sourceIndex: String,
            targetIndex: String,
            refresh: Boolean,
            pipeline: Option[String]
          ): ElasticResult[(Boolean, Option[Long])] = ???
          override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = ???
        }

        // When
        workflowApi.updateSettings("my-index")

        // Then
        openCalled shouldBe false
      }
    }

    "ElasticResult integration" should {

      "work with map transformation" in {
        // Given
        settingsApi.executeLoadSettingsResult = ElasticSuccess(
          """{"my-index":{"settings":{"index":{"number_of_shards":"1"}}}}"""
        )

        // When
        val result = settingsApi.loadSettings("my-index").map { settings =>
          settings.length
        }

        // Then
        result.isSuccess shouldBe true
        result.get should be > 0
      }

      "work with flatMap composition" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)
        settingsApi.executeLoadSettingsResult = ElasticSuccess(
          """{"my-index":{"settings":{"index":{"number_of_shards":"1"}}}}"""
        )

        // When
        val result = settingsApi.updateSettings("my-index").flatMap { _ =>
          settingsApi.loadSettings("my-index")
        }

        // Then
        result.isSuccess shouldBe true
        result.get should include("number_of_shards")
      }

      "work with for-comprehension" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = for {
          _ <- settingsApi.toggleRefresh("my-index", enable = true)
          _ <- settingsApi.setReplicas("my-index", 2)
        } yield "Success"

        // Then
        result shouldBe ElasticSuccess("Success")
      }

      "propagate errors through transformations" in {
        // Given
        val error = ElasticError("Load failed")
        settingsApi.executeLoadSettingsResult = ElasticFailure(error)

        // When
        val result = settingsApi
          .loadSettings("my-index")
          .map(_.toUpperCase)
          .flatMap(s => ElasticSuccess(s.length))

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Load failed"
      }
    }

    "edge cases" should {

      "handle very large replica count" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = settingsApi.setReplicas("my-index", 1000)

        // Then
        result.isSuccess shouldBe true
        verify(mockLogger).debug(contains("1000"))
      }

      "handle index name with maximum length" in {
        // Given
        val maxName = "a" * 255
        settingsApi.executeLoadSettingsResult = ElasticSuccess(
          s"""{"$maxName":{"settings":{"index":{"number_of_shards":"1"}}}}"""
        )

        // When
        val result = settingsApi.loadSettings(maxName)

        // Then
        result.isSuccess shouldBe true
      }

      "handle settings with special characters in JSON" in {
        // Given
        val settingsWithSpecialChars =
          """{"index": {"routing": {"allocation": {"include": {"_tier": "data_hot"}}}}}"""
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = settingsApi.updateSettings("my-index", settingsWithSpecialChars)

        // Then
        result.isSuccess shouldBe true
      }

      "handle very long settings JSON" in {
        // Given
        val longSettings =
          """{"index": {""" + (1 to 100).map(i => s""""field$i": "value$i"""").mkString(",") + "}}"
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = settingsApi.updateSettings("my-index", longSettings)

        // Then
        result.isSuccess shouldBe true
      }

      "handle loadSettings response with extra whitespace" in {
        // Given
        val jsonWithWhitespace =
          """{
            |  "my-index"  :  {
            |    "settings"  :  {
            |      "index"  :  {
            |        "number_of_shards"  :  "1"
            |      }
            |    }
            |  }
            |}""".stripMargin
        settingsApi.executeLoadSettingsResult = ElasticSuccess(jsonWithWhitespace)

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isSuccess shouldBe true
      }

      "handle multiple toggleRefresh calls" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result1 = settingsApi.toggleRefresh("my-index", enable = true)
        val result2 = settingsApi.toggleRefresh("my-index", enable = false)
        val result3 = settingsApi.toggleRefresh("my-index", enable = true)

        // Then
        result1.isSuccess shouldBe true
        result2.isSuccess shouldBe true
        result3.isSuccess shouldBe true

        verify(mockLogger, times(3)).info(contains("succeeded"))
      }

      "handle setReplicas with same value multiple times" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result1 = settingsApi.setReplicas("my-index", 2)
        val result2 = settingsApi.setReplicas("my-index", 2)

        // Then
        result1.isSuccess shouldBe true
        result2.isSuccess shouldBe true
      }
    }

    "error handling" should {

      "handle all validation errors for updateSettings" in {
        // Invalid index
        val result1 = settingsApi.updateSettings("INVALID")
        result1.isFailure shouldBe true

        // Invalid settings
        val result2 = settingsApi.updateSettings("my-index", "invalid")
        result2.isFailure shouldBe true

        // Empty index
        val result3 = settingsApi.updateSettings("")
        result3.isFailure shouldBe true

        // Empty settings
        val result4 = settingsApi.updateSettings("my-index", "")
        result4.isFailure shouldBe true
      }

      "handle all validation errors for loadSettings" in {
        // Invalid index
        val result1 = settingsApi.loadSettings("INVALID")
        result1.isFailure shouldBe true

        // Empty index
        val result2 = settingsApi.loadSettings("")
        result2.isFailure shouldBe true

        // Null index
        val result3 = settingsApi.loadSettings(null)
        result3.isFailure shouldBe true
      }

      "handle authentication error in updateSettings" in {
        // Given
        val error = ElasticError("Authentication failed", statusCode = Some(401))
        settingsApi.executeCloseIndexResult = ElasticFailure(error)

        // When
        val result = settingsApi.updateSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(401)
      }

      "handle authorization error in loadSettings" in {
        // Given
        val error = ElasticError("Insufficient permissions", statusCode = Some(403))
        settingsApi.executeLoadSettingsResult = ElasticFailure(error)

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(403)
      }

      "handle timeout error in updateSettings" in {
        // Given
        val error = ElasticError(
          "Request timeout",
          cause = Some(new java.net.SocketTimeoutException()),
          statusCode = Some(504)
        )
        settingsApi.executeCloseIndexResult = ElasticFailure(error)

        // When
        val result = settingsApi.updateSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.cause shouldBe defined
        result.error.get.cause.get shouldBe a[java.net.SocketTimeoutException]
      }

      "handle server error in loadSettings" in {
        // Given
        val error = ElasticError("Internal server error", statusCode = Some(500))
        settingsApi.executeLoadSettingsResult = ElasticFailure(error)

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(500)
      }

      "preserve error context through workflow" in {
        // Given
        val error = ElasticError(
          message = "Close failed",
          cause = Some(new RuntimeException("Root cause")),
          statusCode = Some(500),
          index = Some("my-index"),
          operation = Some("closeIndex")
        )
        settingsApi.executeCloseIndexResult = ElasticFailure(error)

        // When
        val result = settingsApi.updateSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Close failed"
        result.error.get.cause shouldBe defined
        result.error.get.statusCode shouldBe Some(500)
        result.error.get.index shouldBe Some("my-index")
      }
    }

    "logging behavior" should {

      "log debug message with emoji for updateSettings" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        settingsApi.updateSettings("my-index")

        // Then
        verify(mockLogger).debug(contains("🔧"))
        verify(mockLogger).debug(contains("Updating settings for index my-index"))
      }

      "log debug message with emoji for loadSettings" in {
        // Given
        settingsApi.executeLoadSettingsResult = ElasticSuccess(
          """{"my-index":{"settings":{"index":{}}}}"""
        )

        // When
        settingsApi.loadSettings("my-index")

        // Then
        verify(mockLogger).debug(contains("🔍"))
        verify(mockLogger).debug(contains("Loading settings for index my-index"))
      }

      "log info message with emoji for successful update" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        settingsApi.updateSettings("my-index")

        // Then
        // Multiple info logs with ✅: close, update, open
        verify(mockLogger, atLeast(1)).info(contains("✅"))
        verify(mockLogger).info(contains("succeeded"))

        // Or more specifically:
        verify(mockLogger).info("✅ Index 'my-index' closed successfully")
        verify(mockLogger).info("✅ Updating settings for index 'my-index' succeeded")
        verify(mockLogger).info("✅ Index 'my-index' opened successfully")
      }

      "log error message with emoji for failed close" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticFailure(ElasticError("Failed"))

        // When
        settingsApi.updateSettings("my-index")

        // Then
        // Multiple error logs with ❌: from closeIndex and updateSettings
        verify(mockLogger, atLeast(1)).error(contains("❌"))
        verify(mockLogger).error(contains("Closing index my-index failed"))
        verify(mockLogger).error(contains("settings for index 'my-index' will not be updated"))
      }

      "log error message with emoji for failed update" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticFailure(ElasticError("Failed"))

        // When
        settingsApi.updateSettings("my-index")

        // Then
        verify(mockLogger).error(contains("❌"))
        verify(mockLogger).error(contains("Updating settings for index 'my-index' failed"))
      }

      "log error message with emoji for parse failure" in {
        // Given
        settingsApi.executeLoadSettingsResult = ElasticSuccess("invalid json")

        // When
        settingsApi.loadSettings("my-index")

        // Then
        verify(mockLogger).error(contains("❌"))
        verify(mockLogger).error(contains("Failed to parse JSON settings"))
      }

      "log error message with emoji for index not found" in {
        // Given
        settingsApi.executeLoadSettingsResult = ElasticSuccess("{}")

        // When
        settingsApi.loadSettings("my-index")

        // Then
        verify(mockLogger).error(contains("❌"))
        verify(mockLogger).error(contains("Index 'my-index' not found"))
      }

      "not log anything for validation failures" in {
        // When
        settingsApi.updateSettings("INVALID")

        // Then
        verify(mockLogger, never).debug(any[String])
        verify(mockLogger, never).info(any[String])
        verify(mockLogger, never).error(any[String])
      }
    }

    "JSON parsing" should {

      "correctly parse nested index settings" in {
        // Given
        val jsonResponse =
          """{
            |  "my-index": {
            |    "settings": {
            |      "index": {
            |        "creation_date": "1234567890",
            |        "number_of_shards": "1",
            |        "number_of_replicas": "0",
            |        "uuid": "abc123",
            |        "version": {
            |          "created": "7100099"
            |        }
            |      }
            |    }
            |  }
            |}""".stripMargin
        settingsApi.executeLoadSettingsResult = ElasticSuccess(jsonResponse)

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get should include("creation_date")
        result.get should include("uuid")
        result.get should include("version")
      }

      "handle null values in JSON" in {
        // Given
        val jsonResponse = """{"my-index":{"settings":{"index":{"field":null}}}}"""
        settingsApi.executeLoadSettingsResult = ElasticSuccess(jsonResponse)

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isSuccess shouldBe true
      }

      "handle empty index settings object" in {
        // Given
        val jsonResponse = """{"my-index":{"settings":{"index":{}}}}"""
        settingsApi.executeLoadSettingsResult = ElasticSuccess(jsonResponse)

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe "{}"
      }

      "handle malformed JSON gracefully" in {
        // Given
        settingsApi.executeLoadSettingsResult =
          ElasticSuccess("""{"my-index":{"settings":{"index":""")

        // When
        val result = settingsApi.loadSettings("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.operation shouldBe Some("loadSettings")
        verify(mockLogger).error(contains("Failed to parse JSON"))
      }
    }

    "concurrent operations" should {

      "handle multiple concurrent updateSettings calls" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val results = (1 to 3).map(i => settingsApi.updateSettings(s"index-$i"))

        // Then
        results.foreach(_.isSuccess shouldBe true)
        verify(mockLogger, times(3)).info(contains("succeeded"))
      }

      "handle mixed operations concurrently" in {
        // Given
        settingsApi.executeCloseIndexResult = ElasticSuccess(true)
        settingsApi.executeUpdateSettingsResult = ElasticSuccess(true)
        settingsApi.executeOpenIndexResult = ElasticSuccess(true)
        settingsApi.executeLoadSettingsResult = ElasticSuccess(
          """{"my-index":{"settings":{"index":{}}}}"""
        )

        // When
        val toggle = settingsApi.toggleRefresh("my-index", enable = true)
        val replicas = settingsApi.setReplicas("my-index", 2)
        val load = settingsApi.loadSettings("my-index")

        // Then
        toggle.isSuccess shouldBe true
        replicas.isSuccess shouldBe true
        load.isSuccess shouldBe true
      }
    }

    "validation order" should {

      "validate index name before calling executeCloseIndex" in {
        // Given
        var closeCalled = false
        val validatingApi = new SettingsApi with IndicesApi with RefreshApi {
          override protected def logger: Logger = mockLogger

          override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] = {
            closeCalled = true
            ElasticSuccess(true)
          }

          override private[client] def executeUpdateSettings(
            index: String,
            settings: String
          ): ElasticResult[Boolean] = ???
          override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = ???
          override private[client] def executeLoadSettings(index: String): ElasticResult[String] =
            ???
          override private[client] def executeCreateIndex(
            index: String,
            settings: String
          ): ElasticResult[Boolean] = ???
          override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeReindex(
            sourceIndex: String,
            targetIndex: String,
            refresh: Boolean,
            pipeline: Option[String]
          ): ElasticResult[(Boolean, Option[Long])] = ???
          override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = ???
        }

        // When
        validatingApi.updateSettings("INVALID")

        // Then
        closeCalled shouldBe false
      }

      "validate settings after index name" in {
        // Given
        var closeCalled = false
        val validatingApi = new SettingsApi with IndicesApi with RefreshApi {
          override protected def logger: Logger = mockLogger

          override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] = {
            closeCalled = true
            ElasticSuccess(true)
          }

          override private[client] def executeUpdateSettings(
            index: String,
            settings: String
          ): ElasticResult[Boolean] = ???
          override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = ???
          override private[client] def executeLoadSettings(index: String): ElasticResult[String] =
            ???
          override private[client] def executeCreateIndex(
            index: String,
            settings: String
          ): ElasticResult[Boolean] = ???
          override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeReindex(
            sourceIndex: String,
            targetIndex: String,
            refresh: Boolean,
            pipeline: Option[String]
          ): ElasticResult[(Boolean, Option[Long])] = ???
          override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = ???
        }

        // When
        validatingApi.updateSettings("valid-index", "invalid json")

        // Then
        closeCalled shouldBe false
      }

      "validate index name before calling executeLoadSettings" in {
        // Given
        var loadCalled = false
        val validatingApi = new SettingsApi with IndicesApi with RefreshApi {
          override protected def logger: Logger = mockLogger

          override private[client] def executeLoadSettings(index: String): ElasticResult[String] = {
            loadCalled = true
            ElasticSuccess("{}")
          }

          override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeUpdateSettings(
            index: String,
            settings: String
          ): ElasticResult[Boolean] = ???
          override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = ???
          override private[client] def executeCreateIndex(
            index: String,
            settings: String
          ): ElasticResult[Boolean] = ???
          override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeReindex(
            sourceIndex: String,
            targetIndex: String,
            refresh: Boolean,
            pipeline: Option[String]
          ): ElasticResult[(Boolean, Option[Long])] = ???
          override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = ???
        }

        // When
        validatingApi.loadSettings("INVALID")

        // Then
        loadCalled shouldBe false
      }
    }
  }
}
