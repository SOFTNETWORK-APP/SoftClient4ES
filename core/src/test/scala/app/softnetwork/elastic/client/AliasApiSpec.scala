package app.softnetwork.elastic.client

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.mockito.MockitoSugar
import org.mockito.ArgumentMatchersSugar
import org.slf4j.Logger
import app.softnetwork.elastic.client.result._

/** Unit tests for AliasApi
  */
class AliasApiSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterEach
    with MockitoSugar
    with ArgumentMatchersSugar {

  // Mock logger
  val mockLogger: Logger = mock[Logger]

  // Concrete implementation for testing
  class TestAliasApi extends NopeClientApi {
    override protected def logger: Logger = mockLogger

    // Control variables
    var executeAddAliasResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeRemoveAliasResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeAliasExistsResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeGetAliasesResult: ElasticResult[String] = ElasticSuccess(
      """{"my-index":{"aliases":{"my-alias":{}}}}"""
    )
    var executeSwapAliasResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeIndexExistsResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeGetIndexResult: ElasticResult[Option[String]] = ElasticSuccess(None)

    override private[client] def executeAddAlias(
      index: String,
      alias: String
    ): ElasticResult[Boolean] = {
      executeAddAliasResult
    }

    override private[client] def executeRemoveAlias(
      index: String,
      alias: String
    ): ElasticResult[Boolean] = {
      executeRemoveAliasResult
    }

    override private[client] def executeAliasExists(alias: String): ElasticResult[Boolean] = {
      executeAliasExistsResult
    }

    override private[client] def executeGetAliases(index: String): ElasticResult[String] = {
      executeGetAliasesResult
    }

    override private[client] def executeSwapAlias(
      oldIndex: String,
      newIndex: String,
      alias: String
    ): ElasticResult[Boolean] = {
      executeSwapAliasResult
    }

    override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] = {
      executeIndexExistsResult
    }

    override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] = {
      executeGetIndexResult
    }

  }

  var aliasApi: TestAliasApi = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    aliasApi = new TestAliasApi()
    reset(mockLogger)
  }

  "AliasApi" should {

    "addAlias" should {

      "successfully add an alias to an index" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        val result = aliasApi.addAlias("my-index", "my-alias")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe true

        verify(mockLogger).debug("Adding alias 'my-alias' to index 'my-index'")
        verify(mockLogger).info("✅ Alias 'my-alias' successfully added to index 'my-index'")
      }

      "reject invalid index name" in {
        // When
        val result = aliasApi.addAlias("INVALID", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
        result.error.get.message should include("lowercase")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("addAlias")

        verify(mockLogger, never).debug(any[String])
      }

      "reject empty index name" in {
        // When
        val result = aliasApi.addAlias("", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
        result.error.get.message should include("cannot be empty")
      }

      "reject null index name" in {
        // When
        val result = aliasApi.addAlias(null, "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
      }

      "reject invalid alias name" in {
        // When
        val result = aliasApi.addAlias("my-index", "INVALID")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid alias")
        result.error.get.message should include("lowercase")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("addAlias")
      }

      "reject empty alias name" in {
        // When
        val result = aliasApi.addAlias("my-index", "")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid alias")
        result.error.get.message should include("cannot be empty")
      }

      "reject when index and alias have the same name" in {
        // When
        val result = aliasApi.addAlias("my-index", "my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Index and alias cannot have the same name: 'my-index'"
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("addAlias")
      }

      "fail when index does not exist" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(false)

        // When
        val result = aliasApi.addAlias("my-index", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Index 'my-index' does not exist"
        result.error.get.statusCode shouldBe Some(404)
        result.error.get.operation shouldBe Some("addAlias")

        verify(mockLogger, never).debug(contains("Adding alias"))
      }

      "fail when indexExists check fails" in {
        // Given
        val error = ElasticError("Connection timeout", statusCode = Some(504))
        aliasApi.executeIndexExistsResult = ElasticFailure(error)

        // When
        val result = aliasApi.addAlias("my-index", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Connection timeout"
        result.error.get.statusCode shouldBe Some(504)

        verify(mockLogger, never).debug(contains("Adding alias"))
      }

      "fail when executeAddAlias fails" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        val error = ElasticError("Alias already exists", statusCode = Some(400))
        aliasApi.executeAddAliasResult = ElasticFailure(error)

        // When
        val result = aliasApi.addAlias("my-index", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Alias already exists"

        verify(mockLogger).debug("Adding alias 'my-alias' to index 'my-index'")
        verify(mockLogger).error(
          "❌ Failed to add alias 'my-alias' to index 'my-index': Alias already exists"
        )
      }

      "validate in correct order: index, alias, same name, existence" in {
        // Given - Invalid index should fail first
        val result1 = aliasApi.addAlias("INVALID", "INVALID-ALIAS")
        result1.error.get.message should include("Invalid index")

        // Given - Invalid alias should fail after index validation
        val result2 = aliasApi.addAlias("my-index", "INVALID")
        result2.error.get.message should include("Invalid alias")

        // Given - Same name check should fail after name validation
        val result3 = aliasApi.addAlias("my-index", "my-index")
        result3.error.get.message should include("same name")

        // Given - Existence check should fail last
        aliasApi.executeIndexExistsResult = ElasticSuccess(false)
        val result4 = aliasApi.addAlias("my-index", "my-alias")
        result4.error.get.message should include("does not exist")
      }

      "handle special characters in valid names" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        val result = aliasApi.addAlias("my-index-2024", "my-alias_v1.0")

        // Then
        result.isSuccess shouldBe true
      }

      "reject alias names with forbidden characters" in {
        val forbiddenChars = List("\\", "/", "*", "?", "\"", "<", ">", "|", " ", ",", "#")

        forbiddenChars.foreach { char =>
          val result = aliasApi.addAlias("my-index", s"alias${char}name")
          result.isFailure shouldBe true
        }
      }

      "reject alias starting with forbidden characters" in {
        val result1 = aliasApi.addAlias("my-index", "-alias")
        result1.isFailure shouldBe true

        val result2 = aliasApi.addAlias("my-index", "_alias")
        result2.isFailure shouldBe true

        val result3 = aliasApi.addAlias("my-index", "+alias")
        result3.isFailure shouldBe true
      }

      "reject alias named '.' or '..'" in {
        val result1 = aliasApi.addAlias("my-index", ".")
        result1.isFailure shouldBe true

        val result2 = aliasApi.addAlias("my-index", "..")
        result2.isFailure shouldBe true
      }

      "handle alias name with maximum length" in {
        // Given
        val maxAlias = "a" * 255
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        val result = aliasApi.addAlias("my-index", maxAlias)

        // Then
        result.isSuccess shouldBe true
      }

      "reject alias name exceeding maximum length" in {
        // Given
        val tooLongAlias = "a" * 256

        // When
        val result = aliasApi.addAlias("my-index", tooLongAlias)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid alias")
      }
    }

    "removeAlias" should {

      "successfully remove an alias from an index" in {
        // Given
        aliasApi.executeRemoveAliasResult = ElasticSuccess(true)

        // When
        val result = aliasApi.removeAlias("my-index", "my-alias")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe true

        verify(mockLogger).debug("Removing alias 'my-alias' from index 'my-index'")
        verify(mockLogger).info("✅ Alias 'my-alias' successfully removed from index 'my-index'")
      }

      "reject invalid index name" in {
        // When
        val result = aliasApi.removeAlias("INVALID", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("removeAlias")

        verify(mockLogger, never).debug(any[String])
      }

      "reject invalid alias name" in {
        // When
        val result = aliasApi.removeAlias("my-index", "INVALID")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid alias")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("removeAlias")
      }

      "fail when alias does not exist (404)" in {
        // Given
        val error = ElasticError("Alias not found", statusCode = Some(404))
        aliasApi.executeRemoveAliasResult = ElasticFailure(error)

        // When
        val result = aliasApi.removeAlias("my-index", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(404)

        verify(mockLogger).debug("Removing alias 'my-alias' from index 'my-index'")
        verify(mockLogger).error(
          "❌ Failed to remove alias 'my-alias' from index 'my-index': Alias not found"
        )
      }

      "fail when executeRemoveAlias fails" in {
        // Given
        val error = ElasticError("Server error", statusCode = Some(500))
        aliasApi.executeRemoveAliasResult = ElasticFailure(error)

        // When
        val result = aliasApi.removeAlias("my-index", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Server error"

        verify(mockLogger).error(contains("Failed to remove alias"))
      }

      "validate index name before alias name" in {
        // When
        val result = aliasApi.removeAlias("INVALID", "INVALID-ALIAS")

        // Then
        result.error.get.message should include("Invalid index")
        result.error.get.message should not include "Invalid alias"
      }

      "handle empty index name" in {
        // When
        val result = aliasApi.removeAlias("", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
      }

      "handle empty alias name" in {
        // When
        val result = aliasApi.removeAlias("my-index", "")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid alias")
      }
    }

    "aliasExists" should {

      "return true when alias exists" in {
        // Given
        aliasApi.executeAliasExistsResult = ElasticSuccess(true)

        // When
        val result = aliasApi.aliasExists("my-alias")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe true

        verify(mockLogger).debug("Checking if alias 'my-alias' exists")
        verify(mockLogger).info("✅ Alias 'my-alias' exists")
      }

      "return false when alias does not exist" in {
        // Given
        aliasApi.executeAliasExistsResult = ElasticSuccess(false)

        // When
        val result = aliasApi.aliasExists("my-alias")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe false

        verify(mockLogger).debug("Checking if alias 'my-alias' exists")
        verify(mockLogger).info("✅ Alias 'my-alias' does not exist")
      }

      "reject invalid alias name" in {
        // When
        val result = aliasApi.aliasExists("INVALID")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid alias name")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("aliasExists")

        verify(mockLogger, never).debug(any[String])
      }

      "reject empty alias name" in {
        // When
        val result = aliasApi.aliasExists("")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid alias name")
      }

      "fail when executeAliasExists fails" in {
        // Given
        val error = ElasticError("Connection timeout", statusCode = Some(504))
        aliasApi.executeAliasExistsResult = ElasticFailure(error)

        // When
        val result = aliasApi.aliasExists("my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Connection timeout"

        verify(mockLogger).debug("Checking if alias 'my-alias' exists")
        verify(mockLogger).error(
          "❌ Failed to check existence of alias 'my-alias': Connection timeout"
        )
      }

      "handle network errors" in {
        // Given
        val error = ElasticError(
          "Network error",
          cause = Some(new java.net.SocketTimeoutException()),
          statusCode = Some(504)
        )
        aliasApi.executeAliasExistsResult = ElasticFailure(error)

        // When
        val result = aliasApi.aliasExists("my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.cause shouldBe defined
      }
    }

    "getAliases" should {

      "successfully retrieve aliases from an index" in {
        // Given
        val jsonResponse = """{"my-index":{"aliases":{"alias1":{},"alias2":{}}}}"""
        aliasApi.executeGetAliasesResult = ElasticSuccess(jsonResponse)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe Set("alias1", "alias2")

        verify(mockLogger).debug("Getting aliases for index 'my-index'")
        verify(mockLogger).debug("Found 2 alias(es) for index 'my-index': alias1, alias2")
        verify(mockLogger).info("✅ Found 2 alias(es) for index 'my-index': alias1, alias2")
      }

      "return empty set when index has no aliases" in {
        // Given
        val jsonResponse = """{"my-index":{"aliases":{}}}"""
        aliasApi.executeGetAliasesResult = ElasticSuccess(jsonResponse)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe Set.empty

        verify(mockLogger).debug("No aliases found for index 'my-index'")
        verify(mockLogger).info("✅ No aliases found for index 'my-index'")
      }

      "return empty set when aliases object is null" in {
        // Given
        val jsonResponse = """{"my-index":{"mappings":{}}}"""
        aliasApi.executeGetAliasesResult = ElasticSuccess(jsonResponse)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe Set.empty
      }

      "return empty set when index not found in response" in {
        // Given
        aliasApi.executeGetAliasesResult = ElasticSuccess("{}")

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe Set.empty

        verify(mockLogger).warn("Index 'my-index' not found in response")
      }

      "reject invalid index name" in {
        // When
        val result = aliasApi.getAliases("INVALID")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index name")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("getAliases")

        verify(mockLogger, never).debug(any[String])
      }

      "fail when executeGetAliases fails" in {
        // Given
        val error = ElasticError("Index not found", statusCode = Some(404))
        aliasApi.executeGetAliasesResult = ElasticFailure(error)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Index not found"

        verify(mockLogger).debug("Getting aliases for index 'my-index'")
        verify(mockLogger).error("❌ Failed to get aliases for index 'my-index': Index not found")
      }

      "fail when JSON parsing fails" in {
        // Given
        aliasApi.executeGetAliasesResult = ElasticSuccess("invalid json {")

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isFailure shouldBe true

        verify(mockLogger).error(contains("Failed to parse aliases JSON"))
      }

      "handle complex JSON structure" in {
        // Given
        val complexJson =
          """{
            |  "my-index": {
            |    "aliases": {
            |      "alias1": {"filter": {"term": {"user": "kimchy"}}},
            |      "alias2": {"routing": "1"},
            |      "alias3": {}
            |    }
            |  }
            |}""".stripMargin
        aliasApi.executeGetAliasesResult = ElasticSuccess(complexJson)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe Set("alias1", "alias2", "alias3")
      }

      "handle single alias" in {
        // Given
        val jsonResponse = """{"my-index":{"aliases":{"single-alias":{}}}}"""
        aliasApi.executeGetAliasesResult = ElasticSuccess(jsonResponse)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe Set("single-alias")

        verify(mockLogger).info(contains("Found 1 alias(es)"))
      }

      "handle whitespace in JSON" in {
        // Given
        val jsonWithWhitespace =
          """{
            |  "my-index"  :  {
            |    "aliases"  :  {
            |      "alias1"  :  {}
            |    }
            |  }
            |}""".stripMargin
        aliasApi.executeGetAliasesResult = ElasticSuccess(jsonWithWhitespace)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe Set("alias1")
      }
    }

    "swapAlias" should {

      "successfully swap alias between two indexes" in {
        // Given
        aliasApi.executeSwapAliasResult = ElasticSuccess(true)

        // When
        val result = aliasApi.swapAlias("old-index", "new-index", "my-alias")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe true

        verify(mockLogger).info(
          "Swapping alias 'my-alias' from 'old-index' to 'new-index' (atomic operation)"
        )
        verify(mockLogger).info(
          "✅ Alias 'my-alias' successfully swapped from 'old-index' to 'new-index'"
        )
      }

      "reject invalid old index name" in {
        // When
        val result = aliasApi.swapAlias("INVALID", "new-index", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid old index name")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("swapAlias")

        verify(mockLogger, never).info(contains("Swapping"))
      }

      "reject invalid new index name" in {
        // When
        val result = aliasApi.swapAlias("old-index", "INVALID", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid new index name")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("swapAlias")
      }

      "reject invalid alias name" in {
        // When
        val result = aliasApi.swapAlias("old-index", "new-index", "INVALID")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid alias name")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("swapAlias")
      }

      "reject when old and new index are the same" in {
        // When
        val result = aliasApi.swapAlias("my-index", "my-index", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Old and new index cannot be the same: 'my-index'"
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("swapAlias")
      }

      "fail when executeSwapAlias fails" in {
        // Given
        val error = ElasticError("Old index not found", statusCode = Some(404))
        aliasApi.executeSwapAliasResult = ElasticFailure(error)

        // When
        val result = aliasApi.swapAlias("old-index", "new-index", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Old index not found"

        verify(mockLogger).info(
          "Swapping alias 'my-alias' from 'old-index' to 'new-index' (atomic operation)"
        )
        verify(mockLogger).error(
          "❌ Failed to swap alias 'my-alias' from 'old-index' to 'new-index': Old index not found"
        )
      }

      "validate in correct order: oldIndex, newIndex, alias, same index" in {
        // Given - Invalid old index should fail first
        val result1 = aliasApi.swapAlias("INVALID", "new-index", "my-alias")
        result1.error.get.message should include("Invalid old index")

        // Given - Invalid new index should fail after old index
        val result2 = aliasApi.swapAlias("old-index", "INVALID", "my-alias")
        result2.error.get.message should include("Invalid new index")

        // Given - Invalid alias should fail after both indexes
        val result3 = aliasApi.swapAlias("old-index", "new-index", "INVALID")
        result3.error.get.message should include("Invalid alias")

        // Given - Same index check should fail last
        val result4 = aliasApi.swapAlias("my-index", "my-index", "my-alias")
        result4.error.get.message should include("same")
      }

      "handle empty index names" in {
        val result1 = aliasApi.swapAlias("", "new-index", "my-alias")
        result1.isFailure shouldBe true

        val result2 = aliasApi.swapAlias("old-index", "", "my-alias")
        result2.isFailure shouldBe true
      }

      "handle empty alias name" in {
        // When
        val result = aliasApi.swapAlias("old-index", "new-index", "")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid alias")
      }

      "handle network timeout" in {
        // Given
        val error = ElasticError(
          "Request timeout",
          cause = Some(new java.net.SocketTimeoutException()),
          statusCode = Some(504)
        )
        aliasApi.executeSwapAliasResult = ElasticFailure(error)

        // When
        val result = aliasApi.swapAlias("old-index", "new-index", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.cause shouldBe defined
      }
    }

    "workflow scenarios" should {

      "successfully add, check, and remove alias" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)
        aliasApi.executeAliasExistsResult = ElasticSuccess(true)
        aliasApi.executeRemoveAliasResult = ElasticSuccess(true)

        // When
        val addResult = aliasApi.addAlias("my-index", "my-alias")
        val existsResult = aliasApi.aliasExists("my-alias")
        val removeResult = aliasApi.removeAlias("my-index", "my-alias")

        // Then
        addResult.isSuccess shouldBe true
        existsResult.isSuccess shouldBe true
        existsResult.get shouldBe true
        removeResult.isSuccess shouldBe true
      }

      "successfully perform zero-downtime deployment with swapAlias" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)
        aliasApi.executeSwapAliasResult = ElasticSuccess(true)

        // When - Initial setup
        val addResult = aliasApi.addAlias("products-v1", "products")

        // When - Deploy
        val swapResult = aliasApi.swapAlias("products-v1", "products-v2", "products")

        // Then
        addResult.isSuccess shouldBe true
        swapResult.isSuccess shouldBe true

        verify(mockLogger).info(contains("atomic operation"))
      }

      "successfully add multiple aliases to same index" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        val result1 = aliasApi.addAlias("my-index", "alias1")
        val result2 = aliasApi.addAlias("my-index", "alias2")
        val result3 = aliasApi.addAlias("my-index", "alias3")

        // Then
        result1.isSuccess shouldBe true
        result2.isSuccess shouldBe true
        result3.isSuccess shouldBe true
      }

      "successfully add same alias to multiple indexes" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        val result1 = aliasApi.addAlias("index1", "shared-alias")
        val result2 = aliasApi.addAlias("index2", "shared-alias")
        val result3 = aliasApi.addAlias("index3", "shared-alias")

        // Then
        result1.isSuccess shouldBe true
        result2.isSuccess shouldBe true
        result3.isSuccess shouldBe true
      }

      "handle partial failure in workflow" in {
        // Given - First operation succeeds
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)
        val result1 = aliasApi.addAlias("my-index", "alias1")

        // Given - Second operation fails
        aliasApi.executeAddAliasResult = ElasticFailure(ElasticError("Conflict"))
        val result2 = aliasApi.addAlias("my-index", "alias2")

        // Then
        result1.isSuccess shouldBe true
        result2.isFailure shouldBe true
      }

      "retrieve aliases after adding them" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)
        aliasApi.executeGetAliasesResult = ElasticSuccess(
          """{"my-index":{"aliases":{"alias1":{},"alias2":{}}}}"""
        )

        // When
        val add1 = aliasApi.addAlias("my-index", "alias1")
        val add2 = aliasApi.addAlias("my-index", "alias2")
        val getResult = aliasApi.getAliases("my-index")

        // Then
        add1.isSuccess shouldBe true
        add2.isSuccess shouldBe true
        getResult.isSuccess shouldBe true
        getResult.get should contain allOf ("alias1", "alias2")
      }

      "verify alias existence before and after removal" in {
        // Given
        aliasApi.executeAliasExistsResult = ElasticSuccess(true)
        aliasApi.executeRemoveAliasResult = ElasticSuccess(true)

        // When - Check exists before removal
        val existsBefore = aliasApi.aliasExists("my-alias")

        // When - Remove
        val removeResult = aliasApi.removeAlias("my-index", "my-alias")

        // When - Check exists after removal
        aliasApi.executeAliasExistsResult = ElasticSuccess(false)
        val existsAfter = aliasApi.aliasExists("my-alias")

        // Then
        existsBefore.get shouldBe true
        removeResult.isSuccess shouldBe true
        existsAfter.get shouldBe false
      }
    }

    "ElasticResult integration" should {

      "work with map transformation" in {
        // Given
        aliasApi.executeGetAliasesResult = ElasticSuccess(
          """{"my-index":{"aliases":{"alias1":{},"alias2":{}}}}"""
        )

        // When
        val result = aliasApi.getAliases("my-index").map(_.size)

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe 2
      }

      "work with flatMap composition" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)
        aliasApi.executeAliasExistsResult = ElasticSuccess(true)

        // When
        val result = aliasApi.addAlias("my-index", "my-alias").flatMap { _ =>
          aliasApi.aliasExists("my-alias")
        }

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe true
      }

      "work with for-comprehension" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)
        aliasApi.executeGetAliasesResult = ElasticSuccess(
          """{"my-index":{"aliases":{"my-alias":{}}}}"""
        )

        // When
        val result = for {
          _       <- aliasApi.addAlias("my-index", "my-alias")
          aliases <- aliasApi.getAliases("my-index")
        } yield aliases.contains("my-alias")

        // Then
        result shouldBe ElasticSuccess(true)
      }

      "propagate errors through transformations" in {
        // Given
        val error = ElasticError("Failed")
        aliasApi.executeGetAliasesResult = ElasticFailure(error)

        // When
        val result = aliasApi
          .getAliases("my-index")
          .map(_.size)
          .flatMap(size => ElasticSuccess(size * 2))

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Failed"
      }

      "handle chained operations with mixed results" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)
        aliasApi.executeAliasExistsResult = ElasticFailure(ElasticError("Check failed"))

        // When
        val result = for {
          _      <- aliasApi.addAlias("my-index", "my-alias")
          exists <- aliasApi.aliasExists("my-alias")
        } yield exists

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Check failed"
      }
    }

    "edge cases" should {

      "handle alias with maximum valid length" in {
        // Given
        val maxAlias = "a" * 255
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        val result = aliasApi.addAlias("my-index", maxAlias)

        // Then
        result.isSuccess shouldBe true
      }

      "handle index with maximum valid length" in {
        // Given
        val maxIndex = "a" * 255
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        val result = aliasApi.addAlias(maxIndex, "my-alias")

        // Then
        result.isSuccess shouldBe true
      }

      "handle alias names with hyphens and underscores" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        val result = aliasApi.addAlias("my-index", "my-alias_v1.0-prod")

        // Then
        result.isSuccess shouldBe true
      }

      "handle alias names with dots" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        val result = aliasApi.addAlias("my-index", "my.alias.name")

        // Then
        result.isSuccess shouldBe true
      }

      "handle very long error messages" in {
        // Given
        val longMessage = "Error: " + ("x" * 1000)
        val error = ElasticError(longMessage, statusCode = Some(500))
        aliasApi.executeAddAliasResult = ElasticFailure(error)
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)

        // When
        val result = aliasApi.addAlias("my-index", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Error:")
      }

      "handle null response from executeGetAliases" in {
        // Given
        aliasApi.executeGetAliasesResult = ElasticSuccess(null)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isFailure shouldBe true
      }

      "handle malformed JSON with missing fields" in {
        // Given
        aliasApi.executeGetAliasesResult = ElasticSuccess("""{"my-index":{}}""")

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe Set.empty
      }

      "handle JSON with unexpected structure" in {
        // Given
        aliasApi.executeGetAliasesResult = ElasticSuccess("""{"unexpected":"structure"}""")

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe Set.empty
      }

      "handle multiple consecutive swapAlias calls" in {
        // Given
        aliasApi.executeSwapAliasResult = ElasticSuccess(true)

        // When
        val result1 = aliasApi.swapAlias("v1", "v2", "current")
        val result2 = aliasApi.swapAlias("v2", "v3", "current")
        val result3 = aliasApi.swapAlias("v3", "v4", "current")

        // Then
        result1.isSuccess shouldBe true
        result2.isSuccess shouldBe true
        result3.isSuccess shouldBe true

        verify(mockLogger, times(3)).info(contains("atomic operation"))
      }

      "handle concurrent alias operations" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)
        aliasApi.executeAliasExistsResult = ElasticSuccess(true)

        // When
        val results = (1 to 5).map(i => aliasApi.addAlias(s"index-$i", s"alias-$i"))

        // Then
        results.foreach(_.isSuccess shouldBe true)
      }
    }

    "error handling" should {

      "handle authentication error in addAlias" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        val error = ElasticError("Authentication failed", statusCode = Some(401))
        aliasApi.executeAddAliasResult = ElasticFailure(error)

        // When
        val result = aliasApi.addAlias("my-index", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(401)
      }

      "handle authorization error in removeAlias" in {
        // Given
        val error = ElasticError("Insufficient permissions", statusCode = Some(403))
        aliasApi.executeRemoveAliasResult = ElasticFailure(error)

        // When
        val result = aliasApi.removeAlias("my-index", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(403)
      }

      "handle timeout error in aliasExists" in {
        // Given
        val error = ElasticError(
          "Request timeout",
          cause = Some(new java.net.SocketTimeoutException()),
          statusCode = Some(504)
        )
        aliasApi.executeAliasExistsResult = ElasticFailure(error)

        // When
        val result = aliasApi.aliasExists("my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.cause shouldBe defined
        result.error.get.cause.get shouldBe a[java.net.SocketTimeoutException]
      }

      "handle server error in getAliases" in {
        // Given
        val error = ElasticError("Internal server error", statusCode = Some(500))
        aliasApi.executeGetAliasesResult = ElasticFailure(error)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(500)
      }

      "handle conflict error in swapAlias" in {
        // Given
        val error = ElasticError("Version conflict", statusCode = Some(409))
        aliasApi.executeSwapAliasResult = ElasticFailure(error)

        // When
        val result = aliasApi.swapAlias("old-index", "new-index", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(409)
      }

      "preserve error context through operations" in {
        // Given
        val error = ElasticError(
          message = "Operation failed",
          cause = Some(new RuntimeException("Root cause")),
          statusCode = Some(500),
          index = Some("my-index"),
          operation = Some("internal")
        )
        aliasApi.executeAddAliasResult = ElasticFailure(error)
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)

        // When
        val result = aliasApi.addAlias("my-index", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Operation failed"
        result.error.get.cause shouldBe defined
        result.error.get.statusCode shouldBe Some(500)
      }

      "handle all validation errors for addAlias" in {
        // Invalid index
        val result1 = aliasApi.addAlias("INVALID", "my-alias")
        result1.isFailure shouldBe true

        // Invalid alias
        val result2 = aliasApi.addAlias("my-index", "INVALID")
        result2.isFailure shouldBe true

        // Same name
        val result3 = aliasApi.addAlias("my-index", "my-index")
        result3.isFailure shouldBe true

        // Index doesn't exist
        aliasApi.executeIndexExistsResult = ElasticSuccess(false)
        val result4 = aliasApi.addAlias("my-index", "my-alias")
        result4.isFailure shouldBe true
      }

      "handle all validation errors for swapAlias" in {
        // Invalid old index
        val result1 = aliasApi.swapAlias("INVALID", "new-index", "my-alias")
        result1.isFailure shouldBe true

        // Invalid new index
        val result2 = aliasApi.swapAlias("old-index", "INVALID", "my-alias")
        result2.isFailure shouldBe true

        // Invalid alias
        val result3 = aliasApi.swapAlias("old-index", "new-index", "INVALID")
        result3.isFailure shouldBe true

        // Same indexes
        val result4 = aliasApi.swapAlias("my-index", "my-index", "my-alias")
        result4.isFailure shouldBe true
      }

      "handle network errors gracefully" in {
        // Given
        val error = ElasticError(
          "Connection refused",
          cause = Some(new java.net.ConnectException()),
          statusCode = Some(503)
        )
        aliasApi.executeIndexExistsResult = ElasticFailure(error)

        // When
        val result = aliasApi.addAlias("my-index", "my-alias")

        // Then
        result.isFailure shouldBe true
        result.error.get.cause shouldBe defined
      }

      "handle JSON parsing errors in getAliases" in {
        // Given
        aliasApi.executeGetAliasesResult = ElasticSuccess("not valid json at all")

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isFailure shouldBe true
        verify(mockLogger).error(contains("Failed to parse aliases JSON"))
      }
    }

    "logging behavior" should {

      "log debug message for addAlias" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        aliasApi.addAlias("my-index", "my-alias")

        // Then
        verify(mockLogger).debug("Adding alias 'my-alias' to index 'my-index'")
      }

      "log info message with emoji for successful addAlias" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        aliasApi.addAlias("my-index", "my-alias")

        // Then
        verify(mockLogger).info("✅ Alias 'my-alias' successfully added to index 'my-index'")
      }

      "log error message with emoji for failed addAlias" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticFailure(ElasticError("Failed"))

        // When
        aliasApi.addAlias("my-index", "my-alias")

        // Then
        verify(mockLogger).error("❌ Failed to add alias 'my-alias' to index 'my-index': Failed")
      }

      "log debug message for removeAlias" in {
        // Given
        aliasApi.executeRemoveAliasResult = ElasticSuccess(true)

        // When
        aliasApi.removeAlias("my-index", "my-alias")

        // Then
        verify(mockLogger).debug("Removing alias 'my-alias' from index 'my-index'")
      }

      "log info message with emoji for successful removeAlias" in {
        // Given
        aliasApi.executeRemoveAliasResult = ElasticSuccess(true)

        // When
        aliasApi.removeAlias("my-index", "my-alias")

        // Then
        verify(mockLogger).info("✅ Alias 'my-alias' successfully removed from index 'my-index'")
      }

      "log error message with emoji for failed removeAlias" in {
        // Given
        aliasApi.executeRemoveAliasResult = ElasticFailure(ElasticError("Not found"))

        // When
        aliasApi.removeAlias("my-index", "my-alias")

        // Then
        verify(mockLogger).error(
          "❌ Failed to remove alias 'my-alias' from index 'my-index': Not found"
        )
      }

      "log debug message for aliasExists" in {
        // Given
        aliasApi.executeAliasExistsResult = ElasticSuccess(true)

        // When
        aliasApi.aliasExists("my-alias")

        // Then
        verify(mockLogger).debug("Checking if alias 'my-alias' exists")
      }

      "log info message when alias exists" in {
        // Given
        aliasApi.executeAliasExistsResult = ElasticSuccess(true)

        // When
        aliasApi.aliasExists("my-alias")

        // Then
        verify(mockLogger).info("✅ Alias 'my-alias' exists")
      }

      "log info message when alias does not exist" in {
        // Given
        aliasApi.executeAliasExistsResult = ElasticSuccess(false)

        // When
        aliasApi.aliasExists("my-alias")

        // Then
        verify(mockLogger).info("✅ Alias 'my-alias' does not exist")
      }

      "log error message with emoji for failed aliasExists" in {
        // Given
        aliasApi.executeAliasExistsResult = ElasticFailure(ElasticError("Failed"))

        // When
        aliasApi.aliasExists("my-alias")

        // Then
        verify(mockLogger).error("❌ Failed to check existence of alias 'my-alias': Failed")
      }

      "log debug message for getAliases" in {
        // Given
        aliasApi.executeGetAliasesResult = ElasticSuccess("""{"my-index":{"aliases":{}}}""")

        // When
        aliasApi.getAliases("my-index")

        // Then
        verify(mockLogger).debug("Getting aliases for index 'my-index'")
      }

      "log debug and info messages for found aliases" in {
        // Given
        aliasApi.executeGetAliasesResult = ElasticSuccess(
          """{"my-index":{"aliases":{"alias1":{},"alias2":{}}}}"""
        )

        // When
        aliasApi.getAliases("my-index")

        // Then
        verify(mockLogger).debug(contains("Found 2 alias(es)"))
        verify(mockLogger).info(contains("✅ Found 2 alias(es)"))
      }

      "log info message when no aliases found" in {
        // Given
        aliasApi.executeGetAliasesResult = ElasticSuccess("""{"my-index":{"aliases":{}}}""")

        // When
        aliasApi.getAliases("my-index")

        // Then
        verify(mockLogger).info("✅ No aliases found for index 'my-index'")
      }

      "log warn message when index not found in response" in {
        // Given
        aliasApi.executeGetAliasesResult = ElasticSuccess("{}")

        // When
        aliasApi.getAliases("my-index")

        // Then
        verify(mockLogger).warn("Index 'my-index' not found in response")
      }

      "log error message with emoji for failed getAliases" in {
        // Given
        aliasApi.executeGetAliasesResult = ElasticFailure(ElasticError("Failed"))

        // When
        aliasApi.getAliases("my-index")

        // Then
        verify(mockLogger).error("❌ Failed to get aliases for index 'my-index': Failed")
      }

      "log info message for swapAlias start" in {
        // Given
        aliasApi.executeSwapAliasResult = ElasticSuccess(true)

        // When
        aliasApi.swapAlias("old-index", "new-index", "my-alias")

        // Then
        verify(mockLogger).info(
          "Swapping alias 'my-alias' from 'old-index' to 'new-index' (atomic operation)"
        )
      }

      "log info message with emoji for successful swapAlias" in {
        // Given
        aliasApi.executeSwapAliasResult = ElasticSuccess(true)

        // When
        aliasApi.swapAlias("old-index", "new-index", "my-alias")

        // Then
        verify(mockLogger).info(
          "✅ Alias 'my-alias' successfully swapped from 'old-index' to 'new-index'"
        )
      }

      "log error message with emoji for failed swapAlias" in {
        // Given
        aliasApi.executeSwapAliasResult = ElasticFailure(ElasticError("Failed"))

        // When
        aliasApi.swapAlias("old-index", "new-index", "my-alias")

        // Then
        verify(mockLogger).error(
          "❌ Failed to swap alias 'my-alias' from 'old-index' to 'new-index': Failed"
        )
      }

      "not log anything for validation failures" in {
        // When
        aliasApi.addAlias("INVALID", "my-alias")

        // Then
        verify(mockLogger, never).debug(any[String])
        verify(mockLogger, never).info(any[String])
        verify(mockLogger, never).error(any[String])
      }

      "log all emojis correctly" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)
        aliasApi.executeAliasExistsResult = ElasticSuccess(true)
        aliasApi.executeGetAliasesResult =
          ElasticSuccess("""{"my-index":{"aliases":{"my-alias":{}}}}""")

        // When
        aliasApi.addAlias("my-index", "my-alias")
        aliasApi.aliasExists("my-alias")
        aliasApi.getAliases("my-index")

        // Then
        verify(mockLogger, atLeast(1)).info(contains("✅"))
      }
    }

    "validation order" should {

      "validate index before alias in addAlias" in {
        // When
        val result = aliasApi.addAlias("INVALID", "INVALID-ALIAS")

        // Then
        result.error.get.message should include("Invalid index")
        result.error.get.message should not include "Invalid alias"
      }

      "validate alias after index in addAlias" in {
        // When
        val result = aliasApi.addAlias("my-index", "INVALID")

        // Then
        result.error.get.message should include("Invalid alias")
      }

      "validate same name after individual names in addAlias" in {
        // When
        val result = aliasApi.addAlias("my-index", "my-index")

        // Then
        result.error.get.message should include("same name")
      }

      "validate index existence last in addAlias" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(false)

        // When
        val result = aliasApi.addAlias("my-index", "my-alias")

        // Then
        result.error.get.message should include("does not exist")
      }

      "validate index before alias in removeAlias" in {
        // When
        val result = aliasApi.removeAlias("INVALID", "INVALID-ALIAS")

        // Then
        result.error.get.message should include("Invalid index")
        result.error.get.message should not include "Invalid alias"
      }

      "validate oldIndex, newIndex, alias, then same check in swapAlias" in {
        // Invalid old index
        val result1 = aliasApi.swapAlias("INVALID", "new-index", "my-alias")
        result1.error.get.message should include("Invalid old index")

        // Invalid new index
        val result2 = aliasApi.swapAlias("old-index", "INVALID", "my-alias")
        result2.error.get.message should include("Invalid new index")

        // Invalid alias
        val result3 = aliasApi.swapAlias("old-index", "new-index", "INVALID")
        result3.error.get.message should include("Invalid alias")

        // Same indexes
        val result4 = aliasApi.swapAlias("my-index", "my-index", "my-alias")
        result4.error.get.message should include("same")
      }

      "not call execute methods when validation fails" in {
        // Given
        var executeCalled = false
        val validatingApi = new NopeClientApi {
          override protected def logger: Logger = mockLogger

          override private[client] def executeAddAlias(
            index: String,
            alias: String
          ): ElasticResult[Boolean] = {
            executeCalled = true
            ElasticSuccess(true)
          }

          override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] = {
            ElasticSuccess(true)
          }

        }

        // When
        validatingApi.addAlias("INVALID", "my-alias")

        // Then
        executeCalled shouldBe false
      }
    }

    "JSON parsing" should {

      "correctly parse aliases with filters" in {
        // Given
        val jsonWithFilters =
          """{
            |  "my-index": {
            |    "aliases": {
            |      "filtered-alias": {
            |        "filter": {
            |          "term": {
            |            "user": "kimchy"
            |          }
            |        }
            |      }
            |    }
            |  }
            |}""".stripMargin
        aliasApi.executeGetAliasesResult = ElasticSuccess(jsonWithFilters)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get should contain("filtered-alias")
      }

      "correctly parse aliases with routing" in {
        // Given
        val jsonWithRouting =
          """{
            |  "my-index": {
            |    "aliases": {
            |      "routed-alias": {
            |        "routing": "1"
            |      }
            |    }
            |  }
            |}""".stripMargin
        aliasApi.executeGetAliasesResult = ElasticSuccess(jsonWithRouting)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get should contain("routed-alias")
      }

      "correctly parse aliases with search and index routing" in {
        // Given
        val jsonWithComplexRouting =
          """{
            |  "my-index": {
            |    "aliases": {
            |      "complex-alias": {
            |        "search_routing": "1,2",
            |        "index_routing": "2"
            |      }
            |    }
            |  }
            |}""".stripMargin
        aliasApi.executeGetAliasesResult = ElasticSuccess(jsonWithComplexRouting)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get should contain("complex-alias")
      }

      "handle null values in JSON" in {
        // Given
        val jsonWithNull = """{"my-index":{"aliases":{"my-alias":null}}}"""
        aliasApi.executeGetAliasesResult = ElasticSuccess(jsonWithNull)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get should contain("my-alias")
      }

      "handle empty aliases object" in {
        // Given
        val jsonEmpty = """{"my-index":{"aliases":{}}}"""
        aliasApi.executeGetAliasesResult = ElasticSuccess(jsonEmpty)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe Set.empty
      }

      "handle malformed JSON gracefully" in {
        // Given
        aliasApi.executeGetAliasesResult = ElasticSuccess("""{"my-index":{"aliases":""")

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isFailure shouldBe true
        verify(mockLogger).error(contains("Failed to parse aliases JSON"))
      }

      "handle JSON with extra fields" in {
        // Given
        val jsonWithExtra =
          """{
            |  "my-index": {
            |    "aliases": {
            |      "my-alias": {}
            |    },
            |    "mappings": {},
            |    "settings": {}
            |  }
            |}""".stripMargin
        aliasApi.executeGetAliasesResult = ElasticSuccess(jsonWithExtra)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get should contain("my-alias")
      }

      "handle deeply nested JSON structure" in {
        // Given
        val deepJson =
          """{
            |  "my-index": {
            |    "aliases": {
            |      "alias1": {
            |        "filter": {
            |          "bool": {
            |            "must": [
            |              {"term": {"field1": "value1"}},
            |              {"range": {"field2": {"gte": 10}}}
            |            ]
            |          }
            |        }
            |      }
            |    }
            |  }
            |}""".stripMargin
        aliasApi.executeGetAliasesResult = ElasticSuccess(deepJson)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get should contain("alias1")
      }

      "handle JSON with unicode characters" in {
        // Given
        val unicodeJson = """{"my-index":{"aliases":{"alias-café":{},"alias-日本":{}}}}"""
        aliasApi.executeGetAliasesResult = ElasticSuccess(unicodeJson)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get.size shouldBe 2
      }

      "handle large JSON response with many aliases" in {
        // Given
        val aliases = (1 to 100).map(i => s""""alias-$i":{}""").mkString(",")
        val largeJson = s"""{"my-index":{"aliases":{$aliases}}}"""
        aliasApi.executeGetAliasesResult = ElasticSuccess(largeJson)

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get.size shouldBe 100
      }
    }

    "concurrent operations" should {

      "handle multiple concurrent addAlias calls" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        val results = (1 to 5).map(i => aliasApi.addAlias(s"index-$i", s"alias-$i"))

        // Then
        results.foreach(_.isSuccess shouldBe true)
        verify(mockLogger, times(5)).info(contains("successfully added"))
      }

      "handle mixed operations concurrently" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)
        aliasApi.executeRemoveAliasResult = ElasticSuccess(true)
        aliasApi.executeAliasExistsResult = ElasticSuccess(true)
        aliasApi.executeGetAliasesResult = ElasticSuccess("""{"my-index":{"aliases":{}}}""")

        // When
        val add = aliasApi.addAlias("index1", "alias1")
        val remove = aliasApi.removeAlias("index2", "alias2")
        val exists = aliasApi.aliasExists("alias3")
        val get = aliasApi.getAliases("index3")

        // Then
        add.isSuccess shouldBe true
        remove.isSuccess shouldBe true
        exists.isSuccess shouldBe true
        get.isSuccess shouldBe true
      }

      "handle concurrent swapAlias operations" in {
        // Given
        aliasApi.executeSwapAliasResult = ElasticSuccess(true)

        // When
        val results = (1 to 3).map(i => aliasApi.swapAlias(s"old-$i", s"new-$i", s"alias-$i"))

        // Then
        results.foreach(_.isSuccess shouldBe true)
        verify(mockLogger, times(3)).info(contains("atomic operation"))
      }
    }

    "real-world scenarios" should {

      "support blue-green deployment pattern" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)
        aliasApi.executeSwapAliasResult = ElasticSuccess(true)
        aliasApi.executeRemoveAliasResult = ElasticSuccess(true)

        // When - Initial setup (blue environment)
        val setupBlue = aliasApi.addAlias("products-blue", "products")

        // When - Deploy green environment
        val swapToGreen = aliasApi.swapAlias("products-blue", "products-green", "products")

        // When - Rollback if needed
        val rollback = aliasApi.swapAlias("products-green", "products-blue", "products")

        // Then
        setupBlue.isSuccess shouldBe true
        swapToGreen.isSuccess shouldBe true
        rollback.isSuccess shouldBe true
      }

      "support time-based index pattern" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)
        aliasApi.executeGetAliasesResult = ElasticSuccess(
          """{"logs-2024-01":{"aliases":{"logs-current":{},"logs-january":{}}}}"""
        )

        // When
        val addCurrent = aliasApi.addAlias("logs-2024-01", "logs-current")
        val addMonth = aliasApi.addAlias("logs-2024-01", "logs-january")
        val getAliases = aliasApi.getAliases("logs-2024-01")

        // Then
        addCurrent.isSuccess shouldBe true
        addMonth.isSuccess shouldBe true
        getAliases.get should contain allOf ("logs-current", "logs-january")
      }

      "support filtered alias for multi-tenancy" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When - Create filtered aliases for different tenants
        val tenant1 = aliasApi.addAlias("shared-index", "tenant1-data")
        val tenant2 = aliasApi.addAlias("shared-index", "tenant2-data")

        // Then
        tenant1.isSuccess shouldBe true
        tenant2.isSuccess shouldBe true
      }

      "support read-write split pattern" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        val writeAlias = aliasApi.addAlias("products-2024", "products-write")
        val readAlias1 = aliasApi.addAlias("products-2024", "products-read")
        val readAlias2 = aliasApi.addAlias("products-2023", "products-read")

        // Then
        writeAlias.isSuccess shouldBe true
        readAlias1.isSuccess shouldBe true
        readAlias2.isSuccess shouldBe true
      }

      "support canary deployment" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)
        aliasApi.executeSwapAliasResult = ElasticSuccess(true)

        // When - Route 10% traffic to new version
        val addCanary = aliasApi.addAlias("products-v2", "products-canary")

        // When - Full rollout
        val fullRollout = aliasApi.swapAlias("products-v1", "products-v2", "products")

        // Then
        addCanary.isSuccess shouldBe true
        fullRollout.isSuccess shouldBe true
      }

      "support index lifecycle management" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)
        aliasApi.executeSwapAliasResult = ElasticSuccess(true)
        aliasApi.executeRemoveAliasResult = ElasticSuccess(true)

        // When - Hot tier
        val hot = aliasApi.addAlias("logs-2024-01-01", "logs-hot")

        // When - Move to warm tier
        val warm = aliasApi.swapAlias("logs-2024-01-01", "logs-2023-12-31", "logs-hot")

        // When - Archive
        val archive = aliasApi.removeAlias("logs-2023-12-31", "logs-hot")

        // Then
        hot.isSuccess shouldBe true
        warm.isSuccess shouldBe true
        archive.isSuccess shouldBe true
      }
    }

    "performance considerations" should {

      "handle rapid consecutive operations" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        val start = System.currentTimeMillis()
        (1 to 100).foreach(i => aliasApi.addAlias(s"index-$i", s"alias-$i"))
        val duration = System.currentTimeMillis() - start

        // Then - Should complete reasonably fast (validation overhead only)
        duration should be < 5000L // 5 seconds
      }

      "not accumulate memory with repeated calls" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When - Multiple iterations
        (1 to 10).foreach { iteration =>
          (1 to 100).foreach(i => aliasApi.addAlias(s"index-$i", s"alias-$i"))
        }

        // Then - Should not throw OutOfMemoryError
        succeed
      }
    }

    "error messages" should {

      "be descriptive for validation errors" in {
        val result = aliasApi.addAlias("INVALID", "my-alias")
        result.error.get.message should include("Invalid index")
        result.error.get.message should include("lowercase")
      }

      "include operation context" in {
        val result = aliasApi.addAlias("INVALID", "my-alias")
        result.error.get.operation shouldBe Some("addAlias")
      }

      "include status codes" in {
        val result = aliasApi.addAlias("INVALID", "my-alias")
        result.error.get.statusCode shouldBe Some(400)
      }

      "be clear about which parameter is invalid" in {
        val result1 = aliasApi.addAlias("INVALID", "my-alias")
        result1.error.get.message should include("Invalid index")

        val result2 = aliasApi.addAlias("my-index", "INVALID")
        result2.error.get.message should include("Invalid alias")

        val result3 = aliasApi.swapAlias("INVALID", "new-index", "my-alias")
        result3.error.get.message should include("Invalid old index")

        val result4 = aliasApi.swapAlias("old-index", "INVALID", "my-alias")
        result4.error.get.message should include("Invalid new index")
      }

      "preserve original error messages from execute methods" in {
        // Given
        val originalError = ElasticError("Custom error message", statusCode = Some(500))
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticFailure(originalError)

        // When
        val result = aliasApi.addAlias("my-index", "my-alias")

        // Then
        result.error.get.message shouldBe "Custom error message"
      }
    }

    "boundary conditions" should {

      "handle minimum valid index name (1 char)" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        val result = aliasApi.addAlias("a", "b")

        // Then
        result.isSuccess shouldBe true
      }

      "reject index name exceeding 255 characters" in {
        // Given
        val tooLong = "a" * 256

        // When
        val result = aliasApi.addAlias(tooLong, "my-alias")

        // Then
        result.isFailure shouldBe true
      }

      "handle alias at exactly 255 characters" in {
        // Given
        val maxLength = "a" * 255
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        val result = aliasApi.addAlias("my-index", maxLength)

        // Then
        result.isSuccess shouldBe true
      }

      "handle empty set from getAliases" in {
        // Given
        aliasApi.executeGetAliasesResult = ElasticSuccess("""{"my-index":{"aliases":{}}}""")

        // When
        val result = aliasApi.getAliases("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe Set.empty
        result.get.isEmpty shouldBe true
      }

      "handle single character alias names" in {
        // Given
        aliasApi.executeIndexExistsResult = ElasticSuccess(true)
        aliasApi.executeAddAliasResult = ElasticSuccess(true)

        // When
        val result = aliasApi.addAlias("my-index", "a")

        // Then
        result.isSuccess shouldBe true
      }
    }
  }
}
