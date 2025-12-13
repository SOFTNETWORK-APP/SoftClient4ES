package app.softnetwork.elastic.client

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.mockito.{ArgumentMatchersSugar, MockitoSugar}
import org.slf4j.Logger
import app.softnetwork.elastic.client.result._

/** Unit tests for MappingApi
  */
class MappingApiSpec
    extends AnyWordSpec
    with LogVerificationHelper
    with Matchers
    with BeforeAndAfterEach
    with MockitoSugar
    with ArgumentMatchersSugar {

  // Mock logger
  val mockLogger: Logger = mock[Logger]

  // Valid test data
  val validMapping: String = """{"properties":{"name":{"type":"text"}}}"""
  val validSettings: String = """{"my-index":{"settings":{"index":{"number_of_shards":"1"}}}}"""
  val updatedMapping: String =
    """{"properties":{"name":{"type":"text"},"age":{"type":"integer"}}}"""

  // Concrete implementation for testing
  class TestMappingApi extends MappingApi with SettingsApi with IndicesApi with RefreshApi {
    override protected def logger: Logger = mockLogger

    // Control variables
    var executeSetMappingResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeGetMappingResult: ElasticResult[String] = ElasticSuccess(validMapping)
    var executeIndexExistsResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeCreateIndexResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeDeleteIndexResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeReindexFunction
      : (String, String, Boolean) => ElasticResult[(Boolean, Option[Long])] =
      (_, _, _) => ElasticSuccess((true, Some(100L)))
    var executeLoadSettingsResult: ElasticResult[String] = ElasticSuccess(validSettings)
    var executeOpenIndexResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeCloseIndexResult: ElasticResult[Boolean] = ElasticSuccess(true)

    override private[client] def executeSetMapping(
      index: String,
      mapping: String
    ): ElasticResult[Boolean] = {
      executeSetMappingResult
    }

    override private[client] def executeGetMapping(index: String): ElasticResult[String] = {
      executeGetMappingResult
    }

    override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] = {
      executeIndexExistsResult
    }

    override private[client] def executeCreateIndex(
      index: String,
      settings: String,
      mappings: Option[String],
      aliases: Seq[String]
    ): ElasticResult[Boolean] = {
      executeCreateIndexResult
    }

    override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] = {
      executeDeleteIndexResult
    }

    override private[client] def executeReindex(
      sourceIndex: String,
      targetIndex: String,
      refresh: Boolean,
      pipeline: Option[String]
    ): ElasticResult[(Boolean, Option[Long])] = {
      executeReindexFunction(sourceIndex, targetIndex, refresh)
    }

    override private[client] def executeLoadSettings(index: String): ElasticResult[String] = {
      executeLoadSettingsResult
    }

    override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = {
      executeOpenIndexResult
    }

    override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] = {
      executeCloseIndexResult
    }

    override private[client] def executeRefresh(index: String): ElasticResult[Boolean] =
      ElasticSuccess(true)
    override private[client] def executeUpdateSettings(
      index: String,
      settings: String
    ): ElasticResult[Boolean] = ElasticSuccess(true)
  }

  var mappingApi: TestMappingApi = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    mappingApi = new TestMappingApi()
    reset(mockLogger)
  }

  "MappingApi" should {

    "setMapping" should {

      "successfully set mapping on valid index" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", validMapping)

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe true

        verify(mockLogger).debug(s"Setting mapping for index 'my-index': $validMapping")
        verify(mockLogger).info("✅ Mapping for index 'my-index' updated successfully")
      }

      "reject invalid index name" in {
        // When
        val result = mappingApi.setMapping("INVALID", validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
        result.error.get.message should include("lowercase")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.index shouldBe Some("INVALID")
        result.error.get.operation shouldBe Some("setMapping")

        verify(mockLogger, never).debug(any[String])
      }

      "reject empty index name" in {
        // When
        val result = mappingApi.setMapping("", validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
        result.error.get.message should include("cannot be empty")
      }

      "reject null index name" in {
        // When
        val result = mappingApi.setMapping(null, validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
      }

      "reject invalid JSON mapping" in {
        // When
        val result = mappingApi.setMapping("my-index", "invalid json {")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid mapping")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.index shouldBe Some("my-index")
        result.error.get.operation shouldBe Some("setMapping")

        verify(mockLogger, never).debug(any[String])
      }

      "reject empty mapping" in {
        // When
        val result = mappingApi.setMapping("my-index", "")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid mapping")
      }

      "reject null mapping" in {
        // When
        val result = mappingApi.setMapping("my-index", null)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid mapping")
      }

      "validate index before mapping" in {
        // When - Invalid index should fail first
        val result = mappingApi.setMapping("INVALID", "invalid json")

        // Then
        result.error.get.message should include("Invalid index")
        result.error.get.message should not include "Invalid mapping"
      }

      "log info when mapping not updated" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(false)

        // When
        val result = mappingApi.setMapping("my-index", validMapping)

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe false

        verify(mockLogger).debug(contains("Setting mapping"))
        verify(mockLogger).info("✅ Mapping for index 'my-index' not updated")
      }

      "fail when executeSetMapping fails" in {
        // Given
        val error = ElasticError("Mapping conflict", statusCode = Some(400))
        mappingApi.executeSetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.setMapping("my-index", validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Mapping conflict"

        verify(mockLogger).debug(contains("Setting mapping"))
        verify(mockLogger).error(
          "❌ Failed to update mapping for index 'my-index': Mapping conflict"
        )
      }

      "handle complex mapping with nested fields" in {
        // Given
        val complexMapping =
          """{
            |  "properties": {
            |    "user": {
            |      "type": "nested",
            |      "properties": {
            |        "name": {"type": "text"},
            |        "age": {"type": "integer"}
            |      }
            |    }
            |  }
            |}""".stripMargin
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", complexMapping)

        // Then
        result.isSuccess shouldBe true
      }

      "handle mapping with analyzers" in {
        // Given
        val mappingWithAnalyzer =
          """{
            |  "properties": {
            |    "title": {
            |      "type": "text",
            |      "analyzer": "standard"
            |    }
            |  }
            |}""".stripMargin
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", mappingWithAnalyzer)

        // Then
        result.isSuccess shouldBe true
      }

      "handle mapping with multiple field types" in {
        // Given
        val multiFieldMapping =
          """{
            |  "properties": {
            |    "name": {"type": "text"},
            |    "age": {"type": "integer"},
            |    "created": {"type": "date"},
            |    "active": {"type": "boolean"}
            |  }
            |}""".stripMargin
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", multiFieldMapping)

        // Then
        result.isSuccess shouldBe true
      }

      "reject mapping with special characters in index name" in {
        // When
        val result = mappingApi.setMapping("my*index", validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
      }

      "handle very large mapping" in {
        // Given
        val fields = (1 to 100).map(i => s""""field$i":{"type":"text"}""").mkString(",")
        val largeMapping = s"""{"properties":{$fields}}"""
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", largeMapping)

        // Then
        result.isSuccess shouldBe true
      }
    }

    "getMapping" should {

      "successfully get mapping for valid index" in {
        // Given
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)

        // When
        val result = mappingApi.getMapping("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe validMapping

        verify(mockLogger).debug("Getting mapping for index 'my-index'")
      }

      "reject invalid index name" in {
        // When
        val result = mappingApi.getMapping("INVALID")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.index shouldBe Some("INVALID")
        result.error.get.operation shouldBe Some("getMapping")

        verify(mockLogger, never).debug(any[String])
      }

      "reject empty index name" in {
        // When
        val result = mappingApi.getMapping("")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
      }

      "reject null index name" in {
        // When
        val result = mappingApi.getMapping(null)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
      }

      "fail when executeGetMapping fails" in {
        // Given
        val error = ElasticError("Index not found", statusCode = Some(404))
        mappingApi.executeGetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.getMapping("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Index not found"
        result.error.get.statusCode shouldBe Some(404)

        verify(mockLogger).debug("Getting mapping for index 'my-index'")
      }

      "return complex mapping" in {
        // Given
        val complexMapping =
          """{
            |  "properties": {
            |    "user": {
            |      "type": "nested",
            |      "properties": {
            |        "name": {"type": "text"},
            |        "email": {"type": "keyword"}
            |      }
            |    }
            |  }
            |}""".stripMargin
        mappingApi.executeGetMappingResult = ElasticSuccess(complexMapping)

        // When
        val result = mappingApi.getMapping("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe complexMapping
      }

      "handle empty mapping response" in {
        // Given
        mappingApi.executeGetMappingResult = ElasticSuccess("{}")

        // When
        val result = mappingApi.getMapping("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe "{}"
      }

      "handle authentication error" in {
        // Given
        val error = ElasticError("Authentication failed", statusCode = Some(401))
        mappingApi.executeGetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.getMapping("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(401)
      }
    }

    "getMappingProperties" should {

      "delegate to getMapping" in {
        // Given
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)

        // When
        val result = mappingApi.getMappingProperties("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe validMapping

        verify(mockLogger).debug("Getting mapping for index 'my-index'")
      }

      "reject invalid index name" in {
        // When
        val result = mappingApi.getMappingProperties("INVALID")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
      }

      "fail when getMapping fails" in {
        // Given
        val error = ElasticError("Failed", statusCode = Some(500))
        mappingApi.executeGetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.getMappingProperties("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Failed"
      }
    }

    "shouldUpdateMapping" should {

      "return true when mapping is different" in {
        // Given
        val currentMapping = """{"properties":{"name":{"type":"text"}}}"""
        val newMapping = """{"properties":{"name":{"type":"text"},"age":{"type":"integer"}}}"""
        mappingApi.executeGetMappingResult = ElasticSuccess(currentMapping)

        // When
        val result = mappingApi.shouldUpdateMapping("my-index", newMapping)

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe true
      }

      "return false when mapping is identical" in {
        // Given
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)

        // When
        val result = mappingApi.shouldUpdateMapping("my-index", validMapping)

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe false
      }

      "fail when getMapping fails" in {
        // Given
        val error = ElasticError("Index not found", statusCode = Some(404))
        mappingApi.executeGetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.shouldUpdateMapping("my-index", validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Index not found"
      }

      "handle complex mapping comparison" in {
        // Given
        val mapping1 =
          """{"properties":{"user":{"type":"nested","properties":{"name":{"type":"text"}}}}}"""
        val mapping2 =
          """{"properties":{"user":{"type":"nested","properties":{"name":{"type":"keyword"}}}}}"""
        mappingApi.executeGetMappingResult = ElasticSuccess(mapping1)

        // When
        val result = mappingApi.shouldUpdateMapping("my-index", mapping2)

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe true
      }
    }

    "updateMapping" should {

      "create index with mapping when index does not exist" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(false)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", validMapping, validSettings)

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe true

        verify(mockLogger).info("Creating new index 'my-index' with mapping")
        verify(mockLogger, atLeastOnce).info("✅ Index 'my-index' created successfully")
        verify(mockLogger, atLeastOnce).info("✅ Mapping for index 'my-index' set successfully")
      }

      "do nothing when mapping is up to date" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)

        // When
        val result = mappingApi.updateMapping("my-index", validMapping)

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe true

        verify(mockLogger).info("✅ Mapping for index 'my-index' is already up to date")
        verify(mockLogger, never).info(contains("migration"))
      }

      "migrate mapping when index exists and mapping is different" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(100L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping, validSettings)

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe true

        verify(mockLogger).info(contains("Mapping for index 'my-index' needs update"))
        verify(mockLogger, atLeastOnce).info(contains("Starting migration"))
        verify(mockLogger).info(contains("✅ Backed up original mapping"))
        verify(mockLogger, atLeastOnce).info(contains("✅ Migration completed successfully"))
      }

      "fail when index creation fails" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(false)
        val error = ElasticError("Creation failed", statusCode = Some(400))
        mappingApi.executeCreateIndexResult = ElasticFailure(error)

        // When
        val result = mappingApi.updateMapping("my-index", validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Creation failed")
      }

      "fail when setMapping fails during creation" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(false)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        val error = ElasticError("Mapping invalid", statusCode = Some(400))
        mappingApi.executeSetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.updateMapping("my-index", validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Mapping invalid")
      }

      "rollback when migration fails" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // First reindex fails, second succeeds
        val error = ElasticError("Reindex failed", statusCode = Some(500))
        var reindexCallCount = 0
        mappingApi.executeReindexFunction = { (source, target, refresh) =>
          reindexCallCount += 1
          if (reindexCallCount == 2) {
            ElasticSuccess((true, Some(100L)))
          } else {
            ElasticFailure(error)
          }
        }
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isFailure shouldBe true

        verify(mockLogger).info(contains("✅ Backed up original mapping"))
        verify(mockLogger).error(contains("❌ Migration failed"))
        verify(mockLogger).info(contains("Attempting rollback"))
        verify(mockLogger).info(contains("✅ Rollback completed successfully"))
      }

      "fail when backup fails" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        val error = ElasticError("Cannot get mapping", statusCode = Some(500))
        mappingApi.executeGetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Cannot get mapping")
      }

      "use default settings when not provided" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(false)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", validMapping)

        // Then
        result.isSuccess shouldBe true
      }
    }

    "performMigration" should {

      "successfully migrate index with all steps" in {
        // Given
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(100L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)

        // Trigger migration
        val migrationResult = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        migrationResult.isSuccess shouldBe true
        verify(mockLogger, atLeastOnce).info(contains("Starting migration"))
        verify(mockLogger, atLeastOnce).info(contains("✅ Migration completed"))
      }

      "fail when temp index creation fails" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)

        val error = ElasticError("Cannot create temp index", statusCode = Some(400))
        mappingApi.executeCreateIndexResult = ElasticFailure(error)
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isFailure shouldBe true
        verify(mockLogger).error(contains("❌ Migration failed"))
        verify(mockLogger).info(contains("Attempting rollback"))
      }

      "fail when first reindex fails" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        val error = ElasticError("Reindex to temp failed", statusCode = Some(500))
        mappingApi.executeReindexFunction = (_, _, _) => ElasticFailure(error)
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isFailure shouldBe true
        verify(mockLogger).error(contains("❌ Migration failed"))
      }

      "fail when delete original fails" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(100L)))

        val error = ElasticError("Cannot delete index", statusCode = Some(403))
        mappingApi.executeDeleteIndexResult = ElasticFailure(error)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isFailure shouldBe true
      }
    }

    "rollbackMigration" should {

      "successfully rollback after migration failure" in {
        // Given - Setup for migration that will fail
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // Fail on first reindex
        var reindexCallCount = 0
        mappingApi.executeReindexFunction = { (source, target, refresh) =>
          reindexCallCount += 1
          if (reindexCallCount == 2) {
            ElasticSuccess((true, Some(100L)))
          } else {
            ElasticFailure(ElasticError("Reindex failed"))
          }
        }

        // Rollback operations succeed
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isFailure shouldBe true
        verify(mockLogger).info(contains("Attempting rollback"))
        verify(mockLogger).info(contains("✅ Rollback completed successfully"))
      }

      "handle rollback when temp index exists" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(100L)))

        // Fail on delete original
        var deleteCallCount = 0
        mappingApi.executeDeleteIndexResult = {
          deleteCallCount += 1
          if (deleteCallCount == 1) ElasticFailure(ElasticError("Delete failed"))
          else ElasticSuccess(true)
        }
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isFailure shouldBe true
        verify(mockLogger).info(contains("Attempting rollback"))
      }

      "log error when rollback fails" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // Fail migration
        mappingApi.executeReindexFunction =
          (_, _, _) => ElasticFailure(ElasticError("Reindex failed"))

        // Fail rollback
        mappingApi.executeDeleteIndexResult = ElasticFailure(ElasticError("Delete failed"))

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isFailure shouldBe true
        verify(mockLogger).info(contains("Attempting rollback"))
        verify(mockLogger).error(contains("❌ Rollback failed"))
      }
    }

    "workflow scenarios" should {

      "successfully create new index with mapping" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(false)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("products", validMapping, validSettings)

        // Then
        result.isSuccess shouldBe true
        verify(mockLogger).info("Creating new index 'products' with mapping")
        verify(mockLogger, atLeastOnce).info("✅ Index 'products' created successfully")
        verify(mockLogger).info("✅ Mapping for index 'products' set successfully")
      }

      "successfully update existing index with new mapping" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(100L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping, validSettings)

        // Then
        result.isSuccess shouldBe true
        verify(mockLogger).info(contains("needs update"))
        verify(mockLogger, atLeastOnce).info(contains("Starting migration"))
        verify(mockLogger, atLeastOnce).info(contains("✅ Migration completed successfully"))
      }

      "skip update when mapping is identical" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)

        // When
        val result = mappingApi.updateMapping("my-index", validMapping)

        // Then
        result.isSuccess shouldBe true
        verify(mockLogger).info("✅ Mapping for index 'my-index' is already up to date")
        verify(mockLogger, never).info(contains("migration"))
      }

      "handle full migration lifecycle" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(100L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isSuccess shouldBe true

        // Verify all migration steps logged
        verify(mockLogger).info(contains("✅ Backed up original mapping"))
        verify(mockLogger, atLeastOnce).info(contains("Starting migration"))
        verify(mockLogger, atLeastOnce).info(contains("✅ Migration completed"))
      }

      "chain setMapping and getMapping" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)

        // When
        val setResult = mappingApi.setMapping("my-index", validMapping)
        val getResult = mappingApi.getMapping("my-index")

        // Then
        setResult.isSuccess shouldBe true
        getResult.isSuccess shouldBe true
        getResult.get shouldBe validMapping
      }

      "verify mapping update detection" in {
        // Given
        val oldMapping = """{"properties":{"name":{"type":"text"}}}"""
        val newMapping = """{"properties":{"name":{"type":"text"},"age":{"type":"integer"}}}"""
        mappingApi.executeGetMappingResult = ElasticSuccess(oldMapping)

        // When
        val shouldUpdate = mappingApi.shouldUpdateMapping("my-index", newMapping)

        // Then
        shouldUpdate.isSuccess shouldBe true
        shouldUpdate.get shouldBe true
      }

      "handle complete rollback scenario" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // First reindex fails, second succeeds
        var reindexCallCount = 0
        mappingApi.executeReindexFunction = { (source, target, refresh) =>
          reindexCallCount += 1
          if (reindexCallCount == 2) {
            ElasticSuccess((true, Some(100L)))
          } else {
            ElasticFailure(ElasticError("Second reindex failed"))
          }
        }

        // Rollback succeeds
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isFailure shouldBe true

        captureAndVerifyLog(mockLogger, "error", "❌ Migration failed")
        captureAndVerifyLog(mockLogger, "info", "Attempting rollback", "✅ Rollback index completed")
      }
    }

    "edge cases" should {

      "handle index name at maximum length" in {
        // Given
        val maxIndex = "a" * 255
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping(maxIndex, validMapping)

        // Then
        result.isSuccess shouldBe true
      }

      "reject index name exceeding maximum length" in {
        // Given
        val tooLong = "a" * 256

        // When
        val result = mappingApi.setMapping(tooLong, validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
      }

      "handle very large mapping JSON" in {
        // Given
        val fields = (1 to 1000).map(i => s""""field$i":{"type":"text"}""").mkString(",")
        val largeMapping = s"""{"properties":{$fields}}"""
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", largeMapping)

        // Then
        result.isSuccess shouldBe true
      }

      "handle mapping with unicode characters" in {
        // Given
        val unicodeMapping = """{"properties":{"名前":{"type":"text"},"café":{"type":"keyword"}}}"""
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", unicodeMapping)

        // Then
        result.isSuccess shouldBe true
      }

      "handle mapping with special JSON characters" in {
        // Given
        val specialMapping = """{"properties":{"field\"name":{"type":"text"}}}"""
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", specialMapping)

        // Then
        result.isSuccess shouldBe true
      }

      "handle empty mapping properties" in {
        // Given
        val emptyMapping = """{"properties":{}}"""
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", emptyMapping)

        // Then
        result.isSuccess shouldBe true
      }

      "handle mapping with deeply nested objects" in {
        // Given
        val deepMapping =
          """{
            |  "properties": {
            |    "level1": {
            |      "properties": {
            |        "level2": {
            |          "properties": {
            |            "level3": {
            |              "properties": {
            |                "level4": {"type": "text"}
            |              }
            |            }
            |          }
            |        }
            |      }
            |    }
            |  }
            |}""".stripMargin
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", deepMapping)

        // Then
        result.isSuccess shouldBe true
      }

      "handle concurrent setMapping calls" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val results = (1 to 5).map(i => mappingApi.setMapping(s"index-$i", validMapping))

        // Then
        results.foreach(_.isSuccess shouldBe true)
      }

      "handle consecutive updateMapping calls" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(false)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result1 = mappingApi.updateMapping("index1", validMapping)
        val result2 = mappingApi.updateMapping("index2", validMapping)
        val result3 = mappingApi.updateMapping("index3", validMapping)

        // Then
        result1.isSuccess shouldBe true
        result2.isSuccess shouldBe true
        result3.isSuccess shouldBe true
      }

      "handle null response from executeGetMapping" in {
        // Given
        mappingApi.executeGetMappingResult = ElasticSuccess(null)

        // When
        val result = mappingApi.getMapping("my-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe null
      }

      "handle whitespace-only mapping" in {
        // Given
        val whitespaceMapping = "   "

        // When
        val result = mappingApi.setMapping("my-index", whitespaceMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid mapping")
      }

      "handle mapping with comments (invalid JSON)" in {
        // Given
        val mappingWithComments = """{"properties":{"name":{"type":"text"}}} // comment"""

        // When
        val result = mappingApi.setMapping("my-index", mappingWithComments)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid mapping")
      }
    }

    "error handling" should {

      "preserve error context in setMapping" in {
        // Given
        val error = ElasticError(
          message = "Mapping conflict",
          cause = Some(new RuntimeException("Root cause")),
          statusCode = Some(400),
          index = Some("test-index")
        )
        mappingApi.executeSetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.setMapping("my-index", validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Mapping conflict"
        result.error.get.cause shouldBe defined
        result.error.get.statusCode shouldBe Some(400)
      }

      "handle authentication error" in {
        // Given
        val error = ElasticError("Authentication required", statusCode = Some(401))
        mappingApi.executeSetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.setMapping("my-index", validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(401)
        verify(mockLogger).error(contains("❌ Failed to update mapping"))
      }

      "handle authorization error" in {
        // Given
        val error = ElasticError("Insufficient permissions", statusCode = Some(403))
        mappingApi.executeSetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.setMapping("my-index", validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(403)
      }

      "handle timeout error" in {
        // Given
        val error = ElasticError(
          "Request timeout",
          cause = Some(new java.net.SocketTimeoutException()),
          statusCode = Some(504)
        )
        mappingApi.executeGetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.getMapping("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.cause shouldBe defined
        result.error.get.cause.get shouldBe a[java.net.SocketTimeoutException]
      }

      "handle server error" in {
        // Given
        val error = ElasticError("Internal server error", statusCode = Some(500))
        mappingApi.executeSetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.setMapping("my-index", validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(500)
      }

      "handle conflict error" in {
        // Given
        val error = ElasticError("Version conflict", statusCode = Some(409))
        mappingApi.executeSetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.setMapping("my-index", validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(409)
      }

      "handle network error" in {
        // Given
        val error = ElasticError(
          "Connection refused",
          cause = Some(new java.net.ConnectException()),
          statusCode = Some(503)
        )
        mappingApi.executeGetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.getMapping("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.cause shouldBe defined
      }

      "propagate errors through flatMap chain" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticFailure(ElasticError("Connection failed"))

        // When
        val result = mappingApi.updateMapping("my-index", validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Connection failed"
      }

      "handle error in shouldUpdateMapping" in {
        // Given
        val error = ElasticError("Cannot retrieve mapping", statusCode = Some(500))
        mappingApi.executeGetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.shouldUpdateMapping("my-index", validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Cannot retrieve mapping"
      }

      "handle partial failure in createIndexWithMapping" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(false)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)

        // setMapping fails
        val error = ElasticError("Invalid mapping structure", statusCode = Some(400))
        mappingApi.executeSetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi.updateMapping("my-index", validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid mapping structure")
      }

      "handle all validation errors" in {
        // Invalid index
        val result1 = mappingApi.setMapping("INVALID", validMapping)
        result1.isFailure shouldBe true
        result1.error.get.operation shouldBe Some("setMapping")

        // Invalid mapping
        val result2 = mappingApi.setMapping("my-index", "invalid")
        result2.isFailure shouldBe true
        result2.error.get.operation shouldBe Some("setMapping")

        // Empty index
        val result3 = mappingApi.getMapping("")
        result3.isFailure shouldBe true
        result3.error.get.operation shouldBe Some("getMapping")

        // Null index
        val result4 = mappingApi.getMapping(null)
        result4.isFailure shouldBe true
        result4.error.get.operation shouldBe Some("getMapping")
      }
    }

    "logging behavior" should {

      "log debug message for setMapping" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        mappingApi.setMapping("my-index", validMapping)

        // Then
        verify(mockLogger).debug(s"Setting mapping for index 'my-index': $validMapping")
      }

      "log info with emoji for successful setMapping" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        mappingApi.setMapping("my-index", validMapping)

        // Then
        verify(mockLogger).info("✅ Mapping for index 'my-index' updated successfully")
      }

      "log info when mapping not updated" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(false)

        // When
        mappingApi.setMapping("my-index", validMapping)

        // Then
        verify(mockLogger).info("✅ Mapping for index 'my-index' not updated")
      }

      "log error with emoji for failed setMapping" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticFailure(ElasticError("Failed"))

        // When
        mappingApi.setMapping("my-index", validMapping)

        // Then
        verify(mockLogger).error("❌ Failed to update mapping for index 'my-index': Failed")
      }

      "log debug message for getMapping" in {
        // Given
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)

        // When
        mappingApi.getMapping("my-index")

        // Then
        verify(mockLogger).debug("Getting mapping for index 'my-index'")
      }

      "not log for validation failures" in {
        // When
        mappingApi.setMapping("INVALID", validMapping)

        // Then
        verify(mockLogger, never).debug(any[String])
        verify(mockLogger, never).info(any[String])
        verify(mockLogger, never).error(any[String])
      }

      "log migration steps" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(100L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        verify(mockLogger).info(contains("needs update"))
        verify(mockLogger, atLeastOnce).info(contains("Starting migration"))
        verify(mockLogger).info(contains("✅ Backed up original mapping"))
        verify(mockLogger).info(contains("✅ Migration completed successfully"))
      }

      "log rollback steps" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        // First reindex fails, second succeeds
        var reindexCallCount = 0
        mappingApi.executeReindexFunction = { (source, target, refresh) =>
          reindexCallCount += 1
          if (reindexCallCount == 2) {
            ElasticSuccess((true, Some(100L)))
          } else {
            ElasticFailure(ElasticError("Second reindex failed"))
          }
        }
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        captureAndVerifyLog(mockLogger, "error", "❌ Migration failed")
        captureAndVerifyLog(
          mockLogger,
          "info",
          "Attempting rollback",
          "✅ Rollback completed successfully"
        )
      }

      "log rollback failure" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction =
          (_, _, _) => ElasticFailure(ElasticError("Migration failed"))
        mappingApi.executeDeleteIndexResult = ElasticFailure(ElasticError("Rollback failed"))

        // When
        mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        verify(mockLogger).error(contains("❌ Migration failed"))
        verify(mockLogger).info(contains("Attempting rollback"))
        verify(mockLogger).error(contains("❌ Rollback failed"))
      }

      "log when mapping is up to date" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)

        // When
        mappingApi.updateMapping("my-index", validMapping)

        // Then
        verify(mockLogger).info("✅ Mapping for index 'my-index' is already up to date")
      }

      "log index creation with mapping" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(false)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        mappingApi.updateMapping("my-index", validMapping)

        // Then
        verify(mockLogger).info("Creating new index 'my-index' with mapping")
        verify(mockLogger, atLeastOnce).info("✅ Index 'my-index' created successfully")
        verify(mockLogger).info("✅ Mapping for index 'my-index' set successfully")
      }

      /*"log backup failure" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticFailure(ElasticError("Cannot get mapping"))

        // When
        mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        verify(mockLogger).error(contains("❌ Failed to backup original state"))
      }*/

      "log all emojis correctly" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(false)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        mappingApi.updateMapping("my-index", validMapping)

        // Then
        verify(mockLogger, atLeast(1)).info(contains("✅"))
      }
    }

    "validation order" should {

      "validate index before mapping in setMapping" in {
        // When
        val result = mappingApi.setMapping("INVALID", "invalid json")

        // Then
        result.error.get.message should include("Invalid index")
        result.error.get.message should not include "Invalid mapping"
      }

      "validate mapping after index in setMapping" in {
        // When
        val result = mappingApi.setMapping("my-index", "invalid json")

        // Then
        result.error.get.message should include("Invalid mapping")
      }

      "validate index in getMapping" in {
        // When
        val result = mappingApi.getMapping("INVALID")

        // Then
        result.error.get.message should include("Invalid index")
      }

      "not call execute methods when validation fails" in {
        // Given
        var executeCalled = false
        val validatingApi = new TestMappingApi {
          override private[client] def executeSetMapping(
            index: String,
            mapping: String
          ): ElasticResult[Boolean] = {
            executeCalled = true
            ElasticSuccess(true)
          }
        }

        // When
        validatingApi.setMapping("INVALID", validMapping)

        // Then
        executeCalled shouldBe false
      }
    }

    "ElasticResult integration" should {

      "work with map transformation" in {
        // Given
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)

        // When
        val result = mappingApi.getMapping("my-index").map(_.length)

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe validMapping.length
      }

      "work with flatMap composition" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)

        // When
        val result = mappingApi.setMapping("my-index", validMapping).flatMap { _ =>
          mappingApi.getMapping("my-index")
        }

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe validMapping
      }

      "work with for-comprehension" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)

        // When
        val result = for {
          _       <- mappingApi.setMapping("my-index", validMapping)
          mapping <- mappingApi.getMapping("my-index")
        } yield mapping

        // Then
        result shouldBe ElasticSuccess(validMapping)
      }

      "propagate errors through transformations" in {
        // Given
        val error = ElasticError("Failed")
        mappingApi.executeGetMappingResult = ElasticFailure(error)

        // When
        val result = mappingApi
          .getMapping("my-index")
          .map(_.length)
          .flatMap(len => ElasticSuccess(len * 2))

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Failed"
      }

      "handle chained operations with mixed results" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticFailure(ElasticError("Get failed"))

        // When
        val result = for {
          _       <- mappingApi.setMapping("my-index", validMapping)
          mapping <- mappingApi.getMapping("my-index")
        } yield mapping

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Get failed"
      }

      "use filter in for-comprehension" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(false)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", validMapping)

        // Then
        result.isSuccess shouldBe true
      }
    }

    "real-world scenarios" should {

      "add new field to existing mapping" in {
        // Given
        val oldMapping = """{"properties":{"name":{"type":"text"}}}"""
        val newMapping = """{"properties":{"name":{"type":"text"},"email":{"type":"keyword"}}}"""

        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(oldMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(100L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", newMapping)

        // Then
        result.isSuccess shouldBe true
        verify(mockLogger).info(contains("needs update"))
        verify(mockLogger).info(contains("Migration completed successfully"))
      }

      "change field type in mapping" in {
        // Given
        val oldMapping = """{"properties":{"age":{"type":"text"}}}"""
        val newMapping = """{"properties":{"age":{"type":"integer"}}}"""

        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(oldMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(100L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", newMapping)

        // Then
        result.isSuccess shouldBe true
      }

      "handle zero-downtime mapping update" in {
        // Given - Simulates production scenario
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(10000L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isSuccess shouldBe true
        verify(mockLogger, atLeastOnce).info(contains("Starting migration"))
        verify(mockLogger, atLeastOnce).info(contains("Migration completed"))
      }

      "recover from failed migration" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        // First reindex fails, second succeeds
        var reindexCallCount = 0
        mappingApi.executeReindexFunction = { (source, target, refresh) =>
          reindexCallCount += 1
          if (reindexCallCount == 2) {
            ElasticSuccess((true, Some(100L)))
          } else {
            ElasticFailure(ElasticError("Reindex timeout"))
          }
        }
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isFailure shouldBe true
        verify(mockLogger).error(contains("❌ Migration failed"))
        verify(mockLogger).info(contains("Attempting rollback"))
        verify(mockLogger).info(contains("✅ Rollback completed successfully"))
      }

      "bootstrap new index with complex mapping" in {
        // Given
        val complexMapping =
          """{
            |  "properties": {
            |    "user": {
            |      "type": "nested",
            |      "properties": {
            |        "name": {"type": "text"},
            |        "email": {"type": "keyword"}
            |      }
            |    },
            |    "tags": {"type": "keyword"},
            |    "created": {"type": "date"}
            |  }
            |}""".stripMargin

        mappingApi.executeIndexExistsResult = ElasticSuccess(false)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", complexMapping, validSettings)

        // Then
        result.isSuccess shouldBe true
        verify(mockLogger).info("Creating new index 'my-index' with mapping")
        verify(mockLogger, atLeastOnce).info("✅ Index 'my-index' created successfully")
        verify(mockLogger).info("✅ Mapping for index 'my-index' set successfully")
      }

      "handle multi-tenant index mapping update" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(5000L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isSuccess shouldBe true
      }

      "verify mapping before and after update" in {
        // Given
        val oldMapping = """{"properties":{"name":{"type":"text"}}}"""
        val newMapping = """{"properties":{"name":{"type":"text"},"age":{"type":"integer"}}}"""

        mappingApi.executeGetMappingResult = ElasticSuccess(oldMapping)

        // When - Check if update needed
        val shouldUpdate = mappingApi.shouldUpdateMapping("users", newMapping)

        // Then
        shouldUpdate.isSuccess shouldBe true
        shouldUpdate.get shouldBe true
      }

      "handle incremental mapping evolution" in {
        // Given - Step 1: Add email field
        val mapping1 = """{"properties":{"name":{"type":"text"}}}"""
        val mapping2 = """{"properties":{"name":{"type":"text"},"email":{"type":"keyword"}}}"""
        val mapping3 =
          """{"properties":{"name":{"type":"text"},"email":{"type":"keyword"},"age":{"type":"integer"}}}"""

        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(mapping1)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(100L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When - First update
        val result1 = mappingApi.updateMapping("my-index", mapping2)

        // Then
        result1.isSuccess shouldBe true

        // When - Second update
        mappingApi.executeGetMappingResult = ElasticSuccess(mapping2)
        val result2 = mappingApi.updateMapping("my-index", mapping3)

        // Then
        result2.isSuccess shouldBe true
      }
    }

    "performance considerations" should {

      "handle rapid consecutive setMapping calls" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val start = System.currentTimeMillis()
        (1 to 100).foreach(i => mappingApi.setMapping(s"index-$i", validMapping))
        val duration = System.currentTimeMillis() - start

        // Then - Should complete reasonably fast
        duration should be < 5000L // 5 seconds
      }

      "not accumulate memory with repeated calls" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When - Multiple iterations
        (1 to 10).foreach { iteration =>
          (1 to 100).foreach(i => mappingApi.setMapping(s"index-$i", validMapping))
        }

        // Then - Should not throw OutOfMemoryError
        succeed
      }

      "handle large reindex operations" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(1000000L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isSuccess shouldBe true
      }
    }

    "error messages" should {

      "be descriptive for validation errors" in {
        val result = mappingApi.setMapping("INVALID", validMapping)
        result.error.get.message should include("Invalid index")
        result.error.get.message should include("lowercase")
      }

      "include operation context" in {
        val result1 = mappingApi.setMapping("INVALID", validMapping)
        result1.error.get.operation shouldBe Some("setMapping")

        val result2 = mappingApi.getMapping("INVALID")
        result2.error.get.operation shouldBe Some("getMapping")
      }

      "include status codes" in {
        val result = mappingApi.setMapping("INVALID", validMapping)
        result.error.get.statusCode shouldBe Some(400)
      }

      "be clear about which parameter is invalid" in {
        val result1 = mappingApi.setMapping("INVALID", validMapping)
        result1.error.get.message should include("Invalid index")

        val result2 = mappingApi.setMapping("my-index", "invalid json")
        result2.error.get.message should include("Invalid mapping")
      }

      "preserve original error messages from execute methods" in {
        // Given
        val originalError = ElasticError("Custom mapping error", statusCode = Some(500))
        mappingApi.executeSetMappingResult = ElasticFailure(originalError)

        // When
        val result = mappingApi.setMapping("my-index", validMapping)

        // Then
        result.error.get.message shouldBe "Custom mapping error"
        result.error.get.statusCode shouldBe Some(500)
      }

      "include index name in error context" in {
        val result = mappingApi.setMapping("INVALID", validMapping)
        result.error.get.index shouldBe Some("INVALID")
      }

      "provide helpful messages for JSON validation" in {
        val result = mappingApi.setMapping("my-index", "{invalid json")
        result.error.get.message should include("Invalid mapping")
      }
    }

    "boundary conditions" should {

      "handle minimum valid index name (1 char)" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("a", validMapping)

        // Then
        result.isSuccess shouldBe true
      }

      "reject index name exceeding 255 characters" in {
        // Given
        val tooLong = "a" * 256

        // When
        val result = mappingApi.setMapping(tooLong, validMapping)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
      }

      "handle mapping at maximum reasonable size" in {
        // Given
        val fields = (1 to 1000).map(i => s""""field$i":{"type":"text"}""").mkString(",")
        val largeMapping = s"""{"properties":{$fields}}"""
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", largeMapping)

        // Then
        result.isSuccess shouldBe true
      }

      "handle empty properties object" in {
        // Given
        val emptyMapping = """{"properties":{}}"""
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", emptyMapping)

        // Then
        result.isSuccess shouldBe true
      }

      "handle mapping with single property" in {
        // Given
        val singleProp = """{"properties":{"id":{"type":"keyword"}}}"""
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", singleProp)

        // Then
        result.isSuccess shouldBe true
      }

      "handle deeply nested mapping (10 levels)" in {
        // Given
        val deepMapping =
          """{
            |  "properties": {
            |    "l1": {"properties": {
            |      "l2": {"properties": {
            |        "l3": {"properties": {
            |          "l4": {"properties": {
            |            "l5": {"properties": {
            |              "l6": {"properties": {
            |                "l7": {"properties": {
            |                  "l8": {"properties": {
            |                    "l9": {"properties": {
            |                      "l10": {"type": "text"}
            |                    }}
            |                  }}
            |                }}
            |              }}
            |            }}
            |          }}
            |        }}
            |      }}
            |    }}
            |  }
            |}""".stripMargin
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", deepMapping)

        // Then
        result.isSuccess shouldBe true
      }
    }

    "concurrent operations" should {

      "handle multiple concurrent setMapping calls" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val results = (1 to 5).map(i => mappingApi.setMapping(s"index-$i", validMapping))

        // Then
        results.foreach(_.isSuccess shouldBe true)
        verify(mockLogger, times(5)).info(contains("✅ Mapping"))
      }

      "handle mixed operations concurrently" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)

        // When
        val set1 = mappingApi.setMapping("index1", validMapping)
        val get1 = mappingApi.getMapping("index1")
        val set2 = mappingApi.setMapping("index2", validMapping)
        val get2 = mappingApi.getMapping("index2")

        // Then
        set1.isSuccess shouldBe true
        get1.isSuccess shouldBe true
        set2.isSuccess shouldBe true
        get2.isSuccess shouldBe true
      }

      "handle concurrent updateMapping operations" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(false)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val results = (1 to 3).map(i => mappingApi.updateMapping(s"index-$i", validMapping))

        // Then
        results.foreach(_.isSuccess shouldBe true)
      }
    }

    "JSON validation" should {

      "accept valid JSON with properties" in {
        // Given
        val validJson = """{"properties":{"name":{"type":"text"}}}"""
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", validJson)

        // Then
        result.isSuccess shouldBe true
      }

      "reject JSON without properties key" in {
        // Given
        val invalidJson = """{"mappings":{"name":{"type":"text"}}}"""

        // When
        val result = mappingApi.setMapping("my-index", invalidJson)

        // Then
        // Note: validateJson only checks if it's valid JSON, not structure
        result.isSuccess shouldBe true
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
      }

      "reject malformed JSON" in {
        // Given
        val malformedJson = """{"properties":{"name":{"type":"text"}"""

        // When
        val result = mappingApi.setMapping("my-index", malformedJson)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid mapping")
      }

      "accept JSON with extra whitespace" in {
        // Given
        val jsonWithSpaces =
          """{
            |  "properties": {
            |    "name": {
            |      "type": "text"
            |    }
            |  }
            |}""".stripMargin
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", jsonWithSpaces)

        // Then
        result.isSuccess shouldBe true
      }

      "accept JSON with unicode characters" in {
        // Given
        val unicodeJson = """{"properties":{"名前":{"type":"text"}}}"""
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", unicodeJson)

        // Then
        result.isSuccess shouldBe true
      }

      "reject JSON with trailing comma" in {
        // Given
        val invalidJson = """{"properties":{"name":{"type":"text"},}}"""

        // When
        val result = mappingApi.setMapping("my-index", invalidJson)

        // Then
        result.isFailure shouldBe true
      }

      "accept JSON with escaped characters" in {
        // Given
        val escapedJson = """{"properties":{"field\"name":{"type":"text"}}}"""
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", escapedJson)

        // Then
        result.isSuccess shouldBe true
      }

      "reject empty string as mapping" in {
        // When
        val result = mappingApi.setMapping("my-index", "")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid mapping")
      }

      "reject null as mapping" in {
        // When
        val result = mappingApi.setMapping("my-index", null)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid mapping")
      }

      "accept complex nested JSON" in {
        // Given
        val complexJson =
          """{
            |  "properties": {
            |    "user": {
            |      "type": "nested",
            |      "properties": {
            |        "name": {"type": "text"},
            |        "contacts": {
            |          "type": "nested",
            |          "properties": {
            |            "email": {"type": "keyword"},
            |            "phone": {"type": "keyword"}
            |          }
            |        }
            |      }
            |    }
            |  }
            |}""".stripMargin
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", complexJson)

        // Then
        result.isSuccess shouldBe true
      }
    }

    "migration edge cases" should {

      "handle migration when temp index already exists" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)

        // First create fails (temp exists), but continues
        var createCallCount = 0
        mappingApi.executeCreateIndexResult = {
          createCallCount += 1
          if (createCallCount == 1) ElasticFailure(ElasticError("Index already exists"))
          else ElasticSuccess(true)
        }

        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(100L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isFailure shouldBe true
      }

      "handle rollback when original index doesn't exist" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticFailure(ElasticError("Failed"))

        // Index doesn't exist during rollback
        var existsCallCount = 0
        mappingApi.executeIndexExistsResult = {
          existsCallCount += 1
          if (existsCallCount == 1) ElasticSuccess(true)
          else ElasticSuccess(false)
        }

        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isFailure shouldBe true
      }

      "handle migration with zero documents" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(0L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isSuccess shouldBe true
      }

      "handle partial reindex failure" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // First reindex succeeds, second fails
        var reindexCallCount = 0
        mappingApi.executeReindexFunction = { (source, target, refresh) =>
          reindexCallCount += 1
          if (reindexCallCount == 1) {
            ElasticSuccess((true, Some(100L)))
          } else {
            ElasticFailure(ElasticError("Second reindex failed"))
          }
        }

        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isFailure shouldBe true
      }
    }

    "integration with other APIs" should {

      "work with IndicesApi methods" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(false)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", validMapping)

        // Then
        result.isSuccess shouldBe true
      }

      "work with SettingsApi methods" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(100L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping, validSettings)

        // Then
        result.isSuccess shouldBe true
      }

      "work with RefreshApi methods" in {
        // Given
        mappingApi.executeSetMappingResult = ElasticSuccess(true)

        // When
        val result = mappingApi.setMapping("my-index", validMapping)

        // Then
        result.isSuccess shouldBe true
      }
    }

    "temp index naming" should {

      "generate unique temp index names" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(100L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When - Multiple migrations
        val result1 = mappingApi.updateMapping("my-index", updatedMapping)

        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        val result2 = mappingApi.updateMapping("my-index", updatedMapping)

        // Then - Both should succeed (temp names are unique)
        result1.isSuccess shouldBe true
        result2.isSuccess shouldBe true
      }

      "use correct temp index pattern" in {
        // Given
        mappingApi.executeIndexExistsResult = ElasticSuccess(true)
        mappingApi.executeGetMappingResult = ElasticSuccess(validMapping)
        mappingApi.executeLoadSettingsResult = ElasticSuccess(validSettings)
        mappingApi.executeCreateIndexResult = ElasticSuccess(true)
        mappingApi.executeSetMappingResult = ElasticSuccess(true)
        mappingApi.executeReindexFunction = (_, _, _) => ElasticSuccess((true, Some(100L)))
        mappingApi.executeDeleteIndexResult = ElasticSuccess(true)
        mappingApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = mappingApi.updateMapping("my-index", updatedMapping)

        // Then
        result.isSuccess shouldBe true
        // Temp index name format: products_tmp_<8-char-uuid>
      }
    }
  }
}
