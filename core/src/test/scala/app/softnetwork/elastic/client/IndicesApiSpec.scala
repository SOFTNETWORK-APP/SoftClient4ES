package app.softnetwork.elastic.client

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.mockito.MockitoSugar
import org.mockito.ArgumentMatchersSugar
import org.slf4j.Logger
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql.query
import app.softnetwork.elastic.sql.schema.TableAlias

/** Unit tests for IndicesApi
  */
class IndicesApiSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterEach
    with MockitoSugar
    with ArgumentMatchersSugar {

  // Mock logger
  val mockLogger: Logger = mock[Logger]

  // Concrete implementation for testing
  class TestIndicesApi
      extends IndicesApi
      with RefreshApi
      with PipelineApi
      with VersionApi
      with SerializationApi {
    override protected def logger: Logger = mockLogger

    // Control variables for each operation
    var executeCreateIndexResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeGetIndexResult: ElasticResult[Option[String]] = ElasticSuccess(None)
    var executeDeleteIndexResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeCloseIndexResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeOpenIndexResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeReindexResult: ElasticResult[(Boolean, Option[Long])] = ElasticSuccess(
      (true, Some(100L))
    )
    var executeIndexExistsResult: ElasticResult[Boolean] = ElasticSuccess(true)
    var executeRefreshResult: ElasticResult[Boolean] = ElasticSuccess(true)

    override private[client] def executeCreateIndex(
      index: String,
      settings: String,
      mappings: Option[String],
      aliases: Seq[TableAlias]
    ): ElasticResult[Boolean] = {
      executeCreateIndexResult
    }

    override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] = {
      executeGetIndexResult
    }

    override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] = {
      executeDeleteIndexResult
    }

    override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] = {
      executeCloseIndexResult
    }

    override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = {
      executeOpenIndexResult
    }

    override private[client] def executeReindex(
      sourceIndex: String,
      targetIndex: String,
      refresh: Boolean,
      pipeline: Option[String]
    ): ElasticResult[(Boolean, Option[Long])] = {
      executeReindexResult
    }

    override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] = {
      executeIndexExistsResult
    }

    override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = {
      executeRefreshResult
    }

    override private[client] def executeCreatePipeline(
      pipelineName: String,
      pipelineDefinition: String
    ): ElasticResult[Boolean] = ???

    override private[client] def executeDeletePipeline(
      pipelineName: String,
      ifExists: Boolean
    ): ElasticResult[Boolean] = ???

    override private[client] def executeGetPipeline(
      pipelineName: String
    ): ElasticResult[Option[String]] = ???

    override private[client] def executeVersion(): ElasticResult[String] = ???

    /** Implicit conversion of an SQL query to Elasticsearch JSON. Used for query serialization.
      *
      * @param sqlSearch
      *   the SQL search request to convert
      * @return
      *   JSON string representation of the query
      */
    override private[client] implicit def sqlSearchRequestToJsonQuery(
      sqlSearch: query.SingleSearch
    )(implicit timestamp: Long): String = ???

    override private[client] def executeDeleteByQuery(
      index: String,
      query: String,
      refresh: Boolean
    ): ElasticResult[Long] = ???

    override private[client] def executeIsIndexClosed(index: String): ElasticResult[Boolean] = ???

    override private[client] def executeUpdateByQuery(
      index: String,
      query: String,
      pipelineId: Option[String],
      refresh: Boolean
    ): ElasticResult[Long] = ???
  }

  var indicesApi: TestIndicesApi = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    indicesApi = new TestIndicesApi()
    reset(mockLogger)
  }

  "IndicesApi" should {

    "defaultSettings" should {

      "contain valid JSON" in {
        // When
        val settings = indicesApi.defaultSettings

        // Then
        settings should not be empty
        settings should include("index")
        settings should include("max_ngram_diff")
        settings should include("ngram_analyzer")
        settings should include("ngram_tokenizer")
      }

      "be valid JSON parseable" in {
        // Given
        import org.json4s.jackson.JsonMethods._

        // When & Then
        noException should be thrownBy {
          parse(indicesApi.defaultSettings)
        }
      }

      "contain ngram configuration" in {
        // When
        val settings = indicesApi.defaultSettings

        // Then
        settings should include("min_gram")
        settings should include("max_gram")
        settings should include("\"max_ngram_diff\": \"20\"")
      }

      "contain analyzer configuration" in {
        // When
        val settings = indicesApi.defaultSettings

        // Then
        settings should include("analyzer")
        settings should include("search_analyzer")
        settings should include("lowercase")
        settings should include("asciifolding")
      }

      "contain mapping limits" in {
        // When
        val settings = indicesApi.defaultSettings

        // Then
        settings should include("total_fields")
        settings should include("\"limit\" : \"2000\"")
      }
    }

    "createIndex" should {

      "successfully create index with default settings" in {
        // Given
        indicesApi.executeCreateIndexResult = ElasticSuccess(true)

        // When
        val result = indicesApi.createIndex("my-index", mappings = None, aliases = Nil)

        // Then
        result shouldBe ElasticSuccess(true)
        result.isSuccess shouldBe true

        verify(mockLogger).info(contains("Creating index 'my-index' with settings:"))
        verify(mockLogger).info("✅ Index 'my-index' created successfully")
        verify(mockLogger, never).error(any[String])
      }

      "successfully create index with custom settings" in {
        // Given
        val customSettings = """{"index": {"number_of_shards": 3}}"""
        indicesApi.executeCreateIndexResult = ElasticSuccess(true)

        // When
        val result = indicesApi.createIndex("my-index", customSettings, None, Nil)

        // Then
        result.isSuccess shouldBe true

        verify(mockLogger).info(contains("Creating index 'my-index' with settings:"))
        verify(mockLogger).info(contains(customSettings))
        verify(mockLogger).info("✅ Index 'my-index' created successfully")
      }

      "return false when index creation returns false" in {
        // Given
        indicesApi.executeCreateIndexResult = ElasticSuccess(false)

        // When
        val result = indicesApi.createIndex("my-index", mappings = None, aliases = Nil)

        // Then
        result shouldBe ElasticSuccess(false)
        result.get shouldBe false

        verify(mockLogger).info("✅ Index 'my-index' not created")
      }

      "handle creation failure" in {
        // Given
        val error = ElasticError("Index already exists", statusCode = Some(400))
        indicesApi.executeCreateIndexResult = ElasticFailure(error)

        // When
        val result = indicesApi.createIndex("my-index", mappings = None, aliases = Nil)

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Index already exists"

        verify(mockLogger).error("❌ Failed to create index 'my-index': Index already exists")
      }

      "reject invalid index name" in {
        // When
        val result = indicesApi.createIndex("INVALID", mappings = None, aliases = Nil)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
        result.error.get.message should include("lowercase")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("createIndex")

        verify(mockLogger, never).info(any[String])
      }

      "reject empty index name" in {
        // When
        val result = indicesApi.createIndex("", mappings = None, aliases = Nil)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
        result.error.get.message should include("cannot be empty")
      }

      "reject invalid JSON settings" in {
        // Given
        val invalidJson = """{"index": invalid json}"""

        // When
        val result = indicesApi.createIndex("my-index", invalidJson, None, Nil)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid settings")
        result.error.get.message should include("Invalid JSON")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("createIndex")

        verify(mockLogger, never).info(any[String])
      }

      "reject empty settings" in {
        // When
        val result = indicesApi.createIndex("my-index", "", None, Nil)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid settings")
        result.error.get.message should include("cannot be empty")
      }

      "reject null settings" in {
        // When
        val result = indicesApi.createIndex("my-index", null, None, Nil)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid settings")
      }

      "handle network error during creation" in {
        // Given
        val error = ElasticError(
          "Connection timeout",
          cause = Some(new java.net.SocketTimeoutException()),
          statusCode = Some(504)
        )
        indicesApi.executeCreateIndexResult = ElasticFailure(error)

        // When
        val result = indicesApi.createIndex("my-index", mappings = None, aliases = Nil)

        // Then
        result.isFailure shouldBe true
        result.error.get.cause shouldBe defined

        verify(mockLogger).error(contains("Connection timeout"))
      }

      "validate index name before settings" in {
        // Given
        val invalidJson = """invalid"""

        // When - Invalid index name should fail first
        val result = indicesApi.createIndex("INVALID", invalidJson, None, Nil)

        // Then
        result.error.get.message should include("Invalid index")
        result.error.get.message should include("lowercase")
      }
    }

    "deleteIndex" should {

      "successfully delete existing index" in {
        // Given
        indicesApi.executeDeleteIndexResult = ElasticSuccess(true)

        // When
        val result = indicesApi.deleteIndex("my-index")

        // Then
        result shouldBe ElasticSuccess(true)

        verify(mockLogger).info("Deleting index 'my-index'")
        verify(mockLogger).info("✅ Index 'my-index' deleted successfully")
      }

      "return false when deletion returns false" in {
        // Given
        indicesApi.executeDeleteIndexResult = ElasticSuccess(false)

        // When
        val result = indicesApi.deleteIndex("my-index")

        // Then
        result shouldBe ElasticSuccess(false)

        verify(mockLogger).info("✅ Index 'my-index' not deleted")
      }

      "handle deletion failure" in {
        // Given
        val error = ElasticError("Index not found", statusCode = Some(404))
        indicesApi.executeDeleteIndexResult = ElasticFailure(error)

        // When
        val result = indicesApi.deleteIndex("my-index")

        // Then
        result.isFailure shouldBe true

        verify(mockLogger).error("❌ Failed to delete index 'my-index': Index not found")
      }

      "reject invalid index name" in {
        // When
        val result = indicesApi.deleteIndex("INVALID")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
        result.error.get.operation shouldBe Some("deleteIndex")

        verify(mockLogger, never).info(any[String])
      }

      "reject empty index name" in {
        // When
        val result = indicesApi.deleteIndex("")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("cannot be empty")
      }
    }

    "closeIndex" should {

      "successfully close index" in {
        // Given
        indicesApi.executeCloseIndexResult = ElasticSuccess(true)

        // When
        val result = indicesApi.closeIndex("my-index")

        // Then
        result shouldBe ElasticSuccess(true)

        verify(mockLogger).info("Closing index 'my-index'")
        verify(mockLogger).info("✅ Index 'my-index' closed successfully")
      }

      "return false when close returns false" in {
        // Given
        indicesApi.executeCloseIndexResult = ElasticSuccess(false)

        // When
        val result = indicesApi.closeIndex("my-index")

        // Then
        result shouldBe ElasticSuccess(false)

        verify(mockLogger).info("✅ Index 'my-index' not closed")
      }

      "handle close failure" in {
        // Given
        val error = ElasticError("Cannot close index", statusCode = Some(500))
        indicesApi.executeCloseIndexResult = ElasticFailure(error)

        // When
        val result = indicesApi.closeIndex("my-index")

        // Then
        result.isFailure shouldBe true

        verify(mockLogger).error("❌ Failed to close index 'my-index': Cannot close index")
      }

      "reject invalid index name" in {
        // When
        val result = indicesApi.closeIndex("INVALID")

        // Then
        result.isFailure shouldBe true
        result.error.get.operation shouldBe Some("closeIndex")
      }
    }

    "openIndex" should {

      "successfully open index" in {
        // Given
        indicesApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = indicesApi.openIndex("my-index")

        // Then
        result shouldBe ElasticSuccess(true)

        verify(mockLogger).info("Opening index 'my-index'")
        verify(mockLogger).info("✅ Index 'my-index' opened successfully")
      }

      "return false when open returns false" in {
        // Given
        indicesApi.executeOpenIndexResult = ElasticSuccess(false)

        // When
        val result = indicesApi.openIndex("my-index")

        // Then
        result shouldBe ElasticSuccess(false)

        verify(mockLogger).info("✅ Index 'my-index' not opened")
      }

      "handle open failure" in {
        // Given
        val error = ElasticError("Cannot open index", statusCode = Some(500))
        indicesApi.executeOpenIndexResult = ElasticFailure(error)

        // When
        val result = indicesApi.openIndex("my-index")

        // Then
        result.isFailure shouldBe true

        verify(mockLogger).error("❌ Failed to open index 'my-index': Cannot open index")
      }

      "reject invalid index name" in {
        // When
        val result = indicesApi.openIndex("INVALID")

        // Then
        result.isFailure shouldBe true
        result.error.get.operation shouldBe Some("openIndex")
      }
    }

    "indexExists" should {

      "return true when index exists" in {
        // Given
        indicesApi.executeIndexExistsResult = ElasticSuccess(true)

        // When
        val result = indicesApi.indexExists("my-index", false)

        // Then
        result shouldBe ElasticSuccess(true)

        verify(mockLogger).debug("Checking if index 'my-index' exists")
        verify(mockLogger).debug("✅ Index 'my-index' exists")
      }

      "return false when index does not exist" in {
        // Given
        indicesApi.executeIndexExistsResult = ElasticSuccess(false)

        // When
        val result = indicesApi.indexExists("my-index", false)

        // Then
        result shouldBe ElasticSuccess(false)

        verify(mockLogger).debug("✅ Index 'my-index' does not exist")
      }

      "handle existence check failure" in {
        // Given
        val error = ElasticError("Connection error", statusCode = Some(503))
        indicesApi.executeIndexExistsResult = ElasticFailure(error)

        // When
        val result = indicesApi.indexExists("my-index", false)

        // Then
        result.isFailure shouldBe true

        verify(mockLogger).error(
          "❌ Failed to check existence of index 'my-index': Connection error"
        )
      }

      "reject invalid index name" in {
        // When
        val result = indicesApi.indexExists("INVALID", false)

        // Then
        result.isFailure shouldBe true
        result.error.get.operation shouldBe Some("indexExists")
      }
    }

    "reindex" should {

      "successfully reindex with refresh" in {
        // Given
        indicesApi.executeIndexExistsResult = ElasticSuccess(true)
        indicesApi.executeReindexResult = ElasticSuccess((true, Some(100L)))
        indicesApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = indicesApi.reindex("source-index", "target-index", refresh = true)

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe ((true, Some(100L)))

        verify(mockLogger).info("Reindexing from 'source-index' to 'target-index' (refresh=true)")
        verify(mockLogger).info(
          "✅ Reindex from 'source-index' to 'target-index' succeeded (100 documents)"
        )
        verify(mockLogger).debug("✅ Target index 'target-index' refreshed")
      }

      "successfully reindex without refresh" in {
        // Given
        indicesApi.executeIndexExistsResult = ElasticSuccess(true)
        indicesApi.executeReindexResult = ElasticSuccess((true, Some(50L)))

        // When
        val result = indicesApi.reindex("source-index", "target-index", refresh = false)

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe ((true, Some(50L)))

        verify(mockLogger).info("Reindexing from 'source-index' to 'target-index' (refresh=false)")
        verify(mockLogger).info(
          "✅ Reindex from 'source-index' to 'target-index' succeeded (50 documents)"
        )
        verify(mockLogger, never).debug(contains("refreshed"))
      }

      "successfully reindex without document count" in {
        // Given
        indicesApi.executeIndexExistsResult = ElasticSuccess(true)
        indicesApi.executeReindexResult = ElasticSuccess((true, None))

        // When
        val result = indicesApi.reindex("source-index", "target-index")

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe ((true, None))

        verify(mockLogger).info("✅ Reindex from 'source-index' to 'target-index' succeeded")
      }

      "handle reindex failure" in {
        // Given
        indicesApi.executeIndexExistsResult = ElasticSuccess(true)
        val error = ElasticError("Reindex failed", statusCode = Some(500))
        indicesApi.executeReindexResult = ElasticFailure(error)

        // When
        val result = indicesApi.reindex("source-index", "target-index")

        // Then
        result.isFailure shouldBe true

        verify(mockLogger).error("Reindex failed for index 'target-index': Reindex failed")
      }

      "handle reindex returning false" in {
        // Given
        indicesApi.executeIndexExistsResult = ElasticSuccess(true)
        indicesApi.executeReindexResult = ElasticSuccess((false, None))

        // When
        val result = indicesApi.reindex("source-index", "target-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Reindex failed for index 'target-index'")
        result.error.get.operation shouldBe Some("reindex")
      }

      "succeed even if refresh fails after successful reindex" in {
        // Given
        indicesApi.executeIndexExistsResult = ElasticSuccess(true)
        indicesApi.executeReindexResult = ElasticSuccess((true, Some(100L)))
        indicesApi.executeRefreshResult = ElasticFailure(ElasticError("Refresh failed"))

        // When
        val result = indicesApi.reindex("source-index", "target-index", refresh = true)

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe ((true, Some(100L)))

        verify(mockLogger).warn(contains("Refresh failed but reindex succeeded"))
      }

      "reject when source and target are the same" in {
        // When
        val result = indicesApi.reindex("same-index", "same-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Source and target index cannot be the same"
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.operation shouldBe Some("reindex")

        verify(mockLogger, never).info(contains("Reindexing"))
      }

      "reject invalid source index name" in {
        // When
        val result = indicesApi.reindex("INVALID", "target-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid source index")
        result.error.get.operation shouldBe Some("reindex")
      }

      "reject invalid target index name" in {
        // When
        val result = indicesApi.reindex("source-index", "INVALID")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid target index")
        result.error.get.operation shouldBe Some("reindex")
      }

      "fail when source index does not exist" in {
        // Given
        indicesApi.executeIndexExistsResult = ElasticSuccess(false)

        // When
        val result = indicesApi.reindex("source-index", "target-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Source index 'source-index' does not exist")
        result.error.get.statusCode shouldBe Some(404)
      }

      "fail when target index does not exist" in {
        // Given
        var callCount = 0
        val checkingApi = new IndicesApi
          with RefreshApi
          with PipelineApi
          with VersionApi
          with SerializationApi {
          override protected def logger: Logger = mockLogger

          override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] = {
            callCount += 1
            if (callCount == 1) ElasticSuccess(true) // source exists
            else ElasticSuccess(false) // target doesn't exist
          }

          override private[client] def executeCreateIndex(
            index: String,
            settings: String,
            mappings: Option[String],
            aliases: Seq[TableAlias]
          ): ElasticResult[Boolean] = ???

          override private[client] def executeGetIndex(
            index: String
          ): ElasticResult[Option[String]] = ???

          override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = ???
          override private[client] def executeReindex(
            sourceIndex: String,
            targetIndex: String,
            refresh: Boolean,
            pipeline: Option[String]
          ): ElasticResult[(Boolean, Option[Long])] = ???
          override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = ???

          override private[client] def executeCreatePipeline(
            pipelineName: String,
            pipelineDefinition: String
          ): ElasticResult[Boolean] = ???

          override private[client] def executeDeletePipeline(
            pipelineName: String,
            ifExists: Boolean
          ): ElasticResult[Boolean] = ???

          override private[client] def executeGetPipeline(
            pipelineName: String
          ): ElasticResult[Option[String]] = ???

          override private[client] def executeVersion(): ElasticResult[String] = ???

          /** Implicit conversion of an SQL query to Elasticsearch JSON. Used for query
            * serialization.
            *
            * @param sqlSearch
            *   the SQL search request to convert
            * @return
            *   JSON string representation of the query
            */
          override private[client] implicit def sqlSearchRequestToJsonQuery(
            sqlSearch: query.SingleSearch
          )(implicit timestamp: Long): String = ???

          override private[client] def executeDeleteByQuery(
            index: String,
            query: String,
            refresh: Boolean
          ): ElasticResult[Long] = ???

          override private[client] def executeIsIndexClosed(index: String): ElasticResult[Boolean] =
            ???

          override private[client] def executeUpdateByQuery(
            index: String,
            query: String,
            pipelineId: Option[String],
            refresh: Boolean
          ): ElasticResult[Long] = ???
        }

        // When
        val result = checkingApi.reindex("source-index", "target-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Target index 'target-index' does not exist")
        result.error.get.statusCode shouldBe Some(404)
      }

      "fail when source existence check fails" in {
        // Given
        val error = ElasticError("Connection error", statusCode = Some(503))
        indicesApi.executeIndexExistsResult = ElasticFailure(error)

        // When
        val result = indicesApi.reindex("source-index", "target-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Connection error"
      }

      "fail when target existence check fails" in {
        // Given
        var callCount = 0
        val checkingApi = new IndicesApi
          with RefreshApi
          with PipelineApi
          with VersionApi
          with SerializationApi {
          override protected def logger: Logger = mockLogger

          override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] = {
            callCount += 1
            if (callCount == 1) ElasticSuccess(true) // source exists
            else ElasticFailure(ElasticError("Connection error")) // target check fails
          }

          override private[client] def executeCreateIndex(
            index: String,
            settings: String,
            mappings: Option[String],
            aliases: Seq[TableAlias]
          ): ElasticResult[Boolean] = ???

          override private[client] def executeGetIndex(
            index: String
          ): ElasticResult[Option[String]] = ???

          override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = ???
          override private[client] def executeReindex(
            sourceIndex: String,
            targetIndex: String,
            refresh: Boolean,
            pipeline: Option[String]
          ): ElasticResult[(Boolean, Option[Long])] = ???
          override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = ???

          override private[client] def executeCreatePipeline(
            pipelineName: String,
            pipelineDefinition: String
          ): ElasticResult[Boolean] = ???

          override private[client] def executeDeletePipeline(
            pipelineName: String,
            ifExists: Boolean
          ): ElasticResult[Boolean] = ???

          override private[client] def executeGetPipeline(
            pipelineName: String
          ): ElasticResult[Option[String]] = ???

          override private[client] def executeVersion(): ElasticResult[String] = ???

          /** Implicit conversion of an SQL query to Elasticsearch JSON. Used for query
            * serialization.
            *
            * @param sqlSearch
            *   the SQL search request to convert
            * @return
            *   JSON string representation of the query
            */
          override private[client] implicit def sqlSearchRequestToJsonQuery(
            sqlSearch: query.SingleSearch
          )(implicit timestamp: Long): String = ???

          override private[client] def executeDeleteByQuery(
            index: String,
            query: String,
            refresh: Boolean
          ): ElasticResult[Long] = ???

          override private[client] def executeIsIndexClosed(index: String): ElasticResult[Boolean] =
            ???

          override private[client] def executeUpdateByQuery(
            index: String,
            query: String,
            pipelineId: Option[String],
            refresh: Boolean
          ): ElasticResult[Long] = ???
        }

        // When
        val result = checkingApi.reindex("source-index", "target-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Connection error"
      }
    }

    "ElasticResult integration" should {

      "work with map transformation" in {
        // Given
        indicesApi.executeCreateIndexResult = ElasticSuccess(true)

        // When
        val result =
          indicesApi.createIndex("my-index", mappings = None, aliases = Nil).map { success =>
            if (success) "Created" else "Not created"
          }

        // Then
        result shouldBe ElasticSuccess("Created")
      }

      "work with flatMap composition" in {
        // Given
        indicesApi.executeCreateIndexResult = ElasticSuccess(true)
        indicesApi.executeIndexExistsResult = ElasticSuccess(true)

        // When
        val result =
          indicesApi.createIndex("my-index", mappings = None, aliases = Nil).flatMap { _ =>
            indicesApi.indexExists("my-index", false)
          }

        // Then
        result shouldBe ElasticSuccess(true)
      }

      "work with for-comprehension" in {
        // Given
        indicesApi.executeCreateIndexResult = ElasticSuccess(true)
        indicesApi.executeOpenIndexResult = ElasticSuccess(true)

        // When
        val result = for {
          created <- indicesApi.createIndex("my-index", mappings = None, aliases = Nil)
          opened  <- indicesApi.openIndex("my-index")
        } yield created && opened

        // Then
        result shouldBe ElasticSuccess(true)
      }

      "propagate errors through transformations" in {
        // Given
        val error = ElasticError("Creation failed")
        indicesApi.executeCreateIndexResult = ElasticFailure(error)

        // When
        val result = indicesApi
          .createIndex("my-index", mappings = None, aliases = Nil)
          .map(!_)
          .flatMap(v => ElasticSuccess(s"Result: $v"))

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Creation failed"
      }
    }

    "sequential operations" should {

      "handle create, close, open, delete workflow" in {
        // Given
        indicesApi.executeCreateIndexResult = ElasticSuccess(true)
        indicesApi.executeCloseIndexResult = ElasticSuccess(true)
        indicesApi.executeOpenIndexResult = ElasticSuccess(true)
        indicesApi.executeDeleteIndexResult = ElasticSuccess(true)

        // When
        val created = indicesApi.createIndex("test-index", mappings = None, aliases = Nil)
        val closed = indicesApi.closeIndex("test-index")
        val opened = indicesApi.openIndex("test-index")
        val deleted = indicesApi.deleteIndex("test-index")

        // Then
        created.isSuccess shouldBe true
        closed.isSuccess shouldBe true
        opened.isSuccess shouldBe true
        deleted.isSuccess shouldBe true

        verify(mockLogger).info(contains("created successfully"))
        verify(mockLogger).info(contains("closed successfully"))
        verify(mockLogger).info(contains("opened successfully"))
        verify(mockLogger).info(contains("deleted successfully"))
      }

      "handle multiple index operations" in {
        // Given
        indicesApi.executeCreateIndexResult = ElasticSuccess(true)

        // When
        val result1 = indicesApi.createIndex("index1", mappings = None, aliases = Nil)
        val result2 = indicesApi.createIndex("index2", mappings = None, aliases = Nil)
        val result3 = indicesApi.createIndex("index3", mappings = None, aliases = Nil)

        // Then
        result1.isSuccess shouldBe true
        result2.isSuccess shouldBe true
        result3.isSuccess shouldBe true

        verify(mockLogger, times(3)).info(contains("created successfully"))
      }
    }

    "error handling" should {

      "handle authentication error" in {
        // Given
        val error = ElasticError("Authentication failed", statusCode = Some(401))
        indicesApi.executeCreateIndexResult = ElasticFailure(error)

        // When
        val result = indicesApi.createIndex("my-index", mappings = None, aliases = Nil)

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(401)
      }

      "handle authorization error" in {
        // Given
        val error = ElasticError("Insufficient permissions", statusCode = Some(403))
        indicesApi.executeDeleteIndexResult = ElasticFailure(error)

        // When
        val result = indicesApi.deleteIndex("my-index")

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
        indicesApi.executeCloseIndexResult = ElasticFailure(error)

        // When
        val result = indicesApi.closeIndex("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.cause shouldBe defined
      }

      "handle server error" in {
        // Given
        val error = ElasticError("Internal server error", statusCode = Some(500))
        indicesApi.executeOpenIndexResult = ElasticFailure(error)

        // When
        val result = indicesApi.openIndex("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(500)
      }
    }

    "validation order" should {

      "validate index name before calling execute methods" in {
        // Given
        var executeCalled = false
        val validatingApi = new IndicesApi
          with RefreshApi
          with PipelineApi
          with VersionApi
          with SerializationApi {
          override protected def logger: Logger = mockLogger

          override private[client] def executeCreateIndex(
            index: String,
            settings: String,
            mappings: Option[String],
            aliases: Seq[TableAlias]
          ): ElasticResult[Boolean] = {
            executeCalled = true
            ElasticSuccess(true)
          }

          override private[client] def executeGetIndex(
            index: String
          ): ElasticResult[Option[String]] = ???

          override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = ???
          override private[client] def executeReindex(
            sourceIndex: String,
            targetIndex: String,
            refresh: Boolean,
            pipeline: Option[String]
          ): ElasticResult[(Boolean, Option[Long])] = ???
          override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = ???

          override private[client] def executeCreatePipeline(
            pipelineName: String,
            pipelineDefinition: String
          ): ElasticResult[Boolean] = ???

          override private[client] def executeDeletePipeline(
            pipelineName: String,
            ifExists: Boolean
          ): ElasticResult[Boolean] = ???

          override private[client] def executeGetPipeline(
            pipelineName: String
          ): ElasticResult[Option[String]] = ???

          override private[client] def executeVersion(): ElasticResult[String] = ???

          /** Implicit conversion of an SQL query to Elasticsearch JSON. Used for query
            * serialization.
            *
            * @param sqlSearch
            *   the SQL search request to convert
            * @return
            *   JSON string representation of the query
            */
          override private[client] implicit def sqlSearchRequestToJsonQuery(
            sqlSearch: query.SingleSearch
          )(implicit timestamp: Long): String = ???

          override private[client] def executeDeleteByQuery(
            index: String,
            query: String,
            refresh: Boolean
          ): ElasticResult[Long] = ???

          override private[client] def executeIsIndexClosed(index: String): ElasticResult[Boolean] =
            ???

          override private[client] def executeUpdateByQuery(
            index: String,
            query: String,
            pipelineId: Option[String],
            refresh: Boolean
          ): ElasticResult[Long] = ???
        }

        // When
        validatingApi.createIndex("INVALID", mappings = None, aliases = Nil)

        // Then
        executeCalled shouldBe false
      }

      "validate settings after index name validation" in {
        // Given
        var executeCalled = false
        val validatingApi = new IndicesApi
          with RefreshApi
          with PipelineApi
          with VersionApi
          with SerializationApi {
          override protected def logger: Logger = mockLogger

          override private[client] def executeCreateIndex(
            index: String,
            settings: String,
            mappings: Option[String],
            aliases: Seq[TableAlias]
          ): ElasticResult[Boolean] = {
            executeCalled = true
            ElasticSuccess(true)
          }

          override private[client] def executeGetIndex(
            index: String
          ): ElasticResult[Option[String]] = ???

          override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = ???
          override private[client] def executeReindex(
            sourceIndex: String,
            targetIndex: String,
            refresh: Boolean,
            pipeline: Option[String]
          ): ElasticResult[(Boolean, Option[Long])] = ???
          override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = ???

          override private[client] def executeCreatePipeline(
            pipelineName: String,
            pipelineDefinition: String
          ): ElasticResult[Boolean] = ???

          override private[client] def executeDeletePipeline(
            pipelineName: String,
            ifExists: Boolean
          ): ElasticResult[Boolean] = ???

          override private[client] def executeGetPipeline(
            pipelineName: String
          ): ElasticResult[Option[String]] = ???

          override private[client] def executeVersion(): ElasticResult[String] = ???

          /** Implicit conversion of an SQL query to Elasticsearch JSON. Used for query
            * serialization.
            *
            * @param sqlSearch
            *   the SQL search request to convert
            * @return
            *   JSON string representation of the query
            */
          override private[client] implicit def sqlSearchRequestToJsonQuery(
            sqlSearch: query.SingleSearch
          )(implicit timestamp: Long): String = ???

          override private[client] def executeDeleteByQuery(
            index: String,
            query: String,
            refresh: Boolean
          ): ElasticResult[Long] = ???

          override private[client] def executeIsIndexClosed(index: String): ElasticResult[Boolean] =
            ???

          override private[client] def executeUpdateByQuery(
            index: String,
            query: String,
            pipelineId: Option[String],
            refresh: Boolean
          ): ElasticResult[Long] = ???
        }

        // When
        validatingApi.createIndex("valid-index", "invalid json", None, Nil)

        // Then
        executeCalled shouldBe false
      }

      "validate both indices in reindex before existence checks" in {
        // Given
        var existsCheckCalled = false
        val validatingApi = new IndicesApi
          with RefreshApi
          with PipelineApi
          with VersionApi
          with SerializationApi {
          override protected def logger: Logger = mockLogger

          override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] = {
            existsCheckCalled = true
            ElasticSuccess(true)
          }

          override private[client] def executeCreateIndex(
            index: String,
            settings: String,
            mappings: Option[String],
            aliases: Seq[TableAlias]
          ): ElasticResult[Boolean] = ???

          override private[client] def executeGetIndex(
            index: String
          ): ElasticResult[Option[String]] = ???

          override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = ???
          override private[client] def executeReindex(
            sourceIndex: String,
            targetIndex: String,
            refresh: Boolean,
            pipeline: Option[String]
          ): ElasticResult[(Boolean, Option[Long])] = ???
          override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = ???

          override private[client] def executeCreatePipeline(
            pipelineName: String,
            pipelineDefinition: String
          ): ElasticResult[Boolean] = ???

          override private[client] def executeDeletePipeline(
            pipelineName: String,
            ifExists: Boolean
          ): ElasticResult[Boolean] = ???

          override private[client] def executeGetPipeline(
            pipelineName: String
          ): ElasticResult[Option[String]] = ???

          override private[client] def executeVersion(): ElasticResult[String] = ???

          /** Implicit conversion of an SQL query to Elasticsearch JSON. Used for query
            * serialization.
            *
            * @param sqlSearch
            *   the SQL search request to convert
            * @return
            *   JSON string representation of the query
            */
          override private[client] implicit def sqlSearchRequestToJsonQuery(
            sqlSearch: query.SingleSearch
          )(implicit timestamp: Long): String = ???

          override private[client] def executeDeleteByQuery(
            index: String,
            query: String,
            refresh: Boolean
          ): ElasticResult[Long] = ???

          override private[client] def executeIsIndexClosed(index: String): ElasticResult[Boolean] =
            ???

          override private[client] def executeUpdateByQuery(
            index: String,
            query: String,
            pipelineId: Option[String],
            refresh: Boolean
          ): ElasticResult[Long] = ???
        }

        // When
        validatingApi.reindex("INVALID", "target")

        // Then
        existsCheckCalled shouldBe false
      }

      "check source and target are different before existence checks" in {
        // Given
        var existsCheckCalled = false
        val validatingApi = new IndicesApi
          with RefreshApi
          with PipelineApi
          with VersionApi
          with SerializationApi {
          override protected def logger: Logger = mockLogger

          override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] = {
            existsCheckCalled = true
            ElasticSuccess(true)
          }

          override private[client] def executeCreateIndex(
            index: String,
            settings: String,
            mappings: Option[String],
            aliases: Seq[TableAlias]
          ): ElasticResult[Boolean] = ???

          override private[client] def executeGetIndex(
            index: String
          ): ElasticResult[Option[String]] = ???

          override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] =
            ???
          override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] = ???
          override private[client] def executeReindex(
            sourceIndex: String,
            targetIndex: String,
            refresh: Boolean,
            pipeline: Option[String]
          ): ElasticResult[(Boolean, Option[Long])] = ???
          override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = ???

          override private[client] def executeCreatePipeline(
            pipelineName: String,
            pipelineDefinition: String
          ): ElasticResult[Boolean] = ???

          override private[client] def executeDeletePipeline(
            pipelineName: String,
            ifExists: Boolean
          ): ElasticResult[Boolean] = ???

          override private[client] def executeGetPipeline(
            pipelineName: String
          ): ElasticResult[Option[String]] = ???

          override private[client] def executeVersion(): ElasticResult[String] = ???

          /** Implicit conversion of an SQL query to Elasticsearch JSON. Used for query
            * serialization.
            *
            * @param sqlSearch
            *   the SQL search request to convert
            * @return
            *   JSON string representation of the query
            */
          override private[client] implicit def sqlSearchRequestToJsonQuery(
            sqlSearch: query.SingleSearch
          )(implicit timestamp: Long): String = ???

          override private[client] def executeDeleteByQuery(
            index: String,
            query: String,
            refresh: Boolean
          ): ElasticResult[Long] = ???

          override private[client] def executeIsIndexClosed(index: String): ElasticResult[Boolean] =
            ???

          override private[client] def executeUpdateByQuery(
            index: String,
            query: String,
            pipelineId: Option[String],
            refresh: Boolean
          ): ElasticResult[Long] = ???
        }

        // When
        validatingApi.reindex("same-index", "same-index")

        // Then
        existsCheckCalled shouldBe false
      }
    }

    "logging levels" should {

      "use info for successful operations" in {
        // Given
        indicesApi.executeCreateIndexResult = ElasticSuccess(true)

        // When
        indicesApi.createIndex("my-index", mappings = None, aliases = Nil)

        // Then
        verify(mockLogger, atLeastOnce).info(any[String])
        verify(mockLogger, never).error(any[String])
      }

      "use error for failed operations" in {
        // Given
        indicesApi.executeCreateIndexResult = ElasticFailure(ElasticError("Failed"))

        // When
        indicesApi.createIndex("my-index", mappings = None, aliases = Nil)

        // Then
        verify(mockLogger).error(any[String])
      }

      "use debug for existence checks" in {
        // Given
        indicesApi.executeIndexExistsResult = ElasticSuccess(true)

        // When
        indicesApi.indexExists("my-index", false)

        // Then
        verify(mockLogger, atLeastOnce).debug(any[String])
        verify(mockLogger, never).info(any[String])
        verify(mockLogger, never).error(any[String])
      }

      "use warn when refresh fails but reindex succeeds" in {
        // Given
        indicesApi.executeIndexExistsResult = ElasticSuccess(true)
        indicesApi.executeReindexResult = ElasticSuccess((true, Some(100L)))
        indicesApi.executeRefreshResult = ElasticFailure(ElasticError("Refresh failed"))

        // When
        indicesApi.reindex("source", "target", refresh = true)

        // Then
        verify(mockLogger).warn(contains("Refresh failed but reindex succeeded"))
      }
    }

    "edge cases" should {

      "handle index name with maximum length" in {
        // Given
        val maxName = "a" * 255
        indicesApi.executeCreateIndexResult = ElasticSuccess(true)

        // When
        val result = indicesApi.createIndex(maxName, mappings = None, aliases = Nil)

        // Then
        result.isSuccess shouldBe true
      }

      "handle reindex with zero documents" in {
        // Given
        indicesApi.executeIndexExistsResult = ElasticSuccess(true)
        indicesApi.executeReindexResult = ElasticSuccess((true, Some(0L)))

        // When
        val result = indicesApi.reindex("source", "target")

        // Then
        result.isSuccess shouldBe true
        result.get._2 shouldBe Some(0L)

        verify(mockLogger).info(contains("(0 documents)"))
      }

      "handle reindex with large document count" in {
        // Given
        indicesApi.executeIndexExistsResult = ElasticSuccess(true)
        indicesApi.executeReindexResult = ElasticSuccess((true, Some(1000000L)))

        // When
        val result = indicesApi.reindex("source", "target")

        // Then
        result.isSuccess shouldBe true
        result.get._2 shouldBe Some(1000000L)

        verify(mockLogger).info(contains("(1000000 documents)"))
      }

      "handle custom settings with complex JSON" in {
        // Given
        val complexSettings =
          """{
            |  "index": {
            |    "number_of_shards": 5,
            |    "number_of_replicas": 2,
            |    "analysis": {
            |      "analyzer": {
            |        "custom_analyzer": {
            |          "type": "custom",
            |          "tokenizer": "standard"
            |        }
            |      }
            |    }
            |  }
            |}""".stripMargin
        indicesApi.executeCreateIndexResult = ElasticSuccess(true)

        // When
        val result = indicesApi.createIndex("my-index", complexSettings, None, Nil)

        // Then
        result.isSuccess shouldBe true
        verify(mockLogger).info(contains(complexSettings))
      }

      "handle settings with whitespace variations" in {
        // Given
        val settings = """  {"index": {"number_of_shards": 1}}  """
        indicesApi.executeCreateIndexResult = ElasticSuccess(true)

        // When
        val result = indicesApi.createIndex("my-index", settings, None, Nil)

        // Then
        result.isSuccess shouldBe true
      }
    }

    "complex scenarios" should {

      "handle create-reindex-delete workflow" in {
        // Given
        indicesApi.executeCreateIndexResult = ElasticSuccess(true)
        indicesApi.executeIndexExistsResult = ElasticSuccess(true)
        indicesApi.executeReindexResult = ElasticSuccess((true, Some(50L)))
        indicesApi.executeDeleteIndexResult = ElasticSuccess(true)

        // When
        val created = indicesApi.createIndex("new-index", mappings = None, aliases = Nil)
        val reindexed = indicesApi.reindex("old-index", "new-index")
        val deleted = indicesApi.deleteIndex("old-index")

        // Then
        created.isSuccess shouldBe true
        reindexed.isSuccess shouldBe true
        deleted.isSuccess shouldBe true
      }

      "handle multiple operations with mixed results" in {
        // Given
        indicesApi.executeCreateIndexResult = ElasticSuccess(true)
        val result1 = indicesApi.createIndex("index1", mappings = None, aliases = Nil)

        indicesApi.executeCreateIndexResult = ElasticFailure(ElasticError("Already exists"))
        val result2 = indicesApi.createIndex("index2", mappings = None, aliases = Nil)

        indicesApi.executeCreateIndexResult = ElasticSuccess(false)
        val result3 = indicesApi.createIndex("index3", mappings = None, aliases = Nil)

        // Then
        result1.isSuccess shouldBe true
        result2.isFailure shouldBe true
        result3.isSuccess shouldBe true
        result3.get shouldBe false
      }

      "handle reindex with all validation steps" in {
        // Given - Valid indices that exist
        indicesApi.executeIndexExistsResult = ElasticSuccess(true)
        indicesApi.executeReindexResult = ElasticSuccess((true, Some(100L)))
        indicesApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = indicesApi.reindex("source-index", "target-index", refresh = true)

        // Then
        result.isSuccess shouldBe true

        // Verify all steps were logged
        verify(mockLogger).info(contains("Reindexing from"))
        verify(mockLogger).info(contains("succeeded"))
        verify(mockLogger).debug(contains("refreshed"))
      }
    }

    "thread safety considerations" should {

      "handle concurrent index operations" in {
        // Given
        indicesApi.executeCreateIndexResult = ElasticSuccess(true)

        // When - Simulate concurrent calls
        val results =
          (1 to 5).map(i => indicesApi.createIndex(s"index-$i", mappings = None, aliases = Nil))

        // Then
        results.foreach(_.isSuccess shouldBe true)
        verify(mockLogger, times(5)).info(contains("created successfully"))
      }
    }
  }
}
