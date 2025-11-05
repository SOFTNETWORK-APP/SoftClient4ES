package app.softnetwork.elastic.client

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.mockito.MockitoSugar
import org.mockito.ArgumentMatchersSugar
import org.slf4j.Logger
import app.softnetwork.elastic.client.result._

/** Unit tests for RefreshApi
  */
class RefreshApiSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterEach
    with MockitoSugar
    with ArgumentMatchersSugar {

  // Mock logger
  val mockLogger: Logger = mock[Logger]

  // Concrete implementation for testing
  class TestRefreshApi extends RefreshApi {
    override protected def logger: Logger = mockLogger

    // Variable to control the behavior of executeRefresh
    var executeRefreshResult: ElasticResult[Boolean] = ElasticSuccess(true)

    override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = {
      executeRefreshResult
    }
  }

  var refreshApi: TestRefreshApi = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    refreshApi = new TestRefreshApi()
    reset(mockLogger)
  }

  "RefreshApi" should {

    "refresh" should {

      "successfully refresh a valid index" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = refreshApi.refresh("my-index")

        // Then
        result shouldBe ElasticSuccess(true)
        result.isSuccess shouldBe true
        result.get shouldBe true

        verify(mockLogger).debug("Refreshing index: my-index")
        verify(mockLogger).info("✅ Index 'my-index' refreshed successfully")
        verify(mockLogger, never).error(any[String])
      }

      "return true when refresh succeeds" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = refreshApi.refresh("test-index")

        // Then
        result.isSuccess shouldBe true
        result.toOption shouldBe Some(true)
        result.getOrElse(false) shouldBe true

        verify(mockLogger).info(contains("refreshed successfully"))
      }

      "return false when refresh returns false" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(false)

        // When
        val result = refreshApi.refresh("my-index")

        // Then
        result shouldBe ElasticSuccess(false)
        result.isSuccess shouldBe true
        result.get shouldBe false

        verify(mockLogger).debug("Refreshing index: my-index")
        verify(mockLogger).info("✅ Index 'my-index' not refreshed")
        verify(mockLogger, never).error(any[String])
      }

      "handle refresh failure with error" in {
        // Given
        val error = ElasticError(
          message = "Connection timeout",
          cause = Some(new java.net.SocketTimeoutException("Read timed out")),
          statusCode = Some(504),
          operation = Some("executeRefresh")
        )
        refreshApi.executeRefreshResult = ElasticFailure(error)

        // When
        val result = refreshApi.refresh("my-index")

        // Then
        result.isFailure shouldBe true
        result.error shouldBe Some(error)

        verify(mockLogger).debug("Refreshing index: my-index")
        verify(mockLogger).error("❌ Failed to refresh index 'my-index': Connection timeout")
        verify(mockLogger, never).info(contains("✅"))
      }

      "reject empty index name" in {
        // When
        val result = refreshApi.refresh("")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Invalid index")
        result.error.get.message should include("Index name cannot be empty")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.index shouldBe Some("")
        result.error.get.operation shouldBe Some("refresh")

        verify(mockLogger, never).debug(any[String])
        verify(mockLogger, never).info(any[String])
        verify(mockLogger, never).error(any[String])
      }

      "reject null index name" in {
        // When
        val result = refreshApi.refresh(null)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name cannot be empty")
        result.error.get.statusCode shouldBe Some(400)
      }

      "reject index name with only spaces" in {
        // When
        val result = refreshApi.refresh("   ")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name cannot be empty")
      }

      "reject index name with uppercase letters" in {
        // When
        val result = refreshApi.refresh("MyIndex")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name must be lowercase")
        result.error.get.statusCode shouldBe Some(400)
        result.error.get.index shouldBe Some("MyIndex")
        result.error.get.operation shouldBe Some("refresh")
      }

      "reject index name starting with hyphen" in {
        // When
        val result = refreshApi.refresh("-my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name cannot start with '-', '_', or '+'")
      }

      "reject index name starting with underscore" in {
        // When
        val result = refreshApi.refresh("_my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name cannot start with '-', '_', or '+'")
      }

      "reject index name starting with plus" in {
        // When
        val result = refreshApi.refresh("+my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name cannot start with '-', '_', or '+'")
      }

      "reject index name that is a single dot" in {
        // When
        val result = refreshApi.refresh(".")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name cannot be '.' or '..'")
      }

      "reject index name that is double dots" in {
        // When
        val result = refreshApi.refresh("..")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name cannot be '.' or '..'")
      }

      "reject index name with backslash" in {
        // When
        val result = refreshApi.refresh("my\\index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name contains invalid characters")
      }

      "reject index name with forward slash" in {
        // When
        val result = refreshApi.refresh("my/index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name contains invalid characters")
      }

      "reject index name with asterisk" in {
        // When
        val result = refreshApi.refresh("my*index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name contains invalid characters")
      }

      "reject index name with question mark" in {
        // When
        val result = refreshApi.refresh("my?index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name contains invalid characters")
      }

      "reject index name with double quote" in {
        // When
        val result = refreshApi.refresh("my\"index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name contains invalid characters")
      }

      "reject index name with less than" in {
        // When
        val result = refreshApi.refresh("my<index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name contains invalid characters")
      }

      "reject index name with greater than" in {
        // When
        val result = refreshApi.refresh("my>index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name contains invalid characters")
      }

      "reject index name with pipe" in {
        // When
        val result = refreshApi.refresh("my|index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name contains invalid characters")
      }

      "reject index name with space" in {
        // When
        val result = refreshApi.refresh("my index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name contains invalid characters")
      }

      "reject index name with comma" in {
        // When
        val result = refreshApi.refresh("my,index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name contains invalid characters")
      }

      "reject index name with hash" in {
        // When
        val result = refreshApi.refresh("my#index")

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name contains invalid characters")
      }

      "reject index name longer than 255 characters" in {
        // Given
        val longName = "a" * 256

        // When
        val result = refreshApi.refresh(longName)

        // Then
        result.isFailure shouldBe true
        result.error.get.message should include("Index name is too long")
        result.error.get.message should include("256")
      }

      "accept index name with exactly 255 characters" in {
        // Given
        val maxName = "a" * 255
        refreshApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = refreshApi.refresh(maxName)

        // Then
        result.isSuccess shouldBe true
        verify(mockLogger).debug(s"Refreshing index: $maxName")
      }

      "accept valid index names with hyphens" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = refreshApi.refresh("my-index-name")

        // Then
        result.isSuccess shouldBe true
        verify(mockLogger).info(contains("my-index-name"))
      }

      "accept valid index names with numbers" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = refreshApi.refresh("index123")

        // Then
        result.isSuccess shouldBe true
      }

      "accept valid index names with dots" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = refreshApi.refresh("my.index.name")

        // Then
        result.isSuccess shouldBe true
      }

      "accept valid index names starting with letter" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = refreshApi.refresh("myindex")

        // Then
        result.isSuccess shouldBe true
      }

      "accept valid index names starting with number" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = refreshApi.refresh("123index")

        // Then
        result.isSuccess shouldBe true
      }

      "handle network timeout error" in {
        // Given
        val error = ElasticError(
          message = "Connection timeout",
          cause = Some(new java.net.SocketTimeoutException()),
          statusCode = Some(504)
        )
        refreshApi.executeRefreshResult = ElasticFailure(error)

        // When
        val result = refreshApi.refresh("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.cause shouldBe defined
        result.error.get.cause.get shouldBe a[java.net.SocketTimeoutException]

        verify(mockLogger).error(contains("Connection timeout"))
      }

      "handle connection refused error" in {
        // Given
        val error = ElasticError(
          message = "Connection refused",
          cause = Some(new java.net.ConnectException()),
          statusCode = Some(503)
        )
        refreshApi.executeRefreshResult = ElasticFailure(error)

        // When
        val result = refreshApi.refresh("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(503)

        verify(mockLogger).error(contains("Connection refused"))
      }

      "handle index not found error" in {
        // Given
        val error = ElasticError(
          message = "Index not found",
          statusCode = Some(404),
          index = Some("my-index")
        )
        refreshApi.executeRefreshResult = ElasticFailure(error)

        // When
        val result = refreshApi.refresh("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(404)
        result.error.get.index shouldBe Some("my-index")

        verify(mockLogger).error(contains("Index not found"))
      }

      "handle authentication error" in {
        // Given
        val error = ElasticError(
          message = "Authentication failed",
          statusCode = Some(401)
        )
        refreshApi.executeRefreshResult = ElasticFailure(error)

        // When
        val result = refreshApi.refresh("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(401)

        verify(mockLogger).error(contains("Authentication failed"))
      }

      "handle authorization error" in {
        // Given
        val error = ElasticError(
          message = "Insufficient permissions",
          statusCode = Some(403)
        )
        refreshApi.executeRefreshResult = ElasticFailure(error)

        // When
        val result = refreshApi.refresh("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(403)

        verify(mockLogger).error(contains("Insufficient permissions"))
      }

      "handle server error" in {
        // Given
        val error = ElasticError(
          message = "Internal server error",
          statusCode = Some(500)
        )
        refreshApi.executeRefreshResult = ElasticFailure(error)

        // When
        val result = refreshApi.refresh("my-index")

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(500)

        verify(mockLogger).error(contains("Internal server error"))
      }

      "support ElasticResult operations - map" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = refreshApi.refresh("my-index").map { success =>
          if (success) "Refreshed" else "Not refreshed"
        }

        // Then
        result shouldBe ElasticSuccess("Refreshed")
        result.get shouldBe "Refreshed"
      }

      "support ElasticResult operations - flatMap" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = refreshApi.refresh("my-index").flatMap { success =>
          if (success) ElasticSuccess("Operation completed")
          else ElasticFailure(ElasticError("Refresh failed"))
        }

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe "Operation completed"
      }

      "support ElasticResult operations - getOrElse" in {
        // Given - Success case
        refreshApi.executeRefreshResult = ElasticSuccess(true)
        val result1 = refreshApi.refresh("my-index").getOrElse(false)

        // Given - Failure case
        refreshApi.executeRefreshResult = ElasticFailure(ElasticError("Error"))
        val result2 = refreshApi.refresh("my-index").getOrElse(false)

        // Then
        result1 shouldBe true
        result2 shouldBe false
      }

      "support ElasticResult operations - toOption" in {
        // Given - Success
        refreshApi.executeRefreshResult = ElasticSuccess(true)
        val option1 = refreshApi.refresh("my-index").toOption

        // Given - Failure
        refreshApi.executeRefreshResult = ElasticFailure(ElasticError("Error"))
        val option2 = refreshApi.refresh("my-index").toOption

        // Then
        option1 shouldBe Some(true)
        option2 shouldBe None
      }

      "support ElasticResult operations - toEither" in {
        // Given - Success
        refreshApi.executeRefreshResult = ElasticSuccess(true)
        val either1 = refreshApi.refresh("my-index").toEither

        // Given - Failure
        val error = ElasticError("Error")
        refreshApi.executeRefreshResult = ElasticFailure(error)
        val either2 = refreshApi.refresh("my-index").toEither

        // Then
        either1 shouldBe Right(true)
        either2 shouldBe Left(error)
      }

      "support ElasticResult operations - fold" in {
        // Given - Success
        refreshApi.executeRefreshResult = ElasticSuccess(true)
        val folded1 = refreshApi
          .refresh("my-index")
          .fold(
            error => s"Error: ${error.message}",
            success => if (success) "Success" else "Failed"
          )

        // Given - Failure
        refreshApi.executeRefreshResult = ElasticFailure(ElasticError("Network error"))
        val folded2 = refreshApi
          .refresh("my-index")
          .fold(
            error => s"Error: ${error.message}",
            success => if (success) "Success" else "Failed"
          )

        // Then
        folded1 shouldBe "Success"
        folded2 shouldBe "Error: Network error"
      }

      "support ElasticResult operations - foreach" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(true)
        var sideEffect: Option[Boolean] = None

        // When
        refreshApi.refresh("my-index").foreach { success =>
          sideEffect = Some(success)
        }

        // Then
        sideEffect shouldBe Some(true)
      }

      "support ElasticResult operations - filter" in {
        // Given - Success with filter passing
        refreshApi.executeRefreshResult = ElasticSuccess(true)
        val filtered1 = refreshApi.refresh("my-index").filter(_ == true, "Not refreshed")

        // Given - Success with filter failing
        refreshApi.executeRefreshResult = ElasticSuccess(false)
        val filtered2 = refreshApi.refresh("my-index").filter(_ == true, "Not refreshed")

        // Then
        filtered1.isSuccess shouldBe true
        filtered1.get shouldBe true

        filtered2.isFailure shouldBe true
        filtered2.error.get.message shouldBe "Not refreshed"
      }

      "throw exception when calling get on failure" in {
        // Given
        refreshApi.executeRefreshResult = ElasticFailure(ElasticError("Refresh failed"))

        // When & Then
        val exception = intercept[NoSuchElementException] {
          refreshApi.refresh("my-index").get
        }

        exception.getMessage should include("Refresh failed")
      }

      "handle multiple sequential refresh calls" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result1 = refreshApi.refresh("index1")
        val result2 = refreshApi.refresh("index2")
        val result3 = refreshApi.refresh("index3")

        // Then
        result1.isSuccess shouldBe true
        result2.isSuccess shouldBe true
        result3.isSuccess shouldBe true

        verify(mockLogger).debug("Refreshing index: index1")
        verify(mockLogger).debug("Refreshing index: index2")
        verify(mockLogger).debug("Refreshing index: index3")
        verify(mockLogger, times(3)).info(contains("refreshed successfully"))
      }

      "handle mixed success and failure scenarios" in {
        // Given - First call succeeds
        refreshApi.executeRefreshResult = ElasticSuccess(true)
        val result1 = refreshApi.refresh("index1")

        // Given - Second call fails
        refreshApi.executeRefreshResult = ElasticFailure(ElasticError("Error"))
        val result2 = refreshApi.refresh("index2")

        // Given - Third call succeeds
        refreshApi.executeRefreshResult = ElasticSuccess(false)
        val result3 = refreshApi.refresh("index3")

        // Then
        result1.isSuccess shouldBe true
        result1.get shouldBe true

        result2.isFailure shouldBe true

        result3.isSuccess shouldBe true
        result3.get shouldBe false

        verify(mockLogger, times(1)).info(contains("refreshed successfully"))
        verify(mockLogger, times(1)).error(contains("Failed to refresh"))
        verify(mockLogger, times(1)).info(contains("not refreshed"))
      }

      "trim index name before validation" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = refreshApi.refresh("  my-index  ")

        // Then
        result.isSuccess shouldBe true
        verify(mockLogger).debug("Refreshing index:   my-index  ")
      }

      "handle error with full context information" in {
        // Given
        val error = ElasticError(
          message = "Refresh timeout",
          cause = Some(new java.net.SocketTimeoutException()),
          statusCode = Some(504),
          index = Some("my-index"),
          operation = Some("executeRefresh")
        )
        refreshApi.executeRefreshResult = ElasticFailure(error)

        // When
        val result = refreshApi.refresh("my-index")

        // Then
        result.error.get.fullMessage should include("executeRefresh")
        result.error.get.fullMessage should include("my-index")
        result.error.get.fullMessage should include("504")
        result.error.get.fullMessage should include("Refresh timeout")
      }

      "not call executeRefresh when validation fails" in {
        // Given
        var executeCalled = false
        val validatingApi = new RefreshApi {
          override protected def logger: Logger = mockLogger
          override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = {
            executeCalled = true
            ElasticSuccess(true)
          }
        }

        // When
        validatingApi.refresh("INVALID")

        // Then
        executeCalled shouldBe false
        verify(mockLogger, never).debug(any[String])
        verify(mockLogger, never).info(any[String])
      }

      "call executeRefresh only after successful validation" in {
        // Given
        var executeCalled = false
        val validatingApi = new RefreshApi {
          override protected def logger: Logger = mockLogger
          override private[client] def executeRefresh(index: String): ElasticResult[Boolean] = {
            executeCalled = true
            ElasticSuccess(true)
          }
        }

        // When
        validatingApi.refresh("valid-index")

        // Then
        executeCalled shouldBe true
        verify(mockLogger).debug("Refreshing index: valid-index")
      }
    }

    "ElasticResult integration" should {

      "work with map transformation" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = refreshApi
          .refresh("my-index")
          .map(success => if (success) 1 else 0)
          .map(_ * 100)

        // Then
        result shouldBe ElasticSuccess(100)
      }

      "work with flatMap composition" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = refreshApi.refresh("my-index").flatMap { success =>
          if (success) ElasticSuccess("Index is fresh")
          else ElasticFailure(ElasticError("Index not refreshed"))
        }

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe "Index is fresh"
      }

      "propagate errors through transformations" in {
        // Given
        refreshApi.executeRefreshResult = ElasticFailure(ElasticError("Network error"))

        // When
        val result = refreshApi
          .refresh("my-index")
          .map(!_)
          .flatMap(v => ElasticSuccess(s"Result: $v"))
          .filter(_ => true)

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Network error"
      }

      "work with for-comprehension" in {
        // Given
        refreshApi.executeRefreshResult = ElasticSuccess(true)

        // When
        val result = for {
          refreshed <- refreshApi.refresh("index1")
          message = if (refreshed) "OK" else "KO"
        } yield message

        // Then
        result shouldBe ElasticSuccess("OK")
      }
    }
  }
}
