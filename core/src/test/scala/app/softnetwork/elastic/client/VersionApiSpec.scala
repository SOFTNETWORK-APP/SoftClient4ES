package app.softnetwork.elastic.client

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach
import org.mockito.MockitoSugar
import org.mockito.ArgumentMatchersSugar
import org.slf4j.Logger
import app.softnetwork.elastic.client.result._

/** Unit tests for VersionApi
  */
class VersionApiSpec
    extends AnyWordSpec
    with Matchers
    with BeforeAndAfterEach
    with MockitoSugar
    with ArgumentMatchersSugar {

  // Mock logger
  val mockLogger: Logger = mock[Logger]

  // Concrete implementation for testing
  class TestVersionApi extends VersionApi with SerializationApi {
    override protected def logger: Logger = mockLogger

    // Variable to control the behavior of executeVersion
    var executeVersionResult: ElasticResult[String] = ElasticSuccess("8.11.0")

    override private[client] def executeVersion(): ElasticResult[String] = {
      executeVersionResult
    }
  }

  var versionApi: TestVersionApi = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    versionApi = new TestVersionApi()
    reset(mockLogger)
  }

  "VersionApi" should {

    "version" should {

      "return version on first call and cache it" in {
        // Given
        versionApi.executeVersionResult = ElasticSuccess("8.11.0")

        // When
        val result1 = versionApi.version
        val result2 = versionApi.version

        // Then
        result1 shouldBe ElasticSuccess("8.11.0")
        result2 shouldBe ElasticSuccess("8.11.0")
        result1.isSuccess shouldBe true
        result1.toOption shouldBe Some("8.11.0")

        // Should log success only once (first call)
        verify(mockLogger, times(1)).info(contains("✅ Elasticsearch version: 8.11.0"))
        verify(mockLogger, never).error(any[String])
      }

      "cache version after successful first call" in {
        // Given
        versionApi.executeVersionResult = ElasticSuccess("7.17.0")

        // When - First call
        val result1 = versionApi.version

        // Change the result (simulating that executeVersion would return something different)
        versionApi.executeVersionResult = ElasticSuccess("8.0.0")

        // Second call should still return cached version
        val result2 = versionApi.version

        // Then
        result1 shouldBe ElasticSuccess("7.17.0")
        result2 shouldBe ElasticSuccess("7.17.0") // Still cached!

        verify(mockLogger, times(1)).info(contains("7.17.0"))
      }

      "return failure when executeVersion fails" in {
        // Given
        val error = ElasticError(
          message = "Connection refused",
          cause = Some(new java.net.ConnectException("Connection refused")),
          statusCode = Some(503),
          operation = Some("executeVersion")
        )
        versionApi.executeVersionResult = ElasticFailure(error)

        // When
        val result = versionApi.version

        // Then
        result shouldBe ElasticFailure(error)
        result.isSuccess shouldBe false
        result.isFailure shouldBe true
        result.toOption shouldBe None
        result.error shouldBe Some(error)

        verify(mockLogger, times(1)).error(
          contains("❌ Failed to get Elasticsearch version: Connection refused")
        )
        verify(mockLogger, never).info(any[String])
      }

      "not cache version when executeVersion fails" in {
        // Given - First call fails
        versionApi.executeVersionResult = ElasticFailure(
          ElasticError("Connection timeout", statusCode = Some(503))
        )

        // When - First call
        val result1 = versionApi.version

        // Given - Second call succeeds
        versionApi.executeVersionResult = ElasticSuccess("8.11.0")

        // When - Second call
        val result2 = versionApi.version

        // Then
        result1.isFailure shouldBe true
        result2 shouldBe ElasticSuccess("8.11.0")

        verify(mockLogger, times(1)).error(contains("Connection timeout"))
        verify(mockLogger, times(1)).info(contains("8.11.0"))
      }

      "handle different version formats" in {
        // Given
        val versions = Seq(
          "7.17.0",
          "8.0.0",
          "8.11.0",
          "8.11.1-SNAPSHOT"
        )

        versions.foreach { version =>
          // Reset API for each test
          versionApi = new TestVersionApi()
          versionApi.executeVersionResult = ElasticSuccess(version)

          // When
          val result = versionApi.version

          // Then
          result shouldBe ElasticSuccess(version)
          result.get shouldBe version
        }
      }

      "handle authentication error" in {
        // Given
        val error = ElasticError(
          message = "Authentication failed",
          statusCode = Some(401),
          operation = Some("executeVersion")
        )
        versionApi.executeVersionResult = ElasticFailure(error)

        // When
        val result = versionApi.version

        // Then
        result.isFailure shouldBe true
        result.error.get.statusCode shouldBe Some(401)
        result.error.get.message shouldBe "Authentication failed"

        verify(mockLogger).error(contains("Authentication failed"))
      }

      "handle timeout error" in {
        // Given
        val error = ElasticError(
          message = "Request timeout after 30s",
          cause = Some(new java.net.SocketTimeoutException("Read timed out")),
          statusCode = Some(504),
          operation = Some("executeVersion")
        )
        versionApi.executeVersionResult = ElasticFailure(error)

        // When
        val result = versionApi.version

        // Then
        result.isFailure shouldBe true
        result.error.get.cause shouldBe defined
        result.error.get.cause.get shouldBe a[java.net.SocketTimeoutException]

        verify(mockLogger).error(contains("Request timeout"))
      }

      "handle network error" in {
        // Given
        val error = ElasticError(
          message = "No route to host",
          cause = Some(new java.net.NoRouteToHostException("No route to host")),
          statusCode = Some(503),
          operation = Some("executeVersion")
        )
        versionApi.executeVersionResult = ElasticFailure(error)

        // When
        val result = versionApi.version

        // Then
        result.isFailure shouldBe true
        result.getOrElse("default") shouldBe "default"

        verify(mockLogger).error(contains("No route to host"))
      }

      "handle malformed response error" in {
        // Given
        val error = ElasticError(
          message = "Invalid JSON response",
          cause =
            Some(new com.fasterxml.jackson.core.JsonParseException(null, "Unexpected character")),
          statusCode = Some(500),
          operation = Some("executeVersion")
        )
        versionApi.executeVersionResult = ElasticFailure(error)

        // When
        val result = versionApi.version

        // Then
        result.isFailure shouldBe true
        result.error.get.cause.get shouldBe a[com.fasterxml.jackson.core.JsonParseException]
      }

      "support ElasticResult operations - map" in {
        // Given
        versionApi.executeVersionResult = ElasticSuccess("8.11.0")

        // When
        val result = versionApi.version.map(v => s"Version: $v")

        // Then
        result shouldBe ElasticSuccess("Version: 8.11.0")
        result.get shouldBe "Version: 8.11.0"
      }

      "support ElasticResult operations - flatMap" in {
        // Given
        versionApi.executeVersionResult = ElasticSuccess("8.11.0")

        // When
        val result = versionApi.version.flatMap { v =>
          if (v.startsWith("8")) ElasticSuccess(s"Version $v is ES 8.x")
          else ElasticFailure(ElasticError("Not ES 8.x"))
        }

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe "Version 8.11.0 is ES 8.x"
      }

      "support ElasticResult operations - getOrElse" in {
        // Given - Success case
        versionApi.executeVersionResult = ElasticSuccess("8.11.0")
        val result1 = versionApi.version.getOrElse("unknown")

        // Given - Failure case
        versionApi = new TestVersionApi()
        versionApi.executeVersionResult = ElasticFailure(ElasticError("Error"))
        val result2 = versionApi.version.getOrElse("unknown")

        // Then
        result1 shouldBe "8.11.0"
        result2 shouldBe "unknown"
      }

      "support ElasticResult operations - toOption" in {
        // Given - Success
        versionApi.executeVersionResult = ElasticSuccess("8.11.0")
        val option1 = versionApi.version.toOption

        // Given - Failure
        versionApi = new TestVersionApi()
        versionApi.executeVersionResult = ElasticFailure(ElasticError("Error"))
        val option2 = versionApi.version.toOption

        // Then
        option1 shouldBe Some("8.11.0")
        option2 shouldBe None
      }

      "support ElasticResult operations - toEither" in {
        // Given - Success
        versionApi.executeVersionResult = ElasticSuccess("8.11.0")
        val either1 = versionApi.version.toEither

        // Given - Failure
        versionApi = new TestVersionApi()
        val error = ElasticError("Error")
        versionApi.executeVersionResult = ElasticFailure(error)
        val either2 = versionApi.version.toEither

        // Then
        either1 shouldBe Right("8.11.0")
        either2 shouldBe Left(error)
      }

      "support ElasticResult operations - fold" in {
        // Given - Success
        versionApi.executeVersionResult = ElasticSuccess("8.11.0")
        val folded1 = versionApi.version.fold(
          error => s"Error: ${error.message}",
          version => s"Success: $version"
        )

        // Given - Failure
        versionApi = new TestVersionApi()
        versionApi.executeVersionResult = ElasticFailure(ElasticError("Connection failed"))
        val folded2 = versionApi.version.fold(
          error => s"Error: ${error.message}",
          version => s"Success: $version"
        )

        // Then
        folded1 shouldBe "Success: 8.11.0"
        folded2 shouldBe "Error: Connection failed"
      }

      "support ElasticResult operations - foreach" in {
        // Given
        versionApi.executeVersionResult = ElasticSuccess("8.11.0")
        var sideEffect: Option[String] = None

        // When
        versionApi.version.foreach { v =>
          sideEffect = Some(v)
        }

        // Then
        sideEffect shouldBe Some("8.11.0")
      }

      "support ElasticResult operations - filter" in {
        // Given - Success with filter passing
        versionApi.executeVersionResult = ElasticSuccess("8.11.0")
        val filtered1 = versionApi.version.filter(_.startsWith("8"), "Not ES 8.x")

        // Given - Success with filter failing
        versionApi = new TestVersionApi()
        versionApi.executeVersionResult = ElasticSuccess("7.17.0")
        val filtered2 = versionApi.version.filter(_.startsWith("8"), "Not ES 8.x")

        // Then
        filtered1.isSuccess shouldBe true
        filtered1.get shouldBe "8.11.0"

        filtered2.isFailure shouldBe true
        filtered2.error.get.message shouldBe "Not ES 8.x"
      }

      "throw exception when calling get on failure" in {
        // Given
        versionApi.executeVersionResult = ElasticFailure(ElasticError("Connection failed"))

        // When & Then
        val exception = intercept[NoSuchElementException] {
          versionApi.version.get
        }

        exception.getMessage should include("Connection failed")
      }

      "handle concurrent calls correctly" in {
        // Given
        versionApi.executeVersionResult = ElasticSuccess("8.11.0")

        // When - Simulate concurrent calls
        val results = (1 to 10).map(_ => versionApi.version)

        // Then - All should return the same cached version
        results.foreach { result =>
          result shouldBe ElasticSuccess("8.11.0")
        }

        // Should only log once (first call)
        verify(mockLogger, times(1)).info(contains("8.11.0"))
      }

      "handle error with full context" in {
        // Given
        val error = ElasticError(
          message = "Connection timeout",
          cause = Some(new java.net.SocketTimeoutException()),
          statusCode = Some(504),
          index = Some("my-index"),
          operation = Some("executeVersion")
        )
        versionApi.executeVersionResult = ElasticFailure(error)

        // When
        val result = versionApi.version

        // Then
        result.error.get.fullMessage should include("executeVersion")
        result.error.get.fullMessage should include("my-index")
        result.error.get.fullMessage should include("504")
        result.error.get.fullMessage should include("Connection timeout")
      }

      "handle empty version string" in {
        // Given
        versionApi.executeVersionResult = ElasticSuccess("")

        // When
        val result = versionApi.version

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe ""

        verify(mockLogger).info(contains(""))
      }

      "not call executeVersion on subsequent calls after success" in {
        // Given
        versionApi.executeVersionResult = ElasticSuccess("8.11.0")
        var callCount = 0

        // Override to count calls
        val countingApi = new VersionApi with SerializationApi {
          override protected def logger: Logger = mockLogger
          override private[client] def executeVersion(): ElasticResult[String] = {
            callCount += 1
            ElasticSuccess("8.11.0")
          }
        }

        // When
        countingApi.version
        countingApi.version
        countingApi.version

        // Then
        callCount shouldBe 1 // Only called once!
      }

      "call executeVersion on each call after failure" in {
        // Given
        var callCount = 0

        val failingApi = new VersionApi with SerializationApi {
          override protected def logger: Logger = mockLogger
          override private[client] def executeVersion(): ElasticResult[String] = {
            callCount += 1
            ElasticFailure(ElasticError("Always fails"))
          }
        }

        // When
        failingApi.version
        failingApi.version
        failingApi.version

        // Then
        callCount shouldBe 3 // Called every time!
      }
    }

    "ElasticResult integration" should {

      "work with map transformation" in {
        // Given
        versionApi.executeVersionResult = ElasticSuccess("8.11.0")

        // When
        val result = versionApi.version
          .map(_.split("\\.").head.toInt)
          .map(_ >= 8)

        // Then
        result shouldBe ElasticSuccess(true)
      }

      "work with flatMap composition" in {
        // Given
        versionApi.executeVersionResult = ElasticSuccess("8.11.0")

        // When
        val result = versionApi.version.flatMap { version =>
          val major = version.split("\\.").head.toInt
          if (major >= 8) ElasticSuccess(s"Compatible: $version")
          else ElasticFailure(ElasticError(s"Incompatible version: $version"))
        }

        // Then
        result.isSuccess shouldBe true
        result.get shouldBe "Compatible: 8.11.0"
      }

      "propagate errors through transformations" in {
        // Given
        versionApi.executeVersionResult = ElasticFailure(ElasticError("Network error"))

        // When
        val result = versionApi.version
          .map(_.toUpperCase)
          .flatMap(v => ElasticSuccess(s"Version: $v"))
          .filter(_.contains("8"))

        // Then
        result.isFailure shouldBe true
        result.error.get.message shouldBe "Network error"
      }
    }
  }
}
