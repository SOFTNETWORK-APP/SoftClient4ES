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

package app.softnetwork.elastic.licensing

import com.fasterxml.jackson.databind.ObjectMapper
import com.sun.net.httpserver.{HttpExchange, HttpHandler, HttpServer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.InetSocketAddress

class ApiKeyClientSpec extends AnyFlatSpec with Matchers {

  private val mapper = new ObjectMapper()

  // --- API key format validation ---

  "ApiKeyClient with invalid API key (no prefix)" should "return InvalidLicense immediately" in {
    val client = new ApiKeyClient(baseUrl = "http://localhost:1", instanceId = "test")
    val result = client.fetchJwt("abc123")
    result shouldBe Left(InvalidLicense("Invalid API key format"))
  }

  "ApiKeyClient with empty API key" should "return InvalidLicense immediately" in {
    val client = new ApiKeyClient(baseUrl = "http://localhost:1", instanceId = "test")
    val result = client.fetchJwt("")
    result shouldBe Left(InvalidLicense("Invalid API key format"))
  }

  "ApiKeyClient with bare prefix (sk-)" should "return InvalidLicense immediately" in {
    val client = new ApiKeyClient(baseUrl = "http://localhost:1", instanceId = "test")
    val result = client.fetchJwt("sk-")
    result shouldBe Left(InvalidLicense("Invalid API key format"))
  }

  "ApiKeyClient with minimal valid key (sk-x)" should "proceed to HTTP call" in {
    // This will fail with a network error since localhost:1 is not listening,
    // but it proves the format validation passed
    val client = new ApiKeyClient(
      baseUrl = "http://localhost:1",
      instanceId = "test",
      connectTimeoutMs = 500
    )
    val result = client.fetchJwt("sk-x")
    result.isLeft shouldBe true
    result.left.get shouldBe a[InvalidLicense]
    result.left.get.asInstanceOf[InvalidLicense].reason should startWith("Network error:")
  }

  // --- HTTP 200 success ---

  "ApiKeyClient on HTTP 200 with valid JWT" should "return Right(jwt)" in {
    withServer { (server, port) =>
      var capturedAuth: String = null
      var capturedContentType: String = null
      var capturedBody: String = null

      server.createContext(
        ApiKeyClient.TokenPath,
        new HttpHandler {
          def handle(exchange: HttpExchange): Unit = {
            capturedAuth = exchange.getRequestHeaders.getFirst("Authorization")
            capturedContentType = exchange.getRequestHeaders.getFirst("Content-Type")
            capturedBody =
              scala.io.Source.fromInputStream(exchange.getRequestBody, "UTF-8").mkString
            val response =
              """{"jwt": "eyJhbGciOiJFZERTQSJ9.test.sig", "expires_in": 86400, "message": null}"""
            val bytes = response.getBytes("UTF-8")
            exchange.sendResponseHeaders(200, bytes.length)
            exchange.getResponseBody.write(bytes)
            exchange.getResponseBody.close()
          }
        }
      )

      val client =
        new ApiKeyClient(baseUrl = s"http://localhost:$port", instanceId = "test-instance")
      val result = client.fetchJwt("sk-test-key")

      result shouldBe Right("eyJhbGciOiJFZERTQSJ9.test.sig")
      capturedAuth shouldBe "Bearer sk-test-key"
      capturedContentType shouldBe "application/json"

      // Verify request body contains instance_id and version
      val bodyTree = mapper.readTree(capturedBody)
      bodyTree.has("instance_id") shouldBe true
      bodyTree.get("instance_id").asText() shouldBe "test-instance"
      bodyTree.has("version") shouldBe true
    }
  }

  "ApiKeyClient on HTTP 200 with non-null message" should "return Right(jwt)" in {
    withServer { (server, port) =>
      server.createContext(
        ApiKeyClient.TokenPath,
        jsonHandler(
          200,
          """{"jwt": "eyJ.test", "expires_in": 86400, "message": "Your license expires in 7 days"}"""
        )
      )

      val client = new ApiKeyClient(baseUrl = s"http://localhost:$port", instanceId = "test")
      val result = client.fetchJwt("sk-test")
      result shouldBe Right("eyJ.test")
    }
  }

  // --- HTTP 200 missing jwt ---

  "ApiKeyClient on HTTP 200 with missing jwt field" should "return InvalidLicense" in {
    withServer { (server, port) =>
      server.createContext(
        ApiKeyClient.TokenPath,
        jsonHandler(200, """{"message": "ok"}""")
      )

      val client = new ApiKeyClient(baseUrl = s"http://localhost:$port", instanceId = "test")
      val result = client.fetchJwt("sk-test")
      result shouldBe Left(InvalidLicense("Missing jwt in response"))
    }
  }

  "ApiKeyClient on HTTP 200 with null jwt" should "return InvalidLicense" in {
    withServer { (server, port) =>
      server.createContext(
        ApiKeyClient.TokenPath,
        jsonHandler(200, """{"jwt": null}""")
      )

      val client = new ApiKeyClient(baseUrl = s"http://localhost:$port", instanceId = "test")
      val result = client.fetchJwt("sk-test")
      result shouldBe Left(InvalidLicense("Missing jwt in response"))
    }
  }

  // --- HTTP 401 ---

  "ApiKeyClient on HTTP 401" should "return InvalidLicense with backend message" in {
    withServer { (server, port) =>
      server.createContext(
        ApiKeyClient.TokenPath,
        jsonHandler(
          401,
          """{"error": "api_key_revoked", "message": "API key has been revoked"}"""
        )
      )

      val client = new ApiKeyClient(baseUrl = s"http://localhost:$port", instanceId = "test")
      val result = client.fetchJwt("sk-test")
      result shouldBe Left(InvalidLicense("API key has been revoked"))
    }
  }

  "ApiKeyClient on HTTP 401 with malformed body" should "return fallback message" in {
    withServer { (server, port) =>
      server.createContext(
        ApiKeyClient.TokenPath,
        jsonHandler(401, "not json")
      )

      val client = new ApiKeyClient(baseUrl = s"http://localhost:$port", instanceId = "test")
      val result = client.fetchJwt("sk-test")
      result shouldBe Left(InvalidLicense("API key rejected (HTTP 401)"))
    }
  }

  // --- Unexpected HTTP status ---

  "ApiKeyClient on HTTP 500" should "return InvalidLicense with status code" in {
    withServer { (server, port) =>
      server.createContext(
        ApiKeyClient.TokenPath,
        jsonHandler(500, """{"error": "internal"}""")
      )

      val client = new ApiKeyClient(baseUrl = s"http://localhost:$port", instanceId = "test")
      val result = client.fetchJwt("sk-test")
      result shouldBe Left(InvalidLicense("Unexpected HTTP status: 500"))
    }
  }

  // --- Network error ---

  "ApiKeyClient with unreachable server" should "return InvalidLicense with network error" in {
    // Start server, capture port, stop immediately -> guaranteed ConnectException
    val server = HttpServer.create(new InetSocketAddress(0), 0)
    server.start()
    val port = server.getAddress.getPort
    server.stop(0)

    val client = new ApiKeyClient(baseUrl = s"http://localhost:$port", instanceId = "test")
    val result = client.fetchJwt("sk-test")
    result.isLeft shouldBe true
    result.left.get.asInstanceOf[InvalidLicense].reason should startWith("Network error:")
  }

  // --- Timeout ---

  "ApiKeyClient with slow server" should "return InvalidLicense on timeout" in {
    val timeout = 100
    withServer { (server, port) =>
      server.createContext(
        ApiKeyClient.TokenPath,
        new HttpHandler {
          def handle(exchange: HttpExchange): Unit = {
            Thread.sleep(timeout * 2)
            val bytes = "{}".getBytes("UTF-8")
            exchange.sendResponseHeaders(200, bytes.length)
            exchange.getResponseBody.write(bytes)
            exchange.getResponseBody.close()
          }
        }
      )

      val client = new ApiKeyClient(
        baseUrl = s"http://localhost:$port",
        instanceId = "test",
        readTimeoutMs = timeout
      )
      val result = client.fetchJwt("sk-test")
      result.isLeft shouldBe true
      result.left.get.asInstanceOf[InvalidLicense].reason should startWith("Network error:")
    }
  }

  // --- Helpers ---

  private def withServer(f: (HttpServer, Int) => Unit): Unit = {
    val server = HttpServer.create(new InetSocketAddress(0), 0)
    server.start()
    val port = server.getAddress.getPort
    try {
      f(server, port)
    } finally {
      server.stop(0)
    }
  }

  private def jsonHandler(statusCode: Int, body: String): HttpHandler = {
    new HttpHandler {
      def handle(exchange: HttpExchange): Unit = {
        // Consume request body to avoid broken pipe
        val is = exchange.getRequestBody
        while (is.read() != -1) {}
        is.close()

        val bytes = body.getBytes("UTF-8")
        if (statusCode >= 400) {
          exchange.sendResponseHeaders(statusCode, bytes.length)
          exchange.getResponseBody.write(bytes)
        } else {
          exchange.sendResponseHeaders(statusCode, bytes.length)
          exchange.getResponseBody.write(bytes)
        }
        exchange.getResponseBody.close()
      }
    }
  }
}
