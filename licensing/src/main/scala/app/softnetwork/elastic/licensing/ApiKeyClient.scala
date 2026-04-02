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

import app.softnetwork.elastic.LicensingBuildInfo
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.scalalogging.LazyLogging

import java.io.IOException
import java.net.{HttpURLConnection, SocketTimeoutException, URL}

import scala.jdk.CollectionConverters._

class ApiKeyClient(
  baseUrl: String,
  instanceId: String,
  connectTimeoutMs: Int = 10000,
  readTimeoutMs: Int = 10000
) extends LazyLogging {

  import ApiKeyClient._

  def fetchJwt(apiKey: String): Either[LicenseError, String] = {
    // Step 1: Validate API key format
    if (!apiKey.startsWith("sk-") || apiKey.length <= 3) {
      return Left(InvalidLicense("Invalid API key format"))
    }

    try {
      // Step 2: Build request body
      val body = mapper.writeValueAsString(
        Map("instance_id" -> instanceId, "version" -> LicensingBuildInfo.version).asJava
      )

      // Step 3: HTTP POST
      val url = new URL(baseUrl + TokenPath)
      val conn = url.openConnection().asInstanceOf[HttpURLConnection]
      try {
        conn.setRequestMethod("POST")
        conn.setRequestProperty("Authorization", s"Bearer $apiKey")
        conn.setRequestProperty("Content-Type", "application/json")
        conn.setRequestProperty("Accept", "application/json")
        conn.setConnectTimeout(connectTimeoutMs)
        conn.setReadTimeout(readTimeoutMs)
        conn.setInstanceFollowRedirects(false)
        conn.setDoOutput(true)

        val bodyBytes = body.getBytes("UTF-8")
        val os = conn.getOutputStream
        try {
          os.write(bodyBytes)
        } finally {
          os.close()
        }

        // Step 4: Read response
        val code = conn.getResponseCode
        code match {
          case 200 =>
            val responseBody = readStream(conn.getInputStream)
            val tree = mapper.readTree(responseBody)
            val jwtNode = tree.get("jwt")
            if (jwtNode == null || jwtNode.isNull || !jwtNode.isTextual) {
              Left(InvalidLicense("Missing jwt in response"))
            } else {
              val messageNode = tree.get("message")
              if (messageNode != null && !messageNode.isNull && messageNode.isTextual) {
                logger.info(messageNode.asText())
              }
              Right(jwtNode.asText())
            }

          case 401 =>
            val errorBody = readStream(conn.getErrorStream)
            val message =
              try {
                val tree = mapper.readTree(errorBody)
                val msgNode = tree.get("message")
                if (msgNode != null && msgNode.isTextual) msgNode.asText()
                else "API key rejected (HTTP 401)"
              } catch {
                case _: Exception => "API key rejected (HTTP 401)"
              }
            logger.error(message)
            Left(InvalidLicense(message))

          case other =>
            Left(InvalidLicense(s"Unexpected HTTP status: $other"))
        }
      } finally {
        conn.disconnect()
      }
    } catch {
      case e @ (_: SocketTimeoutException | _: IOException) =>
        logger.warn(s"Network error fetching license: ${e.getMessage}")
        Left(InvalidLicense(s"Network error: ${e.getMessage}"))
    }
  }

  private def readStream(is: java.io.InputStream): String = {
    if (is == null) return ""
    val source = scala.io.Source.fromInputStream(is, "UTF-8")
    try {
      source.mkString
    } finally {
      source.close()
      is.close()
    }
  }
}

object ApiKeyClient {
  private[licensing] val TokenPath: String = "/api/v1/licenses/token"
  private val mapper: ObjectMapper = new ObjectMapper()
}
