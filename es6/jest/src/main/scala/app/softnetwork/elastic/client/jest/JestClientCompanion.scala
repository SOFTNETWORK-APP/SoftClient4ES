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

package app.softnetwork.elastic.client.jest

import app.softnetwork.elastic.client.jest.interceptors.AuthHeaderInterceptor
import app.softnetwork.elastic.client.{
  ApiKeyAuth,
  BasicAuth,
  BearerTokenAuth,
  ElasticClientCompanion,
  ElasticCredentials,
  NoAuth
}
import com.sksamuel.exts.Logging
import com.typesafe.scalalogging.LazyLogging
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.Cat
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder

import java.util.concurrent.TimeUnit
import scala.jdk.CollectionConverters._
import scala.language.reflectiveCalls

/** Created by smanciot on 20/05/2021.
  */
trait JestClientCompanion extends ElasticClientCompanion[JestClient] with Logging {

  /** Create and configure Elasticsearch Client
    */
  override protected def createClient(): JestClient = {
    try {
      val factory = new CustomJestClientFactory(elasticConfig.credentials)
      factory.setHttpClientConfig(buildHttpConfig())
      factory.getObject
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to create JestClient: ${ex.getMessage}", ex)
        throw new IllegalStateException("Cannot create Elasticsearch client", ex)
    }
  }

  /** Test connection to Elasticsearch cluster
    *
    * @return
    *   true if connection is successful
    */
  override def testConnection(): Boolean = {
    try {
      val c = apply()
      val result = c.execute(new Cat.NodesBuilder().build())
      if (result.isSucceeded) {
        logger.info(s"Connected to Elasticsearch ${result.getJsonString}")
        true
      } else {
        logger.error(s"Failed to connect to Elasticsearch: ${result.getErrorMessage}")
        incrementFailures()
        false
      }
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to connect to Elasticsearch: ${ex.getMessage}", ex)
        incrementFailures()
        false
    }
  }

  private def buildHttpConfig(): HttpClientConfig = {
    val httpHosts =
      elasticConfig.credentials.url
        .split(",")
        .map(u => {
          parseHttpHost(u)
        })
        .toSet

    logger.info(s"🔧 Configuring Jest client for: ${elasticConfig.credentials.url}")

    val builder =
      new HttpClientConfig.Builder(elasticConfig.credentials.url)
        .multiThreaded(true) //elasticConfig.multithreaded
        .discoveryEnabled(elasticConfig.discovery.enabled)
        .discoveryFrequency(
          elasticConfig.discovery.frequency.getSeconds,
          TimeUnit.SECONDS
        )
        .connTimeout(elasticConfig.connectionTimeout.toMillis.toInt)
        .readTimeout(elasticConfig.socketTimeout.toMillis.toInt)
        .maxTotalConnection(100)
        .defaultMaxTotalConnectionPerRoute(50)

    // Configure authentication
    val configuredBuilder = elasticConfig.credentials.authMethod match {
      case Some(BasicAuth) if elasticConfig.credentials.username.nonEmpty =>
        logger.info(s"🔐 Configuring Basic Auth")
        builder
          .defaultCredentials(
            elasticConfig.credentials.username,
            elasticConfig.credentials.password
          )
          .preemptiveAuthTargetHosts(httpHosts.asJava)

      case Some(ApiKeyAuth) if elasticConfig.credentials.apiKey.exists(_.nonEmpty) =>
        logger.info("🔐 Configuring API Key Auth")
        builder

      case Some(BearerTokenAuth) if elasticConfig.credentials.bearerToken.exists(_.nonEmpty) =>
        logger.info("🔐 Configuring Bearer Token Auth")
        builder

      case Some(NoAuth) =>
        logger.warn("No authentication configured")
        builder

      case _ =>
        throw new IllegalStateException(
          s"Invalid authentication configuration: ${elasticConfig.credentials.authMethod}"
        )
    }

    configuredBuilder.build()
  }
}

class CustomJestClientFactory(credentials: ElasticCredentials)
    extends JestClientFactory
    with LazyLogging {

  override protected def configureHttpClient(builder: HttpClientBuilder): HttpClientBuilder = {
    credentials.authMethod match {
      case Some(ApiKeyAuth) | Some(BearerTokenAuth) =>
        logger.debug("Adding authentication interceptor to HTTP client")
        builder.addInterceptorLast(new AuthHeaderInterceptor(credentials))

      case _ =>
        logger.debug("No custom authentication interceptor needed")
        builder
    }
  }

  override protected def configureHttpClient(
    builder: HttpAsyncClientBuilder
  ): HttpAsyncClientBuilder = {
    credentials.authMethod match {
      case Some(ApiKeyAuth) | Some(BearerTokenAuth) =>
        logger.debug("Adding authentication interceptor to async HTTP client")
        builder.addInterceptorLast(new AuthHeaderInterceptor(credentials))

      case _ =>
        logger.debug("No custom authentication interceptor needed")
        builder
    }
  }
}
