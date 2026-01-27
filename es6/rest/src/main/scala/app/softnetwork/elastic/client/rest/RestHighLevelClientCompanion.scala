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

package app.softnetwork.elastic.client.rest

import app.softnetwork.elastic.client.{
  ApiKeyAuth,
  BasicAuth,
  BearerTokenAuth,
  ElasticClientCompanion
}
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.message.BasicHeader
import org.elasticsearch.search.SearchModule
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.common.xcontent.NamedXContentRegistry
import org.elasticsearch.plugins.SearchPlugin
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success, Try}

/** Thread-safe companion for RestHighLevelClient with lazy initialization and proper resource
  * management
  */
trait RestHighLevelClientCompanion extends ElasticClientCompanion[RestHighLevelClient] {

  val logger: Logger = LoggerFactory getLogger getClass.getName

  /** Lazy-initialized NamedXContentRegistry (thread-safe by Scala lazy val)
    */
  lazy val namedXContentRegistry: NamedXContentRegistry = {
    val searchModule = new SearchModule(Settings.EMPTY, false, List.empty[SearchPlugin].asJava)
    new NamedXContentRegistry(searchModule.getNamedXContents)
  }

  /** Create and configure RestHighLevelClient Separated for better testability and error handling
    */
  override protected def createClient(): RestHighLevelClient = {
    try {
      val restClientBuilder = buildRestClient()
      new RestHighLevelClient(restClientBuilder)
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to create RestHighLevelClient: ${ex.getMessage}", ex)
        throw new IllegalStateException("Cannot create Elasticsearch client", ex)
    }
  }

  /** Build RestClientBuilder with authentication
    */
  private def buildRestClient(): RestClientBuilder = {
    val httpHost = parseHttpHost(elasticConfig.credentials.url)

    val builder = RestClient
      .builder(httpHost)
      .setRequestConfigCallback { requestConfigBuilder =>
        requestConfigBuilder
          .setConnectTimeout(elasticConfig.connectionTimeout.toMillis.toInt)
          .setSocketTimeout(elasticConfig.socketTimeout.toMillis.toInt)
      }

    // Authenticate
    elasticConfig.credentials.authMethod match {
      case Some(BasicAuth) if elasticConfig.credentials.username.nonEmpty =>
        builder.setHttpClientConfigCallback { httpClientConfigCallback =>
          val credentialsProvider = new BasicCredentialsProvider()
          credentialsProvider.setCredentials(
            AuthScope.ANY,
            new UsernamePasswordCredentials(
              elasticConfig.credentials.username,
              elasticConfig.credentials.password
            )
          )
          httpClientConfigCallback.setDefaultCredentialsProvider(credentialsProvider)
        }
      case Some(ApiKeyAuth) if elasticConfig.credentials.encodedApiKey.exists(_.nonEmpty) =>
        val apiKey = elasticConfig.credentials.encodedApiKey.get
        builder.setHttpClientConfigCallback { httpClientConfigCallback =>
          httpClientConfigCallback.setDefaultHeaders(
            Seq(
              new BasicHeader(
                "Authorization",
                s"ApiKey $apiKey"
              )
            ).asJava
          )
        }
      case Some(BearerTokenAuth) if elasticConfig.credentials.bearerToken.exists(_.nonEmpty) =>
        val bearerToken = elasticConfig.credentials.bearerToken.getOrElse("")
        builder.setHttpClientConfigCallback { httpClientConfigCallback =>
          httpClientConfigCallback.setDefaultHeaders(
            Seq(
              new BasicHeader(
                "Authorization",
                s"Bearer $bearerToken"
              )
            ).asJava
          )
        }
      case _ => // No authentication
        builder
    }
  }

  /** Test connection to Elasticsearch cluster
    * @return
    *   true if connection is successful
    */
  override def testConnection(): Boolean = {
    Try {
      val c = apply()
      val response = c.info(RequestOptions.DEFAULT)
      logger.info(s"Connected to Elasticsearch ${response.getVersion}")
      true
    } match {
      case Success(result) => result
      case Failure(ex) =>
        logger.error(s"Connection test failed: ${ex.getMessage}", ex)
        incrementFailures()
        false
    }
  }

}
