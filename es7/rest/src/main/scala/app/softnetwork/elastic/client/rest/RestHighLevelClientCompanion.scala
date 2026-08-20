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
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.message.BasicHeader
import org.elasticsearch.search.SearchModule
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.plugins.SearchPlugin
import org.elasticsearch.xcontent.NamedXContentRegistry
import org.slf4j.{Logger, LoggerFactory}

import java.util.Base64
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

  /** REST connection pool sized to the slice ceiling (#238 — `ScrollSettings.restPoolPerRoute` /
    * `restPoolTotal`): an extraction may hold up to `elastic.scroll.max-slices` page requests in
    * flight per route on top of the PIT open / close and `_settings` calls sharing the route.
    */
  private def withPoolSizing(httpClient: HttpAsyncClientBuilder): HttpAsyncClientBuilder =
    httpClient
      .setMaxConnPerRoute(elasticConfig.scroll.restPoolPerRoute)
      .setMaxConnTotal(elasticConfig.scroll.restPoolTotal)

  /** Build RestClientBuilder with authentication. ONE `setHttpClientConfigCallback` per builder (a
    * second call replaces the first): the auth branch yields a function and the pool sizing is
    * composed with it in a single callback.
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
    val authenticate: HttpAsyncClientBuilder => HttpAsyncClientBuilder =
      elasticConfig.credentials.authMethod match {
        case Some(BasicAuth) if elasticConfig.credentials.username.nonEmpty =>
          httpClientConfigCallback => {
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
          httpClientConfigCallback =>
            httpClientConfigCallback.setDefaultHeaders(
              Seq(
                new BasicHeader(
                  "Authorization",
                  ApiKeyAuth.createAuthHeader(elasticConfig.credentials)
                )
              ).asJava
            )
        case Some(BearerTokenAuth) if elasticConfig.credentials.bearerToken.exists(_.nonEmpty) =>
          httpClientConfigCallback =>
            httpClientConfigCallback.setDefaultHeaders(
              Seq(
                new BasicHeader(
                  "Authorization",
                  BearerTokenAuth.createAuthHeader(elasticConfig.credentials)
                )
              ).asJava
            )
        case _ => // No authentication
          identity
      }

    builder.setHttpClientConfigCallback(httpClientConfigCallback =>
      withPoolSizing(authenticate(httpClientConfigCallback))
    )
  }

  /** Test connection to Elasticsearch cluster
    * @return
    *   true if connection is successful
    */
  override def testConnection(): Boolean = {
    Try {
      val c = apply()
      val response = c.info(RequestOptions.DEFAULT)
      logger.info(s"Connected to Elasticsearch ${response.getVersion.getNumber}")
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
