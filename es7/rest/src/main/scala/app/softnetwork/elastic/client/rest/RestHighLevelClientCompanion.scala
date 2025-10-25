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

import app.softnetwork.elastic.client.ElasticConfig
import org.elasticsearch.client.{RequestOptions, RestClient, RestClientBuilder, RestHighLevelClient}
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.elasticsearch.search.SearchModule
import org.elasticsearch.common.settings.Settings
import org.elasticsearch.plugins.SearchPlugin
import org.elasticsearch.xcontent.NamedXContentRegistry
import org.slf4j.{Logger, LoggerFactory}

import java.net.URI

//import scala.jdk.CollectionConverters._
import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}
import java.io.Closeable

/** Thread-safe companion for RestHighLevelClient with lazy initialization and proper resource
  * management
  */
trait RestHighLevelClientCompanion extends Closeable {

  val logger: Logger = LoggerFactory getLogger getClass.getName

  def elasticConfig: ElasticConfig

  /** Lazy-initialized NamedXContentRegistry (thread-safe by Scala lazy val)
    */
  lazy val namedXContentRegistry: NamedXContentRegistry = {
    val searchModule = new SearchModule(Settings.EMPTY, false, List.empty[SearchPlugin].asJava)
    new NamedXContentRegistry(searchModule.getNamedXContents)
  }

  /** Thread-safe client instance using double-checked locking pattern
    * @volatile
    *   ensures visibility across threads
    */
  @volatile private var client: Option[RestHighLevelClient] = None

  /** Lock object for synchronized initialization
    */
  private val lock = new Object()

  /** Get or create RestHighLevelClient instance (thread-safe, lazy initialization) Uses
    * double-checked locking for optimal performance
    *
    * @return
    *   RestHighLevelClient instance
    * @throws IllegalStateException
    *   if client creation fails
    */
  def apply(): RestHighLevelClient = {
    // First check (no locking) - fast path for already initialized client
    client match {
      case Some(c) => c
      case None    =>
        // Second check with lock - slow path for initialization
        lock.synchronized {
          client match {
            case Some(c) =>
              c // Another thread initialized while we were waiting
            case None =>
              val c = createClient()
              client = Some(c)
              logger.info(s"RestHighLevelClient initialized for ${elasticConfig.credentials.url}")
              c
          }
        }
    }
  }

  /** Create and configure RestHighLevelClient Separated for better testability and error handling
    */
  private def createClient(): RestHighLevelClient = {
    try {
      val restClientBuilder = buildRestClient()
      new RestHighLevelClient(restClientBuilder)
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to create RestHighLevelClient: ${ex.getMessage}", ex)
        throw new IllegalStateException("Cannot create Elasticsearch client", ex)
    }
  }

  /** Build RestClientBuilder with credentials and configuration
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

    // Add credentials if provided
    if (elasticConfig.credentials.username.nonEmpty) {
      builder.setHttpClientConfigCallback { httpClientBuilder =>
        val credentialsProvider = new BasicCredentialsProvider()
        credentialsProvider.setCredentials(
          AuthScope.ANY,
          new UsernamePasswordCredentials(
            elasticConfig.credentials.username,
            elasticConfig.credentials.password
          )
        )
        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
      }
    } else {
      builder
    }
  }

  /** Parse and validate HTTP host from URL string
    * @throws IllegalArgumentException
    *   if URL is invalid
    */
  private def parseHttpHost(url: String): HttpHost = {
    // Validation de l'URL
    validateUrl(url) match {
      case Success(_) =>
        Try(HttpHost.create(url)) match {
          case Success(host) =>
            host
          case Failure(ex) =>
            logger.error(s"Failed to parse Elasticsearch URL: $url", ex)
            throw new IllegalArgumentException(s"Invalid Elasticsearch URL: $url", ex)
        }
      case Failure(ex) =>
        logger.error(s"Invalid Elasticsearch URL: $url", ex)
        throw new IllegalArgumentException(s"Invalid Elasticsearch URL format: $url", ex)
    }
  }

  /** Validate URL format using java.net.URI
    */
  private def validateUrl(url: String): Try[URI] = {
    Try {
      if (url == null || url.trim.isEmpty) {
        throw new IllegalArgumentException("URL cannot be null or empty")
      }

      val uri = new URI(url)

      // Vérifier le schéma
      if (uri.getScheme == null) {
        throw new IllegalArgumentException(
          s"URL must have a scheme (http:// or https://): $url"
        )
      }

      val scheme = uri.getScheme.toLowerCase
      if (scheme != "http" && scheme != "https") {
        throw new IllegalArgumentException(
          s"URL scheme must be http or https, got: $scheme"
        )
      }

      // Vérifier l'hôte
      if (uri.getHost == null || uri.getHost.trim.isEmpty) {
        throw new IllegalArgumentException(
          s"URL must have a valid hostname: $url"
        )
      }

      // Vérifier le port si présent
      if (uri.getPort != -1) {
        if (uri.getPort < 0 || uri.getPort > 65535) {
          throw new IllegalArgumentException(
            s"Invalid port number: ${uri.getPort} (must be between 0 and 65535)"
          )
        }
      }

      uri
    }
  }

  /** Check if client is initialized and connected
    */
  def isInitialized: Boolean = client.isDefined

  /** Test connection to Elasticsearch cluster
    * @return
    *   true if connection is successful
    */
  def testConnection(): Boolean = {
    Try {
      val c = apply()
      val response = c.info(RequestOptions.DEFAULT)
      logger.info(s"Connected to Elasticsearch ${response.getVersion.getNumber}")
      true
    } match {
      case Success(result) => result
      case Failure(ex) =>
        logger.error(s"Connection test failed: ${ex.getMessage}", ex)
        false
    }
  }

  /** Close the client and release resources Idempotent - safe to call multiple times
    */
  override def close(): Unit = {
    lock.synchronized {
      client.foreach { c =>
        Try {
          c.close()
          logger.info("RestHighLevelClient closed successfully")
        }.recover { case ex: Exception =>
          logger.warn(s"Error closing RestHighLevelClient: ${ex.getMessage}", ex)
        }
        client = None
      }
    }
  }

  /** Reset client (force reconnection on next access) Useful for connection recovery scenarios
    */
  def reset(): Unit = {
    logger.info("Resetting RestHighLevelClient")
    close()
  }
}
