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

package app.softnetwork.elastic.client.java

import app.softnetwork.elastic.client.{
  ApiKeyAuth,
  BasicAuth,
  BearerTokenAuth,
  ElasticClientCompanion
}
import co.elastic.clients.elasticsearch.{ElasticsearchAsyncClient, ElasticsearchClient}
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.apache.http.message.BasicHeader
import org.elasticsearch.client.{RestClient, RestClientBuilder}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.{CompletableFuture, CompletionException, ExecutionException}
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Future, Promise}
import scala.util.Try

import scala.jdk.CollectionConverters._

trait JavaClientCompanion extends ElasticClientCompanion[ElasticsearchClient] {

  val logger: Logger = LoggerFactory getLogger getClass.getName

  private val asyncRef = new AtomicReference[Option[ElasticsearchAsyncClient]](None)

  def async(): ElasticsearchAsyncClient = {
    asyncRef.get() match {
      case Some(c) => c
      case None =>
        val c = createAsyncClient()
        if (asyncRef.compareAndSet(None, Some(c))) {
          logger.info(
            s"Elasticsearch async Client initialized for ${elasticConfig.credentials.url}"
          )
          c
        } else {
          // Another thread initialized while we were waiting: release OUR transport (its own
          // pool + IO reactor would otherwise leak) and use theirs — re-read rather than `.get`,
          // a concurrent close() may have cleared the reference in between
          Try(c.close())
          async()
        }
    }
  }

  private def createAsyncClient(): ElasticsearchAsyncClient = {
    try {
      new ElasticsearchAsyncClient(buildTransport())
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to create ElasticsearchAsyncClient: ${ex.getMessage}", ex)
        throw new IllegalStateException("Cannot create Elasticsearch async client", ex)
    }
  }

  /** Close the sync client AND the async transport (#238): the async client owns its own RestClient
    * pool, and the PIT paging path now runs on it. Idempotent.
    */
  override def close(): Unit = {
    super.close()
    asyncRef.getAndSet(None).foreach { c =>
      Try {
        c.close()
        logger.info("Elasticsearch async Client closed successfully")
      }.recover { case ex: Exception =>
        logger.warn(s"Error closing Elasticsearch async Client: ${ex.getMessage}", ex)
      }
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

  private def buildTransport(): RestClientTransport = {
    new RestClientTransport(buildRestClient().build(), new JacksonJsonpMapper())
  }

  /** Create and configure Elasticsearch Client
    */
  override protected def createClient(): ElasticsearchClient = {
    try {
      new ElasticsearchClient(buildTransport())
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to create ElasticsearchClient: ${ex.getMessage}", ex)
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
      val response = c.info()
      logger.info(s"Connected to Elasticsearch ${response.version().number()}")
      true
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to connect to Elasticsearch: ${ex.getMessage}", ex)
        incrementFailures()
        false
    }
  }

  /** Bridge a Java `CompletableFuture` to a Scala `Future`.
    *
    * `whenComplete` hands over the failure wrapped in a `CompletionException` when the stage failed
    * upstream; that wrapper is unwrapped here (#238) so the cause — e.g. the `IOException` /
    * `SocketTimeoutException` that `isRetriableError` matches — reaches `retryWithBackoff`. Without
    * it the asynchronous PIT paging path would silently never retry.
    */
  def fromCompletableFuture[T](cf: CompletableFuture[T]): Future[T] = {
    val promise = Promise[T]()
    cf.whenComplete { (result: T, err: Throwable) =>
      if (err != null) promise.failure(JavaClientCompanion.unwrapCompletion(err))
      else promise.success(result)
    }
    promise.future
  }

}

object JavaClientCompanion {

  /** Strip the `CompletionException` / `ExecutionException` layers a failed `CompletableFuture`
    * chain adds (bounded — a dependent stage may wrap an already wrapped failure).
    */
  @scala.annotation.tailrec
  def unwrapCompletion(t: Throwable, depth: Int = 8): Throwable = t match {
    case ce: CompletionException if ce.getCause != null && depth > 0 =>
      unwrapCompletion(ce.getCause, depth - 1)
    case ee: ExecutionException if ee.getCause != null && depth > 0 =>
      unwrapCompletion(ee.getCause, depth - 1)
    case other => other
  }

}
