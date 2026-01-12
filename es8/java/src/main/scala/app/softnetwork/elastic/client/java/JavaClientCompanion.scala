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

import app.softnetwork.elastic.client.ElasticClientCompanion
import co.elastic.clients.elasticsearch.{ElasticsearchAsyncClient, ElasticsearchClient}
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.{RestClient, RestClientBuilder}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.CompletableFuture
import java.util.concurrent.atomic.AtomicReference
import scala.concurrent.{Future, Promise}

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
          // Another thread initialized while we were waiting
          asyncRef.get().get
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

  private def buildTransport(): RestClientTransport = {
    val credentialsProvider = new BasicCredentialsProvider()
    if (elasticConfig.credentials.username.nonEmpty) {
      credentialsProvider.setCredentials(
        AuthScope.ANY,
        new UsernamePasswordCredentials(
          elasticConfig.credentials.username,
          elasticConfig.credentials.password
        )
      )
    }
    val restClientBuilder: RestClientBuilder = RestClient
      .builder(
        parseHttpHost(elasticConfig.credentials.url)
      )
      .setHttpClientConfigCallback((httpAsyncClientBuilder: HttpAsyncClientBuilder) =>
        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
      )
    new RestClientTransport(restClientBuilder.build(), new JacksonJsonpMapper())
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

  def fromCompletableFuture[T](cf: CompletableFuture[T]): Future[T] = {
    val promise = Promise[T]()
    cf.whenComplete { (result: T, err: Throwable) =>
      if (err != null) promise.failure(err)
      else promise.success(result)
    }
    promise.future
  }

}
