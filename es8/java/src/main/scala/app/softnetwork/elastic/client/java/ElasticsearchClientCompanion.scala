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

import app.softnetwork.elastic.client.ElasticConfig
import co.elastic.clients.elasticsearch.{ElasticsearchAsyncClient, ElasticsearchClient}
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import co.elastic.clients.transport.rest_client.RestClientTransport
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.ClassTagExtensions
import org.apache.http.HttpHost
import org.apache.http.auth.{AuthScope, UsernamePasswordCredentials}
import org.apache.http.impl.client.BasicCredentialsProvider
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder
import org.elasticsearch.client.{RestClient, RestClientBuilder}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.CompletableFuture
import scala.concurrent.{Future, Promise}

trait ElasticsearchClientCompanion {

  val logger: Logger = LoggerFactory getLogger getClass.getName

  def elasticConfig: ElasticConfig

  private var client: Option[ElasticsearchClient] = None

  private var asyncClient: Option[ElasticsearchAsyncClient] = None

  lazy val mapper: ObjectMapper with ClassTagExtensions = new ObjectMapper() with ClassTagExtensions

  def transport: RestClientTransport = {
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
        HttpHost.create(elasticConfig.credentials.url)
      )
      .setHttpClientConfigCallback((httpAsyncClientBuilder: HttpAsyncClientBuilder) =>
        httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
      )
    new RestClientTransport(restClientBuilder.build(), new JacksonJsonpMapper())
  }

  def apply(): ElasticsearchClient = {
    client match {
      case Some(c) => c
      case _ =>
        val c = new ElasticsearchClient(transport)
        client = Some(c)
        c
    }
  }

  def async(): ElasticsearchAsyncClient = {
    asyncClient match {
      case Some(c) => c
      case _ =>
        val c = new ElasticsearchAsyncClient(transport)
        asyncClient = Some(c)
        c
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
