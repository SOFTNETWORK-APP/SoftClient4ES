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

import app.softnetwork.elastic.client.ElasticClientCompanion
import com.sksamuel.exts.Logging
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.Cat

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
      val factory = new JestClientFactory()
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

    new HttpClientConfig.Builder(elasticConfig.credentials.url)
      .defaultCredentials(
        elasticConfig.credentials.username,
        elasticConfig.credentials.password
      )
      .preemptiveAuthTargetHosts(httpHosts.asJava)
      .multiThreaded(elasticConfig.multithreaded)
      .discoveryEnabled(elasticConfig.discovery.enabled)
      .discoveryFrequency(
        elasticConfig.discovery.frequency.getSeconds,
        TimeUnit.SECONDS
      )
      .connTimeout(elasticConfig.connectionTimeout.toMillis.toInt)
      .readTimeout(elasticConfig.socketTimeout.toMillis.toInt)
      .build()
  }
}
