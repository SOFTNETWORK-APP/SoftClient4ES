/*
 * Copyright 2015 SOFTNETWORK
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

import app.softnetwork.elastic.client.{ElasticConfig, ElasticCredentials}
import com.sksamuel.exts.Logging
import io.searchbox.action.Action
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory, JestResult, JestResultHandler}
import org.apache.http.HttpHost

import java.io.IOException
import java.util
import java.util.concurrent.TimeUnit
import scala.collection.JavaConverters._
//import scala.jdk.CollectionConverters._
import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

/** Created by smanciot on 20/05/2021.
  */
trait JestClientCompanion extends Logging {

  def elasticConfig: ElasticConfig

  private[this] var jestClient: Option[InnerJestClient] = None

  private[this] val factory = new JestClientFactory()

  private[this] var httpClientConfig: HttpClientConfig = _

  private[this] class InnerJestClient(private var _jestClient: JestClient) extends JestClient {
    private[this] var nbFailures: Int = 0

    override def shutdownClient(): Unit = {
      close()
    }

    private def checkClient(): Unit = {
      Option(_jestClient) match {
        case None =>
          factory.setHttpClientConfig(httpClientConfig)
          _jestClient = Try(factory.getObject) match {
            case Success(s) =>
              s
            case Failure(f) =>
              logger.error(f.getMessage, f)
              throw f
          }
        case _ =>
      }
    }

    override def executeAsync[J <: JestResult](
      clientRequest: Action[J],
      jestResultHandler: JestResultHandler[_ >: J]
    ): Unit = {
      Try(checkClient())
      Option(_jestClient) match {
        case Some(s) => s.executeAsync[J](clientRequest, jestResultHandler)
        case _ =>
          close()
          jestResultHandler.failed(new Exception("JestClient not initialized"))
      }
    }

    override def execute[J <: JestResult](clientRequest: Action[J]): J = {
      Try(checkClient())
      Option(_jestClient) match {
        case Some(j) =>
          Try(j.execute[J](clientRequest)) match {
            case Success(s) =>
              nbFailures = 0
              s
            case Failure(f) =>
              f match {
                case e: IOException =>
                  nbFailures += 1
                  logger.error(e.getMessage, e)
                  close()
                  if (nbFailures < 10) {
                    Thread.sleep(1000 * nbFailures)
                    execute(clientRequest)
                  } else {
                    throw f
                  }
                case e: IllegalStateException =>
                  nbFailures += 1
                  logger.error(e.getMessage, e)
                  close()
                  if (nbFailures < 10) {
                    Thread.sleep(1000 * nbFailures)
                    execute(clientRequest)
                  } else {
                    throw f
                  }
                case _ =>
                  close()
                  throw f
              }
          }
        case _ =>
          close()
          throw new Exception("JestClient not initialized")
      }
    }

    override def setServers(servers: util.Set[String]): Unit = {
      Try(checkClient())
      Option(_jestClient).foreach(_.setServers(servers))
    }

    override def close(): Unit = {
      Option(_jestClient).foreach(_.close())
      _jestClient = null
    }
  }

  private[this] def getHttpHosts(esUrl: String): Set[HttpHost] = {
    esUrl
      .split(",")
      .map(u => {
        val url = new java.net.URL(u)
        new HttpHost(url.getHost, url.getPort, url.getProtocol)
      })
      .toSet
  }

  def apply(): JestClient = {
    apply(
      elasticConfig.credentials,
      multithreaded = elasticConfig.multithreaded,
      discoveryEnabled = elasticConfig.discoveryEnabled
    )
  }

  def apply(
    esCredentials: ElasticCredentials,
    multithreaded: Boolean = true,
    timeout: Int = 60000,
    discoveryEnabled: Boolean = false,
    discoveryFrequency: Long = 60L,
    discoveryFrequencyTimeUnit: TimeUnit = TimeUnit.SECONDS
  ): JestClient = {
    jestClient match {
      case Some(s) => s
      case None =>
        httpClientConfig = new HttpClientConfig.Builder(esCredentials.url)
          .defaultCredentials(esCredentials.username, esCredentials.password)
          .preemptiveAuthTargetHosts(getHttpHosts(esCredentials.url).asJava)
          .multiThreaded(multithreaded)
          .discoveryEnabled(discoveryEnabled)
          .discoveryFrequency(discoveryFrequency, discoveryFrequencyTimeUnit)
          .connTimeout(timeout)
          .readTimeout(timeout)
          .build()
        factory.setHttpClientConfig(httpClientConfig)
        jestClient = Some(new InnerJestClient(factory.getObject))
        jestClient.get
    }
  }
}
