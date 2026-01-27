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

package app.softnetwork.elastic.client

import app.softnetwork.common.ClientCompanion
import org.apache.http.HttpHost
import org.slf4j.Logger

import java.io.Closeable
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference
import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

trait ElasticClientCompanion[T <: Closeable] extends ClientCompanion { _: { def logger: Logger } =>

  def elasticConfig: ElasticConfig

  private val failures = new AtomicInteger(0)

  private val ref = new AtomicReference[Option[T]](None)

  /** Get or create Elastic Client instance (thread-safe) using atomic reference
    *
    * @return
    *   Elastic Client instance
    * @throws IllegalStateException
    *   if client creation fails
    */
  def apply(): T = {
    ref.get() match {
      case Some(c) => c
      case None =>
        val c = createClient()
        if (ref.compareAndSet(None, Some(c))) {
          logger.info(s"Elasticsearch Client initialized for ${elasticConfig.credentials.url}")
          c
        } else {
          // Another thread initialized while we were waiting
          ref.get().get
        }
    }
  }

  /** Create and configure Elasticsearch Client
    */
  protected def createClient(): T

  /** Parse and validate HTTP host from URL string
    * @throws IllegalArgumentException
    *   if URL is invalid
    */
  protected def parseHttpHost(url: String): HttpHost = {
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

  /** Check if client is initialized and connected
    */
  override def isInitialized: Boolean = ref.get().isDefined

  /** Close the client and release resources Idempotent - safe to call multiple times
    */
  override def close(): Unit = {
    ref.get().foreach { c =>
      Try {
        c.close()
        logger.info("Elasticsearch Client closed successfully")
      }.recover { case ex: Exception =>
        logger.warn(s"Error closing Elasticsearch Client: ${ex.getMessage}", ex)
      }
    }
    ref.set(None)
  }

  /** Reset client (force reconnection on next access) Useful for connection recovery scenarios
    */
  def reset(): Unit = {
    logger.info("Resetting Elasticsearch Client")
    close()
    failures.set(0)
  }

  protected def incrementFailures(): Int = {
    val nbFailures = failures.incrementAndGet()
    if (nbFailures >= 5) {
      reset()
    }
    nbFailures
  }

  def getFailures: Int = {
    failures.get()
  }
}
