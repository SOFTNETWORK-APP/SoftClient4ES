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

package app.softnetwork.elastic

import akka.actor.ActorSystem
import app.softnetwork.elastic.sql.query.SQLAggregation
import org.slf4j.Logger

import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.reflectiveCalls

/** Created by smanciot on 30/06/2018.
  */
package object client extends SerializationApi {

  /** Type alias for JSON query
    */
  type JSONQuery = String

  /** Type alias for JSON results
    */
  type JSONResults = String

  sealed trait ElasticResult[+T] {
    def success: Boolean
  }
  case class Succeeded[T](value: T) extends ElasticResult[T] {
    override def success: Boolean = true
  }
  case class Failed(error: ElasticError) extends ElasticResult[Nothing] {
    override def success: Boolean = false
  }

  case class ElasticError(
    message: String,
    cause: Option[Throwable] = None,
    statusCode: Option[Int] = None
  )

  /** Elastic response case class
    * @param query
    *   - the JSON query
    * @param results
    *   - the JSON results
    * @param fieldAliases
    *   - the field aliases defined in the SQL query
    * @param aggregations
    *   - the SQL aggregations defined in the SQL query
    */
  case class ElasticResponse(
    query: JSONQuery,
    results: JSONResults,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )

  case class ElasticCredentials(
    url: String = "http://localhost:9200",
    username: String = "",
    password: String = ""
  )

  /** Elastic query wrapper
    * @param query
    *   - the elasticsearch JSON query
    * @param indices
    *   - the target indices
    * @param types
    *   - the target types @deprecated types are deprecated in ES 7+
    */
  case class ElasticQuery(query: JSONQuery, indices: Seq[String], types: Seq[String] = Seq.empty)

  case class ElasticQueries(queries: List[ElasticQuery])

  def tryOrElse[T](block: => T, default: => T)(implicit logger: Logger): T = {
    try {
      block
    } catch {
      case e: Exception =>
        logger.error("An error occurred while executing the block", e)
        default
    }
  }

  /** Retry configuration
    */
  case class RetryConfig(
    maxRetries: Int = 3,
    initialDelay: FiniteDuration = 1.second,
    maxDelay: FiniteDuration = 10.seconds,
    backoffFactor: Double = 2.0
  )

  /** Retry logic with exponential backoff
    */
  // Passer le scheduler en paramètre implicite
  private[client] def retryWithBackoff[T](config: RetryConfig)(
    operation: => Future[T]
  )(implicit
    system: ActorSystem,
    logger: Logger
  ): Future[T] = {
    implicit val ec: ExecutionContext = system.dispatcher
    val scheduler = system.scheduler
    def attempt(retriesLeft: Int, delay: FiniteDuration): Future[T] = {
      operation.recoverWith {
        case ex if retriesLeft > 0 && isRetriableError(ex) =>
          logger.warn(s"Retrying after failure ($retriesLeft retries left): ${ex.getMessage}")
          akka.pattern.after(delay, scheduler) {
            val nextDelay = FiniteDuration(
              (delay * config.backoffFactor).min(config.maxDelay).toMillis,
              TimeUnit.MILLISECONDS
            )
            attempt(retriesLeft - 1, nextDelay)
          }
      }
    }
    attempt(config.maxRetries, config.initialDelay)
  }

  /** Determine if an error is retriable
    */
  private[client] def isRetriableError(ex: Throwable): Boolean = ex match {
    case _: java.net.SocketTimeoutException => true
    case _: java.io.IOException             => true
    // case _: TransportException => true
    case _ => false
  }
}
