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
import app.softnetwork.elastic.sql.function.aggregate._
import app.softnetwork.elastic.sql.query.SQLAggregation
import org.slf4j.Logger

import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.{implicitConversions, reflectiveCalls}

/** Created by smanciot on 30/06/2018.
  */
package object client extends SerializationApi {

  /** Type alias for JSON query
    */
  type JSONQuery = String

  /** Type alias for JSON results
    */
  type JSONResults = String

  /** Elastic response case class
    * @param sql
    *   - the SQL query if any
    * @param query
    *   - the JSON query
    * @param results
    *   - the results as a sequence of rows
    * @param fieldAliases
    *   - the field aliases used
    * @param aggregations
    *   - the aggregations expected
    */
  case class ElasticResponse(
    sql: Option[String] = None,
    query: JSONQuery,
    results: Seq[Map[String, Any]],
    fieldAliases: Map[String, String],
    aggregations: Map[String, ClientAggregation]
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
  case class ElasticQuery(
    query: JSONQuery,
    indices: Seq[String],
    types: Seq[String] = Seq.empty,
    sql: Option[String] = None
  )

  case class ElasticQueries(queries: List[ElasticQuery], sql: Option[String] = None)

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

  /** Aggregation types
    */
  object AggregationType extends Enumeration {
    type AggregationType = Value
    val Count, Min, Max, Avg, Sum, FirstValue, LastValue, ArrayAgg = Value
  }

  /** Client Aggregation
    * @param aggName
    *   - the name of the aggregation
    * @param aggType
    *   - the type of the aggregation
    * @param distinct
    *   - when the aggregation is multivalued define if its values should be returned distinct or
    *     not
    */
  case class ClientAggregation(
    aggName: String,
    aggType: AggregationType.AggregationType,
    distinct: Boolean,
    sourceField: String,
    window: Boolean
  ) {
    def multivalued: Boolean = aggType == AggregationType.ArrayAgg
    def singleValued: Boolean = !multivalued
  }

  implicit def sqlAggregationToClientAggregation(agg: SQLAggregation): ClientAggregation = {
    val aggType = agg.aggType match {
      case COUNT         => AggregationType.Count
      case MIN           => AggregationType.Min
      case MAX           => AggregationType.Max
      case AVG           => AggregationType.Avg
      case SUM           => AggregationType.Sum
      case _: FirstValue => AggregationType.FirstValue
      case _: LastValue  => AggregationType.LastValue
      case _: ArrayAgg   => AggregationType.ArrayAgg
      case _ => throw new IllegalArgumentException(s"Unsupported aggregation type: ${agg.aggType}")
    }
    ClientAggregation(
      agg.aggName,
      aggType,
      agg.distinct,
      agg.sourceField,
      agg.aggType.isInstanceOf[WindowFunction]
    )
  }
}
