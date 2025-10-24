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

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import app.softnetwork.elastic.client.BulkAction.BulkAction
import app.softnetwork.elastic.sql.query.{Asc, SQLAggregation, SortOrder}
import app.softnetwork.serialization._
import com.google.gson.{Gson, JsonElement, JsonObject}
import org.json4s.Formats
import org.slf4j.Logger

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

//import scala.jdk.CollectionConverters._
import scala.collection.JavaConverters._

/** Created by smanciot on 30/06/2018.
  */
package object client {

  type SQL = String

  type ESQuery = String

  type ESResults = String

  case class SQLSearchResponse(
    query: ESQuery,
    results: ESResults,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )

  case class ElasticCredentials(
    url: String = "http://localhost:9200",
    username: String = "",
    password: String = ""
  )

  object BulkAction extends Enumeration {
    type BulkAction = Value
    val INDEX: client.BulkAction.Value = Value(0, "INDEX")
    val UPDATE: client.BulkAction.Value = Value(1, "UPDATE")
    val DELETE: client.BulkAction.Value = Value(2, "DELETE")
  }

  case class BulkItem(
    index: String,
    action: BulkAction,
    body: String,
    id: Option[String],
    parent: Option[String]
  )

  case class BulkOptions(
    index: String,
    documentType: String = "_doc",
    maxBulkSize: Int = 100,
    balance: Int = 1,
    disableRefresh: Boolean = false
  )

  trait BulkElasticAction { def index: String }

  trait BulkElasticResult { def items: List[BulkElasticResultItem] }

  trait BulkElasticResultItem { def index: String }

  case class BulkSettings[A](disableRefresh: Boolean = false)(implicit
    settingsApi: SettingsApi,
    toBulkElasticAction: A => BulkElasticAction
  ) extends GraphStage[FlowShape[A, A]] {

    val in: Inlet[A] = Inlet[A]("Filter.in")
    val out: Outlet[A] = Outlet[A]("Filter.out")

    val shape: FlowShape[A, A] = FlowShape.of(in, out)

    val indices = mutable.Set.empty[String]

    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic = {
      new GraphStageLogic(shape) {
        setHandler(
          in,
          () => {
            val elem = grab(in)
            val index = elem.index
            if (!indices.contains(index)) {
              if (disableRefresh) {
                settingsApi.updateSettings(
                  index,
                  """{"index" : {"refresh_interval" : "-1", "number_of_replicas" : 0} }"""
                )
              }
              indices.add(index)
            }
            push(out, elem)
          }
        )
        setHandler(
          out,
          () => {
            pull(in)
          }
        )
      }
    }
  }

  def docAsUpsert(doc: String): String = s"""{"doc":$doc,"doc_as_upsert":true}"""

  implicit class InnerHits(searchResult: JsonObject) {
    def ~>[M, I](
      innerField: String
    )(implicit formats: Formats, m: Manifest[M], i: Manifest[I]): List[(M, List[I])] = {
      def innerHits(result: JsonElement) = {
        result.getAsJsonObject
          .get("inner_hits")
          .getAsJsonObject
          .get(innerField)
          .getAsJsonObject
          .get("hits")
          .getAsJsonObject
          .get("hits")
          .getAsJsonArray
          .iterator()
      }
      val gson = new Gson()
      val results = searchResult.get("hits").getAsJsonObject.get("hits").getAsJsonArray.iterator()
      (for (result <- results.asScala)
        yield (
          result match {
            case obj: JsonObject =>
              Try {
                serialization.read[M](gson.toJson(obj.get("_source")))
              } match {
                case Success(s) => s
                case Failure(f) =>
                  throw f
              }
            case _ => serialization.read[M](result.getAsString)
          },
          (for (innerHit <- innerHits(result).asScala) yield innerHit match {
            case obj: JsonObject =>
              Try {
                serialization.read[I](gson.toJson(obj.get("_source")))
              } match {
                case Success(s) => s
                case Failure(f) =>
                  throw f
              }
            case _ => serialization.read[I](innerHit.getAsString)
          }).toList
        )).toList
    }
  }

  case class JSONQuery(query: String, indices: Seq[String], types: Seq[String] = Seq.empty)

  case class JSONQueries(queries: List[JSONQuery])

  def tryOrElse[T](block: => T, default: => T)(implicit logger: Logger): T = {
    try {
      block
    } catch {
      case e: Exception =>
        logger.error("An error occurred while executing the block", e)
        default
    }
  }

  /** Scroll configuration
    */
  case class ScrollConfig(
    scrollTimeout: String = "1m",
    scrollSize: Int = 1000,
    maxDocuments: Option[Long] = None,
    preferSearchAfter: Boolean = true, // Préférence pour search_after si possible
    metrics: ScrollMetrics = ScrollMetrics(),
    retryConfig: RetryConfig = RetryConfig()
  )

  /** Scroll strategy based on query type
    */
  sealed trait ScrollStrategy
  case object UseScroll extends ScrollStrategy // Pour agrégations ou requêtes complexes
  case object UseSearchAfter extends ScrollStrategy // Pour hits simples (plus performant)

  /** Scroll metrics
    */
  case class ScrollMetrics(
    totalDocuments: Long = 0,
    totalBatches: Long = 0,
    startTime: Long = System.currentTimeMillis(),
    var endTime: Option[Long] = None
  ) {
    def duration: Long = endTime.getOrElse(System.currentTimeMillis()) - startTime
    def documentsPerSecond: Double = totalDocuments.toDouble / (duration / 1000.0)
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
  private[client] def retryWithBackoff[T](config: RetryConfig)(
    operation: => Future[T]
  )(implicit ec: ExecutionContext, logger: Logger): Future[T] = {

    def attempt(retriesLeft: Int, delay: FiniteDuration): Future[T] = {
      operation.recoverWith {
        case ex if retriesLeft > 0 && isRetriableError(ex) =>
          logger.warn(s"Retrying after failure (${retriesLeft} retries left): ${ex.getMessage}")

          akka.pattern.after(delay, akka.actor.ActorSystem("retry").scheduler) {
            val nextDelay = (delay * config.backoffFactor).min(config.maxDelay).asInstanceOf
            attempt(retriesLeft - 1, nextDelay)
          }
      }
    }

    attempt(config.maxRetries, config.initialDelay)
  }

  /** Determine if an error is retriable
    */
  private def isRetriableError(ex: Throwable): Boolean = ex match {
    case _: java.net.SocketTimeoutException => true
    case _: java.io.IOException             => true
    // case _: TransportException => true
    case _ => false
  }
}
