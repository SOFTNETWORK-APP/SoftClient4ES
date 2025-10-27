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
import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic}
import app.softnetwork.elastic.client.BulkAction.BulkAction
import app.softnetwork.elastic.sql.query.SQLAggregation
import app.softnetwork.serialization._
import com.google.gson.{Gson, JsonElement, JsonObject}
import org.json4s.Formats
import org.slf4j.Logger

import java.util.concurrent.TimeUnit
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._
import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

import scala.jdk.CollectionConverters._

/** Created by smanciot on 30/06/2018.
  */
package object client {

  /** Type alias for JSON query
    */
  type JSONQuery = String

  /** Type alias for JSON results
    */
  type JSONResults = String

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

  /** Scroll configuration
    */
  case class ScrollConfig(
    scrollTimeout: String = "1m",
    scrollSize: Int = 1000,
    logEvery: Int = 10,
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
    endTime: Option[Long] = None
  ) {
    def duration: Long = endTime.getOrElse(System.currentTimeMillis()) - startTime
    def documentsPerSecond: Double = totalDocuments.toDouble / (duration / 1000.0)
    def complete: ScrollMetrics = copy(endTime = Some(System.currentTimeMillis()))
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

  /** Elasticsearch version comparison utilities
    */
  object ElasticsearchVersion {

    /** Parse Elasticsearch version string (e.g., "7.10.2", "8.11.0")
      * @return
      *   (major, minor, patch)
      */
    def parse(versionString: String): (Int, Int, Int) = {
      try {
        val parts = versionString.split('.').take(3)
        val major = parts.headOption.map(_.toInt).getOrElse(0)
        val minor = parts.lift(1).map(_.toInt).getOrElse(0)
        val patch = parts.lift(2).map(_.toInt).getOrElse(0)
        (major, minor, patch)
      } catch {
        case _: NumberFormatException =>
          throw new IllegalArgumentException(s"Invalid version format: $versionString")
      }
    }

    /** Check if version is >= target version
      */
    def isAtLeast(
      version: String,
      targetMajor: Int,
      targetMinor: Int = 0,
      targetPatch: Int = 0
    ): Boolean = {
      val (major, minor, patch) = parse(version)

      if (major > targetMajor) true
      else if (major < targetMajor) false
      else { // major == targetMajor
        if (minor > targetMinor) true
        else if (minor < targetMinor) false
        else { // minor == targetMinor
          patch >= targetPatch
        }
      }
    }

    /** Check if PIT is supported (ES >= 7.10)
      */
    def supportsPit(version: String): Boolean = {
      isAtLeast(version, 7, 10)
    }

    /** Check if version is ES 8+
      */
    def isEs8OrHigher(version: String): Boolean = {
      isAtLeast(version, 8, 0)
    }
  }
}
