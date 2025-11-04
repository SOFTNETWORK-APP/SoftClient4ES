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

import akka.stream.{Attributes, FlowShape, Inlet, Outlet}
import akka.stream.stage.{GraphStage, GraphStageLogic}

import scala.collection.mutable
import scala.concurrent.duration._

package object bulk {

  trait BulkTypes {
    type BulkActionType
    type BulkResultType
  }

  object BulkAction extends Enumeration {
    type BulkAction = Value
    val INDEX: BulkAction.Value = Value(0, "INDEX")
    val UPDATE: BulkAction.Value = Value(1, "UPDATE")
    val DELETE: BulkAction.Value = Value(2, "DELETE")
  }

  case class BulkItem(
    index: String,
    action: BulkAction.BulkAction,
    document: String,
    id: Option[String],
    parent: Option[String]
  )

  /** Detailed result of a bulk operation */
  case class BulkResult(
    successCount: Int,
    successIds: Set[String],
    failedCount: Int,
    failedDocuments: Seq[FailedDocument],
    indices: Set[String],
    metrics: BulkMetrics
  ) {
    def successRate: Double =
      if (successCount + failedCount > 0)
        successCount.toDouble / (successCount + failedCount) * 100
      else 0.0

    def hasFailures: Boolean = failedCount > 0
  }

  sealed trait DocumentResult {
    def id: String
    def index: String
  }

  /** Document failed during bulk processing */
  case class FailedDocument(
    id: String,
    index: String,
    document: String,
    error: BulkError,
    retryable: Boolean
  ) extends DocumentResult

  case class SuccessfulDocument(
    id: String,
    index: String
  ) extends DocumentResult

  /** Detailed error */
  case class BulkError(
    message: String,
    `type`: String,
    status: Int,
    causedBy: Option[BulkError] = None
  ) {
    def isRetryable: Boolean = status match {
      case 429 | 503 | 504 => true // Too Many Requests, Service Unavailable, Gateway Timeout
      case _               => false
    }
  }

  /** Bulk metrics */
  case class BulkMetrics(
    startTime: Long = System.currentTimeMillis(),
    endTime: Option[Long] = None,
    totalBatches: Int = 0,
    totalDocuments: Int = 0,
    failuresByStatus: Map[Int, Int] = Map.empty,
    failuresByType: Map[String, Int] = Map.empty
  ) {
    def durationMs: Long = endTime.getOrElse(System.currentTimeMillis()) - startTime

    def throughput: Double =
      if (durationMs > 0) totalDocuments * 1000.0 / durationMs
      else 0.0

    def complete: BulkMetrics = copy(endTime = Some(System.currentTimeMillis()))

    def addFailure(error: BulkError): BulkMetrics = copy(
      failuresByStatus =
        failuresByStatus + (error.status -> (failuresByStatus.getOrElse(error.status, 0) + 1)),
      failuresByType =
        failuresByType + (error.`type` -> (failuresByType.getOrElse(error.`type`, 0) + 1))
    )
  }

  /** Bulk Configuration */
  case class BulkOptions(
    index: String,
    defaultType: String = "_doc",
    maxBulkSize: Int = 1000,
    balance: Int = 1,
    disableRefresh: Boolean = false,
    retryOnFailure: Boolean = true,
    maxRetries: Int = 3,
    retryDelay: FiniteDuration = 1.second,
    retryBackoffMultiplier: Double = 2.0,
    enableMetrics: Boolean = true,
    logEvery: Int = 10
  )

  /** Callbacks for bulk events */
  trait BulkCallbacks {
    def onSuccess(id: String, index: String): Unit = {}
    def onFailure(failed: FailedDocument): Unit = {}
    def onBatchComplete(batchSize: Int, metrics: BulkMetrics): Unit = {}
    def onComplete(result: BulkResult): Unit = {}
  }

  object BulkCallbacks {
    val default: BulkCallbacks = new BulkCallbacks {}

    def logging(logger: org.slf4j.Logger): BulkCallbacks = new BulkCallbacks {
      override def onSuccess(id: String, index: String): Unit =
        logger.debug(s"✅ Document $id indexed in $index")

      override def onFailure(failed: FailedDocument): Unit =
        logger.error(s"❌ Document ${failed.id} failed: ${failed.error.message}")

      override def onBatchComplete(batchSize: Int, metrics: BulkMetrics): Unit =
        logger.info(s"Batch completed: $batchSize docs (${metrics.throughput} docs/sec)")

      override def onComplete(result: BulkResult): Unit =
        logger.info(
          s"Bulk completed: ${result.successCount} successes, ${result.failedCount} failures " +
          s"in ${result.metrics.durationMs}ms (${result.metrics.throughput} docs/sec)"
        )
    }
  }

  trait BulkElasticAction { def index: String } // TODO rename to BulkItemIndex

  trait BulkElasticResult { def items: List[BulkElasticResultItem] } // TODO remove

  trait BulkElasticResultItem { def index: String } // TODO remove

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

  object BulkErrorAnalyzer {

    /** Determines whether a bulk error is retryable based on the status.
      *
      * @param statusCode
      *   HTTP error status code
      * @return
      *   true if the error is retryable, false otherwise
      */
    def isRetryable(statusCode: Int): Boolean = statusCode match {
      // Temporary errors - retryable
      case 429 => true // Too Many Requests
      case 503 => true // Service Unavailable
      case 504 => true // Gateway Timeout
      case 408 => true // Request Timeout
      case 502 => true // Bad Gateway

      // Permanent errors - not retryable
      case 400 => false // Bad Request
      case 401 => false // Unauthorized
      case 403 => false // Forbidden
      case 404 => false // Not Found
      case 409 => false // Conflict (version)
      case 413 => false // Payload Too Large

      // By default, 5xx errors (except those listed) are retryable.
      case code if code >= 500 && code < 600 => true

      // Other 4xx errors are non-retryable
      case code if code >= 400 && code < 500 => false

      // Status 2xx and 3xx should not be errors
      case _ => false
    }

    /** Determines whether a bulk error is retryable based on the ES error type.
      *
      * @param errorType
      *   Elasticsearch error type
      * @return
      *   true if the error is retryable, false otherwise
      */
    def isRetryableByType(errorType: String): Boolean = errorType match {
      // Retryable errors
      case "es_rejected_execution_exception" => true
      case "circuit_breaking_exception"      => true
      case "timeout_exception"               => true
      case "unavailable_shards_exception"    => true

      // non-retryable errors
      case "mapper_parsing_exception"          => false
      case "illegal_argument_exception"        => false
      case "version_conflict_engine_exception" => false
      case "document_missing_exception"        => false
      case "index_not_found_exception"         => false
      case "strict_dynamic_mapping_exception"  => false

      // By default, it is considered non-retryable
      case _ => false
    }
  }
}
