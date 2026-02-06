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

package app.softnetwork.elastic.client.repl

// ==================== Stream Support ====================

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.{Sink, Source}
import app.softnetwork.elastic.client.GatewayApi
import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticSuccess,
  QueryStream,
  StreamResult
}

import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration._

/** Extended executor with stream support
  */
class StreamingReplExecutor(gateway: GatewayApi)(implicit
  system: ActorSystem,
  ec: ExecutionContext,
  materializer: Materializer
) extends ReplExecutor(gateway) {

  // Current active stream (if any)
  private var activeStream: Option[StreamContext] = None

  case class StreamContext(
    source: Source[Map[String, Any], _],
    name: String,
    startTime: Long
  )

  /** Execute SQL and handle streaming results
    */
  override def execute(sql: String): Future[ExecutionResult] = {
    val startTime = System.nanoTime()

    gateway
      .run(sql)
      .map { elasticResult =>
        val executionTime = (System.nanoTime() - startTime).nanos

        elasticResult match {
          case ElasticSuccess(QueryStream(source)) =>
            // Store stream context for later consumption
            activeStream = Some(StreamContext(source.map(_._1), sql, startTime))
            ExecutionSuccess(
              StreamResult(estimatedSize = None, isActive = true),
              executionTime
            )

          case ElasticSuccess(queryResult) =>
            ExecutionSuccess(queryResult, executionTime)

          case ElasticFailure(error) =>
            ExecutionFailure(error, executionTime)
        }
      }
      .recover { case ex: Throwable =>
        val executionTime = (System.nanoTime() - startTime).nanos
        ExecutionFailure(ElasticError.fromThrowable(ex), executionTime)
      }
  }

  /** Consume stream with configurable batch size and callback
    */
  def consumeStream(
    batchSize: Int = 100,
    maxRows: Option[Int] = None,
    onBatch: Seq[Map[String, Any]] => Unit = _ => ()
  ): Future[StreamConsumptionResult] = {
    activeStream match {
      case None =>
        Future.successful(
          StreamConsumptionResult(
            totalRows = 0,
            batches = 0,
            duration = 0.nanos,
            error = Some("No active stream")
          )
        )

      case Some(ctx) =>
        val startTime = System.nanoTime()
        var totalRows = 0L
        var batches = 0

        val limitedSource = maxRows match {
          case Some(limit) => ctx.source.take(limit)
          case None        => ctx.source
        }

        limitedSource
          .grouped(batchSize)
          .runWith(Sink.foreach { batch =>
            batches += 1
            totalRows += batch.size
            onBatch(batch)
          })
          .map { _ =>
            activeStream = None
            StreamConsumptionResult(
              totalRows = totalRows,
              batches = batches,
              duration = (System.nanoTime() - startTime).nanos,
              error = None
            )
          }
          .recover { case ex =>
            activeStream = None
            StreamConsumptionResult(
              totalRows = totalRows,
              batches = batches,
              duration = (System.nanoTime() - startTime).nanos,
              error = Some(ex.getMessage)
            )
          }
    }
  }

  /** Cancel active stream
    */
  def cancelStream(): Unit = {
    activeStream = None
  }

  /** Check if there's an active stream
    */
  def hasActiveStream: Boolean = activeStream.isDefined
}
