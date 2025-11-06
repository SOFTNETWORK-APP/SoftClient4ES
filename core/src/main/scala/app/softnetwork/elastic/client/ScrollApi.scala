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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import app.softnetwork.elastic.client.scroll.{
  ScrollConfig,
  ScrollMetrics,
  ScrollStrategy,
  UsePIT,
  UseScroll,
  UseSearchAfter
}
import app.softnetwork.elastic.sql.query.{SQLAggregation, SQLQuery}
import org.json4s.{Formats, JNothing}
import org.json4s.jackson.JsonMethods.parse

import scala.concurrent.{ExecutionContext, Promise}
import scala.util.{Failure, Success}

/** API for scrolling through search results using Akka Streams.
  *
  * ==Error Handling==
  *
  * This API handles transient errors automatically in the implementation:
  *   - Network timeouts are retried
  *   - Expired scroll contexts are handled gracefully
  *   - Elasticsearch errors are logged and recovered
  *
  * Users can add additional error handling using standard Akka Streams operators:
  *
  * {{{
  *   // Add custom recovery
  *   client.scroll(sqlQuery)
  *     .recover {
  *       case ex: MyException => handleError(ex)
  *     }
  *     .runWith(Sink.seq)
  *
  *   // Add supervision strategy
  *   implicit val decider: Supervision.Decider = {
  *     case _: TransientException => Supervision.Resume
  *     case _                     => Supervision.Stop
  *   }
  *
  *   client.scroll(sqlQuery)
  *     .withAttributes(ActorAttributes.supervisionStrategy(decider))
  *     .runWith(Sink.seq)
  * }}}
  *
  * ==Performance==
  *
  * The implementation automatically selects the most efficient strategy:
  *
  * {{{
  * ┌─────────────────┬───────────────┬──────────────────────────────────┐
  * │ ES Version      │ Aggregations  │ Strategy                         │
  * ├─────────────────┼───────────────┼──────────────────────────────────┤
  * │ 7.10+           │ No            │ PIT + search_after (recommended) │
  * │ 7.10+           │ Yes           │ Classic scroll                   │
  * │ < 7.10          │ No            │ search_after                     │
  * │ < 7.10          │ Yes           │ Classic scroll                   │
  * └─────────────────┴───────────────┴──────────────────────────────────┘
  * }}}
  *
  * '''Point In Time (PIT) + search_after''' (ES 7.10+, no aggregations):
  *   - Provides a consistent snapshot of data across pagination
  *   - No scroll context timeout issues
  *   - Better resource usage and performance
  *   - Automatic cleanup on completion
  *
  * '''search_after''' (ES < 7.10, no aggregations):
  *   - Efficient pagination without server-side state
  *   - Suitable for deep pagination
  *   - Requires explicit sort fields
  *
  * '''Classic scroll''' (all versions, with aggregations):
  *   - Required for queries with aggregations
  *   - Maintains a consistent snapshot
  *   - Automatic cleanup of scroll contexts
  *   - Subject to scroll timeout (configurable via keepAlive)
  *
  * @note
  *   PIT is not supported for aggregation queries. The implementation automatically falls back to
  *   classic scroll when aggregations are detected.
  * @see
  *   [[ScrollConfig]] for configuration options
  * @see
  *   [[https://www.elastic.co/guide/en/elasticsearch/reference/7.10/point-in-time-api.html PIT API Documentation]]
  */
trait ScrollApi extends ElasticClientHelpers {
  _: VersionApi with SearchApi =>

  // ========================================================================
  // MAIN SCROLL METHODS
  // ========================================================================

  /** Create a scrolling source with automatic strategy selection
    */
  def scroll(
    sql: SQLQuery,
    config: ScrollConfig = ScrollConfig()
  )(implicit system: ActorSystem): Source[(Map[String, Any], ScrollMetrics), NotUsed] = {
    sql.request match {
      case Some(Left(single)) =>
        val sqlRequest = single.copy(score = sql.score)
        val elasticQuery =
          ElasticQuery(sqlRequest, collection.immutable.Seq(sqlRequest.sources: _*))
        scrollWithMetrics(
          elasticQuery,
          sqlRequest.fieldAliases,
          sqlRequest.sqlAggregations,
          config,
          single.sorts.nonEmpty
        )

      case Some(Right(_)) =>
        Source.failed(
          new UnsupportedOperationException("Scrolling is not supported for multi-search queries")
        )

      case None =>
        Source.failed(
          new IllegalArgumentException("SQL query does not contain a valid search request")
        )
    }
  }

  /** Classic scroll (works for both hits and aggregations)
    */
  private[client] def scrollClassic(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation],
    config: ScrollConfig
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed]

  /** Search After (only for hits, more efficient)
    */
  private[client] def searchAfter(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    config: ScrollConfig,
    hasSorts: Boolean = false
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed]

  private[client] def pitSearchAfter(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    config: ScrollConfig,
    hasSorts: Boolean = false
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed]

  /** Typed scroll source
    */
  def scrollAs[T](
    sql: SQLQuery,
    config: ScrollConfig = ScrollConfig()
  )(implicit
    system: ActorSystem,
    m: Manifest[T],
    formats: Formats
  ): Source[(T, ScrollMetrics), NotUsed] = {
    scroll(sql, config).map { row =>
      (convertTo[T](row._1)(m, formats), row._2)
    }
  }

  // ========================================================================
  // PRIVATE METHODS
  // ========================================================================

  /** Determine the best scroll strategy based on the query
    */
  private def determineScrollStrategy(
    elasticQuery: ElasticQuery,
    aggregations: Map[String, SQLAggregation]
  ): ScrollStrategy = {
    // If aggregations are present, use classic scrolling
    if (aggregations.nonEmpty) {
      UseScroll
    } else {
      // Check if the query contains aggregations in the JSON
      if (hasAggregations(elasticQuery.query)) {
        UseScroll
      } else {
        // Detect version and choose implementation
        version match {
          case result.ElasticSuccess(v) =>
            if (ElasticsearchVersion.supportsPit(v)) {
              logger.info(s"ES version $v supports PIT, using pitSearchAfterSource")
              UsePIT
            } else {
              logger.info(s"ES version $v does not support PIT, using classic search_after")
              UseSearchAfter
            }
          case result.ElasticFailure(err) =>
            throw new RuntimeException(s"Failed to get ES version: $err")
        }
      }
    }
  }

  /** Scroll with metrics tracking
    */
  private def scrollWithMetrics(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation],
    config: ScrollConfig,
    hasSorts: Boolean = false
  )(implicit system: ActorSystem): Source[(Map[String, Any], ScrollMetrics), NotUsed] = {

    implicit val ec: ExecutionContext = system.dispatcher

    val metricsPromise = Promise[ScrollMetrics]()

    scroll(elasticQuery, fieldAliases, aggregations, config, hasSorts)
      .take(config.maxDocuments.getOrElse(Long.MaxValue))
      .grouped(config.scrollSize)
      .statefulMapConcat { () =>
        var metrics = config.metrics // Thread-safe as statefulMapConcat is single-threaded
        batch => {
          metrics = metrics.copy(
            totalDocuments = metrics.totalDocuments + batch.size,
            totalBatches = metrics.totalBatches + 1
          )

          if (metrics.totalBatches % config.logEvery == 0) {
            logger.info(
              s"Scroll progress: ${metrics.totalDocuments} docs, " +
              s"${metrics.totalBatches} batches, " +
              s"${metrics.documentsPerSecond} docs/sec"
            )
          }
          batch.map(doc => (doc, metrics))
        }

      }
      .alsoTo(Sink.last.mapMaterializedValue { lastFuture =>
        lastFuture
          .map(_._2)
          .onComplete {
            case Success(finalMetrics) =>
              val completed = finalMetrics.complete
              logger.info(
                s"Scroll completed: ${completed.totalDocuments} docs in ${completed.duration}ms " +
                s"(${completed.documentsPerSecond} docs/sec)"
              )
              metricsPromise.success(completed)
            case Failure(ex) =>
              logger.error("Failed to get final metrics", ex)
              metricsPromise.failure(ex)
          }(system.dispatcher)
      })
      .mapMaterializedValue(_ => NotUsed)
  }

  private def hasAggregations(query: String): Boolean = {
    try {
      val json = parse(query)
      (json \ "aggregations") != JNothing || (json \ "aggs") != JNothing
    } catch {
      case _: Exception => false
    }
  }

  /** Create a scrolling source for JSON query with automatic strategy
    */
  private def scroll(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation],
    config: ScrollConfig,
    hasSorts: Boolean
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = {
    val strategy = determineScrollStrategy(elasticQuery, aggregations)

    logger.info(
      s"Using scroll strategy: $strategy for query on ${elasticQuery.indices.mkString(", ")}"
    )

    strategy match {
      case UseScroll =>
        logger.info("Using classic scroll (supports aggregations)")
        scrollClassic(elasticQuery, fieldAliases, aggregations, config)

      case UseSearchAfter if config.preferSearchAfter =>
        logger.info("Using search_after (optimized for hits only)")
        searchAfter(elasticQuery, fieldAliases, config, hasSorts)

      case UsePIT =>
        logger.info("Using PIT + search_after (optimized for hits only)")
        pitSearchAfter(elasticQuery, fieldAliases, config, hasSorts)

      case _ =>
        logger.info("Falling back to classic scroll")
        scrollClassic(elasticQuery, fieldAliases, aggregations, config)
    }
  }

}
