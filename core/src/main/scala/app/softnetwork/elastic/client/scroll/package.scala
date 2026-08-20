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

import akka.{Done, NotUsed}
import akka.stream.scaladsl.Source

import scala.concurrent.ExecutionContext
import scala.util.Try

package object scroll {

  /** Scroll configuration
    */
  case class ScrollConfig(
    keepAlive: String = "1m", // Keep-alive time for scroll context
    scrollSize: Int = 1000, // Number of documents per batch
    logEvery: Int = 10, // Log progress every n batches
    maxDocuments: Option[Long] = None, // Optional maximum number of documents to retrieve
    preferSearchAfter: Boolean =
      true, // false = force classic scroll even where PIT/search_after is available (slower; for clusters restricting the PIT API)
    metrics: ScrollMetrics = ScrollMetrics(), // Initial scroll metrics
    retryConfig: RetryConfig = RetryConfig(), // Retry configuration
    failOnWindowError: Option[Boolean] = None,
    // Internal (set by ScrollApi, not by callers): parsed pages keep the document id as an
    // `_id` row key. True for window-enrichment base queries (the ordinal lookup matches rows
    // by document id), when `elastic.include-document-id` is enabled, or when the query
    // selects `_id` explicitly. False keeps the hot scroll path free of any per-row overhead.
    retainDocumentId: Boolean = false,
    // #238 — ceiling on concurrent PIT slices for a no-ORDER-BY extraction. None = inherit the
    // client's `elastic.scroll.max-slices` (so the HOCON/env opt-out reaches explicit configs
    // too); Some(n) = explicit, n <= 1 pages sequentially. Honoured only on PIT + search_after
    // (ES >= 7.15) without sorts; the effective count is min(primary shards, ceiling).
    maxSlices: Option[Int] = None,
    // Internal (set by ScrollApi, not by callers): the slice count resolved for this stream.
    slices: Int = 1
  )

  object ScrollConfig {

    /** Default ceiling — one slice per primary shard up to 8: under the REST client's per-route
      * pool (sized from `elastic.scroll.max-slices` by the es7/es8/es9 companions), and an
      * in-flight bound of about 2 x slices x scrollSize rows.
      */
    val DefaultMaxSlices: Int = 8
  }

  /** Scroll strategy based on query type
    */
  sealed trait ScrollStrategy
  case object UsePIT
      extends ScrollStrategy // Point In Time + search_after (ES 7.12+, best performance)
  case object UseScroll extends ScrollStrategy // Classic scroll (supports aggregations)
  case object UseSearchAfter
      extends ScrollStrategy // search_after only (efficient, no server state)

  /** Scroll metrics
    */
  case class ScrollMetrics(
    totalDocuments: Long = 0,
    totalBatches: Long = 0,
    startTime: Long = System.currentTimeMillis(),
    endTime: Option[Long] = None,
    slices: Int = 1 // #238 — PIT slices merged into this stream (1 = sequential)
  ) {
    def duration: Long = endTime.getOrElse(System.currentTimeMillis()) - startTime
    def documentsPerSecond: Double = totalDocuments.toDouble / (duration / 1000.0)
    def complete: ScrollMetrics = copy(endTime = Some(System.currentTimeMillis()))
  }

  /** #238 — merge independent PIT slice PAGE sources into ONE backpressured stream.
    *
    * `onTerminate` fires exactly once: on completion, failure, or downstream cancellation. A
    * failing slice fails the merged stream (it never truncates it — the #228/#209/#224 lesson).
    * `ec` must be the system dispatcher: `onTerminate` closes the PIT with a blocking call.
    *
    * The helper deliberately has no `require`: nothing between a successful `openPit` and the
    * attachment of `watchTermination` may throw (#202 — the single PIT owner rule).
    */
  object SliceMerge {
    def apply[T](slices: Seq[Source[T, NotUsed]])(onTerminate: Try[Done] => Unit)(implicit
      ec: ExecutionContext
    ): Source[T, NotUsed] = {
      val merged: Source[T, NotUsed] = slices match {
        case Seq()       => Source.empty[T]
        case Seq(single) => single
        case many =>
          Source(many.toList).flatMapMerge(many.size, (s: Source[T, NotUsed]) => s)
      }
      merged.watchTermination() { (_, done) =>
        done.onComplete(onTerminate)
        NotUsed
      }
    }
  }

}
