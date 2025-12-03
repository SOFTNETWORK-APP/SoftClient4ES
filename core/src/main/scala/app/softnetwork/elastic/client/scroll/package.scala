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

package object scroll {

  /** Scroll configuration
    */
  case class ScrollConfig(
    keepAlive: String = "1m", // Keep-alive time for scroll context
    scrollSize: Int = 1000, // Number of documents per batch
    logEvery: Int = 10, // Log progress every n batches
    maxDocuments: Option[Long] = None, // Optional maximum number of documents to retrieve
    preferSearchAfter: Boolean = true, // Prefer search_after over scroll when possible
    metrics: ScrollMetrics = ScrollMetrics(), // Initial scroll metrics
    retryConfig: RetryConfig = RetryConfig(), // Retry configuration
    failOnWindowError: Option[Boolean] = None
  )

  /** Scroll strategy based on query type
    */
  sealed trait ScrollStrategy
  case object UsePIT
      extends ScrollStrategy // Point In Time + search_after (ES 7.10+, best performance)
  case object UseScroll extends ScrollStrategy // Classic scroll (supports aggregations)
  case object UseSearchAfter
      extends ScrollStrategy // search_after only (efficient, no server state)

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

}
