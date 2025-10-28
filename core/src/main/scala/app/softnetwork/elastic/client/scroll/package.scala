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

}
