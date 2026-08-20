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

import app.softnetwork.elastic.client.scroll.ScrollConfig

/** Paged row extraction settings (`elastic.scroll` in HOCON, #238).
  *
  * @param size
  *   rows per page (`elastic.scroll.size`, `ELASTIC_SCROLL_SIZE`): larger pages cut round-trips
  *   linearly and raise in-flight memory linearly. Must be positive. Not validated here: on the PIT
  *   / search_after path Elasticsearch rejects a page larger than the index's `max_result_window`
  *   (10,000 by default), so keep `size` at or below it
  * @param maxSlices
  *   ceiling on concurrent PIT slices for a no-ORDER-BY extraction (`elastic.scroll.max-slices`,
  *   `ELASTIC_SCROLL_MAX_SLICES`, ES 7.15+): the effective count is min(primary shards,
  *   max-slices); 1 disables slicing (sequential paging)
  */
case class ScrollSettings(
  size: Int = 1000,
  maxSlices: Int = ScrollConfig.DefaultMaxSlices
) {
  require(size > 0, s"elastic.scroll.size must be positive (ELASTIC_SCROLL_SIZE), got $size")
  require(
    maxSlices >= 1,
    s"elastic.scroll.max-slices must be >= 1 (ELASTIC_SCROLL_MAX_SLICES), got $maxSlices"
  )

  /** REST connection pool per route for the clients that page through slices: every concurrent
    * slice plus the PIT open / close and `_settings` calls that share the route; the Apache default
    * (10) is the floor.
    */
  def restPoolPerRoute: Int = math.max(10, maxSlices + 2)

  /** REST connection pool total; the Apache default (30) is the floor. */
  def restPoolTotal: Int = math.max(30, math.min(restPoolPerRoute, Int.MaxValue / 3) * 3)
}
