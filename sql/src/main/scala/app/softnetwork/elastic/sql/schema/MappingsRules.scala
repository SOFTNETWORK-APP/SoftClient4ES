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

package app.softnetwork.elastic.sql.schema

object MappingsRules {

  // Options can be modified without reindexing
  private val safeOptions = Set(
    "ignore_above",
    "null_value",
    "store",
    "boost",
    "coerce",
    "copy_to",
    "meta"
  )

  // Options requiring a reindex
  private val unsafeOptions = Set(
    "analyzer",
    "search_analyzer",
    "normalizer",
    "index",
    "doc_values",
    "format",
    "fields",
    "similarity",
    "eager_global_ordinals"
  )

  def isSafe(key: String): Boolean = {

    if (safeOptions.contains(key)) return true

    if (unsafeOptions.contains(key)) return false

    // Default: cautious → UNSAFE
    false
  }
}
