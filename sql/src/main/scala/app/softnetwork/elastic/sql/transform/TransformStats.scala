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

package app.softnetwork.elastic.sql.transform

/** Statistics for a Transform
  *
  * @param id
  *   Transform ID
  * @param state
  *   Current state of the Transform
  * @param documentsProcessed
  *   Total number of documents processed
  * @param documentsIndexed
  *   Total number of documents indexed
  * @param indexFailures
  *   Total number of indexing failures
  * @param searchFailures
  *   Total number of search failures
  * @param lastCheckpoint
  *   Last checkpoint number (if any)
  * @param operationsBehind
  *   Number of operations behind source index
  * @param processingTimeMs
  *   Total processing time in milliseconds
  */
case class TransformStats(
  id: String,
  state: TransformState, // "started", "stopped", "failed"
  documentsProcessed: Long,
  documentsIndexed: Long,
  indexFailures: Long,
  searchFailures: Long,
  lastCheckpoint: Option[Long],
  operationsBehind: Long,
  processingTimeMs: Long
)
