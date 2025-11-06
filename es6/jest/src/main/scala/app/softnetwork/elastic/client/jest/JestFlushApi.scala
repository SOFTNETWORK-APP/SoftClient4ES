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

package app.softnetwork.elastic.client.jest

import app.softnetwork.elastic.client.FlushApi
import app.softnetwork.elastic.client.result.ElasticResult
import io.searchbox.indices.Flush

/** Flush management API for Jest (Elasticsearch HTTP Client).
  * @see
  *   [[FlushApi]] for generic API documentation
  */
trait JestFlushApi extends FlushApi with JestClientHelpers { _: JestClientCompanion =>

  /** Flush the index to make sure all operations are written to disk.
    * @see
    *   [[FlushApi.flush]]
    */
  private[client] def executeFlush(
    index: String,
    force: Boolean,
    wait: Boolean
  ): ElasticResult[Boolean] = {
    executeJestBooleanAction(
      operation = "flush",
      index = Some(index),
      retryable = true
    )(
      new Flush.Builder().addIndex(index).force(force).waitIfOngoing(wait).build()
    )
  }
}
