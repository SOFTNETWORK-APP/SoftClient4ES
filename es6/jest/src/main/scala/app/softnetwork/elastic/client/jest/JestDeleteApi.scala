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

import app.softnetwork.elastic.client.{DeleteApi, RefreshApi}
import app.softnetwork.elastic.client.result.ElasticResult
import io.searchbox.core.Delete

import scala.concurrent.{ExecutionContext, Future}

/** Delete Management API for Jest (Elasticsearch HTTP Client).
  * @see
  *   [[DeleteApi]] for generic API documentation
  */
trait JestDeleteApi extends DeleteApi with JestClientHelpers {
  _: RefreshApi with JestClientCompanion =>

  /** Delete an entity from the given index.
    * @see
    *   [[DeleteApi.delete]]
    */
  private[client] def executeDelete(index: String, id: String): ElasticResult[Boolean] =
    executeJestBooleanAction(
      operation = "delete",
      index = Some(index),
      retryable = true
    ) {
      new Delete.Builder(id).index(index).`type`("_doc").build()
    }

  override private[client] def executeDeleteAsync(index: String, id: String)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Boolean]] =
    executeAsyncJestAction(
      operation = "delete",
      index = Some(index),
      retryable = true
    ) {
      new Delete.Builder(id).index(index).`type`("_doc").build()
    }(result => result.isSucceeded)

}
