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

import app.softnetwork.elastic.client.{IndexApi, SerializationApi, SettingsApi}
import app.softnetwork.elastic.client.result.ElasticResult
import io.searchbox.core.Index

import scala.concurrent.{ExecutionContext, Future}

/** Index Management API for Jest (Elasticsearch HTTP Client).
  * @see
  *   [[IndexApi]] for generic API documentation
  */
trait JestIndexApi extends IndexApi with JestClientHelpers {
  _: SettingsApi with JestClientCompanion with SerializationApi =>

  /** Index a document in the given index.
    * @see
    *   [[IndexApi.indexAs]]
    */
  override private[client] def executeIndex(
    index: String,
    id: String,
    source: String,
    wait: Boolean
  ): ElasticResult[Boolean] =
    executeJestBooleanAction(
      operation = "index",
      index = Some(index),
      retryable = true
    )(
      new Index.Builder(source)
        .index(index)
        .`type`("_doc")
        .id(id)
        .setParameter("refresh", if (wait) "wait_for" else "false")
        .build()
    )

  /** Index a document in the given index asynchronously.
    * @see
    *   [[IndexApi.indexAsyncAs]]
    */
  override private[client] def executeIndexAsync(
    index: String,
    id: String,
    source: String,
    wait: Boolean
  )(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Boolean]] =
    executeAsyncJestBooleanAction(
      operation = "indexAsync",
      index = Some(index),
      retryable = true
    )(
      new Index.Builder(source)
        .index(index)
        .`type`("_doc")
        .id(id)
        .setParameter("refresh", if (wait) "wait_for" else "false")
        .build()
    )

}
