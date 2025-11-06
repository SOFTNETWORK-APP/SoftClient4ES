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

import app.softnetwork.elastic.client.result.ElasticResult
import app.softnetwork.elastic.client.{result, GetApi, SerializationApi}
import io.searchbox.core.Get

import scala.concurrent.{ExecutionContext, Future}

/** Get API for Jest (Elasticsearch HTTP Client).
  * @see
  *   [[GetApi]] for generic API documentation
  */
trait JestGetApi extends GetApi with JestClientHelpers {
  _: JestClientCompanion with SerializationApi =>

  /** Get a document by id.
    * @see
    *   [[GetApi.get]]
    */
  override private[client] def executeGet(
    index: String,
    id: String
  ): result.ElasticResult[Option[String]] =
    executeJestAction(
      operation = "get",
      index = Some(index),
      retryable = true
    ) {
      new Get.Builder(index, id).build()
    } { result =>
      if (result.isSucceeded) {
        Some(result.getSourceAsString)
      } else {
        None
      }
    }

  /** Get a document by its id from the given index asynchronously.
    * @see
    *   [[GetApi.getAsync]]
    */
  override private[client] def executeGetAsync(
    index: String,
    id: String
  )(implicit ec: ExecutionContext): Future[ElasticResult[Option[String]]] =
    executeAsyncJestAction(
      operation = "get",
      index = Some(index),
      retryable = true
    ) {
      new Get.Builder(index, id).build()
    } { result =>
      if (result.isSucceeded) {
        Some(result.getSourceAsString)
      } else {
        None
      }
    }

}
