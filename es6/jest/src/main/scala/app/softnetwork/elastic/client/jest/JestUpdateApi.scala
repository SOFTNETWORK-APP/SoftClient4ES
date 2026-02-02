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

import app.softnetwork.elastic.client.UpdateApi
import app.softnetwork.elastic.client.bulk.docAsUpsert
import app.softnetwork.elastic.client.result.ElasticResult
import io.searchbox.core.Update

import scala.concurrent.{ExecutionContext, Future}

/** Update Management API for Jest (Elasticsearch HTTP Client).
  * @see
  *   [[UpdateApi]] for generic API documentation
  */
trait JestUpdateApi extends UpdateApi with JestClientHelpers {
  _: JestSettingsApi with JestClientCompanion =>

  /** Update an entity in the given index.
    * @see
    *   [[UpdateApi.updateAs]]
    */
  override private[client] def executeUpdate(
    index: String,
    id: String,
    source: String,
    upsert: Boolean,
    wait: Boolean
  ): ElasticResult[Boolean] =
    executeJestBooleanAction(
      operation = "update",
      index = Some(index),
      retryable = true
    ) {
      new Update.Builder(
        if (upsert)
          docAsUpsert(source)
        else
          source
      )
        .index(index)
        .`type`("_doc")
        .id(id)
        .setParameter("refresh", if (wait) "wait_for" else "false")
        .build()
    }

  override private[client] def executeUpdateAsync(
    index: String,
    id: String,
    source: String,
    upsert: Boolean,
    wait: Boolean
  )(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]] =
    executeAsyncJestAction(
      operation = "update",
      index = Some(index),
      retryable = true
    ) {
      new Update.Builder(
        if (upsert)
          docAsUpsert(source)
        else
          source
      )
        .index(index)
        .`type`("_doc")
        .id(id)
        .setParameter("refresh", if (wait) "wait_for" else "false")
        .build()
    }(result => result.isSucceeded)

}
