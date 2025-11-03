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

import app.softnetwork.elastic.client.{IndicesApi, MappingApi, RefreshApi, SettingsApi}
import app.softnetwork.elastic.client.result.ElasticResult
import io.searchbox.indices.mapping.{GetMapping, PutMapping}

import scala.util.Try

/** Mapping management API for Jest (Elasticsearch HTTP Client).
  * @see
  *   [[MappingApi]] for generic API documentation
  */
trait JestMappingApi extends MappingApi with JestClientHelpers {
  _: SettingsApi with IndicesApi with RefreshApi with JestClientCompanion =>

  /** Set the mapping for an index.
    * @see
    *   [[MappingApi.setMapping]]
    */
  private[client] def executeSetMapping(index: String, mapping: String): ElasticResult[Boolean] = {
    executeJestBooleanAction(
      operation = "setMapping",
      index = Some(index),
      retryable = false
    )(
      new PutMapping.Builder(index, "_doc", mapping).build()
    )
  }

  private[client] def executeGetMapping(index: String): ElasticResult[String] =
    executeJestAction(
      operation = "getMapping",
      index = Some(index),
      retryable = true
    )(
      new GetMapping.Builder().addIndex(index).addType("_doc").build()
    ) { result =>
      result.getJsonString
    }

  /** Get the mapping properties of an index.
    *
    * @param index
    *   - the name of the index to get the mapping properties for
    * @return
    *   the mapping properties of the index as a JSON string
    */
  override def getMappingProperties(index: String): ElasticResult[String] = {
    getMapping(index)
  }
}
