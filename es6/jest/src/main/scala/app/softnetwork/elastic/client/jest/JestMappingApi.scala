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
import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess
}
import com.google.gson.JsonParser
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
      new GetMapping.Builder().addIndex(index).build()
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
    getMapping(index).flatMap { jsonString =>
      // ✅ Extracting mapping from JSON
      ElasticResult.attempt(
        JsonParser.parseString(jsonString).getAsJsonObject
      ) match {
        case ElasticFailure(error) =>
          logger.error(s"❌ Failed to parse JSON mapping for index '$index': ${error.message}")
          return ElasticFailure(error.copy(operation = Some("getMapping"), index = Some(index)))
        case ElasticSuccess(indexObj) =>
          if (Option(indexObj).isDefined && indexObj.has(index)) {
            val settingsObj = indexObj
              .getAsJsonObject(index)
              .getAsJsonObject("mappings")
              .getAsJsonObject("_doc")
            ElasticSuccess(settingsObj.toString)
          } else {
            val message = s"Index '$index' not found in the loaded mapping."
            logger.error(s"❌ $message")
            ElasticFailure(
              ElasticError(
                message = message,
                operation = Some("getMapping"),
                index = Some(index)
              )
            )
          }
      }
    }
  }
}
