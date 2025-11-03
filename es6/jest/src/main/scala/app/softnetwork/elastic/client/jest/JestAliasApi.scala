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

import app.softnetwork.elastic.client.AliasApi
import app.softnetwork.elastic.client.result.ElasticResult
import io.searchbox.client.JestResult
import io.searchbox.indices.aliases.{AddAliasMapping, GetAliases, ModifyAliases, RemoveAliasMapping}

import scala.jdk.CollectionConverters._

/** Alias management API for Jest (Elasticsearch HTTP Client).
  * @see
  *   [[AliasApi]] for generic API documentation
  */
trait JestAliasApi extends AliasApi with JestClientHelpers {
  _: JestIndicesApi with JestClientCompanion =>

  /** Add an alias to an index.
    * @see
    *   [[AliasApi.addAlias]]
    */
  private[client] def executeAddAlias(index: String, alias: String): ElasticResult[Boolean] =
    executeJestBooleanAction(
      operation = "addAlias",
      index = Some(s"$index -> $alias"),
      retryable = false // Aliases operations can not be retried
    ) {
      new ModifyAliases.Builder(
        new AddAliasMapping.Builder(index, alias).build()
      ).build()
    }

  /** Remove an alias from an index.
    * @see
    *   [[AliasApi.removeAlias]]
    */
  private[client] def executeRemoveAlias(index: String, alias: String): ElasticResult[Boolean] =
    executeJestBooleanAction(
      operation = "removeAlias",
      index = Some(s"$index -> $alias"),
      retryable = false
    ) {
      new ModifyAliases.Builder(
        new RemoveAliasMapping.Builder(index, alias).build()
      ).build()
    }

  /** Check if an alias exists.
    * @see
    *   [[AliasApi.aliasExists]]
    */
  override private[client] def executeAliasExists(alias: String): ElasticResult[Boolean] =
    executeJestBooleanAction(
      operation = "aliasExists",
      index = Some(alias),
      retryable = true
    ) {
      new GetAliases.Builder()
        .addAlias(alias)
        .build()
    }

  /** Get aliases for a given index.
    * @see
    *   [[AliasApi.getAliases]]
    */
  private[client] def executeGetAliases(index: String): ElasticResult[String] =
    executeJestAction[JestResult, String](
      operation = "getAliases",
      index = Some(index),
      retryable = true
    ) {
      new GetAliases.Builder()
        .addIndex(index)
        .build()
    } { result =>
      result.getJsonString
    }

  /** Swap an alias from an old index to a new index atomically.
    * @see
    *   [[AliasApi.swapAlias]]
    */
  override private[client] def executeSwapAlias(
    oldIndex: String,
    newIndex: String,
    alias: String
  ): ElasticResult[Boolean] =
    executeJestBooleanAction(
      operation = "swapAlias",
      index = Some(s"$oldIndex -> $newIndex"),
      retryable = false
    ) {
      new ModifyAliases.Builder(
        Seq(
          // ✅ Remove from old index
          new RemoveAliasMapping.Builder(oldIndex, alias).build(),
          // ✅ Add to new index
          new AddAliasMapping.Builder(newIndex, alias).build()
        ).asJava
      ).build()
    }
}
