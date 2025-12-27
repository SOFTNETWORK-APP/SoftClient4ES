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

import app.softnetwork.elastic.client.IndicesApi
import app.softnetwork.elastic.client.jest.actions.{GetIndex, WaitForShards}
import app.softnetwork.elastic.client.result.ElasticResult
import app.softnetwork.elastic.sql.schema.{mapper, TableAlias}
import com.fasterxml.jackson.databind.node.ObjectNode
import io.searchbox.client.JestResult
import io.searchbox.core.{Cat, CatResult, DeleteByQuery}
import io.searchbox.indices.{CloseIndex, CreateIndex, DeleteIndex, IndicesExists, OpenIndex}
import io.searchbox.indices.reindex.Reindex

import scala.util.Try

/** Index management API for Jest (Elasticsearch HTTP Client).
  * @see
  *   [[IndicesApi]] for generic API documentation
  */
trait JestIndicesApi extends IndicesApi with JestClientHelpers {
  _: JestRefreshApi with JestPipelineApi with JestVersionApi with JestClientCompanion =>

  /** Create an index with the given settings.
    * @see
    *   [[IndicesApi.createIndex]]
    */
  private[client] def executeCreateIndex(
    index: String,
    settings: String = defaultSettings,
    mappings: Option[String],
    aliases: Seq[TableAlias]
  ): ElasticResult[Boolean] = {
    executeJestBooleanAction(
      operation = "createIndex",
      index = Some(index),
      retryable = false // Creation can not be retried
    ) {
      val builder = new CreateIndex.Builder(index)
        .settings(settings)
      if (aliases.nonEmpty) {
        val as = mapper.createObjectNode()
        aliases.foreach { alias =>
          as.set[ObjectNode](alias.alias, alias.node)
        }
        builder.aliases(as.toString)
      }
      mappings.foreach { mapping =>
        builder.mappings(mapping)
      }
      builder.build()
    }
  }

  override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] = {
    executeJestAction[JestResult, Option[String]](
      operation = "getIndex",
      index = Some(index),
      retryable = true
    ) {
      new GetIndex.Builder(index).build()
    } { result =>
      if (result.isSucceeded) {
        Some(result.getJsonString)
      } else {
        None
      }
    }
  }

  /** Delete an index.
    *
    * @see
    *   [[IndicesApi.deleteIndex]]
    */
  private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] = {
    executeJestBooleanAction(
      operation = "deleteIndex",
      index = Some(index),
      retryable = false // Deletion can not be retried
    ) {
      new DeleteIndex.Builder(index).build()
    }
  }

  /** Close an index.
    * @see
    *   [[IndicesApi.closeIndex]]
    */
  private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] = {
    executeJestBooleanAction(
      operation = "closeIndex",
      index = Some(index),
      retryable = true
    ) {
      new CloseIndex.Builder(index).build()
    }
  }

  /** Open an index.
    * @see
    *   [[IndicesApi.openIndex]]
    */
  override def executeOpenIndex(index: String): ElasticResult[Boolean] = {
    executeJestBooleanAction(
      operation = "openIndex",
      index = Some(index),
      retryable = true
    ) {
      new OpenIndex.Builder(index).build()
    }
  }

  /** Reindex documents from a source index to a target index.
    * @see
    *   [[IndicesApi.reindex]]
    */
  private[client] def executeReindex(
    sourceIndex: String,
    targetIndex: String,
    refresh: Boolean,
    pipeline: Option[String]
  ): ElasticResult[(Boolean, Option[Long])] =
    executeJestAction[JestResult, (Boolean, Option[Long])](
      operation = "reindex",
      index = Some(s"$sourceIndex -> $targetIndex"),
      retryable = true
    ) {
      new Reindex.Builder(
        s"""{"index": "$sourceIndex"}""",
        s"""{"index": "$targetIndex"}"""
      ).build()
    } { result =>
      val success = result.isSucceeded
      val docsReindexed = Try {
        result.getJsonObject.get("total").getAsLong
      }.toOption
      (success, docsReindexed)
    }

  /** Check if an index exists.
    * @see
    *   [[IndicesApi.indexExists]]
    */
  private[client] def executeIndexExists(index: String): ElasticResult[Boolean] = {
    executeJestBooleanAction(
      operation = "indexExists",
      index = Some(index),
      retryable = true
    ) {
      new IndicesExists.Builder(index).build()
    }
  }

  private[client] def executeDeleteByQuery(
    index: String,
    jsonQuery: String,
    refresh: Boolean
  ): ElasticResult[Long] =
    executeJestAction[JestResult, Long](
      operation = "deleteByQuery",
      index = Some(index),
      retryable = true
    ) {
      val builder = new DeleteByQuery.Builder(jsonQuery)
        .addIndex(index)

      builder.setParameter("conflicts", "proceed")

      if (refresh)
        builder.setParameter("refresh", true)

      builder.build()
    } { result =>
      val deleted = Try {
        result.getJsonObject.get("deleted").getAsLong
      }.getOrElse(0L)

      deleted
    }

  override private[client] def executeIsIndexClosed(index: String): ElasticResult[Boolean] =
    executeJestAction[CatResult, Boolean](
      operation = "isIndexClosed",
      index = Some(index),
      retryable = true
    ) {
      new Cat.IndicesBuilder()
        .addIndex(index)
        .setParameter("format", "json")
        .build()
    } { result =>
      val json = result.getJsonObject
      val arr = json.getAsJsonArray("result")

      if (arr == null || arr.size() == 0)
        false
      else {
        val entry = arr.get(0).getAsJsonObject
        val status = entry.get("status").getAsString // "open" or "close"
        status == "close"
      }
    }

  override private[client] def waitForShards(
    index: String,
    status: String,
    timeout: Int
  ): ElasticResult[Unit] = {
    executeJestBooleanAction(
      operation = "waitForShards",
      index = Some(index),
      retryable = true
    ) {
      new WaitForShards.Builder(index = index, status = status, timeout = timeout).build()
    }.map(_ => ())
  }
}
