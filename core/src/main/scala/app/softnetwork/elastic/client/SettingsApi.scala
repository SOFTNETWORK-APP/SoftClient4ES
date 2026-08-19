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

package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess
}
import com.google.gson.JsonParser

import scala.jdk.CollectionConverters._

/** Settings management API.
  */
trait SettingsApi { _: IndicesApi =>

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  /** Toggle the refresh interval of an index.
    * @param index
    *   - the name of the index
    * @param enable
    *   - true to enable the refresh interval, false to disable it
    * @return
    *   true if the settings were updated successfully, false otherwise
    */
  def toggleRefresh(index: String, enable: Boolean): ElasticResult[Boolean] = {
    val refreshValue = if (enable) "1s" else "-1"
    updateSettings(index, s"""{"index": {"refresh_interval": "$refreshValue"}}""")
  }

  /** Set the number of replicas for an index.
    * @param index
    *   - the name of the index
    * @param replicas
    *   - the number of replicas to set
    * @return
    *   true if the settings were updated successfully, false otherwise
    */
  def setReplicas(index: String, replicas: Int): ElasticResult[Boolean] = {
    updateSettings(index, s"""{"index" : {"number_of_replicas" : $replicas} }""")
  }

  /** Update index settings.
    * @param index
    *   - the name of the index
    * @param settings
    *   - the settings to apply to the index (default is defaultSettings)
    * @return
    *   true if the settings were updated successfully, false otherwise
    */
  def updateSettings(index: String, settings: String = defaultSettings): ElasticResult[Boolean] = {
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            message = s"Invalid index: ${error.message}",
            statusCode = Some(400),
            index = Some(index),
            operation = Some("updateSettings")
          )
        )
      case None => // OK
    }

    validateJsonSettings(settings) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            message = s"Invalid settings: ${error.message}",
            statusCode = Some(400),
            index = Some(index),
            operation = Some("updateSettings")
          )
        )
      case None => // OK
    }

    logger.debug(s"🔧 Updating settings for index $index: $settings")

    closeIndex(index) match {
      case failure @ ElasticFailure(error) =>
        logger.error(
          s"❌ Closing index $index failed, settings for index '$index' will not be updated: ${error.message}"
        )
        failure
      case ElasticSuccess(true) =>
        executeUpdateSettings(index, settings) match {
          case failure @ ElasticFailure(error) =>
            logger.error(s"❌ Updating settings for index '$index' failed: ${error.message}")
            failure
          case ElasticSuccess(false) =>
            ElasticResult.failure(
              ElasticError(
                message = s"❌ Updating settings for index '$index' failed",
                operation = Some("updateSettings"),
                index = Some(index)
              )
            )
          case _ =>
            logger.info(s"✅ Updating settings for index '$index' succeeded")
            openIndex(index)
        }
    }
  }

  /** Get the refresh interval of an index.
    * @param index
    *   - the name of the index to get the refresh interval for
    * @return
    *   the refresh interval of the index
    */
  def getRefreshInterval(index: String): ElasticResult[String] = {
    loadSettings(index).flatMap { settingsJson =>
      ElasticResult.attempt(
        JsonParser.parseString(settingsJson).getAsJsonObject
      ) match {
        case ElasticFailure(error) =>
          logger.error(s"❌ Failed to parse JSON settings for index '$index': ${error.message}")
          ElasticFailure(error.copy(operation = Some("getRefreshInterval")))
        case ElasticSuccess(settingsObj) =>
          if (Option(settingsObj).isDefined && settingsObj.has("refresh_interval")) {
            val refreshInterval = settingsObj
              .getAsJsonPrimitive("refresh_interval")
              .getAsString
            ElasticSuccess(refreshInterval)
          } else {
            val message = s"refresh_interval not found in the settings for index '$index'."
            logger.error(s"❌ $message")
            ElasticFailure(
              ElasticError(
                message = message,
                operation = Some("getRefreshInterval"),
                index = Some(index)
              )
            )
          }
      }
    }
  }

  /** Check if the refresh interval is enabled for an index.
    * @param index
    *   - the name of the index to check
    * @return
    *   true if the refresh interval is enabled, false otherwise
    */
  def isRefreshEnabled(index: String): ElasticResult[Boolean] = {
    getRefreshInterval(index).flatMap { refreshInterval =>
      ElasticSuccess(refreshInterval != "-1")
    }
  }

  /** Load the settings of an index.
    * @param index
    *   - the name of the index to load the settings for
    * @return
    *   the settings of the index as a JSON string
    */
  def loadSettings(index: String): ElasticResult[String] = {
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            message = s"Invalid index: ${error.message}",
            statusCode = Some(400),
            index = Some(index),
            operation = Some("updateSettings")
          )
        )
      case None => // OK
    }

    logger.debug(s"🔍 Loading settings for index $index")

    executeLoadSettings(index).flatMap { jsonString =>
      // ✅ Extracting settings from JSON
      ElasticResult.attempt(
        JsonParser.parseString(jsonString).getAsJsonObject
      ) match {
        case ElasticFailure(error) =>
          logger.error(s"❌ Failed to parse JSON settings for index '$index': ${error.message}")
          return ElasticFailure(error.copy(operation = Some("loadSettings")))
        case ElasticSuccess(indexObj) =>
          if (Option(indexObj).isDefined && indexObj.has(index)) {
            val settingsObj = indexObj
              .getAsJsonObject(index)
              .getAsJsonObject("settings")
              .getAsJsonObject("index")
            ElasticSuccess(settingsObj.toString)
          } else {
            val message = s"Index '$index' not found in the loaded settings."
            logger.error(s"❌ $message")
            ElasticFailure(
              ElasticError(
                message = message,
                operation = Some("loadSettings"),
                index = Some(index)
              )
            )
          }
      }
    }
  }

  /** #238 — primary shard count of a PIT over `indices`: the sum of `index.number_of_shards` over
    * every concrete index the expressions resolve to (aliases, wildcards, data streams — one
    * top-level key per concrete index, the shape `loadSettings` parses), deduplicated by concrete
    * index name across expressions. Elasticsearch compares a slice `max` with the shard count of
    * the WHOLE request (`SliceBuilder.toFilter`), so the sum is the right quantity. Failures are an
    * `ElasticFailure` (the caller degrades to sequential paging — never a throw); zero keys (an
    * empty match, `NopeClientApi`'s `"{}"`) count as 1; cross-cluster expressions (`remote:index`)
    * have no `_settings` route and are skipped.
    */
  private[client] def primaryShardCount(indices: Seq[String]): ElasticResult[Int] = {
    val perIndex = scala.collection.mutable.LinkedHashMap.empty[String, Int]
    val failure = indices.distinct.iterator
      .filter { expr =>
        val crossCluster = expr.contains(":")
        if (crossCluster) {
          logger.debug(s"Skipping shard lookup for cross-cluster expression '$expr'")
        }
        !crossCluster
      }
      .map { expr =>
        executeLoadSettings(expr).flatMap { json =>
          ElasticResult.attempt {
            JsonParser.parseString(json).getAsJsonObject.entrySet().asScala.foreach { e =>
              val n = Option(e.getValue)
                .filter(_.isJsonObject)
                .map(_.getAsJsonObject)
                .flatMap(o => Option(o.getAsJsonObject("settings")))
                .flatMap(s => Option(s.getAsJsonObject("index")))
                .flatMap(i => Option(i.get("number_of_shards")))
                .map(_.getAsString) // a string on every client; number-safe too
                .filter(s => s.nonEmpty && s.forall(_.isDigit))
                .map(_.toInt)
                .getOrElse(1)
              perIndex.getOrElseUpdate(e.getKey, n)
            }
          }
        }
      }
      .collectFirst { case f @ ElasticFailure(_) => f }
    failure.getOrElse(ElasticSuccess(math.max(1, perIndex.values.sum)))
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeUpdateSettings(
    index: String,
    settings: String
  ): ElasticResult[Boolean]

  private[client] def executeLoadSettings(
    index: String
  ): ElasticResult[String]
}
