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
        new JsonParser().parse(jsonString).getAsJsonObject
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
