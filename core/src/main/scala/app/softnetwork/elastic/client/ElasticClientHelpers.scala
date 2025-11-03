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

import app.softnetwork.elastic.client.result.ElasticError
import org.slf4j.Logger

import scala.language.reflectiveCalls
import scala.util.{Failure, Success, Try}

trait ElasticClientHelpers {

  protected def logger: Logger

  /** Validate the name of an index. Elasticsearch rules:
    *   - Not empty
    *   - Lowercase only
    *   - No characters: \, /, *, ?, ", <, >, |, space, comma, #
    *   - No colon (:) except for system indexes
    *   - Does not start with -, _, +
    *   - Is not . or ..
    * @param index
    *   name of the index to validate
    * @return
    *   Some(ElasticError) if invalid, None if valid
    */
  protected def validateIndexName(index: String): Option[ElasticError] = {
    if (index == null || index.trim.isEmpty) {
      return Some(
        ElasticError(
          message = "Index name cannot be empty",
          cause = None,
          statusCode = Some(400),
          operation = Some("validateIndexName")
        )
      )
    }

    val trimmed = index.trim

    // ✅ Elasticsearch rules
    if (trimmed == "." || trimmed == "..") {
      return Some(
        ElasticError(
          message = s"Index name cannot be '.' or '..'",
          cause = None,
          statusCode = Some(400),
          operation = Some("validateIndexName")
        )
      )
    }

    if (trimmed.startsWith("-") || trimmed.startsWith("_") || trimmed.startsWith("+")) {
      return Some(
        ElasticError(
          message = s"Index name cannot start with '-', '_', or '+'",
          cause = None,
          statusCode = Some(400),
          operation = Some("validateIndexName")
        )
      )
    }

    if (trimmed != trimmed.toLowerCase) {
      return Some(
        ElasticError(
          message = s"Index name must be lowercase",
          cause = None,
          statusCode = Some(400),
          operation = Some("validateIndexName")
        )
      )
    }

    val invalidChars = """[\\/*?"<>| ,#]""".r
    if (invalidChars.findFirstIn(trimmed).isDefined) {
      return Some(
        ElasticError(
          message =
            s"Index name contains invalid characters: \\, /, *, ?, \", <, >, |, space, comma, #",
          cause = None,
          statusCode = Some(400),
          operation = Some("validateIndexName")
        )
      )
    }

    if (trimmed.length > 255) {
      return Some(
        ElasticError(
          message = s"Index name is too long (max 255 characters): ${trimmed.length}",
          cause = None,
          statusCode = Some(400),
          operation = Some("validateIndexName")
        )
      )
    }

    None // Valid
  }

  /** Validate the JSON.
    * @param operation
    *   name of the operation
    * @param jsonString
    *   the JSON to validate
    * @return
    *   Some(ElasticError) if invalid, None if valid
    */
  protected def validateJson(
    operation: String = "validateJson",
    jsonString: String
  ): Option[ElasticError] = {
    if (jsonString == null || jsonString.trim.isEmpty) {
      return Some(
        ElasticError(
          message = "Settings cannot be empty",
          cause = None,
          statusCode = Some(400),
          operation = Some(operation)
        )
      )
    }

    // ✅ Basic JSON validation
    Try {
      import org.json4s.jackson.JsonMethods._
      parse(jsonString)
    } match {
      case Success(_) => None // Valid
      case Failure(ex) =>
        Some(
          ElasticError(
            message = s"Invalid JSON: ${ex.getMessage}",
            cause = Some(ex),
            statusCode = Some(400),
            operation = Some(operation)
          )
        )
    }
  }

  /** Validate the JSON settings.
    * @param settings
    *   settings in JSON format
    * @return
    *   Some(ElasticError) if invalid, None if valid
    */
  protected def validateJsonSettings(settings: String): Option[ElasticError] = {
    validateJson("validateJsonSettings", settings)
  }

  /** Validate the alias name. Aliases follow the same rules as indexes.
    * @param alias
    *   alias name to validate
    * @return
    *   Some(ElasticError) if invalid, None if valid
    */
  protected def validateAliasName(alias: String): Option[ElasticError] = {
    // ✅ Aliases follow the same rules as indexes
    validateIndexName(alias) match {
      case Some(error) =>
        Some(
          error.copy(
            operation = Some("validateAliasName")
          )
        )
      case None => None
    }
  }

  /** Logger une erreur avec le niveau approprié selon le status code. */
  protected def logError(
    operation: String,
    indexStr: String,
    error: ElasticError
  ): Unit = {
    error.statusCode match {
      case Some(404) =>
        // 404 n'est pas forcément une erreur (ex: indexExists)
        logger.debug(s"Operation '$operation'$indexStr: ${error.message}")

      case Some(status) if status >= 500 =>
        // Server error
        logger.error(s"Server error during '$operation'$indexStr: ${error.message}")

      case Some(status) if status >= 400 =>
        // Client error
        logger.warn(s"Client error during '$operation'$indexStr: ${error.message}")

      case _ =>
        logger.error(s"Operation '$operation'$indexStr failed: ${error.message}")
    }
  }

}
