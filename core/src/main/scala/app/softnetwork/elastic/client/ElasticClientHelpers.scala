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
    *   - Max length 255 characters
    * @param index
    *   name of the index to validate
    * @return
    *   Some(ElasticError) if invalid, None if valid
    */
  protected def validateIndexName(index: String, pattern: Boolean = false): Option[ElasticError] = {
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

    val invalidChars = if (pattern) """[\\/?"<>| ,#]""".r else """[\\/*?"<>| ,#]""".r
    if (invalidChars.findFirstIn(trimmed).isDefined) {
      return Some(
        ElasticError(
          message = "Index name contains invalid characters: /, *, ?, \", <, >, |, space, comma, #",
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

    val trimmed = jsonString.trim
    if (trimmed.contains("//") || trimmed.contains("/*")) {
      return Some(
        ElasticError(
          message = "Invalid JSON: Comments are not allowed in JSON",
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

  /** Validate the JSON mappings.
    * @param mappings
    *   mappings in JSON format
    * @return
    *   Some(ElasticError) if invalid, None if valid
    */
  protected def validateJsonMappings(mappings: String): Option[ElasticError] = {
    validateJson("validateJsonMappings", mappings)
  }

  /** Validate the JSON pipeline definition.
    * @param pipelineDefinition
    *   pipeline definition in JSON format
    * @return
    *   Some(ElasticError) if invalid, None if valid
    */
  protected def validateJsonPipeline(pipelineDefinition: String): Option[ElasticError] = {
    validateJson("validateJsonPipeline", pipelineDefinition)
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
            operation = Some("validateAliasName"),
            message = error.message.replaceAll("Index", "Alias")
          )
        )
      case None => None
    }
  }

  /** Validate the pipeline name. Pipelines do not follow the same rules as indexes. only
    * alphanumeric characters, points (.), underscores (_), hyphens (-) and at-signs (@) are allowed
    * @param pipelineName
    *   pipeline name to validate
    * @return
    *   Some(ElasticError) if invalid, None if valid
    */
  protected def validatePipelineName(pipelineName: String): Option[ElasticError] = {
    if (pipelineName == null || pipelineName.trim.isEmpty) {
      return Some(
        ElasticError(
          message = "Pipeline name cannot be empty",
          cause = None,
          statusCode = Some(400),
          operation = Some("validatePipelineName")
        )
      )
    }

    val trimmed = pipelineName.trim

    val pattern = "^[a-zA-Z0-9._\\-@]+$".r

    if (!pattern.matches(trimmed)) {
      return Some(
        ElasticError(
          message =
            "Pipeline name contains invalid characters: only alphanumeric characters, points (.), underscores (_), hyphens (-) and at-signs (@) are allowed",
          cause = None,
          statusCode = Some(400),
          operation = Some("validatePipelineName")
        )
      )
    }

    None
  }

  /** Validate the template name. Index templates follow the same rules as indexes:
    *   - Not empty
    *   - Lowercase only
    *   - No characters: \, /, *, ?, ", <, >, |, space, comma, #
    *   - No colon (:)
    *   - Does not start with -, _, +
    *   - Is not . or ..
    *   - Max length 255 characters
    *   - Can contain wildcards (* and ?) in index_patterns but not in the template name itself
    *
    * @param templateName
    *   name of the template to validate
    * @return
    *   Some(ElasticError) if invalid, None if valid
    */
  protected def validateTemplateName(templateName: String): Option[ElasticError] = {
    validateIndexName(templateName) match {
      case Some(error) =>
        Some(
          error.copy(
            operation = Some("validateTemplateName"),
            message = error.message.replaceAll("Index", "Template")
          )
        )
      case None => None
    }
  }

  /** Validate composable template JSON definition (ES 7.8+) */
  protected def validateJsonComposableTemplate(
    templateDefinition: String
  ): Option[result.ElasticError] = {
    if (templateDefinition == null || templateDefinition.trim.isEmpty) {
      return Some(
        result.ElasticError(
          message = "Template definition cannot be null or empty",
          statusCode = Some(400),
          operation = Some("validateJsonComposableTemplate")
        )
      )
    }

    try {
      import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
      import com.fasterxml.jackson.databind.node.ObjectNode

      val mapper = new ObjectMapper()
      val rootNode: JsonNode = mapper.readTree(templateDefinition)

      if (!rootNode.isObject) {
        return Some(
          result.ElasticError(
            message = "Template definition must be a JSON object",
            statusCode = Some(400),
            operation = Some("validateJsonComposableTemplate")
          )
        )
      }

      val objectNode = rootNode.asInstanceOf[ObjectNode]

      // Composable templates require 'index_patterns'
      if (!objectNode.has("index_patterns")) {
        return Some(
          result.ElasticError(
            message = "Composable template must contain 'index_patterns' field",
            statusCode = Some(400),
            operation = Some("validateJsonComposableTemplate")
          )
        )
      }

      val indexPatternsNode = objectNode.get("index_patterns")
      if (!indexPatternsNode.isArray || indexPatternsNode.size() == 0) {
        return Some(
          result.ElasticError(
            message = "'index_patterns' must be a non-empty array",
            statusCode = Some(400),
            operation = Some("validateJsonComposableTemplate")
          )
        )
      }

      // Valid fields for composable templates
      val validFields = Set(
        "index_patterns",
        "template", // Contains settings, mappings, aliases
        "priority", // Replaces 'order'
        "version",
        "composed_of", // Component templates
        "_meta",
        "data_stream", // For data streams
        "allow_auto_create"
      )

      import scala.jdk.CollectionConverters._
      val templateFields = objectNode.fieldNames().asScala.toSet
      val invalidFields = templateFields -- validFields

      if (invalidFields.nonEmpty) {
        logger.warn(
          s"⚠️ Composable template contains potentially invalid fields: ${invalidFields.mkString(", ")}"
        )
      }

      None

    } catch {
      case e: com.fasterxml.jackson.core.JsonParseException =>
        Some(
          result.ElasticError(
            message = s"Invalid JSON syntax: ${e.getMessage}",
            statusCode = Some(400),
            operation = Some("validateJsonComposableTemplate"),
            cause = Some(e)
          )
        )
      case e: Exception =>
        Some(
          result.ElasticError(
            message = s"Invalid composable template definition: ${e.getMessage}",
            statusCode = Some(400),
            operation = Some("validateJsonComposableTemplate"),
            cause = Some(e)
          )
        )
    }
  }

  /** Validate legacy template JSON definition (ES < 7.8) */
  protected def validateJsonLegacyTemplate(
    templateDefinition: String
  ): Option[result.ElasticError] = {
    if (templateDefinition == null || templateDefinition.trim.isEmpty) {
      return Some(
        result.ElasticError(
          message = "Template definition cannot be null or empty",
          statusCode = Some(400),
          operation = Some("validateJsonLegacyTemplate")
        )
      )
    }

    try {
      import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
      import com.fasterxml.jackson.databind.node.ObjectNode

      val mapper = new ObjectMapper()
      val rootNode: JsonNode = mapper.readTree(templateDefinition)

      if (!rootNode.isObject) {
        return Some(
          result.ElasticError(
            message = "Template definition must be a JSON object",
            statusCode = Some(400),
            operation = Some("validateJsonLegacyTemplate")
          )
        )
      }

      val objectNode = rootNode.asInstanceOf[ObjectNode]

      // Legacy templates require 'index_patterns' or 'template'
      if (!objectNode.has("index_patterns") && !objectNode.has("template")) {
        return Some(
          result.ElasticError(
            message = "Legacy template must contain 'index_patterns' or 'template' field",
            statusCode = Some(400),
            operation = Some("validateJsonLegacyTemplate")
          )
        )
      }

      // Valid fields for legacy templates
      val validFields = Set(
        "index_patterns",
        "template", // Old name for index_patterns
        "settings",
        "mappings",
        "aliases",
        "order",
        "version",
        "_meta"
      )

      import scala.jdk.CollectionConverters._
      val templateFields = objectNode.fieldNames().asScala.toSet
      val invalidFields = templateFields -- validFields

      if (invalidFields.nonEmpty) {
        logger.warn(
          s"⚠️ Legacy template contains potentially invalid fields: ${invalidFields.mkString(", ")}"
        )
      }

      None

    } catch {
      case e: com.fasterxml.jackson.core.JsonParseException =>
        Some(
          result.ElasticError(
            message = s"Invalid JSON syntax: ${e.getMessage}",
            statusCode = Some(400),
            operation = Some("validateJsonLegacyTemplate"),
            cause = Some(e)
          )
        )
      case e: Exception =>
        Some(
          result.ElasticError(
            message = s"Invalid legacy template definition: ${e.getMessage}",
            statusCode = Some(400),
            operation = Some("validateJsonLegacyTemplate"),
            cause = Some(e)
          )
        )
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
