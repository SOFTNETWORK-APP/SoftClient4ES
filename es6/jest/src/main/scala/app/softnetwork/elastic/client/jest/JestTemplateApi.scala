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

import app.softnetwork.elastic.client.jest.actions.Template
import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess
}
import app.softnetwork.elastic.client.TemplateApi
import app.softnetwork.elastic.sql.serialization._
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.searchbox.client.JestResult

import scala.jdk.CollectionConverters._
import scala.util.{Failure, Success}

trait JestTemplateApi extends TemplateApi with JestClientHelpers {
  _: JestVersionApi with JestClientCompanion =>

  // ==================== COMPOSABLE TEMPLATES (ES 7.8+) ====================

  override private[client] def executeCreateComposableTemplate(
    templateName: String,
    templateDefinition: String
  ): ElasticResult[Boolean] =
    ElasticFailure(
      ElasticError(
        message = "Composable templates are not supported by Jest client (ES < 7.8 only)",
        statusCode = Some(501), // Not Implemented
        operation = Some("createTemplate")
      )
    )

  override private[client] def executeDeleteComposableTemplate(
    templateName: String,
    ifExists: Boolean
  ): ElasticResult[Boolean] =
    ElasticFailure(
      ElasticError(
        message = "Composable templates are not supported by Jest client (ES < 7.8 only)",
        statusCode = Some(501), // Not Implemented
        operation = Some("deleteTemplate")
      )
    )

  override private[client] def executeGetComposableTemplate(
    templateName: String
  ): ElasticResult[Option[String]] =
    ElasticFailure(
      ElasticError(
        message = "Composable templates are not supported by Jest client (ES < 7.8 only)",
        statusCode = Some(501), // Not Implemented
        operation = Some("getTemplate")
      )
    )

  override private[client] def executeListComposableTemplates()
    : ElasticResult[Map[String, String]] =
    ElasticFailure(
      ElasticError(
        message = "Composable templates are not supported by Jest client (ES < 7.8 only)",
        statusCode = Some(501), // Not Implemented
        operation = Some("listTemplates")
      )
    )

  override private[client] def executeComposableTemplateExists(
    templateName: String
  ): ElasticResult[Boolean] =
    ElasticFailure(
      ElasticError(
        message = "Composable templates are not supported by Jest client (ES < 7.8 only)",
        statusCode = Some(501), // Not Implemented
        operation = Some("templateExists")
      )
    )

  // ==================== LEGACY TEMPLATES ====================

  override private[client] def executeCreateLegacyTemplate(
    templateName: String,
    templateDefinition: String
  ): ElasticResult[Boolean] =
    executeJestBooleanAction[JestResult](
      operation = "createLegacyTemplate",
      retryable = false // Creation can not be retried
    ) {
      Template.Create(templateName, templateDefinition)
    }

  override private[client] def executeDeleteLegacyTemplate(
    templateName: String,
    ifExists: Boolean
  ): ElasticResult[Boolean] = {
    if (ifExists) {
      executeLegacyTemplateExists(templateName) match {
        case ElasticSuccess(exists) =>
          if (!exists) {
            logger.debug(s"Legacy template '$templateName' does not exist, skipping deletion")
            return ElasticSuccess(false)
          }
        case failure @ ElasticFailure(_) =>
          return failure
      }
    }
    executeJestBooleanAction[JestResult](
      operation = "deleteLegacyTemplate",
      retryable = false // Deletion can not be retried
    ) {
      Template.Delete(templateName)
    }
  }

  override private[client] def executeGetLegacyTemplate(
    templateName: String
  ): ElasticResult[Option[String]] =
    executeJestAction[JestResult, Option[String]](
      operation = "getLegacyTemplate",
      retryable = true
    ) {
      Template.Get(templateName)
    } { jestResult =>
      val jsonString = jestResult.getJsonString
      if (jsonString != null && jsonString.nonEmpty) {
        val node: JsonNode = jsonString
        node match {
          case objectNode: ObjectNode if objectNode.has(templateName) =>
            Some(objectNode.get(templateName))
          case _ =>
            None
        }
      } else {
        None
      }
    }

  override private[client] def executeListLegacyTemplates(): ElasticResult[Map[String, String]] =
    executeJestAction[JestResult, Map[String, String]](
      operation = "listLegacyTemplates",
      retryable = true
    ) {
      Template.GetAll()
    } { jestResult =>
      val jsonString = jestResult.getJsonString
      if (jsonString != null && jsonString.nonEmpty) {
        val node: JsonNode = jsonString
        node match {
          case objectNode: ObjectNode =>
            objectNode
              .fields()
              .asScala
              .map { entry =>
                entry.getKey -> entry.getValue.toString
              }
              .toMap
          case _ =>
            Map.empty[String, String]
        }
      } else {
        Map.empty[String, String]
      }
    }

  /** Deliberately NOT routed through `executeJestAction`: an existence probe answers "no" with a
    * 404, and the wrapper would turn that into an `ElasticFailure` — which
    * `executeDeleteLegacyTemplate(ifExists = true)` propagates, so `DROP … IF EXISTS` would start
    * failing on the very case it exists to tolerate. `tryAction` supplies the half that #215 is
    * about (a network fault becomes a value, not a throw) while any HTTP response stays a success
    * carrying the boolean.
    */
  override private[client] def executeLegacyTemplateExists(
    templateName: String
  ): ElasticResult[Boolean] =
    tryAction(apply().execute(Template.Exists(templateName))) match {
      case Success(jestResult) if jestResult.getResponseCode == 200 =>
        ElasticSuccess(true)
      case Success(jestResult) if jestResult.getResponseCode == 404 =>
        ElasticSuccess(false)
      case Success(jestResult) =>
        // Only 200 and 404 are answers to "does it exist". Anything else — 503 from an
        // unavailable cluster, 401, a proxy's 502 — was previously read as `false`, so
        // `DROP … IF EXISTS` reported success while the template was still there. An outage is
        // not an absence.
        val statusCode = jestResult.getResponseCode
        ElasticFailure(
          ElasticError(
            message = Option(jestResult.getErrorMessage)
              .filter(_.nonEmpty)
              .getOrElse(s"Unexpected status $statusCode probing template '$templateName'"),
            cause = None,
            statusCode = if (statusCode == 0) None else Some(statusCode),
            operation = Some("legacyTemplateExists")
          )
        )
      case Failure(ex) =>
        logger.error(s"Exception during templateExists for '$templateName': ${ex.getMessage}", ex)
        ElasticFailure(
          ElasticError(
            message = s"Exception during legacyTemplateExists: ${ex.getMessage}",
            cause = Some(ex),
            statusCode = None,
            operation = Some("legacyTemplateExists")
          )
        )
    }

}
