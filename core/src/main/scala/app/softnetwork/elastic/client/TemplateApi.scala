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

import scala.util.{Failure, Success}

/** API for managing Elasticsearch index templates.
  *
  * Automatically handles format conversion:
  *   - Legacy format → Composable format (ES 7.8+)
  *   - Composable format → Legacy format (ES < 7.8)
  */
trait TemplateApi extends ElasticClientHelpers { _: VersionApi =>

  // ========================================================================
  // TEMPLATE API
  // ========================================================================

  /** Create or update an index template.
    *
    * Accepts both legacy and composable template formats. Automatically converts to the appropriate
    * format based on ES version.
    *
    * @param templateName
    *   the name of the template
    * @param templateDefinition
    *   the JSON definition (legacy or composable format)
    * @return
    *   ElasticResult with true if successful
    */
  def createTemplate(
    templateName: String,
    templateDefinition: String
  ): ElasticResult[Boolean] = {
    validateTemplateName(templateName) match {
      case Some(error) => ElasticFailure(error)
      case None        =>
        // Get Elasticsearch version
        val elasticVersion = {
          this.version match {
            case ElasticSuccess(v) => v
            case ElasticFailure(error) =>
              logger.error(s"❌ Failed to retrieve Elasticsearch version: ${error.message}")
              return ElasticFailure(error)
          }
        }

        // Normalize template format based on ES version
        val normalizedTemplate = TemplateConverter.normalizeTemplate(
          templateDefinition,
          elasticVersion
        ) match {
          case Success(json) => json
          case Failure(ex) =>
            logger.error(s"❌ Failed to normalize template format: ${ex.getMessage}", ex)
            return ElasticFailure(
              ElasticError(
                message = s"Invalid template format: ${ex.getMessage}",
                statusCode = Some(400),
                operation = Some("createTemplate"),
                cause = Some(ex)
              )
            )
        }

        // Determine which API to use
        val isLegacyFormat = TemplateConverter.isLegacyFormat(templateDefinition)
        val supportsComposable = ElasticsearchVersion.supportsComposableTemplates(elasticVersion)

        if (supportsComposable) {
          logger.info(
            s"✅ Using composable template API (ES $elasticVersion)" +
            (if (isLegacyFormat) " [converted from legacy format]" else "")
          )

          validateJsonComposableTemplate(normalizedTemplate) match {
            case Some(error) => ElasticFailure(error)
            case None        => executeCreateComposableTemplate(templateName, normalizedTemplate)
          }
        } else {
          logger.info(s"⚠️ Using legacy template API (ES $elasticVersion < 7.8)")

          validateJsonLegacyTemplate(normalizedTemplate) match {
            case Some(error) => ElasticFailure(error)
            case None        => executeCreateLegacyTemplate(templateName, normalizedTemplate)
          }
        }
    }
  }

  /** Delete an index template. Automatically uses composable (ES 7.8+) or legacy templates based on
    * ES version.
    *
    * @param templateName
    *   the name of the template to delete
    * @param ifExists
    *   if true, do not fail if template doesn't exist
    * @return
    *   ElasticResult with true if successful
    */
  def deleteTemplate(
    templateName: String,
    ifExists: Boolean = false
  ): ElasticResult[Boolean] = {
    validateTemplateName(templateName) match {
      case Some(error) => ElasticFailure(error)
      case None =>
        val elasticVersion = {
          this.version match {
            case ElasticSuccess(v) => v
            case ElasticFailure(error) =>
              logger.error(s"❌ Failed to retrieve Elasticsearch version: ${error.message}")
              return ElasticFailure(error)
          }
        }

        if (ElasticsearchVersion.supportsComposableTemplates(elasticVersion)) {
          logger.debug(s"Using composable template API for deletion (ES $elasticVersion)")
          executeDeleteComposableTemplate(templateName, ifExists)
        } else {
          logger.debug(s"Using legacy template API for deletion (ES $elasticVersion < 7.8)")
          executeDeleteLegacyTemplate(templateName, ifExists)
        }
    }
  }

  /** Get an index template definition.
    *
    * Returns the template in the format used by the current ES version:
    *   - Composable format for ES 7.8+
    *   - Legacy format for ES < 7.8
    *
    * @param templateName
    *   the name of the template
    * @return
    *   ElasticResult with Some(json) if found, None if not found
    */
  def getTemplate(templateName: String): ElasticResult[Option[String]] = {
    validateTemplateName(templateName) match {
      case Some(error) => ElasticFailure(error)
      case None =>
        val elasticVersion = {
          this.version match {
            case ElasticSuccess(v) => v
            case ElasticFailure(error) =>
              logger.error(s"❌ Failed to retrieve Elasticsearch version: ${error.message}")
              return ElasticFailure(error)
          }
        }

        (if (ElasticsearchVersion.supportsComposableTemplates(elasticVersion)) {
           executeGetComposableTemplate(templateName)
         } else {
           executeGetLegacyTemplate(templateName)
         }) match {
          case success @ ElasticSuccess(_) => success
          case failure @ ElasticFailure(error) =>
            error.statusCode match {
              case Some(404) =>
                logger.warn(s"⚠️ Template $templateName not found")
                return ElasticSuccess(None)
              case _ =>
            }
            failure
        }
    }
  }

  /** List all index templates.
    *
    * Returns templates in the format used by the current ES version:
    *   - Composable format for ES 7.8+
    *   - Legacy format for ES < 7.8
    *
    * @return
    *   ElasticResult with Map of template name -> JSON definition
    */
  def listTemplates(): ElasticResult[Map[String, String]] = {
    val elasticVersion = {
      this.version match {
        case ElasticSuccess(v) => v
        case ElasticFailure(error) =>
          logger.error(s"❌ Failed to retrieve Elasticsearch version: ${error.message}")
          return ElasticFailure(error)
      }
    }

    if (ElasticsearchVersion.supportsComposableTemplates(elasticVersion)) {
      executeListComposableTemplates()
    } else {
      executeListLegacyTemplates()
    }
  }

  /** Check if an index template exists. Automatically uses composable (ES 7.8+) or legacy templates
    * based on ES version.
    *
    * @param templateName
    *   the name of the template
    * @return
    *   ElasticResult with true if exists, false otherwise
    */
  def templateExists(templateName: String): ElasticResult[Boolean] = {
    validateTemplateName(templateName) match {
      case Some(error) => ElasticFailure(error)
      case None =>
        val elasticVersion = {
          this.version match {
            case ElasticSuccess(v) => v
            case ElasticFailure(error) =>
              logger.error(s"❌ Failed to retrieve Elasticsearch version: ${error.message}")
              return ElasticFailure(error)
          }
        }

        if (ElasticsearchVersion.supportsComposableTemplates(elasticVersion)) {
          executeComposableTemplateExists(templateName)
        } else {
          executeLegacyTemplateExists(templateName)
        }
    }
  }

  // ==================== ABSTRACT METHODS - COMPOSABLE (ES 7.8+) ====================

  private[client] def executeCreateComposableTemplate(
    templateName: String,
    templateDefinition: String
  ): ElasticResult[Boolean]

  private[client] def executeDeleteComposableTemplate(
    templateName: String,
    ifExists: Boolean
  ): ElasticResult[Boolean]

  private[client] def executeGetComposableTemplate(
    templateName: String
  ): ElasticResult[Option[String]]

  private[client] def executeListComposableTemplates(): ElasticResult[Map[String, String]]

  private[client] def executeComposableTemplateExists(templateName: String): ElasticResult[Boolean]

  // ==================== ABSTRACT METHODS - LEGACY (ES < 7.8) ====================

  private[client] def executeCreateLegacyTemplate(
    templateName: String,
    templateDefinition: String
  ): ElasticResult[Boolean]

  private[client] def executeDeleteLegacyTemplate(
    templateName: String,
    ifExists: Boolean
  ): ElasticResult[Boolean]

  private[client] def executeGetLegacyTemplate(templateName: String): ElasticResult[Option[String]]

  private[client] def executeListLegacyTemplates(): ElasticResult[Map[String, String]]

  private[client] def executeLegacyTemplateExists(templateName: String): ElasticResult[Boolean]

}
