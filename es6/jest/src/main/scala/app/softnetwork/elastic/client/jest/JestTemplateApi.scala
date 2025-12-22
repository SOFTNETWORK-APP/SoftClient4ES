package app.softnetwork.elastic.client.jest

import app.softnetwork.elastic.client.jest.actions.Template
import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess
}
import app.softnetwork.elastic.client.{SerializationApi, TemplateApi}
import app.softnetwork.elastic.sql.serialization._
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.searchbox.client.JestResult

import scala.jdk.CollectionConverters._

trait JestTemplateApi extends TemplateApi with JestClientHelpers with JestVersionApi {
  _: SerializationApi with JestClientCompanion =>

  // ==================== COMPOSABLE TEMPLATES (ES 7.8+) ====================

  override private[client] def executeCreateComposableTemplate(
    templateName: String,
    templateDefinition: String
  ): ElasticResult[Boolean] = ElasticSuccess(false)

  override private[client] def executeDeleteComposableTemplate(
    templateName: String,
    ifExists: Boolean
  ): ElasticResult[Boolean] = ElasticSuccess(false)

  override private[client] def executeGetComposableTemplate(
    templateName: String
  ): ElasticResult[Option[String]] = ElasticSuccess(None)

  override private[client] def executeListComposableTemplates()
    : ElasticResult[Map[String, String]] = ElasticSuccess(Map.empty[String, String])

  override private[client] def executeComposableTemplateExists(
    templateName: String
  ): ElasticResult[Boolean] = ElasticSuccess(false)

  // ==================== LEGACY TEMPLATES ====================

  override private[client] def executeCreateLegacyTemplate(
    templateName: String,
    templateDefinition: String
  ): ElasticResult[Boolean] = {
    apply().execute(Template.Create(templateName, templateDefinition)) match {
      case jestResult: JestResult if jestResult.isSucceeded =>
        ElasticSuccess(true)
      case jestResult: JestResult =>
        val errorMessage = jestResult.getErrorMessage
        ElasticFailure(
          ElasticError(
            s"Failed to create template '$templateName': $errorMessage"
          )
        )
    }
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
    apply().execute(Template.Delete(templateName)) match {
      case jestResult: JestResult if jestResult.isSucceeded =>
        ElasticSuccess(true)
      case jestResult: JestResult =>
        val errorMessage = jestResult.getErrorMessage
        ElasticFailure(
          ElasticError(
            s"Failed to delete template '$templateName': $errorMessage"
          )
        )
    }
  }

  override private[client] def executeGetLegacyTemplate(
    templateName: String
  ): ElasticResult[Option[String]] = {
    apply().execute(Template.Get(templateName)) match {
      case jestResult: JestResult if jestResult.isSucceeded =>
        val jsonString = jestResult.getJsonString
        if (jsonString != null && jsonString.nonEmpty) {
          val node: JsonNode = jsonString
          node match {
            case objectNode: ObjectNode if objectNode.has(templateName) =>
              val templateNode = objectNode.get(templateName)
              ElasticSuccess(Some(templateNode))
            case _ =>
              ElasticSuccess(None)
          }
        } else {
          ElasticSuccess(None)
        }
      case jestResult: JestResult =>
        val errorMessage = jestResult.getErrorMessage
        ElasticFailure(
          ElasticError(
            s"Failed to get template '$templateName': $errorMessage"
          )
        )
    }
  }

  override private[client] def executeListLegacyTemplates(): ElasticResult[Map[String, String]] = {
    apply().execute(Template.GetAll()) match {
      case jestResult: JestResult if jestResult.isSucceeded =>
        val jsonString = jestResult.getJsonString
        if (jsonString != null && jsonString.nonEmpty) {
          val node: JsonNode = jsonString
          node match {
            case objectNode: ObjectNode =>
              val templates = objectNode
                .fields()
                .asScala
                .map { entry =>
                  entry.getKey -> entry.getValue.toString
                }
                .toMap
              ElasticSuccess(templates)
            case _ =>
              ElasticSuccess(Map.empty[String, String])
          }
        } else {
          ElasticSuccess(Map.empty[String, String])
        }
      case jestResult: JestResult =>
        val errorMessage = jestResult.getErrorMessage
        ElasticFailure(
          ElasticError(
            s"Failed to list templates: $errorMessage"
          )
        )
    }
  }

  override private[client] def executeLegacyTemplateExists(
    templateName: String
  ): ElasticResult[Boolean] = {
    apply().execute(Template.Exists(templateName)) match {
      case jestResult: JestResult =>
        val statusCode = jestResult.getResponseCode
        ElasticSuccess(statusCode == 200)
      case _ =>
        ElasticSuccess(false)
    }
  }

}
