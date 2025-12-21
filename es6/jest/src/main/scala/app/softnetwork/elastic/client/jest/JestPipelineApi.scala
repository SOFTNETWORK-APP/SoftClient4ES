package app.softnetwork.elastic.client.jest

import app.softnetwork.elastic.client.jest.actions.Pipeline
import app.softnetwork.elastic.client.{result, PipelineApi, SerializationApi}
import app.softnetwork.elastic.sql.serialization._
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.searchbox.client.JestResult

trait JestPipelineApi extends PipelineApi with JestClientHelpers with JestVersionApi {
  _: SerializationApi with JestClientCompanion =>

  override private[client] def executeCreatePipeline(
    pipelineName: String,
    pipelineDefinition: String
  ): result.ElasticResult[Boolean] = {
    // There is no direct API to create a pipeline in Jest.
    apply().execute(Pipeline.Create(pipelineName, pipelineDefinition)) match {
      case jestResult: JestResult if jestResult.isSucceeded =>
        result.ElasticSuccess(true)
      case jestResult: JestResult =>
        val errorMessage = jestResult.getErrorMessage
        result.ElasticFailure(
          result.ElasticError(
            s"Failed to create pipeline '$pipelineName': $errorMessage"
          )
        )
    }
  }

  override private[client] def executeDeletePipeline(
    pipelineName: String,
    ifExists: Boolean
  ): result.ElasticResult[Boolean] = {
    // There is no direct API to delete a pipeline in Jest.
    apply().execute(Pipeline.Delete(pipelineName)) match {
      case jestResult: JestResult if jestResult.isSucceeded =>
        result.ElasticSuccess(true)
      case jestResult: JestResult =>
        val errorMessage = jestResult.getErrorMessage
        result.ElasticFailure(
          result.ElasticError(
            s"Failed to delete pipeline '$pipelineName': $errorMessage"
          )
        )
    }
  }

  override private[client] def executeGetPipeline(
    pipelineName: String
  ): result.ElasticResult[Option[String]] = {
    // There is no direct API to get a pipeline in Jest.
    apply().execute(Pipeline.Get(pipelineName)) match {
      case jestResult: JestResult if jestResult.isSucceeded =>
        val jsonString = jestResult.getJsonString
        if (jsonString != null && jsonString.nonEmpty) {
          val node: JsonNode = jsonString
          node match {
            case objectNode: ObjectNode if objectNode.has(pipelineName) =>
              val pipelineNode = objectNode.get(pipelineName)
              result.ElasticSuccess(Some(pipelineNode))
            case _ =>
              result.ElasticSuccess(None)
          }
        } else {
          result.ElasticSuccess(None)
        }
      case jestResult: JestResult =>
        val errorMessage = jestResult.getErrorMessage
        result.ElasticFailure(
          result.ElasticError(
            s"Failed to get pipeline '$pipelineName': $errorMessage"
          )
        )
    }
  }

}
