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

import app.softnetwork.elastic.client.jest.actions.Pipeline
import app.softnetwork.elastic.client.{result, PipelineApi}
import app.softnetwork.elastic.sql.serialization._
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.searchbox.client.JestResult

import scala.jdk.CollectionConverters._

/** Every operation routes through [[JestClientHelpers.executeJestAction]]. They used to call
  * `apply().execute(...)` directly, so a network fault threw instead of returning an
  * `ElasticFailure` (SoftClient4ES#215, the same defect as #204 in the licence API). The wrapper
  * also carries the HTTP status, names the failing operation, and guards the response-parsing
  * transformers — several of which navigate the JSON with `get(...)` and could NPE.
  *
  * The absent-pipeline contract is unchanged: `Pipeline.Get` marks 404 as *succeeded* with no body,
  * so a missing pipeline still reaches the transformer and yields `None` rather than a failure.
  */
trait JestPipelineApi extends PipelineApi with JestClientHelpers {
  _: JestVersionApi with JestClientCompanion =>

  override private[client] def executeCreatePipeline(
    pipelineName: String,
    pipelineDefinition: String
  ): result.ElasticResult[Boolean] =
    // There is no direct API to create a pipeline in Jest.
    executeJestBooleanAction[JestResult](
      operation = "createPipeline",
      retryable = false // Creation can not be retried
    ) {
      Pipeline.Create(pipelineName, pipelineDefinition)
    }

  override private[client] def executeDeletePipeline(
    pipelineName: String,
    ifExists: Boolean
  ): result.ElasticResult[Boolean] =
    // There is no direct API to delete a pipeline in Jest.
    executeJestBooleanAction[JestResult](
      operation = "deletePipeline",
      retryable = false // Deletion can not be retried
    ) {
      Pipeline.Delete(pipelineName)
    }

  override private[client] def executeGetPipeline(
    pipelineName: String
  ): result.ElasticResult[Option[String]] =
    // There is no direct API to get a pipeline in Jest.
    executeJestAction[JestResult, Option[String]](
      operation = "getPipeline",
      retryable = true
    ) {
      Pipeline.Get(pipelineName)
    } { jestResult =>
      val jsonString = jestResult.getJsonString
      if (jsonString != null && jsonString.nonEmpty) {
        val node: JsonNode = jsonString
        node match {
          case objectNode: ObjectNode if objectNode.has(pipelineName) =>
            Some(objectNode.get(pipelineName))
          case _ =>
            None
        }
      } else {
        None
      }
    }

  override private[client] def executeListPipelines(): result.ElasticResult[Map[String, String]] =
    // There is no direct API to list pipelines in Jest.
    executeJestAction[JestResult, Map[String, String]](
      operation = "listPipelines",
      retryable = true
    ) {
      Pipeline.List()
    } { jestResult =>
      val jsonString = jestResult.getJsonString
      if (jsonString != null && jsonString.nonEmpty) {
        val node: JsonNode = jsonString
        node match {
          case objectNode: ObjectNode =>
            objectNode
              .fieldNames()
              .asScala
              .map { name =>
                name -> objectNode.get(name).toString
              }
              .toMap
          case _ =>
            Map.empty[String, String]
        }
      } else {
        Map.empty[String, String]
      }
    }
}
