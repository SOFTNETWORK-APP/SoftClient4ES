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
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{AlterPipeline, CreatePipeline, DropPipeline}
import app.softnetwork.elastic.sql.schema.DdlPipeline

trait PipelineApi extends ElasticClientHelpers {

  def pipeline(sql: String): ElasticResult[Boolean] = {
    ElasticResult.attempt(Parser(sql)) match {
      case ElasticSuccess(parsedStatement) =>
        parsedStatement match {

          case Right(statement) =>
            statement match {
              case ddl: CreatePipeline =>
                createPipeline(ddl.name, ddl.ddlPipeline.json)
              case ddl: DropPipeline =>
                deletePipeline(ddl.name)
              case ddl: AlterPipeline =>
                getPipeline(ddl.name) match {
                  case ElasticSuccess(Some(existing)) =>
                    val existingPipeline = DdlPipeline(name = ddl.name, json = existing)
                    val updatingPipeline = existingPipeline.merge(ddl.statements)
                    updatePipeline(ddl.name, updatingPipeline.json)
                  case ElasticSuccess(None) =>
                    val error =
                      ElasticError(
                        message = s"Pipeline with name '${ddl.name}' not found",
                        statusCode = Some(404),
                        operation = Some("pipeline")
                      )
                    logger.error(s"❌ ${error.message}")
                    ElasticResult.failure(error)
                  case failure @ ElasticFailure(error) =>
                    logger.error(
                      s"❌ Failed to retrieve pipeline with name '${ddl.name}': ${error.message}"
                    )
                    failure
                }
              case _ =>
                val error =
                  ElasticError(
                    message = s"Unsupported pipeline DDL statement: $statement",
                    statusCode = Some(400),
                    operation = Some("pipeline")
                  )
                logger.error(s"❌ ${error.message}")
                ElasticResult.failure(error)
            }
          case Left(l) =>
            val error =
              ElasticError(
                message = s"Error parsing pipeline DDL statement: ${l.msg}",
                statusCode = Some(400),
                operation = Some("pipeline")
              )
            logger.error(s"❌ ${error.message}")
            ElasticResult.failure(error)
        }
      case ElasticFailure(elasticError) =>
        ElasticResult.failure(elasticError)
    }
  }

  def createPipeline(pipelineName: String, pipelineDefinition: String): ElasticResult[Boolean] = {
    validatePipelineName(pipelineName) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("createPipeline"),
            statusCode = Some(400),
            message = s"Invalid pipeline: ${error.message}"
          )
        )
      case None => // OK
    }

    validateJsonPipeline(pipelineDefinition) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("createPipeline"),
            statusCode = Some(400),
            message = s"Invalid pipeline: ${error.message}"
          )
        )
      case None => // OK
    }

    executeCreatePipeline(pipelineName, pipelineDefinition) match {
      case success @ ElasticSuccess(created) =>
        if (created) {
          logger.info(s"✅ Successfully created pipeline '$pipelineName'")
        } else {
          logger.warn(s"⚠️ Pipeline '$pipelineName' not created (it may already exist)")
        }
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to create pipeline '$pipelineName': ${error.message}")
        failure
    }
  }

  def updatePipeline(pipelineName: String, pipelineDefinition: String): ElasticResult[Boolean] = {
    // In Elasticsearch, creating a pipeline with an existing name updates it
    createPipeline(pipelineName, pipelineDefinition) match {
      case success @ ElasticSuccess(_) => success
      case failure @ ElasticFailure(_) => failure
    }
  }

  def deletePipeline(pipelineName: String): ElasticResult[Boolean] = {
    executeDeletePipeline(pipelineName) match {
      case success @ ElasticSuccess(deleted) =>
        if (deleted) {
          logger.info(s"✅ Successfully deleted pipeline '$pipelineName'")
        } else {
          logger.warn(s"⚠️ Pipeline '$pipelineName' not deleted (it may not exist)")
        }
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to delete pipeline '$pipelineName': ${error.message}")
        failure
    }
  }

  def getPipeline(pipelineName: String): ElasticResult[Option[String]] = {
    executeGetPipeline(pipelineName) match {
      case success @ ElasticSuccess(maybePipeline) =>
        maybePipeline match {
          case Some(_) =>
            logger.info(s"✅ Successfully retrieved pipeline '$pipelineName'")
          case None =>
            logger.warn(s"⚠️ Pipeline '$pipelineName' not found")
        }
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to retrieve pipeline '$pipelineName': ${error.message}")
        failure
    }
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeCreatePipeline(
    pipelineName: String,
    pipelineDefinition: String
  ): ElasticResult[Boolean]

  private[client] def executeDeletePipeline(pipelineName: String): ElasticResult[Boolean]

  private[client] def executeGetPipeline(pipelineName: String): ElasticResult[Option[String]]

}
