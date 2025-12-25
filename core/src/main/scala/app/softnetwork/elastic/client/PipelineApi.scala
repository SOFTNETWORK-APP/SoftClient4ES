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
import app.softnetwork.elastic.sql.schema.{GenericProcessor, IngestPipeline}

trait PipelineApi extends ElasticClientHelpers { _: VersionApi =>

  // ========================================================================
  // PIPELINE API
  // ========================================================================

  /** Execute a pipeline DDL statement
    *
    * @param sql
    *   the pipeline DDL statement
    * @return
    *   ElasticResult[Boolean] indicating success or failure
    */
  def pipeline(sql: String): ElasticResult[Boolean] = {
    ElasticResult.attempt(Parser(sql)) match {
      case ElasticSuccess(parsedStatement) =>
        parsedStatement match {

          case Right(statement) =>
            statement match {
              case ddl: CreatePipeline =>
                val elasticVersion = {
                  this.version match {
                    case ElasticSuccess(v) => v
                    case ElasticFailure(error) =>
                      logger.error(s"❌ Failed to retrieve Elasticsearch version: ${error.message}")
                      return ElasticResult.failure(error)
                  }
                }
                if (ElasticsearchVersion.isEs6(elasticVersion)) {
                  val pipeline = ddl.ddlPipeline.copy(
                    processors = ddl.ddlPipeline.processors.map { processor =>
                      GenericProcessor(
                        processorType = processor.processorType,
                        properties = processor.properties.filterNot(_._1 == "description")
                      )
                    }
                  )
                  createPipeline(ddl.name, pipeline.json)
                } else
                  createPipeline(ddl.name, ddl.ddlPipeline.json)
              case ddl: DropPipeline =>
                deletePipeline(ddl.name, ifExists = ddl.ifExists)
              case ddl: AlterPipeline =>
                getPipeline(ddl.name) match {
                  case ElasticSuccess(Some(existing)) =>
                    val existingPipeline = IngestPipeline(name = ddl.name, json = existing)
                    val updatingPipeline = existingPipeline.merge(ddl.statements)
                    val elasticVersion = {
                      this.version match {
                        case ElasticSuccess(v) => v
                        case ElasticFailure(error) =>
                          logger.error(
                            s"❌ Failed to retrieve Elasticsearch version: ${error.message}"
                          )
                          return ElasticResult.failure(error)
                      }
                    }
                    if (ElasticsearchVersion.isEs6(elasticVersion)) {
                      val pipeline = updatingPipeline.copy(
                        processors = updatingPipeline.processors.map { processor =>
                          GenericProcessor(
                            processorType = processor.processorType,
                            properties = processor.properties.filterNot(_._1 == "description")
                          )
                        }
                      )
                      updatePipeline(ddl.name, pipeline.json)
                    } else
                      updatePipeline(ddl.name, updatingPipeline.json)
                  case ElasticSuccess(None) if !ddl.ifExists =>
                    val error =
                      ElasticError(
                        message = s"Pipeline with name '${ddl.name}' not found",
                        statusCode = Some(404),
                        operation = Some("pipeline")
                      )
                    logger.error(s"❌ ${error.message}")
                    ElasticResult.failure(error)
                  case ElasticSuccess(None) if ddl.ifExists =>
                    logger.info(
                      s"ℹ️ Pipeline with name '${ddl.name}' not found, skipping update as 'ifExists' is true"
                    )
                    ElasticSuccess(false)
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
        ElasticResult.failure(elasticError.copy(operation = Some("pipeline")))
    }
  }

  /** Create a new ingest pipeline
    *
    * @param pipelineName
    *   the name of the pipeline
    * @param pipelineDefinition
    *   the pipeline definition in JSON format
    * @return
    *   ElasticResult[Boolean] indicating success or failure
    */
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

  /** Update an existing ingest pipeline
    *
    * @param pipelineName
    *   the name of the pipeline
    * @param pipelineDefinition
    *   the new pipeline definition in JSON format
    * @return
    *   ElasticResult[Boolean] indicating success or failure
    */
  def updatePipeline(pipelineName: String, pipelineDefinition: String): ElasticResult[Boolean] = {
    // In Elasticsearch, creating a pipeline with an existing name updates it
    createPipeline(pipelineName, pipelineDefinition) match {
      case success @ ElasticSuccess(_) => success
      case failure @ ElasticFailure(_) => failure
    }
  }

  /** Delete an existing ingest pipeline
    *
    * @param pipelineName
    *   the name of the pipeline
    * @param ifExists
    *   flag indicating whether to ignore if the pipeline does not exist
    * @return
    *   ElasticResult[Boolean] indicating success or failure
    */
  def deletePipeline(pipelineName: String, ifExists: Boolean): ElasticResult[Boolean] = {
    if (ifExists) {
      getPipeline(pipelineName) match {
        case ElasticSuccess(Some(_)) => // Pipeline exists, proceed to delete
        case ElasticSuccess(None) =>
          logger.info(
            s"ℹ️ Pipeline '$pipelineName' does not exist, skipping deletion as 'ifExists' is true"
          )
          return ElasticSuccess(false) // Indicate that nothing was deleted
        case failure @ ElasticFailure(error) =>
          logger.error(s"❌ Failed to check existence of pipeline '$pipelineName': ${error.message}")
          return failure
      }
    }
    executeDeletePipeline(pipelineName, ifExists = ifExists) match {
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

  /** Retrieve an existing ingest pipeline
    *
    * @param pipelineName
    *   the name of the pipeline
    * @return
    *   ElasticResult[Option[String]\] containing the pipeline definition in JSON format if found
    */
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

  private[client] def executeDeletePipeline(
    pipelineName: String,
    ifExists: Boolean
  ): ElasticResult[Boolean]

  private[client] def executeGetPipeline(pipelineName: String): ElasticResult[Option[String]]

}
