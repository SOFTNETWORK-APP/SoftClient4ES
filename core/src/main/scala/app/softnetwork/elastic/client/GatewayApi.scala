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

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.result.{
  DdlResult,
  DmlResult,
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess,
  QueryPipeline,
  QueryResult,
  QueryRows,
  QueryStream,
  QueryStructured,
  QueryTable
}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{
  AlterTable,
  CopyInto,
  CreatePipeline,
  CreateTable,
  DdlStatement,
  Delete,
  DescribePipeline,
  DescribeTable,
  DmlStatement,
  DqlStatement,
  DropTable,
  Insert,
  MultiSearch,
  PipelineStatement,
  SelectStatement,
  ShowPipeline,
  ShowTable,
  SingleSearch,
  Statement,
  TableStatement,
  TruncateTable,
  Update
}
import app.softnetwork.elastic.sql.schema.{
  Impossible,
  IngestPipeline,
  IngestPipelineType,
  PipelineDiff,
  Safe,
  Table,
  TableDiff,
  UnsafeReindex
}
import app.softnetwork.elastic.sql.serialization._
import org.slf4j.Logger

import scala.concurrent.{ExecutionContext, Future}

trait Executor[T <: Statement] {
  def execute(statement: T)(implicit
    system: ActorSystem
  ): Future[ElasticResult[QueryResult]]
}

class DqlExecutor(api: ScrollApi with SearchApi, logger: Logger) extends Executor[DqlStatement] {

  override def execute(
    statement: DqlStatement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {

    implicit val ec: ExecutionContext = system.dispatcher

    statement match {

      // ============================
      // SELECT ... (AST SQL)
      // ============================
      case select: SelectStatement =>
        select.statement match {

          case Some(single: SingleSearch) =>
            execute(single.copy(score = select.score))

          case Some(multiple: MultiSearch) =>
            execute(multiple)

          case None =>
            val error = ElasticError(
              message = s"SELECT statement could not be translated into a search query: $statement",
              statusCode = Some(400),
              operation = Some("dql")
            )
            logger.error(s"❌ ${error.message}")
            Future.successful(ElasticFailure(error))
        }

      // ============================
      // SingleSearch → SCROLL
      // ============================
      case single: SingleSearch =>
        single.limit match {
          case Some(l) if l.offset.map(_.offset).getOrElse(0) > 0 =>
            logger.info(s"▶ Executing classic search on index ${single.from.tables.mkString(",")}")
            api.searchAsync(single) map {
              case ElasticSuccess(results) =>
                logger.info(s"✅ Search returned ${results.results.size} hits.")
                ElasticSuccess(QueryStructured(results))
              case ElasticFailure(err) =>
                ElasticFailure(err.copy(operation = Some("dql")))
            }
          case _ =>
            logger.info(s"▶ Executing scroll search on index ${single.from.tables.mkString(",")}")
            Future.successful(
              ElasticSuccess(QueryStream(api.scroll(single)))
            )
        }

      // ============================
      // MultiSearch → searchAsync
      // ============================
      case multiple: MultiSearch =>
        logger.info(s"▶ Executing multi-search on ${multiple.requests.size} indices")
        api.searchAsync(multiple).map {
          case ElasticSuccess(results) =>
            logger.info(s"✅ Multi-search returned ${results.results.size} hits.")
            ElasticSuccess(QueryStructured(results))

          case ElasticFailure(err) =>
            ElasticFailure(err.copy(operation = Some("dql")))
        }

      // ============================
      // Unsupported DQL
      // ============================
      case _ =>
        val error = ElasticError(
          message = s"Unsupported DQL statement: $statement",
          statusCode = Some(400),
          operation = Some("dql")
        )
        logger.error(s"❌ ${error.message}")
        Future.successful(ElasticFailure(error))
    }
  }
}

class DmlExecutor(api: IndicesApi, logger: Logger) extends Executor[DmlStatement] {
  override def execute(
    statement: DmlStatement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    statement match {
      case delete: Delete =>
        api.deleteByQuery(delete.table.name, delete.sql) match {
          case ElasticSuccess(count) =>
            logger.info(s"✅ Deleted $count documents from ${delete.table.name}.")
            Future.successful(ElasticResult.success(DmlResult(deleted = count)))
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("schema"))
              )
            )
        }
      case update: Update =>
        api.updateByQuery(update.table, update.sql) match {
          case ElasticSuccess(count) =>
            logger.info(s"✅ Updated $count documents in ${update.table}.")
            Future.successful(ElasticResult.success(DmlResult(updated = count)))
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("schema"))
              )
            )
        }
      case insert: Insert =>
        api.insertByQuery(insert.table, insert.sql).map {
          case success @ ElasticSuccess(res) =>
            logger.info(s"✅ Inserted ${res.inserted} documents into ${insert.table}.")
            success
          case ElasticFailure(elasticError) =>
            ElasticFailure(
              elasticError.copy(operation = Some("schema"))
            )
        }
      case copy: CopyInto =>
        api
          .copyInto(
            copy.source,
            copy.targetTable,
            doUpdate = copy.onConflict.exists(_.doUpdate),
            fileFormat = copy.fileFormat
          )
          .map {
            case success @ ElasticSuccess(res) =>
              logger.info(
                s"✅ Copied ${res.inserted} documents into ${copy.targetTable} from ${copy.source}."
              )
              success
            case ElasticFailure(elasticError) =>
              ElasticFailure(
                elasticError.copy(operation = Some("schema"))
              )
          }
      case _ =>
        // unsupported DML statement
        val error =
          ElasticError(
            message = s"Unsupported DML statement: $statement",
            statusCode = Some(400),
            operation = Some("schema")
          )
        logger.error(s"❌ ${error.message}")
        Future.successful(ElasticFailure(error))
    }
  }
}

trait DdlExecutor[T <: DdlStatement] extends Executor[T]

class PipelineExecutor(api: PipelineApi, logger: Logger) extends DdlExecutor[PipelineStatement] {
  override def execute(
    statement: PipelineStatement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    statement match {
      case show: ShowPipeline =>
        // handle SHOW PIPELINE statement
        api.loadPipeline(show.name) match {
          case ElasticSuccess(pipeline) =>
            logger.info(s"✅ Retrieved pipeline ${show.name}.")
            Future.successful(ElasticResult.success(QueryPipeline(pipeline)))
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("pipeline"))
              )
            )
        }
      case describe: DescribePipeline =>
        // handle DESCRIBE PIPELINE statement
        api.loadPipeline(describe.name) match {
          case ElasticSuccess(pipeline) =>
            logger.info(s"✅ Retrieved pipeline ${describe.name}.")
            Future.successful(
              ElasticResult.success(QueryRows(pipeline.processors.map(_.properties)))
            )
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("pipeline"))
              )
            )
        }
      case _ =>
        // handle PIPELINE statement
        api.pipeline(statement) match {
          case ElasticSuccess(result) =>
            logger.info(s"✅ Executed pipeline statement: $statement.")
            Future.successful(ElasticResult.success(DdlResult(result)))
          case ElasticFailure(elasticError) =>
            logger.error(
              s"❌ Error executing pipeline statement: $statement. ${elasticError.message}"
            )
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("pipeline"))
              )
            )
        }
    }
  }
}

class TableExecutor(
  api: IndicesApi with PipelineApi with TemplateApi with SettingsApi with MappingApi with AliasApi,
  logger: Logger
) extends DdlExecutor[TableStatement] {
  override def execute(
    statement: TableStatement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    // handle TABLE statement
    statement match {
      // handle SHOW TABLE statement
      case show: ShowTable =>
        api.loadSchema(show.table) match {
          case ElasticSuccess(schema) =>
            logger.info(s"✅ Retrieved schema for index ${show.table}.")
            Future.successful(ElasticResult.success(QueryTable(schema)))
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("schema"))
              )
            )
        }
      // handle DESCRIBE TABLE statement
      case describe: DescribeTable =>
        api.loadSchema(describe.table) match {
          case ElasticSuccess(schema) =>
            logger.info(s"✅ Retrieved schema for index ${describe.table}.")
            Future.successful(ElasticResult.success(QueryRows(schema.columns.flatMap(_.asMap))))
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("schema"))
              )
            )
        }
      // handle CREATE TABLE statement
      case create: CreateTable =>
        val ifNotExists = create.ifNotExists
        val orReplace = create.orReplace
        // will create template if partitioned
        val partitioned = create.partitioned
        val indexName = create.table
        val single: Option[SingleSearch] = create.ddl match {
          case Left(dql) =>
            dql match {
              case s: SingleSearch => Some(s)
              case _ =>
                val error =
                  ElasticError(
                    message = s"Only single search DQL supported in CREATE TABLE.",
                    statusCode = Some(400),
                    operation = Some("schema")
                  )
                logger.error(s"❌ ${error.message}")
                return Future.successful(ElasticFailure(error))
            }
          case Right(_) => None
        }

        // check if index exists
        api.indexExists(indexName, pattern = false) match {
          // 1) Index exists + IF NOT EXISTS → skip creation, DdlResult(false)
          case ElasticSuccess(true) if ifNotExists =>
            // skip creation
            logger.info(s"⚠️ Index $indexName already exists, skipping creation.")
            Future.successful(ElasticSuccess(DdlResult(false)))

          // 2) Index exists + no OR REPLACE specified → error
          case ElasticSuccess(true) if !orReplace =>
            val error =
              ElasticError(
                message = s"Index $indexName already exists.",
                statusCode = Some(400),
                operation = Some("schema")
              )
            logger.error(s"❌ ${error.message}")
            Future.successful(ElasticFailure(error))

          // 3) Index exists + OR REPLACE → remplacement
          case ElasticSuccess(true) =>
            // proceed with replacement
            logger.info(s"♻️ Replacing index $indexName.")
            replaceExistingIndex(indexName, create, partitioned, single)

          // 4) Index not exists → creation
          case ElasticSuccess(false) =>
            // proceed with creation
            logger.info(s"✅ Creating index $indexName.")
            createNonExistentIndex(indexName, create, partitioned, single)

          // 5) Error on indexExists
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(elasticError.copy(operation = Some("schema")))
            )
        }

      // handle ALTER TABLE statement
      case alter: AlterTable =>
        val ifExists = alter.ifExists
        val indexName = alter.table

        // check if index exists
        api.indexExists(indexName, pattern = false) match {
          // 1) Index does not exist + IF EXISTS → skip alteration, DdlResult(false)
          case ElasticSuccess(false) if ifExists =>
            // skip alteration
            logger.info(s"⚠️ Index $indexName does not exist, skipping alteration.")
            Future.successful(ElasticSuccess(DdlResult(false)))

          // 2) Index does not exists + no IF EXISTS → error
          case ElasticSuccess(false) =>
            val error =
              ElasticError(
                message = s"Index $indexName does not exist.",
                statusCode = Some(404),
                operation = Some("schema")
              )
            logger.error(s"❌ ${error.message}")
            Future.successful(ElasticFailure(error))

          // 3) Index exists → alteration
          case ElasticSuccess(true) =>
            // proceed with alteration
            logger.info(s"♻️ Alter index $indexName.")
            alterExistingIndex(indexName, alter)

          // 5) Error on indexExists
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(elasticError.copy(operation = Some("schema")))
            )
        }

      // handle TRUNCATE TABLE statement
      case truncate: TruncateTable =>
        // handle TRUNCATE TABLE statement
        val indexName = truncate.table
        api.truncateIndex(indexName) match {
          case ElasticSuccess(count) =>
            // index truncated successfully
            logger.info(s"✅ Index $indexName truncated successfully ($count documents deleted).")
            Future.successful(ElasticResult.success(DdlResult(true)))
          case ElasticFailure(elasticError) =>
            // error truncating index
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("truncate"))
              )
            )
        }

      // handle DROP TABLE statement
      case drop: DropTable =>
        // handle DROP TABLE statement
        val indexName = drop.table
        // check if index exists - pattern indices not supported for drop
        api.indexExists(indexName, pattern = false) match {
          case ElasticSuccess(false) if drop.ifExists =>
            // index does not exist and IF EXISTS specified, skip deletion
            logger.info(s"⚠️ Index $indexName does not exist, skipping deletion.")
            Future.successful(ElasticResult.success(DdlResult(false)))
          case ElasticSuccess(false) if !drop.ifExists =>
            // index does not exist and no IF EXISTS specified, return error
            val error =
              ElasticError(
                message = s"Index $indexName does not exist.",
                statusCode = Some(404),
                operation = Some("drop")
              )
            logger.error(s"❌ ${error.message}")
            Future.successful(ElasticFailure(error))
          case ElasticSuccess(true) =>
            // proceed with deletion
            api.deleteIndex(indexName) match {
              case ElasticSuccess(true) =>
                // index deleted successfully
                logger.info(s"✅ Index $indexName deleted successfully.")
                Future.successful(ElasticResult.success(DdlResult(true)))
              case ElasticSuccess(false) =>
                // index deletion failed
                logger.warn(s"⚠️ Index $indexName could not be deleted.")
                Future.successful(ElasticResult.success(DdlResult(false)))
              case ElasticFailure(elasticError) =>
                // error deleting index
                Future.successful(
                  ElasticFailure(
                    elasticError.copy(operation = Some("drop"))
                  )
                )
            }
          case ElasticFailure(elasticError) =>
            // error checking index existence
            Future.successful(ElasticFailure(elasticError.copy(operation = Some("drop"))))
        }

      // handle unsupported table ddl statement
      case _ =>
        // unsupported table ddl statement
        val error =
          ElasticError(
            message = s"Unsupported table DDL statement: $statement",
            statusCode = Some(400),
            operation = Some("table")
          )
        logger.error(s"❌ ${error.message}")
        Future.successful(ElasticFailure(error))
    }
  }

  /* Alter existing index
   */
  private def alterExistingIndex(
    indexName: String,
    alter: AlterTable
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    // index exists and REPLACE specified, proceed with replacement
    logger.info(s"♻️ Altering index $indexName.")
    // load existing index schema
    api.loadSchema(indexName) match {
      case ElasticSuccess(schema) =>
        logger.info(
          s"🔄 Merging existing index $indexName DDL with new DDL."
        )
        val updatedTable: Table = schema.merge(alter.statements)

        // load default pipeline diff if needed
        val defaultPipelineDiff: Option[List[PipelineDiff]] =
          loadTablePipelineDiff(
            updatedTable,
            IngestPipelineType.Default,
            updatedTable.defaultPipelineName,
            schema.defaultPipelineName
          ) match {
            case ElasticSuccess(result) =>
              result
            case ElasticFailure(elasticError) =>
              return Future.successful(ElasticFailure(elasticError))
          }

        // load final pipeline diff if needed
        val finalPipelineDiff: Option[List[PipelineDiff]] =
          loadTablePipelineDiff(
            updatedTable,
            IngestPipelineType.Final,
            updatedTable.finalPipelineName,
            schema.finalPipelineName
          ) match {
            case ElasticSuccess(result) =>
              result
            case ElasticFailure(elasticError) =>
              return Future.successful(ElasticFailure(elasticError))
          }

        // compute diff
        var diff = schema.diff(updatedTable)

        // handle pipeline diffs
        defaultPipelineDiff match {
          case Some(pipelineDiffs) =>
            diff = diff.copy(pipeline =
              diff.pipeline.filterNot(_.pipelineType == IngestPipelineType.Default) ++ pipelineDiffs
            )
            if (pipelineDiffs.nonEmpty) {
              logger.warn(
                s"🔄 Default ingesting pipeline for index $indexName has changes: $pipelineDiffs"
              )
              logger.warn(
                s"""⚠️ Default Pipeline may be used by multiple indexes.
                  | Modifying it may impact other indexes.
                  | Consider creating a dedicated pipeline if isolation is required.""".stripMargin
              )
            }
          case _ =>
        }
        finalPipelineDiff match {
          case Some(pipelineDiffs) =>
            diff = diff.copy(pipeline =
              diff.pipeline.filterNot(_.pipelineType == IngestPipelineType.Final) ++ pipelineDiffs
            )
            if (pipelineDiffs.nonEmpty) {
              logger.warn(
                s"🔄 Final ingesting pipeline for index $indexName has changes: $pipelineDiffs"
              )
              logger.warn(
                s"""⚠️ Final Pipeline may be used by multiple indexes.
                   | Modifying it may impact other indexes.
                   | Consider creating a dedicated pipeline if isolation is required.""".stripMargin
              )
            }
          case _ =>
        }

        if (diff.isEmpty) {
          logger.info(
            s"⚠️ No changes detected for index $indexName, skipping update."
          )
          Future.successful(ElasticResult.success(DdlResult(false)))
        } else {
          logger.info(s"🔄 Updating index $indexName with changes: $diff")
          diff.safety match {

            // ------------------------------------------------------------
            // SAFE → mise à jour en place
            // ------------------------------------------------------------
            case Safe =>
              logger.info(s"🔧 Applying SAFE ALTER TABLE changes on $indexName.")
              applyUpdatesInPlace(indexName, updatedTable, diff) match {
                case Right(result) => Future.successful(ElasticSuccess(DdlResult(result)))
                case Left(error) =>
                  Future.successful(ElasticFailure(error.copy(operation = Some("schema"))))
              }

            // ------------------------------------------------------------
            // UNSAFE → reindex
            // ------------------------------------------------------------
            case UnsafeReindex =>
              logger.warn(s"⚠️ ALTER TABLE requires REINDEX for $indexName.")
              migrateToNewSchema(
                indexName,
                schema,
                updatedTable.copy(
                  settings = schema.settings // remove settings that cannot be copied
                    - "uuid"
                    - "creation_date"
                    - "provided_name"
                    - "version"
                    - "default_pipeline"
                    - "final_pipeline"
                ),
                diff
              ) match {
                case Right(result) => Future.successful(ElasticSuccess(DdlResult(result)))
                case Left(error) =>
                  Future.successful(ElasticFailure(error.copy(operation = Some("schema"))))
              }

            // ------------------------------------------------------------
            // IMPOSSIBLE
            // ------------------------------------------------------------
            case Impossible =>
              val error = ElasticError(
                message = s"ALTER TABLE cannot be applied to index $indexName: $diff",
                statusCode = Some(400),
                operation = Some("schema")
              )
              logger.error(s"❌ ${error.message}")
              Future.successful(ElasticFailure(error))
          }
        }

      case ElasticFailure(elasticError) =>
        // error retrieving existing index info
        Future.successful(
          ElasticFailure(elasticError.copy(operation = Some("schema")))
        )
    }
  }

  private def replaceExistingIndex(
    indexName: String,
    create: CreateTable,
    partitioned: Boolean,
    single: Option[SingleSearch]
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {

    logger.info(s"♻️ Replacing existing index $indexName.")

    // 1. Delete existing index
    api.deleteIndex(indexName) match {
      case ElasticFailure(err) =>
        logger.error(s"❌ Failed to delete index $indexName: ${err.message}")
        return Future.successful(ElasticFailure(err.copy(operation = Some("schema"))))

      case ElasticSuccess(false) =>
        logger.warn(s"⚠️ Index $indexName could not be deleted.")
        return Future.successful(ElasticSuccess(DdlResult(false)))

      case ElasticSuccess(true) =>
        logger.info(s"🗑️ Index $indexName deleted successfully.")
    }

    // 2. Recreate index using the same logic as createNonExistentIndex
    createNonExistentIndex(indexName, create, partitioned, single)
  }

  /* Create index / template for non-existent index
   */
  private def createNonExistentIndex(
    indexName: String,
    create: CreateTable,
    partitioned: Boolean,
    single: Option[SingleSearch]
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    // index does not exist, proceed with creation
    logger.info(s"✅ Creating index $indexName.")
    var table: Table =
      single match {
        case Some(single) =>
          single.from.tables.map(_.name).toSet.headOption match {
            case Some(from) =>
              api.loadSchema(from) match {
                case ElasticSuccess(fromSchema) =>
                  // we update the schema based on the DQL select clause
                  fromSchema
                    .mergeWithSearch(single)
                    .copy(
                      name = indexName, // set index name
                      settings = fromSchema.settings // remove settings that cannot be copied
                        - "uuid"
                        - "creation_date"
                        - "provided_name"
                        - "version"
                        - "default_pipeline"
                        - "final_pipeline"
                    )
                case ElasticFailure(elasticError) =>
                  val error = ElasticError(
                    message =
                      s"Error retrieving source schema $from for DQL in CREATE TABLE: ${elasticError.message}",
                    statusCode = elasticError.statusCode,
                    operation = Some("schema")
                  )
                  logger.error(s"❌ ${error.message}")
                  return Future.successful(ElasticFailure(error))
              }
            case _ =>
              val error =
                ElasticError(
                  message = s"Source index not specified for DQL in CREATE TABLE.",
                  statusCode = Some(400),
                  operation = Some("schema")
                )
              logger.error(s"❌ ${error.message}")
              return Future.successful(ElasticFailure(error))
          }
        case _ =>
          create.schema
      }

    // create index pipeline(s) if needed
    table.defaultPipeline match {
      case pipeline if pipeline.processors.nonEmpty =>
        createIndexPipeline(
          indexName,
          CreatePipeline(
            pipeline.name,
            pipeline.pipelineType,
            ifNotExists = true,
            orReplace = false,
            pipeline.processors
          )
        ) match {
          case ElasticSuccess(_) =>
            table = table.setDefaultPipelineName(pipelineName = pipeline.name)
          case ElasticFailure(elasticError) =>
            return Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("schema"))
              )
            )
        }
      case _ =>
    }

    table.finalPipeline match {
      case pipeline if pipeline.processors.nonEmpty =>
        createIndexPipeline(
          indexName,
          CreatePipeline(
            pipeline.name,
            pipeline.pipelineType,
            ifNotExists = true,
            orReplace = false,
            pipeline.processors
          )
        ) match {
          case ElasticSuccess(_) =>
            table = table.setFinalPipelineName(pipelineName = pipeline.name)
          case ElasticFailure(elasticError) =>
            return Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("schema"))
              )
            )
        }
      case _ =>
    }

    // create index / template
    createIndexOrTemplate(table, partitioned = partitioned) match {
      case ElasticSuccess(_) =>
        // index / template created successfully
        single match {
          case Some(single) =>
            // populate index based on DQL
            logger.info(
              s"🚚 Populating index $indexName based on DQL."
            )
            val query =
              s"""INSERT INTO ${table.name} AS ${single.sql} ON CONFLICT DO NOTHING"""
            val result = api.insertByQuery(
              index = indexName,
              query = query
            )
            result.map {
              case ElasticSuccess(res) =>
                logger.info(
                  s"✅ Index $indexName populated successfully (${res.inserted} documents indexed)."
                )
                ElasticResult.success(res)
              case ElasticFailure(elasticError) =>
                ElasticFailure(
                  elasticError.copy(operation = Some("schema"))
                )
            }
          case _ =>
            // no population needed
            Future.successful(ElasticResult.success(DdlResult(true)))
        }
      case ElasticFailure(elasticError) =>
        // error creating index / template
        Future.successful(
          ElasticFailure(
            elasticError.copy(operation = Some("schema"))
          )
        )
    }
  }

  /* Create ingesting pipeline for index
   */
  private def createIndexPipeline(
    indexName: String,
    statement: CreatePipeline
  ): ElasticResult[Boolean] = {
    logger.info(
      s"🔧 Creating ${statement.pipelineType.name} ingesting pipeline ${statement.name} for index $indexName."
    )
    api.pipeline(statement) match {
      case success @ ElasticSuccess(true) =>
        logger.info(s"✅ Pipeline ${statement.name} created successfully.")
        success
      case ElasticSuccess(_) =>
        // pipeline creation failed
        val error =
          ElasticError(
            message = s"Failed to create pipeline ${statement.name}.",
            statusCode = Some(500),
            operation = Some("schema")
          )
        logger.error(s"❌ ${error.message}")
        ElasticFailure(error)
      case failure @ ElasticFailure(_) =>
        failure
    }

  }

  /* Create index or template based on partitioning
   */
  private def createIndexOrTemplate(table: Table, partitioned: Boolean): ElasticResult[Boolean] = {
    val indexName = table.name
    if (partitioned) {
      // create index template
      api.createTemplate(indexName, table.indexTemplate) match {
        case success @ ElasticSuccess(true) =>
          logger.info(s"✅ Template $indexName created successfully.")
          success
        case ElasticSuccess(_) =>
          // template creation failed
          val error =
            ElasticError(
              message = s"Failed to create template $indexName.",
              statusCode = Some(500),
              operation = Some("schema")
            )
          logger.error(s"❌ ${error.message}")
          ElasticFailure(error)
        case failure @ ElasticFailure(error) =>
          logger.error(s"❌ ${error.message}")
          failure
      }
    } else {
      // create index
      api.createIndex(
        index = indexName,
        settings = table.indexSettings,
        mappings = Some(table.indexMappings),
        aliases = table.indexAliases
      ) match {
        case success @ ElasticSuccess(true) =>
          // index created successfully
          logger.info(s"✅ Index $indexName created successfully.")
          success
        case ElasticSuccess(_) =>
          // index creation failed
          val error =
            ElasticError(
              message = s"Failed to create index $indexName.",
              statusCode = Some(500),
              operation = Some("schema")
            )
          logger.error(s"❌ ${error.message}")
          ElasticFailure(error)
        case failure @ ElasticFailure(_) =>
          failure
      }
    }
  }

  private def loadTablePipelineDiff(
    table: Table,
    pipelineType: IngestPipelineType,
    updatedPipelineName: Option[String],
    existingPipelineName: Option[String]
  ): ElasticResult[Option[List[PipelineDiff]]] = {
    updatedPipelineName match {
      case Some(pipelineName) if pipelineName != existingPipelineName.getOrElse("") =>
        logger.info(
          s"🔄 ${pipelineType.name} ingesting pipeline for index ${table.name} has been updated to $pipelineName."
        )
        // load new pipeline
        api.getPipeline(pipelineName) match {
          case ElasticSuccess(maybePipeline) if maybePipeline.isDefined =>
            val pipeline = IngestPipeline(
              name = pipelineName,
              json = maybePipeline.get,
              pipelineType = Some(IngestPipelineType.Default)
            )
            // compute diff for pipeline update
            val pipelineDiff: List[PipelineDiff] = pipeline.diff(table.defaultPipeline)
            ElasticSuccess(Some(pipelineDiff))
          case ElasticSuccess(_) =>
            val error =
              ElasticError(
                message =
                  s"${pipelineType.name} ingesting pipeline $pipelineName for index ${table.name} not found.",
                statusCode = Some(404),
                operation = Some("schema")
              )
            logger.error(s"❌ ${error.message}")
            ElasticFailure(error)
          case ElasticFailure(elasticError) =>
            val error =
              ElasticError(
                message =
                  s"Error retrieving ${pipelineType.name} ingesting pipeline $pipelineName for index ${table.name}: ${elasticError.message}",
                statusCode = elasticError.statusCode,
                operation = Some("schema")
              )
            logger.error(s"❌ ${error.message}")
            ElasticFailure(error)
        }
      case _ => ElasticSuccess(None)
    }
  }

  private def applyUpdatesInPlace(
    indexName: String,
    updated: Table,
    diff: TableDiff
  ): Either[ElasticError, Boolean] = {

    val mappingUpdate =
      if (diff.mappings.nonEmpty || diff.columns.nonEmpty)
        api.setMapping(indexName, updated.indexMappings)
      else ElasticSuccess(true)

    val settingsUpdate =
      if (diff.settings.nonEmpty)
        api.updateSettings(indexName, updated.indexSettings)
      else ElasticSuccess(true)

    val aliasUpdate =
      if (diff.aliases.nonEmpty)
        api.setAliases(indexName, updated.indexAliases)
      else ElasticSuccess(true)

    val defaultPipelineCreateOrUpdate =
      if (diff.defaultPipeline.nonEmpty) {
        updated.defaultPipelineName match {
          case Some(pipelineName) =>
            logger.info(
              s"🔧 Updating default ingesting pipeline for index $indexName to $pipelineName."
            )
            api.pipeline(diff.alterPipeline(pipelineName, IngestPipelineType.Default))
          case None =>
            val pipelineName = updated.defaultPipeline.name
            api.pipeline(
              diff.createPipeline(pipelineName, IngestPipelineType.Default)
            )
        }
      } else ElasticSuccess(true)

    val finalPipelineCreateOrUpdate =
      if (diff.finalPipeline.nonEmpty) {
        updated.finalPipelineName match {
          case Some(pipelineName) =>
            logger.info(
              s"🔧 Updating final ingesting pipeline for index $indexName to $pipelineName."
            )
            api.pipeline(diff.alterPipeline(pipelineName, IngestPipelineType.Final))
          case None =>
            val pipelineName = updated.finalPipeline.name
            api.pipeline(
              diff.createPipeline(pipelineName, IngestPipelineType.Final)
            )
        }
      } else ElasticSuccess(true)

    for {
      _    <- mappingUpdate.toEither
      _    <- settingsUpdate.toEither
      _    <- aliasUpdate.toEither
      _    <- defaultPipelineCreateOrUpdate.toEither
      last <- finalPipelineCreateOrUpdate.toEither
    } yield last
  }

  private def migrateToNewSchema(
    indexName: String,
    oldSchema: Table,
    newSchema: Table,
    diff: TableDiff
  ): Either[ElasticError, Boolean] = {

    val tmpIndex = s"${indexName}_tmp_${System.currentTimeMillis()}"

    // migrate index with updated schema
    val migrate: ElasticResult[Boolean] =
      api.performMigration(
        index = indexName,
        tempIndex = tmpIndex,
        mapping = newSchema.indexMappings,
        settings = newSchema.indexSettings,
        aliases = newSchema.indexAliases
      ) match {
        case ElasticFailure(err) =>
          logger.error(s"❌ Failed to perform migration for index $indexName: ${err.message}")
          api.rollbackMigration(
            index = indexName,
            tempIndex = tmpIndex,
            originalMapping = oldSchema.indexMappings,
            originalSettings = oldSchema.indexSettings,
            originalAliases = oldSchema.indexAliases
          ) match {
            case ElasticSuccess(_) =>
              logger.info(s"✅ Rollback of migration for index $indexName completed successfully.")
              ElasticFailure(err.copy(operation = Some("schema")))
            case ElasticFailure(rollbackErr) =>
              logger.error(
                s"❌ Failed to rollback migration for index $indexName: ${rollbackErr.message}"
              )
              ElasticFailure(
                ElasticError(
                  message =
                    s"Migration failed: ${err.message}. Rollback failed: ${rollbackErr.message}",
                  statusCode = Some(500),
                  operation = Some("schema")
                )
              )
          }

        case success @ ElasticSuccess(_) =>
          logger.info(
            s"🔄 Migration performed successfully for index $indexName to temporary index $tmpIndex."
          )
          success
      }

    val defaultPipelineCreateOrUpdate: ElasticResult[Boolean] =
      if (diff.defaultPipeline.nonEmpty) {
        newSchema.defaultPipelineName match {
          case Some(pipelineName) =>
            logger.info(
              s"🔧 Updating default ingesting pipeline for index $indexName to $pipelineName."
            )
            api.pipeline(diff.alterPipeline(pipelineName, IngestPipelineType.Default))
          case None =>
            val pipelineName = newSchema.defaultPipeline.name
            api.pipeline(
              diff.createPipeline(pipelineName, IngestPipelineType.Default)
            )
        }
      } else ElasticSuccess(true)

    val finalPipelineCreateOrUpdate: ElasticResult[Boolean] =
      if (diff.finalPipeline.nonEmpty) {
        newSchema.finalPipelineName match {
          case Some(pipelineName) =>
            logger.info(
              s"🔧 Updating final ingesting pipeline for index $indexName to $pipelineName."
            )
            api.pipeline(diff.alterPipeline(pipelineName, IngestPipelineType.Final))
          case None =>
            val pipelineName = newSchema.finalPipeline.name
            api.pipeline(
              diff.createPipeline(pipelineName, IngestPipelineType.Final)
            )
        }
      } else ElasticSuccess(true)

    for {
      _    <- migrate.toEither
      _    <- defaultPipelineCreateOrUpdate.toEither
      last <- finalPipelineCreateOrUpdate.toEither
    } yield last
  }
}

class DdlRouterExecutor(
  pipelineExec: PipelineExecutor,
  tableExec: TableExecutor
) extends Executor[DdlStatement] {

  override def execute(
    statement: DdlStatement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = statement match {

    case p: PipelineStatement => pipelineExec.execute(p)
    case t: TableStatement    => tableExec.execute(t)

    case _ =>
      Future.successful(
        ElasticFailure(
          ElasticError(s"Unsupported DDL statement: $statement", statusCode = Some(400))
        )
      )
  }
}

trait GatewayApi extends ElasticClientHelpers {
  _: IndicesApi
    with PipelineApi
    with MappingApi
    with SettingsApi
    with AliasApi
    with TemplateApi
    with SearchApi
    with ScrollApi
    with VersionApi =>

  lazy val dqlExecutor = new DqlExecutor(
    api = this,
    logger = logger
  )

  lazy val dmlExecutor = new DmlExecutor(
    api = this,
    logger = logger
  )

  lazy val pipelineExecutor = new PipelineExecutor(
    api = this,
    logger = logger
  )

  lazy val tableExecutor = new TableExecutor(
    api = this,
    logger = logger
  )

  lazy val ddlExecutor = new DdlRouterExecutor(
    pipelineExec = pipelineExecutor,
    tableExec = tableExecutor
  )

  // ========================================================================
  // SQL GATEWAY API
  // ========================================================================

  def run(sql: String)(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    val normalizedQuery =
      sql
        .split("\n")
        .map(_.split("--")(0).trim)
        .filterNot(w => w.isEmpty || w.startsWith("--"))
        .mkString(" ")
    normalizedQuery.split(";\\s*$").toList match {
      case Nil =>
        val error =
          ElasticError(
            message = s"Empty SQL query.",
            statusCode = Some(400),
            operation = Some("sql")
          )
        logger.error(s"❌ ${error.message}")
        Future.successful(ElasticFailure(error))
      case statement :: Nil =>
        ElasticResult.attempt(Parser(statement)) match {
          case ElasticSuccess(parsedStatement) =>
            parsedStatement match {
              case Right(statement) =>
                run(statement)
              case Left(l) =>
                // parsing error
                val error =
                  ElasticError(
                    message = s"Error parsing schema DDL statement: ${l.msg}",
                    statusCode = Some(400),
                    operation = Some("schema")
                  )
                logger.error(s"❌ ${error.message}")
                Future.successful(ElasticFailure(error))
            }

          case ElasticFailure(elasticError) =>
            // parsing error
            Future.successful(ElasticFailure(elasticError.copy(operation = Some("schema"))))
        }

      case statements =>
        implicit val ec: ExecutionContext = system.dispatcher
        // run each statement sequentially and return the result of the last one
        val last = statements
          .foldLeft(
            Future.successful[ElasticResult[QueryResult]](ElasticSuccess(QueryResult.empty))
          ) { (acc, statement) =>
            acc.flatMap {
              case ElasticSuccess(_) =>
                run(statement)
              case failure @ ElasticFailure(_) =>
                return Future.successful(failure)
            }
          }
        last
    }
  }

  def run(
    statement: Statement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    statement match {

      case dql: DqlStatement =>
        dqlExecutor.execute(dql)

      // handle DML statements
      case dml: DmlStatement =>
        dmlExecutor.execute(dml)

      // handle DDL statements
      case ddl: DdlStatement =>
        ddlExecutor.execute(ddl)

      case _ =>
        // unsupported SQL statement
        val error =
          ElasticError(
            message = s"Unsupported SQL statement: $statement",
            statusCode = Some(400),
            operation = Some("schema")
          )
        logger.error(s"❌ ${error.message}")
        Future.successful(ElasticFailure(error))
    }
  }

}
