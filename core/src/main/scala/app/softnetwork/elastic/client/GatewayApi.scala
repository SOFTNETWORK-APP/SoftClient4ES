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
import app.softnetwork.elastic.licensing.{GraceStatus, LicenseRefreshStrategy}
import app.softnetwork.elastic.client.result.{
  DdlResult,
  DmlResult,
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess,
  PipelineResult,
  QueryResult,
  QueryRows,
  QueryStream,
  QueryStructured,
  SQLResult,
  TableResult
}
import app.softnetwork.elastic.sql
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{
  AlterTable,
  AlterTableSetting,
  ClusterStatement,
  CopyInto,
  CreateEnrichPolicy,
  CreatePipeline,
  CreateTable,
  CreateWatcher,
  DdlStatement,
  Delete,
  DescribePipeline,
  DescribeTable,
  DmlStatement,
  DqlStatement,
  DropEnrichPolicy,
  DropPipeline,
  DropTable,
  DropWatcher,
  EnrichPolicyStatement,
  ExecuteEnrichPolicy,
  FromlessSelect,
  Insert,
  LicenseStatement,
  MultiSearch,
  PipelineStatement,
  RefreshLicense,
  SearchStatement,
  SelectStatement,
  ShowClusterName,
  ShowCreatePipeline,
  ShowCreateTable,
  ShowEnrichPolicies,
  ShowEnrichPolicy,
  ShowLicense,
  ShowPipeline,
  ShowPipelines,
  ShowTable,
  ShowTables,
  ShowWatcherStatus,
  ShowWatchers,
  SingleSearch,
  Statement,
  TableStatement,
  TruncateTable,
  Update,
  WatcherStatement
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

import scala.collection.immutable.ListMap
import scala.concurrent.{ExecutionContext, Future}

trait Executor[T <: Statement] {
  def execute(statement: T)(implicit
    system: ActorSystem
  ): Future[ElasticResult[QueryResult]]
}

class SearchExecutor(api: ScrollApi with SearchApi, logger: Logger)
    extends Executor[SearchStatement] {

  override def execute(
    statement: SearchStatement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {

    implicit val ec: ExecutionContext = system.dispatcher

    implicit val context: ConversionContext = NativeContext

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
        if (single.limit.isDefined || single.fields.isEmpty) {
          logger.info(s"▶ Executing classic search on index ${single.from.tables.mkString(",")}")
          api.searchAsync(single) map {
            case ElasticSuccess(results) =>
              logger.info(s"✅ Search returned ${results.results.size} hits.")
              ElasticSuccess(QueryStructured(results))
            case ElasticFailure(err) =>
              ElasticFailure(err.copy(operation = Some("dql")))
          }
        } else {
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
        // Pass the AST directly — avoids the AST → Delete.sql → re-parse round-trip.
        api.deleteByQuery(delete.table.name, delete, refresh = true) match {
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
        // Pass the AST directly — avoids the AST → Update.sql → re-parse round-trip that
        // historically dropped the syntactic shape of string literals (issue #92).
        api.updateByQuery(update.table, update, None, refresh = true) match {
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
        // Pass the AST directly — avoids the AST → Insert.sql → re-parse round-trip.
        api.insertByQuery(insert.table, insert, refresh = true).map {
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

class EnrichPolicyExecutor(
  api: EnrichPolicyApi,
  logger: Logger
) extends Executor[EnrichPolicyStatement] {
  override def execute(
    statement: EnrichPolicyStatement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    // handle ENRICH POLICY statement
    statement match {
      case ShowEnrichPolicies =>
        api.listEnrichPolicies() match {
          case ElasticSuccess(policies) =>
            logger.info(s"✅ Retrieved ${policies.size} enrich policies.")
            Future.successful(
              ElasticResult.success(QueryRows(policies.map(_.toMap)))
            )
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("enrich_policy"))
              )
            )
        }
      case show: ShowEnrichPolicy =>
        api.getEnrichPolicy(show.name) match {
          case ElasticSuccess(policy) =>
            policy match {
              case None =>
                val error = ElasticError(
                  message = s"Enrich policy ${show.name} not found.",
                  statusCode = Some(404),
                  operation = Some("enrich_policy")
                )
                logger.error(s"❌ ${error.message}")
                Future.successful(ElasticFailure(error))
              case Some(policy) =>
                logger.info(s"✅ Retrieved enrich policy ${policy.name}.")
                Future.successful(ElasticResult.success(QueryRows(Seq(policy.toMap))))
            }
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("enrich_policy"))
              )
            )
        }
      case create: CreateEnrichPolicy =>
        api.createEnrichPolicy(create.policy) match {
          case ElasticSuccess(result) =>
            logger.info(s"✅ Created enrich policy ${create.policy.name}.")
            Future.successful(ElasticResult.success(DdlResult(result)))
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("enrich_policy"))
              )
            )
        }

      case execute: ExecuteEnrichPolicy =>
        api.executeEnrichPolicy(execute.name) match {
          case ElasticSuccess(result) =>
            logger.info(s"✅ Executed enrich policy ${execute.name}.")
            Future.successful(ElasticResult.success(QueryRows(Seq(result.toMap))))
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("enrich_policy"))
              )
            )
        }

      case drop: DropEnrichPolicy =>
        api.deleteEnrichPolicy(drop.name) match {
          case ElasticSuccess(result) =>
            logger.info(s"✅ Deleted enrich policy ${drop.name}.")
            Future.successful(ElasticResult.success(DdlResult(result)))
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("enrich_policy"))
              )
            )
        }
    }
  }
}

class WatcherExecutor(api: WatcherApi, logger: Logger) extends Executor[WatcherStatement] {
  override def execute(
    statement: WatcherStatement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    // handle WATCHER statement
    statement match {
      case ShowWatchers =>
        api.listWatchers() match {
          case ElasticSuccess(watchers) =>
            logger.info(s"✅ Retrieved ${watchers.size} watchers.")
            Future.successful(
              ElasticResult.success(QueryRows(watchers.map(_.toMap)))
            )
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("watcher"))
              )
            )
        }
      case status: ShowWatcherStatus =>
        api.getWatcherStatus(status.name) match {
          case ElasticSuccess(stats) =>
            stats match {
              case None =>
                val error = ElasticError(
                  message = s"Watcher ${status.name} not found.",
                  statusCode = Some(404),
                  operation = Some("watcher")
                )
                logger.error(s"❌ ${error.message}")
                Future.successful(ElasticFailure(error))
              case Some(status) =>
                logger.info(s"✅ Retrieved watcher status for ${status.id}.")
                Future.successful(ElasticResult.success(QueryRows(Seq(status.toMap))))
            }
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("watcher"))
              )
            )
        }

      case create: CreateWatcher =>
        api.createWatcher(create.watcher) match {
          case ElasticSuccess(result) =>
            logger.info(s"✅ Created watcher ${create.watcher.id}.")
            Future.successful(ElasticResult.success(DdlResult(result)))
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("watcher"))
              )
            )
        }

      case drop: DropWatcher =>
        api.deleteWatcher(drop.name) match {
          case ElasticSuccess(result) =>
            logger.info(s"✅ Deleted watcher ${drop.name}.")
            Future.successful(ElasticResult.success(DdlResult(result)))
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("watcher"))
              )
            )
        }
    }
  }
}

class PipelineExecutor(api: PipelineApi, logger: Logger) extends Executor[PipelineStatement] {
  override def execute(
    statement: PipelineStatement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    statement match {
      case ShowPipelines =>
        // handle SHOW PIPELINES statement
        api.pipelines() match {
          case ElasticSuccess(pipelines) =>
            logger.info(s"✅ Retrieved all pipelines.")
            Future.successful(
              ElasticResult.success(
                QueryRows(
                  pipelines.map { pipeline =>
                    ListMap(
                      "name"             -> pipeline.name,
                      "processors_count" -> pipeline.processors.size
                    )
                  }.toSeq
                )
              )
            )
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("pipelines"))
              )
            )
        }
      case show: ShowPipeline =>
        // handle SHOW PIPELINE statement
        api.loadPipeline(show.name) match {
          case ElasticSuccess(pipeline) =>
            logger.info(s"✅ Retrieved pipeline ${show.name}.")
            Future.successful(ElasticResult.success(PipelineResult(pipeline)))
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("pipeline"))
              )
            )
        }
      case showCreate: ShowCreatePipeline =>
        // handle SHOW CREATE PIPELINE statement
        api.loadPipeline(showCreate.name) match {
          case ElasticSuccess(pipeline) =>
            logger.info(s"✅ Retrieved pipeline ${showCreate.name}.")
            Future.successful(
              ElasticResult.success(SQLResult(pipeline.ddl))
            )
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
              ElasticResult.success(QueryRows(pipeline.describe))
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
) extends Executor[TableStatement] {
  override def execute(
    statement: TableStatement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    // handle TABLE statement
    statement match {
      // handle SHOW TABLE statement
      case tables: ShowTables =>
        api.allMappings(tables.indices) match {
          case ElasticSuccess(mappings) =>
            logger.info("✅ Retrieved all tables.")
            Future.successful(
              ElasticResult.success(
                QueryRows(
                  mappings
                    // Issue #251/AD-10 — the handshake index is infrastructure, not a table:
                    // never listed, ANY pattern. This one seam also covers jdbc getTables and
                    // Flight GET_TABLES (both execute SHOW TABLES through gateway.run).
                    // DESCRIBE TABLE deliberately still works on it.
                    .filterNot { case (index, _) => index == GatewayApi.HandshakeIndex }
                    .map { case (index, mappings) =>
                      ListMap(
                        "name" -> index,
                        "type" -> mappings.tableType.name.toUpperCase,
                        "pk"   -> mappings.primaryKey.mkString(","),
                        "partitioned" -> mappings.partitionBy
                          .map(p => s"PARTITION BY ${p.column} (${p.granularity})")
                          .getOrElse("")
                      )
                    }
                    .toSeq
                )
              )
            )
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("tables"))
              )
            )
        }
      case show: ShowTable =>
        api.loadSchema(show.table) match {
          case ElasticSuccess(schema) =>
            logger.info(s"✅ Retrieved schema for index ${show.table}.")
            Future.successful(ElasticResult.success(TableResult(schema)))
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("schema"))
              )
            )
        }
      case showCreate: ShowCreateTable =>
        api.loadSchema(showCreate.table) match {
          case ElasticSuccess(schema) =>
            logger.info(s"✅ Retrieved schema for index ${showCreate.table}.")
            Future.successful(
              ElasticResult.success(SQLResult(schema.sql))
            )
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
            Future.successful(ElasticResult.success(QueryRows(schema.describe)))
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

          // 4) Error on indexExists
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
                api.invalidateSchema(indexName)
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
                case Right(result) =>
                  if (result) api.updateSchema(indexName, updatedTable)
                  Future.successful(ElasticSuccess(DdlResult(result)))
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
//                    - "default_pipeline"
//                    - "final_pipeline"
                ),
                diff
              ) match {
                case Right(result) =>
                  if (result) api.updateSchema(indexName, updatedTable)
                  Future.successful(ElasticSuccess(DdlResult(result)))
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
      case Some(pipelineName) =>
        if (pipelineName != existingPipelineName.getOrElse(""))
          logger.info(
            s"🔄 ${pipelineType.name} ingesting pipeline for index ${table.name} has been updated to $pipelineName."
          )
        // load new pipeline
        api.getPipeline(pipelineName) match {
          case ElasticSuccess(maybePipeline) if maybePipeline.isDefined =>
            val pipeline = IngestPipeline(
              name = pipelineName,
              json = maybePipeline.get,
              pipelineType = Some(pipelineType)
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

    // create temporary index name
    val tmpIndex = s"${indexName}_tmp_${System.currentTimeMillis()}"

    // handle all steps for migration
    var steps = collection.mutable.Seq[() => Either[ElasticError, Boolean]]()

    // handle tmp schema settings updates
    var updatingSchema = newSchema

    // handle target schema settings updates
    var alterTableSettings: Seq[AlterTableSetting] = Seq()

    // handle target schema pipelines creation
    var createOrReplaceTablePipelines: collection.mutable.Seq[() => Either[ElasticError, Boolean]] =
      collection.mutable.Seq()

    // handle target schema pipelines deletion
    var dropTmpPipelines: collection.mutable.Seq[() => Either[ElasticError, Boolean]] =
      collection.mutable.Seq()

    val (tmpDefaultPipelineName, tmpDefaultPipeline) =
      migratePipeline(
        indexName,
        tmpIndex,
        oldSchema.defaultPipeline,
        newSchema.defaultPipeline,
        diff
      )

    tmpDefaultPipelineName match {
      case Some(pipelineName) =>
        updatingSchema = updatingSchema.setDefaultPipelineName(pipelineName)
        if (newSchema.defaultPipeline.processors.nonEmpty) {
          alterTableSettings :+=
            AlterTableSetting(
              "default_pipeline",
              sql.StringValue(newSchema.defaultPipeline.name)
            )
          createOrReplaceTablePipelines :+= { () =>
            api
              .pipeline(
                CreatePipeline(
                  name = newSchema.defaultPipeline.name,
                  pipelineType = newSchema.defaultPipeline.pipelineType,
                  ifNotExists = false,
                  orReplace = true,
                  processors = newSchema.defaultPipeline.processors
                )
              )
              .toEither
          }
          dropTmpPipelines :+= { () =>
            api.pipeline(DropPipeline(pipelineName, ifExists = true)).toEither
          }
        }
      case None => // do Nothing
    }

    val (tmpFinalPipelineName, tmpFinalPipeline) =
      migratePipeline(
        indexName,
        tmpIndex,
        oldSchema.finalPipeline,
        newSchema.finalPipeline,
        diff
      )

    tmpFinalPipelineName match {
      case Some(pipelineName) =>
        updatingSchema = updatingSchema.setFinalPipelineName(pipelineName)
        if (newSchema.finalPipeline.processors.nonEmpty) {
          alterTableSettings :+=
            AlterTableSetting(
              "final_pipeline",
              sql.StringValue(newSchema.finalPipeline.name)
            )
          createOrReplaceTablePipelines :+= { () =>
            api
              .pipeline(
                CreatePipeline(
                  name = newSchema.finalPipeline.name,
                  pipelineType = newSchema.finalPipeline.pipelineType,
                  ifNotExists = false,
                  orReplace = true,
                  processors = newSchema.finalPipeline.processors
                )
              )
              .toEither
          }
          dropTmpPipelines :+= { () =>
            api.pipeline(DropPipeline(pipelineName, ifExists = true)).toEither
          }
        }
      case None => // do Nothing
    }

    // migrate index with updated schema
    val migrate: () => Either[ElasticError, Boolean] =
      () =>
        {
          api.performMigration(
            index = indexName,
            tempIndex = tmpIndex,
            mapping = updatingSchema.indexMappings,
            settings = updatingSchema.indexSettings,
            aliases = updatingSchema.indexAliases
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
                  logger.info(
                    s"✅ Rollback of migration for index $indexName completed successfully."
                  )
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
        }.toEither

    val updateTableSettings =
      () => {
        if (alterTableSettings.nonEmpty) {
          logger.info(
            s"🔧 Applying $indexName new pipeline settings [${alterTableSettings.map(_.sql).mkString(", ")}]."
          )
          val schema = updatingSchema.merge(alterTableSettings)
          api
            .updateSettings(
              indexName,
              schema
                .copy(settings = schema.settings - "number_of_shards" - "number_of_replicas")
                .indexSettings
            )
            .toEither
        } else Right(true)
      }

    steps ++= Seq(
      tmpDefaultPipeline,
      tmpFinalPipeline,
      migrate
    ) ++ (createOrReplaceTablePipelines :+ updateTableSettings) ++ dropTmpPipelines

    steps.foldLeft(Right(true): Either[ElasticError, Boolean]) {
      case (Left(err), _) => Left(err)
      case (_, step)      => step()
    }
  }

  private def migratePipeline(
    indexName: String,
    tmpIndexName: String,
    oldPipeline: IngestPipeline,
    newPipeline: IngestPipeline,
    diff: TableDiff
  ): (Option[String], () => Either[ElasticError, Boolean]) = {
    if (oldPipeline.pipelineType != newPipeline.pipelineType) {
      logger.error(
        s"❌ Cannot change pipeline type from ${oldPipeline.pipelineType.name} to ${newPipeline.pipelineType.name} for index $indexName."
      )
      return (
        None,
        () =>
          Left(
            ElasticError(
              message =
                s"Cannot change pipeline type from ${oldPipeline.pipelineType.name} to ${newPipeline.pipelineType.name} for index $indexName.",
              statusCode = Some(400),
              operation = Some("schema")
            )
          )
      )
    }
    if (oldPipeline.name != newPipeline.name) {
      logger.info(
        s"🔄 ${oldPipeline.pipelineType.name} Ingesting pipeline for index $indexName has been updated to ${newPipeline.name}."
      )
      // create updated default pipeline
      val step: () => Either[ElasticError, Boolean] = { () =>
        api
          .pipeline(
            CreatePipeline(
              name = newPipeline.name,
              pipelineType = newPipeline.pipelineType,
              ifNotExists = false,
              orReplace = true,
              processors = newPipeline.processors
            )
          )
          .toEither
          .flatMap { result =>
            if (result) {
              logger.info(
                s"🔧 Created updated ${oldPipeline.pipelineType.name} ingesting pipeline ${newPipeline.name} for index $tmpIndexName."
              )
              Right(result)
            } else {
              val error =
                ElasticError(
                  message =
                    s"Failed to create ${oldPipeline.pipelineType.name} ingesting pipeline ${newPipeline.name}.",
                  statusCode = Some(500),
                  operation = Some("schema")
                )
              logger.error(s"❌ ${error.message}")
              Left(error)
            }
          }
      }
      (None, step)
    } else {
      diff.alterPipeline(oldPipeline.name, oldPipeline.pipelineType) match {
        case Some(alterPipeline) =>
          val tmpDefaultPipeline =
            s"${tmpIndexName}_ddl_${oldPipeline.pipelineType.name.toLowerCase}_pipeline"
          logger.warn(
            s"⚠️ Default ingesting pipeline ${oldPipeline.name} for index $indexName has been updated. Creating a temporary pipeline $tmpDefaultPipeline that will be used for reindex."
          )
          val step = () => {
            // create updated default pipeline
            val mergedDefaultPipeline = oldPipeline.merge(alterPipeline.statements)
            api
              .pipeline(
                CreatePipeline(
                  name = tmpDefaultPipeline,
                  pipelineType = oldPipeline.pipelineType,
                  ifNotExists = false,
                  orReplace = true,
                  processors = mergedDefaultPipeline.processors
                )
              )
              .toEither
              .flatMap { result =>
                if (result) {
                  logger.info(
                    s"🔧 Created updated ${oldPipeline.pipelineType.name} ingesting pipeline $tmpDefaultPipeline for index $tmpIndexName."
                  )
                  Right(result)
                } else {
                  val error =
                    ElasticError(
                      message =
                        s"Failed to create ${oldPipeline.pipelineType.name} ingesting pipeline $tmpDefaultPipeline.",
                      statusCode = Some(500),
                      operation = Some("schema")
                    )
                  logger.error(s"❌ ${error.message}")
                  Left(error)
                }
              }
          }
          (Some(tmpDefaultPipeline), step)
        case _ => // no changes to default pipeline
          (None, () => Right(false))
      }
    }
  }
}

class ClusterExecutor(
  api: ClusterApi,
  logger: Logger
) extends Executor[ClusterStatement] {
  override def execute(
    statement: ClusterStatement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    statement match {
      case ShowClusterName =>
        api.clusterName match {
          case ElasticSuccess(name) =>
            Future.successful(
              ElasticResult.success(QueryRows(Seq(ListMap("name" -> name))))
            )
          case ElasticFailure(elasticError) =>
            Future.successful(
              ElasticFailure(
                elasticError.copy(operation = Some("cluster"))
              )
            )
        }
    }
  }
}

class LicenseExecutor(
  strategy: LicenseRefreshStrategy
) extends Executor[LicenseStatement] {

  override def execute(
    statement: LicenseStatement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] =
    statement match {
      case ShowLicense    => Future.successful(executeShowLicense())
      case RefreshLicense => Future.successful(executeRefreshLicense())
    }

  private def executeShowLicense(): ElasticResult[QueryResult] = {
    val mgr = strategy.licenseManager
    val key = mgr.currentLicenseKey
    val graceStatus = mgr.graceStatus match {
      case GraceStatus.NotInGrace => "Active"
      case GraceStatus.EarlyGrace(days) =>
        s"Expired ($days days ago, in grace period)"
      case GraceStatus.MidGrace(days, remaining) =>
        s"Expired ($days days ago, $remaining days until Community fallback)"
    }
    val degradedNote = if (mgr.wasDegraded) " (degraded)" else ""
    val trialNote = if (key.isTrial) " (trial)" else ""
    val row = ListMap[String, Any](
      "license_type"           -> s"${mgr.licenseType}$trialNote$degradedNote",
      "trial"                  -> key.isTrial,
      "platform"               -> key.platform.map(_.name).getOrElse("PRODUCTION"),
      "max_materialized_views" -> formatQuota(mgr.quotas.maxMaterializedViews),
      "max_clusters"           -> formatQuota(mgr.quotas.maxClusters),
      "max_result_rows"        -> formatQuota(mgr.quotas.maxQueryResults),
      "max_joins"              -> formatQuota(mgr.quotas.maxJoins),
      "expires_at"             -> formatExpiry(key.expiresAt),
      "days_remaining"         -> key.daysRemaining.getOrElse(-1L),
      "status"                 -> graceStatus
    )
    ElasticSuccess(QueryRows(Seq(row)))
  }

  private def executeRefreshLicense(): ElasticResult[QueryResult] = {
    val previousTier = strategy.licenseManager.licenseType
    strategy.refresh() match {
      case Right(key) =>
        val row = ListMap[String, Any](
          "previous_tier" -> previousTier.toString,
          "new_tier"      -> key.licenseType.toString,
          "trial"         -> key.isTrial,
          "expires_at"    -> formatExpiry(key.expiresAt),
          "status"        -> "Refreshed",
          "message"       -> ""
        )
        ElasticSuccess(QueryRows(Seq(row)))
      case Left(err) =>
        val currentKey = strategy.licenseManager.currentLicenseKey
        val row = ListMap[String, Any](
          "previous_tier" -> previousTier.toString,
          "new_tier"      -> previousTier.toString,
          "trial"         -> currentKey.isTrial,
          "expires_at"    -> formatExpiry(currentKey.expiresAt),
          "status"        -> "Failed",
          "message"       -> err.message
        )
        ElasticSuccess(QueryRows(Seq(row)))
    }
  }

  private def formatQuota(q: Option[Int]): String =
    q.map(_.toString).getOrElse("unlimited")
  private def formatExpiry(exp: Option[java.time.Instant]): String =
    exp.map(_.toString).getOrElse("never")
}

/** Issue #251 (story 20.9) — FROM-less SELECT: Painless handshake AGAINST Elasticsearch. The
  * statement's LIMIT/OFFSET are applied engine-side on the one assembled row, AFTER the ES
  * round-trip (AD-12) — `LIMIT 0` still connection-checks (AD-8), and the internal rewrite always
  * carries its own `LIMIT 1` so it can never reach scroll/PIT.
  */
class FromlessSelectExecutor(
  evaluator: HandshakeEvaluator,
  logger: Logger
) extends Executor[FromlessSelect] {

  override def execute(
    statement: FromlessSelect
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher
    // run(statement) never calls validate(): a programmatic FromlessSelect reaches this
    // executor without the parser's gate — re-run the guards (surviving review critical #5).
    statement.validate() match {
      case Left(reason) =>
        val error =
          ElasticError(message = reason, statusCode = Some(400), operation = Some("sql"))
        logger.error(s"❌ ${error.message}")
        Future.successful(ElasticFailure(error))
      case Right(_) =>
        evaluator.evaluateHandshake(statement).map {
          case ElasticSuccess(row) =>
            val offset = statement.limit.flatMap(_.offset).map(_.offset).getOrElse(0)
            val max = statement.limit.map(_.limit).getOrElse(1)
            ElasticSuccess(QueryRows(Seq(row).drop(offset).take(max)))
          case ElasticFailure(error) => ElasticFailure(error)
        }
    }
  }
}

class DqlRouterExecutor(
  searchExec: SearchExecutor,
  pipelineExec: PipelineExecutor,
  tableExec: TableExecutor,
  watcherExec: WatcherExecutor,
  policyExec: EnrichPolicyExecutor,
  clusterExec: ClusterExecutor,
  licenseExec: LicenseExecutor,
  fromlessExec: FromlessSelectExecutor // issue #251 — see the story 20.9 AD-4′ arity note
) extends Executor[DqlStatement] {

  override def execute(
    statement: DqlStatement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = statement match {

    case s: SearchStatement       => searchExec.execute(s)
    case p: PipelineStatement     => pipelineExec.execute(p)
    case t: TableStatement        => tableExec.execute(t)
    case w: WatcherStatement      => watcherExec.execute(w)
    case e: EnrichPolicyStatement => policyExec.execute(e)
    case c: ClusterStatement      => clusterExec.execute(c)
    case l: LicenseStatement      => licenseExec.execute(l)
    // Issue #251 — FROM-less SELECT: Painless handshake AGAINST Elasticsearch.
    case f: FromlessSelect => fromlessExec.execute(f)

    case _ =>
      Future.successful(
        ElasticFailure(
          ElasticError(s"Unsupported DQL statement: $statement", statusCode = Some(400))
        )
      )
  }
}

class DdlRouterExecutor(
  pipelineExec: PipelineExecutor,
  tableExec: TableExecutor,
  watcherExec: WatcherExecutor,
  policyExec: EnrichPolicyExecutor
) extends Executor[DdlStatement] {

  override def execute(
    statement: DdlStatement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = statement match {

    case p: PipelineStatement     => pipelineExec.execute(p)
    case t: TableStatement        => tableExec.execute(t)
    case w: WatcherStatement      => watcherExec.execute(w)
    case e: EnrichPolicyStatement => policyExec.execute(e)

    case _ =>
      Future.successful(
        ElasticFailure(
          ElasticError(s"Unsupported DDL statement: $statement", statusCode = Some(400))
        )
      )
  }
}

trait GatewayApi extends IndicesApi with ElasticClientHelpers {
  self: ElasticClientApi =>

  lazy val searchExecutor = new SearchExecutor(
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

  lazy val watcherExecutor = new WatcherExecutor(
    api = this,
    logger = logger
  )

  lazy val policyExecutor = new EnrichPolicyExecutor(
    api = this,
    logger = logger
  )

  lazy val clusterExecutor = new ClusterExecutor(
    api = this,
    logger = logger
  )

  lazy val licenseExecutor = new LicenseExecutor(
    strategy = licenseRefreshStrategy // No longer Option — NopRefreshStrategy for Community
  )

  /** Issue #251 — the seam behind the FROM-less SELECT handshake. Today's backend searches the
    * dedicated handshake index; a future Painless-execute-API backend swaps in HERE, touching
    * nothing else (AD-4′).
    */
  lazy val handshakeEvaluator: HandshakeEvaluator =
    new SearchHandshakeEvaluator(api = this, logger = logger)

  lazy val fromlessSelectExecutor = new FromlessSelectExecutor(
    evaluator = handshakeEvaluator,
    logger = logger
  )

  lazy val dqlExecutor = new DqlRouterExecutor(
    searchExec = searchExecutor,
    pipelineExec = pipelineExecutor,
    tableExec = tableExecutor,
    watcherExec = watcherExecutor,
    policyExec = policyExecutor,
    clusterExec = clusterExecutor,
    licenseExec = licenseExecutor,
    fromlessExec = fromlessSelectExecutor
  )

  lazy val ddlExecutor = new DdlRouterExecutor(
    pipelineExec = pipelineExecutor,
    tableExec = tableExecutor,
    watcherExec = watcherExecutor,
    policyExec = policyExecutor
  )

  // ========================================================================
  // SQL GATEWAY API
  // ========================================================================

  def run(sql: String)(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    logger.info(s"📥 SQL: $sql")
    // Raw input goes straight to the quote- and comment-aware splitter: a line-based
    // pre-normalization here (`split("\n").map(_.split("--")(0))`) used to sever literals
    // containing `--` before the splitter could protect them. Comment stripping and newline
    // collapsing happen inside splitStatements and Parser.apply, both literal-aware.
    GatewayApi.splitStatements(sql) match {
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
        // `Parser.apply` is total since #250: it returns `Left` for every NonFatal rejection, so
        // the `ElasticResult.attempt` wrapper this used to carry — and its `ElasticFailure` branch,
        // which relabelled a thrown rejection — are gone. `attempt` catches exactly `NonFatal` and
        // so does `Parser.apply`, which makes that branch unreachable rather than merely unlikely.
        // What changes for a caller: the inputs that used to throw now get an honest
        // `statusCode = Some(400)` instead of `None`, and a failure originating BELOW the grammar
        // arrives as `Internal parser error: <Class>[: <detail>]` so it stays distinguishable.
        Parser(statement) match {
          case Right(parsed) =>
            run(parsed)
          case Left(l) =>
            // Parse rejection. `run` is the front door for DQL, DML AND DDL, so the message must
            // not claim a statement class — and it cannot know one: `ParserError` is a bare
            // string (see GatewayApi.ParseRejectionPrefix). Echo the statement instead and let
            // the reader classify it; BI tools surface this text verbatim (jdbc#35).
            // `l.msg` is passed through RAW on purpose: bounding and sanitising it is
            // parseRejectionMessage's job.
            val error =
              ElasticError(
                message = GatewayApi.parseRejectionMessage(statement, l.msg),
                // `l.cause` is set ONLY by `Parser.apply`'s NonFatal boundary catch (#250 AD-10);
                // a grammar rejection carries None. `ElasticError extends Throwable(message,
                // cause.orNull)`, so this restores exactly the stack trace `ElasticResult.attempt`
                // used to supply for an internal parser fault.
                cause = l.cause,
                statusCode = Some(400),
                operation = Some("sql")
              )
            logger.error(s"❌ ${error.message}")
            Future.successful(ElasticFailure(error))
        }

      case statements =>
        implicit val ec: ExecutionContext = system.dispatcher
        // Run each statement sequentially and return the result of the last one; the first
        // failure becomes the accumulated value and every later flatMap passes it through
        // without running its statement. NOT a `return`: this closure executes asynchronously
        // on the dispatcher once the previous Future completes, where a non-local return
        // throws `NonLocalReturnControl` — which `NonFatal` excludes, so nothing completes
        // the Future and the caller hangs. Unreachable while the old anchored split kept this
        // branch dead; live now that the split is real.
        statements
          .foldLeft(
            Future.successful[ElasticResult[QueryResult]](ElasticSuccess(QueryResult.empty))
          ) { (acc, statement) =>
            acc.flatMap {
              case ElasticSuccess(_) =>
                run(statement)
              case failure @ ElasticFailure(_) =>
                Future.successful(failure)
            }
          }
    }
  }

  def run(
    statement: Statement
  )(implicit system: ActorSystem): Future[ElasticResult[QueryResult]] = {
    implicit val ec: ExecutionContext = system.dispatcher

    // ✅ TRY EXTENSIONS FIRST
    extensionRegistry.findHandler(statement) match {
      case Some(extension) =>
        logger.info(s"🔌 Routing to extension: ${extension.extensionName}")
        extension.execute(statement, self) // ✅ Pass full client API

      case None =>
        // ✅ FALLBACK TO STANDARD EXECUTORS
        statement match {
          case dql: DqlStatement =>
            logger.debug("🔧 Executing DQL with base executor")
            val result = dqlExecutor.execute(dql)
            result.foreach {
              case ElasticSuccess(_) =>
                licenseRefreshStrategy.telemetryCollector.incrementQueries()
              case _ =>
            }(system.dispatcher)
            result

          case dml: DmlStatement =>
            logger.debug("🔧 Executing DML with base executor")
            dmlExecutor.execute(dml)

          case ddl: DdlStatement =>
            logger.debug("🔧 Executing DDL with base executor")
            ddlExecutor.execute(ddl)

          case _ =>
            val error = ElasticError(
              message = s"Unsupported SQL statement: $statement",
              statusCode = Some(400),
              operation = Some("schema")
            )
            logger.error(s"❌ ${error.message}")
            Future.successful(ElasticFailure(error))
        }
    }
  }

}

object GatewayApi {

  /** Prefix of every parse rejection `run(sql: String)` returns.
    *
    * It says SQL, not "schema DDL" (jdbc#35). `run` is the single front door for DQL, DML '''and'''
    * DDL, and the statement that failed to parse has no known kind: `Parser.apply` hands back
    * `Left(ParserError(msg, cause))` — a reason string with no AST, no position and no statement
    * class (`cause` is set only for an internal fault, #250 AD-10, and carries a stack trace, not a
    * classification). So the message names what it can actually observe (the statement, and the
    * parser's reason) and nothing it cannot.
    *
    * The `Error parsing` stem is deliberate, not inherited by accident: it is what
    * `ReplGatewayIntegrationSpec` asserts, and it is the wording jdbc#35 itself proposed.
    */
  private[client] val ParseRejectionPrefix: String = "Error parsing SQL statement"

  /** Budget for EACH part of a parse-rejection message — the statement excerpt and the parser's
    * reason alike. One constant, applied uniformly, one sentence: '''no part of a parse-rejection
    * message exceeds MaxExcerpt characters'''.
    *
    * Bounding only the statement would have been incoherent
    * (`feedback_constant_uniform_justification`): the justification is the medium — a narrow BI
    * error dialog — and it binds both halves. It is also not hypothetical that the reason needs it.
    * `Parser.apply` maps post-parse `validate()` failures to a `Left` too, and those messages embed
    * whole AST renderings — `sql/.../query/From.scala` repeats `"ON clause $this ..."` nine times,
    * and `Parser.scala:312` echoes the '''entire SCRIPT body''' into `"Invalid SCRIPT AS expression
    * ($body): $msg"`. Unbounded, one of those buries the diagnosis and re-exposes a whole script
    * inside an error dialog.
    *
    * An excerpt NEVER exceeds this many characters — the ellipsis is inside the budget, not added
    * to it, so the constant means what its name says. Whole-message ceiling: prefix + brackets +
    * MaxExcerpt + ": " + MaxExcerpt, i.e. under 500 characters for every input.
    */
  private[client] val MaxExcerpt: Int = 200

  /** ASCII on purpose — see the encoding rule in the story's build facts. */
  private[client] val ExcerptEllipsis: String = "..."

  /** How the budget is split when a part is too long: head + ellipsis + tail.
    *
    * '''A prefix alone would not do the job this story exists for.''' Epic 19 measured 89/89
    * Tableau-emitted statements rejected, and every one of them opens with the same long, fully
    * quoted SELECT list — so the first 200 characters identify the TOOL, not the QUERY, and AC-2
    * ("names the offending statement") would be satisfied only on paper. Keeping the tail keeps the
    * discriminating part — the FROM / WHERE / GROUP BY, and the region a parse error usually sits
    * in. It also defuses the `/* app=Tableau ... */` banner case: block comments are stripped by
    * '''neither''' `splitStatements` (which handles `--` only) '''nor''' `Parser.normalize`, so a
    * long leading banner would consume a prefix-only excerpt whole and hide the statement
    * completely — with a tail, the statement still shows. Hand-typed SQL, the story's other
    * audience, is short enough to be echoed whole and is unaffected. 120 + 3 + 77 = 200 exactly.
    */
  private[client] val ExcerptHead: Int = 120
  private[client] val ExcerptTail: Int = MaxExcerpt - ExcerptHead - ExcerptEllipsis.length

  /** Runs of whitespace '''and control characters''' collapse to one space.
    *
    * Not cosmetics — this is the story's one security control. An excerpt is attacker-influenced
    * text (it is the caller's own statement) that lands in a log record, in a `SQLException`
    * message, and in a BI error dialog. Collapsing newlines is what stops a crafted statement from
    * forging a second log line; collapsing the rest of the C0 controls is what stops an ANSI escape
    * from reaching the terminal that renders the REPL's error output. Java's `\s` is ASCII-only and
    * covers NONE of ESC / NUL / BEL, so `\p{Cntrl}` is required alongside it; U+0085, U+2028 and
    * U+2029 are added because renderers treat them as line breaks. Written as `\\u` escapes so the
    * source stays ASCII.
    */
  private val ExcerptNoise: String = "[\\s\\p{Cntrl}\\u0085\\u2028\\u2029]+"

  /** Never split a surrogate pair when cutting an excerpt.
    *
    * `String.take` / `substring` count UTF-16 code units, so a naive cut at a fixed offset can emit
    * a lone surrogate. That string is not valid UTF-16, and it is re-encoded at least twice on its
    * way to the analyst (`SQLException` then the host's own serialisation; a log file's charset).
    * Moving the cut by one character is always safe and costs at most one character of the budget.
    */
  private def cutBefore(s: String, at: Int): Int =
    if (at > 0 && Character.isHighSurrogate(s.charAt(at - 1))) at - 1 else at

  private def cutAfter(s: String, at: Int): Int =
    if (at < s.length && Character.isLowSurrogate(s.charAt(at))) at + 1 else at

  /** Single-line, length-bounded rendering of one message part — '''display only'''.
    *
    * Used for BOTH halves of a parse-rejection message (the statement and the parser's reason), so
    * the bound is uniform and neither half can bury the other.
    *
    * This is NOT `Parser.normalize` and must never be used as one: it is quote-blind and collapses
    * whitespace inside string literals too. Nothing parses, splits or matches on its output — and
    * nothing may start to, because a statement may legitimately contain `]` (`SELECT a[1] FRM t`, a
    * measured rejection) and would make the enclosing `[...]` framing ambiguous to any consumer
    * naive enough to try. Stripping `]` was considered and REJECTED: it would corrupt the display
    * of valid SQL in order to protect a consumer that must not exist.
    *
    * TRAP — for the statement half this receives the '''post-split''' statement, not the caller's
    * raw input: `splitStatements` has already replaced each `--` comment with a space and consumed
    * the separating `;`. Stated here because it is what makes `message should include(<raw sql>)` a
    * safe assertion only for inputs free of comments, `;` and repeated whitespace.
    *
    * Post-condition: `excerpt(s).length <= MaxExcerpt`, for every `s`.
    */
  private[client] def excerpt(text: String): String = {
    val oneLine = text.replaceAll(ExcerptNoise, " ").trim
    if (oneLine.length <= MaxExcerpt) oneLine
    else {
      val head = oneLine.substring(0, cutBefore(oneLine, ExcerptHead))
      val tail = oneLine.substring(cutAfter(oneLine, oneLine.length - ExcerptTail))
      head + ExcerptEllipsis + tail
    }
  }

  /** Last-resort reason text. A message ending `...]: ` with nothing after the colon is exactly the
    * shape AC-2 forbids, so the reason is never allowed to be empty.
    */
  private[client] val NoParserReason: String = "no reason reported by the parser"

  /** The one message builder every parse rejection of `run(sql: String)` goes through.
    *
    * Since #250 there is only one route: `Parser.apply` is total for `NonFatal`, so the thrown
    * route it used to share this builder with is gone. A failure raised BELOW the grammar arrives
    * here as `Parser.InternalParseFailure: <Class>[: <detail>]`, which is what keeps an internal
    * fault distinguishable from a syntax error now that both carry `400`.
    *
    * It owns BOTH message-shape invariants: each half is bounded and single-lined by `excerpt`, and
    * the reason half is never empty — `ParserError(msg)` carries no non-empty invariant, so it can
    * hand over a blank string.
    */
  private[client] def parseRejectionMessage(statement: String, reason: String): String = {
    val shownReason = excerpt(reason)
    val safeReason = if (shownReason.isEmpty) NoParserReason else shownReason
    s"$ParseRejectionPrefix [${excerpt(statement)}]: $safeReason"
  }

  /** Issue #251 — the dedicated FROM-less-handshake index. NOT dot-prefixed (ES 8+
    * deprecation-warns dot-prefixed non-system creation — and the deprecation LOG itself creates a
    * `.ds-*` index, the measured all_templates trap); product-prefixed so operators can attribute
    * it; excluded from SHOW TABLES by TableExecutor (AD-10).
    */
  val HandshakeIndex: String = "softclient4es_handshake"

  /** 1 shard / 0 replicas: single-node clusters stay green. NEVER defaultSettings (ngram). */
  private[client] val HandshakeSettings: String =
    """{"index": {"number_of_shards": 1, "number_of_replicas": 0}}"""

  /** index.hidden exists only from ES 7.7 — used when the cluster supports it (AD-9/AD-10). */
  private[client] val HandshakeSettingsHidden: String =
    """{"index": {"number_of_shards": 1, "number_of_replicas": 0, "hidden": true}}"""

  private[client] val HandshakeMapping: String =
    """{"properties": {"dummy": {"type": "keyword"}}}"""

  private[client] val HandshakeDocId: String = "1"
  private[client] val HandshakeDoc: String = """{"dummy": "dummy"}"""

  /** Split a normalized SQL string into statements on top-level `;`.
    *
    * This replaces `split(";\\s*$")`, which — `$` anchoring to the end of the whole (newline-free)
    * string — could only ever strip a trailing semicolon, never separate statements: the
    * multi-statement branch of `run` was unreachable and `run("a; b")` silently executed `a` alone
    * under the old lenient parse. The documented contract ("splits multiple statements separated by
    * `;`") is now real.
    *
    * Quote-aware: a `;` inside a single-quoted literal or a double-quoted identifier is not a
    * boundary. Backslash escapes are honored inside quotes, matching the grammar's literal rule
    * (`([^'\\]|\\.)*`). An unterminated quote consumes to the end of the input — the parser then
    * reports the malformed statement itself, which beats guessing where it was meant to end. Empty
    * fragments (`;;`, a trailing `;`) are dropped.
    */
  private[client] def splitStatements(sql: String): List[String] = {
    val statements = scala.collection.mutable.ListBuffer.empty[String]
    val current = new StringBuilder
    var quote: Char = 0
    var i = 0
    while (i < sql.length) {
      val c = sql.charAt(i)
      if (quote != 0) {
        current.append(c)
        if (c == '\\' && i + 1 < sql.length) {
          current.append(sql.charAt(i + 1))
          i += 1
        } else if (c == quote) {
          quote = 0
        }
      } else if (c == '-' && i + 1 < sql.length && sql.charAt(i + 1) == '-') {
        // A `--` comment (outside quotes) runs to the end of its line. Skipping it here — rather
        // than relying on the line-based pre-normalization, which the REPL batch path does not
        // apply — keeps an apostrophe in a comment (`-- don't split`) from opening a phantom
        // quote and a `;` in a comment from splitting mid-comment. Inside quotes `--` is data.
        // A space stands in for the comment so tokens on either side stay separated.
        while (i < sql.length && sql.charAt(i) != '\n') {
          i += 1
        }
        current.append(' ')
      } else {
        c match {
          case '\'' | '"' =>
            quote = c
            current.append(c)
          case ';' =>
            statements += current.toString
            current.clear()
          case _ =>
            current.append(c)
        }
      }
      i += 1
    }
    statements += current.toString
    statements.iterator.map(_.trim).filter(_.nonEmpty).toList
  }
}
