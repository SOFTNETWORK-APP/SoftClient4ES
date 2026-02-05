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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import app.softnetwork.elastic.client.bulk.{
  BulkElasticAction,
  BulkItem,
  BulkOptions,
  FailedDocument,
  SuccessfulDocument
}
import app.softnetwork.elastic.sql.{query, PainlessContextType}
import app.softnetwork.elastic.sql.query.SQLAggregation
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.client.scroll._
import app.softnetwork.elastic.sql.policy.{EnrichPolicy, EnrichPolicyTask, EnrichPolicyTaskStatus}
import app.softnetwork.elastic.sql.schema.TableAlias
import app.softnetwork.elastic.sql.transform.{TransformConfig, TransformStats}
import app.softnetwork.elastic.sql.watcher.{Watcher, WatcherStatus}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

trait NopeClientApi extends ElasticClientApi {

  override private[client] def executeAddAlias(
    alias: TableAlias
  ): ElasticResult[Boolean] = ElasticResult.success(false)

  override private[client] def executeRemoveAlias(
    index: String,
    alias: String
  ): ElasticResult[Boolean] = ElasticResult.success(false)

  override private[client] def executeAliasExists(alias: String): ElasticResult[Boolean] =
    ElasticResult.success(false)

  override private[client] def executeGetAliases(index: String): ElasticResult[String] =
    ElasticResult.success("{}")

  override private[client] def executeSwapAlias(
    oldIndex: String,
    newIndex: String,
    alias: String
  ): ElasticResult[Boolean] = ElasticResult.success(false)

  /** Check if client is initialized and connected
    */
  override def isInitialized: Boolean = true

  /** Test connection
    *
    * @return
    *   true if connection is successful
    */
  override def testConnection(): Boolean = true

  override def close(): Unit = {}

  override private[client] def executeCount(query: ElasticQuery): ElasticResult[Option[Double]] =
    ElasticResult.success(None)

  override private[client] def executeCountAsync(
    query: ElasticQuery
  )(implicit ec: ExecutionContext): Future[ElasticResult[Option[Double]]] = Future {
    ElasticResult.success(None)
  }

  override private[client] def executeDelete(
    index: String,
    id: String,
    wait: Boolean
  ): ElasticResult[Boolean] = ElasticResult.success(false)

  override private[client] def executeDeleteAsync(index: String, id: String, wait: Boolean)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Boolean]] = Future {
    ElasticResult.success(false)
  }

  override private[client] def executeFlush(
    index: String,
    force: Boolean,
    wait: Boolean
  ): ElasticResult[Boolean] = ElasticResult.success(false)

  override private[client] def executeGet(
    index: String,
    id: String
  ): ElasticResult[Option[String]] = ElasticResult.success(None)

  override private[client] def executeGetAsync(index: String, id: String)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[String]]] = Future {
    ElasticResult.success(None)
  }

  override private[client] def executeIndex(
    index: String,
    id: String,
    source: String,
    wait: Boolean
  ): ElasticResult[Boolean] = ElasticResult.success(false)

  override private[client] def executeIndexAsync(
    index: String,
    id: String,
    source: String,
    wait: Boolean
  )(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]] = Future {
    ElasticResult.success(false)
  }

  override private[client] def executeCreateIndex(
    index: String,
    settings: String,
    mappings: Option[String],
    aliases: Seq[TableAlias]
  ): ElasticResult[Boolean] = ElasticResult.success(false)

  override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
    ElasticResult.success(false)

  override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] =
    ElasticResult.success(false)

  override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] =
    ElasticResult.success(false)

  override private[client] def executeReindex(
    sourceIndex: String,
    targetIndex: String,
    refresh: Boolean,
    pipeline: Option[String]
  ): ElasticResult[(Boolean, Option[Long])] = ElasticResult.success((false, None))

  override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
    ElasticResult.success(false)

  override private[client] def executeSetMapping(
    index: String,
    mapping: String
  ): ElasticResult[Boolean] = ElasticResult.success(false)

  override private[client] def executeGetMapping(index: String): ElasticResult[String] =
    ElasticResult.success("{}")

  override private[client] def executeRefresh(index: String): ElasticResult[Boolean] =
    ElasticResult.success(false)

  /** Classic scroll (works for both hits and aggregations)
    */
  override private[client] def scrollClassic(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation],
    config: ScrollConfig
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = Source.empty

  /** Search After (only for hits, more efficient)
    */
  override private[client] def searchAfter(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    config: ScrollConfig,
    hasSorts: Boolean
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = Source.empty

  override private[client] def pitSearchAfter(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    config: ScrollConfig,
    hasSorts: Boolean
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = Source.empty

  override private[client] def executeSingleSearch(
    elasticQuery: ElasticQuery
  ): ElasticResult[Option[String]] = ElasticResult.success(None)

  override private[client] def executeMultiSearch(
    elasticQueries: ElasticQueries
  ): ElasticResult[Option[String]] = ElasticResult.success(None)

  override private[client] def executeSingleSearchAsync(elasticQuery: ElasticQuery)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[String]]] = Future {
    ElasticResult.success(None)
  }

  override private[client] def executeMultiSearchAsync(elasticQueries: ElasticQueries)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[String]]] = Future {
    ElasticResult.success(None)
  }

  /** Implicit conversion of an SQL query to Elasticsearch JSON. Used for query serialization.
    *
    * @param sqlSearch
    *   the SQL search request to convert
    * @return
    *   JSON string representation of the query
    */
  override private[client] implicit def singleSearchToJsonQuery(
    sqlSearch: query.SingleSearch
  )(implicit
    timestamp: Long,
    contextType: PainlessContextType = PainlessContextType.Query
  ): String = "{\"query\": {\"match_all\": {}}}"

  override private[client] def executeUpdateSettings(
    index: String,
    settings: String
  ): ElasticResult[Boolean] = ElasticResult.success(false)

  override private[client] def executeLoadSettings(index: String): ElasticResult[String] =
    ElasticResult.success("{}")

  override private[client] def executeUpdate(
    index: String,
    id: String,
    source: String,
    upsert: Boolean,
    wait: Boolean
  ): ElasticResult[Boolean] = ElasticResult.success(false)

  override private[client] def executeUpdateAsync(
    index: String,
    id: String,
    source: String,
    upsert: Boolean,
    wait: Boolean
  )(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]] = Future {
    ElasticResult.success(false)
  }

  override private[client] def executeVersion(): ElasticResult[String] =
    ElasticResult.success("0.0.0")

  override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] =
    ElasticResult.success(None)

  override private[client] def executeCreatePipeline(
    pipelineName: String,
    pipelineDefinition: String
  ): ElasticResult[Boolean] =
    ElasticResult.success(false)

  override private[client] def executeDeletePipeline(
    pipelineName: String,
    ifExists: Boolean
  ): ElasticResult[Boolean] =
    ElasticResult.success(false)

  override private[client] def executeGetPipeline(
    pipelineName: String
  ): ElasticResult[Option[String]] =
    ElasticResult.success(None)

  override private[client] def executeCreateComposableTemplate(
    templateName: String,
    templateDefinition: String
  ): ElasticResult[Boolean] =
    ElasticResult.success(false)

  override private[client] def executeDeleteComposableTemplate(
    templateName: String,
    ifExists: Boolean
  ): ElasticResult[Boolean] =
    ElasticResult.success(false)

  override private[client] def executeGetComposableTemplate(
    templateName: String
  ): ElasticResult[Option[String]] =
    ElasticResult.success(None)

  override private[client] def executeListComposableTemplates()
    : ElasticResult[Map[String, String]] =
    ElasticResult.success(Map.empty)

  override private[client] def executeComposableTemplateExists(
    templateName: String
  ): ElasticResult[Boolean] =
    ElasticResult.success(false)

  override private[client] def executeCreateLegacyTemplate(
    templateName: String,
    templateDefinition: String
  ): ElasticResult[Boolean] =
    ElasticResult.success(false)

  override private[client] def executeDeleteLegacyTemplate(
    templateName: String,
    ifExists: Boolean
  ): ElasticResult[Boolean] =
    ElasticResult.success(false)

  override private[client] def executeGetLegacyTemplate(
    templateName: String
  ): ElasticResult[Option[String]] =
    ElasticResult.success(None)

  override private[client] def executeListLegacyTemplates(): ElasticResult[Map[String, String]] =
    ElasticResult.success(Map.empty)

  override private[client] def executeLegacyTemplateExists(
    templateName: String
  ): ElasticResult[Boolean] =
    ElasticResult.success(false)

  override private[client] def executeDeleteByQuery(
    index: String,
    query: String,
    refresh: Boolean
  ): ElasticResult[Long] = ElasticSuccess(0L)

  override private[client] def executeIsIndexClosed(index: String): ElasticResult[Boolean] =
    ElasticSuccess(false)

  override private[client] def executeUpdateByQuery(
    index: String,
    query: String,
    pipelineId: Option[String],
    refresh: Boolean
  ): ElasticResult[Long] =
    ElasticSuccess(0L)

  override type BulkActionType = this.type

  override type BulkResultType = this.type

  override private[client] def toBulkAction(bulkItem: BulkItem): BulkActionType =
    throw new UnsupportedOperationException

  override private[client] implicit def toBulkElasticAction(a: BulkActionType): BulkElasticAction =
    throw new UnsupportedOperationException

  /** Basic flow for executing a bulk action. This method must be implemented by concrete classes
    * depending on the Elasticsearch version and client used.
    *
    * @param bulkOptions
    *   configuration options
    * @return
    *   Flow transforming bulk actions into results
    */
  override private[client] def bulkFlow(implicit
    bulkOptions: BulkOptions,
    system: ActorSystem
  ): Flow[Seq[BulkActionType], BulkResultType, NotUsed] =
    throw new UnsupportedOperationException

  /** Convert a BulkResultType into individual results. This method must extract the successes and
    * failures from the ES response.
    *
    * @param result
    *   raw result from the bulk
    * @return
    *   sequence of Right(id) for success or Left(failed) for failure
    */
  override private[client] def extractBulkResults(
    result: BulkResultType,
    originalBatch: Seq[BulkItem]
  ): Seq[Either[FailedDocument, SuccessfulDocument]] =
    throw new UnsupportedOperationException

  /** Conversion BulkActionType -> BulkItem */
  override private[client] def actionToBulkItem(action: BulkActionType): BulkItem =
    throw new UnsupportedOperationException

  override private[client] def executeGetAllMappings(): ElasticResult[Map[String, String]] =
    ElasticResult.success(Map.empty)

  override private[client] def executeCreateEnrichPolicy(
    policy: EnrichPolicy
  ): ElasticResult[Boolean] =
    ElasticSuccess(false)

  override private[client] def executeDeleteEnrichPolicy(
    policyName: String
  ): ElasticResult[Boolean] =
    ElasticSuccess(false)

  override private[client] def executeExecuteEnrichPolicy(
    policyName: String
  ): ElasticResult[EnrichPolicyTask] =
    ElasticSuccess(EnrichPolicyTask(policyName, "not_implemented", EnrichPolicyTaskStatus.Failed))

  override private[client] def executeCreateTransform(
    config: TransformConfig,
    start: Boolean
  ): ElasticResult[Boolean] =
    ElasticSuccess(false)

  override private[client] def executeDeleteTransform(
    transformId: String,
    force: Boolean
  ): ElasticResult[Boolean] =
    ElasticSuccess(false)

  override private[client] def executeStartTransform(transformId: String): ElasticResult[Boolean] =
    ElasticSuccess(false)

  override private[client] def executeStopTransform(
    transformId: String,
    force: Boolean,
    waitForCompletion: Boolean
  ): ElasticResult[Boolean] =
    ElasticSuccess(false)

  override private[client] def executeGetTransformStats(
    transformId: String
  ): ElasticResult[Option[TransformStats]] =
    ElasticSuccess(None)

  override private[client] def executeScheduleTransformNow(
    transformId: String
  ): ElasticResult[Boolean] =
    ElasticSuccess(false)

  override private[client] def executeCreateWatcher(
    watcher: Watcher,
    active: Boolean
  ): ElasticResult[Boolean] =
    ElasticSuccess(false)

  override private[client] def executeDeleteWatcher(id: String): ElasticResult[Boolean] =
    ElasticSuccess(false)

  override private[client] def executeGetWatcherStatus(
    id: String
  ): ElasticResult[Option[WatcherStatus]] =
    ElasticSuccess(None)

  override private[client] def executeLicenseInfo: ElasticResult[Option[String]] =
    ElasticSuccess(None)

  override private[client] def executeEnableBasicLicense(): ElasticResult[Boolean] =
    ElasticSuccess(false)

  override private[client] def executeEnableTrialLicense(): ElasticResult[Boolean] =
    ElasticSuccess(false)
}
