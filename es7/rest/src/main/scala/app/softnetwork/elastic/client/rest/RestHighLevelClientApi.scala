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

package app.softnetwork.elastic.client.rest

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import app.softnetwork.elastic.client._
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticResult, ElasticSuccess}
import app.softnetwork.elastic.client.scroll._
import app.softnetwork.elastic.sql.{schema, ObjectValue, PainlessContextType, Value}
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.query.{Asc, Criteria, Desc, SQLAggregation, SingleSearch}
import app.softnetwork.elastic.sql.schema.{
  AvgTransformAggregation,
  CardinalityTransformAggregation,
  CountTransformAggregation,
  Delay,
  EnrichPolicy,
  EnrichPolicyTask,
  EnrichPolicyTaskStatus,
  MaxTransformAggregation,
  MinTransformAggregation,
  SumTransformAggregation,
  TableAlias,
  TermsGroupBy,
  TopHitsTransformAggregation,
  TransformState,
  TransformTimeInterval,
  WatcherExecutionState
}
import app.softnetwork.elastic.sql.serialization._
import app.softnetwork.elastic.utils.CronIntervalCalculator
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.gson.JsonParser
import org.apache.http.util.EntityUtils
import org.elasticsearch.action.admin.indices.alias.Alias
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.flush.{FlushRequest, FlushResponse}
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest
import org.elasticsearch.action.admin.indices.refresh.{RefreshRequest, RefreshResponse}
import org.elasticsearch.action.admin.indices.settings.get.{GetSettingsRequest, GetSettingsResponse}
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.admin.indices.template.delete.DeleteIndexTemplateRequest
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.action.delete.{DeleteRequest, DeleteResponse}
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.action.ingest.{
  DeletePipelineRequest,
  GetPipelineRequest,
  GetPipelineResponse,
  PutPipelineRequest
}
import org.elasticsearch.action.search.{
  ClearScrollRequest,
  ClosePointInTimeRequest,
  MultiSearchRequest,
  MultiSearchResponse,
  OpenPointInTimeRequest,
  SearchRequest,
  SearchResponse,
  SearchScrollRequest
}
import org.elasticsearch.action.support.WriteRequest
import org.elasticsearch.action.support.master.AcknowledgedResponse
import org.elasticsearch.action.update.{UpdateRequest, UpdateResponse}
import org.elasticsearch.action.{ActionListener, DocWriteRequest, DocWriteResponse}
import org.elasticsearch.client.{GetAliasesResponse, Request, RequestOptions, Response}
import org.elasticsearch.client.core.{CountRequest, CountResponse}
import org.elasticsearch.client.enrich.{DeletePolicyRequest, ExecutePolicyRequest, PutPolicyRequest}
import org.elasticsearch.client.indices.{
  CloseIndexRequest,
  ComposableIndexTemplateExistRequest,
  CreateIndexRequest,
  DeleteComposableIndexTemplateRequest,
  GetComposableIndexTemplateRequest,
  GetIndexRequest,
  GetIndexTemplatesRequest,
  GetIndexTemplatesResponse,
  GetMappingsRequest,
  IndexTemplateMetadata,
  IndexTemplatesExistRequest,
  PutComposableIndexTemplateRequest,
  PutIndexTemplateRequest,
  PutMappingRequest
}
import org.elasticsearch.client.license.{
  GetLicenseRequest,
  GetLicenseResponse,
  StartBasicRequest,
  StartBasicResponse,
  StartTrialRequest,
  StartTrialResponse
}
import org.elasticsearch.client.transform.transforms.latest.LatestConfig
import org.elasticsearch.client.transform.{
  DeleteTransformRequest,
  GetTransformStatsRequest,
  PutTransformRequest,
  StartTransformRequest,
  StopTransformRequest
}
import org.elasticsearch.client.transform.transforms.pivot.{
  AggregationConfig,
  GroupConfig,
  PivotConfig,
  TermsGroupSource
}
import org.elasticsearch.client.transform.transforms.{
  DestConfig,
  QueryConfig,
  SourceConfig,
  TimeSyncConfig,
  TransformConfig
}
import org.elasticsearch.client.watcher.{
  DeleteWatchRequest,
  DeleteWatchResponse,
  GetWatchRequest,
  GetWatchResponse,
  PutWatchRequest,
  PutWatchResponse
}
import org.elasticsearch.cluster.metadata.{AliasMetadata, ComposableIndexTemplate}
import org.elasticsearch.common.Strings
import org.elasticsearch.common.bytes.BytesArray
import org.elasticsearch.core.TimeValue
import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import org.elasticsearch.xcontent.{DeprecationHandler, ToXContent, XContentFactory, XContentType}
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.aggregations.AggregatorFactories
import org.elasticsearch.search.aggregations.metrics.{
  AvgAggregationBuilder,
  CardinalityAggregationBuilder,
  MaxAggregationBuilder,
  MinAggregationBuilder,
  SumAggregationBuilder,
  TopHitsAggregationBuilder,
  ValueCountAggregationBuilder
}
import org.elasticsearch.search.aggregations.pipeline.BucketSelectorPipelineAggregationBuilder
import org.elasticsearch.search.builder.{PointInTimeBuilder, SearchSourceBuilder}
import org.elasticsearch.search.sort.{FieldSortBuilder, SortOrder}
import org.json4s.jackson.JsonMethods
import org.json4s.DefaultFormats

import java.io.IOException
import java.time.ZonedDateTime
import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

trait RestHighLevelClientApi
    extends ElasticClientApi
    with RestHighLevelClientIndicesApi
    with RestHighLevelClientAliasApi
    with RestHighLevelClientSettingsApi
    with RestHighLevelClientMappingApi
    with RestHighLevelClientRefreshApi
    with RestHighLevelClientFlushApi
    with RestHighLevelClientCountApi
    with RestHighLevelClientIndexApi
    with RestHighLevelClientUpdateApi
    with RestHighLevelClientDeleteApi
    with RestHighLevelClientGetApi
    with RestHighLevelClientSearchApi
    with RestHighLevelClientBulkApi
    with RestHighLevelClientScrollApi
    with RestHighLevelClientCompanion
    with RestHighLevelClientVersionApi
    with RestHighLevelClientPipelineApi
    with RestHighLevelClientTemplateApi
    with RestHighLevelClientEnrichPolicyApi
    with RestHighLevelClientTransformApi
    with RestHighLevelClientWatcherApi
    with RestHighLevelClientLicenseApi

/** Version API implementation for RestHighLevelClient
  * @see
  *   [[VersionApi]] for generic API documentation
  */
trait RestHighLevelClientVersionApi extends VersionApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientCompanion =>

  override private[client] def executeVersion(): ElasticResult[String] =
    executeRestLowLevelAction[String](
      operation = "version",
      index = None,
      retryable = true
    )(
      request = new Request("GET", "/")
    )(
      transformer = resp => {
        val jsonString = EntityUtils.toString(resp.getEntity)
        implicit val formats: DefaultFormats.type = DefaultFormats
        val json = JsonMethods.parse(jsonString)
        (json \ "version" \ "number").extract[String]
      }
    )

}

/** Indices management API for RestHighLevelClient
  * @see
  *   [[IndicesApi]] for generic API documentation
  */
trait RestHighLevelClientIndicesApi extends IndicesApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientRefreshApi
    with RestHighLevelClientPipelineApi
    with RestHighLevelClientScrollApi
    with RestHighLevelClientBulkApi
    with RestHighLevelClientVersionApi
    with RestHighLevelClientTemplateApi
    with RestHighLevelClientCompanion =>

  override private[client] def executeCreateIndex(
    index: String,
    settings: String,
    mappings: Option[String],
    aliases: Seq[TableAlias]
  ): ElasticResult[Boolean] = {
    executeRestBooleanAction[CreateIndexRequest, AcknowledgedResponse](
      operation = "createIndex",
      index = Some(index),
      retryable = false
    )(
      request = {
        val req = new CreateIndexRequest(index)
          .settings(settings, XContentType.JSON)
          .aliases(
            aliases
              .map(alias => {
                var a =
                  new Alias(alias.alias).writeIndex(alias.isWriteIndex).isHidden(alias.isHidden)
                if (alias.filter.nonEmpty) {
                  val filterNode = Value(alias.filter).asInstanceOf[ObjectValue].toJson
                  a = a.filter(filterNode.toString)
                }
                alias.indexRouting.foreach(ir => a = a.indexRouting(ir))
                alias.searchRouting.foreach(sr => a = a.searchRouting(sr))
                a
              })
              .asJava
          )
        mappings match {
          case Some(m) if m.trim.startsWith("{") && m.trim.endsWith("}") =>
            req.mapping(m, XContentType.JSON)
          case _ => req
        }
      }
    )(
      executor = req => apply().indices().create(req, RequestOptions.DEFAULT)
    )
  }

  override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] = {
    executeRestAction[Request, Response, Option[String]](
      operation = "getIndex",
      index = Some(index),
      retryable = true
    )(
      request = new Request("GET", s"/$index")
    )(
      executor = req => apply().getLowLevelClient.performRequest(req)
    )(resp => {
      resp.getStatusLine match {
        case statusLine if statusLine.getStatusCode >= 400 =>
          None
        case _ =>
          val json = scala.io.Source.fromInputStream(resp.getEntity.getContent).mkString
          Some(json)
      }
    })
  }

  override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
    executeRestBooleanAction[DeleteIndexRequest, AcknowledgedResponse](
      operation = "deleteIndex",
      index = Some(index),
      retryable = false
    )(
      request = new DeleteIndexRequest(index)
    )(
      executor = req => apply().indices().delete(req, RequestOptions.DEFAULT)
    )

  override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] =
    executeRestBooleanAction[CloseIndexRequest, AcknowledgedResponse](
      operation = "closeIndex",
      index = Some(index),
      retryable = false
    )(
      request = new CloseIndexRequest(index)
    )(
      executor = req => apply().indices().close(req, RequestOptions.DEFAULT)
    )

  override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] =
    executeRestBooleanAction[OpenIndexRequest, AcknowledgedResponse](
      operation = "openIndex",
      index = Some(index),
      retryable = false
    )(
      request = new OpenIndexRequest(index)
    )(
      executor = req => apply().indices().open(req, RequestOptions.DEFAULT)
    )

  override private[client] def executeReindex(
    sourceIndex: String,
    targetIndex: String,
    refresh: Boolean,
    pipeline: Option[String]
  ): ElasticResult[(Boolean, Option[Long])] =
    executeRestAction[Request, org.elasticsearch.client.Response, (Boolean, Option[Long])](
      operation = "reindex",
      index = Some(s"$sourceIndex->$targetIndex"),
      retryable = false
    )(
      request = {
        val req = new Request("POST", s"/_reindex?refresh=$refresh")
        req.setJsonEntity(
          s"""
             |{
             |  "source": {
             |    "index": "$sourceIndex"
             |  },
             |  "dest": {
             |    "index": "$targetIndex"
             |  }
             |}
           """.stripMargin
        )
        req
      }
    )(
      executor = req => apply().getLowLevelClient.performRequest(req)
    )(resp => {
      resp.getStatusLine match {
        case statusLine if statusLine.getStatusCode >= 400 =>
          (false, None)
        case _ =>
          val json = JsonParser
            .parseString(
              scala.io.Source.fromInputStream(resp.getEntity.getContent).mkString
            )
            .getAsJsonObject
          if (json.has("failures") && json.get("failures").getAsJsonArray.size() > 0) {
            (false, None)
          } else {
            (true, Some(json.get("created").getAsLong))
          }
      }
    })

  override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
    executeRestAction[GetIndexRequest, Boolean, Boolean](
      operation = "indexExists",
      index = Some(index),
      retryable = false
    )(
      request = new GetIndexRequest(index)
    )(
      executor = req => apply().indices().exists(req, RequestOptions.DEFAULT)
    )(
      identity
    )

  override private[client] def executeDeleteByQuery(
    index: String,
    jsonQuery: String,
    refresh: Boolean
  ): ElasticResult[Long] =
    executeRestAction[Request, Response, Long](
      operation = "deleteByQuery",
      index = Some(index),
      retryable = true
    )(
      request = {
        val req = new Request(
          "POST",
          s"/$index/_delete_by_query?refresh=$refresh&conflicts=proceed"
        )
        req.setJsonEntity(jsonQuery)
        req
      }
    )(
      executor = req => apply().getLowLevelClient.performRequest(req)
    )(
      transformer = resp => {
        val json = JsonParser
          .parseString(scala.io.Source.fromInputStream(resp.getEntity.getContent).mkString)
          .getAsJsonObject

        // ES6/ES7 return "deleted"
        val deleted =
          if (json.has("deleted")) json.get("deleted").getAsLong
          else 0L

        deleted
      }
    )

  override private[client] def executeIsIndexClosed(index: String): ElasticResult[Boolean] =
    executeRestAction[Request, Response, Boolean](
      operation = "isIndexClosed",
      index = Some(index),
      retryable = true
    )(
      request = {
        val req = new Request("GET", s"/_cat/indices/$index?format=json")
        req
      }
    )(
      executor = req => apply().getLowLevelClient.performRequest(req)
    )(
      transformer = resp => {
        val json = JsonParser
          .parseString(scala.io.Source.fromInputStream(resp.getEntity.getContent).mkString)
          .getAsJsonArray

        if (json.size() == 0)
          false
        else {
          val entry = json.get(0).getAsJsonObject
          val status = entry.get("status").getAsString // "open" or "close"
          status == "close"
        }
      }
    )

  override private[client] def executeUpdateByQuery(
    index: String,
    jsonQuery: String,
    pipelineId: Option[String],
    refresh: Boolean
  ): ElasticResult[Long] = {

    executeRestAction[Request, Response, Long](
      operation = "updateByQuery",
      index = Some(index),
      retryable = true
    )(
      request = {
        val req = new Request(
          "POST",
          s"/$index/_update_by_query?refresh=$refresh" +
          pipelineId.map(id => s"&pipeline=$id").getOrElse("")
        )
        req.setJsonEntity(jsonQuery)
        req
      }
    )(
      executor = req => apply().getLowLevelClient.performRequest(req)
    )(
      transformer = resp => {
        val json = JsonParser
          .parseString(
            scala.io.Source.fromInputStream(resp.getEntity.getContent).mkString
          )
          .getAsJsonObject
        json.get("updated").getAsLong
      }
    )
  }
}

/** Alias management API for RestHighLevelClient
  * @see
  *   [[AliasApi]] for generic API documentation
  */
trait RestHighLevelClientAliasApi extends AliasApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientIndicesApi with RestHighLevelClientCompanion =>

  override private[client] def executeAddAlias(
    alias: TableAlias
  ): ElasticResult[Boolean] =
    executeRestBooleanAction(
      operation = "addAlias",
      index = Some(alias.table),
      retryable = false
    )(
      request = {
        val aliasAction = new AliasActions(AliasActions.Type.ADD)
          .index(alias.table)
          .alias(alias.alias)
        if (alias.isWriteIndex) {
          aliasAction.writeIndex(true)
        }
        if (alias.filter.nonEmpty) {
          val filterNode = Value(alias.filter).asInstanceOf[ObjectValue].toJson
          aliasAction.filter(filterNode.toString)
        }
        alias.routing.foreach(aliasAction.routing)
        alias.indexRouting.foreach(aliasAction.indexRouting)
        alias.searchRouting.foreach(aliasAction.searchRouting)
        new IndicesAliasesRequest().addAliasAction(aliasAction)
      }
    )(
      executor = req => apply().indices().updateAliases(req, RequestOptions.DEFAULT)
    )

  override private[client] def executeRemoveAlias(
    index: String,
    alias: String
  ): ElasticResult[Boolean] =
    executeRestBooleanAction(
      operation = "removeAlias",
      index = Some(index),
      retryable = false
    )(
      request = new IndicesAliasesRequest()
        .addAliasAction(
          new AliasActions(AliasActions.Type.REMOVE)
            .index(index)
            .alias(alias)
        )
    )(
      executor = req => apply().indices().updateAliases(req, RequestOptions.DEFAULT)
    )

  override private[client] def executeAliasExists(alias: String): ElasticResult[Boolean] =
    executeRestAction[GetAliasesRequest, GetAliasesResponse, Boolean](
      operation = "aliasExists",
      index = Some(alias),
      retryable = true
    )(
      request = new GetAliasesRequest().aliases(alias)
    )(
      executor = req => apply().indices().getAlias(req, RequestOptions.DEFAULT)
    )(response => !response.getAliases.isEmpty)

  override private[client] def executeGetAliases(index: String): ElasticResult[String] =
    executeRestAction[GetAliasesRequest, GetAliasesResponse, String](
      operation = "getAliases",
      index = Some(index),
      retryable = true
    )(
      request = new GetAliasesRequest().indices(index)
    )(
      executor = req => apply().indices().getAlias(req, RequestOptions.DEFAULT)
    )(response => Strings.toString(response))

  override private[client] def executeSwapAlias(
    oldIndex: String,
    newIndex: String,
    alias: String
  ): ElasticResult[Boolean] =
    executeRestBooleanAction(
      operation = "swapAlias",
      index = Some(s"$oldIndex -> $newIndex"),
      retryable = false
    )(
      request = new IndicesAliasesRequest()
        .addAliasAction(
          new AliasActions(AliasActions.Type.REMOVE)
            .index(oldIndex)
            .alias(alias)
        )
        .addAliasAction(
          new AliasActions(AliasActions.Type.ADD)
            .index(newIndex)
            .alias(alias)
        )
    )(
      executor = req => apply().indices().updateAliases(req, RequestOptions.DEFAULT)
    )
}

/** Settings management API for RestHighLevelClient
  * @see
  *   [[SettingsApi]] for generic API documentation
  */
trait RestHighLevelClientSettingsApi extends SettingsApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientIndicesApi with RestHighLevelClientCompanion =>

  override private[client] def executeUpdateSettings(
    index: String,
    settings: String
  ): ElasticResult[Boolean] =
    executeRestBooleanAction(
      operation = "updateSettings",
      index = Some(index),
      retryable = false
    )(
      request = new UpdateSettingsRequest(index)
        .settings(settings, XContentType.JSON)
    )(
      executor = req => apply().indices().putSettings(req, RequestOptions.DEFAULT)
    )

  override private[client] def executeLoadSettings(index: String): ElasticResult[String] =
    executeRestAction[GetSettingsRequest, GetSettingsResponse, String](
      operation = "loadSettings",
      index = Some(index),
      retryable = true
    )(
      request = new GetSettingsRequest().indices(index)
    )(
      executor = req => apply().indices().getSettings(req, RequestOptions.DEFAULT)
    )(response => response.toString)

}

/** Mapping API implementation for RestHighLevelClient
  * @see
  *   [[MappingApi]] for generic API documentation
  */
trait RestHighLevelClientMappingApi extends MappingApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientSettingsApi
    with RestHighLevelClientIndicesApi
    with RestHighLevelClientRefreshApi
    with RestHighLevelClientVersionApi
    with RestHighLevelClientAliasApi
    with RestHighLevelClientCompanion =>

  override private[client] def executeSetMapping(
    index: String,
    mapping: String
  ): ElasticResult[Boolean] =
    executeRestBooleanAction(
      operation = "setMapping",
      index = Some(index),
      retryable = false
    )(
      request = new PutMappingRequest(index)
        .source(mapping, XContentType.JSON)
    )(
      executor = req => apply().indices().putMapping(req, RequestOptions.DEFAULT)
    )

  override private[client] def executeGetMapping(index: String): ElasticResult[String] =
    executeRestAction[
      GetMappingsRequest,
      org.elasticsearch.client.indices.GetMappingsResponse,
      String
    ](
      operation = "getMapping",
      index = Some(index),
      retryable = true
    )(
      request = new GetMappingsRequest().indices(index)
    )(
      executor = req => apply().indices().getMapping(req, RequestOptions.DEFAULT)
    )(response => {
      val mappings = response.mappings().asScala.get(index)
      mappings match {
        case Some(metadata) => metadata.source().toString
        case None           => s"""{"properties": {}}"""
      }
    })

  override private[client] def executeGetAllMappings(): ElasticResult[Map[String, String]] =
    executeRestAction[
      GetMappingsRequest,
      org.elasticsearch.client.indices.GetMappingsResponse,
      Map[String, String]
    ](
      operation = "getAllMappings",
      index = None,
      retryable = true
    )(
      request = new GetMappingsRequest().indices()
    )(
      executor = req => apply().indices().getMapping(req, RequestOptions.DEFAULT)
    )(response => {
      response
        .mappings()
        .asScala
        .map { case (index, mappings) =>
          (index, mappings.source().toString)
        }
        .toMap
    })
}

/** Refresh API implementation for RestHighLevelClient
  * @see
  *   [[RefreshApi]] for generic API documentation
  */
trait RestHighLevelClientRefreshApi extends RefreshApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientCompanion =>

  override private[client] def executeRefresh(index: String): ElasticResult[Boolean] =
    executeRestAction[RefreshRequest, RefreshResponse, Boolean](
      operation = "refresh",
      index = Some(index),
      retryable = true
    )(
      request = new RefreshRequest(index)
    )(
      executor = req => apply().indices().refresh(req, RequestOptions.DEFAULT)
    )(response => response.getStatus.getStatus < 400)

}

/** Flush API implementation for RestHighLevelClient
  * @see
  *   [[FlushApi]] for generic API documentation
  */
trait RestHighLevelClientFlushApi extends FlushApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientCompanion =>
  override private[client] def executeFlush(
    index: String,
    force: Boolean,
    wait: Boolean
  ): ElasticResult[Boolean] =
    executeRestAction[FlushRequest, FlushResponse, Boolean](
      operation = "flush",
      index = Some(index),
      retryable = true
    )(
      request = new FlushRequest(index).force(force).waitIfOngoing(wait)
    )(
      executor = req => apply().indices().flush(req, RequestOptions.DEFAULT)
    )(response => response.getStatus == RestStatus.OK)

}

/** Count API implementation for RestHighLevelClient
  * @see
  *   [[CountApi]] for generic API documentation
  */
trait RestHighLevelClientCountApi extends CountApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientCompanion =>
  override private[client] def executeCount(
    query: ElasticQuery
  ): ElasticResult[Option[Double]] =
    executeRestAction[CountRequest, CountResponse, Option[Double]](
      operation = "count",
      index = Some(query.indices.mkString(",")),
      retryable = true
    )(
      request = new CountRequest().indices(query.indices: _*).types(query.types: _*)
    )(
      executor = req => apply().count(req, RequestOptions.DEFAULT)
    )(response => Option(response.getCount.toDouble))

  override private[client] def executeCountAsync(
    query: ElasticQuery
  )(implicit ec: ExecutionContext): Future[ElasticResult[Option[Double]]] = {
    executeAsyncRestAction[CountRequest, CountResponse, Option[Double]](
      operation = "countAsync",
      index = Some(query.indices.mkString(",")),
      retryable = true
    )(
      request = new CountRequest().indices(query.indices: _*).types(query.types: _*)
    )(
      executor = (req, listener) => apply().countAsync(req, RequestOptions.DEFAULT, listener)
    )(response => Option(response.getCount.toDouble))
  }

}

/** Index API implementation for RestHighLevelClient
  * @see
  *   [[IndexApi]] for generic API documentation
  */
trait RestHighLevelClientIndexApi extends IndexApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientSettingsApi with RestHighLevelClientCompanion =>
  override private[client] def executeIndex(
    index: String,
    id: String,
    source: String,
    wait: Boolean
  ): ElasticResult[Boolean] =
    executeRestAction[IndexRequest, IndexResponse, Boolean](
      operation = "index",
      index = Some(index),
      retryable = false
    )(request =
      new IndexRequest(index)
        .id(id)
        .source(source, XContentType.JSON)
        .setRefreshPolicy(
          if (wait) WriteRequest.RefreshPolicy.WAIT_UNTIL else WriteRequest.RefreshPolicy.NONE
        )
    )(
      executor = req => apply().index(req, RequestOptions.DEFAULT)
    )(
      transformer = resp =>
        resp.getResult match {
          case DocWriteResponse.Result.CREATED | DocWriteResponse.Result.UPDATED |
              DocWriteResponse.Result.NOOP =>
            true
          case _ => false
        }
    )

  override private[client] def executeIndexAsync(
    index: String,
    id: String,
    source: String,
    wait: Boolean
  )(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Boolean]] =
    executeAsyncRestAction[IndexRequest, IndexResponse, Boolean](
      operation = "indexAsync",
      index = Some(index),
      retryable = false
    )(
      request = new IndexRequest(index)
        .id(id)
        .source(source, XContentType.JSON)
        .setRefreshPolicy(
          if (wait) WriteRequest.RefreshPolicy.WAIT_UNTIL else WriteRequest.RefreshPolicy.NONE
        )
    )(
      executor = (req, listener) => apply().indexAsync(req, RequestOptions.DEFAULT, listener)
    )(
      transformer = resp =>
        resp.getResult match {
          case DocWriteResponse.Result.CREATED | DocWriteResponse.Result.UPDATED |
              DocWriteResponse.Result.NOOP =>
            true
          case _ => false
        }
    )

}

/** Update API implementation for RestHighLevelClient
  * @see
  *   [[UpdateApi]] for generic API documentation
  */
trait RestHighLevelClientUpdateApi extends UpdateApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientSettingsApi with RestHighLevelClientCompanion =>
  override private[client] def executeUpdate(
    index: String,
    id: String,
    source: String,
    upsert: Boolean,
    wait: Boolean
  ): ElasticResult[Boolean] =
    executeRestAction[UpdateRequest, UpdateResponse, Boolean](
      operation = "update",
      index = Some(index),
      retryable = false
    )(
      request = new UpdateRequest(index, id)
        .doc(source, XContentType.JSON)
        .docAsUpsert(upsert)
        .setRefreshPolicy(
          if (wait) WriteRequest.RefreshPolicy.WAIT_UNTIL else WriteRequest.RefreshPolicy.NONE
        )
    )(
      executor = req => apply().update(req, RequestOptions.DEFAULT)
    )(
      transformer = resp =>
        resp.getResult match {
          case DocWriteResponse.Result.CREATED | DocWriteResponse.Result.UPDATED |
              DocWriteResponse.Result.NOOP =>
            true
          case DocWriteResponse.Result.NOT_FOUND =>
            throw new IOException(
              s"Document with id: $id not found in index: $index"
            ) // if upsert is false
          case _ => false
        }
    )

  override private[client] def executeUpdateAsync(
    index: String,
    id: String,
    source: String,
    upsert: Boolean,
    wait: Boolean
  )(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]] =
    executeAsyncRestAction[UpdateRequest, UpdateResponse, Boolean](
      operation = "updateAsync",
      index = Some(index),
      retryable = false
    )(
      request = new UpdateRequest(index, id)
        .doc(source, XContentType.JSON)
        .docAsUpsert(upsert)
        .setRefreshPolicy(
          if (wait) WriteRequest.RefreshPolicy.WAIT_UNTIL else WriteRequest.RefreshPolicy.NONE
        )
    )(
      executor = (req, listener) => apply().updateAsync(req, RequestOptions.DEFAULT, listener)
    )(
      transformer = resp =>
        resp.getResult match {
          case DocWriteResponse.Result.CREATED | DocWriteResponse.Result.UPDATED |
              DocWriteResponse.Result.NOOP =>
            true
          case DocWriteResponse.Result.NOT_FOUND =>
            throw new IOException(
              s"Document with id: $id not found in index: $index"
            ) // if upsert is false
          case _ => false
        }
    )

}

/** Delete API implementation for RestHighLevelClient
  * @see
  *   [[DeleteApi]] for generic API documentation
  */
trait RestHighLevelClientDeleteApi extends DeleteApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientSettingsApi with RestHighLevelClientCompanion =>

  override private[client] def executeDelete(
    index: String,
    id: String,
    wait: Boolean
  ): ElasticResult[Boolean] =
    executeRestAction[DeleteRequest, DeleteResponse, Boolean](
      operation = "delete",
      index = Some(index),
      retryable = false
    )(
      request = new DeleteRequest(index, id)
        .setRefreshPolicy(
          if (wait) WriteRequest.RefreshPolicy.WAIT_UNTIL else WriteRequest.RefreshPolicy.NONE
        )
    )(
      executor = req => apply().delete(req, RequestOptions.DEFAULT)
    )(
      transformer = resp =>
        resp.getResult match {
          case DocWriteResponse.Result.DELETED | DocWriteResponse.Result.NOOP => true
          case _                                                              => false
        }
    )

  override private[client] def executeDeleteAsync(index: String, id: String, wait: Boolean)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Boolean]] =
    executeAsyncRestAction[DeleteRequest, DeleteResponse, Boolean](
      operation = "deleteAsync",
      index = Some(index),
      retryable = false
    )(
      request = new DeleteRequest(index, id)
        .setRefreshPolicy(
          if (wait) WriteRequest.RefreshPolicy.WAIT_UNTIL else WriteRequest.RefreshPolicy.NONE
        )
    )(
      executor = (req, listener) => apply().deleteAsync(req, RequestOptions.DEFAULT, listener)
    )(
      transformer = resp =>
        resp.getResult match {
          case DocWriteResponse.Result.DELETED | DocWriteResponse.Result.NOOP => true
          case _                                                              => false
        }
    )

}

/** Get API implementation for RestHighLevelClient
  * @see
  *   [[GetApi]] for generic API documentation
  */
trait RestHighLevelClientGetApi extends GetApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientCompanion =>
  override private[client] def executeGet(
    index: String,
    id: String
  ): ElasticResult[Option[String]] =
    executeRestAction[GetRequest, GetResponse, Option[String]](
      operation = "get",
      index = Some(index),
      retryable = true
    )(
      request = new GetRequest(index, id)
    )(
      executor = req => apply().get(req, RequestOptions.DEFAULT)
    )(response => {
      if (response.isExists) {
        Some(response.getSourceAsString)
      } else {
        None
      }
    })

  override private[client] def executeGetAsync(index: String, id: String)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[String]]] =
    executeAsyncRestAction[GetRequest, GetResponse, Option[String]](
      operation = "getAsync",
      index = Some(index),
      retryable = true
    )(
      request = new GetRequest(index, id)
    )(
      executor = (req, listener) => apply().getAsync(req, RequestOptions.DEFAULT, listener)
    )(response => {
      if (response.isExists) {
        Some(response.getSourceAsString)
      } else {
        None
      }
    })

}

/** Search API implementation for RestHighLevelClient
  * @see
  *   [[SearchApi]] for generic API documentation
  */
trait RestHighLevelClientSearchApi extends SearchApi with RestHighLevelClientHelpers {
  _: ElasticConversion with RestHighLevelClientCompanion =>

  override implicit def singleSearchToJsonQuery(singleSearch: SingleSearch)(implicit
    timestamp: Long,
    contextType: PainlessContextType = PainlessContextType.Query
  ): String =
    implicitly[ElasticSearchRequest](singleSearch).query

  override private[client] def executeSingleSearch(
    elasticQuery: ElasticQuery
  ): ElasticResult[Option[String]] =
    executeRestAction[SearchRequest, SearchResponse, Option[String]](
      operation = "singleSearch",
      index = Some(elasticQuery.indices.mkString(",")),
      retryable = true
    )(
      request = {
        val req = new SearchRequest(elasticQuery.indices: _*).types(elasticQuery.types: _*)
        val xContentParser = XContentType.JSON
          .xContent()
          .createParser(
            namedXContentRegistry,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            elasticQuery.query
          )
        req.source(SearchSourceBuilder.fromXContent(xContentParser))
        req
      }
    )(
      executor = req => apply().search(req, RequestOptions.DEFAULT)
    )(response => {
      if (response.status() == RestStatus.OK) {
        Some(Strings.toString(response))
      } else {
        None
      }
    })

  override private[client] def executeMultiSearch(
    elasticQueries: ElasticQueries
  ): ElasticResult[Option[String]] =
    executeRestAction[MultiSearchRequest, MultiSearchResponse, Option[String]](
      operation = "multiSearch",
      index = Some(
        elasticQueries.queries
          .flatMap(_.indices)
          .distinct
          .mkString(",")
      ),
      retryable = true
    )(
      request = {
        val req = new MultiSearchRequest()
        for (query <- elasticQueries.queries) {
          val xContentParser = XContentType.JSON
            .xContent()
            .createParser(
              namedXContentRegistry,
              DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
              query.query
            )
          val searchSourceBuilder = SearchSourceBuilder.fromXContent(xContentParser)
          req.add(
            new SearchRequest(query.indices: _*)
              .types(query.types: _*)
              .source(searchSourceBuilder)
          )
        }
        req
      }
    )(
      executor = req => apply().msearch(req, RequestOptions.DEFAULT)
    )(response => Some(Strings.toString(response)))

  override private[client] def executeSingleSearchAsync(
    elasticQuery: ElasticQuery
  )(implicit ec: ExecutionContext): Future[ElasticResult[Option[String]]] =
    executeAsyncRestAction[SearchRequest, SearchResponse, Option[String]](
      operation = "executeSingleSearchAsync",
      index = Some(elasticQuery.indices.mkString(",")),
      retryable = true
    )(
      request = {
        val req = new SearchRequest(elasticQuery.indices: _*).types(elasticQuery.types: _*)
        val xContentParser = XContentType.JSON
          .xContent()
          .createParser(
            namedXContentRegistry,
            DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
            elasticQuery.query
          )
        req.source(SearchSourceBuilder.fromXContent(xContentParser))
        req
      }
    )(
      executor = (req, listener) => apply().searchAsync(req, RequestOptions.DEFAULT, listener)
    )(response => {
      if (response.status() == RestStatus.OK) {
        Some(Strings.toString(response))
      } else {
        None
      }
    })

  override private[client] def executeMultiSearchAsync(
    elasticQueries: ElasticQueries
  )(implicit ec: ExecutionContext): Future[ElasticResult[Option[String]]] =
    executeAsyncRestAction[MultiSearchRequest, MultiSearchResponse, Option[String]](
      operation = "executeMultiSearchAsync",
      index = Some(
        elasticQueries.queries
          .flatMap(_.indices)
          .distinct
          .mkString(",")
      ),
      retryable = true
    )(
      request = {
        val req = new MultiSearchRequest()
        for (query <- elasticQueries.queries) {
          val xContentParser = XContentType.JSON
            .xContent()
            .createParser(
              namedXContentRegistry,
              DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
              query.query
            )
          val searchSourceBuilder = SearchSourceBuilder.fromXContent(xContentParser)
          req.add(
            new SearchRequest(query.indices: _*)
              .types(query.types: _*)
              .source(searchSourceBuilder)
          )
        }
        req
      }
    )(
      executor = (req, listener) => apply().msearchAsync(req, RequestOptions.DEFAULT, listener)
    )(response => Some(Strings.toString(response)))

}

/** Bulk API implementation for RestHighLevelClient
  * @see
  *   [[BulkApi]] for generic API documentation
  */
trait RestHighLevelClientBulkApi extends BulkApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientRefreshApi
    with RestHighLevelClientSettingsApi
    with RestHighLevelClientIndexApi
    with RestHighLevelClientCompanion =>

  override type BulkActionType = DocWriteRequest[_]
  override type BulkResultType = BulkResponse

  override implicit def toBulkElasticAction(a: BulkActionType): BulkElasticAction = {
    new BulkElasticAction {
      override def index: String = a.index
    }
  }

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
  ): Flow[Seq[BulkActionType], BulkResultType, NotUsed] = {
    val parallelism = Math.max(1, bulkOptions.balance)
    Flow[Seq[BulkActionType]]
      .named("bulk")
      .mapAsyncUnordered[R](parallelism) { items =>
        val request = new BulkRequest(bulkOptions.defaultIndex)
        items.foreach(request.add)
        val promise: Promise[R] = Promise[R]()
        apply().bulkAsync(
          request,
          RequestOptions.DEFAULT,
          new ActionListener[BulkResponse] {
            override def onResponse(response: BulkResponse): Unit = {
              if (response.hasFailures) {
                logger.error(s"Bulk operation failed: ${response.buildFailureMessage()}")
              } else {
                logger.info(s"Bulk operation succeeded with ${response.getItems.length} items.")
              }
              promise.success(response)
            }

            override def onFailure(e: Exception): Unit = {
              logger.error("Bulk operation failed", e)
              promise.failure(e)
            }
          }
        )
        promise.future
      }
  }

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
  ): Seq[Either[FailedDocument, SuccessfulDocument]] = {
    // no results at all
    if (
      originalBatch.nonEmpty &&
      (result == null || (result.getItems == null || result.getItems.isEmpty))
    ) {
      logger.error("Bulk result is null or has no items")
      return originalBatch.map { item =>
        Left(
          FailedDocument(
            id = item.id.getOrElse("unknown"),
            index = item.index,
            document = item.document,
            error = BulkError(
              message = "Null bulk result",
              `type` = "internal_error",
              status = 500
            ),
            retryable = false
          )
        )
      }
    }

    // process failed items
    val failedItems = result.getItems.filter(_.isFailed).map { item =>
      val failure = item.getFailure
      val statusCode = item.status().getStatus
      val errorType = Option(failure.getType).getOrElse("unknown")
      val errorReason = Option(failure.getMessage).getOrElse("Unknown error")

      val itemId = item.getId
      val itemIndex = item.getIndex

      val originalItemOpt = originalBatch
        .find(o => o.id.contains(itemId) && o.index == itemIndex)

      // Determine if the error is retryable
      val isRetryable = originalItemOpt.isDefined && (BulkErrorAnalyzer.isRetryable(statusCode) ||
      BulkErrorAnalyzer.isRetryableByType(errorType))

      val originalItem = originalItemOpt.getOrElse(
        BulkItem(
          index = itemIndex,
          id = Some(itemId),
          document = "",
          parent = None,
          action = item.getOpType match {
            case DocWriteRequest.OpType.INDEX  => BulkAction.INDEX
            case DocWriteRequest.OpType.CREATE => BulkAction.INDEX
            case DocWriteRequest.OpType.UPDATE => BulkAction.UPDATE
            case DocWriteRequest.OpType.DELETE => BulkAction.DELETE
          }
        )
      )

      Left(
        FailedDocument(
          id = originalItem.id.getOrElse("unknown"),
          index = originalItem.index,
          document = originalItem.document,
          error = BulkError(
            message = errorReason,
            `type` = errorType,
            status = statusCode
          ),
          retryable = isRetryable
        )
      )
    }

    // process successful items
    val successfulItems =
      result.getItems.filterNot(_.isFailed).map { item =>
        Right(SuccessfulDocument(id = item.getId, index = item.getIndex))
      }

    val results = failedItems ++ successfulItems

    // if no individual results but overall failure, mark all as failed
    if (results.isEmpty && originalBatch.nonEmpty) {
      val statusCode = result.status().getStatus
      val errorString = result.buildFailureMessage()
      logger.error(s"Bulk operation completed with errors: $errorString")
      val bulkError =
        BulkError(
          message = errorString,
          `type` = "unknown",
          status = statusCode
        )
      return originalBatch.map { item =>
        Left(
          FailedDocument(
            id = item.id.getOrElse("unknown"),
            index = item.index,
            document = item.document,
            error = bulkError,
            retryable = BulkErrorAnalyzer.isRetryable(statusCode)
          )
        )
      }
    }

    results
  }

  override def toBulkAction(bulkItem: BulkItem): A = {
    import bulkItem._
    val request = action match {
      case BulkAction.UPDATE =>
        new UpdateRequest(bulkItem.index, id.orNull)
          .doc(document, XContentType.JSON)
          .docAsUpsert(true)
      case BulkAction.DELETE =>
        new DeleteRequest(bulkItem.index).id(id.getOrElse("_all"))
      case _ =>
        new IndexRequest(bulkItem.index).source(document, XContentType.JSON).id(id.orNull)
    }
    request
  }

  /** Conversion BulkActionType -> BulkItem */
  override private[client] def actionToBulkItem(action: BulkActionType): BulkItem = {
    action match {
      case req: IndexRequest =>
        BulkItem(
          index = req.index(),
          id = Option(req.id()),
          document = req.source().utf8ToString(),
          parent = None,
          action = BulkAction.INDEX
        )
      case req: UpdateRequest =>
        BulkItem(
          index = req.index(),
          id = Option(req.id()),
          document = req.doc().source().utf8ToString(),
          parent = None,
          action = BulkAction.UPDATE
        )
      case req: DeleteRequest =>
        BulkItem(
          index = req.index(),
          id = Option(req.id()),
          document = "",
          parent = None,
          action = BulkAction.DELETE
        )
      case _ =>
        throw new IllegalArgumentException(
          s"Unsupported BulkActionType: ${action.getClass.getName}"
        )
    }
  }

}

/** Scroll API implementation for RestHighLevelClient
  * @see
  *   [[ScrollApi]] for generic API documentation
  */
trait RestHighLevelClientScrollApi extends ScrollApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientSearchApi
    with RestHighLevelClientVersionApi
    with RestHighLevelClientCompanion =>

  /** Classic scroll (works for both hits and aggregations)
    */
  override private[client] def scrollClassic(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation],
    config: ScrollConfig
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = {
    implicit val ec: ExecutionContext = system.dispatcher
    Source
      .unfoldAsync[Option[String], Seq[Map[String, Any]]](None) { scrollIdOpt =>
        retryWithBackoff(config.retryConfig) {
          Future {
            scrollIdOpt match {
              case None =>
                // Initial search with scroll
                logger.info(
                  s"Starting classic scroll on indices: ${elasticQuery.indices.mkString(", ")}"
                )

                val query = elasticQuery.query
                // Create a parser for the query
                val xContentParser = XContentType.JSON
                  .xContent()
                  .createParser(
                    namedXContentRegistry,
                    DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    query
                  )
                // Execute the search
                val searchRequest =
                  new SearchRequest(elasticQuery.indices: _*)
                    .types(elasticQuery.types: _*)
                    .source(
                      SearchSourceBuilder.fromXContent(xContentParser).size(config.scrollSize)
                    )

                searchRequest.scroll(
                  TimeValue.parseTimeValue(config.keepAlive, "scroll_timeout")
                )

                val response = apply().search(searchRequest, RequestOptions.DEFAULT)

                if (response.status() != RestStatus.OK) {
                  throw new IOException(s"Initial scroll failed with status: ${response.status()}")
                }

                val scrollId = response.getScrollId

                if (scrollId == null) {
                  throw new IllegalStateException("Scroll ID is null in response")
                }

                // Extract both hits AND aggregations
                val results = extractAllResults(response, fieldAliases, aggregations)

                logger.info(s"Initial scroll returned ${results.size} results, scrollId: $scrollId")

                if (results.isEmpty) {
                  None
                } else {
                  Some((Some(scrollId), results))
                }

              case Some(scrollId) =>
                // Subsequent scroll requests
                logger.debug(s"Fetching next scroll batch (scrollId: $scrollId)")

                val scrollRequest = new SearchScrollRequest(scrollId)
                scrollRequest.scroll(
                  TimeValue.parseTimeValue(config.keepAlive, "scroll_timeout")
                )

                val result = apply().scroll(scrollRequest, RequestOptions.DEFAULT)

                if (result.status() != RestStatus.OK) {
                  clearScroll(scrollId)
                  throw new IOException(
                    s"Scroll continuation failed with status: ${result.status()}"
                  )
                }

                val newScrollId = result.getScrollId
                val results = extractAllResults(result, fieldAliases, aggregations)

                logger.debug(s"Scroll returned ${results.size} results")

                if (results.isEmpty) {
                  clearScroll(scrollId)
                  None
                } else {
                  Some((Some(newScrollId), results))
                }
            }
          }
        }(system, logger).recover { case ex: Exception =>
          logger.error(s"Scroll failed after retries: ${ex.getMessage}", ex)
          scrollIdOpt.foreach(clearScroll)
          None
        }
      }
      .mapConcat(identity)
  }

  /** Search After (only for hits, more efficient)
    */
  override private[client] def searchAfter(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    config: ScrollConfig,
    hasSorts: Boolean = false
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = {
    implicit val ec: ExecutionContext = system.dispatcher
    Source
      .unfoldAsync[Option[Array[Object]], Seq[Map[String, Any]]](None) { searchAfterOpt =>
        retryWithBackoff(config.retryConfig) {
          Future {
            searchAfterOpt match {
              case None =>
                logger.info(
                  s"Starting search_after on indices: ${elasticQuery.indices.mkString(", ")}"
                )
              case Some(values) =>
                logger.debug(s"Fetching next search_after batch (after: ${if (values.length > 3)
                  s"[${values.take(3).mkString(", ")}...]"
                else values.mkString(", ")})")
            }

            val query = elasticQuery.query
            // Create a parser for the query
            val xContentParser = XContentType.JSON
              .xContent()
              .createParser(
                namedXContentRegistry,
                DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                query
              )
            val sourceBuilder =
              SearchSourceBuilder.fromXContent(xContentParser).size(config.scrollSize)

            // Check if sorts already exist in the query
            if (!hasSorts && sourceBuilder.sorts() == null) {
              logger.warn(
                "No sort fields in query for search_after, adding default _id sort. " +
                "This may lead to inconsistent results if documents are updated during scroll."
              )
              sourceBuilder.sort("_id", SortOrder.ASC)
            } else if (hasSorts && sourceBuilder.sorts() != null) {
              // Sorts already present, check that a tie-breaker exists
              val hasIdSort = sourceBuilder.sorts().asScala.exists { sortBuilder =>
                sortBuilder match {
                  case fieldSort: FieldSortBuilder =>
                    fieldSort.getFieldName == "_id"
                  case _ =>
                    false
                }
              }
              if (!hasIdSort) {
                // Add _id as tie-breaker
                logger.debug("Adding _id as tie-breaker to existing sorts")
                sourceBuilder.sort("_id", SortOrder.ASC)
              }
            }

            // Add search_after if available
            searchAfterOpt.foreach { searchAfter =>
              sourceBuilder.searchAfter(searchAfter)
            }

            // Execute the search
            val searchRequest =
              new SearchRequest(elasticQuery.indices: _*)
                .types(elasticQuery.types: _*)
                .source(
                  sourceBuilder
                )

            val response = apply().search(searchRequest, RequestOptions.DEFAULT)

            if (response.status() != RestStatus.OK) {
              throw new IOException(s"Search after failed with status: ${response.status()}")
            }

            // Extract ONLY hits (no aggregations for search_after)
            val hits = extractHitsOnly(response, fieldAliases)

            if (hits.isEmpty) {
              None
            } else {
              val searchHits = response.getHits.getHits
              val lastHit = searchHits.last
              val nextSearchAfter = Option(lastHit.getSortValues)

              logger.debug(
                s"Retrieved ${hits.size} hits, next search_after: ${nextSearchAfter
                  .map(arr =>
                    if (arr.length > 3) s"[${arr.take(3).mkString(", ")}...]"
                    else arr.mkString(", ")
                  )
                  .getOrElse("None")}"
              )

              Some((nextSearchAfter, hits))
            }
          }
        }(system, logger).recover { case ex: Exception =>
          logger.error(s"Search after failed after retries: ${ex.getMessage}", ex)
          None
        }
      }
      .mapConcat(identity)
  }

  /** PIT + search_after for ES 7.10+
    *
    * @note
    *   Requires ES 7.10+. For ES 6.x, use searchAfterSource instead.
    */
  private[client] def pitSearchAfter(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    config: ScrollConfig,
    hasSorts: Boolean = false
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = {
    implicit val ec: ExecutionContext = system.dispatcher

    // Open PIT
    val pitIdFuture: Future[String] = openPit(elasticQuery.indices, config.keepAlive)

    Source
      .futureSource {
        pitIdFuture.map { pitId =>
          logger.info(
            s"Opened PIT: ${pitId.take(20)}... for indices: ${elasticQuery.indices.mkString(", ")}"
          )

          Source
            .unfoldAsync[Option[Array[Object]], Seq[Map[String, Any]]](None) { searchAfterOpt =>
              retryWithBackoff(config.retryConfig) {
                Future {
                  searchAfterOpt match {
                    case None =>
                      logger.info(s"Starting PIT search_after (pitId: ${pitId.take(20)}...)")
                    case Some(values) =>
                      logger.debug(
                        s"Fetching next PIT search_after batch (after: ${if (values.length > 3)
                          s"[${values.take(3).mkString(", ")}...]"
                        else values.mkString(", ")})"
                      )
                  }

                  // Parse query
                  val xContentParser = XContentType.JSON
                    .xContent()
                    .createParser(
                      namedXContentRegistry,
                      DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                      elasticQuery.query
                    )

                  val sourceBuilder = SearchSourceBuilder
                    .fromXContent(xContentParser)
                    .size(config.scrollSize)

                  // Check if sorts already exist in the query
                  if (!hasSorts && sourceBuilder.sorts() == null) {
                    logger.warn(
                      "No sort fields in query for PIT search_after, adding default _shard_doc sort."
                    )
                    sourceBuilder.sort("_shard_doc", SortOrder.ASC)
                  } else if (hasSorts && sourceBuilder.sorts() != null) {
                    // Sorts already present, check that a tie-breaker exists
                    val hasShardDocSort = sourceBuilder.sorts().asScala.exists {
                      case fieldSort: FieldSortBuilder =>
                        fieldSort.getFieldName == "_shard_doc" || fieldSort.getFieldName == "_id"
                      case _ =>
                        false
                    }

                    if (!hasShardDocSort) {
                      // Add _id as tie-breaker
                      logger.debug("Adding _shard_doc as tie-breaker to existing sorts")
                      sourceBuilder.sort("_shard_doc", SortOrder.ASC)
                    }
                  }

                  // Add search_after
                  searchAfterOpt.foreach { searchAfter =>
                    sourceBuilder.searchAfter(searchAfter)
                  }

                  // Set PIT
                  val pitBuilder = new PointInTimeBuilder(pitId)
                  pitBuilder.setKeepAlive(
                    TimeValue.parseTimeValue(config.keepAlive, "pit_keep_alive")
                  )
                  sourceBuilder.pointInTimeBuilder(pitBuilder)

                  // Build request with PIT
                  val searchRequest = new SearchRequest()
                    .source(sourceBuilder)
                    .requestCache(false) // Disable cache for PIT

                  val response = apply().search(searchRequest, RequestOptions.DEFAULT)

                  if (response.status() != RestStatus.OK) {
                    throw new IOException(
                      s"PIT search_after failed with status: ${response.status()}"
                    )
                  }

                  val hits = extractHitsOnly(response, fieldAliases)

                  if (hits.isEmpty) {
                    closePit(pitId)
                    None
                  } else {
                    val searchHits = response.getHits.getHits
                    val lastHit = searchHits.last
                    val nextSearchAfter = Option(lastHit.getSortValues)

                    logger.debug(s"Retrieved ${hits.size} hits, continuing with PIT")
                    Some((nextSearchAfter, hits))
                  }
                }
              }(system, logger).recover { case ex: Exception =>
                logger.error(s"PIT search_after failed after retries: ${ex.getMessage}", ex)
                closePit(pitId)
                None
              }
            }
            .watchTermination() { (_, done) =>
              done.onComplete {
                case scala.util.Success(_) =>
                  logger.info(s"PIT search_after completed, closing PIT: ${pitId.take(20)}...")
                  closePit(pitId)
                case scala.util.Failure(ex) =>
                  logger.error(
                    s"PIT search_after failed: ${ex.getMessage}, closing PIT: ${pitId.take(20)}..."
                  )
                  closePit(pitId)
              }
              NotUsed
            }
            .mapConcat(identity)
        }
      }
      .mapMaterializedValue(_ => NotUsed)
  }

  /** Open PIT (ES 7.10+)
    */
  private def openPit(indices: Seq[String], keepAlive: String)(implicit
    ec: ExecutionContext
  ): Future[String] = {
    Future {
      logger.debug(s"Opening PIT for indices: ${indices.mkString(", ")}")

      val openPitRequest = new OpenPointInTimeRequest(indices: _*)
        .keepAlive(TimeValue.parseTimeValue(keepAlive, "pit_keep_alive"))

      val response = apply().openPointInTime(openPitRequest, RequestOptions.DEFAULT)
      val pitId = response.getPointInTimeId

      if (pitId == null || pitId.isEmpty) {
        throw new IllegalStateException("PIT ID is null or empty")
      }

      logger.info(s"PIT opened: ${pitId.take(20)}...")
      pitId
    }.recoverWith { case ex: Exception =>
      logger.error(s"Failed to open PIT: ${ex.getMessage}", ex)
      Future.failed(
        new IOException(s"Failed to open PIT for indices: ${indices.mkString(", ")}", ex)
      )
    }
  }

  /** Close PIT
    */
  private def closePit(pitId: String): Unit = {
    Try {
      logger.debug(s"Closing PIT: ${pitId.take(20)}...")

      val closePitRequest = new ClosePointInTimeRequest(pitId)
      val response = apply().closePointInTime(closePitRequest, RequestOptions.DEFAULT)

      if (response.isSucceeded) {
        logger.info(s"PIT closed successfully: ${pitId.take(20)}...")
      } else {
        logger.warn(s"PIT close reported failure: ${pitId.take(20)}...")
      }
    }.recover { case ex: Exception =>
      logger.warn(s"Failed to close PIT ${pitId.take(20)}...: ${ex.getMessage}")
    }
  }

  /** Extract ALL results: hits + aggregations This is crucial for queries with aggregations
    */
  private def extractAllResults(
    response: SearchResponse,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): Seq[Map[String, Any]] = {
    val jsonString = response.toString

    parseResponse(
      jsonString,
      fieldAliases,
      aggregations.map(kv => kv._1 -> implicitly[ClientAggregation](kv._2))
    ) match {
      case Success(rows) =>
        logger.debug(s"Parsed ${rows.size} rows from response")
        rows
      case Failure(ex) =>
        logger.error(s"Failed to parse scroll response: ${ex.getMessage}", ex)
        Seq.empty
    }
  }

  /** Extract ONLY hits (for search_after optimization)
    */
  private def extractHitsOnly(
    response: SearchResponse,
    fieldAliases: Map[String, String]
  ): Seq[Map[String, Any]] = {
    val jsonString = response.toString

    parseResponse(jsonString, fieldAliases, Map.empty) match {
      case Success(rows) => rows
      case Failure(ex) =>
        logger.error(s"Failed to parse search after response: ${ex.getMessage}", ex)
        Seq.empty
    }
  }

  private def clearScroll(scrollId: String): Unit = {
    Try {
      logger.debug(s"Clearing scroll: $scrollId")
      val clearScrollRequest = new ClearScrollRequest()
      clearScrollRequest.addScrollId(scrollId)
      apply().clearScroll(clearScrollRequest, RequestOptions.DEFAULT)
    }.recover { case ex: Exception =>
      logger.warn(s"Failed to clear scroll $scrollId: ${ex.getMessage}")
    }
  }
}

trait RestHighLevelClientPipelineApi extends PipelineApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientVersionApi with RestHighLevelClientCompanion =>

  override private[client] def executeCreatePipeline(
    pipelineName: String,
    pipelineDefinition: String
  ): ElasticResult[Boolean] =
    executeRestBooleanAction[PutPipelineRequest, AcknowledgedResponse](
      operation = "createPipeline",
      retryable = false
    )(
      request = {
        val req = new PutPipelineRequest(
          pipelineName,
          new BytesArray(pipelineDefinition),
          XContentType.JSON
        )
        logger.info(
          s"Creating Ingest Pipeline '$pipelineName':\n${Strings.toString(req, true, true)}"
        )
        req
      }
    )(
      executor = req => apply().ingest().putPipeline(req, RequestOptions.DEFAULT)
    )

  override private[client] def executeDeletePipeline(
    pipelineName: String,
    ifExists: Boolean
  ): ElasticResult[Boolean] = {
    executeRestBooleanAction[DeletePipelineRequest, AcknowledgedResponse](
      operation = "deletePipeline",
      retryable = false
    )(
      request = new DeletePipelineRequest(pipelineName)
    )(
      executor = req => apply().ingest().deletePipeline(req, RequestOptions.DEFAULT)
    )
  }

  override private[client] def executeGetPipeline(
    pipelineName: JSONQuery
  ): ElasticResult[Option[JSONQuery]] = {
    executeRestAction[GetPipelineRequest, GetPipelineResponse, Option[JSONQuery]](
      operation = "getPipeline",
      retryable = true
    )(
      request = new GetPipelineRequest(pipelineName)
    )(
      executor = req => apply().ingest().getPipeline(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => {
        val pipelines = resp.pipelines().asScala
        if (pipelines.nonEmpty) {
          val pipeline = pipelines.head
          val config = pipeline.getConfigAsMap
          val mapper = JacksonConfig.objectMapper
          Some(mapper.writeValueAsString(config))
        } else {
          None
        }
      }
    )
  }
}

trait RestHighLevelClientTemplateApi extends TemplateApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientVersionApi with RestHighLevelClientCompanion =>

  // ==================== COMPOSABLE TEMPLATES (ES 7.8+) ====================

  override private[client] def executeCreateComposableTemplate(
    templateName: String,
    templateDefinition: String
  ): ElasticResult[Boolean] = {
    executeRestBooleanAction[PutComposableIndexTemplateRequest, AcknowledgedResponse](
      operation = "createTemplate",
      retryable = false
    )(
      request = {
        val req = new PutComposableIndexTemplateRequest()
        req.name(templateName)
        req.indexTemplate(
          ComposableIndexTemplate.parse(
            org.elasticsearch.common.xcontent.XContentHelper.createParser(
              org.elasticsearch.xcontent.NamedXContentRegistry.EMPTY,
              org.elasticsearch.xcontent.DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
              new BytesArray(templateDefinition),
              XContentType.JSON
            )
          )
        )
        req
      }
    )(
      executor = req => apply().indices().putIndexTemplate(req, RequestOptions.DEFAULT)
    )
  }

  override private[client] def executeDeleteComposableTemplate(
    templateName: String,
    ifExists: Boolean
  ): ElasticResult[Boolean] = {
    if (ifExists) {
      executeComposableTemplateExists(templateName) match {
        case ElasticSuccess(exists) =>
          if (!exists) {
            logger.debug(s"Composable template '$templateName' does not exist, skipping deletion")
            return ElasticSuccess(false)
          }
        case failure @ ElasticFailure(_) =>
          return failure
      }
    }
    executeRestBooleanAction[DeleteComposableIndexTemplateRequest, AcknowledgedResponse](
      operation = "deleteTemplate",
      retryable = false
    )(
      request = new DeleteComposableIndexTemplateRequest(templateName)
    )(
      executor = req => apply().indices().deleteIndexTemplate(req, RequestOptions.DEFAULT)
    )
  }

  override private[client] def executeGetComposableTemplate(
    templateName: String
  ): ElasticResult[Option[String]] = {
    executeRestAction(
      operation = "getTemplate",
      retryable = true
    )(
      request = new GetComposableIndexTemplateRequest(templateName)
    )(
      executor = req => apply().indices().getIndexTemplate(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => {
        val templates = resp.getIndexTemplates
        if (templates != null && !templates.isEmpty) {
          val template = templates.get(templateName)
          if (template != null) {
            Some(composableTemplateToJson(template))
          } else {
            None
          }
        } else {
          None
        }
      }
    )
  }

  override private[client] def executeListComposableTemplates()
    : ElasticResult[Map[String, String]] = {
    executeRestAction(
      operation = "listTemplates",
      retryable = true
    )(
      request = new GetComposableIndexTemplateRequest("*")
    )(
      executor = req => apply().indices().getIndexTemplate(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => {
        val templates = resp.getIndexTemplates
        if (templates != null) {
          templates.asScala.map { case (name, template) =>
            name -> composableTemplateToJson(template)
          }.toMap
        } else {
          Map.empty
        }
      }
    )
  }

  override private[client] def executeComposableTemplateExists(
    templateName: String
  ): ElasticResult[Boolean] = {
    executeRestAction[ComposableIndexTemplateExistRequest, Boolean, Boolean](
      operation = "existsTemplate",
      retryable = false
    )(
      request = new ComposableIndexTemplateExistRequest(templateName)
    )(
      executor = req => apply().indices().existsIndexTemplate(req, RequestOptions.DEFAULT)
    )(response => response)
  }

  // ==================== LEGACY TEMPLATES ====================

  override private[client] def executeCreateLegacyTemplate(
    templateName: String,
    templateDefinition: String
  ): ElasticResult[Boolean] = {
    executeRestBooleanAction[PutIndexTemplateRequest, AcknowledgedResponse](
      operation = "createTemplate",
      retryable = false
    )(
      request = {
        val req = new PutIndexTemplateRequest(templateName)
        req.source(new BytesArray(templateDefinition), XContentType.JSON)
        req
      }
    )(
      executor = req => apply().indices().putTemplate(req, RequestOptions.DEFAULT)
    )
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
    executeRestBooleanAction[DeleteIndexTemplateRequest, AcknowledgedResponse](
      operation = "deleteTemplate",
      retryable = false
    )(
      request = new DeleteIndexTemplateRequest(templateName)
    )(
      executor = req => apply().indices().deleteTemplate(req, RequestOptions.DEFAULT)
    )
  }

  override private[client] def executeGetLegacyTemplate(
    templateName: String
  ): ElasticResult[Option[String]] = {
    executeRestAction[GetIndexTemplatesRequest, GetIndexTemplatesResponse, Option[String]](
      operation = "getTemplate",
      retryable = true
    )(
      request = new GetIndexTemplatesRequest(templateName)
    )(
      executor = req => apply().indices().getIndexTemplate(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => {
        val templates = resp.getIndexTemplates.asScala
        if (templates.nonEmpty) {
          val template = templates.head
          Some(legacyTemplateToJson(template))
        } else {
          None
        }
      }
    )
  }

  override private[client] def executeListLegacyTemplates(): ElasticResult[Map[String, String]] = {
    executeRestAction[GetIndexTemplatesRequest, GetIndexTemplatesResponse, Map[String, String]](
      operation = "listTemplates",
      retryable = true
    )(
      request = new GetIndexTemplatesRequest("*")
    )(
      executor = req => apply().indices().getIndexTemplate(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => {
        val mapper = JacksonConfig.objectMapper
        resp.getIndexTemplates.asScala.map { template =>
          template.name() -> legacyTemplateToJson(template)
        }.toMap
      }
    )
  }

  override private[client] def executeLegacyTemplateExists(
    templateName: String
  ): ElasticResult[Boolean] = {
    executeRestAction[IndexTemplatesExistRequest, Boolean, Boolean](
      operation = "existsTemplate",
      retryable = false
    )(
      request = new IndexTemplatesExistRequest(templateName)
    )(
      executor = req => apply().indices().existsTemplate(req, RequestOptions.DEFAULT)
    )(response => response)
  }

  // ==================== CONVERSION HELPERS ====================

  private def composableTemplateToJson(template: ComposableIndexTemplate): String = {
    try {
      val builder = XContentFactory.jsonBuilder()
      template.toXContent(builder, ToXContent.EMPTY_PARAMS)
      builder.close()
      org.elasticsearch.common.Strings.toString(builder)
    } catch {
      case e: Exception =>
        logger.error(s"Failed to convert composable template to JSON: ${e.getMessage}", e)
        "{}"
    }
  }

  private def legacyTemplateToJson(template: IndexTemplateMetadata): String = {

    val root = mapper.createObjectNode()

    // index_patterns
    val patternsArray = mapper.createArrayNode()
    template.patterns().asScala.foreach(patternsArray.add)
    root.set("index_patterns", patternsArray)

    // order
    root.put("order", template.order())

    // version
    if (template.version() != null) {
      root.put("version", template.version())
    }

    // settings
    if (template.settings() != null && !template.settings().isEmpty) {
      val settingsNode = mapper.createObjectNode()
      template.settings().keySet().asScala.foreach { key =>
        val value = template.settings().get(key)
        if (value != null) {
          settingsNode.put(key, value)
        }
      }
      root.set("settings", settingsNode)
    }

    // mappings
    if (template.mappings() != null && template.mappings().source().string().nonEmpty) {
      try {
        val mappingsNode = mapper.readTree(template.mappings().source().string())
        root.set("mappings", mappingsNode)
      } catch {
        case e: Exception =>
          logger.warn(s"Failed to parse mappings: ${e.getMessage}")
      }
    }

    // aliases
    if (template.aliases() != null && !template.aliases().isEmpty) {
      val aliasesNode = mapper.createObjectNode()

      template.aliases().keysIt().asScala.foreach { aliasName =>
        // Type explicite pour éviter l'inférence vers Nothing$
        val aliasObjectNode: ObjectNode =
          try {
            val aliasMetadata = template.aliases().get(aliasName)

            // Convert AliasMetadata to JSON
            val aliasJson = convertAliasMetadataToJson(aliasMetadata)

            // Parse and validate
            val parsedAlias = mapper.readTree(aliasJson)

            if (parsedAlias.isInstanceOf[ObjectNode]) {
              parsedAlias.asInstanceOf[ObjectNode].get(aliasName) match {
                case objNode: ObjectNode => objNode
                case _ =>
                  logger.debug(
                    s"Alias '$aliasName' does not contain an object node, creating empty object"
                  )
                  mapper.createObjectNode()
              }
            } else {
              logger.debug(
                s"Alias '$aliasName' is not an ObjectNode (type: ${parsedAlias.getClass.getName}), creating empty object"
              )
              mapper.createObjectNode()
            }

          } catch {
            case e: Exception =>
              logger.warn(s"Failed to process alias '$aliasName': ${e.getMessage}", e)
              mapper.createObjectNode()
          }

        // Set with explicit type
        aliasesNode.set[ObjectNode](aliasName, aliasObjectNode)
      }

      root.set("aliases", aliasesNode)
    }

    mapper.writeValueAsString(root)
  }

  /** Convert AliasMetadata to JSON string
    *
    * @param aliasMetadata
    *   the alias metadata
    * @return
    *   JSON string representation
    */
  private def convertAliasMetadataToJson(
    aliasMetadata: AliasMetadata
  ): String = {
    try {
      import org.elasticsearch.xcontent.{ToXContent, XContentFactory}

      val builder = XContentFactory.jsonBuilder()
      builder.startObject()
      aliasMetadata.toXContent(builder, ToXContent.EMPTY_PARAMS)
      builder.endObject()
      builder.close()

      org.elasticsearch.common.Strings.toString(builder)

    } catch {
      case e: Exception =>
        logger.warn(s"Failed to convert AliasMetadata to JSON: ${e.getMessage}", e)
        "{}"
    }
  }
}

// ==================== ENRICH POLICY API IMPLEMENTATION FOR REST HIGH LEVEL CLIENT ====================

trait RestHighLevelClientEnrichPolicyApi extends EnrichPolicyApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientVersionApi with RestHighLevelClientCompanion =>

  override private[client] def executeCreateEnrichPolicy(
    policy: EnrichPolicy
  ): result.ElasticResult[Boolean] =
    executeRestAction(
      operation = "CreateEnrichPolicy",
      retryable = false
    )(
      request = {
        val req = new PutPolicyRequest(
          policy.name,
          policy.policyType.name.toLowerCase,
          policy.indices.asJava,
          policy.matchField,
          policy.enrichFields.asJava
        )
        policy.criteria.foreach { criteria =>
          implicit val timestamp: Long = System.currentTimeMillis()
          val queryJson: String = implicitly[JsonNode](criteria)
          val queryBuilder = parseQueryFromJson(queryJson)
          req.setQuery(queryBuilder)
        }
        logger.info(s"Creating Enrich Policy ${policy.name}:\n${Strings.toString(req, true, true)}")
        req
      }
    )(
      executor = req => apply().enrich().putPolicy(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => resp.isAcknowledged
    )

  override private[client] def executeDeleteEnrichPolicy(
    policyName: String
  ): result.ElasticResult[Boolean] =
    executeRestAction(
      operation = "DeleteEnrichPolicy",
      retryable = false
    )(
      request = new DeletePolicyRequest(policyName)
    )(
      executor = req => apply().enrich().deletePolicy(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => resp.isAcknowledged
    )

  override private[client] def executeExecuteEnrichPolicy(
    policyName: String
  ): ElasticResult[EnrichPolicyTask] = {
    val startTime = ZonedDateTime.now()
    executeRestAction(
      operation = "ExecuteEnrichPolicy",
      retryable = false
    )(
      request = {
        val req = new ExecutePolicyRequest(policyName)
        req.setWaitForCompletion(true)
        req
      }
    )(
      executor = req => apply().enrich().executePolicy(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => {
        val endTime = ZonedDateTime.now()
        val status = EnrichPolicyTaskStatus(resp.getExecutionStatus.getPhase)
        logger.info(
          s"Enrich Policy '$policyName' executed in ${endTime.toInstant.toEpochMilli - startTime.toInstant.toEpochMilli} ms with status: ${status.name}"
        )
        EnrichPolicyTask(
          policyName = policyName,
          taskId = resp.getTaskId,
          status = status,
          startTime = Some(startTime),
          endTime = Some(endTime)
        )
      }
    )
  }

  /** Parse JSON query string into QueryBuilder
    */
  private def parseQueryFromJson(queryJson: String): QueryBuilder = {
    val parser = XContentType.JSON
      .xContent()
      .createParser(
        this.namedXContentRegistry,
        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
        queryJson
      )

    try {
      parser.nextToken() // Move to first token
      org.elasticsearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder(parser)
    } finally {
      parser.close()
    }
  }
}

// ==================== TRANSFORM API IMPLEMENTATION FOR REST HIGH LEVEL CLIENT ====================

trait RestHighLevelClientTransformApi extends TransformApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientVersionApi with RestHighLevelClientCompanion =>

  override private[client] def executeCreateTransform(
    config: schema.TransformConfig,
    start: Boolean
  ): result.ElasticResult[Boolean] = {
    /*executeRestAction(
      operation = "createTransform",
      retryable = false
    )(request = {
      implicit val timestamp: Long = System.currentTimeMillis()
      implicit val context: PainlessContextType = PainlessContextType.Transform
      val json: String = config.node
      logger.info(s"Creating transform: ${config.id} with definition:\n$json")
      val req = new Request(
        "PUT",
        s"/_transform/${config.id}"
      )
      req.setJsonEntity(json)
      req
    })(
      executor = req => apply().getLowLevelClient.performRequest(req)
    )(
      transformer = resp =>
        resp.getStatusLine.getStatusCode match {
          case 200 | 201 =>
            true

          case code =>
            logger.error(s"❌ Unexpected response code for [${config.id}]: $code")
            false
        }
    )*/
    executeRestAction(
      operation = "createTransform",
      retryable = false
    )(
      request = {
        implicit val timestamp: Long = System.currentTimeMillis()
        implicit val context: PainlessContextType = PainlessContextType.Transform
        val conf = convertToElasticTransformConfig(config)
        val req = new PutTransformRequest(conf)
        req.setDeferValidation(false)
        logger.info(s"Creating Transform ${config.id} :\n${Strings.toString(conf, true, true)}")
        req
      }
    )(
      executor = req => apply().transform().putTransform(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => resp.isAcknowledged
    )
  }

  private def convertToElasticTransformConfig(
    config: schema.TransformConfig
  )(implicit criteriaToNode: Criteria => JsonNode): TransformConfig = {
    val builder = TransformConfig.builder()

    // Set ID
    builder.setId(config.id)

    // Set description
    builder.setDescription(config.description)

    // Set source
    val sourceConfig = SourceConfig
      .builder()
      .setIndex(config.source.index: _*)

    config.source.query.foreach { criteria =>
      // Convert Criteria to QueryBuilder
      val node: JsonNode = criteria
      val queryJson = mapper.writeValueAsString(node)
      sourceConfig.setQueryConfig(
        new QueryConfig(QueryBuilders.wrapperQuery(queryJson))
      )
    }
    builder.setSource(sourceConfig.build())

    // Set destination
    val destConfig = DestConfig
      .builder()
      .setIndex(config.dest.index)
    config.dest.pipeline.foreach(destConfig.setPipeline)
    builder.setDest(destConfig.build())

    // Set frequency
    builder.setFrequency(
      org.elasticsearch.core.TimeValue.parseTimeValue(
        config.frequency.toTransformFormat,
        "frequency"
      )
    )

    // Set sync (if present)
    config.sync.foreach { sync =>
      val syncConfig = TimeSyncConfig
        .builder()
        .setField(sync.time.field)
        .setDelay(
          org.elasticsearch.core.TimeValue.parseTimeValue(
            sync.time.delay.toTransformFormat,
            "delay"
          )
        )
        .build()
      builder.setSyncConfig(syncConfig)
    }

    // Set pivot (if present)
    config.pivot.foreach { pivot =>
      val pivotConfig = PivotConfig.builder()

      // Group by
      val groupConfig = GroupConfig.builder()
      pivot.groupBy.foreach { case (name, gb) =>
        gb match {
          case TermsGroupBy(field) =>
            groupConfig
              .groupBy(name, TermsGroupSource.builder().setField(field).build())
        }
      }
      pivotConfig.setGroups(groupConfig.build())

      // Aggregations
      val aggBuilder = AggregatorFactories.builder()
      pivot.aggregations.foreach { case (name, agg) =>
        agg match {
          case MaxTransformAggregation(field) =>
            aggBuilder.addAggregator(new MaxAggregationBuilder(name).field(field))
          case MinTransformAggregation(field) =>
            aggBuilder.addAggregator(new MinAggregationBuilder(name).field(field))
          case SumTransformAggregation(field) =>
            aggBuilder.addAggregator(new SumAggregationBuilder(name).field(field))
          case AvgTransformAggregation(field) =>
            aggBuilder.addAggregator(new AvgAggregationBuilder(name).field(field))
          case CountTransformAggregation(field) =>
            aggBuilder.addAggregator(new ValueCountAggregationBuilder(name).field(field))
          case CardinalityTransformAggregation(field) =>
            aggBuilder.addAggregator(new CardinalityAggregationBuilder(name).field(field))
          case TopHitsTransformAggregation(fields, size, sortFields) =>
            val topHitsBuilder = new TopHitsAggregationBuilder(name)
              .size(size)
              .fetchSource(fields.toArray, Array.empty[String])
            sortFields.foreach { sortField =>
              val sortOrder = sortField.order.getOrElse(Desc) match {
                case Asc  => SortOrder.ASC
                case Desc => SortOrder.DESC
                case _    => SortOrder.DESC
              }
              topHitsBuilder.sort(sortField.name, sortOrder)
            }
            aggBuilder.addAggregator(topHitsBuilder)
          case _ =>
            throw new UnsupportedOperationException(s"Unsupported aggregation: $agg")
        }
      }
      pivot.bucketSelector foreach { bs =>
        val bucketSelector = new BucketSelectorPipelineAggregationBuilder(
          bs.name,
          bs.bucketsPath.asJava,
          new org.elasticsearch.script.Script(bs.script)
        )
        aggBuilder.addPipelineAggregator(bucketSelector)
      }
      pivotConfig.setAggregationConfig(new AggregationConfig(aggBuilder))
      builder.setPivotConfig(pivotConfig.build())
    }

    config.latest.foreach { latest =>
      val latestConfig = LatestConfig
        .builder()
        .setUniqueKey(latest.uniqueKey.asJava)
        .setSort(latest.sort)
        .build()

      builder.setLatestConfig(latestConfig)
    }

    builder.setMetadata(config.metadata.asJava)

    builder.build()
  }

  override private[client] def executeDeleteTransform(
    transformId: String,
    force: Boolean
  ): result.ElasticResult[Boolean] = {
    executeRestAction(
      operation = "deleteTransform",
      retryable = false
    )(
      request = {
        val req = new DeleteTransformRequest(transformId)
        req.setForce(force)
        req
      }
    )(
      executor = req => apply().transform().deleteTransform(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => resp.isAcknowledged
    )
  }

  override private[client] def executeStartTransform(
    transformId: String
  ): result.ElasticResult[Boolean] = {
    executeRestAction(
      operation = "startTransform",
      retryable = false
    )(
      request = new StartTransformRequest(transformId)
    )(
      executor = req => apply().transform().startTransform(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => resp.isAcknowledged
    )
  }

  override private[client] def executeStopTransform(
    transformId: String,
    force: Boolean,
    waitForCompletion: Boolean
  ): result.ElasticResult[Boolean] = {
    executeRestAction(
      operation = "stopTransform",
      retryable = false
    )(
      request = {
        val req = new StopTransformRequest(transformId)
        req.setWaitForCheckpoint(!force)
        req.setWaitForCompletion(waitForCompletion)
        req
      }
    )(
      executor = req => apply().transform().stopTransform(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => resp.isAcknowledged
    )
  }

  override private[client] def executeGetTransformStats(
    transformId: String
  ): result.ElasticResult[Option[schema.TransformStats]] = {
    executeRestAction(
      operation = "getTransformStats",
      retryable = true
    )(
      request = new GetTransformStatsRequest(transformId)
    )(
      executor = req => apply().transform().getTransformStats(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => {
        val statsArray = resp.getTransformsStats.asScala
        statsArray.headOption.map { stats =>
          schema.TransformStats(
            id = stats.getId,
            state = TransformState(stats.getState.value()),
            documentsProcessed = stats.getIndexerStats.getDocumentsProcessed,
            documentsIndexed = stats.getIndexerStats.getDocumentsIndexed,
            indexFailures = stats.getIndexerStats.getIndexFailures,
            searchFailures = stats.getIndexerStats.getSearchFailures,
            lastCheckpoint = Option(stats.getCheckpointingInfo)
              .flatMap(c => Option(c.getLast))
              .map(_.getCheckpoint),
            operationsBehind = Option(stats.getCheckpointingInfo)
              .flatMap(info => Option(info.getOperationsBehind))
              .getOrElse(0L),
            processingTimeMs = stats.getIndexerStats.getProcessingTime
          )
        }
      }
    )
  }

  override private[client] def executeScheduleTransformNow(
    transformId: String
  ): result.ElasticResult[Boolean] = {
    executeRestAction(
      operation = "scheduleNow",
      retryable = false
    )(
      request = {
        val req = new Request(
          "POST",
          s"/_transform/$transformId/_schedule_now"
        )
        req.setJsonEntity("{}")
        req
      }
    )(
      executor = req => apply().getLowLevelClient.performRequest(req)
    )(
      transformer = resp =>
        resp.getStatusLine.getStatusCode match {
          case 200 | 201 =>
            logger.info(s"✅ Transform [$transformId] scheduled for immediate execution")
            true

          case 409 =>
            logger.warn(s"⚠️ Transform [$transformId] is already running")
            true

          case code =>
            logger.error(s"❌ Unexpected response code for [$transformId]: $code")
            false
        }
    )
  }
}

// ==================== WATCHER API IMPLEMENTATION FOR REST HIGH LEVEL CLIENT ====================

trait RestHighLevelClientWatcherApi extends WatcherApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientCompanion =>

  override private[client] def executeCreateWatcher(
    watcher: schema.Watcher,
    active: Boolean
  ): result.ElasticResult[Boolean] =
    executeRestAction[PutWatchRequest, PutWatchResponse, Boolean](
      operation = "createWatcher",
      index = None,
      retryable = false
    )(
      request = {
        implicit val timestamp: Long = System.currentTimeMillis()
        val json = watcher.node
        logger.info(s"Creating Watcher ${watcher.id} :\n${sanitizeWatcherJson(json)}")
        val req = new PutWatchRequest(
          watcher.id,
          new BytesArray(json),
          XContentType.JSON
        )
        req.setActive(active)
        req
      }
    )(
      executor = req => apply().watcher().putWatch(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => resp.isCreated
    )

  override private[client] def executeDeleteWatcher(id: JSONQuery): ElasticResult[Boolean] =
    executeRestAction[DeleteWatchRequest, DeleteWatchResponse, Boolean](
      operation = "deleteWatcher",
      index = None,
      retryable = false
    )(
      request = new DeleteWatchRequest(id)
    )(
      executor = req => apply().watcher().deleteWatch(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => resp.isFound
    )

  override private[client] def executeGetWatcherStatus(
    id: String
  ): result.ElasticResult[Option[schema.WatcherStatus]] =
    executeRestAction[GetWatchRequest, GetWatchResponse, Option[schema.WatcherStatus]](
      operation = "getWatcher",
      index = None,
      retryable = true
    )(
      request = new GetWatchRequest(id)
    )(
      executor = req => apply().watcher().getWatch(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => {
        if (resp.isFound) {
          val source = resp.getSourceAsMap
          val interval: Option[TransformTimeInterval] = {
            source.get("trigger") match {
              case triggerMap: java.util.Map[String, _] =>
                triggerMap.asScala.get("schedule") match {
                  case Some(scheduleMap: java.util.Map[String, _]) =>
                    scheduleMap.asScala.get("interval") match {
                      case Some(interval: String) =>
                        TransformTimeInterval(interval) match {
                          case Some(ti) => Some(ti)
                          case _ =>
                            logger.warn(
                              s"Watcher [$id] has invalid interval: $interval"
                            )
                            None
                        }
                      case _ =>
                        scheduleMap.asScala.get("cron") match {
                          case Some(cron: String) =>
                            CronIntervalCalculator.validateAndCalculate(cron) match {
                              case Right(interval) =>
                                val tuple = TransformTimeInterval.fromSeconds(interval._2)
                                Some(
                                  Delay(
                                    timeUnit = tuple._1,
                                    interval = tuple._2
                                  )
                                )
                              case _ =>
                                logger.warn(
                                  s"Watcher [$id] has invalid cron expression: $cron"
                                )
                                None
                            }
                          case _ => None
                        }
                    }
                  case _ => None
                }
              case _ => None
            }
          }

          interval match {
            case None =>
              logger.warn(s"Watcher [$id] does not have a valid schedule interval")
            case Some(t) => // valid interval
              logger.info(s"Watcher [$id] has schedule interval: $t")
          }

          Option(resp.getStatus).map { status =>
            val version = resp.getVersion
            val state = Option(status.state())
            val active = state.exists(_.isActive)
            import _root_.java.time.ZonedDateTime
            val timestamp =
              state.map(_.getTimestamp).getOrElse(ZonedDateTime.now())
            val activationState =
              schema.WatcherActivationState(active = active, timestamp = timestamp)
            schema.WatcherStatus(
              id = id,
              version = version,
              activationState = activationState,
              executionState =
                Option(status.getExecutionState).map(es => WatcherExecutionState(es.name())),
              lastChecked = Option(status.lastChecked()),
              lastMetCondition = Option(status.lastMetCondition()),
              interval = interval
            )
          }
        } else {
          None
        }
      }
    )
}

// ==================== LICENSE API IMPLEMENTATION FOR REST HIGH LEVEL CLIENT ====================

trait RestHighLevelClientLicenseApi extends LicenseApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientCompanion =>

  override private[client] def executeLicenseInfo: ElasticResult[Option[String]] = {
    executeRestAction[GetLicenseRequest, GetLicenseResponse, Option[String]](
      operation = "licenseInfo",
      retryable = true
    )(
      request = new GetLicenseRequest()
    )(
      executor = req => apply().license().getLicense(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => Option(resp.getLicenseDefinition)
    )
  }

  override private[client] def executeEnableBasicLicense(): ElasticResult[Boolean] = {
    executeRestAction[StartBasicRequest, StartBasicResponse, Boolean](
      operation = "enableBasicLicense",
      retryable = false
    )(
      request = new StartBasicRequest(true)
    )(
      executor = req => apply().license().startBasic(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => resp.isAcknowledged
    )
  }

  override private[client] def executeEnableTrialLicense(): ElasticResult[Boolean] = {
    executeRestAction[StartTrialRequest, StartTrialResponse, Boolean](
      operation = "enableTrialLicense",
      retryable = false
    )(
      request = new StartTrialRequest(true)
    )(
      executor = req => apply().license().startTrial(req, RequestOptions.DEFAULT)
    )(
      transformer = resp => resp.isAcknowledged
    )
  }
}
