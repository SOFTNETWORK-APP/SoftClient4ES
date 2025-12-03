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
import app.softnetwork.elastic.client.scroll._
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.query.{SQLAggregation, SQLSearchRequest}
import com.google.gson.JsonParser
import org.apache.http.util.EntityUtils
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.flush.{FlushRequest, FlushResponse}
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest
import org.elasticsearch.action.admin.indices.refresh.{RefreshRequest, RefreshResponse}
import org.elasticsearch.action.admin.indices.settings.get.{GetSettingsRequest, GetSettingsResponse}
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.bulk.{BulkRequest, BulkResponse}
import org.elasticsearch.action.delete.{DeleteRequest, DeleteResponse}
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
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
import org.elasticsearch.client.{GetAliasesResponse, Request, RequestOptions}
import org.elasticsearch.client.core.{CountRequest, CountResponse}
import org.elasticsearch.client.indices.{
  CloseIndexRequest,
  CreateIndexRequest,
  GetIndexRequest,
  GetMappingsRequest,
  PutMappingRequest
}
import org.elasticsearch.common.Strings
import org.elasticsearch.core.TimeValue
import org.elasticsearch.xcontent.{DeprecationHandler, XContentType}
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.builder.{PointInTimeBuilder, SearchSourceBuilder}
import org.elasticsearch.search.sort.{FieldSortBuilder, SortOrder}
import org.json4s.jackson.JsonMethods
import org.json4s.DefaultFormats

import java.io.IOException
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
    with RestHighLevelClientVersion

/** Version API implementation for RestHighLevelClient
  * @see
  *   [[VersionApi]] for generic API documentation
  */
trait RestHighLevelClientVersion extends VersionApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientCompanion with SerializationApi =>

  override private[client] def executeVersion(): result.ElasticResult[String] =
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
  _: RefreshApi with RestHighLevelClientCompanion =>

  override private[client] def executeCreateIndex(
    index: String,
    settings: String
  ): result.ElasticResult[Boolean] = {
    executeRestBooleanAction[CreateIndexRequest, AcknowledgedResponse](
      operation = "createIndex",
      index = Some(index),
      retryable = false
    )(
      request = new CreateIndexRequest(index).settings(settings, XContentType.JSON)
    )(
      executor = req => apply().indices().create(req, RequestOptions.DEFAULT)
    )
  }

  override private[client] def executeDeleteIndex(index: String): result.ElasticResult[Boolean] =
    executeRestBooleanAction[DeleteIndexRequest, AcknowledgedResponse](
      operation = "deleteIndex",
      index = Some(index),
      retryable = false
    )(
      request = new DeleteIndexRequest(index)
    )(
      executor = req => apply().indices().delete(req, RequestOptions.DEFAULT)
    )

  override private[client] def executeCloseIndex(index: String): result.ElasticResult[Boolean] =
    executeRestBooleanAction[CloseIndexRequest, AcknowledgedResponse](
      operation = "closeIndex",
      index = Some(index),
      retryable = false
    )(
      request = new CloseIndexRequest(index)
    )(
      executor = req => apply().indices().close(req, RequestOptions.DEFAULT)
    )

  override private[client] def executeOpenIndex(index: String): result.ElasticResult[Boolean] =
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
    refresh: Boolean
  ): result.ElasticResult[(Boolean, Option[Long])] =
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
          val json = new JsonParser()
            .parse(
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

  override private[client] def executeIndexExists(index: String): result.ElasticResult[Boolean] =
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

}

/** Alias management API for RestHighLevelClient
  * @see
  *   [[AliasApi]] for generic API documentation
  */
trait RestHighLevelClientAliasApi extends AliasApi with RestHighLevelClientHelpers {
  _: IndicesApi with RestHighLevelClientCompanion =>

  override private[client] def executeAddAlias(
    index: String,
    alias: String
  ): result.ElasticResult[Boolean] =
    executeRestBooleanAction(
      operation = "addAlias",
      index = Some(index),
      retryable = false
    )(
      request = new IndicesAliasesRequest()
        .addAliasAction(
          new AliasActions(AliasActions.Type.ADD)
            .index(index)
            .alias(alias)
        )
    )(
      executor = req => apply().indices().updateAliases(req, RequestOptions.DEFAULT)
    )

  override private[client] def executeRemoveAlias(
    index: String,
    alias: String
  ): result.ElasticResult[Boolean] =
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

  override private[client] def executeAliasExists(alias: String): result.ElasticResult[Boolean] =
    executeRestAction[GetAliasesRequest, GetAliasesResponse, Boolean](
      operation = "aliasExists",
      index = Some(alias),
      retryable = true
    )(
      request = new GetAliasesRequest().aliases(alias)
    )(
      executor = req => apply().indices().getAlias(req, RequestOptions.DEFAULT)
    )(response => !response.getAliases.isEmpty)

  override private[client] def executeGetAliases(index: String): result.ElasticResult[String] =
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
  ): result.ElasticResult[Boolean] =
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
  _: IndicesApi with RestHighLevelClientCompanion =>

  override private[client] def executeUpdateSettings(
    index: String,
    settings: String
  ): result.ElasticResult[Boolean] =
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

  override private[client] def executeLoadSettings(index: String): result.ElasticResult[String] =
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
  _: SettingsApi with IndicesApi with RefreshApi with RestHighLevelClientCompanion =>

  override private[client] def executeSetMapping(
    index: String,
    mapping: String
  ): result.ElasticResult[Boolean] =
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

  override private[client] def executeGetMapping(index: String): result.ElasticResult[String] =
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

}

/** Refresh API implementation for RestHighLevelClient
  * @see
  *   [[RefreshApi]] for generic API documentation
  */
trait RestHighLevelClientRefreshApi extends RefreshApi with RestHighLevelClientHelpers {
  _: RestHighLevelClientCompanion =>

  override private[client] def executeRefresh(index: String): result.ElasticResult[Boolean] =
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
  ): result.ElasticResult[Boolean] =
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
  ): result.ElasticResult[Option[Double]] =
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
  )(implicit ec: ExecutionContext): Future[result.ElasticResult[Option[Double]]] = {
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
  _: SettingsApi with RestHighLevelClientCompanion with SerializationApi =>
  override private[client] def executeIndex(
    index: String,
    id: String,
    source: String,
    wait: Boolean
  ): result.ElasticResult[Boolean] =
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
  ): Future[result.ElasticResult[Boolean]] =
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
  _: SettingsApi with RestHighLevelClientCompanion with SerializationApi =>
  override private[client] def executeUpdate(
    index: String,
    id: String,
    source: String,
    upsert: Boolean,
    wait: Boolean
  ): result.ElasticResult[Boolean] =
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
  )(implicit ec: ExecutionContext): Future[result.ElasticResult[Boolean]] =
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
  _: SettingsApi with RestHighLevelClientCompanion =>

  override private[client] def executeDelete(
    index: String,
    id: String,
    wait: Boolean
  ): result.ElasticResult[Boolean] =
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
  ): Future[result.ElasticResult[Boolean]] =
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
  _: RestHighLevelClientCompanion with SerializationApi =>
  override private[client] def executeGet(
    index: String,
    id: String
  ): result.ElasticResult[Option[String]] =
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
  ): Future[result.ElasticResult[Option[String]]] =
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
  _: ElasticConversion with RestHighLevelClientCompanion with SerializationApi =>

  override implicit def sqlSearchRequestToJsonQuery(sqlSearch: SQLSearchRequest): String =
    implicitly[ElasticSearchRequest](sqlSearch).query

  override private[client] def executeSingleSearch(
    elasticQuery: ElasticQuery
  ): result.ElasticResult[Option[String]] =
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
  ): result.ElasticResult[Option[String]] =
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
  )(implicit ec: ExecutionContext): Future[result.ElasticResult[Option[String]]] =
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
  )(implicit ec: ExecutionContext): Future[result.ElasticResult[Option[String]]] =
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
  _: RefreshApi with SettingsApi with IndexApi with RestHighLevelClientCompanion =>

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
  _: SearchApi with VersionApi with RestHighLevelClientCompanion =>

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
