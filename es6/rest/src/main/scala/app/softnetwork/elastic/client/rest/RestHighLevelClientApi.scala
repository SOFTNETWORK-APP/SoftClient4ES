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
import app.softnetwork.elastic.sql.query.{SQLAggregation, SQLQuery, SQLSearchRequest}
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.client
import app.softnetwork.persistence.model.Timestamped
import app.softnetwork.serialization.serialization
import com.google.gson.JsonParser
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest
import org.elasticsearch.action.admin.indices.alias.IndicesAliasesRequest.AliasActions
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest
import org.elasticsearch.action.admin.indices.flush.FlushRequest
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest
import org.elasticsearch.action.bulk.{BulkItemResponse, BulkRequest, BulkResponse}
import org.elasticsearch.action.delete.{DeleteRequest, DeleteResponse}
import org.elasticsearch.action.get.{GetRequest, GetResponse}
import org.elasticsearch.action.index.{IndexRequest, IndexResponse}
import org.elasticsearch.action.search.{
  ClearScrollRequest,
  MultiSearchRequest,
  SearchRequest,
  SearchResponse,
  SearchScrollRequest
}
import org.elasticsearch.action.update.{UpdateRequest, UpdateResponse}
import org.elasticsearch.action.{ActionListener, DocWriteRequest}
import org.elasticsearch.client.{Request, RequestOptions}
import org.elasticsearch.client.core.{CountRequest, CountResponse}
import org.elasticsearch.client.indices.{
  CreateIndexRequest,
  GetIndexRequest,
  GetMappingsRequest,
  PutMappingRequest
}
import org.elasticsearch.common.Strings
import org.elasticsearch.common.io.stream.InputStreamStreamInput
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.{DeprecationHandler, XContentType}
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.elasticsearch.search.sort.{FieldSortBuilder, SortOrder}
import org.json4s.Formats

import java.io.{ByteArrayInputStream, IOException}
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
    with RestHighLevelClientSingleValueAggregateApi
    with RestHighLevelClientIndexApi
    with RestHighLevelClientUpdateApi
    with RestHighLevelClientDeleteApi
    with RestHighLevelClientGetApi
    with RestHighLevelClientSearchApi
    with RestHighLevelClientBulkApi
    with RestHighLevelClientScrollApi
    with RestHighLevelClientCompanion

trait RestHighLevelClientIndicesApi extends IndicesApi { _: RestHighLevelClientCompanion =>
  override def createIndex(index: String, settings: String): Boolean = {
    tryOrElse(
      apply()
        .indices()
        .create(
          new CreateIndexRequest(index)
            .settings(settings, XContentType.JSON),
          RequestOptions.DEFAULT
        )
        .isAcknowledged,
      false
    )(logger)
  }

  override def deleteIndex(index: String): Boolean = {
    tryOrElse(
      apply()
        .indices()
        .delete(new DeleteIndexRequest(index), RequestOptions.DEFAULT)
        .isAcknowledged,
      false
    )(logger)
  }

  override def openIndex(index: String): Boolean = {
    tryOrElse(
      apply().indices().open(new OpenIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged,
      false
    )(logger)
  }

  override def closeIndex(index: String): Boolean = {
    tryOrElse(
      apply().indices().close(new CloseIndexRequest(index), RequestOptions.DEFAULT).isAcknowledged,
      false
    )(logger)
  }

  /** Reindex from source index to target index.
    *
    * @param sourceIndex
    *   - the name of the source index
    * @param targetIndex
    *   - the name of the target index
    * @param refresh
    *   - true to refresh the target index after reindexing, false otherwise
    * @return
    *   true if the reindexing was successful, false otherwise
    */
  override def reindex(sourceIndex: String, targetIndex: String, refresh: Boolean): Boolean = {
    val request = new Request("POST", s"/_reindex?refresh=$refresh")
    request.setJsonEntity(
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
    tryOrElse(
      apply().getLowLevelClient.performRequest(request).getStatusLine.getStatusCode < 400,
      false
    )(logger)
  }

  /** Check if an index exists.
    *
    * @param index
    *   - the name of the index to check
    * @return
    *   true if the index exists, false otherwise
    */
  override def indexExists(index: String): Boolean = {
    tryOrElse(
      apply().indices().exists(new GetIndexRequest(index), RequestOptions.DEFAULT),
      false
    )(logger)
  }

}

trait RestHighLevelClientAliasApi extends AliasApi { _: RestHighLevelClientCompanion =>
  override def addAlias(index: String, alias: String): Boolean = {
    tryOrElse(
      apply()
        .indices()
        .updateAliases(
          new IndicesAliasesRequest()
            .addAliasAction(
              new AliasActions(AliasActions.Type.ADD)
                .index(index)
                .alias(alias)
            ),
          RequestOptions.DEFAULT
        )
        .isAcknowledged,
      false
    )(logger)
  }

  override def removeAlias(index: String, alias: String): Boolean = {
    tryOrElse(
      apply()
        .indices()
        .updateAliases(
          new IndicesAliasesRequest()
            .addAliasAction(
              new AliasActions(AliasActions.Type.REMOVE)
                .index(index)
                .alias(alias)
            ),
          RequestOptions.DEFAULT
        )
        .isAcknowledged,
      false
    )(logger)
  }
}

trait RestHighLevelClientSettingsApi extends SettingsApi {
  _: RestHighLevelClientIndicesApi with RestHighLevelClientCompanion =>

  override def updateSettings(index: String, settings: String): Boolean = {
    tryOrElse(
      apply()
        .indices()
        .putSettings(
          new UpdateSettingsRequest(index)
            .settings(settings, XContentType.JSON),
          RequestOptions.DEFAULT
        )
        .isAcknowledged,
      false
    )(logger)
  }

  override def loadSettings(index: String): String = {
    tryOrElse(
      {
        new JsonParser()
          .parse(
            apply()
              .indices()
              .getSettings(
                new GetSettingsRequest().indices(index),
                RequestOptions.DEFAULT
              )
              .toString
          )
          .getAsJsonObject
          .get(index)
          .getAsJsonObject
          .get("settings")
          .getAsJsonObject
          .get("index")
          .getAsJsonObject
          .toString
      },
      "{}"
    )(logger)
  }
}

trait RestHighLevelClientMappingApi extends MappingApi { _: RestHighLevelClientCompanion =>
  override def setMapping(index: String, mapping: String): Boolean = {
    tryOrElse(
      apply()
        .indices()
        .putMapping(
          new PutMappingRequest(index)
            .source(mapping, XContentType.JSON),
          RequestOptions.DEFAULT
        )
        .isAcknowledged,
      false
    )(logger)
  }

  override def getMapping(index: String): String = {
    tryOrElse(
      apply()
        .indices()
        .getMapping(
          new GetMappingsRequest().indices(index),
          RequestOptions.DEFAULT
        )
        .mappings()
        .asScala
        .get(index)
        .map(metadata => metadata.source().string()),
      None
    )(logger).getOrElse(s""""{$index: {"mappings": {}}}""")
  }

  override def getMappingProperties(index: String): String = {
    tryOrElse(
      getMapping(index),
      "{\"properties\": {}}"
    )(logger)
  }

}

trait RestHighLevelClientRefreshApi extends RefreshApi { _: RestHighLevelClientCompanion =>
  override def refresh(index: String): Boolean = {
    tryOrElse(
      apply()
        .indices()
        .refresh(
          new RefreshRequest(index),
          RequestOptions.DEFAULT
        )
        .getStatus
        .getStatus < 400,
      false
    )(logger)
  }
}

trait RestHighLevelClientFlushApi extends FlushApi { _: RestHighLevelClientCompanion =>
  override def flush(index: String, force: Boolean = true, wait: Boolean = true): Boolean = {
    tryOrElse(
      apply()
        .indices()
        .flush(
          new FlushRequest(index).force(force).waitIfOngoing(wait),
          RequestOptions.DEFAULT
        )
        .getStatus == RestStatus.OK,
      false
    )(logger)
  }
}

trait RestHighLevelClientCountApi extends CountApi { _: RestHighLevelClientCompanion =>
  override def countAsync(
    query: client.ElasticQuery
  )(implicit ec: ExecutionContext): Future[Option[Double]] = {
    val promise = Promise[Option[Double]]()
    apply().countAsync(
      new CountRequest().indices(query.indices: _*).types(query.types: _*),
      RequestOptions.DEFAULT,
      new ActionListener[CountResponse] {
        override def onResponse(response: CountResponse): Unit =
          promise.success(Option(response.getCount.toDouble))

        override def onFailure(e: Exception): Unit = promise.failure(e)
      }
    )
    promise.future
  }

  override def count(query: client.ElasticQuery): Option[Double] = {
    tryOrElse(
      Option(
        apply()
          .count(
            new CountRequest().indices(query.indices: _*).types(query.types: _*),
            RequestOptions.DEFAULT
          )
          .getCount
          .toDouble
      ),
      None
    )(logger)
  }
}

trait RestHighLevelClientSingleValueAggregateApi
    extends SingleValueAggregateApi
    with RestHighLevelClientCountApi {
  _: SearchApi with ElasticConversion with RestHighLevelClientCompanion =>
}

trait RestHighLevelClientIndexApi extends IndexApi {
  _: RestHighLevelClientRefreshApi with RestHighLevelClientCompanion =>
  override def index(index: String, id: String, source: String): Boolean = {
    tryOrElse(
      apply()
        .index(
          new IndexRequest(index)
            .`type`("_doc")
            .id(id)
            .source(source, XContentType.JSON),
          RequestOptions.DEFAULT
        )
        .status()
        .getStatus < 400,
      false
    )(logger)
  }

  override def indexAsync(index: String, id: String, source: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    val promise: Promise[Boolean] = Promise()
    apply().indexAsync(
      new IndexRequest(index)
        .`type`("_doc")
        .id(id)
        .source(source, XContentType.JSON),
      RequestOptions.DEFAULT,
      new ActionListener[IndexResponse] {
        override def onResponse(response: IndexResponse): Unit =
          promise.success(response.status().getStatus < 400)

        override def onFailure(e: Exception): Unit = promise.failure(e)
      }
    )
    promise.future
  }
}

trait RestHighLevelClientUpdateApi extends UpdateApi {
  _: RestHighLevelClientRefreshApi with RestHighLevelClientCompanion =>
  override def update(
    index: String,
    id: String,
    source: String,
    upsert: Boolean
  ): Boolean = {
    tryOrElse(
      apply()
        .update(
          new UpdateRequest(index, "_doc", id)
            .doc(source, XContentType.JSON)
            .docAsUpsert(upsert),
          RequestOptions.DEFAULT
        )
        .status()
        .getStatus < 400,
      false
    )(logger)
  }

  override def updateAsync(
    index: String,
    id: String,
    source: String,
    upsert: Boolean
  )(implicit ec: ExecutionContext): Future[Boolean] = {
    val promise: Promise[Boolean] = Promise()
    apply().updateAsync(
      new UpdateRequest(index, "_doc", id)
        .doc(source, XContentType.JSON)
        .docAsUpsert(upsert),
      RequestOptions.DEFAULT,
      new ActionListener[UpdateResponse] {
        override def onResponse(response: UpdateResponse): Unit =
          promise.success(response.status().getStatus < 400)

        override def onFailure(e: Exception): Unit = promise.failure(e)
      }
    )
    promise.future
  }
}

trait RestHighLevelClientDeleteApi extends DeleteApi {
  _: RestHighLevelClientRefreshApi with RestHighLevelClientCompanion =>

  override def delete(uuid: String, index: String): Boolean = {
    tryOrElse(
      apply()
        .delete(
          new DeleteRequest(index, "_doc", uuid),
          RequestOptions.DEFAULT
        )
        .status()
        .getStatus < 400,
      false
    )(logger)
  }

  override def deleteAsync(uuid: String, index: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    val promise: Promise[Boolean] = Promise()
    apply().deleteAsync(
      new DeleteRequest(index, "_doc", uuid),
      RequestOptions.DEFAULT,
      new ActionListener[DeleteResponse] {
        override def onResponse(response: DeleteResponse): Unit =
          promise.success(response.status().getStatus < 400)

        override def onFailure(e: Exception): Unit = promise.failure(e)
      }
    )
    promise.future
  }
}

trait RestHighLevelClientGetApi extends GetApi { _: RestHighLevelClientCompanion =>
  def get[U <: Timestamped](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit m: Manifest[U], formats: Formats): Option[U] = {
    Try(
      apply().get(
        new GetRequest(
          index.getOrElse(
            maybeType.getOrElse(
              m.runtimeClass.getSimpleName.toLowerCase
            )
          ),
          "_doc",
          id
        ),
        RequestOptions.DEFAULT
      )
    ) match {
      case Success(response) =>
        if (response.isExists) {
          val source = response.getSourceAsString
          logger.info(s"Deserializing response $source for id: $id, index: ${index
            .getOrElse("default")}, type: ${maybeType.getOrElse("_all")}")
          // Deserialize the source string to the expected type
          // Note: This assumes that the source is a valid JSON representation of U
          // and that the serialization library is capable of handling it.
          Try(serialization.read[U](source)) match {
            case Success(value) => Some(value)
            case Failure(f) =>
              logger.error(
                s"Failed to deserialize response $source for id: $id, index: ${index
                  .getOrElse("default")}, type: ${maybeType.getOrElse("_all")}",
                f
              )
              None
          }
        } else {
          None
        }
      case Failure(f) =>
        logger.error(
          s"Failed to get document with id: $id, index: ${index
            .getOrElse("default")}, type: ${maybeType.getOrElse("_all")}",
          f
        )
        None
    }
  }

  override def getAsync[U <: Timestamped](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit m: Manifest[U], ec: ExecutionContext, formats: Formats): Future[Option[U]] = {
    val promise = Promise[Option[U]]()
    apply().getAsync(
      new GetRequest(
        index.getOrElse(
          maybeType.getOrElse(
            m.runtimeClass.getSimpleName.toLowerCase
          )
        ),
        "_doc",
        id
      ),
      RequestOptions.DEFAULT,
      new ActionListener[GetResponse] {
        override def onResponse(response: GetResponse): Unit = {
          if (response.isExists) {
            promise.success(Some(serialization.read[U](response.getSourceAsString)))
          } else {
            promise.success(None)
          }
        }

        override def onFailure(e: Exception): Unit = promise.failure(e)
      }
    )
    promise.future
  }
}

trait RestHighLevelClientSearchApi extends SearchApi {
  _: ElasticConversion with RestHighLevelClientCompanion =>
  override implicit def sqlSearchRequestToJsonQuery(sqlSearch: SQLSearchRequest): String =
    implicitly[ElasticSearchRequest](sqlSearch).query

  /** Search for entities matching the given JSON query.
    *
    * @param elasticQuery
    *   - the JSON query to search for
    * @param fieldAliases
    *   - the field aliases to use for the search
    * @param aggregations
    *   - the aggregations to use for the search
    * @return
    *   the SQL search response containing the results of the query
    */
  override def search(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): ElasticResponse = {
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
    val response = apply().search(
      new SearchRequest(elasticQuery.indices: _*)
        .types(elasticQuery.types: _*)
        .source(
          SearchSourceBuilder.fromXContent(xContentParser)
        ),
      RequestOptions.DEFAULT
    )
    // Return the SQL search response
    val sqlResponse =
      ElasticResponse(query, Strings.toString(response), fieldAliases, aggregations)
    logger.info(s"Search response: $sqlResponse")
    sqlResponse
  }

  /** Perform a multi-search operation with the given JSON multi-search query.
    *
    * @param jsonQueries
    *   - the JSON multi-search query to perform
    * @param fieldAliases
    *   - the field aliases to use for the search
    * @param aggregations
    *   - the aggregations to use for the search
    * @return
    *   the SQL search response containing the results of the multi-search query
    */
  override def multisearch(
    jsonQueries: ElasticQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): ElasticResponse = {
    val request = new MultiSearchRequest()
    val queries = jsonQueries.queries.map(_.query)
    val query = queries.mkString("\n")
    jsonQueries.queries.zipWithIndex.map { case (q, i) =>
      val query = queries(i)
      logger.info(s"Searching with query ${i + 1}: $query on indices: ${q.indices
        .mkString(", ")}")
      request.add(
        new SearchRequest(q.indices: _*)
          .types(q.types: _*)
          .source(
            new SearchSourceBuilder(
              new InputStreamStreamInput(
                new ByteArrayInputStream(
                  query.getBytes()
                )
              )
            )
          )
      )
    }
    /*for (query <- jsonQueries.queries) {
      request.add(
        new SearchRequest(query.indices: _*)
          .types(query.types: _*)
          .source(
            new SearchSourceBuilder(
              new InputStreamStreamInput(
                new ByteArrayInputStream(
                  query.query.getBytes()
                )
              )
            )
          )
      )
    }*/
    val responses = apply().msearch(request, RequestOptions.DEFAULT)
    ElasticResponse(query, Strings.toString(responses), fieldAliases, aggregations)
  }

  override def searchAsyncAs[U](
    sqlQuery: SQLQuery
  )(implicit m: Manifest[U], ec: ExecutionContext, formats: Formats): Future[List[U]] = {
    val elasticQuery: ElasticQuery = sqlQuery
    import elasticQuery._
    val promise = Promise[List[U]]()
    logger.info(s"Searching with query: $query on indices: ${indices.mkString(", ")}")
    // Create a parser for the query
    val xContentParser = XContentType.JSON
      .xContent()
      .createParser(
        namedXContentRegistry,
        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
        query
      )
    // Execute the search asynchronously
    apply().searchAsync(
      new SearchRequest(indices: _*)
        .types(types: _*)
        .source(
          SearchSourceBuilder.fromXContent(xContentParser)
        ),
      RequestOptions.DEFAULT,
      new ActionListener[SearchResponse] {
        override def onResponse(response: SearchResponse): Unit = {
          if (response.getHits.getTotalHits > 0) {
            promise.success(response.getHits.getHits.toList.map { hit =>
              serialization.read[U](hit.getSourceAsString)
            })
          } else {
            promise.success(List.empty[U])
          }
        }

        override def onFailure(e: Exception): Unit = promise.failure(e)
      }
    )
    promise.future
  }

  override def searchWithInnerHits[U, I](elasticQuery: ElasticQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[(U, List[I])] = {
    import elasticQuery._
    // Create a parser for the query
    val xContentParser = XContentType.JSON
      .xContent()
      .createParser(
        namedXContentRegistry,
        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
        elasticQuery.query
      )
    val response = apply().search(
      new SearchRequest(indices: _*)
        .types(types: _*)
        .source(
          SearchSourceBuilder.fromXContent(xContentParser)
        ),
      RequestOptions.DEFAULT
    )
    Try(new JsonParser().parse(response.toString).getAsJsonObject ~> [U, I] innerField) match {
      case Success(s) => s
      case Failure(f) =>
        logger.error(f.getMessage, f)
        List.empty
    }
  }

  override def multisearchWithInnerHits[U, I](jsonQueries: ElasticQueries, innerField: String)(
    implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[List[(U, List[I])]] = {
    import jsonQueries._
    val request = new MultiSearchRequest()
    for (query <- queries) {
      request.add(
        new SearchRequest(query.indices: _*)
          .types(query.types: _*)
          .source(
            new SearchSourceBuilder(
              new InputStreamStreamInput(
                new ByteArrayInputStream(
                  query.query.getBytes()
                )
              )
            )
          )
      )
    }
    val responses = apply().msearch(request, RequestOptions.DEFAULT)
    responses.getResponses.toList.map { response =>
      if (response.isFailure) {
        logger.error(s"Error in multi search: ${response.getFailureMessage}")
        List.empty[(U, List[I])]
      } else {
        Try(
          new JsonParser().parse(response.getResponse.toString).getAsJsonObject ~> [U, I] innerField
        ) match {
          case Success(s) => s
          case Failure(f) =>
            logger.error(f.getMessage, f)
            List.empty
        }
      }
    }
  }

}

trait RestHighLevelClientBulkApi
    extends RestHighLevelClientRefreshApi
    with RestHighLevelClientSettingsApi
    with RestHighLevelClientIndicesApi
    with BulkApi { _: RestHighLevelClientCompanion =>
  override type A = DocWriteRequest[_]
  override type R = BulkResponse

  override def toBulkAction(bulkItem: BulkItem): A = {
    import bulkItem._
    val request = action match {
      case BulkAction.UPDATE =>
        val r = new UpdateRequest(index, null, if (id.isEmpty) null else id.get)
          .doc(body, XContentType.JSON)
          .docAsUpsert(true)
        parent.foreach(r.parent)
        r
      case BulkAction.DELETE =>
        val r = new DeleteRequest(index).id(id.getOrElse("_all"))
        parent.foreach(r.parent)
        r
      case _ =>
        val r = new IndexRequest(index).source(body, XContentType.JSON)
        id.foreach(r.id)
        parent.foreach(r.parent)
        r
    }
    request
  }

  override def bulkResult: Flow[R, Set[String], NotUsed] =
    Flow[BulkResponse]
      .named("result")
      .map(result => {
        val items = result.getItems
        val grouped = items.groupBy(_.getIndex)
        val indices = grouped.keys.toSet
        for (index <- indices) {
          logger
            .info(s"Bulk operation succeeded for index $index with ${grouped(index).length} items.")
        }
        indices
      })

  override def bulk(implicit
    bulkOptions: BulkOptions,
    system: ActorSystem
  ): Flow[Seq[A], R, NotUsed] = {
    val parallelism = Math.max(1, bulkOptions.balance)
    Flow[Seq[A]]
      .named("bulk")
      .mapAsyncUnordered[R](parallelism) { items =>
        val request = new BulkRequest(bulkOptions.index, bulkOptions.documentType)
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

  private[this] def toBulkElasticResultItem(i: BulkItemResponse): BulkElasticResultItem =
    new BulkElasticResultItem {
      override def index: String = i.getIndex
    }

  override implicit def toBulkElasticAction(a: DocWriteRequest[_]): BulkElasticAction = {
    new BulkElasticAction {
      override def index: String = a.index
    }
  }

  override implicit def toBulkElasticResult(r: BulkResponse): BulkElasticResult = {
    new BulkElasticResult {
      override def items: List[BulkElasticResultItem] =
        r.getItems.toList.map(toBulkElasticResultItem)
    }
  }
}

trait RestHighLevelClientScrollApi extends ScrollApi { _: RestHighLevelClientCompanion =>

  /** Classic scroll (works for both hits and aggregations)
    */
  override def scrollClassic(
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
                  TimeValue.parseTimeValue(config.scrollTimeout, "scroll_timeout")
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
                  TimeValue.parseTimeValue(config.scrollTimeout, "scroll_timeout")
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
    * @note
    *   Uses Array[Object] for searchAfter values to match RestHighLevelClient API
    */
  override def searchAfter(
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

  /** Extract ALL results: hits + aggregations This is crucial for queries with aggregations
    */
  private def extractAllResults(
    response: SearchResponse,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): Seq[Map[String, Any]] = {
    val jsonString = response.toString
    val sqlResponse = ElasticResponse("", jsonString, fieldAliases, aggregations)

    parseResponse(sqlResponse) match {
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
    val sqlResponse = ElasticResponse("", jsonString, fieldAliases, Map.empty)

    parseResponse(sqlResponse) match {
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
