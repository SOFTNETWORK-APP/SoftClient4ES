package app.softnetwork.elastic.client.rest

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import app.softnetwork.elastic.client._
import app.softnetwork.elastic.sql.SQLQuery
import app.softnetwork.elastic.{client, sql}
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
import org.elasticsearch.action.search.{MultiSearchRequest, SearchRequest, SearchResponse}
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
import org.elasticsearch.common.io.stream.InputStreamStreamInput
import org.elasticsearch.common.xcontent.{DeprecationHandler, XContentType}
import org.elasticsearch.rest.RestStatus
import org.elasticsearch.search.aggregations.bucket.filter.Filter
import org.elasticsearch.search.aggregations.bucket.nested.Nested
import org.elasticsearch.search.aggregations.metrics.avg.Avg
import org.elasticsearch.search.aggregations.metrics.cardinality.Cardinality
import org.elasticsearch.search.aggregations.metrics.max.Max
import org.elasticsearch.search.aggregations.metrics.min.Min
import org.elasticsearch.search.aggregations.metrics.sum.Sum
import org.elasticsearch.search.aggregations.metrics.valuecount.ValueCount
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.json4s.Formats

import java.io.ByteArrayInputStream
import scala.collection.JavaConverters.mapAsScalaMapConverter
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

trait RestHighLevelClientIndicesApi extends IndicesApi with RestHighLevelClientCompanion {
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

trait RestHighLevelClientAliasApi extends AliasApi with RestHighLevelClientCompanion {
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

trait RestHighLevelClientSettingsApi extends SettingsApi with RestHighLevelClientCompanion {
  _: RestHighLevelClientIndicesApi =>

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
      apply()
        .indices()
        .getSettings(
          new GetSettingsRequest().indices(index),
          RequestOptions.DEFAULT
        )
        .toString,
      s"""{"$index": {"settings": {"index": {}}}}"""
    )(logger)
  }
}

trait RestHighLevelClientMappingApi extends MappingApi with RestHighLevelClientCompanion {
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
}

trait RestHighLevelClientRefreshApi extends RefreshApi with RestHighLevelClientCompanion {
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

trait RestHighLevelClientFlushApi extends FlushApi with RestHighLevelClientCompanion {
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

trait RestHighLevelClientCountApi extends CountApi with RestHighLevelClientCompanion {
  override def countAsync(
    query: client.JSONQuery
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

  override def count(query: client.JSONQuery): Option[Double] = {
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
  override def aggregate(
    sqlQuery: SQLQuery
  )(implicit ec: ExecutionContext): Future[Seq[SingleValueAggregateResult]] = {
    val futures = for (aggregation <- sqlQuery.aggregations) yield {
      val promise: Promise[SingleValueAggregateResult] = Promise()
      val field = aggregation.field
      val sourceField = aggregation.sourceField
      val aggType = aggregation.aggType
      val aggName = aggregation.aggName
      val query = aggregation.query.getOrElse("")
      val sources = aggregation.sources
      sourceField match {
        case "_id" if aggType.sql == "count" =>
          countAsync(
            JSONQuery(
              query,
              collection.immutable.Seq(sources: _*),
              collection.immutable.Seq.empty[String]
            )
          ).onComplete {
            case Success(result) =>
              promise.success(
                SingleValueAggregateResult(
                  field,
                  aggType,
                  result.map(r => NumericValue(r.doubleValue())).getOrElse(EmptyValue),
                  None
                )
              )
            case Failure(f) =>
              logger.error(f.getMessage, f.fillInStackTrace())
              promise.success(
                SingleValueAggregateResult(field, aggType, EmptyValue, Some(f.getMessage))
              )
          }
          promise.future
        case _ =>
          val jsonQuery = JSONQuery(
            query,
            collection.immutable.Seq(sources: _*),
            collection.immutable.Seq.empty[String]
          )
          import jsonQuery._
          // Create a parser for the query
          val xContentParser = XContentType.JSON
            .xContent()
            .createParser(
              namedXContentRegistry,
              DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
              jsonQuery.query
            )
          apply().searchAsync(
            new SearchRequest(indices: _*)
              .types(types: _*)
              .source(
                SearchSourceBuilder.fromXContent(xContentParser)
              ),
            RequestOptions.DEFAULT,
            new ActionListener[SearchResponse] {
              override def onResponse(response: SearchResponse): Unit = {
                val agg = aggName.split("\\.").last

                val itAgg = aggName.split("\\.").iterator

                var root =
                  if (aggregation.nested) {
                    response.getAggregations.get(itAgg.next()).asInstanceOf[Nested].getAggregations
                  } else {
                    response.getAggregations
                  }

                if (aggregation.filtered) {
                  root = root.get(itAgg.next()).asInstanceOf[Filter].getAggregations
                }

                promise.success(
                  SingleValueAggregateResult(
                    field,
                    aggType,
                    aggType match {
                      case sql.Count =>
                        if (aggregation.distinct) {
                          NumericValue(root.get(agg).asInstanceOf[Cardinality].value())
                        } else {
                          NumericValue(root.get(agg).asInstanceOf[ValueCount].value())
                        }
                      case sql.Sum =>
                        NumericValue(root.get(agg).asInstanceOf[Sum].value())
                      case sql.Avg =>
                        NumericValue(root.get(agg).asInstanceOf[Avg].value())
                      case sql.Min =>
                        NumericValue(root.get(agg).asInstanceOf[Min].value())
                      case sql.Max =>
                        NumericValue(root.get(agg).asInstanceOf[Max].value())
                      case _ => EmptyValue
                    },
                    None
                  )
                )
              }

              override def onFailure(e: Exception): Unit = promise.failure(e)
            }
          )
          promise.future
      }
    }
    Future.sequence(futures)
  }
}

trait RestHighLevelClientIndexApi extends IndexApi with RestHighLevelClientCompanion {
  _: RestHighLevelClientRefreshApi =>
  override def index(index: String, indexType: String, id: String, source: String): Boolean = {
    tryOrElse(
      apply()
        .index(
          new IndexRequest(index)
            .`type`(indexType)
            .id(id)
            .source(source, XContentType.JSON),
          RequestOptions.DEFAULT
        )
        .status()
        .getStatus < 400,
      false
    )(logger)
  }

  override def indexAsync(index: String, indexType: String, id: String, source: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    val promise: Promise[Boolean] = Promise()
    apply().indexAsync(
      new IndexRequest(index)
        .`type`(indexType)
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

trait RestHighLevelClientUpdateApi extends UpdateApi with RestHighLevelClientCompanion {
  _: RestHighLevelClientRefreshApi =>
  override def update(
    index: String,
    indexType: String,
    id: String,
    source: String,
    upsert: Boolean
  ): Boolean = {
    tryOrElse(
      apply()
        .update(
          new UpdateRequest(index, indexType, id)
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
    indexType: String,
    id: String,
    source: String,
    upsert: Boolean
  )(implicit ec: ExecutionContext): Future[Boolean] = {
    val promise: Promise[Boolean] = Promise()
    apply().updateAsync(
      new UpdateRequest(index, indexType, id)
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

trait RestHighLevelClientDeleteApi extends DeleteApi with RestHighLevelClientCompanion {
  _: RestHighLevelClientRefreshApi =>

  override def delete(uuid: String, index: String, indexType: String): Boolean = {
    tryOrElse(
      apply()
        .delete(
          new DeleteRequest(index, indexType, uuid),
          RequestOptions.DEFAULT
        )
        .status()
        .getStatus < 400,
      false
    )(logger)
  }

  override def deleteAsync(uuid: String, index: String, indexType: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    val promise: Promise[Boolean] = Promise()
    apply().deleteAsync(
      new DeleteRequest(index, indexType, uuid),
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

trait RestHighLevelClientGetApi extends GetApi with RestHighLevelClientCompanion {
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
          maybeType.getOrElse("_all"),
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
        maybeType.getOrElse("_all"),
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

trait RestHighLevelClientSearchApi extends SearchApi with RestHighLevelClientCompanion {
  override def search[U](
    jsonQuery: JSONQuery
  )(implicit m: Manifest[U], formats: Formats): List[U] = {
    import jsonQuery._
    logger.info(s"Searching with query: $query on indices: ${indices.mkString(", ")}")
    // Create a parser for the query
    val xContentParser = XContentType.JSON
      .xContent()
      .createParser(
        namedXContentRegistry,
        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
        query
      )
    val response = apply().search(
      new SearchRequest(indices: _*)
        .types(types: _*)
        .source(
          SearchSourceBuilder.fromXContent(xContentParser)
        ),
      RequestOptions.DEFAULT
    )
    if (response.getHits.getTotalHits > 0) {
      response.getHits.getHits.toList.map { hit =>
        logger.info(s"Deserializing hit: ${hit.getSourceAsString}")
        serialization.read[U](hit.getSourceAsString)
      }
    } else {
      List.empty[U]
    }
  }

  override def searchAsync[U](
    sqlQuery: SQLQuery
  )(implicit m: Manifest[U], ec: ExecutionContext, formats: Formats): Future[List[U]] = {
    val promise = Promise[List[U]]()
    sqlQuery.search match {
      case Some(searchRequest) =>
        val indices = collection.immutable.Seq(searchRequest.sources: _*)
        val jsonQuery = JSONQuery(searchRequest.query, indices)
        import jsonQuery._
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
      case None =>
        promise.failure(
          new IllegalArgumentException(
            s"SQL query ${sqlQuery.query} does not contain a valid search request"
          )
        )
    }
    promise.future
  }

  override def searchWithInnerHits[U, I](jsonQuery: JSONQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[(U, List[I])] = {
    import jsonQuery._
    // Create a parser for the query
    val xContentParser = XContentType.JSON
      .xContent()
      .createParser(
        namedXContentRegistry,
        DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
        jsonQuery.query
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

  override def multiSearch[U](
    jsonQueries: JSONQueries
  )(implicit m: Manifest[U], formats: Formats): List[List[U]] = {
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
        List.empty[U]
      } else {
        response.getResponse.getHits.getHits.toList.map { hit =>
          serialization.read[U](hit.getSourceAsString)
        }
      }
    }
  }

  override def multiSearchWithInnerHits[U, I](jsonQueries: JSONQueries, innerField: String)(implicit
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
    with BulkApi {
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
