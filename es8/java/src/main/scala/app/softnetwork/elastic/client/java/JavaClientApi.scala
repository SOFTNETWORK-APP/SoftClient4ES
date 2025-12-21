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

package app.softnetwork.elastic.client.java

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl
import akka.stream.scaladsl.{Flow, Source}
import app.softnetwork.elastic.client._
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.scroll._
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.query.{SQLAggregation, SingleSearch}
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticResult, ElasticSuccess}
import co.elastic.clients.elasticsearch._types.mapping.TypeMapping
import co.elastic.clients.elasticsearch._types.{
  FieldSort,
  FieldValue,
  Refresh,
  Result,
  SortOptions,
  SortOrder,
  Time
}
import co.elastic.clients.elasticsearch.core.bulk.{
  BulkOperation,
  DeleteOperation,
  IndexOperation,
  UpdateAction,
  UpdateOperation
}
import co.elastic.clients.elasticsearch.core.msearch.{
  MultisearchBody,
  MultisearchHeader,
  RequestItem
}
import co.elastic.clients.elasticsearch.core._
import co.elastic.clients.elasticsearch.core.reindex.{Destination, Source => ESSource}
import co.elastic.clients.elasticsearch.core.search.PointInTimeReference
import co.elastic.clients.elasticsearch.indices.update_aliases.{Action, AddAction, RemoveAction}
import co.elastic.clients.elasticsearch.indices.{ExistsRequest => IndexExistsRequest, _}
import co.elastic.clients.elasticsearch.ingest.{
  DeletePipelineRequest,
  GetPipelineRequest,
  PutPipelineRequest
}
import com.google.gson.JsonParser

import _root_.java.io.{IOException, StringReader}
import _root_.java.util.{Map => JMap}
import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

trait JavaClientApi
    extends ElasticClientApi
    with JavaClientIndicesApi
    with JavaClientAliasApi
    with JavaClientSettingsApi
    with JavaClientMappingApi
    with JavaClientRefreshApi
    with JavaClientFlushApi
    with JavaClientCountApi
    with JavaClientIndexApi
    with JavaClientUpdateApi
    with JavaClientDeleteApi
    with JavaClientGetApi
    with JavaClientSearchApi
    with JavaClientBulkApi
    with JavaClientScrollApi
    with JavaClientCompanion
    with JavaClientVersionApi
    with JavaClientPipelineApi

/** Elasticsearch client implementation using the Java Client
  * @see
  *   [[VersionApi]] for version information
  */
trait JavaClientVersionApi extends VersionApi with JavaClientHelpers {
  _: SerializationApi with JavaClientCompanion =>
  override private[client] def executeVersion(): result.ElasticResult[String] =
    executeJavaAction(
      operation = "version",
      index = None,
      retryable = true
    )(
      apply().info()
    ) { response =>
      response.version().number()
    }
}

/** Elasticsearch client implementation of Indices API using the Java Client
  * @see
  *   [[IndicesApi]] for index management operations
  */
trait JavaClientIndicesApi extends IndicesApi with RefreshApi with JavaClientHelpers {
  _: JavaClientCompanion =>
  override private[client] def executeCreateIndex(
    index: String,
    settings: String,
    mappings: Option[String],
    aliases: Seq[String]
  ): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "createIndex",
      index = Some(index),
      retryable = false
    )(
      apply()
        .indices()
        .create(
          new CreateIndexRequest.Builder()
            .index(index)
            .settings(new IndexSettings.Builder().withJson(new StringReader(settings)).build())
            .aliases(aliases.map(key => (key, new Alias.Builder().build())).toMap.asJava)
            .mappings(
              new TypeMapping.Builder()
                .withJson(
                  new StringReader(mappings.getOrElse("{}"))
                )
                .build()
            )
            .build()
        )
    )(_.acknowledged())

  override private[client] def executeDeleteIndex(index: String): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "deleteIndex",
      index = Some(index),
      retryable = false
    )(
      apply()
        .indices()
        .delete(new DeleteIndexRequest.Builder().index(index).build())
    )(_.acknowledged())

  override private[client] def executeCloseIndex(index: String): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "closeIndex",
      index = Some(index),
      retryable = false
    )(
      apply()
        .indices()
        .close(new CloseIndexRequest.Builder().index(index).build())
    )(_.acknowledged())

  override private[client] def executeOpenIndex(index: String): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "openIndex",
      index = Some(index),
      retryable = false
    )(
      apply()
        .indices()
        .open(new OpenRequest.Builder().index(index).build())
    )(_.acknowledged())

  override private[client] def executeReindex(
    sourceIndex: String,
    targetIndex: String,
    refresh: Boolean,
    pipeline: Option[String]
  ): result.ElasticResult[(Boolean, Option[Long])] =
    executeJavaAction(
      operation = "reindex",
      index = Some(s"$sourceIndex -> $targetIndex"),
      retryable = false
    )(
      apply()
        .reindex(
          new ReindexRequest.Builder()
            .source(new ESSource.Builder().index(sourceIndex).build())
            .dest(new Destination.Builder().index(targetIndex).build())
            .refresh(refresh)
            .build()
        )
    ) { response =>
      val failures = response.failures().asScala.map(_.cause().reason())
      if (failures.nonEmpty) {
        logger.error(
          s"Reindexing from $sourceIndex to $targetIndex failed with errors: ${failures.take(10).mkString(", ")}"
        )
      }
      (failures.isEmpty, Option(response.total()))
    }

  override private[client] def executeIndexExists(index: String): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "indexExists",
      index = Some(index),
      retryable = false
    )(
      apply()
        .indices()
        .exists(
          new IndexExistsRequest.Builder().index(index).build()
        )
    )(_.value())

}

/** Elasticsearch client implementation of Alias API using the Java Client
  * @see
  *   [[AliasApi]] for alias management operations
  */
trait JavaClientAliasApi extends AliasApi with JavaClientHelpers {
  _: IndicesApi with JavaClientCompanion =>

  override private[client] def executeAddAlias(
    index: String,
    alias: String
  ): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "addAlias",
      index = Some(index),
      retryable = false
    )(
      apply()
        .indices()
        .updateAliases(
          new UpdateAliasesRequest.Builder()
            .actions(
              new Action.Builder()
                .add(new AddAction.Builder().index(index).alias(alias).build())
                .build()
            )
            .build()
        )
    )(_.acknowledged())

  override private[client] def executeRemoveAlias(
    index: String,
    alias: String
  ): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "removeAlias",
      index = Some(index),
      retryable = false
    )(
      apply()
        .indices()
        .updateAliases(
          new UpdateAliasesRequest.Builder()
            .actions(
              new Action.Builder()
                .remove(new RemoveAction.Builder().index(index).alias(alias).build())
                .build()
            )
            .build()
        )
    )(_.acknowledged())

  override private[client] def executeAliasExists(alias: String): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "aliasExists",
      index = None,
      retryable = false
    )(
      apply()
        .indices()
        .existsAlias(
          new ExistsAliasRequest.Builder().name(alias).build()
        )
    )(_.value())

  override private[client] def executeGetAliases(index: String): result.ElasticResult[String] =
    executeJavaAction(
      operation = "getAliases",
      index = Some(index),
      retryable = false
    )(
      apply()
        .indices()
        .getAlias(
          new GetAliasRequest.Builder().index(index).build()
        )
    )(response => convertToJson(response))

  override private[client] def executeSwapAlias(
    oldIndex: String,
    newIndex: String,
    alias: String
  ): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "swapAlias",
      index = Some(s"$oldIndex <-> $newIndex"),
      retryable = false
    )(
      apply()
        .indices()
        .updateAliases(
          new UpdateAliasesRequest.Builder()
            .actions(
              List(
                new Action.Builder()
                  .remove(new RemoveAction.Builder().index(oldIndex).alias(alias).build())
                  .build(),
                new Action.Builder()
                  .add(new AddAction.Builder().index(newIndex).alias(alias).build())
                  .build()
              ).asJava
            )
            .build()
        )
    )(_.acknowledged())

}

/** Elasticsearch client implementation of Settings API using the Java Client
  * @see
  *   [[SettingsApi]] for settings management operations
  */
trait JavaClientSettingsApi extends SettingsApi with JavaClientHelpers {
  _: IndicesApi with JavaClientCompanion =>

  override private[client] def executeUpdateSettings(
    index: String,
    settings: String
  ): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "updateSettings",
      index = Some(index),
      retryable = false
    )(
      apply()
        .indices()
        .putSettings(
          new PutIndicesSettingsRequest.Builder()
            .index(index)
            .settings(new IndexSettings.Builder().withJson(new StringReader(settings)).build())
            .build()
        )
    )(_.acknowledged())

  override private[client] def executeLoadSettings(index: String): result.ElasticResult[String] =
    executeJavaAction(
      operation = "loadSettings",
      index = Some(index),
      retryable = true
    )(
      apply()
        .indices()
        .getSettings(
          new GetIndicesSettingsRequest.Builder().index(index).build()
        )
    )(response => convertToJson(response))

}

/** Elasticsearch client implementation of Mapping API using the Java Client
  * @see
  *   [[MappingApi]] for mapping management operations
  */
trait JavaClientMappingApi extends MappingApi with JavaClientHelpers {
  _: SettingsApi with IndicesApi with RefreshApi with JavaClientCompanion =>

  override private[client] def executeSetMapping(
    index: String,
    mapping: String
  ): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "setMapping",
      index = Some(index),
      retryable = false
    )(
      apply()
        .indices()
        .putMapping(
          new PutMappingRequest.Builder().index(index).withJson(new StringReader(mapping)).build()
        )
    )(_.acknowledged())

  override private[client] def executeGetMapping(index: String): result.ElasticResult[String] =
    executeJavaAction(
      operation = "getMapping",
      index = Some(index),
      retryable = true
    )(
      apply()
        .indices()
        .getMapping(
          new GetMappingRequest.Builder().index(index).build()
        )
    ) { response =>
      val valueOpt = response.result().asScala.get(index)
      valueOpt match {
        case Some(value) => convertToJson(value)
        case None        => """"{"properties": {}}"""
      }
    }

  /** Get the mapping properties of an index.
    *
    * @param index
    *   - the name of the index to get the mapping properties for
    * @return
    *   the mapping properties of the index as a JSON string
    */
  override def getMappingProperties(index: String): ElasticResult[String] = {
    getMapping(index).flatMap { jsonString =>
      // ✅ Extracting mapping from JSON
      ElasticResult.attempt(
        JsonParser.parseString(jsonString).getAsJsonObject
      ) match {
        case ElasticFailure(error) =>
          logger.error(s"❌ Failed to parse JSON mapping for index '$index': ${error.message}")
          return ElasticFailure(error.copy(operation = Some("getMapping"), index = Some(index)))
        case ElasticSuccess(indexObj) =>
          val settingsObj = indexObj
            .getAsJsonObject("mappings")
          ElasticSuccess(settingsObj.toString)
      }
    }
  }
}

/** Elasticsearch client implementation of Refresh API using the Java Client
  * @see
  *   [[RefreshApi]] for index refresh operations
  */
trait JavaClientRefreshApi extends RefreshApi with JavaClientHelpers {
  _: JavaClientCompanion =>

  override private[client] def executeRefresh(index: String): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "refresh",
      index = Some(index),
      retryable = false
    )(
      apply()
        .indices()
        .refresh(
          new RefreshRequest.Builder().index(index).build()
        )
    )(
      _.shards()
        .failed()
        .intValue() == 0
    )

}

/** Elasticsearch client implementation of Flush API using the Java Client
  * @see
  *   [[FlushApi]] for index flush operations
  */
trait JavaClientFlushApi extends FlushApi with JavaClientHelpers {
  _: JavaClientCompanion =>

  override private[client] def executeFlush(
    index: String,
    force: Boolean,
    wait: Boolean
  ): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "flush",
      index = Some(index),
      retryable = false
    )(
      apply()
        .indices()
        .flush(
          new FlushRequest.Builder().index(index).force(force).waitIfOngoing(wait).build()
        )
    )(
      _.shards()
        .failed()
        .intValue() == 0
    )

}

/** Elasticsearch client implementation of Count API using the Java Client
  * @see
  *   [[CountApi]] for count operations
  */
trait JavaClientCountApi extends CountApi with JavaClientHelpers {
  _: JavaClientCompanion =>

  override private[client] def executeCount(
    query: ElasticQuery
  ): result.ElasticResult[Option[Double]] =
    executeJavaAction(
      operation = "count",
      index = Some(query.indices.mkString(",")),
      retryable = true
    )(
      apply()
        .count(
          new CountRequest.Builder().index(query.indices.asJava).build()
        )
    ) { response =>
      Option(response.count().toDouble)
    }

  override private[client] def executeCountAsync(
    query: ElasticQuery
  )(implicit ec: ExecutionContext): Future[result.ElasticResult[Option[Double]]] =
    fromCompletableFuture(
      async()
        .count(
          new CountRequest.Builder().index(query.indices.asJava).build()
        )
    ).map { response =>
      result.ElasticSuccess(Option(response.count().toDouble))
    }

}

/** Elasticsearch client implementation of Index API using the Java Client
  * @see
  *   [[IndexApi]] for index operations
  */
trait JavaClientIndexApi extends IndexApi with JavaClientHelpers {
  _: SettingsApi with JavaClientCompanion with SerializationApi =>

  override private[client] def executeIndex(
    index: String,
    id: String,
    source: String,
    wait: Boolean
  ): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "index",
      index = Some(index),
      retryable = false
    )(
      apply()
        .index(
          new IndexRequest.Builder()
            .index(index)
            .id(id)
            .withJson(new StringReader(source))
            .refresh(if (wait) Refresh.WaitFor else Refresh.False)
            .build()
        )
    )(resp =>
      resp.result() match {
        case Result.Created | Result.Updated | Result.NoOp => true
        case _                                             => false
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
    fromCompletableFuture(
      async()
        .index(
          new IndexRequest.Builder()
            .index(index)
            .id(id)
            .withJson(new StringReader(source))
            .refresh(if (wait) Refresh.WaitFor else Refresh.False)
            .build()
        )
    ).map { resp =>
      resp.result() match {
        case Result.Created | Result.Updated | Result.NoOp => result.ElasticSuccess(true)
        case _                                             => result.ElasticSuccess(false)
      }
    }
}

/** Elasticsearch client implementation of Update API using the Java Client
  * @see
  *   [[UpdateApi]] for update operations
  */
trait JavaClientUpdateApi extends UpdateApi with JavaClientHelpers {
  _: SettingsApi with JavaClientCompanion with SerializationApi =>

  override private[client] def executeUpdate(
    index: String,
    id: String,
    source: String,
    upsert: Boolean,
    wait: Boolean
  ): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "update",
      index = Some(index),
      retryable = false
    )(
      apply()
        .update(
          new UpdateRequest.Builder[JMap[String, Object], JMap[String, Object]]()
            .index(index)
            .id(id)
            .doc(mapper.readValue(source, classOf[JMap[String, Object]]))
            .docAsUpsert(upsert)
            .refresh(if (wait) Refresh.WaitFor else Refresh.False)
            .build(),
          classOf[JMap[String, Object]]
        )
    )(resp =>
      resp.result() match {
        case Result.Created | Result.Updated | Result.NoOp => true
        case Result.NotFound =>
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
    fromCompletableFuture(
      async()
        .update(
          new UpdateRequest.Builder[JMap[String, Object], JMap[String, Object]]()
            .index(index)
            .id(id)
            .doc(mapper.readValue(source, classOf[JMap[String, Object]]))
            .docAsUpsert(upsert)
            .refresh(if (wait) Refresh.WaitFor else Refresh.False)
            .build(),
          classOf[JMap[String, Object]]
        )
    ).map { resp =>
      resp.result() match {
        case Result.Created | Result.Updated | Result.NoOp => result.ElasticSuccess(true)
        case Result.NotFound =>
          result.ElasticFailure(
            result.ElasticError(
              s"Document with id: $id not found in index: $index"
            ) // if upsert is false
          )
        case _ => result.ElasticSuccess(false)
      }
    }

}

/** Elasticsearch client implementation of Delete API using the Java Client
  * @see
  *   [[DeleteApi]] for delete operations
  */
trait JavaClientDeleteApi extends DeleteApi with JavaClientHelpers {
  _: SettingsApi with JavaClientCompanion =>

  override private[client] def executeDelete(
    index: String,
    id: String,
    wait: Boolean
  ): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "delete",
      index = Some(index),
      retryable = false
    )(
      apply()
        .delete(
          new DeleteRequest.Builder()
            .index(index)
            .id(id)
            .refresh(if (wait) Refresh.WaitFor else Refresh.False)
            .build()
        )
    )(resp =>
      resp.result() match {
        case Result.Deleted | Result.NoOp => true
        case _                            => false
      }
    )

  override private[client] def executeDeleteAsync(index: String, id: String, wait: Boolean)(implicit
    ec: ExecutionContext
  ): Future[result.ElasticResult[Boolean]] =
    fromCompletableFuture(
      async()
        .delete(
          new DeleteRequest.Builder()
            .index(index)
            .id(id)
            .refresh(if (wait) Refresh.WaitFor else Refresh.False)
            .build()
        )
    ).map { resp =>
      resp.result() match {
        case Result.Deleted | Result.NoOp => result.ElasticSuccess(true)
        case _                            => result.ElasticSuccess(false)
      }
    }

}

/** Elasticsearch client implementation of Get API using the Java Client
  * @see
  *   [[GetApi]] for get operations
  */
trait JavaClientGetApi extends GetApi with JavaClientHelpers {
  _: JavaClientCompanion with SerializationApi =>

  override private[client] def executeGet(
    index: String,
    id: String
  ): result.ElasticResult[Option[String]] =
    executeJavaAction(
      operation = "get",
      index = Some(index),
      retryable = true
    )(
      apply()
        .get(
          new GetRequest.Builder()
            .index(index)
            .id(id)
            .build(),
          classOf[JMap[String, Object]]
        )
    ) { response =>
      if (response.found()) {
        Some(mapper.writeValueAsString(response.source()))
      } else {
        None
      }
    }

  override private[client] def executeGetAsync(index: String, id: String)(implicit
    ec: ExecutionContext
  ): Future[result.ElasticResult[Option[String]]] =
    fromCompletableFuture(
      async()
        .get(
          new GetRequest.Builder()
            .index(index)
            .id(id)
            .build(),
          classOf[JMap[String, Object]]
        )
    ).map { response =>
      if (response.found()) {
        result.ElasticSuccess(Some(mapper.writeValueAsString(response.source())))
      } else {
        result.ElasticSuccess(None)
      }
    }

}

/** Elasticsearch client implementation of Search API using the Java Client
  * @see
  *   [[SearchApi]] for search operations
  */
trait JavaClientSearchApi extends SearchApi with JavaClientHelpers {
  _: JavaClientCompanion with SerializationApi =>

  override implicit def sqlSearchRequestToJsonQuery(sqlSearch: SingleSearch)(implicit
    timestamp: Long
  ): String =
    implicitly[ElasticSearchRequest](sqlSearch).query

  override private[client] def executeSingleSearch(
    elasticQuery: ElasticQuery
  ): result.ElasticResult[Option[String]] =
    executeJavaAction(
      operation = "singleSearch",
      index = Some(elasticQuery.indices.mkString(",")),
      retryable = true
    )(
      apply()
        .search(
          new SearchRequest.Builder()
            .index(elasticQuery.indices.asJava)
            .withJson(
              new StringReader(elasticQuery.query)
            )
            .build(),
          classOf[JMap[String, Object]]
        )
    )(resp => Some(convertToJson(resp)))

  override private[client] def executeMultiSearch(
    elasticQueries: ElasticQueries
  ): result.ElasticResult[Option[String]] =
    executeJavaAction(
      operation = "multiSearch",
      index = Some(elasticQueries.queries.flatMap(_.indices).distinct.mkString(",")),
      retryable = true
    ) {
      val items = elasticQueries.queries.map { q =>
        new RequestItem.Builder()
          .header(new MultisearchHeader.Builder().index(q.indices.asJava).build())
          .body(new MultisearchBody.Builder().withJson(new StringReader(q.query)).build())
          .build()
      }

      val request = new MsearchRequest.Builder().searches(items.asJava).build()
      apply().msearch(request, classOf[JMap[String, Object]])
    }(resp => Some(convertToJson(resp)))

  override private[client] def executeSingleSearchAsync(
    elasticQuery: ElasticQuery
  )(implicit ec: ExecutionContext): Future[result.ElasticResult[Option[String]]] =
    fromCompletableFuture(
      async()
        .search(
          new SearchRequest.Builder()
            .index(elasticQuery.indices.asJava)
            .withJson(new StringReader(elasticQuery.query))
            .build(),
          classOf[JMap[String, Object]]
        )
    ).map { response =>
      result.ElasticSuccess(Some(convertToJson(response)))
    }

  override private[client] def executeMultiSearchAsync(
    elasticQueries: ElasticQueries
  )(implicit ec: ExecutionContext): Future[result.ElasticResult[Option[String]]] =
    fromCompletableFuture {
      val items = elasticQueries.queries.map { q =>
        new RequestItem.Builder()
          .header(new MultisearchHeader.Builder().index(q.indices.asJava).build())
          .body(new MultisearchBody.Builder().withJson(new StringReader(q.query)).build())
          .build()
      }

      val request = new MsearchRequest.Builder().searches(items.asJava).build()
      async().msearch(request, classOf[JMap[String, Object]])
    }
      .map { response =>
        result.ElasticSuccess(Some(convertToJson(response)))
      }

}

/** Elasticsearch client implementation of Bulk API using the Java Client
  * @see
  *   [[BulkApi]] for bulk operations
  */
trait JavaClientBulkApi extends BulkApi with JavaClientHelpers {
  _: RefreshApi with SettingsApi with IndexApi with JavaClientCompanion =>
  override type BulkActionType = BulkOperation
  override type BulkResultType = BulkResponse

  override implicit private[client] def toBulkElasticAction(a: BulkOperation): BulkElasticAction =
    new BulkElasticAction {
      override def index: String = {
        a match {
          case op if op.isIndex  => op.index().index()
          case op if op.isDelete => op.delete().index()
          case op if op.isUpdate => op.update().index()
          case _ =>
            throw new IllegalArgumentException(s"Unsupported bulk operation type: ${a.getClass}")
        }
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
  ): Flow[Seq[A], R, NotUsed] = {
    val parallelism = Math.max(1, bulkOptions.balance)
    Flow[Seq[A]]
      .named("bulk")
      .mapAsyncUnordered[R](parallelism) { items =>
        val request =
          new BulkRequest.Builder().index(bulkOptions.defaultIndex).operations(items.asJava).build()
        Try(apply().bulk(request)) match {
          case Success(response) =>
            if (response.errors()) {
              val failedItems = response.items().asScala.filter(_.status() >= 400)
              if (failedItems.nonEmpty) {
                val errorMessages =
                  failedItems
                    .take(10)
                    .map(i => s"(${i.index()}, ${i.id()}) -> ${i.error().reason()}")
                    .mkString(", ")
                logger.error(s"Bulk operation failed for items: $errorMessages")
              } else {
                logger.warn("Bulk operation reported errors but no failed items found")
              }
            }
            Future.successful(response)
          case Failure(exception) =>
            logger.error(s"Bulk operation failed : ${exception.getMessage}")
            Future.failed(exception)
        }
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
    result: BulkResponse,
    originalBatch: Seq[BulkItem]
  ): Seq[Either[FailedDocument, SuccessfulDocument]] = {
    // no results at all
    if (
      originalBatch.nonEmpty &&
      (result == null || (result.items() == null || result.items().isEmpty))
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
    val failedItems =
      result
        .items()
        .asScala
        .filter(item => Option(item.error()).isDefined)
        .map { item =>
          val errorStatus = item.status()
          val errorType = item.error().`type`
          val errorReason = item.error().reason()

          val originalItemOpt = originalBatch.find { originalItem =>
            originalItem.index == item.index() && originalItem.id.contains(item.id())
          }

          // Determine if the error is retryable
          val isRetryable =
            originalItemOpt.isDefined && (BulkErrorAnalyzer.isRetryable(errorStatus) ||
            BulkErrorAnalyzer.isRetryableByType(errorType))

          val document = originalItemOpt.map(_.document).getOrElse("")
          Left(
            FailedDocument(
              id = item.id(),
              index = item.index(),
              document = document,
              error = BulkError(
                message = errorReason,
                `type` = errorType,
                status = errorStatus
              ),
              retryable = isRetryable
            )
          )
        }
        .toSeq

    // process successful items
    val successfulItems = result
      .items()
      .asScala
      .filter(item => Option(item.error()).isEmpty)
      .map { item =>
        Right(
          SuccessfulDocument(
            id = item.id(),
            index = item.index()
          )
        )
      }
      .toSeq

    val results = failedItems ++ successfulItems

    // if no individual results but overall failure, mark all as failed
    if (results.isEmpty && originalBatch.nonEmpty) {
      logger.error("Bulk operation failed with no individual item results")
      return originalBatch.map { item =>
        Left(
          FailedDocument(
            id = item.id.getOrElse("unknown"),
            index = item.index,
            document = item.document,
            error = BulkError(
              message = "Bulk operation failed with no individual item results",
              `type` = "internal_error",
              status = 500
            ),
            retryable = false
          )
        )
      }
    }

    results
  }

  override private[client] def toBulkAction(bulkItem: BulkItem): A = {
    import bulkItem._

    action match {
      case BulkAction.UPDATE =>
        new BulkOperation.Builder()
          .update(
            new UpdateOperation.Builder()
              .index(bulkItem.index)
              .id(id.orNull)
              .action(
                new UpdateAction.Builder[JMap[String, Object], JMap[String, Object]]()
                  .doc(mapper.readValue(document, classOf[JMap[String, Object]]))
                  .docAsUpsert(true)
                  .build()
              )
              .build()
          )
          .build()

      case BulkAction.DELETE =>
        val deleteId = id.getOrElse {
          throw new IllegalArgumentException(s"Missing id for delete on index ${bulkItem.index}")
        }
        new BulkOperation.Builder()
          .delete(new DeleteOperation.Builder().index(bulkItem.index).id(deleteId).build())
          .build()

      case _ =>
        new BulkOperation.Builder()
          .index(
            new IndexOperation.Builder[JMap[String, Object]]()
              .index(bulkItem.index)
              .id(id.orNull)
              .document(mapper.readValue(document, classOf[JMap[String, Object]]))
              .build()
          )
          .build()
    }
  }

  /** Conversion BulkActionType -> BulkItem */
  override private[client] def actionToBulkItem(action: BulkActionType): BulkItem =
    action match {
      case op if op.isIndex =>
        BulkItem(
          index = op.index().index(),
          id = Option(op.index().id()),
          document = mapper.writeValueAsString(op.index().document()),
          action = BulkAction.INDEX,
          parent = None
        )
      case op if op.isDelete =>
        BulkItem(
          index = op.delete().index(),
          id = Some(op.delete().id()),
          document = "",
          action = BulkAction.DELETE,
          parent = None
        )
      case op if op.isUpdate =>
        BulkItem(
          index = op.update().index(),
          id = Some(op.update().id()),
          document = mapper.writeValueAsString(op.update().action().doc()),
          action = BulkAction.UPDATE,
          parent = None
        )
      case _ =>
        throw new IllegalArgumentException(s"Unsupported bulk operation type: ${action.getClass}")
    }

}

/** Elasticsearch client implementation of Scroll API using the Java Client
  * @see
  *   [[ScrollApi]] for scroll operations
  */
trait JavaClientScrollApi extends ScrollApi with JavaClientHelpers {
  _: VersionApi with SearchApi with JavaClientCompanion =>

  /** Classic scroll (works for both hits and aggregations)
    */
  override private[client] def scrollClassic(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation],
    config: ScrollConfig
  )(implicit system: ActorSystem): scaladsl.Source[Map[String, Any], NotUsed] = {
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

                val searchRequest = new SearchRequest.Builder()
                  .index(elasticQuery.indices.asJava)
                  .withJson(new StringReader(elasticQuery.query))
                  .scroll(Time.of(t => t.time(config.keepAlive)))
                  .size(config.scrollSize)
                  .build()

                val response = apply().search(searchRequest, classOf[JMap[String, Object]])

                if (
                  response.shards() != null && response
                    .shards()
                    .failed() != null && response.shards().failed().intValue() > 0
                ) {
                  val failures = response.shards().failures()
                  val errorMsg = if (failures != null && !failures.isEmpty) {
                    failures.asScala.map(_.reason()).mkString("; ")
                  } else {
                    "Unknown shard failure"
                  }
                  throw new IOException(s"Initial scroll failed: $errorMsg")
                }

                val scrollId = response.scrollId()

                if (scrollId == null) {
                  throw new IllegalStateException("Scroll ID is null in response")
                }

                val results = extractAllResults(Left(response), fieldAliases, aggregations)

                if (results.isEmpty || scrollId == null) None
                else Some((Some(scrollId), results))

              case Some(scrollId) =>
                // Subsequent scroll
                logger.debug(s"Fetching next scroll batch (scrollId: $scrollId)")

                val scrollRequest = new ScrollRequest.Builder()
                  .scrollId(scrollId)
                  .scroll(Time.of(t => t.time(config.keepAlive)))
                  .build()

                val response = apply().scroll(scrollRequest, classOf[JMap[String, Object]])

                if (
                  response.shards() != null && response
                    .shards()
                    .failed() != null && response.shards().failed().intValue() > 0
                ) {
                  clearScroll(scrollId)
                  val failures = response.shards().failures()
                  val errorMsg = if (failures != null && !failures.isEmpty) {
                    failures.asScala.map(_.reason()).mkString("; ")
                  } else {
                    "Unknown shard failure"
                  }
                  throw new IOException(s"Scroll continuation failed: $errorMsg")
                }

                val newScrollId = response.scrollId()
                val results = extractAllResults(Right(response), fieldAliases, aggregations)

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
  )(implicit system: ActorSystem): scaladsl.Source[Map[String, Any], NotUsed] = {
    pitSearchAfter(elasticQuery, fieldAliases, config, hasSorts)
  }

  /** PIT + search_after (recommended for ES 7.10+, required for ES 8+)
    *
    * Advantages:
    *   - More efficient than classic scroll (stateless)
    *   - Better for deep pagination
    *   - Can be parallelized
    *   - Lower memory footprint on ES cluster
    *
    * @note
    *   Only works for hits, not for aggregations (use scrollSourceClassic for aggregations)
    */
  private[client] def pitSearchAfter(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    config: ScrollConfig,
    hasSorts: Boolean = false
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = {
    implicit val ec: ExecutionContext = system.dispatcher

    // Step 1: Open PIT
    val pitIdFuture: Future[String] = openPit(elasticQuery.indices, config.keepAlive)

    Source
      .futureSource {
        pitIdFuture.map { pitId =>
          logger.info(s"Opened PIT: $pitId for indices: ${elasticQuery.indices.mkString(", ")}")

          Source
            .unfoldAsync[Option[Seq[Any]], Seq[Map[String, Any]]](None) { searchAfterOpt =>
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

                  // Build search request with PIT
                  val requestBuilder = new SearchRequest.Builder()
                    .size(config.scrollSize)
                    .pit(
                      PointInTimeReference
                        .of(p => p.id(pitId).keepAlive(Time.of(t => t.time(config.keepAlive))))
                    )

                  // Parse query to add query clause (not indices, they're in PIT)
                  val queryJson = JsonParser.parseString(elasticQuery.query).getAsJsonObject

                  // Extract query clause if present
                  if (queryJson.has("query")) {
                    requestBuilder.withJson(new StringReader(elasticQuery.query))
                  }

                  // Check if sorts already exist in the query
                  if (!hasSorts && !queryJson.has("sort")) {
                    logger.warn(
                      "No sort fields in query for PIT search_after, adding default _shard_doc sort. " +
                      "_shard_doc is more efficient than _id for PIT."
                    )
                    requestBuilder.sort(
                      SortOptions.of { sortBuilder =>
                        sortBuilder.field(
                          FieldSort.of(fieldSortBuilder =>
                            fieldSortBuilder.field("_shard_doc").order(SortOrder.Asc)
                          )
                        )
                      }
                    )
                  } else if (hasSorts && queryJson.has("sort")) {
                    // Sorts already present, check that a tie-breaker exists
                    val existingSorts = queryJson.getAsJsonArray("sort")
                    val hasShardDocSort = existingSorts.asScala.exists { sortElem =>
                      sortElem.isJsonObject && (
                        sortElem.getAsJsonObject.has("_shard_doc") ||
                        sortElem.getAsJsonObject.has("_id")
                      )
                    }
                    if (!hasShardDocSort) {
                      // Add _id as tie-breaker
                      logger.debug("Adding _shard_doc as tie-breaker to existing sorts")
                      requestBuilder.sort(
                        SortOptions.of { sortBuilder =>
                          sortBuilder.field(
                            FieldSort.of(fieldSortBuilder =>
                              fieldSortBuilder.field("_shard_doc").order(SortOrder.Asc)
                            )
                          )
                        }
                      )
                    }
                  }

                  // Add search_after if available
                  searchAfterOpt.foreach { searchAfter =>
                    val fieldValues: Seq[FieldValue] = searchAfter.map {
                      case s: String  => FieldValue.of(s)
                      case i: Int     => FieldValue.of(i.toLong)
                      case l: Long    => FieldValue.of(l)
                      case d: Double  => FieldValue.of(d)
                      case b: Boolean => FieldValue.of(b)
                      case other      => FieldValue.of(other.toString)
                    }
                    requestBuilder.searchAfter(fieldValues.asJava)
                  }

                  val response = apply().search(
                    requestBuilder.build(),
                    classOf[JMap[String, Object]]
                  )

                  // Check errors
                  if (
                    response.shards() != null &&
                    response.shards().failed() != null &&
                    response.shards().failed().intValue() > 0
                  ) {
                    val failures = response.shards().failures()
                    val errorMsg = if (failures != null && !failures.isEmpty) {
                      failures.asScala.map(_.reason()).mkString("; ")
                    } else {
                      "Unknown shard failure"
                    }
                    throw new IOException(s"PIT search_after failed: $errorMsg")
                  }

                  val hits = extractHitsOnly(response, fieldAliases)

                  if (hits.isEmpty) {
                    // Close PIT when done
                    closePit(pitId)
                    None
                  } else {
                    val lastHit = response.hits().hits().asScala.lastOption
                    val nextSearchAfter = lastHit.flatMap { hit =>
                      val sortValues = hit.sort().asScala
                      if (sortValues.nonEmpty) {
                        Some(sortValues.map { fieldValue =>
                          if (fieldValue.isString) fieldValue.stringValue()
                          else if (fieldValue.isDouble) fieldValue.doubleValue()
                          else if (fieldValue.isLong) fieldValue.longValue()
                          else if (fieldValue.isBoolean) fieldValue.booleanValue()
                          else if (fieldValue.isNull) null
                          else fieldValue.toString
                        }.toSeq)
                      } else {
                        None
                      }
                    }

                    logger.debug(s"Retrieved ${hits.size} documents, continuing with PIT")
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
              // Cleanup PIT on stream completion/failure
              done.onComplete {
                case scala.util.Success(_) =>
                  logger.info(
                    s"PIT search_after completed successfully, closing PIT: ${pitId.take(20)}..."
                  )
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

  /** Open a Point In Time
    */
  private def openPit(indices: Seq[String], keepAlive: String)(implicit
    ec: ExecutionContext
  ): Future[String] = {
    Future {
      logger.debug(s"Opening PIT for indices: ${indices.mkString(", ")} with keepAlive: $keepAlive")

      val openPitRequest = new OpenPointInTimeRequest.Builder()
        .index(indices.asJava)
        .keepAlive(Time.of(t => t.time(keepAlive)))
        .build()

      val response = apply().openPointInTime(openPitRequest)
      val pitId = response.id()

      if (pitId == null || pitId.isEmpty) {
        throw new IllegalStateException("PIT ID is null or empty in response")
      }

      logger.info(s"PIT opened successfully: ${pitId.take(20)}... (keepAlive: $keepAlive)")
      pitId
    }.recoverWith { case ex: Exception =>
      logger.error(s"Failed to open PIT: ${ex.getMessage}", ex)
      Future.failed(
        new IOException(s"Failed to open PIT for indices: ${indices.mkString(", ")}", ex)
      )
    }
  }

  /** Close a Point In Time
    */
  private def closePit(pitId: String): Unit = {
    Try {
      logger.debug(s"Closing PIT: ${pitId.take(20)}...")

      val closePitRequest = new ClosePointInTimeRequest.Builder()
        .id(pitId)
        .build()

      val response = apply().closePointInTime(closePitRequest)

      if (response.succeeded()) {
        logger.info(s"PIT closed successfully: ${pitId.take(20)}...")
      } else {
        logger.warn(s"PIT close reported failure: ${pitId.take(20)}...")
      }
    }.recover { case ex: Exception =>
      logger.warn(s"Failed to close PIT ${pitId.take(20)}...: ${ex.getMessage}")
    }
  }

  /** Extract ALL results: hits + aggregations This is crucial for queries with aggregations (GROUP
    * BY, COUNT, AVG, etc.)
    */
  private def extractAllResults(
    response: Either[SearchResponse[JMap[String, Object]], ScrollResponse[JMap[String, Object]]],
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): Seq[Map[String, Any]] = {
    val jsonString =
      response match {
        case Left(l)  => convertToJson(l)
        case Right(r) => convertToJson(r)
      }

    parseResponse(
      jsonString,
      fieldAliases,
      aggregations.map(kv => kv._1 -> implicitly[ClientAggregation](kv._2))
    ) match {
      case Success(rows) =>
        logger.debug(s"Parsed ${rows.size} rows from response (hits + aggregations)")
        rows
      case Failure(ex) =>
        logger.error(s"Failed to parse scroll response: ${ex.getMessage}", ex)
        Seq.empty
    }
  }

  /** Extract ONLY hits (for search_after optimization) Ignores aggregations for better performance
    */
  private def extractHitsOnly(
    response: SearchResponse[JMap[String, Object]],
    fieldAliases: Map[String, String]
  ): Seq[Map[String, Any]] = {
    val jsonString = convertToJson(response)

    parseResponse(jsonString, fieldAliases, Map.empty) match {
      case Success(rows) =>
        logger.debug(s"Parsed ${rows.size} hits from response")
        rows
      case Failure(ex) =>
        logger.error(s"Failed to parse search after response: ${ex.getMessage}", ex)
        Seq.empty
    }
  }

  /** Clear scroll context to free resources
    */
  private def clearScroll(scrollId: String): Unit = {
    Try {
      logger.debug(s"Clearing scroll: $scrollId")
      val clearRequest = new ClearScrollRequest.Builder()
        .scrollId(scrollId)
        .build()
      apply().clearScroll(clearRequest)
    }.recover { case ex: Exception =>
      logger.warn(s"Failed to clear scroll $scrollId: ${ex.getMessage}")
    }
  }
}

trait JavaClientPipelineApi extends PipelineApi with JavaClientHelpers with JavaClientVersionApi {
  _: JavaClientCompanion with SerializationApi =>

  override private[client] def executeCreatePipeline(
    pipelineName: String,
    pipelineDefinition: String
  ): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "createPipeline",
      index = None,
      retryable = false
    )(
      apply()
        .ingest()
        .putPipeline(
          new PutPipelineRequest.Builder()
            .id(pipelineName)
            .withJson(new StringReader(pipelineDefinition))
            .build()
        )
    )(resp => resp.acknowledged())

  override private[client] def executeDeletePipeline(
    pipelineName: String,
    ifExists: Boolean
  ): result.ElasticResult[Boolean] =
    executeJavaBooleanAction(
      operation = "deletePipeline",
      index = None,
      retryable = false
    )(
      apply()
        .ingest()
        .deletePipeline(
          new DeletePipelineRequest.Builder()
            .id(pipelineName)
            .build()
        )
    )(resp => resp.acknowledged())

  override private[client] def executeGetPipeline(
    pipelineName: String
  ): ElasticResult[Option[String]] = {
    executeJavaAction(
      operation = "getPipeline",
      index = None,
      retryable = true
    )(
      apply()
        .ingest()
        .getPipeline(
          new GetPipelineRequest.Builder()
            .id(pipelineName)
            .build()
        )
    ) { resp =>
      resp.result().asScala.get(pipelineName).map { pipeline =>
        convertToJson(pipeline)
      }
    }
  }
}
