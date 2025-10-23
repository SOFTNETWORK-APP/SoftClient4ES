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
import akka.stream.scaladsl.Flow
import app.softnetwork.elastic.client._
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.query.{SQLAggregation, SQLQuery, SQLSearchRequest}
import app.softnetwork.elastic.{client, sql}
import app.softnetwork.persistence.model.Timestamped
import app.softnetwork.serialization.serialization
import co.elastic.clients.elasticsearch.core.bulk.{
  BulkOperation,
  BulkResponseItem,
  DeleteOperation,
  IndexOperation,
  UpdateAction,
  UpdateOperation
}
import co.elastic.clients.elasticsearch.core.msearch.{MultisearchHeader, RequestItem}
import co.elastic.clients.elasticsearch.core._
import co.elastic.clients.elasticsearch.core.reindex.{Destination, Source}
import co.elastic.clients.elasticsearch.core.search.SearchRequestBody
import co.elastic.clients.elasticsearch.indices.update_aliases.{Action, AddAction, RemoveAction}
import co.elastic.clients.elasticsearch.indices.{ExistsRequest => IndexExistsRequest, _}
import co.elastic.clients.json.JsonpSerializable
import co.elastic.clients.json.jackson.JacksonJsonpMapper
import com.google.gson.{Gson, JsonParser}

import _root_.java.io.{StringReader, StringWriter}
import _root_.java.util.{Map => JMap}
//import scala.jdk.CollectionConverters._
import scala.collection.JavaConverters._
import org.json4s.Formats

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

trait ElasticsearchClientApi
    extends ElasticClientApi
    with ElasticsearchClientIndicesApi
    with ElasticsearchClientAliasApi
    with ElasticsearchClientSettingsApi
    with ElasticsearchClientMappingApi
    with ElasticsearchClientRefreshApi
    with ElasticsearchClientFlushApi
    with ElasticsearchClientCountApi
    with ElasticsearchClientSingleValueAggregateApi
    with ElasticsearchClientIndexApi
    with ElasticsearchClientUpdateApi
    with ElasticsearchClientDeleteApi
    with ElasticsearchClientGetApi
    with ElasticsearchClientSearchApi
    with ElasticsearchClientBulkApi

trait ElasticsearchClientIndicesApi extends IndicesApi with ElasticsearchClientCompanion {
  override def createIndex(index: String, settings: String): Boolean = {
    tryOrElse(
      apply()
        .indices()
        .create(
          new CreateIndexRequest.Builder()
            .index(index)
            .settings(new IndexSettings.Builder().withJson(new StringReader(settings)).build())
            .build()
        )
        .acknowledged(),
      false
    )(logger)
  }

  override def deleteIndex(index: String): Boolean = {
    tryOrElse(
      apply()
        .indices()
        .delete(new DeleteIndexRequest.Builder().index(index).build())
        .acknowledged(),
      false
    )(logger)
  }

  override def openIndex(index: String): Boolean = {
    tryOrElse(
      apply().indices().open(new OpenRequest.Builder().index(index).build()).acknowledged(),
      false
    )(logger)
  }

  override def closeIndex(index: String): Boolean = {
    tryOrElse(
      apply().indices().close(new CloseIndexRequest.Builder().index(index).build()).acknowledged(),
      false
    )(logger)
  }

  override def reindex(
    sourceIndex: String,
    targetIndex: String,
    refresh: Boolean = true
  ): Boolean = {
    val failures = apply()
      .reindex(
        new ReindexRequest.Builder()
          .source(new Source.Builder().index(sourceIndex).build())
          .dest(new Destination.Builder().index(targetIndex).build())
          .refresh(refresh)
          .build()
      )
      .failures()
      .asScala
      .map(_.cause().reason())
    if (failures.nonEmpty) {
      logger.error(
        s"Reindexing from $sourceIndex to $targetIndex failed with errors: ${failures.take(100).mkString(", ")}"
      )
    }
    failures.isEmpty
  }

  override def indexExists(index: String): Boolean = {
    tryOrElse(
      apply()
        .indices()
        .exists(
          new IndexExistsRequest.Builder().index(index).build()
        )
        .value(),
      false
    )(logger)
  }
}

trait ElasticsearchClientAliasApi extends AliasApi with ElasticsearchClientCompanion {
  override def addAlias(index: String, alias: String): Boolean = {
    tryOrElse(
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
        .acknowledged(),
      false
    )(logger)
  }

  override def removeAlias(index: String, alias: String): Boolean = {
    tryOrElse(
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
        .acknowledged(),
      false
    )(logger)
  }
}

trait ElasticsearchClientSettingsApi extends SettingsApi with ElasticsearchClientCompanion {
  _: ElasticsearchClientIndicesApi =>

  override def updateSettings(index: String, settings: String): Boolean = {
    tryOrElse(
      apply()
        .indices()
        .putSettings(
          new PutIndicesSettingsRequest.Builder()
            .index(index)
            .settings(new IndexSettings.Builder().withJson(new StringReader(settings)).build())
            .build()
        )
        .acknowledged(),
      false
    )(logger)
  }

  override def loadSettings(index: String): String = {
    tryOrElse(
      Option(
        apply()
          .indices()
          .getSettings(
            new GetIndicesSettingsRequest.Builder().index(index).build()
          )
          .get(index)
      ).map { value =>
        val mapper = new JacksonJsonpMapper()
        val writer = new StringWriter()
        val generator = mapper.jsonProvider().createGenerator(writer)
        mapper.serialize(value.settings().index(), generator)
        generator.close()
        writer.toString
      },
      None
    )(logger).getOrElse("{}")
  }
}

trait ElasticsearchClientMappingApi
    extends MappingApi
    with ElasticsearchClientIndicesApi
    with ElasticsearchClientRefreshApi
    with ElasticsearchClientCompanion {
  override def setMapping(index: String, mapping: String): Boolean = {
    tryOrElse(
      apply()
        .indices()
        .putMapping(
          new PutMappingRequest.Builder().index(index).withJson(new StringReader(mapping)).build()
        )
        .acknowledged(),
      false
    )(logger)
  }

  override def getMapping(index: String): String = {
    tryOrElse(
      {
        Option(
          apply()
            .indices()
            .getMapping(
              new GetMappingRequest.Builder().index(index).build()
            )
            .get(index)
        ).map { value =>
          val mapper = new JacksonJsonpMapper()
          val writer = new StringWriter()
          val generator = mapper.jsonProvider().createGenerator(writer)
          mapper.serialize(value, generator)
          generator.close()
          writer.toString
        }
      },
      None
    )(logger).getOrElse(s""""{$index: {"mappings": {}}}""")
  }
}

trait ElasticsearchClientRefreshApi extends RefreshApi with ElasticsearchClientCompanion {
  override def refresh(index: String): Boolean = {
    tryOrElse(
      apply()
        .indices()
        .refresh(
          new RefreshRequest.Builder().index(index).build()
        )
        .shards()
        .failed()
        .intValue() == 0,
      false
    )(logger)
  }
}

trait ElasticsearchClientFlushApi extends FlushApi with ElasticsearchClientCompanion {
  override def flush(index: String, force: Boolean = true, wait: Boolean = true): Boolean = {
    tryOrElse(
      apply()
        .indices()
        .flush(
          new FlushRequest.Builder().index(index).force(force).waitIfOngoing(wait).build()
        )
        .shards()
        .failed()
        .intValue() == 0,
      false
    )(logger)
  }
}

trait ElasticsearchClientCountApi extends CountApi with ElasticsearchClientCompanion {
  override def count(query: client.JSONQuery): Option[Double] = {
    tryOrElse(
      Option(
        apply()
          .count(
            new CountRequest.Builder().index(query.indices.asJava).build()
          )
          .count()
          .toDouble
      ),
      None
    )(logger)
  }

  override def countAsync(query: client.JSONQuery)(implicit
    ec: ExecutionContext
  ): Future[Option[Double]] = {
    fromCompletableFuture(
      async()
        .count(
          new CountRequest.Builder().index(query.indices.asJava).build()
        )
    ).map(response => Option(response.count().toDouble))
  }
}

trait ElasticsearchClientSingleValueAggregateApi
    extends SingleValueAggregateApi
    with ElasticsearchClientCountApi { _: SearchApi with ElasticConversion => }

trait ElasticsearchClientIndexApi extends IndexApi with ElasticsearchClientCompanion {
  _: ElasticsearchClientRefreshApi =>
  override def index(index: String, id: String, source: String): Boolean = {
    tryOrElse(
      apply()
        .index(
          new IndexRequest.Builder()
            .index(index)
            .id(id)
            .withJson(new StringReader(source))
            .build()
        )
        .shards()
        .failed()
        .intValue() == 0,
      false
    )(logger)
  }

  override def indexAsync(index: String, id: String, source: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    fromCompletableFuture(
      async()
        .index(
          new IndexRequest.Builder()
            .index(index)
            .id(id)
            .withJson(new StringReader(source))
            .build()
        )
    ).flatMap { response =>
      if (response.shards().failed().intValue() == 0) {
        Future.successful(true)
      } else {
        Future.failed(new Exception(s"Failed to index document with id: $id in index: $index"))
      }
    }
  }
}

trait ElasticsearchClientUpdateApi extends UpdateApi with ElasticsearchClientCompanion {
  _: ElasticsearchClientRefreshApi =>
  override def update(
    index: String,
    id: String,
    source: String,
    upsert: Boolean
  ): Boolean = {
    tryOrElse(
      apply()
        .update(
          new UpdateRequest.Builder[JMap[String, Object], JMap[String, Object]]()
            .index(index)
            .id(id)
            .doc(mapper.readValue(source, classOf[JMap[String, Object]]))
            .docAsUpsert(upsert)
            .build(),
          classOf[JMap[String, Object]]
        )
        .shards()
        .failed()
        .intValue() == 0,
      false
    )(logger)
  }

  override def updateAsync(index: String, id: String, source: String, upsert: Boolean)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    fromCompletableFuture(
      async()
        .update(
          new UpdateRequest.Builder[JMap[String, Object], JMap[String, Object]]()
            .index(index)
            .id(id)
            .doc(mapper.readValue(source, classOf[JMap[String, Object]]))
            .docAsUpsert(upsert)
            .build(),
          classOf[JMap[String, Object]]
        )
    ).flatMap { response =>
      if (response.shards().failed().intValue() == 0) {
        Future.successful(true)
      } else {
        Future.failed(new Exception(s"Failed to update document with id: $id in index: $index"))
      }
    }
  }
}

trait ElasticsearchClientDeleteApi extends DeleteApi with ElasticsearchClientCompanion {
  _: ElasticsearchClientRefreshApi =>

  override def delete(uuid: String, index: String): Boolean = {
    tryOrElse(
      apply()
        .delete(
          new DeleteRequest.Builder().index(index).id(uuid).build()
        )
        .shards()
        .failed()
        .intValue() == 0,
      false
    )(logger)
  }

  override def deleteAsync(uuid: String, index: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    fromCompletableFuture(
      async()
        .delete(
          new DeleteRequest.Builder().index(index).id(uuid).build()
        )
    ).flatMap { response =>
      if (response.shards().failed().intValue() == 0) {
        Future.successful(true)
      } else {
        Future.failed(new Exception(s"Failed to delete document with id: $uuid in index: $index"))
      }
    }
  }

}

trait ElasticsearchClientGetApi extends GetApi with ElasticsearchClientCompanion {

  def get[U <: Timestamped](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit m: Manifest[U], formats: Formats): Option[U] = {
    Try(
      apply().get(
        new GetRequest.Builder()
          .index(
            index.getOrElse(
              maybeType.getOrElse(
                m.runtimeClass.getSimpleName.toLowerCase
              )
            )
          )
          .id(id)
          .build(),
        classOf[JMap[String, Object]]
      )
    ) match {
      case Success(response) =>
        if (response.found()) {
          val source = mapper.writeValueAsString(response.source())
          logger.debug(s"Deserializing response $source for id: $id, index: ${index
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
    fromCompletableFuture(
      async()
        .get(
          new GetRequest.Builder()
            .index(
              index.getOrElse(
                maybeType.getOrElse(
                  m.runtimeClass.getSimpleName.toLowerCase
                )
              )
            )
            .id(id)
            .build(),
          classOf[JMap[String, Object]]
        )
    ).flatMap {
      case response if response.found() =>
        val source = mapper.writeValueAsString(response.source())
        logger.debug(s"Deserializing response $source for id: $id, index: ${index
          .getOrElse("default")}, type: ${maybeType.getOrElse("_all")}")
        // Deserialize the source string to the expected type
        // Note: This assumes that the source is a valid JSON representation of U
        // and that the serialization library is capable of handling it.
        Try(serialization.read[U](source)) match {
          case Success(value) => Future.successful(Some(value))
          case Failure(f) =>
            logger.error(
              s"Failed to deserialize response $source for id: $id, index: ${index
                .getOrElse("default")}, type: ${maybeType.getOrElse("_all")}",
              f
            )
            Future.successful(None)
        }
      case _ => Future.successful(None)
    }
    Future {
      this.get[U](id, index, maybeType)
    }
  }
}

trait ElasticsearchClientSearchApi extends SearchApi with ElasticsearchClientCompanion {
  _: ElasticConversion =>
  override implicit def sqlSearchRequestToJsonQuery(sqlSearch: SQLSearchRequest): String =
    implicitly[ElasticSearchRequest](sqlSearch).query

  private[this] val jsonpMapper = new JacksonJsonpMapper(mapper)

  /** Convert any Elasticsearch response to JSON string */
  private[this] def convertToJson[T <: JsonpSerializable](response: T): String = {
    val stringWriter = new StringWriter()
    val generator = jsonpMapper.jsonProvider().createGenerator(stringWriter)

    try {
      response.serialize(generator, jsonpMapper)
      generator.flush()
      stringWriter.toString
    } finally {
      generator.close()
    }
  }

  /** Search for entities matching the given JSON query.
    *
    * @param jsonQuery
    *   - the JSON query to search for
    * @param fieldAliases
    *   - the field aliases to use for the search
    * @param aggregations
    *   - the aggregations to use for the search
    * @return
    *   the SQL search response containing the results of the query
    */
  override def search(
    jsonQuery: JSONQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): SQLSearchResponse = {
    val query = jsonQuery.query
    logger.info(s"Searching with query: $query on indices: ${jsonQuery.indices.mkString(", ")}")
    // Execute the search request
    val response = apply().search(
      new SearchRequest.Builder()
        .index(jsonQuery.indices.asJava)
        .withJson(
          new StringReader(query)
        )
        .build(),
      classOf[JMap[String, Object]]
    )
    // Return the SQL search response
    val sqlResponse = SQLSearchResponse(
      query,
      convertToJson(response),
      fieldAliases,
      aggregations
    )
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
    jsonQueries: JSONQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): SQLSearchResponse = {
    val queries = jsonQueries.queries.map(_.query)
    val query = queries.mkString("\n")
    logger.info(
      s"Performing multi-search with ${queries.size} queries."
    )
    // Build the multi-search request
    val items = jsonQueries.queries.zipWithIndex.map { case (q, i) =>
      val query = queries(i)
      logger.info(s"Searching with query ${i + 1}: $query on indices: ${q.indices
        .mkString(", ")}")
      new RequestItem.Builder()
        .header(new MultisearchHeader.Builder().index(q.indices.asJava).build())
        .body(new SearchRequestBody.Builder().withJson(new StringReader(query)).build())
        .build()
    }

    val request = new MsearchRequest.Builder().searches(items.asJava).build()
    // Execute the multi-search request
    val responses = apply().msearch(request, classOf[JMap[String, Object]])
    // Return the SQL search response
    val sqlResponse = SQLSearchResponse(
      query,
      convertToJson(responses),
      fieldAliases,
      aggregations
    )
    logger.info(s"Search response: $sqlResponse")
    sqlResponse
  }

  override def searchAsyncAs[U](
    sqlQuery: SQLQuery
  )(implicit m: Manifest[U], ec: ExecutionContext, formats: Formats): Future[List[U]] = {
    val jsonQuery: JSONQuery = sqlQuery
    import jsonQuery._
    fromCompletableFuture(
      async()
        .search(
          new SearchRequest.Builder()
            .index(indices.asJava)
            .withJson(new StringReader(query))
            .build(),
          classOf[JMap[String, Object]]
        )
    ).flatMap {
      case response if response.hits().total().value() > 0 =>
        Future.successful(
          response
            .hits()
            .hits()
            .asScala
            .map { hit =>
              val source = mapper.writeValueAsString(hit.source())
              logger.debug(s"Deserializing hit: $source")
              serialization.read[U](source)
            }
            .toList
        )
      case _ =>
        logger.warn(
          s"No hits found for query: ${sqlQuery.query} on indices: ${indices.mkString(", ")}"
        )
        Future.successful(List.empty[U])
    }
  }

  override def searchWithInnerHits[U, I](jsonQuery: JSONQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[(U, List[I])] = {
    import jsonQuery._
    logger.info(s"Searching with query: $query on indices: ${indices.mkString(", ")}")
    val response = apply()
      .search(
        new SearchRequest.Builder()
          .index(indices.asJava)
          .withJson(
            new StringReader(query)
          )
          .build(),
        classOf[JMap[String, Object]]
      )
    val results = response
      .hits()
      .hits()
      .asScala
      .toList
    if (results.nonEmpty) {
      results.flatMap { hit =>
        val hitSource = hit.source()
        Option(hitSource)
          .map(mapper.writeValueAsString)
          .flatMap { source =>
            logger.debug(s"Deserializing hit: $source")
            Try(serialization.read[U](source)) match {
              case Success(mainObject) =>
                Some(mainObject)
              case Failure(f) =>
                logger.error(
                  s"Failed to deserialize hit: $source for query: $query on indices: ${indices.mkString(", ")}",
                  f
                )
                None
            }
          }
          .map { mainObject =>
            val innerHits = hit
              .innerHits()
              .asScala
              .get(innerField)
              .map(_.hits().hits().asScala.toList)
              .getOrElse(Nil)
            val innerObjects = innerHits.flatMap { innerHit =>
              val mapper = new JacksonJsonpMapper()
              val writer = new StringWriter()
              val generator = mapper.jsonProvider().createGenerator(writer)
              mapper.serialize(innerHit, generator)
              generator.close()
              val innerSource = writer.toString
              logger.debug(s"Processing inner hit: $innerSource")
              val json = new JsonParser().parse(innerSource).getAsJsonObject
              val gson = new Gson()
              Try(serialization.read[I](gson.toJson(json.get("_source")))) match {
                case Success(innerObject) => Some(innerObject)
                case Failure(f) =>
                  logger.error(s"Failed to deserialize inner hit: $innerSource", f)
                  None
              }
            }
            (mainObject, innerObjects)
          }
      }
    } else {
      logger.warn(s"No hits found for query: $query on indices: ${indices.mkString(", ")}")
      List.empty[(U, List[I])]
    }
  }

  override def multisearchWithInnerHits[U, I](jsonQueries: JSONQueries, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[List[(U, List[I])]] = {
    import jsonQueries._
    val items = queries.map { query =>
      new RequestItem.Builder()
        .header(new MultisearchHeader.Builder().index(query.indices.asJava).build())
        .body(new SearchRequestBody.Builder().withJson(new StringReader(query.query)).build())
        .build()
    }

    val request = new MsearchRequest.Builder().searches(items.asJava).build()
    val responses = apply().msearch(request, classOf[JMap[String, Object]])

    responses.responses().asScala.toList.map {
      case response if response.isFailure =>
        logger.error(s"Error in multi search: ${response.failure().error().reason()}")
        List.empty[(U, List[I])]

      case response =>
        Try(
          new JsonParser().parse(response.result().toString).getAsJsonObject ~> [U, I] innerField
        ) match {
          case Success(s) => s
          case Failure(f) =>
            logger.error(f.getMessage, f)
            List.empty
        }
    }
  }

}

trait ElasticsearchClientBulkApi
    extends ElasticsearchClientRefreshApi
    with ElasticsearchClientSettingsApi
    with ElasticsearchClientIndicesApi
    with BulkApi {
  override type A = BulkOperation
  override type R = BulkResponse

  override def toBulkAction(bulkItem: BulkItem): A = {
    import bulkItem._

    action match {
      case BulkAction.UPDATE =>
        new BulkOperation.Builder()
          .update(
            new UpdateOperation.Builder()
              .index(index)
              .id(id.orNull)
              .action(
                new UpdateAction.Builder[JMap[String, Object], JMap[String, Object]]()
                  .doc(mapper.readValue(body, classOf[JMap[String, Object]]))
                  .docAsUpsert(true)
                  .build()
              )
              .build()
          )
          .build()

      case BulkAction.DELETE =>
        val deleteId = id.getOrElse {
          throw new IllegalArgumentException(s"Missing id for delete on index $index")
        }
        new BulkOperation.Builder()
          .delete(new DeleteOperation.Builder().index(index).id(deleteId).build())
          .build()

      case _ =>
        new BulkOperation.Builder()
          .index(
            new IndexOperation.Builder[JMap[String, Object]]()
              .index(index)
              .id(id.orNull)
              .document(mapper.readValue(body, classOf[JMap[String, Object]]))
              .build()
          )
          .build()
    }
  }
  override def bulkResult: Flow[R, Set[String], NotUsed] =
    Flow[BulkResponse]
      .named("result")
      .map(result => {
        val items = result.items().asScala.toList
        val grouped = items.groupBy(_.index())
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
        val request =
          new BulkRequest.Builder().index(bulkOptions.index).operations(items.asJava).build()
        Try(apply().bulk(request)) match {
          case Success(response) if response.errors() =>
            val failedItems = response.items().asScala.filter(_.status() >= 400)
            if (failedItems.nonEmpty) {
              val errorMessages =
                failedItems.map(i => s"${i.id()} - ${i.error().reason()}").mkString(", ")
              Future.failed(new Exception(s"Bulk operation failed for items: $errorMessages"))
            } else {
              Future.successful(response)
            }
          case Success(response) =>
            Future.successful(response)
          case Failure(exception) =>
            logger.error("Bulk operation failed", exception)
            Future.failed(exception)
        }
      }
  }

  private[this] def toBulkElasticResultItem(i: BulkResponseItem): BulkElasticResultItem =
    new BulkElasticResultItem {
      override def index: String = i.index()
    }

  override implicit def toBulkElasticAction(a: BulkOperation): BulkElasticAction =
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

  override implicit def toBulkElasticResult(r: BulkResponse): BulkElasticResult = {
    new BulkElasticResult {
      override def items: List[BulkElasticResultItem] =
        r.items().asScala.toList.map(toBulkElasticResultItem)
    }
  }
}
