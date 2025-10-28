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

package app.softnetwork.elastic.client.jest

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Flow, Source}
import app.softnetwork.elastic.client._
import app.softnetwork.elastic.sql.query.{SQLAggregation, SQLQuery, SQLSearchRequest}
import app.softnetwork.elastic.sql.bridge._
import com.google.gson.{JsonNull, JsonObject, JsonParser}
import io.searchbox.action.BulkableAction
import io.searchbox.core._
import io.searchbox.indices._
import io.searchbox.indices.aliases.{AddAliasMapping, ModifyAliases, RemoveAliasMapping}
import io.searchbox.indices.mapping.{GetMapping, PutMapping}
import io.searchbox.indices.reindex.Reindex
import io.searchbox.indices.settings.{GetSettings, UpdateSettings}
import io.searchbox.params.Parameters
import org.json4s.Formats

import java.io.IOException
import scala.jdk.CollectionConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.implicitConversions
import scala.util.{Failure, Success, Try}

/** Created by smanciot on 20/05/2021.
  */
trait JestClientApi
    extends ElasticClientApi
    with JestIndicesApi
    with JestAliasApi
    with JestSettingsApi
    with JestMappingApi
    with JestRefreshApi
    with JestFlushApi
    with JestCountApi
    with JestSingleValueAggregateApi
    with JestIndexApi
    with JestUpdateApi
    with JestDeleteApi
    with JestGetApi
    with JestSearchApi
    with JestScrollApi
    with JestBulkApi
    with JestClientCompanion

trait JestIndicesApi extends IndicesApi with JestRefreshApi { _: JestClientCompanion =>
  override def createIndex(index: String, settings: String = defaultSettings): Boolean =
    tryOrElse(
      apply()
        .execute(
          new CreateIndex.Builder(index).settings(settings).build()
        )
        .isSucceeded,
      false
    )(logger)

  override def deleteIndex(index: String): Boolean =
    tryOrElse(
      apply()
        .execute(
          new DeleteIndex.Builder(index).build()
        )
        .isSucceeded,
      false
    )(logger)

  override def closeIndex(index: String): Boolean =
    tryOrElse(
      apply()
        .execute(
          new CloseIndex.Builder(index).build()
        )
        .isSucceeded,
      false
    )(logger)

  override def openIndex(index: String): Boolean =
    tryOrElse(
      apply()
        .execute(
          new OpenIndex.Builder(index).build()
        )
        .isSucceeded,
      false
    )(logger)

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
    tryOrElse(
      {
        apply()
          .execute(
            new Reindex.Builder(s"""{"index": "$sourceIndex"}""", s"""{"index": "$targetIndex"}""")
              .build()
          )
          .isSucceeded && {
          if (refresh) {
            this.refresh(targetIndex)
          } else {
            true
          }
        }
      },
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
  override def indexExists(index: String): Boolean =
    tryOrElse(
      apply()
        .execute(
          new IndicesExists.Builder(index).build()
        )
        .isSucceeded,
      false
    )(logger)
}

trait JestAliasApi extends AliasApi { _: JestClientCompanion =>
  override def addAlias(index: String, alias: String): Boolean = {
    tryOrElse(
      apply()
        .execute(
          new ModifyAliases.Builder(
            new AddAliasMapping.Builder(index, alias).build()
          ).build()
        )
        .isSucceeded,
      false
    )(logger)
  }

  override def removeAlias(index: String, alias: String): Boolean = {
    tryOrElse(
      apply()
        .execute(
          new ModifyAliases.Builder(
            new RemoveAliasMapping.Builder(index, alias).build()
          ).build()
        )
        .isSucceeded,
      false
    )(logger)
  }
}

trait JestSettingsApi extends SettingsApi {
  _: IndicesApi with JestClientCompanion =>
  override def updateSettings(index: String, settings: String = defaultSettings): Boolean =
    closeIndex(index) &&
    tryOrElse(
      apply()
        .execute(
          new UpdateSettings.Builder(settings).addIndex(index).build()
        )
        .isSucceeded,
      false
    )(logger) &&
    openIndex(index)

  override def loadSettings(index: String): String =
    tryOrElse(
      {
        new JsonParser()
          .parse(
            apply()
              .execute(
                new GetSettings.Builder().addIndex(index).build()
              )
              .getJsonString
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

trait JestMappingApi extends MappingApi {
  _: IndicesApi with JestClientCompanion =>
  override def setMapping(index: String, mapping: String): Boolean =
    tryOrElse(
      apply()
        .execute(
          new PutMapping.Builder(index, "_doc", mapping).build()
        )
        .isSucceeded,
      false
    )(logger)

  override def getMapping(index: String): String =
    tryOrElse(
      {
        new JsonParser()
          .parse(
            apply()
              .execute(
                new GetMapping.Builder().addIndex(index).addType("_doc").build()
              )
              .getJsonString
          )
          .getAsJsonObject
          .get(index)
          .getAsJsonObject
          .get("mappings")
          .getAsJsonObject
          .get("_doc")
          .getAsJsonObject
          .toString
      },
      s""""{"properties": {}}""" // empty mapping
    )(logger)

  /** Get the mapping properties of an index.
    *
    * @param index
    *   - the name of the index to get the mapping properties for
    * @return
    *   the mapping properties of the index as a JSON string
    */
  override def getMappingProperties(index: String): String = {
    tryOrElse(
      getMapping(index),
      "{\"properties\": {}}"
    )(logger)
  }
}

trait JestRefreshApi extends RefreshApi { _: JestClientCompanion =>
  override def refresh(index: String): Boolean =
    tryOrElse(
      apply()
        .execute(
          new Refresh.Builder().addIndex(index).build()
        )
        .isSucceeded,
      false
    )(logger)
}

trait JestFlushApi extends FlushApi { _: JestClientCompanion =>
  override def flush(index: String, force: Boolean = true, wait: Boolean = true): Boolean =
    tryOrElse(
      apply()
        .execute(
          new Flush.Builder().addIndex(index).force(force).waitIfOngoing(wait).build()
        )
        .isSucceeded,
      false
    )(logger)
}

trait JestCountApi extends CountApi { _: JestClientCompanion =>
  override def countAsync(
    elasticQuery: ElasticQuery
  )(implicit ec: ExecutionContext): Future[Option[Double]] = {
    import JestClientResultHandler._
    import elasticQuery._
    val count = new Count.Builder().query(query)
    for (indice <- indices) count.addIndex(indice)
    for (t      <- types) count.addType(t)
    val promise = Promise[Option[Double]]()
    apply().executeAsyncPromise(count.build()) onComplete {
      case Success(result) =>
        if (!result.isSucceeded)
          logger.error(result.getErrorMessage)
        promise.success(Option(result.getCount))
      case Failure(f) =>
        logger.error(f.getMessage, f)
        promise.failure(f)
    }
    promise.future
  }

  override def count(elasticQuery: ElasticQuery): Option[Double] = {
    import elasticQuery._
    val count = new Count.Builder().query(query)
    for (indice <- indices) count.addIndex(indice)
    for (t      <- types) count.addType(t)
    Try {
      apply().execute(count.build())
    } match {
      case Success(result) =>
        if (!result.isSucceeded)
          logger.error(result.getErrorMessage)
        Option(result.getCount)
      case Failure(f) =>
        logger.error(f.getMessage, f)
        None
    }
  }
}

trait JestSingleValueAggregateApi extends SingleValueAggregateApi with JestCountApi {
  _: SearchApi with ElasticConversion with JestClientCompanion =>
}

trait JestIndexApi extends IndexApi {
  _: RefreshApi with JestClientCompanion with SerializationApi =>
  override def index(index: String, id: String, source: String): Boolean = {
    Try(
      apply().execute(
        new Index.Builder(source).index(index).`type`("_doc").id(id).build()
      )
    ) match {
      case Success(s) =>
        if (!s.isSucceeded)
          logger.error(s.getErrorMessage)
        s.isSucceeded && this.refresh(index)
      case Failure(f) =>
        logger.error(f.getMessage, f)
        false
    }
  }

  override def indexAsync(index: String, id: String, source: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    import JestClientResultHandler._
    val promise: Promise[Boolean] = Promise()
    apply().executeAsyncPromise(
      new Index.Builder(source).index(index).`type`("_doc").id(id).build()
    ) onComplete {
      case Success(s) => promise.success(s.isSucceeded && this.refresh(index))
      case Failure(f) =>
        logger.error(f.getMessage, f)
        promise.failure(f)
    }
    promise.future
  }

}

trait JestUpdateApi extends UpdateApi {
  _: RefreshApi with JestClientCompanion with SerializationApi =>
  override def update(
    index: String,
    id: String,
    source: String,
    upsert: Boolean
  ): Boolean = {
    Try(
      apply().execute(
        new Update.Builder(
          if (upsert)
            docAsUpsert(source)
          else
            source
        ).index(index).`type`("_doc").id(id).build()
      )
    ) match {
      case Success(s) =>
        if (!s.isSucceeded)
          logger.error(s.getErrorMessage)
        s.isSucceeded && this.refresh(index)
      case Failure(f) =>
        logger.error(f.getMessage, f)
        false
    }
  }

  override def updateAsync(
    index: String,
    id: String,
    source: String,
    upsert: Boolean
  )(implicit ec: ExecutionContext): Future[Boolean] = {
    import JestClientResultHandler._
    val promise: Promise[Boolean] = Promise()
    apply().executeAsyncPromise(
      new Update.Builder(
        if (upsert)
          docAsUpsert(source)
        else
          source
      ).index(index).`type`("_doc").id(id).build()
    ) onComplete {
      case Success(s) =>
        if (!s.isSucceeded)
          logger.error(s.getErrorMessage)
        promise.success(s.isSucceeded && this.refresh(index))
      case Failure(f) =>
        logger.error(f.getMessage, f)
        promise.failure(f)
    }
    promise.future
  }

}

trait JestDeleteApi extends DeleteApi {
  _: RefreshApi with JestClientCompanion =>
  override def delete(id: String, index: String): Boolean = {
    Try(
      apply()
        .execute(
          new Delete.Builder(id).index(index).`type`("_doc").build()
        )
    ) match {
      case Success(result) =>
        if (!result.isSucceeded)
          logger.error(result.getErrorMessage)
        result.isSucceeded && this.refresh(index)
      case Failure(f) =>
        logger.error(f.getMessage, f)
        false
    }
  }

  override def deleteAsync(id: String, index: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    import JestClientResultHandler._
    val promise: Promise[Boolean] = Promise()
    apply().executeAsyncPromise(
      new Delete.Builder(id).index(index).`type`("_doc").build()
    ) onComplete {
      case Success(s) =>
        if (!s.isSucceeded)
          logger.error(s.getErrorMessage)
        promise.success(s.isSucceeded && this.refresh(index))
      case Failure(f) =>
        logger.error(f.getMessage, f)
        promise.failure(f)
    }
    promise.future
  }

}

trait JestGetApi extends GetApi { _: JestClientCompanion with SerializationApi =>

  // GetApi
  override def get[U <: AnyRef](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit m: Manifest[U], formats: Formats): Option[U] = {
    val result = apply().execute(
      new Get.Builder(
        index.getOrElse(
          maybeType.getOrElse(
            m.runtimeClass.getSimpleName.toLowerCase
          )
        ),
        id
      ).build()
    )
    if (result.isSucceeded) {
      Some(serialization.read[U](result.getSourceAsString))
    } else {
      logger.error(result.getErrorMessage)
      None
    }
  }

  override def getAsync[U <: AnyRef](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit m: Manifest[U], ec: ExecutionContext, formats: Formats): Future[Option[U]] = {
    import JestClientResultHandler._
    val promise: Promise[Option[U]] = Promise()
    apply().executeAsyncPromise(
      new Get.Builder(
        index.getOrElse(
          maybeType.getOrElse(
            m.runtimeClass.getSimpleName.toLowerCase
          )
        ),
        id
      ).build()
    ) onComplete {
      case Success(result) =>
        if (result.isSucceeded)
          promise.success(Some(serialization.read[U](result.getSourceAsString)))
        else {
          logger.error(result.getErrorMessage)
          promise.success(None)
        }
      case Failure(f) =>
        logger.error(f.getMessage, f)
        promise.failure(f)
    }
    promise.future
  }

}

trait JestSearchApi extends SearchApi {
  _: ElasticConversion with JestClientCompanion with SerializationApi =>

  override implicit def sqlSearchRequestToJsonQuery(sqlSearch: SQLSearchRequest): String =
    implicitly[ElasticSearchRequest](sqlSearch).query

  import JestClientApi._

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
    // Create a parser for the query
    val search = elasticQuery.search
    val query = search._2
    val response = tryOrElse(
      {
        apply()
          .execute(
            search._1
          )
          .getJsonString
      },
      ""
    )(logger)
    ElasticResponse(query, response, fieldAliases, aggregations)
  }

  /** Perform a multi-search operation with the given JSON multi-search query.
    *
    * @param elasticQueries
    *   - the JSON multi-search query to perform
    * @param fieldAliases
    *   - the field aliases to use for the search
    * @param aggregations
    *   - the aggregations to use for the search
    * @return
    *   the SQL search response containing the results of the multi-search query
    */
  override def multisearch(
    elasticQueries: ElasticQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): ElasticResponse = {
    val queries = elasticQueries.queries.map(_.search)
    val query = queries.map(_._2).mkString("\n")
    val response = tryOrElse(
      {
        val multiSearchResult =
          apply().execute(
            new MultiSearch.Builder(
              queries
                .map(_._1)
                .asJava
            ).build()
          )
        multiSearchResult.getJsonString
      },
      ""
    )(logger)
    ElasticResponse(query, response, fieldAliases, aggregations)
  }

  override def searchAsyncAs[U](
    sqlQuery: SQLQuery
  )(implicit m: Manifest[U], ec: ExecutionContext, formats: Formats): Future[List[U]] = {
    val promise = Promise[List[U]]()
    val search: Option[Search] = sqlQuery.jestSearch
    search match {
      case Some(s) =>
        import JestClientResultHandler._
        apply().executeAsyncPromise(s) onComplete {
          case Success(searchResult) =>
            promise.success(
              searchResult.getSourceAsStringList.asScala
                .map(source => serialization.read[U](source))
                .toList
            )
          case Failure(f) =>
            promise.failure(f)
        }
      case _ => promise.success(List.empty)
    }
    promise.future
  }

  override def searchWithInnerHits[U, I](elasticQuery: ElasticQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[(U, List[I])] = {
    Try(apply().execute(elasticQuery.search._1)).toOption match {
      case Some(result) =>
        if (!result.isSucceeded) {
          logger.error(result.getErrorMessage)
          return List.empty
        }
        Try(result.getJsonObject ~> [U, I] innerField) match {
          case Success(s) => s
          case Failure(f) =>
            logger.error(f.getMessage, f)
            List.empty
        }
      case _ => List.empty
    }
  }

  override def multisearchWithInnerHits[U, I](elasticQueries: ElasticQueries, innerField: String)(
    implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[List[(U, List[I])]] = {
    val multiSearch =
      new MultiSearch.Builder(elasticQueries.queries.map(_.search._1).asJava).build()
    Try(apply().execute(multiSearch)).toOption match {
      case Some(multiSearchResult) =>
        if (!multiSearchResult.isSucceeded) {
          logger.error(multiSearchResult.getErrorMessage)
          return List.empty
        }
        multiSearchResult.getResponses.asScala
          .map(searchResponse => {
            Try(searchResponse.searchResult.getJsonObject ~> [U, I] innerField) match {
              case Success(s) => s
              case Failure(f) =>
                logger.error(f.getMessage, f)
                List.empty[(U, List[I])]
            }
          })
          .toList
      case _ => List.empty
    }
  }

}

trait JestBulkApi extends JestRefreshApi with JestSettingsApi with JestIndicesApi with BulkApi {
  _: JestClientCompanion =>
  override type BulkActionType = BulkableAction[DocumentResult]
  override type BulkResultType = BulkResult

  override implicit def toBulkElasticAction(a: A): BulkElasticAction =
    new BulkElasticAction {
      override def index: String = a.getIndex
    }

  private[this] def toBulkElasticResultItem(i: BulkResult#BulkResultItem): BulkElasticResultItem =
    new BulkElasticResultItem {
      override def index: String = i.index
    }

  override implicit def toBulkElasticResult(r: R): BulkElasticResult =
    new BulkElasticResult {
      override def items: List[BulkElasticResultItem] =
        r.getItems.asScala.toList.map(toBulkElasticResultItem)
    }

  override def bulk(implicit
    bulkOptions: BulkOptions,
    system: ActorSystem
  ): Flow[Seq[A], R, NotUsed] = {
    import JestClientResultHandler._
    val parallelism = Math.max(1, bulkOptions.balance)

    Flow[Seq[BulkableAction[DocumentResult]]]
      .named("bulk")
      .mapAsyncUnordered[BulkResult](parallelism)(items => {
        logger.info(s"Starting to write batch of ${items.size}...")
        val init =
          new Bulk.Builder().defaultIndex(bulkOptions.index).defaultType(bulkOptions.documentType)
        val bulkQuery = items.foldLeft(init) { (current, query) =>
          current.addAction(query)
        }
        apply().executeAsyncPromise(bulkQuery.build())
      })
  }

  override def bulkResult: Flow[R, Set[String], NotUsed] =
    Flow[BulkResult]
      .named("result")
      .map(result => {
        val items = result.getItems
        val indices = items.asScala.map(_.index).toSet
        logger.info(s"Finished to write batch of ${items.size} within ${indices.mkString(",")}.")
        indices
      })

  override def toBulkAction(bulkItem: BulkItem): A = {
    val builder = bulkItem.action match {
      case BulkAction.DELETE => new Delete.Builder(bulkItem.body)
      case BulkAction.UPDATE => new Update.Builder(docAsUpsert(bulkItem.body))
      case _                 => new Index.Builder(bulkItem.body)
    }
    bulkItem.id.foreach(builder.id)
    builder.index(bulkItem.index)
    bulkItem.parent.foreach(s => builder.setParameter(Parameters.PARENT, s))
    builder.build()
  }

}

trait JestScrollApi extends ScrollApi { _: JestClientCompanion =>

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
                logger.info(
                  s"Starting classic scroll on indices: ${elasticQuery.indices.mkString(", ")}"
                )

                val searchBuilder =
                  new Search.Builder(elasticQuery.query)
                    .setParameter(Parameters.SIZE, config.scrollSize)
                    .setParameter(Parameters.SCROLL, config.scrollTimeout)

                for (indice <- elasticQuery.indices) searchBuilder.addIndex(indice)
                for (t      <- elasticQuery.types) searchBuilder.addType(t)

                val result = apply().execute(searchBuilder.build())
                if (!result.isSucceeded) {
                  throw new IOException(s"Initial scroll failed: ${result.getErrorMessage}")
                }

                val scrollId = result.getJsonObject.get("_scroll_id").getAsString

                // Extract ALL results (hits + aggregations)
                val results =
                  extractAllResultsFromJest(result.getJsonObject, fieldAliases, aggregations)

                logger.info(
                  s"Initial scroll returned ${results.size} results, scrollId: $scrollId"
                )

                if (results.isEmpty) {
                  None
                } else {
                  Some((Some(scrollId), results))
                }

              case Some(scrollId) =>
                logger.debug(s"Fetching next scroll batch (scrollId: $scrollId)")

                val scrollBuilder = new SearchScroll.Builder(scrollId, config.scrollTimeout)

                val result = apply().execute(scrollBuilder.build())
                if (!result.isSucceeded) {
                  // Lancer une exception pour trigger le retry
                  throw new IOException(s"Scroll failed: ${result.getErrorMessage}")
                }
                val newScrollId = result.getJsonObject.get("_scroll_id").getAsString
                val results =
                  extractAllResultsFromJest(result.getJsonObject, fieldAliases, aggregations)

                logger.debug(s"Scroll returned ${results.size} results")

                if (results.isEmpty) {
                  clearJestScroll(scrollId)
                  None
                } else {
                  Some((Some(newScrollId), results))
                }
            }
          }
        }(system, logger).recover { case ex: Exception =>
          logger.error(s"Scroll failed after retries: ${ex.getMessage}", ex)
          scrollIdOpt.foreach(clearJestScroll)
          None
        }
      }
      .mapConcat(identity)
  }

  /** Search After (only for hits, more efficient)
    */
  override def searchAfter(
    elasticQuery: ElasticQuery,
    fieldAliases: Map[String, String],
    config: ScrollConfig,
    hasSorts: Boolean = false
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = {
    implicit val ec: ExecutionContext = system.dispatcher
    Source
      .unfoldAsync[Option[Seq[Any]], Seq[Map[String, Any]]](None) { searchAfterOpt =>
        retryWithBackoff(config.retryConfig) {
          Future {
            searchAfterOpt match {
              case None =>
                logger.info(
                  s"Starting search_after on indices: ${elasticQuery.indices.mkString(", ")}"
                )
              case Some(values) =>
                logger.debug(s"Fetching next search_after batch (after: ${values.mkString(", ")})")
            }

            val queryJson = new JsonParser().parse(elasticQuery.query).getAsJsonObject

            // Check if sorts already exist in the query
            if (!hasSorts && !queryJson.has("sort")) {
              // No sorting defined, add _id by default
              logger.warn(
                "No sort fields in query for search_after, adding default _id sort. " +
                "This may lead to inconsistent results if documents are updated during scroll."
              )
              val sortArray = new com.google.gson.JsonArray()
              val sortObj = new JsonObject()
              sortObj.addProperty("_id", "asc")
              sortArray.add(sortObj)
              queryJson.add("sort", sortArray)
            } else if (hasSorts && queryJson.has("sort")) {
              // Sorts already present, check that a tie-breaker exists
              val existingSorts = queryJson.getAsJsonArray("sort")
              val hasIdSort = existingSorts.asScala.exists { sortElem =>
                sortElem.isJsonObject && sortElem.getAsJsonObject.has("_id")
              }
              if (!hasIdSort) {
                // Add _id as tie-breaker
                logger.debug("Adding _id as tie-breaker to existing sorts")
                val tieBreaker = new JsonObject()
                tieBreaker.addProperty("_id", "asc")
                existingSorts.add(tieBreaker)
              }
            }

            queryJson.addProperty("size", config.scrollSize)

            // Add search_after
            searchAfterOpt.foreach { searchAfter =>
              val searchAfterArray = new com.google.gson.JsonArray()
              searchAfter.foreach {
                case s: String  => searchAfterArray.add(s)
                case n: Number  => searchAfterArray.add(n)
                case b: Boolean => searchAfterArray.add(b)
                case null       => searchAfterArray.add(JsonNull.INSTANCE)
                case other      => searchAfterArray.add(other.toString)
              }
              queryJson.add("search_after", searchAfterArray)
            }

            val searchBuilder = new Search.Builder(queryJson.toString)
            for (indice <- elasticQuery.indices) searchBuilder.addIndex(indice)
            for (t      <- elasticQuery.types) searchBuilder.addType(t)

            val result = apply().execute(searchBuilder.build())

            if (!result.isSucceeded) {
              throw new IOException(s"Search after failed: ${result.getErrorMessage}")
            }
            // Extract ONLY hits (no aggregations)
            val hits = extractHitsOnlyFromJest(result.getJsonObject, fieldAliases)

            if (hits.isEmpty) {
              None
            } else {
              val hitsArray = result.getJsonObject
                .getAsJsonObject("hits")
                .getAsJsonArray("hits")

              val lastHit = hitsArray.get(hitsArray.size() - 1).getAsJsonObject
              val nextSearchAfter = if (lastHit.has("sort")) {
                Some(
                  lastHit
                    .getAsJsonArray("sort")
                    .asScala
                    .map { elem =>
                      if (elem.isJsonPrimitive) {
                        val prim = elem.getAsJsonPrimitive
                        if (prim.isString) prim.getAsString
                        else if (prim.isBoolean) prim.getAsBoolean
                        else if (prim.isNumber) {
                          val num = prim.getAsNumber
                          if (num.toString.contains(".")) num.doubleValue()
                          else num.longValue()
                        } else prim.getAsString
                      } else if (elem.isJsonNull) {
                        null
                      } else {
                        elem.toString
                      }
                    }
                    .toSeq
                )
              } else {
                None
              }

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

  /** Extract ALL results: hits + aggregations
    */
  private def extractAllResultsFromJest(
    jsonObject: JsonObject,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): Seq[Map[String, Any]] = {
    val jsonString = jsonObject.toString
    val sqlResponse = ElasticResponse("", jsonString, fieldAliases, aggregations)

    parseResponse(sqlResponse) match {
      case Success(rows) => rows
      case Failure(ex) =>
        logger.error(s"Failed to parse Jest scroll response: ${ex.getMessage}", ex)
        Seq.empty
    }
  }

  /** Extract ONLY hits (for search_after)
    */
  private def extractHitsOnlyFromJest(
    jsonObject: JsonObject,
    fieldAliases: Map[String, String]
  ): Seq[Map[String, Any]] = {
    val jsonString = jsonObject.toString
    val sqlResponse = ElasticResponse("", jsonString, fieldAliases, Map.empty)

    parseResponse(sqlResponse) match {
      case Success(rows) => rows
      case Failure(ex) =>
        logger.error(s"Failed to parse Jest search after response: ${ex.getMessage}", ex)
        Seq.empty
    }
  }

  private def clearJestScroll(scrollId: String): Unit = {
    Try {
      logger.debug(s"Clearing Jest scroll: $scrollId")
      val clearScroll = new ClearScroll.Builder()
        .addScrollId(scrollId)
        .build()
      apply().execute(clearScroll)
    }.recover { case ex: Exception =>
      logger.warn(s"Failed to clear Jest scroll $scrollId: ${ex.getMessage}")
    }
  }
}

object JestClientApi extends SerializationApi {

  implicit def requestToSearch(elasticSelect: ElasticSearchRequest): Search = {
    import elasticSelect._
    val search = new Search.Builder(query)
    for (source <- sources) search.addIndex(source)
    search.build()
  }

  implicit class SearchSQLQuery(sqlQuery: SQLQuery) {
    def jestSearch: Option[Search] = {
      sqlQuery.request match {
        case Some(Left(value)) =>
          val request: ElasticSearchRequest = value
          Some(request)
        case _ => None
      }
    }
  }

  implicit class SearchElasticQuery(elasticQuery: ElasticQuery) {
    def search: (Search, JSONQuery) = {
      import elasticQuery._
      val _search = new Search.Builder(query)
      for (indice <- indices) _search.addIndex(indice)
      for (t      <- types) _search.addType(t)
      (_search.build(), query)
    }
  }

  implicit class SearchResults(searchResult: SearchResult) {
    def apply[M: Manifest]()(implicit formats: Formats): List[M] = {
      searchResult.getSourceAsStringList.asScala.map(source => serialization.read[M](source)).toList
    }
  }

  implicit class JestBulkAction(bulkableAction: BulkableAction[DocumentResult]) {
    def index: String = bulkableAction.getIndex
  }
}
