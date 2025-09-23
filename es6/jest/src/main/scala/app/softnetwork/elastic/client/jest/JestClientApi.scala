package app.softnetwork.elastic.client.jest

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import app.softnetwork.elastic.client._
import app.softnetwork.elastic.sql
import app.softnetwork.elastic.sql.query.{SQLQuery, SQLSearchRequest}
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.persistence.model.Timestamped
import app.softnetwork.serialization._
import com.google.gson.JsonParser
import io.searchbox.action.BulkableAction
import io.searchbox.core._
import io.searchbox.core.search.aggregation.RootAggregation
import io.searchbox.indices._
import io.searchbox.indices.aliases.{AddAliasMapping, ModifyAliases, RemoveAliasMapping}
import io.searchbox.indices.mapping.{GetMapping, PutMapping}
import io.searchbox.indices.reindex.Reindex
import io.searchbox.indices.settings.{GetSettings, UpdateSettings}
import io.searchbox.params.Parameters
import org.json4s.Formats

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
    with JestBulkApi

trait JestIndicesApi extends IndicesApi with JestRefreshApi with JestClientCompanion {
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

trait JestAliasApi extends AliasApi with JestClientCompanion {
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

trait JestSettingsApi extends SettingsApi with JestClientCompanion {
  _: IndicesApi =>
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

trait JestMappingApi extends MappingApi with JestClientCompanion {
  _: IndicesApi =>
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

trait JestRefreshApi extends RefreshApi with JestClientCompanion {
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

trait JestFlushApi extends FlushApi with JestClientCompanion {
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

trait JestCountApi extends CountApi with JestClientCompanion {
  override def countAsync(
    jsonQuery: JSONQuery
  )(implicit ec: ExecutionContext): Future[Option[Double]] = {
    import JestClientResultHandler._
    import jsonQuery._
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

  override def count(jsonQuery: JSONQuery): Option[Double] = {
    import jsonQuery._
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
  override def aggregate(
    sqlQuery: SQLQuery
  )(implicit ec: ExecutionContext): Future[Seq[SingleValueAggregateResult]] = {
    val aggregations: Seq[ElasticAggregation] = sqlQuery
    val futures = for (aggregation <- aggregations) yield {
      val promise: Promise[SingleValueAggregateResult] = Promise()
      val field = aggregation.field
      val sourceField = aggregation.sourceField
      val aggType = aggregation.aggType
      val aggName = aggregation.aggName
      val query = aggregation.query
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
          import JestClientApi._
          import JestClientResultHandler._
          apply()
            .executeAsyncPromise(
              JSONQuery(
                query,
                collection.immutable.Seq(sources: _*),
                collection.immutable.Seq.empty[String]
              ).search
            )
            .onComplete {
              case Success(result) =>
                val agg = aggName.split("\\.").last

                val itAgg = aggName.split("\\.").iterator

                var root =
                  if (aggregation.nested)
                    result.getAggregations.getAggregation(itAgg.next(), classOf[RootAggregation])
                  else
                    result.getAggregations

                if (aggregation.filtered) {
                  root = root.getAggregation(itAgg.next(), classOf[RootAggregation])
                }

                promise.success(
                  SingleValueAggregateResult(
                    field,
                    aggType,
                    aggType match {
                      case sql.function.aggregate.Count =>
                        if (aggregation.distinct)
                          NumericValue(
                            root.getCardinalityAggregation(agg).getCardinality.doubleValue()
                          )
                        else {
                          NumericValue(
                            root.getValueCountAggregation(agg).getValueCount.doubleValue()
                          )
                        }
                      case sql.function.aggregate.Sum =>
                        NumericValue(root.getSumAggregation(agg).getSum)
                      case sql.function.aggregate.Avg =>
                        NumericValue(root.getAvgAggregation(agg).getAvg)
                      case sql.function.aggregate.Min =>
                        NumericValue(root.getMinAggregation(agg).getMin)
                      case sql.function.aggregate.Max =>
                        NumericValue(root.getMaxAggregation(agg).getMax)
                      case _ => EmptyValue
                    },
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
      }
    }
    Future.sequence(futures)
  }
}

trait JestIndexApi extends IndexApi with JestClientCompanion {
  _: RefreshApi =>
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

trait JestUpdateApi extends UpdateApi with JestClientCompanion {
  _: RefreshApi =>
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

trait JestDeleteApi extends DeleteApi with JestClientCompanion {
  _: RefreshApi =>
  override def delete(uuid: String, index: String): Boolean = {
    Try(
      apply()
        .execute(
          new Delete.Builder(uuid).index(index).`type`("_doc").build()
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

  override def deleteAsync(uuid: String, index: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    import JestClientResultHandler._
    val promise: Promise[Boolean] = Promise()
    apply().executeAsyncPromise(
      new Delete.Builder(uuid).index(index).`type`("_doc").build()
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

trait JestGetApi extends GetApi with JestClientCompanion {

  // GetApi
  override def get[U <: Timestamped](
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

  override def getAsync[U <: Timestamped](
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

trait JestSearchApi extends SearchApi with JestClientCompanion {

  override implicit def sqlSearchRequestToJsonQuery(sqlSearch: SQLSearchRequest): String =
    implicitly[ElasticSearchRequest](sqlSearch).query

  import JestClientApi._

  override def search[U](
    jsonQuery: JSONQuery
  )(implicit m: Manifest[U], formats: Formats): List[U] = {
    import jsonQuery._
    val search = new Search.Builder(query)
    for (indice <- indices) search.addIndex(indice)
    for (t      <- types) search.addType(t)
    Try(
      apply()
        .execute(search.build())
        .getSourceAsStringList
        .asScala
        .map(source => serialization.read[U](source))
        .toList
    ) match {
      case Success(s) => s
      case Failure(f) =>
        logger.error(f.getMessage, f)
        List.empty
    }
  }

  override def searchAsync[U](
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

  override def searchWithInnerHits[U, I](jsonQuery: JSONQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[(U, List[I])] = {
    Try(apply().execute(jsonQuery.search)).toOption match {
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

  override def multiSearch[U](
    jsonQueries: JSONQueries
  )(implicit m: Manifest[U], formats: Formats): List[List[U]] = {
    tryOrElse(
      {
        val multiSearchResult =
          apply().execute(new MultiSearch.Builder(jsonQueries.queries.map(_.search).asJava).build())
        multiSearchResult.getResponses.asScala
          .map(searchResponse =>
            searchResponse.searchResult.getSourceAsStringList.asScala
              .map(source => serialization.read[U](source))
              .toList
          )
          .toList
      },
      List.empty
    )(logger)
  }

  override def multiSearchWithInnerHits[U, I](jsonQueries: JSONQueries, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[List[(U, List[I])]] = {
    val multiSearch = new MultiSearch.Builder(jsonQueries.queries.map(_.search).asJava).build()
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

trait JestBulkApi
    extends JestRefreshApi
    with JestSettingsApi
    with JestIndicesApi
    with BulkApi
    with JestClientCompanion {
  override type A = BulkableAction[DocumentResult]
  override type R = BulkResult

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

object JestClientApi {

  implicit def requestToSearch(elasticSelect: ElasticSearchRequest): Search = {
    import elasticSelect._
    Console.println(query)
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

  implicit class SearchJSONQuery(jsonQuery: JSONQuery) {
    def search: Search = {
      import jsonQuery._
      val _search = new Search.Builder(query)
      for (indice <- indices) _search.addIndex(indice)
      for (t      <- types) _search.addType(t)
      _search.build()
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
