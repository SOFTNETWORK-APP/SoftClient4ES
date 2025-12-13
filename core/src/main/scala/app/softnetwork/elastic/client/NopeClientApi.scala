package app.softnetwork.elastic.client

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import app.softnetwork.elastic.sql.query
import app.softnetwork.elastic.sql.query.SQLAggregation
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.client.scroll._

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

trait NopeClientApi extends ElasticClientApi {

  override private[client] def executeAddAlias(
    index: String,
    alias: String
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
    aliases: Seq[String]
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
  override private[client] implicit def sqlSearchRequestToJsonQuery(
    sqlSearch: query.SingleSearch
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
}
