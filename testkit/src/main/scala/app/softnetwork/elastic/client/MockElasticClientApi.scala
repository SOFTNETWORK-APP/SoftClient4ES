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
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.result.ElasticResult
import app.softnetwork.elastic.client.scroll._
import app.softnetwork.elastic.sql.PainlessContextType
import app.softnetwork.elastic.sql.query.{SQLAggregation, SingleSearch}
import app.softnetwork.elastic.sql.schema.TableAlias
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions

/** Created by smanciot on 12/04/2020.
  */
trait MockElasticClientApi extends NopeClientApi {

  def elasticVersion: String

  protected lazy val logger: Logger = LoggerFactory getLogger getClass.getName

  protected val elasticDocuments: ElasticDocuments = new ElasticDocuments() {}

  private def allDocumentsAsHits(index: String): String = {
    val allDocuments = elasticDocuments.getAll
    val hits = allDocuments
      .map { doc =>
        s"""
           |{
           |  "_index": "$index",
           |  "_type": "_doc",
           |  "_id": "${doc._1}",
           |  "_score": 1.0,
           |  "_source": ${serialization.write(doc._2)(formats)}
           |}
           |""".stripMargin
      }
      .mkString(",")
    s"""
       |{
       |  "took": 1,
       |  "timed_out": false,
       |  "_shards": {
       |    "total": 1,
       |    "successful": 1,
       |    "skipped": 0,
       |    "failed": 0
       |  },
       |  "hits": {
       |    "total": {
       |      "value": ${allDocuments.keys.size},
       |      "relation": "eq"
       |    },
       |    "max_score": 1.0,
       |    "hits": [
       |      $hits
       |    ]
       |  }
       |}
       |""".stripMargin
  }

  // ==================== Closeable ====================

  override def close(): Unit = ()

  // ==================== VersionApi ====================

  override private[client] def executeVersion(): ElasticResult[String] =
    ElasticResult.success(elasticVersion)

  // ==================== IndicesApi ====================

  override private[client] def executeCreateIndex(
    index: String,
    settings: String,
    mappings: Option[String],
    aliases: Seq[TableAlias]
  ): ElasticResult[Boolean] =
    ElasticResult.success(true)

  override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] =
    ElasticResult.success(true)

  override private[client] def executeCloseIndex(index: String): ElasticResult[Boolean] =
    ElasticResult.success(true)

  override private[client] def executeOpenIndex(index: String): ElasticResult[Boolean] =
    ElasticResult.success(true)

  override private[client] def executeReindex(
    sourceIndex: String,
    targetIndex: String,
    refresh: Boolean,
    pipeline: Option[String]
  ): ElasticResult[(Boolean, Option[Long])] =
    ElasticResult.success((true, Some(elasticDocuments.getAll.keys.size)))

  override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
    ElasticResult.success(true)

  // ==================== AliasApi ====================

  override private[client] def executeAddAlias(
    alias: TableAlias
  ): ElasticResult[Boolean] =
    ElasticResult.success(true)

  override private[client] def executeRemoveAlias(
    index: String,
    alias: String
  ): ElasticResult[Boolean] =
    ElasticResult.success(true)

  override private[client] def executeAliasExists(alias: String): ElasticResult[Boolean] =
    ElasticResult.success(true)

  override private[client] def executeGetAliases(index: String): ElasticResult[String] =
    ElasticResult.success("{}")

  override private[client] def executeSwapAlias(
    oldIndex: String,
    newIndex: String,
    alias: String
  ): ElasticResult[Boolean] =
    ElasticResult.success(true)

  // ==================== SettingsApi ====================

  override private[client] def executeUpdateSettings(
    index: String,
    settings: String
  ): ElasticResult[Boolean] =
    ElasticResult.success(true)

  override private[client] def executeLoadSettings(index: String): ElasticResult[String] = {
    ElasticResult.success(
      s"""{"$index":{"settings":{"index":{"number_of_shards":"1","number_of_replicas":"1"}}}}"""
    )
  }

  // ==================== MappingApi ====================

  override private[client] def executeSetMapping(
    index: String,
    mapping: String
  ): ElasticResult[Boolean] =
    ElasticResult.success(true)

  override private[client] def executeGetMapping(index: String): ElasticResult[String] = {
    ElasticResult.success(s"""{"$index":{"mappings":{}}}""")
  }

  // ==================== RefreshApi ====================

  override private[client] def executeRefresh(index: String): ElasticResult[Boolean] =
    ElasticResult.success(true)

  // ==================== FlushApi ====================

  override private[client] def executeFlush(
    index: String,
    force: Boolean,
    wait: Boolean
  ): ElasticResult[Boolean] =
    ElasticResult.success(true)

  // ==================== IndexApi ====================

  override private[client] def executeIndex(
    index: String,
    id: String,
    source: String,
    wait: Boolean
  ): ElasticResult[Boolean] =
    ElasticResult.success {
      elasticDocuments.createOrUpdate(serialization.read(source), id)
      true
    }

  override private[client] def executeIndexAsync(
    index: String,
    id: String,
    source: String,
    wait: Boolean
  )(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]] =
    Future {
      executeIndex(index, id, source, wait)
    }

  // ==================== UpdateApi ====================

  override private[client] def executeUpdate(
    index: String,
    id: String,
    source: String,
    upsert: Boolean,
    wait: Boolean
  ): ElasticResult[Boolean] =
    ElasticResult.success {
      elasticDocuments.createOrUpdate(serialization.read(source), id)
      true
    }

  override private[client] def executeUpdateAsync(
    index: String,
    id: String,
    source: String,
    upsert: Boolean,
    wait: Boolean
  )(implicit ec: ExecutionContext): Future[ElasticResult[Boolean]] =
    Future {
      executeUpdate(index, id, source, upsert, wait)
    }

  // ==================== DeleteApi ====================

  override private[client] def executeDelete(
    index: String,
    id: String,
    wait: Boolean
  ): ElasticResult[Boolean] =
    ElasticResult.success(if (elasticDocuments.get(id).isDefined) {
      elasticDocuments.delete(id)
      true
    } else {
      false
    })

  override private[client] def executeDeleteAsync(index: String, id: String, wait: Boolean)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Boolean]] =
    Future {
      executeDelete(index, id, wait)
    }

  // ==================== GetApi ====================

  override private[client] def executeGet(
    index: String,
    id: String
  ): ElasticResult[Option[String]] =
    elasticDocuments.get(id) match {
      case Some(doc) => ElasticResult.success(Some(serialization.write(doc)(formats)))
      case None      => ElasticResult.success(None)
    }

  override private[client] def executeGetAsync(index: String, id: String)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[String]]] =
    Future {
      executeGet(index, id)
    }

  // ==================== CountApi ====================

  override private[client] def executeCount(query: ElasticQuery): ElasticResult[Option[Double]] =
    ElasticResult.success(
      Some(elasticDocuments.getAll.keys.size.toDouble)
    )

  override private[client] def executeCountAsync(query: ElasticQuery)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[Double]]] =
    Future {
      executeCount(query)
    }

  // ==================== SearchApi ====================

  override private[client] implicit def singleSearchToJsonQuery(singleSearch: SingleSearch)(implicit
    timestamp: Long,
    contextType: PainlessContextType = PainlessContextType.Query
  ): String =
    """{
      |  "query": {
      |    "match_all": {}
      |  }
      |}""".stripMargin

  override private[client] def executeSingleSearch(
    elasticQuery: ElasticQuery
  ): ElasticResult[Option[String]] =
    ElasticResult.success(
      Some(
        allDocumentsAsHits(
          elasticQuery.indices.headOption.getOrElse("default_index")
        )
      )
    )

  override private[client] def executeMultiSearch(
    elasticQueries: ElasticQueries
  ): ElasticResult[Option[String]] =
    ElasticResult.success(
      Some(
        allDocumentsAsHits(
          elasticQueries.queries.head.indices.headOption.getOrElse("default_index")
        )
      )
    )

  override private[client] def executeSingleSearchAsync(elasticQuery: ElasticQuery)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[String]]] =
    Future {
      executeSingleSearch(elasticQuery)
    }

  override private[client] def executeMultiSearchAsync(elasticQueries: ElasticQueries)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[Option[String]]] =
    Future {
      executeMultiSearch(elasticQueries)
    }

  // ==================== ScrollApi ====================

  override private[client] def scrollClassic(
    elasticQuery: ElasticQuery,
    fieldAliases: ListMap[String, String],
    aggregations: ListMap[String, SQLAggregation],
    config: ScrollConfig
  )(implicit system: ActorSystem): Source[ListMap[String, Any], NotUsed] =
    Source.single(elasticDocuments.getAll).mapConcat(_.values.toList)

  override private[client] def searchAfter(
    elasticQuery: ElasticQuery,
    fieldAliases: ListMap[String, String],
    config: ScrollConfig,
    hasSorts: Boolean
  )(implicit system: ActorSystem): Source[ListMap[String, Any], NotUsed] =
    scrollClassic(
      elasticQuery,
      fieldAliases,
      ListMap.empty,
      config
    )

  // ==================== BulkApi ====================

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
}

trait ElasticDocuments {

  private[this] var documents: Map[String, ListMap[String, AnyRef]] = Map()

  def createOrUpdate(entity: ListMap[String, AnyRef], uuid: String): Unit = {
    documents = documents.updated(uuid, entity)
  }

  def delete(uuid: String): Unit = {
    documents = documents - uuid
  }

  def getAll: Map[String, ListMap[String, AnyRef]] = documents

  def get(uuid: String): Option[ListMap[String, AnyRef]] = documents.get(uuid)

}
