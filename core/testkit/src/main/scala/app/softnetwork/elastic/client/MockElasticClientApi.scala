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
import akka.stream.scaladsl.Flow
import app.softnetwork.elastic.sql.query.{SQLAggregation, SQLQuery, SQLSearchRequest}
import app.softnetwork.serialization._
import org.json4s.Formats
import app.softnetwork.persistence.model.Timestamped
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.reflect.ClassTag

/** Created by smanciot on 12/04/2020.
  */
trait MockElasticClientApi extends ElasticClientApi {

  protected lazy val logger: Logger = LoggerFactory getLogger getClass.getName

  implicit def sqlSearchRequestToJsonQuery(sqlSearch: SQLSearchRequest): String =
    """{
      |  "query": {
      |    "match_all": {}
      |  }
      |}""".stripMargin

  implicit def formats: Formats = commonFormats

  def allDocumentsAsHits(index: String): String = {
    val hits = elasticDocuments.getAll
      .map { doc =>
        s"""
           |{
           |  "_index": "$index",
           |  "_type": "_doc",
           |  "_id": "${doc.uuid}",
           |  "_score": 1.0,
           |  "_source": ${serialization.write(doc)(formats)}
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
       |      "value": ${elasticDocuments.getAll.size},
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

  /** Search for entities matching the given JSON query.
    *
    * @param elasticQuery
    *   - the JSON query to search for
    * @param fieldAliases
    *   - the field aliases to use for the search
    * @param aggregations
    *   - the aggregations to use for the search
    * @return
    *   the SQL Result containing the results of the query
    */
  override def search(
                       elasticQuery: ElasticQuery,
                       fieldAliases: Map[String, String],
                       aggregations: Map[String, SQLAggregation]
  ): ElasticResponse =
    ElasticResponse(
      """{
        |  "query": {
        |    "match_all": {}
        |  }
        |}""".stripMargin,
      allDocumentsAsHits(
        elasticQuery.indices.headOption.getOrElse("default_index")
      ),
      fieldAliases,
      aggregations
    )

  /** Perform a multi-search operation with the given JSON multi-search query.
    *
    * @param jsonQueries
    *   - the JSON multi-search query to perform
    * @param fieldAliases
    *   - the field aliases to use for the search
    * @param aggregations
    *   - the aggregations to use for the search
    * @return
    *   the SQL Result containing the results of the multi-search query
    */
  override def multisearch(
                            jsonQueries: ElasticQueries,
                            fieldAliases: Map[String, String],
                            aggregations: Map[String, SQLAggregation]
  ): ElasticResponse =
    ElasticResponse(
      """{
        |  "query": {
        |    "match_all": {}
        |  }
        |}""".stripMargin,
      allDocumentsAsHits(
        jsonQueries.queries.headOption.flatMap(_.indices.headOption).getOrElse("default_index")
      ),
      fieldAliases,
      aggregations
    )

  protected val elasticDocuments: ElasticDocuments = new ElasticDocuments() {}

  override def toggleRefresh(index: String, enable: Boolean): Boolean = true

  override def setReplicas(index: String, replicas: Int): Boolean = true

  override def updateSettings(index: String, settings: String) = true

  override def addAlias(index: String, alias: String): Boolean = true

  /** Remove an alias from the given index.
    *
    * @param index
    *   - the name of the index
    * @param alias
    *   - the name of the alias
    * @return
    *   true if the alias was removed successfully, false otherwise
    */
  override def removeAlias(index: String, alias: String): Boolean = true

  override def createIndex(index: String, settings: String): Boolean = true

  override def setMapping(index: String, mapping: String): Boolean = true

  override def deleteIndex(index: String): Boolean = true

  override def closeIndex(index: String): Boolean = true

  override def openIndex(index: String): Boolean = true

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
  override def reindex(sourceIndex: String, targetIndex: String, refresh: Boolean = true): Boolean =
    true

  /** Check if an index exists.
    *
    * @param index
    *   - the name of the index to check
    * @return
    *   true if the index exists, false otherwise
    */
  override def indexExists(index: String): Boolean = false

  override def count(elasticQuery: ElasticQuery): Option[Double] =
    throw new UnsupportedOperationException

  override def get[U <: Timestamped](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit m: Manifest[U], formats: Formats): Option[U] =
    elasticDocuments.get(id).asInstanceOf[Option[U]]

  override def index(index: String, id: String, source: String): Boolean =
    throw new UnsupportedOperationException

  override def update[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None,
    upsert: Boolean = true
  )(implicit u: ClassTag[U], formats: Formats): Boolean = {
    elasticDocuments.createOrUpdate(entity)
    true
  }

  override def update(
    index: String,
    id: String,
    source: String,
    upsert: Boolean
  ): Boolean = {
    logger.warn(s"MockElasticClient - $id not updated for $source")
    false
  }

  override def delete(uuid: String, index: String): Boolean = {
    if (elasticDocuments.get(uuid).isDefined) {
      elasticDocuments.delete(uuid)
      true
    } else {
      false
    }
  }

  override def refresh(index: String): Boolean = true

  override def flush(index: String, force: Boolean, wait: Boolean): Boolean = true

  override type BulkActionType = this.type

  override def bulk(implicit
    bulkOptions: BulkOptions,
    system: ActorSystem
  ): Flow[Seq[A], R, NotUsed] =
    throw new UnsupportedOperationException

  override def bulkResult: Flow[R, Set[String], NotUsed] =
    throw new UnsupportedOperationException

  override type BulkResultType = this.type

  override def toBulkAction(bulkItem: BulkItem): A =
    throw new UnsupportedOperationException

  override implicit def toBulkElasticAction(a: A): BulkElasticAction =
    throw new UnsupportedOperationException

  override implicit def toBulkElasticResult(r: R): BulkElasticResult =
    throw new UnsupportedOperationException

  override def multisearchWithInnerHits[U, I](jsonQueries: ElasticQueries, innerField: String)(implicit
                                                                                               m1: Manifest[U],
                                                                                               m2: Manifest[I],
                                                                                               formats: Formats
  ): List[List[(U, List[I])]] = List.empty

  override def searchWithInnerHits[U, I](elasticQuery: ElasticQuery, innerField: String)(implicit
                                                                                      m1: Manifest[U],
                                                                                      m2: Manifest[I],
                                                                                      formats: Formats
  ): List[(U, List[I])] = List.empty

  override def getMapping(index: String): String =
    throw new UnsupportedOperationException

  override def aggregate(sqlQuery: SQLQuery)(implicit
    ec: ExecutionContext
  ): Future[Seq[SingleValueAggregateResult]] =
    throw new UnsupportedOperationException

  override def loadSettings(index: String): String =
    throw new UnsupportedOperationException
}

trait ElasticDocuments {

  private[this] var documents: Map[String, Timestamped] = Map()

  def createOrUpdate(entity: Timestamped): Unit = {
    documents = documents.updated(entity.uuid, entity)
  }

  def delete(uuid: String): Unit = {
    documents = documents - uuid
  }

  def getAll: Iterable[Timestamped] = documents.values

  def get(uuid: String): Option[Timestamped] = documents.get(uuid)

}
