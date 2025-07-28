package app.softnetwork.elastic.client

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Flow
import app.softnetwork.elastic.sql.SQLQuery
import org.json4s.Formats
import app.softnetwork.persistence.model.Timestamped
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{ExecutionContext, Future}
import scala.language.implicitConversions
import scala.reflect.ClassTag

/** Created by smanciot on 12/04/2020.
  */
trait MockElasticClientApi extends ElasticClientApi {

  protected lazy val log: Logger = LoggerFactory getLogger getClass.getName

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

  override def count(jsonQuery: JSONQuery): Option[Double] =
    throw new UnsupportedOperationException

  override def get[U <: Timestamped](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit m: Manifest[U], formats: Formats): Option[U] =
    elasticDocuments.get(id).asInstanceOf[Option[U]]

  override def search[U](sqlQuery: SQLQuery)(implicit m: Manifest[U], formats: Formats): List[U] =
    elasticDocuments.getAll.toList.asInstanceOf[List[U]]

  override def multiSearch[U](
    jsonQueries: JSONQueries
  )(implicit m: Manifest[U], formats: Formats): List[List[U]] =
    throw new UnsupportedOperationException

  override def index(index: String, indexType: String, id: String, source: String): Boolean =
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
    indexType: String,
    id: String,
    source: String,
    upsert: Boolean
  ): Boolean = {
    log.warn(s"MockElasticClient - $id not updated for $source")
    false
  }

  override def delete(uuid: String, index: String, indexType: String): Boolean = {
    if (elasticDocuments.get(uuid).isDefined) {
      elasticDocuments.delete(uuid)
      true
    } else {
      false
    }
  }

  override def refresh(index: String): Boolean = true

  override def flush(index: String, force: Boolean, wait: Boolean): Boolean = true

  override type A = this.type

  override def bulk(implicit
    bulkOptions: BulkOptions,
    system: ActorSystem
  ): Flow[Seq[A], R, NotUsed] =
    throw new UnsupportedOperationException

  override def bulkResult: Flow[R, Set[String], NotUsed] =
    throw new UnsupportedOperationException

  override type R = this.type

  override def toBulkAction(bulkItem: BulkItem): A =
    throw new UnsupportedOperationException

  override implicit def toBulkElasticAction(a: A): BulkElasticAction =
    throw new UnsupportedOperationException

  override implicit def toBulkElasticResult(r: R): BulkElasticResult =
    throw new UnsupportedOperationException

  override def multiSearchWithInnerHits[U, I](jsonQueries: JSONQueries, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[List[(U, List[I])]] = List.empty

  override def search[U](jsonQuery: JSONQuery)(implicit m: Manifest[U], formats: Formats): List[U] =
    List.empty

  override def searchWithInnerHits[U, I](jsonQuery: JSONQuery, innerField: String)(implicit
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
