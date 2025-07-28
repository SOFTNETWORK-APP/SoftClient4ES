package app.softnetwork.elastic.persistence.query

import app.softnetwork.elastic.client.ElasticClientApi
import app.softnetwork.elastic.sql.SQLQuery
import mustache.Mustache
import org.json4s.Formats
import app.softnetwork.persistence._
import app.softnetwork.persistence.model.Timestamped
import app.softnetwork.persistence.query.ExternalPersistenceProvider
import app.softnetwork.serialization.commonFormats
import app.softnetwork.elastic.persistence.typed.Elastic._
import com.sksamuel.exts.Logging

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/** Created by smanciot on 16/05/2020.
  */
trait ElasticProvider[T <: Timestamped] extends ExternalPersistenceProvider[T] with Logging {
  _: ElasticClientApi with ManifestWrapper[T] =>

  implicit def formats: Formats = commonFormats

  protected lazy val index: String = getIndex[T](manifestWrapper.wrapped)

  protected lazy val _type: String = getType[T](manifestWrapper.wrapped)

  protected lazy val alias: String = getAlias[T](manifestWrapper.wrapped)

  protected def mappingPath: Option[String] = None

  protected def loadMapping(path: Option[String] = None): String = {
    val pathOrElse: String = path.getOrElse(s"""mapping/${_type}.mustache""")
    Try(Mustache(pathOrElse).render(Map("type" -> _type))) match {
      case Success(s) =>
        s
      case Failure(f) =>
        logger.error(s"$pathOrElse -> f.getMessage", f)
        "{}"
    }
  }

  protected def initIndex(): Unit = {
    Try {
      createIndex(index)
      addAlias(index, alias)
      setMapping(index, loadMapping(mappingPath))
    } match {
      case Success(_) => logger.info(s"index:$index alias:$alias created")
      case Failure(f) =>
        logger.error(s"!!!!! index:$index type:${_type} alias:$alias -> ${f.getMessage}", f)
    }
  }

  // ExternalPersistenceProvider

  /** Creates the unerlying document to the external system
    *
    * @param document
    *   - the document to create
    * @param t
    *   - implicit ClassTag for T
    * @return
    *   whether the operation is successful or not
    */
  override def createDocument(document: T)(implicit t: ClassTag[T]): Boolean = {
    Try(index(document, Some(index), Some(_type))) match {
      case Success(_) => refresh(index)
      case Failure(f) =>
        logger.error(f.getMessage, f)
        false
    }
  }

  /** Updates the unerlying document to the external system
    *
    * @param document
    *   - the document to update
    * @param upsert
    *   - whether or not to create the underlying document if it does not exist in the external
    *     system
    * @param t
    *   - implicit ClassTag for T
    * @return
    *   whether the operation is successful or not
    */
  override def updateDocument(document: T, upsert: Boolean)(implicit t: ClassTag[T]): Boolean = {
    Try(update(document, Some(index), Some(_type), upsert)) match {
      case Success(_) => refresh(index)
      case Failure(f) =>
        logger.error(f.getMessage, f)
        false
    }
  }

  /** Deletes the unerlying document referenced by its uuid to the external system
    *
    * @param uuid
    *   - the uuid of the document to delete
    * @return
    *   whether the operation is successful or not
    */
  override def deleteDocument(uuid: String): Boolean = {
    Try(
      delete(uuid, index, _type)
    ) match {
      case Success(value) => value && refresh(index)
      case Failure(f) =>
        logger.error(f.getMessage, f)
        false
    }
  }

  /** Upserts the unerlying document referenced by its uuid to the external system
    *
    * @param uuid
    *   - the uuid of the document to upsert
    * @param data
    *   - a map including all the properties and values tu upsert for the document
    * @return
    *   whether the operation is successful or not
    */
  override def upsertDocument(uuid: String, data: String): Boolean = {
    if (logger.isDebugEnabled) {
      logger.debug(s"Upserting document $uuid for index $index with $data")
    }
    Try(
      update(
        index,
        _type,
        uuid,
        data,
        upsert = true
      )
    ) match {
      case Success(_) => refresh(index)
      case Failure(f) =>
        logger.error(f.getMessage, f)
        false
    }
  }

  /** Load the document referenced by its uuid
    *
    * @param uuid
    *   - the document uuid
    * @return
    *   the document retrieved, None otherwise
    */
  override def loadDocument(uuid: String)(implicit m: Manifest[T], formats: Formats): Option[T] = {
    Try(get(uuid, Some(index), Some(_type))) match {
      case Success(s) => s
      case Failure(f) =>
        logger.error(f.getMessage, f)
        None
    }
  }

  /** Search documents
    *
    * @param query
    *   - the search query
    * @return
    *   the documents founds or an empty list otherwise
    */
  override def searchDocuments(
    query: String
  )(implicit m: Manifest[T], formats: Formats): List[T] = {
    Try(search(SQLQuery(query))) match {
      case Success(s) => s
      case Failure(f) =>
        logger.error(f.getMessage, f)
        List.empty
    }
  }

}
