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

package app.softnetwork.elastic.persistence.query

import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticSuccess}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.client.{ElasticClientApi, ElasticClientDelegator}
import app.softnetwork.elastic.sql.query.SQLQuery
import mustache.Mustache
import org.json4s.Formats
import app.softnetwork.persistence._
import app.softnetwork.persistence.model.Timestamped
import app.softnetwork.persistence.query.ExternalPersistenceProvider
import app.softnetwork.serialization.commonFormats
import app.softnetwork.elastic.persistence.typed.Elastic._
import org.slf4j.Logger

import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/** Created by smanciot on 16/05/2020.
  */
trait ElasticProvider[T <: Timestamped]
    extends ExternalPersistenceProvider[T]
    with ElasticClientDelegator {
  self: ManifestWrapper[T] =>

  lazy val delegate: ElasticClientApi = ElasticClientFactory.create(self.config)

  protected def logger: Logger

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
        logger.error(s"$pathOrElse -> ${f.getMessage}", f)
        "{}"
    }
  }

  protected def initIndex(): Unit = {
    updateMapping(index, loadMapping(mappingPath)) match {
      case ElasticSuccess(_) =>
        logger.info(s"index:$index type:${_type} mapping updated")
        addAlias(index, alias) match {
          case ElasticSuccess(_) => logger.info(s"index:$index type:${_type} alias:$alias created")
          case ElasticFailure(elasticError) =>
            logger.error(
              s"!!!!! index:$index type:${_type} alias:$alias -> ${elasticError.message}"
            )
        }
      case ElasticFailure(elasticError) =>
        logger.error(s"!!!!! index:$index type:${_type} mapping update -> ${elasticError.message}")
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
    indexAs(document, document.uuid, Some(index), Some(_type), wait = true) match {
      case ElasticSuccess(_) => true
      case ElasticFailure(elasticError) =>
        logger.error(s"${elasticError.message}")
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
    updateAs(document, document.uuid, Some(index), Some(_type), upsert) match {
      case ElasticSuccess(_) => true
      case ElasticFailure(elasticError) =>
        logger.error(s"${elasticError.message}")
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
    delete(uuid, index) match {
      case ElasticSuccess(value) => value
      case ElasticFailure(elasticError) =>
        logger.error(s"${elasticError.message}")
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
    logger.debug(s"Upserting document $uuid for index $index with $data")
    update(
      index,
      uuid,
      data,
      upsert = true
    ) match {
      case ElasticSuccess(_) => true
      case ElasticFailure(elasticError) =>
        logger.error(s"upsertDocument failed -> ${elasticError.message}")
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
    getAs(uuid, Some(index), Some(_type)) match {
      case ElasticSuccess(result) => result
      case ElasticFailure(elasticError) =>
        logger.error(s"loadDocument failed -> ${elasticError.message}")
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
    searchAs[T](SQLQuery(query)) match {
      case ElasticSuccess(results) => results.toList
      case ElasticFailure(elasticError) =>
        logger.error(s"searchDocuments failed -> ${elasticError.message}")
        List.empty[T]
    }
  }

}
