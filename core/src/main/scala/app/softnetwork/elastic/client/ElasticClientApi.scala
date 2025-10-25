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

import java.time.LocalDate
import java.time.format.DateTimeFormatter
import akka.NotUsed
import akka.actor.ActorSystem
import _root_.akka.stream.{FlowShape, Materializer}
import akka.stream.scaladsl._
import app.softnetwork.persistence.model.Timestamped
import app.softnetwork.serialization._
import app.softnetwork.elastic.sql.query.{
  SQLAggregation,
  SQLMultiSearchRequest,
  SQLQuery,
  SQLSearchRequest,
  SortOrder
}
import com.google.gson.JsonParser
import com.typesafe.config.{Config, ConfigFactory}
import org.json4s.{DefaultFormats, Formats, JNothing}
import org.json4s.jackson.JsonMethods._
import org.slf4j.Logger

import java.util.UUID
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration
import scala.language.{implicitConversions, postfixOps, reflectiveCalls}
import scala.reflect.ClassTag
import scala.util.{Failure, Success, Try}

/** Created by smanciot on 28/06/2018.
  */
trait ElasticClientApi
    extends IndicesApi
    with SettingsApi
    with AliasApi
    with MappingApi
    with CountApi
    with SingleValueAggregateApi
    with ScrollApi
    with IndexApi
    with UpdateApi
    with GetApi
    with BulkApi
    with DeleteApi
    with RefreshApi
    with FlushApi { _: { def logger: Logger } =>

  def config: Config = ConfigFactory.load()

  final lazy val elasticConfig: ElasticConfig = ElasticConfig(config)
}

trait IndicesApi {

  /** Default settings for indices. This is used when creating an index without providing specific
    * settings. It includes ngram tokenizer and analyzer, as well as some default limits.
    */
  val defaultSettings: String =
    """
      |{
      |  "index": {
      |    "max_ngram_diff": "20",
      |    "mapping" : {
      |      "total_fields" : {
      |        "limit" : "2000"
      |      }
      |    },
      |    "analysis": {
      |      "analyzer": {
      |        "ngram_analyzer": {
      |          "tokenizer": "ngram_tokenizer",
      |          "filter": [
      |            "lowercase",
      |            "asciifolding"
      |          ]
      |        },
      |        "search_analyzer": {
      |          "type": "custom",
      |          "tokenizer": "standard",
      |          "filter": [
      |            "lowercase",
      |            "asciifolding"
      |          ]
      |        }
      |      },
      |      "tokenizer": {
      |        "ngram_tokenizer": {
      |          "type": "ngram",
      |          "min_gram": 1,
      |          "max_gram": 20,
      |          "token_chars": [
      |            "letter",
      |            "digit"
      |          ]
      |        }
      |      }
      |    }
      |  }
      |}
    """.stripMargin

  /** Create an index with the provided name and settings.
    * @param index
    *   - the name of the index to create
    * @param settings
    *   - the settings to apply to the index (default is defaultSettings)
    * @return
    *   true if the index was created successfully, false otherwise
    */
  def createIndex(index: String, settings: String = defaultSettings): Boolean

  /** Delete an index with the provided name.
    * @param index
    *   - the name of the index to delete
    * @return
    *   true if the index was deleted successfully, false otherwise
    */
  def deleteIndex(index: String): Boolean

  /** Close an index with the provided name.
    * @param index
    *   - the name of the index to close
    * @return
    *   true if the index was closed successfully, false otherwise
    */
  def closeIndex(index: String): Boolean

  /** Open an index with the provided name.
    * @param index
    *   - the name of the index to open
    * @return
    *   true if the index was opened successfully, false otherwise
    */
  def openIndex(index: String): Boolean

  /** Reindex from source index to target index.
    * @param sourceIndex
    *   - the name of the source index
    * @param targetIndex
    *   - the name of the target index
    * @param refresh
    *   - true to refresh the target index after reindexing, false otherwise
    * @return
    *   true if the reindexing was successful, false otherwise
    */
  def reindex(sourceIndex: String, targetIndex: String, refresh: Boolean = true): Boolean

  /** Check if an index exists.
    * @param index
    *   - the name of the index to check
    * @return
    *   true if the index exists, false otherwise
    */
  def indexExists(index: String): Boolean
}

trait AliasApi {

  /** Add an alias to the given index.
    * @param index
    *   - the name of the index
    * @param alias
    *   - the name of the alias
    * @return
    *   true if the alias was added successfully, false otherwise
    */
  def addAlias(index: String, alias: String): Boolean

  /** Remove an alias from the given index.
    * @param index
    *   - the name of the index
    * @param alias
    *   - the name of the alias
    * @return
    *   true if the alias was removed successfully, false otherwise
    */
  def removeAlias(index: String, alias: String): Boolean
}

trait SettingsApi { _: IndicesApi =>

  /** Toggle the refresh interval of an index.
    * @param index
    *   - the name of the index
    * @param enable
    *   - true to enable the refresh interval, false to disable it
    * @return
    *   true if the settings were updated successfully, false otherwise
    */
  def toggleRefresh(index: String, enable: Boolean): Boolean = {
    updateSettings(
      index,
      if (!enable) """{"index" : {"refresh_interval" : -1} }"""
      else """{"index" : {"refresh_interval" : "1s"} }"""
    )
  }

  /** Set the number of replicas for an index.
    * @param index
    *   - the name of the index
    * @param replicas
    *   - the number of replicas to set
    * @return
    *   true if the settings were updated successfully, false otherwise
    */
  def setReplicas(index: String, replicas: Int): Boolean = {
    updateSettings(index, s"""{"index" : {"number_of_replicas" : $replicas} }""")
  }

  /** Update the settings of an index.
    * @param index
    *   - the name of the index
    * @param settings
    *   - the settings to apply to the index (default is defaultSettings)
    * @return
    *   true if the settings were updated successfully, false otherwise
    */
  def updateSettings(index: String, settings: String = defaultSettings): Boolean

  /** Load the settings of an index.
    * @param index
    *   - the name of the index to load the settings for
    * @return
    *   the settings of the index as a JSON string
    */
  def loadSettings(index: String): String
}

trait MappingApi extends IndicesApi with RefreshApi {

  protected def logger: Logger

  /** Set the mapping of an index.
    * @param index
    *   - the name of the index to set the mapping for
    * @param mapping
    *   - the mapping to set on the index
    * @return
    *   true if the mapping was set successfully, false otherwise
    */
  def setMapping(index: String, mapping: String): Boolean

  /** Get the mapping of an index.
    * @param index
    *   - the name of the index to get the mapping for
    * @return
    *   the mapping of the index as a JSON string
    */
  def getMapping(index: String): String

  /** Get the mapping properties of an index.
    * @param index
    *   - the name of the index to get the mapping properties for
    * @return
    *   the mapping properties of the index as a JSON string
    */
  def getMappingProperties(index: String): String = {
    val mapping = tryOrElse(getMapping(index), "{\"mappings\": {\"properties\": {}}}")(logger)
    Try(
      new JsonParser()
        .parse(mapping)
        .getAsJsonObject
        .get("mappings")
        .toString
    ) match {
      case Success(properties) => properties
      case Failure(exception) =>
        logger.error(s"Failed to parse mapping properties for index $index and $mapping", exception)
        "{\"properties\": {}}" // Return an empty properties object in case of failure
    }
  }

  /** Check if the mapping of an index is different from the provided mapping.
    * @param index
    *   - the name of the index to check
    * @param mapping
    *   - the mapping to compare with the current mapping of the index
    * @return
    *   true if the mapping is different, false otherwise
    */
  def shouldUpdateMapping(
    index: String,
    mapping: String
  ): Boolean = {
    MappingComparator.isMappingDifferent(this.getMappingProperties(index), mapping)
  }

  /** Update the mapping of an index to a new mapping.
    * @param index
    *   - the name of the index to migrate
    * @param mapping
    *   - the new mapping to set on the index
    * @param settings
    *   - the settings to apply to the index (default is defaultSettings)
    * @return
    *   true if the mapping was updated successfully, false otherwise
    */
  def updateMapping(
    index: String,
    mapping: String,
    settings: String = defaultSettings
  ): Boolean = {
    // Check if the index exists
    if (!tryOrElse(this.indexExists(index), false)(logger)) {
      if (!tryOrElse(this.createIndex(index, settings), false)(logger)) {
        logger.error(s"Failed to create index: $index")
        return false
      }
      logger.info(s"Index $index created successfully.")
      if (!tryOrElse(this.setMapping(index, mapping), false)(logger)) {
        logger.error(s"Failed to set mapping for index: $index")
        return false
      }
      logger.info(s"Mapping for index $index set successfully.")
      true
    }
    // Check if the mapping needs to be updated
    else if (shouldUpdateMapping(index, mapping)) {
      val tempIndex = index + "_tmp_" + UUID.randomUUID()
      var tempCreated = false
      var originalDeleted = false
      logger.info("--- Starting dynamic mapping migration ---")
      logger.info("Target index: " + index)
      logger.info("Temporary index: " + tempIndex)
      def migrate(): Boolean = {
        // Create a temporary index with the new mapping
        tempCreated = tryOrElse(this.createIndex(tempIndex, settings), false)(logger)
        if (tempCreated) {
          logger.info(s"Temporary index $tempIndex created successfully.")
          // Set the new mapping on the temporary index
          if (!tryOrElse(this.setMapping(tempIndex, mapping), false)(logger)) {
            logger.error(s"Failed to set mapping for temporary index: $tempIndex")
            return false
          }
          logger.info(s"Mapping for temporary index $tempIndex set successfully.")
          // Reindex from the original index to the temporary index
          if (!tryOrElse(this.reindex(index, tempIndex), false)(logger)) {
            logger.error(
              s"Failed to reindex from original index: $index to temporary index: $tempIndex"
            )
            return false
          }
          logger.info(
            s"Reindexing from original index $index to temporary index $tempIndex completed successfully."
          )
          // Delete the original index
          originalDeleted = this.deleteIndex(index)
          if (originalDeleted) {
            logger.info(s"Original index $index deleted successfully.")
            // Rename the temporary index to the original index name
            if (!tryOrElse(this.createIndex(index, settings), false)(logger)) {
              logger.error(s"Failed to recreate original index: $index")
              return false
            }
            logger.info(s"Original index $index recreated successfully.")
            if (!tryOrElse(this.setMapping(index, mapping), false)(logger)) {
              logger.error(s"Failed to set mapping for original index: $index")
              return false
            }
            logger.info(s"Mapping for original index $index set successfully.")
            if (!tryOrElse(this.reindex(tempIndex, index), false)(logger)) {
              logger.error(
                s"Failed to reindex from temporary index: $tempIndex to original index: $index"
              )
              return false
            }
            logger.info(
              s"Reindexing from temporary index $tempIndex to original index $index completed successfully."
            )
            if (!tryOrElse(this.openIndex(index), false)(logger)) {
              logger.error(s"Failed to open original index: $index")
              return false
            }
            logger.info(s"Original index $index opened successfully.")
            logger.info("Dynamic mapping migration completed successfully.")
            true
          } else {
            logger.error(s"Failed to delete original index: $index")
            false
          }
        } else {
          logger.error(s"Failed to create temporary index: $tempIndex")
          false
        }
      }
      val migration = Try(migrate()) match {
        case Success(result) => result
        case Failure(exception) =>
          logger.error("Exception during dynamic mapping migration", exception)
          false
      }
      if (!migration) {
        logger.error("Error during dynamic mapping migration")
        if (originalDeleted) {
          // If the original index was deleted, we need to recreate it
          if (!tryOrElse(this.createIndex(index, settings), false)(logger)) {
            logger.error(s"Failed to recreate original index: $index")
          } else {
            logger.info(s"Original index $index recreated successfully.")
            // Set the original mapping back
            if (!tryOrElse(this.setMapping(index, mapping), false)(logger)) {
              logger.error(s"Failed to set mapping for original index: $index")
            } else {
              logger.info(s"Mapping for original index $index set successfully.")
              if (!tryOrElse(this.reindex(tempIndex, index), false)(logger)) {
                logger.error(
                  s"Failed to reindex from temporary index $tempIndex to original index $index"
                )
              } else {
                logger.info(
                  s"Reindexing from temporary index $tempIndex to original index $index completed successfully."
                )
                if (!tryOrElse(this.refresh(index), false)(logger)) {
                  logger.error(s"Failed to refresh original index: $index")
                } else {
                  logger.info(s"Original index $index refreshed successfully.")
                }
              }
            }
          }
        }
      }
      if (tempCreated) {
        // Clean up the temporary index if it was created
        if (!tryOrElse(this.deleteIndex(tempIndex), false)(logger)) {
          logger.error(s"Failed to delete temporary index: $tempIndex")
        } else {
          logger.info(s"Temporary index $tempIndex deleted successfully.")
        }
      } else {
        logger.error(s"Temporary index $tempIndex was not created, skipping deletion.")
      }
      migration
    } else {
      false
    }
  }
}

trait RefreshApi {

  /** Refresh the index to make sure all documents are indexed and searchable.
    * @param index
    *   - the name of the index to refresh
    * @return
    *   true if the index was refreshed successfully, false otherwise
    */
  def refresh(index: String): Boolean
}

trait FlushApi {

  /** Flush the index to make sure all operations are written to disk.
    * @param index
    *   - the name of the index to flush
    * @param force
    *   - true to force the flush, false otherwise
    * @param wait
    *   - true to wait for the flush to complete, false otherwise
    * @return
    *   true if the index was flushed successfully, false otherwise
    */
  def flush(index: String, force: Boolean = true, wait: Boolean = true): Boolean
}

trait IndexApi { _: RefreshApi =>

  /** Index an entity in the given index.
    * @param entity
    *   - the entity to index
    * @param index
    *   - the name of the index to index the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   true if the entity was indexed successfully, false otherwise
    */
  def index[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit u: ClassTag[U], formats: Formats): Boolean = {
    val indexType = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    this.index(
      index.getOrElse(indexType),
      entity.uuid,
      serialization.write[U](entity)
    )
  }

  /** Index an entity in the given index.
    * @param index
    *   - the name of the index to index the entity in
    * @param id
    *   - the id of the entity to index
    * @param source
    *   - the source of the entity to index in JSON format
    * @return
    *   true if the entity was indexed successfully, false otherwise
    */
  def index(index: String, id: String, source: String): Boolean

  /** Index an entity in the given index asynchronously.
    * @param entity
    *   - the entity to index
    * @param index
    *   - the name of the index to index the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   a Future that completes with true if the entity was indexed successfully, false otherwise
    */
  def indexAsync[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit u: ClassTag[U], ec: ExecutionContext, formats: Formats): Future[Boolean] = {
    val indexType = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    indexAsync(index.getOrElse(indexType), entity.uuid, serialization.write[U](entity))
  }

  /** Index an entity in the given index asynchronously.
    * @param index
    *   - the name of the index to index the entity in
    * @param id
    *   - the id of the entity to index
    * @param source
    *   - the source of the entity to index in JSON format
    * @return
    *   a Future that completes with true if the entity was indexed successfully, false otherwise
    */
  def indexAsync(index: String, id: String, source: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    Future {
      this.index(index, id, source)
    }
  }
}

trait UpdateApi { _: RefreshApi =>

  /** Update an entity in the given index.
    * @param entity
    *   - the entity to update
    * @param index
    *   - the name of the index to update the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   true if the entity was updated successfully, false otherwise
    */
  def update[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None,
    upsert: Boolean = true
  )(implicit u: ClassTag[U], formats: Formats): Boolean = {
    val indexType = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    this.update(
      index.getOrElse(indexType),
      entity.uuid,
      serialization.write[U](entity),
      upsert
    )
  }

  /** Update an entity in the given index.
    * @param index
    *   - the name of the index to update the entity in
    * @param id
    *   - the id of the entity to update
    * @param source
    *   - the source of the entity to update in JSON format
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   true if the entity was updated successfully, false otherwise
    */
  def update(index: String, id: String, source: String, upsert: Boolean): Boolean

  /** Update an entity in the given index asynchronously.
    * @param entity
    *   - the entity to update
    * @param index
    *   - the name of the index to update the entity in (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   a Future that completes with true if the entity was updated successfully, false otherwise
    */
  def updateAsync[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None,
    upsert: Boolean = true
  )(implicit u: ClassTag[U], ec: ExecutionContext, formats: Formats): Future[Boolean] = {
    val indexType = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    this
      .updateAsync(
        index.getOrElse(indexType),
        entity.uuid,
        serialization.write[U](entity),
        upsert
      )
  }

  /** Update an entity in the given index asynchronously.
    * @param index
    *   - the name of the index to update the entity in
    * @param id
    *   - the id of the entity to update
    * @param source
    *   - the source of the entity to update in JSON format
    * @param upsert
    *   - true to upsert the entity if it does not exist, false otherwise
    * @return
    *   a Future that completes with true if the entity was updated successfully, false otherwise
    */
  def updateAsync(index: String, id: String, source: String, upsert: Boolean)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    Future {
      this.update(index, id, source, upsert)
    }
  }
}

trait DeleteApi { _: RefreshApi =>

  /** Delete an entity from the given index.
    * @param entity
    *   - the entity to delete
    * @param index
    *   - the name of the index to delete the entity from (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   true if the entity was deleted successfully, false otherwise
    */
  def delete[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit u: ClassTag[U]): Boolean = {
    val indexType = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    delete(entity.uuid, index.getOrElse(indexType))
  }

  /** Delete an entity from the given index.
    * @param uuid
    *   - the id of the entity to delete
    * @param index
    *   - the name of the index to delete the entity from
    * @return
    *   true if the entity was deleted successfully, false otherwise
    */
  def delete(uuid: String, index: String): Boolean

  /** Delete an entity from the given index asynchronously.
    * @param entity
    *   - the entity to delete
    * @param index
    *   - the name of the index to delete the entity from (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   a Future that completes with true if the entity was deleted successfully, false otherwise
    */
  def deleteAsync[U <: Timestamped](
    entity: U,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit u: ClassTag[U], ec: ExecutionContext): Future[Boolean] = {
    val indexType = maybeType.getOrElse(u.runtimeClass.getSimpleName.toLowerCase)
    deleteAsync(entity.uuid, index.getOrElse(indexType))
  }

  /** Delete an entity from the given index asynchronously.
    * @param uuid
    *   - the id of the entity to delete
    * @param index
    *   - the name of the index to delete the entity from
    * @return
    *   a Future that completes with true if the entity was deleted successfully, false otherwise
    */
  def deleteAsync(uuid: String, index: String)(implicit
    ec: ExecutionContext
  ): Future[Boolean] = {
    Future {
      this.delete(uuid, index)
    }
  }

}

trait BulkApi { _: RefreshApi with SettingsApi =>
  type A
  type R

  def toBulkAction(bulkItem: BulkItem): A

  implicit def toBulkElasticAction(a: A): BulkElasticAction

  implicit def toBulkElasticResult(r: R): BulkElasticResult

  def bulk(implicit bulkOptions: BulkOptions, system: ActorSystem): Flow[Seq[A], R, NotUsed]

  def bulkResult: Flow[R, Set[String], NotUsed]

  /** +----------+
    * |          |
    * |  Source  |  items: Iterator[D]
    * |          |
    * +----------+
    *      |
    *      v
    * +----------+
    * |          |
    * |transform | BulkableAction
    * |          |
    * +----------+
    *      |
    *      v
    * +----------+
    * |          |
    * | settings | Update elasticsearch settings (refresh and replicas)
    * |          |
    * +----------+
    *      |
    *      v
    * +----------+
    * |          |
    * |  group   |
    * |          |
    * +----------+
    *      |
    *      v
    * +----------+        +----------+
    * |          |------->|          |
    * |  balance |        |   bulk   |
    * |          |------->|          |
    * +----------+        +----------+
    *                        |    |
    *                        |    |
    *                        |    |
    * +---------+            |    |
    * |         |<-----------'    |
    * |  merge  |                 |
    * |         |<----------------'
    * +---------+
    *      |
    *      v
    * +----------+
    * |          |
    * | result   | BulkResult
    * |          |
    * +----------+
    *      |
    *      v
    * +----------+
    * |          |
    * |   Sink   | indices: Set[String]
    * |          |
    * +----------+
    *
    * Asynchronously bulk items to Elasticsearch
    *
    * @param items         the items for which a bulk has to be performed
    * @param toDocument    the function to transform items to elastic documents in json format
    * @param idKey         the key mapping to the document id
    * @param suffixDateKey the key mapping to the date used to suffix the index
    * @param suffixDatePattern the date pattern used to suffix the index
    * @param update        whether to upsert or not the items
    * @param delete        whether to delete or not the items
    * @param parentIdKey   the key mapping to the elastic parent document id
    * @param bulkOptions   bulk options
    * @param system        actor system
    * @tparam D the type of the items
    * @return the indexes on which the documents have been indexed
    */
  def bulk[D](
    items: Iterator[D],
    toDocument: D => String,
    idKey: Option[String] = None,
    suffixDateKey: Option[String] = None,
    suffixDatePattern: Option[String] = None,
    update: Option[Boolean] = None,
    delete: Option[Boolean] = None,
    parentIdKey: Option[String] = None
  )(implicit bulkOptions: BulkOptions, system: ActorSystem): Set[String] = {

    implicit val materializer: Materializer = Materializer(system)

    import GraphDSL.Implicits._

    val source = Source.fromIterator(() => items)

    val sink = Sink.fold[Set[String], Set[String]](Set.empty[String])(_ ++ _)

    val g = Flow.fromGraph(GraphDSL.create() { implicit b =>
      val transform =
        b.add(
          Flow[D].map(item =>
            toBulkAction(
              toBulkItem(
                toDocument,
                idKey,
                suffixDateKey,
                suffixDatePattern,
                update,
                delete,
                parentIdKey,
                item
              )
            )
          )
        )

      val settings = b.add(BulkSettings[A](bulkOptions.disableRefresh)(this, toBulkElasticAction))

      val group = b.add(Flow[A].named("group").grouped(bulkOptions.maxBulkSize).map { items =>
        //          logger.info(s"Preparing to write batch of ${items.size}...")
        items
      })

      val parallelism = Math.max(1, bulkOptions.balance)

      val bulkFlow: FlowShape[Seq[A], R] = b.add(bulk)

      val result = b.add(bulkResult)

      if (parallelism > 1) {
        val balancer = b.add(Balance[Seq[A]](parallelism))

        val merge = b.add(Merge[R](parallelism))

        transform ~> settings ~> group ~> balancer

        1 to parallelism foreach { _ =>
          balancer ~> bulkFlow ~> merge
        }

        merge ~> result
      } else {
        transform ~> settings ~> group ~> bulkFlow ~> result
      }

      FlowShape(transform.in, result.out)
    })

    val future = source.via(g).toMat(sink)(Keep.right).run()

    val indices = Await.result(future, Duration.Inf)
    indices.foreach(refresh)
    indices
  }

  def toBulkItem[D](
    toDocument: D => String,
    idKey: Option[String],
    suffixDateKey: Option[String],
    suffixDatePattern: Option[String],
    update: Option[Boolean],
    delete: Option[Boolean],
    parentIdKey: Option[String],
    item: D
  )(implicit bulkOptions: BulkOptions): BulkItem = {

    implicit val formats: DefaultFormats = org.json4s.DefaultFormats
    val document = toDocument(item)
    val jsonMap = parse(document, useBigDecimalForDouble = false).extract[Map[String, Any]]
    // extract id
    val id = idKey.flatMap { i =>
      jsonMap.get(i).map(_.toString)
    }

    // extract final index name
    val index = suffixDateKey
      .flatMap { s =>
        // Expecting a date field YYYY-MM-dd ...
        jsonMap.get(s).map { d =>
          val strDate = d.toString.substring(0, 10)
          val date = LocalDate.parse(strDate, DateTimeFormatter.ofPattern("yyyy-MM-dd"))
          date.format(
            suffixDatePattern
              .map(DateTimeFormatter.ofPattern)
              .getOrElse(DateTimeFormatter.ofPattern("yyyy-MM-dd"))
          )
        }
      }
      .map(s => s"${bulkOptions.index}-$s")
      // use suffix if available otherwise only index
      .getOrElse(bulkOptions.index)

    // extract parent key
    val parent = parentIdKey.flatMap { i =>
      jsonMap.get(i).map(_.toString)
    }

    val action = delete match {
      case Some(d) if d => BulkAction.DELETE
      case _ =>
        update match {
          case Some(u) if u => BulkAction.UPDATE
          case _            => BulkAction.INDEX
        }
    }

    BulkItem(index, action, document, id, parent)
  }

}

trait CountApi {

  /** Count the number of documents matching the given JSON query asynchronously.
    * @param query
    *   - the query to count the documents for
    * @return
    *   the number of documents matching the query, or None if the count could not be determined
    */
  def countAsync(query: JSONQuery)(implicit ec: ExecutionContext): Future[Option[Double]] = {
    Future {
      this.count(query)
    }
  }

  /** Count the number of documents matching the given JSON query.
    * @param query
    *   - the query to count the documents for
    * @return
    *   the number of documents matching the query, or None if the count could not be determined
    */
  def count(query: JSONQuery): Option[Double]

}

trait AggregateApi[T <: AggregateResult] {

  /** Aggregate the results of the given SQL query.
    * @param sqlQuery
    *   - the query to aggregate the results for
    * @return
    *   a sequence of aggregated results
    */
  def aggregate(sqlQuery: SQLQuery)(implicit
    ec: ExecutionContext
  ): Future[_root_.scala.collection.Seq[T]]
}

trait SingleValueAggregateApi extends AggregateApi[SingleValueAggregateResult] {
  _: SearchApi with ElasticConversion =>

  /** Aggregate the results of the given SQL query.
    *
    * @param sqlQuery
    *   - the query to aggregate the results for
    * @return
    *   a sequence of aggregated results
    */
  override def aggregate(
    sqlQuery: SQLQuery
  )(implicit ec: ExecutionContext): Future[collection.Seq[SingleValueAggregateResult]] = {
    Future {
      val response = search(sqlQuery)
      parseResponse(response) match {
        case Success(results) =>
          results.flatMap(result =>
            response.aggregations
              .filterNot(_._2.multivalued)
              .map { case (name, aggregation) =>
                SingleValueAggregateResult(
                  name,
                  aggregation.aggType,
                  result.getOrElse(name, null) match {
                    case n: Number           => NumericValue(n.doubleValue())
                    case s: String           => StringValue(s)
                    case m: Map[String, Any] => ObjectValue(m)
                    case _                   => EmptyValue
                  }
                )
              }
              .toSeq
          )
        case Failure(exception) =>
          throw new IllegalArgumentException(
            s"Failed to parse search results for SQL query: ${sqlQuery.query}",
            exception
          )
      }
    }
  }
}

trait GetApi {

  /** Get an entity by its id from the given index.
    * @param id
    *   - the id of the entity to get
    * @param index
    *   - the name of the index to get the entity from (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   an Option containing the entity if it was found, None otherwise
    */
  def get[U <: Timestamped](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit m: Manifest[U], formats: Formats): Option[U]

  /** Get an entity by its id from the given index asynchronously.
    * @param id
    *   - the id of the entity to get
    * @param index
    *   - the name of the index to get the entity from (default is the entity type name)
    * @param maybeType
    *   - the type of the entity (default is the entity class name in lowercase)
    * @return
    *   a Future that completes with an Option containing the entity if it was found, None otherwise
    */
  def getAsync[U <: Timestamped](
    id: String,
    index: Option[String] = None,
    maybeType: Option[String] = None
  )(implicit m: Manifest[U], ec: ExecutionContext, formats: Formats): Future[Option[U]] = {
    Future {
      this.get[U](id, index, maybeType)
    }
  }
}

trait SearchApi extends ElasticConversion {
  implicit def sqlSearchRequestToJsonQuery(sqlSearch: SQLSearchRequest): String

  implicit def sqlQueryToJSONQuery(sqlQuery: SQLQuery): JSONQuery = {
    sqlQuery.request match {
      case Some(Left(value)) =>
        JSONQuery(value.copy(score = sqlQuery.score), collection.immutable.Seq(value.sources: _*))
      case _ =>
        throw new IllegalArgumentException(
          s"SQL query ${sqlQuery.query} does not contain a valid search request"
        )
    }
  }

  implicit def sqlQueryToJSONQueries(sqlQuery: SQLQuery): JSONQueries = {
    sqlQuery.request match {
      case Some(Right(value)) =>
        JSONQueries(
          value.requests
            .map(request =>
              JSONQuery(
                request.copy(score = sqlQuery.score),
                collection.immutable.Seq(request.sources: _*)
              )
            )
            .toList
        )
      case _ =>
        throw new IllegalArgumentException(
          s"SQL query ${sqlQuery.query} does not contain a valid search request"
        )
    }
  }

  /** Search for entities matching the given SQL query.
    * @param sql
    *   - the SQL query to search for
    * @return
    *   the SQL search response containing the results of the query
    */
  final def search(sql: SQLQuery): SQLSearchResponse = {
    sql.request match {
      case Some(Left(single)) => search(single.copy(score = sql.score))
      case Some(Right(multiple)) =>
        multisearch(multiple.copy(requests = multiple.requests.map(r => r.copy(score = sql.score))))
      case None =>
        throw new IllegalArgumentException("SQL query does not contain a valid search request")
    }
  }

  /** Search for entities matching the given SQL query.
    * @param sql
    *   - the SQL query to search for
    * @return
    *   the SQL search response containing the results of the query
    */
  private[this] def search(sql: SQLSearchRequest): SQLSearchResponse = {
    // Build the JSON query from the SQL query
    val jsonQuery =
      JSONQuery(
        sql,
        collection.immutable.Seq(sql.sources: _*)
      )
    search(jsonQuery, sql.fieldAliases, sql.sqlAggregations)
  }

  /** Search for entities matching the given JSON query.
    * @param jsonQuery
    *   - the query to search for
    * @param fieldAliases
    *   - the field aliases to use for the search
    * @param aggregations
    *   - the aggregations to use for the search
    * @return
    *   the SQL search response containing the results of the query
    */
  def search(
    jsonQuery: JSONQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): SQLSearchResponse

  /** Perform a multi-search operation with the given SQL query.
    * @param sql
    *   - the SQL multi-search query to perform
    * @return
    *   the SQL search response containing the results of the multi-search query
    */
  private[this] def multisearch(sql: SQLMultiSearchRequest): SQLSearchResponse = {
    // Build the JSON queries from the SQL multi-search query
    val jsonQueries: JSONQueries =
      JSONQueries(
        sql.requests.map { query =>
          JSONQuery(
            query,
            collection.immutable.Seq(query.sources: _*)
          )
        }.toList
      )
    multisearch(jsonQueries, sql.fieldAliases, sql.sqlAggregations)
  }

  /** Perform a multi-search operation with the given JSON queries.
    * @param jsonQueries
    *   - the JSON queries to perform the multi-search for
    * @param fieldAliases
    *   - the field aliases to use for the search
    * @param aggregations
    *   - the aggregations to use for the search
    * @return
    *   the SQL search response containing the results of the multi-search query
    */
  def multisearch(
    jsonQueries: JSONQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  ): SQLSearchResponse

  /** Search for entities matching the given JSON query.
    * @param jsonQuery
    *   - the query to search for
    * @param fieldAliases
    *   - the field aliases to use for the search
    * @param aggregations
    *   - the aggregations to use for the search
    * @param m
    *   - the manifest of the type to search for
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of entities matching the query
    */
  def searchAs[U](
    jsonQuery: JSONQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )(implicit
    m: Manifest[U],
    formats: Formats
  ): List[U] = {
    convertTo[U](search(jsonQuery, fieldAliases, aggregations)).getOrElse(Seq.empty).toList
  }

  /** Search for entities matching the given SQL query.
    * @param sqlQuery
    *   - the SQL query to search for
    * @param m
    *   - the manifest of the type to search for
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of entities matching the query
    */
  final def searchAs[U](sqlQuery: SQLQuery)(implicit m: Manifest[U], formats: Formats): List[U] = {
    convertTo[U](search(sqlQuery)).getOrElse(Seq.empty).toList
  }

  /** Search for entities matching the given SQL query asynchronously.
    * @param sqlQuery
    *   - the SQL query to search for
    * @param m
    *   - the manifest of the type to search for
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a Future that completes with a list of entities matching the query
    */
  def searchAsyncAs[U](
    sqlQuery: SQLQuery
  )(implicit m: Manifest[U], ec: ExecutionContext, formats: Formats): Future[List[U]] = Future(
    this.searchAs[U](sqlQuery)
  )

  /** Search for entities matching the given JSON query with inner hits.
    * @param sqlQuery
    *   - the SQL query to search for
    * @param innerField
    *   - the field to use for inner hits
    * @param m1
    *   - the manifest of the type to search for
    * @param m2
    *   - the manifest of the inner hit type
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of tuples containing the main entity and a list of inner hits
    */
  def searchWithInnerHits[U, I](sqlQuery: SQLQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[(U, List[I])] = {
    searchWithInnerHits[U, I](implicitly[JSONQuery](sqlQuery), innerField)(m1, m2, formats)
  }

  /** Search for entities matching the given JSON query with inner hits.
    * @param jsonQuery
    *   - the JSON query to search for
    * @param innerField
    *   - the field to use for inner hits
    * @param m1
    *   - the manifest of the type to search for
    * @param m2
    *   - the manifest of the inner hit type
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of tuples containing the main entity and a list of inner hits
    */
  def searchWithInnerHits[U, I](jsonQuery: JSONQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[(U, List[I])]

  /** Perform a multi-search operation with the given SQL query.
    * @param sqlQuery
    *   - the SQL query to perform the multi-search for
    * @param m
    *   - the manifest of the type to search for
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of lists of entities matching the queries in the multi-search request
    */
  final def multisearchAs[U](
    sqlQuery: SQLQuery
  )(implicit m: Manifest[U], formats: Formats): List[U] = {
    convertTo[U](search(sqlQuery)).getOrElse(Seq.empty).toList
  }

  /** Perform a multi-search operation with the given JSON queries.
    * @param jsonQueries
    *   - the JSON queries to perform the multi-search for
    * @param fieldAliases
    *   - the field aliases to use for the search
    * @param aggregations
    *   - the aggregations to use for the search
    * @param m
    *   - the manifest of the type to search for
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of lists of entities matching the queries in the multi-search request
    */
  def multisearchAs[U](
    jsonQueries: JSONQueries,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation]
  )(implicit m: Manifest[U], formats: Formats): List[U] = {
    convertTo[U](
      multisearch(jsonQueries, fieldAliases, aggregations)
    )
      .getOrElse(Seq.empty)
      .toList
  }

  /** Perform a multi-search operation with the given SQL query and inner hits.
    * @param sqlQuery
    *   - the SQL query to perform the multi-search for
    * @param innerField
    *   - the field to use for inner hits
    * @param m1
    *   - the manifest of the type to search for
    * @param m2
    *   - the manifest of the inner hit type
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of lists of tuples containing the main entity and a list of inner hits
    */
  def multisearchWithInnerHits[U, I](sqlQuery: SQLQuery, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[List[(U, List[I])]] = {
    multisearchWithInnerHits[U, I](implicitly[JSONQueries](sqlQuery), innerField)(m1, m2, formats)
  }

  /** Perform a multi-search operation with the given JSON queries and inner hits.
    * @param jsonQueries
    *   - the JSON queries to perform the multi-search for
    * @param innerField
    *   - the field to use for inner hits
    * @param m1
    *   - the manifest of the type to search for
    * @param m2
    *   - the manifest of the inner hit type
    * @param formats
    *   - the formats to use for serialization/deserialization
    * @return
    *   a list of lists of tuples containing the main entity and a list of inner hits
    */
  def multisearchWithInnerHits[U, I](jsonQueries: JSONQueries, innerField: String)(implicit
    m1: Manifest[U],
    m2: Manifest[I],
    formats: Formats
  ): List[List[(U, List[I])]]
}

/** API for scrolling through search results using Akka Streams
  */
trait ScrollApi extends SearchApi { _: { def logger: Logger } =>

  /** Determine the best scroll strategy based on the query
    */
  private[this] def determineScrollStrategy(
    jsonQuery: JSONQuery,
    aggregations: Map[String, SQLAggregation]
  ): ScrollStrategy = {
    // If aggregations are present, use classic scrolling
    if (aggregations.nonEmpty) {
      UseScroll
    } else {
      // Check if the query contains aggregations in the JSON
      if (hasAggregations(jsonQuery.query)) {
        UseScroll
      } else {
        UseSearchAfter
      }
    }
  }

  /** Create a scrolling source with automatic strategy selection
    */
  final def scrollSource(
    sql: SQLQuery,
    config: ScrollConfig = ScrollConfig()
  )(implicit system: ActorSystem): Source[(Map[String, Any], ScrollMetrics), NotUsed] = {
    sql.request match {
      case Some(Left(single)) =>
        val sqlRequest = single.copy(score = sql.score)
        val jsonQuery = JSONQuery(sqlRequest, collection.immutable.Seq(sqlRequest.sources: _*))
        scrollSourceWithMetrics(
          jsonQuery,
          sqlRequest.fieldAliases,
          sqlRequest.sqlAggregations,
          config,
          single.sorts.nonEmpty
        )

      case Some(Right(_)) =>
        Source.failed(
          new UnsupportedOperationException("Scrolling is not supported for multi-search queries")
        )

      case None =>
        Source.failed(
          new IllegalArgumentException("SQL query does not contain a valid search request")
        )
    }
  }

  /** Scroll with metrics tracking
    */
  private[this] def scrollSourceWithMetrics(
    jsonQuery: JSONQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation],
    config: ScrollConfig,
    hasSorts: Boolean = false
  )(implicit system: ActorSystem): Source[(Map[String, Any], ScrollMetrics), NotUsed] = {

    var metrics = config.metrics // FIXME should be immutable

    scrollSource(jsonQuery, fieldAliases, aggregations, config, hasSorts)
      .take(config.maxDocuments.getOrElse(Long.MaxValue))
      .grouped(config.scrollSize)
      .map { batch =>
        metrics = metrics.copy(
          totalDocuments = metrics.totalDocuments + batch.size,
          totalBatches = metrics.totalBatches + 1
        )

        if (metrics.totalBatches % 10 == 0) {
          logger.info(
            s"Scroll progress: ${metrics.totalDocuments} docs, " +
            s"${metrics.totalBatches} batches, " +
            s"${metrics.documentsPerSecond} docs/sec"
          )
        }

        batch.map(doc => (doc, metrics))
      }
      .mapConcat(identity)
      .watchTermination() { (_, done) =>
        done.onComplete { _ =>
          val finalMetrics = metrics.complete
          logger.info(
            s"Scroll completed: ${finalMetrics.totalDocuments} docs in ${finalMetrics.duration}ms " +
            s"(${finalMetrics.documentsPerSecond} docs/sec)"
          )
        }(system.dispatcher)
        NotUsed
      }
  }

  private[this] def hasAggregations(query: String): Boolean = {
    try {
      val json = parse(query)
      (json \ "aggregations") != JNothing || (json \ "aggs") != JNothing
    } catch {
      case _: Exception => false
    }
  }

  /** Create a scrolling source for JSON query with automatic strategy
    */
  private[this] def scrollSource(
    jsonQuery: JSONQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation],
    config: ScrollConfig,
    hasSorts: Boolean
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed] = {
    val strategy = determineScrollStrategy(jsonQuery, aggregations)

    logger.info(
      s"Using scroll strategy: $strategy for query on ${jsonQuery.indices.mkString(", ")}"
    )

    strategy match {
      case UseScroll =>
        logger.info("Using classic scroll (supports aggregations)")
        scrollSourceClassic(jsonQuery, fieldAliases, aggregations, config)

      case UseSearchAfter if config.preferSearchAfter =>
        logger.info("Using search_after (optimized for hits only)")
        searchAfterSource(jsonQuery, fieldAliases, config, hasSorts)

      case _ =>
        logger.info("Falling back to classic scroll")
        scrollSourceClassic(jsonQuery, fieldAliases, aggregations, config)
    }
  }

  /** Classic scroll (works for both hits and aggregations)
    */
  def scrollSourceClassic(
    jsonQuery: JSONQuery,
    fieldAliases: Map[String, String],
    aggregations: Map[String, SQLAggregation],
    config: ScrollConfig
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed]

  /** Search After (only for hits, more efficient)
    */
  def searchAfterSource(
    jsonQuery: JSONQuery,
    fieldAliases: Map[String, String],
    config: ScrollConfig,
    hasSorts: Boolean = false
  )(implicit system: ActorSystem): Source[Map[String, Any], NotUsed]

  /** Typed scroll source
    */
  final def scrollSourceAs[T](
    sql: SQLQuery,
    config: ScrollConfig = ScrollConfig()
  )(implicit
    system: ActorSystem,
    m: Manifest[T],
    formats: Formats
  ): Source[T, NotUsed] = {
    scrollSource(sql, config).map(_._1).mapConcat { row =>
      Seq(convertTo[T](row)(m, formats)) // FIXME
    }
  }
}
