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

import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{Delete, From, SingleSearch}
import app.softnetwork.elastic.sql.schema.TableAlias
import app.softnetwork.elastic.sql.serialization._
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode

/** Index management API.
  *
  * This implementation provides:
  *   - Robust error handling with [[ElasticResult]]
  *   - Detailed logging for debugging
  *   - Parameter validation
  *   - Automatic retry for transient errors
  */
trait IndicesApi extends ElasticClientHelpers { _: RefreshApi with PipelineApi with VersionApi =>

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

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
    * @param mappings
    *   - optional mappings to apply to the index
    * @param aliases
    *   - optional aliases to apply to the index
    * @return
    *   true if the index was created successfully, false otherwise
    */
  def createIndex(
    index: String,
    settings: String = defaultSettings,
    mappings: Option[String] = None,
    aliases: Seq[TableAlias] = Nil
  ): ElasticResult[Boolean] = {
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("createIndex"),
            statusCode = Some(400),
            index = Some(index),
            message = s"Invalid index: ${error.message}"
          )
        )
      case None => // OK
    }

    validateJsonSettings(settings) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("createIndex"),
            statusCode = Some(400),
            message = s"Invalid settings: ${error.message}"
          )
        )
      case None => // OK
    }

    mappings.map(validateJsonMappings) match {
      case Some(Some(error)) =>
        return ElasticFailure(
          error.copy(
            operation = Some("createIndex"),
            statusCode = Some(400),
            message = s"Invalid mappings: ${error.message}"
          )
        )
      case _ => // OK
    }

    aliases.flatMap(alias => validateAliasName(alias.alias)) match {
      case error :: _ =>
        return ElasticFailure(
          error.copy(
            operation = Some("createIndex"),
            statusCode = Some(400),
            message = s"Invalid alias: ${error.message}"
          )
        )
      case Nil => // OK
    }

    logger.info(s"Creating index '$index' with settings: $settings")

    // Get Elasticsearch version
    val elasticVersion = {
      this.version match {
        case ElasticSuccess(v) => v
        case ElasticFailure(error) =>
          logger.error(s"❌ Failed to retrieve Elasticsearch version: ${error.message}")
          return ElasticFailure(error)
      }
    }

    val updatedMappings =
      if (ElasticsearchVersion.requiresDocTypeWrapper(elasticVersion)) {
        mappings match {
          case Some(m) =>
            val node: JsonNode = m
            if (node.has("properties")) {
              logger.info(s"Wrapping mappings with '_doc' type for ES version $elasticVersion")
              val doc = mapper.createObjectNode()
              val properties = node.get("properties")
              doc.set("properties", properties)
              val root: ObjectNode = node.asInstanceOf[ObjectNode]
              root.remove("properties")
              root.set[ObjectNode]("_doc", doc)
              Some(root.toString)
            } else {
              Some(m)
            }
          case None => None
        }
      } else {
        mappings
      }

    executeCreateIndex(index, settings, updatedMappings, aliases) match {
      case success @ ElasticSuccess(true) =>
        logger.info(s"✅ Index '$index' created successfully")
        success
      case success @ ElasticSuccess(_) =>
        logger.info(s"✅ Index '$index' not created")
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to create index '$index': ${error.message}")
        failure
    }
  }

  /** Get an index with the provided name.
    * @param index
    *   - the name of the index to get
    * @return
    *   the index if it exists, None otherwise
    */
  def getIndex(index: String): ElasticResult[Option[Index]] = {
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("getIndex"),
            statusCode = Some(400),
            index = Some(index),
            message = s"Invalid index: ${error.message}"
          )
        )
      case None => // OK
    }

    logger.info(s"Getting index '$index'")

    executeGetIndex(index) match {
      case ElasticSuccess(Some(json)) =>
        logger.info(s"✅ Index '$index' retrieved successfully")
        var tempIndex = Index(index, json)
        tempIndex.defaultIngestPipelineName match {
          case Some(pipeline) =>
            logger.info(
              s"Index '$index' has default ingest pipeline '$pipeline'"
            )
            getPipeline(pipeline) match {
              case ElasticSuccess(Some(json)) =>
                logger.info(
                  s"✅ Ingest pipeline '$pipeline' for index '$index' exists"
                )
                tempIndex = tempIndex.copy(defaultPipeline = Some(json))
              case ElasticSuccess(None) =>
                logger.warn(
                  s"⚠️ Ingest pipeline '$pipeline' for index '$index' does not exist"
                )
                return ElasticFailure(
                  ElasticError(
                    message =
                      s"Default ingest pipeline '$pipeline' for index '$index' does not exist",
                    cause = None,
                    statusCode = Some(404),
                    index = Some(index),
                    operation = Some("getIndex")
                  )
                )
              case ElasticFailure(error) =>
                logger.error(
                  s"❌ Failed to get ingest pipeline '$pipeline' for index '$index': ${error.message}"
                )
                return ElasticFailure(error.copy(operation = Some("getIndex")))
            }
          case None => // No default ingest pipeline
        }
        tempIndex.finalIngestPipelineName match {
          case Some(pipeline) =>
            logger.info(
              s"Index '$index' has final ingest pipeline '$pipeline'"
            )
            getPipeline(pipeline) match {
              case ElasticSuccess(Some(json)) =>
                logger.info(
                  s"✅ Ingest pipeline '$pipeline' for index '$index' exists"
                )
                tempIndex = tempIndex.copy(finalPipeline = Some(json))
              case ElasticSuccess(None) =>
                logger.warn(
                  s"⚠️ Ingest pipeline '$pipeline' for index '$index' does not exist"
                )
                return ElasticFailure(
                  ElasticError(
                    message =
                      s"Final ingest pipeline '$pipeline' for index '$index' does not exist",
                    cause = None,
                    statusCode = Some(404),
                    index = Some(index),
                    operation = Some("getIndex")
                  )
                )
              case ElasticFailure(error) =>
                logger.error(
                  s"❌ Failed to get ingest pipeline '$pipeline' for index '$index': ${error.message}"
                )
                return ElasticFailure(error.copy(operation = Some("getIndex")))
            }
          case None => // No default ingest pipeline
        }
        ElasticSuccess(Some(tempIndex))
      case ElasticSuccess(None) =>
        logger.info(s"✅ Index '$index' not found")
        ElasticSuccess(None)
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to get index '$index': ${error.message}")
        failure
    }
  }

  /** Delete an index with the provided name.
    * @param index
    *   - the name of the index to delete
    * @return
    *   true if the index was deleted successfully, false otherwise
    */
  def deleteIndex(index: String): ElasticResult[Boolean] = {
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("deleteIndex"),
            statusCode = Some(400),
            index = Some(index),
            message = s"Invalid index: ${error.message}"
          )
        )
      case None => // OK
    }

    logger.info(s"Deleting index '$index'")

    executeDeleteIndex(index) match {
      case success @ ElasticSuccess(true) =>
        logger.info(s"✅ Index '$index' deleted successfully")
        success
      case success @ ElasticSuccess(_) =>
        logger.info(s"✅ Index '$index' not deleted")
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to delete index '$index': ${error.message}")
        failure
    }
  }

  /** Close an index with the provided name.
    * @param index
    *   - the name of the index to close
    * @return
    *   true if the index was closed successfully, false otherwise
    */
  def closeIndex(index: String): ElasticResult[Boolean] = {
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("closeIndex"),
            statusCode = Some(400),
            index = Some(index),
            message = s"Invalid index: ${error.message}"
          )
        )
      case None => // OK
    }

    logger.info(s"Closing index '$index'")

    executeCloseIndex(index) match {
      case success @ ElasticSuccess(true) =>
        logger.info(s"✅ Index '$index' closed successfully")
        success
      case success @ ElasticSuccess(_) =>
        logger.info(s"✅ Index '$index' not closed")
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to close index '$index': ${error.message}")
        failure
    }
  }

  /** Open an index with the provided name.
    * @param index
    *   - the name of the index to open
    * @return
    *   true if the index was opened successfully, false otherwise
    */
  def openIndex(index: String): ElasticResult[Boolean] = {
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("openIndex"),
            statusCode = Some(400),
            index = Some(index),
            message = s"Invalid index: ${error.message}"
          )
        )
      case None => // OK
    }

    logger.info(s"Opening index '$index'")

    executeOpenIndex(index) match {
      case success @ ElasticSuccess(true) =>
        logger.info(s"✅ Index '$index' opened successfully")
        success
      case success @ ElasticSuccess(_) =>
        logger.info(s"✅ Index '$index' not opened")
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to open index '$index': ${error.message}")
        failure
    }
  }

  /** Reindex from source index to target index.
    * @param sourceIndex
    *   - the name of the source index
    * @param targetIndex
    *   - the name of the target index
    * @param refresh
    *   - true to refresh the target index after reindexing, false otherwise
    * @return
    *   true and the number of documents re indexed if the reindexing was successful, false
    *   otherwise
    */
  def reindex(
    sourceIndex: String,
    targetIndex: String,
    refresh: Boolean = true,
    pipeline: Option[String] = None
  ): ElasticResult[(Boolean, Option[Long])] = {
    // Validation...
    validateIndexName(sourceIndex) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("reindex"),
            statusCode = Some(400),
            index = Some(sourceIndex),
            message = s"Invalid source index: ${error.message}"
          )
        )
      case None => // OK
    }

    validateIndexName(targetIndex) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("reindex"),
            statusCode = Some(400),
            index = Some(targetIndex),
            message = s"Invalid target index: ${error.message}"
          )
        )
      case None => // OK
    }

    if (sourceIndex == targetIndex) {
      return ElasticFailure(
        ElasticError(
          message = "Source and target index cannot be the same",
          cause = None,
          statusCode = Some(400),
          index = Some(sourceIndex),
          operation = Some("reindex")
        )
      )
    }

    logger.info(s"Reindexing from '$sourceIndex' to '$targetIndex' (refresh=$refresh)")

    // Existence checks...
    indexExists(sourceIndex, pattern = false) match {
      case ElasticSuccess(false) =>
        return ElasticFailure(
          ElasticError(
            message = s"Source index '$sourceIndex' does not exist",
            cause = None,
            statusCode = Some(404),
            index = Some(sourceIndex),
            operation = Some("reindex")
          )
        )
      case ElasticFailure(error) => return ElasticFailure(error)
      case _                     => // OK
    }

    indexExists(targetIndex, pattern = false) match {
      case ElasticSuccess(false) =>
        return ElasticFailure(
          ElasticError(
            message = s"Target index '$targetIndex' does not exist",
            cause = None,
            statusCode = Some(404),
            index = Some(targetIndex),
            operation = Some("reindex")
          )
        )
      case ElasticFailure(error) => return ElasticFailure(error)
      case _                     => // OK
    }

    // ✅ Performing the reindex with extracting the number of documents
    executeReindex(sourceIndex, targetIndex, refresh, pipeline) match {
      case ElasticFailure(error) =>
        logger.error(s"Reindex failed for index '$targetIndex': ${error.message}")
        ElasticFailure(error)

      case ElasticSuccess((true, docsCount)) =>
        val countStr = docsCount.map(c => s" ($c documents)").getOrElse("")
        logger.info(s"✅ Reindex from '$sourceIndex' to '$targetIndex' succeeded$countStr")

        if (refresh) {
          this.refresh(targetIndex) match {
            case ElasticSuccess(_) =>
              logger.debug(s"✅ Target index '$targetIndex' refreshed")
              ElasticSuccess((true, docsCount))
            case ElasticFailure(error) =>
              logger.warn(
                s"✅ Refresh failed but reindex succeeded for index '$targetIndex': ${error.message}"
              )
              ElasticSuccess((true, docsCount))
          }
        } else {
          ElasticSuccess((true, docsCount))
        }

      case ElasticSuccess((false, _)) =>
        ElasticFailure(
          ElasticError(
            message = s"Reindex failed for index '$targetIndex'",
            cause = None,
            statusCode = None,
            index = Some(targetIndex),
            operation = Some("reindex")
          )
        )
    }
  }

  /** Check if an index exists.
    * @param index
    *   - the name of the index to check
    * @return
    *   true if the index exists, false otherwise
    */
  def indexExists(index: String, pattern: Boolean): ElasticResult[Boolean] = {
    validateIndexName(index, pattern) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("indexExists"),
            statusCode = Some(400),
            index = Some(index),
            message = s"Invalid index: ${error.message}"
          )
        )
      case None => // OK
    }

    logger.debug(s"Checking if index '$index' exists")

    executeIndexExists(index) match {
      case success @ ElasticSuccess(exists) =>
        val existenceStr = if (exists) "exists" else "does not exist"
        logger.debug(s"✅ Index '$index' $existenceStr")
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to check existence of index '$index': ${error.message}")
        failure
    }
  }

  def isIndexClosed(index: String): ElasticResult[Boolean] = {
    val result = for {
      // 1. Validate index name
      _ <- validateIndexName(index)
        .toLeft(())
        .left
        .map(err =>
          err.copy(
            operation = Some("isIndexClosed"),
            statusCode = Some(400),
            index = Some(index),
            message = s"Invalid index: ${err.message}"
          )
        )
      // 2. Check index exists
      _ <- indexExists(index, pattern = false) match {
        case ElasticSuccess(true) => Right(())
        case ElasticSuccess(false) =>
          Left(
            ElasticError(
              message = s"Index '$index' does not exist",
              statusCode = Some(404),
              index = Some(index),
              operation = Some("isIndexClosed")
            )
          )
        case ElasticFailure(err) => Left(err)
      }
      // 3. Retrieve index status
      closed <- executeIsIndexClosed(index).toEither

    } yield closed

    result match {
      case Right(closed) =>
        val statusStr = if (closed) "closed" else "open"
        logger.info(s"✅ Index '$index' is $statusStr")
        ElasticSuccess(closed)
      case Left(err) =>
        logger.error(s"❌ Failed to check if index '$index' is closed: ${err.message}")
        return ElasticFailure(err)
    }
  }

  /** Truncate an index by deleting all its documents.
    * @param index
    *   - the name of the index to truncate
    * @return
    *   the number of documents deleted
    */
  def truncateIndex(index: String): ElasticResult[Long] =
    deleteByQuery(index, """{"query": {"match_all": {}}}""")

  /** Delete documents by query from an index.
    * @param index
    *   - the name of the index to delete from
    * @param query
    *   - the query to delete documents by (can be JSON or SQL)
    * @param refresh
    *   - true to refresh the index after deletion, false otherwise
    * @return
    *   the number of documents deleted
    */
  def deleteByQuery(
    index: String,
    query: String,
    refresh: Boolean = true
  ): ElasticResult[Long] = {

    val result = for {
      // 1. Validate index name
      _ <- validateIndexName(index)
        .toLeft(())
        .left
        .map(err =>
          err.copy(
            operation = Some("deleteByQuery"),
            statusCode = Some(400),
            index = Some(index),
            message = s"Invalid index: ${err.message}"
          )
        )

      // 2. Parse query (SQL or JSON)
      jsonQuery <- parseQueryForDeletion(index, query)

      // 3. Check index exists
      _ <- indexExists(index, pattern = false) match {
        case ElasticSuccess(true) => Right(())
        case ElasticSuccess(false) =>
          Left(
            ElasticError(
              message = s"Index '$index' does not exist",
              statusCode = Some(404),
              index = Some(index),
              operation = Some("deleteByQuery")
            )
          )
        case ElasticFailure(err) => Left(err)
      }

      // 4. Open index if needed
      tuple <- openIfNeeded(index)
      (_, restore) = tuple

      // 5. Execute delete-by-query
      deleted <- executeDeleteByQuery(index, jsonQuery, refresh).toEither

      // 6. Restore state
      _ <- restore().toEither.left.map { restoreErr =>
        logger.warn(s"❌ Failed to restore index state for '$index': ${restoreErr.message}")
        restoreErr
      }

    } yield deleted

    result match {
      case Right(count) =>
        logger.info(s"✅ Deleted $count documents from index '$index'")
        ElasticSuccess(count)
      case Left(err) =>
        logger.error(s"❌ Failed to delete by query on index '$index': ${err.message}")
        ElasticFailure(err)
    }
  }

  /** Parse a query for deletion, determining if it's SQL or JSON.
    *
    * @param index
    *   - the name of the index to delete from
    * @param query
    *   - the query (SQL or JSON)
    * @return
    *   the validated JSON query
    */
  private def parseQueryForDeletion(index: String, query: String): Either[ElasticError, String] = {
    val trimmed = query.trim.toUpperCase
    val isSql = trimmed.startsWith("SELECT") ||
      trimmed.startsWith("DELETE") ||
      trimmed.startsWith("WITH")
    if (isSql) parseSqlQueryForDeletion(index, query)
    else parseJsonQueryForDeletion(index, query)
  }

  /** Validate a JSON query for deletion.
    *
    * @param index
    *   - the name of the index to delete from
    * @param query
    *   - the JSON query
    * @return
    *   the validated JSON query
    */
  private def parseJsonQueryForDeletion(
    index: String,
    query: String
  ): Either[ElasticError, String] = {
    validateJson("deleteByQuery", query) match {
      case None =>
        logger.info(s"Processing JSON query for deleteByQuery on index '$index': $query")
        Right(query)

      case Some(err) =>
        Left(
          err.copy(
            operation = Some("deleteByQuery"),
            statusCode = Some(400),
            index = Some(index),
            message = s"Invalid JSON query: ${err.message}"
          )
        )
    }
  }

  /** Parse an SQL query for deletion and convert it to Elasticsearch JSON.
    *
    * @param index
    *   - the name of the index to delete from
    * @param query
    *   - the SQL query
    * @return
    *   the Elasticsearch JSON query
    */
  private def parseSqlQueryForDeletion(
    index: String,
    query: String
  ): Either[ElasticError, String] = {
    logger.info(s"Processing SQL query for deleteByQuery on index '$index': $query")

    Parser(query) match {
      case Left(err) =>
        Left(
          ElasticError(
            operation = Some("deleteByQuery"),
            statusCode = Some(400),
            index = Some(index),
            message = s"Invalid SQL query: ${err.msg}"
          )
        )

      case Right(statement) =>
        statement match {

          case deleteStmt: Delete =>
            if (deleteStmt.table.name != index)
              Left(
                sqlErrorForDeletion(
                  index = index,
                  message =
                    s"SQL query index '${deleteStmt.table.name}' does not match provided index '$index'"
                )
              )
            else
              deleteStmt.where match {
                case None =>
                  logger.info(
                    s"SQL delete query has no WHERE clause, deleting all documents from index '$index'"
                  )
                  Right("""{"query": {"match_all": {}}}""")

                case Some(where) =>
                  implicit val timestamp: Long = System.currentTimeMillis()
                  val search: String =
                    SingleSearch(
                      from = From(tables = Seq(deleteStmt.table)),
                      where = Some(where),
                      deleteByQuery = true
                    )
                  logger.info(s"✅ Converted SQL delete query to search for deleteByQuery: $search")
                  Right(search)
              }

          case search: SingleSearch =>
            val tables = search.from.tables
            if (tables.size != 1 || tables.head.name != index)
              Left(
                sqlErrorForDeletion(
                  index = index,
                  message =
                    s"SQL query index '${tables.map(_.name).mkString(",")}' does not match provided index '$index'"
                )
              )
            else {
              implicit val timestamp: Long = System.currentTimeMillis()
              val query: String = search.copy(deleteByQuery = true)
              logger.info(s"✅ Converted SQL search query to search for deleteByQuery: $query")
              Right(query)
            }

          case _ =>
            Left(
              sqlErrorForDeletion(
                index = index,
                message = s"Invalid SQL query for deleteByQuery"
              )
            )
        }
    }
  }

  private def openIfNeeded(
    index: String
  ): Either[ElasticError, (Boolean, () => ElasticResult[Boolean])] = {
    for {
      // Detect initial state
      isClosed <- isIndexClosed(index).toEither

      // Open only if needed
      _ <- if (isClosed) openIndex(index).toEither else Right(())

    } yield {
      val restore = () =>
        if (isClosed) closeIndex(index)
        else ElasticSuccess(true)

      (isClosed, restore)
    }
  }

  private def sqlErrorForDeletion(index: String, message: String): ElasticError =
    ElasticError(
      operation = Some("deleteByQuery"),
      statusCode = Some(400),
      index = Some(index),
      message = message
    )

  // ================================================================================
  // IMPLICIT CONVERSIONS
  // ================================================================================

  /** Implicit conversion of an SQL query to Elasticsearch JSON. Used for query serialization.
    *
    * @param sqlSearch
    *   the SQL search request to convert
    * @return
    *   JSON string representation of the query
    */
  private[client] implicit def sqlSearchRequestToJsonQuery(sqlSearch: SingleSearch)(implicit
    timestamp: Long
  ): String

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeCreateIndex(
    index: String,
    settings: String,
    mappings: Option[String],
    aliases: Seq[TableAlias]
  ): ElasticResult[Boolean]

  private[client] def executeGetIndex(index: String): ElasticResult[Option[String]]

  private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean]

  private[client] def executeCloseIndex(index: String): ElasticResult[Boolean]

  private[client] def executeOpenIndex(index: String): ElasticResult[Boolean]

  private[client] def executeReindex(
    sourceIndex: String,
    targetIndex: String,
    refresh: Boolean,
    pipeline: Option[String]
  ): ElasticResult[(Boolean, Option[Long])]

  private[client] def executeIndexExists(index: String): ElasticResult[Boolean]

  private[client] def executeDeleteByQuery(
    index: String,
    query: String,
    refresh: Boolean
  ): ElasticResult[Long]

  private[client] def executeIsIndexClosed(index: String): ElasticResult[Boolean]
}
