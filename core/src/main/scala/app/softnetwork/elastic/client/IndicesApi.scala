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

/** Index management API.
  *
  * This implementation provides:
  *   - Robust error handling with [[ElasticResult]]
  *   - Detailed logging for debugging
  *   - Parameter validation
  *   - Automatic retry for transient errors
  */
trait IndicesApi extends ElasticClientHelpers { _: RefreshApi =>

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
    aliases: Seq[String] = Nil
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

    aliases.flatMap(alias => validateAliasName(alias)) match {
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

    executeCreateIndex(index, settings, mappings, aliases) match {
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
    indexExists(sourceIndex) match {
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

    indexExists(targetIndex) match {
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
  def indexExists(index: String): ElasticResult[Boolean] = {
    validateIndexName(index) match {
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

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeCreateIndex(
    index: String,
    settings: String,
    mappings: Option[String],
    aliases: Seq[String]
  ): ElasticResult[Boolean]

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
}
