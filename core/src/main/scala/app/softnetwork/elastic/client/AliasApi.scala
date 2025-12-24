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

import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess
}
import com.google.gson.JsonParser

import scala.jdk.CollectionConverters._
import scala.util.Try

/** Alias management API.
  *
  * This implementation provides:
  *   - Adding/removing aliases
  *   - Existence checking
  *   - Retrieving aliases from an index
  *   - Atomic operations (swap)
  *   - Full parameter validation
  *
  * ==Elasticsearch rules for aliases==
  *
  * Aliases follow the same naming rules as indexes:
  *   - Lowercase only
  *   - No special characters: \, /, *, ?, ", <, >, |, space, comma, #
  *   - Does not start with -, _, +
  *   - Is not . or ..
  *   - Maximum 255 characters
  */
trait AliasApi extends ElasticClientHelpers { _: IndicesApi =>

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  //format:off
  /** Add an alias to an index.
    *
    * This operation:
    *   1. Validates the index and alias names 2. Checks that the index exists 3. Adds the alias
    *
    * @param index
    *   the index name
    * @param alias
    *   the alias name to add
    * @return
    *   ElasticSuccess(true) if added, ElasticFailure otherwise
    *
    * @example
    * {{{
    * addAlias("my-index-2024", "my-index-current") match {
    *   case ElasticSuccess(_)     => println("Alias added")
    *   case ElasticFailure(error) => println(s"Error: ${error.message}")
    * }
    * }}}
    *
    * @note
    *   An alias can point to multiple indexes (useful for searches)
    * @note
    *   An index can have multiple aliases
    */
  //format:on
  def addAlias(index: String, alias: String): ElasticResult[Boolean] = {
    // Validation...
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            message = s"Invalid index: ${error.message}",
            statusCode = Some(400),
            operation = Some("addAlias")
          )
        )
      case None => // OK
    }

    validateAliasName(alias) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            message = s"Invalid alias: ${error.message}",
            statusCode = Some(400),
            operation = Some("addAlias")
          )
        )
      case None => // OK
    }

    if (index == alias) {
      return ElasticFailure(
        ElasticError(
          message = s"Index and alias cannot have the same name: '$index'",
          cause = None,
          statusCode = Some(400),
          operation = Some("addAlias")
        )
      )
    }

    indexExists(index, false) match {
      case ElasticSuccess(false) =>
        return ElasticFailure(
          ElasticError(
            message = s"Index '$index' does not exist",
            cause = None,
            statusCode = Some(404),
            operation = Some("addAlias")
          )
        )
      case ElasticFailure(error) => return ElasticFailure(error)
      case _                     => // OK
    }

    logger.debug(s"Adding alias '$alias' to index '$index'")

    executeAddAlias(index, alias) match {
      case success @ ElasticSuccess(_) =>
        logger.info(s"✅ Alias '$alias' successfully added to index '$index'")
        success

      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to add alias '$alias' to index '$index': ${error.message}")
        failure
    }
  }

  //format:off
  /** Remove an alias from an index.
    *
    * @param index
    *   the name of the index
    * @param alias
    *   the name of the alias to remove
    * @return
    *   ElasticSuccess(true) if removed, ElasticFailure otherwise
    *
    * @example
    * {{{
    * removeAlias("my-index-2024", "my-index-current") match {
    *   case ElasticSuccess(_)     => println("Alias removed")
    *   case ElasticFailure(error) => println(s"Error: ${error.message}")
    * }
    * }}}
    *
    * @note
    *   If the alias does not exist, Elasticsearch returns a 404 error
    */
  //format:on
  def removeAlias(index: String, alias: String): ElasticResult[Boolean] = {
    // Validation...
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("removeAlias"),
            statusCode = Some(400),
            message = s"Invalid index: ${error.message}"
          )
        )
      case None => // OK
    }

    validateAliasName(alias) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("removeAlias"),
            statusCode = Some(400),
            message = s"Invalid alias: ${error.message}"
          )
        )
      case None => // OK
    }

    logger.debug(s"Removing alias '$alias' from index '$index'")

    executeRemoveAlias(index, alias) match {
      case success @ ElasticSuccess(_) =>
        logger.info(s"✅ Alias '$alias' successfully removed from index '$index'")
        success

      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to remove alias '$alias' from index '$index': ${error.message}")
        failure
    }

  }

  //format:off
  /** Check if an alias exists.
    *
    * @param alias
    *   the name of the alias to check
    * @return
    *   ElasticSuccess(true) if it exists, ElasticSuccess(false) otherwise, ElasticFailure in case
    *   of error
    *
    * @example
    * {{{
    * aliasExists("my-alias") match {
    *   case ElasticSuccess(true)  => println("Alias exists")
    *   case ElasticSuccess(false) => println("Alias does not exist")
    *   case ElasticFailure(error) => println(s"Error: ${error.message}")
    * }
    * }}}
    */
  //format:on
  def aliasExists(alias: String): ElasticResult[Boolean] = {

    validateAliasName(alias) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            message = s"Invalid alias name: ${error.message}",
            statusCode = Some(400),
            operation = Some("aliasExists")
          )
        )
      case None => // OK
    }

    logger.debug(s"Checking if alias '$alias' exists")

    executeAliasExists(alias) match {
      case success @ ElasticSuccess(exists) =>
        if (exists) {
          logger.info(s"✅ Alias '$alias' exists")
        } else {
          logger.info(s"✅ Alias '$alias' does not exist")
        }
        success

      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to check existence of alias '$alias': ${error.message}")
        failure
    }
  }

  //format:off
  /** Retrieve all aliases from an index.
    *
    * @param index
    *   the index name
    * @return
    *   ElasticResult with the list of aliases
    *
    * @example
    * {{{
    * getAliases("my-index") match {
    *   case ElasticSuccess(aliases) => println(s"Aliases: ${aliases.mkString(", ")}")
    *   case ElasticFailure(error)   => println(s"Error: ${error.message}")
    * }
    *
    * }}}
    */
  //format:on
  def getAliases(index: String): ElasticResult[Set[String]] = {

    validateIndexName(index) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            message = s"Invalid index name: ${error.message}",
            statusCode = Some(400),
            operation = Some("getAliases")
          )
        )
      case None => // OK
    }

    logger.debug(s"Getting aliases for index '$index'")

    executeGetAliases(index).flatMap { jsonString =>
      // ✅ Extracting aliases from JSON
      ElasticResult.fromTry(
        Try {
          JsonParser.parseString(jsonString).getAsJsonObject
        }
      ) match {
        case ElasticFailure(error) =>
          logger.error(s"❌ Failed to parse aliases JSON for index '$index': ${error.message}")
          return ElasticFailure(error)
        case ElasticSuccess(rootObj) =>
          if (!rootObj.has(index)) {
            logger.warn(s"Index '$index' not found in response")
            return ElasticResult.success(Set.empty[String])
          }

          val indexObj = rootObj.getAsJsonObject(index)
          if (indexObj == null) {
            logger.warn(s"Index '$index' is null in response")
            return ElasticResult.success(Set.empty[String])
          }

          val aliasesObj = indexObj.getAsJsonObject("aliases")
          if (aliasesObj == null || aliasesObj.size() == 0) {
            logger.debug(s"No aliases found for index '$index'")
            ElasticResult.success(Set.empty[String])
          } else {
            val aliases = aliasesObj.entrySet().asScala.map(_.getKey).toSet
            logger.debug(
              s"Found ${aliases.size} alias(es) for index '$index': ${aliases.mkString(", ")}"
            )
            ElasticResult.success(aliases)
          }
      }
    } match {
      case success @ ElasticSuccess(aliases) =>
        if (aliases.nonEmpty)
          logger.info(
            s"✅ Found ${aliases.size} alias(es) for index '$index': ${aliases.mkString(", ")}"
          )
        else
          logger.info(s"✅ No aliases found for index '$index'")
        success

      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to get aliases for index '$index': ${error.message}")
        failure
    }
  }

  //format:off
  /** Atomic swap of an alias between two indexes.
    *
    * This operation is atomic: the alias is removed from oldIndex and added to newIndex in a single
    * query, thus avoiding any period when the alias does not exist. This is the recommended
    * operation for zero-downtime deployments.
    *
    * @param oldIndex
    *   the current index pointed to by the alias
    * @param newIndex
    *   the new index that should point to the alias
    * @param alias
    *   the name of the alias to swap
    * @return
    *   ElasticSuccess(true) if swapped, ElasticFailure otherwise
    *
    * @example
    * {{{
    * // Zero-downtime deployment
    * swapAlias(oldIndex = "products-v1", newIndex = "products-v2", alias = "products") match {
    *   case ElasticSuccess(_)     => println("✅ Alias swapped, new version deployed")
    *   case ElasticFailure(error) => println(s"❌ Error: ${error.message}")
    * }
    * }}}
    *
    * @note
    *   This operation is atomic and therefore preferable to removeAlias + addAlias
    */
  //format:on
  def swapAlias(
    oldIndex: String,
    newIndex: String,
    alias: String
  ): ElasticResult[Boolean] = {

    // ✅ Validation...
    validateIndexName(oldIndex) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("swapAlias"),
            statusCode = Some(400),
            message = s"Invalid old index name: ${error.message}"
          )
        )
      case None => // OK
    }

    validateIndexName(newIndex) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("swapAlias"),
            statusCode = Some(400),
            message = s"Invalid new index name: ${error.message}"
          )
        )
      case None => // OK
    }

    validateAliasName(alias) match {
      case Some(error) =>
        return ElasticFailure(
          error.copy(
            operation = Some("swapAlias"),
            statusCode = Some(400),
            message = s"Invalid alias name: ${error.message}"
          )
        )
      case None => // OK
    }

    if (oldIndex == newIndex) {
      return ElasticFailure(
        ElasticError(
          message = s"Old and new index cannot be the same: '$oldIndex'",
          cause = None,
          statusCode = Some(400),
          operation = Some("swapAlias")
        )
      )
    }

    logger.info(s"Swapping alias '$alias' from '$oldIndex' to '$newIndex' (atomic operation)")

    // ✅ Atomic operation : remove + add in a single request
    executeSwapAlias(oldIndex, newIndex, alias) match {
      case success @ ElasticSuccess(_) =>
        logger.info(s"✅ Alias '$alias' successfully swapped from '$oldIndex' to '$newIndex'")
        success

      case failure @ ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to swap alias '$alias' from '$oldIndex' to '$newIndex': ${error.message}"
        )
        failure
    }
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeAddAlias(index: String, alias: String): ElasticResult[Boolean]

  private[client] def executeRemoveAlias(index: String, alias: String): ElasticResult[Boolean]

  private[client] def executeAliasExists(alias: String): ElasticResult[Boolean]

  private[client] def executeGetAliases(index: String): ElasticResult[String]

  private[client] def executeSwapAlias(
    oldIndex: String,
    newIndex: String,
    alias: String
  ): ElasticResult[Boolean]

}
