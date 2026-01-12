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
import app.softnetwork.elastic.sql.schema.TableAlias
import app.softnetwork.elastic.sql.serialization._
import com.fasterxml.jackson.databind.JsonNode

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
    *   the alias to add
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
  @deprecated("Use addAlias(TableAlias) instead", "0.15.0")
  def addAlias(index: String, alias: String): ElasticResult[Boolean] = {
    addAlias(TableAlias(index, alias))
  }

  //format:off
  /** Add an alias to an index.
    *
    * This operation:
    *   1. Validates the index and alias names 2. Checks that the index exists 3. Adds the alias
    *
    * @param alias
    *   the TableAlias to add
    * @return
    *   ElasticSuccess(true) if added, ElasticFailure otherwise
    *
    * @example
    * {{{
    * val alias = TableAlias(table = "my-index-2024", alias = "my-index-current")
    * addAlias(alias) match {
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
  def addAlias(alias: TableAlias): ElasticResult[Boolean] = {
    val index = alias.table
    val aliasName = alias.alias
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

    validateAliasName(aliasName) match {
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

    if (index == aliasName) {
      return ElasticFailure(
        ElasticError(
          message = s"Index and alias cannot have the same name: '$index'",
          cause = None,
          statusCode = Some(400),
          operation = Some("addAlias")
        )
      )
    }

    indexExists(index, pattern = false) match {
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

    logger.debug(s"Adding alias '$aliasName' to index '$index'")

    executeAddAlias(alias) match {
      case success @ ElasticSuccess(_) =>
        logger.info(s"✅ Alias '$aliasName' successfully added to index '$index'")
        success

      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to add alias '$aliasName' to index '$index': ${error.message}")
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
        if (error.statusCode.getOrElse(0) == 404) {
          logger.info(s"✅ Alias '$alias' does not exist")
          return ElasticResult.success(false)
        }
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
    *   case ElasticSuccess(aliases) => println(s"Aliases: ${aliases.map(_.alias).mkString(", ")}")
    *   case ElasticFailure(error)   => println(s"Error: ${error.message}")
    * }
    *
    * }}}
    */
  //format:on
  def getAliases(index: String): ElasticResult[Seq[TableAlias]] = {

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
          mapper.readTree(jsonString)
        }
      ) match {
        case ElasticFailure(error) =>
          logger.error(s"❌ Failed to parse aliases JSON for index '$index': ${error.message}")
          return ElasticFailure(error)
        case ElasticSuccess(rootObj) =>
          if (!rootObj.has(index)) {
            logger.warn(s"Index '$index' not found in response")
            return ElasticResult.success(Seq.empty[TableAlias])
          }

          val root = rootObj.get(index)

          if (!root.has("aliases")) {
            logger.debug(s"No aliases found for index '$index'")
            return ElasticResult.success(Seq.empty[TableAlias])
          }

          val aliasesNode = root.path("aliases")
          val aliases: Map[String, JsonNode] =
            if (aliasesNode != null && aliasesNode.isObject) {
              aliasesNode
                .properties()
                .asScala
                .map { entry =>
                  val aliasName = entry.getKey
                  val aliasValue = entry.getValue
                  aliasName -> aliasValue
                }
                .toMap
            } else {
              Map.empty
            }

          ElasticSuccess(aliases.toSeq.map(alias => TableAlias(index, alias._1, alias._2)))
      }
    } match {
      case success @ ElasticSuccess(aliases) =>
        if (aliases.nonEmpty)
          logger.info(
            s"✅ Found ${aliases.size} alias(es) for index '$index': ${aliases.map(_.alias).sorted.mkString(", ")}"
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

  //format:off
  /** Set the exact set of aliases for an index.
    *
    * This method ensures that the specified index has exactly the provided set of aliases. It adds
    * any missing aliases and removes any extra aliases that are not in the provided set.
    *
    * @param index
    *   the name of the index
    * @param aliases
    *   the desired set of aliases for the index
    * @return
    *   ElasticSuccess(true) if the operation was successful, ElasticFailure otherwise
    *
    * @example
    * {{{
    * setAliases("my-index", Seq(TableAlias("my-index", "alias1"), TableAlias("my-index", "alias2"))) match {
    *   case ElasticSuccess(_)     => println("Aliases set successfully")
    *   case ElasticFailure(error) => println(s"Error: ${error.message}")
    * }
    * }}}
    */
  //format:on
  def setAliases(index: String, aliases: Seq[TableAlias]): ElasticResult[Boolean] = {
    getAliases(index).flatMap { existingAliases =>
      val notIndexAliases = aliases.filter(_.table != index)
      if (notIndexAliases.nonEmpty) {
        return ElasticFailure(
          ElasticError(
            message = s"All aliases must belong to the index '$index': ${notIndexAliases
              .map(_.alias)
              .mkString(", ")}",
            cause = None,
            statusCode = Some(400),
            operation = Some("setAliases")
          )
        )
      }

      val existingAliasNames = existingAliases.map(_.alias).toSet
      val aliasesMap = aliases.map(alias => alias.alias -> alias).toMap

      val toSet = aliasesMap.keys.toSet.diff(existingAliasNames) ++ aliasesMap.keys.toSet.intersect(
        existingAliasNames
      )
      val toRemove = existingAliasNames.diff(aliasesMap.keys.toSet)

      val setResults = toSet.map(alias => addAlias(aliasesMap(alias)))
      val removeResults = toRemove.map(alias => removeAlias(index, alias))

      val allResults = setResults ++ removeResults

      val failures = allResults.collect { case ElasticFailure(error) => error }

      if (failures.nonEmpty) {
        ElasticFailure(
          ElasticError(
            message =
              s"Failed to set aliases for index '$index': ${failures.map(_.message).mkString(", ")}",
            cause = None,
            statusCode = Some(500),
            operation = Some("setAliases")
          )
        )
      } else {
        ElasticResult.success(true)
      }
    }
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeAddAlias(alias: TableAlias): ElasticResult[Boolean]

  private[client] def executeRemoveAlias(index: String, alias: String): ElasticResult[Boolean]

  private[client] def executeAliasExists(alias: String): ElasticResult[Boolean]

  private[client] def executeGetAliases(index: String): ElasticResult[String]

  private[client] def executeSwapAlias(
    oldIndex: String,
    newIndex: String,
    alias: String
  ): ElasticResult[Boolean]

}
