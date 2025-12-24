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

import java.util.UUID

/** Mapping management API.
  */
trait MappingApi extends ElasticClientHelpers { _: SettingsApi with IndicesApi with RefreshApi =>

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  /** Set the mapping of an index.
    * @param index
    *   - the name of the index to set the mapping for
    * @param mapping
    *   - the mapping to set on the index
    * @return
    *   true if the mapping was set successfully, false otherwise
    */
  def setMapping(index: String, mapping: String): ElasticResult[Boolean] = {
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticResult.failure(
          error.copy(
            message = s"Invalid index: ${error.message}",
            statusCode = Some(400),
            index = Some(index),
            operation = Some("setMapping")
          )
        )
      case None => // continue
    }

    validateJson("mapping", mapping) match {
      case Some(error) =>
        return ElasticResult.failure(
          error.copy(
            message = s"Invalid mapping: ${error.message}",
            statusCode = Some(400),
            index = Some(index),
            operation = Some("setMapping")
          )
        )
      case None => // continue
    }

    logger.debug(s"Setting mapping for index '$index': $mapping")

    executeSetMapping(index, mapping) match {
      case success @ ElasticSuccess(true) =>
        logger.info(s"✅ Mapping for index '$index' updated successfully")
        success
      case success @ ElasticSuccess(_) =>
        logger.info(s"✅ Mapping for index '$index' not updated")
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to update mapping for index '$index': ${error.message}")
        failure
    }
  }

  /** Get the mapping of an index.
    * @param index
    *   - the name of the index to get the mapping for
    * @return
    *   the mapping of the index as a JSON string
    */
  def getMapping(index: String): ElasticResult[String] = {
    validateIndexName(index) match {
      case Some(error) =>
        return ElasticResult.failure(
          error.copy(
            message = s"Invalid index: ${error.message}",
            statusCode = Some(400),
            index = Some(index),
            operation = Some("getMapping")
          )
        )
      case None => // continue
    }

    logger.debug(s"Getting mapping for index '$index'")

    executeGetMapping(index)
  }

  /** Get the mapping properties of an index.
    * @param index
    *   - the name of the index to get the mapping properties for
    * @return
    *   the mapping properties of the index as a JSON string
    */
  def getMappingProperties(index: String): ElasticResult[String] =
    getMapping(index)

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
  ): ElasticResult[Boolean] = {
    getMappingProperties(index).map { properties =>
      MappingComparator.isMappingDifferent(properties, mapping)
    }
  }

  /** Update the mapping of an index to a new mapping.
    *
    * This method handles three scenarios:
    *   1. Index doesn't exist: Create it with the new mapping 2. Index exists but mapping is
    *      outdated: Migrate to new mapping 3. Index exists and mapping is current: Do nothing
    *
    * @param index
    *   - the name of the index to migrate
    * @param mapping
    *   - the new mapping to set on the index
    * @param settings
    *   - the settings to apply to the index (default is defaultSettings)
    * @return
    *   true if the mapping was created or updated successfully, false otherwise
    */
  def updateMapping(
    index: String,
    mapping: String,
    settings: String = defaultSettings
  ): ElasticResult[Boolean] = {
    indexExists(index, false).flatMap {
      case false =>
        // Scenario 1: Index doesn't exist
        createIndex(index, settings, Some(mapping), Nil).flatMap {
          case true =>
            logger.info(s"✅ Index '$index' created with mapping successfully")
            ElasticResult.success(true)
          case false =>
            ElasticResult.failure(
              ElasticError(
                message = s"Failed to create index '$index' with mapping",
                index = Some(index),
                operation = Some("updateMapping")
              )
            )
        }

      case true =>
        // Check if mapping needs update
        shouldUpdateMapping(index, mapping).flatMap {
          case true =>
            // Scenario 2: Migrate to new mapping
            logger.info(s"Mapping for index '$index' needs update. Starting migration.")
            migrateMappingWithRollback(index, mapping, settings)

          case false =>
            // Scenario 3: Mapping is current
            logger.info(s"✅ Mapping for index '$index' is already up to date")
            ElasticResult.success(true)

        }
    }
  }

  private def migrateMappingWithRollback(
    index: String,
    newMapping: String,
    settings: String
  ): ElasticResult[Boolean] = {

    val tempIndex = s"${index}_tmp_${UUID.randomUUID().toString.take(8)}"

    // Backup original state
    val backupResult = for {
      originalMapping  <- getMapping(index)
      originalSettings <- loadSettings(index)
    } yield (originalMapping, originalSettings)

    backupResult match {
      case ElasticSuccess((origMapping, origSettings)) =>
        logger.info(s"✅ Backed up original mapping and settings for '$index'")

        val migrationResult = performMigration(
          index,
          tempIndex,
          newMapping,
          settings
        )

        migrationResult match {
          case ElasticSuccess(true) =>
            logger.info(s"✅ Migration completed successfully for '$index'")
            ElasticSuccess(true)

          case ElasticFailure(error) =>
            logger.error(s"❌ Migration failed for '$index': ${error.fullMessage}")
            logger.info(s"Attempting rollback for '$index'")

            rollbackMigration(index, tempIndex, origMapping, origSettings) match {
              case ElasticSuccess(_) =>
                logger.info(s"✅ Rollback completed successfully for '$index'")
              case ElasticFailure(rollbackError) =>
                logger.error(s"❌ Rollback failed for '$index': ${rollbackError.fullMessage}")
            }

            ElasticFailure(error)
        }

      case ElasticFailure(error) =>
        logger.error(s"❌ Failed to backup original state for '$index': ${error.fullMessage}")
        ElasticFailure(error)
    }
  }

  /** Migrate an existing index to a new mapping.
    *
    * Process:
    *   1. Create temporary index with new mapping 2. Reindex data from original to temporary 3.
    *      Delete original index 4. Recreate original index with new mapping 5. Reindex data from
    *      temporary to original 6. Delete temporary index
    */
  private def performMigration(
    index: String,
    tempIndex: String,
    mapping: String,
    settings: String
  ): ElasticResult[Boolean] = {

    logger.info(s"Starting migration: $index -> $tempIndex")

    for {
      // Create temp index
      _ <- createIndex(tempIndex, settings, None, Nil)
        .filter(_ == true, s"❌ Failed to create temp index '$tempIndex'")

      _ <- setMapping(tempIndex, mapping)
        .filter(_ == true, s"❌ Failed to set mapping on temp index")

      // Reindex to temp
      _ <- reindex(index, tempIndex, refresh = true)
        .filter(_._1 == true, s"❌ Failed to reindex to temp")

      // Delete original
      _ <- deleteIndex(index)
        .filter(_ == true, s"❌ Failed to delete original index")

      // Recreate original with new mapping
      _ <- createIndex(index, settings, None, Nil)
        .filter(_ == true, s"❌ Failed to recreate original index")

      _ <- setMapping(index, mapping)
        .filter(_ == true, s"❌ Failed to set new mapping")

      // Reindex back from temp
      _ <- reindex(tempIndex, index, refresh = true)
        .filter(_._1 == true, s"❌ Failed to reindex from temp")

      _ <- openIndex(index)
        .filter(_ == true, s"❌ Failed to open index")

      // Cleanup temp
      _ <- deleteIndex(tempIndex)

    } yield {
      logger.info(s"✅ Migration completed: $index")
      true
    }
  }

  private def rollbackMigration(
    index: String,
    tempIndex: String,
    originalMapping: String,
    originalSettings: String
  ): ElasticResult[Boolean] = {

    logger.warn(s"Rolling back migration for '$index'")

    for {
      // Check if temp index exists and has data
      tempExists <- indexExists(tempIndex, false)

      // Delete current (potentially corrupted) index if it exists
      _ <- indexExists(index, false).flatMap {
        case true  => deleteIndex(index)
        case false => ElasticResult.success(true)
      }

      // Recreate with original settings and mapping
      _ <- createIndex(index, originalSettings, None, Nil)
        .filter(_ == true, s"❌ Rollback: Failed to recreate index")

      _ <- setMapping(index, originalMapping)
        .filter(_ == true, s"❌ Rollback: Failed to restore mapping")

      // If temp exists, reindex from it
      _ <-
        if (tempExists) {
          reindex(tempIndex, index, refresh = true)
            .filter(_._1 == true, s"❌ Rollback: Failed to reindex from temp")
        } else {
          ElasticResult.success(true)
        }

      _ <- openIndex(index)

      // Cleanup temp if it exists
      _ <-
        if (tempExists) {
          deleteIndex(tempIndex)
        } else {
          ElasticResult.success(true)
        }

    } yield {
      logger.info(s"✅ Rollback index completed for '$index'")
      true
    }
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeSetMapping(index: String, mapping: String): ElasticResult[Boolean]

  private[client] def executeGetMapping(index: String): ElasticResult[String]
}
