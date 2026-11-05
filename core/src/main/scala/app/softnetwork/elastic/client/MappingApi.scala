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
import app.softnetwork.elastic.schema.IndexMappings
import app.softnetwork.elastic.sql.schema.{copyableSettings, TableAlias}
import com.fasterxml.jackson.databind.node.ObjectNode
import com.google.gson.JsonParser

import java.util.UUID

/** Mapping management API.
  */
trait MappingApi extends ElasticClientHelpers {
  _: SettingsApi with IndicesApi with RefreshApi with VersionApi with AliasApi =>

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

    // Get Elasticsearch version
    val elasticVersion = {
      this.version match {
        case ElasticSuccess(v) => v
        case ElasticFailure(error) =>
          logger.error(s"❌ Failed to retrieve Elasticsearch version: ${error.message}")
          return ElasticFailure(error)
      }
    }

    val updatedMapping: String =
      if (ElasticsearchVersion.requiresDocTypeWrapper(elasticVersion)) {
        val root = mapper.readTree(mapping).asInstanceOf[ObjectNode]

        if (root.has("properties") || root.has("_meta")) {
          logger.info(s"Wrapping mappings with '_doc' type for ES version $elasticVersion")

          val doc = mapper.createObjectNode()

          // Move properties
          if (root.has("properties")) {
            doc.set("properties", root.get("properties"))
            root.remove("properties")
          }

          // Move _meta
          if (root.has("_meta")) {
            doc.set("_meta", root.get("_meta"))
            root.remove("_meta")
          }

          // Wrap into _doc
          root.set("_doc", doc)

          root.toString
        } else {
          mapping
        }
      } else {
        mapping
      }

    logger.debug(s"Setting mapping for index '$index': $updatedMapping")

    executeSetMapping(index, updatedMapping) match {
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

  /** Get the mappings of all indices.
    * @param indices
    *   - optional list of index names to filter by (if empty, retrieves mappings for all indices)
    * @return
    *   a map of index names to their mappings
    */
  def allMappings(indices: Seq[String] = Seq.empty): ElasticResult[Map[String, IndexMappings]] = {
    // Get mappings for all indices
    executeGetAllMappings(indices.map(_.replaceAll("%", "*"))).map { mappingsMap =>
      mappingsMap.map { case (index, mappingJson) =>
        val indexMappings = IndexMappings(mappingJson)
        (index, indexMappings)
      }
    }
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
    indexExists(index, pattern = false).flatMap {
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
      originalAliases  <- getAliases(index)
    } yield (originalMapping, originalSettings, originalAliases)

    backupResult match {
      case ElasticSuccess((origMapping, origSettings, origAliases)) =>
        logger.info(s"✅ Backed up original mapping and settings for '$index'")

        migrateIndex(
          index,
          tempIndex,
          newMapping,
          settings,
          Nil,
          IndexBackup(origMapping, origSettings, origAliases)
        ) match {
          case ElasticSuccess(_) =>
            logger.info(s"✅ Migration completed successfully for '$index'")
            ElasticSuccess(true)
          case failure @ ElasticFailure(_) =>
            failure
        }

      case ElasticFailure(error) =>
        logger.error(s"❌ Failed to backup original state for '$index': ${error.fullMessage}")
        ElasticFailure(error)
    }
  }

  /** Migrate an existing index to a new mapping, and roll back a failure.
    *
    * Process:
    *   1. Create temporary index with new mapping 2. Reindex data from original to temporary 3.
    *      Delete original index 4. Recreate original index with new mapping 5. Reindex data from
    *      temporary to original 6. Open the index 7. Delete temporary index
    *
    * 🔴 The ONE entry point of a reindex migration (`updateMapping` and the ALTER TABLE reindex
    * both call it): the forward steps, then on a failure the step-aware rollback, then the error. A
    * step that throws is a failure of that step. A failed deletion of the temporary index is not a
    * failure: the index already holds every document under the new definition, so the migration
    * answers success and a warning names the temporary index.
    *
    * The error of a failed migration is the cause plus one sentence saying where the documents are
    * ([[IndexMigration.migrationError]]).
    *
    * @param aliases
    *   the aliases the temporary index is created with (step 1); the original index is re-created
    *   without aliases
    * @param original
    *   the definition the rollback restores
    */
  private[client] def migrateIndex(
    index: String,
    tempIndex: String,
    mapping: String,
    settings: String,
    aliases: Seq[TableAlias],
    original: IndexBackup
  ): ElasticResult[Boolean] = {
    import MigrationStep._

    logger.info(s"Starting migration: $index -> $tempIndex")

    var failedAt: MigrationStep = CreateTemp

    def at[T](step: MigrationStep)(request: => ElasticResult[T]): ElasticResult[T] = {
      failedAt = step
      ElasticResult.attempt(request).flatMap(r => r)
    }

    val forward = for {
      // Create temp index
      _ <- at(CreateTemp)(
        createIndex(tempIndex, settings, Some(mapping), aliases)
          .filter(_ == true, s"❌ Failed to create temp index '$tempIndex'")
      )

      // Reindex to temp
      _ <- at(CopyToTemp)(
        reindex(index, tempIndex, refresh = true)
          .filter(_._1 == true, s"❌ Failed to reindex to temp")
      )

      // Delete original
      _ <- at(DeleteOriginal)(
        deleteIndex(index)
          .filter(_ == true, s"❌ Failed to delete original index")
      )

      // Recreate original with new settings, mapping and aliases
      _ <- at(RecreateOriginal)(
        createIndex(index, settings, Some(mapping), Nil)
          .filter(_ == true, s"❌ Failed to recreate original index")
      )

      // Reindex back from temp
      _ <- at(CopyBack)(
        reindex(tempIndex, index, refresh = true)
          .filter(_._1 == true, s"❌ Failed to reindex from temp")
      )

      _ <- at(OpenOriginal)(
        openIndex(index)
          .filter(_ == true, s"❌ Failed to open index")
      )
    } yield true

    forward match {
      case ElasticSuccess(_) =>
        // Cleanup temp
        at(DeleteTemp)(deleteIndex(tempIndex)) match {
          case ElasticSuccess(true) =>
          case other =>
            val why = other match {
              case ElasticFailure(error) => error.message
              case _                     => "the deletion was not acknowledged"
            }
            val aliasesLeft = removeTempAliases(tempIndex, aliases)
              .map(e => s"; ${e.message}")
              .getOrElse("")
            logger.warn(
              s"⚠️ Index '$index' was migrated, but the temporary index '$tempIndex' could not be deleted: $why$aliasesLeft. Delete it once '$index' is checked."
            )
        }
        logger.info(s"✅ Migration completed: $index")
        ElasticSuccess(true)

      case ElasticFailure(cause) =>
        logger.error(s"❌ Migration failed for '$index': ${cause.fullMessage}")
        logger.info(s"Attempting rollback for '$index'")

        val outcome = rollbackMigration(index, tempIndex, original, failedAt, aliases)
        val error = IndexMigration.migrationError(index, tempIndex, cause, outcome)
        outcome match {
          case RollbackOutcome.Unchanged(None) | RollbackOutcome.Restored(None) =>
            logger.info(s"✅ Rollback completed successfully for '$index'")
          case _ =>
            logger.error(s"❌ Rollback failed for '$index': ${error.message}")
        }
        ElasticFailure(error)
    }
  }

  /** Roll back a migration that failed at `failedAt`.
    *
    *   - The original index was never deleted (`CreateTemp`, `CopyToTemp`): the temporary index is
    *     removed if it exists, the original is never touched.
    *   - `DeleteOriginal`: a deletion that answered a failure may still take effect, so ONE probe
    *     decides. Present: nothing is deleted, the temporary index is kept and named. Absent: the
    *     deletion took effect, the index is restored. Unknown: nothing is touched.
    *   - The original index was deleted (`RecreateOriginal`, `CopyBack`, `OpenOriginal`): it is
    *     restored, only if the temporary index exists. The restore deletes the index if it exists
    *     (it holds no document the temporary copy lacks), re-creates it with the original mapping
    *     and settings (without the generated ones), its pipelines set to `_none` and no alias,
    *     copies the documents back through no pipeline, attaches the original pipelines back
    *     (close, update, open; with none, opens it), re-adds each original alias, and deletes the
    *     temporary index LAST.
    *   - `DeleteTemp` is never rolled back: the index holds every document under the new
    *     definition; nothing is touched.
    *
    * 🔴 Any failing step stops the rollback, and after a failed deletion of one copy nothing
    * deletes the other: a deletion that answered a failure may land later. A temporary index kept
    * while the index holds every document loses the aliases it was created with (`tempAliases`), so
    * that no alias reads both copies.
    *
    * @param tempAliases
    *   the aliases the temporary index was created with
    */
  private[client] def rollbackMigration(
    index: String,
    tempIndex: String,
    original: IndexBackup,
    failedAt: MigrationStep,
    tempAliases: Seq[TableAlias] = Nil
  ): RollbackOutcome = {
    import MigrationStep._
    import RollbackOutcome._

    logger.warn(s"Rolling back migration for '$index' (failed to ${failedAt.name})")

    def step[T](request: => ElasticResult[T]): ElasticResult[T] =
      ElasticResult.attempt(request).flatMap(r => r)

    /** The temporary index is kept while `index` holds every document. */
    def keptBeside(reason: ElasticError): ElasticError =
      removeTempAliases(tempIndex, tempAliases) match {
        case Some(aliasError) => reason.copy(message = s"${reason.message}; ${aliasError.message}")
        case None             => reason
      }

    def removeTemp(): Option[ElasticError] =
      step(indexExists(tempIndex, pattern = false)) match {
        case ElasticSuccess(false) => None
        case ElasticSuccess(true) =>
          step(deleteIndex(tempIndex)) match {
            case ElasticSuccess(true) => None
            case ElasticSuccess(_) =>
              Some(keptBeside(ElasticError("its deletion was not acknowledged")))
            case ElasticFailure(error) => Some(keptBeside(error))
          }
        case ElasticFailure(error) =>
          Some(
            keptBeside(error.copy(message = s"could not tell whether it exists: ${error.message}"))
          )
      }

    def restore(): RollbackOutcome =
      step(indexExists(tempIndex, pattern = false)) match {
        case ElasticSuccess(false) => NoTemporaryCopy
        case ElasticFailure(error) =>
          Kept(
            error.copy(message =
              s"could not tell whether the temporary index exists: ${error.message}"
            )
          )
        case ElasticSuccess(true) =>
          val copiedBack = for {
            // The original settings without the generated ones, its pipelines detached: the
            // documents copied back are already as the original held them
            detached <- step(
              ElasticResult.success(
                IndexMigration.detachPipelines(copyableSettings(original.settings))
              )
            )

            // Delete the index left by the forward steps, if any
            _ <- step(indexExists(index, pattern = false)).flatMap {
              case true =>
                step(deleteIndex(index))
                  .filter(_ == true, s"the deletion of index '$index' was not acknowledged")
              case false => ElasticResult.success(true)
            }

            // Recreate with the original settings and mapping, no pipeline, without aliases
            _ <- step(
              createIndex(
                index,
                detached._1,
                Some(IndexMigration.creatableMapping(index, original.mapping)),
                Nil
              )
            ).filter(_ == true, s"the creation of index '$index' was not acknowledged")

            // Copy the documents back, through no pipeline
            _ <- step(reindex(tempIndex, index, refresh = true))
              .filter(_._1 == true, s"the copy back to index '$index' failed")
          } yield detached._2

          copiedBack match {
            case ElasticFailure(error)           => Kept(error)
            case ElasticSuccess(attachPipelines) =>
              // From here the index holds every document
              val reopened = for {
                // Attach the original pipelines back (close, update, open), else open the index
                _ <- attachPipelines match {
                  case Some(pipelines) =>
                    step(updateSettings(index, pipelines)) match {
                      case ElasticSuccess(true) => ElasticResult.success(true)
                      case ElasticSuccess(_) =>
                        ElasticResult.failure(
                          ElasticError(
                            s"re-attaching the original pipelines to index '$index' (close, update the settings, open) was not acknowledged, the index may be left closed"
                          )
                        )
                      case ElasticFailure(error) =>
                        ElasticResult.failure(
                          error.copy(message =
                            s"re-attaching the original pipelines to index '$index' (close, update the settings, open) failed, the index may be left closed: ${error.message}"
                          )
                        )
                    }
                  case None =>
                    step(openIndex(index))
                      .filter(_ == true, s"the opening of index '$index' was not acknowledged")
                }

                // Re-add the original aliases, their filter as Elasticsearch accepts it
                _ <- original.aliases.foldLeft(ElasticResult.success(true)) { (previous, alias) =>
                  previous.flatMap { _ =>
                    step(addAlias(IndexMigration.plainAlias(alias).copy(table = index)))
                      .filter(_ == true, s"alias '${alias.alias}' was not added back")
                  }
                }
              } yield true

              reopened match {
                case ElasticFailure(error) => Kept(keptBeside(error))
                case ElasticSuccess(_)     =>
                  // Cleanup temp, LAST
                  step(deleteIndex(tempIndex)) match {
                    case ElasticSuccess(true) =>
                      logger.info(s"✅ Rollback index completed for '$index'")
                      Restored(None)
                    case ElasticSuccess(_) =>
                      Restored(Some(keptBeside(ElasticError("its deletion was not acknowledged"))))
                    case ElasticFailure(error) =>
                      Restored(Some(keptBeside(error)))
                  }
              }
          }
      }

    failedAt match {
      case CreateTemp | CopyToTemp =>
        removeTemp() match {
          case None =>
            logger.info(s"✅ Rollback index completed for '$index'")
            Unchanged(None)
          case kept => Unchanged(kept)
        }

      case DeleteOriginal =>
        step(indexExists(index, pattern = false)) match {
          case ElasticSuccess(true) =>
            Unchanged(
              Some(
                keptBeside(
                  ElasticError(s"the deletion of index '$index' failed and may still take effect")
                )
              )
            )
          case ElasticSuccess(false) => restore()
          case ElasticFailure(error) =>
            // nothing is deleted; if the index is still there, no alias may read both copies
            Kept(
              keptBeside(
                error.copy(message = s"could not tell whether it was deleted: ${error.message}")
              )
            )
        }

      case RecreateOriginal | CopyBack | OpenOriginal =>
        restore()

      case DeleteTemp =>
        Unchanged(
          Some(
            ElasticError(s"index '$index' already holds every document; nothing was rolled back")
          )
        )
    }
  }

  /** Removes from a kept temporary index the aliases it was created with; the failures, joined. */
  private def removeTempAliases(
    tempIndex: String,
    aliases: Seq[TableAlias]
  ): Option[ElasticError] = {
    val failures = aliases.flatMap { alias =>
      ElasticResult.attempt(removeAlias(tempIndex, alias.alias)).flatMap(r => r) match {
        case ElasticFailure(error) => Some(s"alias '${alias.alias}': ${error.message}")
        case _                     => None
      }
    }
    if (failures.isEmpty) None
    else
      Some(
        ElasticError(
          s"its aliases could not all be removed (${failures.mkString("; ")})",
          index = Some(tempIndex)
        )
      )
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeSetMapping(index: String, mapping: String): ElasticResult[Boolean]

  private[client] def executeGetMapping(index: String): ElasticResult[String]

  private[client] def executeGetAllMappings(
    indices: Seq[String]
  ): ElasticResult[Map[String, String]]
}
