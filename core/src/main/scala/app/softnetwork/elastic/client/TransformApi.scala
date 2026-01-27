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
import app.softnetwork.elastic.sql.schema.{TransformConfig, TransformCreationStatus, TransformStats}

trait TransformApi extends ElasticClientHelpers { _: VersionApi =>

  /** Create a transform with the given configuration. If start is true, the transform will be
    * started immediately after creation.
    * @param config
    *   The configuration of the transform to create.
    * @param start
    *   Whether to start the transform immediately after creation.
    * @return
    *   ElasticResult[TransformCreationStatus] indicating whether the create operation and its
    *   optional start were successful.
    */
  def createTransform(
    config: TransformConfig,
    start: Boolean = false
  ): ElasticResult[TransformCreationStatus] = {
    logger.info(s"Creating transform [${config.id}]...")
    executeCreateTransform(config, start) match {
      case ElasticSuccess(true) if start =>
        logger.info(s"✅ Transform [${config.id}] created successfully. Starting transform...")
        startTransform(config.id) match {
          case ElasticSuccess(status) =>
            ElasticSuccess(TransformCreationStatus(created = true, started = Some(status)))
          case failure @ ElasticFailure(_) =>
            failure
        }
      case ElasticSuccess(status) =>
        ElasticSuccess(TransformCreationStatus(created = status))
      case failure @ ElasticFailure(_) => failure
    }
  }

  /** Delete the transform with the given ID. If force is true, the transform will be deleted even
    * if it is currently running.
    * @param transformId
    *   The ID of the transform to delete.
    * @param force
    *   Whether to force delete the transform.
    * @return
    *   ElasticResult[Boolean] indicating whether the delete operation was successful.
    */
  def deleteTransform(transformId: String, force: Boolean = false): ElasticResult[Boolean] = {
    logger.info(s"Deleting transform [$transformId]...")
    executeDeleteTransform(transformId, force) match {
      case success @ ElasticSuccess(true) =>
        logger.info(s"✅ Transform [$transformId] deleted successfully.")
        success
      case success @ ElasticSuccess(false) =>
        logger.warn(s"⚠️ Transform [$transformId] could not be deleted.")
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to delete transform [$transformId]: ${error.message}")
        failure
    }
  }

  /** Start the transform with the given ID.
    * @param transformId
    *   The ID of the transform to start.
    * @return
    *   ElasticResult[Boolean] indicating whether the start operation was successful.
    */
  def startTransform(transformId: String): ElasticResult[Boolean] = {
    logger.info(s"Starting transform [$transformId]...")
    executeStartTransform(transformId) match {
      case success @ ElasticSuccess(true) =>
        logger.info(s"✅ Transform [$transformId] started successfully.")
        success
      case success @ ElasticSuccess(false) =>
        logger.warn(s"⚠️ Transform [$transformId] could not be started.")
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to start transform [$transformId]: ${error.message}")
        failure
    }
  }

  /** Stop the transform with the given ID. If force is true, the transform will be stopped
    * immediately. If waitForCompletion is true, the method will wait until the transform is fully
    * stopped before returning.
    * @param transformId
    *   The ID of the transform to stop.
    * @param force
    *   Whether to force stop the transform.
    * @param waitForCompletion
    *   Whether to wait for the transform to be fully stopped.
    * @return
    *   ElasticResult[Boolean] indicating whether the stop operation was successful.
    */
  def stopTransform(
    transformId: String,
    force: Boolean = false,
    waitForCompletion: Boolean = true
  ): ElasticResult[Boolean] = {
    logger.info(s"Stopping transform [$transformId]...")
    executeStopTransform(transformId, force, waitForCompletion) match {
      case success @ ElasticSuccess(true) =>
        logger.info(s"✅ Transform [$transformId] stopped successfully.")
        success
      case success @ ElasticSuccess(false) =>
        logger.warn(s"⚠️ Transform [$transformId] could not be stopped.")
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to stop transform [$transformId]: ${error.message}")
        failure
    }
  }

  /** Get the statistics of the transform with the given ID.
    * @param transformId
    *   The ID of the transform to get statistics for.
    * @return
    *   ElasticResult[Option[TransformStats]\] containing the transform statistics if available.
    */
  def getTransformStats(transformId: String): ElasticResult[Option[TransformStats]] = {
    logger.info(s"Getting stats for transform [$transformId]...")
    executeGetTransformStats(transformId) match {
      case success @ ElasticSuccess(Some(_)) =>
        logger.info(s"✅ Retrieved stats for transform [$transformId] successfully.")
        success
      case success @ ElasticSuccess(None) =>
        logger.warn(s"⚠️ No stats found for transform [$transformId].")
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to get stats for transform [$transformId]: ${error.message}")
        failure
    }
  }

  def scheduleTransformNow(transformId: String): ElasticResult[Boolean] = {
    // Implementation depends on Elasticsearch capabilities; placeholder here
    val elasticsearchVersion = version match {
      case ElasticSuccess(version) => version
      case ElasticFailure(error) =>
        val failure = s"Cannot schedule transform [$transformId] to run now: " +
          s"failed to get Elasticsearch version: ${error.message}"
        logger.error(s"❌ $failure")
        return ElasticFailure(
          ElasticError(
            message = failure,
            cause = Some(error),
            statusCode = error.statusCode,
            operation = Some("scheduleNow")
          )
        )
    }
    if (!ElasticsearchVersion.supportsScheduleTransformNow(elasticsearchVersion)) {
      val failure = s"Cannot schedule transform [$transformId] to run now: " +
        s"Elasticsearch version $elasticsearchVersion does not support latest transform features."
      logger.error(s"❌ $failure")
      return ElasticFailure(
        ElasticError(
          message = failure,
          statusCode = Some(400),
          operation = Some("scheduleNow")
        )
      )
    }
    logger.info(s"Scheduling transform [$transformId] to run now...")
    executeScheduleTransformNow(transformId) match {
      case success @ ElasticSuccess(true) =>
        logger.info(s"✅ Transform [$transformId] scheduled to run now successfully.")
        success
      case success @ ElasticSuccess(false) =>
        logger.warn(s"⚠️ Transform [$transformId] could not be scheduled to run now.")
        success
      case failure @ ElasticFailure(error) =>
        logger.error(s"❌ Failed to schedule transform [$transformId] to run now: ${error.message}")
        failure
    }
  }

  private[client] def executeCreateTransform(
    config: TransformConfig,
    start: Boolean
  ): ElasticResult[Boolean]

  private[client] def executeDeleteTransform(
    transformId: String,
    force: Boolean
  ): ElasticResult[Boolean]

  private[client] def executeStartTransform(transformId: String): ElasticResult[Boolean]

  private[client] def executeStopTransform(
    transformId: String,
    force: Boolean,
    waitForCompletion: Boolean
  ): ElasticResult[Boolean]

  private[client] def executeGetTransformStats(
    transformId: String
  ): ElasticResult[Option[TransformStats]]

  private[client] def executeScheduleTransformNow(
    transformId: String
  ): ElasticResult[Boolean]
}
