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

package app.softnetwork.elastic.client.jest

import app.softnetwork.elastic.client.jest.actions.{Watcher => JestWatcher}
import app.softnetwork.elastic.client.result.{ElasticError, ElasticFailure}
import app.softnetwork.elastic.client.{result, WatcherApi}
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.serialization._
import app.softnetwork.elastic.sql.transform.{Delay, TransformTimeInterval}
import app.softnetwork.elastic.sql.watcher.{
  LoggingAction,
  Watcher,
  WatcherActivationState,
  WatcherStatus,
  WebhookAction
}
import app.softnetwork.elastic.utils.CronIntervalCalculator
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.searchbox.client.JestResult

import java.time.ZonedDateTime
import scala.util.{Failure, Success, Try}

trait JestWatcherApi extends WatcherApi with JestClientHelpers {
  _: JestVersionApi with JestClientCompanion =>

  override private[client] def executeCreateWatcher(
    watcher: Watcher,
    active: Boolean
  ): result.ElasticResult[Boolean] = {
    // There is no direct API to create a watcher in Jest.
    implicit val timestamp: Long = System.currentTimeMillis()
    val json = watcher
      .copy(actions = watcher.actions.map { case (name, action) =>
        name -> (action match {
          case l: LoggingAction =>
            l.copy(foreach = None, limit = None)
          case w: WebhookAction =>
            w.copy(foreach = None, limit = None)
          case other => other
        })
      })
      .node
    logger.info(s"Creating Watcher ${watcher.id} :\n${sanitizeWatcherJson(json)}")
    // Kept on the explicit shape rather than `executeJestBooleanAction` so the response-body
    // diagnostic below survives — `logError` records the message and status but not the body, and
    // watcher creation is the path extensions#49 was chased down. `tryAction` still supplies what
    // #215 is about: a network fault becomes an ElasticFailure instead of a throw, and the status
    // now reaches the caller rather than only the log.
    tryAction(apply().execute(JestWatcher.Create(watcher.id, json))) match {
      case Success(jestResult) if jestResult.isSucceeded =>
        result.ElasticSuccess(true)
      case Success(jestResult) =>
        val errorMessage = jestResult.getErrorMessage
        // FIXME: diagnostic logging for CI watcher creation failures — remove once root cause is identified
        val responseBody = Option(jestResult.getJsonString).getOrElse("")
        val statusCode = jestResult.getResponseCode
        logger.error(
          s"Failed to create watcher '${watcher.id}': $errorMessage (status: $statusCode). Response: $responseBody"
        )
        // end FIXME
        result.ElasticFailure(
          result.ElasticError(
            message = s"Failed to create watcher '${watcher.id}': $errorMessage",
            cause = None,
            statusCode = if (statusCode == 0) None else Some(statusCode),
            operation = Some("createWatcher")
          )
        )
      case Failure(ex) =>
        logger.error(
          s"Exception during createWatcher '${watcher.id}': ${ex.getMessage}",
          ex
        )
        result.ElasticFailure(
          result.ElasticError(
            message = s"Exception during createWatcher: ${ex.getMessage}",
            cause = Some(ex),
            statusCode = None,
            operation = Some("createWatcher")
          )
        )
    }
  }

  override private[client] def executeDeleteWatcher(id: String): result.ElasticResult[Boolean] =
    // There is no direct API to delete a watcher in Jest.
    executeJestBooleanAction[JestResult](
      operation = "deleteWatcher",
      retryable = false // Deletion can not be retried
    ) {
      JestWatcher.Delete(id)
    }

  override private[client] def executeGetWatcherStatus(
    id: String
  ): result.ElasticResult[Option[WatcherStatus]] =
    // There is no direct API to get a watcher in Jest.
    // The transformer walks the response with bare `get(...)` calls; routing through the wrapper
    // means a shape it does not expect becomes an ElasticFailure rather than an NPE escaping the
    // ElasticResult protocol.
    executeJestAction[JestResult, Option[WatcherStatus]](
      operation = "getWatcherStatus",
      retryable = true
    ) {
      JestWatcher.Get(id)
    } { jestResult =>
      val jsonString = jestResult.getJsonString
      if (jsonString != null && jsonString.nonEmpty) {
        val node: JsonNode = jsonString
        node match {
          case watcherNode: ObjectNode
              if watcherNode.has("found") && watcherNode.get("found").asBoolean() =>
            // extract interval
            val interval: Option[TransformTimeInterval] =
              if (watcherNode.has("watch")) {
                val watchNode = watcherNode.get("watch")
                val triggerNode = watchNode.get("trigger")
                if (triggerNode.has("schedule")) {
                  val scheduleNode = triggerNode.get("schedule")
                  if (scheduleNode.has("cron")) {
                    val cron = scheduleNode.get("cron").asText()
                    logger.info(s"Watcher $id has cron schedule: $cron")
                    CronIntervalCalculator.validateAndCalculate(cron) match {
                      case Right(interval) =>
                        val tuple = TransformTimeInterval.fromSeconds(interval._2)
                        Some(
                          Delay(
                            timeUnit = tuple._1,
                            interval = tuple._2
                          )
                        )
                      case _ =>
                        logger.warn(
                          s"Watcher [$id] has invalid cron expression: $cron"
                        )
                        None
                    }
                  } else if (scheduleNode.has("interval")) {
                    val interval = scheduleNode.get("interval").asText()
                    logger.info(s"Watcher $id has interval schedule: $interval")
                    TransformTimeInterval(interval) match {
                      case Some(ti) => Some(ti)
                      case _ =>
                        logger.warn(
                          s"Watcher [$id] has invalid interval: $interval"
                        )
                        None
                    }
                  } else {
                    logger.info(s"Watcher $id has unknown schedule")
                    None
                  }
                } else {
                  logger.info(s"Watcher $id has no schedule")
                  None
                }
              } else {
                logger.info(s"Watcher $id has no watch node")
                None
              }

            interval match {
              case None =>
                logger.warn(s"Watcher [$id] does not have a valid schedule interval")
              case Some(t) => // valid interval
                logger.info(s"Watcher [$id] has schedule interval: $t")
            }

            // extract status
            val statusNode = watcherNode.get("status")
            val version = statusNode.get("version").asLong()
            statusNode.get("state") match {
              case stateNode: ObjectNode if stateNode.has("active") =>
                val active = stateNode.get("active").asBoolean()
                val updatedTime = stateNode.get("timestamp").asText()
                val timestamp =
                  Try(ZonedDateTime.parse(updatedTime)).toOption.getOrElse(ZonedDateTime.now())
                Some(
                  WatcherStatus(
                    id = id,
                    version = version,
                    activationState = WatcherActivationState(
                      active = active,
                      timestamp = timestamp
                    ),
                    interval = interval
                  )
                )
              case _ => // do nothing
                None
            }
          case _ =>
            None
        }
      } else {
        None
      }
    }

  override private[client] def executeListWatchers(): result.ElasticResult[Seq[WatcherStatus]] =
    ElasticFailure(
      ElasticError(
        message = "Listing watchers is not supported by Jest client",
        cause = None,
        statusCode = Some(501),
        operation = Some("listWatchers")
      )
    )
}
