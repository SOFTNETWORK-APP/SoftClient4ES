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

import app.softnetwork.elastic.client.jest.actions.Watcher
import app.softnetwork.elastic.client.{result, WatcherApi}
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.schema
import app.softnetwork.elastic.sql.schema.{Delay, TransformTimeInterval}
import app.softnetwork.elastic.sql.serialization._
import app.softnetwork.elastic.utils.CronIntervalCalculator
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import io.searchbox.client.JestResult

import java.time.ZonedDateTime
import scala.util.Try

trait JestWatcherApi extends WatcherApi with JestClientHelpers { _: JestClientCompanion =>

  override private[client] def executeCreateWatcher(
    watcher: schema.Watcher,
    active: Boolean
  ): result.ElasticResult[Boolean] = {
    // There is no direct API to create a watcher in Jest.
    implicit val timestamp: Long = System.currentTimeMillis()
    val json = watcher
      .copy(actions = watcher.actions.map { case (name, action) =>
        name -> (action match {
          case l: schema.LoggingAction =>
            l.copy(foreach = None, maxIterations = None)
          case w: schema.WebhookAction =>
            w.copy(foreach = None, maxIterations = None)
          case other => other
        })
      })
      .node
    logger.info(s"Creating Watcher ${watcher.id} :\n${sanitizeWatcherJson(json)}")
    apply().execute(Watcher.Create(watcher.id, json)) match {
      case jestResult: JestResult if jestResult.isSucceeded =>
        result.ElasticSuccess(true)
      case jestResult: JestResult =>
        val errorMessage = jestResult.getErrorMessage
        result.ElasticFailure(
          result.ElasticError(
            s"Failed to create watcher '${watcher.id}': $errorMessage"
          )
        )
    }
  }

  override private[client] def executeDeleteWatcher(id: String): result.ElasticResult[Boolean] = {
    // There is no direct API to delete a watcher in Jest.
    apply().execute(Watcher.Delete(id)) match {
      case jestResult: JestResult if jestResult.isSucceeded =>
        result.ElasticSuccess(true)
      case jestResult: JestResult =>
        val errorMessage = jestResult.getErrorMessage
        result.ElasticFailure(
          result.ElasticError(
            s"Failed to delete watcher '$id': $errorMessage"
          )
        )
    }
  }

  override private[client] def executeGetWatcherStatus(
    id: String
  ): result.ElasticResult[Option[schema.WatcherStatus]] = {
    // There is no direct API to get a watcher in Jest.
    apply().execute(Watcher.Get(id)) match {
      case jestResult: JestResult if jestResult.isSucceeded =>
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
                  result.ElasticSuccess(
                    Some(
                      schema.WatcherStatus(
                        id = id,
                        version = version,
                        activationState = schema.WatcherActivationState(
                          active = active,
                          timestamp = timestamp
                        ),
                        interval = interval
                      )
                    )
                  )
                case _ => // do nothing
                  result.ElasticSuccess(None)
              }
            case _ =>
              result.ElasticSuccess(None)
          }
        } else {
          result.ElasticSuccess(None)
        }
      case jestResult: JestResult =>
        val errorMessage = jestResult.getErrorMessage
        result.ElasticFailure(
          result.ElasticError(
            s"Failed to get watcher '$id': $errorMessage"
          )
        )
    }
  }
}
