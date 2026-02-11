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

package app.softnetwork.elastic.sql.watcher

import app.softnetwork.elastic.sql.DdlToken
import app.softnetwork.elastic.sql.health.HealthStatus
import app.softnetwork.elastic.sql.transform.TransformTimeInterval

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit
import scala.collection.immutable.ListMap

/** Watcher activation state
  *
  * @param active
  *   Whether the watcher is active
  * @param timestamp
  *   Timestamp of the state
  */
case class WatcherActivationState(active: Boolean, timestamp: ZonedDateTime)

sealed trait WatcherExecutionState extends DdlToken {
  def name: String
  def sql: String = name
  def health: HealthStatus
}

object WatcherExecutionState {

  /** Condition was not met, no actions were executed. Example: Error count below threshold Health:
    * Yellow (normal behavior, but monitored)
    */
  case object ConditionNotMet extends WatcherExecutionState {
    val name: String = "ConditionNotMet"
    val health: HealthStatus = HealthStatus.Green
  }

  /** Watcher executed successfully, actions were performed. Example: Alert sent successfully
    * Health: Green
    */
  case object Executed extends WatcherExecutionState {
    val name: String = "Executed"
    val health: HealthStatus = HealthStatus.Green
  }

  /** Watcher execution failed. Example: Script error, webhook timeout Health: Red (requires
    * immediate attention)
    */
  case object Failed extends WatcherExecutionState {
    val name: String = "Failed"
    val health: HealthStatus = HealthStatus.Red
  }

  /** Actions were throttled to prevent spam. Example: Throttle period not elapsed since last
    * execution Health: Yellow (expected behavior, but worth monitoring)
    */
  case object Throttled extends WatcherExecutionState {
    val name: String = "Throttled"
    val health: HealthStatus = HealthStatus.Yellow
  }

  /** Watcher was acknowledged (manual override). Example: Operator acknowledged alert to suppress
    * notifications Health: Yellow (manual intervention required)
    */
  case object Acknowledged extends WatcherExecutionState {
    val name: String = "Acknowledged"
    val health: HealthStatus = HealthStatus.Yellow
  }

  case class Other(name: String) extends WatcherExecutionState {
    val health: HealthStatus = HealthStatus.Other("unknown")
  }

  def apply(name: String): WatcherExecutionState = name.toLowerCase() match {
    case "execution_not_needed" => ConditionNotMet
    case "executed"             => Executed
    case "failed"               => Failed
    case "throttled"            => Throttled
    case "acknowledged"         => Acknowledged
    case other                  => Other(other)
  }
}

/** Watcher status
  * @param id
  *   Watcher id
  * @param version
  *   Watcher version
  * @param activationState
  *   Activation state of the watcher
  * @param executionState
  *   Execution state of the watcher
  * @param lastChecked
  *   Optional timestamp of the last check
  * @param lastMetCondition
  *   Optional timestamp of the last time the condition was met
  * @param interval
  *   Optional Watcher interval
  */
case class WatcherStatus(
  id: String,
  version: Long,
  activationState: WatcherActivationState,
  executionState: Option[WatcherExecutionState] = None,
  lastChecked: Option[ZonedDateTime] = None,
  lastMetCondition: Option[ZonedDateTime] = None,
  interval: Option[TransformTimeInterval] = None
) {
  lazy val healthStatus: Option[WatcherHealthStatus] =
    interval.map { iv =>
      WatcherHealthStatus(
        id = id,
        active = activationState.active,
        lastChecked = lastChecked,
        frequency = iv,
        createdAt = Some(activationState.timestamp),
        executionState = executionState
      )
    }

  lazy val active: Boolean = activationState.active

  def isHealthy: Boolean = {
    healthStatus match {
      case Some(hs) => hs.isHealthy
      case None     => active
    }
  }

  private[this] lazy val timeSinceLastCheck: Option[Long] = lastChecked.map { lc =>
    ChronoUnit.SECONDS.between(lc, ZonedDateTime.now())
  }

  lazy val health: HealthStatus = {
    healthStatus match {
      case Some(hs) => hs.healthStatus
      case None =>
        if (!active) HealthStatus.Yellow
        else HealthStatus.Green
    }
  }

  lazy val overallHealth: HealthStatus = {
    healthStatus match {
      case Some(hs) => hs.overallHealthStatus
      case None =>
        executionState match {
          case Some(state) => health + state.health
          case None        => health
        }
    }
  }

  /** Get health status as a map (useful for JSON serialization) */
  def toMap: ListMap[String, Any] = ListMap(
    "id"                            -> id,
    "active"                        -> active,
    "status"                        -> health.name,
    "status_emoji"                  -> health.emoji,
    "severity"                      -> health.severity,
    "is_healthy"                    -> isHealthy,
    "is_operational"                -> health.isOperational,
    "last_checked"                  -> lastChecked.map(_.toString).getOrElse("never"),
    "time_since_last_check_seconds" -> timeSinceLastCheck.getOrElse(-1), // -1 = unknown
    "frequency_seconds"             -> interval.map(_.toSeconds).getOrElse(-1),
    "created_at"                    -> activationState.timestamp.toString,
    "execution_status"              -> executionState.map(_.name).orNull, // ✅ null if absent
    "execution_status_emoji"        -> executionState.map(_.health.emoji).orNull,
    "execution_severity"   -> executionState.map(_.health.severity).getOrElse(-1), // -1 = unknown
    "overall_status"       -> overallHealth.name,
    "overall_status_emoji" -> overallHealth.emoji,
    "overall_severity"     -> overallHealth.severity
  )

}
