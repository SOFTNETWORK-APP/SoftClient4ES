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

import app.softnetwork.elastic.sql.health.HealthStatus
import app.softnetwork.elastic.sql.transform.TransformTimeInterval
import org.slf4j.Logger

import java.time.ZonedDateTime
import java.time.temporal.ChronoUnit

/** Watcher health status
  *
  * @param id
  *   Watcher ID
  * @param active
  *   Whether the watcher is active
  * @param lastChecked
  *   Optional timestamp of the last check
  */
case class WatcherHealthStatus(
  id: String,
  active: Boolean,
  lastChecked: Option[ZonedDateTime],
  frequency: TransformTimeInterval,
  createdAt: Option[ZonedDateTime] = None,
  executionState: Option[WatcherExecutionState] = None
) {
  private[this] lazy val timeSinceLastCheck: Option[Long] = lastChecked.map { lc =>
    ChronoUnit.SECONDS.between(lc, ZonedDateTime.now())
  }

  private lazy val timeSinceCreation: Option[Long] = createdAt.map { ca =>
    ChronoUnit.SECONDS.between(ca, ZonedDateTime.now())
  }

  /** Determines if the watcher is healthy
    *
    * Health criteria:
    *   - If never checked but recently created (< 2x frequency): Considered healthy (warming up)
    *   - If checked: Must be within frequency interval
    *   - If inactive: Always unhealthy
    */
  def isHealthy: Boolean = {
    if (!active) return false

    timeSinceLastCheck match {
      case Some(seconds) =>
        // Watcher a déjà été exécuté
        seconds < frequency.toSeconds

      case None =>
        // Watcher jamais exécuté : vérifier s'il est récent
        timeSinceCreation match {
          case Some(seconds) if seconds < (frequency.toSeconds * 2) =>
            true // ✅ Nouveau watcher en période de warm-up
          case Some(_) =>
            false // ❌ Créé depuis trop longtemps sans exécution
          case None =>
            false // ❌ Pas d'info de création et jamais exécuté
        }
    }
  }

  def overallHealthStatus: HealthStatus = {
    executionState match {
      case Some(state) =>
        healthStatus + state.health

      case None =>
        // If the watcher is recent (< 2x frequency), it's OK
        // Otherwise, it's suspicious.
        timeSinceCreation match {
          case Some(age) if age < (frequency.toSeconds * 2) =>
            healthStatus // ✅ New watcher, no execution yet
          case Some(_) =>
            healthStatus + HealthStatus.Yellow // ⚠️ Former watcher with no execution status
          case None =>
            healthStatus // No information, we assume OK
        }
    }
  }

  def healthStatus: HealthStatus = {
    if (!active) HealthStatus.Yellow
    else if (isHealthy) HealthStatus.Green
    else HealthStatus.Red
  }

  /** Get detailed health info with formatted output */
  def getHealthDetails: String = {
    val status = healthStatus
    val lastCheckInfo = lastChecked match {
      case Some(lc) =>
        val delay = timeSinceLastCheck.getOrElse(0L)
        s"Last checked: ${lc.format(java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME)} (${delay}s ago)"
      case None =>
        val creationInfo = createdAt match {
          case Some(ca) =>
            val age = timeSinceCreation.getOrElse(0L)
            s" (created ${age}s ago)"
          case None => ""
        }
        s"Never checked$creationInfo"
    }

    val frequencyInfo =
      s"Expected frequency: ${frequency.toSeconds}s (${frequency.interval} ${frequency.timeUnit.name})"

    s"""${status.display} Watcher: $id
       |  Active: $active
       |  $lastCheckInfo
       |  $frequencyInfo
       |  Operational: ${status.isOperational}
       |""".stripMargin
  }

  /** Get health status as a map (useful for JSON serialization) */
  def toMap: Map[String, Any] = Map(
    "id"                            -> id,
    "active"                        -> active,
    "status"                        -> healthStatus.name,
    "status_emoji"                  -> healthStatus.emoji,
    "severity"                      -> healthStatus.severity,
    "is_healthy"                    -> isHealthy,
    "is_operational"                -> healthStatus.isOperational,
    "last_checked"                  -> lastChecked.map(_.toString).getOrElse("never"),
    "time_since_last_check_seconds" -> timeSinceLastCheck.getOrElse(-1), // -1 = unknown
    "frequency_seconds"             -> frequency.toSeconds,
    "created_at"                    -> createdAt.map(_.toString).getOrElse("unknown"),
    "execution_status"              -> executionState.map(_.name).orNull, // ✅ null if absent
    "execution_status_emoji"        -> executionState.map(_.health.emoji).orNull,
    "execution_severity"   -> executionState.map(_.health.severity).getOrElse(-1), // -1 = unknown
    "overall_status"       -> overallHealthStatus.name,
    "overall_status_emoji" -> overallHealthStatus.emoji,
    "overall_severity"     -> overallHealthStatus.severity
  )

  def logWatcherHealth(implicit logger: Logger): Unit = { // UPDATED
    overallHealthStatus match {
      case HealthStatus.Green =>
        logger.info(s"${overallHealthStatus.emoji} Watcher $id is healthy")

      case HealthStatus.Yellow =>
        val reason = (active, executionState) match {
          case (false, _) =>
            "watcher is inactive"
          case (true, Some(WatcherExecutionState.ConditionNotMet)) =>
            "condition not met (no action required)"
          case (true, Some(WatcherExecutionState.Throttled)) =>
            "actions throttled (too frequent executions)"
          case (true, Some(WatcherExecutionState.Acknowledged)) =>
            "watcher acknowledged (manual intervention)"
          case (true, None) =>
            "no recent execution data"
          case _ =>
            "unknown reason"
        }

        logger.warn(
          s"${overallHealthStatus.emoji} Watcher $id is in warning state: $reason"
        )

      case HealthStatus.Red =>
        logger.error(
          s"${overallHealthStatus.emoji} Watcher $id is unhealthy!\n" +
          getHealthDetails
        )

      case HealthStatus.Other(name) =>
        logger.warn(
          s"${overallHealthStatus.emoji} Watcher $id has unknown status: $name"
        )
    }
  }

}

object WatcherHealthStatus {

  /** Creates WatcherHealthStatus from status and trigger info */
  def fromStatus(
    watcherId: String,
    status: WatcherStatus,
    trigger: WatcherTrigger
  ): WatcherHealthStatus = {
    WatcherHealthStatus(
      id = watcherId,
      active = status.activationState.active,
      lastChecked = status.lastChecked,
      frequency = trigger.interval,
      executionState = status.executionState
    )
  }

  /** Creates WatcherHealthStatus from a Watcher instance
    *
    * @param watcher
    *   Watcher instance
    * @return
    *   WatcherHealthStatus
    */
  def fromWatcher(watcher: Watcher): WatcherHealthStatus = {
    val status = watcher.status.getOrElse {
      throw new IllegalArgumentException(
        s"Cannot create health status: Watcher '${watcher.id}' has no status"
      )
    }
    fromStatus(watcher.id, status, watcher.trigger)
  }
}
