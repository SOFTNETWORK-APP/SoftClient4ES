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

package app.softnetwork.elastic.sql.health

import app.softnetwork.elastic.sql.DdlToken

/** Health status
  */
sealed trait HealthStatus extends DdlToken {
  def name: String
  def emoji: String
  def sql: String = name

  /** Check if this status is considered healthy/operational */
  def isOperational: Boolean = this match {
    case HealthStatus.Green | HealthStatus.Yellow => true
    case _                                        => false
  }

  /** Check if this is an unknown/custom status */
  def isCustom: Boolean = this.isInstanceOf[HealthStatus.Other]

  /** Get formatted display string */
  def display: String = s"$emoji $name"

  /** Get severity (0 = best, 3 = worst) */
  def severity: Int = HealthStatus.severityLevel(this)

  def +(other: HealthStatus): HealthStatus = {
    if (HealthStatus.ordering.gt(this, other)) this else other
  }
}

object HealthStatus {
  case object Green extends HealthStatus {
    val name: String = "Healthy"
    val emoji = "🟢"
  }
  case object Yellow extends HealthStatus {
    val name: String = "Warning"
    val emoji = "🟡"
  }
  case object Red extends HealthStatus {
    val name: String = "Unhealthy"
    val emoji = "🔴"
  }
  case class Other(name: String) extends HealthStatus {
    val emoji = "⚪"
  }

  def apply(name: String): HealthStatus = name.toLowerCase match {
    case "green" | "healthy"  => Green
    case "yellow" | "warning" => Yellow
    case "red" | "unhealthy"  => Red
    case other                => Other(other)
  }

  /** Check if status is considered operational */
  def isOperational(status: HealthStatus): Boolean = status match {
    case Green | Yellow => true
    case Red | Other(_) => false
  }

  /** Get severity level (0 = best, 3 = worst) */
  def severityLevel(status: HealthStatus): Int = status match {
    case Green    => 0
    case Yellow   => 1
    case Red      => 2
    case Other(_) => 3
  }

  /** Compare two health statuses (worse is "greater") */
  implicit val ordering: Ordering[HealthStatus] = Ordering.by(severityLevel)

  /** Get all known statuses (excluding Other) */
  val knownStatuses: Seq[HealthStatus] = Seq(Green, Yellow, Red)
}
