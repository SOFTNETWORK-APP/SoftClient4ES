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

package app.softnetwork.elastic.sql.policy

import app.softnetwork.elastic.sql.DdlToken
import app.softnetwork.elastic.sql.health.HealthStatus

sealed trait EnrichPolicyTaskStatus extends DdlToken {
  def name: String
  override def sql: String = name
  def health: HealthStatus
}

object EnrichPolicyTaskStatus {
  case object Scheduled extends EnrichPolicyTaskStatus {
    val name: String = "SCHEDULED"
    def health: HealthStatus = HealthStatus.Yellow
  }
  case object Running extends EnrichPolicyTaskStatus {
    val name: String = "RUNNING"
    def health: HealthStatus = HealthStatus.Yellow
  }
  case object Completed extends EnrichPolicyTaskStatus {
    val name: String = "COMPLETE"
    def health: HealthStatus = HealthStatus.Green
  }
  case object Cancelled extends EnrichPolicyTaskStatus {
    val name: String = "CANCELLED"
    def health: HealthStatus = HealthStatus.Yellow
  }
  case object Failed extends EnrichPolicyTaskStatus {
    val name: String = "FAILED"
    def health: HealthStatus = HealthStatus.Red
  }
  case class Other(status: String) extends EnrichPolicyTaskStatus {
    val name: String = status
    def health: HealthStatus = HealthStatus.Other(status)
  }

  def apply(name: String): EnrichPolicyTaskStatus = name.toUpperCase() match {
    case "SCHEDULED" => Scheduled
    case "RUNNING"   => Running
    case "COMPLETE"  => Completed
    case "CANCELLED" => Cancelled
    case "FAILED"    => Failed
    case other       => Other(other)
  }
}
