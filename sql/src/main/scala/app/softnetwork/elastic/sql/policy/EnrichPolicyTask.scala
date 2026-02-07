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

import java.time.ZonedDateTime
import scala.collection.immutable.ListMap

case class EnrichPolicyTask(
  policyName: String,
  taskId: String,
  status: EnrichPolicyTaskStatus,
  startTime: Option[ZonedDateTime] = None,
  endTime: Option[ZonedDateTime] = None,
  failureReason: Option[String] = None
) extends DdlToken {
  def sql: String =
    s"ENRICH POLICY TASK FOR $policyName IS ${status.name}${startTime
      .map(t => s" STARTED AT $t")
      .getOrElse("")}${endTime.map(t => s" ENDED AT $t").getOrElse("")}${failureReason
      .map(r => s" FAILURE REASON: $r")
      .getOrElse("")}"
  def toMap: ListMap[String, Any] = ListMap(
    "policy_name"   -> policyName,
    "task_id"       -> taskId,
    "status"        -> status.name,
    "health"        -> status.health.name,
    "health_emoji"  -> status.health.emoji,
    "startTime"     -> startTime.map(_.toString).getOrElse(""),
    "endTime"       -> endTime.map(_.toString).getOrElse(""),
    "failureReason" -> failureReason.getOrElse("")
  )
}
