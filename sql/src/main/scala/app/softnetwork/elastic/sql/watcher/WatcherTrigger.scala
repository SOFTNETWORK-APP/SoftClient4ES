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
import app.softnetwork.elastic.sql.schema.mapper
import app.softnetwork.elastic.sql.transform.{Delay, TransformTimeInterval, TransformTimeUnit}
import app.softnetwork.elastic.utils.CronIntervalCalculator
import com.fasterxml.jackson.databind.JsonNode

import scala.util.Try

sealed trait WatcherTrigger extends DdlToken {
  def interval: TransformTimeInterval
  def node: JsonNode
}

/** Cron-based trigger
  */
class CronWatcherTrigger private (
  val cron: String,
  private val intervalSeconds: Long
) extends WatcherTrigger {

  override def sql: String = s" AT SCHEDULE '$cron'"

  override val interval: TransformTimeInterval =
    Delay(TransformTimeUnit.Seconds, intervalSeconds.toInt)

  override def node: JsonNode = {
    val node = mapper.createObjectNode()
    val cronNode = mapper.createObjectNode()
    cronNode.put("cron", cron)
    node.set("schedule", cronNode)
    node
  }
}

object CronWatcherTrigger {

  /** Safe constructor that validates the cron expression */
  def apply(cron: String): CronWatcherTrigger = {
    CronIntervalCalculator.validateAndCalculate(cron) match {
      case Right((_, interval)) =>
        new CronWatcherTrigger(cron, interval)
      case Left(error) =>
        throw new IllegalArgumentException(
          s"Invalid cron expression '$cron': $error"
        )
    }
  }

  /** Try-based constructor for safer error handling */
  def safe(cron: String): Try[CronWatcherTrigger] = Try(apply(cron))
}

/* Interval-based trigger
 *
 * @param interval
 *   Time interval
 */
case class IntervalWatcherTrigger(interval: TransformTimeInterval) extends WatcherTrigger {
  override def sql: String = s" EVERY ${interval.interval} ${interval.timeUnit}"

  override def node: JsonNode = {
    val node = mapper.createObjectNode()
    val intervalNode = mapper.createObjectNode()
    intervalNode.put("interval", s"${interval.interval}${interval.timeUnit.format}")
    node.set("schedule", intervalNode)
    node
  }
}
