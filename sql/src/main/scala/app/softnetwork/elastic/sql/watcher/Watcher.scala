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

import app.softnetwork.elastic.sql.query.Criteria
import app.softnetwork.elastic.sql.schema.mapper
import app.softnetwork.elastic.sql.transform.{Delay, TransformTimeInterval, TransformTimeUnit}
import app.softnetwork.elastic.sql.{DdlToken, ObjectValue, StringValue, Value}
import com.fasterxml.jackson.databind.JsonNode

import scala.collection.immutable.ListMap

/** Watcher definition
  *
  * A watcher monitors data and triggers actions based on conditions.
  *
  * @example
  *   {{{ // Create a watcher that monitors error logs val watcher = Watcher( id = "error_monitor",
  *   trigger = IntervalWatcherTrigger(Delay(TransformTimeUnit.Minutes, 5)), input =
  *   SearchWatcherInput( index = Seq("logs-*"), query = Some(Term("level", StringValue("ERROR")))
  *   ), condition = CompareWatcherCondition( "ctx.payload.hits.total", GT, Left(IntValue(10)) ),
  *   actions = ListMap( "log_alert" -> LoggingAction( LoggingActionConfig("Found
  *   {{ctx.payload.hits.total}} errors") ) ) )
  *
  * // Generate SQL val ddl = watcher.sql
  *
  * // Generate Elasticsearch JSON val json = watcher.node }}}
  * @param id
  *   Watcher ID
  * @param trigger
  *   Watcher trigger configuration
  * @param input
  *   Watcher input configuration
  * @param condition
  *   Watcher condition configuration
  * @param actions
  *   Map of action name to WatcherAction
  * @param throttlePeriod
  *   Optional throttle period to limit action frequency
  * @param metadata
  *   Optional metadata associated with the watcher
  * @param status
  *   Optional status of the watcher
  */
case class Watcher(
  id: String,
  trigger: WatcherTrigger = IntervalWatcherTrigger(Delay(TransformTimeUnit.Minutes, 5)),
  input: WatcherInput = SimpleWatcherInput(ObjectValue(Map.empty)),
  condition: WatcherCondition = AlwaysWatcherCondition,
  actions: ListMap[String, WatcherAction] = ListMap.empty,
  throttlePeriod: Option[TransformTimeInterval] = None,
  metadata: Option[ObjectValue] = None,
  status: Option[WatcherStatus] = None
) extends DdlToken {
  lazy val options: Map[String, Value[_]] = {
    List(
      throttlePeriod.map { tp =>
        "throttle_period" -> StringValue(s"${tp.interval}${tp.timeUnit.format}")
      },
      metadata.map { md =>
        "metadata" -> md
      }
    ).flatten.toMap
  }

  override def sql: String = {
    val optionsClause =
      options match {
        case opts if opts.nonEmpty => s"\n\tWITH ${ObjectValue(opts).ddl}"
        case _                     => ""
      }
    s"CREATE OR REPLACE WATCHER $id AS\n\t${trigger.sql.trim}\n\t${input.ddl}\n\t${condition.ddl}\n\tDO\n" +
    actions
      .map { case (name, action) =>
        s"\t\t$name AS ${action.sql}"
      }
      .mkString(",\n") + "\n\tEND" + optionsClause
  }

  def node(implicit criteriaToNode: Criteria => JsonNode): JsonNode = {
    val node = mapper.createObjectNode()
    node.set("trigger", trigger.node)
    node.set("input", input.node)
    node.set("condition", condition.node)
    val actionsNode = mapper.createObjectNode()
    actions.foreach { case (name, action) =>
      actionsNode.set(name, action.node)
      ()
    }
    node.set("actions", actionsNode)
    throttlePeriod.foreach { tp =>
      val throttleNode = mapper.createObjectNode()
      throttleNode.put("period", s"${tp.interval}${tp.timeUnit.format}")
      node.set("throttle_period", throttleNode)
      ()
    }
    metadata.foreach { md =>
      node.set("metadata", md.toJson)
      ()
    }
    node
  }
}
