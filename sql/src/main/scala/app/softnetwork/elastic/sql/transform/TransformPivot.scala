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

package app.softnetwork.elastic.sql.transform

import app.softnetwork.elastic.sql.DdlToken
import app.softnetwork.elastic.sql.schema.mapper
import com.fasterxml.jackson.databind.JsonNode

case class TransformPivot(
  groupBy: Map[String, TransformGroupBy],
  aggregations: Map[String, TransformAggregation],
  bucketSelector: Option[TransformBucketSelectorConfig] = None,
  script: Option[String] = None
) extends DdlToken {
  override def sql: String = {
    val groupByStr = groupBy
      .map { case (name, gb) =>
        s"$name BY ${gb.sql}"
      }
      .mkString(", ")

    val aggStr = aggregations
      .map { case (name, agg) =>
        s"$name AS ${agg.sql}"
      }
      .mkString(", ")

    val havingStr = bucketSelector.map(bs => s" HAVING ${bs.script}").getOrElse("")

    s"PIVOT ($groupByStr) AGGREGATE ($aggStr)$havingStr"
  }

  /** Converts to JSON for Elasticsearch
    */
  def node: JsonNode = {
    val node = mapper.createObjectNode()

    val groupByNode = mapper.createObjectNode()
    groupBy.foreach { case (name, gb) =>
      groupByNode.set(name, gb.node)
      ()
    }
    node.set("group_by", groupByNode)

    val aggsNode = mapper.createObjectNode()
    aggregations.foreach { case (name, agg) =>
      aggsNode.set(name, agg.node)
      ()
    }
    bucketSelector.foreach { bs =>
      aggsNode.set(bs.name, bs.node)
      ()
    }
    node.set("aggregations", aggsNode)

    node
  }
}
