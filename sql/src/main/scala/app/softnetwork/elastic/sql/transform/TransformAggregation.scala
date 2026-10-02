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

import app.softnetwork.elastic.sql.query.{Desc, FieldSort}
import app.softnetwork.elastic.sql.schema.{mapper, sqlConfig}
import app.softnetwork.elastic.sql.{DdlToken, Identifier}
import com.fasterxml.jackson.databind.JsonNode

import scala.collection.immutable.ListMap

sealed trait TransformAggregation extends DdlToken {
  def name: String

  def field: String

  override def sql: String = s"${name.toUpperCase}($field)"

  def node: JsonNode = {
    val node = mapper.createObjectNode()
    val fieldNode = mapper.createObjectNode()
    fieldNode.put("field", field)
    node.set(name.toLowerCase(), fieldNode)
    node
  }
}

case class MaxTransformAggregation(field: String) extends TransformAggregation {
  override def name: String = "max"
}

case class MinTransformAggregation(field: String) extends TransformAggregation {
  override def name: String = "min"
}

case class SumTransformAggregation(field: String) extends TransformAggregation {
  override def name: String = "sum"
}

case class AvgTransformAggregation(field: String) extends TransformAggregation {
  override def name: String = "avg"
}

case class CountTransformAggregation(field: String) extends TransformAggregation {
  override def name: String = "value_count"

  override def sql: String =
    if (field == CountAllDocumentsField) "COUNT(*)" else s"COUNT($field)"
}

case class CardinalityTransformAggregation(field: String) extends TransformAggregation {
  override def name: String = "cardinality"

  override def sql: String = s"COUNT(DISTINCT $field)"
}

/** The per-group calculation of a SELECT item that is arithmetic over aggregates (`MAX(a) - MIN(b)
  * AS d`), as a `bucket_script` in a materialized view's pivot -- the same calculation a search
  * runs for it (`SingleSearch.transformBucketScripts`).
  *
  * It reads its operands from sibling aggregations of the pivot BY NAME: `bucketsPath` maps each
  * parameter the script reads (`params.<key>`) to the name of the pivot aggregation that computes
  * it. The aggregation itself is named after the SELECT alias, so a view's HAVING over `d` reads it
  * like any other metric.
  *
  * @param expression
  *   the SQL expression it computes, for display
  * @param params
  *   the script parameters the script reads that are NOT bucket paths, in the order it first reads
  *   them -- `__now__`, the request clock `CURRENT_DATE` / `NOW()` render against. The pivot must
  *   bind each one ([[node]] carries none): every other parameter the script reads is a key of
  *   `bucketsPath`.
  */
case class BucketScriptTransformAggregation(
  expression: String,
  bucketsPath: ListMap[String, String],
  script: String,
  params: Seq[String]
) extends TransformAggregation {
  override def name: String = "bucket_script"

  /** The pivot aggregations it reads. */
  override def field: String = bucketsPath.values.mkString(", ")

  override def sql: String = expression

  override def node: JsonNode = {
    val node = mapper.createObjectNode()
    val bucketScriptNode = mapper.createObjectNode()
    val bucketsPathNode = mapper.createObjectNode()
    bucketsPath.foreach { case (param, aggregation) =>
      bucketsPathNode.put(param, aggregation)
      ()
    }
    bucketScriptNode.set("buckets_path", bucketsPathNode)
    bucketScriptNode.put("script", script)
    node.set(name, bucketScriptNode)
    node
  }
}

case class TopHitsTransformAggregation(
  fields: Seq[String],
  size: Int = 1,
  sort: Seq[FieldSort] = Seq(
    FieldSort(Identifier(sqlConfig.transformLastUpdatedColumnName), Some(Desc))
  )
) extends TransformAggregation {
  override def field: String = fields.mkString(", ")

  override def name: String = "top_hits"

  override def sql: String = {
    val fieldsStr = fields.mkString(", ")
    val sortStr = sort
      .map { s =>
        s.sql
      }
      .mkString(", ")
    s"TOP_HITS($fieldsStr) SIZE $size SORT BY ($sortStr)"
  }

  override def node: JsonNode = {
    val node = mapper.createObjectNode()
    val topHitsNode = mapper.createObjectNode()
    topHitsNode.put("size", size)

    // Sort configuration
    val sortArray = mapper.createArrayNode()
    sort.foreach { sortField =>
      val sortObj = mapper.createObjectNode()
      val sortFieldObj = mapper.createObjectNode()
      sortFieldObj.put("order", sortField.order.getOrElse(Desc).sql)
      sortObj.set(sortField.name, sortFieldObj)
      sortArray.add(sortObj)
    }
    topHitsNode.set("sort", sortArray)

    // Source filtering to include only the desired field
    val sourceObj = mapper.createObjectNode()
    val includesArray = mapper.createArrayNode()
    fields.foreach { field =>
      includesArray.add(field)
      ()
    }
    sourceObj.set("includes", includesArray)
    topHitsNode.set("_source", sourceObj)

    node.set("top_hits", topHitsNode)
    node
  }
}
