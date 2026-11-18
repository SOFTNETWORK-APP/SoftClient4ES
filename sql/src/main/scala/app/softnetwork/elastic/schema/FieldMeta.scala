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

package app.softnetwork.elastic.schema

import app.softnetwork.elastic.sql.{ObjectValue, StringValue, Value}
import app.softnetwork.elastic.sql.query.TemporalLiterals
import app.softnetwork.elastic.sql.schema.Column
import com.fasterxml.jackson.databind.JsonNode

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._

/** Core's declaration of a LEAF field, copied into the field's mapping `meta`: the one place a user
  * who may search an index but not read its mapping is told how the field is declared, since
  * `_field_caps` returns a field's `meta` and nothing of the mapping's `_meta`.
  *
  *   - [[TypeKey]]: the declared SQL type, as `_meta.columns` spells it (`ARRAY<INT>`, `DATE`,
  *     `TIME`);
  *   - [[FormatKey]]: a date field's mapping format, Elasticsearch's default included;
  *   - [[ListsKey]]: for a leaf under ancestors declared `ARRAY<STRUCT>` over an `object` field,
  *     how many levels up each one is (`1`, `1,3`) -- Elasticsearch refuses `meta` on the object
  *     itself.
  *
  * Elasticsearch's limits (mapping parameter `meta`, 7.6+): at most [[MaxEntries]] entries, keys of
  * at most [[MaxKeyLength]] characters, string values of at most [[MaxValueLength]]; never on an
  * `object`, `nested`, `passthrough` or `alias` field; a mapping update REPLACES the field's
  * `meta`. Core's keys go beside the user's own, all or none per field ([[merged]]); a format over
  * the value limit drops only [[FormatKey]]. The read side removes them ([[userOnly]]), so `SHOW
  * CREATE TABLE`, a diff and a column's options never see them.
  */
private[elastic] object FieldMeta {

  val TypeKey: String = "sql_type"
  val FormatKey: String = "sql_format"
  val ListsKey: String = "sql_list_depths"

  /** Core's keys: the read side drops exactly these. */
  val Keys: Set[String] = Set(TypeKey, FormatKey, ListsKey)

  val MaxEntries: Int = 5
  val MaxKeyLength: Int = 20
  val MaxValueLength: Int = 50

  /** Elasticsearch's default format of a `date_nanos` field. */
  private val DefaultDateNanosFormat: String = "strict_date_optional_time_nanos||epoch_millis"

  /** The Elasticsearch types whose mapper takes no `meta`: the object family (`object`, `nested`,
    * `passthrough`) and a field alias, whose parser takes `path` only.
    */
  private val NoMeta: Set[String] = Set("object", "nested", "passthrough", "alias")

  /** Whether a field mapped `mappingType` takes a `meta`. */
  def takesMeta(mappingType: String): Boolean = !NoMeta.contains(mappingType)

  /** Core's keys for `column`, a leaf whose ancestors declared `ARRAY<STRUCT>` over `object` are
    * `listDepths` levels up; empty for a field that takes no `meta`. A format over the value limit
    * is left out, alone.
    */
  def of(column: Column, listDepths: Seq[Int]): ListMap[String, String] =
    if (!takesMeta(column.mappingType)) ListMap.empty
    else {
      val format: Option[String] = column.mappingType match {
        case "date" | "date_nanos" =>
          Some(
            column.options
              .get("format")
              .map(_.value)
              .collect { case spec: String => spec }
              .getOrElse(
                if (column.dateNanos) DefaultDateNanosFormat
                else TemporalLiterals.DefaultDateFormat
              )
          )
        case _ => None
      }
      ListMap(TypeKey                                         -> column.dataType.typeId) ++
      format.filter(_.length <= MaxValueLength).map(FormatKey -> _) ++
      (if (listDepths.nonEmpty) ListMap(ListsKey -> listDepths.mkString(","))
       else ListMap.empty[String, String])
    }

  /** The field's `meta`: the user's own keys beside core's, when together they stay within
    * Elasticsearch's limits; `None` when they would not (the caller then writes the user's keys
    * alone, as main did). A user key spelled like one of core's is core's.
    */
  def merged(
    user: ListMap[String, Value[_]],
    core: ListMap[String, String]
  ): Option[ListMap[String, Value[_]]] = {
    val all: ListMap[String, Value[_]] =
      (user -- Keys) ++ core.map { case (k, v) => k -> (StringValue(v): Value[_]) }
    val withinLimits =
      all.size <= MaxEntries && core.forall { case (k, v) =>
        k.length <= MaxKeyLength && v.length <= MaxValueLength
      }
    if (withinLimits) Some(all) else None
  }

  /** The read side: a field's `meta` option without core's keys; `None` when only core's were
    * there.
    */
  def userOnly(meta: ObjectValue): Option[ObjectValue] = {
    val kept = meta.value -- Keys
    if (kept.isEmpty) None else Some(ObjectValue(kept))
  }

  /** The `_field_caps` side: core's keys of a field's `meta` with their values -- an array per key
    * (Elasticsearch merges each key across the indices that hold it), or one string.
    */
  def declarations(meta: JsonNode): ListMap[String, Seq[String]] =
    Option(meta).filter(_.isObject) match {
      case Some(node) =>
        ListMap(node.properties().asScala.toSeq.collect {
          case entry if Keys.contains(entry.getKey) =>
            val value = entry.getValue
            val values =
              if (value.isArray) value.elements().asScala.map(_.asText()).toSeq
              else Seq(value.asText())
            entry.getKey -> values.distinct
        }: _*)
      case None => ListMap.empty
    }
}
