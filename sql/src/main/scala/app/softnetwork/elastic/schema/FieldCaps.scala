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

import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.query.TemporalLiterals
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._

/** SCHEMA-7 (D7) -- the indices a `GET <source>/_field_caps?fields=*` answer describes, each as the
  * document `GET <index>` would answer for it (`{"mappings": {"properties": …}}`), so that a user
  * who may search an index but not read its mapping is typed by the same seam as one who may:
  * `Index(name, document).schema`, then `schema.mergedSchema`.
  *
  * What `_field_caps` holds, MEASURED on Elasticsearch 6.8.23 and 8.18.3:
  *   - `fields`: each field by its full dotted path, each of its Elasticsearch types with the
  *     `indices` mapping it with that type when the indices disagree (`k: {keyword: {…, indices:
  *     [a]}, long: {…, indices: [b]}}`);
  *   - `indices`: every index the source resolved to -- 7.0+; Elasticsearch 6.8 answers none, so
  *     the members are then the indices the conflicts name, else the source itself;
  *   - the metadata fields (`_id`, `_index`, `_source`, `_doc_count`, …): `metadata_field: true`
  *     from 7.x on, a type of their own name on 6.8 -- dropped;
  *   - an object field and its properties (`obj`, `obj.a`), a multi-field under its parent (`k`,
  *     `k.raw`), both by their dotted path: a path is a property of a parent mapped `object` or
  *     `nested`, else a multi-field of it;
  *   - no `_meta`, no option, no date `format`: a `date` / `date_nanos` field is given the ISO
  *     format ([[TemporalLiterals.FieldCapsDateFormat]], A-40), the one spelling read as an instant
  *     without knowing the field's own;
  *   - each field's mapping `meta`, merged per key across the indices that hold it (an array of
  *     values per key; 7.6+): core's copy of its declaration ([[FieldMeta]], SCHEMA-17 A-172) is
  *     turned into the document's `_meta.columns`, exactly for ONE index, list-ness only over
  *     several;
  *   - a `constant_keyword` field as `keyword`, a runtime field as a mapped one: neither can be
  *     told apart (recorded).
  *
  * Pure: no request, no cache.
  */
object FieldCaps {

  private val factory = JsonNodeFactory.instance

  /** One `(index, document)` per index the answer describes, in the order it lists them; empty when
    * it describes none (a pattern matching no index).
    */
  def members(source: String, response: JsonNode): Seq[(String, JsonNode)] = {
    val fields: Seq[(String, Seq[Caps])] =
      Option(response)
        .map(_.path("fields"))
        .filter(_.isObject)
        .toSeq
        .flatMap(_.properties().asScala.toSeq)
        .flatMap { field =>
          val types = field.getValue.properties().asScala.toSeq.flatMap { entry =>
            val caps = entry.getValue
            val tpe = Option(caps.get("type")).map(_.asText()).getOrElse(entry.getKey)
            val metadata =
              Option(caps.get("metadata_field")).exists(_.asBoolean()) || tpe.startsWith("_")
            if (metadata || tpe == "unmapped") None
            else
              Some(
                Caps(
                  tpe,
                  Option(caps.get("indices")).filter(_.isArray).map(indicesOf),
                  FieldMeta.declarations(caps.get("meta"))
                )
              )
          }
          if (types.isEmpty) None else Some(field.getKey -> types)
        }
    val listed: Option[Seq[String]] =
      Option(response)
        .flatMap(r => Option(r.get("indices")))
        .filter(_.isArray)
        .map(indicesOf(_).toSeq)
    val members: Seq[String] = listed.getOrElse {
      val named = fields.flatMap(_._2.flatMap(_.indices.toSeq.flatten)).distinct.sorted
      if (named.nonEmpty) named else if (fields.nonEmpty) Seq(source) else Nil
    }
    // SCHEMA-17 (A-172): core's keys declare a field exactly where ONE index answers; over several,
    // Elasticsearch merges only the keys an index HAS, so a member without them is invisible: list-
    // ness alone applies there (A-179)
    val exact = members.size == 1
    members.map { index =>
      val typed = fields.flatMap { case (path, types) =>
        types
          .find(_.indices.exists(_.contains(index)))
          .orElse(types.find(_.indices.isEmpty))
          .map(path -> _)
      }
      index -> document(typed, exact)
    }
  }

  /** One field's capabilities under one Elasticsearch type: the `indices` mapping it so when the
    * indices disagree, and core's keys of its `meta` ([[FieldMeta.declarations]]).
    */
  private final case class Caps(
    tpe: String,
    indices: Option[Set[String]],
    declarations: ListMap[String, Seq[String]]
  )

  private def indicesOf(node: JsonNode): Set[String] =
    node.elements().asScala.map(_.asText()).toSet

  /** SCHEMA-17 (A-172) -- the `_meta.columns` declarations core's keys make, by path, and the date
    * formats they name, by path.
    *
    *   - `exact` (ONE index): [[FieldMeta.TypeKey]] declares the field, [[FieldMeta.FormatKey]] its
    *     date format (in place of [[TemporalLiterals.FieldCapsDateFormat]]), [[FieldMeta.ListsKey]]
    *     the ancestors it names `ARRAY<STRUCT>`;
    *   - several indices: a [[FieldMeta.TypeKey]] value `array<…>` declares ARRAY of the field's
    *     undeclared reading, [[FieldMeta.ListsKey]] 's depths (every member's) the ancestors; every
    *     other declaration is dropped -- another index's format read into a member that lacks the
    *     key would be silently wrong.
    */
  private def declarationsOf(
    fields: Seq[(String, Caps)],
    exact: Boolean
  ): (ListMap[String, String], Map[String, String]) = {
    val types = scala.collection.mutable.LinkedHashMap.empty[String, String]
    val formats = scala.collection.mutable.Map.empty[String, String]
    fields.foreach { case (path, caps) =>
      val keys = caps.declarations
      def single(key: String): Option[String] = keys.get(key).collect { case Seq(one) => one }
      val segments = path.split("\\.")
      val depths = keys
        .getOrElse(FieldMeta.ListsKey, Nil)
        .flatMap(_.split(",").map(_.trim))
        .flatMap(d => scala.util.Try(d.toInt).toOption)
        .filter(d => d > 0 && d < segments.length)
        .distinct
      depths.foreach { d =>
        types.put(segments.dropRight(d).mkString("."), SQLTypes.Array(SQLTypes.Struct).typeId)
      }
      if (exact) {
        single(FieldMeta.TypeKey).foreach(declared => types.put(path, declared))
        caps.tpe match {
          case "date" | "date_nanos" => single(FieldMeta.FormatKey).foreach(formats.put(path, _))
          case _                     =>
        }
      } else if (
        keys.getOrElse(FieldMeta.TypeKey, Nil).exists(_.toLowerCase.startsWith("array<"))
      ) {
        SQLTypes(IndexField.undeclaredType(caps.tpe)) match {
          case SQLTypes.Any | SQLTypes.Struct | SQLTypes.Array(_) =>
          case undeclared => types.put(path, SQLTypes.Array(undeclared).typeId)
        }
      }
    }
    (ListMap(types.toSeq: _*), formats.toMap)
  }

  private def child(parent: ObjectNode, name: String): ObjectNode =
    parent.get(name) match {
      case existing: ObjectNode => existing
      case _                    => parent.putObject(name)
    }

  /** The `GET <index>` document of one index, from its fields' paths and types, and the
    * declarations core's keys of their `meta` make ([[declarationsOf]]) as its `_meta.columns`: the
    * mapping reader's ONE declaration seam (`IndexField.apply`) then reads them.
    */
  private def document(caps: Seq[(String, Caps)], exact: Boolean): JsonNode = {
    val root = factory.objectNode()
    val mappings = root.putObject("mappings")
    val properties = mappings.putObject("properties")
    val fields: Seq[(String, String)] = caps.map { case (path, c) => path -> c.tpe }
    val typeOf = fields.toMap
    val (declared, formats) = declarationsOf(caps, exact)
    if (declared.nonEmpty) {
      val columns = mappings.putObject("_meta").putObject("columns")
      declared.foreach { case (path, dataType) =>
        val segments = path.split("\\.")
        var container = columns
        segments.init.foreach { segment =>
          container = child(child(container, segment), "multi_fields")
        }
        child(container, segments.last).put("data_type", dataType)
        ()
      }
    }
    // in path order, so that a parent is placed before its properties and multi-fields
    fields.sortBy(_._1).foreach { case (path, tpe) =>
      val segments = path.split("\\.")
      var container = properties
      var prefix = ""
      segments.init.foreach { segment =>
        prefix = if (prefix.isEmpty) segment else s"$prefix.$segment"
        val parent = child(container, segment)
        container = typeOf.getOrElse(prefix, "object") match {
          case "object" | "nested" => child(parent, "properties")
          case _                   => child(parent, "fields")
        }
      }
      val leaf = child(container, segments.last)
      leaf.put("type", tpe)
      tpe match {
        case "date" =>
          leaf.put("format", formats.getOrElse(path, TemporalLiterals.FieldCapsDateFormat))
        case "date_nanos" =>
          leaf.put("format", formats.getOrElse(path, TemporalLiterals.FieldCapsDateNanosFormat))
        case _ => leaf
      }
      ()
    }
    root
  }
}
