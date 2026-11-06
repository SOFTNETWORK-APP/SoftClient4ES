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

import app.softnetwork.elastic.sql.query.TemporalLiterals
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.{JsonNodeFactory, ObjectNode}

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
    val fields: Seq[(String, Seq[(String, Option[Set[String]])])] =
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
              Some(tpe -> Option(caps.get("indices")).filter(_.isArray).map(indicesOf))
          }
          if (types.isEmpty) None else Some(field.getKey -> types)
        }
    val listed: Option[Seq[String]] =
      Option(response)
        .flatMap(r => Option(r.get("indices")))
        .filter(_.isArray)
        .map(indicesOf(_).toSeq)
    val members: Seq[String] = listed.getOrElse {
      val named = fields.flatMap(_._2.flatMap(_._2.toSeq.flatten)).distinct.sorted
      if (named.nonEmpty) named else if (fields.nonEmpty) Seq(source) else Nil
    }
    members.map { index =>
      val typed = fields.flatMap { case (path, types) =>
        types
          .find(_._2.exists(_.contains(index)))
          .orElse(types.find(_._2.isEmpty))
          .map(path -> _._1)
      }
      index -> document(typed)
    }
  }

  private def indicesOf(node: JsonNode): Set[String] =
    node.elements().asScala.map(_.asText()).toSet

  private def child(parent: ObjectNode, name: String): ObjectNode =
    parent.get(name) match {
      case existing: ObjectNode => existing
      case _                    => parent.putObject(name)
    }

  /** The `GET <index>` document of one index, from its fields' paths and types. */
  private def document(fields: Seq[(String, String)]): JsonNode = {
    val root = factory.objectNode()
    val properties = root.putObject("mappings").putObject("properties")
    val typeOf = fields.toMap
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
        case "date"       => leaf.put("format", TemporalLiterals.FieldCapsDateFormat)
        case "date_nanos" => leaf.put("format", TemporalLiterals.FieldCapsDateNanosFormat)
        case _            => leaf
      }
      ()
    }
    root
  }
}
