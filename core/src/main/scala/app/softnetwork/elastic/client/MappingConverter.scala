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

package app.softnetwork.elastic.client

import app.softnetwork.elastic.sql.serialization.JacksonConfig
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.{Logger, LoggerFactory}

import scala.jdk.CollectionConverters._

object MappingConverter {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private lazy val mapper: ObjectMapper = JacksonConfig.objectMapper

  def convert(mapping: String, elasticVersion: String): String = {
    val root = mapper.readTree(mapping).asInstanceOf[ObjectNode]
    mapper.writeValueAsString(convert(root, elasticVersion))
  }

  def convert(mapping: ObjectNode, elasticVersion: String): ObjectNode = {
    val stripped = withoutCoreFieldMeta(mapping, elasticVersion)
    if (ElasticsearchVersion.requiresDocTypeWrapper(elasticVersion) && !stripped.has("_doc")) {
      wrapDocType(stripped)
    } else if (
      !ElasticsearchVersion.requiresDocTypeWrapper(elasticVersion) && stripped.has("_doc")
    ) {
      unwrapDocType(stripped)
    } else stripped
  }

  /** SCHEMA-17 (A-170) -- `mapping` without core's keys in its fields' `meta` (`FieldMeta.Keys`),
    * and without a `meta` they leave empty, below Elasticsearch 7.6 (`supportsFieldMeta`), which
    * has no `meta` mapping parameter; `mapping` itself from 7.6 on. A user's own `meta` keys still
    * reach Elasticsearch, and fail there as they always did. A COPY is changed: the table's own
    * mapping (`Table.indexMappings`, a lazy value) is never touched.
    */
  def withoutCoreFieldMeta(mapping: ObjectNode, elasticVersion: String): ObjectNode =
    if (ElasticsearchVersion.supportsFieldMeta(elasticVersion)) mapping
    else {
      val copy = mapping.deepCopy()
      def strip(fields: JsonNode): Unit =
        if (fields != null && fields.isObject)
          fields.elements().asScala.foreach { field =>
            field match {
              case node: ObjectNode =>
                node.get("meta") match {
                  case meta: ObjectNode =>
                    app.softnetwork.elastic.schema.FieldMeta.Keys.foreach(key => meta.remove(key))
                    if (meta.size() == 0) node.remove("meta")
                  case _ =>
                }
                strip(node.get("properties"))
                strip(node.get("fields"))
              case _ =>
            }
          }
      strip(copy.get("properties"))
      Option(copy.get("_doc")).foreach(doc => strip(doc.get("properties")))
      copy
    }

  /** [[withoutCoreFieldMeta]] of a mapping as text (`MappingApi.setMapping`). */
  def withoutCoreFieldMeta(mapping: String, elasticVersion: String): String =
    if (ElasticsearchVersion.supportsFieldMeta(elasticVersion)) mapping
    else
      mapper.readTree(mapping) match {
        case root: ObjectNode =>
          mapper.writeValueAsString(withoutCoreFieldMeta(root, elasticVersion))
        case _ => mapping
      }

  private def wrapDocType(mappingsNode: ObjectNode): ObjectNode = {
    if (!mappingsNode.has("_doc")) {
      logger.info(s"Wrapping mappings from '_doc' type")
      val root = mapper.createObjectNode()
      // Wrap into _doc
      root.set("_doc", mappingsNode)
      root
    } else {
      mappingsNode
    }
  }

  private def unwrapDocType(mappingsNode: ObjectNode): ObjectNode = {
    val root = mapper.createObjectNode()

    if (mappingsNode.has("_doc")) {
      logger.info(s"Unwrapping mappings from '_doc' type")
      val doc = mappingsNode.get("_doc").asInstanceOf[ObjectNode]
      doc
        .properties()
        .forEach(entry => {
          root.set(entry.getKey, entry.getValue)
        })
      root
    } else {
      mappingsNode
    }
  }
}
