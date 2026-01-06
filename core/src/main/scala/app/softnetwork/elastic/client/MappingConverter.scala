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
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.{Logger, LoggerFactory}

object MappingConverter {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private lazy val mapper: ObjectMapper = JacksonConfig.objectMapper

  def convert(mapping: String, elasticVersion: String): String = {
    val root = mapper.readTree(mapping).asInstanceOf[ObjectNode]
    mapper.writeValueAsString(convert(root, elasticVersion))
  }

  def convert(mapping: ObjectNode, elasticVersion: String): ObjectNode = {
    if (ElasticsearchVersion.requiresDocTypeWrapper(elasticVersion) && !mapping.has("_doc")) {
      wrapDocType(mapping)
    } else if (
      !ElasticsearchVersion.requiresDocTypeWrapper(elasticVersion) && mapping.has("_doc")
    ) {
      unwrapDocType(mapping)
    } else mapping
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
