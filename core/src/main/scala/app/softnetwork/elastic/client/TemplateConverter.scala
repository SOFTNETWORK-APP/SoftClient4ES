package app.softnetwork.elastic.client

import app.softnetwork.elastic.sql.serialization.JacksonConfig
import com.fasterxml.jackson.databind.node.ObjectNode

import scala.util.{Success, Try}

object TemplateConverter {

  private lazy val mapper = JacksonConfig.objectMapper

  /** Detect if a template JSON is in legacy format
    *
    * Legacy format has settings/mappings/aliases at root level Composable format has them nested
    * under 'template' key
    *
    * @param templateJson
    *   the template JSON string
    * @return
    *   true if legacy format
    */
  def isLegacyFormat(templateJson: String): Boolean = {
    Try {
      val root = mapper.readTree(templateJson)

      // Legacy format: has settings/mappings/aliases at root
      val hasRootSettings = root.has("settings")
      val hasRootMappings = root.has("mappings")
      val hasRootAliases = root.has("aliases")

      // Composable format: has 'template' key containing settings/mappings/aliases
      val hasTemplateKey = root.has("template")

      // Legacy if has root-level settings/mappings/aliases AND no 'template' key
      (hasRootSettings || hasRootMappings || hasRootAliases) && !hasTemplateKey

    }.getOrElse(false)
  }

  /** Convert legacy template format to composable template format
    *
    * Legacy format:
    * {{{
    * {
    *   "index_patterns": ["logs-*"],
    *   "order": 1,
    *   "settings": {...},
    *   "mappings": {...},
    *   "aliases": {...}
    * }
    * }}}
    *
    * Composable format:
    * {{{
    * {
    *   "index_patterns": ["logs-*"],
    *   "priority": 1,
    *   "template": {
    *     "settings": {...},
    *     "mappings": {...},
    *     "aliases": {...}
    *   }
    * }
    * }}}
    *
    * @param legacyJson
    *   the legacy template JSON
    * @return
    *   composable template JSON
    */
  def convertLegacyToComposable(legacyJson: String): Try[String] = {
    Try {
      val root = mapper.readTree(legacyJson).asInstanceOf[ObjectNode]
      val composable = mapper.createObjectNode()

      // 1. Copy index_patterns (required)
      if (root.has("index_patterns")) {
        composable.set("index_patterns", root.get("index_patterns"))
      } else if (root.has("template")) {
        // Old legacy format used 'template' instead of 'index_patterns'
        val patterns = mapper.createArrayNode()
        patterns.add(root.get("template").asText())
        composable.set("index_patterns", patterns)
      }

      // 2. Convert 'order' to 'priority'
      if (root.has("order")) {
        composable.put("priority", root.get("order").asInt())
      }

      // 3. Copy version if present
      if (root.has("version")) {
        composable.set("version", root.get("version"))
      }

      // 4. Copy _meta if present
      if (root.has("_meta")) {
        composable.set("_meta", root.get("_meta"))
      }

      // 5. Create nested 'template' object with settings/mappings/aliases
      val templateNode = mapper.createObjectNode()

      if (root.has("settings")) {
        templateNode.set("settings", root.get("settings"))
      }

      if (root.has("mappings")) {
        templateNode.set("mappings", root.get("mappings"))
      }

      if (root.has("aliases")) {
        templateNode.set("aliases", root.get("aliases"))
      }

      // Only add 'template' key if it has content
      if (templateNode.size() > 0) {
        composable.set("template", templateNode)
      }

      mapper.writeValueAsString(composable)
    }
  }

  /** Convert composable template format to legacy template format
    *
    * Used for backward compatibility when ES version < 7.8
    *
    * @param composableJson
    *   the composable template JSON
    * @return
    *   legacy template JSON
    */
  def convertComposableToLegacy(composableJson: String): Try[String] = {
    Try {
      val root = mapper.readTree(composableJson).asInstanceOf[ObjectNode]
      val legacy = mapper.createObjectNode()

      // 1. Copy index_patterns (required)
      if (root.has("index_patterns")) {
        legacy.set("index_patterns", root.get("index_patterns"))
      }

      // 2. Convert 'priority' to 'order'
      if (root.has("priority")) {
        legacy.put("order", root.get("priority").asInt())
      }

      // 3. Copy version if present
      if (root.has("version")) {
        legacy.set("version", root.get("version"))
      }

      // 4. Copy _meta if present
      if (root.has("_meta")) {
        legacy.set("_meta", root.get("_meta"))
      }

      // 5. Flatten 'template' object to root level
      if (root.has("template")) {
        val templateNode = root.get("template")

        if (templateNode.has("settings")) {
          legacy.set("settings", templateNode.get("settings"))
        }

        if (templateNode.has("mappings")) {
          legacy.set("mappings", templateNode.get("mappings"))
        }

        if (templateNode.has("aliases")) {
          legacy.set("aliases", templateNode.get("aliases"))
        }
      }

      // Note: 'composed_of' and 'data_stream' are not supported in legacy format
      // They will be silently ignored

      mapper.writeValueAsString(legacy)
    }
  }

  /** Validate and normalize template JSON based on ES version
    *
    * @param templateJson
    *   the template JSON (legacy or composable format)
    * @param esVersion
    *   the Elasticsearch version
    * @return
    *   normalized template JSON appropriate for the ES version
    */
  def normalizeTemplate(templateJson: String, esVersion: String): Try[String] = {
    val isLegacy = isLegacyFormat(templateJson)
    val supportsComposable = ElasticsearchVersion.supportsComposableTemplates(esVersion)

    (isLegacy, supportsComposable) match {
      case (true, true) =>
        // Legacy format + ES 7.8+ → Convert to composable
        convertLegacyToComposable(templateJson)

      case (false, false) =>
        // Composable format + ES < 7.8 → Convert to legacy
        convertComposableToLegacy(templateJson)

      case (true, false) =>
        // Legacy format + ES < 7.8 → Keep as is
        Success(templateJson)

      case (false, true) =>
        // Composable format + ES 7.8+ → Keep as is
        Success(templateJson)
    }
  }

}
