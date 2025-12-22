package app.softnetwork.elastic.client

import app.softnetwork.elastic.sql.serialization.JacksonConfig
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Success, Try}

object TemplateConverter {

  private val logger: Logger = LoggerFactory.getLogger(getClass)

  private lazy val mapper: ObjectMapper = JacksonConfig.objectMapper

  /** Normalize template format based on Elasticsearch version
    *
    * @param templateJson
    *   the template JSON (legacy or composable format)
    * @param elasticVersion
    *   the Elasticsearch version (e.g., "7.10.0")
    * @return
    *   normalized template JSON
    */
  def normalizeTemplate(templateJson: String, elasticVersion: String): Try[String] = {
    val supportsComposable = ElasticsearchVersion.supportsComposableTemplates(elasticVersion)
    val isLegacy = isLegacyFormat(templateJson)

    (supportsComposable, isLegacy) match {
      case (true, true) =>
        // ES 7.8+ with legacy format → convert to composable
        logger.debug("Converting legacy template to composable format")
        convertLegacyToComposable(templateJson)

      case (false, _) =>
        // ES < 7.8 with composable format → convert to legacy
        // ES < 6.8 with legacy format → ensure _doc wrapper in mappings
        logger.debug(
          "Converting composable template to legacy format for ES < 7.8 / Ensuring _doc wrapper in legacy template mappings for ES < 6.8"
        )
        convertComposableToLegacy(templateJson, elasticVersion)

      case _ =>
        // Already in correct format
        logger.debug("Template already in correct format")
        Success(templateJson)
    }
  }

  /** Check if template is in legacy format
    *
    * Legacy format has 'order' field, composable has 'priority'
    *
    * @param templateJson
    *   the template JSON
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
      } else {
        composable.put("priority", 1) // Default priority
      }

      // 3. Copy version if present
      if (root.has("version")) {
        composable.set("version", root.get("version"))
      }

      // 4. Wrap settings, mappings, aliases in 'template' object
      val templateNode = mapper.createObjectNode()

      if (root.has("settings")) {
        templateNode.set("settings", root.get("settings"))
      }

      if (root.has("mappings")) {
        // Remove _doc wrapper if present (legacy ES 6.x format)
        val mappingsNode = root.get("mappings")
        if (mappingsNode.isObject && mappingsNode.has("_doc")) {
          templateNode.set("mappings", mappingsNode.get("_doc"))
        } else {
          templateNode.set("mappings", mappingsNode)
        }
      }

      if (root.has("aliases")) {
        templateNode.set("aliases", root.get("aliases"))
      }

      composable.set("template", templateNode)

      // 5. Copy _meta if present (valid in composable format)
      if (root.has("_meta")) {
        composable.set("_meta", root.get("_meta"))
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
    * @param elasticVersion
    *   the Elasticsearch version (e.g., "6.8.0")
    * @return
    *   legacy template JSON
    */
  def convertComposableToLegacy(
    composableJson: String,
    elasticVersion: String
  ): Try[String] = {
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
      } else if (root.has("order")) {
        // In case 'order' is already present
        legacy.put("order", root.get("order").asInt())
      } else {
        legacy.put("order", 1) // Default order
      }

      // 3. Copy version if present
      if (root.has("version")) {
        legacy.set("version", root.get("version"))
      }

      // 4. ❌ DO NOT copy _meta (not supported in legacy format)
      // Legacy templates don't support _meta field

      // 5. Flatten 'template' object to root level
      if (root.has("template")) {
        val templateNode = root.get("template")

        if (templateNode.has("settings")) {
          legacy.set("settings", templateNode.get("settings"))
        }

        if (templateNode.has("mappings")) {
          val mappingsNode = templateNode.get("mappings")

          // Check if we need to wrap in _doc for ES 6.x
          if (
            ElasticsearchVersion.requiresDocTypeWrapper(elasticVersion) && !mappingsNode.has("_doc")
          ) {
            val wrappedMappings = mapper.createObjectNode()
            wrappedMappings.set("_doc", mappingsNode)
            legacy.set("mappings", wrappedMappings)
            logger.debug(s"Wrapped mappings in '_doc' for ES $elasticVersion")
          } else if (
            !ElasticsearchVersion.requiresDocTypeWrapper(elasticVersion) && mappingsNode.has("_doc")
          ) {
            legacy.set("mappings", mappingsNode.get("_doc"))
          } else {
            legacy.set("mappings", mappingsNode)
          }
        }

        if (templateNode.has("aliases")) {
          legacy.set("aliases", templateNode.get("aliases"))
        }
      }

      if (root.has("settings")) {
        legacy.set("settings", root.get("settings"))
      }

      if (root.has("mappings")) {
        val mappingsNode = root.get("mappings")

        // Check if we need to wrap in _doc for ES 6.x
        if (
          ElasticsearchVersion.requiresDocTypeWrapper(elasticVersion) && !mappingsNode.has("_doc")
        ) {
          val wrappedMappings = mapper.createObjectNode()
          wrappedMappings.set("_doc", mappingsNode)
          legacy.set("mappings", wrappedMappings)
          logger.debug(s"Wrapped mappings in '_doc' for ES $elasticVersion")
        } else if (
          !ElasticsearchVersion.requiresDocTypeWrapper(elasticVersion) && mappingsNode.has("_doc")
        ) {
          legacy.set("mappings", mappingsNode.get("_doc"))
        } else {
          legacy.set("mappings", mappingsNode)
        }
      }

      if (root.has("aliases")) {
        legacy.set("aliases", root.get("aliases"))
      }

      // Note: 'composed_of' and 'data_stream' are not supported in legacy format
      // They will be silently ignored
      if (root.has("composed_of")) {
        logger.warn(
          "Composable templates are not supported in legacy template format and will be ignored"
        )
      }
      if (root.has("data_stream")) {
        logger.warn("Data streams are not supported in legacy template format and will be ignored")
      }

      mapper.writeValueAsString(legacy)
    }
  }

  /** Validate that a template has required fields
    *
    * @param templateJson
    *   the template JSON
    * @return
    *   None if valid, Some(error message) if invalid
    */
  def validateTemplate(templateJson: String): Option[String] = {
    Try {
      val root = mapper.readTree(templateJson)

      // Check for index_patterns (required in both formats)
      if (!root.has("index_patterns")) {
        return Some("Missing required field: index_patterns")
      }

      val indexPatterns = root.get("index_patterns")
      if (!indexPatterns.isArray || indexPatterns.size() == 0) {
        return Some("index_patterns must be a non-empty array")
      }

      // Check for either priority (composable) or order (legacy)
      if (!root.has("priority") && !root.has("order")) {
        logger.warn("Template missing both 'priority' and 'order', will use default")
      }

      None
    }.recover { case e: Exception =>
      Some(s"Invalid JSON: ${e.getMessage}")
    }.get
  }

}
