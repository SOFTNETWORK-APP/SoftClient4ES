# TEMPLATE API

## Overview

The **TemplateApi** trait provides comprehensive functionality to manage Elasticsearch index templates. It supports both **legacy templates** (ES 6.x-7.x) and **composable (index) templates** (ES 7.8+), with automatic version detection and conversion capabilities.

Index templates define settings, mappings, and aliases that are automatically applied to new indices matching specified patterns.

**Dependencies:** Extends `ElasticClientHelpers` for validation and logging utilities.

---

## Public Methods

### createTemplate

Creates or updates an index template with the specified configuration.

**Signature:**

```scala
def createTemplate(
  templateName: String,
  templateDefinition: String
): ElasticResult[Boolean]
```

**Parameters:**
- `templateName` - The name of the template to create or update
- `templateDefinition` - JSON string containing the template configuration

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if template was created/updated successfully
- `ElasticFailure` with error details if operation fails

**Behavior:**
- Validates template name before execution (returns 400 error if invalid)
- Automatically detects template type (composable vs legacy) based on JSON structure
- Converts between formats if necessary based on Elasticsearch version
- For ES < 6.8: Uses legacy template format with `_doc` wrapper in mappings
- For ES 7.8+: Supports both legacy and composable templates
- Logs success with ✅ or failure with ❌

**Examples:**

```scala
// Legacy template (ES 6.x-7.x)
val legacyTemplate =
  """
    |{
    |  "index_patterns": ["logs-*"],
    |  "order": 1,
    |  "version": 1,
    |  "settings": {
    |    "number_of_shards": 2,
    |    "number_of_replicas": 1
    |  },
    |  "mappings": {
    |    "_doc": {
    |      "properties": {
    |        "timestamp": {"type": "date"},
    |        "message": {"type": "text"}
    |      }
    |    }
    |  },
    |  "aliases": {
    |    "logs-current": {}
    |  }
    |}
    |""".stripMargin

client.createTemplate("logs-template", legacyTemplate) match {
  case ElasticSuccess(true) => println("Template created")
  case ElasticFailure(e) => println(s"Error: ${e.message}")
}

// Composable template (ES 7.8+)
val composableTemplate =
  """
    |{
    |  "index_patterns": ["metrics-*"],
    |  "priority": 200,
    |  "version": 2,
    |  "template": {
    |    "settings": {
    |      "number_of_shards": 1,
    |      "refresh_interval": "30s"
    |    },
    |    "mappings": {
    |      "properties": {
    |        "cpu_usage": {"type": "float"},
    |        "timestamp": {"type": "date"}
    |      }
    |    },
    |    "aliases": {
    |      "metrics-latest": {
    |        "is_write_index": true
    |      }
    |    }
    |  },
    |  "composed_of": ["component-template-1"],
    |  "_meta": {
    |    "description": "Metrics template"
    |  }
    |}
    |""".stripMargin

client.createTemplate("metrics-template", composableTemplate)

// Update existing template
val updatedTemplate = legacyTemplate.replace("\"order\": 1", "\"order\": 10")
client.createTemplate("logs-template", updatedTemplate) // Overwrites existing

// Monadic chaining
for {
  _ <- client.createTemplate("users-template", userTemplate)
  _ <- client.createIndex("users-2024")
  result <- client.search("users-2024", query)
} yield result
```

**Template Type Detection:**

The API automatically detects template type based on JSON structure:

| Field Present                 | Template Type  | Supported Versions  |
|-------------------------------|----------------|---------------------|
| `template` (object)           | Composable     | ES 7.8+             |
| `index_patterns` (root level) | Legacy         | ES 6.x-7.x          |
| `mappings` (root level)       | Legacy         | ES 6.x-7.x          |

---

### getTemplate

Retrieves the configuration of one or more index templates.

**Signature:**

```scala
def getTemplate(templateName: String): ElasticResult[Option[String]]
```

**Parameters:**
- `templateName` - The name of the template to retrieve (supports wildcards)

**Returns:**
- `ElasticSuccess[Some(String)]` with JSON template configuration if found
- `ElasticSuccess[None]` if template does not exist
- `ElasticFailure` with error details if operation fails

**Behavior:**
- Validates template name before execution
- Returns normalized JSON format regardless of template type
- For composable templates: Returns full template structure
- For legacy templates: Returns converted format with all fields
- Supports wildcard patterns (e.g., `logs-*`)
- Logs success with ✅ or failure with ❌

**Examples:**

```scala
// Retrieve single template
client.getTemplate("logs-template") match {
  case ElasticSuccess(Some(json)) => 
    println(s"Template found: $json")
  case ElasticSuccess(None) => 
    println("Template not found")
  case ElasticFailure(e) => 
    println(s"Error: ${e.message}")
}

// Check if template exists
def templateExists(name: String): Boolean = {
  client.getTemplate(name) match {
    case ElasticSuccess(Some(_)) => true
    case _ => false
  }
}

// Retrieve with wildcard
client.getTemplate("logs-*") match {
  case ElasticSuccess(Some(json)) =>
    // Returns all matching templates
    println(s"Found templates: $json")
  case _ => println("No matching templates")
}

// Parse template configuration
import com.fasterxml.jackson.databind.ObjectMapper

client.getTemplate("users-template") match {
  case ElasticSuccess(Some(json)) =>
    val mapper = new ObjectMapper()
    val root = mapper.readTree(json)
    
    val patterns = root.get("index_patterns")
    val order = root.get("order").asInt()
    
    println(s"Patterns: $patterns, Order: $order")
    
  case _ => println("Template not found")
}

// Conditional operations
for {
  existing <- client.getTemplate("my-template")
  result <- existing match {
    case Some(_) => 
      client.deleteTemplate("my-template", ifExists = true)
    case None => 
      ElasticResult.success(true)
  }
  _ <- client.createTemplate("my-template", newTemplate)
} yield result
```

---

### deleteTemplate

Deletes one or more index templates.

**Signature:**

```scala
def deleteTemplate(
  templateName: String,
  ifExists: Boolean = false
): ElasticResult[Boolean]
```

**Parameters:**
- `templateName` - The name of the template to delete (supports wildcards)
- `ifExists` - If `true`, returns success even if template doesn't exist (default: `false`)

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if template was deleted, `false` if it didn't exist (when `ifExists = true`)
- `ElasticFailure` with error details if operation fails

**Behavior:**
- Validates template name before execution
- When `ifExists = true`: Checks existence before deletion, returns success if not found
- When `ifExists = false`: Returns failure if template doesn't exist
- Supports wildcard patterns for bulk deletion
- Logs success with ✅ or failure with ❌

**Examples:**

```scala
// Delete single template
client.deleteTemplate("logs-template") match {
  case ElasticSuccess(true) => println("Template deleted")
  case ElasticFailure(e) => println(s"Error: ${e.message}")
}

// Safe deletion (no error if missing)
client.deleteTemplate("old-template", ifExists = true)

// Delete multiple templates with wildcard
client.deleteTemplate("test-*", ifExists = true)

// Cleanup after tests
def cleanupTemplates(names: List[String]): Unit = {
  names.foreach { name =>
    client.deleteTemplate(name, ifExists = true)
  }
}

// Conditional deletion
client.getTemplate("temporary-template") match {
  case ElasticSuccess(Some(_)) =>
    client.deleteTemplate("temporary-template")
  case _ => 
    ElasticResult.success(true)
}

// Monadic cleanup
for {
  _ <- client.deleteTemplate("old-users-template", ifExists = true)
  _ <- client.createTemplate("users-template", newTemplate)
  _ <- client.createIndex("users-2024")
} yield ()
```

---

### templateExists

Checks if an index template exists.

**Signature:**

```scala
def templateExists(templateName: String): ElasticResult[Boolean]
```

**Parameters:**
- `templateName` - The name of the template to check

**Returns:**
- `ElasticSuccess[Boolean]` with `true` if template exists, `false` otherwise
- `ElasticFailure` with error details if operation fails

**Behavior:**
- Validates template name before execution
- Efficiently checks existence without retrieving full template
- Works with both legacy and composable templates
- Logs debug information

**Examples:**

```scala
// Simple existence check
client.templateExists("logs-template") match {
  case ElasticSuccess(true) => println("Template exists")
  case ElasticSuccess(false) => println("Template not found")
  case ElasticFailure(e) => println(s"Error: ${e.message}")
}

// Conditional creation
def createTemplateIfMissing(name: String, definition: String): ElasticResult[Boolean] = {
  client.templateExists(name).flatMap {
    case true => 
      println(s"Template '$name' already exists")
      ElasticResult.success(false)
    case false => 
      client.createTemplate(name, definition)
  }
}

// Guard clause pattern
for {
  exists <- client.templateExists("users-template")
  _ <- if (!exists) {
    client.createTemplate("users-template", userTemplate)
  } else {
    ElasticResult.success(true)
  }
  result <- client.createIndex("users-2024")
} yield result

// Bulk existence check
val templates = List("logs", "metrics", "traces")
val existenceMap = templates.map { name =>
  name -> client.templateExists(name)
}.toMap
```

---

## Template Types

### Legacy Templates (ES 6.x - 7.x)

Legacy templates apply to indices matching specified patterns.

**Structure:**

```
{
  "index_patterns": ["pattern-*"],
  "order": 0,
  "version": 1,
  "settings": { ... },
  "mappings": { ... },
  "aliases": { ... }
}
```

**Key Fields:**

| Field            | Type    | Description                                          |
|------------------|---------|------------------------------------------------------|
| `index_patterns` | Array   | Index patterns to match (e.g., `["logs-*"]`)         |
| `order`          | Integer | Priority when multiple templates match (higher wins) |
| `version`        | Integer | Optional version number for tracking                 |
| `settings`       | Object  | Index settings (shards, replicas, etc.)              |
| `mappings`       | Object  | Field mappings and types                             |
| `aliases`        | Object  | Index aliases to create                              |

**Example:**

```scala
val legacyTemplate =
  """
    |{
    |  "index_patterns": ["app-logs-*"],
    |  "order": 10,
    |  "version": 3,
    |  "settings": {
    |    "number_of_shards": 3,
    |    "number_of_replicas": 2,
    |    "refresh_interval": "5s"
    |  },
    |  "mappings": {
    |    "_doc": {
    |      "properties": {
    |        "timestamp": {
    |          "type": "date",
    |          "format": "strict_date_optional_time||epoch_millis"
    |        },
    |        "level": {
    |          "type": "keyword"
    |        },
    |        "message": {
    |          "type": "text",
    |          "fields": {
    |            "keyword": {
    |              "type": "keyword",
    |              "ignore_above": 256
    |            }
    |          }
    |        },
    |        "user_id": {
    |          "type": "keyword"
    |        }
    |      }
    |    }
    |  },
    |  "aliases": {
    |    "app-logs-current": {},
    |    "app-logs-errors": {
    |      "filter": {
    |        "term": {
    |          "level": "ERROR"
    |        }
    |      }
    |    }
    |  }
    |}
    |""".stripMargin

client.createTemplate("app-logs-template", legacyTemplate)
```

---

### Composable Templates (ES 7.8+)

Composable templates provide more flexibility with component templates and better composition.

**Structure:**

```
{
  "index_patterns": ["pattern-*"],
  "priority": 100,
  "version": 1,
  "template": {
    "settings": { ... },
    "mappings": { ... },
    "aliases": { ... }
  },
  "composed_of": ["component1", "component2"],
  "_meta": { ... }
}
```

**Key Fields:**

| Field            | Type    | Description                          |
|------------------|---------|--------------------------------------|
| `index_patterns` | Array   | Index patterns to match              |
| `priority`       | Integer | Template priority (higher wins)      |
| `version`        | Integer | Optional version number              |
| `template`       | Object  | Contains settings, mappings, aliases |
| `composed_of`    | Array   | Component templates to include       |
| `_meta`          | Object  | Optional metadata                    |

**Example:**

```scala
val composableTemplate =
  """
    |{
    |  "index_patterns": ["metrics-*"],
    |  "priority": 500,
    |  "version": 2,
    |  "template": {
    |    "settings": {
    |      "number_of_shards": 1,
    |      "number_of_replicas": 1,
    |      "refresh_interval": "30s",
    |      "codec": "best_compression"
    |    },
    |    "mappings": {
    |      "properties": {
    |        "timestamp": {
    |          "type": "date"
    |        },
    |        "cpu_percent": {
    |          "type": "float"
    |        },
    |        "memory_used": {
    |          "type": "long"
    |        },
    |        "host": {
    |          "type": "keyword"
    |        }
    |      }
    |    },
    |    "aliases": {
    |      "metrics-latest": {
    |        "is_write_index": true
    |      },
    |      "metrics-high-cpu": {
    |        "filter": {
    |          "range": {
    |            "cpu_percent": {
    |              "gte": 80
    |            }
    |          }
    |        }
    |      }
    |    }
    |  },
    |  "composed_of": [
    |    "metrics-component-settings",
    |    "metrics-component-mappings"
    |  ],
    |  "_meta": {
    |    "description": "Template for system metrics",
    |    "managed_by": "monitoring-system",
    |    "version": "2.0"
    |  }
    |}
    |""".stripMargin

client.createTemplate("metrics-template", composableTemplate)
```

---

## Template Priority and Matching

When multiple templates match an index pattern, priority determines which template applies:

### Legacy Templates (`order`)

```scala
// Lower order
val template1 =
  """
    |{
    |  "index_patterns": ["logs-*"],
    |  "order": 1,
    |  "settings": {
    |    "number_of_shards": 1
    |  }
    |}
    |""".stripMargin

// Higher order wins
val template2 =
  """
    |{
    |  "index_patterns": ["logs-*"],
    |  "order": 10,
    |  "settings": {
    |    "number_of_shards": 3
    |  }
    |}
    |""".stripMargin

client.createTemplate("logs-default", template1)
client.createTemplate("logs-production", template2)

// New index "logs-2024" will have 3 shards (template2 wins)
```

### Composable Templates (`priority`)

```scala
// Lower priority
val template1 =
  """
    |{
    |  "index_patterns": ["app-*"],
    |  "priority": 100,
    |  "template": {
    |    "settings": {
    |      "number_of_replicas": 1
    |    }
    |  }
    |}
    |""".stripMargin

// Higher priority wins
val template2 =
  """
    |{
    |  "index_patterns": ["app-*"],
    |  "priority": 200,
    |  "template": {
    |    "settings": {
    |      "number_of_replicas": 2
    |    }
    |  }
    |}
    |""".stripMargin

client.createTemplate("app-default", template1)
client.createTemplate("app-production", template2)

// New index "app-users" will have 2 replicas (template2 wins)
```

---

## Common Use Cases

### 1. Time-Series Data

```scala
val timeSeriesTemplate =
  """
    |{
    |  "index_patterns": ["logs-*"],
    |  "order": 1,
    |  "settings": {
    |    "number_of_shards": 3,
    |    "number_of_replicas": 1,
    |    "refresh_interval": "5s",
    |    "index.lifecycle.name": "logs-policy",
    |    "index.lifecycle.rollover_alias": "logs-current"
    |  },
    |  "mappings": {
    |    "_doc": {
    |      "properties": {
    |        "@timestamp": {
    |          "type": "date"
    |        },
    |        "message": {
    |          "type": "text"
    |        },
    |        "level": {
    |          "type": "keyword"
    |        }
    |      }
    |    }
    |  },
    |  "aliases": {
    |    "logs-all": {}
    |  }
    |}
    |""".stripMargin

client.createTemplate("logs-template", timeSeriesTemplate)

// Indices created with date pattern automatically use template
client.createIndex("logs-2024-01-01")
client.createIndex("logs-2024-01-02")
```

### 2. Multi-Environment Templates

```scala
def createEnvironmentTemplate(env: String, order: Int): String = {
  val replicas = env match {
    case "production" => 2
    case "staging" => 1
    case _ => 0
  }
  
  s"""
     |{
     |  "index_patterns": ["${env}-*"],
     |  "order": $order,
     |  "settings": {
     |    "number_of_shards": 3,
     |    "number_of_replicas": $replicas
     |  },
     |  "mappings": {
     |    "_doc": {
     |      "properties": {
     |        "environment": {
     |          "type": "keyword"
     |        }
     |      }
     |    }
     |  }
     |}
     |""".stripMargin
}

client.createTemplate("production-template", createEnvironmentTemplate("production", 100))
client.createTemplate("staging-template", createEnvironmentTemplate("staging", 50))
client.createTemplate("development-template", createEnvironmentTemplate("development", 10))
```

### 3. Dynamic Mapping with Templates

```scala
val dynamicTemplate =
  """
    |{
    |  "index_patterns": ["dynamic-*"],
    |  "order": 1,
    |  "settings": {
    |    "number_of_shards": 1
    |  },
    |  "mappings": {
    |    "_doc": {
    |      "dynamic_templates": [
    |        {
    |          "strings_as_keywords": {
    |            "match_mapping_type": "string",
    |            "mapping": {
    |              "type": "keyword"
    |            }
    |          }
    |        },
    |        {
    |          "longs_as_integers": {
    |            "match_mapping_type": "long",
    |            "mapping": {
    |              "type": "integer"
    |            }
    |          }
    |        }
    |      ],
    |      "properties": {
    |        "timestamp": {
    |          "type": "date"
    |        }
    |      }
    |    }
    |  }
    |}
    |""".stripMargin

client.createTemplate("dynamic-template", dynamicTemplate)
```

### 4. Filtered Aliases

```scala
val templateWithFilteredAliases =
  """
    |{
    |  "index_patterns": ["events-*"],
    |  "order": 1,
    |  "settings": {
    |    "number_of_shards": 2
    |  },
    |  "mappings": {
    |    "_doc": {
    |      "properties": {
    |        "event_type": {"type": "keyword"},
    |        "severity": {"type": "keyword"},
    |        "timestamp": {"type": "date"}
    |      }
    |    }
    |  },
    |  "aliases": {
    |    "events-all": {},
    |    "events-errors": {
    |      "filter": {
    |        "term": {
    |          "severity": "error"
    |        }
    |      }
    |    },
    |    "events-warnings": {
    |      "filter": {
    |        "term": {
    |          "severity": "warning"
    |        }
    |      }
    |    },
    |    "events-critical": {
    |      "filter": {
    |        "terms": {
    |          "severity": ["critical", "fatal"]
    |        }
    |      },
    |      "routing": "1"
    |    }
    |  }
    |}
    |""".stripMargin

client.createTemplate("events-template", templateWithFilteredAliases)

// Search specific severity levels through aliases
client.search("events-errors", query)
client.search("events-critical", query)
```

---

## Version Compatibility

### Automatic Format Normalization

The API automatically normalizes template format based on Elasticsearch version:

```scala
def normalizeTemplate(templateJson: String, elasticVersion: String): Try[String]
```

**Normalization Logic:**

| ES Version | Input Format | Action Taken |
|------------|-------------|--------------|
| **ES 7.8+** | Legacy | Convert to Composable |
| **ES 7.8+** | Composable | Keep as-is |
| **ES 6.8 - 7.7** | Legacy | Keep as-is |
| **ES 6.8 - 7.7** | Composable | Convert to Legacy |
| **ES < 6.8** | Legacy | Ensure `_doc` wrapper in mappings |
| **ES < 6.8** | Composable | Convert to Legacy + ensure `_doc` wrapper |

---

### Type Name Handling

#### ES < 6.8: Requires `_doc` Type Wrapper

For Elasticsearch versions **before 6.8**, mappings **must** be wrapped in a type name (typically `_doc`):

**Input (Typeless Mappings):**

```json
{
  "index_patterns": ["logs-*"],
  "mappings": {
    "properties": {
      "message": {"type": "text"}
    }
  }
}
```

**Normalized Output (ES < 6.8):**

```json
{
  "index_patterns": ["logs-*"],
  "mappings": {
    "_doc": {
      "properties": {
        "message": {"type": "text"}
      }
    }
  }
}
```

**Code Example:**

```scala
// ES < 6.8
val template =
  """
    |{
    |  "index_patterns": ["logs-*"],
    |  "order": 1,
    |  "mappings": {
    |    "properties": {
    |      "timestamp": {"type": "date"},
    |      "message": {"type": "text"}
    |    }
    |  }
    |}
    |""".stripMargin

// API automatically wraps mappings in _doc for ES < 6.8
client.createTemplate("logs-template", template)

// Resulting template in ES < 6.8:
// {
//   "index_patterns": ["logs-*"],
//   "order": 1,
//   "mappings": {
//     "_doc": {
//       "properties": {
//         "timestamp": {"type": "date"},
//         "message": {"type": "text"}
//       }
//     }
//   }
// }
```

---

#### ES 6.8 - 7.x: Optional Type Names

Elasticsearch 6.8+ supports typeless mappings but still accepts `_doc` wrapper for backward compatibility:

**Both formats work:**

```scala
// Format 1: With _doc wrapper (backward compatible)
val templateWithType =
  """
    |{
    |  "index_patterns": ["logs-*"],
    |  "mappings": {
    |    "_doc": {
    |      "properties": {
    |        "field": {"type": "text"}
    |      }
    |    }
    |  }
    |}
    |""".stripMargin

// Format 2: Typeless (modern)
val templateTypeless =
  """
    |{
    |  "index_patterns": ["logs-*"],
    |  "mappings": {
    |    "properties": {
    |      "field": {"type": "text"}
    |    }
    |  }
    |}
    |""".stripMargin

// Both work in ES 6.8-7.x
client.createTemplate("template1", templateWithType)
client.createTemplate("template2", templateTypeless)
```

---

#### ES 7.8+: Typeless Only (Composable Templates)

Composable templates in ES 7.8+ **do not support type names**:

```scala
val composableTemplate =
  """
    |{
    |  "index_patterns": ["metrics-*"],
    |  "priority": 100,
    |  "template": {
    |    "mappings": {
    |      "properties": {
    |        "cpu": {"type": "float"}
    |      }
    |    }
    |  }
    |}
    |""".stripMargin

// No _doc wrapper needed
client.createTemplate("metrics-template", composableTemplate)
```

---

### Composable → Legacy Conversion

When using composable templates on **ES < 7.8**, the API automatically converts to legacy format:

**Input (Composable Format):**

```json
{
  "index_patterns": ["app-*"],
  "priority": 200,
  "version": 2,
  "template": {
    "settings": {
      "number_of_shards": 3
    },
    "mappings": {
      "properties": {
        "user_id": {"type": "keyword"}
      }
    },
    "aliases": {
      "app-current": {}
    }
  },
  "composed_of": ["component1"],
  "_meta": {
    "description": "App template"
  }
}
```

**Normalized Output (ES 6.8):**

```json
{
  "index_patterns": ["app-*"],
  "order": 200,
  "version": 2,
  "settings": {
    "number_of_shards": 3
  },
  "mappings": {
    "_doc": {
      "properties": {
        "user_id": {"type": "keyword"}
      }
    }
  },
  "aliases": {
    "app-current": {}
  }
}
```

**Conversion Rules:**

| Composable Field | Legacy Field | Notes |
|-----------------|--------------|-------|
| `priority` | `order` | Direct value mapping |
| `template.settings` | `settings` | Flattened to root level |
| `template.mappings` | `mappings` | Flattened + wrapped in `_doc` for ES < 6.8 |
| `template.aliases` | `aliases` | Flattened to root level |
| `composed_of` | ❌ Ignored | Not supported in legacy format (warning logged) |
| `_meta` | ❌ Ignored | Not supported in legacy format |
| `data_stream` | ❌ Ignored | Not supported in legacy format (warning logged) |

**Code Example:**

```scala
// Using composable format on ES 6.8
val composableTemplate =
  """
    |{
    |  "index_patterns": ["events-*"],
    |  "priority": 150,
    |  "template": {
    |    "settings": {
    |      "number_of_shards": 2
    |    },
    |    "mappings": {
    |      "properties": {
    |        "event_type": {"type": "keyword"}
    |      }
    |    }
    |  },
    |  "composed_of": ["base-settings"],
    |  "_meta": {
    |    "owner": "analytics-team"
    |  }
    |}
    |""".stripMargin

// API automatically converts for ES 6.8:
// - priority → order
// - Flattens template object
// - Wraps mappings in _doc
// - Logs warnings for composed_of and _meta
client.createTemplate("events-template", composableTemplate)

// Logs:
// [WARN] Composable templates are not supported in legacy template format and will be ignored
// [DEBUG] Wrapped mappings in '_doc' for ES 6.8.0
```

---

### Legacy → Composable Conversion

When using legacy templates on **ES 7.8+**, the API can optionally convert to composable format:

**Input (Legacy Format):**

```json
{
  "index_patterns": ["logs-*"],
  "order": 10,
  "version": 1,
  "settings": {
    "number_of_shards": 3
  },
  "mappings": {
    "_doc": {
      "properties": {
        "message": {"type": "text"}
      }
    }
  },
  "aliases": {
    "logs-all": {}
  }
}
```

**Normalized Output (ES 7.8+):**

```json
{
  "index_patterns": ["logs-*"],
  "priority": 10,
  "version": 1,
  "template": {
    "settings": {
      "number_of_shards": 3
    },
    "mappings": {
      "properties": {
        "message": {"type": "text"}
      }
    },
    "aliases": {
      "logs-all": {}
    }
  }
}
```

**Conversion Rules:**

| Legacy Field | Composable Field | Notes |
|-------------|------------------|-------|
| `order` | `priority` | Direct value mapping |
| `settings` | `template.settings` | Nested under `template` |
| `mappings` | `template.mappings` | Nested + `_doc` wrapper removed |
| `aliases` | `template.aliases` | Nested under `template` |

---

### Version Detection Helpers

The API uses internal helpers to determine version-specific behavior:

```scala
object ElasticsearchVersion {
  
  /** Check if ES version supports composable templates (7.8+)
    */
  def supportsComposableTemplates(version: String): Boolean = {
    // Returns true for ES >= 7.8
  }
  
  /** Check if ES version requires _doc type wrapper (< 6.8)
    */
  def requiresDocTypeWrapper(version: String): Boolean = {
    // Returns true for ES < 6.8
  }
  
  /** Check if ES version requires include_type_name parameter (6.8 - 7.x)
    */
  def requiresIncludeTypeName(version: String): Boolean = {
    // Returns true for ES 6.8 - 7.x
  }
}
```

**Usage Example:**

```scala
def createTemplateForVersion(
  name: String,
  template: String,
  esVersion: String
): ElasticResult[Boolean] = {
  
  // Normalize based on version
  val normalized = TemplateConverter.normalizeTemplate(template, esVersion) match {
    case Success(json) => json
    case Failure(e) => return ElasticResult.failure(e.getMessage)
  }
  
  // Version-specific handling
  if (ElasticsearchVersion.supportsComposableTemplates(esVersion)) {
    println("Using composable template API")
    executeCreateComposableTemplate(name, normalized)
  } else if (ElasticsearchVersion.requiresIncludeTypeName(esVersion)) {
    println("Using legacy template API with include_type_name")
    executeCreateLegacyTemplateWithTypeName(name, normalized)
  } else {
    println("Using legacy template API")
    executeCreateLegacyTemplate(name, normalized)
  }
}
```

---

### Complete Version Matrix

| ES Version    | Template Type          | Mappings Format         | `include_type_name`  | API Endpoint                                             |
|---------------|------------------------|-------------------------|----------------------|----------------------------------------------------------|
| **6.0 - 6.7** | Legacy                 | `_doc` wrapper required | ❌ Not supported      | `PUT /_template/{name}`                                  |
| **6.8 - 7.7** | Legacy                 | Typeless or `_doc`      | ✅ Optional           | `PUT /_template/{name}`                                  |
| **7.8+**      | Legacy or Composable   | Typeless only           | ❌ Deprecated         | `PUT /_template/{name}` or `PUT /_index_template/{name}` |
| **8.0+**      | Composable (preferred) | Typeless only           | ❌ Removed            | `PUT /_index_template/{name}`                            |

---

### Troubleshooting Version Issues

#### Issue: Template creation fails with "unknown field [_doc]"

**Cause:** Using `_doc` wrapper on ES 7.8+ composable templates

**Solution:**

```scala
// The API automatically unwrap _doc wrapper for ES >= 6.8
val template =
  """
    |{
    |  "index_patterns": ["test-*"],
    |  "priority": 100,
    |  "template": {
    |    "mappings": {
    |      "_doc": {
    |        "properties": {
    |          "field": {"type": "text"}
    |        }
    |      }
    |    }
    |  }
    |}
    |""".stripMargin

// ✅ Correct for ES 6.8+
val template =
  """
    |{
    |  "index_patterns": ["test-*"],
    |  "priority": 100,
    |  "template": {
    |    "mappings": {
    |      "properties": {
    |        "field": {"type": "text"}
    |      }
    |    }
    |  }
    |}
    |""".stripMargin
```

#### Issue: Template creation fails with "missing type name"

**Cause:** Missing `_doc` wrapper on ES < 6.8

**Solution :**

```scala
// The API automatically adds _doc wrapper for ES < 6.8
// No manual action needed - just use typeless format
val template =
  """
    |{
    |  "index_patterns": ["test-*"],
    |  "mappings": {
    |    "properties": {
    |      "field": {"type": "text"}
    |    }
    |  }
    |}
    |""".stripMargin

// API automatically wraps for ES < 6.8
client.createTemplate("test-template", template)
```

#### Issue: Warning "Composable templates are not supported"

**Cause:** Using `composed_of` or `data_stream` on ES < 7.8

**Solution :**

```scala
// These fields are automatically ignored with warning
// Remove them for ES < 7.8 to avoid confusion
val template =
  """
    |{
    |  "index_patterns": ["test-*"],
    |  "order": 1,
    |  "settings": {
    |    "number_of_shards": 1
    |  }
    |}
    |""".stripMargin

// No composed_of or _meta fields
client.createTemplate("test-template", template)
```

---

## Error Handling

### Invalid Template Name

```scala
client.createTemplate("", templateDefinition) match {
  case ElasticFailure(error) =>
    assert(error.statusCode.contains(400))
    assert(error.operation.contains("createTemplate"))
    assert(error.message.contains("Invalid"))
}
```

### Invalid JSON

```scala
val invalidJson = """{"index_patterns": ["test-*"}""" // Missing closing brace

client.createTemplate("test", invalidJson) match {
  case ElasticFailure(error) =>
    println(s"JSON parsing error: ${error.message}")
}
```

### Template Not Found

```scala
client.getTemplate("non-existent") match {
  case ElasticSuccess(None) =>
    println("Template not found")
  case ElasticSuccess(Some(json)) =>
    println(s"Template: $json")
  case ElasticFailure(e) =>
    println(s"Error: ${e.message}")
}
```

### Conflicting Templates

```scala
// Template with same pattern but different order
val template1 = """{"index_patterns": ["app-*"], "order": 1}"""
val template2 = """{"index_patterns": ["app-*"], "order": 10}"""

client.createTemplate("template1", template1)
client.createTemplate("template2", template2)

// template2 will take precedence due to higher order
```

---

## Performance Considerations

### Template Application

⚠️ Templates are applied **only when indices are created**, not to existing indices.

```scala
// ❌ Bad - template won't affect existing index
client.createIndex("logs-2024")
client.createTemplate("logs-template", template) // Too late!

// ✅ Good - create template first
client.createTemplate("logs-template", template)
client.createIndex("logs-2024") // Template applied
```

### Bulk Template Creation

```scala
// ✅ Create templates before bulk indexing
val templates = Map(
  "logs-template" -> logsTemplate,
  "metrics-template" -> metricsTemplate,
  "events-template" -> eventsTemplate
)

templates.foreach { case (name, definition) =>
  client.createTemplate(name, definition, ifExists = true)
}

// Now bulk create indices
val indices = List("logs-2024", "metrics-2024", "events-2024")
indices.foreach(client.createIndex)
```

### Template Caching

```scala
// Templates are cached by Elasticsearch
// Frequent updates can impact performance

// ❌ Bad - updating template repeatedly
(1 to 100).foreach { i =>
  val updated = template.replace("\"version\": 1", s""""version": $i""")
  client.createTemplate("my-template", updated)
}

// ✅ Good - update once with final configuration
client.createTemplate("my-template", finalTemplate)
```

---

## Implementation Requirements

### executeCreateLegacyTemplate

Must be implemented by each client-specific trait for legacy templates.

**Signature:**

```scala
private[client] def executeCreateLegacyTemplate(
  templateName: String,
  templateDefinition: String
): ElasticResult[Boolean]
```

**REST High Level Client (ES 6-7):**

```scala
private[client] def executeCreateLegacyTemplate(
  templateName: String,
  templateDefinition: String
): ElasticResult[Boolean] = {
  executeRestBooleanAction[PutIndexTemplateRequest, AcknowledgedResponse](
    operation = "createTemplate",
    retryable = false
  )(
    request = {
      val req = new PutIndexTemplateRequest(templateName)
      req.source(new BytesArray(templateDefinition), XContentType.JSON)
      req
    }
  )(
    executor = req => apply().indices().putTemplate(req, RequestOptions.DEFAULT)
  )
}
```

### executeCreateComposableTemplate

Must be implemented for composable templates (ES 7.8+).

**Signature:**

```scala
private[client] def executeCreateComposableTemplate(
  templateName: String,
  templateDefinition: String
): ElasticResult[Boolean]
```

**Java Client (ES 8+):**

```scala
private[client] def executeCreateComposableTemplate(
  templateName: String,
  templateDefinition: String
): ElasticResult[Boolean] = {
  executeJavaBooleanAction(
    operation = "createTemplate",
    retryable = false
  )(
    apply()
      .indices()
      .putIndexTemplate(
        PutIndexTemplateRequest.of { builder =>
          builder
            .name(templateName)
            .withJson(new StringReader(templateDefinition))
        }
      )
  )(resp => resp.acknowledged())
}
```

### executeGetLegacyTemplate / executeGetComposableTemplate

Retrieves legacy / composable template configuration.

**Signature:**

```scala
private[client] def executeGetLegacyTemplate(
  templateName: String
): ElasticResult[Option[String]]
```

### executeDeleteLegacyTemplate / executeDeleteComposableTemplate

Deletes legacy / composable template.

**Signature:**

```scala
private[client] def executeDeleteLegacyTemplate(
  templateName: String,
  ifExists: Boolean
): ElasticResult[Boolean]
```

### executeListLegacyTemplates / executeListComposableTemplates

Retrieves legacy / composable template configurations.

**Signature:**

```scala
private[client] def executeListLegacyTemplates(): ElasticResult[Map[String, String]]
```

---

## Testing Examples

### Complete Test Suite

```scala
class TemplateApiSpec extends AnyFlatSpec with Matchers {
  
  val client: ElasticClient = // Initialize client
  
  "createTemplate" should "create a legacy template" in {
    val template =
      """
        |{
        |  "index_patterns": ["test-*"],
        |  "order": 1,
        |  "settings": {
        |    "number_of_shards": 1
        |  }
        |}
        |""".stripMargin
    
    client.createTemplate("test-template", template) shouldBe a[ElasticSuccess[_]]
    
    client.templateExists("test-template") match {
      case ElasticSuccess(true) => succeed
      case _ => fail("Template not created")
    }
    
    client.deleteTemplate("test-template", ifExists = true)
  }
  
  it should "update an existing template" in {
    val template1 = """{"index_patterns": ["update-*"], "order": 1}"""
    val template2 = """{"index_patterns": ["update-*"], "order": 2, "version": 2}"""
    
    client.createTemplate("update-test", template1)
    client.createTemplate("update-test", template2)
    
    client.getTemplate("update-test") match {
      case ElasticSuccess(Some(json)) =>
        json should include("\"order\":2")
        json should include("\"version\":2")
      case _ => fail("Failed to get updated template")
    }
    
    client.deleteTemplate("update-test", ifExists = true)
  }
  
  "getTemplate" should "return None for non-existent template" in {
    client.getTemplate("non-existent-template") match {
      case ElasticSuccess(None) => succeed
      case _ => fail("Should return None")
    }
  }
  
  "deleteTemplate" should "delete existing template" in {
    val template = """{"index_patterns": ["delete-*"], "order": 1}"""
    
    client.createTemplate("delete-test", template)
    client.deleteTemplate("delete-test") shouldBe a[ElasticSuccess[_]]
    
    client.templateExists("delete-test") match {
      case ElasticSuccess(false) => succeed
      case _ => fail("Template not deleted")
    }
  }
  
  it should "succeed with ifExists=true for non-existent template" in {
    client.deleteTemplate("non-existent", ifExists = true) match {
      case ElasticSuccess(false) => succeed
      case _ => fail("Should succeed with ifExists=true")
    }
  }
  
  "templateExists" should "return true for existing template" in {
    val template = """{"index_patterns": ["exists-*"], "order": 1}"""
    
    client.createTemplate("exists-test", template)
    
    client.templateExists("exists-test") match {
      case ElasticSuccess(true) => succeed
      case _ => fail("Template should exist")
    }
    
    client.deleteTemplate("exists-test", ifExists = true)
  }
  
  it should "return false for non-existent template" in {
    client.templateExists("non-existent") match {
      case ElasticSuccess(false) => succeed
      case _ => fail("Template should not exist")
    }
  }
}
```

---

[Back to index](README.md) | [Next: Component Templates](component-templates.md)