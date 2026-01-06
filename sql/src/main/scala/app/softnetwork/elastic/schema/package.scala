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

package app.softnetwork.elastic

import app.softnetwork.elastic.sql.{BooleanValue, ObjectValue, StringValue, StringValues, Value}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.schema.{
  Column,
  DateIndexNameProcessor,
  DefaultValueProcessor,
  IngestPipelineType,
  IngestProcessor,
  PartitionDate,
  PrimaryKeyProcessor,
  Schema,
  ScriptProcessor,
  Table
}
import app.softnetwork.elastic.sql.serialization._
import app.softnetwork.elastic.sql.time.TimeUnit
import com.fasterxml.jackson.databind.JsonNode

import scala.jdk.CollectionConverters._

package object schema {

  final case class IndexField(
    name: String,
    `type`: String,
    script: Option[ScriptProcessor] = None,
    null_value: Option[Value[_]] = None,
    not_null: Option[Boolean] = None,
    comment: Option[String] = None,
    fields: List[IndexField] = Nil,
    options: Map[String, Value[_]] = Map.empty
  ) {
    lazy val ddlColumn: Column = {
      Column(
        name = name,
        dataType = SQLTypes(this),
        script = script,
        multiFields = fields.map(_.ddlColumn),
        defaultValue = null_value,
        notNull = not_null.getOrElse(false),
        comment = comment,
        options = options
      )
    }
  }

  object IndexField {
    def apply(name: String, node: JsonNode, _meta: Option[ObjectValue]): IndexField = {
      val tpe = _meta
        .flatMap {
          case m: ObjectValue =>
            m.value.get("data_type") match {
              case Some(v: StringValue) => Some(v.value)
              case _                    => None
            }
          case _ => None
        }
        .getOrElse(Option(node.get("type")).map(_.asText()).getOrElse("object"))

      val nullValue =
        Option(node.get("null_value"))
          .flatMap(Value(_))
          .map(Value(_))
          .orElse(_meta.flatMap {
            case m: ObjectValue =>
              m.value.get("default_value") match {
                case Some(v: Value[_]) => Some(v)
                case _                 => None
              }
            case _ => None
          })

      val fields_meta = _meta.flatMap {
        case m: ObjectValue =>
          m.value.get("multi_fields") match {
            case Some(f: ObjectValue) => Some(f)
            case _                    => None
          }
        case _ => None
      }

      val fields =
        Option(node.get("fields")) // multi-fields
          .orElse(Option(node.get("properties"))) // object/nested fields
          .map(_.properties().asScala.map { entry =>
            val name = entry.getKey
            val value = entry.getValue
            val _meta = fields_meta.flatMap {
              case m: ObjectValue =>
                m.value.get(name) match {
                  case Some(colMeta: ObjectValue) => Some(colMeta)
                  case _                          => None
                }
              case _ => None
            }
            apply(name, value, _meta)
          }.toList)
          .getOrElse(Nil)

      val options =
        extractObject(node, ignoredKeys = Set("type", "null_value", "fields", "properties"))

      val comment = _meta.flatMap {
        case m: ObjectValue =>
          m.value.get("comment") match {
            case Some(c: StringValue) => Some(c.value)
            case _                    => None
          }
        case _ => None
      }

      val notNull = _meta.flatMap {
        case m: ObjectValue =>
          m.value.get("not_null") match {
            case Some(c: BooleanValue) => Some(c.value)
            case Some(c: StringValue)  => Some(c.value.toBoolean)
            case _                     => None
          }
        case _ => None
      }

      val script = _meta.flatMap {
        case m: ObjectValue =>
          m.value.get("script") match {
            case Some(st: ObjectValue) =>
              val map = st.value
              map.get("sql") match {
                case Some(script: StringValue) =>
                  map.get("painless") match {
                    case Some(source: StringValue) =>
                      Some(
                        ScriptProcessor(
                          script = script.value,
                          column = name,
                          dataType = SQLTypes(tpe),
                          source = source.value
                        )
                      )
                  }
                case _ => None
              }
            case _ => None
          }
        case _ => None
      }
      IndexField(
        name = name,
        `type` = tpe,
        script = script,
        null_value = nullValue,
        not_null = notNull,
        comment = comment,
        fields = fields,
        options = options
      )

    }
  }

  final case class IndexMappings(
    fields: List[IndexField] = Nil,
    primaryKey: List[String] = Nil,
    partitionBy: Option[IndexDatePartition] = None,
    options: Map[String, Value[_]] = Map.empty
  )

  object IndexMappings {
    def apply(root: JsonNode): IndexMappings = {
      if (root.has("mappings")) {
        val mappingsNode = root.path("mappings")
        return apply(mappingsNode)
      }
      if (root.has("_doc")) {
        val docNode = root.path("_doc")
        return apply(docNode)
      }
      val options = extractObject(root, ignoredKeys = Set("properties"))
      val meta = options.get("_meta")
      val columns = meta.flatMap {
        case m: ObjectValue =>
          m.value.get("columns") match {
            case Some(cols: ObjectValue) => Some(cols)
            case _                       => None
          }
        case _ => None
      }
      val fields = Option(root.get("properties"))
        .map(_.properties().asScala.map { entry =>
          val name = entry.getKey
          val value = entry.getValue
          val _meta = columns.flatMap {
            case c: ObjectValue =>
              c.value.get(name) match {
                case Some(colMeta: ObjectValue) => Some(colMeta)
                case _                          => None
              }
            case _ => None
          }
          IndexField(name, value, _meta)
        }.toList)
        .getOrElse(Nil)

      val primaryKey: List[String] = meta
        .map {
          case m: ObjectValue =>
            m.value.get("primary_key") match {
              case Some(pk: StringValues) => pk.values.map(_.ddl.replaceAll("\"", "")).toList
              case Some(pk: StringValue)  => List(pk.ddl.replaceAll("\"", ""))
              case _                      => List.empty
            }
          case _ => List.empty
        }
        .getOrElse(List.empty)

      val partitionBy: Option[IndexDatePartition] = meta.flatMap {
        case m: ObjectValue =>
          m.value.get("partition_by") match {
            case Some(pb: ObjectValue) =>
              pb.value.get("column") match {
                case Some(column: StringValue) => // valid
                  pb.value.get("granularity") match {
                    case Some(granularity: StringValue) =>
                      Some(IndexDatePartition(column.value, granularity.value))
                    case _ => Some(IndexDatePartition(column.value, "d"))
                  }
                case _ => None
              }
            case _ => None
          }
        case _ => None
      }

      IndexMappings(
        fields = fields,
        primaryKey = primaryKey,
        partitionBy = partitionBy,
        options = options
      )
    }

  }

  final case class IndexDatePartition(
    column: String,
    granularity: String // "d", "M", "y", etc.
  )

  final case class IndexSettings(
    options: Map[String, Value[_]] = Map.empty
  )

  object IndexSettings {
    def apply(settings: JsonNode): IndexSettings = {
      if (settings.has("settings")) {
        val settingsNode = settings.path("settings")
        return apply(settingsNode)
      }
      val index = settings.path("index")

      val options = extractObject(index)

      IndexSettings(
        options = options
      )
    }
  }

  final case class IndexIngestProcessor(
    pipelineType: IngestPipelineType,
    processor: JsonNode
  ) {
    lazy val ddlProcesor: IngestProcessor = IngestProcessor(pipelineType, processor)
  }

  final case class IndexIngestPipeline(
    pipelineType: IngestPipelineType,
    pipeline: JsonNode
  ) {
    lazy val processors: Seq[IngestProcessor] = {
      val processorsNode = pipeline.get("processors")
      if (processorsNode != null && processorsNode.isArray) {
        processorsNode
          .elements()
          .asScala
          .toSeq
          .map(IndexIngestProcessor(pipelineType, _).ddlProcesor)
      } else {
        Seq.empty
      }
    }
  }

  final case class IndexAlias(
    name: String,
    filter: Map[String, Any] = Map.empty,
    routing: Option[String] = None,
    indexRouting: Option[String] = None,
    searchRouting: Option[String] = None,
    isWriteIndex: Boolean = false,
    isHidden: Boolean = false,
    node: JsonNode
  )

  object IndexAlias {
    def apply(name: String, node: JsonNode): IndexAlias = {
      if (node.has(name)) {
        val aliasNode = node.path(name)
        return apply(name, aliasNode)
      }
      val filter: Map[String, Any] =
        if (node.has("filter")) node.path("filter")
        else Map.empty
      val routing =
        Option(node.get("routing")).map(_.asText())
      val indexRouting =
        Option(node.get("index_routing")).map(_.asText())
      val searchRouting =
        Option(node.get("search_routing")).map(_.asText())
      val isWriteIndex =
        Option(node.get("is_write_index")).exists(_.asBoolean())
      val isHidden =
        Option(node.get("is_hidden")).exists(_.asBoolean())
      IndexAlias(
        name = name,
        filter = filter,
        routing = routing,
        indexRouting = indexRouting,
        searchRouting = searchRouting,
        isWriteIndex = isWriteIndex,
        isHidden = isHidden,
        node = node
      )
    }
  }

  final case class IndexAliases(
    aliases: Map[String, IndexAlias] = Map.empty
  )

  object IndexAliases {
    def apply(nodes: Seq[(String, JsonNode)]): IndexAliases = {
      IndexAliases(nodes.map(entry => entry._1 -> IndexAlias(entry._1, entry._2)).toMap)
    }
  }

  final case class Index(
    name: String,
    mappings: JsonNode,
    settings: JsonNode,
    aliases: Map[String, JsonNode] = Map.empty,
    defaultPipeline: Option[JsonNode] = None,
    finalPipeline: Option[JsonNode] = None
  ) {

    lazy val defaultIngestPipelineName: Option[String] = esSettings.options.get("index") match {
      case Some(obj: ObjectValue) =>
        obj.value.get("default_pipeline") match {
          case Some(s: StringValue) => Some(s.value)
          case _                    => None
        }
      case _ => None
    }

    lazy val finalIngestPipelineName: Option[String] = esSettings.options.get("index") match {
      case Some(obj: ObjectValue) =>
        obj.value.get("final_pipeline") match {
          case Some(s: StringValue) => Some(s.value)
          case _                    => None
        }
      case _ => None
    }

    lazy val esMappings: IndexMappings = IndexMappings(mappings)

    lazy val esSettings: IndexSettings = IndexSettings(settings)

    lazy val esAliases: IndexAliases = IndexAliases(aliases.toSeq)

    lazy val esDefaultPipeline: Option[IndexIngestPipeline] =
      defaultPipeline.map(IndexIngestPipeline(IngestPipelineType.Default, _))

    lazy val esFinalPipeline: Option[IndexIngestPipeline] =
      finalPipeline.map(IndexIngestPipeline(IngestPipelineType.Final, _))

    lazy val esProcessors: Seq[IngestProcessor] = {
      val defaultProcessors = esDefaultPipeline.map(_.processors).getOrElse(Seq.empty)
      val finalProcessors = esFinalPipeline.map(_.processors).getOrElse(Seq.empty)
      defaultProcessors ++ finalProcessors
    }

    lazy val schema: Schema = {
      // 1. Columns from the mapping
      val initialCols: Map[String, Column] =
        esMappings.fields.map { field =>
          val name = field.name
          name -> field.ddlColumn
        }.toMap

      // 2. PK + partition + pipelines from index mappings and settings
      var primaryKey: List[String] = esMappings.primaryKey
      var partitionBy: Option[PartitionDate] = esMappings.partitionBy.map { p =>
        val granularity = TimeUnit(p.granularity)
        PartitionDate(p.column, granularity)
      }

      // 3. Enrichment from the pipeline (if provided)
      val enrichedCols = scala.collection.mutable.Map(initialCols.toSeq: _*)

      var processors: collection.mutable.Seq[IngestProcessor] = collection.mutable.Seq.empty

      esProcessors.foreach {
        case p: ScriptProcessor =>
          val col = p.column
          enrichedCols.get(col).foreach { c =>
            enrichedCols.update(col, c.copy(script = Some(p)))
          }

        case p: DefaultValueProcessor =>
          val col = p.column
          enrichedCols.get(col).foreach { c =>
            enrichedCols.update(col, c.copy(defaultValue = Some(p.value)))
          }

        case p: DateIndexNameProcessor =>
          if (partitionBy.isEmpty) {
            val granularity = TimeUnit(p.dateRounding)
            partitionBy = Some(PartitionDate(p.column, granularity))
          }

        case p: PrimaryKeyProcessor =>
          if (primaryKey.isEmpty) {
            primaryKey = p.value.toList
          }

        case p: IngestProcessor => processors = processors :+ p

      }

      // 4. Final construction of the Table
      Table(
        name = name,
        columns = enrichedCols.values.toList.sortBy(_.name),
        primaryKey = primaryKey,
        partitionBy = partitionBy,
        mappings = esMappings.options,
        settings = esSettings.options,
        processors = processors.toSeq,
        aliases = esAliases.aliases.map(entry => entry._1 -> entry._2.node)
      ).update()
    }
  }

  object Index {
    def apply(name: String, json: String): Index = {
      val root: JsonNode = json
      apply(name, root)
    }

    def apply(name: String, root: JsonNode): Index = {
      if (root.has(name)) {
        val indexNode = root.path(name)
        return apply(name, indexNode)
      }
      val mappings = root.path("mappings")
      val settings = root.path("settings")
      val aliasesNode = root.path("aliases")
      val aliases: Map[String, JsonNode] =
        if (aliasesNode != null && aliasesNode.isObject) {
          aliasesNode
            .properties()
            .asScala
            .map { entry =>
              val aliasName = entry.getKey
              val aliasValue = entry.getValue
              aliasName -> aliasValue
            }
            .toMap
        } else {
          Map.empty
        }
      Index(
        name = name,
        mappings = mappings,
        settings = settings,
        aliases = aliases
      )
    }
  }
}
