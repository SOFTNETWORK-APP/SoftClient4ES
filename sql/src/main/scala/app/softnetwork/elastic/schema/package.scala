package app.softnetwork.elastic

import app.softnetwork.elastic.sql.{BooleanValue, ObjectValue, StringValue, StringValues, Value}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.schema.{
  mapper,
  DdlColumn,
  DdlDateIndexNameProcessor,
  DdlDefaultValueProcessor,
  DdlPartition,
  DdlPrimaryKeyProcessor,
  DdlProcessor,
  DdlProcessorType,
  DdlScriptProcessor,
  DdlTable,
  GenericProcessor
}
import app.softnetwork.elastic.sql.time.TimeUnit
import com.fasterxml.jackson.databind.JsonNode

import scala.jdk.CollectionConverters._

package object schema {

  private[schema] def extractOptions(
    node: JsonNode,
    ignoredKeys: Set[String] = Set.empty
  ): Map[String, Value[_]] = {
    node
      .properties()
      .asScala
      .flatMap { entry =>
        val key = entry.getKey
        val value = entry.getValue

        if (ignoredKeys.contains(key)) {
          None
        } else {
          Value(value).map(key -> Value(_))
        }
      }
      .toMap
  }

  final case class EsField(
    name: String,
    `type`: String,
    script: Option[DdlScriptProcessor] = None,
    null_value: Option[Value[_]] = None,
    not_null: Option[Boolean] = None,
    comment: Option[String] = None,
    fields: List[EsField] = Nil,
    options: Map[String, Value[_]] = Map.empty
  ) {
    lazy val ddlColumn: DdlColumn = {
      DdlColumn(
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

  object EsField {
    def apply(name: String, node: JsonNode): EsField = {
      val tpe = Option(node.get("type")).map(_.asText()).getOrElse("object")

      val nullValue =
        Option(node.get("null_value")).flatMap(Value(_)).map(Value(_))

      val fields =
        Option(node.get("fields")) // multi-fields
          .orElse(Option(node.get("properties"))) // object/nested fields
          .map(_.properties().asScala.map { entry =>
            val name = entry.getKey
            val value = entry.getValue
            apply(name, value)
          }.toList)
          .getOrElse(Nil)

      val options =
        extractOptions(node, ignoredKeys = Set("type", "null_value", "fields", "properties"))

      val meta = options.get("meta")
      val comment = meta.flatMap {
        case m: ObjectValue =>
          m.value.get("comment") match {
            case Some(c: StringValue) => Some(c.value)
            case _                    => None
          }
        case _ => None
      }
      val notNull = meta.flatMap {
        case m: ObjectValue =>
          m.value.get("not_null") match {
            case Some(c: BooleanValue) => Some(c.value)
            case _                     => None
          }
        case _ => None
      }
      val script = meta.flatMap {
        case m: ObjectValue =>
          m.value.get("script") match {
            case Some(st: ObjectValue) =>
              val map = st.value
              map.get("sql") match {
                case Some(script: StringValue) =>
                  map.get("painless") match {
                    case Some(source: StringValue) =>
                      Some(
                        DdlScriptProcessor(
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
      EsField(
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

  final case class EsMappings(
    fields: List[EsField] = Nil,
    primaryKey: List[String] = Nil,
    partitionBy: Option[EsPartitionBy] = None,
    options: Map[String, Value[_]] = Map.empty
  )

  object EsMappings {
    def apply(root: JsonNode): EsMappings = {
      val mappings = root.path("mappings")
      val fields = Option(mappings.get("properties"))
        .orElse(Option(mappings.path("_doc").get("properties")))
        .map(_.properties().asScala.map { entry =>
          val name = entry.getKey
          val value = entry.getValue
          EsField(name, value)
        }.toList)
        .getOrElse(Nil)

      val options = extractOptions(mappings, ignoredKeys = Set("properties", "_doc"))
      val meta = options.get("_meta")
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

      val partitionBy: Option[EsPartitionBy] = meta.flatMap {
        case m: ObjectValue =>
          m.value.get("partition_by") match {
            case Some(pb: ObjectValue) =>
              pb.value.get("column") match {
                case Some(column: StringValue) => // valid
                  pb.value.get("granularity") match {
                    case Some(granularity: StringValue) =>
                      Some(EsPartitionBy(column.value, granularity.value))
                    case _ => Some(EsPartitionBy(column.value, "d"))
                  }
                case _ => None
              }
            case _ => None
          }
        case _ => None
      }

      EsMappings(
        fields = fields,
        primaryKey = primaryKey,
        partitionBy = partitionBy,
        options = options
      )
    }

  }

  final case class EsPartitionBy(
    column: String,
    granularity: String // "d", "M", "y", etc.
  )

  final case class EsSettings(
    options: Map[String, Value[_]] = Map.empty
  )

  object EsSettings {
    def apply(settings: JsonNode): EsSettings = {
      val index = settings.path("settings").path("index")

      val options = extractOptions(index)

      EsSettings(
        options = options
      )
    }
  }

  final case class EsProcessor(
    processor: JsonNode
  ) {
    private val ScriptDescRegex =
      """^\s*([a-zA-Z0-9_]+)\s([a-zA-Z]+)\s+SCRIPT\s+AS\s*\((.*)\)\s*$""".r

    lazy val ddlProcesor: DdlProcessor = {
      val processorType = processor.fieldNames().next() // "set", "script", "date_index_name", etc.
      val props = processor.get(processorType)

      processorType match {
        case "set" =>
          val field = props.get("field").asText()
          val desc = Option(props.get("description")).map(_.asText()).getOrElse("")
          val valueNode = props.get("value")
          val ignoreFailure = Option(props.get("ignore_failure")).exists(_.asBoolean())

          if (field == "_id" && desc.startsWith("PRIMARY KEY")) {
            // DdlPrimaryKeyProcessor
            // description: "PRIMARY KEY (id)"
            val inside = desc.stripPrefix("PRIMARY KEY").trim.stripPrefix("(").stripSuffix(")")
            val cols =
              inside.split(",").map(_.replaceAll("\"", "")).map(_.trim).filter(_.nonEmpty).toSet
            DdlPrimaryKeyProcessor(
              sql = desc,
              column = "_id",
              value = cols,
              ignoreFailure = ignoreFailure
            )
          } else if (desc.startsWith(s"$field DEFAULT")) {
            DdlDefaultValueProcessor(
              sql = desc,
              column = field,
              value = Value(valueNode.asText()),
              ignoreFailure = ignoreFailure
            )
          } else {
            GenericProcessor(
              processorType = DdlProcessorType.Set,
              properties =
                mapper.convertValue(props, classOf[java.util.Map[String, Object]]).asScala.toMap
            )
          }

        case "script" =>
          val desc = props.get("description").asText()
          val lang = props.get("lang").asText()
          require(lang == "painless", s"Only painless supported, got $lang")
          val source = props.get("source").asText()
          val ignoreFailure = Option(props.get("ignore_failure")).exists(_.asBoolean())

          desc match {
            case ScriptDescRegex(col, dataType, script) =>
              DdlScriptProcessor(
                script = script,
                column = col,
                dataType = SQLTypes(dataType),
                source = source,
                ignoreFailure = ignoreFailure
              )
            case _ =>
              GenericProcessor(
                processorType = DdlProcessorType.Script,
                properties =
                  mapper.convertValue(props, classOf[java.util.Map[String, Object]]).asScala.toMap
              )
          }

        case "date_index_name" =>
          val field = props.get("field").asText()
          val desc = Option(props.get("description")).map(_.asText()).getOrElse("")
          val rounding = props.get("date_rounding").asText()
          val formats = Option(props.get("date_formats"))
            .map(_.elements().asScala.toList.map(_.asText()))
            .getOrElse(Nil)
          val prefix = props.get("index_name_prefix").asText()

          DdlDateIndexNameProcessor(
            sql = desc,
            column = field,
            dateRounding = rounding,
            dateFormats = formats,
            prefix = prefix
          )

        case other =>
          GenericProcessor(
            processorType = DdlProcessorType(other),
            properties =
              mapper.convertValue(props, classOf[java.util.Map[String, Object]]).asScala.toMap
          )

      }
    }
  }

  final case class EsPipeline(
    pipeline: JsonNode
  ) {
    lazy val processors: Seq[DdlProcessor] = {
      val processorsNode = pipeline.get("processors")
      if (processorsNode != null && processorsNode.isArray) {
        processorsNode.elements().asScala.toSeq.map(EsProcessor(_).ddlProcesor)
      } else {
        Seq.empty
      }
    }
  }

  final case class EsIndex(
    name: String,
    mappings: JsonNode,
    settings: JsonNode,
    pipeline: Option[JsonNode] = None
  ) {

    lazy val esMappings: EsMappings = EsMappings(mappings)

    lazy val esSettings: EsSettings = EsSettings(settings)

    lazy val esPipeline: Option[EsPipeline] = pipeline.map(EsPipeline(_))

    lazy val ddlTable: DdlTable = {
      // 1. Columns from the mapping
      val initialCols: Map[String, DdlColumn] =
        esMappings.fields.map { field =>
          val name = field.name
          name -> field.ddlColumn
        }.toMap

      // 2. PK + partition + pipelines from index mappings and settings
      var primaryKey: List[String] = esMappings.primaryKey
      var partitionBy: Option[DdlPartition] = esMappings.partitionBy.map { p =>
        val granularity = TimeUnit(p.granularity)
        DdlPartition(p.column, granularity)
      }

      // 3. Enrichment from the pipeline (if provided)
      val enrichedCols = scala.collection.mutable.Map.from(initialCols)

      esPipeline.foreach { pipeline =>
        pipeline.processors.foreach {
          case p: DdlScriptProcessor =>
            val col = p.column
            enrichedCols.get(col).foreach { c =>
              enrichedCols.update(col, c.copy(script = Some(p)))
            }

          case p: DdlDefaultValueProcessor =>
            val col = p.column
            enrichedCols.get(col).foreach { c =>
              enrichedCols.update(col, c.copy(defaultValue = Some(p.value)))
            }

          case p: DdlDateIndexNameProcessor =>
            if (partitionBy.isEmpty) {
              val granularity = TimeUnit(p.dateRounding)
              partitionBy = Some(DdlPartition(p.column, granularity))
            }

          case p: DdlPrimaryKeyProcessor =>
            if (primaryKey.isEmpty) {
              primaryKey = p.value.toList
            }

          case _ => // ignore others (rename/remove...) ou gère-les si tu veux les remonter en DDL
        }
      }

      // 4. Final construction of the DdlTable
      DdlTable(
        name = name,
        columns = enrichedCols.values.toList.sortBy(_.name),
        primaryKey = primaryKey,
        partitionBy = partitionBy,
        esMappings.options,
        esSettings.options
      ).update()
    }
  }

}
