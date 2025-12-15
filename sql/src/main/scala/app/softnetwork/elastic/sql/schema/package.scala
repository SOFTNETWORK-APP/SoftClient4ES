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

package app.softnetwork.elastic.sql

import app.softnetwork.elastic.schema.{Field, Index}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils, SQLTypes}
import app.softnetwork.elastic.sql.query._
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import app.softnetwork.elastic.sql.time.TimeUnit
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode

import scala.language.implicitConversions
import scala.jdk.CollectionConverters._

package object schema {
  val mapper: ObjectMapper = JacksonConfig.objectMapper

  sealed trait DdlProcessorType {
    def name: String
  }

  object DdlProcessorType {
    case object Script extends DdlProcessorType {
      def name: String = "script"
    }
    case object Rename extends DdlProcessorType {
      def name: String = "rename"
    }
    case object Remove extends DdlProcessorType {
      def name: String = "remove"
    }
    case object Set extends DdlProcessorType {
      def name: String = "set"
    }
    case object DateIndexName extends DdlProcessorType {
      def name: String = "date_index_name"
    }
  }

  sealed trait DdlProcessor extends Token {
    def column: String
    def ignoreFailure: Boolean
    final def node: ObjectNode = {
      val node = mapper.createObjectNode()
      val props = mapper.createObjectNode()
      for ((key, value) <- properties) {
        value match {
          case v: String     => props.put(key, v)
          case v: Boolean    => props.put(key, v)
          case v: Int        => props.put(key, v)
          case v: Long       => props.put(key, v)
          case v: Double     => props.put(key, v)
          case v: ObjectNode => props.set(key, v)
          case v: Seq[_] =>
            val arrayNode = mapper.createArrayNode()
            v.foreach {
              case s: String     => arrayNode.add(s)
              case b: Boolean    => arrayNode.add(b)
              case i: Int        => arrayNode.add(i)
              case l: Long       => arrayNode.add(l)
              case d: Double     => arrayNode.add(d)
              case o: ObjectNode => arrayNode.add(o)
              case _             =>
            }
            props.set(key, arrayNode)
          case _ =>
        }
      }
      node.set(processorType.name, props)
      node
    }
    def json: String = node.toString
    def processorType: DdlProcessorType
    def description: String = sql.trim
    def name: String = processorType.name
    def properties: Map[String, Any]
  }

  case class DdlScriptProcessor(
    script: String,
    column: String,
    dataType: SQLType,
    source: String,
    ignoreFailure: Boolean = true
  ) extends DdlProcessor {
    override def sql: SQL = s"$column $dataType SCRIPT AS ($script)"

    override def baseType: SQLType = dataType

    def processorType: DdlProcessorType = DdlProcessorType.Script

    override def properties: Map[SQL, Any] = Map(
      "description"    -> description,
      "lang"           -> "painless",
      "source"         -> source,
      "ignore_failure" -> ignoreFailure
    )

  }

  case class DdlRenameProcessor(
    column: String,
    newName: String,
    ignoreFailure: Boolean = true,
    ignoreMissing: Boolean = true
  ) extends DdlProcessor {
    def processorType: DdlProcessorType = DdlProcessorType.Rename

    def sql: String = s"$column RENAME TO $newName"

    override def properties: Map[SQL, Any] = Map(
      "description"    -> description,
      "field"          -> column,
      "target_field"   -> newName,
      "ignore_failure" -> ignoreFailure,
      "ignore_missing" -> ignoreMissing
    )

  }

  case class DdlRemoveProcessor(
    sql: String,
    column: String,
    ignoreFailure: Boolean = true,
    ignoreMissing: Boolean = true
  ) extends DdlProcessor {
    def processorType: DdlProcessorType = DdlProcessorType.Remove

    override def properties: Map[SQL, Any] = Map(
      "description"    -> description,
      "field"          -> column,
      "ignore_failure" -> ignoreFailure,
      "ignore_missing" -> ignoreMissing
    )

  }

  case class DdlPrimaryKeyProcessor(
    sql: String,
    column: String,
    value: Set[String],
    ignoreFailure: Boolean = false,
    ignoreEmptyValue: Boolean = false,
    separator: String = "|"
  ) extends DdlProcessor {
    def processorType: DdlProcessorType = DdlProcessorType.Set

    override def properties: Map[SQL, Any] = Map(
      "description"        -> description,
      "field"              -> column,
      "value"              -> value.mkString("{{", separator, "}}"),
      "ignore_failure"     -> ignoreFailure,
      "ignore_empty_value" -> ignoreEmptyValue
    )

  }

  case class DdlDefaultValueProcessor(
    sql: String,
    column: String,
    value: Value[_],
    ignoreFailure: Boolean = true
  ) extends DdlProcessor {
    def processorType: DdlProcessorType = DdlProcessorType.Set

    def _if: String = {
      if (column.contains("."))
        s"""ctx.${column.split(".").mkString("?.")} == null"""
      else
        s"""ctx.$column == null"""
    }

    override def properties: Map[SQL, Any] = Map(
      "description" -> description,
      "field"       -> column,
      "value" -> {
        value match {
          case IdValue | IngestTimestampValue => s"{{${value.value}}}"
          case _                              => value.value
        }
      },
      "ignore_failure" -> ignoreFailure,
      "if"             -> _if
    )
  }

  case class DdlDateIndexNameProcessor(
    sql: String,
    column: String,
    dateRounding: String,
    dateFormats: List[String],
    prefix: String,
    separator: String = "-",
    ignoreFailure: Boolean = true
  ) extends DdlProcessor {
    def processorType: DdlProcessorType = DdlProcessorType.DateIndexName

    override def properties: Map[SQL, Any] = Map(
      "description"       -> description,
      "field"             -> column,
      "date_rounding"     -> dateRounding,
      "date_formats"      -> dateFormats,
      "index_name_prefix" -> prefix,
      "separator"         -> separator,
      "ignore_failure"    -> ignoreFailure
    )

  }

  implicit def primaryKeyToDdlProcessor(
    primaryKey: List[String]
  ): Seq[DdlProcessor] = {
    if (primaryKey.nonEmpty) {
      Seq(
        DdlPrimaryKeyProcessor(
          sql = s"PRIMARY KEY (${primaryKey.mkString(", ")})",
          column = "_id",
          value = primaryKey.toSet
        )
      )
    } else {
      Nil
    }
  }

  object DdlProcessor {
    private val ScriptDescRegex =
      """^\s*([a-zA-Z0-9_]+)\s([a-zA-Z]+)\s+SCRIPT\s+AS\s*\((.*)\)\s*$""".r

    def apply(node: JsonNode): Option[DdlProcessor] = {
      val processorType = node.fieldNames().next() // "set", "script", "date_index_name", etc.
      val props = node.get(processorType)

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
            val cols = inside.split(",").map(_.trim).filter(_.nonEmpty).toSet
            Some(
              DdlPrimaryKeyProcessor(
                sql = desc,
                column = "_id",
                value = cols,
                ignoreFailure = ignoreFailure
              )
            )
          } else if (desc.startsWith(s"$field DEFAULT")) {
            Some(
              DdlDefaultValueProcessor(
                sql = desc,
                column = field,
                value = Value(valueNode.asText()),
                ignoreFailure = ignoreFailure
              )
            )
          } else {
            None
          }

        case "script" =>
          val desc = props.get("description").asText()
          val lang = props.get("lang").asText()
          require(lang == "painless", s"Only painless supported, got $lang")
          val source = props.get("source").asText()
          val ignoreFailure = Option(props.get("ignore_failure")).exists(_.asBoolean())

          desc match {
            case ScriptDescRegex(col, dataType, script) =>
              Some(
                DdlScriptProcessor(
                  script = script,
                  column = col,
                  dataType = SQLTypes(dataType),
                  source = source,
                  ignoreFailure = ignoreFailure
                )
              )
            case _ =>
              None
          }

        case "date_index_name" =>
          val field = props.get("field").asText()
          val desc = Option(props.get("description")).map(_.asText()).getOrElse("")
          val rounding = props.get("date_rounding").asText()
          val formats = Option(props.get("date_formats"))
            .map(_.elements().asScala.toList.map(_.asText()))
            .getOrElse(Nil)
          val prefix = props.get("index_name_prefix").asText()

          Some(
            DdlDateIndexNameProcessor(
              sql = desc,
              column = field,
              dateRounding = rounding,
              dateFormats = formats,
              prefix = prefix
            )
          )

        case _ => None
      }
    }
  }

  sealed trait DdlPipelineType {
    def name: String
  }

  object DdlPipelineType {
    case object Default extends DdlPipelineType {
      def name: String = "DEFAULT"
    }
    case object Final extends DdlPipelineType {
      def name: String = "FINAL"
    }
    case object Custom extends DdlPipelineType {
      def name: String = "CUSTOM"
    }
  }

  case class DdlPipeline(
    name: String,
    ddlPipelineType: DdlPipelineType,
    ddlProcessors: Seq[DdlProcessor]
  ) extends Token {
    def sql: String =
      s"CREATE OR REPLACE ${ddlPipelineType.name} PIPELINE $name WITH PROCESSORS (${ddlProcessors.map(_.sql.trim).mkString(", ")})"

    def node: ObjectNode = {
      val node = mapper.createObjectNode()
      val processorsNode = mapper.createArrayNode()
      ddlProcessors.foreach { processor =>
        processorsNode.add(processor.node)
      }
      node.put("description", sql)
      node.set("processors", processorsNode)
      node
    }

    def json: String = node.toString
  }

  private[schema] def update(node: ObjectNode, updates: Map[String, Value[_]]): ObjectNode = {
    updates.foreach { case (k, v) =>
      v match {
        case Null            => node.putNull(k)
        case BooleanValue(b) => node.put(k, b)
        case StringValue(s)  => node.put(k, s)
        case ByteValue(b)    => node.put(k, b)
        case ShortValue(s)   => node.put(k, s)
        case IntValue(i)     => node.put(k, i)
        case LongValue(l)    => node.put(k, l)
        case DoubleValue(d)  => node.put(k, d)
        case FloatValue(f)   => node.put(k, f)
        case ObjectValue(value) =>
          if (value.nonEmpty)
            node.set(k, update(mapper.createObjectNode(), value))
        case _ => // do nothing
      }
    }
    node
  }

  case class DdlColumn(
    name: String,
    dataType: SQLType,
    script: Option[DdlScriptProcessor] = None,
    multiFields: List[DdlColumn] = Nil,
    notNull: Boolean = false,
    defaultValue: Option[Value[_]] = None,
    options: Map[String, Value[_]] = Map.empty
  ) extends Token {
    def sql: String = {
      val opts = if (options.nonEmpty) {
        s" OPTIONS (${options.map { case (k, v) => s"$k = $v" }.mkString(", ")}) "
      } else {
        ""
      }
      val notNullOpt = if (notNull) " NOT NULL" else ""
      val defaultOpt = defaultValue.map(v => s" DEFAULT $v").getOrElse("")
      val fieldsOpt = if (multiFields.nonEmpty) {
        s" FIELDS (${multiFields.mkString(", ")})"
      } else {
        ""
      }
      val scriptOpt = script.map(s => s" SCRIPT AS (${s.script})").getOrElse("")
      s"$name $dataType$fieldsOpt$scriptOpt$notNullOpt$defaultOpt$opts"
    }

    def ddlProcessors: Seq[DdlProcessor] = script.toSeq ++
      defaultValue.map { dv =>
        DdlDefaultValueProcessor(
          sql = s"$name DEFAULT $dv",
          column = name,
          value = dv
        )
      }.toSeq

    def node: ObjectNode = {
      val root = mapper.createObjectNode()
      val esType = SQLTypeUtils.elasticType(dataType)
      root.put("type", esType)
      defaultValue.foreach { dv =>
        update(root, Map("null_value" -> dv))
      }
      if (multiFields.nonEmpty) {
        val name =
          esType match {
            case "object" | "nested" => "properties"
            case _                   => "fields"
          }
        val fieldsNode = mapper.createObjectNode()
        multiFields.foreach { field =>
          fieldsNode.replace(field.name, field.node)
        }
        root.set(name, fieldsNode)
      }
      update(root, options)
      root
    }
  }

  object DdlColumn {
    def apply(field: Field): DdlColumn = {
      DdlColumn(
        name = field.name,
        dataType = SQLTypes(field),
        multiFields = field.fields.map(apply),
        defaultValue = field.null_value,
        options = field.options
      )
    }
  }

  case class DdlPartition(column: String, granularity: TimeUnit = TimeUnit.DAYS) extends Token {
    def sql: String = s" PARTITION BY $column ($granularity)"

    val dateRounding: String = granularity.script.get

    val dateFormats: List[String] = granularity match {
      case TimeUnit.YEARS  => List("yyyy")
      case TimeUnit.MONTHS => List("yyyy-MM")
      case TimeUnit.DAYS   => List("yyyy-MM-dd")
      case TimeUnit.HOURS  => List("yyyy-MM-dd'T'HH", "yyyy-MM-dd HH")
      case TimeUnit.MINUTES =>
        List("yyyy-MM-dd'T'HH:mm", "yyyy-MM-dd HH:mm")
      case TimeUnit.SECONDS =>
        List("yyyy-MM-dd'T'HH:mm:ss", "yyyy-MM-dd HH:mm:ss")
      case _ => List.empty
    }

    def ddlProcessor(table: DdlTable): DdlDateIndexNameProcessor =
      DdlDateIndexNameProcessor(
        sql,
        column,
        dateRounding,
        dateFormats,
        prefix = s"${table.name}-"
      )
  }

  case class DdlColumnNotFound(column: String, table: String)
      extends Exception(s"Column $column  does not exist in table $table")

  case class DdlTable(
    name: String,
    columns: List[DdlColumn],
    primaryKey: List[String] = Nil,
    partitionBy: Option[DdlPartition] = None,
    defaultPipeline: Option[String] = None,
    finalPipeline: Option[String] = None,
    mappings: Map[String, Value[_]] = Map.empty,
    settings: Map[String, Value[_]] = Map.empty
  ) extends Token {
    private[schema] lazy val cols: Map[String, DdlColumn] = columns.map(c => c.name -> c).toMap

    def sql: String = {
      val opts =
        if (mappings.nonEmpty || settings.nonEmpty) {
          val mappingOpts =
            if (mappings.nonEmpty) {
              s"mappings = (${mappings.map { case (k, v) => s"$k = $v" }.mkString(", ")})"
            } else {
              ""
            }
          val settingsOpts =
            if (settings.nonEmpty) {
              s"settings = (${mappings.map { case (k, v) => s"$k = $v" }.mkString(", ")})"
            } else {
              ""
            }
          val separator = if (partitionBy.nonEmpty) "," else ""
          s"$separator OPTIONS = (${Seq(mappingOpts, settingsOpts).filter(_.nonEmpty).mkString(", ")})"
        } else {
          ""
        }
      val cols = columns.map(_.sql).mkString(",\n\t")
      val pkStr = if (primaryKey.nonEmpty) {
        s",\n\tPRIMARY KEY (${primaryKey.mkString(", ")})\n"
      } else {
        ""
      }
      s"CREATE OR REPLACE TABLE $name (\n\t$cols$pkStr)${partitionBy.getOrElse("")}$opts"
    }

    def ddlProcessors: Seq[DdlProcessor] =
      columns.flatMap(_.ddlProcessors) ++ partitionBy.map(_.ddlProcessor(this)).toSeq ++ primaryKey

    def merge(statements: Seq[AlterTableStatement]): DdlTable = {
      statements.foldLeft(this) { (table, statement) =>
        statement match {
          case AddColumn(column, ifNotExists) =>
            if (ifNotExists && table.cols.contains(column.name)) table
            else if (!table.cols.contains(column.name))
              table.copy(columns = table.columns :+ column)
            else throw DdlColumnNotFound(column.name, table.name)
          case DropColumn(columnName, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(columns = table.columns.filterNot(_.name == columnName))
            else throw DdlColumnNotFound(columnName, table.name)
          case RenameColumn(oldName, newName) =>
            if (cols.contains(oldName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == oldName) col.copy(name = newName) else col
                }
              )
            else throw DdlColumnNotFound(oldName, table.name)
          case AlterColumnType(columnName, newType, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName) col.copy(dataType = newType)
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case AlterColumnDefault(columnName, newDefault, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName) col.copy(defaultValue = Some(newDefault))
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case DropColumnDefault(columnName, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName) col.copy(defaultValue = None)
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case AlterColumnNotNull(columnName, ifExists) =>
            if (!table.cols.contains(columnName) && ifExists) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName) col.copy(notNull = true)
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case DropColumnNotNull(columnName, ifExists) =>
            if (!table.cols.contains(columnName) && ifExists) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName) col.copy(notNull = false)
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case AlterColumnOptions(columnName, newOptions, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName)
                    col.copy(options = col.options ++ newOptions)
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case AlterColumnFields(columnName, newFields, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName)
                    col.copy(multiFields = newFields.toList)
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case _ => table
        }
      }

    }

    override def validate(): Either[SQL, Unit] = {
      var errors = Seq[String]()
      // check that primary key columns exist
      primaryKey.foreach { pk =>
        if (!cols.contains(pk)) {
          errors = errors :+ s"Primary key column $pk does not exist in table $name"
        }
      }
      // check that partition column exists
      partitionBy.foreach { partition =>
        if (!cols.contains(partition.column)) {
          errors = errors :+ s"Partition column ${partition.column} does not exist in table $name"
        }
      }
      if (errors.isEmpty) Right(()) else Left(errors.mkString("\n"))
    }

    lazy val ddlPipeline: DdlPipeline = DdlPipeline(
      name = s"${name}_ddl_default_pipeline",
      ddlPipelineType = DdlPipelineType.Default,
      ddlProcessors = ddlProcessors
    )

    lazy val indexMappings: ObjectNode = {
      val node = mapper.createObjectNode()
      val fields = mapper.createObjectNode()
      columns.foreach { column =>
        fields.replace(column.name, column.node)
      }
      node.set("properties", fields)
      update(node, mappings)
      if (primaryKey.nonEmpty || partitionBy.nonEmpty) {
        val meta = Option(node.get("_meta")).getOrElse(mapper.createObjectNode())
        if (meta != null && meta.isObject) {
          val metaObj = meta.asInstanceOf[ObjectNode]
          if (primaryKey.nonEmpty) {
            val pkArray = mapper.createArrayNode()
            primaryKey.foreach(pk => pkArray.add(pk))
            metaObj.replace("primary_key", pkArray)
          }
          partitionBy.foreach { partition =>
            val partitionObj = mapper.createObjectNode()
            partitionObj.put("column", partition.column)
            partitionObj.put("granularity", partition.granularity.script.get)
            metaObj.replace("partition_by", partitionObj)
          }
          node.replace("_meta", metaObj)
        }
      }
      node
    }

    lazy val indexSettings: ObjectNode = {
      val node = mapper.createObjectNode()
      val index = mapper.createObjectNode()
      update(index, settings)
      node.set("index", index)
      node
    }

    lazy val pipeline: ObjectNode = {
      ddlPipeline.node
    }
  }

  object DdlTable {
    def apply(index: Index): DdlTable = {
      // 1. Columns from the mapping
      val initialCols: Map[String, DdlColumn] =
        index.mappings.fields.map { field =>
          val name = field.name
          name -> DdlColumn(
            name = name,
            dataType = SQLTypes(field),
            script = None,
            multiFields = field.fields.map(DdlColumn(_)),
            notNull = false, // TODO add required
            defaultValue = field.null_value,
            options = field.options
          )
        }.toMap

      // 2. PK + partition + pipelines from index mappings and settings
      var primaryKey: List[String] = index.mappings.primaryKey
      var partitionBy: Option[DdlPartition] = index.mappings.partitionBy.map { p =>
        val granularity = TimeUnit(p.granularity)
        DdlPartition(p.column, granularity)
      }
      val defaultPipelineName = index.settings.defaultPipeline
      val finalPipelineName = index.settings.finalPipeline

      // 3. Enrichment from the pipeline (if provided)
      val enrichedCols = scala.collection.mutable.Map.from(initialCols)

      index.pipeline.foreach { pipeline =>
        val processorsNode = pipeline.get("processors")
        if (processorsNode != null && processorsNode.isArray) {
          val processors: Seq[DdlProcessor] =
            processorsNode.elements().asScala.toSeq.flatMap(DdlProcessor(_))

          processors.foreach {
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
      }

      // 4. Final construction of the DdlTable
      DdlTable(
        name = index.name,
        columns = enrichedCols.values.toList.sortBy(_.name),
        primaryKey = primaryKey,
        partitionBy = partitionBy,
        defaultPipeline = defaultPipelineName,
        finalPipeline = finalPipelineName
      )
    }
  }
}
