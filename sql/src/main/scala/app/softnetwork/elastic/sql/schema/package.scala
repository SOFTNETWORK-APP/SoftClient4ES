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

import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils, SQLTypes}
import app.softnetwork.elastic.sql.config.ElasticSqlConfig
import app.softnetwork.elastic.sql.query._
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import app.softnetwork.elastic.sql.time.TimeUnit
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode

import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

package object schema {
  val mapper: ObjectMapper = JacksonConfig.objectMapper

  lazy val sqlConfig: ElasticSqlConfig = ElasticSqlConfig()

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
    def apply(n: String): DdlProcessorType = new DdlProcessorType {
      override def name: String = n
    }
  }

  sealed trait DdlProcessor extends DdlToken {
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
    def json: String = mapper.writeValueAsString(node)
    def processorType: DdlProcessorType
    def description: String = sql.trim
    def name: String = processorType.name
    def properties: Map[String, Any]

    private def normalizeValue(v: Any): Any = v match {
      case s: String     => s.trim
      case b: Boolean    => b
      case i: Int        => i
      case l: Long       => l
      case d: Double     => d
      case o: ObjectNode => mapper.writeValueAsString(o) // canonical JSON
      case seq: Seq[_]   => seq.map(normalizeValue) // recursive
      case other         => other
    }

    private def normalizeProperties(): Map[String, Any] = {
      properties
        .filterNot { case (k, _) => k == "description" } // we should not compare description
        .toSeq
        .sortBy(_._1) // order properties by key
        .map { case (k, v) => (k, normalizeValue(v)) }
        .toMap
    }

    def diff(to: DdlProcessor): Option[ProcessorDiff] = {

      val from = this

      // 1. Diff of the type
      val typeChanged: Option[ProcessorTypeChanged] =
        if (from.processorType != to.processorType)
          Some(ProcessorTypeChanged(from.processorType, to.processorType))
        else
          None

      // 2. Diff of the properties
      val fromProps = from.normalizeProperties()
      val toProps = to.normalizeProperties()

      val allKeys = fromProps.keySet ++ toProps.keySet

      val propertyDiffs: List[ProcessorPropertyDiff] =
        allKeys.toList.flatMap { key =>
          (fromProps.get(key), toProps.get(key)) match {

            case (None, Some(v)) =>
              Some(ProcessorPropertyAdded(key, v))

            case (Some(_), None) =>
              Some(ProcessorPropertyRemoved(key))

            case (Some(v1), Some(v2)) if v1 != v2 =>
              Some(ProcessorPropertyChanged(key, v1, v2))

            case _ =>
              None
          }
        }

      // 3. If nothing has changed → None
      if (typeChanged.isEmpty && propertyDiffs.isEmpty)
        None
      else
        Some(ProcessorDiff(typeChanged, propertyDiffs))
    }

    override def ddl: String = s"${processorType.name.toUpperCase}${Value(properties).ddl}"
  }

  object DdlProcessor {
    private val ScriptDescRegex =
      """^\s*([a-zA-Z0-9_\\.]+)\s([a-zA-Z]+)\s+SCRIPT\s+AS\s*\((.*)\)\s*$""".r

    def apply(processorType: DdlProcessorType, properties: ObjectValue): DdlProcessor = {
      val node = mapper.createObjectNode()
      val props = mapper.createObjectNode()
      updateNode(props, properties.value)
      node.set(processorType.name, props)
      apply(node)
    }

    def apply(processor: JsonNode): DdlProcessor = {
      val processorType = processor.fieldNames().next() // "set", "script", "date_index_name", etc.
      val props = processor.get(processorType)

      processorType match {
        case "set" =>
          val field = props.get("field").asText()
          val desc = Option(props.get("description")).map(_.asText()).getOrElse("")
          val valueNode = props.get("value")
          val ignoreFailure = Option(props.get("ignore_failure")).exists(_.asBoolean())

          if (field == "_id") {
            val value =
              valueNode
                .asText()
                .trim
                .stripPrefix("{{")
                .stripSuffix("}}")
                .trim
            // DdlPrimaryKeyProcessor
            val cols = value.split(sqlConfig.compositeKeySeparator).toSet
            DdlPrimaryKeyProcessor(
              sql = desc,
              column = "_id",
              value = cols,
              ignoreFailure = ignoreFailure
            )
          } else {
            DdlDefaultValueProcessor(
              sql = desc,
              column = field,
              value = Value(valueNode.asText()),
              ignoreFailure = ignoreFailure
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

  case class GenericProcessor(processorType: DdlProcessorType, properties: Map[String, Any])
      extends DdlProcessor {
    override def sql: String = ddl
    override def column: String = properties.get("field") match {
      case Some(s: String) => s
      case _               => UUID.randomUUID().toString
    }
    override def ignoreFailure: Boolean = properties.get("ignore_failure") match {
      case Some(b: Boolean) => b
      case _                => false
    }
  }

  case class DdlScriptProcessor(
    script: String,
    column: String,
    dataType: SQLType,
    source: String,
    ignoreFailure: Boolean = true
  ) extends DdlProcessor {
    override def sql: String = s"$column $dataType SCRIPT AS ($script)"

    override def baseType: SQLType = dataType

    def processorType: DdlProcessorType = DdlProcessorType.Script

    override def properties: Map[String, Any] = Map(
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

    override def properties: Map[String, Any] = Map(
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

    override def properties: Map[String, Any] = Map(
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
    separator: String = "\\|\\|"
  ) extends DdlProcessor {
    def processorType: DdlProcessorType = DdlProcessorType.Set

    override def properties: Map[String, Any] = Map(
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
        s"""ctx.${column.split("\\.").mkString("?.")} == null"""
      else
        s"""ctx.$column == null"""
    }

    override def properties: Map[String, Any] = Map(
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

    override def properties: Map[String, Any] = Map(
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
  ) extends DdlToken {
    def sql: String =
      s"CREATE OR REPLACE PIPELINE $name WITH PROCESSORS (${ddlProcessors.map(_.sql.trim).mkString(", ")})"

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

    override def ddl: String =
      s"CREATE OR REPLACE PIPELINE $name WITH PROCESSORS (${ddlProcessors.map(_.ddl.trim).mkString(", ")})"

    def json: String = mapper.writeValueAsString(node)

    def diff(pipeline: DdlPipeline): List[PipelineDiff] = {

      val actual = this.ddlProcessors

      val desired = pipeline.ddlProcessors

      // 1. Index processors by logical key
      def key(p: DdlProcessor) = (p.processorType, p.column)

      val desiredMap = desired.map(p => key(p) -> p).toMap
      val actualMap = actual.map(p => key(p) -> p).toMap

      val diffs = scala.collection.mutable.ListBuffer[PipelineDiff]()

      // 2. Added processors
      for ((k, p) <- desiredMap if !actualMap.contains(k)) {
        diffs += ProcessorAdded(p)
      }

      // 3. Removed processors
      for ((k, p) <- actualMap if !desiredMap.contains(k)) {
        diffs += ProcessorRemoved(p)
      }

      // 4. Modified processors
      for ((k, from) <- actualMap if desiredMap.contains(k)) {
        val to = desiredMap(k)
        from.diff(to) match {
          case Some(d) => diffs += ProcessorChanged(from, to, d)
          case None    => // identical → nothing
        }
      }

      // 5. Optional: detect reordering for processors where order matters
      diffs.toList
    }
  }

  private[schema] def updateNode(node: ObjectNode, updates: Map[String, Value[_]]): ObjectNode = {
    updates.foreach { case (k, v) =>
      v match {
        case Null                 => node.putNull(k)
        case BooleanValue(b)      => node.put(k, b)
        case StringValue(s)       => node.put(k, s)
        case ByteValue(b)         => node.put(k, b)
        case ShortValue(s)        => node.put(k, s)
        case IntValue(i)          => node.put(k, i)
        case LongValue(l)         => node.put(k, l)
        case DoubleValue(d)       => node.put(k, d)
        case FloatValue(f)        => node.put(k, f)
        case IdValue              => node.put(k, s"${v.value}")
        case IngestTimestampValue => node.put(k, s"${v.value}")
        case v: Values[_, _] =>
          val arrayNode = mapper.createArrayNode()
          v.values.foreach {
            case Null            => arrayNode.addNull()
            case BooleanValue(b) => arrayNode.add(b)
            case StringValue(s)  => arrayNode.add(s)
            case ByteValue(b)    => arrayNode.add(b)
            case ShortValue(s)   => arrayNode.add(s)
            case IntValue(i)     => arrayNode.add(i)
            case LongValue(l)    => arrayNode.add(l)
            case DoubleValue(d)  => arrayNode.add(d)
            case FloatValue(f)   => arrayNode.add(f)
            case ObjectValue(o)  => arrayNode.add(updateNode(mapper.createObjectNode(), o))
            case _               => // do nothing
          }
          node.set(k, arrayNode)
        case ObjectValue(value) =>
          if (value.nonEmpty)
            node.set(k, updateNode(mapper.createObjectNode(), value))
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
    defaultValue: Option[Value[_]] = None,
    notNull: Boolean = false,
    comment: Option[String] = None,
    options: Map[String, Value[_]] = Map.empty,
    struct: Option[DdlColumn] = None
  ) extends DdlToken {
    def path: String = struct.map(st => s"${st.name}.$name").getOrElse(name)
    private def level: Int = struct.map(_.level + 1).getOrElse(0)
    def update(struct: Option[DdlColumn] = None): DdlColumn = {
      val updated = this.copy(struct = struct)
      val updated_script =
        script.map { sc =>
          sc.copy(
            column = updated.path,
            source = sc.source.replace(s"ctx.$name", s"ctx.${updated.path}")
          )
        }
      val sql_script: Option[ObjectValue] =
        updated_script match {
          case Some(s) =>
            Some(
              ObjectValue(
                Map(
                  "sql"      -> StringValue(s.script),
                  "column"   -> StringValue(updated.path),
                  "painless" -> StringValue(s.source)
                )
              )
            )
          case _ => None
        }
      updated.copy(
        multiFields = multiFields.map { field =>
          field.update(Some(updated))
        },
        script = updated_script,
        options = options ++ Map(
          "meta" -> ObjectValue(
            options.get("meta") match {
              case Some(ObjectValue(value)) =>
                value ++ Map("not_null" -> BooleanValue(notNull)) ++ comment.map(ct =>
                  "comment" -> StringValue(ct)
                ) ++ sql_script.map(st => "script" -> st)
              case _ =>
                Map("not_null" -> BooleanValue(notNull)) ++ comment.map(ct =>
                  "comment" -> StringValue(ct)
                ) ++ sql_script.map(st => "script" -> st)
            }
          )
        )
      )
    }
    def sql: String = {
      val opts = if (options.nonEmpty) {
        s" OPTIONS ${ObjectValue(options).ddl}"
      } else {
        ""
      }
      val defaultOpt = defaultValue.map(v => s" DEFAULT ${v.sql}").getOrElse("")
      val notNullOpt = if (notNull) " NOT NULL" else ""
      val commentOpt = comment.map(c => s" COMMENT '$c'").getOrElse("")
      val fieldsOpt = if (multiFields.nonEmpty) {
        s" FIELDS (\n\t${multiFields.mkString(s",\n\t")}\n\t)"
      } else {
        ""
      }
      val scriptOpt = script.map(s => s" SCRIPT AS (${s.script})").getOrElse("")
      val tabs = "\t" * level
      s"$tabs$name $dataType$fieldsOpt$scriptOpt$defaultOpt$notNullOpt$commentOpt$opts"
    }

    def ddlProcessors: Seq[DdlProcessor] = script.map(st => st.copy(column = path)).toSeq ++
      defaultValue.map { dv =>
        DdlDefaultValueProcessor(
          sql = s"$path DEFAULT $dv",
          column = path,
          value = dv
        )
      }.toSeq ++ multiFields.flatMap(_.ddlProcessors)

    def node: ObjectNode = {
      val root = mapper.createObjectNode()
      val esType = SQLTypeUtils.elasticType(dataType)
      root.put("type", esType)
      defaultValue.foreach { dv =>
        updateNode(root, Map("null_value" -> dv))
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
      updateNode(root, options)
      root
    }

    def diff(desired: DdlColumn, parent: Option[DdlColumn] = None): List[ColumnDiff] = {
      val actual = this
      val diffs = scala.collection.mutable.ListBuffer[ColumnDiff]()

      // 1. Type
      if (SQLTypeUtils.elasticType(actual.dataType) != SQLTypeUtils.elasticType(desired.dataType))
        diffs += ColumnTypeChanged(path, actual.dataType, desired.dataType)

      // 2. Default
      (actual.defaultValue, desired.defaultValue) match {
        case (None, Some(v)) => diffs += ColumnDefaultSet(path, v)
        case (Some(_), None) => diffs += ColumnDefaultRemoved(path)
        case (Some(a), Some(b)) if a != b =>
          diffs += ColumnDefaultSet(path, b)
        case _ =>
      }

      // 3. Script
      (actual.script, desired.script) match {
        case (None, Some(s)) => diffs += ColumnScriptSet(path, s)
        case (Some(_), None) => diffs += ColumnScriptRemoved(path)
        case (Some(a), Some(b)) if a.sql != b.sql =>
          diffs += ColumnScriptSet(path, b)
        case _ =>
      }

      // 4. Comment
      (actual.comment, desired.comment) match {
        case (None, Some(c)) => diffs += ColumnCommentSet(path, c)
        case (Some(_), None) => diffs += ColumnCommentRemoved(path)
        case (Some(a), Some(b)) if a != b =>
          diffs += ColumnCommentSet(path, b)
        case _ =>
      }

      // 5. Not Null
      if (actual.notNull != desired.notNull) {
        if (desired.notNull) diffs += ColumnNotNullSet(path)
        else diffs += ColumnNotNullRemoved(path)
      }

      // 6. Options
      val allOptions = actual.options.keySet ++ desired.options.keySet
      for (key <- allOptions) {
        (actual.options.get(key), desired.options.get(key)) match {
          case (None, Some(v)) => diffs += ColumnOptionSet(path, key, v)
          case (Some(_), None) => diffs += ColumnOptionRemoved(path, key)
          case (Some(a), Some(b)) if a != b =>
            diffs += ColumnOptionSet(path, key, b)
          case _ =>
        }
      }

      // 7. STRUCT / multi-fields
      val actualFields = actual.multiFields.map(f => f.name -> f).toMap
      val desiredFields = desired.multiFields.map(f => f.name -> f).toMap

      // 7.1. Fields added
      for ((name, f) <- desiredFields if !actualFields.contains(name)) {
        diffs += FieldAdded(path, f)
      }

      // 7.2. Fields removed
      for ((name, f) <- actualFields if !desiredFields.contains(name)) {
        diffs += FieldRemoved(path, name)
      }

      // 7.3. Fields modified
      for ((name, a) <- actualFields if desiredFields.contains(name)) {
        val d = desiredFields(name)
        diffs ++= a.diff(d, Some(desired))
      }

      diffs.toList
    }
  }

  case class DdlPartition(column: String, granularity: TimeUnit = TimeUnit.DAYS) extends DdlToken {
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
    mappings: Map[String, Value[_]] = Map.empty,
    settings: Map[String, Value[_]] = Map.empty,
    processors: Seq[DdlProcessor] = Seq.empty
  ) extends DdlToken {
    private[schema] lazy val cols: Map[String, DdlColumn] = columns.map(c => c.name -> c).toMap

    private lazy val _meta: Map[String, Value[_]] = Map.empty ++ {
      if (primaryKey.nonEmpty)
        Option("primary_key" -> StringValues(primaryKey.map(StringValue)))
      else
        None
    }.toMap ++ partitionBy
      .map(pb =>
        Map(
          "partition_by" -> ObjectValue(
            Map(
              "column"      -> StringValue(pb.column),
              "granularity" -> StringValue(pb.granularity.script.get)
            )
          )
        )
      )
      .getOrElse(Map.empty)

    def update(): DdlTable = this.copy(
      columns = columns.map(_.update()),
      mappings = mappings ++ Map(
        "_meta" ->
        ObjectValue(mappings.get("_meta") match {
          case Some(ObjectValue(value)) =>
            value ++ _meta
          case _ => _meta
        })
      )
    )

    def sql: String = {
      val opts =
        if (mappings.nonEmpty || settings.nonEmpty) {
          val mappingOpts =
            if (mappings.nonEmpty) {
              s"mappings = ${ObjectValue(mappings).ddl}"
            } else {
              ""
            }
          val settingsOpts =
            if (settings.nonEmpty) {
              s"settings = ${ObjectValue(settings).ddl}"
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
          case AlterColumnScript(columnName, newScript, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName)
                    col.copy(script = Some(newScript.copy(dataType = col.dataType)))
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case DropColumnScript(columnName, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName) col.copy(script = None)
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
          case AlterColumnOption(
                columnName,
                optionKey,
                optionValue,
                ifExists
              ) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName)
                    col.copy(options = col.options ++ Map(optionKey -> optionValue))
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case DropColumnOption(
                columnName,
                optionKey,
                ifExists
              ) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName)
                    col.copy(options = col.options - optionKey)
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case AlterColumnComment(columnName, newComment, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName)
                    col.copy(comment = Some(newComment))
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case DropColumnComment(columnName, ifExists) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName)
                    col.copy(comment = None)
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
          case AlterColumnField(
                columnName,
                field,
                ifExists
              ) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName) {
                    val updatedFields = if (col.multiFields.exists(_.name == field.name)) {
                      col.multiFields.map { f =>
                        if (f.name == field.name) field else f
                      }
                    } else {
                      col.multiFields :+ field
                    }
                    col.copy(multiFields = updatedFields)
                  } else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case DropColumnField(
                columnName,
                fieldName,
                ifExists
              ) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName)
                    col.copy(
                      multiFields = col.multiFields.filterNot(_.name == fieldName)
                    )
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case AlterColumnOption(
                columnName,
                optionKey,
                optionValue,
                ifExists
              ) =>
            if (ifExists && !table.cols.contains(columnName)) table
            else if (table.cols.contains(columnName))
              table.copy(
                columns = table.columns.map { col =>
                  if (col.name == columnName)
                    col.copy(
                      options = col.options ++ Map(optionKey -> optionValue)
                    )
                  else col
                }
              )
            else throw DdlColumnNotFound(columnName, table.name)
          case AlterTableMapping(optionKey, optionValue) =>
            table.copy(
              mappings = table.mappings ++ Map(optionKey -> optionValue)
            )
          case DropTableMapping(optionKey) =>
            table.copy(
              mappings = table.mappings - optionKey
            )
          case AlterTableSetting(optionKey, optionValue) =>
            table.copy(
              settings = table.settings ++ Map(optionKey -> optionValue)
            )
          case DropTableSetting(optionKey) =>
            table.copy(
              settings = table.settings - optionKey
            )
          case _ => table
        }
      }

    }

    override def validate(): Either[String, Unit] = {
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

    lazy val ddlPipeline: DdlPipeline = {
      val processorsFromColumns = ddlProcessors.map(p => p.column -> p).toMap
      DdlPipeline(
        name = s"${name}_ddl_default_pipeline",
        ddlPipelineType = DdlPipelineType.Default,
        ddlProcessors =
          ddlProcessors ++ processors.filterNot(p => processorsFromColumns.contains(p.column))
      )
    }

    lazy val defaultPipeline: String =
      settings
        .get("default_pipeline")
        .map(_.value)
        .flatMap {
          case v: String => Some(v)
          case _         => None
        }
        .getOrElse("_none")

    lazy val finalPipeline: String =
      settings
        .get("final_pipeline")
        .map(_.value)
        .flatMap {
          case v: String => Some(v)
          case _         => None
        }
        .getOrElse("_none")

    lazy val indexMappings: ObjectNode = {
      val node = mapper.createObjectNode()
      val fields = mapper.createObjectNode()
      columns.foreach { column =>
        fields.replace(column.name, column.node)
      }
      node.set("properties", fields)
      updateNode(node, mappings)
      node
    }

    lazy val indexSettings: ObjectNode = {
      val node = mapper.createObjectNode()
      val index = mapper.createObjectNode()
      updateNode(index, settings)
      node.set("index", index)
      node
    }

    lazy val pipeline: ObjectNode = {
      ddlPipeline.node
    }

    def diff(desired: DdlTable): DdlTableDiff = {
      val actual = this.update()
      val desiredUpdated = desired.update()

      val columnDiffs = scala.collection.mutable.ListBuffer[ColumnDiff]()

      // 1. Columns added
      val actualCols = actual.cols
      val desiredCols = desiredUpdated.cols

      for ((name, col) <- desiredCols if !actualCols.contains(name)) {
        columnDiffs += ColumnAdded(col)
      }

      // 2. Columns removed
      for ((name, col) <- actualCols if !desiredCols.contains(name)) {
        columnDiffs += ColumnRemoved(col.name)
      }

      // 3. Columns modified
      for ((name, a) <- actualCols if desiredCols.contains(name)) {
        val d = desiredCols(name)
        columnDiffs ++= a.diff(d)
      }

      // 4. Mappings
      val mappingDiffs = scala.collection.mutable.ListBuffer[MappingDiff]()
      val allMappings = actual.mappings.keySet ++ desiredUpdated.mappings.keySet
      for (key <- allMappings) {
        (actual.mappings.get(key), desiredUpdated.mappings.get(key)) match {
          case (None, Some(v)) => mappingDiffs += MappingSet(key, v)
          case (Some(_), None) => mappingDiffs += MappingRemoved(key)
          case (Some(a), Some(b)) if a != b =>
            mappingDiffs += MappingSet(key, b)
          case _ =>
        }
      }

      // 5. Settings
      val settingDiffs = scala.collection.mutable.ListBuffer[SettingDiff]()
      val allSettings = actual.settings.keySet ++ desiredUpdated.settings.keySet
      for (key <- allSettings) {
        (actual.settings.get(key), desiredUpdated.settings.get(key)) match {
          case (None, Some(v)) => settingDiffs += SettingSet(key, v)
          case (Some(_), None) => settingDiffs += SettingRemoved(key)
          case (Some(a), Some(b)) if a != b =>
            settingDiffs += SettingSet(key, b)
          case _ =>
        }
      }

      // 6. Pipeline
      val pipelineDiffs = scala.collection.mutable.ListBuffer[PipelineDiff]()
      val actualPipeline = actual.ddlPipeline
      val desiredPipeline = desiredUpdated.ddlPipeline
      actualPipeline.diff(desiredPipeline).foreach { d =>
        pipelineDiffs += d
      }

      DdlTableDiff(
        columns = columnDiffs.toList,
        mappings = mappingDiffs.toList,
        settings = settingDiffs.toList,
        pipeline = pipelineDiffs.toList
      )
    }
  }

}
