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
import app.softnetwork.elastic.sql.function.aggregate.{
  AggregateFunction,
  AvgAgg,
  CountAgg,
  MaxAgg,
  MinAgg,
  SumAgg
}
import app.softnetwork.elastic.sql.query._
import app.softnetwork.elastic.sql.serialization._
import app.softnetwork.elastic.sql.time.TimeUnit
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode

import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

package object schema {
  val mapper: ObjectMapper = JacksonConfig.objectMapper

  type Schema = Table

  lazy val sqlConfig: ElasticSqlConfig = ElasticSqlConfig()

  sealed trait IngestProcessorType {
    def name: String
  }

  object IngestProcessorType {
    case object Script extends IngestProcessorType {
      val name: String = "script"
    }
    case object Rename extends IngestProcessorType {
      val name: String = "rename"
    }
    case object Remove extends IngestProcessorType {
      val name: String = "remove"
    }
    case object Set extends IngestProcessorType {
      val name: String = "set"
    }
    case object DateIndexName extends IngestProcessorType {
      val name: String = "date_index_name"
    }
    case object Enrich extends IngestProcessorType {
      val name: String = "enrich"
    }
    def apply(n: String): IngestProcessorType = new IngestProcessorType {
      override val name: String = n
    }
  }

  sealed trait IngestProcessor extends DdlToken {
    def column: String
    def ignoreFailure: Boolean
    final def node: ObjectNode = {
      val node = mapper.createObjectNode()
      node.set(processorType.name, properties)
      node
    }
    def json: String = mapper.writeValueAsString(node)
    def pipelineType: IngestPipelineType
    def processorType: IngestProcessorType
    def sql: String // = s"${processorType.name.toUpperCase}${Value(properties).ddl}"
    def description: Option[String]
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

    def diff(to: IngestProcessor): Option[ProcessorDiff] = {

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

  object IngestProcessor {
    private val ScriptDescRegex =
      """^\s*([a-zA-Z0-9_\\.]+)\s([a-zA-Z]+)\s+SCRIPT\s+AS\s*\((.*)\)\s*$""".r

    def apply(processorType: IngestProcessorType, properties: ObjectValue): IngestProcessor = {
      val node = mapper.createObjectNode()
      node.set(processorType.name, properties.toJson)
      apply(IngestPipelineType.Default, node)
    }

    def apply(pipelineType: IngestPipelineType, processor: JsonNode): IngestProcessor = {
      val processorType = processor.fieldNames().next() // "set", "script", "date_index_name", etc.
      val props = processor.get(processorType)

      processorType match {
        case IngestProcessorType.Set.name =>
          val field = props.get("field").asText()
          val desc = Option(props.get("description")).map(_.asText())
          val valueNode = Option(props.get("value"))
          val ignoreFailure = Option(props.get("ignore_failure")).exists(_.asBoolean())

          if (field == "_id" && valueNode.isDefined) {
            val value =
              valueNode.get
                .asText()
                .trim
                .stripPrefix("{{")
                .stripSuffix("}}")
                .trim
            // DdlPrimaryKeyProcessor
            val cols = value.split(sqlConfig.compositeKeySeparator).toSet
            PrimaryKeyProcessor(
              pipelineType = pipelineType,
              description = desc,
              column = "_id",
              value = cols,
              ignoreFailure = ignoreFailure
            )
          } else {
            val copyFrom = Option(props.get("copy_from")).map(_.asText())
            val doOverride = Option(props.get("override")).map(_.asBoolean())
            val ignoreEmptyValue = Option(props.get("ignore_empty_value")).map(_.asBoolean())
            val doIf =
              if (props.has("if")) {
                val cond = props.get("if")
                if (cond.has("source")) {
                  Option(cond.get("source")).map(_.asText())
                } else Option(cond).map(_.asText())
              } else None
            SetProcessor(
              pipelineType = pipelineType,
              description = desc,
              column = field,
              value = valueNode.map(v => Value(v.asText())).getOrElse(Null),
              copyFrom = copyFrom,
              doOverride = doOverride,
              ignoreEmptyValue = ignoreEmptyValue,
              doIf = doIf match {
                case Some(condition) if condition.nonEmpty => Some(condition)
                case _                                     => None
              },
              ignoreFailure = ignoreFailure
            )
          }

        case IngestProcessorType.Script.name =>
          val desc = Option(props.get("description")).map(_.asText())
          val lang = props.get("lang").asText()
          require(lang == "painless", s"Only painless supported, got $lang")
          val source = props.get("source").asText()
          val ignoreFailure = Option(props.get("ignore_failure")).exists(_.asBoolean())

          desc match {
            case Some(ScriptDescRegex(col, dataType, script)) =>
              ScriptProcessor(
                pipelineType = pipelineType,
                description = desc,
                script = script,
                column = col,
                dataType = SQLTypes(dataType),
                source = source,
                ignoreFailure = ignoreFailure
              )
            case _ =>
              GenericProcessor(
                pipelineType = pipelineType,
                processorType = IngestProcessorType.Script,
                properties =
                  mapper.convertValue(props, classOf[java.util.Map[String, Object]]).asScala.toMap
              )
          }

        case IngestProcessorType.DateIndexName.name =>
          val field = props.get("field").asText()
          val desc = Option(props.get("description")).map(_.asText())
          val rounding = props.get("date_rounding").asText()
          val formats = Option(props.get("date_formats"))
            .map(_.elements().asScala.toList.map(_.asText()))
            .getOrElse(Nil)
          val prefix = props.get("index_name_prefix").asText()

          DateIndexNameProcessor(
            pipelineType = pipelineType,
            description = desc,
            column = field,
            dateRounding = rounding,
            dateFormats = formats,
            prefix = prefix
          )

        case IngestProcessorType.Remove.name =>
          val field = props.get("field").asText()
          val desc = Option(props.get("description")).map(_.asText())

          RemoveProcessor(
            pipelineType = pipelineType,
            description = desc,
            column = field
          )

        case IngestProcessorType.Rename.name =>
          val field = props.get("field").asText()
          val desc = Option(props.get("description")).map(_.asText())
          val targetField = props.get("target_field").asText()

          RenameProcessor(
            pipelineType = pipelineType,
            description = desc,
            column = field,
            newName = targetField
          )

        case IngestProcessorType.Enrich.name =>
          val field = props.get("field").asText()
          val desc = Option(props.get("description")).map(_.asText())
          val policyName = props.get("policy_name").asText()
          val targetField = props.get("target_field").asText()
          val maxMatches = Option(props.get("max_matches")).map(_.asInt()).getOrElse(1)
          EnrichProcessor(
            pipelineType = pipelineType,
            description = desc,
            column = targetField,
            policyName = policyName,
            field = field,
            maxMatches = maxMatches
          )

        case other =>
          GenericProcessor(
            pipelineType = pipelineType,
            processorType = IngestProcessorType(other),
            properties =
              mapper.convertValue(props, classOf[java.util.Map[String, Object]]).asScala.toMap
          )

      }
    }
  }

  case class GenericProcessor(
    pipelineType: IngestPipelineType = IngestPipelineType.Default,
    processorType: IngestProcessorType,
    properties: Map[String, Any]
  ) extends IngestProcessor {
    override def description: Option[String] = properties.get("description") match {
      case Some(s: String) => Some(s)
      case _               => None
    }

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

  case class ScriptProcessor(
    pipelineType: IngestPipelineType = IngestPipelineType.Default,
    description: Option[String] = None,
    script: String,
    column: String,
    dataType: SQLType,
    source: String,
    ignoreFailure: Boolean = true
  ) extends IngestProcessor {
    override def sql: String = s"$column $dataType SCRIPT AS ($script)"

    override def baseType: SQLType = dataType

    def processorType: IngestProcessorType = IngestProcessorType.Script

    override def properties: Map[String, Any] = Map(
      "description"    -> description.getOrElse(sql),
      "lang"           -> "painless",
      "source"         -> source,
      "ignore_failure" -> ignoreFailure
    )

  }

  case class RenameProcessor(
    pipelineType: IngestPipelineType = IngestPipelineType.Default,
    description: Option[String] = None,
    column: String,
    newName: String,
    ignoreFailure: Boolean = true,
    ignoreMissing: Option[Boolean] = None
  ) extends IngestProcessor {
    def processorType: IngestProcessorType = IngestProcessorType.Rename

    override def sql: String = s"$column RENAME TO $newName"

    override def properties: Map[String, Any] = Map(
      "description"    -> description.getOrElse(sql),
      "field"          -> column,
      "target_field"   -> newName,
      "ignore_failure" -> ignoreFailure
    ) ++ ignoreMissing
      .map("ignore_missing" -> _)
      .toMap

  }

  case class RemoveProcessor(
    pipelineType: IngestPipelineType = IngestPipelineType.Default,
    description: Option[String] = None,
    column: String,
    ignoreFailure: Boolean = true,
    ignoreMissing: Option[Boolean] = None
  ) extends IngestProcessor {
    def processorType: IngestProcessorType = IngestProcessorType.Remove

    override def sql: String = s"REMOVE $column"

    override def properties: Map[String, Any] = Map(
      "description"    -> description.getOrElse(sql),
      "field"          -> column,
      "ignore_failure" -> ignoreFailure
    ) ++ ignoreMissing
      .map("ignore_missing" -> _)
      .toMap

  }

  case class PrimaryKeyProcessor(
    pipelineType: IngestPipelineType = IngestPipelineType.Default,
    description: Option[String] = None,
    column: String,
    value: Set[String],
    ignoreFailure: Boolean = false,
    ignoreEmptyValue: Option[Boolean] = Some(false),
    separator: String = "\\|\\|"
  ) extends IngestProcessor {
    def processorType: IngestProcessorType = IngestProcessorType.Set

    override def sql: String = s"PRIMARY KEY (${value.mkString(", ")})"

    override def properties: Map[String, Any] = Map(
      "description"    -> description.getOrElse(sql),
      "field"          -> column,
      "value"          -> value.mkString("{{", separator, "}}"),
      "ignore_failure" -> ignoreFailure
    ) ++ ignoreEmptyValue
      .map("ignore_empty_value" -> _)
      .toMap

  }

  case class SetProcessor(
    pipelineType: IngestPipelineType = IngestPipelineType.Default,
    description: Option[String] = None,
    column: String,
    value: Value[_],
    copyFrom: Option[String] = None,
    doOverride: Option[Boolean] = None,
    ignoreEmptyValue: Option[Boolean] = None,
    doIf: Option[String] = None,
    ignoreFailure: Boolean = true
  ) extends IngestProcessor {
    def processorType: IngestProcessorType = IngestProcessorType.Set

    def isDefault: Boolean = copyFrom.isEmpty && value != Null && (doIf match {
      case Some(i) if i.contains(s"ctx.$column == null") => true
      case _                                             => false
    })

    override def sql: String = {
      if (isDefault)
        return s"$column SET DEFAULT ${value.sql}"
      val base = copyFrom match {
        case Some(source) => s"$column COPY FROM $source"
        case None         => s"$column SET VALUE ${value.sql}"
      }
      val withOverride = doOverride match {
        case Some(overrideValue) => s"$base OVERRIDE $overrideValue"
        case None                => base
      }
      val withIgnoreEmpty = ignoreEmptyValue match {
        case Some(ignoreEmpty) => s"$withOverride IGNORE EMPTY VALUE $ignoreEmpty"
        case None              => withOverride
      }
      val withIf = doIf match {
        case Some(condition) => s"$withIgnoreEmpty IF $condition"
        case None            => withIgnoreEmpty
      }
      withIf
    }

    lazy val defaultValue: Option[Any] = {
      if (copyFrom.isDefined) None
      else
        value match {
          case IdValue | IngestTimestampValue => Some(s"{{${value.value}}}")
          case Null                           => None
          case _                              => Some(value.value)
        }
    }

    override def properties: Map[String, Any] = {
      Map(
        "description"               -> description.getOrElse(sql),
        "field"                     -> column,
        "ignore_failure"            -> ignoreFailure
      ) ++ defaultValue.map("value" -> _) ++
      copyFrom.map("copy_from" -> _) ++
      doOverride.map("override" -> _) ++
      doIf.map("if" -> _) ++
      ignoreEmptyValue.map("ignore_empty_value" -> _)
    }

    override def validate(): Either[String, Unit] = {
      for {
        _ <-
          if (value != Null && copyFrom.isDefined)
            Left(
              s"Only one of value or copy_from should be defined for SET processor on column $column"
            )
          else Right(())
      } yield ()
    }
  }

  case class DateIndexNameProcessor(
    pipelineType: IngestPipelineType = IngestPipelineType.Default,
    description: Option[String] = None,
    column: String,
    dateRounding: String,
    dateFormats: List[String],
    prefix: String,
    ignoreFailure: Boolean = true
  ) extends IngestProcessor {
    def processorType: IngestProcessorType = IngestProcessorType.DateIndexName

    override def sql: String = s"PARTITION BY $column (${TimeUnit(dateRounding).sql})"

    override def properties: Map[String, Any] = Map(
      "description"       -> description.getOrElse(sql),
      "field"             -> column,
      "date_rounding"     -> dateRounding,
      "date_formats"      -> dateFormats,
      "index_name_prefix" -> prefix,
      "ignore_failure"    -> ignoreFailure
    )

  }

  implicit def primaryKeyToDdlProcessor(
    primaryKey: List[String]
  ): Seq[IngestProcessor] = {
    if (primaryKey.nonEmpty) {
      Seq(
        PrimaryKeyProcessor(
          column = "_id",
          value = primaryKey.toSet,
          separator = sqlConfig.compositeKeySeparator
        )
      )
    } else {
      Nil
    }
  }

  case class EnrichProcessor(
    pipelineType: IngestPipelineType = IngestPipelineType.Default,
    description: Option[String] = None,
    column: String,
    policyName: String,
    field: String,
    maxMatches: Int = 1,
    ignoreFailure: Boolean = true
  ) extends IngestProcessor {
    def processorType: IngestProcessorType = IngestProcessorType.Enrich

    def targetField: String = column

    override def sql: String = s"ENRICH USING POLICY $policyName FROM $field INTO $targetField"

    override def properties: Map[String, Any] = Map(
      "description"    -> description.getOrElse(sql),
      "policy_name"    -> policyName,
      "field"          -> field,
      "target_field"   -> targetField,
      "max_matches"    -> maxMatches,
      "ignore_failure" -> ignoreFailure
    )

  }

  sealed trait IngestPipelineType {
    def name: String
  }

  object IngestPipelineType {
    case object Default extends IngestPipelineType {
      val name: String = "DEFAULT"
    }
    case object Final extends IngestPipelineType {
      val name: String = "FINAL"
    }
    case object Custom extends IngestPipelineType {
      val name: String = "CUSTOM"
    }
  }

  case class IngestPipeline(
    name: String,
    pipelineType: IngestPipelineType,
    processors: Seq[IngestProcessor]
  ) extends DdlToken {
    def sql: String =
      s"CREATE OR REPLACE PIPELINE $name WITH PROCESSORS (${processors.map(_.sql.trim).mkString(", ")})"

    def node: ObjectNode = {
      val node = mapper.createObjectNode()
      val processorsNode = mapper.createArrayNode()
      processors.foreach { processor =>
        processorsNode.add(processor.node)
        ()
      }
      node.put("description", sql)
      node.set("processors", processorsNode)
      node
    }

    override def ddl: String =
      s"CREATE OR REPLACE PIPELINE $name WITH PROCESSORS (${processors.map(_.ddl.trim).mkString(", ")})"

    def json: String = mapper.writeValueAsString(node)

    def diff(pipeline: IngestPipeline): List[PipelineDiff] = {

      val actual = this.processors

      val desired = pipeline.processors

      // 1. Index processors by logical key
      def key(p: IngestProcessor) = s"${p.pipelineType.name}-${p.processorType.name}-${p.column}"

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

    def merge(statements: Seq[AlterPipelineStatement]): IngestPipeline = {
      statements.foldLeft(this) { (current, alter) =>
        alter match {
          case AddPipelineProcessor(processor) =>
            current.copy(processors =
              current.processors.filterNot(p =>
                p.processorType == processor.processorType && p.column == processor.column
              ) :+ processor
            )
          case DropPipelineProcessor(processorType, column) =>
            current.copy(processors =
              current.processors.filterNot(p =>
                p.processorType == processorType && p.column == column
              )
            )
          case AlterPipelineProcessor(processor) =>
            current.copy(processors = current.processors.map { p =>
              if (p.processorType == processor.processorType && p.column == processor.column) {
                processor
              } else {
                p
              }
            })
        }
      }
    }

    def merge(other: IngestPipeline): IngestPipeline = {
      other.processors.foldLeft(this) { (current, processor) =>
        current.copy(processors =
          current.processors.filterNot(p =>
            p.processorType == processor.processorType && p.column == processor.column
          ) :+ processor
        )
      }
    }
  }

  object IngestPipeline {
    def apply(
      name: String,
      json: String,
      pipelineType: Option[IngestPipelineType] = None
    ): IngestPipeline = {
      val ddlPipelineType = pipelineType.getOrElse(
        if (name.startsWith("default-")) IngestPipelineType.Default
        else if (name.startsWith("final-")) IngestPipelineType.Final
        else IngestPipelineType.Custom
      )
      val node = mapper.readTree(json)
      val processorsNode = node.get("processors")
      val processors = processorsNode.elements().asScala.toSeq.map { p =>
        IngestProcessor(ddlPipelineType, p)
      }
      IngestPipeline(
        name = name,
        pipelineType = ddlPipelineType,
        processors = processors
      )
    }
  }

  sealed trait EnrichPolicyType {
    def name: String
    override def toString: String = name
  }

  object EnrichPolicyType {
    case object Match extends EnrichPolicyType {
      val name: String = "MATCH"
    }
    case object GeoMatch extends EnrichPolicyType {
      val name: String = "GEO_MATCH"
    }
    case object Range extends EnrichPolicyType {
      val name: String = "RANGE"
    }
  }

  case class EnrichPolicy(
    name: String,
    policyType: EnrichPolicyType = EnrichPolicyType.Match,
    indices: Seq[String],
    matchField: String,
    enrichFields: List[String],
    criteria: Option[Criteria] = None
  ) extends DdlToken {
    def sql: String =
      s"CREATE OR REPLACE ENRICH POLICY $name TYPE $policyType WITH SOURCE INDICES ${indices
        .mkString(",")} MATCH FIELD $matchField ENRICH FIELDS (${enrichFields.mkString(", ")})${Where(criteria)}"

    def node(implicit criteriaToNode: Criteria => JsonNode): JsonNode = {
      val node = mapper.createObjectNode()
      node.put("name", name)
      node.put("type", policyType.name)
      val indicesNode = mapper.createArrayNode()
      indices.foreach { index =>
        indicesNode.add(index)
        ()
      }
      node.set("indices", indicesNode)
      node.put("match_field", matchField)
      val enrichFieldsNode = mapper.createArrayNode()
      enrichFields.foreach { field =>
        enrichFieldsNode.add(field)
        ()
      }
      node.set("enrich_fields", enrichFieldsNode)
      criteria.foreach { c =>
        node.set("query", implicitly[JsonNode](c))
      }
      node
    }
  }

  // ==================== Transform ====================

  sealed trait TransformTimeUnit extends DdlToken {
    def name: String
  }

  object TransformTimeUnit {
    case object Milliseconds extends TransformTimeUnit {
      val name: String = "MILLISECONDS"
      override def sql: String = "ms"
    }

    case object Seconds extends TransformTimeUnit {
      val name: String = "SECONDS"
      override def sql: String = "s"
    }

    case object Minutes extends TransformTimeUnit {
      val name: String = "MINUTES"
      override def sql: String = "m"
    }

    case object Hours extends TransformTimeUnit {
      val name: String = "HOURS"
      override def sql: String = "h"
    }

    case object Days extends TransformTimeUnit {
      val name: String = "DAYS"
      override def sql: String = "d"
    }

    case object Weeks extends TransformTimeUnit {
      val name: String = "WEEKS"
      override def sql: String = "w"
    }

    def apply(name: String): TransformTimeUnit = name.toUpperCase() match {
      case "MILLISECONDS" => Milliseconds
      case "SECONDS"      => Seconds
      case "MINUTES"      => Minutes
      case "HOURS"        => Hours
      case "DAYS"         => Days
      case "WEEKS"        => Weeks
      case other          => throw new IllegalArgumentException(s"Invalid delay unit: $other")
    }
  }

  sealed trait TransformTimeInterval extends DdlToken {
    def interval: Int
    def timeUnit: TransformTimeUnit
    def toSeconds: Int = timeUnit match {
      case TransformTimeUnit.Milliseconds => interval / 1000
      case TransformTimeUnit.Seconds      => interval
      case TransformTimeUnit.Minutes      => interval * 60
      case TransformTimeUnit.Hours        => interval * 3600
      case TransformTimeUnit.Days         => interval * 86400
      case TransformTimeUnit.Weeks        => interval * 604800
    }
    def toTransformFormat: String = s"$interval$timeUnit"

  }

  object TransformTimeInterval {

    /** Creates a time interval from seconds */
    def fromSeconds(seconds: Int): (TransformTimeUnit, Int) = {
      if (seconds >= 86400 && seconds % 86400 == 0) {
        (TransformTimeUnit.Days, seconds / 86400)
      } else if (seconds >= 3600 && seconds % 3600 == 0) {
        (TransformTimeUnit.Hours, seconds / 3600)
      } else if (seconds >= 60 && seconds % 60 == 0) {
        (TransformTimeUnit.Minutes, seconds / 60)
      } else {
        (TransformTimeUnit.Seconds, seconds)
      }
    }
  }

  case class Delay(
    timeUnit: TransformTimeUnit,
    interval: Int
  ) extends TransformTimeInterval {
    def sql: String = s"WITH DELAY $interval $timeUnit"
  }

  object Delay {
    val Default: Delay = Delay(TransformTimeUnit.Minutes, 1)

    def fromSeconds(seconds: Int): Delay = {
      val timeInterval = TransformTimeInterval.fromSeconds(seconds)
      Delay(timeInterval._1, timeInterval._2)
    }

    /** Calculates optimal delay based on frequency and number of stages
      *
      * Formula: delay = frequency / (nb_stages * buffer_factor)
      *
      * This ensures the complete chain can refresh within the specified frequency. The buffer
      * factor adds safety margin for processing time.
      *
      * @param frequency
      *   Desired refresh frequency
      * @param nbStages
      *   Total number of stages (changelog + enrichment + aggregate)
      * @param bufferFactor
      *   Safety factor (default 1.5)
      * @return
      *   Optimal delay for each Transform, or error if constraints cannot be met
      */
    def calculateOptimal(
      frequency: Frequency,
      nbStages: Int,
      bufferFactor: Double = 1.5
    ): Either[String, Delay] = {
      if (nbStages <= 0) {
        return Left("Number of stages must be positive")
      }

      val frequencySeconds = frequency.toSeconds
      val optimalDelaySeconds = (frequencySeconds / (nbStages * bufferFactor)).toInt

      // Validate constraints
      if (optimalDelaySeconds < 10) {
        Left(
          s"Calculated delay ($optimalDelaySeconds seconds) is too small. " +
          s"Consider increasing frequency or reducing number of stages. " +
          s"Minimum required frequency: ${nbStages * bufferFactor * 10} seconds"
        )
      } else if (optimalDelaySeconds > frequencySeconds / 2) {
        Left(
          s"Calculated delay ($optimalDelaySeconds seconds) is too large. " +
          s"Each stage needs at least delay × 2 = frequency."
        )
      } else {
        Right(Delay.fromSeconds(optimalDelaySeconds))
      }
    }

    /** Validates that a chain of transforms can refresh within the given frequency
      *
      * Requirements:
      *   - Total latency (delay × nbStages) must be less than frequency
      *   - Each transform runs every (delay × 2), so: delay × 2 × nbStages ≤ frequency
      *
      * @param delay
      *   Delay for each transform
      * @param frequency
      *   Target refresh frequency
      * @param nbStages
      *   Number of stages in the chain
      * @return
      *   Success or error message
      */
    def validate(
      delay: Delay,
      frequency: Frequency,
      nbStages: Int
    ): Either[String, Unit] = {
      val totalLatency = delay.toSeconds * nbStages
      val frequencySeconds = frequency.toSeconds
      val requiredFrequency = delay.toSeconds * 2 * nbStages

      if (totalLatency > frequencySeconds) {
        Left(
          s"Total latency ($totalLatency seconds) exceeds frequency ($frequencySeconds seconds). " +
          s"Minimum required frequency: $requiredFrequency seconds"
        )
      } else if (requiredFrequency > frequencySeconds) {
        Left(
          s"Frequency ($frequencySeconds seconds) is too low for $nbStages stages with delay ${delay.toSeconds} seconds. " +
          s"Minimum required frequency: $requiredFrequency seconds"
        )
      } else {
        Right(())
      }
    }
  }

  case class Frequency(
    timeUnit: TransformTimeUnit,
    interval: Int
  ) extends TransformTimeInterval {
    def sql: String = s"REFRESH EVERY $interval $timeUnit"
  }

  case object Frequency {
    val Default: Frequency = apply(Delay.Default)
    def apply(delay: Delay): Frequency = Frequency(delay.timeUnit, delay.interval * 2)
    def fromSeconds(seconds: Int): Frequency = {
      val timeInterval = TransformTimeInterval.fromSeconds(seconds)
      Frequency(timeInterval._1, timeInterval._2)
    }
  }

  case class TransformSource(
    index: Seq[String],
    query: Option[Criteria]
  ) extends DdlToken {
    override def sql: String = {
      val queryStr = query.map(q => s" WHERE ${q.sql}").getOrElse("")
      s"INDEX (${index.mkString(", ")})$queryStr"
    }

    /** Converts to JSON for Elasticsearch
      */
    def node(implicit criteriaToNode: Criteria => JsonNode): JsonNode = {
      val node = mapper.createObjectNode()
      val indicesNode = mapper.createArrayNode()
      index.foreach(indicesNode.add)
      node.set("index", indicesNode)
      query.foreach { q =>
        node.set("query", implicitly[JsonNode](q))
        ()
      }
      node
    }
  }

  case class TransformDest(
    index: String,
    pipeline: Option[String] = None
  ) extends DdlToken {
    override def sql: String = {
      val pipelineStr = pipeline.map(p => s" PIPELINE $p").getOrElse("")
      s"INDEX $index$pipelineStr"
    }
    def node: JsonNode = {
      val node = mapper.createObjectNode()
      node.put("index", index)
      pipeline.foreach(p => node.put("pipeline", p))
      node
    }
  }

  /** Configuration for bucket selector (HAVING clause)
    */
  case class TransformBucketSelectorConfig(
    name: String = "having_filter",
    bucketsPath: Map[String, String],
    script: String
  ) {
    def node: JsonNode = {
      val node = mapper.createObjectNode()
      val bucketSelectorNode = mapper.createObjectNode()
      val bucketsPathNode = mapper.createObjectNode()
      bucketsPath.foreach { case (k, v) =>
        bucketsPathNode.put(k, v)
        ()
      }
      bucketSelectorNode.set("buckets_path", bucketsPathNode)
      if (script.nonEmpty) {
        bucketSelectorNode.put("script", script)
      }
      node.set("bucket_selector", bucketSelectorNode)
      node
    }
  }

  case class TransformPivot(
    groupBy: Map[String, TransformGroupBy],
    aggregations: Map[String, TransformAggregation],
    bucketSelector: Option[TransformBucketSelectorConfig] = None,
    script: Option[String] = None
  ) extends DdlToken {
    override def sql: String = {
      val groupByStr = groupBy
        .map { case (name, gb) =>
          s"$name BY ${gb.sql}"
        }
        .mkString(", ")

      val aggStr = aggregations
        .map { case (name, agg) =>
          s"$name AS ${agg.sql}"
        }
        .mkString(", ")

      val havingStr = bucketSelector.map(bs => s" HAVING ${bs.script}").getOrElse("")

      s"PIVOT ($groupByStr) AGGREGATE ($aggStr)$havingStr"
    }

    /** Converts to JSON for Elasticsearch
      */
    def node: JsonNode = {
      val node = mapper.createObjectNode()

      val groupByNode = mapper.createObjectNode()
      groupBy.foreach { case (name, gb) =>
        groupByNode.set(name, gb.node)
        ()
      }
      node.set("group_by", groupByNode)

      val aggsNode = mapper.createObjectNode()
      aggregations.foreach { case (name, agg) =>
        aggsNode.set(name, agg.node)
        ()
      }
      bucketSelector.foreach { bs =>
        aggsNode.set(bs.name, bs.node)
        ()
      }
      node.set("aggregations", aggsNode)

      node
    }
  }

  sealed trait TransformGroupBy extends DdlToken {
    def node: JsonNode
  }

  case class TermsGroupBy(field: String) extends TransformGroupBy {
    override def sql: String = s"TERMS($field)"

    override def node: JsonNode = {
      val node = mapper.createObjectNode()
      val termsNode = mapper.createObjectNode()
      termsNode.put("field", field)
      node.set("terms", termsNode)
      node
    }
  }

  sealed trait TransformAggregation extends DdlToken {
    def name: String

    def field: String

    override def sql: String = s"${name.toUpperCase}($field)"

    def node: JsonNode = {
      val node = mapper.createObjectNode()
      val fieldNode = mapper.createObjectNode()
      fieldNode.put("field", field)
      node.set(name.toLowerCase(), fieldNode)
      node
    }
  }

  case class MaxTransformAggregation(field: String) extends TransformAggregation {
    override def name: String = "max"
  }

  case class MinTransformAggregation(field: String) extends TransformAggregation {
    override def name: String = "min"
  }

  case class SumTransformAggregation(field: String) extends TransformAggregation {
    override def name: String = "sum"
  }

  case class AvgTransformAggregation(field: String) extends TransformAggregation {
    override def name: String = "avg"
  }

  case class CountTransformAggregation(field: String) extends TransformAggregation {
    override def name: String = "value_count"

    override def sql: String = if (field == "_id") "COUNT(*)" else s"COUNT($field)"
  }

  case class CardinalityTransformAggregation(field: String) extends TransformAggregation {
    override def name: String = "cardinality"

    override def sql: String = s"COUNT(DISTINCT $field)"
  }

  /** Extension methods for AggregateFunction
    */
  implicit class AggregateConversion(agg: AggregateFunction) {

    /** Converts SQL aggregate function to Elasticsearch aggregation
      */
    def toTransformAggregation: Option[TransformAggregation] = agg match {
      case ma: MaxAgg =>
        Some(MaxTransformAggregation(ma.identifier.name))

      case ma: MinAgg =>
        Some(MinTransformAggregation(ma.identifier.name))

      case sa: SumAgg =>
        Some(SumTransformAggregation(sa.identifier.name))

      case aa: AvgAgg =>
        Some(AvgTransformAggregation(aa.identifier.name))

      case ca: CountAgg =>
        val field = ca.identifier.name
        Some(
          if (field == "*" || field.isEmpty)
            if (ca.isCardinality) CardinalityTransformAggregation("_id")
            else CountTransformAggregation("_id")
          else if (ca.isCardinality) CardinalityTransformAggregation(field)
          else CountTransformAggregation(field)
        )

      case _ =>
        // For other aggregate functions, default to none
        None
    }
  }

  /** Captures the latest value for a field (for changelog snapshots)
    *
    * Uses top_hits with size=1 sorted by a timestamp field This works for ANY field type (text,
    * numeric, date, etc.)
    */
  case class LatestValueTransformAggregation(
    field: String,
    sortBy: String = "_ingest.timestamp" // Default sort by ingest timestamp
  ) extends TransformAggregation {

    override def name: String = "latest_value"

    override def sql: String = s"LAST_VALUE($field)"

    override def node: JsonNode = {
      val node = mapper.createObjectNode()
      val topHitsNode = mapper.createObjectNode()

      // Configure top_hits to get the latest value
      topHitsNode.put("size", 1)

      // Sort by timestamp descending to get latest
      val sortArray = mapper.createArrayNode()
      val sortObj = mapper.createObjectNode()
      val sortFieldObj = mapper.createObjectNode()
      sortFieldObj.put("order", "desc")
      sortObj.set(sortBy, sortFieldObj)
      sortArray.add(sortObj)
      topHitsNode.set("sort", sortArray)

      // Only retrieve the field we need
      val sourceObj = mapper.createObjectNode()
      val includesArray = mapper.createArrayNode()
      includesArray.add(field)
      sourceObj.set("includes", includesArray)
      topHitsNode.set("_source", sourceObj)

      node.set("top_hits", topHitsNode)
      node
    }
  }

  /** Alternative: Use MAX aggregation for compatible types
    *
    * This is simpler but only works for:
    *   - Numeric fields (int, long, double, float)
    *   - Date fields
    *   - Keyword fields (lexicographic max)
    */
  case class MaxValueTransformAggregation(field: String) extends TransformAggregation {
    override def name: String = "max"

    override def sql: String = s"MAX($field)"
  }

  /** Alternative: Use MIN aggregation (for specific use cases)
    */
  case class MinValueTransformAggregation(field: String) extends TransformAggregation {
    override def name: String = "min"

    override def sql: String = s"MIN($field)"
  }

  object ChangelogAggregationStrategy {

    /** Selects the appropriate aggregation for a field in a changelog based on its data type
      *
      * @param field
      *   The field to aggregate
      * @param dataType
      *   The SQL data type of the field
      * @return
      *   The transform aggregation to use
      */
    def selectAggregation(field: String, dataType: SQLType): TransformAggregation = {
      dataType match {
        // Numeric types: Use MAX directly
        case SQLTypes.Int | SQLTypes.BigInt | SQLTypes.Double | SQLTypes.Real =>
          MaxValueTransformAggregation(field)

        // Date/Timestamp: Use MAX directly
        case SQLTypes.Date | SQLTypes.Timestamp =>
          MaxValueTransformAggregation(field)

        // Boolean: Use MAX directly (1=true, 0=false)
        case SQLTypes.Boolean =>
          MaxValueTransformAggregation(field)

        // Keyword: Already a keyword type, use MAX directly
        case SQLTypes.Keyword =>
          MaxValueTransformAggregation(field) // ✅ NO .keyword suffix

        // Text/Varchar: Analyzed field, must use .keyword multi-field
        case SQLTypes.Text | SQLTypes.Varchar =>
          MaxValueTransformAggregation(s"$field.keyword") // ✅ Use .keyword multi-field

        // For complex types, use top_hits as fallback
        case _ =>
          LatestValueTransformAggregation(field)
      }
    }

    /** Determines if a field can use simple MAX aggregation (without needing a multi-field)
      */
    def canUseMaxDirectly(dataType: SQLType): Boolean = dataType match {
      case SQLTypes.Int | SQLTypes.BigInt | SQLTypes.Double | SQLTypes.Real | SQLTypes.Date |
          SQLTypes.Timestamp | SQLTypes.Keyword | SQLTypes.Boolean =>
        true
      case _ => false
    }

    /** Determines if a field needs a .keyword multi-field for aggregation
      */
    def needsKeywordMultiField(dataType: SQLType): Boolean = dataType match {
      case SQLTypes.Text | SQLTypes.Varchar => true
      case _                                => false
    }
  }

  case class TransformSync(time: TransformTimeSync) extends DdlToken {
    override def sql: String = s"SYNC ${time.sql}"

    def node: JsonNode = {
      val node = mapper.createObjectNode()
      node.set("time", time.node)
      node
    }
  }

  case class TransformTimeSync(field: String, delay: Delay) extends DdlToken {
    override def sql: String = s"TIME FIELD $field ${delay.sql}"

    def node: JsonNode = {
      val node = mapper.createObjectNode()
      node.put("field", field)
      node.put("delay", delay.toTransformFormat)
      node
    }
  }

  /** Transform configuration models with built-in JSON serialization
    */
  case class TransformConfig(
    id: String,
    source: TransformSource,
    dest: TransformDest,
    pivot: Option[TransformPivot] = None,
    sync: Option[TransformSync] = None,
    delay: Delay,
    frequency: Frequency
  ) extends DdlToken {

    /** Delay in seconds for display */
    lazy val delaySeconds: Int = delay.toSeconds

    /** Frequency in seconds for display */
    lazy val frequencySeconds: Int = frequency.toSeconds

    /** Converts to Elasticsearch JSON format
      */
    def node(implicit criteriaToNode: Criteria => JsonNode): JsonNode = {
      val node = mapper.createObjectNode()
      node.set("source", source.node)
      node.set("dest", dest.node)
      node.put("frequency", frequency.toTransformFormat)
      sync.foreach { s =>
        node.set("sync", s.node)
        ()
      }
      pivot.foreach { p =>
        node.set("pivot", p.node)
        ()
      }
      node
    }

    /** SQL DDL representation
      */
    override def sql: String = {
      val sb = new StringBuilder()

      sb.append(s"CREATE TRANSFORM $id\n")
      sb.append(s"  SOURCE ${source.sql}\n")
      sb.append(s"  DEST ${dest.sql}\n")

      pivot.foreach { p =>
        sb.append(s"  ${p.sql}\n")
      }

      sync.foreach { s =>
        sb.append(s"  ${s.sql}\n")
      }

      sb.append(s"  ${frequency.sql}\n")
      sb.append(s"  ${delay.sql}")

      sb.toString()
    }

    /** Human-readable summary
      */
    def summary: String = {
      s"Transform $id: ${source.index.mkString(", ")} → ${dest.index} " +
      s"(every ${frequencySeconds}s, delay ${delaySeconds}s)"
    }
  }

  // ==================== Schema ====================

  /** Definition of a column within a table
    *
    * @param name
    *   the column name
    * @param dataType
    *   the column SQL type
    * @param script
    *   optional script processor associated to this column
    * @param multiFields
    *   optional multi fields associated to this column
    * @param defaultValue
    *   optional default value for this column
    * @param notNull
    *   whether this column is not null
    * @param comment
    *   optional comment for this column
    * @param options
    *   optional options for this column (search analyzer, ...)
    * @param struct
    *   optional parent struct column
    */
  case class Column(
    name: String,
    dataType: SQLType,
    script: Option[ScriptProcessor] = None,
    multiFields: List[Column] = Nil,
    defaultValue: Option[Value[_]] = None,
    notNull: Boolean = false,
    comment: Option[String] = None,
    options: Map[String, Value[_]] = Map.empty,
    struct: Option[Column] = None
  ) extends DdlToken {
    def path: String = struct.map(st => s"${st.name}.$name").getOrElse(name)
    private def level: Int = struct.map(_.level + 1).getOrElse(0)

    private def cols: Map[String, Column] = multiFields.map(field => field.name -> field).toMap

    /* Recursive find */
    def find(path: String): Option[Column] = {
      if (path.contains(".")) {
        val parts = path.split("\\.")
        cols.get(parts.head).flatMap(col => col.find(parts.tail.mkString(".")))
      } else {
        cols.get(path)
      }
    }

    def _meta: Map[String, Value[_]] = {
      Map(
        "data_type" -> StringValue(dataType.typeId),
        "not_null"  -> StringValue(s"$notNull")
      ) ++ defaultValue.map(d => "default_value" -> d) ++ comment.map(ct =>
        "comment" -> StringValue(ct)
      ) ++ script
        .map { sc =>
          ObjectValue(
            Map(
              "sql"      -> StringValue(sc.script),
              "column"   -> StringValue(path),
              "painless" -> StringValue(sc.source)
            )
          )
        }
        .map("script" -> _) ++ Map(
        "multi_fields" -> ObjectValue(
          multiFields.map(field => field.name -> ObjectValue(field._meta)).toMap
        )
      )
    }

    def updateStruct(): Column = {
      struct
        .map { st =>
          val updated = st.copy(multiFields = st.multiFields.filterNot(_.name == this.name) :+ this)
          updated.updateStruct()
        }
        .getOrElse(this)
        .update()
    }

    def update(struct: Option[Column] = None): Column = {
      val updated = this.copy(struct = struct)
      // update script accordingly with new path
      val updated_script =
        script.map { sc =>
          sc.copy(
            column = updated.path,
            source = sc.source.replace(s"ctx.$name", s"ctx.${updated.path}")
          )
        }
      updated.copy(
        multiFields = multiFields.map { field =>
          field.update(Some(updated))
        },
        script = updated_script,
        options = options
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

    def asMap: Seq[Map[String, Any]] = Seq(
      Map(
        "name"    -> path,
        "type"    -> dataType.typeId,
        "script"  -> script.map(_.script),
        "default" -> defaultValue.map(_.value).getOrElse(""),
        "notNull" -> notNull,
        "comment" -> comment.getOrElse(""),
        "options" -> ObjectValue(options).ddl
      )
    ) ++ multiFields.flatMap(_.asMap)

    def processors: Seq[IngestProcessor] = script.map(st => st.copy(column = path)).toSeq ++
      defaultValue.map { dv =>
        SetProcessor(
          column = path,
          value = dv,
          doIf = Some {
            if (path.contains("."))
              s"""ctx.${path.split("\\.").mkString("?.")} == null"""
            else
              s"""ctx.$path == null"""
          }
        )
      }.toSeq ++ multiFields.flatMap(_.processors)

    def node: ObjectNode = {
      val root = mapper.createObjectNode()
      val esType = SQLTypeUtils.elasticType(dataType)
      root.put("type", esType)
      dataType match {
        case SQLTypes.Varchar | SQLTypes.Text => // do not set null_value for text types
        case _ =>
          defaultValue.foreach {
            case IngestTimestampValue => () // do not set null_value for ingest timestamp
            case dv => // set null_value for other types
              updateNode(root, Map("null_value" -> dv))
          }
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

    def diff(desired: Column, parent: Option[Column] = None): List[ColumnDiff] = {
      val actual = this
      val diffs = scala.collection.mutable.ListBuffer[ColumnDiff]()

      // 1. Type
      if (SQLTypeUtils.elasticType(actual.dataType) != SQLTypeUtils.elasticType(desired.dataType)) {
        parent match {
          case Some(p) =>
            diffs += FieldAltered(
              p.path,
              desired
            )
          case None => diffs += ColumnTypeChanged(path, actual.dataType, desired.dataType)
        }
      }

      // 2. Default
      (actual.defaultValue, desired.defaultValue) match {
        case (None, Some(v)) =>
          parent match {
            case Some(p) =>
              diffs += FieldAltered(
                p.path,
                desired
              )
            case None =>
              diffs += ColumnDefaultSet(path, v)
          }
        case (Some(_), None) =>
          parent match {
            case Some(p) =>
              diffs += FieldAltered(
                p.path,
                desired
              )
            case None => diffs += ColumnDefaultRemoved(path)
          }
        case (Some(a), Some(b)) if a != b =>
          parent match {
            case Some(p) =>
              diffs += FieldAltered(
                p.path,
                desired
              )
            case None => diffs += ColumnDefaultSet(path, b)
          }
        case _ =>
      }

      // 3. Script
      (actual.script, desired.script) match {
        case (None, Some(s)) =>
          parent match {
            case Some(p) =>
              diffs += FieldAltered(
                p.path,
                desired
              )
            case None =>
              diffs += ColumnScriptSet(path, s)
          }
        case (Some(_), None) =>
          parent match {
            case Some(p) =>
              diffs += FieldAltered(
                p.path,
                desired
              )
            case None => diffs += ColumnScriptRemoved(path)
          }
        case (Some(a), Some(b)) if a.sql != b.sql =>
          parent match {
            case Some(p) =>
              diffs += FieldAltered(
                p.path,
                desired
              )
            case None => diffs += ColumnScriptSet(path, b)
          }
        case _ =>
      }

      // 4. Comment
      (actual.comment, desired.comment) match {
        case (None, Some(c)) =>
          parent match {
            case Some(p) =>
              diffs += FieldAltered(
                p.path,
                desired
              )
            case None => diffs += ColumnCommentSet(path, c)
          }
        case (Some(_), None) =>
          parent match {
            case Some(p) =>
              diffs += FieldAltered(
                p.path,
                desired
              )
            case None => diffs += ColumnCommentRemoved(path)
          }
        case (Some(a), Some(b)) if a != b =>
          parent match {
            case Some(p) =>
              diffs += FieldAltered(
                p.path,
                desired
              )
            case None => diffs += ColumnCommentSet(path, b)
          }
        case _ =>
      }

      // 5. Not Null
      if (actual.notNull != desired.notNull) {
        parent match {
          case Some(p) =>
            diffs += FieldAltered(
              p.path,
              desired
            )
          case None =>
            if (desired.notNull) diffs += ColumnNotNullSet(path)
            else diffs += ColumnNotNullRemoved(path)
        }
      }

      // 6. Options
      val optionDiffs = ObjectValue(actual.options).diff(ObjectValue(desired.options))
      parent match {
        case Some(p) =>
          if (optionDiffs.nonEmpty) {
            diffs += FieldAltered(
              p.path,
              desired
            )
          }
        case None =>
          diffs ++= optionDiffs.map {
            case Altered(name, value) => ColumnOptionSet(path, name, value)
            case Removed(name)        => ColumnOptionRemoved(path, name)
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

  case class PartitionDate(column: String, granularity: TimeUnit = TimeUnit.DAYS) extends DdlToken {
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

    def processor(table: Table): DateIndexNameProcessor =
      DateIndexNameProcessor(
        column = column,
        dateRounding = dateRounding,
        dateFormats = dateFormats,
        prefix = s"${table.name}-"
      )
  }

  case class ColumnNotFound(column: String, table: String)
      extends Exception(s"Column $column  does not exist in table $table")

  case class TableAlias(
    table: String,
    alias: String,
    filter: Map[String, Any] = Map.empty,
    routing: Option[String] = None,
    indexRouting: Option[String] = None,
    searchRouting: Option[String] = None,
    isWriteIndex: Boolean = false,
    isHidden: Boolean = false
  ) {

    def node: ObjectNode = {
      val node = mapper.createObjectNode()
      if (filter.nonEmpty) {
        val filterNode = mapper.valueToTree[JsonNode](filter.asJava)
        node.set("filter", filterNode)
      }
      routing.foreach(r => node.put("routing", r))
      indexRouting.foreach(r => node.put("index_routing", r))
      searchRouting.foreach(r => node.put("search_routing", r))
      if (isWriteIndex) {
        node.put("is_write_index", true)
      }
      if (isHidden) {
        node.put("is_hidden", true)
      }
      node
    }

    def json: String = node

    def ddl: String = {
      val opts = scala.collection.mutable.ListBuffer[String]()
      if (filter.nonEmpty) {
        opts += s"filter = ${Value(filter).ddl}"
      }
      routing.foreach(r => opts += s"routing = '$r'")
      indexRouting.foreach(r => opts += s"index_routing = '$r'")
      searchRouting.foreach(r => opts += s"search_routing = '$r'")
      if (isWriteIndex) {
        opts += s"is_write_index = true"
      }
      if (isHidden) {
        opts += s"is_hidden = true"
      }
      s"ALTER TABLE $table SET ALIAS ($alias = ${if (opts.nonEmpty) s"(${opts.mkString(", ")})"
      else "()"})"
    }
  }

  object TableAlias {
    def apply(table: String, alias: String, value: Value[_]): TableAlias = {
      val obj = value.asInstanceOf[ObjectValue]
      val filter = obj.value.get("filter") match {
        case Some(ObjectValue(f)) =>
          f.map { case (k, v) => k -> v.value }
        case _ => Map.empty[String, Any]
      }
      val routing = obj.value.get("routing") match {
        case Some(StringValue(r)) => Some(r)
        case _                    => None
      }
      val indexRouting = obj.value.get("index_routing") match {
        case Some(StringValue(r)) => Some(r)
        case _                    => None
      }
      val searchRouting = obj.value.get("search_routing") match {
        case Some(StringValue(r)) => Some(r)
        case _                    => None
      }
      val isWriteIndex = obj.value.get("is_write_index") match {
        case Some(BooleanValue(b)) => b
        case _                     => false
      }
      val isHidden = obj.value.get("is_hidden") match {
        case Some(BooleanValue(b)) => b
        case _                     => false
      }
      TableAlias(
        table = table,
        alias = alias,
        filter = filter,
        routing = routing,
        indexRouting = indexRouting,
        searchRouting = searchRouting,
        isWriteIndex = isWriteIndex,
        isHidden = isHidden
      )
    }
  }

  /** Definition of a table within the schema
    *
    * @param name
    *   the table name
    * @param columns
    *   the list of columns within the table
    * @param primaryKey
    *   optional list of columns composing the primary key
    * @param partitionBy
    *   optional partition by date definition
    * @param mappings
    *   optional index mappings
    * @param settings
    *   optional index settings
    * @param processors
    *   optional list of ingest processors associated to this table (apart from column processors)
    * @param aliases
    *   optional map of aliases associated to this table
    */
  case class Table(
    name: String,
    columns: List[Column],
    primaryKey: List[String] = Nil,
    partitionBy: Option[PartitionDate] = None,
    mappings: Map[String, Value[_]] = Map.empty,
    settings: Map[String, Value[_]] = Map.empty,
    processors: Seq[IngestProcessor] = Seq.empty,
    aliases: Map[String, Value[_]] = Map.empty
  ) extends DdlToken {
    lazy val indexName: String = name.toLowerCase
    private[schema] lazy val cols: Map[String, Column] = columns.map(c => c.name -> c).toMap

    def find(path: String): Option[Column] = {
      if (path.contains(".")) {
        val parts = path.split("\\.")
        cols.get(parts.head).flatMap(col => col.find(parts.tail.mkString(".")))
      } else {
        cols.get(path)
      }
    }

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
      .getOrElse(Map.empty) ++
      Map("columns" -> ObjectValue(cols.map { case (name, col) => name -> ObjectValue(col._meta) }))

    def update(): Table = {
      val updated =
        this.copy(columns = columns.map(_.update())) // update columns first with struct info
      updated.copy(
        mappings = updated.mappings ++ Map(
          "_meta" ->
          ObjectValue(updated.mappings.get("_meta") match {
            case Some(ObjectValue(value)) =>
              (value - "primary_key" - "partition_by" - "columns") ++ updated._meta
            case _ => updated._meta
          })
        )
      )
    }

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
          val aliasesOpts =
            if (aliases.nonEmpty) {
              s"aliases = ${ObjectValue(aliases).ddl}"
            } else {
              ""
            }
          val separator = if (partitionBy.nonEmpty) "," else ""
          s"$separator OPTIONS = (${Seq(mappingOpts, settingsOpts, aliasesOpts).filter(_.nonEmpty).mkString(", ")})"
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

    def tableProcessors: Seq[IngestProcessor] =
      columns.flatMap(_.processors) ++ partitionBy
        .map(_.processor(this))
        .toSeq ++ implicitly[Seq[IngestProcessor]](primaryKey)

    def merge(statements: Seq[AlterTableStatement]): Table = {
      statements
        .foldLeft(this) { (table, statement) =>
          statement match {
            // table columns
            case AddColumn(column, ifNotExists) =>
              if (ifNotExists && table.cols.contains(column.name)) table
              else if (!table.cols.contains(column.name))
                table.copy(columns = table.columns :+ column)
              else throw ColumnNotFound(column.name, table.name)
            case DropColumn(columnName, ifExists) =>
              if (ifExists && !table.cols.contains(columnName)) table
              else if (table.cols.contains(columnName))
                table.copy(columns = table.columns.filterNot(_.name == columnName))
              else throw ColumnNotFound(columnName, table.name)
            case RenameColumn(oldName, newName) =>
              if (cols.contains(oldName))
                table.copy(
                  columns = table.columns.map { col =>
                    if (col.name == oldName) col.copy(name = newName) else col
                  }
                )
              else throw ColumnNotFound(oldName, table.name)
            // column type
            case AlterColumnType(columnName, newType, ifExists) =>
              if (ifExists && !table.cols.contains(columnName)) table
              else if (table.cols.contains(columnName))
                table.copy(
                  columns = table.columns.map { col =>
                    if (col.name == columnName) col.copy(dataType = newType)
                    else col
                  }
                )
              else throw ColumnNotFound(columnName, table.name)
            // column script
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
              else throw ColumnNotFound(columnName, table.name)
            case DropColumnScript(columnName, ifExists) =>
              if (ifExists && !table.cols.contains(columnName)) table
              else if (table.cols.contains(columnName))
                table.copy(
                  columns = table.columns.map { col =>
                    if (col.name == columnName) col.copy(script = None)
                    else col
                  }
                )
              else throw ColumnNotFound(columnName, table.name)
            // column default value
            case AlterColumnDefault(columnName, newDefault, ifExists) =>
              if (ifExists && !table.cols.contains(columnName)) table
              else if (table.cols.contains(columnName))
                table.copy(
                  columns = table.columns.map { col =>
                    if (col.name == columnName) col.copy(defaultValue = Some(newDefault))
                    else col
                  }
                )
              else throw ColumnNotFound(columnName, table.name)
            case DropColumnDefault(columnName, ifExists) =>
              if (ifExists && !table.cols.contains(columnName)) table
              else if (table.cols.contains(columnName))
                table.copy(
                  columns = table.columns.map { col =>
                    if (col.name == columnName) col.copy(defaultValue = None)
                    else col
                  }
                )
              else throw ColumnNotFound(columnName, table.name)
            // column not null
            case AlterColumnNotNull(columnName, ifExists) =>
              if (!table.cols.contains(columnName) && ifExists) table
              else if (table.cols.contains(columnName))
                table.copy(
                  columns = table.columns.map { col =>
                    if (col.name == columnName) col.copy(notNull = true)
                    else col
                  }
                )
              else throw ColumnNotFound(columnName, table.name)
            case DropColumnNotNull(columnName, ifExists) =>
              if (!table.cols.contains(columnName) && ifExists) table
              else if (table.cols.contains(columnName))
                table.copy(
                  columns = table.columns.map { col =>
                    if (col.name == columnName) col.copy(notNull = false)
                    else col
                  }
                )
              else throw ColumnNotFound(columnName, table.name)
            // column options
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
              else throw ColumnNotFound(columnName, table.name)
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
                        options = ObjectValue(col.options).set(optionKey, optionValue).value
                      )
                    else col
                  }
                )
              else throw ColumnNotFound(columnName, table.name)
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
                      col.copy(
                        options = ObjectValue(col.options).remove(optionKey).value
                      )
                    else col
                  }
                )
              else throw ColumnNotFound(columnName, table.name)
            // column comments
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
              else throw ColumnNotFound(columnName, table.name)
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
              else throw ColumnNotFound(columnName, table.name)
            // multi-fields
            case AlterColumnFields(columnName, newFields, ifExists) =>
              val col = find(columnName)
              val exists = col.isDefined
              if (ifExists && !exists) table
              else {
                col match {
                  case Some(c) =>
                    val updated = c
                      .copy(
                        multiFields = newFields.toList.map(_.update(Some(c)))
                      )
                      .updateStruct()
                    table.copy(
                      columns = table.columns.map { col =>
                        if (col.name == updated.name) updated else col
                      }
                    )
                  case _ => throw ColumnNotFound(columnName, table.name)
                }
              }
            case AlterColumnField(
                  columnName,
                  field,
                  ifExists
                ) =>
              val col = find(columnName)
              val exists = col.isDefined
              if (ifExists && !exists) table
              else {
                col match {
                  case Some(c) =>
                    val updatedFields = c.multiFields.filterNot(_.name == field.name) :+ field
                    c.copy(multiFields = updatedFields)
                    table
                  case _ => throw ColumnNotFound(columnName, table.name)
                }
              }
            case DropColumnField(
                  columnName,
                  fieldName,
                  ifExists
                ) =>
              val col = find(columnName)
              val exists = col.isDefined
              if (ifExists && !exists) table
              else {
                col match {
                  case Some(c) =>
                    c.copy(
                      multiFields = c.multiFields.filterNot(_.name == fieldName)
                    )
                    table
                  case _ => throw ColumnNotFound(columnName, table.name)
                }
              }
            // mappings / settings
            case AlterTableMapping(optionKey, optionValue) =>
              table.copy(
                mappings = ObjectValue(table.mappings).set(optionKey, optionValue).value
              )
            case DropTableMapping(optionKey) =>
              table.copy(
                mappings = ObjectValue(table.mappings).remove(optionKey).value
              )
            case AlterTableSetting(optionKey, optionValue) =>
              table.copy(
                settings = ObjectValue(table.settings).set(optionKey, optionValue).value
              )
            case DropTableSetting(optionKey) =>
              table.copy(
                settings = ObjectValue(table.settings).remove(optionKey).value
              )
            case AlterTableAlias(aliasName, aliasValue) =>
              table.copy(
                aliases = table.aliases + (aliasName -> aliasValue)
              )
            case DropTableAlias(aliasName) =>
              table.copy(
                aliases = table.aliases - aliasName
              )
            case _ => table
          }
        }
        .update()

    }

    def mergeWithSearch(search: SingleSearch): Table = {
      val fields = search.update(Some(this)).select.fields
      val cols =
        fields.foldLeft(List.empty[Column]) { (cols, field) =>
          val colName = field.fieldAlias.map(_.alias).getOrElse(field.sourceField)
          val col = this.find(colName) match {
            case Some(c) if field.functions.isEmpty => c
            case None =>
              Column(
                name = colName,
                dataType = field.out
              )
          }
          cols :+ col
        }
      val primaryKey =
        this.primaryKey.filter(pk => cols.exists(_.name == pk))
      val partitionBy =
        this.partitionBy match {
          case Some(partition) =>
            if (cols.exists(_.name == partition.column))
              Some(partition)
            else
              None
          case _ => None
        }
      this.copy(columns = cols, primaryKey = primaryKey, partitionBy = partitionBy).update()
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

    private[schema] lazy val diffPipeline: IngestPipeline = {
      val processorsFromColumns = tableProcessors.map(p => p.column -> p).toMap
      IngestPipeline(
        name = s"${name}_ddl_diff_pipeline",
        pipelineType = IngestPipelineType.Custom,
        processors =
          tableProcessors ++ processors.filterNot(p => processorsFromColumns.contains(p.column))
      )
    }

    lazy val defaultPipeline: IngestPipeline = {
      val processorsFromColumns = tableProcessors.map(p => p.column -> p).toMap
      IngestPipeline(
        name = defaultPipelineName.getOrElse(s"${name}_ddl_default_pipeline"),
        pipelineType = IngestPipelineType.Default,
        processors = tableProcessors ++ processors
          .filter(p => p.pipelineType == IngestPipelineType.Default)
          .filterNot(p => processorsFromColumns.contains(p.column))
      )
    }

    lazy val finalPipeline: IngestPipeline = {
      IngestPipeline(
        name = finalPipelineName.getOrElse(s"${name}_ddl_final_pipeline"),
        pipelineType = IngestPipelineType.Final,
        processors = processors.filter(p => p.pipelineType == IngestPipelineType.Final)
      )
    }

    def setDefaultPipelineName(pipelineName: String): Table = {
      this.copy(
        settings = this.settings + ("default_pipeline" -> StringValue(pipelineName))
      )
    }

    lazy val defaultPipelineName: Option[String] =
      settings
        .get("default_pipeline")
        .map(_.value)
        .flatMap {
          case v: String if v != "_none" => Some(v)
          case _                         => None
        }

    def setFinalPipelineName(pipelineName: String): Table = {
      this.copy(
        settings = this.settings + ("final_pipeline" -> StringValue(pipelineName))
      )
    }

    lazy val finalPipelineName: Option[String] =
      settings
        .get("final_pipeline")
        .map(_.value)
        .flatMap {
          case v: String if v != "_none" => Some(v)
          case _                         => None
        }

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

    lazy val indexAliases: Seq[TableAlias] = aliases.map { case (aliasName, value) =>
      TableAlias(name, aliasName, value)
    }.toSeq

    lazy val indexTemplate: ObjectNode = {
      val node = mapper.createObjectNode()
      val patterns = mapper.createArrayNode()
      patterns.add(s"$name-*")
      node.set("index_patterns", patterns)
      node.put("priority", 1)
      val template = mapper.createObjectNode()
      template.set("mappings", indexMappings)
      template.set("settings", indexSettings)
      if (aliases.nonEmpty) {
        val aliasesNode = mapper.createObjectNode()
        indexAliases.foreach { alias =>
          aliasesNode.set(alias.alias, alias.node)
        }
        template.set("aliases", aliasesNode)
      }
      node.set("template", template)
      node
    }

    lazy val defaultPipelineNode: ObjectNode = defaultPipeline.node

    lazy val finalPipelineNode: ObjectNode = finalPipeline.node

    def diff(desired: Table): TableDiff = {
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
      mappingDiffs ++= ObjectValue(actual.mappings).diff(ObjectValue(desired.mappings)).map {
        case Altered(name, value) => MappingSet(name, value)
        case Removed(name)        => MappingRemoved(name)
      }

      // 5. Settings
      val settingDiffs = scala.collection.mutable.ListBuffer[SettingDiff]()
      settingDiffs ++= ObjectValue(actual.settings).diff(ObjectValue(desired.settings)).map {
        case Altered(name, value) => SettingSet(name, value)
        case Removed(name)        => SettingRemoved(name)
      }

      // 6. Aliases
      val aliasDiffs = scala.collection.mutable.ListBuffer[AliasDiff]()
      aliasDiffs ++= ObjectValue(actual.aliases).diff(ObjectValue(desired.aliases)).map {
        case Altered(name, value) => AliasSet(name, value)
        case Removed(name)        => AliasRemoved(name)
      }

      // 7. Default Pipeline
      val pipelineDiffs = scala.collection.mutable.ListBuffer[PipelineDiff]()
      val actualPipeline = actual.diffPipeline
      val desiredPipeline = desiredUpdated.diffPipeline
      actualPipeline.diff(desiredPipeline).foreach { d =>
        pipelineDiffs += d
      }

      TableDiff(
        columns = columnDiffs.toList,
        mappings = mappingDiffs.toList,
        settings = settingDiffs.toList,
        pipeline = pipelineDiffs.toList,
        aliases = aliasDiffs.toList
      )
    }
  }

}
