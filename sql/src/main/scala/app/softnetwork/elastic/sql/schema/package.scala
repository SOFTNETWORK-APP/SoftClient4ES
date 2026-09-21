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

import app.softnetwork.elastic.sql.`type`.{SQLLiteral, SQLType, SQLTypeUtils, SQLTypes}
import app.softnetwork.elastic.sql.config.ElasticSqlConfig
import app.softnetwork.elastic.sql.function.{
  Function => SQLFunction,
  FunctionN,
  FunctionWithIdentifier
}
import app.softnetwork.elastic.sql.function.cond.Case
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query._
import app.softnetwork.elastic.sql.serialization._
import app.softnetwork.elastic.sql.time.TimeUnit
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

package object schema {
  val mapper: ObjectMapper = JacksonConfig.objectMapper

  type Schema = Table

  /** The receipt for the two places this module DEGRADES a computed column instead of rejecting it:
    * a projection that drops a script whose operands it does not carry ([[Table.mergeWithSearch]]),
    * and a stored expression this grammar can no longer parse ([[ScriptProcessor.validationExpr]]).
    * Both are deliberate fail-opens, and silent degradation is the failure mode this area keeps
    * paying for -- the WARN is what makes each one reviewable in a log.
    *
    * Named explicitly rather than derived from `getClass`: a package object's runtime name is
    * `...schema.package$`, which is neither what an operator would configure nor what a log-capture
    * assertion can bind to by reading the source.
    */
  private[schema] val logger: Logger =
    LoggerFactory.getLogger("app.softnetwork.elastic.sql.schema")

  lazy val sqlConfig: ElasticSqlConfig = ElasticSqlConfig()

  // ========================================================================
  // PIPELINE COMPONENTS
  // ========================================================================

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
    def properties: ListMap[String, Any]
    def boolToString(b: Boolean): String = if (b) "yes" else "no"
    def describe: ListMap[String, Any] = {
      ListMap(
        "processor_type" -> processorType.name,
        "description"    -> description.getOrElse(sql),
        "field"          -> column,
        "ignore_failure" -> boolToString(ignoreFailure)
      ) ++ ListMap(
        "options" -> ObjectValue(
          properties
            .filterNot(p =>
              Set(
                "processor_type",
                "description",
                "field",
                "ignore_failure"
              ).contains(p._1)
            )
            .map(kv => kv._1 -> Value(kv._2))
        ).ddl
      )
    }

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

    /** Read a stored processor value back WITHOUT changing its type.
      *
      * 🔴 21.8 Part F.1, second half. This was `Value(v.asText())`, which renders every JSON scalar
      * as text, so `reputation DOUBLE DEFAULT 0.0` was declared as the number `0.0` and read back
      * as the string `"0.0"`. `IngestProcessor.diff` compares `properties`, so that processor
      * reported `ProcessorPropertyChanged("value", "0.0", 0.0)` on EVERY ALTER — the same churn as
      * the anonymous script processor, and unlike it not confined to Elasticsearch 6.8: it did not
      * depend on `description`, so it fired on every version. It also reads identically in the log,
      * both sides rendering as `0.0`.
      *
      * `Value(node)` is the typed read `IndexField` already uses for `null_value`; using it here is
      * the same derivation rather than a second one.
      *
      * Containers are included, and the write side is what makes that safe. `Values.value` and
      * `ObjectValue.value` hold WRAPPED elements, so handing either to `mapToJsonNode` serialises
      * Jackson BEANS; `SetProcessor.defaultValue` therefore goes through `Value.unwrap`, and an
      * array read here is written back as `["a","b"]`. Before this story a container value was lost
      * to the EMPTY STRING, because Jackson's `asText()` on a container node is `""`.
      *
      * 🔴 The `Try` is not defensive padding. `Value.apply(Any)` THROWS
      * `IllegalArgumentException("Unsupported Values type")` for a list whose head has no `Values`
      * companion — MEASURED on `"value": [null, "a"]`, where the head is `Null` and no arm of the
      * sealed hierarchy matches. That read runs inside `PipelineApi`'s pipeline load and inside
      * `IndicesApi`'s schema cache, neither of which catches it, so without the guard a value that
      * was merely wrong would become an ALTER that dies. Such a value keeps the old text read, i.e.
      * exactly today's behaviour.
      */
    private def readValue(node: JsonNode): Value[_] =
      Value(node)
        .flatMap(v => scala.util.Try(Value(v)).toOption)
        .getOrElse(Value(node.asText()))

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
          val field = Option(props.get("field"))
            .map(_.asText())
            .getOrElse(
              throw new IllegalArgumentException(
                s"SET processor must have a 'field' property, got: $props"
              )
            )
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
              value = valueNode.map(readValue).getOrElse(Null),
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
          val lang = Option(props.get("lang")).map(_.asText()).getOrElse("painless")
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
                properties = props
              )
          }

        case IngestProcessorType.DateIndexName.name =>
          val field = Option(props.get("field"))
            .map(_.asText())
            .getOrElse(
              throw new IllegalArgumentException(
                s"date_index_name processor must have a 'field' property, got: $props"
              )
            )
          val desc = Option(props.get("description")).map(_.asText())
          val rounding = Option(props.get("date_rounding"))
            .map(_.asText())
            .getOrElse(
              throw new IllegalArgumentException(
                s"date_index_name processor must have a 'date_rounding' property, got: $props"
              )
            )
          val formats = Option(props.get("date_formats"))
            .map(_.elements().asScala.toList.map(_.asText()))
            .getOrElse(Nil)
          val prefix = Option(props.get("index_name_prefix"))
            .map(_.asText())
            .getOrElse(
              throw new IllegalArgumentException(
                s"date_index_name processor must have an 'index_name_prefix' property, got: $props"
              )
            )

          DateIndexNameProcessor(
            pipelineType = pipelineType,
            description = desc,
            column = field,
            dateRounding = rounding,
            dateFormats = formats,
            prefix = prefix
          )

        case IngestProcessorType.Remove.name =>
          val field = Option(props.get("field"))
            .map(_.asText())
            .getOrElse(
              throw new IllegalArgumentException(
                s"remove processor must have a 'field' property, got: $props"
              )
            )
          val desc = Option(props.get("description")).map(_.asText())

          RemoveProcessor(
            pipelineType = pipelineType,
            description = desc,
            column = field
          )

        case IngestProcessorType.Rename.name =>
          val field = props.get("field").asText()
          val desc = Option(props.get("description")).map(_.asText())
          val targetField = Option(props.get("target_field"))
            .map(_.asText())
            .getOrElse(
              throw new IllegalArgumentException(
                s"rename processor must have a 'target_field' property, got: $props"
              )
            )

          RenameProcessor(
            pipelineType = pipelineType,
            description = desc,
            column = field,
            newName = targetField
          )

        case IngestProcessorType.Enrich.name =>
          val field = Option(props.get("field"))
            .map(_.asText())
            .getOrElse(
              throw new IllegalArgumentException(
                s"enrich processor must have a 'field' property, got: $props"
              )
            )
          val desc = Option(props.get("description")).map(_.asText())
          val policyName = Option(props.get("policy_name"))
            .map(_.asText())
            .getOrElse(
              throw new IllegalArgumentException(
                s"enrich processor must have a 'policy_name' property, got: $props"
              )
            )
          val targetField = Option(props.get("target_field"))
            .map(_.asText())
            .getOrElse(
              throw new IllegalArgumentException(
                s"enrich processor must have a 'target_field' property, got: $props"
              )
            )
          val maxMatches = Option(props.get("max_matches")).map(_.asInt()).getOrElse(1)
          val ignoreFailure = Option(props.get("ignore_failure")).exists(_.asBoolean())
          val ignoreMissing = Option(props.get("ignore_missing")).map(_.asBoolean())
          val overrideTarget = Option(props.get("override")).map(_.asBoolean())
          val shapeRelation =
            Option(props.get("shape_relation")).map(_.asText()).map(EnrichShapeRelation.apply)
          EnrichProcessor(
            pipelineType = pipelineType,
            description = desc,
            column = targetField,
            policyName = policyName,
            field = field,
            maxMatches = maxMatches,
            ignoreFailure = ignoreFailure,
            ignoreMissing = ignoreMissing,
            overrideTarget = overrideTarget,
            shapeRelation = shapeRelation
          )

        case other =>
          GenericProcessor(
            pipelineType = pipelineType,
            processorType = IngestProcessorType(other),
            properties = props
          )

      }
    }
  }

  case class GenericProcessor(
    pipelineType: IngestPipelineType = IngestPipelineType.Default,
    processorType: IngestProcessorType,
    properties: ListMap[String, Any]
  ) extends IngestProcessor {
    override def description: Option[String] = properties.get("description") match {
      case Some(s: String) => Some(s)
      case _               => None
    }

    override def sql: String = ddl

    /** 🔴 Deterministic, and identifier-safe. It was `UUID.randomUUID().toString`, which is
      * neither, and that single expression produced BOTH halves of the ALTER-pipeline defect —
      * MEASURED: three consecutive reads of one processor's `column` gave three different values.
      *
      *   - `Table.diff` keys processors by `"<pipeline>-<type>-<column>"`, so a processor whose
      *     column is random can never match its counterpart: every diff reports it as REMOVED and
      *     re-ADDED, on a processor nobody touched. That is the churn.
      *   - `ProcessorRemoved.stmt` renders `DROP PROCESSOR <TYPE>(<column>)`, so the statement came
      *     out as `DROP PROCESSOR SCRIPT(b18b1864-5e89-44fd-b4f8-191faa9e9e0c)` — a UUID's hyphens
      *     are not an `ident`, so the generated DDL did not parse. On ES 6.8 that is a hard ALTER
      *     failure; 7/8/9 never reach it because their processors keep the `description` that
      *     re-types them (processor `description` is ES 7.9+), so they stay `ScriptProcessor`.
      *
      * A processor with no `field` has no column, so the fallback is an identity derived from the
      * processor's own CONTENT: equal processors get equal ids, and the id is `[a-z0-9_]` so it
      * survives the DDL round trip. Nothing persists this value — it is computed at diff time —
      * which is why the change is backward compatible with pipelines already stored.
      *
      * 🔴 The content is CANONICALISED (pairs rendered, then sorted) before hashing, so equality is
      * a property rather than an observation. `properties` is a `ListMap`, i.e. INSERTION-ordered:
      * hashing `toString` directly would make the id depend on the order the JSON happened to be
      * parsed in, so two processors with identical content but different key order would get
      * different ids — and the diff keys on this string. Sorting removes that dependency, and
      * `String.hashCode` is specified by the JLS, so equal content yields equal ids across separate
      * JVM runs and not merely within one.
      *
      * Collisions: 32 bits over the handful of anonymous processors a pipeline can hold. If two DID
      * collide the diff would treat them as one processor and compare their properties, so the
      * failure mode is a spurious `ProcessorChanged` — noisy, not silent.
      *
      * ⚠️ That reassurance covers THIS fallback only. Once 21.8 Part F.1 keys a script processor by
      * the column its source assigns, two scripts writing the SAME column collide, and the diff
      * builds its side maps with `toMap`, so one is dropped SILENTLY. That is not new behaviour on
      * ES 7+ — `ScriptDescRegex` has always keyed them by their declared column there — so F.1
      * aligns 6.8 with every other version rather than introducing it. It is stated because the
      * paragraph above would otherwise read as a guarantee it does not give.
      */
    override def column: String = properties.get("field") match {
      case Some(s: String) => s
      case _               => s"anonymous_$canonicalContentId"
    }

    /** Order-independent identity of this processor's properties. See `column`. */
    private def canonicalContentId: String = {
      val canonical =
        properties.toSeq.map { case (k, v) => s"$k=$v" }.sorted.mkString("\u0000")
      f"${canonical.hashCode & 0xffffffffL}%08x"
    }
    override def ignoreFailure: Boolean = properties.get("ignore_failure") match {
      case Some(b: Boolean) => b
      case _                => false
    }
  }

  /** @param materialized
    *   the column's value is ALREADY materialized in this index -- the script records how it was
    *   DERIVED, and must not be executed here.
    *
    * 🔴 The distinction exists because `script` does two jobs: it is the provenance `DESCRIBE` and
    * `_meta` publish, and it is the ingest processor `Column.processors` emits. A stage downstream
    * of the one that computed the column needs the first and must not have the second — re-running
    * a projection over a document that already carries its result is redundant, and wrong whenever
    * the source operands did not survive the transform that produced it.
    *
    * ⚠️ The flag is carried by `_meta`, NOT by the DDL text: `Column.sql` omits `SCRIPT AS` when it
    * is set, precisely so that re-parsing the rendered DDL cannot resurrect the processor. Reading
    * it back is `IndexField`'s job (`elastic/schema/package.scala`), which is the path `DESCRIBE`
    * and `SHOW CREATE` take. The two surfaces have different authorities and that is deliberate.
    */
  case class ScriptProcessor(
    pipelineType: IngestPipelineType = IngestPipelineType.Default,
    description: Option[String] = None,
    script: String,
    column: String,
    dataType: SQLType,
    source: String,
    ignoreFailure: Boolean = true,
    materialized: Boolean = false,
    /** The parsed expression this processor's `source` was derived FROM, when it came from a
      * `SCRIPT AS (…)` in DDL rather than from a pipeline read back out of Elasticsearch.
      *
      * 🔴 It exists so the derivation can be RE-RUN once the sibling columns are known. At parse
      * time a `CREATE TABLE`'s column list does not exist yet, so every operand's `baseType` was
      * `Any` and no `SQLTypeUtils.coerce` arm fired: `CREATE TABLE t (zip_code KEYWORD, zip_n
      * BIGINT SCRIPT AS (CAST(zip_code AS BIGINT)))` stored `ctx.zip_n = param1` — the UNCONVERTED
      * keyword string into a `long`-mapped field. Elasticsearch never rewrites `_source` and
      * `coerce` defaults true, so the mapping stayed correct while the document was wrong
      * (`local-21.8-ddl-script-processor-has-no-schema.md`, the #205 silent-wrong-value family).
      *
      * Appended LAST and defaulted, so every positional construction keeps compiling; `None` for a
      * processor loaded from Elasticsearch, which has JSON and no AST, and whose stored `source` is
      * therefore left exactly as it was found.
      */
    expr: Option[PainlessScript] = None
  ) extends IngestProcessor {
    override def sql: String =
      s"$column $dataType SCRIPT AS ($script)${if (materialized) " STORED" else ""}"

    /** Re-derive `source` from [[expr]] with `schema` attached, so the conversion arms that `#306`
      * made reachable for a query are reachable for an ingest script too.
      *
      * Returns `this` unchanged whenever there is no expression to re-derive from — which is both
      * the Elasticsearch-load path and the lead's OQ-4 ruling for an operand the schema cannot
      * resolve: it stays SILENT and emits the identity, exactly as before. A DDL statement that
      * works today keeps working; it just stops storing the wrong value.
      */
    def resolvedAgainst(schema: Schema, columnPath: String): ScriptProcessor =
      expr match {
        case Some(e) =>
          ScriptProcessor
            .fromScript(
              column = columnPath,
              script = resolveAgainstSchema(e, schema),
              dataType = Some(dataType),
              pipelineType = pipelineType,
              materialized = materialized
            )
            .copy(description = description, ignoreFailure = ignoreFailure)
        case None => this
      }

    /** The expression to walk FOR VALIDATION, and for nothing else.
      *
      * 🔴 [[expr]] is `None` for every processor read back out of Elasticsearch, so a rule that
      * walks `expr` alone is blind on exactly the path production takes: `ALTER TABLE t DROP COLUMN
      * n` on a LIVE table left an orphaned processor reading `ctx.n` behind, and a CTAS off a live
      * source copied one (issue #385). The SQL text survives the round-trip -- `_meta.script.sql`
      * carries it -- so the reference set is recoverable by re-parsing it through the SAME
      * `scriptValue` production the DDL grammar uses.
      *
      * 🔴 This NEVER feeds emission. `expr` deliberately stays `None`, so
      * [[ScriptProcessor.resolvedAgainst]] keeps its documented behaviour ("returns `this`
      * unchanged whenever there is no expression to re-derive from") and no stored Painless moves
      * under a table that is merely being altered. Populating `expr` at load time would re-derive
      * `source` for every loaded processor -- a far wider change than the rule this serves.
      *
      * 🔴 A text this grammar does NOT accept is SKIPPED, with a WARN, not rejected. A legacy or
      * hand-written `_meta.script.sql` must not block every future ALTER on that table; the cost of
      * the fail-open is that such a column's references go unchecked, which is exactly today's
      * behaviour for it. A reviewer will challenge this: it is deliberate.
      *
      * `lazy`, because both consumers (the projection drop and `validateScriptReferences`) can ask
      * the same processor and the parse is not free.
      */
    private[elastic] lazy val validationExpr: Option[PainlessScript] =
      expr.orElse {
        val reparsed = Parser.parseScriptExpression(script)
        if (reparsed.isEmpty)
          logger.warn(
            s"The stored expression of computed column '$column' cannot be parsed [$script]; " +
            "its column references are not checked."
          )
        reparsed
      }

    override def baseType: SQLType = dataType

    def processorType: IngestProcessorType = IngestProcessorType.Script

    override def properties: ListMap[String, Any] = ListMap(
      "description"    -> description.getOrElse(sql),
      "lang"           -> "painless",
      "source"         -> source,
      "ignore_failure" -> ignoreFailure
    )

  }

  /** Every column a `SCRIPT AS (…)` expression READS, paired with the function that consumes it.
    *
    * Both DDL defects this answers are the same one seen from two sides: a reference the table does
    * not have, and a reference whose DECLARED type the consuming function cannot take. Either way
    * the emitted Painless is written for a value the document will not carry.
    *
    * 🔴 The discriminator is `name.nonEmpty`, and that is a property of the AST rather than a
    * guess: a container node (the wrapper an expression is parsed into), a literal (`functions =
    * [LongValue]`) and `CURRENT_DATE` (`functions = [CurrentDate]`) ALL carry an empty name, and
    * only a real column reference carries one. Measured over arithmetic, CASE, CONCAT, nested calls
    * and constants.
    *
    * 🔴 The consuming function is `functions.LAST`, not the head. The chain is stored outermost
    * first — `YEAR(DATE_TRUNC(d, MONTH))` is `[Year, DateTrunc]` — so the function that actually
    * receives the column is the innermost one. Taking the head would check `YEAR`'s input against
    * the column and pass, or fail, for the wrong reason.
    */
  private[sql] object ScriptReferences {

    def of(script: PainlessScript): Seq[(Identifier, Option[SQLFunction])] = of(script, None)

    private def of(
      script: PainlessScript,
      enclosing: Option[SQLFunction]
    ): Seq[(Identifier, Option[SQLFunction])] = script match {
      case i: Identifier =>
        // 🔴 Two shapes, and only one of them puts the consumer on the identifier.
        // `YEAR(d)` parses as the identifier `d` CARRYING the chain, so its consumer is the
        // innermost of its own functions. `UPPER(n)` parses as a container whose function holds
        // `n` as an ARGUMENT, and there `n` has no chain at all -- its consumer is the function
        // that encloses it. Reading only `functions.lastOption` left every argument-shaped operand
        // unchecked, which accepted `UPPER(n)` over an INTEGER.
        val consumer = i.functions.lastOption.orElse(enclosing)
        val here = if (i.name.nonEmpty) Seq(i -> consumer) else Nil
        // ...and ALWAYS recurse: a named identifier can still carry arguments of its own, as
        // `DATE_DIFF(a, b, YEAR)` does.
        // 🔴 `filterNot(_ eq i)` is load-bearing, not tidiness. A `FunctionWithIdentifier`
        // (`DATE_PARSE`, `DATE_FORMAT`, `LAST_DAY`, `ISNULL`, ...) lists its OWN receiver in
        // `args`, so without it every such node yielded itself twice and the walk was 2^depth:
        // measured 16,384 pairs and 148 ms at depth 14, with one rejection message repeated 8
        // times for a 3-deep expression.
        here ++ i.functions.flatMap(f => operandsOf(f, i).flatMap(of(_, Some(f))))
      // 🔴 A CASE's WHEN condition is a CRITERIA, not an identifier -- `nosuch > 0` parses as a
      // `GenericExpression`. Without these arms the walk bottomed out at the condition and
      // `CASE WHEN nosuch > 0 THEN 1 ELSE 0 END` was accepted with an undeclared column in it.
      case e: Expression =>
        of(e.identifier, enclosing) ++
          // 🔴 BETWEEN's bounds are NOT reachable as a `PainlessScript`: `BetweenExpr.maybeValue`
          // is a `FromTo`, which is not one, so `n BETWEEN lo AND nosuch` walked to `n` alone and
          // the undeclared bound was ACCEPTED -- the original defect, reached through a criteria
          // class instead of a function. Found by independent review; the completeness guard now
          // carries a BETWEEN row.
          e.maybeValue.toSeq
            .flatMap {
              case ft: FromTo => Seq(ft.from, ft.to)
              case other      => Seq(other)
            }
            .collect { case p: PainlessScript => p }
            .flatMap(of(_, enclosing))
      case p: Predicate =>
        Seq(p.leftCriteria, p.rightCriteria)
          .collect { case c: PainlessScript => c }
          .flatMap(of(_, enclosing))
      // 🔴 A function can also appear as an operand DIRECTLY, with no identifier wrapping it:
      // in `(a + b) * e` the left operand of `*` is the `ArithmeticExpression` for `a + b`. Without
      // this arm the walk saw only `e`.
      case f: SQLFunction => operandsOf(f).flatMap(of(_, Some(f)))
      case _              => Nil
    }

    /** The operands a function READS.
      *
      * 🔴 NOT `FunctionN.args`, which is a TYPE-COMPUTATION list and not an operand list:
      * `Case.args` deliberately omits the WHEN conditions (it feeds `leastCommonSuperType` over the
      * result branches), so a walk over `args` alone accepted `CASE WHEN nosuch > 0 THEN 1 ELSE 0
      * END`. Anything carrying operands some other way is likewise invisible here -- the #318
      * failure mode, where enumerating by type fails OPEN and no rejection test notices.
      * `ScriptReferenceValidationSpec` guards it by asserting this walk finds exactly the columns
      * the EMISSION reads, derived independently; that assertion is what caught the `Case`
      * omission.
      */
    /** The operands of `f`, EXCLUDING the receiver `self` already being visited.
      *
      * 🔴 A `FunctionWithIdentifier` (`DATE_PARSE`, `DATE_FORMAT`, `LAST_DAY`, `ISNULL`, ...) lists
      * its OWN receiver among its operands, so a naive walk yielded every such node twice and was
      * 2^depth -- measured 32 pairs at depth 5 for `LAST_DAY(LAST_DAY(...))`, and 16,384 at depth
      * 14. Reference equality does NOT catch it (the receiver is a distinct instance), so the
      * exclusion is by the trait that declares the relationship.
      */
    private def operandsOf(f: SQLFunction, self: Identifier): List[PainlessScript] =
      operandsOf(f).filterNot { arg =>
        f match {
          // the operand that IS this function's receiver -- `self` is already being visited
          case fwi: FunctionWithIdentifier => (fwi.identifier eq arg) || (fwi.identifier eq self)
          case _                           => false
        }
      }

    private def operandsOf(f: SQLFunction): List[PainlessScript] = f match {
      case c: Case =>
        c.expression.toList ++
          c.conditions.flatMap { case (cond, res) => List(cond, res) } ++
          c.default.toList
      case fn: FunctionN[_, _] => fn.args
      case _                   => Nil
    }
  }

  // `private[elastic]` and not `private[sql]`: the ALTER half of this rule lives in `core`
  // (`GatewayApi`), because only the point where an ALTER is APPLIED holds the whole table.
  /** May a column declared `declared` be handed to a function whose input type is `required`?
    *
    * 🔴 It takes BOTH relations because they answer complementary halves, and neither alone is
    * right -- measured:
    *
    *   - `matches` handles ABSTRACT input types. A function often declares `NUMERIC`, `TEMPORAL` or
    *     `ANY`, which no column is ever DECLARED as, and only `matches` relates a concrete type to
    *     its abstract supertype (`INT -> NUMERIC`, `ANY -> TEMPORAL`). `canConvert` says false.
    *   - `canConvert` handles CONCRETE widening inside one family. `DATE -> DATETIME` is the
    *     published `DATE_DIFF(birthdate, CURRENT_DATE, YEAR)` example, which stores `age` correctly
    *     on all four majors; `matches` says false, because it is a same-family IDENTITY check and
    *     deliberately not a conversion check. Using `matches` alone REJECTED that example.
    *
    * Both say false for every operand that must be refused (`KEYWORD -> TEMPORAL`, `INT ->
    * TEMPORAL`, `INT -> VARCHAR`, `BOOLEAN -> VARCHAR`, `KEYWORD -> NUMERIC`), so the union widens
    * what is ACCEPTED without weakening what is REJECTED.
    *
    * ⚠️ `canConvert` is READ here, never changed. Its "expansion only" numeric rule exists for
    * `TableDiff`, where loosening it would silently legalise a narrowing column-type change in an
    * ALTER. If this operand rule ever needs to be more permissive it needs its OWN relation --
    * widening `canConvert` would reach the schema-evolution check too.
    */
  private def acceptableOperand(declared: SQLType, required: SQLType): Boolean =
    // 🔴 A STRING-typed input is NOT checked, and that is a measured limit rather than an omission.
    // The declared `inputType` does not say whether the emission coerces: `CONCAT` and `UPPER` BOTH
    // declare VARCHAR, yet `CONCAT(a, n)` emits `String.valueOf(param2)` and stores `"ORD-42"`
    // correctly on ES 8.18.3, while `UPPER(n)` emits `param1.toUpperCase()` and breaks. Checking
    // the declared type rejected BOTH -- and `CONCAT(name, ' ', id)` is the most ordinary computed
    // column there is. `DATE_PARSE(d, 'yyyy-MM-dd')` over a declared DATE is the same shape: at
    // ingest `ctx.d` IS the raw JSON string, which is the reason the function exists.
    //
    // So the check is confined to the inputs where no coercion exists and the emission calls a
    // method straight on the operand -- temporal and numeric. That is also where the original
    // defect lives: a wrong declared type picks the wrong parse. `UPPER(n)` is knowingly left
    // accepted; catching it needs the EMISSION's coercion behaviour, not a declared type.
    required.isInstanceOf[SQLLiteral] ||
    SQLTypeUtils.matches(declared, required) ||
    SQLTypeUtils.canConvert(declared, required)

  /** The columns `column`'s EXECUTING script reads that `projection` does not carry, in the order
    * the walk finds them and without duplicates.
    *
    * 🔴 ONE walk, and ONE lookup: `ScriptReferences.of` over [[ScriptProcessor.validationExpr]],
    * then `schema.find(id.path)` -- the same pair `validateScriptReferences` uses, so the
    * projection rule and the rejection rule cannot disagree about what a reference IS or about how
    * a `STRUCT` path resolves. A second walk here would be a second answer to the same question.
    *
    * 🔴 A `materialized` (`STORED`) script is NOT considered. It emits nothing (`Column.processors`
    * is `script.filterNot(_.materialized)`), and carrying the derivation forward into an index
    * whose source operands deliberately did not survive is the documented POINT of `STORED` -- the
    * same carve-out `validateScriptReferences` makes.
    */
  private def unsatisfiedScriptReferences(column: Column, projection: Schema): Seq[String] =
    column.script
      .filterNot(_.materialized)
      .toSeq
      .flatMap(_.validationExpr)
      .flatMap(ScriptReferences.of)
      .map(_._1.path)
      .filter(path => projection.find(path).isEmpty)
      .distinct

  /** Reject a computed column whose expression reads a column the table does not declare, or hands
    * a column to a function that cannot take its declared type.
    *
    * 🔴 Runs on the RESOLVED table -- after `Table.update()` has settled every path and re-derived
    * every script -- which is why a forward reference such as `CREATE TABLE t (c INTEGER SCRIPT AS
    * (n + 1), n INTEGER)` is accepted: at that point `n` is in the column list regardless of
    * declaration order. Validating the PARSED columns instead would reject it.
    *
    * 🔴 The type read is the column's DECLARED type, never `Identifier.baseType`: `runtimeType`
    * collapses every temporal declaration to `Timestamp`, and the declaration is exactly what the
    * ingest emission needs -- a DATE column parses as a `LocalDate`, a TIMESTAMP as a
    * `ZonedDateTime`, and getting that wrong is the defect this rejects.
    */
  private[elastic] def validateScriptReferences(schema: Schema): Either[String, Unit] = {
    // 🔴 A REGULAR table only. Every other table type projects columns from somewhere else, so its
    // own column list is not the population a reference is checked against: a materialized view's
    // computed column legitimately reads a column of its SOURCE (`effective_date` from
    // `createdAt`), and at ingest that value is in the incoming document even though the view never
    // declares it. Validating those here rejected an MV round-trip -- and MV deployment RENDERS a
    // stage's schema to DDL and runs the text, so it would have broken deployment, not a test.
    // Reaching them needs the source schema, which this seam does not hold.
    if (!schema.isRegular) return Right(())

    def columnErrors(column: Column): Seq[String] =
      // 🔴 A `STORED` column is skipped: `Column.processors` is `script.filterNot(_.materialized)`,
      // so it emits NOTHING that can run at ingest, and the whole justification for this check --
      // "the emitted Painless is written for a value the document will not carry" -- does not
      // apply. It is also the documented POINT of `STORED`: the same column is an executing
      // `SCRIPT AS` in the index that computes it and a `STORED` one in every index it flows into,
      // where the source columns deliberately did not survive the transform.
      column.script
        .filterNot(_.materialized)
        .toSeq
        .flatMap(_.validationExpr)
        .flatMap(ScriptReferences.of)
        .flatMap { case (id, consumer) =>
          schema.find(id.path) match {
            case None =>
              Seq(
                s"Column '${column.path}' reads '${id.path}', which does not exist " +
                s"in table '${schema.name}'"
              )
            case Some(referenced) =>
              consumer.collect { case fn: FunctionN[_, _] => fn }.toSeq.flatMap { fn =>
                if (acceptableOperand(referenced.dataType, fn.inputType)) Nil
                else
                  Seq(
                    s"Column '${column.path}' applies ${fn.sql} to '${id.path}', which is " +
                    s"declared ${referenced.dataType.typeId} and not ${fn.inputType.typeId}"
                  )
              }
          }
        } ++ column.multiFields.flatMap(columnErrors)

    schema.columns.flatMap(columnErrors).distinct match {
      case Nil    => Right(())
      case errors => Left(errors.mkString("; "))
    }
  }

  /** Attach `schema` to a DDL `SCRIPT AS (…)` expression, so each operand resolves to its declared
    * column and `SQLTypeUtils.coerce` can see a real type instead of `Any`.
    *
    * 🔴 It goes through the SAME `Identifier.update` every executed query uses, over a minimal
    * synthetic `SingleSearch`, rather than through a second identifier-resolution walk written for
    * DDL. A parallel walk would be a second derivation of "which column is this operand", and this
    * area has already paid for that four times over (story 21.3). Every `scriptValue` alternative
    * yields an `Identifier`, so one `update` call is the whole job.
    *
    * The synthetic statement carries no SELECT, no GROUP BY and one placeholder table: `update`
    * reads `request.schemas.get(<main table>)` first and falls back to `request.schema`, which is
    * the schema handed in here, so the table's NAME is irrelevant and no alias, bucket or field
    * alias can resolve. An operand the schema does not know simply keeps `col = None`.
    *
    * ⚠️ That was the lead's OQ-4 ruling, which also decided such a statement should be ACCEPTED.
    * The ACCEPTANCE half was overturned on 2026-09-20 ("Existence and type otherwise the painless
    * will not be accurate") and is now enforced by `validateScriptReferences`. RESOLUTION is
    * unchanged and still byte-for-byte today's emission: nothing rejected here, so nothing that
    * reaches emission behaves differently.
    *
    * 🔴 A temporal-SOURCE arm still must NOT fire here, and does not: the processor context makes
    * `SQLTypeUtils.coerce`'s `isProcessorContext` guard decline them, because in an ingest script
    * the operand is `ctx.<field>` — the raw JSON scalar — not the temporal object a query's
    * `doc['f'].value` yields. Attaching the schema is what makes those arms REACHABLE for the first
    * time, so that guard stops being theoretical the moment this function exists.
    */
  private[schema] def resolveAgainstSchema(
    script: PainlessScript,
    schema: Schema
  ): PainlessScript =
    script match {
      case id: Identifier =>
        id.update(
          SingleSearch(
            select = Select(Seq.empty),
            from = From(Seq(query.Table(schema.name))),
            where = None,
            schema = Some(schema)
          )
        )
      case other => other
    }

  /** The ONE place that knows how an ingest script names the column it computes.
    *
    * A `script` processor has no `field` property, so Elasticsearch stores nothing that says which
    * column it feeds. The only carrier was the processor `description` — an Elasticsearch **7.9+**
    * field, which 6.8 silently drops — and `IngestProcessor.apply` re-types a script processor from
    * it (`ScriptDescRegex`). Without it the read-back is an anonymous `GenericProcessor`, and
    * `IngestPipeline.diff` keys processors by `"<pipeline>-<type>-<column>"`, so it could never
    * match the processor that declared it: every ALTER reported it REMOVED and re-ADDED, on a
    * processor nobody had touched (21.8 Part F.1, the churn).
    *
    * 🔴 The identity was already in the script all along. `assign` is what `ScriptProcessor` emits
    * as the LAST statement of every generated source, so `of` reads back exactly what `assign`
    * wrote: ONE derivation of "which column does this script feed", used by both sides, rather than
    * a second one that can drift from it. `Column.update` relies on the same contract when it
    * rewrites the target of a nested column's script.
    *
    * `of` returns `None` for any source this object did not write — a hand-authored `ALTER PIPELINE
    * … ADD PROCESSOR SCRIPT(…)`, say — and the caller keeps its content-addressed fallback, i.e.
    * exactly today's behaviour for anything unrecognised.
    */
  private[schema] object ScriptTarget {

    /** How an ingest script NAMES a column. `assign` writes an assignment to it; `Column.update`
      * rewrites it when a nested column's path changes. Both go through here so the format has one
      * spelling — the claim this object's name makes.
      */
    def reference(column: String): String = s"ctx.$column"

    def assign(column: String, expression: String): String = s"${reference(column)} = $expression"

    /** The LAST `ctx.<column> = …` in `source`, which is the one `assign` appended.
      *
      * Greedy on purpose: every statement before it is a `def paramN = …` preamble, and those read
      * `ctx.<path>` on the RIGHT of the `=`, never on the left, so anchoring on the assignment and
      * taking the last match cannot pick one of them up.
      */
    def of(source: String): Option[String] =
      // 🔴 On the literals-blanked source, never the raw one. A string literal may legitimately
      // CONTAIN `ctx.<name> = ` -- `CONCAT(name, '; ctx.d = 1')` is ordinary SQL -- and the greedy
      // match then took the one inside the literal, reporting this processor as another column's.
      // `IngestPipeline.diff` keys processors by this, and a `Map` drops the loser of a collision
      // (issue #373). Blanking preserves offsets and length, so the captured name is unchanged
      // wherever no literal is involved.
      PainlessOperandForm.withoutLiterals(source) match {
        case Assignment(column) => Some(column)
        case _                  => None
      }

    private val Assignment = """(?s).*(?:^|;)\s*ctx\.([A-Za-z0-9_.]+)\s*=[^=].*""".r
  }

  object ScriptProcessor {
    def fromScript(
      column: String,
      script: PainlessScript,
      dataType: Option[SQLType] = None,
      pipelineType: IngestPipelineType = IngestPipelineType.Default,
      materialized: Boolean = false
    ): ScriptProcessor = {
      val ctx = PainlessContext(PainlessContextType.Processor)
      val scr = script.painless(Some(ctx))
      val painless = s"$ctx$scr"
      val source = PainlessOperandForm.splitStatements(painless) match {
        case Array(single) if single.trim.startsWith("return ") =>
          val stripped = single.trim.stripPrefix("return ").trim
          ScriptTarget.assign(column, stripped)
        case parts =>
          val last = parts.last.trim
          val updated = parts.dropRight(1) :+ s" ${ScriptTarget.assign(column, last)}"
          updated.mkString(";")
      }
      ScriptProcessor(
        pipelineType = pipelineType,
        script = script.sql,
        column = column,
        dataType = dataType.getOrElse(script.out),
        source = source,
        materialized = materialized,
        expr = Some(script)
      )
    }
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

    override def properties: ListMap[String, Any] = ListMap(
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

    override def properties: ListMap[String, Any] = ListMap(
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

    override def properties: ListMap[String, Any] = ListMap(
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

    /** 🔴 `Value.unwrap`, not `value.value`. For a container the latter is the WRAPPED elements —
      * `Seq[Value[_]]` / `ListMap[String, Value[_]]` — and `properties` feeds this straight into
      * `mapToJsonNode`, which then serialises each element as a Jackson BEAN. `ALTER PIPELINE`
      * rewrites every processor of the merged pipeline, so an untouched list-valued processor would
      * be written back to Elasticsearch in that shape. For a scalar `unwrap` IS `value.value`, so
      * nothing else moves.
      */
    lazy val defaultValue: Option[Any] = {
      if (copyFrom.isDefined) None
      else
        value match {
          case IdValue | IngestTimestampValue => Some(s"{{${value.value}}}")
          case Null                           => None
          case _                              => Some(Value.unwrap(value))
        }
    }

    override def properties: ListMap[String, Any] = {
      ListMap(
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

    override def properties: ListMap[String, Any] = ListMap(
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

  sealed trait EnrichShapeRelation extends DdlToken {
    def name: String
    def sql: String = name.toUpperCase
  }

  object EnrichShapeRelation {
    case object Intersects extends EnrichShapeRelation {
      val name: String = "intersects"
    }

    case object Disjoint extends EnrichShapeRelation {
      val name: String = "disjoint"
    }

    case object Contains extends EnrichShapeRelation {
      val name: String = "contains"
    }

    case object Within extends EnrichShapeRelation {
      val name: String = "within"
    }

    case class Other(name: String) extends EnrichShapeRelation

    def apply(n: String): EnrichShapeRelation = n.toLowerCase match {
      case "intersects" => Intersects
      case "disjoint"   => Disjoint
      case "contains"   => Contains
      case "within"     => Within
      case other        => Other(other)
    }
  }

  case class EnrichProcessor(
    pipelineType: IngestPipelineType = IngestPipelineType.Default,
    description: Option[String] = None,
    column: String,
    policyName: String,
    field: String,
    maxMatches: Int = 1,
    ignoreFailure: Boolean = true,
    ignoreMissing: Option[Boolean] = None,
    overrideTarget: Option[Boolean] = None,
    shapeRelation: Option[EnrichShapeRelation] = None
  ) extends IngestProcessor {
    def processorType: IngestProcessorType = IngestProcessorType.Enrich

    def targetField: String = column

    override def sql: String = s"ENRICH USING POLICY $policyName FROM $field INTO $targetField"

    override def properties: ListMap[String, Any] = ListMap(
      "description"    -> description.getOrElse(sql),
      "policy_name"    -> policyName,
      "field"          -> field,
      "target_field"   -> targetField,
      "max_matches"    -> maxMatches,
      "ignore_failure" -> ignoreFailure
    ) ++ ignoreMissing.map("ignore_missing" -> _).toMap ++
      overrideTarget.map("override" -> _).toMap ++
      shapeRelation.map("shape_relation" -> _.sql).toMap
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
      //
      // 🔴 21.8 Part F.1 — the churn lived in this one expression. A `script` processor is the only
      // kind Elasticsearch stores with no `field`, so an anonymous one has no column; processor
      // `description`, which `IngestProcessor.apply` re-types it from, is an ES 7.9+ field that 6.8
      // drops and that `PipelineApi` strips before sending on 6.x besides. Keyed by `column`, such
      // a processor could never match the one that DECLARED it: every ALTER reported it REMOVED and
      // re-ADDED, on a processor nobody had touched.
      //
      // The identity was in the script all along — `ScriptProcessor` writes `ctx.<column> = …` as
      // the last statement of every source it generates — so `ScriptTarget.of` reads back exactly
      // what `ScriptTarget.assign` wrote.
      //
      // 🔴 It is applied HERE and to BOTH sides, rather than inside `GenericProcessor.column`,
      // and that placement is the design. `column` is read by `Table.defaultPipeline`'s
      // `filterNot`, by `IngestPipeline.merge`, by `ProcessorRemoved.stmt` and by `describe`;
      // giving an anonymous processor a real column name there made a hand-added
      // `ADD PROCESSOR SCRIPT(ctx.age = 1)` collide with a declared `age SCRIPT AS (…)` column and
      // get DROPPED — data loss, to fix a cosmetic diff. Confined to the key, the recovery reaches
      // the only consumer that needs it, and because both sides derive it the same way from the
      // same string, a source `of` misreads still yields the SAME key on both sides and so cannot
      // manufacture a difference.
      def identity(p: IngestProcessor): String =
        if (p.processorType == IngestProcessorType.Script)
          p.properties
            .get("source")
            .collect { case source: String => source }
            .flatMap(ScriptTarget.of)
            .getOrElse(p.column)
        else p.column

      def key(p: IngestProcessor) =
        s"${p.pipelineType.name}-${p.processorType.name}-${identity(p)}"

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

    def describe: Seq[ListMap[String, Any]] = processors.map(_.describe)
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

  // ========================================================================
  // SCHEMA COMPONENTS
  // ========================================================================

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
    * @param lineage
    *   sequence of (table, column) pairs indicating the lineage of this column
    */
  case class Column(
    name: String,
    dataType: SQLType,
    script: Option[ScriptProcessor] = None,
    multiFields: List[Column] = Nil,
    defaultValue: Option[Value[_]] = None,
    notNull: Boolean = false,
    comment: Option[String] = None,
    options: ListMap[String, Value[_]] = ListMap.empty,
    struct: Option[Column] = None,
    lineage: ListMap[String, Seq[(String, String)]] =
      ListMap.empty // ✅ Key = path ID, Value = chain
  ) extends DdlToken {
    def path: String = struct.map(st => s"${st.name}.$name").getOrElse(name)
    private def level: Int = struct.map(_.level + 1).getOrElse(0)

    private def cols: ListMap[String, Column] = ListMap(
      multiFields.map(field => field.name -> field): _*
    )

    /* Recursive find */
    def find(path: String): Option[Column] = {
      if (path.contains(".")) {
        val parts = path.split("\\.")
        cols.get(parts.head).flatMap(col => col.find(parts.tail.mkString(".")))
      } else {
        cols.get(path)
      }
    }

    /** Flattens all lineage paths into a set of (table, column) pairs */
    def allLineageSources: Set[(String, String)] =
      lineage.values.flatten.toSet

    /** Gets the immediate sources (last element of each path) */
    def immediateSources: Set[(String, String)] =
      lineage.values.flatMap(_.lastOption).toSet

    /** Gets the original sources (first element of each path) */
    def originalSources: Set[(String, String)] =
      lineage.values.flatMap(_.headOption).toSet

    def _meta: ListMap[String, Value[_]] = {
      ListMap(
        "data_type" -> StringValue(dataType.typeId),
        "not_null"  -> StringValue(s"$notNull")
      ) ++ defaultValue.map(d => "default_value" -> d) ++ comment.map(ct =>
        "comment" -> StringValue(ct)
      ) ++ script
        .map { sc =>
          ObjectValue(
            ListMap(
              "sql"      -> StringValue(sc.script),
              "column"   -> StringValue(path),
              "painless" -> StringValue(sc.source)
            ) ++ (if (sc.materialized) ListMap("materialized" -> BooleanValue(true))
                  else ListMap.empty[String, Value[_]])
          )
        }
        .map("script" -> _) ++ (if (multiFields.nonEmpty)
                                  ListMap(
                                    "multi_fields" -> ObjectValue(
                                      ListMap(
                                        multiFields.map(field =>
                                          field.name -> ObjectValue(field._meta)
                                        ): _*
                                      )
                                    )
                                  )
                                else ListMap.empty[String, Value[_]]) ++ (if (lineage.nonEmpty) {
                                                                            // ✅ Lineage as map of paths
                                                                            ListMap(
                                                                              "lineage" -> ObjectValue(
                                                                                lineage.map { case (pathId, chain) =>
                                                                                  pathId -> ObjectValues(
                                                                                    chain.map {
                                                                                      case (
                                                                                            table,
                                                                                            column
                                                                                          ) =>
                                                                                        ObjectValue(
                                                                                          ListMap(
                                                                                            "table" -> StringValue(
                                                                                              table
                                                                                            ),
                                                                                            "column" -> StringValue(
                                                                                              column
                                                                                            )
                                                                                          )
                                                                                        )
                                                                                    }
                                                                                  )
                                                                                }
                                                                              )
                                                                            )
                                                                          } else ListMap.empty)
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
            source =
              sc.source.replace(ScriptTarget.reference(name), ScriptTarget.reference(updated.path))
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

    /** Re-derive this column's ingest script — and its sub-fields' — against `schema`.
      *
      * Runs AFTER [[update]], so `path` is settled and the derivation targets the final
      * `ctx.<path>` directly instead of patching a string that was built for a different name.
      */
    def resolveScript(schema: Schema): Column =
      copy(
        script = script.map(_.resolvedAgainst(schema, path)),
        multiFields = multiFields.map(_.resolveScript(schema))
      )

    def sql: String = {
      val opts = if (options.nonEmpty) {
        s" OPTIONS ${ObjectValue(options).ddl}"
      } else {
        ""
      }
      val defaultOpt = defaultValue.map(v => s" DEFAULT ${v.sql}").getOrElse("")
      val notNullOpt = if (notNull) " NOT NULL" else ""
      val commentOpt = comment.map(c => s" COMMENT '${escapeStringLiteral(c)}'").getOrElse("")
      val fieldsOpt = if (multiFields.nonEmpty) {
        s" FIELDS (\n\t${multiFields.mkString(s",\n\t")}\n\t)"
      } else {
        ""
      }
      // 🔴 The `STORED` marker MUST be rendered. The DDL text is the only carrier: `Table.update()`
      // rebuilds `_meta.columns` from the parsed columns, so a flag that lives only in `_meta` is
      // erased on the first round-trip and the suppressed processor comes back.
      val scriptOpt = script
        .map(s => s" SCRIPT AS (${s.script})${if (s.materialized) " STORED" else ""}")
        .getOrElse("")
      val tabs = "\t" * level
      s"$tabs${renderColumnName(name)} $dataType$fieldsOpt$scriptOpt$defaultOpt$notNullOpt$commentOpt$opts"
    }

    def asMap(table: Table): Seq[ListMap[String, Any]] = Seq(
      ListMap(
        "Field" -> path,
        "Type"  -> dataType.typeId,
        "Null" -> (if (notNull) {
                     "no"
                   } else {
                     "yes"
                   }),
        "Key"     -> (if (table.primaryKey.contains(path)) "PRI" else ""),
        "Default" -> defaultValue.map(_.value).getOrElse("NULL"),
        "Comment" -> comment.getOrElse(""),
        "Script"  -> script.map(_.script).getOrElse(""),
        "Extra"   -> ObjectValue(options).ddl
      )
    ) ++ multiFields.flatMap(_.asMap(table))

    def processors: Seq[IngestProcessor] =
      script.filterNot(_.materialized).map(st => st.copy(column = path)).toSeq ++
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
        // `Char` joins them because story 21.5 gave it the same `elasticType` ("text"), and
        // Elasticsearch refuses `null_value` on a `text` field. The rule is "a text-mapped column
        // takes no null_value", with no "except" — leaving CHAR out would emit a mapping ES
        // rejects, which is a worse outcome than the `object` mapping it replaces.
        case SQLTypes.Varchar | SQLTypes.Text | SQLTypes.Char =>
        // do not set null_value for text types
        case _ =>
          defaultValue.foreach {
            case IngestTimestampValue => () // do not set null_value for ingest timestamp
            case dv => // set null_value for other types
              updateNode(root, ListMap("null_value" -> dv))
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
    def sql: String = s" PARTITION BY ${renderColumnName(column)} ($granularity)"

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
      extends Exception(s"Column $column does not exist in table $table")

  case class ColumnAlreadyExists(column: String, table: String)
      extends Exception(s"Column $column already exists in table $table")

  case class TableAlias(
    table: String,
    alias: String,
    filter: ListMap[String, Any] = ListMap.empty,
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
        case _ => ListMap.empty[String, Any]
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

  /** The type of a table, as recorded in the index mapping's `_meta.type`.
    *
    * Two names, deliberately, because they answer two different questions and only one of them is
    * allowed to change:
    *
    *   - `name` is what is STORED. It is written into `_meta.type` and read back by
    *     `TableType.apply`, so an existing index whose mapping says `"regular"` depends on it byte
    *     for byte.
    *   - `sqlName` is what is SHOWN on the ENGINE surface — the `type` column of `SHOW TABLES` and
    *     the `SHOW TABLE <t>` header, i.e. what the REPL and anything reading `SHOW TABLES` through
    *     the gateway receives. It must be a table type a SQL user recognises.
    *
    * ⚠️ `sqlName` is not, by itself, the JDBC/Flight `TABLE_TYPE`. Those drivers map it onto the
    * two values their `getTableTypes` advertises (`TABLE` / `VIEW`) — see `MaterializedView` below,
    * where that mapping is permanent and load-bearing rather than a temporary shim.
    *
    * The projection used to be `name.toUpperCase`, which published the internal `REGULAR`.
    * Measured: the sidecar's ADBC leg failed `Expected 'TABLE' in ['REGULAR']`. Story BIDC-10a Part
    * D.
    *
    * `sqlName` is ABSTRACT on purpose: a seventh table type cannot compile until someone decides
    * what to call it. That decision must not default silently.
    *
    * ⚠️ EXACTLY ONE place outside storage reads `name`, and it is deliberate: `Table.merge` throws
    * `Cannot alter table <t> of type <name>` (below, in this file) when an ALTER targets a
    * non-regular table. That message is the ENGINE refusing an operation on its own construct, not
    * a catalogue answer to a client, so it legitimately names the engine's own type. It is listed
    * here rather than left silent, because the whole point of this split is that no display of a
    * table type may be discovered by surprise — that is how `REGULAR` reached clients, and how the
    * `SHOW TABLE` header (`ResultRenderer`) went on printing the Scala type name `Regular`
    * unnoticed: neither could be found by searching for `tableType.name`.
    *
    * A new read of `name` outside `_meta.type` and that one error message is a design change, not a
    * detail: use `sqlName`, or amend this list.
    */
  sealed trait TableType {
    def name: String
    def sqlName: String
  }

  object TableType {
    case object Regular extends TableType {
      override def name: String = "regular"
      override def sqlName: String = "TABLE"
    }
    case object External extends TableType {
      override def name: String = "external"
      override def sqlName: String = "EXTERNAL"
    }
    case object Changelog extends TableType {
      override def name: String = "changelog"
      override def sqlName: String = "CHANGELOG"
    }
    case object Enrichment extends TableType {
      override def name: String = "enrichment"
      override def sqlName: String = "ENRICHMENT"
    }
    case object View extends TableType {
      override def name: String = "view"
      override def sqlName: String = "VIEW"
    }

    /** A materialized view keeps its OWN name on the ENGINE surface (`SHOW TABLES`, the REPL)
      * rather than collapsing into `VIEW`: it has storage, a refresh schedule and (optionally) a
      * watcher, and collapsing it would lose information for the human reading the listing.
      *
      * 🔴 The spelling is SPACED, and the reason is self-consistency with our own SQL, not any
      * other engine's convention. Every statement that names this object is spaced - `CREATE
      * MATERIALIZED VIEW`, `SHOW MATERIALIZED VIEW`, `SHOW MATERIALIZED VIEW STATUS`, `SHOW CREATE
      * MATERIALIZED VIEW`, `DESCRIBE MATERIALIZED VIEW` (`sql/.../query/package.scala`). An
      * underscore here would be the only place the product spells its own object differently from
      * the statement that creates it.
      *
      * ⚠️ This is NOT what a JDBC or Flight SQL client sees. Those `TABLE_TYPE` contracts report
      * `VIEW` for a materialized view, because `getTableTypes` advertises exactly `TABLE` and
      * `VIEW` and `getTables` filters by EXACT string match - so a third value, with the advertised
      * list unchanged, makes every materialized view vanish from an object browser that asks for
      * the advertised types. The drivers own that collapse; it is PERMANENT and load-bearing, not a
      * shim to be cleaned up. Story BIDC-10a, AD-A-6-SUPERSEDED.
      */
    case object MaterializedView extends TableType {
      override def name: String = "materialized_view"
      override def sqlName: String = "MATERIALIZED VIEW"
    }

    /** Parses the STORED name (`_meta.type`), never the display `sqlName`. Widening it to accept
      * `"TABLE"` would make two spellings of the same type storable and is not what any caller
      * needs: every caller reads a mapping this engine wrote.
      */
    def apply(name: String): TableType =
      name.toLowerCase match {
        case "regular"           => Regular
        case "external"          => External
        case "changelog"         => Changelog
        case "enrichment"        => Enrichment
        case "view"              => View
        case "materialized_view" => MaterializedView
        case other               => throw new Exception(s"Unknown table type: $other")
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
    * @param materializedViews
    *   optional list of materialized views associated to this table
    * @param materializedView
    *   whether this table is a materialized view
    */
  case class Table(
    name: String,
    columns: List[Column],
    primaryKey: List[String] = Nil,
    partitionBy: Option[PartitionDate] = None,
    mappings: ListMap[String, Value[_]] = ListMap.empty,
    settings: ListMap[String, Value[_]] = ListMap.empty,
    processors: Seq[IngestProcessor] = Seq.empty,
    aliases: ListMap[String, Value[_]] = ListMap.empty,
    materializedViews: List[String] = Nil,
    tableType: TableType = TableType.Regular
  ) extends DdlToken {
    lazy val isRegular: Boolean = tableType == TableType.Regular

    lazy val isPartitioned: Boolean = partitionBy.isDefined

    /** ⚠️ **Never put this on the DDL routing path.** It exists for ONE caller — the materialized
      * view generator, which needs a source index name to interpolate into a generated `INSERT INTO
      * … FROM <index>` — and there the table it is asked about already exists, so the lowercasing
      * is a no-op.
      *
      * Used anywhere that decides WHICH index a `CREATE TABLE` / `DROP TABLE` addresses, it is a
      * defect, because it silently repairs a name Elasticsearch would have refused. Tableau's
      * SQL-92 connection-capability probe is the live case: it issues `CREATE LOCAL TEMPORARY TABLE
      * "XT__…_Connect_CheckCreateTempTableCap"`, a name whose ONLY disqualifier is its uppercase
      * letters, so lowercasing it turns a deliberate `400` into a real index created in the
      * customer's cluster on every connect — with nobody deciding that it should be.
      * `ElasticClientHelpers.validateIndexName` is the layer that owns that decision and
      * `IndexNameProbeRefusalSpec` pins the outcome; this member must not pre-empt either.
      */
    lazy val indexName: String = name.toLowerCase
    private[schema] lazy val cols: ListMap[String, Column] = ListMap(
      columns.map(c => c.name -> c): _*
    )

    def find(path: String): Option[Column] = {
      if (path.contains(".")) {
        val parts = path.split("\\.")
        cols.get(parts.head).flatMap(col => col.find(parts.tail.mkString(".")))
      } else {
        cols.get(path)
      }
    }

    private lazy val _meta: ListMap[String, Value[_]] = ListMap.empty ++ ListMap({
      if (primaryKey.nonEmpty)
        Option("primary_key" -> StringValues(primaryKey.map(StringValue)))
      else
        None
    }.toList: _*) ++ partitionBy
      .map(pb =>
        ListMap(
          "partition_by" -> ObjectValue(
            ListMap(
              "column"      -> StringValue(pb.column),
              "granularity" -> StringValue(pb.granularity.script.get)
            )
          )
        )
      )
      .getOrElse(ListMap.empty) ++
      ListMap(
        "columns" -> ObjectValue(cols.map { case (name, col) => name -> ObjectValue(col._meta) })
      ) ++ ListMap(
        "type" -> StringValue(tableType.name)
      ) ++ (if (materializedViews.nonEmpty)
              ListMap(
                "materialized_views" -> StringValues(materializedViews.map(StringValue))
              )
            else ListMap.empty[String, Value[_]])

    def update(): Table = {
      val withPaths =
        this.copy(columns = columns.map(_.update())) // update columns first with struct info
      // ...THEN re-derive every `SCRIPT AS (…)` against the settled column list. This is the single
      // seam that covers BOTH DDL sites: `CreateTable.schema` builds its table from the parsed
      // columns and ends here, and `Table.merge` applies `ALTER … SET SCRIPT AS` to the LIVE table
      // and ends here too. The spec expected `ALTER` to need a client-side schema load, because the
      // STATEMENT carries no column list — but the point where it is APPLIED already holds the
      // whole table, so both are one derivation with no I/O and nothing new on the parse path.
      val updated = withPaths.copy(columns = withPaths.columns.map(_.resolveScript(withPaths)))
      updated.copy(
        mappings = updated.mappings ++ ListMap(
          "_meta" ->
          ObjectValue(updated.mappings.get("_meta") match {
            case Some(ObjectValue(value)) =>
              (value - "primary_key" - "partition_by" - "columns" - "materialized_views" - "type") ++ updated._meta
            case _ => updated._meta
          })
        )
      )
    }

    lazy val metadata: ListMap[String, Any] =
      mappings.get("_meta") match {
        case Some(ObjectValue(value)) => ObjectValue(value)
        case _                        => ListMap.empty
      }

    def sql: String = {
      val opts =
        if (mappings.nonEmpty || settings.nonEmpty || aliases.nonEmpty) {
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
          s"$separator\nOPTIONS ${Seq(
            mappingOpts,
            settingsOpts,
            aliasesOpts
          )
            .filter(_.nonEmpty)
            .mkString("(\n\t", ",\n\t", "\n)")}"
        } else {
          ""
        }
      val cols = columns.map(_.sql).mkString(",\n\t")
      val pkStr = if (primaryKey.nonEmpty) {
        s",\n\tPRIMARY KEY (${primaryKey.mkString(", ")})"
      } else {
        ""
      }
      s"CREATE OR REPLACE TABLE $name (\n\t$cols$pkStr\n)${partitionBy.map(p => s"\n$p").getOrElse("")}$opts"
    }

    def tableProcessors: Seq[IngestProcessor] =
      columns.flatMap(_.processors) ++ partitionBy
        .map(_.processor(this))
        .toSeq ++ implicitly[Seq[IngestProcessor]](primaryKey)

    def merge(statements: Seq[AlterTableStatement]): Table = {
      if (!isRegular)
        // BIDC-10a Part D - the deliberate exception recorded in TableType's scaladoc: this is the
        // engine refusing an operation on its own construct, so it names the STORED type. Every
        // client-facing rendering of a table type uses `TableType.sqlName` instead.
        throw new Exception(s"Cannot alter table $name of type ${tableType.name}")
      statements
        .foldLeft(this) { (table, statement) =>
          statement match {
            // table columns
            case AddColumn(column, ifNotExists) =>
              if (ifNotExists && table.cols.contains(column.name)) table
              else if (!table.cols.contains(column.name))
                table.copy(columns = table.columns :+ column)
              else throw ColumnAlreadyExists(column.name, table.name)
            case DropColumn(columnName, ifExists) =>
              if (ifExists && !table.cols.contains(columnName)) table
              else if (table.cols.contains(columnName))
                table.copy(columns = table.columns.filterNot(_.name == columnName))
              else throw ColumnNotFound(columnName, table.name)
            case RenameColumn(oldName, newName) =>
              if (table.cols.contains(oldName))
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
                      // A column is either multi-field or script-defined, never both — the grammar
                      // says so (`ident ~ extension_type ~ (script | optionalMultiFields)`), so a
                      // column carrying both renders DDL that cannot be parsed back.
                      col.copy(
                        script = Some(newScript.copy(dataType = col.dataType)),
                        multiFields = Nil
                      )
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
              val col = table.find(columnName)
              val exists = col.isDefined
              if (ifExists && !exists) table
              else {
                col match {
                  case Some(c) =>
                    // …and symmetrically: declaring sub-fields drops a script the column had.
                    val updated = c
                      .copy(
                        multiFields = newFields.toList.map(_.update(Some(c))),
                        script = None
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
              val col = table.find(columnName)
              val exists = col.isDefined
              if (ifExists && !exists) table
              else {
                col match {
                  case Some(c) =>
                    // The copy is the whole point of the branch: assigning it to nothing and
                    // returning `table` made SET|ADD FIELD a no-op that still reported success.
                    val updated = c
                      .copy(
                        multiFields =
                          c.multiFields.filterNot(_.name == field.name) :+ field.update(Some(c)),
                        script = None
                      )
                      .updateStruct()
                    table.copy(
                      columns = table.columns.map { existing =>
                        if (existing.name == updated.name) updated else existing
                      }
                    )
                  case _ => throw ColumnNotFound(columnName, table.name)
                }
              }
            case DropColumnField(
                  columnName,
                  fieldName,
                  ifExists
                ) =>
              val col = table.find(columnName)
              val exists = col.isDefined
              if (ifExists && !exists) table
              else {
                col match {
                  case Some(c) =>
                    val updated = c
                      .copy(
                        multiFields = c.multiFields.filterNot(_.name == fieldName)
                      )
                      .updateStruct()
                    table.copy(
                      columns = table.columns.map { existing =>
                        if (existing.name == updated.name) updated else existing
                      }
                    )
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
            case _ =>
              Column(
                name = colName,
                dataType = field.out
              )
          }
          cols :+ col
        }
      // 🔴 Decided AFTER the fold, over the COMPLETE projection. A script may legitimately read a
      // column that appears LATER in the SELECT list (`SELECT c, d FROM t1`), so an in-fold
      // decision would drop a script the projection does in fact satisfy -- measured, and pinned.
      //
      // 🔴 ...and BEFORE `update()`, which re-derives every SURVIVING script against the settled
      // column list. Applying the drop afterwards would leave `update()` deriving Painless for an
      // operand that is about to be taken away.
      val projected = this.copy(columns = cols)
      val retained = cols.map(withSatisfiableScripts(projected, _))
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
      this
        .copy(
          columns = retained,
          primaryKey = primaryKey,
          partitionBy = partitionBy,
          processors = retainedProcessors(projected),
          // 🔴 A CTAS target is a PLAIN INDEX, whatever it was projected FROM. Inheriting the
          // source's type made `CREATE TABLE t AS SELECT … FROM <a materialized view>` produce a
          // table typed `materialized_view`, which then:
          //   - short-circuits `validateScriptReferences` (its `isRegular` carve-out), so the
          //     computed-column rule is inert on exactly that target;
          //   - makes `Table.merge` THROW ("Cannot alter table … of type materialized_view") for
          //     every later ALTER on an ordinary index;
          //   - publishes `_meta.type = materialized_view`, so `SHOW TABLES` reports it as a view
          //     and the extension's metadata lookup can claim it.
          // `CREATE MATERIALIZED VIEW` is a different statement, owned by the extension, and it
          // builds its own schema with the type set explicitly. Verified 2026-09-21 against
          // softclient4es-extensions: nothing there calls `mergeWithSearch`, and every MV /
          // changelog / enrichment schema sets `tableType` itself.
          tableType = TableType.Regular,
          // ...and the source's OWN materialized views do not refresh from the copy.
          materializedViews = Nil
        )
        .update()
    }

    /** The TABLE-level processors (`Table.processors`, i.e. the ones a pipeline declares rather
      * than a column) a projection can carry.
      *
      * 🔴 The invariant: **a CTAS target's pipeline must not contain a processor that reads, or
      * writes, a column the target does not carry.** `mergeWithSearch` inherited `processors`
      * WHOLESALE, and `defaultPipeline` admits a table-level processor exactly when no COLUMN
      * contributes one for the same name -- so `CREATE TABLE t2 AS SELECT c FROM t1` over a source
      * carrying `set {field: c, copy_from: d}` shipped a pipeline reading `ctx.d` into an index
      * with no `d`. Measured, not reasoned: the processor was present in `t2.defaultPipeline` and
      * `validateScriptReferences` could not see it, because that rule walks COLUMNS.
      *
      * 🔴 The predicate is deliberately narrow: a dependency is disqualifying ONLY when it names a
      * column the SOURCE table declares and the projection did not carry. A name the source never
      * declared -- `_id`, an enrich target, any metadata field -- is none of the projection's
      * business, and treating it as missing would drop every primary-key processor. Filtering by
      * `p.column` alone does NOT fix the measured example either: there `column = c` IS projected
      * and the lost operand is `copy_from: d`.
      *
      * ⚠️ `GenericProcessor` is the one arm whose READS cannot be determined -- it is an arbitrary
      * JSON body read back from Elasticsearch with no schema. Its WRITE target is known
      * (`properties("field")`) and is checked; a `GenericProcessor` whose target survives while an
      * operand of its own does not is CARRIED, exactly as before. That is stated rather than
      * hidden, and pinned by a test.
      */
    private def retainedProcessors(projection: Schema): Seq[IngestProcessor] =
      processors.filter { processor =>
        val lost = processorDependencies(processor)
          .filter(name => this.find(name).isDefined && projection.find(name).isEmpty)
          .distinct
        if (lost.isEmpty) true
        else {
          logger.warn(
            s"Processor '${processor.sql}' of table '$name' is NOT carried into the table created " +
            s"from it: the projection does not select ${lost.map(m => s"'$m'").mkString(", ")}."
          )
          false
        }
      }

    /** Every column `processor` reads or writes, as far as its type allows that to be determined.
      *
      * `IngestProcessor` is `sealed`, so this match is exhaustive by construction and a new
      * processor type cannot be added without the compiler asking what it depends on -- the #318
      * failure mode (enumerating by type and failing OPEN) in the one place it would be silent.
      */
    private def processorDependencies(processor: IngestProcessor): Seq[String] =
      processor match {
        case s: ScriptProcessor =>
          s.column +: s.validationExpr.toSeq.flatMap(ScriptReferences.of).map(_._1.path)
        case r: RenameProcessor        => Seq(r.column, r.newName)
        case r: RemoveProcessor        => Seq(r.column)
        case k: PrimaryKeyProcessor    => k.column +: k.value.toSeq
        case s: SetProcessor           => s.column +: s.copyFrom.toSeq
        case d: DateIndexNameProcessor => Seq(d.column)
        case e: EnrichProcessor        => Seq(e.column, e.field)
        // the write target only -- see `retainedProcessors`: an arbitrary JSON body has no
        // determinable operands, and guessing at key names would be a second, weaker answer.
        case g: GenericProcessor => Seq(g.column)
      }

    /** `column` as the projection can carry it: a copied column whose EXECUTING script reads
      * operands the projection does not select keeps everything EXCEPT that script.
      *
      * 🔴 The script is DROPPED, not rejected. `CREATE TABLE t2 AS SELECT c FROM t1` populates `c`
      * from the `INSERT INTO … AS SELECT` that follows, so the data is right and the statement
      * works today; refusing it would break a statement the user never wrote a script for. What
      * does NOT work is the processor: it reads a column `t2` has no mapping for, throws on every
      * document, `ignore_failure` swallows it, and `SHOW CREATE TABLE t2` advertises a derivation
      * the index cannot perform. Copying it as a plain stored column is the honest shape.
      *
      * Recurses into `multiFields` for the same reason `validateScriptReferences` does: a computed
      * column can be declared inside a `STRUCT`, and its operands can be top-level columns the
      * projection dropped. ⚠️ MEASURED, because the recursion's lookup looks asymmetric and is not:
      * inside a `STRUCT`, `y INTEGER SCRIPT AS (YEAR(dt))` resolves `dt` to the TOP-LEVEL column of
      * that name -- the emission itself reads `ctx.dt`, not `ctx.meta.dt` -- so a top-level lookup
      * is the right one and there is no struct-relative resolution in this engine to be wrong
      * about. `validateScriptReferences` reads it the same way, which is why that shape is rejected
      * at parse time today (`Column 'meta.y' reads 'dt', which does not exist in table 's'`) and
      * can only be met on a table stored by an older version.
      *
      * 🔴 There is NO `isRegular` carve-out here, and the asymmetry with `validateScriptReferences`
      * is deliberate. That rule's carve-out exists because a non-regular table's computed column
      * legitimately reads a column of its SOURCE, which the INCOMING DOCUMENT carries at ingest
      * even though the table never declares it. A projection is the opposite situation: the rows
      * come from this statement's own `INSERT INTO … AS SELECT`, and a column the SELECT does not
      * project is not in them -- for a view exactly as for a table. Adding the carve-out here would
      * keep a script that is guaranteed to read `null`. (`mergeWithSearch` also types its target
      * `Regular`, so the backstop applies to it either way.)
      */
    private def withSatisfiableScripts(projection: Schema, column: Column): Column = {
      val kept = unsatisfiedScriptReferences(column, projection) match {
        case Nil => column
        case missing =>
          logger.warn(
            s"Computed column '${column.path}' is copied from table '$name' WITHOUT its script: " +
            s"the projection does not select ${missing.map(m => s"'$m'").mkString(", ")}. " +
            "The column keeps the values the query produces."
          )
          column.copy(script = None)
      }
      kept.copy(multiFields = kept.multiFields.map(withSatisfiableScripts(projection, _)))
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
      // A column is either multi-field or script-defined, never both: the grammar offers one or the
      // other (`ident ~ extension_type ~ (script | optionalMultiFields)`), so such a column renders
      // `name TEXT FIELDS (…) SCRIPT AS (…)`, which cannot be parsed back — and that rendering is
      // what SHOW CREATE TABLE and the diff statements emit. Loud here beats invalid DDL there.
      columns.filter(c => c.script.isDefined && c.multiFields.nonEmpty).foreach { c =>
        errors = errors :+
          s"Column ${c.name} of table $name cannot declare both FIELDS and SCRIPT AS"
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

    /** The pipeline this table DECLARES for `pipelineType`.
      *
      * 🔴 21.8 Part F.1. `GatewayApi.loadTablePipelineDiff` takes the pipeline type, uses it to
      * READ the right pipeline out of Elasticsearch, and then compared what it read against
      * `defaultPipeline` for BOTH types.
      *
      * MEASURED, because the obvious reading of that is wrong in both directions. Filtered to the
      * Final-typed entries the caller actually applies, comparing a stored final pipeline against
      * `defaultPipeline` and against `finalPipeline` order the SAME `ProcessorRemoved` — so this is
      * not, as first recorded, a change that turns churn into a deletion; the deletion is already
      * there. What the old comparison ADDS is a set of Default-typed `ProcessorAdded` entries drawn
      * from the default pipeline, which `alterExistingIndex` splices into `diff.pipeline` and then
      * applies as DEFAULT-pipeline changes. Comparing like with like removes those.
      *
      * ⚠️ It does not make final-pipeline handling correct. A table never DECLARES a final script
      * processor — the parser has no syntax for one, and the `Index -> Table` load attaches a final
      * pipeline's `ScriptProcessor` to its COLUMN, which puts it in `tableProcessors` and hence in
      * the DEFAULT pipeline — so a stored final script still diffs as removed. Only `rename`,
      * `remove` and a non-default `set` survive the load as Final-typed. Fixing that is a change to
      * the load path, not to this choice.
      *
      * Exhaustive on purpose: a catch-all answered `Custom` with the default pipeline, which is the
      * same defect one enum value over.
      */
    def declaredPipeline(pipelineType: IngestPipelineType): IngestPipeline =
      pipelineType match {
        case IngestPipelineType.Final   => finalPipeline
        case IngestPipelineType.Default => defaultPipeline
        case IngestPipelineType.Custom  => diffPipeline
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

    lazy val indexAliases: Seq[TableAlias] = {
      val temp = aliases.map { case (aliasName, value) =>
        TableAlias(name, aliasName, value)
      }.toSeq
      // add default read only alias name for partitioned tables
      /*if(isPartitioned){
        temp :+ TableAlias(
          table = name,
          alias = name,
          isWriteIndex = false
        )
      } else*/
      temp
    }

    lazy val indexTemplate: ObjectNode = {
      val node = mapper.createObjectNode()
      val patterns = mapper.createArrayNode()
      patterns.add(s"$name-*")
      node.set("index_patterns", patterns)
      node.put("priority", 1)
      val template = mapper.createObjectNode()
      template.set("mappings", indexMappings)
      template.set("settings", indexSettings)
      if (indexAliases.nonEmpty) {
        val aliasesNode = mapper.createObjectNode()
        indexAliases.foreach { alias =>
          aliasesNode.set(alias.alias, alias.node)
          // Same value-discard trap as #211: `foreach`'s `U` and `ObjectNode.set`'s `T` are both
          // free, so together they infer `Nothing` and the call site gets a cast that throws.
          // Without this `()`, every partitioned CREATE TABLE declaring an alias died with a
          // ClassCastException before its template was sent.
          ()
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

    def describe: Seq[ListMap[String, Any]] = columns.flatMap(_.asMap(this))
  }

}
