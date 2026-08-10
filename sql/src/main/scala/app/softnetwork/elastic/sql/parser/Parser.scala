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

package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql._
import app.softnetwork.elastic.sql.function.time.DateTimeFunction
import app.softnetwork.elastic.sql.function._
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql.parser.`type`.TypeParser
import app.softnetwork.elastic.sql.parser.function.aggregate.AggregateParser
import app.softnetwork.elastic.sql.parser.function.cond.CondParser
import app.softnetwork.elastic.sql.parser.function.convert.ConvertParser
import app.softnetwork.elastic.sql.parser.function.geo.GeoParser
import app.softnetwork.elastic.sql.parser.function.math.MathParser
import app.softnetwork.elastic.sql.parser.function.string.StringParser
import app.softnetwork.elastic.sql.parser.function.time.TemporalParser
import app.softnetwork.elastic.sql.parser.operator.math.ArithmeticParser
import app.softnetwork.elastic.sql.policy.EnrichPolicyType
import app.softnetwork.elastic.sql.query._
import app.softnetwork.elastic.sql.schema.{
  Column,
  IngestPipelineType,
  IngestProcessor,
  IngestProcessorType,
  PartitionDate,
  ScriptProcessor
}
import app.softnetwork.elastic.sql.time.TimeUnit
import app.softnetwork.elastic.sql.transform.{Delay, Frequency, TransformTimeUnit}
import app.softnetwork.elastic.sql.parser.http.HttpParser
import app.softnetwork.elastic.sql.watcher.{
  AlwaysWatcherCondition,
  ChainInput,
  CompareWatcherCondition,
  CronWatcherTrigger,
  EmptyWatcherInput,
  HttpInput,
  IntervalWatcherTrigger,
  LoggingAction,
  LoggingActionConfig,
  LoggingLevel,
  NeverWatcherCondition,
  ScriptWatcherCondition,
  SearchWatcherInput,
  SimpleWatcherInput,
  WatcherAction,
  WatcherCondition,
  WatcherInput,
  WatcherTrigger,
  WebhookAction
}

import scala.collection.immutable.ListMap
import scala.language.implicitConversions
import scala.language.existentials
import scala.util.matching.Regex
import scala.util.parsing.combinator.{PackratParsers, RegexParsers}
import scala.util.parsing.input.CharSequenceReader

/** Created by smanciot on 27/06/2018.
  *
  * SQL Parser for ElasticSearch
  */
object Parser
    extends Parser
    with SelectParser
    with FromParser
    with WhereParser
    with GroupByParser
    with HavingParser
    with OrderByParser
    with LimitParser {

  def single: PackratParser[SingleSearch] = {
    select ~ from ~ where.? ~ groupBy.? ~ having.? ~ orderBy.? ~ limit.? ~ onConflict.? ^^ {
      case s ~ f ~ w ~ g ~ h ~ o ~ l ~ oc =>
        SingleSearch(s, f, w, g, h, o, l, onConflict = oc).update()
    }
  }

  def union: PackratParser[UNION.type] = UNION.regex ^^ (_ => UNION)

  def searchStatement: PackratParser[SearchStatement] = rep1sep(single, union) ^^ {
    case x :: Nil => x
    case s        => MultiSearch(s)
  }

  def row: PackratParser[List[Value[_]]] =
    lparen ~> repsep(array_of_struct | struct | value, comma) <~ rparen

  def rows: PackratParser[List[List[Value[_]]]] =
    repsep(row, comma)

  def processorType: PackratParser[IngestProcessorType] =
    ident ^^ { name =>
      name.toLowerCase match {
        case "set"             => IngestProcessorType.Set
        case "script"          => IngestProcessorType.Script
        case "rename"          => IngestProcessorType.Rename
        case "remove"          => IngestProcessorType.Remove
        case "date_index_name" => IngestProcessorType.DateIndexName
        case "enrich"          => IngestProcessorType.Enrich
        case other             => IngestProcessorType(other)
      }
    }

  def processor: PackratParser[IngestProcessor] =
    processorType ~ objectValue ^^ { case pt ~ opts =>
      IngestProcessor(pt, opts)
    }

  def createOrReplacePipeline: PackratParser[CreatePipeline] =
    (keyword("CREATE") ~ keyword("OR") ~ keyword("REPLACE") ~ keyword(
      "PIPELINE"
    )) ~ ident ~ (keyword("WITH") ~ keyword("PROCESSORS")) ~ start ~ repsep(
      processor,
      separator
    ) ~ end ^^ { case _ ~ name ~ _ ~ _ ~ proc ~ _ =>
      CreatePipeline(name, IngestPipelineType.Custom, orReplace = true, processors = proc)
    }

  def createPipeline: PackratParser[CreatePipeline] =
    (keyword("CREATE") ~ keyword("PIPELINE")) ~ ifNotExists ~ ident ~ (keyword("WITH") ~ keyword(
      "PROCESSORS"
    ) ~ start) ~ repsep(
      processor,
      separator
    ) <~ end ^^ { case _ ~ ine ~ name ~ _ ~ proc =>
      CreatePipeline(name, IngestPipelineType.Custom, ifNotExists = ine, processors = proc)
    }

  def dropPipeline: PackratParser[DropPipeline] =
    (keyword("DROP") ~ keyword("PIPELINE")) ~ ifExists ~ ident ^^ { case _ ~ ie ~ name =>
      DropPipeline(name, ifExists = ie)
    }

  def showPipeline: PackratParser[ShowPipeline] =
    (keyword("SHOW") ~ keyword("PIPELINE")) ~ ident ^^ { case _ ~ pipeline =>
      ShowPipeline(pipeline)
    }

  def showPipelines: PackratParser[ShowPipelines.type] =
    (keyword("SHOW") ~ keyword("PIPELINES")) ^^ { _ =>
      ShowPipelines
    }

  def showCreatePipeline: PackratParser[ShowCreatePipeline] =
    (keyword("SHOW") ~ keyword("CREATE") ~ keyword("PIPELINE")) ~ ident ^^ {
      case _ ~ _ ~ _ ~ pipeline =>
        ShowCreatePipeline(pipeline)
    }

  def describePipeline: PackratParser[DescribePipeline] =
    ((keyword("DESCRIBE") | keyword("DESC")) ~ keyword("PIPELINE")) ~ ident ^^ {
      case _ ~ pipeline =>
        DescribePipeline(pipeline)
    }

  def addProcessor: PackratParser[AddPipelineProcessor] =
    (keyword("ADD") ~ keyword("PROCESSOR")) ~ processor ^^ { case _ ~ proc =>
      AddPipelineProcessor(proc)
    }

  def dropProcessor: PackratParser[DropPipelineProcessor] =
    (keyword("DROP") ~ keyword("PROCESSOR")) ~ processorType ~ start ~ ident ~ end ^^ {
      case _ ~ pt ~ _ ~ name ~ _ =>
        DropPipelineProcessor(pt, name)
    }

  def alterPipelineStatement: PackratParser[AlterPipelineStatement] =
    addProcessor | dropProcessor

  def alterPipeline: PackratParser[AlterPipeline] =
    (keyword("ALTER") ~ keyword("PIPELINE")) ~ ifExists ~ ident ~ start.? ~ repsep(
      alterPipelineStatement,
      separator
    ) ~ end.? >> { case _ ~ ie ~ pipeline ~ s ~ stmts ~ e =>
      if (s.isDefined && e.isEmpty) {
        err("Mismatched closing parentheses in ALTER PIPELINE statement")
      } else if (s.isEmpty && e.isDefined) {
        err("Mismatched opening parentheses in ALTER PIPELINE statement")
      } else if (s.isEmpty && e.isEmpty && stmts.size > 1) {
        err("Multiple ALTER PIPELINE statements require parentheses")
      } else
        success(AlterPipeline(pipeline, ie, stmts))
    }

  /** `FIELDS (…)` — required. The empty fallback belongs to `optionalMultiFields`, whose only
    * caller is a column definition (a column need not declare sub-fields). Folding it in here made
    * every consumer optional, and `alterColumnFields` (`ALTER COLUMN c SET <multiFields>`) then
    * matched a bare `SET` with an empty field list: `ALTER COLUMN c SET` parsed as a no-op, and
    * `ALTER COLUMN c SET FIELD raw KEYWORD` matched that same alternative and left `FIELD raw
    * KEYWORD` unconsumed — so `SET FIELD` silently did nothing before #213 made trailing input an
    * error, and could not parse at all afterwards.
    */
  def multiFields: PackratParser[List[Column]] =
    keyword("FIELDS") ~ start ~> repsep(column, separator) <~ end ^^ (cols => cols)

  def optionalMultiFields: PackratParser[List[Column]] = multiFields | success(Nil)

  def ifExists: PackratParser[Boolean] =
    opt(keyword("IF") ~ keyword("EXISTS")) ^^ {
      case Some(_) => true
      case None    => false
    }

  def ifNotExists: PackratParser[Boolean] =
    opt(keyword("IF") ~ keyword("NOT") ~ keyword("EXISTS")) ^^ {
      case Some(_) => true
      case None    => false
    }

  def notNull: PackratParser[Boolean] =
    opt(keyword("NOT") ~ keyword("NULL")) ^^ {
      case Some(_) => true
      case None    => false
    }

  def defaultVal: PackratParser[Option[Value[_]]] =
    opt(keyword("DEFAULT") ~ (value | ingest_id | ingest_timestamp)) ^^ {
      case Some(_ ~ v) => Some(v)
      case None        => None
    }

  def comment: PackratParser[Option[String]] =
    opt(keyword("COMMENT") ~ literal) ^^ {
      case Some(_ ~ v) => Some(v.value)
      case None        => None
    }

  def scriptValue: PackratParser[PainlessScript] = identifierWithArithmeticExpression |
    identifierWithTransformation |
    identifierWithIntervalFunction |
    identifierWithFunction

  def script: PackratParser[PainlessScript] =
    (keyword("SCRIPT") ~ keyword("AS")) ~ start ~ scriptValue ~ end ^^ { case _ ~ _ ~ s ~ _ => s }

  def column: PackratParser[Column] =
    ident ~ extension_type ~ (script | optionalMultiFields) ~ defaultVal ~ notNull ~ comment ~ (options | success(
      ListMap.empty[String, Value[_]]
    )) ^^ { case name ~ dt ~ mfs ~ dv ~ nn ~ ct ~ opts =>
      mfs match {
        case script: PainlessScript =>
          Column(
            name,
            dt,
            Some(ScriptProcessor.fromScript(name, script, Some(dt))),
            Nil,
            dv,
            nn,
            ct,
            opts
          )
        case cols: List[_] =>
          Column(name, dt, None, cols.asInstanceOf[List[Column]], dv, nn, ct, opts)
      }
    }

  def columns: PackratParser[List[Column]] =
    start ~ repsep(column, separator) ~ end ^^ { case _ ~ cols ~ _ => cols }

  def primaryKey: PackratParser[List[String]] =
    separator ~ keyword("PRIMARY") ~ keyword("KEY") ~ start ~ repsep(ident, separator) ~ end ^^ {
      case _ ~ _ ~ _ ~ _ ~ keys ~ _ =>
        keys
    } | success(Nil)

  def granularity: PackratParser[TimeUnit] = start ~
    ((keyword("YEAR") ^^^ TimeUnit.YEARS) |
    (keyword("MONTH") ^^^ TimeUnit.MONTHS) |
    (keyword("DAY") ^^^ TimeUnit.DAYS) |
    (keyword("HOUR") ^^^ TimeUnit.HOURS) |
    (keyword("MINUTE") ^^^ TimeUnit.MINUTES) |
    (keyword("SECOND") ^^^ TimeUnit.SECONDS)) ~ end ^^ { case _ ~ gf ~ _ => gf }

  def partitionBy: PackratParser[Option[PartitionDate]] =
    opt(keyword("PARTITION") ~ keyword("BY") ~ ident ~ opt(granularity)) ^^ {
      case Some(_ ~ _ ~ pb ~ gf) => Some(PartitionDate(pb, gf.getOrElse(TimeUnit.DAYS)))
      case None                  => None
    }

  def columnsWithPartitionBy
    : PackratParser[(List[Column], List[String], Option[PartitionDate], ListMap[String, Any])] =
    start ~ repsep(
      column,
      separator
    ) ~ primaryKey ~ end ~ partitionBy ~ ((separator.? ~> options) | success(
      ListMap.empty[String, Value[_]]
    )) ^^ { case _ ~ cols ~ pk ~ _ ~ pb ~ opts =>
      (cols, pk, pb, opts)
    }

  def createOrReplaceTable: PackratParser[CreateTable] =
    (keyword("CREATE") ~ keyword("OR") ~ keyword("REPLACE") ~ keyword(
      "TABLE"
    )) ~ ident ~ (columnsWithPartitionBy | (keyword("AS") ~> searchStatement)) ^^ {
      case _ ~ name ~ lr =>
        lr match {
          case (
                cols: List[Column],
                pk: List[String],
                p: Option[PartitionDate],
                opts: ListMap[String, Value[_]]
              ) =>
            CreateTable(
              name,
              Right(cols),
              ifNotExists = false,
              orReplace = true,
              primaryKey = pk,
              partitionBy = p,
              options = opts
            )
          case sel: SearchStatement =>
            CreateTable(name, Left(sel), ifNotExists = false, orReplace = true)
        }
    }

  def createTable: PackratParser[CreateTable] =
    (keyword("CREATE") ~ keyword(
      "TABLE"
    )) ~ ifNotExists ~ ident ~ (columnsWithPartitionBy | (keyword("AS") ~> searchStatement)) ^^ {
      case _ ~ ine ~ name ~ lr =>
        lr match {
          case (
                cols: List[Column],
                pk: List[String],
                p: Option[PartitionDate],
                opts: ListMap[String, Value[_]]
              ) =>
            CreateTable(name, Right(cols), ine, primaryKey = pk, partitionBy = p, options = opts)
          case sel: SearchStatement => CreateTable(name, Left(sel), ine)
        }
    }

  def patterns: PackratParser[List[String]] = keyword("LIKE") ~> repsep(literal, comma) ^^ {
    patterns =>
      patterns.map(_.value)
  }

  def showTables: PackratParser[ShowTables] =
    (keyword("SHOW") ~ keyword("TABLES")) ~> opt(patterns) ^^ { indices =>
      ShowTables(indices.getOrElse(Seq.empty))
    }

  def showTable: PackratParser[ShowTable] =
    (keyword("SHOW") ~ keyword("TABLE")) ~ ident ^^ { case _ ~ table =>
      ShowTable(table)
    }

  def showCreateTable: PackratParser[ShowCreateTable] =
    (keyword("SHOW") ~ keyword("CREATE") ~ keyword("TABLE")) ~ ident ^^ { case _ ~ _ ~ _ ~ table =>
      ShowCreateTable(table)
    }

  def describeTable: PackratParser[DescribeTable] =
    ((keyword("DESCRIBE") | keyword("DESC")) ~ opt(keyword("TABLE"))) ~ ident ^^ { case _ ~ table =>
      DescribeTable(table)
    }

  def dropTable: PackratParser[DropTable] =
    (keyword("DROP") ~ (keyword("TABLE") | keyword("INDEX"))) ~ ifExists ~ ident ^^ {
      case _ ~ ie ~ name =>
        DropTable(name, ifExists = ie)
    }

  def truncateTable: PackratParser[TruncateTable] =
    (keyword("TRUNCATE") ~ keyword("TABLE")) ~ ident ^^ { case _ ~ name =>
      TruncateTable(name)
    }

  def frequency: PackratParser[Frequency] =
    (keyword("REFRESH") ~ keyword(
      "EVERY"
    )) ~> """\d+\s+(MILLISECOND|SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)S?""".r ^^ { str =>
      val parts = str.trim.split("\\s+")
      Frequency(TransformTimeUnit(parts(1)), parts(0).toLong)
    }

  def withOptions: PackratParser[ListMap[String, Value[_]]] =
    (keyword("WITH") ~ lparen) ~> repsep(option, separator) <~ rparen ^^ { opts =>
      ListMap(opts: _*)
    }

  def createOrReplaceMaterializedView: PackratParser[CreateMaterializedView] =
    (keyword("CREATE") ~ keyword("OR") ~ keyword("REPLACE") ~ keyword("MATERIALIZED") ~ keyword(
      "VIEW"
    )) ~ ident ~ opt(frequency) ~ opt(
      withOptions
    ) ~ (keyword("AS") ~> searchStatement) ^^ { case _ ~ view ~ freq ~ opts ~ dql =>
      CreateMaterializedView(
        view,
        dql,
        ifNotExists = false,
        orReplace = true,
        frequency = freq,
        options = opts.getOrElse(ListMap.empty)
      )
    }

  def createMaterializedView: PackratParser[CreateMaterializedView] =
    (keyword("CREATE") ~ keyword("MATERIALIZED") ~ keyword("VIEW")) ~ ifNotExists ~ ident ~ opt(
      frequency
    ) ~ opt(
      withOptions
    ) ~ (keyword("AS") ~> searchStatement) ^^ { case _ ~ ine ~ view ~ freq ~ opts ~ dql =>
      CreateMaterializedView(
        view,
        dql,
        ifNotExists = ine,
        orReplace = false,
        frequency = freq,
        options = opts.getOrElse(ListMap.empty)
      )
    }

  def dropMaterializedView: PackratParser[DropMaterializedView] =
    (keyword("DROP") ~ keyword("MATERIALIZED") ~ keyword("VIEW")) ~ ifExists ~ ident ^^ {
      case _ ~ ie ~ name =>
        DropMaterializedView(name, ifExists = ie)
    }

  def refreshMaterializedView: PackratParser[RefreshMaterializedView] =
    (keyword("REFRESH") ~ keyword("MATERIALIZED") ~ keyword("VIEW")) ~ ifExists ~ ident ~ opt(
      keyword("WITH") ~ keyword("SCHEDULE") ~ keyword("NOW")
    ) ^^ { case _ ~ ie ~ view ~ wn =>
      RefreshMaterializedView(view, ifExists = ie, scheduleNow = wn.isDefined)
    }

  def showMaterializedViewStatus: PackratParser[ShowMaterializedViewStatus] =
    (keyword("SHOW") ~ keyword("MATERIALIZED") ~ keyword("VIEW") ~ keyword("STATUS")) ~ ident ^^ {
      case _ ~ _ ~ _ ~ _ ~ view =>
        ShowMaterializedViewStatus(view)
    }

  def showCreateMaterializedView: PackratParser[ShowCreateMaterializedView] =
    (keyword("SHOW") ~ keyword("CREATE") ~ keyword("MATERIALIZED") ~ keyword("VIEW")) ~ ident ^^ {
      case _ ~ _ ~ _ ~ _ ~ view =>
        ShowCreateMaterializedView(view)
    }

  def showMaterializedView: PackratParser[ShowMaterializedView] =
    (keyword("SHOW") ~ keyword("MATERIALIZED") ~ keyword("VIEW")) ~ ident ^^ { case _ ~ _ ~ view =>
      ShowMaterializedView(view)
    }

  def showMaterializedViews: PackratParser[ShowMaterializedViews.type] =
    (keyword("SHOW") ~ keyword("MATERIALIZED") ~ keyword("VIEWS")) ^^ { _ =>
      ShowMaterializedViews
    }

  def describeMaterializedView: PackratParser[DescribeMaterializedView] =
    ((keyword("DESCRIBE") | keyword("DESC")) ~ keyword("MATERIALIZED") ~ keyword(
      "VIEW"
    )) ~ ident ^^ { case _ ~ _ ~ _ ~ view =>
      DescribeMaterializedView(view)
    }

  def addColumn: PackratParser[AddColumn] =
    (keyword("ADD") ~ keyword("COLUMN")) ~ ifNotExists ~ column ^^ { case _ ~ ine ~ col =>
      AddColumn(col, ifNotExists = ine)
    }

  def dropColumn: PackratParser[DropColumn] =
    (keyword("DROP") ~ keyword("COLUMN")) ~ ifExists ~ ident ^^ { case _ ~ ie ~ name =>
      DropColumn(name, ifExists = ie)
    }

  def renameColumn: PackratParser[RenameColumn] =
    (keyword("RENAME") ~ keyword("COLUMN")) ~ ident ~ (keyword("TO") ~> ident) ^^ {
      case _ ~ oldName ~ newName =>
        RenameColumn(oldName, newName)
    }

  def alterColumnIfExists: PackratParser[Boolean] =
    (keyword("ALTER") ~ keyword("COLUMN")) ~ ifExists ^^ { case _ ~ ie =>
      ie
    }

  def alterColumnOptions: PackratParser[AlterColumnOptions] =
    alterColumnIfExists ~ ident ~ keyword("SET") ~ options ^^ { case ie ~ col ~ _ ~ opts =>
      AlterColumnOptions(col, opts, ifExists = ie)
    }

  def alterColumnOption: PackratParser[AlterColumnOption] =
    alterColumnIfExists ~ ident ~ ((keyword("SET") | keyword("ADD")) ~ keyword(
      "OPTION"
    )) ~ start ~ option ~ end ^^ { case ie ~ col ~ _ ~ _ ~ opt ~ _ =>
      AlterColumnOption(col, opt._1, opt._2, ifExists = ie)
    }

  def dropColumnOption: PackratParser[DropColumnOption] =
    alterColumnIfExists ~ ident ~ (keyword("DROP") ~ keyword("OPTION")) ~ ident ^^ {
      case ie ~ col ~ _ ~ optionName =>
        DropColumnOption(col, optionName, ifExists = ie)
    }

  def alterColumnFields: PackratParser[AlterColumnFields] =
    alterColumnIfExists ~ ident ~ keyword("SET") ~ multiFields ^^ { case ie ~ col ~ _ ~ fields =>
      AlterColumnFields(col, fields, ifExists = ie)
    }

  def alterColumnField: PackratParser[AlterColumnField] =
    alterColumnIfExists ~ ident ~ ((keyword("SET") | keyword("ADD")) ~ keyword(
      "FIELD"
    )) ~ column ^^ { case ie ~ col ~ _ ~ field =>
      AlterColumnField(col, field, ifExists = ie)
    }

  def dropColumnField: PackratParser[DropColumnField] =
    alterColumnIfExists ~ ident ~ (keyword("DROP") ~ keyword("FIELD")) ~ ident ^^ {
      case ie ~ col ~ _ ~ fieldName =>
        DropColumnField(col, fieldName, ifExists = ie)
    }

  def alterColumnType: PackratParser[AlterColumnType] =
    alterColumnIfExists ~ ident ~ (keyword("SET") ~ keyword("DATA") ~ keyword(
      "TYPE"
    )) ~ extension_type ^^ { case ie ~ name ~ _ ~ newType =>
      AlterColumnType(name, newType, ifExists = ie)
    }

  def alterColumnScript: PackratParser[AlterColumnScript] =
    alterColumnIfExists ~ ident ~ keyword("SET") ~ script ^^ { case ie ~ name ~ _ ~ ns =>
      AlterColumnScript(
        name,
        ScriptProcessor.fromScript(name, ns, Some(ns.out)),
        ifExists = ie
      )
    }

  def dropColumnScript: PackratParser[DropColumnScript] =
    alterColumnIfExists ~ ident ~ (keyword("DROP") ~ keyword("SCRIPT")) ^^ { case ie ~ name ~ _ =>
      DropColumnScript(name, ifExists = ie)
    }

  /** The value grammar must match `defaultVal`'s: a column declares `DEFAULT _ingest.timestamp` at
    * CREATE time, so an ALTER that sets the same default on an existing column has to accept it
    * too. It did not, which broke the `_last_updated` column the materialized-view machinery adds
    * whenever that column already exists (`TableDiff` renders `ColumnDefaultSet` and the extension
    * runs the rendered SQL).
    */
  def alterColumnDefault: PackratParser[AlterColumnDefault] =
    alterColumnIfExists ~ ident ~ (keyword("SET") ~ keyword(
      "DEFAULT"
    )) ~ (value | ingest_id | ingest_timestamp) ^^ { case ie ~ name ~ _ ~ dv =>
      AlterColumnDefault(name, dv, ifExists = ie)
    }

  def dropColumnDefault: PackratParser[DropColumnDefault] =
    alterColumnIfExists ~ ident ~ (keyword("DROP") ~ keyword("DEFAULT")) ^^ { case ie ~ name ~ _ =>
      DropColumnDefault(name, ifExists = ie)
    }

  def alterColumnNotNull: PackratParser[AlterColumnNotNull] =
    alterColumnIfExists ~ ident ~ (keyword("SET") ~ keyword("NOT") ~ keyword("NULL")) ^^ {
      case ie ~ name ~ _ =>
        AlterColumnNotNull(name, ifExists = ie)
    }

  def dropColumnNotNull: PackratParser[DropColumnNotNull] =
    alterColumnIfExists ~ ident ~ (keyword("DROP") ~ keyword("NOT") ~ keyword("NULL")) ^^ {
      case ie ~ name ~ _ =>
        DropColumnNotNull(name, ifExists = ie)
    }

  def alterColumnComment: PackratParser[AlterColumnComment] =
    alterColumnIfExists ~ ident ~ (keyword("SET") ~ keyword("COMMENT")) ~ literal ^^ {
      case ie ~ name ~ _ ~ c =>
        AlterColumnComment(name, c.value, ifExists = ie)
    }

  def dropColumnComment: PackratParser[DropColumnComment] =
    alterColumnIfExists ~ ident ~ (keyword("DROP") ~ keyword("COMMENT")) ^^ { case ie ~ name ~ _ =>
      DropColumnComment(name, ifExists = ie)
    }

  def alterTableMapping: PackratParser[AlterTableMapping] =
    ((keyword("SET") | keyword("ADD")) ~ keyword("MAPPING")) ~ option ^^ { case _ ~ opt =>
      AlterTableMapping(opt._1, opt._2)
    }

  def dropTableMapping: PackratParser[DropTableMapping] =
    (keyword("DROP") ~ keyword("MAPPING")) ~> ident ^^ { m => DropTableMapping(m) }

  def alterTableSetting: PackratParser[AlterTableSetting] =
    ((keyword("SET") | keyword("ADD")) ~ keyword("SETTING")) ~ option ^^ { case _ ~ opt =>
      AlterTableSetting(opt._1, opt._2)
    }

  def dropTableSetting: PackratParser[DropTableSetting] =
    (keyword("DROP") ~ keyword("SETTING")) ~> ident ^^ { m => DropTableSetting(m) }

  def alterTableAlias: PackratParser[AlterTableAlias] =
    ((keyword("SET") | keyword("ADD")) ~ keyword("ALIAS")) ~ option ^^ { case _ ~ opt =>
      AlterTableAlias(opt._1, opt._2)
    }

  def dropTableAlias: PackratParser[DropTableAlias] =
    (keyword("DROP") ~ keyword("ALIAS")) ~> ident ^^ { m => DropTableAlias(m) }

  def alterTableStatement: PackratParser[AlterTableStatement] =
    addColumn |
    dropColumn |
    renameColumn |
    alterColumnOptions |
    alterColumnOption |
    dropColumnOption |
    alterColumnType |
    alterColumnScript |
    dropColumnScript |
    alterColumnDefault |
    dropColumnDefault |
    alterColumnNotNull |
    dropColumnNotNull |
    alterColumnComment |
    dropColumnComment |
    alterColumnFields |
    alterColumnField |
    dropColumnField |
    alterTableMapping |
    dropTableMapping |
    alterTableSetting |
    dropTableSetting |
    alterTableAlias |
    dropTableAlias

  def alterTable: PackratParser[AlterTable] =
    (keyword("ALTER") ~ keyword("TABLE")) ~ ifExists ~ ident ~ start.? ~ repsep(
      alterTableStatement,
      separator
    ) ~ end.? >> { case _ ~ ie ~ table ~ s ~ stmts ~ e =>
      // `err`, not `throw`: these run inside a combinator, and `Parser.apply` is typed
      // `Either[ParserError, Statement]` — a raw exception escapes that signature and only
      // `GatewayApi` happens to wrap the call in `ElasticResult.attempt`.
      if (s.isDefined && e.isEmpty) {
        err("Mismatched closing parentheses in ALTER TABLE statement")
      } else if (s.isEmpty && e.isDefined) {
        err("Mismatched opening parentheses in ALTER TABLE statement")
      } else if (s.isEmpty && e.isEmpty && stmts.size > 1) {
        err("Multiple ALTER TABLE statements require parentheses")
      } else
        success(AlterTable(table, ie, stmts))
    }

  // Watcher parsers

  // Watcher condition parsers
  def alwaysWatcherCondition: PackratParser[AlwaysWatcherCondition.type] =
    keyword("ALWAYS") ^^ { _ => AlwaysWatcherCondition }

  def neverWatcherCondition: PackratParser[NeverWatcherCondition.type] =
    keyword("NEVER") ^^ { _ => NeverWatcherCondition }

  private def comparison_operator: PackratParser[ComparisonOperator] =
    eq | ne | diff | gt | ge | lt | le

  private def dateMathScript
    : PackratParser[DateTimeFunction with FunctionWithIdentifier with DateMathScript] =
    date_add | datetime_add | date_sub | datetime_sub

  def compareWatcherCondition: PackratParser[CompareWatcherCondition] =
    keyword("WHEN") ~> opt(not) ~ ident ~ comparison_operator ~ opt(value) ~ opt(
      dateMathScript
    ) >> { case n ~ field ~ op ~ v ~ fun =>
      val target_op =
        n match {
          case Some(_) => op.not
          case None    => op
        }
      v match {
        case Some(value) =>
          success(CompareWatcherCondition(field, target_op, Left(value)))
        case None =>
          fun match {
            case Some(f) if f.identifier.dependencies.isEmpty =>
              success(
                CompareWatcherCondition(
                  field,
                  target_op,
                  Right(f.identifier.withFunctions(f +: f.identifier.functions))
                )
              )
            case Some(_) =>
              err(
                "Date/datetime functions with field dependencies are not supported for comparison"
              )
            case None =>
              err("A value or a date/datetime function must be provided for comparison")
          }
      }
    }

  private def scriptParams: PackratParser[ListMap[String, Value[_]]] =
    (keyword("WITH") ~ keyword("PARAMS")) ~> lparen ~ repsep(option, comma) ~ rparen ^^ {
      case _ ~ opts ~ _ =>
        ListMap(opts: _*)
    }

  def scriptWatcherCondition: PackratParser[ScriptWatcherCondition] =
    (keyword("WHEN") ~ keyword("SCRIPT")) ~> literal ~ opt(
      keyword("USING") ~ keyword("LANG") ~> literal
    ) ~ opt(
      scriptParams
    ) ~ opt(keyword("RETURNS") ~ keyword("TRUE")) ^^ { case scr ~ lang ~ p ~ _ =>
      ScriptWatcherCondition(
        scr.value,
        lang.map(_.value).getOrElse("painless"),
        p.getOrElse(ListMap.empty)
      )
    }

  def watcherCondition: PackratParser[WatcherCondition] =
    neverWatcherCondition | alwaysWatcherCondition | compareWatcherCondition | scriptWatcherCondition

  // Watcher trigger parsers
  def triggerWatcherEveryInterval: PackratParser[IntervalWatcherTrigger] =
    keyword("EVERY") ~> """\d+\s+(MILLISECOND|SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)S?""".r ^^ {
      str =>
        val parts = str.trim.split("\\s+")
        IntervalWatcherTrigger(Delay(TransformTimeUnit(parts(1)), parts(0).toLong))
    }

  def triggerWatcherAtSchedule: PackratParser[CronWatcherTrigger] =
    (keyword("AT") ~ keyword("SCHEDULE")) ~> literal ^^ { cronExpr =>
      CronWatcherTrigger(cronExpr.value)
    }

  def watcherTrigger: PackratParser[WatcherTrigger] =
    triggerWatcherEveryInterval | triggerWatcherAtSchedule

  // Watcher input parsers
  def simpleWatcherInput: PackratParser[SimpleWatcherInput] =
    opt(keyword("WITH") ~ keyword("INPUT")) ~> start ~ repsep(option, comma) ~ end ^^ {
      case _ ~ opts ~ _ =>
        SimpleWatcherInput(payload = ObjectValue(ListMap(opts: _*)))
    }

  def withinTimeout: PackratParser[Option[Delay]] =
    opt(
      keyword("WITHIN") ~> """(\d+\s+(MILLISECOND|SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)S?)""".r
    ) ^^ {
      case Some(str) =>
        val parts = str.trim.split("\\s+")
        Some(Delay(TransformTimeUnit(parts(1)), parts(0).toLong))
      case None => None
    }

  /** Resolve alias-qualified identifiers in a WHERE against its own FROM clause — `o.status`
    * becomes `status`, tagged with `table = orders` — exactly as the SELECT path does. Any
    * production that keeps the criteria while flattening the FROM to bare index names MUST run
    * this: without it the alias is dropped from the index list but kept in the field name, and the
    * query targets a field that exists in no index (#212).
    *
    * `Select(Nil)` keeps the throwaway request's SELECT pass a no-op — the default `Select()` is
    * `SELECT *` — leaving `update()` to do its FROM and WHERE passes and nothing else. `update()`
    * reaches the criteria through `map`, so a `Some` WHERE can never come back `None`; callers for
    * which a lost WHERE would be destructive assert that anyway.
    */
  private def resolveWhere(f: From, w: Option[Where]): Option[Where] =
    SingleSearch(select = Select(Nil), from = f, where = w).update().where

  /** A single Elasticsearch search applies one query to every index it names, so a table-qualified
    * predicate over a multi-index FROM asks for per-index scoping that cannot be honored — whether
    * it correlates the indices (`WHERE o.id = c.order_id`, an ANSI-89 join, #191's defect in the
    * older syntax) or merely scopes to one of them (`WHERE orders.status = 'F'`, which would
    * silently filter `refunds` too).
    *
    * Testing for "any qualifier" rather than "qualifiers from two tables" is deliberate: the
    * narrower test lets a correlation through whenever one side fails to resolve — a function
    * argument (`WHERE o.id = LOWER(c.id)`), or a self-join through duplicate table names, where
    * `From.tableAliases` is keyed by table name and keeps only the last alias.
    */
  private def qualifiedOverManyIndices(f: From, criteria: Option[Criteria]): Boolean =
    f.tables.size > 1 && criteria.exists(_.referencedIdentifiers.exists(_.table.isDefined))

  // A watcher search input maps to Elasticsearch's `search` input, which knows nothing but a list
  // of indices — there is no join engine behind it. `from` parses `a JOIN b ON …` happily, so
  // without this guard the join is dropped and the watcher silently watches `a` alone (#191).
  // `err` (not `failure`) is deliberate: it short-circuits the enclosing alternatives instead of
  // letting `watcherInput` fall through to `success(EmptyWatcherInput)` and report a position
  // error that names neither JOIN nor the watcher.
  def searchInput: PackratParser[SearchWatcherInput] =
    from ~ opt(where) ~ withinTimeout >> { case f ~ w ~ t =>
      f.joins match {
        case Nil =>
          val criteria = resolveWhere(f, w).flatMap(_.criteria)
          // `FROM a, b` stays a legitimate multi-index search; only a qualifier over it is
          // unserviceable — see `qualifiedOverManyIndices`.
          if (qualifiedOverManyIndices(f, criteria))
            err(
              s"A watcher input cannot qualify a column by table when it searches several " +
              s"indices (${f.tables.map(_.name).mkString(", ")}): one Elasticsearch search " +
              "applies one query to all of them, so it can neither join them nor scope a " +
              "predicate to one. Watch a single index, drop the qualifiers, or pre-join the " +
              "sources with a MATERIALIZED VIEW and watch the view."
            )
          else
            success(
              SearchWatcherInput(
                f.tables.map(_.name).distinct,
                criteria,
                t
              )
            )
        case joins =>
          err(
            s"JOIN is not supported in a watcher input (${joins.map(_.sql.trim).mkString(" ")}): " +
            "a watcher input can only search one or more indices (FROM index1, index2). " +
            "Pre-join the sources with a MATERIALIZED VIEW and have the watcher search the view."
          )
      }
    }

  def httpInput: PackratParser[HttpInput] =
    opt(keyword("WITH") ~ keyword("INPUT")) ~> httpRequest ^^ { req =>
      HttpInput(req)
    }

  def chainInput: PackratParser[(String, WatcherInput)] =
    ident ~ opt(keyword("AS")) ~ watcherInput ^^ { case name ~ _ ~ input =>
      (name, input)
    }

  def chainInputs: PackratParser[WatcherInput] =
    (keyword("WITH") ~ keyword("INPUTS")) ~> rep1sep(
      chainInput,
      comma
    ) ^^ { inputs =>
      ChainInput(ListMap(inputs: _*))
    }

  def watcherInput: PackratParser[WatcherInput] =
    chainInputs | searchInput | httpInput | simpleWatcherInput | success(EmptyWatcherInput)

  // logging action parsers
  def info: Parser[LoggingLevel] = "(?i)(INFO)\\b".r ^^ { _ => LoggingLevel.INFO }
  def debug: Parser[LoggingLevel] = "(?i)(DEBUG)\\b".r ^^ { _ => LoggingLevel.DEBUG }
  def warn: Parser[LoggingLevel] = "(?i)(WARN)\\b".r ^^ { _ => LoggingLevel.WARN }
  def error: Parser[LoggingLevel] = "(?i)(ERROR)\\b".r ^^ { _ => LoggingLevel.ERROR }

  def loggingLevel: PackratParser[LoggingLevel] =
    info | debug | warn | error

  // action foreach limit parser
  def foreachWithLimit: PackratParser[(String, Int)] =
    (keyword("FOREACH") ~> literal) ~ (keyword("LIMIT") ~> """\d+""".r) ^^ { case fe ~ l =>
      (fe.value, l.toInt)
    }

  // simple logging action parser
  def loggingAction: PackratParser[Option[LoggingAction]] =
    (keyword("LOG") ~> literal) ~ opt(keyword("AT") ~> loggingLevel) ~ opt(foreachWithLimit) ^^ {
      case text ~ levelOpt ~ feOpt =>
        val foreach = feOpt.map(_._1)
        val limit = feOpt.map(_._2)
        Some(LoggingAction(LoggingActionConfig(text.value, levelOpt), foreach, limit))
    }

  // webhook action parser
  def webhookAction: PackratParser[Option[WebhookAction]] =
    keyword("WEBHOOK") ~> httpRequest ~ opt(foreachWithLimit) ^^ { case req ~ feOpt =>
      val foreach = feOpt.map(_._1)
      val limit = feOpt.map(_._2)
      Some(WebhookAction(req, foreach, limit))
    }

  def watcherAction: PackratParser[(String, WatcherAction)] =
    ident ~ opt(keyword("AS")) ~ (loggingAction | webhookAction) >> { case name ~ _ ~ wa =>
      wa match {
        case Some(wa) => success((name, wa))
        case _        => err(s"Unsupported watcher action type in action '$name'")
      }
    }

  def watcherActions: PackratParser[ListMap[String, WatcherAction]] =
    rep1sep(
      watcherAction,
      separator
    ) ^^ { actions =>
      ListMap(actions: _*)
    }

  def createOrReplaceWatcher: PackratParser[CreateWatcher] =
    (keyword("CREATE") ~ keyword("OR") ~ keyword("REPLACE") ~ keyword("WATCHER")) ~> ident ~ opt(
      keyword("AS")
    ) ~ watcherTrigger ~ watcherInput ~ watcherCondition ~ (keyword(
      "DO"
    ) ~> watcherActions <~ keyword("END")) ^^ {
      case name ~ _ ~ trigger ~ input ~ condition ~ actions =>
        CreateWatcher(
          name = name,
          orReplace = true,
          ifNotExists = false,
          condition = condition,
          trigger = trigger,
          actions = actions,
          input = input
        )
    }

  def createWatcher: PackratParser[CreateWatcher] =
    (keyword("CREATE") ~ keyword("WATCHER")) ~ ifNotExists ~ ident ~ opt(
      keyword("AS")
    ) ~ watcherTrigger ~ watcherInput ~ watcherCondition ~ (keyword(
      "DO"
    ) ~> watcherActions <~ keyword("END")) ^^ {
      case _ ~ _ ~ ine ~ name ~ _ ~ trigger ~ input ~ condition ~ actions =>
        CreateWatcher(
          name = name,
          orReplace = false,
          ifNotExists = ine,
          condition = condition,
          trigger = trigger,
          actions = actions,
          input = input
        )
    }

  def showWatcherStatus: PackratParser[ShowWatcherStatus] =
    (keyword("SHOW") ~ keyword("WATCHER") ~ keyword("STATUS")) ~> ident ^^ { name =>
      ShowWatcherStatus(name)
    }

  def showWatchers: PackratParser[ShowWatchers.type] =
    (keyword("SHOW") ~ keyword("WATCHERS")) ^^ { _ =>
      ShowWatchers
    }

  def dropWatcher: PackratParser[DropWatcher] =
    (keyword("DROP") ~ keyword("WATCHER")) ~ ifExists ~ ident ^^ { case _ ~ ie ~ name =>
      DropWatcher(name, ifExists = ie)
    }

  def createEnrichPolicy: PackratParser[CreateEnrichPolicy] =
    (keyword("CREATE") ~ keyword("ENRICH") ~ keyword("POLICY")) ~
    ifNotExists ~
    ident ~
    opt(keyword("TYPE") ~> (keyword("MATCH") | keyword("GEO_MATCH") | keyword("RANGE"))) ~
    (keyword("FROM") ~> repsep(ident, separator)) ~
    (keyword("ON") ~> ident) ~
    (keyword("ENRICH") ~> repsep(ident, separator)) ~
    opt(where) ^^ { case _ ~ ine ~ name ~ policyTypeOpt ~ sources ~ on ~ refreshFields ~ whereOpt =>
      val policyType = policyTypeOpt match {
        case Some(value) => EnrichPolicyType(value)
        case _           => EnrichPolicyType.Match
      }
      CreateEnrichPolicy(
        name = name,
        policyType = policyType,
        from = sources,
        on = on,
        refreshFields,
        whereOpt,
        ifNotExists = ine
      )
    }

  def createOrReplaceEnrichPolicy: PackratParser[CreateEnrichPolicy] =
    (keyword("CREATE") ~ keyword("OR") ~ keyword("REPLACE") ~ keyword("ENRICH") ~ keyword(
      "POLICY"
    )) ~
    ident ~
    opt(keyword("TYPE") ~> (keyword("MATCH") | keyword("GEO_MATCH") | keyword("RANGE"))) ~
    (keyword("FROM") ~> repsep(ident, separator)) ~
    (keyword("ON") ~> ident) ~
    (keyword("ENRICH") ~> repsep(ident, separator)) ~
    opt(where) ^^ { case _ ~ name ~ policyTypeOpt ~ sources ~ on ~ refreshFields ~ whereOpt =>
      val policyType = policyTypeOpt match {
        case Some("MATCH")     => EnrichPolicyType.Match
        case Some("GEO_MATCH") => EnrichPolicyType.GeoMatch
        case Some("RANGE")     => EnrichPolicyType.Range
        case _                 => EnrichPolicyType.Match
      }
      CreateEnrichPolicy(
        name = name,
        policyType = policyType,
        from = sources,
        on = on,
        refreshFields,
        whereOpt,
        orReplace = true
      )
    }

  def executeEnrichPolicy: PackratParser[ExecuteEnrichPolicy] =
    (keyword("EXECUTE") ~ keyword("ENRICH") ~ keyword("POLICY")) ~> ident ^^ { name =>
      ExecuteEnrichPolicy(name)
    }

  def dropEnrichPolicy: PackratParser[DropEnrichPolicy] =
    (keyword("DROP") ~ keyword("ENRICH") ~ keyword("POLICY")) ~ ifExists ~ ident ^^ {
      case _ ~ ie ~ name =>
        DropEnrichPolicy(name, ifExists = ie)
    }

  def showEnrichPolicy: PackratParser[ShowEnrichPolicy] =
    (keyword("SHOW") ~ keyword("ENRICH") ~ keyword("POLICY")) ~> ident ^^ { name =>
      ShowEnrichPolicy(name)
    }

  def showEnrichPolicies: PackratParser[ShowEnrichPolicies.type] =
    (keyword("SHOW") ~ keyword("ENRICH") ~ keyword("POLICIES")) ^^ { _ =>
      ShowEnrichPolicies
    }

  def showClusterName: PackratParser[ShowClusterName.type] =
    (keyword("SHOW") ~ keyword("CLUSTER") ~ keyword("NAME")) ^^ { _ =>
      ShowClusterName
    }

  def showLicense: PackratParser[ShowLicense.type] =
    (keyword("SHOW") ~ keyword("LICENSE")) ^^ { _ =>
      ShowLicense
    }

  def refreshLicense: PackratParser[RefreshLicense.type] =
    (keyword("REFRESH") ~ keyword("LICENSE")) ^^ { _ =>
      RefreshLicense
    }

  def dqlStatement: PackratParser[DqlStatement] = {
    searchStatement |
    showTables |
    showTable |
    showCreateTable |
    showPipelines |
    showPipeline |
    showCreatePipeline |
    describePipeline |
    showMaterializedViewStatus |
    showMaterializedViews |
    showMaterializedView |
    showCreateMaterializedView |
    describeMaterializedView |
    describeTable |
    showWatchers |
    showWatcherStatus |
    showEnrichPolicy |
    showEnrichPolicies |
    showLicense |
    showClusterName |
    refreshLicense
  }

  def ddlStatement: PackratParser[DdlStatement] =
    createTable |
    createPipeline |
    createOrReplaceTable |
    createOrReplacePipeline |
    alterTable |
    alterPipeline |
    dropTable |
    truncateTable |
    dropPipeline |
    createMaterializedView |
    createOrReplaceMaterializedView |
    dropMaterializedView |
    refreshMaterializedView |
    createWatcher |
    createOrReplaceWatcher |
    dropWatcher |
    createEnrichPolicy |
    createOrReplaceEnrichPolicy |
    executeEnrichPolicy |
    dropEnrichPolicy

  def onConflict: PackratParser[OnConflict] =
    (keyword("ON") ~ keyword("CONFLICT") ~> opt(conflictTarget) <~ keyword("DO")) ~ (keyword(
      "UPDATE"
    ) | keyword("NOTHING")) ^^ { case target ~ action =>
      OnConflict(target, action == "UPDATE")
    }

  def conflictTarget: PackratParser[List[String]] =
    start ~> repsep(ident, separator) <~ end

  /** INSERT INTO table [(col1, col2, ...)] VALUES (v1, v2, ...) */
  def insert: PackratParser[Insert] =
    (keyword("INSERT") ~ keyword("INTO")) ~ ident ~ opt(lparen ~> repsep(ident, comma) <~ rparen) ~
    ((keyword("VALUES") ~> rows) ^^ { vs => Right(vs) }
    | keyword("AS").? ~> searchStatement ^^ { q => Left(q) }) ~ opt(onConflict) ^^ {
      case _ ~ table ~ colsOpt ~ vals ~ conflict =>
        conflict match {
          case Some(c) => Insert(table, colsOpt.getOrElse(Nil), vals, Some(c))
          case _ =>
            vals match {
              case Left(q: SingleSearch) =>
                Insert(table, colsOpt.getOrElse(Nil), vals, q.onConflict)
              case _ => Insert(table, colsOpt.getOrElse(Nil), vals)
            }
        }
    }

  /** FILE_FORMAT [=] {PARQUET | JSON | JSON_ARRAY | DELTA_LAKE} — bare or quoted, `=` optional.
    *
    * `FILE_FORMAT = X` is the form every published example uses (documentation, help JSON — and
    * `FileFormat.sql` itself renders it), yet the grammar only accepted the bare `FILE_FORMAT X`:
    * under the pre-#213 prefix parse, `opt(fileFormat)` backtracked on the `=` and the clause —
    * plus any ON CONFLICT after it — was discarded in silence, with format auto-detection masking
    * the loss. A FILE_FORMAT followed by anything but a known format is a hard `err` for the same
    * reason: backtracking here can only ever mean dropping what the user wrote.
    */
  def fileFormat: PackratParser[FileFormat] =
    (keyword("FILE_FORMAT") ~ opt("=")) ~> (
      (keyword("PARQUET") ^^^ Parquet) |
      (keyword("JSON_ARRAY") ^^^ JsonArray) |
      (keyword("JSON") ^^^ Json) |
      (keyword("DELTA_LAKE") ^^^ Delta) |
      // Quoted format names (the form dml_statements.md documents) land here; so does anything
      // unrecognised, which must err rather than backtrack into a silent drop.
      ((literal ^^ (_.value) | ident) >> { name =>
        name.toUpperCase(java.util.Locale.ROOT) match {
          case "PARQUET"    => success(Parquet)
          case "JSON_ARRAY" => success(JsonArray)
          case "JSON"       => success(Json)
          case "DELTA_LAKE" => success(Delta)
          case other =>
            err(
              s"Unsupported FILE_FORMAT '$other': expected PARQUET, JSON, JSON_ARRAY or DELTA_LAKE"
            )
        }
      })
    )

  /** COPY INTO table FROM source */
  def copy: PackratParser[CopyInto] =
    (keyword("COPY") ~ keyword("INTO")) ~ ident ~ (keyword("FROM") ~> literal) ~ opt(
      fileFormat
    ) ~ opt(onConflict) ^^ { case _ ~ table ~ source ~ format ~ conflict =>
      CopyInto(source.value, table, fileFormat = format, onConflict = conflict)
    }

  /** UPDATE table SET col1 = v1, col2 = v2 [WHERE ...]
    *
    * UPDATE has no FROM clause. `opt(from)` is here only to catch one being written anyway —
    * `Parser.apply` runs `parse`, not `phrase`, so an unconsumed `FROM customers JOIN x ON …` used
    * to be discarded in silence and the UPDATE ran against the first table alone (#213). It catches
    * both operand orders: written before the WHERE, `where.?` yields None and this fires.
    */
  def update: PackratParser[Update] =
    (keyword("UPDATE") ~> ident) ~ (keyword("SET") ~> repsep(
      ident ~ "=" ~ (value | scriptValue),
      separator
    )) ~ where.? ~ opt(from) >> { case table ~ assigns ~ w ~ extraFrom =>
      extraFrom match {
        case Some(f) =>
          err(
            s"UPDATE does not support a FROM clause (${f.sql.trim}): " +
            "UPDATE targets exactly one table, named right after the UPDATE keyword. " +
            "Filter with WHERE, or pre-join the sources with a MATERIALIZED VIEW."
          )
        case None =>
          val values = ListMap(assigns.map { case col ~ _ ~ v => col -> v }: _*)
          // UPDATE keeps only the bare table name, so its WHERE needs the same qualifier
          // resolution DELETE and watcher inputs get — `WHERE orders.id = 1` must filter on `id`.
          success(Update(table, values, resolveWhere(From(Seq(Table(table))), w)))
      }
    }

  /** DELETE FROM table [WHERE ...]
    *
    * Parses the full FROM shape — alias and joins included — rather than a bare `ident`, then
    * rejects what DELETE cannot express. `Parser.apply` runs `parse`, not `phrase`, so everything
    * past the first table name used to be discarded in silence: `DELETE FROM a, b WHERE …` became
    * `DELETE FROM a` with **no** WHERE, which the client turns into `match_all` — wiping the whole
    * index instead of the matching rows (#213).
    */
  def delete: PackratParser[Delete] =
    (keyword("DELETE") ~ keyword("FROM")) ~> rep1sep(table, separator) ~ where.? >> {
      case tables ~ w =>
        tables.flatMap(_.joins) match {
          case Nil if tables.size > 1 =>
            err(
              s"DELETE targets a single table, got ${tables.map(_.name).mkString(", ")}: " +
              "issue one DELETE per table."
            )
          case Nil =>
            resolveWhere(From(tables), w) match {
              // Fail closed. A DELETE with no WHERE is `match_all`, so a WHERE lost in resolution
              // would empty the index — the very outcome #213 is about. `update()` reaches the
              // criteria through `map` and cannot drop it, which is exactly why this must stay
              // loud rather than becoming a comment claiming it cannot happen.
              case None if w.isDefined =>
                err(
                  s"Could not resolve the WHERE clause of DELETE FROM ${tables.head.name}: " +
                  "refusing to run it as an unfiltered delete."
                )
              case resolved => success(Delete(tables.head, resolved))
            }
          case joins =>
            err(
              s"JOIN is not supported in DELETE (${joins.map(_.sql.trim).mkString(" ")}): " +
              "Elasticsearch deletes by query over a single index. Select the ids to remove with " +
              "a JOIN query first, then DELETE on that key."
            )
        }
    }

  def dmlStatement: PackratParser[DmlStatement] = insert | update | delete | copy

  def statement: PackratParser[Statement] = ddlStatement | dqlStatement | dmlStatement

  /** Strip `--` comments and collapse newlines OUTSIDE string literals only. The previous
    * line-based normalizer (`split("\n").map(_.split("--")(0))`) was blind to quotes: it cut `WHERE
    * code = 'AB--12'` at the `--`, severing the literal and failing a valid statement. Literal
    * interiors — including their backslash escapes — are copied verbatim.
    */
  private def normalize(query: String): String = {
    val out = new StringBuilder(query.length)
    var quote: Char = 0
    var i = 0
    while (i < query.length) {
      val c = query.charAt(i)
      if (quote != 0) {
        out.append(c)
        if (c == '\\' && i + 1 < query.length) {
          out.append(query.charAt(i + 1))
          i += 1
        } else if (c == quote) {
          quote = 0
        }
        i += 1
      } else if (c == '-' && i + 1 < query.length && query.charAt(i + 1) == '-') {
        // comment runs to end of line; the newline itself is handled by the next iteration
        while (i < query.length && query.charAt(i) != '\n') i += 1
      } else {
        c match {
          case '\'' | '"' =>
            quote = c
            out.append(c)
          case '\n' | '\r' => out.append(' ')
          case _           => out.append(c)
        }
        i += 1
      }
    }
    out.toString
  }

  def apply(
    query: String
  ): Either[ParserError, Statement] = {
    val normalizedQuery =
      normalize(query)
        // Trailing statement terminators are idiomatic SQL and every caller that splits on `;`
        // already drops them; keep tolerating them now that anything else left over is an error.
        // A run rather than one, because tools that append `;` to SQL a user already terminated
        // produce `;;`. Anchored, so a `;` inside a literal is out of reach — a literal always
        // ends in its closing quote.
        .replaceFirst("(?:;\\s*)+$", "")
        .trim
    val reader = new PackratReader(new CharSequenceReader(normalizedQuery))
    // `phrase`, not a bare `parse` (#213): a bare `parse` succeeds on the longest matching PREFIX
    // and silently discards the rest, so the statement that ran was not the statement written.
    // Measured before/after on this exact grammar:
    //
    //   DELETE FROM orders WHEREE id = 1     ran as DELETE FROM orders  -> emptied the index
    //   DELETE FROM orders LIMIT 10          ran as DELETE FROM orders  -> emptied the index
    //   UPDATE orders SET a = 1 LIMIT 10     updated every document
    //   SELECT a FROM x UNION SELECT b ...   ran as SELECT a FROM x     (bare UNION is not the
    //                                        UNION ALL token, so the whole second leg vanished)
    //   SELECT * FROM t WHERE a IS  NOT  NULL  ran as SELECT * FROM t   (two spaces broke the
    //                                        token match and the WHERE was dropped — fixed in
    //                                        TokenRegex with \s+, and now loud here if it recurs)
    //
    // All of those are hard errors now. The per-statement guards added for #213 cover only the
    // shapes someone enumerated; requiring the whole input to be consumed covers the rest. The
    // one shape this cannot catch: `DELETE FROM orders customers` stays a valid single-table
    // DELETE, because an alias without AS is standard SQL — pinned as such in ParserSpec.
    parse(phrase(statement), reader) match {
      case NoSuccess(msg, _) =>
        Console.err.println(msg)
        Left(ParserError(msg))
      case Success(result, _) =>
        result.validate() match {
          case Left(error) => Left(ParserError(error))
          case _           => Right(result)
        }
    }
  }

}

trait CompilationError

case class ParserError(msg: String) extends CompilationError

trait Parser
    extends RegexParsers
    with PackratParsers
    with AggregateParser
    with ArithmeticParser
    with CondParser
    with ConvertParser
    with GeoParser
    with MathParser
    with StringParser
    with TemporalParser
    with TypeParser
    with HttpParser { _: WhereParser with OrderByParser with LimitParser =>

  protected def keyword(word: String): Parser[String] = s"(?i)$word\\b".r ^^ (_ => word)

  def ident: Parser[String] = """[a-zA-Z_][a-zA-Z0-9_.]*""".r

  val lparen: Parser[String] = "("
  val rparen: Parser[String] = ")"
  val comma: Parser[String] = ","
  val lbracket: Parser[String] = "["
  val rbracket: Parser[String] = "]"
  val startStruct: Parser[String] = "{"
  val endStruct: Parser[String] = "}"

  def objectValue: PackratParser[ObjectValue] =
    lparen ~> repsep(option, comma) <~ rparen ^^ { opts =>
      ObjectValue(ListMap(opts: _*))
    }

  def objectValues: PackratParser[ObjectValues] =
    lbracket ~> rep1sep(objectValue, comma) <~ rbracket ^^ { ovs =>
      ObjectValues(ovs)
    }

  // `ingest_id | ingest_timestamp` for the same reason as `alterColumnDefault`: the mapping
  // metadata a column's DEFAULT is mirrored into (`_meta.columns.<c>.default_value`) is written
  // through this production.
  def option: PackratParser[(String, Value[_])] =
    (ident | literal) ~ "=" ~ (objectValues | objectValue | value | ingest_id | ingest_timestamp) ^^ {
      case key ~ _ ~ value =>
        key match {
          case lit: StringValue => (lit.value, value)
          case id: String       => (id, value)
        }
    }

  def options: PackratParser[ListMap[String, Value[_]]] =
    keyword("OPTIONS") ~ lparen ~ repsep(option, comma) ~ rparen ^^ { case _ ~ _ ~ opts ~ _ =>
      ListMap(opts: _*)
    }

  def array_of_struct: PackratParser[ObjectValues] =
    lbracket ~> repsep(struct, comma) <~ rbracket ^^ { ovs =>
      ObjectValues(ovs)
    }

  def struct_entry: PackratParser[(String, Value[_])] =
    ident ~ "=" ~ (array_of_struct | struct | value) ^^ { case key ~ _ ~ v =>
      key -> v
    }

  def struct: PackratParser[ObjectValue] =
    startStruct ~> repsep(struct_entry, comma) <~ endStruct ^^ { entries =>
      ObjectValue(ListMap(entries: _*))
    }

  def start: PackratParser[Delimiter] = "(" ^^ (_ => StartPredicate)

  def end: PackratParser[Delimiter] = ")" ^^ (_ => EndPredicate)

  def separator: PackratParser[Delimiter] = "," ^^ (_ => Separator)

  def valueExpr: PackratParser[PainlessScript] = {
    // the order is important here
    identifierWithWindowFunction |
    identifierWithTransformation | // transformations applied to an identifier
    identifierWithIntervalFunction |
    identifierWithFunction | // fonctions applied to an identifier
    quotedIdentifier | // double-quoted identifiers (ANSI SQL-92 delimited identifiers)
    identifierWithValue |
    identifier
  }

  implicit def functionAsIdentifier(mf: Function): Identifier = mf match {
    case id: Identifier => id
    case fid: FunctionWithIdentifier =>
      fid.identifier //.withFunctions(fid +: fid.identifier.functions)
    case _ => Identifier(mf)
  }

  def sql_function: PackratParser[Function] =
    aggregate_function | time_function | conditional_function

  private val reservedKeywords = Seq(
    "select",
    "insert",
    "update",
    "copy",
    "delete",
    "create",
    "alter",
    "drop",
    "truncate",
    "column",
    "from",
    "join",
    "inner",
    "left",
    "right",
    "full",
    "cross",
    "outer",
    "on",
    "where",
    "group",
    "having",
    "order",
    "limit",
    "offset",
    "as",
    "by",
    "except",
    "unnest",
    "current_date",
    "current_time",
    "current_datetime",
    "current_timestamp",
    "now",
    "today",
    "coalesce",
    "nullif",
    "isnull",
    "isnotnull",
    "greatest",
    "least",
    "row_number",
    "rank",
    "dense_rank",
    "date_add",
    "date_sub",
    "parse_date",
    "parse_datetime",
    "format_date",
    "format_datetime",
    "date_trunc",
    "extract",
    "date_diff",
    "datetime_add",
    "datetime_sub",
    "interval",
//    "year",
//    "month",
//    "day",
//    "hour",
//    "minute",
//    "second",
//    "quarter",
//    "char",
//    "string",
//    "byte",
//    "tinyint",
//    "short",
//    "smallint",
//    "int",
//    "integer",
//    "long",
//    "bigint",
//    "real",
//    "float",
//    "double",
    "pi",
//    "boolean",
    "distance",
//    "time",
//    "date",
//    "datetime",
//    "timestamp",
    "and",
    "or",
    "not",
    "like",
    "in",
    "between",
    "distinct",
    "cast",
    "count",
    "min",
    "max",
    "avg",
    "sum",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "variance",
    "var_pop",
    "var_samp",
    "percentile_cont",
    "percentile_disc",
    "case",
    "when",
    "then",
    "else",
    "end",
    "union",
    "all",
    "exists",
    "true",
    "false",
//    "nested",
//    "parent",
//    "child",
    "match",
    "against",
    "abs",
    "ceil",
    "floor",
    "exp",
    "log",
    "log10",
    "sqrt",
    "round",
    "pow",
    "sign",
    "sin",
    "asin",
    "cos",
    "acos",
    "tan",
    "atan",
    "atan2",
    "concat",
    "substr",
    "substring",
    "to",
    "length",
    "lower",
    "upper",
    "trim",
    "first",
    "last",
    "array_agg",
    "first_value",
    "last_value",
    "ltrim",
    "rtrim",
    "replace",
    "on",
    "conflict",
    "do",
    "show",
    "describe",
    "every",
    "at",
    "never",
    "always",
    "foreach",
    "within"
//    "protocol",
//    "http",
//    "https",
//    "host",
//    "port"
  )

  private val identifierRegexStr =
    s"""(?i)(?!(?:${reservedKeywords.mkString("|")})\\b)[\\*a-zA-Z_\\-][a-zA-Z0-9_\\-.\\[\\]\\*]*"""

  val identifierRegex: Regex = identifierRegexStr.r // scala.util.matching.Regex

  def quotedIdentifier: PackratParser[Identifier] =
    ("\"" ~> """([^"\\]|\\.)*""".r <~ "\"") ^^ { str =>
      GenericIdentifier(
        str.replace("\\\"", "\"").replace("\\\\", "\\"),
        None,
        distinct = false
      )
    }

  def identifier: PackratParser[Identifier] =
    (Distinct.regex.? ~ identifierRegex ^^ { case d ~ i =>
      GenericIdentifier(
        i,
        None,
        d.isDefined
      )
    }) >> cast

  def identifierWithTransformation: PackratParser[Identifier] =
    (mathematicalFunctionWithIdentifier |
    conversionFunctionWithIdentifier |
    conditionalFunctionWithIdentifier |
    timeFunctionWithIdentifier |
    stringFunctionWithIdentifier |
    geoFunctionWithIdentifier) >> cast

  def identifierWithFunction: PackratParser[Identifier] =
    (rep1sep(
      sql_function,
      start
    ) ~ start.? ~ (identifierWithTransformation | identifierWithIntervalFunction | identifier).? ~ rep1(
      end
    ) ^^ { case f ~ _ ~ i ~ _ =>
      i match {
        case None =>
          f.lastOption match {
            case Some(fi: FunctionWithIdentifier) =>
              fi.identifier.withFunctions(f ++ fi.identifier.functions)
            case _ => Identifier(f)
          }
        case Some(id) => id.withFunctions(f ++ id.functions)
      }
    }) >> cast

  private val regexAlias =
    s"""\\b(?i)(?!(?:${reservedKeywords.mkString("|")})\\b)[a-zA-Z0-9_.]*""".stripMargin

  def alias: PackratParser[Alias] = Alias.regex.? ~ regexAlias.r ^^ { case _ ~ b => Alias(b) }

  def quotedAlias: PackratParser[Alias] =
    Alias.regex.? ~ ("\"" ~> """([^"\\]|\\.)*""".r <~ "\"") ^^ { case _ ~ b =>
      Alias(b.replace("\\\"", "\"").replace("\\\\", "\\"))
    }

}
