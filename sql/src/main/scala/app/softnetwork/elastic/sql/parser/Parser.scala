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

import app.softnetwork.elastic.sql.PainlessContextType.Processor
import app.softnetwork.elastic.sql._
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

  def dqlStatement: PackratParser[DqlStatement] = rep1sep(single, union) ^^ {
    case x :: Nil => x
    case s        => MultiSearch(s)
  }

  def ident: Parser[String] = """[a-zA-Z_][a-zA-Z0-9_]*""".r

  private val lparen: Parser[String] = "("
  private val rparen: Parser[String] = ")"
  private val comma: Parser[String] = ","
  private val lbracket: Parser[String] = "["
  private val rbracket: Parser[String] = "]"
  private val startStruct: Parser[String] = "{"
  private val endStruct: Parser[String] = "}"

  def objectValue: PackratParser[ObjectValue] =
    lparen ~> rep1sep(option, comma) <~ rparen ^^ { opts =>
      ObjectValue(opts.toMap)
    }

  def objectValues: PackratParser[ObjectValues] =
    lbracket ~> rep1sep(objectValue, comma) <~ rbracket ^^ { ovs =>
      ObjectValues(ovs)
    }

  def option: PackratParser[(String, Value[_])] =
    ident ~ "=" ~ (objectValues | objectValue | value) ^^ { case key ~ _ ~ value =>
      (key, value)
    }

  def options: PackratParser[Map[String, Value[_]]] =
    "OPTIONS" ~ lparen ~ repsep(option, comma) ~ rparen ^^ { case _ ~ _ ~ opts ~ _ =>
      opts.toMap
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
      ObjectValue(entries.toMap)
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
        case other             => IngestProcessorType(other)
      }
    }

  def processor: PackratParser[IngestProcessor] =
    processorType ~ objectValue ^^ { case pt ~ opts =>
      IngestProcessor(pt, opts)
    }

  def createOrReplacePipeline: PackratParser[CreatePipeline] =
    ("CREATE" ~ "OR" ~ "REPLACE" ~ "PIPELINE") ~ ident ~ ("WITH" ~ "PROCESSORS") ~ start ~ repsep(
      processor,
      separator
    ) ~ end ^^ { case _ ~ name ~ _ ~ _ ~ proc ~ _ =>
      CreatePipeline(name, IngestPipelineType.Custom, orReplace = true, processors = proc)
    }

  def createPipeline: PackratParser[CreatePipeline] =
    ("CREATE" ~ "PIPELINE") ~ ifNotExists ~ ident ~ ("WITH" ~ "PROCESSORS" ~ start) ~ repsep(
      processor,
      separator
    ) <~ end ^^ { case _ ~ ine ~ name ~ _ ~ proc =>
      CreatePipeline(name, IngestPipelineType.Custom, ifNotExists = ine, processors = proc)
    }

  def dropPipeline: PackratParser[DropPipeline] =
    ("DROP" ~ "PIPELINE") ~ ifExists ~ ident ^^ { case _ ~ ie ~ name =>
      DropPipeline(name, ifExists = ie)
    }

  def showPipeline: PackratParser[ShowPipeline] =
    ("SHOW" ~ "PIPELINE") ~ ident ^^ { case _ ~ pipeline =>
      ShowPipeline(pipeline)
    }

  def describePipeline: PackratParser[DescribePipeline] =
    ("DESCRIBE" ~ "PIPELINE") ~ ident ^^ { case _ ~ pipeline =>
      DescribePipeline(pipeline)
    }

  def addProcessor: PackratParser[AddPipelineProcessor] =
    ("ADD" ~ "PROCESSOR") ~ processor ^^ { case _ ~ proc =>
      AddPipelineProcessor(proc)
    }

  def dropProcessor: PackratParser[DropPipelineProcessor] =
    ("DROP" ~ "PROCESSOR") ~ processorType ~ start ~ ident ~ end ^^ { case _ ~ pt ~ _ ~ name ~ _ =>
      DropPipelineProcessor(pt, name)
    }

  def alterPipelineStatement: PackratParser[AlterPipelineStatement] =
    addProcessor | dropProcessor

  def alterPipeline: PackratParser[AlterPipeline] =
    ("ALTER" ~ "PIPELINE") ~ ifExists ~ ident ~ start.? ~ repsep(
      alterPipelineStatement,
      separator
    ) ~ end.? ^^ { case _ ~ ie ~ pipeline ~ s ~ stmts ~ e =>
      if (s.isDefined && e.isEmpty) {
        throw new Exception("Mismatched closing parentheses in ALTER PIPELINE statement")
      } else if (s.isEmpty && e.isDefined) {
        throw new Exception("Mismatched opening parentheses in ALTER PIPELINE statement")
      } else if (s.isEmpty && e.isEmpty && stmts.size > 1) {
        throw new Exception("Multiple ALTER PIPELINE statements require parentheses")
      } else
        AlterPipeline(pipeline, ie, stmts)
    }

  def multiFields: PackratParser[List[Column]] =
    "FIELDS" ~ start ~> repsep(column, separator) <~ end ^^ (cols => cols) | success(Nil)

  def ifExists: PackratParser[Boolean] =
    opt("IF" ~ "EXISTS") ^^ {
      case Some(_) => true
      case None    => false
    }

  def ifNotExists: PackratParser[Boolean] =
    opt("IF" ~ "NOT" ~ "EXISTS") ^^ {
      case Some(_) => true
      case None    => false
    }

  def notNull: PackratParser[Boolean] =
    opt("NOT" ~ "NULL") ^^ {
      case Some(_) => true
      case None    => false
    }

  def ingest_id: PackratParser[Value[_]] = "_id" ^^ (_ => IdValue)

  def ingest_timestamp: PackratParser[Value[_]] = "_ingest.timestamp" ^^ (_ => IngestTimestampValue)

  def defaultVal: PackratParser[Option[Value[_]]] =
    opt("DEFAULT" ~ (value | ingest_id | ingest_timestamp)) ^^ {
      case Some(_ ~ v) => Some(v)
      case None        => None
    }

  def comment: PackratParser[Option[String]] =
    opt("COMMENT" ~ literal) ^^ {
      case Some(_ ~ v) => Some(v.value)
      case None        => None
    }

  def script: PackratParser[PainlessScript] =
    ("SCRIPT" ~ "AS") ~ start ~ (identifierWithArithmeticExpression |
    identifierWithTransformation |
    identifierWithIntervalFunction |
    identifierWithFunction) ~ end ^^ { case _ ~ _ ~ s ~ _ => s }

  def column: PackratParser[Column] =
    ident ~ extension_type ~ (script | multiFields) ~ defaultVal ~ notNull ~ comment ~ (options | success(
      Map.empty[String, Value[_]]
    )) ^^ { case name ~ dt ~ mfs ~ dv ~ nn ~ ct ~ opts =>
      mfs match {
        case script: PainlessScript =>
          val ctx = PainlessContext(Processor)
          val scr = script.painless(Some(ctx))
          val temp = s"$ctx$scr"
          val ret =
            temp.split(";") match {
              case Array(single) if single.trim.startsWith("return ") =>
                val stripReturn = single.trim.stripPrefix("return ").trim
                s"ctx.$name = $stripReturn"
              case multiple =>
                val last = multiple.last.trim
                val temp = multiple.dropRight(1) :+ s" ctx.$name = $last"
                temp.mkString(";")
            }
          Column(
            name,
            dt,
            Some(
              ScriptProcessor(
                script = script.sql,
                column = name,
                dataType = dt,
                source = ret
              )
            ),
            Nil,
            dv,
            nn,
            ct,
            opts
          )
        case cols: List[Column] =>
          Column(name, dt, None, cols, dv, nn, ct, opts)
      }
    }

  def columns: PackratParser[List[Column]] =
    start ~ repsep(column, separator) ~ end ^^ { case _ ~ cols ~ _ => cols }

  def primaryKey: PackratParser[List[String]] =
    separator ~ "PRIMARY" ~ "KEY" ~ start ~ repsep(ident, separator) ~ end ^^ {
      case _ ~ _ ~ _ ~ _ ~ keys ~ _ =>
        keys
    } | success(Nil)

  def granularity: PackratParser[TimeUnit] = start ~
    (("YEAR" ^^^ TimeUnit.YEARS) |
    ("MONTH" ^^^ TimeUnit.MONTHS) |
    ("DAY" ^^^ TimeUnit.DAYS) |
    ("HOUR" ^^^ TimeUnit.HOURS) |
    ("MINUTE" ^^^ TimeUnit.MINUTES) |
    ("SECOND" ^^^ TimeUnit.SECONDS)) ~ end ^^ { case _ ~ gf ~ _ => gf }

  def partitionBy: PackratParser[Option[PartitionDate]] =
    opt("PARTITION" ~ "BY" ~ ident ~ opt(granularity)) ^^ {
      case Some(_ ~ _ ~ pb ~ gf) => Some(PartitionDate(pb, gf.getOrElse(TimeUnit.DAYS)))
      case None                  => None
    }

  def columnsWithPartitionBy
    : PackratParser[(List[Column], List[String], Option[PartitionDate], Map[String, Any])] =
    start ~ repsep(
      column,
      separator
    ) ~ primaryKey ~ end ~ partitionBy ~ ((separator.? ~> options) | success(
      Map.empty[String, Value[_]]
    )) ^^ { case _ ~ cols ~ pk ~ _ ~ pb ~ opts =>
      (cols, pk, pb, opts)
    }

  def createOrReplaceTable: PackratParser[CreateTable] =
    ("CREATE" ~ "OR" ~ "REPLACE" ~ "TABLE") ~ ident ~ (columnsWithPartitionBy | ("AS" ~> dqlStatement)) ^^ {
      case _ ~ name ~ lr =>
        lr match {
          case (
                cols: List[Column],
                pk: List[String],
                p: Option[PartitionDate],
                opts: Map[String, Value[_]]
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
          case sel: DqlStatement =>
            CreateTable(name, Left(sel), ifNotExists = false, orReplace = true)
        }
    }

  def createTable: PackratParser[CreateTable] =
    ("CREATE" ~ "TABLE") ~ ifNotExists ~ ident ~ (columnsWithPartitionBy | ("AS" ~> dqlStatement)) ^^ {
      case _ ~ ine ~ name ~ lr =>
        lr match {
          case (
                cols: List[Column],
                pk: List[String],
                p: Option[PartitionDate],
                opts: Map[String, Value[_]]
              ) =>
            CreateTable(name, Right(cols), ine, primaryKey = pk, partitionBy = p, options = opts)
          case sel: DqlStatement => CreateTable(name, Left(sel), ine)
        }
    }

  def showTable: PackratParser[ShowTable] =
    ("SHOW" ~ "TABLE") ~ ident ^^ { case _ ~ table =>
      ShowTable(table)
    }

  def describeTable: PackratParser[DescribeTable] =
    ("DESCRIBE" ~ "TABLE") ~ ident ^^ { case _ ~ table =>
      DescribeTable(table)
    }

  def dropTable: PackratParser[DropTable] =
    ("DROP" ~ "TABLE") ~ ifExists ~ ident ^^ { case _ ~ ie ~ name =>
      DropTable(name, ifExists = ie)
    }

  def truncateTable: PackratParser[TruncateTable] =
    ("TRUNCATE" ~ "TABLE") ~ ident ^^ { case _ ~ name =>
      TruncateTable(name)
    }

  def addColumn: PackratParser[AddColumn] =
    ("ADD" ~ "COLUMN") ~ ifNotExists ~ column ^^ { case _ ~ ine ~ col =>
      AddColumn(col, ifNotExists = ine)
    }

  def dropColumn: PackratParser[DropColumn] =
    ("DROP" ~ "COLUMN") ~ ifExists ~ ident ^^ { case _ ~ ie ~ name =>
      DropColumn(name, ifExists = ie)
    }

  def renameColumn: PackratParser[RenameColumn] =
    ("RENAME" ~ "COLUMN") ~ ident ~ ("TO" ~> ident) ^^ { case _ ~ oldName ~ newName =>
      RenameColumn(oldName, newName)
    }

  def alterColumnIfExists: PackratParser[Boolean] =
    ("ALTER" ~ "COLUMN") ~ ifExists ^^ { case _ ~ ie =>
      ie
    }

  def alterColumnOptions: PackratParser[AlterColumnOptions] =
    alterColumnIfExists ~ ident ~ "SET" ~ options ^^ { case ie ~ col ~ _ ~ opts =>
      AlterColumnOptions(col, opts, ifExists = ie)
    }

  def alterColumnOption: PackratParser[AlterColumnOption] =
    alterColumnIfExists ~ ident ~ (("SET" | "ADD") ~ "OPTION") ~ start ~ option ~ end ^^ {
      case ie ~ col ~ _ ~ _ ~ opt ~ _ =>
        AlterColumnOption(col, opt._1, opt._2, ifExists = ie)
    }

  def dropColumnOption: PackratParser[DropColumnOption] =
    alterColumnIfExists ~ ident ~ ("DROP" ~ "OPTION") ~ ident ^^ { case ie ~ col ~ _ ~ optionName =>
      DropColumnOption(col, optionName, ifExists = ie)
    }

  def alterColumnFields: PackratParser[AlterColumnFields] =
    alterColumnIfExists ~ ident ~ "SET" ~ multiFields ^^ { case ie ~ col ~ _ ~ fields =>
      AlterColumnFields(col, fields, ifExists = ie)
    }

  def alterColumnField: PackratParser[AlterColumnField] =
    alterColumnIfExists ~ ident ~ (("SET" | "ADD") ~ "FIELD") ~ column ^^ {
      case ie ~ col ~ _ ~ field =>
        AlterColumnField(col, field, ifExists = ie)
    }

  def dropColumnField: PackratParser[DropColumnField] =
    alterColumnIfExists ~ ident ~ ("DROP" ~ "FIELD") ~ ident ^^ { case ie ~ col ~ _ ~ fieldName =>
      DropColumnField(col, fieldName, ifExists = ie)
    }

  def alterColumnType: PackratParser[AlterColumnType] =
    alterColumnIfExists ~ ident ~ ("SET" ~ "DATA" ~ "TYPE") ~ extension_type ^^ {
      case ie ~ name ~ _ ~ newType => AlterColumnType(name, newType, ifExists = ie)
    }

  def alterColumnScript: PackratParser[AlterColumnScript] =
    alterColumnIfExists ~ ident ~ "SET" ~ script ^^ { case ie ~ name ~ _ ~ ns =>
      val ctx = PainlessContext(Processor)
      val scr = ns.painless(Some(ctx))
      val temp = s"$ctx$scr"
      val ret =
        temp.split(";") match {
          case Array(single) if single.trim.startsWith("return ") =>
            val stripReturn = single.trim.stripPrefix("return ").trim
            s"ctx.$name = $stripReturn"
          case multiple =>
            val last = multiple.last.trim
            val temp = multiple.dropRight(1) :+ s" ctx.$name = $last"
            temp.mkString(";")
        }
      AlterColumnScript(
        name,
        ScriptProcessor(
          script = ns.sql,
          column = name,
          dataType = ns.out,
          source = ret
        ),
        ifExists = ie
      )
    }

  def dropColumnScript: PackratParser[DropColumnScript] =
    alterColumnIfExists ~ ident ~ ("DROP" ~ "SCRIPT") ^^ { case ie ~ name ~ _ =>
      DropColumnScript(name, ifExists = ie)
    }

  def alterColumnDefault: PackratParser[AlterColumnDefault] =
    alterColumnIfExists ~ ident ~ ("SET" ~ "DEFAULT") ~ value ^^ { case ie ~ name ~ _ ~ dv =>
      AlterColumnDefault(name, dv, ifExists = ie)
    }

  def dropColumnDefault: PackratParser[DropColumnDefault] =
    alterColumnIfExists ~ ident ~ ("DROP" ~ "DEFAULT") ^^ { case ie ~ name ~ _ =>
      DropColumnDefault(name, ifExists = ie)
    }

  def alterColumnNotNull: PackratParser[AlterColumnNotNull] =
    alterColumnIfExists ~ ident ~ ("SET" ~ "NOT" ~ "NULL") ^^ { case ie ~ name ~ _ =>
      AlterColumnNotNull(name, ifExists = ie)
    }

  def dropColumnNotNull: PackratParser[DropColumnNotNull] =
    alterColumnIfExists ~ ident ~ ("DROP" ~ "NOT" ~ "NULL") ^^ { case ie ~ name ~ _ =>
      DropColumnNotNull(name, ifExists = ie)
    }

  def alterColumnComment: PackratParser[AlterColumnComment] =
    alterColumnIfExists ~ ident ~ ("SET" ~ "COMMENT") ~ literal ^^ { case ie ~ name ~ _ ~ c =>
      AlterColumnComment(name, c.value, ifExists = ie)
    }

  def dropColumnComment: PackratParser[DropColumnComment] =
    alterColumnIfExists ~ ident ~ ("DROP" ~ "COMMENT") ^^ { case ie ~ name ~ _ =>
      DropColumnComment(name, ifExists = ie)
    }

  def alterTableMapping: PackratParser[AlterTableMapping] =
    (("SET" | "ADD") ~ "MAPPING") ~ start ~> option <~ end ^^ { opt =>
      AlterTableMapping(opt._1, opt._2)
    }

  def dropTableMapping: PackratParser[DropTableMapping] =
    ("DROP" ~ "MAPPING") ~> ident ^^ { m => DropTableMapping(m) }

  def alterTableSetting: PackratParser[AlterTableSetting] =
    (("SET" | "ADD") ~ "SETTING") ~ start ~> option <~ end ^^ { opt =>
      AlterTableSetting(opt._1, opt._2)
    }

  def dropTableSetting: PackratParser[DropTableSetting] =
    ("DROP" ~ "SETTING") ~> ident ^^ { m => DropTableSetting(m) }

  def alterTableAlias: PackratParser[AlterTableAlias] =
    (("SET" | "ADD") ~ "ALIAS") ~ start ~> option <~ end ^^ { opt =>
      AlterTableAlias(opt._1, opt._2)
    }

  def dropTableAlias: PackratParser[DropTableAlias] =
    ("DROP" ~ "ALIAS") ~> ident ^^ { m => DropTableAlias(m) }

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
    ("ALTER" ~ "TABLE") ~ ifExists ~ ident ~ start.? ~ repsep(
      alterTableStatement,
      separator
    ) ~ end.? ^^ { case _ ~ ie ~ table ~ s ~ stmts ~ e =>
      if (s.isDefined && e.isEmpty) {
        throw new Exception("Mismatched closing parentheses in ALTER TABLE statement")
      } else if (s.isEmpty && e.isDefined) {
        throw new Exception("Mismatched opening parentheses in ALTER TABLE statement")
      } else if (s.isEmpty && e.isEmpty && stmts.size > 1) {
        throw new Exception("Multiple ALTER TABLE statements require parentheses")
      } else
        AlterTable(table, ie, stmts)
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
    showTable |
    describeTable |
    dropPipeline |
    showPipeline |
    describePipeline

  def onConflict: PackratParser[OnConflict] =
    ("ON" ~ "CONFLICT" ~> opt(conflictTarget) <~ "DO") ~ ("UPDATE" | "NOTHING") ^^ {
      case target ~ action =>
        OnConflict(target, action == "UPDATE")
    }

  def conflictTarget: PackratParser[List[String]] =
    start ~> repsep(ident, separator) <~ end

  /** INSERT INTO table [(col1, col2, ...)] VALUES (v1, v2, ...) */
  def insert: PackratParser[Insert] =
    ("INSERT" ~ "INTO") ~ ident ~ opt(lparen ~> repsep(ident, comma) <~ rparen) ~
    (("VALUES" ~> rows) ^^ { vs => Right(vs) }
    | "AS".? ~> dqlStatement ^^ { q => Left(q) }) ~ opt(onConflict) ^^ {
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

  def fileFormat: PackratParser[FileFormat] =
    ("FILE_FORMAT" ~> (
      ("PARQUET" ^^^ Parquet) |
      ("JSON" ^^^ Json) |
      ("JSON_ARRAY" ^^^ JsonArray) |
      ("DELTA_LAKE" ^^^ Delta)
    )) ^^ { ff => ff }

  /** COPY INTO table FROM source */
  def copy: PackratParser[CopyInto] =
    ("COPY" ~ "INTO") ~ ident ~ ("FROM" ~> literal) ~ opt(fileFormat) ~ opt(onConflict) ^^ {
      case _ ~ table ~ source ~ format ~ conflict =>
        CopyInto(source.value, table, fileFormat = format, onConflict = conflict)
    }

  /** UPDATE table SET col1 = v1, col2 = v2 [WHERE ...] */
  def update: PackratParser[Update] =
    ("UPDATE" ~> ident) ~ ("SET" ~> repsep(ident ~ "=" ~ value, separator)) ~ where.? ^^ {
      case table ~ assigns ~ w =>
        val values = assigns.map { case col ~ _ ~ v => col -> v }.toMap
        Update(table, values, w)
    }

  /** DELETE FROM table [WHERE ...] */
  def delete: PackratParser[Delete] =
    ("DELETE" ~ "FROM") ~> ident ~ where.? ^^ { case table ~ w =>
      Delete(Table(table), w)
    }

  def dmlStatement: PackratParser[DmlStatement] = insert | update | delete | copy

  def statement: PackratParser[Statement] = ddlStatement | dqlStatement | dmlStatement

  def apply(
    query: String
  ): Either[ParserError, Statement] = {
    val normalizedQuery =
      query
        .split("\n")
        .map(_.split("--")(0).trim)
        .filterNot(w => w.isEmpty || w.startsWith("--"))
        .mkString(" ")
    val reader = new PackratReader(new CharSequenceReader(normalizedQuery))
    parse(statement, reader) match {
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
    with TypeParser { _: WhereParser with OrderByParser with LimitParser =>

  def start: PackratParser[Delimiter] = "(" ^^ (_ => StartPredicate)

  def end: PackratParser[Delimiter] = ")" ^^ (_ => EndPredicate)

  def separator: PackratParser[Delimiter] = "," ^^ (_ => Separator)

  def valueExpr: PackratParser[PainlessScript] = {
    // the order is important here
    identifierWithWindowFunction |
    identifierWithTransformation | // transformations applied to an identifier
    identifierWithIntervalFunction |
    identifierWithFunction | // fonctions applied to an identifier
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
    "table",
    "pipeline",
    "column",
    "from",
    "join",
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
    "describe"
  )

  private val identifierRegexStr =
    s"""(?i)(?!(?:${reservedKeywords.mkString("|")})\\b)[\\*a-zA-Z_\\-][a-zA-Z0-9_\\-.\\[\\]\\*]*"""

  val identifierRegex: Regex = identifierRegexStr.r // scala.util.matching.Regex

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
    s"""\\b(?i)(?!(?:${reservedKeywords.mkString("|")})\\b)[a-zA-Z0-9_]*""".stripMargin

  def alias: PackratParser[Alias] = Alias.regex.? ~ regexAlias.r ^^ { case _ ~ b => Alias(b) }

}
