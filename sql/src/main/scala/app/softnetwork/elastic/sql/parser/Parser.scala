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
import app.softnetwork.elastic.sql.schema.Column

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
    phrase(select ~ from ~ where.? ~ groupBy.? ~ having.? ~ orderBy.? ~ limit.?) ^^ {
      case s ~ f ~ w ~ g ~ h ~ o ~ l =>
        SingleSearch(s, f, w, g, h, o, l).update()
    }
  }

  def union: PackratParser[UNION.type] = UNION.regex ^^ (_ => UNION)

  def dqlStatement: PackratParser[DqlStatement] = rep1sep(single, union) ^^ {
    case x :: Nil => x
    case s        => MultiSearch(s)
  }

  def ident: Parser[String] = """[a-zA-Z_][a-zA-Z0-9_]*""".r

  def option: PackratParser[(String, Value[_])] =
    ident ~ "=" ~ value ^^ { case key ~ _ ~ value =>
      (key, value)
    }

  def options: PackratParser[Map[String, Value[_]]] =
    "OPTIONS" ~ "(" ~ repsep(option, separator) ~ ")" ^^ { case _ ~ _ ~ opts ~ _ =>
      opts.toMap
    }

  def multiFields: PackratParser[List[Column]] =
    "FIELDS" ~ "(" ~ repsep(column, separator) ~ ")" ^^ { case _ ~ _ ~ cols ~ _ =>
      cols
    } | success(Nil)

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

  def defaultVal: PackratParser[Option[Value[_]]] =
    opt("DEFAULT" ~ value) ^^ {
      case Some(_ ~ v) => Some(v)
      case None        => None
    }

  def column: PackratParser[Column] =
    ident ~ sql_type ~ (options | success(
      Map.empty[String, Value[_]]
    )) ~ notNull ~ defaultVal ~ multiFields ^^ { case name ~ dt ~ opts ~ nn ~ dv ~ mfs =>
      Column(name, dt, opts, mfs, nn, dv)
    }

  def columns: PackratParser[List[Column]] =
    "(" ~ repsep(column, separator) ~ ")" ^^ { case _ ~ cols ~ _ => cols }

  def createOrReplaceTable: PackratParser[CreateTable] =
    ("CREATE" ~ "OR" ~ "REPLACE" ~ "TABLE") ~ ident ~ (columns | ("AS" ~> dqlStatement)) ^^ {
      case _ ~ name ~ lr =>
        lr match {
          case cols: List[Column] =>
            CreateTable(name, Right(cols), ifNotExists = false, orReplace = true)
          case sel: DqlStatement =>
            CreateTable(name, Left(sel), ifNotExists = false, orReplace = true)
        }
    }

  def createTable: PackratParser[CreateTable] =
    ("CREATE" ~ "TABLE") ~ ifNotExists ~ ident ~ (columns | ("AS" ~> dqlStatement)) ^^ {
      case _ ~ ine ~ name ~ lr =>
        lr match {
          case cols: List[Column] => CreateTable(name, Right(cols), ine)
          case sel: DqlStatement  => CreateTable(name, Left(sel), ine)
        }
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

  def setColumnOptions: PackratParser[AlterColumnOptions] =
    alterColumnIfExists ~ ident ~ "SET" ~ options ^^ { case ie ~ col ~ _ ~ opts =>
      AlterColumnOptions(col, opts, ifExists = ie)
    }

  def setColumnType: PackratParser[AlterColumnType] =
    alterColumnIfExists ~ ident ~ ("SET" ~ "DATA" ~ "TYPE") ~ sql_type ^^ {
      case ie ~ name ~ _ ~ newType => AlterColumnType(name, newType, ifExists = ie)
    }

  def setColumnDefault: PackratParser[AlterColumnDefault] =
    alterColumnIfExists ~ ident ~ ("SET" ~ "DEFAULT") ~ value ^^ { case ie ~ name ~ _ ~ dv =>
      AlterColumnDefault(name, dv, ifExists = ie)
    }

  def dropColumnDefault: PackratParser[DropColumnDefault] =
    alterColumnIfExists ~ ident ~ ("DROP" ~ "DEFAULT") ^^ { case ie ~ name ~ _ =>
      DropColumnDefault(name, ifExists = ie)
    }

  def setColumnNotNull: PackratParser[AlterColumnNotNull] =
    alterColumnIfExists ~ ident ~ ("SET" ~ "NOT" ~ "NULL") ^^ { case ie ~ name ~ _ =>
      AlterColumnNotNull(name, ifExists = ie)
    }

  def dropColumnNotNull: PackratParser[DropColumnNotNull] =
    alterColumnIfExists ~ ident ~ ("DROP" ~ "NOT" ~ "NULL") ^^ { case ie ~ name ~ _ =>
      DropColumnNotNull(name, ifExists = ie)
    }

  def alterTableStatement: PackratParser[AlterTableStatement] =
    addColumn |
    dropColumn |
    renameColumn |
    setColumnOptions |
    setColumnType |
    setColumnDefault |
    dropColumnDefault |
    setColumnNotNull |
    dropColumnNotNull

  def alterTable: PackratParser[AlterTable] =
    ("ALTER" ~ "TABLE") ~ ifExists ~ ident ~ alterTableStatement ^^ { case _ ~ ie ~ table ~ stmt =>
      AlterTable(table, ie, stmt)
    }

  def ddlStatement: PackratParser[DdlStatement] =
    createTable | createOrReplaceTable | alterTable | dropTable | truncateTable

  /** INSERT INTO table [(col1, col2, ...)] VALUES (v1, v2, ...) */
  def insert: PackratParser[Insert] =
    ("INSERT" ~ "INTO") ~ ident ~ opt("(" ~> repsep(ident, separator) <~ ")") ~
    (("VALUES" ~ "(" ~> repsep(value, separator) <~ ")") ^^ { vs => Right(vs) }
    | dqlStatement ^^ { q => Left(q) }) ^^ { case _ ~ table ~ colsOpt ~ vals =>
      Insert(table, colsOpt.getOrElse(Nil), vals)
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

  def dmlStatement: PackratParser[DmlStatement] = insert | update | delete

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
    "delete",
    "create",
    "alter",
    "drop",
    "truncate",
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
    "replace"
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
        case Some(id) => id.withFunctions(id.functions ++ f)
      }
    }) >> cast

  private val regexAlias =
    s"""\\b(?i)(?!(?:${reservedKeywords.mkString("|")})\\b)[a-zA-Z0-9_]*""".stripMargin

  def alias: PackratParser[Alias] = Alias.regex.? ~ regexAlias.r ^^ { case _ ~ b => Alias(b) }

}
