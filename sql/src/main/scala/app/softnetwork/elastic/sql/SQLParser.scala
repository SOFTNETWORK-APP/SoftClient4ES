package app.softnetwork.elastic.sql

import scala.util.parsing.combinator.{PackratParsers, RegexParsers}
import scala.util.parsing.input.CharSequenceReader
import TimeUnit._

/** Created by smanciot on 27/06/2018.
  *
  * SQL Parser for ElasticSearch
  */
object SQLParser
    extends SQLParser
    with SQLSelectParser
    with SQLFromParser
    with SQLWhereParser
    with SQLGroupByParser
    with SQLHavingParser
    with SQLOrderByParser
    with SQLLimitParser
    with PackratParsers {

  def request: PackratParser[SQLSearchRequest] = {
    phrase(select ~ from ~ where.? ~ groupBy.? ~ having.? ~ orderBy.? ~ limit.?) ^^ {
      case s ~ f ~ w ~ g ~ h ~ o ~ l =>
        val request = SQLSearchRequest(s, f, w, g, h, o, l).update()
        request.validate() match {
          case Left(error) => throw SQLValidationError(error)
          case _           =>
        }
        request
    }
  }

  def union: PackratParser[Union.type] = Union.regex ^^ (_ => Union)

  def requests: PackratParser[List[SQLSearchRequest]] = rep1sep(request, union) ^^ (s => s)

  def apply(
    query: String
  ): Either[SQLParserError, Either[SQLSearchRequest, SQLMultiSearchRequest]] = {
    val reader = new PackratReader(new CharSequenceReader(query))
    parse(requests, reader) match {
      case NoSuccess(msg, _) =>
        Console.err.println(msg)
        Left(SQLParserError(msg))
      case Success(result, _) =>
        result match {
          case x :: Nil => Right(Left(x))
          case _        => Right(Right(SQLMultiSearchRequest(result)))
        }
    }
  }

}

trait SQLCompilationError

case class SQLParserError(msg: String) extends SQLCompilationError

trait SQLParser extends RegexParsers with PackratParsers { _: SQLWhereParser =>

  def literal: PackratParser[SQLStringValue] =
    """"[^"]*"|'[^']*'""".r ^^ (str => SQLStringValue(str.substring(1, str.length - 1)))

  def long: PackratParser[SQLLongValue] =
    """(-)?(0|[1-9]\d*)""".r ^^ (str => SQLLongValue(str.toLong))

  def double: PackratParser[SQLDoubleValue] =
    """(-)?(\d+\.\d+)""".r ^^ (str => SQLDoubleValue(str.toDouble))

  def boolean: PackratParser[SQLBoolean] =
    """(true|false)""".r ^^ (bool => SQLBoolean(bool.toBoolean))

  /*def value_identifier: PackratParser[SQLIdentifier] = (literal | long | double | boolean) ^^ { v =>
    SQLIdentifier("", functions = v :: Nil)
  }*/

  def start: PackratParser[SQLDelimiter] = "(" ^^ (_ => StartPredicate)

  def end: PackratParser[SQLDelimiter] = ")" ^^ (_ => EndPredicate)

  def separator: PackratParser[SQLDelimiter] = "," ^^ (_ => Separator)

  def count: PackratParser[AggregateFunction] = Count.regex ^^ (_ => Count)

  def min: PackratParser[AggregateFunction] = Min.regex ^^ (_ => Min)

  def max: PackratParser[AggregateFunction] = Max.regex ^^ (_ => Max)

  def avg: PackratParser[AggregateFunction] = Avg.regex ^^ (_ => Avg)

  def sum: PackratParser[AggregateFunction] = Sum.regex ^^ (_ => Sum)

  def year: PackratParser[TimeUnit] = Year.regex ^^ (_ => Year)

  def month: PackratParser[TimeUnit] = Month.regex ^^ (_ => Month)

  def quarter: PackratParser[TimeUnit] = Quarter.regex ^^ (_ => Quarter)

  def week: PackratParser[TimeUnit] = Week.regex ^^ (_ => Week)

  def day: PackratParser[TimeUnit] = Day.regex ^^ (_ => Day)

  def hour: PackratParser[TimeUnit] = Hour.regex ^^ (_ => Hour)

  def minute: PackratParser[TimeUnit] = Minute.regex ^^ (_ => Minute)

  def second: PackratParser[TimeUnit] = Second.regex ^^ (_ => Second)

  def time_unit: PackratParser[TimeUnit] =
    year | month | quarter | week | day | hour | minute | second

  def interval: PackratParser[TimeInterval] =
    Interval.regex ~ long ~ time_unit ^^ { case _ ~ l ~ u =>
      TimeInterval(l.value.toInt, u)
    }

  def parens: PackratParser[List[SQLDelimiter]] =
    start ~ end ^^ { case s ~ e => s :: e :: Nil }

  def current_date: PackratParser[CurrentFunction] =
    CurrentDate.regex ~ parens.? ^^ { case _ ~ p =>
      if (p.isDefined) CurentDateWithParens else CurrentDate
    }

  def current_time: PackratParser[CurrentFunction] =
    CurrentTime.regex ~ parens.? ^^ { case _ ~ p =>
      if (p.isDefined) CurrentTimeWithParens else CurrentTime
    }

  def current_timestamp: PackratParser[CurrentFunction] =
    CurrentTimestamp.regex ~ parens.? ^^ { case _ ~ p =>
      if (p.isDefined) CurrentTimestampWithParens else CurrentTimestamp
    }

  def now: PackratParser[CurrentFunction] = Now.regex ~ parens.? ^^ { case _ ~ p =>
    if (p.isDefined) NowWithParens else Now
  }

  def add: PackratParser[ArithmeticOperator] = Add.sql ^^ (_ => Add)

  def subtract: PackratParser[ArithmeticOperator] = Subtract.sql ^^ (_ => Subtract)

  def intervalOperator: PackratParser[ArithmeticOperator] = add | subtract

  def arithmeticOperator: PackratParser[ArithmeticOperator] = intervalOperator

  def add_interval: PackratParser[SQLAddInterval] =
    add ~ interval ^^ { case _ ~ it =>
      SQLAddInterval(it)
    }

  def substract_interval: PackratParser[SQLSubtractInterval] =
    subtract ~ interval ^^ { case _ ~ it =>
      SQLSubtractInterval(it)
    }

  def intervalFunction: PackratParser[SQLArithmeticFunction[SQLTemporal, SQLTemporal]] =
    add_interval | substract_interval

  def identifierWithSystemFunction: PackratParser[SQLIdentifier] =
    (current_date | current_time | current_timestamp | now) ~ intervalFunction.? ^^ {
      case f1 ~ f2 =>
        f2 match {
          case Some(f) => SQLIdentifier("", functions = List(f, f1))
          case None    => SQLIdentifier("", functions = List(f1))
        }
    }

  def date_trunc: PackratParser[SQLFunctionWithIdentifier] =
    "(?i)date_trunc".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithArithmeticFunction | identifier) ~ separator ~ time_unit ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ u ~ _ =>
        DateTrunc(i, u)
    }

  def extract: PackratParser[SQLUnaryFunction[SQLTemporal, SQLNumeric]] =
    "(?i)extract".r ~ start ~ time_unit ~ end ^^ { case _ ~ _ ~ u ~ _ =>
      Extract(u)
    }

  def extract_year: PackratParser[SQLUnaryFunction[SQLTemporal, SQLNumeric]] =
    Year.regex ^^ (_ => YEAR)

  def extract_month: PackratParser[SQLUnaryFunction[SQLTemporal, SQLNumeric]] =
    Month.regex ^^ (_ => MONTH)

  def extract_day: PackratParser[SQLUnaryFunction[SQLTemporal, SQLNumeric]] =
    Day.regex ^^ (_ => DAY)

  def extract_hour: PackratParser[SQLUnaryFunction[SQLTemporal, SQLNumeric]] =
    Hour.regex ^^ (_ => HOUR)

  def extract_minute: PackratParser[SQLUnaryFunction[SQLTemporal, SQLNumeric]] =
    Minute.regex ^^ (_ => MINUTE)

  def extract_second: PackratParser[SQLUnaryFunction[SQLTemporal, SQLNumeric]] =
    Second.regex ^^ (_ => SECOND)

  def extractors: PackratParser[SQLUnaryFunction[SQLTemporal, SQLNumeric]] =
    extract | extract_year | extract_month | extract_day | extract_hour | extract_minute | extract_second

  def date_add: PackratParser[DateFunction with SQLFunctionWithIdentifier] =
    "(?i)date_add".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithArithmeticFunction | identifier) ~ separator ~ interval ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ t ~ _ =>
        DateAdd(i, t)
    }

  def date_sub: PackratParser[DateFunction with SQLFunctionWithIdentifier] =
    "(?i)date_sub".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithArithmeticFunction | identifier) ~ separator ~ interval ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ t ~ _ =>
        DateSub(i, t)
    }

  def parse_date: PackratParser[DateFunction with SQLFunctionWithIdentifier] =
    "(?i)parse_date".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithArithmeticFunction | literal | identifier) ~ separator ~ literal ~ end ^^ {
      case _ ~ _ ~ li ~ _ ~ f ~ _ =>
        li match {
          case l: SQLStringValue =>
            ParseDate(SQLIdentifier("", functions = l :: Nil), f.value)
          case i: SQLIdentifier =>
            ParseDate(i, f.value)
        }
    }

  def format_date: PackratParser[DateFunction with SQLFunctionWithIdentifier] =
    "(?i)format_date".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithArithmeticFunction | identifier) ~ separator ~ literal ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ f ~ _ =>
        FormatDate(i, f.value)
    }

  def date_functions: PackratParser[DateFunction] = date_add | date_sub | parse_date | format_date

  def datetime_add: PackratParser[DateTimeFunction with SQLFunctionWithIdentifier] =
    "(?i)datetime_add".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithArithmeticFunction | identifier) ~ separator ~ interval ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ t ~ _ =>
        DateTimeAdd(i, t)
    }

  def datetime_sub: PackratParser[DateTimeFunction with SQLFunctionWithIdentifier] =
    "(?i)datetime_sub".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithArithmeticFunction | identifier) ~ separator ~ interval ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ t ~ _ =>
        DateTimeSub(i, t)
    }

  def parse_datetime: PackratParser[DateTimeFunction with SQLFunctionWithIdentifier] =
    "(?i)parse_datetime".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithArithmeticFunction | literal | identifier) ~ separator ~ literal ~ end ^^ {
      case _ ~ _ ~ li ~ _ ~ f ~ _ =>
        li match {
          case l: SQLLiteral =>
            ParseDateTime(SQLIdentifier("", functions = l :: Nil), f.value)
          case i: SQLIdentifier =>
            ParseDateTime(i, f.value)
        }
    }

  def format_datetime: PackratParser[DateTimeFunction with SQLFunctionWithIdentifier] =
    "(?i)format_datetime".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithArithmeticFunction | identifier) ~ separator ~ literal ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ f ~ _ =>
        FormatDateTime(i, f.value)
    }

  def datetime_functions: PackratParser[DateTimeFunction] =
    datetime_add | datetime_sub | parse_datetime | format_datetime

  def aggregates: PackratParser[AggregateFunction] = count | min | max | avg | sum

  def distance: PackratParser[SQLFunction] = Distance.regex ^^ (_ => Distance)

  def identifierWithTemporalFunction: PackratParser[SQLIdentifier] =
    rep1sep(
      date_trunc | extractors | date_functions | datetime_functions,
      start
    ) ~ start.? ~ (identifierWithSystemFunction | identifier).? ~ rep(
      end
    ) ^^ { case f ~ _ ~ i ~ _ =>
      i match {
        case Some(id) => id.copy(functions = id.functions ++ f)
        case None =>
          f.lastOption match {
            case Some(fi: SQLFunctionWithIdentifier) =>
              fi.identifier.copy(functions = f ++ fi.identifier.functions)
            case _ => SQLIdentifier("", functions = f)
          }
      }
    }

  def date_diff: PackratParser[SQLBinaryFunction[_, _, _]] =
    "(?i)date_diff".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithArithmeticFunction | identifier) ~ separator ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithArithmeticFunction | identifier) ~ separator ~ time_unit ~ end ^^ {
      case _ ~ _ ~ d1 ~ _ ~ d2 ~ _ ~ u ~ _ => DateDiff(d1, d2, u)
    }

  def date_diff_identifier: PackratParser[SQLIdentifier] = date_diff ^^ { dd =>
    SQLIdentifier("", functions = dd :: Nil)
  }

  def case_when_identifier: Parser[SQLIdentifier] = case_when ^^ { cw =>
    SQLIdentifier("", functions = cw :: Nil)
  }

  def is_null: PackratParser[SQLConditionalFunction[_]] =
    "(?i)isnull".r ~ start ~ (identifierWithTransformation | identifierWithArithmeticFunction | identifierWithTemporalFunction | identifier) ~ end ^^ {
      case _ ~ _ ~ i ~ _ => SQLIsNullFunction(i)
    }

  def is_notnull: PackratParser[SQLConditionalFunction[_]] =
    "(?i)isnotnull".r ~ start ~ (identifierWithTransformation | identifierWithArithmeticFunction | identifierWithTemporalFunction | identifier) ~ end ^^ {
      case _ ~ _ ~ i ~ _ => SQLIsNotNullFunction(i)
    }

  def valueExpr: PackratParser[PainlessScript] =
    // les plus spécifiques en premier
    identifierWithTransformation | // transformations appliquées à un identifier
    date_diff_identifier | // date_diff(...) retournant un identifier-like
    identifierWithSystemFunction | // CURRENT_DATE, NOW, etc. (+/- interval)
    identifierWithArithmeticFunction | // foo - interval ...
    identifierWithTemporalFunction | // chaîne de fonctions appliquées à un identifier
    identifierWithFunction | // fonctions appliquées à un identifier
    literal | // 'string'
    long |
    double |
    boolean |
    identifier

  def coalesce: PackratParser[SQLCoalesce] =
    Coalesce.regex ~ start ~ rep1sep(
      valueExpr,
      separator
    ) ~ end ^^ { case _ ~ _ ~ ids ~ _ =>
      SQLCoalesce(ids)
    }

  def nullif: PackratParser[SQLNullIf] =
    NullIf.regex ~ start ~ valueExpr ~ separator ~ valueExpr ~ end ^^ {
      case _ ~ _ ~ id1 ~ _ ~ id2 ~ _ => SQLNullIf(id1, id2)
    }

  def start_case: PackratParser[StartCase.type] = Case.regex ^^ (_ => StartCase)

  def when_case: PackratParser[WhenCase.type] = When.regex ^^ (_ => WhenCase)

  def then_case: PackratParser[ThenCase.type] = Then.regex ^^ (_ => ThenCase)

  def else_case: PackratParser[Else.type] = Else.regex ^^ (_ => Else)

  def end_case: PackratParser[EndCase.type] = End.regex ^^ (_ => EndCase)

  def case_condition: Parser[(PainlessScript, PainlessScript)] =
    when_case ~ (whereCriteria | valueExpr) ~ then_case.? ~ valueExpr ^^ { case _ ~ c ~ _ ~ r =>
      c match {
        case p: PainlessScript => p -> r
        case rawTokens: List[SQLToken] =>
          processTokens(rawTokens) match {
            case Some(criteria) => criteria -> r
            case _              => SQLNull  -> r
          }
      }
    }

  def case_else: Parser[PainlessScript] = else_case ~ valueExpr ^^ { case _ ~ r => r }

  def case_when: PackratParser[SQLCaseWhen] =
    start_case ~ valueExpr.? ~ rep1(case_condition) ~ case_else.? ~ end_case ^^ {
      case _ ~ e ~ c ~ r ~ _ => SQLCaseWhen(e, c, r)
    }

  def logical_functions: PackratParser[SQLTransformFunction[_, _]] =
    is_null | is_notnull | coalesce | nullif | case_when

  def sql_functions: PackratParser[SQLFunction] =
    aggregates | distance | date_diff | date_trunc | extractors | date_functions | datetime_functions | logical_functions

  //private val regexIdentifier = """[\*a-zA-Z_\-][a-zA-Z0-9_\-\.\[\]\*]*"""

  private val reservedKeywords = Seq(
    "select",
    "from",
    "where",
    "group",
    "having",
    "order",
    "limit",
    "as",
    "by",
    "except",
    "unnest",
    "current_date",
    "current_time",
    "current_datetime",
    "current_timestamp",
    "now",
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
    "year",
    "month",
    "day",
    "hour",
    "minute",
    "second",
    "quarter",
    "string",
    "int",
    "integer",
    "long",
    "bigint",
    "double",
    "boolean",
    "time",
    "date",
    "datetime",
    "timestamp",
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
    "end"
  )

  private val identifierRegexStr =
    s"""(?i)(?!(?:${reservedKeywords.mkString(
      "|"
    )})\\b)[\\*a-zA-Z_\\-][a-zA-Z0-9_\\-.\\[\\]\\*]*"""

  private val identifierRegex = identifierRegexStr.r // scala.util.matching.Regex

  def identifier: PackratParser[SQLIdentifier] =
    Distinct.regex.? ~ identifierRegex ^^ { case d ~ i =>
      SQLIdentifier(
        i,
        None,
        d.isDefined
      )
    }

  def string_type: PackratParser[SQLTypes.Varchar.type] = "(?i)string".r ^^ (_ => SQLTypes.Varchar)

  def date_type: PackratParser[SQLTypes.Date.type] = "(?i)date".r ^^ (_ => SQLTypes.Date)

  def time_type: PackratParser[SQLTypes.Time.type] = "(?i)time".r ^^ (_ => SQLTypes.Time)

  def datetime_type: PackratParser[SQLTypes.DateTime.type] =
    "(?i)(datetime)".r ^^ (_ => SQLTypes.DateTime)

  def timestamp_type: PackratParser[SQLTypes.Timestamp.type] =
    "(?i)(timestamp)".r ^^ (_ => SQLTypes.Timestamp)

  def boolean_type: PackratParser[SQLTypes.Boolean.type] =
    "(?i)boolean".r ^^ (_ => SQLTypes.Boolean)

  def long_type: PackratParser[SQLTypes.BigInt.type] = "(?i)long|bigint".r ^^ (_ => SQLTypes.BigInt)

  def double_type: PackratParser[SQLTypes.Double.type] = "(?i)double".r ^^ (_ => SQLTypes.Double)

  def int_type: PackratParser[SQLTypes.Int.type] = "(?i)(int|integer)".r ^^ (_ => SQLTypes.Int)

  def sql_type: PackratParser[SQLType] =
    string_type | datetime_type | timestamp_type | date_type | time_type | boolean_type | long_type | double_type | int_type

  private[this] def castFunctionWithIdentifier: PackratParser[SQLIdentifier] =
    "(?i)cast".r ~ start ~ (identifierWithTransformation | identifierWithSystemFunction | identifierWithArithmeticFunction | identifierWithFunction | date_diff_identifier | identifier) ~ Alias.regex.? ~ sql_type ~ end ~ arithmeticFunction.? ^^ {
      case _ ~ _ ~ i ~ as ~ t ~ _ ~ a =>
        i.copy(functions =
          (SQLCast(i, targetType = t, as = as.isDefined) +: i.functions) ++ a.toList
        )
    }

  private[this] def dateFunctionWithIdentifier: PackratParser[SQLIdentifier] =
    (parse_date | format_date | date_add | date_sub) ~ arithmeticFunction.? ^^ { case t ~ af =>
      af match {
        case Some(f) => t.identifier.copy(functions = f +: t +: t.identifier.functions)
        case None    => t.identifier.copy(functions = t +: t.identifier.functions)
      }
    }

  private[this] def dateTimeFunctionWithIdentifier: PackratParser[SQLIdentifier] =
    (date_trunc | parse_datetime | format_datetime | datetime_add | datetime_sub) ~ arithmeticFunction.? ^^ {
      case t ~ af =>
        af match {
          case Some(f) => t.identifier.copy(functions = f +: t +: t.identifier.functions)
          case None    => t.identifier.copy(functions = t +: t.identifier.functions)
        }
    }

  private[this] def conditionalFunctionWithIdentifier: PackratParser[SQLIdentifier] =
    (is_null | is_notnull | coalesce | nullif) ^^ { t =>
      t.identifier.copy(functions = t +: t.identifier.functions)
    }

  def identifierWithTransformation: PackratParser[SQLIdentifier] =
    castFunctionWithIdentifier | conditionalFunctionWithIdentifier | dateFunctionWithIdentifier | dateTimeFunctionWithIdentifier

  def arithmeticFunction: PackratParser[SQLArithmeticFunction[_, _]] = intervalFunction

  def identifierWithArithmeticFunction: PackratParser[SQLIdentifier] =
    (identifierWithFunction | identifier) ~ arithmeticFunction ^^ { case i ~ af =>
      i.copy(functions = af +: i.functions)
    }

  def identifierWithAggregation: PackratParser[SQLIdentifier] =
    aggregates ~ start ~ (identifierWithFunction | identifierWithArithmeticFunction | identifier) ~ end ^^ {
      case a ~ _ ~ i ~ _ =>
        i.copy(functions = a +: i.functions)
    }

  def identifierWithFunction: PackratParser[SQLIdentifier] =
    rep1sep(
      sql_functions,
      start
    ) ~ start.? ~ (identifierWithSystemFunction | identifierWithArithmeticFunction | identifier).? ~ rep1(
      end
    ) ^^ { case f ~ _ ~ i ~ _ =>
      i match {
        case None =>
          f.lastOption match {
            case Some(fi: SQLFunctionWithIdentifier) =>
              fi.identifier.copy(functions = f ++ fi.identifier.functions)
            case _ => SQLIdentifier("", functions = f)
          }
        case Some(id) => id.copy(functions = id.functions ++ f)
      }
    }

  private val regexAlias =
    """\b(?!(?i)as\b)\b(?!(?i)except\b)\b(?!(?i)where\b)\b(?!(?i)filter\b)\b(?!(?i)from\b)\b(?!(?i)group\b)\b(?!(?i)having\b)\b(?!(?i)order\b)\b(?!(?i)limit\b)[a-zA-Z0-9_]*"""

  def alias: PackratParser[SQLAlias] = Alias.regex.? ~ regexAlias.r ^^ { case _ ~ b => SQLAlias(b) }

  def field: PackratParser[Field] =
    (identifierWithTransformation | identifierWithAggregation | identifierWithSystemFunction | identifierWithArithmeticFunction | identifierWithFunction | date_diff_identifier | case_when_identifier | identifier) ~ alias.? ^^ {
      case i ~ a =>
        SQLField(i, a)
    }

}

trait SQLSelectParser {
  self: SQLParser with SQLWhereParser =>

  def except: PackratParser[SQLExcept] = Except.regex ~ start ~ rep1sep(field, separator) ~ end ^^ {
    case _ ~ _ ~ e ~ _ =>
      SQLExcept(e)
  }

  def select: PackratParser[SQLSelect] =
    Select.regex ~ rep1sep(
      field,
      separator
    ) ~ except.? ^^ { case _ ~ fields ~ e =>
      SQLSelect(fields, e)
    }

}

trait SQLFromParser {
  self: SQLParser with SQLLimitParser =>

  def unnest: PackratParser[SQLTable] =
    Unnest.regex ~ start ~ identifier ~ limit.? ~ end ~ alias ^^ { case _ ~ _ ~ i ~ l ~ _ ~ a =>
      SQLTable(SQLUnnest(i, l), Some(a))
    }

  def table: PackratParser[SQLTable] = identifier ~ alias.? ^^ { case i ~ a => SQLTable(i, a) }

  def from: PackratParser[SQLFrom] = From.regex ~ rep1sep(unnest | table, separator) ^^ {
    case _ ~ tables =>
      SQLFrom(tables)
  }

}

trait SQLWhereParser {
  self: SQLParser with SQLGroupByParser with SQLOrderByParser =>

  def isNull: PackratParser[SQLCriteria] = identifier ~ IsNull.regex ^^ { case i ~ _ =>
    SQLIsNull(i)
  }

  def isNotNull: PackratParser[SQLCriteria] = identifier ~ IsNotNull.regex ^^ { case i ~ _ =>
    SQLIsNotNull(i)
  }

  private def eq: PackratParser[SQLComparisonOperator] = Eq.sql ^^ (_ => Eq)

  private def ne: PackratParser[SQLComparisonOperator] = Ne.sql ^^ (_ => Ne)

  private def diff: PackratParser[SQLComparisonOperator] = Diff.sql ^^ (_ => Diff)

  private def any_identifier: PackratParser[SQLIdentifier] =
    identifierWithTransformation | identifierWithAggregation | identifierWithSystemFunction | identifierWithArithmeticFunction | identifierWithFunction | date_diff_identifier | identifier

  private def equality: PackratParser[SQLExpression] =
    not.? ~ any_identifier ~ (eq | ne | diff) ~ (boolean | literal | double | long | any_identifier) ^^ {
      case n ~ i ~ o ~ v => SQLExpression(i, o, v, n)
    }

  def like: PackratParser[SQLExpression] =
    any_identifier ~ not.? ~ Like.regex ~ literal ^^ { case i ~ n ~ _ ~ v =>
      SQLExpression(i, Like, v, n)
    }

  private def ge: PackratParser[SQLComparisonOperator] = Ge.sql ^^ (_ => Ge)

  def gt: PackratParser[SQLComparisonOperator] = Gt.sql ^^ (_ => Gt)

  private def le: PackratParser[SQLComparisonOperator] = Le.sql ^^ (_ => Le)

  def lt: PackratParser[SQLComparisonOperator] = Lt.sql ^^ (_ => Lt)

  private def comparison: PackratParser[SQLExpression] =
    not.? ~ any_identifier ~ (ge | gt | le | lt) ~ (double | long | literal | any_identifier) ^^ {
      case n ~ i ~ o ~ v => SQLExpression(i, o, v, n)
    }

  def in: PackratParser[SQLExpressionOperator] = In.regex ^^ (_ => In)

  private def inLiteral: PackratParser[SQLCriteria] =
    any_identifier ~ not.? ~ in ~ start ~ rep1sep(literal, separator) ~ end ^^ {
      case i ~ n ~ _ ~ _ ~ v ~ _ =>
        SQLIn(
          i,
          SQLStringValues(v),
          n
        )
    }

  private def inDoubles: PackratParser[SQLCriteria] =
    any_identifier ~ not.? ~ in ~ start ~ rep1sep(
      double,
      separator
    ) ~ end ^^ { case i ~ n ~ _ ~ _ ~ v ~ _ =>
      SQLIn(
        i,
        SQLDoubleValues(v),
        n
      )
    }

  private def inLongs: PackratParser[SQLCriteria] =
    any_identifier ~ not.? ~ in ~ start ~ rep1sep(
      long,
      separator
    ) ~ end ^^ { case i ~ n ~ _ ~ _ ~ v ~ _ =>
      SQLIn(
        i,
        SQLLongValues(v),
        n
      )
    }

  def between: PackratParser[SQLCriteria] =
    any_identifier ~ not.? ~ Between.regex ~ literal ~ and ~ literal ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => SQLBetween(i, SQLLiteralFromTo(from, to), n)
    }

  def betweenLongs: PackratParser[SQLCriteria] =
    any_identifier ~ not.? ~ Between.regex ~ long ~ and ~ long ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => SQLBetween(i, SQLLongFromTo(from, to), n)
    }

  def betweenDoubles: PackratParser[SQLCriteria] =
    any_identifier ~ not.? ~ Between.regex ~ double ~ and ~ double ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => SQLBetween(i, SQLDoubleFromTo(from, to), n)
    }

  def sql_distance: PackratParser[SQLCriteria] =
    distance ~ start ~ identifier ~ separator ~ start ~ double ~ separator ~ double ~ end ~ end ~ le ~ literal ^^ {
      case _ ~ _ ~ i ~ _ ~ _ ~ lat ~ _ ~ lon ~ _ ~ _ ~ _ ~ d => ElasticGeoDistance(i, d, lat, lon)
    }

  def matchCriteria: PackratParser[SQLMatch] =
    Match.regex ~ start ~ rep1sep(
      any_identifier,
      separator
    ) ~ end ~ Against.regex ~ start ~ literal ~ end ^^ { case _ ~ _ ~ i ~ _ ~ _ ~ _ ~ l ~ _ =>
      SQLMatch(i, l)
    }

  def and: PackratParser[SQLPredicateOperator] = And.regex ^^ (_ => And)

  def or: PackratParser[SQLPredicateOperator] = Or.regex ^^ (_ => Or)

  def not: PackratParser[Not.type] = Not.regex ^^ (_ => Not)

  def logical_criteria: PackratParser[SQLCriteria] =
    (is_null | is_notnull) ^^ { case SQLConditionalFunctionAsCriteria(c) =>
      c
    }

  def criteria: PackratParser[SQLCriteria] =
    (equality | like | comparison | inLiteral | inLongs | inDoubles | between | betweenLongs | betweenDoubles | isNotNull | isNull | /*coalesce | nullif |*/ sql_distance | matchCriteria | logical_criteria) ^^ (
      c => c
    )

  def predicate: PackratParser[SQLPredicate] = criteria ~ (and | or) ~ not.? ~ criteria ^^ {
    case l ~ o ~ n ~ r => SQLPredicate(l, o, r, n)
  }

  def nestedCriteria: PackratParser[ElasticRelation] =
    Nested.regex ~ start.? ~ criteria ~ end.? ^^ { case _ ~ _ ~ c ~ _ =>
      ElasticNested(c, None)
    }

  def nestedPredicate: PackratParser[ElasticRelation] = Nested.regex ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticNested(p, None)
  }

  def childCriteria: PackratParser[ElasticRelation] = Child.regex ~ start.? ~ criteria ~ end.? ^^ {
    case _ ~ _ ~ c ~ _ => ElasticChild(c)
  }

  def childPredicate: PackratParser[ElasticRelation] = Child.regex ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticChild(p)
  }

  def parentCriteria: PackratParser[ElasticRelation] =
    Parent.regex ~ start.? ~ criteria ~ end.? ^^ { case _ ~ _ ~ c ~ _ =>
      ElasticParent(c)
    }

  def parentPredicate: PackratParser[ElasticRelation] = Parent.regex ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticParent(p)
  }

  private def allPredicate: PackratParser[SQLCriteria] =
    nestedPredicate | childPredicate | parentPredicate | predicate

  private def allCriteria: PackratParser[SQLToken] =
    nestedCriteria | childCriteria | parentCriteria | criteria

  def whereCriteria: PackratParser[List[SQLToken]] = rep1(
    allPredicate | allCriteria | start | or | and | end | then_case
  )

  def where: PackratParser[SQLWhere] =
    Where.regex ~ whereCriteria ^^ { case _ ~ rawTokens =>
      SQLWhere(processTokens(rawTokens))
    }

  import scala.annotation.tailrec

  /** This method is used to recursively process a list of SQL tokens and construct SQL criteria and
    * predicates from these tokens. Here are the key points:
    *
    * Base case (Nil): If the list of tokens is empty (Nil), we check the contents of the stack to
    * determine the final result.
    *
    * If the stack contains an operator, a left criterion and a right criterion, we create a
    * SQLPredicate predicate. Otherwise, we return the first criterion (SQLCriteria) of the stack if
    * it exists. Case of criteria (SQLCriteria): If the first token is a criterion, we treat it
    * according to the content of the stack:
    *
    * If the stack contains a predicate operator, we create a predicate with the left and right
    * criteria and update the stack. Otherwise, we simply add the criterion to the stack. Case of
    * operators (SQLPredicateOperator): If the first token is a predicate operator, we treat it
    * according to the contents of the stack:
    *
    * If the stack contains at least two elements, we create a predicate with the left and right
    * criterion and update the stack. If the stack contains only one element (a single operator), we
    * simply add the operator to the stack. Otherwise, it's a battery status error. Case of
    * delimiters (StartDelimiter and EndDelimiter): If the first token is a start delimiter
    * (StartDelimiter), we extract the tokens up to the corresponding end delimiter (EndDelimiter),
    * we recursively process the extracted sub-tokens, then we continue with the rest of the tokens.
    *
    * Other cases: If none of the previous cases match, an IllegalStateException is thrown to
    * indicate an unexpected token type.
    *
    * @param tokens
    *   - liste des tokens SQL
    * @param stack
    *   - stack de tokens
    * @return
    */
  @tailrec
  private def processTokensHelper(
    tokens: List[SQLToken],
    stack: List[SQLToken]
  ): Option[SQLCriteria] = {
    tokens match {
      case Nil =>
        stack match {
          case (right: SQLCriteria) :: (op: SQLPredicateOperator) :: (left: SQLCriteria) :: Nil =>
            Option(
              SQLPredicate(left, op, right)
            )
          case _ =>
            stack.headOption.collect { case c: SQLCriteria => c }
        }
      case (_: StartDelimiter) :: rest =>
        val (subTokens, remainingTokens) = extractSubTokens(rest, 1)
        val subCriteria = processSubTokens(subTokens) match {
          case p: SQLPredicate => p.copy(group = true)
          case c               => c
        }
        processTokensHelper(remainingTokens, subCriteria :: stack)
      case (c: SQLCriteria) :: rest =>
        stack match {
          case (op: SQLPredicateOperator) :: (left: SQLCriteria) :: tail =>
            val predicate = SQLPredicate(left, op, c)
            processTokensHelper(rest, predicate :: tail)
          case _ =>
            processTokensHelper(rest, c :: stack)
        }
      case (op: SQLPredicateOperator) :: rest =>
        stack match {
          case (right: SQLCriteria) :: (left: SQLCriteria) :: tail =>
            val predicate = SQLPredicate(left, op, right)
            processTokensHelper(rest, predicate :: tail)
          case (right: SQLCriteria) :: (o: SQLPredicateOperator) :: tail =>
            tail match {
              case (left: SQLCriteria) :: tt =>
                val predicate = SQLPredicate(left, op, right)
                processTokensHelper(rest, o :: predicate :: tt)
              case _ =>
                processTokensHelper(rest, op :: stack)
            }
          case _ :: Nil =>
            processTokensHelper(rest, op :: stack)
          case _ =>
            throw SQLValidationError("Invalid stack state for predicate creation")
        }
      case ThenCase :: _ =>
        processTokensHelper(Nil, stack) // exit processing on THEN
      case (_: EndDelimiter) :: rest =>
        processTokensHelper(rest, stack) // Ignore and move on
      case _ => processTokensHelper(Nil, stack)
    }
  }

  /** This method calls processTokensHelper with an empty stack (Nil) to begin processing primary
    * tokens.
    *
    * @param tokens
    *   - list of SQL tokens
    * @return
    */
  protected def processTokens(
    tokens: List[SQLToken]
  ): Option[SQLCriteria] = {
    processTokensHelper(tokens, Nil)
  }

  /** This method is used to process subtokens extracted between delimiters. It calls
    * processTokensHelper and returns the result as a SQLCriteria, or throws an exception if no
    * criteria is found.
    *
    * @param tokens
    *   - list of SQL tokens
    * @return
    */
  private def processSubTokens(tokens: List[SQLToken]): SQLCriteria = {
    processTokensHelper(tokens, Nil).getOrElse(
      throw SQLValidationError("Empty sub-expression")
    )
  }

  /** This method is used to extract subtokens between a start delimiter (StartDelimiter) and its
    * corresponding end delimiter (EndDelimiter). It uses a recursive approach to maintain the count
    * of open and closed delimiters and correctly construct the list of extracted subtokens.
    *
    * @param tokens
    *   - list of SQL tokens
    * @param openCount
    *   - count of open delimiters
    * @param subTokens
    *   - list of extracted subtokens
    * @return
    */
  @tailrec
  private def extractSubTokens(
    tokens: List[SQLToken],
    openCount: Int,
    subTokens: List[SQLToken] = Nil
  ): (List[SQLToken], List[SQLToken]) = {
    tokens match {
      case Nil => throw SQLValidationError("Unbalanced parentheses")
      case (start: StartDelimiter) :: rest =>
        extractSubTokens(rest, openCount + 1, start :: subTokens)
      case (end: EndDelimiter) :: rest =>
        if (openCount - 1 == 0) {
          (subTokens.reverse, rest)
        } else extractSubTokens(rest, openCount - 1, end :: subTokens)
      case head :: rest => extractSubTokens(rest, openCount, head :: subTokens)
    }
  }
}

trait SQLGroupByParser {
  self: SQLParser with SQLWhereParser =>

  def bucket: PackratParser[SQLBucket] = identifier ^^ { i =>
    SQLBucket(i)
  }

  def groupBy: PackratParser[SQLGroupBy] =
    GroupBy.regex ~ rep1sep(bucket, separator) ^^ { case _ ~ buckets =>
      SQLGroupBy(buckets)
    }

}

trait SQLHavingParser {
  self: SQLParser with SQLWhereParser =>

  def having: PackratParser[SQLHaving] = Having.regex ~> whereCriteria ^^ { rawTokens =>
    SQLHaving(
      processTokens(rawTokens)
    )
  }

}

trait SQLOrderByParser {
  self: SQLParser =>

  def asc: PackratParser[Asc.type] = Asc.regex ^^ (_ => Asc)

  def desc: PackratParser[Desc.type] = Desc.regex ^^ (_ => Desc)

  private def fieldName: PackratParser[String] =
    """\b(?!(?i)limit\b)[a-zA-Z_][a-zA-Z0-9_]*""".r ^^ (f => f)

  def fieldWithFunction: PackratParser[(String, List[SQLFunction])] =
    rep1sep(sql_functions, start) ~ start.? ~ fieldName ~ rep1(end) ^^ { case f ~ _ ~ n ~ _ =>
      (n, f)
    }

  def sort: PackratParser[SQLFieldSort] =
    (fieldWithFunction | fieldName) ~ (asc | desc).? ^^ { case f ~ o =>
      f match {
        case i: (String, List[SQLFunction]) => SQLFieldSort(i._1, o, i._2)
        case s: String                      => SQLFieldSort(s, o, List.empty)
      }
    }

  def orderBy: PackratParser[SQLOrderBy] = OrderBy.regex ~ rep1sep(sort, separator) ^^ {
    case _ ~ s =>
      SQLOrderBy(s)
  }

}

trait SQLLimitParser {
  self: SQLParser =>

  def limit: PackratParser[SQLLimit] = Limit.regex ~ long ^^ { case _ ~ i =>
    SQLLimit(i.value.toInt)
  }

}
