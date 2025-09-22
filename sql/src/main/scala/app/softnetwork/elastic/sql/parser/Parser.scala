package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.`type`._
import app.softnetwork.elastic.sql.function._
import app.softnetwork.elastic.sql.function.aggregate._
import app.softnetwork.elastic.sql.function.cond._
import app.softnetwork.elastic.sql.function.convert._
import app.softnetwork.elastic.sql.function.geo.Distance
import app.softnetwork.elastic.sql.function.math._
import app.softnetwork.elastic.sql.function.string._
import app.softnetwork.elastic.sql.function.time._
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql.operator.math._
import app.softnetwork.elastic.sql.time.TimeUnit._
import app.softnetwork.elastic.sql.time._
import app.softnetwork.elastic.sql._
import app.softnetwork.elastic.sql.query._

import scala.language.implicitConversions
import scala.language.existentials
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
    with LimitParser
    with PackratParsers {

  def request: PackratParser[SQLSearchRequest] = {
    phrase(select ~ from ~ where.? ~ groupBy.? ~ having.? ~ orderBy.? ~ limit.?) ^^ {
      case s ~ f ~ w ~ g ~ h ~ o ~ l =>
        val request = SQLSearchRequest(s, f, w, g, h, o, l).update()
        request.validate() match {
          case Left(error) => throw ValidationError(error)
          case _           =>
        }
        request
    }
  }

  def union: PackratParser[Union.type] = Union.regex ^^ (_ => Union)

  def requests: PackratParser[List[SQLSearchRequest]] = rep1sep(request, union) ^^ (s => s)

  def apply(
    query: String
  ): Either[ParserError, Either[SQLSearchRequest, SQLMultiSearchRequest]] = {
    val reader = new PackratReader(new CharSequenceReader(query))
    parse(requests, reader) match {
      case NoSuccess(msg, _) =>
        Console.err.println(msg)
        Left(ParserError(msg))
      case Success(result, _) =>
        result match {
          case x :: Nil => Right(Left(x))
          case _        => Right(Right(SQLMultiSearchRequest(result)))
        }
    }
  }

}

trait CompilationError

case class ParserError(msg: String) extends CompilationError

trait Parser extends RegexParsers with PackratParsers { _: WhereParser =>

  def literal: PackratParser[StringValue] =
    """"[^"]*"|'[^']*'""".r ^^ (str => StringValue(str.substring(1, str.length - 1)))

  def long: PackratParser[LongValue] =
    """(-)?(0|[1-9]\d*)""".r ^^ (str => LongValue(str.toLong))

  def double: PackratParser[DoubleValue] =
    """(-)?(\d+\.\d+)""".r ^^ (str => DoubleValue(str.toDouble))

  def pi: PackratParser[Value[Double]] =
    Pi.regex ^^ (_ => PiValue)

  def boolean: PackratParser[BooleanValue] =
    """(true|false)""".r ^^ (bool => BooleanValue(bool.toBoolean))

  def value_identifier: PackratParser[Identifier] =
    (literal | long | double | pi | boolean) ^^ { v =>
      Identifier(v)
    }

  def start: PackratParser[Delimiter] = "(" ^^ (_ => StartPredicate)

  def end: PackratParser[Delimiter] = ")" ^^ (_ => EndPredicate)

  def separator: PackratParser[Delimiter] = "," ^^ (_ => Separator)

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

  def parens: PackratParser[List[Delimiter]] =
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

  def multiply: PackratParser[ArithmeticOperator] = Multiply.sql ^^ (_ => Multiply)

  def divide: PackratParser[ArithmeticOperator] = Divide.sql ^^ (_ => Divide)

  def modulo: PackratParser[ArithmeticOperator] = Modulo.sql ^^ (_ => Modulo)

  def factor: PackratParser[PainlessScript] =
    "(" ~> arithmeticExpressionLevel2 <~ ")" ^^ {
      case expr: ArithmeticExpression =>
        expr.copy(group = true)
      case other => other
    } | valueExpr

  def arithmeticExpressionLevel1: Parser[PainlessScript] =
    factor ~ rep((multiply | divide | modulo) ~ factor) ^^ { case left ~ list =>
      list.foldLeft(left) { case (acc, op ~ right) =>
        ArithmeticExpression(acc, op, right)
      }
    }

  def arithmeticExpressionLevel2: Parser[PainlessScript] =
    arithmeticExpressionLevel1 ~ rep((add | subtract) ~ arithmeticExpressionLevel1) ^^ {
      case left ~ list =>
        list.foldLeft(left) { case (acc, op ~ right) =>
          ArithmeticExpression(acc, op, right)
        }
    }

  def identifierWithArithmeticExpression: Parser[Identifier] =
    arithmeticExpressionLevel2 ^^ {
      case af: ArithmeticExpression  => Identifier(af)
      case id: Identifier            => id
      case f: FunctionWithIdentifier => f.identifier
      case f: Function               => Identifier(f)
      case other                     => throw new Exception(s"Unexpected expression $other")
    }

  def interval: PackratParser[TimeInterval] =
    Interval.regex ~ long ~ time_unit ^^ { case _ ~ l ~ u =>
      TimeInterval(l.value.toInt, u)
    }

  def add_interval: PackratParser[SQLAddInterval] =
    add ~ interval ^^ { case _ ~ it =>
      SQLAddInterval(it)
    }

  def substract_interval: PackratParser[SQLSubtractInterval] =
    subtract ~ interval ^^ { case _ ~ it =>
      SQLSubtractInterval(it)
    }

  def intervalFunction: PackratParser[TransformFunction[SQLTemporal, SQLTemporal]] =
    add_interval | substract_interval

  def identifierWithIntervalFunction: PackratParser[Identifier] =
    (identifierWithFunction | identifier) ~ intervalFunction ^^ { case i ~ f =>
      i.withFunctions(f +: i.functions)
    }

  def identifierWithSystemFunction: PackratParser[Identifier] =
    (current_date | current_time | current_timestamp | now) ~ intervalFunction.? ^^ {
      case f1 ~ f2 =>
        f2 match {
          case Some(f) => Identifier(List(f, f1))
          case None    => Identifier(f1)
        }
    }

  def date_trunc: PackratParser[FunctionWithIdentifier] =
    "(?i)date_trunc".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ time_unit ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ u ~ _ =>
        DateTrunc(i, u)
    }

  def extract_identifier: PackratParser[Identifier] =
    "(?i)extract".r ~ start ~ time_unit ~ "(?i)from".r ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ end ^^ {
      case _ ~ _ ~ u ~ _ ~ i ~ _ =>
        i.withFunctions(Extract(u) +: i.functions)
    }

  def extract_year: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
    Year.regex ^^ (_ => YEAR)

  def extract_month: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
    Month.regex ^^ (_ => MONTH)

  def extract_day: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
    Day.regex ^^ (_ => DAY)

  def extract_hour: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
    Hour.regex ^^ (_ => HOUR)

  def extract_minute: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
    Minute.regex ^^ (_ => MINUTE)

  def extract_second: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
    Second.regex ^^ (_ => SECOND)

  def extractors: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
    extract_year | extract_month | extract_day | extract_hour | extract_minute | extract_second

  def date_add: PackratParser[DateFunction with FunctionWithIdentifier] =
    "(?i)date_add".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ interval ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ t ~ _ =>
        DateAdd(i, t)
    }

  def date_sub: PackratParser[DateFunction with FunctionWithIdentifier] =
    "(?i)date_sub".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ interval ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ t ~ _ =>
        DateSub(i, t)
    }

  def parse_date: PackratParser[DateFunction with FunctionWithIdentifier] =
    "(?i)parse_date".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | literal | identifier) ~ separator ~ literal ~ end ^^ {
      case _ ~ _ ~ li ~ _ ~ f ~ _ =>
        li match {
          case l: StringValue =>
            ParseDate(Identifier(l), f.value)
          case i: Identifier =>
            ParseDate(i, f.value)
        }
    }

  def format_date: PackratParser[DateFunction with FunctionWithIdentifier] =
    "(?i)format_date".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ literal ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ f ~ _ =>
        FormatDate(i, f.value)
    }

  def date_functions: PackratParser[DateFunction] = date_add | date_sub | parse_date | format_date

  def datetime_add: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
    "(?i)datetime_add".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ interval ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ t ~ _ =>
        DateTimeAdd(i, t)
    }

  def datetime_sub: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
    "(?i)datetime_sub".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ interval ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ t ~ _ =>
        DateTimeSub(i, t)
    }

  def parse_datetime: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
    "(?i)parse_datetime".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | literal | identifier) ~ separator ~ literal ~ end ^^ {
      case _ ~ _ ~ li ~ _ ~ f ~ _ =>
        li match {
          case l: SQLLiteral =>
            ParseDateTime(Identifier(l), f.value)
          case i: Identifier =>
            ParseDateTime(i, f.value)
        }
    }

  def format_datetime: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
    "(?i)format_datetime".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ literal ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ f ~ _ =>
        FormatDateTime(i, f.value)
    }

  def datetime_functions: PackratParser[DateTimeFunction] =
    datetime_add | datetime_sub | parse_datetime | format_datetime

  def aggregates: PackratParser[AggregateFunction] = count | min | max | avg | sum

  def distance: PackratParser[Function] = Distance.regex ^^ (_ => Distance)

  def identifierWithTemporalFunction: PackratParser[Identifier] =
    rep1sep(
      date_trunc | extractors | date_functions | datetime_functions,
      start
    ) ~ start.? ~ (identifierWithSystemFunction | identifier).? ~ rep(
      end
    ) ^^ { case f ~ _ ~ i ~ _ =>
      i match {
        case Some(id) => id.withFunctions(id.functions ++ f)
        case None =>
          f.lastOption match {
            case Some(fi: FunctionWithIdentifier) =>
              fi.identifier.withFunctions(f ++ fi.identifier.functions)
            case _ => Identifier(f)
          }
      }
    }

  def date_diff: PackratParser[BinaryFunction[_, _, _]] =
    "(?i)date_diff".r ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ time_unit ~ end ^^ {
      case _ ~ _ ~ d1 ~ _ ~ d2 ~ _ ~ u ~ _ => DateDiff(d1, d2, u)
    }

  def date_diff_identifier: PackratParser[Identifier] = date_diff ^^ { dd =>
    Identifier(dd)
  }

  def is_null: PackratParser[ConditionalFunction[_]] =
    "(?i)isnull".r ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithTemporalFunction | identifier) ~ end ^^ {
      case _ ~ _ ~ i ~ _ => IsNullFunction(i)
    }

  def is_notnull: PackratParser[ConditionalFunction[_]] =
    "(?i)isnotnull".r ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithTemporalFunction | identifier) ~ end ^^ {
      case _ ~ _ ~ i ~ _ => IsNotNullFunction(i)
    }

  def valueExpr: PackratParser[PainlessScript] =
    // les plus spécifiques en premier
    identifierWithTransformation | // transformations appliquées à un identifier
    date_diff_identifier | // date_diff(...) retournant un identifier-like
    extract_identifier |
    identifierWithSystemFunction | // CURRENT_DATE, NOW, etc. (+/- interval)
    identifierWithIntervalFunction |
    identifierWithTemporalFunction | // chaîne de fonctions appliquées à un identifier
    identifierWithFunction | // fonctions appliquées à un identifier
    literal | // 'string'
    pi |
    double |
    long |
    boolean |
    identifier

  def coalesce: PackratParser[Coalesce] =
    Coalesce.regex ~ start ~ rep1sep(
      valueExpr,
      separator
    ) ~ end ^^ { case _ ~ _ ~ ids ~ _ =>
      Coalesce(ids)
    }

  def nullif: PackratParser[NullIf] =
    NullIf.regex ~ start ~ valueExpr ~ separator ~ valueExpr ~ end ^^ {
      case _ ~ _ ~ id1 ~ _ ~ id2 ~ _ => NullIf(id1, id2)
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
        case rawTokens: List[Token] =>
          processTokens(rawTokens) match {
            case Some(criteria) => criteria -> r
            case _              => Null     -> r
          }
      }
    }

  def case_else: Parser[PainlessScript] = else_case ~ valueExpr ^^ { case _ ~ r => r }

  def case_when: PackratParser[Case] =
    start_case ~ valueExpr.? ~ rep1(case_condition) ~ case_else.? ~ end_case ^^ {
      case _ ~ e ~ c ~ r ~ _ => Case(e, c, r)
    }

  def case_when_identifier: Parser[Identifier] = case_when ^^ { cw =>
    Identifier(cw)
  }

  def logical_functions: PackratParser[TransformFunction[_, _]] =
    is_null | is_notnull | coalesce | nullif | case_when

  private[this] def abs: PackratParser[MathOp] = Abs.regex ^^ (_ => Abs)

  private[this] def ceil: PackratParser[MathOp] = Ceil.regex ^^ (_ => Ceil)

  private[this] def floor: PackratParser[MathOp] = Floor.regex ^^ (_ => Floor)

  private[this] def exp: PackratParser[MathOp] = Exp.regex ^^ (_ => Exp)

  private[this] def sqrt: PackratParser[MathOp] = Sqrt.regex ^^ (_ => Sqrt)

  private[this] def log: PackratParser[MathOp] = Log.regex ^^ (_ => Log)

  private[this] def log10: PackratParser[MathOp] = Log10.regex ^^ (_ => Log10)

  implicit def functionAsIdentifier(mf: Function): Identifier = mf match {
    case id: Identifier              => id
    case fid: FunctionWithIdentifier => fid.identifier
    case _                           => Identifier(mf)
  }

  def arithmeticFunction: PackratParser[MathematicalFunction] =
    (abs | ceil | exp | floor | log | log10 | sqrt) ~ start ~ valueExpr ~ end ^^ {
      case op ~ _ ~ v ~ _ => MathematicalFunctionWithOp(op, v)
    }

  private[this] def sin: PackratParser[Trigonometric] = Sin.regex ^^ (_ => Sin)

  private[this] def asin: PackratParser[Trigonometric] = Asin.regex ^^ (_ => Asin)

  private[this] def cos: PackratParser[Trigonometric] = Cos.regex ^^ (_ => Cos)

  private[this] def acos: PackratParser[Trigonometric] = Acos.regex ^^ (_ => Acos)

  private[this] def tan: PackratParser[Trigonometric] = Tan.regex ^^ (_ => Tan)

  private[this] def atan: PackratParser[Trigonometric] = Atan.regex ^^ (_ => Atan)

  private[this] def atan2: PackratParser[Trigonometric] = Atan2.regex ^^ (_ => Atan2)

  def atan2Function: PackratParser[MathematicalFunction] =
    atan2 ~ start ~ (double | valueExpr) ~ separator ~ (double | valueExpr) ~ end ^^ {
      case _ ~ _ ~ y ~ _ ~ x ~ _ => Atan2(y, x)
    }

  def trigonometricFunction: PackratParser[MathematicalFunction] =
    atan2Function | ((sin | asin | cos | acos | tan | atan) ~ start ~ valueExpr ~ end ^^ {
      case op ~ _ ~ v ~ _ => MathematicalFunctionWithOp(op, v)
    })

  private[this] def round: PackratParser[MathOp] = Round.regex ^^ (_ => Round)

  def roundFunction: PackratParser[MathematicalFunction] =
    round ~ start ~ valueExpr ~ separator.? ~ long.? ~ end ^^ { case _ ~ _ ~ v ~ _ ~ s ~ _ =>
      Round(v, s.map(_.value.toInt))
    }

  private[this] def pow: PackratParser[MathOp] = Pow.regex ^^ (_ => Pow)

  def powFunction: PackratParser[MathematicalFunction] =
    pow ~ start ~ valueExpr ~ separator ~ long ~ end ^^ { case _ ~ _ ~ v1 ~ _ ~ e ~ _ =>
      Pow(v1, e.value.toInt)
    }

  private[this] def sign: PackratParser[MathOp] = Sign.regex ^^ (_ => Sign)

  def signFunction: PackratParser[MathematicalFunction] =
    sign ~ start ~ valueExpr ~ end ^^ { case _ ~ _ ~ v ~ _ => Sign(v) }

  def mathematicalFunction: PackratParser[MathematicalFunction] =
    arithmeticFunction | trigonometricFunction | roundFunction | powFunction | signFunction

  def mathematicalFunctionWithIdentifier: PackratParser[Identifier] =
    mathematicalFunction ^^ { mf =>
      mf.identifier
    }

  def concatFunction: PackratParser[StringFunction[SQLVarchar]] =
    Concat.regex ~ start ~ rep1sep(valueExpr, separator) ~ end ^^ { case _ ~ _ ~ vs ~ _ =>
      Concat(vs)
    }

  def substringFunction: PackratParser[StringFunction[SQLVarchar]] =
    Substring.regex ~ start ~ valueExpr ~ (From.regex | separator) ~ long ~ ((To.regex | separator) ~ long).? ~ end ^^ {
      case _ ~ _ ~ v ~ _ ~ s ~ eOpt ~ _ =>
        Substring(v, s.value.toInt, eOpt.map { case _ ~ e => e.value.toInt })
    }

  def stringFunctionWithIdentifier: PackratParser[Identifier] =
    (concatFunction | substringFunction) ^^ { sf =>
      sf.identifier
    }

  def length: PackratParser[StringFunction[SQLBigInt]] =
    Length.regex ^^ { _ =>
      SQLLength
    }

  def lower: PackratParser[StringFunction[SQLVarchar]] =
    Lower.regex ^^ { _ =>
      StringFunctionWithOp(Lower)
    }

  def upper: PackratParser[StringFunction[SQLVarchar]] =
    Upper.regex ^^ { _ =>
      StringFunctionWithOp(Upper)
    }

  def trim: PackratParser[StringFunction[SQLVarchar]] =
    Trim.regex ^^ { _ =>
      StringFunctionWithOp(Trim)
    }

  def string_functions: Parser[
    StringFunction[_]
  ] = /*concatFunction | substringFunction |*/ length | lower | upper | trim

  def sql_functions: PackratParser[Function] =
    aggregates | distance | date_diff | date_trunc | extractors | date_functions | datetime_functions | logical_functions | string_functions

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
    "char",
    "string",
    "byte",
    "tinyint",
    "short",
    "smallint",
    "int",
    "integer",
    "long",
    "bigint",
    "real",
    "float",
    "double",
    "pi",
    "boolean",
    "distance",
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
    "trim"
//    "ltrim",
//    "rtrim",
//    "replace",
  )

  private val identifierRegexStr =
    s"""(?i)(?!(?:${reservedKeywords.mkString(
      "|"
    )})\\b)[\\*a-zA-Z_\\-][a-zA-Z0-9_\\-.\\[\\]\\*]*"""

  private val identifierRegex = identifierRegexStr.r // scala.util.matching.Regex

  def identifier: PackratParser[Identifier] =
    Distinct.regex.? ~ identifierRegex ^^ { case d ~ i =>
      GenericIdentifier(
        i,
        None,
        d.isDefined
      )
    }

  def char_type: PackratParser[SQLTypes.Char.type] =
    "(?i)char".r ^^ (_ => SQLTypes.Char)

  def string_type: PackratParser[SQLTypes.Varchar.type] =
    "(?i)varchar|string".r ^^ (_ => SQLTypes.Varchar)

  def date_type: PackratParser[SQLTypes.Date.type] = "(?i)date".r ^^ (_ => SQLTypes.Date)

  def time_type: PackratParser[SQLTypes.Time.type] = "(?i)time".r ^^ (_ => SQLTypes.Time)

  def datetime_type: PackratParser[SQLTypes.DateTime.type] =
    "(?i)(datetime)".r ^^ (_ => SQLTypes.DateTime)

  def timestamp_type: PackratParser[SQLTypes.Timestamp.type] =
    "(?i)(timestamp)".r ^^ (_ => SQLTypes.Timestamp)

  def boolean_type: PackratParser[SQLTypes.Boolean.type] =
    "(?i)boolean".r ^^ (_ => SQLTypes.Boolean)

  def byte_type: PackratParser[SQLTypes.TinyInt.type] =
    "(?i)(byte|tinyint)".r ^^ (_ => SQLTypes.TinyInt)

  def short_type: PackratParser[SQLTypes.SmallInt.type] =
    "(?i)(short|smallint)".r ^^ (_ => SQLTypes.SmallInt)

  def int_type: PackratParser[SQLTypes.Int.type] = "(?i)(int|integer)".r ^^ (_ => SQLTypes.Int)

  def long_type: PackratParser[SQLTypes.BigInt.type] = "(?i)long|bigint".r ^^ (_ => SQLTypes.BigInt)

  def double_type: PackratParser[SQLTypes.Double.type] = "(?i)double".r ^^ (_ => SQLTypes.Double)

  def float_type: PackratParser[SQLTypes.Real.type] = "(?i)float|real".r ^^ (_ => SQLTypes.Real)

  def sql_type: PackratParser[SQLType] =
    char_type | string_type | datetime_type | timestamp_type | date_type | time_type | boolean_type | long_type | double_type | float_type | int_type | short_type | byte_type

  private[this] def castFunctionWithIdentifier: PackratParser[Identifier] =
    "(?i)cast".r ~ start ~ (identifierWithTransformation | identifierWithSystemFunction | identifierWithIntervalFunction | identifierWithFunction | date_diff_identifier | extract_identifier | identifier) ~ Alias.regex.? ~ sql_type ~ end ~ intervalFunction.? ^^ {
      case _ ~ _ ~ i ~ as ~ t ~ _ ~ a =>
        i.withFunctions(a.toList ++ (Cast(i, targetType = t, as = as.isDefined) +: i.functions))
    }

  private[this] def dateFunctionWithIdentifier: PackratParser[Identifier] =
    (parse_date | format_date | date_add | date_sub) ~ intervalFunction.? ^^ { case t ~ af =>
      af match {
        case Some(f) => t.identifier.withFunctions(f +: t +: t.identifier.functions)
        case None    => t.identifier.withFunctions(t +: t.identifier.functions)
      }
    }

  private[this] def dateTimeFunctionWithIdentifier: PackratParser[Identifier] =
    (date_trunc | parse_datetime | format_datetime | datetime_add | datetime_sub) ~ intervalFunction.? ^^ {
      case t ~ af =>
        af match {
          case Some(f) => t.identifier.withFunctions(f +: t +: t.identifier.functions)
          case None    => t.identifier.withFunctions(t +: t.identifier.functions)
        }
    }

  private[this] def conditionalFunctionWithIdentifier: PackratParser[Identifier] =
    (is_null | is_notnull | coalesce | nullif) ^^ { t =>
      t.identifier.withFunctions(t +: t.identifier.functions)
    }

  def identifierWithTransformation: PackratParser[Identifier] =
    mathematicalFunctionWithIdentifier | castFunctionWithIdentifier | conditionalFunctionWithIdentifier | dateFunctionWithIdentifier | dateTimeFunctionWithIdentifier | stringFunctionWithIdentifier

  def identifierWithAggregation: PackratParser[Identifier] =
    aggregates ~ start ~ (identifierWithFunction | identifierWithIntervalFunction | identifier) ~ end ^^ {
      case a ~ _ ~ i ~ _ =>
        i.withFunctions(a +: i.functions)
    }

  def identifierWithFunction: PackratParser[Identifier] =
    rep1sep(
      sql_functions,
      start
    ) ~ start.? ~ (identifierWithSystemFunction | identifierWithIntervalFunction | identifier).? ~ rep1(
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
    }

  private val regexAlias =
    """\b(?!(?i)as\b)\b(?!(?i)except\b)\b(?!(?i)where\b)\b(?!(?i)filter\b)\b(?!(?i)from\b)\b(?!(?i)group\b)\b(?!(?i)having\b)\b(?!(?i)order\b)\b(?!(?i)limit\b)[a-zA-Z0-9_]*"""

  def alias: PackratParser[Alias] = Alias.regex.? ~ regexAlias.r ^^ { case _ ~ b => Alias(b) }

  def field: PackratParser[Field] =
    (identifierWithArithmeticExpression | identifierWithTransformation | identifierWithAggregation | identifierWithSystemFunction | identifierWithIntervalFunction | identifierWithFunction | date_diff_identifier | extract_identifier | case_when_identifier | identifier) ~ alias.? ^^ {
      case i ~ a =>
        Field(i, a)
    }

}

trait SelectParser {
  self: Parser with WhereParser =>

  def except: PackratParser[Except] = Except.regex ~ start ~ rep1sep(field, separator) ~ end ^^ {
    case _ ~ _ ~ e ~ _ =>
      Except(e)
  }

  def select: PackratParser[Select] =
    Select.regex ~ rep1sep(
      field,
      separator
    ) ~ except.? ^^ { case _ ~ fields ~ e =>
      Select(fields, e)
    }

}

trait FromParser {
  self: Parser with LimitParser =>

  def unnest: PackratParser[Table] =
    Unnest.regex ~ start ~ identifier ~ limit.? ~ end ~ alias ^^ { case _ ~ _ ~ i ~ l ~ _ ~ a =>
      Table(Unnest(i, l), Some(a))
    }

  def table: PackratParser[Table] = identifier ~ alias.? ^^ { case i ~ a => Table(i, a) }

  def from: PackratParser[From] = From.regex ~ rep1sep(unnest | table, separator) ^^ {
    case _ ~ tables =>
      From(tables)
  }

}

trait WhereParser {
  self: Parser with GroupByParser with OrderByParser =>

  def isNull: PackratParser[Criteria] = identifier ~ IsNull.regex ^^ { case i ~ _ =>
    IsNullExpr(i)
  }

  def isNotNull: PackratParser[Criteria] = identifier ~ IsNotNull.regex ^^ { case i ~ _ =>
    IsNotNullExpr(i)
  }

  private def eq: PackratParser[ComparisonOperator] = Eq.sql ^^ (_ => Eq)

  private def ne: PackratParser[ComparisonOperator] = Ne.sql ^^ (_ => Ne)

  private def diff: PackratParser[ComparisonOperator] = Diff.sql ^^ (_ => Diff)

  private def any_identifier: PackratParser[Identifier] =
    identifierWithTransformation | identifierWithAggregation | identifierWithSystemFunction | identifierWithIntervalFunction | identifierWithArithmeticExpression | identifierWithFunction | date_diff_identifier | extract_identifier | identifier

  private def equality: PackratParser[GenericExpression] =
    not.? ~ any_identifier ~ (eq | ne | diff) ~ (boolean | literal | double | pi | long | any_identifier) ^^ {
      case n ~ i ~ o ~ v => GenericExpression(i, o, v, n)
    }

  def like: PackratParser[GenericExpression] =
    any_identifier ~ not.? ~ Like.regex ~ literal ^^ { case i ~ n ~ _ ~ v =>
      GenericExpression(i, Like, v, n)
    }

  private def ge: PackratParser[ComparisonOperator] = Ge.sql ^^ (_ => Ge)

  def gt: PackratParser[ComparisonOperator] = Gt.sql ^^ (_ => Gt)

  private def le: PackratParser[ComparisonOperator] = Le.sql ^^ (_ => Le)

  def lt: PackratParser[ComparisonOperator] = Lt.sql ^^ (_ => Lt)

  private def comparison: PackratParser[GenericExpression] =
    not.? ~ any_identifier ~ (ge | gt | le | lt) ~ (double | pi | long | literal | any_identifier) ^^ {
      case n ~ i ~ o ~ v => GenericExpression(i, o, v, n)
    }

  def in: PackratParser[ExpressionOperator] = In.regex ^^ (_ => In)

  private def inLiteral: PackratParser[Criteria] =
    any_identifier ~ not.? ~ in ~ start ~ rep1sep(literal, separator) ~ end ^^ {
      case i ~ n ~ _ ~ _ ~ v ~ _ =>
        InExpr(
          i,
          StringValues(v),
          n
        )
    }

  private def inDoubles: PackratParser[Criteria] =
    any_identifier ~ not.? ~ in ~ start ~ rep1sep(
      double,
      separator
    ) ~ end ^^ { case i ~ n ~ _ ~ _ ~ v ~ _ =>
      InExpr(
        i,
        DoubleValues(v),
        n
      )
    }

  private def inLongs: PackratParser[Criteria] =
    any_identifier ~ not.? ~ in ~ start ~ rep1sep(
      long,
      separator
    ) ~ end ^^ { case i ~ n ~ _ ~ _ ~ v ~ _ =>
      InExpr(
        i,
        LongValues(v),
        n
      )
    }

  def between: PackratParser[Criteria] =
    any_identifier ~ not.? ~ Between.regex ~ literal ~ and ~ literal ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => BetweenExpr(i, LiteralFromTo(from, to), n)
    }

  def betweenLongs: PackratParser[Criteria] =
    any_identifier ~ not.? ~ Between.regex ~ long ~ and ~ long ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => BetweenExpr(i, LongFromTo(from, to), n)
    }

  def betweenDoubles: PackratParser[Criteria] =
    any_identifier ~ not.? ~ Between.regex ~ double ~ and ~ double ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => BetweenExpr(i, DoubleFromTo(from, to), n)
    }

  def sql_distance: PackratParser[Criteria] =
    distance ~ start ~ identifier ~ separator ~ start ~ double ~ separator ~ double ~ end ~ end ~ le ~ literal ^^ {
      case _ ~ _ ~ i ~ _ ~ _ ~ lat ~ _ ~ lon ~ _ ~ _ ~ _ ~ d => ElasticGeoDistance(i, d, lat, lon)
    }

  def matchCriteria: PackratParser[MatchCriteria] =
    Match.regex ~ start ~ rep1sep(
      any_identifier,
      separator
    ) ~ end ~ Against.regex ~ start ~ literal ~ end ^^ { case _ ~ _ ~ i ~ _ ~ _ ~ _ ~ l ~ _ =>
      MatchCriteria(i, l)
    }

  def and: PackratParser[PredicateOperator] = And.regex ^^ (_ => And)

  def or: PackratParser[PredicateOperator] = Or.regex ^^ (_ => Or)

  def not: PackratParser[Not.type] = Not.regex ^^ (_ => Not)

  def logical_criteria: PackratParser[Criteria] =
    (is_null | is_notnull) ^^ { case ConditionalFunctionAsCriteria(c) =>
      c
    }

  def criteria: PackratParser[Criteria] =
    (equality | like | comparison | inLiteral | inLongs | inDoubles | between | betweenLongs | betweenDoubles | isNotNull | isNull | /*coalesce | nullif |*/ sql_distance | matchCriteria | logical_criteria) ^^ (
      c => c
    )

  def predicate: PackratParser[Predicate] = criteria ~ (and | or) ~ not.? ~ criteria ^^ {
    case l ~ o ~ n ~ r => Predicate(l, o, r, n)
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

  private def allPredicate: PackratParser[Criteria] =
    nestedPredicate | childPredicate | parentPredicate | predicate

  private def allCriteria: PackratParser[Token] =
    nestedCriteria | childCriteria | parentCriteria | criteria

  def whereCriteria: PackratParser[List[Token]] = rep1(
    allPredicate | allCriteria | start | or | and | end | then_case
  )

  def where: PackratParser[Where] =
    Where.regex ~ whereCriteria ^^ { case _ ~ rawTokens =>
      Where(processTokens(rawTokens))
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
    tokens: List[Token],
    stack: List[Token]
  ): Option[Criteria] = {
    tokens match {
      case Nil =>
        stack match {
          case (right: Criteria) :: (op: PredicateOperator) :: (left: Criteria) :: Nil =>
            Option(
              Predicate(left, op, right)
            )
          case _ =>
            stack.headOption.collect { case c: Criteria => c }
        }
      case (_: StartDelimiter) :: rest =>
        val (subTokens, remainingTokens) = extractSubTokens(rest, 1)
        val subCriteria = processSubTokens(subTokens) match {
          case p: Predicate => p.copy(group = true)
          case c            => c
        }
        processTokensHelper(remainingTokens, subCriteria :: stack)
      case (c: Criteria) :: rest =>
        stack match {
          case (op: PredicateOperator) :: (left: Criteria) :: tail =>
            val predicate = Predicate(left, op, c)
            processTokensHelper(rest, predicate :: tail)
          case _ =>
            processTokensHelper(rest, c :: stack)
        }
      case (op: PredicateOperator) :: rest =>
        stack match {
          case (right: Criteria) :: (left: Criteria) :: tail =>
            val predicate = Predicate(left, op, right)
            processTokensHelper(rest, predicate :: tail)
          case (right: Criteria) :: (o: PredicateOperator) :: tail =>
            tail match {
              case (left: Criteria) :: tt =>
                val predicate = Predicate(left, op, right)
                processTokensHelper(rest, o :: predicate :: tt)
              case _ =>
                processTokensHelper(rest, op :: stack)
            }
          case _ :: Nil =>
            processTokensHelper(rest, op :: stack)
          case _ =>
            throw ValidationError("Invalid stack state for predicate creation")
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
    tokens: List[Token]
  ): Option[Criteria] = {
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
  private def processSubTokens(tokens: List[Token]): Criteria = {
    processTokensHelper(tokens, Nil).getOrElse(
      throw ValidationError("Empty sub-expression")
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
    tokens: List[Token],
    openCount: Int,
    subTokens: List[Token] = Nil
  ): (List[Token], List[Token]) = {
    tokens match {
      case Nil => throw ValidationError("Unbalanced parentheses")
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

trait GroupByParser {
  self: Parser with WhereParser =>

  def bucket: PackratParser[Bucket] = identifier ^^ { i =>
    Bucket(i)
  }

  def groupBy: PackratParser[GroupBy] =
    GroupBy.regex ~ rep1sep(bucket, separator) ^^ { case _ ~ buckets =>
      GroupBy(buckets)
    }

}

trait HavingParser {
  self: Parser with WhereParser =>

  def having: PackratParser[Having] = Having.regex ~> whereCriteria ^^ { rawTokens =>
    Having(
      processTokens(rawTokens)
    )
  }

}

trait OrderByParser {
  self: Parser =>

  def asc: PackratParser[Asc.type] = Asc.regex ^^ (_ => Asc)

  def desc: PackratParser[Desc.type] = Desc.regex ^^ (_ => Desc)

  private def fieldName: PackratParser[String] =
    """\b(?!(?i)limit\b)[a-zA-Z_][a-zA-Z0-9_]*""".r ^^ (f => f)

  def fieldWithFunction: PackratParser[(String, List[Function])] =
    rep1sep(sql_functions, start) ~ start.? ~ fieldName ~ rep1(end) ^^ { case f ~ _ ~ n ~ _ =>
      (n, f)
    }

  def sort: PackratParser[FieldSort] =
    (fieldWithFunction | fieldName) ~ (asc | desc).? ^^ { case f ~ o =>
      f match {
        case i: (String, List[Function]) => FieldSort(i._1, o, i._2)
        case s: String                   => FieldSort(s, o, List.empty)
      }
    }

  def orderBy: PackratParser[OrderBy] = OrderBy.regex ~ rep1sep(sort, separator) ^^ { case _ ~ s =>
    OrderBy(s)
  }

}

trait LimitParser {
  self: Parser =>

  def limit: PackratParser[Limit] = Limit.regex ~ long ^^ { case _ ~ i =>
    Limit(i.value.toInt)
  }

}
