package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.{Identifier, StringValue}
import app.softnetwork.elastic.sql.`type`.{SQLLiteral, SQLNumeric, SQLTemporal}
import app.softnetwork.elastic.sql.function.{
  BinaryFunction,
  FunctionWithIdentifier,
  TransformFunction
}
import app.softnetwork.elastic.sql.function.time.{
  CurentDateWithParens,
  CurrentDate,
  CurrentFunction,
  CurrentTime,
  CurrentTimeWithParens,
  CurrentTimestamp,
  CurrentTimestampWithParens,
  DAY,
  DateAdd,
  DateDiff,
  DateFormat,
  DateFunction,
  DateParse,
  DateSub,
  DateTimeAdd,
  DateTimeFormat,
  DateTimeFunction,
  DateTimeParse,
  DateTimeSub,
  DateTrunc,
  Extract,
  HOUR,
  MINUTE,
  MONTH,
  Now,
  NowWithParens,
  SECOND,
  YEAR
}
import app.softnetwork.elastic.sql.parser.time.TimeParser
import app.softnetwork.elastic.sql.parser.{Delimiter, Parser}
import app.softnetwork.elastic.sql.time.TimeUnit.{Day, Hour, Minute, Month, Second, Year}

package object time {

  trait SystemParser { self: Parser with TimeParser =>

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

    def identifierWithSystemFunction: PackratParser[Identifier] =
      (current_date | current_time | current_timestamp | now) ~ intervalFunction.? ^^ {
        case f1 ~ f2 =>
          f2 match {
            case Some(f) => Identifier(List(f, f1))
            case None    => Identifier(f1)
          }
      }

  }

  trait DateParser { self: Parser with TemporalParser =>

    def date_add: PackratParser[DateFunction with FunctionWithIdentifier] =
      DateAdd.regex ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateAdd(i, t)
      }

    def date_sub: PackratParser[DateFunction with FunctionWithIdentifier] =
      DateSub.regex ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateSub(i, t)
      }

    def date_parse: PackratParser[DateFunction with FunctionWithIdentifier] =
      DateParse.regex ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | literal | identifier) ~ separator ~ literal ~ end ^^ {
        case _ ~ _ ~ li ~ _ ~ f ~ _ =>
          li match {
            case l: StringValue =>
              DateParse(Identifier(l), f.value)
            case i: Identifier =>
              DateParse(i, f.value)
          }
      }

    def date_format: PackratParser[DateFunction with FunctionWithIdentifier] =
      DateFormat.regex ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ literal ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ f ~ _ =>
          DateFormat(i, f.value)
      }

    def date_functions: PackratParser[DateFunction] = date_add | date_sub | date_parse | date_format

    def dateFunctionWithIdentifier: PackratParser[Identifier] =
      (date_parse | date_format | date_add | date_sub) ~ intervalFunction.? ^^ { case t ~ af =>
        af match {
          case Some(f) => t.identifier.withFunctions(f +: t +: t.identifier.functions)
          case None    => t.identifier.withFunctions(t +: t.identifier.functions)
        }
      }

  }

  trait DateTimeParser { self: Parser with TemporalParser =>

    def datetime_add: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
      DateTimeAdd.regex ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateTimeAdd(i, t)
      }

    def datetime_sub: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
      DateTimeSub.regex ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateTimeSub(i, t)
      }

    def datetime_parse: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
      DateTimeParse.regex ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | literal | identifier) ~ separator ~ literal ~ end ^^ {
        case _ ~ _ ~ li ~ _ ~ f ~ _ =>
          li match {
            case l: SQLLiteral =>
              DateTimeParse(Identifier(l), f.value)
            case i: Identifier =>
              DateTimeParse(i, f.value)
          }
      }

    def datetime_format: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
      DateTimeFormat.regex ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ literal ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ f ~ _ =>
          DateTimeFormat(i, f.value)
      }

    def datetime_functions: PackratParser[DateTimeFunction] =
      datetime_add | datetime_sub | datetime_parse | datetime_format

    def dateTimeFunctionWithIdentifier: PackratParser[Identifier] =
      (date_trunc | datetime_parse | datetime_format | datetime_add | datetime_sub) ~ intervalFunction.? ^^ {
        case t ~ af =>
          af match {
            case Some(f) => t.identifier.withFunctions(f +: t +: t.identifier.functions)
            case None    => t.identifier.withFunctions(t +: t.identifier.functions)
          }
      }

  }

  trait TemporalParser extends SystemParser with TimeParser with DateParser with DateTimeParser {
    self: Parser =>

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
      DateDiff.regex ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ time_unit ~ end ^^ {
        case _ ~ _ ~ d1 ~ _ ~ d2 ~ _ ~ u ~ _ => DateDiff(d1, d2, u)
      }

    def date_diff_identifier: PackratParser[Identifier] = date_diff ^^ { dd =>
      Identifier(dd)
    }

    def date_trunc: PackratParser[FunctionWithIdentifier] =
      DateTrunc.regex ~ start ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ separator ~ time_unit ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ u ~ _ =>
          DateTrunc(i, u)
      }

    def extract_identifier: PackratParser[Identifier] =
      Extract.regex ~ start ~ time_unit ~ "(?i)from".r ~ (identifierWithTemporalFunction | identifierWithSystemFunction | identifierWithIntervalFunction | identifier) ~ end ^^ {
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

  }

}
