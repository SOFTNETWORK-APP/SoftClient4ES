package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.{Identifier, StringValue}
import app.softnetwork.elastic.sql.`type`.{SQLLiteral, SQLNumeric, SQLTemporal}
import app.softnetwork.elastic.sql.function.{
  BinaryFunction,
  FunctionWithIdentifier,
  TransformFunction
}
import app.softnetwork.elastic.sql.function.time._
import app.softnetwork.elastic.sql.parser.time.TimeParser
import app.softnetwork.elastic.sql.parser.{Delimiter, Parser}
import app.softnetwork.elastic.sql.time.{IsoField, TimeField}

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

    def today: PackratParser[CurrentFunction] = Today.regex ~ parens.? ^^ { case _ ~ p =>
      if (p.isDefined) TodayWithParens else Today
    }

    def systemFunctions: PackratParser[CurrentFunction] =
      current_date | current_time | current_timestamp | now | today

    def systemFunctionWithIdentifier: PackratParser[Identifier] =
      systemFunctions ~ intervalFunction.? ^^ { case f1 ~ f2 =>
        f2 match {
          case Some(f) => Identifier(List(f, f1))
          case None    => Identifier(f1)
        }
      }

  }

  trait DateParser { self: Parser with TemporalParser =>

    def date_add: PackratParser[DateFunction with FunctionWithIdentifier] =
      DateAdd.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateAdd(i, t)
      }

    def date_sub: PackratParser[DateFunction with FunctionWithIdentifier] =
      DateSub.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateSub(i, t)
      }

    def date_parse: PackratParser[DateFunction with FunctionWithIdentifier] =
      DateParse.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | literal | identifier) ~ separator ~ literal ~ end ^^ {
        case _ ~ _ ~ li ~ _ ~ f ~ _ =>
          li match {
            case l: StringValue =>
              DateParse(Identifier(l), f.value)
            case i: Identifier =>
              DateParse(i, f.value)
          }
      }

    def date_format: PackratParser[DateFunction with FunctionWithIdentifier] =
      DateFormat.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ literal ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ f ~ _ =>
          DateFormat(i, f.value)
      }

    def last_day: Parser[DateFunction with FunctionWithIdentifier] =
      LastDayOfMonth.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ end ^^ {
        case _ ~ _ ~ i ~ _ => LastDayOfMonth(i)
      }

    def date_functions: PackratParser[DateFunction] =
      date_add | date_sub | date_parse | date_format | last_day

    def dateFunctionWithIdentifier: PackratParser[Identifier] =
      (date_parse | date_format | date_add | date_sub | last_day) ~ intervalFunction.? ^^ {
        case t ~ af =>
          af match {
            case Some(f) => t.identifier.withFunctions(f +: t +: t.identifier.functions)
            case None    => t.identifier.withFunctions(t +: t.identifier.functions)
          }
      }

  }

  trait DateTimeParser { self: Parser with TemporalParser =>

    def datetime_add: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
      DateTimeAdd.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateTimeAdd(i, t)
      }

    def datetime_sub: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
      DateTimeSub.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateTimeSub(i, t)
      }

    def datetime_parse: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
      DateTimeParse.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | literal | identifier) ~ separator ~ literal ~ end ^^ {
        case _ ~ _ ~ li ~ _ ~ f ~ _ =>
          li match {
            case l: SQLLiteral =>
              DateTimeParse(Identifier(l), f.value)
            case i: Identifier =>
              DateTimeParse(i, f.value)
          }
      }

    def datetime_format: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
      DateTimeFormat.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ literal ~ end ^^ {
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

    def date_diff: PackratParser[BinaryFunction[_, _, _]] =
      DateDiff.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ time_unit ~ end ^^ {
        case _ ~ _ ~ d1 ~ _ ~ d2 ~ _ ~ u ~ _ => DateDiff(d1, d2, u)
      }

    def date_diff_identifier: PackratParser[Identifier] = date_diff ^^ { dd =>
      Identifier(dd)
    }

    def date_trunc: PackratParser[FunctionWithIdentifier] =
      DateTrunc.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ time_unit ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ u ~ _ =>
          DateTrunc(i, u)
      }

    def extract_identifier: PackratParser[Identifier] =
      Extract.regex ~ start ~ time_field ~ "(?i)from".r ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ end ^^ {
        case _ ~ _ ~ u ~ _ ~ i ~ _ =>
          i.withFunctions(Extract(u) +: i.functions)
      }

    import TimeField._

    def year_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      YEAR.regex ^^ (_ => Year)
    def month_of_year_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      MONTH_OF_YEAR.regex ^^ (_ => MonthOfYear)
    def day_of_month_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      DAY_OF_MONTH.regex ^^ (_ => DayOfMonth)
    def day_of_week_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      DAY_OF_WEEK.regex ^^ (_ => DayOfWeek)
    def day_of_year_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      DAY_OF_YEAR.regex ^^ (_ => DayOfYear)
    def hour_of_day_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      HOUR_OF_DAY.regex ^^ (_ => HourOfDay)
    def minute_of_hour_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      MINUTE_OF_HOUR.regex ^^ (_ => MinuteOfHour)
    def second_of_minute_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      SECOND_OF_MINUTE.regex ^^ (_ => SecondOfMinute)
    def nano_of_second_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      NANO_OF_SECOND.regex ^^ (_ => NanoOfSecond)
    def micro_of_second_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      MICRO_OF_SECOND.regex ^^ (_ => MicroOfSecond)
    def milli_of_second_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      MILLI_OF_SECOND.regex ^^ (_ => MilliOfSecond)
    def epoch_day_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      EPOCH_DAY.regex ^^ (_ => EpochDay)
    def offset_seconds_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      OFFSET_SECONDS.regex ^^ (_ => OffsetSeconds)

    def quarter_of_year_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      IsoField.QUARTER_OF_YEAR.regex ^^ (_ => QuarterOfYear)

    def week_of_week_based_year_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      IsoField.WEEK_OF_WEEK_BASED_YEAR.regex ^^ (_ => WeekOfWeekBasedYear)

    def extractors: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      year_tr |
      month_of_year_tr |
      day_of_month_tr |
      day_of_week_tr |
      day_of_year_tr |
      hour_of_day_tr |
      minute_of_hour_tr |
      second_of_minute_tr |
      milli_of_second_tr |
      micro_of_second_tr |
      nano_of_second_tr |
      epoch_day_tr |
      offset_seconds_tr |
      quarter_of_year_tr |
      week_of_week_based_year_tr

  }

}
