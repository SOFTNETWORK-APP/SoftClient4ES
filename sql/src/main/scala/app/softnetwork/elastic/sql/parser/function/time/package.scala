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

package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.{function, DateMathScript, Identifier, StringValue}
import app.softnetwork.elastic.sql.`type`.{SQLLiteral, SQLNumeric, SQLTemporal, SQLTypes}
import app.softnetwork.elastic.sql.function.{
  BinaryFunction,
  FunctionWithIdentifier,
  TransformFunction
}
import app.softnetwork.elastic.sql.function.time._
import app.softnetwork.elastic.sql.parser.time.TimeParser
import app.softnetwork.elastic.sql.parser.{Delimiter, Parser}
import app.softnetwork.elastic.sql.time.{IsoField, TimeField, TimeInterval, TimeUnit}

package object time {

  trait CurrentParser { self: Parser with TimeParser =>

    def parens: PackratParser[List[Delimiter]] =
      start ~ end ^^ { case s ~ e => s :: e :: Nil }

    def current_date: PackratParser[Identifier] =
      CurrentDate.regex ~ parens.? ^^ { case _ ~ p =>
        Identifier(CurrentDate(p.isDefined))
      }

    def current_time: PackratParser[Identifier] =
      CurrentTime.regex ~ parens.? ^^ { case _ ~ p =>
        Identifier(CurrentTime(p.isDefined))
      }

    def current_timestamp: PackratParser[Identifier] =
      CurrentTimestamp.regex ~ parens.? ^^ { case _ ~ p =>
        Identifier(CurrentTimestamp(p.isDefined))
      }

    def now: PackratParser[Identifier] = Now.regex ~ parens.? ^^ { case _ ~ p =>
      Identifier(Now(p.isDefined))
    }

    def today: PackratParser[Identifier] = Today.regex ~ parens.? ^^ { case _ ~ p =>
      Identifier(Today(p.isDefined))
    }

    private[this] def current_function: PackratParser[Identifier] =
      current_date | current_time | current_timestamp | now | today

    def currentFunctionWithIdentifier: PackratParser[Identifier] =
      current_function ^^ functionAsIdentifier

  }

  trait DateParser { self: Parser with TemporalParser =>

    def date_add: PackratParser[DateFunction with FunctionWithIdentifier with DateMathScript] =
      DateAdd.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateAdd(i, t)
      }

    def date_add_transact_sql
      : PackratParser[DateFunction with FunctionWithIdentifier with DateMathScript] =
      DateAdd.regex ~ start ~> time_unit ~ separator ~
      long ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) <~ end ^^ {
        case u ~ _ ~ l ~ _ ~ i =>
          DateAdd(i, TimeInterval(l.value.toInt, u), transactSql = true)
      }

    def date_sub: PackratParser[DateFunction with FunctionWithIdentifier with DateMathScript] =
      DateSub.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateSub(i, t)
      }

    def date_sub_transact_sql
      : PackratParser[DateFunction with FunctionWithIdentifier with DateMathScript] =
      DateSub.regex ~ start ~> time_unit ~ separator ~ long ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) <~ end ^^ {
        case u ~ _ ~ l ~ _ ~ i =>
          DateSub(i, TimeInterval(l.value.toInt, u), transactSql = true)
      }

    def date_parse: PackratParser[DateFunction with FunctionWithIdentifier with DateMathScript] =
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
        case _ ~ _ ~ i ~ _ =>
          i.cast(SQLTypes.Date)
          LastDayOfMonth(i)
      }

    def date_function: PackratParser[DateFunction with FunctionWithIdentifier] =
      date_add |
      date_add_transact_sql |
      date_sub |
      date_sub_transact_sql |
      date_parse |
      date_format |
      last_day

    def dateFunctionWithIdentifier: PackratParser[Identifier] =
      date_function ^^ (t => t.identifier.withFunctions(t +: t.identifier.functions))

  }

  trait DateTimeParser { self: Parser with TemporalParser =>

    def datetime_add
      : PackratParser[DateTimeFunction with FunctionWithIdentifier with DateMathScript] =
      DateTimeAdd.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateTimeAdd(i, t)
      }

    def datetime_add_transact_sql
      : PackratParser[DateTimeFunction with FunctionWithIdentifier with DateMathScript] =
      DateTimeAdd.regex ~ start ~> time_unit ~ separator ~
      long ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) <~ end ^^ {
        case u ~ _ ~ l ~ _ ~ i =>
          DateTimeAdd(i, TimeInterval(l.value.toInt, u), transactSql = true)
      }

    def datetime_sub
      : PackratParser[DateTimeFunction with FunctionWithIdentifier with DateMathScript] =
      DateTimeSub.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateTimeSub(i, t)
      }

    def datetime_sub_transact_sql
      : PackratParser[DateTimeFunction with FunctionWithIdentifier with DateMathScript] =
      DateTimeSub.regex ~ start ~> time_unit ~ separator ~ long ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) <~ end ^^ {
        case u ~ _ ~ l ~ _ ~ i =>
          DateTimeSub(i, TimeInterval(l.value.toInt, u), transactSql = true)
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

    def datetime_function: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
      datetime_add |
      datetime_add_transact_sql |
      datetime_sub |
      datetime_sub_transact_sql |
      datetime_parse |
      datetime_format

    def dateTimeFunctionWithIdentifier: PackratParser[Identifier] =
      datetime_function ^^ { t =>
        t.identifier.withFunctions(t +: t.identifier.functions)
      }

  }

  trait TemporalParser extends CurrentParser with TimeParser with DateParser with DateTimeParser {
    self: Parser =>

    def date_diff: PackratParser[BinaryFunction[_, _, _]] =
      DateDiff.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ (separator ~ time_unit).? ~ end ^^ {
        case _ ~ _ ~ d1 ~ _ ~ d2 ~ u ~ _ =>
          DateDiff(
            d1,
            d2,
            u match {
              case Some(_ ~ unit) => unit
              case None           => TimeUnit.DAYS
            }
          )
      }

    def date_diff_transact_sql: PackratParser[BinaryFunction[_, _, _]] =
      DateDiff.regex ~ start ~> time_unit ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) <~ end ^^ {
        case u ~ _ ~ d1 ~ _ ~ d2 =>
          DateDiff(d1, d2, u, transactSql = true)
      }

    def date_diff_identifier: PackratParser[Identifier] = (date_diff | date_diff_transact_sql) ^^ {
      dd =>
        Identifier(dd)
    }

    def date_trunc: PackratParser[FunctionWithIdentifier] =
      DateTrunc.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ time_unit ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ u ~ _ =>
          DateTrunc(i, u)
      }

    def date_trunc_transact_sql: PackratParser[FunctionWithIdentifier] =
      DateTrunc.regex ~ start ~> time_unit ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) <~ end ^^ {
        case u ~ _ ~ i =>
          DateTrunc(i, u, transactSql = true)
      }

    def date_trunc_identifier: PackratParser[Identifier] =
      (date_trunc | date_trunc_transact_sql) ^^ { dt =>
        dt.identifier.withFunctions(dt +: dt.identifier.functions)
      }

    def extract_identifier: PackratParser[Identifier] =
      Extract.regex ~ start ~ time_field ~ "(?i)from".r ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ end ^^ {
        case _ ~ _ ~ u ~ _ ~ i ~ _ =>
          i.withFunctions(Extract(u) +: i.functions)
      }

    import TimeField._

    def day_of_week_tr: PackratParser[FunctionWithIdentifier] =
      DAY_OF_WEEK.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ end ^^ {
        case _ ~ _ ~ i ~ _ => new DayOfWeek(i)
      }

    def day_of_week_identifier: PackratParser[Identifier] = day_of_week_tr ^^ { dw =>
      dw.identifier.withFunctions(dw +: dw.identifier.functions)
    }

    def year_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      YEAR.regex ^^ (_ => new Year)
    def month_of_year_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      MONTH_OF_YEAR.regex ^^ (_ => new MonthOfYear)
    def day_of_month_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      DAY_OF_MONTH.regex ^^ (_ => new DayOfMonth)
    def day_of_year_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      DAY_OF_YEAR.regex ^^ (_ => new DayOfYear)
    def hour_of_day_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      HOUR_OF_DAY.regex ^^ (_ => new HourOfDay)
    def minute_of_hour_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      MINUTE_OF_HOUR.regex ^^ (_ => new MinuteOfHour)
    def second_of_minute_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      SECOND_OF_MINUTE.regex ^^ (_ => new SecondOfMinute)
    def nano_of_second_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      NANO_OF_SECOND.regex ^^ (_ => new NanoOfSecond)
    def micro_of_second_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      MICRO_OF_SECOND.regex ^^ (_ => new MicroOfSecond)
    def milli_of_second_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      MILLI_OF_SECOND.regex ^^ (_ => new MilliOfSecond)
    def epoch_day_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      EPOCH_DAY.regex ^^ (_ => new EpochDay)
    def offset_seconds_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      OFFSET_SECONDS.regex ^^ (_ => new OffsetSeconds)

    def quarter_of_year_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      IsoField.QUARTER_OF_YEAR.regex ^^ (_ => new QuarterOfYear)

    def week_of_week_based_year_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      IsoField.WEEK_OF_WEEK_BASED_YEAR.regex ^^ (_ => new WeekOfWeekBasedYear)

    def extractor_function: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      year_tr |
      month_of_year_tr |
      day_of_month_tr |
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

    def time_function: Parser[function.Function] =
      date_function |
      datetime_function |
      date_diff |
      date_diff_transact_sql |
      date_trunc |
      date_trunc_transact_sql |
      extractor_function

    def timeFunctionWithIdentifier: Parser[Identifier] =
      (currentFunctionWithIdentifier |
      dateFunctionWithIdentifier |
      dateTimeFunctionWithIdentifier |
      date_diff_identifier |
      date_trunc_identifier |
      day_of_week_identifier |
      extract_identifier) ~ rep(intervalFunction) ^^ { case i ~ f =>
        i.withFunctions(f ++ i.functions)
      }
  }

}
