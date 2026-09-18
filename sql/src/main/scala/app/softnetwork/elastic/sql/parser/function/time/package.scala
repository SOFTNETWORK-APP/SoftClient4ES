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

    lazy val parens: PackratParser[List[Delimiter]] =
      start ~ end ^^ { case s ~ e => s :: e :: Nil }

    lazy val current_date: PackratParser[Identifier] =
      CurrentDate.regex ~ parens.? ^^ { case _ ~ p =>
        Identifier(CurrentDate(p.isDefined))
      }

    lazy val current_time: PackratParser[Identifier] =
      CurrentTime.regex ~ parens.? ^^ { case _ ~ p =>
        Identifier(CurrentTime(p.isDefined))
      }

    lazy val current_timestamp: PackratParser[Identifier] =
      CurrentTimestamp.regex ~ parens.? ^^ { case _ ~ p =>
        Identifier(CurrentTimestamp(p.isDefined))
      }

    lazy val now: PackratParser[Identifier] = Now.regex ~ parens.? ^^ { case _ ~ p =>
      Identifier(Now(p.isDefined))
    }

    lazy val today: PackratParser[Identifier] = Today.regex ~ parens.? ^^ { case _ ~ p =>
      Identifier(Today(p.isDefined))
    }

    private[this] lazy val current_function: PackratParser[Identifier] =
      current_date | current_time | current_timestamp | now | today

    lazy val currentFunctionWithIdentifier: PackratParser[Identifier] =
      current_function ^^ functionAsIdentifier

  }

  trait DateParser { self: Parser with TemporalParser =>

    lazy val date_add: PackratParser[DateFunction with FunctionWithIdentifier with DateMathScript] =
      DateAdd.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateAdd(i, t)
      }

    lazy val date_add_transact_sql
      : PackratParser[DateFunction with FunctionWithIdentifier with DateMathScript] =
      DateAdd.regex ~ start ~> time_unit ~ separator ~
      long ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) <~ end ^^ {
        case u ~ _ ~ l ~ _ ~ i =>
          DateAdd(i, TimeInterval(l.value.toInt, u), transactSql = true)
      }

    lazy val date_sub: PackratParser[DateFunction with FunctionWithIdentifier with DateMathScript] =
      DateSub.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateSub(i, t)
      }

    lazy val date_sub_transact_sql
      : PackratParser[DateFunction with FunctionWithIdentifier with DateMathScript] =
      DateSub.regex ~ start ~> time_unit ~ separator ~ long ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) <~ end ^^ {
        case u ~ _ ~ l ~ _ ~ i =>
          DateSub(i, TimeInterval(l.value.toInt, u), transactSql = true)
      }

    lazy val date_parse
      : PackratParser[DateFunction with FunctionWithIdentifier with DateMathScript] =
      DateParse.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | literal | identifier) ~ separator ~ literal ~ end ^^ {
        case _ ~ _ ~ li ~ _ ~ f ~ _ =>
          li match {
            case l: StringValue =>
              DateParse(Identifier(l), f.value)
            case i: Identifier =>
              DateParse(i, f.value)
          }
      }

    lazy val date_format: PackratParser[DateFunction with FunctionWithIdentifier] =
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

    lazy val date_function: PackratParser[DateFunction with FunctionWithIdentifier] =
      date_add |
      date_add_transact_sql |
      date_sub |
      date_sub_transact_sql |
      date_parse |
      date_format |
      last_day

    lazy val dateFunctionWithIdentifier: PackratParser[Identifier] =
      date_function ^^ (t => t.identifier.withFunctions(t +: t.identifier.functions))

  }

  trait DateTimeParser { self: Parser with TemporalParser =>

    lazy val datetime_add
      : PackratParser[DateTimeFunction with FunctionWithIdentifier with DateMathScript] =
      DateTimeAdd.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateTimeAdd(i, t)
      }

    lazy val datetime_add_transact_sql
      : PackratParser[DateTimeFunction with FunctionWithIdentifier with DateMathScript] =
      DateTimeAdd.regex ~ start ~> time_unit ~ separator ~
      long ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) <~ end ^^ {
        case u ~ _ ~ l ~ _ ~ i =>
          DateTimeAdd(i, TimeInterval(l.value.toInt, u), transactSql = true)
      }

    lazy val datetime_sub
      : PackratParser[DateTimeFunction with FunctionWithIdentifier with DateMathScript] =
      DateTimeSub.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ interval ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ =>
          DateTimeSub(i, t)
      }

    lazy val datetime_sub_transact_sql
      : PackratParser[DateTimeFunction with FunctionWithIdentifier with DateMathScript] =
      DateTimeSub.regex ~ start ~> time_unit ~ separator ~ long ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) <~ end ^^ {
        case u ~ _ ~ l ~ _ ~ i =>
          DateTimeSub(i, TimeInterval(l.value.toInt, u), transactSql = true)
      }

    lazy val datetime_parse: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
      DateTimeParse.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | literal | identifier) ~ separator ~ literal ~ end ^^ {
        case _ ~ _ ~ li ~ _ ~ f ~ _ =>
          li match {
            case l: SQLLiteral =>
              DateTimeParse(Identifier(l), f.value)
            case i: Identifier =>
              DateTimeParse(i, f.value)
          }
      }

    lazy val datetime_format: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
      DateTimeFormat.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ literal ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ f ~ _ =>
          DateTimeFormat(i, f.value)
      }

    lazy val datetime_function: PackratParser[DateTimeFunction with FunctionWithIdentifier] =
      datetime_add |
      datetime_add_transact_sql |
      datetime_sub |
      datetime_sub_transact_sql |
      datetime_parse |
      datetime_format

    lazy val dateTimeFunctionWithIdentifier: PackratParser[Identifier] =
      datetime_function ^^ { t =>
        t.identifier.withFunctions(t +: t.identifier.functions)
      }

  }

  trait TemporalParser extends CurrentParser with TimeParser with DateParser with DateTimeParser {
    self: Parser =>

    lazy val date_diff: PackratParser[BinaryFunction[_, _, _]] =
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

    /** 🔴 BOTH names. `DATEDIFF(unit, start, end)` is T-SQL's and DuckDB's spelling — what a BI
      * tool set to a SQL Server or DuckDB dialect emits — and it is `end - start` in both, which is
      * exactly what this production binds. Issue #363 moved `DATEDIFF` onto its own token to give
      * the TWO-argument MySQL form its own meaning, and keying this production on `DateDiff.regex`
      * alone silently dropped the unit-first spelling with it.
      *
      * Ordering is safe: this production is tried BEFORE `mysql_date_diff`, and `time_unit` fails
      * on a plain column, so `DATEDIFF(a, b)` still falls through to the MySQL form.
      */
    lazy val date_diff_transact_sql: PackratParser[BinaryFunction[_, _, _]] =
      (DateDiff.regex | MySqlDateDiff.regex) ~ start ~> time_unit ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) <~ end ^^ {
        case u ~ _ ~ d1 ~ _ ~ d2 =>
          DateDiff(d1, d2, u, DateDiffSpelling.UnitFirst)
      }

    /** MySQL's `DATEDIFF` (issue #363), which is NOT the function `date_diff` above parses.
      *
      * Two arguments — the shape MySQL actually defines — means `expr1 - expr2`, so the operands
      * are stored SWAPPED and the node keeps its single meaning of `end - start`. The dialect lives
      * here and in `toSQL`, nowhere else.
      *
      * 🔴 The THREE-argument `DATEDIFF(a, b, unit)` is NOT MySQL — MySQL's is days-only — it is
      * this engine's own extension, and a lead ruling keeps it exactly as it behaved before: `end -
      * start`, rendered as `DATE_DIFF`. So on this ONE spelling the arity changes the sign, and
      * `DateDiffSignSpec` pins both readings side by side so that stays deliberate and visible
      * rather than discovered.
      */
    lazy val mysql_date_diff: PackratParser[BinaryFunction[_, _, _]] =
      MySqlDateDiff.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ (separator ~ time_unit).? ~ end ^^ {
        case _ ~ _ ~ d1 ~ _ ~ d2 ~ u ~ _ =>
          u match {
            case Some(_ ~ unit) => DateDiff(d1, d2, unit, DateDiffSpelling.DateFirst)
            case None           => DateDiff(d2, d1, TimeUnit.DAYS, DateDiffSpelling.MySql)
          }
      }

    lazy val date_diff_identifier: PackratParser[Identifier] =
      (date_diff | date_diff_transact_sql | mysql_date_diff) ^^ { dd =>
        Identifier(dd)
      }

    lazy val date_trunc: PackratParser[FunctionWithIdentifier] =
      DateTrunc.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ separator ~ time_unit ~ end ^^ {
        case _ ~ _ ~ i ~ _ ~ u ~ _ =>
          DateTrunc(i, u)
      }

    lazy val date_trunc_transact_sql: PackratParser[FunctionWithIdentifier] =
      DateTrunc.regex ~ start ~> time_unit ~ separator ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) <~ end ^^ {
        case u ~ _ ~ i =>
          DateTrunc(i, u, transactSql = true)
      }

    lazy val date_trunc_identifier: PackratParser[Identifier] =
      (date_trunc | date_trunc_transact_sql) ^^ { dt =>
        dt.identifier.withFunctions(dt +: dt.identifier.functions)
      }

    lazy val extract_identifier: PackratParser[Identifier] =
      Extract.regex ~ start ~ time_field ~ "(?i)from".r ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ end ^^ {
        case _ ~ _ ~ u ~ _ ~ i ~ _ =>
          i.withFunctions(Extract(u) +: i.functions)
      }

    import TimeField._

    lazy val day_of_week_tr: PackratParser[FunctionWithIdentifier] =
      DAY_OF_WEEK.regex ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ end ^^ {
        case _ ~ _ ~ i ~ _ => new DayOfWeek(i)
      }

    lazy val day_of_week_identifier: PackratParser[Identifier] = day_of_week_tr ^^ { dw =>
      dw.identifier.withFunctions(dw +: dw.identifier.functions)
    }

    lazy val year_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      YEAR.regex ^^ (_ => new Year)
    lazy val month_of_year_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      MONTH_OF_YEAR.regex ^^ (_ => new MonthOfYear)
    lazy val day_of_month_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      DAY_OF_MONTH.regex ^^ (_ => new DayOfMonth)
    lazy val day_of_year_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      DAY_OF_YEAR.regex ^^ (_ => new DayOfYear)
    lazy val hour_of_day_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      HOUR_OF_DAY.regex ^^ (_ => new HourOfDay)
    lazy val minute_of_hour_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      MINUTE_OF_HOUR.regex ^^ (_ => new MinuteOfHour)
    lazy val second_of_minute_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      SECOND_OF_MINUTE.regex ^^ (_ => new SecondOfMinute)
    lazy val nano_of_second_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      NANO_OF_SECOND.regex ^^ (_ => new NanoOfSecond)
    lazy val micro_of_second_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      MICRO_OF_SECOND.regex ^^ (_ => new MicroOfSecond)
    lazy val milli_of_second_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      MILLI_OF_SECOND.regex ^^ (_ => new MilliOfSecond)
    lazy val epoch_day_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      EPOCH_DAY.regex ^^ (_ => new EpochDay)
    lazy val offset_seconds_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      OFFSET_SECONDS.regex ^^ (_ => new OffsetSeconds)

    lazy val quarter_of_year_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      IsoField.QUARTER_OF_YEAR.regex ^^ (_ => new QuarterOfYear)

    lazy val week_of_week_based_year_tr: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
      IsoField.WEEK_OF_WEEK_BASED_YEAR.regex ^^ (_ => new WeekOfWeekBasedYear)

    lazy val extractor_function: PackratParser[TransformFunction[SQLTemporal, SQLNumeric]] =
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
      mysql_date_diff |
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
