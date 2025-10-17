/*
 * Copyright 2015 SOFTNETWORK
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

import app.softnetwork.elastic.sql.Identifier
import app.softnetwork.elastic.sql.`type`.SQLTemporal
import app.softnetwork.elastic.sql.function.TransformFunction
import app.softnetwork.elastic.sql.function.time.{SQLAddInterval, SQLSubtractInterval}
import app.softnetwork.elastic.sql.time.{Interval, IsoField, TimeField, TimeInterval, TimeUnit}

package object time {

  trait TimeParser { self: Parser =>

    import TimeField._

    def year: PackratParser[TimeField] = YEAR.regex ^^ (_ => YEAR)
    def month_of_year: PackratParser[TimeField] = MONTH_OF_YEAR.regex ^^ (_ => MONTH_OF_YEAR)
    def day_of_month: PackratParser[TimeField] =
      DAY_OF_MONTH.regex ^^ (_ => DAY_OF_MONTH)
    def day_of_week: PackratParser[TimeField] =
      DAY_OF_WEEK.regex ^^ (_ => DAY_OF_WEEK)
    def day_of_year: PackratParser[TimeField] =
      DAY_OF_YEAR.regex ^^ (_ => DAY_OF_YEAR)
    def hour_of_day: PackratParser[TimeField] = HOUR_OF_DAY.regex ^^ (_ => HOUR_OF_DAY)
    def minute_of_hour: PackratParser[TimeField] = MINUTE_OF_HOUR.regex ^^ (_ => MINUTE_OF_HOUR)
    def second_of_minute: PackratParser[TimeField] =
      SECOND_OF_MINUTE.regex ^^ (_ => SECOND_OF_MINUTE)
    def nano_of_second: PackratParser[TimeField] =
      NANO_OF_SECOND.regex ^^ (_ => NANO_OF_SECOND)
    def micro_of_second: PackratParser[TimeField] =
      MICRO_OF_SECOND.regex ^^ (_ => MICRO_OF_SECOND)
    def milli_of_second: PackratParser[TimeField] =
      MILLI_OF_SECOND.regex ^^ (_ => MILLI_OF_SECOND)
    def epoch_day: PackratParser[TimeField] =
      EPOCH_DAY.regex ^^ (_ => EPOCH_DAY)
    def offset_seconds: PackratParser[TimeField] =
      OFFSET_SECONDS.regex ^^ (_ => OFFSET_SECONDS)

    import IsoField._

    def quarter_of_year: PackratParser[TimeField] =
      QUARTER_OF_YEAR.regex ^^ (_ => QUARTER_OF_YEAR)

    def week_of_week_based_year: PackratParser[TimeField] =
      WEEK_OF_WEEK_BASED_YEAR.regex ^^ (_ => WEEK_OF_WEEK_BASED_YEAR)

    def time_field: PackratParser[TimeField] =
      year |
      month_of_year |
      day_of_month |
      day_of_week |
      day_of_year |
      hour_of_day |
      minute_of_hour |
      second_of_minute |
      nano_of_second |
      micro_of_second |
      milli_of_second |
      epoch_day |
      offset_seconds |
      quarter_of_year |
      week_of_week_based_year

    import TimeUnit._

    def years: PackratParser[TimeUnit] = YEARS.regex ^^ (_ => YEARS)
    def months: PackratParser[TimeUnit] = MONTHS.regex ^^ (_ => MONTHS)
    def quarters: PackratParser[TimeUnit] = QUARTERS.regex ^^ (_ => QUARTERS)
    def weeks: PackratParser[TimeUnit] = WEEKS.regex ^^ (_ => WEEKS)
    def days: PackratParser[TimeUnit] = DAYS.regex ^^ (_ => DAYS)
    def hours: PackratParser[TimeUnit] = HOURS.regex ^^ (_ => HOURS)
    def minutes: PackratParser[TimeUnit] = MINUTES.regex ^^ (_ => MINUTES)
    def seconds: PackratParser[TimeUnit] = SECONDS.regex ^^ (_ => SECONDS)

    def time_unit: PackratParser[TimeUnit] =
      years | months | quarters | weeks | days | hours | minutes | seconds

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
      ((identifierWithTransformation |
      identifierWithFunction |
      identifierWithValue |
      identifier) ~ rep(intervalFunction) ^^ { case i ~ f =>
        i.withFunctions(f ++ i.functions)
      }) >> cast

  }
}
