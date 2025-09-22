package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.Identifier
import app.softnetwork.elastic.sql.`type`.SQLTemporal
import app.softnetwork.elastic.sql.function.TransformFunction
import app.softnetwork.elastic.sql.function.time.{SQLAddInterval, SQLSubtractInterval}
import app.softnetwork.elastic.sql.time.{Interval, TimeInterval, TimeUnit}
import app.softnetwork.elastic.sql.time.TimeUnit.{
  Day,
  Hour,
  Minute,
  Month,
  Quarter,
  Second,
  Week,
  Year
}

package object time {

  trait TimeParser { self: Parser =>

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

  }
}
