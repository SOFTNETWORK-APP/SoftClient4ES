package app.softnetwork.elastic.sql

import scala.util.matching.Regex

sealed trait SQLFunction extends SQLRegex

sealed trait AggregateFunction extends SQLFunction
case object Count extends SQLExpr("count") with AggregateFunction
case object Min extends SQLExpr("min") with AggregateFunction
case object Max extends SQLExpr("max") with AggregateFunction
case object Avg extends SQLExpr("avg") with AggregateFunction
case object Sum extends SQLExpr("sum") with AggregateFunction

case object Distance extends SQLExpr("distance") with SQLFunction with SQLOperator

sealed trait TimeUnit extends PainlessScript with MathScript {
  lazy val regex: Regex = s"\\b(?i)${sql}s?\\b".r

  override def painless: String = s"ChronoUnit.${sql.toUpperCase()}"
}

sealed trait CalendarUnit extends TimeUnit
sealed trait FixedUnit extends TimeUnit

case object Year extends SQLExpr("year") with CalendarUnit {
  override def script: String = "y"
}
case object Month extends SQLExpr("month") with CalendarUnit {
  override def script: String = "M"
}
case object Quarter extends SQLExpr("quarter") with CalendarUnit {
  override def script: String = throw new IllegalArgumentException(
    "Quarter must be converted to months (value * 3) before creating date-math"
  )
}
case object Week extends SQLExpr("week") with CalendarUnit {
  override def script: String = "w"
}

case object Day extends SQLExpr("day") with CalendarUnit with FixedUnit {
  override def script: String = "d"
}

case object Hour extends SQLExpr("hour") with FixedUnit {
  override def script: String = "H"
}
case object Minute extends SQLExpr("minute") with FixedUnit {
  override def script: String = "m"
}
case object Second extends SQLExpr("second") with FixedUnit {
  override def script: String = "s"
}

case object Interval extends SQLExpr("interval") with SQLFunction with SQLRegex

sealed trait TimeInterval extends PainlessScript with MathScript {
  def value: Int
  def unit: TimeUnit
  override def sql: String = s"$Interval $value ${unit.sql}"

  override def painless: String = s"$value, ${unit.painless}"

  override def script: String = TimeInterval.script(this)
}

case class CalendarInterval(value: Int, unit: CalendarUnit) extends TimeInterval
case class FixedInterval(value: Int, unit: FixedUnit) extends TimeInterval

object TimeInterval {
  def apply(value: Int, unit: TimeUnit): TimeInterval = unit match {
    case cu: CalendarUnit => CalendarInterval(value, cu)
    case fu: FixedUnit    => FixedInterval(value, fu)
  }
  def script(interval: TimeInterval): String = interval match {
    case CalendarInterval(v, Quarter) => s"${v * 3}M"
    case CalendarInterval(v, u)       => s"$v${u.script}"
    case FixedInterval(v, u)          => s"$v${u.script}"
  }
}

sealed trait DateTimeFunction extends SQLFunction

sealed trait CurrentDateTimeFunction extends DateTimeFunction with PainlessScript with MathScript {
  override def painless: String =
    "ZonedDateTime.of(LocalDateTime.now(), ZoneId.of('Z')).toLocalDateTime()"
  override def script: String = "now"
}

sealed trait CurrentDateFunction extends CurrentDateTimeFunction {
  override def painless: String = "ZonedDateTime.of(LocalDate.now(), ZoneId.of('Z')).toLocalDate()"
}

sealed trait CurrentTimeFunction extends CurrentDateTimeFunction {
  override def painless: String = "ZonedDateTime.of(LocalDate.now(), ZoneId.of('Z')).toLocalTime()"
}

case object CurrentDate extends SQLExpr("current_date") with CurrentDateFunction

case object CurentDateWithParens extends SQLExpr("current_date()") with CurrentDateFunction

case object CurrentTime extends SQLExpr("current_time") with CurrentTimeFunction

case object CurrentTimeWithParens extends SQLExpr("current_time()") with CurrentTimeFunction

case object CurrentTimestamp extends SQLExpr("current_timestamp") with CurrentDateTimeFunction

case object CurrentTimestampWithParens
    extends SQLExpr("current_timestamp()")
    with CurrentDateTimeFunction

case object Now extends SQLExpr("now") with CurrentDateTimeFunction

case object NowWithParens extends SQLExpr("now()") with CurrentDateTimeFunction

case class DateAdd(interval: TimeInterval) extends SQLExpr("date_add") with DateTimeFunction
case class DateDiff(interval: TimeInterval) extends SQLExpr("date_diff") with DateTimeFunction
case class DateSub(interval: TimeInterval) extends SQLExpr("date_sub") with DateTimeFunction
case class DateTrunc(unit: TimeUnit) extends SQLExpr("date_trunc") with DateTimeFunction
case class Extract(unit: TimeUnit) extends SQLExpr("extract") with DateTimeFunction
case class FormatDate(format: String) extends SQLExpr("format_date") with DateTimeFunction
case class ParseDate(format: String) extends SQLExpr("parse_date") with DateTimeFunction
