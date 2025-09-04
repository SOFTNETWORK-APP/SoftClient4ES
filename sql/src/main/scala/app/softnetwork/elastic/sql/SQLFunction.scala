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

sealed trait TimeUnit extends PainlessScript {
  lazy val regex: Regex = s"\\b(?i)${sql}s?\\b".r

  override def painless: String = s"ChronoUnit.${sql.toUpperCase()}"
}

sealed trait CalendarUnit extends TimeUnit
sealed trait FixedUnit extends TimeUnit

case object Year extends SQLExpr("year") with CalendarUnit
case object Month extends SQLExpr("month") with CalendarUnit
case object Quarter extends SQLExpr("quarter") with CalendarUnit
case object Week extends SQLExpr("week") with CalendarUnit

case object Day extends SQLExpr("day") with CalendarUnit with FixedUnit

case object Hour extends SQLExpr("hour") with FixedUnit
case object Minute extends SQLExpr("minute") with FixedUnit
case object Second extends SQLExpr("second") with FixedUnit

case object Interval extends SQLExpr("interval") with SQLFunction with SQLRegex

sealed trait TimeInterval extends PainlessScript {
  def value: Int
  def unit: TimeUnit
  override def sql: String = s"$Interval $value ${unit.sql}"

  override def painless: String = s"$value, ${unit.painless}"
}

case class CalendarInterval(value: Int, unit: CalendarUnit) extends TimeInterval
case class FixedInterval(value: Int, unit: FixedUnit) extends TimeInterval

object TimeInterval {
  def apply(value: Int, unit: TimeUnit): TimeInterval = unit match {
    case cu: CalendarUnit => CalendarInterval(value, cu)
    case fu: FixedUnit    => FixedInterval(value, fu)
  }
}

sealed trait DateTimeFunction extends SQLFunction

case object CurrentDate extends SQLExpr("current_date") with DateTimeFunction with PainlessScript {
  override def painless: String = "ZonedDateTime.of(LocalDate.now(), ZoneId.of('Z')).toLocalDate()"
}
case object CurentDateWithParens
    extends SQLExpr("current_date()")
    with DateTimeFunction
    with PainlessScript {
  override def painless: String = "ZonedDateTime.of(LocalDate.now(), ZoneId.of('Z')).toLocalDate()"
}
case object CurrentTime extends SQLExpr("current_time") with DateTimeFunction with PainlessScript {
  override def painless: String =
    "ZonedDateTime.of(LocalDateTime.now(), ZoneId.of('Z')).toLocalTime()"
}
case object CurrentTimeWithParens
    extends SQLExpr("current_time()")
    with DateTimeFunction
    with PainlessScript {
  override def painless: String =
    "ZonedDateTime.of(LocalDateTime.now(), ZoneId.of('Z')).toLocalTime()"
}
case object CurrentTimestamp
    extends SQLExpr("current_timestamp")
    with DateTimeFunction
    with PainlessScript {
  override def painless: String =
    "ZonedDateTime.of(LocalDateTime.now(), ZoneId.of('Z')).toLocalDateTime()"
}
case object CurrentTimestampWithParens
    extends SQLExpr("current_timestamp()")
    with DateTimeFunction
    with PainlessScript {
  override def painless: String =
    "ZonedDateTime.of(LocalDateTime.now(), ZoneId.of('Z')).toLocalDateTime()"
}
case object Now extends SQLExpr("now") with DateTimeFunction with PainlessScript {
  override def painless: String =
    "ZonedDateTime.of(LocalDateTime.now(), ZoneId.of('Z')).toLocalDateTime()"
}
case object NowWithParens extends SQLExpr("now()") with DateTimeFunction with PainlessScript {
  override def painless: String =
    "ZonedDateTime.of(LocalDateTime.now(), ZoneId.of('Z')).toLocalDateTime()"
}

case class DateAdd(interval: TimeInterval) extends SQLExpr("date_add") with DateTimeFunction
case class DateDiff(interval: TimeInterval) extends SQLExpr("date_diff") with DateTimeFunction
case class DateSub(interval: TimeInterval) extends SQLExpr("date_sub") with DateTimeFunction
case class DateTrunc(unit: TimeUnit) extends SQLExpr("date_trunc") with DateTimeFunction
case class Extract(unit: TimeUnit) extends SQLExpr("extract") with DateTimeFunction
case class FormatDate(format: String) extends SQLExpr("format_date") with DateTimeFunction
case class ParseDate(format: String) extends SQLExpr("parse_date") with DateTimeFunction
