package app.softnetwork.elastic.sql

import scala.util.matching.Regex

sealed trait SQLFunction extends SQLRegex {
  def toSQL(base: String): String = if (base.nonEmpty) s"$sql($base)" else sql
}

object SQLFunctionUtils {
  def aggregateAndTransformFunctions(
    identifier: Identifier
  ): (List[SQLFunction], List[SQLFunction]) = {
    identifier.functions.partition {
      case _: AggregateFunction => true
      case _                    => false
    }
  }

  def transformFunctions(identifier: Identifier): List[SQLFunction] = {
    aggregateAndTransformFunctions(identifier)._2
  }

  def buildPainless(
    identifier: Identifier
  ): String = {
    val base = identifier.painless
    val orderedFunctions = transformFunctions(identifier).reverse
    orderedFunctions.foldLeft(base) {
      case (expr, f: SQLTransformFunction[_, _]) => f.toPainless(expr)
      case (expr, f: PainlessScript)             => s"$expr${f.painless}"
      case (expr, f)                             => f.toSQL(expr) // fallback
    }
  }
}

trait SQLFunctionChain extends SQLFunction with SQLValidation {
  def functions: List[SQLFunction]

  override def validate(): Either[String, Unit] =
    SQLValidator.validateChain(functions)

  override def toSQL(base: String): String =
    functions.reverse.foldLeft(base)((expr, fun) => {
      fun.toSQL(expr)
    })

  lazy val aggregateFunction: Option[AggregateFunction] = functions.headOption match {
    case Some(af: AggregateFunction) => Some(af)
    case _                           => None
  }

  lazy val aggregation: Boolean = aggregateFunction.isDefined
}

sealed trait SQLUnaryFunction[In <: SQLType, Out <: SQLType]
    extends SQLFunction
    with PainlessScript {
  def inputType: In
  def outputType: Out
}

sealed trait SQLBinaryFunction[In1 <: SQLType, In2 <: SQLType, Out <: SQLType]
    extends SQLUnaryFunction[SQLAny, Out] { self: SQLFunction =>

  override def inputType: SQLAny = SQLTypes.Any

  def left: PainlessScript
  def right: PainlessScript

}

sealed trait SQLTransformFunction[In <: SQLType, Out <: SQLType] extends SQLUnaryFunction[In, Out] {
  def toPainless(base: String): String = s"$base$painless"
}

sealed trait SQLArithmeticFunction[In <: SQLType, Out <: SQLType]
    extends SQLTransformFunction[In, Out] {
  def operator: ArithmeticOperator
  override def toSQL(base: String): String = s"$base$operator$sql"
}

sealed trait ParametrizedFunction extends SQLFunction {
  def params: Seq[String]
  override def toSQL(base: String): String = {
    params match {
      case Nil => s"$sql($base)"
      case _ =>
        val paramsStr = params.mkString(", ")
        s"$sql($paramsStr)($base)"
    }
  }
}

sealed trait AggregateFunction extends SQLFunction
case object Count extends SQLExpr("count") with AggregateFunction
case object Min extends SQLExpr("min") with AggregateFunction
case object Max extends SQLExpr("max") with AggregateFunction
case object Avg extends SQLExpr("avg") with AggregateFunction
case object Sum extends SQLExpr("sum") with AggregateFunction

case object Distance extends SQLExpr("distance") with SQLFunction with SQLOperator

sealed trait TimeUnit extends PainlessScript with MathScript {
  lazy val regex: Regex = s"\\b(?i)$sql(s)?\\b".r

  override def painless: String = s"ChronoUnit.${sql.toUpperCase()}S"
}

sealed trait CalendarUnit extends TimeUnit
sealed trait FixedUnit extends TimeUnit

object TimeUnit {
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

}

case object Interval extends SQLExpr("interval") with SQLFunction with SQLRegex

sealed trait TimeInterval extends PainlessScript with MathScript {
  def value: Int
  def unit: TimeUnit
  override def sql: String = s"$Interval $value ${unit.sql}"

  override def painless: String = s"$value, ${unit.painless}"

  override def script: String = TimeInterval.script(this)
}

import TimeUnit._

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

case class SQLAddInterval(interval: TimeInterval)
    extends SQLExpr(interval.sql)
    with SQLArithmeticFunction[SQLDateTime, SQLDateTime]
    with MathScript {
  override def operator: ArithmeticOperator = Add
  override def inputType: SQLDateTime = SQLTypes.DateTime
  override def outputType: SQLDateTime = SQLTypes.DateTime
  override def painless: String = s".plus(${interval.painless})"
  override def script: String = s"${operator.script}${interval.script}"
}

case class SQLSubstractInterval(interval: TimeInterval)
    extends SQLExpr(interval.sql)
    with SQLArithmeticFunction[SQLDateTime, SQLDateTime]
    with MathScript {
  override def operator: ArithmeticOperator = Subtract
  override def inputType: SQLDateTime = SQLTypes.DateTime
  override def outputType: SQLDateTime = SQLTypes.DateTime
  override def painless: String = s".minus(${interval.painless})"
  override def script: String = s"${operator.script}${interval.script}"
}

sealed trait DateTimeFunction extends SQLFunction

sealed trait DateFunction extends DateTimeFunction

sealed trait TimeFunction extends DateTimeFunction

sealed trait CurrentDateTimeFunction extends DateTimeFunction with PainlessScript with MathScript {
  override def painless: String =
    "ZonedDateTime.of(LocalDateTime.now(), ZoneId.of('Z')).toLocalDateTime()"
  override def script: String = "now"
}

sealed trait CurrentDateFunction extends CurrentDateTimeFunction with DateFunction {
  override def painless: String = "ZonedDateTime.of(LocalDate.now(), ZoneId.of('Z')).toLocalDate()"
}

sealed trait CurrentTimeFunction extends CurrentDateTimeFunction with TimeFunction {
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

case class DateTrunc(unit: TimeUnit)
    extends SQLExpr("date_trunc")
    with DateTimeFunction
    with SQLTransformFunction[SQLTemporal, SQLTemporal]
    with ParametrizedFunction {
  override def inputType: SQLTemporal = SQLTypes.Temporal // par défaut
  override def outputType: SQLTemporal = SQLTypes.Temporal // idem
  override def params: Seq[String] = Seq(unit.sql)
  override def painless: String = s".truncatedTo(${unit.painless})"
}

case class Extract(unit: TimeUnit, override val sql: String = "extract")
    extends SQLExpr(sql)
    with DateTimeFunction
    with SQLTransformFunction[SQLTemporal, SQLNumber]
    with ParametrizedFunction {
  override def inputType: SQLTemporal = SQLTypes.Temporal
  override def outputType: SQLNumber = SQLTypes.Number
  override def params: Seq[String] = Seq(unit.sql)
  override def painless: String = s".get(${unit.painless})"
}

object YEAR extends Extract(Year, Year.sql) {
  override def params: Seq[String] = Seq.empty
}

object MONTH extends Extract(Month, Month.sql) {
  override def params: Seq[String] = Seq.empty
}

object DAY extends Extract(Day, Day.sql) {
  override def params: Seq[String] = Seq.empty
}

object HOUR extends Extract(Hour, Hour.sql) {
  override def params: Seq[String] = Seq.empty
}

object MINUTE extends Extract(Minute, Minute.sql) {
  override def params: Seq[String] = Seq.empty
}

object SECOND extends Extract(Second, Second.sql) {
  override def params: Seq[String] = Seq.empty
}

case class DateDiff(end: PainlessScript, start: PainlessScript, unit: TimeUnit)
    extends SQLExpr("date_diff")
    with DateTimeFunction
    with SQLBinaryFunction[SQLDateTime, SQLDateTime, SQLNumber]
    with PainlessScript {
  override def outputType: SQLNumber = SQLTypes.Number
  override def left: PainlessScript = end
  override def right: PainlessScript = start
  override def toSQL(base: String): String = {
    s"$sql(${end.sql}, ${start.sql}, ${unit.sql})"
  }
  lazy val startPainless: String = start match {
    case i: Identifier => SQLFunctionUtils.buildPainless(i)
    case _             => start.painless
  }
  lazy val endPainless: String = end match {
    case i: Identifier => SQLFunctionUtils.buildPainless(i)
    case _             => end.painless
  }
  override def painless: String = s"${unit.painless}.between($startPainless, $endPainless)"
}

case class DateAdd(interval: TimeInterval)
    extends SQLExpr("date_add")
    with DateFunction
    with SQLTransformFunction[SQLDate, SQLDate]
    with ParametrizedFunction {
  override def inputType: SQLDate = SQLTypes.Date
  override def outputType: SQLDate = SQLTypes.Date
  override def params: Seq[String] = Seq(interval.sql)
  override def painless: String = s".plus(${interval.painless})"
}

case class DateSub(interval: TimeInterval)
    extends SQLExpr("date_sub")
    with DateFunction
    with SQLTransformFunction[SQLDate, SQLDate]
    with ParametrizedFunction {
  override def inputType: SQLDate = SQLTypes.Date
  override def outputType: SQLDate = SQLTypes.Date
  override def params: Seq[String] = Seq(interval.sql)
  override def painless: String = s".minus(${interval.painless})"
}

case class ParseDate(format: String)
    extends SQLExpr("parse_date")
    with DateFunction
    with SQLTransformFunction[SQLString, SQLDate]
    with ParametrizedFunction {
  override def inputType: SQLString = SQLTypes.String
  override def outputType: SQLDate = SQLTypes.Date
  override def params: Seq[String] = Seq(s"'$format'")
  override def painless: String = throw new NotImplementedError("Use toPainless instead")
  override def toPainless(base: String): String =
    s"DateTimeFormatter.ofPattern('$format').parse($base, LocalDate::from)"
}

case class FormatDate(format: String)
    extends SQLExpr("format_date")
    with DateFunction
    with SQLTransformFunction[SQLDate, SQLString]
    with ParametrizedFunction {
  override def inputType: SQLDate = SQLTypes.Date
  override def outputType: SQLString = SQLTypes.String
  override def params: Seq[String] = Seq(s"'$format'")
  override def painless: String = throw new NotImplementedError("Use toPainless instead")
  override def toPainless(base: String): String =
    s"DateTimeFormatter.ofPattern('$format').format($base)"
}

case class DateTimeAdd(interval: TimeInterval)
    extends SQLExpr("datetime_add")
    with DateTimeFunction
    with SQLTransformFunction[SQLDateTime, SQLDateTime]
    with ParametrizedFunction {
  override def inputType: SQLDateTime = SQLTypes.DateTime
  override def outputType: SQLDateTime = SQLTypes.DateTime
  override def params: Seq[String] = Seq(interval.sql)
  override def painless: String = s".plus(${interval.painless})"
}

case class DateTimeSub(interval: TimeInterval)
    extends SQLExpr("datetime_sub")
    with DateTimeFunction
    with SQLTransformFunction[SQLDateTime, SQLDateTime]
    with ParametrizedFunction {
  override def inputType: SQLDateTime = SQLTypes.DateTime
  override def outputType: SQLDateTime = SQLTypes.DateTime
  override def params: Seq[String] = Seq(interval.sql)
  override def painless: String = s".minus(${interval.painless})"
}

case class ParseDateTime(format: String)
    extends SQLExpr("parse_datetime")
    with DateTimeFunction
    with SQLTransformFunction[SQLString, SQLDateTime]
    with ParametrizedFunction {
  override def inputType: SQLString = SQLTypes.String
  override def outputType: SQLDateTime = SQLTypes.DateTime
  override def params: Seq[String] = Seq(s"'$format'")
  override def painless: String = throw new NotImplementedError("Use toPainless instead")
  override def toPainless(base: String): String =
    s"DateTimeFormatter.ofPattern('$format').parse($base, ZonedDateTime::from)"
}

case class FormatDateTime(format: String)
    extends SQLExpr("format_datetime")
    with DateTimeFunction
    with SQLTransformFunction[SQLDateTime, SQLString]
    with ParametrizedFunction {
  override def inputType: SQLDateTime = SQLTypes.DateTime
  override def outputType: SQLString = SQLTypes.String
  override def params: Seq[String] = Seq(s"'$format'")
  override def painless: String = throw new NotImplementedError("Use toPainless instead")
  override def toPainless(base: String): String =
    s"DateTimeFormatter.ofPattern('$format').format($base)"
}
