package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.{Expr, Identifier, MathScript, PainlessScript, TokenRegex}
import app.softnetwork.elastic.sql.operator.time._
import app.softnetwork.elastic.sql.`type`.{
  SQLDate,
  SQLDateTime,
  SQLNumeric,
  SQLTemporal,
  SQLType,
  SQLTypeUtils,
  SQLTypes,
  SQLVarchar
}
import app.softnetwork.elastic.sql.time.{TimeField, TimeInterval, TimeUnit}

package object time {

  sealed trait IntervalFunction[IO <: SQLTemporal]
      extends TransformFunction[IO, IO]
      with MathScript {
    def operator: IntervalOperator

    override def fun: Option[IntervalOperator] = Some(operator)

    def interval: TimeInterval

    override def args: List[PainlessScript] = List(interval)

    override def argsSeparator: String = " "
    override def sql: String = s"$operator${args.map(_.sql).mkString(argsSeparator)}"

    override def script: String = s"${operator.script}${interval.script}"

    private[this] var _out: SQLType = outputType

    override def out: SQLType = _out

    override def applyType(in: SQLType): SQLType = {
      _out = interval.checkType(in).getOrElse(out)
      _out
    }

    override def validate(): Either[String, Unit] = interval.checkType(out) match {
      case Left(err) => Left(err)
      case Right(_)  => Right(())
    }

    override def toPainless(base: String, idx: Int): String =
      if (nullable)
        s"(def e$idx = $base; e$idx != null ? ${SQLTypeUtils.coerce(s"e$idx", expr.out, out, nullable = false)}$painless : null)"
      else
        s"${SQLTypeUtils.coerce(base, expr.out, out, nullable = expr.nullable)}$painless"
  }

  sealed trait AddInterval[IO <: SQLTemporal] extends IntervalFunction[IO] {
    override def operator: IntervalOperator = PLUS
  }

  sealed trait SubtractInterval[IO <: SQLTemporal] extends IntervalFunction[IO] {
    override def operator: IntervalOperator = MINUS
  }

  case class SQLAddInterval(interval: TimeInterval) extends AddInterval[SQLTemporal] {
    override def inputType: SQLTemporal = SQLTypes.Temporal
    override def outputType: SQLTemporal = SQLTypes.Temporal
  }

  case class SQLSubtractInterval(interval: TimeInterval) extends SubtractInterval[SQLTemporal] {
    override def inputType: SQLTemporal = SQLTypes.Temporal
    override def outputType: SQLTemporal = SQLTypes.Temporal
  }

  sealed trait DateTimeFunction extends Function {
    def now: String = "ZonedDateTime.now(ZoneId.of('Z'))"
    override def out: SQLType = SQLTypes.DateTime
  }

  sealed trait DateFunction extends DateTimeFunction {
    override def out: SQLType = SQLTypes.Date
  }

  sealed trait TimeFunction extends DateTimeFunction {
    override def out: SQLType = SQLTypes.Time
  }

  sealed trait SystemFunction extends Function {
    override def system: Boolean = true
  }

  sealed trait CurrentFunction extends SystemFunction with PainlessScript

  sealed trait CurrentDateTimeFunction
      extends DateTimeFunction
      with CurrentFunction
      with MathScript {
    override def painless: String = now
    override def script: String = "now"
  }

  sealed trait CurrentDateFunction extends DateFunction with CurrentFunction with MathScript {
    override def painless: String = s"$now.toLocalDate()"
    override def script: String = "now"
  }

  sealed trait CurrentTimeFunction extends TimeFunction with CurrentFunction {
    override def painless: String = s"$now.toLocalTime()"
  }

  case object CurrentDate extends Expr("CURRENT_DATE") with CurrentDateFunction {
    override lazy val words: List[String] = List(sql, "CURDATE")
  }

  case object CurentDateWithParens extends Expr("CURRENT_DATE()") with CurrentDateFunction

  case object CurrentTime extends Expr("CURRENT_TIME") with CurrentTimeFunction {
    override lazy val words: List[String] = List(sql, "CURTIME")
  }

  case object CurrentTimeWithParens extends Expr("CURRENT_TIME()") with CurrentTimeFunction

  case object CurrentTimestamp extends Expr("CURRENT_TIMESTAMP") with CurrentDateTimeFunction

  case object CurrentTimestampWithParens
      extends Expr("CURRENT_TIMESTAMP()")
      with CurrentDateTimeFunction

  case object Now extends Expr("NOW") with CurrentDateTimeFunction

  case object NowWithParens extends Expr("NOW()") with CurrentDateTimeFunction

  case object DateTrunc extends Expr("DATE_TRUNC") with TokenRegex with PainlessScript {
    override def painless: String = ".truncatedTo"
    override lazy val words: List[String] = List(sql, "DATETRUNC")
  }

  case class DateTrunc(identifier: Identifier, unit: TimeUnit)
      extends DateTimeFunction
      with TransformFunction[SQLTemporal, SQLTemporal]
      with FunctionWithIdentifier {
    override def fun: Option[PainlessScript] = Some(DateTrunc)

    override def args: List[PainlessScript] = List(unit)

    override def inputType: SQLTemporal = SQLTypes.Temporal // par défaut
    override def outputType: SQLTemporal = SQLTypes.Temporal // idem

    override def sql: String = DateTrunc.sql
    override def toSQL(base: String): String = {
      s"$sql($base, ${unit.sql})"
    }
  }

  case object Extract extends Expr("EXTRACT") with TokenRegex with PainlessScript {
    override def painless: String = ".get"
  }

  case class Extract(field: TimeField, override val sql: String = Extract.sql)
      extends DateTimeFunction
      with TransformFunction[SQLTemporal, SQLNumeric] {
    override def fun: Option[PainlessScript] = Some(Extract)

    override def args: List[PainlessScript] = List(field)

    override def inputType: SQLTemporal = SQLTypes.Temporal
    override def outputType: SQLNumeric = SQLTypes.Numeric

    override def toSQL(base: String): String = s"$sql(${field.sql} FROM $base)"

  }

  import TimeField._

  object Year extends Extract(YEAR, YEAR.sql) {
    override def toSQL(base: String): String = s"$sql($base)"
  }

  object MonthOfYear extends Extract(MONTH_OF_YEAR, MONTH_OF_YEAR.sql) {
    override def toSQL(base: String): String = s"$sql($base)"
  }

  object DayOfMonth extends Extract(DAY_OF_MONTH, DAY_OF_MONTH.sql) {
    override def toSQL(base: String): String = s"$sql($base)"
  }

  object DayOfWeek extends Extract(DAY_OF_WEEK, DAY_OF_WEEK.sql) {
    override def toSQL(base: String): String = s"$sql($base)"
  }

  object DayOfYear extends Extract(DAY_OF_YEAR, DAY_OF_YEAR.sql) {
    override def toSQL(base: String): String = s"$sql($base)"
  }

  object HourOfDay extends Extract(HOUR_OF_DAY, HOUR_OF_DAY.sql) {
    override def toSQL(base: String): String = s"$sql($base)"
  }

  object MinuteOfHour extends Extract(MINUTE_OF_HOUR, MINUTE_OF_HOUR.sql) {
    override def toSQL(base: String): String = s"$sql($base)"
  }

  object SecondOfMinute extends Extract(SECOND_OF_MINUTE, SECOND_OF_MINUTE.sql) {
    override def toSQL(base: String): String = s"$sql($base)"
  }

  case object DateDiff extends Expr("DATE_DIFF") with TokenRegex with PainlessScript {
    override def painless: String = ".between"
    override lazy val words: List[String] = List(sql, "DATEDIFF")
  }

  case class DateDiff(end: PainlessScript, start: PainlessScript, unit: TimeUnit)
      extends DateTimeFunction
      with BinaryFunction[SQLDateTime, SQLDateTime, SQLNumeric]
      with PainlessScript {
    override def fun: Option[PainlessScript] = Some(DateDiff)

    override def inputType: SQLDateTime = SQLTypes.DateTime
    override def outputType: SQLNumeric = SQLTypes.Numeric

    override def left: PainlessScript = start
    override def right: PainlessScript = end

    override def sql: String = DateDiff.sql

    override def toSQL(base: String): String = s"$sql(${end.sql}, ${start.sql}, ${unit.sql})"

    override def toPainlessCall(callArgs: List[String]): String =
      s"${unit.painless}${DateDiff.painless}(${callArgs.mkString(", ")})"
  }

  case object DateAdd extends Expr("DATE_ADD") with TokenRegex {
    override lazy val words: List[String] = List(sql, "DATEADD")
  }

  case class DateAdd(identifier: Identifier, interval: TimeInterval)
      extends DateFunction
      with AddInterval[SQLDate]
      with TransformFunction[SQLDate, SQLDate]
      with FunctionWithIdentifier {
    override def inputType: SQLDate = SQLTypes.Date
    override def outputType: SQLDate = SQLTypes.Date
    override def sql: String = DateAdd.sql
    override def toSQL(base: String): String = {
      s"$sql($base, ${interval.sql})"
    }
  }

  case object DateSub extends Expr("DATE_SUB") with TokenRegex {
    override lazy val words: List[String] = List(sql, "DATESUB")
  }

  case class DateSub(identifier: Identifier, interval: TimeInterval)
      extends DateFunction
      with SubtractInterval[SQLDate]
      with TransformFunction[SQLDate, SQLDate]
      with FunctionWithIdentifier {
    override def inputType: SQLDate = SQLTypes.Date
    override def outputType: SQLDate = SQLTypes.Date
    override def sql: String = DateSub.sql
    override def toSQL(base: String): String = {
      s"$sql($base, ${interval.sql})"
    }
  }

  case object DateParse extends Expr("DATE_PARSE") with TokenRegex with PainlessScript {
    override def painless: String = ".parse"
  }

  case class DateParse(identifier: Identifier, format: String)
      extends DateFunction
      with TransformFunction[SQLVarchar, SQLDate]
      with FunctionWithIdentifier {
    override def fun: Option[PainlessScript] = Some(DateParse)

    override def args: List[PainlessScript] = List.empty

    override def inputType: SQLVarchar = SQLTypes.Varchar
    override def outputType: SQLDate = SQLTypes.Date

    override def sql: String = DateParse.sql
    override def toSQL(base: String): String = {
      s"$sql($base, '$format')"
    }

    override def painless: String = throw new NotImplementedError("Use toPainless instead")
    override def toPainless(base: String, idx: Int): String =
      if (nullable)
        s"(def e$idx = $base; e$idx != null ? DateTimeFormatter.ofPattern('$format').parse(e$idx, LocalDate::from) : null)"
      else
        s"DateTimeFormatter.ofPattern('$format').parse($base, LocalDate::from)"
  }

  case object DateFormat extends Expr("DATE_FORMAT") with TokenRegex with PainlessScript {
    override def painless: String = ".format"
  }

  case class DateFormat(identifier: Identifier, format: String)
      extends DateFunction
      with TransformFunction[SQLDate, SQLVarchar]
      with FunctionWithIdentifier {
    override def fun: Option[PainlessScript] = Some(DateFormat)

    override def args: List[PainlessScript] = List.empty

    override def inputType: SQLDate = SQLTypes.Date
    override def outputType: SQLVarchar = SQLTypes.Varchar

    override def sql: String = DateFormat.sql
    override def toSQL(base: String): String = {
      s"$sql($base, '$format')"
    }

    override def painless: String = throw new NotImplementedError("Use toPainless instead")
    override def toPainless(base: String, idx: Int): String =
      if (nullable)
        s"(def e$idx = $base; e$idx != null ? DateTimeFormatter.ofPattern('$format').format(e$idx) : null)"
      else
        s"DateTimeFormatter.ofPattern('$format').format($base)"
  }

  case object DateTimeAdd extends Expr("DATETIME_ADD") with TokenRegex {
    override lazy val words: List[String] = List(sql, "DATETIMEADD")
  }

  case class DateTimeAdd(identifier: Identifier, interval: TimeInterval)
      extends DateTimeFunction
      with AddInterval[SQLDateTime]
      with TransformFunction[SQLDateTime, SQLDateTime]
      with FunctionWithIdentifier {
    override def inputType: SQLDateTime = SQLTypes.DateTime
    override def outputType: SQLDateTime = SQLTypes.DateTime
    override def sql: String = DateTimeAdd.sql
    override def toSQL(base: String): String = {
      s"$sql($base, ${interval.sql})"
    }
  }

  case object DateTimeSub extends Expr("DATETIME_SUB") with TokenRegex {
    override lazy val words: List[String] = List(sql, "DATETIMESUB")
  }

  case class DateTimeSub(identifier: Identifier, interval: TimeInterval)
      extends DateTimeFunction
      with SubtractInterval[SQLDateTime]
      with TransformFunction[SQLDateTime, SQLDateTime]
      with FunctionWithIdentifier {
    override def inputType: SQLDateTime = SQLTypes.DateTime
    override def outputType: SQLDateTime = SQLTypes.DateTime
    override def sql: String = DateTimeSub.sql
    override def toSQL(base: String): String = {
      s"$sql($base, ${interval.sql})"
    }
  }

  case object DateTimeParse extends Expr("DATETIME_PARSE") with TokenRegex with PainlessScript {
    override def painless: String = ".parse"
  }

  case class DateTimeParse(identifier: Identifier, format: String)
      extends DateTimeFunction
      with TransformFunction[SQLVarchar, SQLDateTime]
      with FunctionWithIdentifier {
    override def fun: Option[PainlessScript] = Some(DateTimeParse)

    override def args: List[PainlessScript] = List.empty

    override def inputType: SQLVarchar = SQLTypes.Varchar
    override def outputType: SQLDateTime = SQLTypes.DateTime

    override def sql: String = DateTimeParse.sql
    override def toSQL(base: String): String = {
      s"$sql($base, '$format')"
    }

    override def painless: String = throw new NotImplementedError("Use toPainless instead")
    override def toPainless(base: String, idx: Int): String =
      if (nullable)
        s"(def e$idx = $base; e$idx != null ? DateTimeFormatter.ofPattern('$format').parse(e$idx, ZonedDateTime::from) : null)"
      else
        s"DateTimeFormatter.ofPattern('$format').parse($base, ZonedDateTime::from)"
  }

  case object DateTimeFormat extends Expr("DATETIME_FORMAT") with TokenRegex with PainlessScript {
    override def painless: String = ".format"
  }

  case class DateTimeFormat(identifier: Identifier, format: String)
      extends DateTimeFunction
      with TransformFunction[SQLDateTime, SQLVarchar]
      with FunctionWithIdentifier {
    override def fun: Option[PainlessScript] = Some(DateTimeFormat)

    override def args: List[PainlessScript] = List.empty

    override def inputType: SQLDateTime = SQLTypes.DateTime
    override def outputType: SQLVarchar = SQLTypes.Varchar

    override def sql: String = DateTimeFormat.sql
    override def toSQL(base: String): String = {
      s"$sql($base, '$format')"
    }

    override def painless: String = throw new NotImplementedError("Use toPainless instead")
    override def toPainless(base: String, idx: Int): String =
      if (nullable)
        s"(def e$idx = $base; e$idx != null ? DateTimeFormatter.ofPattern('$format').format(e$idx) : null)"
      else
        s"DateTimeFormatter.ofPattern('$format').format($base)"
  }

}
