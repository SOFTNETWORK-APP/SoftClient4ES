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
import app.softnetwork.elastic.sql.time.{TimeInterval, TimeUnit}

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
    override def operator: IntervalOperator = Plus
  }

  sealed trait SubtractInterval[IO <: SQLTemporal] extends IntervalFunction[IO] {
    override def operator: IntervalOperator = Minus
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

  case object CurrentDate extends Expr("current_date") with CurrentDateFunction

  case object CurentDateWithParens extends Expr("current_date()") with CurrentDateFunction

  case object CurrentTime extends Expr("current_time") with CurrentTimeFunction

  case object CurrentTimeWithParens extends Expr("current_time()") with CurrentTimeFunction

  case object CurrentTimestamp extends Expr("current_timestamp") with CurrentDateTimeFunction

  case object CurrentTimestampWithParens
      extends Expr("current_timestamp()")
      with CurrentDateTimeFunction

  case object Now extends Expr("now") with CurrentDateTimeFunction

  case object NowWithParens extends Expr("now()") with CurrentDateTimeFunction

  case object DateTrunc extends Expr("date_trunc") with TokenRegex with PainlessScript {
    override def painless: String = ".truncatedTo"
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

  case object Extract extends Expr("extract") with TokenRegex with PainlessScript {
    override def painless: String = ".get"
  }

  case class Extract(unit: TimeUnit, override val sql: String = "extract")
      extends DateTimeFunction
      with TransformFunction[SQLTemporal, SQLNumeric] {
    override def fun: Option[PainlessScript] = Some(Extract)

    override def args: List[PainlessScript] = List(unit)

    override def inputType: SQLTemporal = SQLTypes.Temporal
    override def outputType: SQLNumeric = SQLTypes.Numeric

    override def toSQL(base: String): String = s"$sql(${unit.sql} from $base)"

  }

  import TimeUnit._

  object YEAR extends Extract(Year, Year.sql) {
    override def toSQL(base: String): String = s"$sql($base)"
  }

  object MONTH extends Extract(Month, Month.sql) {
    override def toSQL(base: String): String = s"$sql($base)"
  }

  object DAY extends Extract(Day, Day.sql) {
    override def toSQL(base: String): String = s"$sql($base)"
  }

  object HOUR extends Extract(Hour, Hour.sql) {
    override def toSQL(base: String): String = s"$sql($base)"
  }

  object MINUTE extends Extract(Minute, Minute.sql) {
    override def toSQL(base: String): String = s"$sql($base)"
  }

  object SECOND extends Extract(Second, Second.sql) {
    override def toSQL(base: String): String = s"$sql($base)"
  }

  case object DateDiff extends Expr("date_diff") with TokenRegex with PainlessScript {
    override def painless: String = ".between"
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

  case object DateAdd extends Expr("date_add") with TokenRegex

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

  case object DateSub extends Expr("date_sub") with TokenRegex

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

  case object ParseDate extends Expr("parse_date") with TokenRegex with PainlessScript {
    override def painless: String = ".parse"
  }

  case class ParseDate(identifier: Identifier, format: String)
      extends DateFunction
      with TransformFunction[SQLVarchar, SQLDate]
      with FunctionWithIdentifier {
    override def fun: Option[PainlessScript] = Some(ParseDate)

    override def args: List[PainlessScript] = List.empty

    override def inputType: SQLVarchar = SQLTypes.Varchar
    override def outputType: SQLDate = SQLTypes.Date

    override def sql: String = ParseDate.sql
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

  case object FormatDate extends Expr("format_date") with TokenRegex with PainlessScript {
    override def painless: String = ".format"
  }

  case class FormatDate(identifier: Identifier, format: String)
      extends DateFunction
      with TransformFunction[SQLDate, SQLVarchar]
      with FunctionWithIdentifier {
    override def fun: Option[PainlessScript] = Some(FormatDate)

    override def args: List[PainlessScript] = List.empty

    override def inputType: SQLDate = SQLTypes.Date
    override def outputType: SQLVarchar = SQLTypes.Varchar

    override def sql: String = FormatDate.sql
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

  case object DateTimeAdd extends Expr("datetime_add") with TokenRegex

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

  case object DateTimeSub extends Expr("datetime_sub") with TokenRegex

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

  case object ParseDateTime extends Expr("parse_datetime") with TokenRegex with PainlessScript {
    override def painless: String = ".parse"
  }

  case class ParseDateTime(identifier: Identifier, format: String)
      extends DateTimeFunction
      with TransformFunction[SQLVarchar, SQLDateTime]
      with FunctionWithIdentifier {
    override def fun: Option[PainlessScript] = Some(ParseDateTime)

    override def args: List[PainlessScript] = List.empty

    override def inputType: SQLVarchar = SQLTypes.Varchar
    override def outputType: SQLDateTime = SQLTypes.DateTime

    override def sql: String = ParseDateTime.sql
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

  case object FormatDateTime extends Expr("format_datetime") with TokenRegex with PainlessScript {
    override def painless: String = ".format"
  }

  case class FormatDateTime(identifier: Identifier, format: String)
      extends DateTimeFunction
      with TransformFunction[SQLDateTime, SQLVarchar]
      with FunctionWithIdentifier {
    override def fun: Option[PainlessScript] = Some(FormatDateTime)

    override def args: List[PainlessScript] = List.empty

    override def inputType: SQLDateTime = SQLTypes.DateTime
    override def outputType: SQLVarchar = SQLTypes.Varchar

    override def sql: String = FormatDateTime.sql
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
