package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.{
  DateMathRounding,
  DateMathScript,
  Expr,
  Identifier,
  PainlessContext,
  PainlessScript,
  StringValue,
  TokenRegex
}
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
import app.softnetwork.elastic.sql.time.{IsoField, TimeField, TimeInterval, TimeUnit}

package object time {

  sealed trait IntervalFunction[IO <: SQLTemporal]
      extends TransformFunction[IO, IO]
      with DateMathScript {
    def operator: IntervalOperator

    override def fun: Option[IntervalOperator] = Some(operator)

    def interval: TimeInterval

    override def args: List[PainlessScript] = List(interval)

    override def argsSeparator: String = " "
    override def sql: String = s"$operator${args.map(_.sql).mkString(argsSeparator)}"

    override def script: Option[String] = (operator.script, interval.script) match {
      case (Some(op), Some(iv)) => Some(s"$op$iv")
      case _                    => None
    }

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
        s"(def e$idx = $base; e$idx != null ? ${SQLTypeUtils.coerce(s"e$idx", expr.baseType, out, nullable = false)}${painless()} : null)"
      else
        s"${SQLTypeUtils.coerce(base, expr.baseType, out, nullable = expr.nullable)}${painless()}"
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
    override def baseType: SQLType = SQLTypes.DateTime
  }

  sealed trait DateFunction extends DateTimeFunction {
    override def baseType: SQLType = SQLTypes.Date
  }

  sealed trait TimeFunction extends DateTimeFunction {
    override def baseType: SQLType = SQLTypes.Time
  }

  sealed trait SystemFunction extends Function {
    override def system: Boolean = true
  }

  sealed trait CurrentFunction extends SystemFunction with PainlessScript with DateMathScript {
    override def script: Option[String] = Some("now")
  }

  sealed trait CurrentDateTimeFunction extends DateTimeFunction with CurrentFunction {
    override def painless(context: Option[PainlessContext]): String =
      SQLTypeUtils.coerce(now, this.baseType, this.out, nullable = false)
  }

  sealed trait CurrentDateFunction extends DateFunction with CurrentFunction {
    override def painless(context: Option[PainlessContext]): String =
      SQLTypeUtils.coerce(s"$now.toLocalDate()", this.baseType, this.out, nullable = false)
  }

  sealed trait CurrentTimeFunction extends TimeFunction with CurrentFunction {
    override def painless(context: Option[PainlessContext]): String =
      SQLTypeUtils.coerce(s"$now.toLocalTime()", this.baseType, this.out, nullable = false)
  }

  case object CurrentDate extends Expr("CURRENT_DATE") with TokenRegex {
    override lazy val words: List[String] = List(sql, "CURDATE")
  }

  case class CurrentDate(parens: Boolean = false) extends CurrentDateFunction {
    override def sql: String =
      if (parens) s"$CurrentDate()"
      else CurrentDate.sql
  }

  case object CurrentTime extends Expr("CURRENT_TIME") with TokenRegex {
    override lazy val words: List[String] = List(sql, "CURTIME")
  }

  case class CurrentTime(parens: Boolean = false) extends CurrentTimeFunction {
    override def sql: String =
      if (parens) s"$CurrentTime()"
      else CurrentTime.sql
  }

  case object CurrentTimestamp extends Expr("CURRENT_TIMESTAMP") with TokenRegex

  case class CurrentTimestamp(parens: Boolean = false) extends CurrentDateTimeFunction {
    override def sql: String =
      if (parens) s"$CurrentTimestamp()"
      else CurrentTimestamp.sql
  }

  case object Now extends Expr("NOW") with TokenRegex

  case class Now(parens: Boolean = false) extends CurrentDateTimeFunction {
    override def sql: String = if (parens) s"$Now()" else Now.sql
  }

  case object Today extends Expr("TODAY") with TokenRegex

  case class Today(parens: Boolean = false) extends CurrentDateFunction {
    override def sql: String =
      if (parens) s"$Today()"
      else Today.sql
  }

  case object DateTrunc extends Expr("DATE_TRUNC") with TokenRegex with PainlessScript {
    override def painless(context: Option[PainlessContext]): String = ".truncatedTo"
    override lazy val words: List[String] = List(sql, "DATETRUNC")
  }

  case class DateTrunc(identifier: Identifier, unit: TimeUnit)
      extends DateTimeFunction
      with TransformFunction[SQLTemporal, SQLTemporal]
      with FunctionWithIdentifier
      with DateMathRounding {
    override def fun: Option[PainlessScript] = Some(DateTrunc)

    override def args: List[PainlessScript] = List(unit)

    override def inputType: SQLTemporal = SQLTypes.Temporal // par défaut
    override def outputType: SQLTemporal = SQLTypes.Temporal // idem

    override def sql: String = DateTrunc.sql
    override def toSQL(base: String): String = {
      s"$sql($base, ${unit.sql})"
    }

    override def roundingScript: Option[String] = unit.roundingScript

    override def dateMathScript: Boolean = identifier.dateMathScript
  }

  case object Extract extends Expr("EXTRACT") with TokenRegex with PainlessScript {
    override def painless(context: Option[PainlessContext]): String = ".get"
  }

  case class Extract(field: TimeField)
      extends DateTimeFunction
      with TransformFunction[SQLTemporal, SQLNumeric] {

    override val sql: String = Extract.sql

    override def fun: Option[PainlessScript] = Some(Extract)

    override def args: List[PainlessScript] = List(field)

    override def inputType: SQLTemporal = SQLTypes.Temporal
    override def outputType: SQLNumeric = SQLTypes.Numeric

    override def toSQL(base: String): String = s"$sql(${field.sql} FROM $base)"

  }

  import TimeField._

  sealed abstract class TimeFieldExtract(field: TimeField) extends Extract(field) {
    override val sql: String = field.sql
    override def toSQL(base: String): String = s"$sql($base)"
  }

  class Year extends TimeFieldExtract(YEAR)

  class MonthOfYear extends TimeFieldExtract(MONTH_OF_YEAR)

  class DayOfMonth extends TimeFieldExtract(DAY_OF_MONTH)

  class DayOfWeek extends TimeFieldExtract(DAY_OF_WEEK)

  class DayOfYear extends TimeFieldExtract(DAY_OF_YEAR)

  class HourOfDay extends TimeFieldExtract(HOUR_OF_DAY)

  class MinuteOfHour extends TimeFieldExtract(MINUTE_OF_HOUR)

  class SecondOfMinute extends TimeFieldExtract(SECOND_OF_MINUTE)

  class NanoOfSecond extends TimeFieldExtract(NANO_OF_SECOND)

  class MicroOfSecond extends TimeFieldExtract(MICRO_OF_SECOND)

  class MilliOfSecond extends TimeFieldExtract(MILLI_OF_SECOND)

  class EpochDay extends TimeFieldExtract(EPOCH_DAY)

  class OffsetSeconds extends TimeFieldExtract(OFFSET_SECONDS)

  import IsoField._

  class QuarterOfYear extends TimeFieldExtract(QUARTER_OF_YEAR)

  class WeekOfWeekBasedYear extends TimeFieldExtract(WEEK_OF_WEEK_BASED_YEAR)

  case object LastDayOfMonth extends Expr("LAST_DAY") with TokenRegex with PainlessScript {
    override def painless(context: Option[PainlessContext]): String = ".withDayOfMonth"
    override lazy val words: List[String] = List(sql, "LASTDAY")
  }

  case class LastDayOfMonth(identifier: Identifier)
      extends DateFunction
      with TransformFunction[SQLDate, SQLDate]
      with FunctionWithIdentifier {
    override def fun: Option[PainlessScript] = Some(LastDayOfMonth)

    override def args: List[PainlessScript] = List(identifier)

    override def inputType: SQLDate = SQLTypes.Date
    override def outputType: SQLDate = SQLTypes.Date

    override def nullable: Boolean = identifier.nullable

    override def sql: String = LastDayOfMonth.sql

    override def toSQL(base: String): String = {
      s"$sql($base)"
    }

    override def toPainless(base: String, idx: Int): String = {
      val arg = SQLTypeUtils.coerce(base, identifier.baseType, SQLTypes.Date, nullable = false)
      if (nullable && base.nonEmpty)
        s"(def e$idx = $arg; e$idx != null ? ${toPainlessCall(List(s"e$idx"))} : null)"
      else
        s"(def e$idx = $arg; ${toPainlessCall(List(s"e$idx"))})"
    }

    override def toPainlessCall(callArgs: List[String]): String = {
      callArgs match {
        case arg :: Nil => s"$arg${LastDayOfMonth.painless()}($arg.lengthOfMonth())"
        case _ => throw new IllegalArgumentException("LastDayOfMonth requires exactly one argument")
      }
    }

  }

  case object DateDiff extends Expr("DATE_DIFF") with TokenRegex with PainlessScript {
    override def painless(context: Option[PainlessContext]): String = ".between"
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
      s"${unit.painless()}${DateDiff.painless()}(${callArgs.mkString(", ")})"
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
    override def dateMathScript: Boolean = identifier.dateMathScript
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
    override def dateMathScript: Boolean = identifier.dateMathScript
  }

  sealed trait FunctionWithDateTimeFormat {
    def format: String

    val sqlToJava: Map[String, String] = Map(
      "%Y" -> "yyyy",
      "%y" -> "yy",
      "%m" -> "MM",
      "%c" -> "M",
      "%d" -> "dd",
      "%e" -> "d",
      "%H" -> "HH",
      "%k" -> "H",
      "%h" -> "hh",
      "%I" -> "hh",
      "%l" -> "h",
      "%i" -> "mm",
      "%s" -> "ss",
      "%S" -> "ss",
      "%f" -> "SSS", // microseconds
      "%p" -> "a",
      "%W" -> "EEEE",
      "%a" -> "EEE",
      "%M" -> "MMMM",
      "%b" -> "MMM",
      "%T" -> "HH:mm:ss",
      "%r" -> "hh:mm:ss a",
      "%j" -> "DDD",
      "%x" -> "YY",
      "%X" -> "YYYY"
    )

    def convert(includeTimeZone: Boolean = false): String = {
      val basePattern = sqlToJava.foldLeft(format) { case (pattern, (sql, java)) =>
        pattern.replace(sql, java)
      }

      val patternWithTZ =
        if (basePattern.contains("Z")) basePattern.replace("Z", "X")
        else if (includeTimeZone) s"$basePattern XXX"
        else basePattern

      patternWithTZ
    }
  }

  case object DateParse extends Expr("DATE_PARSE") with TokenRegex with PainlessScript {
    override def painless(context: Option[PainlessContext]): String = ".parse"
  }

  case class DateParse(identifier: Identifier, format: String)
      extends DateFunction
      with TransformFunction[SQLVarchar, SQLDate]
      with FunctionWithIdentifier
      with FunctionWithDateTimeFormat
      with DateMathScript {
    override def fun: Option[PainlessScript] = Some(DateParse)

    override def args: List[PainlessScript] = List.empty

    override def inputType: SQLVarchar = SQLTypes.Varchar
    override def outputType: SQLDate = SQLTypes.Date

    override def sql: String = DateParse.sql
    override def toSQL(base: String): String = {
      s"$sql($base, '$format')"
    }

    override def painless(context: Option[PainlessContext]): String = throw new NotImplementedError(
      "Use toPainless instead"
    )
    override def toPainless(base: String, idx: Int): String =
      if (nullable)
        s"(def e$idx = $base; e$idx != null ? DateTimeFormatter.ofPattern('${convert()}').parse(e$idx, LocalDate::from) : null)"
      else
        s"DateTimeFormatter.ofPattern('${convert()}').parse($base, LocalDate::from)"

    override def script: Option[String] = {
      val base: String = FunctionUtils
        .transformFunctions(identifier)
        .reverse
        .collectFirst { case s: StringValue => s.value }
        .getOrElse(identifier.name)
      if (base.nonEmpty) {
        Some(s"$base||")
      } else {
        None
      }
    }

    override def formatScript: Option[String] = Some(format)
  }

  case object DateFormat extends Expr("DATE_FORMAT") with TokenRegex with PainlessScript {
    override def painless(context: Option[PainlessContext]): String = ".format"
  }

  case class DateFormat(identifier: Identifier, format: String)
      extends DateFunction
      with TransformFunction[SQLDate, SQLVarchar]
      with FunctionWithIdentifier
      with FunctionWithDateTimeFormat {
    override def fun: Option[PainlessScript] = Some(DateFormat)

    override def args: List[PainlessScript] = List.empty

    override def inputType: SQLDate = SQLTypes.Date
    override def outputType: SQLVarchar = SQLTypes.Varchar

    override def sql: String = DateFormat.sql
    override def toSQL(base: String): String = {
      s"$sql($base, '$format')"
    }

    override def painless(context: Option[PainlessContext]): String = throw new NotImplementedError(
      "Use toPainless instead"
    )
    override def toPainless(base: String, idx: Int): String =
      if (nullable)
        s"(def e$idx = $base; e$idx != null ? DateTimeFormatter.ofPattern('${convert()}').format(e$idx) : null)"
      else
        s"DateTimeFormatter.ofPattern('${convert()}').format($base)"
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
    override def dateMathScript: Boolean = identifier.dateMathScript
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
    override def dateMathScript: Boolean = identifier.dateMathScript
  }

  case object DateTimeParse extends Expr("DATETIME_PARSE") with TokenRegex with PainlessScript {
    override def painless(context: Option[PainlessContext]): String = ".parse"
  }

  case class DateTimeParse(identifier: Identifier, format: String)
      extends DateTimeFunction
      with TransformFunction[SQLVarchar, SQLDateTime]
      with FunctionWithIdentifier
      with FunctionWithDateTimeFormat
      with DateMathScript {
    override def fun: Option[PainlessScript] = Some(DateTimeParse)

    override def args: List[PainlessScript] = List.empty

    override def inputType: SQLVarchar = SQLTypes.Varchar
    override def outputType: SQLDateTime = SQLTypes.DateTime

    override def sql: String = DateTimeParse.sql
    override def toSQL(base: String): String = {
      s"$sql($base, '$format')"
    }

    override def painless(context: Option[PainlessContext]): String = throw new NotImplementedError(
      "Use toPainless instead"
    )
    override def toPainless(base: String, idx: Int): String =
      if (nullable)
        s"(def e$idx = $base; e$idx != null ? DateTimeFormatter.ofPattern('${convert(includeTimeZone = true)}').parse(e$idx, ZonedDateTime::from) : null)"
      else
        s"DateTimeFormatter.ofPattern('${convert(includeTimeZone = true)}').parse($base, ZonedDateTime::from)"

    override def script: Option[String] = {
      val base: String = FunctionUtils
        .transformFunctions(identifier)
        .reverse
        .collectFirst { case s: StringValue => s.value }
        .getOrElse(identifier.name)
      if (base.nonEmpty) {
        Some(s"$base||")
      } else {
        None
      }
    }

    override def formatScript: Option[String] = Some(format)
  }

  case object DateTimeFormat extends Expr("DATETIME_FORMAT") with TokenRegex with PainlessScript {
    override def painless(context: Option[PainlessContext]): String = ".format"
  }

  case class DateTimeFormat(identifier: Identifier, format: String)
      extends DateTimeFunction
      with TransformFunction[SQLDateTime, SQLVarchar]
      with FunctionWithIdentifier
      with FunctionWithDateTimeFormat {
    override def fun: Option[PainlessScript] = Some(DateTimeFormat)

    override def args: List[PainlessScript] = List.empty

    override def inputType: SQLDateTime = SQLTypes.DateTime
    override def outputType: SQLVarchar = SQLTypes.Varchar

    override def sql: String = DateTimeFormat.sql
    override def toSQL(base: String): String = {
      s"$sql($base, '$format')"
    }

    override def painless(context: Option[PainlessContext]): String = throw new NotImplementedError(
      "Use toPainless instead"
    )
    override def toPainless(base: String, idx: Int): String =
      if (nullable)
        s"(def e$idx = $base; e$idx != null ? DateTimeFormatter.ofPattern('${convert(includeTimeZone = true)}').format(e$idx) : null)"
      else
        s"DateTimeFormatter.ofPattern('${convert(includeTimeZone = true)}').format($base)"
  }

}
