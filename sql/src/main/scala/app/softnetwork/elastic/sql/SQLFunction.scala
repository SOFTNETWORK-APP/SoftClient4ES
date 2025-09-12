package app.softnetwork.elastic.sql

import scala.util.Try
import scala.util.matching.Regex

sealed trait SQLFunction extends SQLRegex {
  def toSQL(base: String): String = if (base.nonEmpty) s"$sql($base)" else sql
  def applyType(in: SQLType): SQLType = out
  var expr: SQLToken = _
  def applyTo(expr: SQLToken): Unit = {
    this.expr = expr
  }
  override def nullable: Boolean = Try(expr.nullable).getOrElse(true)
}

sealed trait SQLFunctionWithIdentifier extends SQLFunction {
  def identifier: SQLIdentifier
}

trait SQLFunctionWithValue[+T] extends SQLFunction {
  def value: T
}

object SQLFunctionUtils {
  def aggregateAndTransformFunctions(
    chain: SQLFunctionChain
  ): (List[SQLFunction], List[SQLFunction]) = {
    chain.functions.partition {
      case _: AggregateFunction => true
      case _                    => false
    }
  }

  def transformFunctions(chain: SQLFunctionChain): List[SQLFunction] = {
    aggregateAndTransformFunctions(chain)._2
  }

}

trait SQLFunctionChain extends SQLFunction {
  def functions: List[SQLFunction]

  override def validate(): Either[String, Unit] = {
    if (aggregations.size > 1) {
      Left("Only one aggregation function is allowed in a function chain")
    } else if (aggregations.size == 1 && !functions.head.isInstanceOf[AggregateFunction]) {
      Left("Aggregation function must be the first function in the chain")
    } else {
      SQLValidator.validateChain(functions)
    }
  }

  override def toSQL(base: String): String =
    functions.reverse.foldLeft(base)((expr, fun) => {
      fun.toSQL(expr)
    })

  def toScript: Option[String] = {
    val orderedFunctions = SQLFunctionUtils.transformFunctions(this).reverse
    orderedFunctions.foldLeft(Option("")) {
      case (expr, f: MathScript) if expr.isDefined => Option(s"${expr.get}${f.script}")
      case (_, _)                                  => None // ignore non math scripts
    } match {
      case Some(s) if s.nonEmpty =>
        out match {
          case SQLTypes.Date => Some(s"$s/d")
          case _             => Some(s)
        }
      case _ => None
    }
  }

  override def system: Boolean = functions.lastOption.exists(_.system)

  override def applyTo(expr: SQLToken): Unit = {
    super.applyTo(expr)
    val orderedFunctions = functions.reverse
    orderedFunctions.foldLeft(expr) { (currentExpr, fun) =>
      fun.applyTo(currentExpr)
      fun
    }
  }

  private[this] lazy val aggregations = functions.collect { case af: AggregateFunction =>
    af
  }

  lazy val aggregateFunction: Option[AggregateFunction] = aggregations.headOption

  lazy val aggregation: Boolean = aggregateFunction.isDefined

  override def in: SQLType = functions.lastOption.map(_.in).getOrElse(super.in)

  override def out: SQLType = {
    val baseType = functions.lastOption.map(_.in).getOrElse(super.baseType)
    functions.reverse.foldLeft(baseType) { (currentType, fun) =>
      fun.applyType(currentType)
    }
  }

}

sealed trait SQLUnaryFunction[In <: SQLType, Out <: SQLType]
    extends SQLFunction
    with PainlessScript {
  def inputType: In
  def outputType: Out
  override def in: SQLType = inputType
  override def out: SQLType = outputType
  override def applyType(in: SQLType): SQLType = outputType
}

sealed trait SQLBinaryFunction[In1 <: SQLType, In2 <: SQLType, Out <: SQLType]
    extends SQLUnaryFunction[SQLAny, Out] { self: SQLFunction =>

  override def inputType: SQLAny = SQLTypes.Any

  def left: PainlessScript
  def right: PainlessScript

  override def nullable: Boolean = left.nullable || right.nullable
}

sealed trait SQLTransformFunction[In <: SQLType, Out <: SQLType] extends SQLUnaryFunction[In, Out] {
  def toPainless(base: String, idx: Int): String = {
    if (nullable)
      s"(def e$idx = $base; e$idx != null ? e$idx$painless : null)"
    else
      s"$base$painless"
  }
}

sealed trait SQLArithmeticFunction[In <: SQLType, Out <: SQLType]
    extends SQLTransformFunction[In, Out]
    with MathScript {
  def operator: ArithmeticOperator
  override def toSQL(base: String): String = s"$base$operator$sql"
  override def applyType(in: SQLType): SQLType = in
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

  def applyType(in: SQLType): Either[String, SQLType] = {
    import TimeUnit._
    in match {
      case SQLTypes.Date =>
        unit match {
          case Year | Month | Day     => Right(SQLTypes.Date)
          case Hour | Minute | Second => Right(SQLTypes.Timestamp)
          case _                      => Left(s"Invalid interval unit $unit for DATE")
        }
      case SQLTypes.Time =>
        unit match {
          case Hour | Minute | Second => Right(SQLTypes.Time)
          case _                      => Left(s"Invalid interval unit $unit for TIME")
        }
      case SQLTypes.DateTime =>
        Right(SQLTypes.Timestamp)
      case SQLTypes.Timestamp =>
        Right(SQLTypes.Timestamp)
      case SQLTypes.Temporal =>
        Right(SQLTypes.Timestamp)
      case _ =>
        Left(s"Intervals not supported for type $in")
    }
  }
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

sealed trait SQLIntervalFunction extends SQLArithmeticFunction[SQLDateTime, SQLDateTime] {
  def interval: TimeInterval
  override def inputType: SQLDateTime = SQLTypes.DateTime
  override def outputType: SQLDateTime = SQLTypes.DateTime
  override def script: String = s"${operator.script}${interval.script}"

  override def applyType(in: SQLType): SQLType = interval.applyType(in).getOrElse(out)

  override def validate(): Either[String, Unit] = interval.applyType(out) match {
    case Left(err) => Left(err)
    case Right(_)  => Right(())
  }
}

case class SQLAddInterval(interval: TimeInterval)
    extends SQLExpr(interval.sql)
    with SQLIntervalFunction {
  override def operator: ArithmeticOperator = Add
  override def painless: String = s".plus(${interval.painless})"
}

case class SQLSubtractInterval(interval: TimeInterval)
    extends SQLExpr(interval.sql)
    with SQLIntervalFunction {
  override def operator: ArithmeticOperator = Subtract
  override def painless: String = s".minus(${interval.painless})"
}

sealed trait DateTimeFunction extends SQLFunction {
  def now: String = "ZonedDateTime.now(ZoneId.of('Z'))"
  override def out: SQLType = SQLTypes.DateTime
}

sealed trait DateFunction extends DateTimeFunction {
  override def out: SQLType = SQLTypes.Date
}

sealed trait TimeFunction extends DateTimeFunction {
  override def out: SQLType = SQLTypes.Time
}

sealed trait SystemFunction extends SQLFunction {
  override def system: Boolean = true
}

sealed trait CurrentFunction extends SystemFunction with PainlessScript

sealed trait CurrentDateTimeFunction extends DateTimeFunction with CurrentFunction with MathScript {
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

case class DateTrunc(identifier: SQLIdentifier, unit: TimeUnit)
    extends SQLExpr("date_trunc")
    with DateTimeFunction
    with SQLTransformFunction[SQLTemporal, SQLTemporal]
    with SQLFunctionWithIdentifier {
  override def inputType: SQLTemporal = SQLTypes.Temporal // par défaut
  override def outputType: SQLTemporal = SQLTypes.Temporal // idem
  override def toSQL(base: String): String = {
    s"$sql($base, ${unit.sql})"
  }
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
  override def painless: String = {
    if (start.nullable && end.nullable)
      s"(def s = ${start.painless}; def e = ${end.painless}; s != null && e != null ? ${unit.painless}.between(s, e) : null)"
    else if (start.nullable)
      s"(def s = ${start.painless}; s != null ? ${unit.painless}.between(s, ${end.painless}) : null)"
    else if (end.nullable)
      s"(def e = ${end.painless}; e != null ? ${unit.painless}.between(${start.painless}, e) : null)"
    else
      s"${unit.painless}.between(${start.painless}, ${end.painless})"
  }
}

case class DateAdd(identifier: SQLIdentifier, interval: TimeInterval)
    extends SQLExpr("date_add")
    with DateFunction
    with SQLTransformFunction[SQLDate, SQLDate]
    with SQLFunctionWithIdentifier {
  override def inputType: SQLDate = SQLTypes.Date
  override def outputType: SQLDate = SQLTypes.Date
  override def toSQL(base: String): String = {
    s"$sql($base, ${interval.sql})"
  }
  override def painless: String = s".plus(${interval.painless})"
}

case class DateSub(identifier: SQLIdentifier, interval: TimeInterval)
    extends SQLExpr("date_sub")
    with DateFunction
    with SQLTransformFunction[SQLDate, SQLDate]
    with SQLFunctionWithIdentifier {
  override def inputType: SQLDate = SQLTypes.Date
  override def outputType: SQLDate = SQLTypes.Date
  override def toSQL(base: String): String = {
    s"$sql($base, ${interval.sql})"
  }
  override def painless: String = s".minus(${interval.painless})"
}

case class ParseDate(identifier: SQLIdentifier, format: String)
    extends SQLExpr("parse_date")
    with DateFunction
    with SQLTransformFunction[SQLString, SQLDate]
    with SQLFunctionWithIdentifier {
  override def inputType: SQLString = SQLTypes.String
  override def outputType: SQLDate = SQLTypes.Date
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

case class FormatDate(identifier: SQLIdentifier, format: String)
    extends SQLExpr("format_date")
    with DateFunction
    with SQLTransformFunction[SQLDate, SQLString]
    with SQLFunctionWithIdentifier {
  override def inputType: SQLDate = SQLTypes.Date
  override def outputType: SQLString = SQLTypes.String
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

case class DateTimeAdd(identifier: SQLIdentifier, interval: TimeInterval)
    extends SQLExpr("datetime_add")
    with DateTimeFunction
    with SQLTransformFunction[SQLDateTime, SQLDateTime]
    with SQLFunctionWithIdentifier {
  override def inputType: SQLDateTime = SQLTypes.DateTime
  override def outputType: SQLDateTime = SQLTypes.DateTime
  override def toSQL(base: String): String = {
    s"$sql($base, ${interval.sql})"
  }
  override def painless: String = s".plus(${interval.painless})"
}

case class DateTimeSub(identifier: SQLIdentifier, interval: TimeInterval)
    extends SQLExpr("datetime_sub")
    with DateTimeFunction
    with SQLTransformFunction[SQLDateTime, SQLDateTime]
    with SQLFunctionWithIdentifier {
  override def inputType: SQLDateTime = SQLTypes.DateTime
  override def outputType: SQLDateTime = SQLTypes.DateTime
  override def toSQL(base: String): String = {
    s"$sql($base, ${interval.sql})"
  }
  override def painless: String = s".minus(${interval.painless})"
}

case class ParseDateTime(identifier: SQLIdentifier, format: String)
    extends SQLExpr("parse_datetime")
    with DateTimeFunction
    with SQLTransformFunction[SQLString, SQLDateTime]
    with SQLFunctionWithIdentifier {
  override def inputType: SQLString = SQLTypes.String
  override def outputType: SQLDateTime = SQLTypes.DateTime
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

case class FormatDateTime(identifier: SQLIdentifier, format: String)
    extends SQLExpr("format_datetime")
    with DateTimeFunction
    with SQLTransformFunction[SQLDateTime, SQLString]
    with SQLFunctionWithIdentifier {
  override def inputType: SQLDateTime = SQLTypes.DateTime
  override def outputType: SQLString = SQLTypes.String
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

sealed trait SQLConditionalFunction[In <: SQLType]
    extends SQLTransformFunction[In, SQLBool]
    with SQLFunctionWithIdentifier {
  def operator: SQLConditionalOperator
  override def outputType: SQLBool = SQLTypes.Boolean
  override def toPainless(base: String, idx: Int): String = s"($base$painless)"
}

case class SQLIsNullFunction(identifier: SQLIdentifier)
    extends SQLExpr("isnull")
    with SQLConditionalFunction[SQLAny] {
  override def operator: SQLConditionalOperator = IsNull
  override def inputType: SQLAny = SQLTypes.Any
  override def painless: String = s" == null"
  override def toPainless(base: String, idx: Int): String = {
    if (nullable)
      s"(def e$idx = $base; e$idx$painless)"
    else
      s"$base$painless"
  }
}

case class SQLIsNotNullFunction(identifier: SQLIdentifier)
    extends SQLExpr("isnotnull")
    with SQLConditionalFunction[SQLAny] {
  override def operator: SQLConditionalOperator = IsNotNull
  override def inputType: SQLAny = SQLTypes.Any
  override def painless: String = s" != null"
  override def toPainless(base: String, idx: Int): String = {
    if (nullable)
      s"(def e$idx = $base; e$idx$painless)"
    else
      s"$base$painless"
  }
}

case class SQLCoalesce(values: List[PainlessScript]) extends SQLConditionalFunction[SQLAny] {
  override def operator: SQLConditionalOperator = Coalesce

  override def identifier: SQLIdentifier = SQLIdentifier("")

  override def inputType: SQLAny = SQLTypes.Any

  override lazy val sql: String = s"$Coalesce(${values.map(_.sql).mkString(", ")})"

  // Reprend l’idée de SQLValues mais pour n’importe quel token
  override def out: SQLType = SQLTypeUtils.leastCommonSuperType(values.map(_.out).distinct)

  override def applyType(in: SQLType): SQLType = out

  override def validate(): Either[String, Unit] = {
    if (values.isEmpty) Left("COALESCE requires at least one argument")
    else Right(())
  }

  override def toPainless(base: String, idx: Int): String = s"$base$painless"

  override def painless: String = {
    require(values.nonEmpty, "COALESCE requires at least one argument")

    val checks = values
      .take(values.length - 1)
      .zipWithIndex
      .map { case (v, index) =>
        var check = s"def v$index = ${SQLTypeUtils.coerce(v, out)};"
        check += s"if (v$index != null) return v$index;"
        check
      }
      .mkString(" ")
    // final fallback
    s"{ $checks return ${SQLTypeUtils.coerce(values.last, out)}; }"
  }

  override def nullable: Boolean = values.forall(_.nullable)
}

case class SQLNullIf(expr1: PainlessScript, expr2: PainlessScript)
    extends SQLConditionalFunction[SQLAny] {
  override def operator: SQLConditionalOperator = NullIf

  override def identifier: SQLIdentifier = SQLIdentifier("")

  override def inputType: SQLAny = SQLTypes.Any

  override def sql: String = s"$NullIf(${expr1.sql}, ${expr2.sql})"

  override def out: SQLType = expr1.out

  override def applyType(in: SQLType): SQLType = out

  override def painless: String = {
    val e1 = expr1.painless
    val e2 = expr2.painless
    s"""{ def e1 = $e1;
       |def e2 = $e2;
       |return e1 == e2 ? null : e1;
       |}""".stripMargin.replaceAll("\n", " ")
  }
}

case class SQLCast(value: PainlessScript, targetType: SQLType, as: Boolean = true)
    extends SQLTransformFunction[SQLType, SQLType] {
  override def inputType: SQLType = value.out
  override def outputType: SQLType = targetType

  override def sql: String =
    s"$Cast(${value.sql} ${if (as) s"$Alias " else ""}${targetType.typeId})"

  override def painless: String = SQLTypeUtils.coerce(value, targetType)
}
