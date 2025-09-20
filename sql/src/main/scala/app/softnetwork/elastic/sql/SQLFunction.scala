package app.softnetwork.elastic.sql

import scala.util.matching.Regex

sealed trait SQLFunction extends SQLRegex {
  def toSQL(base: String): String = if (base.nonEmpty) s"$sql($base)" else sql
  def applyType(in: SQLType): SQLType = out
  private[this] var _expr: SQLToken = SQLNull
  def expr_=(e: SQLToken): Unit = {
    _expr = e
  }
  def expr: SQLToken = _expr
  override def nullable: Boolean = expr.nullable
}

sealed trait SQLFunctionWithIdentifier extends SQLFunction {
  def identifier: SQLIdentifier //= SQLIdentifier("", functions = this :: Nil)
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

  def applyTo(expr: SQLToken): Unit = {
    this.expr = expr
    functions.reverse.foldLeft(expr) { (currentExpr, fun) =>
      fun.expr = currentExpr
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

  def arithmetic: Boolean = functions.nonEmpty && functions.forall {
    case _: SQLArithmeticExpression => true
    case _                          => false
  }
}

sealed trait SQLFunctionN[In <: SQLType, Out <: SQLType] extends SQLFunction with PainlessScript {
  def fun: Option[PainlessScript] = None

  def args: List[PainlessScript]
  def argsSeparator: String = ", "

  def inputType: In
  def outputType: Out

  override def in: SQLType = inputType
  override def out: SQLType = outputType

  override def applyType(in: SQLType): SQLType = outputType

  override def sql: String =
    s"${fun.map(_.sql).getOrElse("")}(${args.map(_.sql).mkString(argsSeparator)})"

  override def toSQL(base: String): String = s"$base$sql"

  override def painless: String = {
    val nullCheck =
      args.filter(_.nullable).zipWithIndex.map { case (_, i) => s"arg$i == null" }.mkString(" || ")

    val assignments =
      args
        .filter(_.nullable)
        .zipWithIndex
        .map { case (a, i) => s"def arg$i = ${a.painless};" }
        .mkString(" ")

    val callArgs = args.zipWithIndex
      .map { case (a, i) =>
        if (a.nullable)
          s"arg$i"
        else
          a.painless
      }

    if (args.exists(_.nullable))
      s"($assignments ($nullCheck) ? null : ${toPainlessCall(callArgs)})"
    else
      s"${toPainlessCall(callArgs)}"
  }

  def toPainlessCall(callArgs: List[String]): String =
    if (callArgs.nonEmpty)
      s"${fun.map(_.painless).getOrElse("")}(${callArgs.mkString(argsSeparator)})"
    else
      fun.map(_.painless).getOrElse("")
}

sealed trait SQLBinaryFunction[In1 <: SQLType, In2 <: SQLType, Out <: SQLType]
    extends SQLFunctionN[In2, Out] { self: SQLFunction =>

  def left: PainlessScript
  def right: PainlessScript

  override def args: List[PainlessScript] = List(left, right)

  override def nullable: Boolean = left.nullable || right.nullable
}

sealed trait SQLTransformFunction[In <: SQLType, Out <: SQLType] extends SQLFunctionN[In, Out] {
  def toPainless(base: String, idx: Int): String = {
    if (nullable && base.nonEmpty)
      s"(def e$idx = $base; e$idx != null ? e$idx$painless : null)"
    else
      s"$base$painless"
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

  override def nullable: Boolean = false
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

  def checkType(in: SQLType): Either[String, SQLType] = {
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

  override def nullable: Boolean = false
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

sealed trait SQLIntervalFunction[IO <: SQLTemporal]
    extends SQLTransformFunction[IO, IO]
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

sealed trait AddInterval[IO <: SQLTemporal] extends SQLIntervalFunction[IO] {
  override def operator: IntervalOperator = Plus
}

sealed trait SubtractInterval[IO <: SQLTemporal] extends SQLIntervalFunction[IO] {
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

case object DateTrunc extends SQLExpr("date_trunc") with SQLRegex with PainlessScript {
  override def painless: String = ".truncatedTo"
}

case class DateTrunc(identifier: SQLIdentifier, unit: TimeUnit)
    extends DateTimeFunction
    with SQLTransformFunction[SQLTemporal, SQLTemporal]
    with SQLFunctionWithIdentifier {
  override def fun: Option[PainlessScript] = Some(DateTrunc)

  override def args: List[PainlessScript] = List(unit)

  override def inputType: SQLTemporal = SQLTypes.Temporal // par défaut
  override def outputType: SQLTemporal = SQLTypes.Temporal // idem

  override def sql: String = DateTrunc.sql
  override def toSQL(base: String): String = {
    s"$sql($base, ${unit.sql})"
  }
}

case object Extract extends SQLExpr("extract") with SQLRegex with PainlessScript {
  override def painless: String = ".get"
}

case class Extract(unit: TimeUnit, override val sql: String = "extract")
    extends DateTimeFunction
    with SQLTransformFunction[SQLTemporal, SQLNumeric] {
  override def fun: Option[PainlessScript] = Some(Extract)

  override def args: List[PainlessScript] = List(unit)

  override def inputType: SQLTemporal = SQLTypes.Temporal
  override def outputType: SQLNumeric = SQLTypes.Numeric

  override def toSQL(base: String): String = s"$sql(${unit.sql} from $base)"

}

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

case object DateDiff extends SQLExpr("date_diff") with SQLRegex with PainlessScript {
  override def painless: String = ".between"
}

case class DateDiff(end: PainlessScript, start: PainlessScript, unit: TimeUnit)
    extends DateTimeFunction
    with SQLBinaryFunction[SQLDateTime, SQLDateTime, SQLNumeric]
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

case object DateAdd extends SQLExpr("date_add") with SQLRegex

case class DateAdd(identifier: SQLIdentifier, interval: TimeInterval)
    extends DateFunction
    with AddInterval[SQLDate]
    with SQLTransformFunction[SQLDate, SQLDate]
    with SQLFunctionWithIdentifier {
  override def inputType: SQLDate = SQLTypes.Date
  override def outputType: SQLDate = SQLTypes.Date
  override def sql: String = DateAdd.sql
  override def toSQL(base: String): String = {
    s"$sql($base, ${interval.sql})"
  }
}

case object DateSub extends SQLExpr("date_sub") with SQLRegex

case class DateSub(identifier: SQLIdentifier, interval: TimeInterval)
    extends DateFunction
    with SubtractInterval[SQLDate]
    with SQLTransformFunction[SQLDate, SQLDate]
    with SQLFunctionWithIdentifier {
  override def inputType: SQLDate = SQLTypes.Date
  override def outputType: SQLDate = SQLTypes.Date
  override def sql: String = DateSub.sql
  override def toSQL(base: String): String = {
    s"$sql($base, ${interval.sql})"
  }
}

case object ParseDate extends SQLExpr("parse_date") with SQLRegex with PainlessScript {
  override def painless: String = ".parse"
}

case class ParseDate(identifier: SQLIdentifier, format: String)
    extends DateFunction
    with SQLTransformFunction[SQLVarchar, SQLDate]
    with SQLFunctionWithIdentifier {
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

case object FormatDate extends SQLExpr("format_date") with SQLRegex with PainlessScript {
  override def painless: String = ".format"
}

case class FormatDate(identifier: SQLIdentifier, format: String)
    extends DateFunction
    with SQLTransformFunction[SQLDate, SQLVarchar]
    with SQLFunctionWithIdentifier {
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

case object DateTimeAdd extends SQLExpr("datetime_add") with SQLRegex

case class DateTimeAdd(identifier: SQLIdentifier, interval: TimeInterval)
    extends DateTimeFunction
    with AddInterval[SQLDateTime]
    with SQLTransformFunction[SQLDateTime, SQLDateTime]
    with SQLFunctionWithIdentifier {
  override def inputType: SQLDateTime = SQLTypes.DateTime
  override def outputType: SQLDateTime = SQLTypes.DateTime
  override def sql: String = DateTimeAdd.sql
  override def toSQL(base: String): String = {
    s"$sql($base, ${interval.sql})"
  }
}

case object DateTimeSub extends SQLExpr("datetime_sub") with SQLRegex

case class DateTimeSub(identifier: SQLIdentifier, interval: TimeInterval)
    extends DateTimeFunction
    with SubtractInterval[SQLDateTime]
    with SQLTransformFunction[SQLDateTime, SQLDateTime]
    with SQLFunctionWithIdentifier {
  override def inputType: SQLDateTime = SQLTypes.DateTime
  override def outputType: SQLDateTime = SQLTypes.DateTime
  override def sql: String = DateTimeSub.sql
  override def toSQL(base: String): String = {
    s"$sql($base, ${interval.sql})"
  }
}

case object ParseDateTime extends SQLExpr("parse_datetime") with SQLRegex with PainlessScript {
  override def painless: String = ".parse"
}

case class ParseDateTime(identifier: SQLIdentifier, format: String)
    extends DateTimeFunction
    with SQLTransformFunction[SQLVarchar, SQLDateTime]
    with SQLFunctionWithIdentifier {
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

case object FormatDateTime extends SQLExpr("format_datetime") with SQLRegex with PainlessScript {
  override def painless: String = ".format"
}

case class FormatDateTime(identifier: SQLIdentifier, format: String)
    extends DateTimeFunction
    with SQLTransformFunction[SQLDateTime, SQLVarchar]
    with SQLFunctionWithIdentifier {
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

sealed trait SQLConditionalFunction[In <: SQLType]
    extends SQLTransformFunction[In, SQLBool]
    with SQLFunctionWithIdentifier {
  def operator: SQLConditionalOperator

  override def fun: Option[PainlessScript] = Some(operator)

  override def outputType: SQLBool = SQLTypes.Boolean

  override def toPainless(base: String, idx: Int): String = s"($base$painless)"
}

case class SQLIsNullFunction(identifier: SQLIdentifier) extends SQLConditionalFunction[SQLAny] {
  override def operator: SQLConditionalOperator = IsNullFunction

  override def args: List[PainlessScript] = List(identifier)

  override def inputType: SQLAny = SQLTypes.Any

  override def toSQL(base: String): String = sql

  override def painless: String = s" == null"
  override def toPainless(base: String, idx: Int): String = {
    if (nullable)
      s"(def e$idx = $base; e$idx$painless)"
    else
      s"$base$painless"
  }
}

case class SQLIsNotNullFunction(identifier: SQLIdentifier) extends SQLConditionalFunction[SQLAny] {
  override def operator: SQLConditionalOperator = IsNotNullFunction

  override def args: List[PainlessScript] = List(identifier)

  override def inputType: SQLAny = SQLTypes.Any

  override def toSQL(base: String): String = sql

  override def painless: String = s" != null"
  override def toPainless(base: String, idx: Int): String = {
    if (nullable)
      s"(def e$idx = $base; e$idx$painless)"
    else
      s"$base$painless"
  }
}

case class SQLCoalesce(values: List[PainlessScript])
    extends SQLTransformFunction[SQLAny, SQLType]
    with SQLFunctionWithIdentifier {
  def operator: SQLConditionalOperator = Coalesce

  override def fun: Option[SQLConditionalOperator] = Some(operator)

  override def args: List[PainlessScript] = values

  override def outputType: SQLType = SQLTypeUtils.leastCommonSuperType(args.map(_.out))

  override def identifier: SQLIdentifier = SQLIdentifier("")

  override def inputType: SQLAny = SQLTypes.Any

  override def sql: String = s"$Coalesce(${values.map(_.sql).mkString(", ")})"

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

  override def args: List[PainlessScript] = List(expr1, expr2)

  override def identifier: SQLIdentifier = SQLIdentifier("")

  override def inputType: SQLAny = SQLTypes.Any

  override def out: SQLType = expr1.out

  override def applyType(in: SQLType): SQLType = out

  override def toPainlessCall(callArgs: List[String]): String = {
    callArgs match {
      case List(arg0, arg1) => s"${arg0.trim} == ${arg1.trim} ? null : $arg0"
      case _ => throw new IllegalArgumentException("NULLIF requires exactly two arguments")
    }
  }
}

case class SQLCast(value: PainlessScript, targetType: SQLType, as: Boolean = true)
    extends SQLTransformFunction[SQLType, SQLType] {
  override def inputType: SQLType = value.out
  override def outputType: SQLType = targetType

  override def args: List[PainlessScript] = List.empty

  override def sql: String =
    s"$Cast(${value.sql} ${if (as) s"$Alias " else ""}${targetType.typeId})"

  override def toSQL(base: String): String = sql

  override def painless: String =
    SQLTypeUtils.coerce(value, targetType)

  override def toPainless(base: String, idx: Int): String =
    SQLTypeUtils.coerce(base, value.out, targetType, value.nullable)
}

case class SQLCaseWhen(
  expression: Option[PainlessScript],
  conditions: List[(PainlessScript, PainlessScript)],
  default: Option[PainlessScript]
) extends SQLTransformFunction[SQLAny, SQLAny] {
  override def args: List[PainlessScript] = List.empty

  override def inputType: SQLAny = SQLTypes.Any
  override def outputType: SQLAny = SQLTypes.Any

  override def sql: String = {
    val exprPart = expression.map(e => s"$Case ${e.sql}").getOrElse(Case.sql)
    val whenThen = conditions
      .map { case (cond, res) => s"$When ${cond.sql} $Then ${res.sql}" }
      .mkString(" ")
    val elsePart = default.map(d => s" $Else ${d.sql}").getOrElse("")
    s"$exprPart $whenThen$elsePart $End"
  }

  override def out: SQLType =
    SQLTypeUtils.leastCommonSuperType(
      conditions.map(_._2.out) ++ default.map(_.out).toList
    )

  override def applyType(in: SQLType): SQLType = out

  override def validate(): Either[String, Unit] = {
    if (conditions.isEmpty) Left("CASE WHEN requires at least one condition")
    else if (
      expression.isEmpty && conditions.exists { case (cond, _) => cond.out != SQLTypes.Boolean }
    )
      Left("CASE WHEN conditions must be of type BOOLEAN")
    else if (
      expression.isDefined && conditions.exists { case (cond, _) =>
        !SQLTypeUtils.matches(cond.out, expression.get.out)
      }
    )
      Left("CASE WHEN conditions must be of the same type as the expression")
    else Right(())
  }

  override def painless: String = {
    val base =
      expression match {
        case Some(expr) =>
          s"def expr = ${SQLTypeUtils.coerce(expr, expr.out)}; "
        case _ => ""
      }
    val cases = conditions.zipWithIndex
      .map { case ((cond, res), idx) =>
        val name =
          cond match {
            case e: Expression =>
              e.identifier.name
            case i: Identifier =>
              i.name
            case _ => ""
          }
        expression match {
          case Some(expr) =>
            val c = SQLTypeUtils.coerce(cond, expr.out)
            if (cond.sql == res.sql) {
              s"def val$idx = $c; if (expr == val$idx) return val$idx;"
            } else {
              res match {
                case i: Identifier if i.name == name && cond.isInstanceOf[Identifier] =>
                  i.nullable = false
                  if (cond.asInstanceOf[Identifier].functions.isEmpty)
                    s"def val$idx = $c; if (expr == val$idx) return ${SQLTypeUtils.coerce(i.toPainless(s"val$idx"), i.out, out, nullable = false)};"
                  else {
                    cond.asInstanceOf[Identifier].nullable = false
                    s"def e$idx = ${i.checkNotNull}; def val$idx = e$idx != null ? ${SQLTypeUtils
                      .coerce(cond.asInstanceOf[Identifier].toPainless(s"e$idx"), cond.out, out, nullable = false)} : null; if (expr == val$idx) return ${SQLTypeUtils
                      .coerce(i.toPainless(s"e$idx"), i.out, out, nullable = false)};"
                  }
                case _ =>
                  s"if (expr == $c) return ${SQLTypeUtils.coerce(res, out)};"
              }
            }
          case None =>
            val c = SQLTypeUtils.coerce(cond, SQLTypes.Boolean)
            val r =
              res match {
                case i: Identifier if i.name == name && cond.isInstanceOf[Expression] =>
                  i.nullable = false
                  SQLTypeUtils.coerce(i.toPainless("left"), i.out, out, nullable = false)
                case _ => SQLTypeUtils.coerce(res, out)
              }
            s"if ($c) return $r;"
        }
      }
      .mkString(" ")
    val defaultCase = default
      .map(d => s"def dval = ${SQLTypeUtils.coerce(d, out)}; return dval;")
      .getOrElse("return null;")
    s"{ $base$cases $defaultCase }"
  }

  override def toPainless(base: String, idx: Int): String = s"$base$painless"

  override def nullable: Boolean =
    conditions.exists { case (_, res) => res.nullable } || default.forall(_.nullable)
}

case class SQLArithmeticExpression(
  left: PainlessScript,
  operator: ArithmeticOperator,
  right: PainlessScript,
  group: Boolean = false
) extends SQLTransformFunction[SQLNumeric, SQLNumeric]
    with SQLBinaryFunction[SQLNumeric, SQLNumeric, SQLNumeric] {

  override def fun: Option[ArithmeticOperator] = Some(operator)

  override def inputType: SQLNumeric = SQLTypes.Numeric
  override def outputType: SQLNumeric = SQLTypes.Numeric

  override def applyType(in: SQLType): SQLType = in

  override def sql: String = {
    val expr = s"${left.sql}$operator${right.sql}"
    if (group)
      s"($expr)"
    else
      expr
  }

  override def out: SQLType =
    SQLTypeUtils.leastCommonSuperType(List(left.out, right.out))

  override def validate(): Either[String, Unit] = {
    for {
      _ <- left.validate()
      _ <- right.validate()
      _ <- SQLValidator.validateTypesMatching(left.out, right.out)
    } yield ()
  }

  override def nullable: Boolean = left.nullable || right.nullable

  override def toPainless(base: String, idx: Int): String = {
    if (nullable) {
      val l = left match {
        case t: SQLTransformFunction[_, _] =>
          SQLTypeUtils.coerce(t.toPainless("", idx + 1), left.out, out, nullable = false)
        case _ => SQLTypeUtils.coerce(left.painless, left.out, out, nullable = false)
      }
      val r = right match {
        case t: SQLTransformFunction[_, _] =>
          SQLTypeUtils.coerce(t.toPainless("", idx + 1), right.out, out, nullable = false)
        case _ => SQLTypeUtils.coerce(right.painless, right.out, out, nullable = false)
      }
      var expr = ""
      if (left.nullable)
        expr += s"def lv$idx = ($l); "
      if (right.nullable)
        expr += s"def rv$idx = ($r); "
      if (left.nullable && right.nullable)
        expr += s"(lv$idx == null || rv$idx == null) ? null : (lv$idx ${operator.painless} rv$idx)"
      else if (left.nullable)
        expr += s"(lv$idx == null) ? null : (lv$idx ${operator.painless} $r)"
      else
        expr += s"(rv$idx == null) ? null : ($l ${operator.painless} rv$idx)"
      if (group)
        expr = s"($expr)"
      return s"$base$expr"
    }
    s"$base$painless"
  }

  override def painless: String = {
    val l = SQLTypeUtils.coerce(left, out)
    val r = SQLTypeUtils.coerce(right, out)
    val expr = s"$l ${operator.painless} $r"
    if (group)
      s"($expr)"
    else
      expr
  }

}

sealed trait MathematicalFunction
    extends SQLTransformFunction[SQLNumeric, SQLNumeric]
    with SQLFunctionWithIdentifier {
  override def inputType: SQLNumeric = SQLTypes.Numeric

  override def outputType: SQLNumeric = SQLTypes.Double

  def operator: UnaryArithmeticOperator

  override def fun: Option[PainlessScript] = Some(operator)

  override def identifier: SQLIdentifier = SQLIdentifier("", functions = this :: Nil)

}

case class SQLMathematicalFunction(
  operator: UnaryArithmeticOperator,
  arg: PainlessScript
) extends MathematicalFunction {
  override def args: List[PainlessScript] = List(arg)
}

case class SQLPow(arg: PainlessScript, exponent: Int) extends MathematicalFunction {
  override def operator: UnaryArithmeticOperator = Pow
  override def args: List[PainlessScript] = List(arg, SQLIntValue(exponent))
  override def nullable: Boolean = arg.nullable
}

case class SQLRound(arg: PainlessScript, scale: Option[Int]) extends MathematicalFunction {
  override def operator: UnaryArithmeticOperator = Round

  override def args: List[PainlessScript] =
    List(arg) ++ scale.map(SQLIntValue(_)).toList

  override def toPainlessCall(callArgs: List[String]): String =
    s"(def p = ${SQLPow(SQLIntValue(10), scale.getOrElse(0)).painless}; ${operator.painless}((${callArgs.head} * p) / p))"
}

case class SQLSign(arg: PainlessScript) extends MathematicalFunction {
  override def operator: UnaryArithmeticOperator = Sign

  override def args: List[PainlessScript] = List(arg)

  override def outputType: SQLNumeric = SQLTypes.Int

  override def painless: String = {
    val ret = "arg0 > 0 ? 1 : (arg0 < 0 ? -1 : 0)"
    if (arg.nullable)
      s"(def arg0 = ${arg.painless}; arg0 != null ? ($ret) : null)"
    else
      s"(def arg0 = ${arg.painless}; $ret)"
  }
}

case class SQLAtan2(y: PainlessScript, x: PainlessScript) extends MathematicalFunction {
  override def operator: UnaryArithmeticOperator = Atan2
  override def args: List[PainlessScript] = List(y, x)
  override def nullable: Boolean = y.nullable || x.nullable
}

sealed trait StringFunction[Out <: SQLType]
    extends SQLTransformFunction[SQLVarchar, Out]
    with SQLFunctionWithIdentifier {
  override def inputType: SQLVarchar = SQLTypes.Varchar

  override def outputType: Out

  def operator: SQLStringOperator

  override def fun: Option[PainlessScript] = Some(operator)

  override def identifier: SQLIdentifier = SQLIdentifier("", functions = this :: Nil)

  override def toSQL(base: String): String = s"$sql($base)"

  override def sql: String =
    if (args.isEmpty)
      s"${fun.map(_.sql).getOrElse("")}"
    else
      super.sql
}

case class SQLStringFunction(operator: SQLStringOperator) extends StringFunction[SQLVarchar] {
  override def outputType: SQLVarchar = SQLTypes.Varchar
  override def args: List[PainlessScript] = List.empty

}

case class SQLSubstring(str: PainlessScript, start: Int, length: Option[Int])
    extends StringFunction[SQLVarchar] {
  override def outputType: SQLVarchar = SQLTypes.Varchar
  override def operator: SQLStringOperator = Substring

  override def args: List[PainlessScript] =
    List(str, SQLIntValue(start)) ++ length.map(l => SQLIntValue(l)).toList

  override def nullable: Boolean = str.nullable

  override def toPainlessCall(callArgs: List[String]): String = {
    callArgs match {
      // SUBSTRING(expr, start, length)
      case List(arg0, arg1, arg2) =>
        s"(($arg1 - 1) < 0 || ($arg1 - 1 + $arg2) > $arg0.length()) ? null : $arg0.substring(($arg1 - 1), ($arg1 - 1 + $arg2))"

      // SUBSTRING(expr, start)
      case List(arg0, arg1) =>
        s"(($arg1 - 1) < 0 || ($arg1 - 1) >= $arg0.length()) ? null : $arg0.substring(($arg1 - 1))"

      case _ => throw new IllegalArgumentException("SUBSTRING requires 2 or 3 arguments")
    }
  }

  override def validate(): Either[String, Unit] =
    if (start < 1)
      Left("SUBSTRING start position must be greater than or equal to 1 (SQL is 1-based)")
    else if (length.exists(_ < 0))
      Left("SUBSTRING length must be non-negative")
    else Right(())

  override def toSQL(base: String): String = sql

}

case class SQLConcat(values: List[PainlessScript]) extends StringFunction[SQLVarchar] {
  override def outputType: SQLVarchar = SQLTypes.Varchar
  override def operator: SQLStringOperator = Concat

  override def args: List[PainlessScript] = values

  override def nullable: Boolean = values.exists(_.nullable)

  override def toPainlessCall(callArgs: List[String]): String = {
    if (callArgs.isEmpty)
      throw new IllegalArgumentException("CONCAT requires at least one argument")
    else
      callArgs.zipWithIndex
        .map { case (arg, idx) =>
          SQLTypeUtils.coerce(arg, values(idx).out, SQLTypes.Varchar, nullable = false)
        }
        .mkString(operator.painless)
  }

  override def validate(): Either[String, Unit] =
    if (values.isEmpty) Left("CONCAT requires at least one argument")
    else Right(())

  override def toSQL(base: String): String = sql
}

case object SQLLength extends StringFunction[SQLBigInt] {
  override def outputType: SQLBigInt = SQLTypes.BigInt
  override def operator: SQLStringOperator = Length
  override def args: List[PainlessScript] = List.empty
}
