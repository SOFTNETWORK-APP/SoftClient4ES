/*
 * Copyright 2025 SOFTNETWORK
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

package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.{
  escapePainlessString,
  escapeStringLiteral,
  query,
  DateMathRounding,
  DateMathScript,
  Expr,
  Identifier,
  LiteralParam,
  PainlessContext,
  PainlessParam,
  PainlessScript,
  StringValue,
  TokenRegex,
  Updateable
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
import app.softnetwork.elastic.sql.function.time.CurrentFunction.queryTimestamp
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

    override def applyType(in: SQLType): SQLType = {
      interval.checkType(in) match {
        case Left(_)  => baseType
        case Right(_) => cast(in)
      }
    }

    override def validate(): Either[String, Unit] = interval.checkType(out) match {
      case Left(err) => Left(err)
      case Right(_)  => Right(())
    }

    override def toPainless(base: String, idx: Int, context: Option[PainlessContext]): String = {
      context match {
        case Some(ctx) =>
          // 🔴 Fold onto the OPERAND's parameter -- `ctx.find(base)`, the way
          // `TransformFunction.toPainless` does -- and not onto `ctx.last`, "whatever parameter was
          // created last" (issue #370, found by review). The two coincide only while the operand
          // is the newest registration; the second occurrence of the same chain in a script, or a
          // chain whose operand was registered earlier, landed the `.plus(…)` on SOMEONE ELSE's
          // parameter: `CASE WHEN DATE_ADD(d, INTERVAL 1 DAY) > ts THEN DATE_ADD(d, INTERVAL 1 DAY)
          // ELSE d END` added the day to `ts` and returned it (silent, HTTP 200); with the ELSE and
          // THEN swapped it folded onto the boolean CONDITION (compile error); and
          // `CAST(d AS DATE) - INTERVAL 3 DAY > d` read `x > x`. Measured on the control.
          ctx.find(base) match {
            case Some(param) =>
              param.addPainlessMethod(painless(context))
              return base
            case _ if nullable =>
              // `base` is an EXPRESSION (a guarded ternary from the previous function): bind it,
              // then apply the interval INSIDE the guard -- 21.8's rule 2, the same shape
              // `TransformFunction.toPainless` uses.
              ctx.addParam(LiteralParam(base)) match {
                case Some(bound) =>
                  return s"($bound == null) ? null : (def)(${SQLTypeUtils
                    .coerce(bound, expr.baseType, out, nullable = false, context)}${painless(context)})"
                case None =>
              }
            case _ =>
          }
        case _ =>
      }
      if (nullable) {
        // ensure unique variable names
        s"(def e$idx = $base; e$idx != null ? ${SQLTypeUtils.coerce(s"e$idx", expr.baseType, out, nullable = false, context)}${painless(context)} : null)"
      } else
        s"${SQLTypeUtils.coerce(base, expr.baseType, out, nullable = expr.nullable, context)}${painless(context)}"
    }
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
    override def update(request: query.SingleSearch): SQLAddInterval = this
  }

  case class SQLSubtractInterval(interval: TimeInterval) extends SubtractInterval[SQLTemporal] {
    override def inputType: SQLTemporal = SQLTypes.Temporal
    override def outputType: SQLTemporal = SQLTypes.Temporal
    override def update(request: query.SingleSearch): SQLSubtractInterval = this
  }

  sealed trait DateTimeFunction extends Function {
    def now: String = "ZonedDateTime.ofInstant(Instant.ofEpochMilli({{__now__}}), ZoneId.of('Z'))"
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

    def param: String

    /** The processor form. In an ingest script every temporal value is a `ZonedDateTime` — the
      * operand parse yields one and Elasticsearch will only accept one back into `ctx.<field>` — so
      * the DATE and TIME narrowings are dropped rather than producing a `LocalDate` that
      * `ChronoUnit.between` then refuses to pair with the other operand. One type, one rule.
      */
    def processorParam: String = param

    override def painless(context: Option[PainlessContext]): String = {
      context match {
        case Some(ctx) =>
          val effective = if (ctx.isProcessor) processorParam else param
          ctx.addParam(LiteralParam(effective.replaceAll("\\{\\{__now__}}", ctx.timestamp))) match {
            case Some(p) =>
              return SQLTypeUtils.coerce(p, this.baseType, this.out, nullable = false, context)
            case _ =>
          }
        case _ =>
      }
      SQLTypeUtils.coerce(
        param.replaceAll("\\{\\{__now__}}", queryTimestamp),
        this.baseType,
        this.out,
        nullable = false,
        context
      )
    }
  }

  object CurrentFunction {

    /** 🔴 The ingest clock, and it was `ctx['_ingest']['timestamp']` — which is NULL.
      *
      * MEASURED on REAL indices (not `_simulate`) across every supported major — ES 6.8.23,
      * 7.17.29, 8.18.3 and 9.0.3 — that access throws `null_pointer_exception` and, because a
      * computed column's processor carries `ignore_failure: true`, the column was silently ABSENT.
      * So `CURRENT_DATE` / `CURRENT_TIMESTAMP` / `NOW` / `TODAY` inside `CREATE TABLE … SCRIPT AS`
      * has never worked on any version — including the PUBLISHED `DATE_DIFF(birthdate,
      * CURRENT_DATE, YEAR)` example in `documentation/sql/ddl_statements.md` and the REPL testkit's
      * `users` table.
      *
      * ⚠️ It is NOT a version split, which is what it first looked like: `metadata().now` works on
      * ES 8+ and does not exist on 6/7, and a `metadata()` mention would fail COMPILATION on 6/7
      * even inside a branch never taken. `System.currentTimeMillis()` is whitelisted on all four
      * majors (measured) and is already the unit the surrounding emission expects — the wrapper has
      * always been `ZonedDateTime.ofInstant(Instant.ofEpochMilli(<this>), ZoneId.of('Z'))`, written
      * for a millis source. The accessor was the only thing wrong.
      */
    val processorTimestamp: String = "System.currentTimeMillis()"
    val queryTimestamp: String = "params.__now__"
  }

  sealed trait CurrentDateTimeFunction extends DateTimeFunction with CurrentFunction {
    override def param: String = now
  }

  sealed trait CurrentDateFunction extends DateFunction with CurrentFunction {
    override def param: String = s"$now.toLocalDate()"
    override def processorParam: String = now
  }

  sealed trait CurrentTimeFunction extends TimeFunction with CurrentFunction {
    override def param: String = s"$now.toLocalTime()"
    override def processorParam: String = now
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

  /** The start of the ISO-8601 week a temporal falls in: its MONDAY. It is what `DATE_TRUNC(…,
    * WEEK)` truncates to and where `DATEDIFF` / `DATE_DIFF` count a WEEK boundary, and it is what
    * Elasticsearch's calendar week (`date_histogram`, `/w` date math) rounds to, so every venue
    * that truncates to a week agrees.
    *
    * 🔴 `with(DayOfWeek.SUNDAY)`, rendered before, is NOT a Sunday-start week: `DayOfWeek` adjusts
    * within the Monday-to-Sunday week, so it moved a Wednesday FORWARD to the Sunday that ENDS its
    * week (2025-01-08 -> 2025-01-12, JDK-verified), while a `GROUP BY` of the same expression went
    * back to the Monday.
    */
  private[sql] val isoWeekStart: String = ".with(DayOfWeek.MONDAY)"

  case object DateTrunc extends Expr("DATE_TRUNC") with TokenRegex with PainlessScript {
    override def painless(context: Option[PainlessContext]): String = ".truncatedTo"
    override lazy val words: List[String] = List(sql, "DATETRUNC")
  }

  case class DateTrunc(identifier: Identifier, unit: TimeUnit, transactSql: Boolean = false)
      extends DateTimeFunction
      with TransformFunction[SQLTemporal, SQLTemporal]
      with FunctionWithIdentifier
      with DateMathRounding { // FIXME check Unit compatibility with inputType
    override def fun: Option[PainlessScript] = Some(DateTrunc)

    /** `QUARTER` is the one unit whose context-bearing rendering does NOT fold: it needs the
      * operand three times, so it binds a parameter of its own (see `painless` below) and
      * `toPainless` receives that name. Context-free it still renders a `.withMonth(...)` suffix,
      * which is what the default test would read -- hence the override (issue #370).
      */
    override def foldsOntoOperand: Boolean = unit != TimeUnit.QUARTERS && super.foldsOntoOperand

    override def args: List[PainlessScript] = List(unit)

    override def inputType: SQLTemporal = SQLTypes.Temporal // par défaut
    override def outputType: SQLTemporal = SQLTypes.Temporal // idem

    override def sql: String = DateTrunc.sql
    override def toSQL(base: String): String =
      if (transactSql) s"$sql(${unit.sql}, $base)"
      else s"$sql($base, ${unit.sql})"

    override def roundingScript: Option[String] = unit.roundingScript

    override def dateMathScript: Boolean = identifier.dateMathScript

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      // 🔴 `truncatedTo` does not exist on `LocalDate`, which is what a DATE-typed operand is.
      // MEASURED on real Elasticsearch 8.18.3: `DATE_TRUNC(DATE_PARSE(col,fmt), MONTH)` was a
      // compile error -- `member method [java.time.LocalDate, truncatedTo/1] not found`. The
      // day-field methods below (`withDayOfYear`, `withDayOfMonth`, `with(DayOfWeek)`) all exist on
      // `LocalDate`; only the time truncation does not, and on a date-only value it has nothing to
      // truncate, so omitting it is the same value and not an approximation.
      // `expr` is what this function is applied TO, which is the only thing that says whether the
      // value carries a time. `in` is `SQLTypes.Temporal` for every DATE_TRUNC and says nothing.
      //
      // ⚠️ `baseType` is right in BOTH contexts, and that is not an accident: an ingest script now
      // parses `ctx.<field>` into a `ZonedDateTime` whatever the column was declared as, which is
      // the same collapse `SQLTypeUtils.runtimeType` already applies for a query. One rule, one
      // type — an ingest-only arm here would be an "except" with nothing behind it.
      //
      // 🔴 ...and `baseType` is not the whole answer either (issue #370): a comparison against a
      // DATE literal, a CAST or a CASE NARROWS the operand to a `LocalDate` by appending
      // `.toLocalDate()` to its parameter object before this chain renders, while `baseType` still
      // says TIMESTAMP. `DATE_TRUNC(d, MONTH) = CAST('2025-01-01' AS DATE)` then emitted
      // `…toLocalDate().withDayOfMonth(1).truncatedTo(…)` -- MEASURED on ES 8.18:
      // `LocalDate.truncatedTo/1 not found`. The receiver is what the parameter's methods have
      // rendered it into, and the parameter is the thing that knows.
      //
      // 🔴 THE REGISTERED parameter, not `expr`'s own object. Two instances of one identifier can
      // share a parameter (`DATE_FORMAT(DATE_TRUNC(d, MONTH), …)` holds a second `d`), and the
      // fold appends to the object the context registered -- so the decision is read off that
      // same object, the way `toPainless` finds it. Reading `expr` gave the two instances
      // different answers and the shared parameter both renderings
      // (`…truncatedTo(…).withDayOfMonth(1)`, MEASURED). Context-free -- which is what
      // `foldsOntoOperand` and the identity key read -- the answer is the TYPE alone: reading
      // the object's methods there made the key depend on whether a narrowing had ALREADY landed
      // on the object, i.e. on the order of the predicates in the script (review).
      val receiver: Option[PainlessParam] =
        context.flatMap(ctx => ctx.get(expr).flatMap(ctx.find))
      val receiverIsDateOnly = expr.baseType == SQLTypes.Date || receiver.exists(_.rendersLocalDate)
      val truncateTime = if (receiverIsDateOnly) "" else ".truncatedTo(ChronoUnit.DAYS)"
      unit match {
        case TimeUnit.YEARS  => s".withDayOfYear(1)$truncateTime"
        case TimeUnit.MONTHS => s".withDayOfMonth(1)$truncateTime"
        case TimeUnit.WEEKS  => s"$isoWeekStart$truncateTime"
        case TimeUnit.QUARTERS =>
          context match {
            case Some(ctx) =>
              ctx.addParam(identifier) match {
                case Some(p) =>
                  val quarter =
                    s"$p.withMonth(((($p.getMonthValue() - 1) / 3) * 3) + 1).withDayOfMonth(1)$truncateTime"
                  val quarterExpr =
                    if (identifier.nullable) {
                      s"$p != null ? $quarter : null"
                    } else {
                      quarter
                    }
                  ctx.addParam(
                    LiteralParam(
                      quarterExpr
                    )
                  ) match {
                    case Some(p) => return p
                    case _       =>
                  }
                case _ =>
              }
            case _ =>
          }
          super.toPainlessCall(callArgs, context)
        case _ => super.toPainlessCall(callArgs, context)
      }
    }

    override def shouldBeScripted: Boolean = false

    override def update(request: query.SingleSearch): DateTrunc = {
      this.copy(identifier = identifier.update(request))
    }
  }

  /** The `EXTRACT` token. It is NOT a `PainlessScript`: the accessor an extraction reads its field
    * with is decided per FIELD by `TimeField.accessor` and emitted through `FieldAccessor` below
    * (issue #368). A `.get` here would be a second, silently divergent copy of that decision --
    * which is the shape of the defect, not a leftover.
    */
  case object Extract extends Expr("EXTRACT") with TokenRegex

  /** The Painless accessor an `Extract` reads its field with.
    *
    * It is looked up on the FIELD (`TimeField.accessor`) rather than fixed here, because the choice
    * between `.get` and `.getLong` is decided by the field's value RANGE -- see the scaladoc on
    * `TimeField.accessor` and issue #368.
    */
  case class FieldAccessor(field: TimeField) extends PainlessScript {
    override def painless(context: Option[PainlessContext]): String = field.accessor
    override def nullable: Boolean = false
    override def sql: String = field.sql
  }

  case class Extract(field: TimeField)
      extends DateTimeFunction
      with TransformFunction[SQLTemporal, SQLNumeric] {

    override val sql: String = Extract.sql

    override def fun: Option[PainlessScript] = Some(FieldAccessor(field))

    override def args: List[PainlessScript] = List(field)

    override def inputType: SQLTemporal = SQLTypes.Temporal
    override def outputType: SQLNumeric = SQLTypes.Numeric

    override def toSQL(base: String): String = s"$sql(${field.sql} FROM $base)"

    override def update(request: query.SingleSearch): Extract = this
  }

  import TimeField._

  sealed abstract class TimeFieldExtract(field: TimeField) extends Extract(field) {
    override val sql: String = field.sql
    override def toSQL(base: String): String = s"$sql($base)"
  }

  class Year extends TimeFieldExtract(YEAR)

  class MonthOfYear extends TimeFieldExtract(MONTH_OF_YEAR)

  class DayOfMonth extends TimeFieldExtract(DAY_OF_MONTH)

  class DayOfWeek(date: Identifier)
      extends TimeFieldExtract(DAY_OF_WEEK)
      with FunctionWithIdentifier {
    override def identifier: Identifier = date
    override def args: List[PainlessScript] = List(identifier)
    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      callArgs match {
        case arg :: Nil =>
          // Through `field.accessor`, not a literal `.get`, so this hand-written call cannot
          // disagree with the one `toPainlessCall` assembles for every other field (#368).
          s"($arg${field.accessor}(${field.painless(context)}) + 6) % 7"
        case _ => throw new IllegalArgumentException("DayOfWeek requires exactly one argument")
      }
    }

    /** 🔴 Resolved like every other transform function that holds a copy of the column (story
      * IDENT-1). The inherited `Extract.update` returns `this`, which is right for a bare extractor
      * (it holds no identifier) and wrong here: the receiver kept the spelling it was parsed with
      * at EVERY resolution, so `WEEKDAY(t.d)` read the literal field `t.d` on a client search, and
      * under an UNNEST aliased by its own field the stale copy agreed with the column on the doc
      * access while disagreeing on the correlation -- the one pair identifier equality would have
      * re-answered.
      */
    override def update(request: query.SingleSearch): DayOfWeek = new DayOfWeek(
      date.update(request)
    )

  }

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

    /** 🔴 `.with(TemporalAdjusters.lastDayOfMonth())`, and NOT the arithmetic
      * `.withDayOfMonth(<operand>.lengthOfMonth())` this emitted before issue #368.
      *
      * `lengthOfMonth()` exists on `LocalDate` and NOT on `ZonedDateTime`, which is what
      * `doc['<date field>'].value` actually is -- so `LAST_DAY(<date column>)` failed the whole
      * query on every supported Elasticsearch and had never worked. It is not enough to insert a
      * `.toLocalDate()`: the operand reaching here is a `ZonedDateTime` for a bare column but
      * ALREADY a `LocalDate` wherever a cast or a `SQLTypes.Date` coercion ran first
      * (`LAST_DAY(CAST(d AS DATE))`, `LAST_DAY(CURRENT_DATE)`, `LAST_DAY(CURRENT_TIMESTAMP)`, and
      * the whole no-schema path), and `LocalDate` has no `toLocalDate()` -- MEASURED, that swaps
      * one broken receiver for another.
      *
      * `with(TemporalAdjusters.lastDayOfMonth())` is defined on `Temporal`, so it needs no
      * knowledge of the receiver at all and returns the receiver's own type. Executed on real
      * Elasticsearch 6.8.23, 7.17.29, 8.18.3 and 9.0.3 over `ZonedDateTime`, `LocalDate` and
      * `LocalDateTime` receivers -- see `DateDocValueEmissionSpec` for what the emission is pinned
      * against.
      */
    override def painless(context: Option[PainlessContext]): String = ".with"
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

    override def sql: String = LastDayOfMonth.sql

    override def toSQL(base: String): String = {
      s"$sql($base)"
    }

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      callArgs match {
        case arg :: Nil =>
          s"$arg${LastDayOfMonth.painless(context)}(TemporalAdjusters.lastDayOfMonth())"
        case _ => throw new IllegalArgumentException("LastDayOfMonth requires exactly one argument")
      }
    }

    override def update(request: query.SingleSearch): LastDayOfMonth = {
      this.copy(identifier = identifier.update(request))
    }
  }

  case object DateDiff extends Expr("DATE_DIFF") with TokenRegex with PainlessScript {
    override def painless(context: Option[PainlessContext]): String = ".between"
    override lazy val words: List[String] = List(sql, "TIMESTAMPDIFF")

    /** MySQL's `TIMESTAMPDIFF`, this token's second word. It takes the layouts `DATE_DIFF` takes
      * but COUNTS differently (whole units elapsed, see [[DateDiffSpelling]]), so the parser asks
      * which word it read and the render writes this one back.
      */
    lazy val timestampDiff: String = words(1)

    /** A calendar date alone, in the two separators the DATE parse accepts (`SQLTypeUtils.coerce`).
      */
    private val CalendarDate = """\d{4}[-/]\d{2}[-/]\d{2}""".r

    /** The temporal a STRING-LITERAL operand spells: a DATE when it is a calendar date alone
      * (`'2024-01-01'`), a TIMESTAMP when it carries anything more -- a time of day, a zone
      * (`'2024-01-01 10:00:00'`, `'2024-01-01T10:00:00Z'`). `None` for any other operand.
      */
    private[time] def literalType(operand: PainlessScript): Option[SQLType] = operand match {
      case id: Identifier if id.name.isEmpty =>
        id.functions match {
          case List(s: StringValue) =>
            Some(
              if (CalendarDate.pattern.matcher(s.value).matches()) SQLTypes.Date
              else SQLTypes.Timestamp
            )
          case _ => None
        }
      case _ => None
    }

    /** The scale of [[monthKey]]: more milliseconds than any month holds (31 days: 2678400000). */
    private[time] val MonthKeyScale: Long = 4294967296L

    /** A temporal as one number whose difference with another, divided by [[MonthKeyScale]] and
      * truncated toward zero, is MySQL's whole months elapsed between them (`TIMESTAMPDIFF`): the
      * months between the two, less one when the later one's day of month and time of day come
      * before the earlier one's.
      *
      * 🔴 Not `ChronoUnit.MONTHS.between`, which differs on one edge. It first moves an end whose
      * time of day comes before the start's back by one DAY — on the 1st of a month, into the month
      * before — and then counts that month short as well: `2024-12-31 23:59:59` to `2025-07-01
      * 00:00:00` is 5 months there and 6 in MySQL (found by the testkit's population).
      */
    private[time] def monthKey(rendered: String): String = {
      val t = if (postfixable(rendered)) rendered else s"($rendered)"
      s"(($t.getYear() * 12L + $t.getMonthValue()) * ${MonthKeyScale}L + ($t.getDayOfMonth() - 1) * 86400000L + $t.getLong(ChronoField.MILLI_OF_DAY))"
    }

    /** A temporal as the instant it denotes, in UTC, decided by its RUNTIME type: a `LocalDate` is
      * the start of its day, a `LocalDateTime` that wall-clock time in UTC, anything zoned the same
      * instant. `TIMESTAMPDIFF` compares instants, and an operand's declared type does not say
      * which of the three it holds: `COALESCE(ts, CURRENT_DATE)` is either, row by row. `DATEDIFF`
      * and `DATE_DIFF` read a calendar date through it too, off a row-level operand that is not a
      * column: a zoned value there may be Elasticsearch 6.8's doc value, which is no `java.time`
      * type until `withZoneSameInstant` (as in [[utcZoned]]) returns the `ZonedDateTime` it wraps.
      *
      * `ref` is read up to four times, so it is a name or an expression cast to `def` (the methods
      * below are resolved at runtime). NULL stays NULL: the MONTH, QUARTER and YEAR calls bind this
      * value in the script's prologue, which runs before the null guard.
      */
    private[time] def utcInstant(ref: String): String =
      s"($ref == null ? null : $ref instanceof LocalDate ? $ref.atStartOfDay(ZoneId.of('Z')) : $ref instanceof LocalDateTime ? $ref.atZone(ZoneId.of('Z')) : $ref.withZoneSameInstant(ZoneId.of('Z')))"

    /** A column a SEARCH script reads, as a genuine `java.time.ZonedDateTime` in UTC; NULL stays
      * NULL (the MONTH, QUARTER and YEAR calls bind it in the prologue, before the null guard).
      *
      * The column's script parameter is shared by every call that reads the column, and holds what
      * the FIRST of them made of it (the parameter-identity family, issue #370): the UTC instant
      * `DateDiff.in` folds onto it, or the raw doc value when a function over the same column read
      * it first (`TIMESTAMPDIFF(MONTH, ts::DATE, ts)`). On Elasticsearch 6.8 that value is a
      * `JodaCompatibleZonedDateTime`, which implements no `java.time` interface: it has no
      * `getLong` ([[monthKey]] failed with `dynamic method [..., getLong/1] not found`) and is no
      * `Temporal` (`ChronoUnit.between` failed with a `ClassCastException`). `withZoneSameInstant`
      * is on its allow-list and returns the `ZonedDateTime` it wraps; on 7.x, 8.x and 9.x it reads
      * the same instant, so every major counts the same units.
      */
    private[time] def utcZoned(ref: String): String = {
      val r = if (postfixable(ref)) ref else s"($ref)"
      s"($r == null ? null : $r.withZoneSameInstant(ZoneId.of('Z')))"
    }

    /** Whether a method can be appended to a rendered operand as it stands: outside parentheses,
      * brackets and string literals it holds nothing but names, digits and dots. Anything else — a
      * ternary (`p4 ? p3 : p1`, which is how a CASE operand renders), a cast (`(long) x`) — is
      * parenthesised first, or the method would bind to its last term alone.
      */
    private[time] def postfixable(rendered: String): Boolean = {
      var depth = 0
      var quote: Char = 0
      var i = 0
      while (i < rendered.length) {
        val c = rendered.charAt(i)
        if (quote != 0) {
          if (c == '\\') i += 1
          else if (c == quote) quote = 0
        } else if (c == '"' || c == '\'') quote = c
        else if (c == '(' || c == '[') depth += 1
        else if (c == ')' || c == ']') depth -= 1
        else if (depth == 0 && !(c.isLetterOrDigit || c == '_' || c == '.')) return false
        i += 1
      }
      true
    }
  }

  /** `DATEDIFF`, the name MySQL, SQL Server, Snowflake, Redshift, DuckDB and Elasticsearch SQL give
    * the function. It needs its own token because its two-argument form is MySQL's, which the
    * `DATE_DIFF` productions do not parse (issue #363).
    *
    * 🔴 Neither spelling is a prefix of the other (`DATE_DIFF` has an underscore where `DATEDIFF`
    * has a `D`), so the two regexes cannot shadow one another whatever order they are tried in.
    */
  case object MySqlDateDiff extends Expr("DATEDIFF") with TokenRegex

  /** How a `DateDiff` was written, which decides its RENDER and what it COUNTS. The node itself
    * always means `end - start`: the parser stores the operands so, and every other consumer
    * (`args`, `left`/`right`, the Painless emission, validation, `update`) reads that one encoding.
    *
    * The vendors' definitions, fetched from their references (2026-10-02), and the lead's ruling to
    * follow them on both axes:
    *
    *   - DIRECTION, by layout. Every vendor computes `end - start`; the layout decides where the
    *     end sits. Dates first is `first - second`: MySQL's `DATEDIFF(expr1, expr2)`, BigQuery's
    *     `DATE_DIFF(end_date, start_date, part)` (`DATE_DIFF('2010-07-07', '2008-12-25', DAY)` is
    *     559). Unit first is `last - middle`: `DATEDIFF(unit, start, end)` in SQL Server,
    *     Snowflake, Redshift, DuckDB and Elasticsearch SQL, and MySQL's `TIMESTAMPDIFF(unit, dt1,
    *     dt2)`.
    *   - COUNTING, by name. `DATEDIFF` and `DATE_DIFF` count the calendar BOUNDARIES crossed, as
    *     SQL Server, Snowflake, Redshift, BigQuery and DuckDB's `date_diff` do: one second across a
    *     year end is one YEAR. `TIMESTAMPDIFF` counts the whole units ELAPSED, truncated toward
    *     zero, as MySQL does: a month counts once its day and time of day are reached.
    *
    * 🔴 Issue #363 read BigQuery's `DATE_DIFF` as `(start, end, unit)`, so until 0.24.0 the
    * dates-first forms with a unit subtracted the other way round, and every unit from WEEK up
    * counted elapsed units. Sealed, so the compiler forces every arm of the render.
    */
  sealed trait DateDiffSpelling {

    /** `TIMESTAMPDIFF` counts whole units ELAPSED; every other spelling counts BOUNDARIES. */
    def elapsed: Boolean = false
  }
  object DateDiffSpelling {

    /** `DATE_DIFF(end, start[, unit])` (BigQuery's order) and `DATEDIFF(end, start, unit)`: dates
      * first, so the parser stores the SECOND operand as the start.
      */
    case object DateFirst extends DateDiffSpelling

    /** `DATE_DIFF(unit, start, end)` / `DATEDIFF(unit, start, end)`: unit first. */
    case object UnitFirst extends DateDiffSpelling

    /** `DATEDIFF(end, start)`: MySQL's, in days. It means what `DATE_DIFF(end, start, DAY)` means;
      * the spelling is kept so that `SHOW CREATE …` hands back what was written.
      */
    case object MySql extends DateDiffSpelling

    /** `TIMESTAMPDIFF(unit, start, end)`: MySQL's, whole units ELAPSED. */
    case object TimestampDiff extends DateDiffSpelling {
      override val elapsed: Boolean = true
    }

    /** `TIMESTAMPDIFF(end, start[, unit])`. No vendor writes it; it parses because `TIMESTAMPDIFF`
      * is a word of the `DATE_DIFF` token, and the two rules above give it its meaning: dates
      * first, whole units elapsed.
      */
    case object TimestampDiffDateFirst extends DateDiffSpelling {
      override val elapsed: Boolean = true
    }
  }

  case class DateDiff(
    start: PainlessScript,
    end: PainlessScript,
    unit: TimeUnit,
    spelling: DateDiffSpelling = DateDiffSpelling.DateFirst
  ) extends DateTimeFunction
      with BinaryFunction[SQLDateTime, SQLDateTime, SQLNumeric]
      with PainlessScript {
    override def fun: Option[PainlessScript] = Some(DateDiff)

    override def inputType: SQLDateTime = SQLTypes.DateTime
    override def outputType: SQLNumeric = SQLTypes.Numeric

    override def left: PainlessScript = start
    override def right: PainlessScript = end

    override def sql: String = DateDiff.sql

    /** The render must RE-PARSE to this same node, and it is not cosmetic that it does:
      * `MaterializedViewExtension` persists this text and re-runs `client.run(alter.sql)`, `SHOW
      * CREATE MATERIALIZED VIEW` echoes it, and `SCRIPT AS` stores it beside the Painless.
      *
      * 🔴 The dates-first forms are stored with their operands SWAPPED (the second is the start),
      * and the render swaps them back. What the swap protects against is the other design, the one
      * where the node keeps the operands as written and reverses them at emission: there the
      * canonical render flips the sign of a stored statement on its next round trip (issue #363).
      *
      * 🔴 `TIMESTAMPDIFF` keeps its name, and that is correctness, not readability: it counts
      * elapsed units where `DATE_DIFF` counts boundaries, so a render as `DATE_DIFF` would change
      * what a stored statement answers. `DATEDIFF(a, b)` keeps its name for readability only (it
      * means `DATE_DIFF(a, b, DAY)`), so that `SHOW CREATE …` hands back what was written.
      */
    override def toSQL(base: String): String = spelling match {
      case DateDiffSpelling.UnitFirst => s"$sql(${unit.sql}, ${start.sql}, ${end.sql})"
      case DateDiffSpelling.MySql     => s"${MySqlDateDiff.sql}(${end.sql}, ${start.sql})"
      case DateDiffSpelling.DateFirst => s"$sql(${end.sql}, ${start.sql}, ${unit.sql})"
      case DateDiffSpelling.TimestampDiff =>
        s"${DateDiff.timestampDiff}(${unit.sql}, ${start.sql}, ${end.sql})"
      case DateDiffSpelling.TimestampDiffDateFirst =>
        s"${DateDiff.timestampDiff}(${end.sql}, ${start.sql}, ${unit.sql})"
    }

    /** The type a COLUMN operand is read as, whatever the unit: the instant it denotes, in UTC
      * (`FunctionN`'s argument path folds that read onto the column's parameter).
      * [[toPainlessCall]] then brings every operand to [[comparedIn]].
      *
      * 🔴 Not the unit's own type. One column is ONE parameter per script, shared by every call
      * that reads it, and the conversion folded onto it is the first caller's (the
      * parameter-identity family, issue #370). A fold that depended on the unit made `CASE WHEN
      * DATE_DIFF(d, ts, HOUR) > 1 THEN DATE_DIFF(d, ts, DAY) END` read both calls through the
      * HOUR's `ZonedDateTime` and count ELAPSED days. Read losslessly once, each call narrows its
      * own operands.
      */
    override def in: SQLType = SQLTypes.Timestamp

    /** The type the two operands are COMPARED in, in UTC; the counting and the unit decide it.
      *
      *   - `TIMESTAMPDIFF` counts the whole units ELAPSED between two instants, whatever the unit:
      *     a DATE operand is the start of its day.
      *   - `DATEDIFF` / `DATE_DIFF` count BOUNDARIES: between two calendar dates from `DAY` up
      *     (23:30 and 00:30 the next day are one day apart), between two instants for `HOUR`,
      *     `MINUTE` and `SECOND`, which a `LocalDate` cannot hold (`Unsupported unit: Hours`).
      */
    private def comparedIn: SQLType = unit match {
      case _ if spelling.elapsed                                => SQLTypes.Timestamp
      case TimeUnit.HOURS | TimeUnit.MINUTES | TimeUnit.SECONDS => SQLTypes.Timestamp
      case _                                                    => SQLTypes.Date
    }

    /** What each operand is moved to before the units between them are counted: for a BOUNDARY
      * count, the start of the unit it falls in (UTC), so that the whole units elapsed between the
      * two starts are the boundaries crossed between the operands — 2005-12-31 23:59:59 and
      * 2006-01-01 00:00:00 are one YEAR apart, 1992-09-15 and 1992-11-14 two MONTHs, 10:59 and
      * 11:00 one HOUR. A WEEK starts on the ISO Monday ([[isoWeekStart]]), a QUARTER on January,
      * April, July or October 1st. An ELAPSED count moves nothing.
      */
    private def unitStart: String =
      if (spelling.elapsed) ""
      else
        unit match {
          case TimeUnit.YEARS    => ".withDayOfYear(1)"
          case TimeUnit.QUARTERS => ".with(java.time.temporal.IsoFields.DAY_OF_QUARTER, 1)"
          case TimeUnit.MONTHS   => ".withDayOfMonth(1)"
          case TimeUnit.WEEKS    => isoWeekStart
          case TimeUnit.DAYS     => ""
          case subDay            => s".truncatedTo(${subDay.painless(None)})"
        }

    /** A context-free rendering over an aggregate is the PER-GROUP calculation: the `bucket_script`
      * of a SELECT item (`DATEDIFF(MAX(d), '2024-01-01') AS x`), which a HAVING over `x` reads by
      * name and a materialized view's pivot computes too (`SingleSearch.transformBucketScripts`).
      * Its operands are converted in [[toPainlessCall]], the rendering row level ends in too.
      *
      * An empty group needs no guard here: under the default gap policy Elasticsearch SKIPS the
      * script when an operand metric has no value (6.8 to 9.0, `BucketScriptPipelineAggregator`),
      * so the item has no value for that group and SELECT and HAVING read NULL.
      */
    override def painless(context: Option[PainlessContext]): String =
      context match {
        case None if hasAggregation => toPainlessCall(args.map(_.painless(None)), context)
        case _                      => super[BinaryFunction].painless(context)
      }

    /** One operand, brought to [[comparedIn]] through the coercion arms a CAST uses, then to the
      * start of its unit ([[unitStart]]).
      *
      *   - A string LITERAL is first read as the temporal it spells ([[DateDiff.literalType]]: UTC
      *     unless it names a zone), in every venue. Row level used to hand Painless the bare string
      *     (`between("2024-01-01", param1)`, which it cannot call), and an ingest processor still
      *     did.
      *   - Per group, an aggregate is the metric Elasticsearch computed, which a `bucket_script`
      *     receives as a `java.lang.Double` holding EPOCH MILLIS (`(long)` is required: Painless
      *     refuses to cast a `def` double to `long` implicitly). Any other operand is converted
      *     from its own type.
      *   - In an INGEST processor any other operand is a UTC temporal the processor made itself: a
      *     column parsed at runtime by `SQLTypeUtils.processorTemporal`, the clock, a `::DATE`
      *     literal. The first two are `ZonedDateTime`s whatever their type, the last a `LocalDate`,
      *     and `LocalDate.from` reads the calendar date of either. A DATE under a time-of-day
      *     comparison is the start of its day, as at row level — `CURRENT_DATE` included, which a
      *     processor holds as the current instant.
      *   - At row level a column operand holds its UTC instant ([[in]]), so its calendar date is
      *     `toLocalDate()`. Any other operand (`'2025-01-10'::DATE`, `CURRENT_DATE`, `NOW()`, a
      *     COALESCE, a CASE) is read for a calendar date by `LocalDate.from`, once it is a UTC
      *     `java.time` value by its RUNTIME type ([[DateDiff.utcInstant]]): a COALESCE or a CASE
      *     over a column hands on the column's raw doc value, which on Elasticsearch 6.8 is no
      *     `java.time` type, and `LocalDate.from` refused it (`ClassCastException`). For a
      *     time-of-day comparison a DATE is the start of its day (a `LocalDate` has no hours) and
      *     anything else renders as it did.
      *   - `TIMESTAMPDIFF` takes [[elapsedOperand]] instead, in every venue.
      */
    private def operand(
      arg: PainlessScript,
      rendered: String,
      context: Option[PainlessContext],
      perGroup: Boolean
    ): String = {
      val compared = DateDiff.literalType(arg) match {
        case Some(literal) =>
          val value =
            SQLTypeUtils.coerce(rendered, SQLTypes.Varchar, literal, nullable = false, None)
          SQLTypeUtils.coerce(value, literal, comparedIn, nullable = false, None)
        case None if perGroup =>
          arg match {
            case metric: Identifier if metric.isAggregation =>
              val epochMillis = SQLTypeUtils
                .coerce(rendered, SQLTypes.Double, SQLTypes.BigInt, nullable = false, None)
              // The instant in UTC already (`Instant.ofEpochMilli(...).atZone(ZoneId.of('Z'))`), so
              // a calendar date is read off it: ONE conversion per aggregate. The TIMESTAMP -> DATE
              // arm would normalise it to UTC a second time (`.toInstant().atZone(ZoneId.of('Z'))`),
              // as it must an operand of unknown zone.
              val utc = SQLTypeUtils
                .coerce(epochMillis, SQLTypes.BigInt, SQLTypes.Timestamp, nullable = false, None)
              if (comparedIn == SQLTypes.Date) s"$utc.toLocalDate()" else utc
            case _ if spelling.elapsed => elapsedOperand(arg, rendered, context)
            case other =>
              SQLTypeUtils.coerce(rendered, other.baseType, comparedIn, nullable = false, None)
          }
        case None if spelling.elapsed => elapsedOperand(arg, rendered, context)
        case None if context.exists(_.isProcessor) =>
          if (comparedIn == SQLTypes.Date) s"LocalDate.from($rendered)"
          else if (arg.baseType == SQLTypes.Date)
            s"LocalDate.from($rendered).atStartOfDay(ZoneId.of('Z'))"
          else rendered
        case None if context.isDefined =>
          arg match {
            case column: Identifier if column.name.trim.nonEmpty =>
              if (comparedIn == SQLTypes.Date) s"$rendered.toLocalDate()" else rendered
            case value: Identifier
                if comparedIn == SQLTypes.Timestamp && value.baseType == SQLTypes.Date =>
              SQLTypeUtils.coerce(rendered, SQLTypes.Date, comparedIn, nullable = false, None)
            // a `ZonedDateTime` here (`'…'::TIMESTAMP`, `NOW()`, a CASE) would keep its time of day
            // through `unitStart`, and a calendar unit would count it; read by its runtime type
            // first, for a COALESCE or a CASE may hand on a column's raw 6.8 doc value
            case _ if comparedIn == SQLTypes.Date =>
              s"LocalDate.from(${DateDiff.utcInstant(bound(rendered, context))})"
            case _ => rendered
          }
        case None => rendered
      }
      if (unitStart.isEmpty) compared
      else if (DateDiff.postfixable(compared)) s"$compared$unitStart"
      else s"($compared)$unitStart"
    }

    /** A `TIMESTAMPDIFF` operand, as the instant it denotes in UTC. The function compares INSTANTS:
      * `ChronoUnit.between` reads its end as the type of its start, so a `ZonedDateTime` start
      * refused a `LocalDate` or a `LocalDateTime` end, and the month count ([[DateDiff.monthKey]])
      * reads a time of day a `LocalDate` does not have. Either way the statement failed.
      *
      *   - An ingest processor reads a DATE-typed operand as the start of its calendar day
      *     (`LocalDate.from`): it holds `CURRENT_DATE` as the current INSTANT, so there the
      *     declared type, not the runtime one, is what says "a date".
      *   - A column is an instant in a script that reads it, though not always a `java.time` one.
      *     An ingest processor parses it into a UTC `ZonedDateTime`
      *     (`SQLTypeUtils.processorTemporal`). Row level reads its column parameter, which holds
      *     its UTC instant ([[in]]) unless a function over the same column read it first: then it
      *     is the raw doc value, which on Elasticsearch 6.8 is no `java.time` type, so it goes
      *     through [[DateDiff.utcZoned]]. A function over a column need not be an instant at all:
      *     `ts::DATE` is a `LocalDate`.
      *   - Any other operand is converted by its RUNTIME type ([[DateDiff.utcInstant]]), which its
      *     declared type does not give: `COALESCE(ts, CURRENT_DATE)`, or a CASE over `CURRENT_DATE`
      *     and a DATE, is a `LocalDate` on one row and a `ZonedDateTime` on another, a `::DATETIME`
      *     a `LocalDateTime`. It is evaluated once, bound to a name where the script can bind one;
      *     a `bucket_script` cannot, and per group such an operand is a constant of the request.
      */
    private def elapsedOperand(
      arg: PainlessScript,
      rendered: String,
      context: Option[PainlessContext]
    ): String = arg match {
      case _ if context.exists(_.isProcessor) && arg.baseType == SQLTypes.Date =>
        s"LocalDate.from($rendered).atStartOfDay(ZoneId.of('Z'))"
      case column: Identifier
          if context.isDefined && column.name.trim.nonEmpty && column.functions.isEmpty =>
        if (context.exists(_.isProcessor)) rendered else DateDiff.utcZoned(rendered)
      case _ => DateDiff.utcInstant(bound(rendered, context))
    }

    /** A rendered operand as [[DateDiff.utcInstant]] may read it, up to four times: a name as it
      * stands, any other expression evaluated once into a parameter where the script can bind one,
      * cast to `def` where it cannot.
      */
    private def bound(rendered: String, context: Option[PainlessContext]): String =
      context match {
        case Some(_) if FunctionN.isName(rendered) => rendered
        case Some(ctx) => ctx.addParam(LiteralParam(rendered)).getOrElse(s"((def) ($rendered))")
        case None      => s"((def) ($rendered))"
      }

    /** `ChronoUnit` has no `QUARTERS`, so a `QUARTER` call failed to compile in Elasticsearch. The
      * ISO quarter-year unit counts the whole quarters between two calendar dates, which between
      * two quarter starts ([[unitStart]]) are the quarter boundaries crossed.
      */
    private def unitPainless(context: Option[PainlessContext]): String = unit match {
      case TimeUnit.QUARTERS => "java.time.temporal.IsoFields.QUARTER_YEARS"
      case other             => other.painless(context)
    }

    /** `TIMESTAMPDIFF`'s MONTH, QUARTER and YEAR are whole months elapsed, divided by 1, 3 or 12.
      */
    private def monthsPerUnit: Option[Int] =
      if (!spelling.elapsed) None
      else
        unit match {
          case TimeUnit.MONTHS   => Some(1)
          case TimeUnit.QUARTERS => Some(3)
          case TimeUnit.YEARS    => Some(12)
          case _                 => None
        }

    /** The function's ONE rendering: row level reaches it from `FunctionN.painless` with its
      * operands rendered, per group from [[painless]]; [[operand]] converts each one.
      */
    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      val perGroup = context.isEmpty && hasAggregation
      val operands = args.zip(callArgs).map { case (arg, rendered) =>
        operand(arg, rendered, context, perGroup)
      }
      val ret = monthsPerUnit match {
        case Some(perUnit) =>
          // each operand is read four times by `monthKey`: bound once where a script can bind it
          val bound =
            operands.map(op => context.flatMap(_.addParam(LiteralParam(op))).getOrElse(op))
          val months =
            s"(${DateDiff.monthKey(bound(1))} - ${DateDiff.monthKey(bound.head)}) / ${DateDiff.MonthKeyScale}L"
          s"Long.valueOf(${if (perUnit == 1) months else s"$months / $perUnit"})"
        case None =>
          s"Long.valueOf(${unitPainless(context)}${DateDiff.painless(context)}(${operands.mkString(", ")}))"
      }
      context match {
        case Some(ctx)
            if ctx.isProcessor => // to fix bug in painless script processor context with elasticsearch v6
          ctx.addParam(LiteralParam(ret)) match {
            case Some(p) => return p
            case _       =>
          }
        case _ =>
      }
      ret
    }

    override def update(request: query.SingleSearch): DateDiff = {
      this.copy(
        start = start match {
          case updatable: Updateable =>
            updatable.update(request).asInstanceOf[PainlessScript]
          case _ => start
        },
        end = end match {
          case updatable: Updateable =>
            updatable.update(request).asInstanceOf[PainlessScript]
          case _ => end
        }
      )
    }
  }

  case object DateAdd extends Expr("DATE_ADD") with TokenRegex {
    override lazy val words: List[String] = List(sql, "DATEADD")
  }

  case class DateAdd(identifier: Identifier, interval: TimeInterval, transactSql: Boolean = false)
      extends DateFunction
      with AddInterval[SQLDate]
      with TransformFunction[SQLDate, SQLDate]
      with FunctionWithIdentifier {
    override def inputType: SQLDate = SQLTypes.Date
    override def outputType: SQLDate = SQLTypes.Date
    override def sql: String = DateAdd.sql
    override def toSQL(base: String): String = {
      if (transactSql) s"$sql(${interval.unit.sql}, ${interval.value}, $base)"
      else s"$sql($base, ${interval.sql})"
    }
    override def dateMathScript: Boolean = identifier.dateMathScript
    override def update(request: query.SingleSearch): DateAdd = {
      this.copy(identifier = identifier.update(request))
    }
  }

  case object DateSub extends Expr("DATE_SUB") with TokenRegex {
    override lazy val words: List[String] = List(sql, "DATESUB")
  }

  case class DateSub(identifier: Identifier, interval: TimeInterval, transactSql: Boolean = false)
      extends DateFunction
      with SubtractInterval[SQLDate]
      with TransformFunction[SQLDate, SQLDate]
      with FunctionWithIdentifier {
    override def inputType: SQLDate = SQLTypes.Date
    override def outputType: SQLDate = SQLTypes.Date
    override def sql: String = DateSub.sql
    override def toSQL(base: String): String =
      if (transactSql) s"$sql(${interval.unit.sql}, ${interval.value}, $base)"
      else s"$sql($base, ${interval.sql})"
    override def dateMathScript: Boolean = identifier.dateMathScript
    override def update(request: query.SingleSearch): DateSub = {
      this.copy(identifier = identifier.update(request))
    }
  }

  sealed trait FunctionWithDateTimeFormat {
    def format: String

    /** The `%f` (fractional seconds) marker, left in the pattern by `convert()` and turned into a
      * VARIABLE-WIDTH fraction here. It is not a letter substitution because no `ofPattern` letter
      * can express one: `S` is fixed width in BOTH directions.
      */
    private val FractionMarker = "%f"

    /** 🔴 The caller's pattern is a SQL STRING LITERAL (`TypeParser.literal`), so it can carry a
      * `"` or a `\\` and it lands inside an emitted Painless DOUBLE-QUOTED literal. Unescaped, a
      * pattern that closes the literal and re-opens one so the parens balance EXECUTES as Painless
      * -- measured on real Elasticsearch 8.18.3: an ingest pipeline wrote an undeclared field into
      * every document it touched (issue #383). `escapePainlessString` is the one owner of this
      * channel; story 21.5 part D applied it to `Value.painless` and `Where.likePainless` and
      * missed these three sites.
      *
      * 🔴 ORDERING, and it is load-bearing: the escaping happens AFTER `convert()` and AFTER the
      * `%f` split, because `at` / `head` / `tail` index into the UNESCAPED pattern. Escaping each
      * piece independently is sound only because the escape map is per-character; escaping the
      * ASSEMBLED call instead would corrupt the emitted code around the literals, and the two
      * bridge `SQLQuerySpec` byte-identity fixtures are what catch that.
      */
    protected def param: String = {
      val pattern = convert()
      val at = pattern.indexOf(FractionMarker)
      if (at < 0) "DateTimeFormatter.ofPattern(\"" + escapePainlessString(pattern) + "\")"
      else {
        // A decimal point written immediately before `%f` BELONGS to the fraction: handing it to
        // `appendFraction` is what makes a zero-nanosecond value format as `12:00:00` instead of
        // `12:00:00.`, and what lets a value with no fraction at all still parse.
        val absorbsPoint = at > 0 && pattern.charAt(at - 1) == '.'
        val head = pattern.substring(0, if (absorbsPoint) at - 1 else at)
        // A second `%f` in one format is meaningless; keep the historical fixed-width mapping for
        // it rather than emitting a pattern `ofPattern` would reject.
        val tail = pattern.substring(at + FractionMarker.length).replace(FractionMarker, "SSS")
        val b = new StringBuilder("new DateTimeFormatterBuilder()")
        if (head.nonEmpty) b.append(s""".appendPattern("${escapePainlessString(head)}")""")
        b.append(s".appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, $absorbsPoint)")
        if (tail.nonEmpty) b.append(s""".appendPattern("${escapePainlessString(tail)}")""")
        b.append(".toFormatter()")
        b.toString
      }
    }

    /** The same formatter with a DEFAULT zone, for a parse that must produce a `ZonedDateTime`.
      *
      * 🔴 This replaces appending `" XXX"` to the user's pattern, which was wrong in BOTH
      * directions and MEASURED so on real ES 8.18 (story 21.8):
      *
      *   - formatting: `DATETIME_FORMAT(ts, 'yyyy')` emitted `ofPattern("yyyy XXX")` and returned
      *     `"2025 Z"` — a silent wrong answer on the ordinary script-field path, for a caller who
      *     asked for four characters;
      *   - parsing: `DATETIME_PARSE('2025-01-10 10:00:00', 'yyyy-MM-dd HH:mm:ss')` emitted
      *     `ofPattern("yyyy-MM-dd HH:mm:ss XXX")`, which demands a space and an offset the caller's
      *     format never declared, so the parse failed at runtime. It "worked" only for input that
      *     happened to carry ` +01:00`.
      *
      * `withZone` is the right tool because it does NOT rewrite the caller's pattern: at parse time
      * it supplies a zone only when the text carried none, and an explicit offset still wins
      * (verified — `2025-01-10T14:30:00+01:00` resolves to `13:30Z`). It is the same mechanism
      * `SQLTypeUtils`' `<string> -> TIMESTAMP` arm uses, deliberately: one derivation, not two.
      */
    protected def zonedParam: String = s"$param.withZone(ZoneId.of('Z'))"

    /** MySQL-style format letters to `java.time` pattern letters.
      *
      * 🔴 `%f` is deliberately ABSENT. MySQL's `%f` is fractional seconds, and it was mapped to
      * `SSS` — three digits — under a comment that said "microseconds". MEASURED on real ES 8.18.3,
      * `S` is FIXED WIDTH in both directions: `SSS` formats `.123456789` as `.123` and REFUSES to
      * parse `.123456`, while `SSSSSS` refuses to parse `.123`. Optional sections do not rescue it
      * either — `[.SSSSSS][.SSS]` formats as `.123456.123`.
      *
      * So no letter substitution can be correct, and picking a width would NARROW one direction to
      * widen the other — an "except" inside the very rule story 21.8's temporal arms are justified
      * by (widen, never narrow). `param` emits a variable-width `appendFraction` instead, which
      * formats the value's real precision and parses any number of digits, including none.
      */
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

    def convert(): String = {
      val basePattern = sqlToJava.foldLeft(format) { case (pattern, (sql, java)) =>
        pattern.replace(sql, java)
      }

      // A literal `Z` in the caller's pattern means "zone NAME" to `ofPattern`, which does not
      // accept the `Z` that ISO-8601 writes for UTC; `X` does. Unchanged, and unrelated to the
      // default-zone question `zonedParam` answers.
      if (basePattern.contains("Z")) basePattern.replace("Z", "X")
      else basePattern
    }
  }

  case object DateParse extends Expr("DATE_PARSE") with TokenRegex with PainlessScript {
    override def painless(context: Option[PainlessContext]): String = ".parse"
    override lazy val words: List[String] = List(sql, "DATEPARSE", "TO_DATE", "PARSE_DATE")
  }

  case class DateParse(identifier: Identifier, format: String)
      extends DateFunction
      with DateMathScript
      with TransformFunction[SQLVarchar, SQLDate]
      with FunctionWithIdentifier
      with FunctionWithDateTimeFormat {
    override def fun: Option[PainlessScript] = None

    override def args: List[PainlessScript] = List(identifier)

    override def inputType: SQLVarchar = SQLTypes.Varchar
    override def outputType: SQLDate = SQLTypes.Date

    /** `LocalDate.parse(arg, fmt)` is a standalone call over a STRING: there is no temporal
      * receiver whose type could be preserved, so this is the second `LocalDate` producer beside
      * `Conversion` — the one issue #384's predicate could not see (§1d).
      */
    override def producesOutputJavaType: Boolean = true

    override def sql: String = DateParse.sql
    override def toSQL(base: String): String = {
      // The pattern is re-emitted as a SQL string literal, so it needs that channel's escaper --
      // `escapeStringLiteral`, the same owner `StringValue.sql` uses. Without it the IDIOMATIC
      // java.time spelling for embedded text (`yyyy'T'MM`) rendered SQL the grammar REJECTS, and
      // this render is not decorative: `SHOW CREATE TABLE` publishes it and
      // `MaterializedViewExtension` persists it and runs `client.run(alter.sql)` (issue #383).
      s"$sql($base, '${escapeStringLiteral(format)}')"
    }

    override def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      callArgs match {
        case arg :: Nil =>
          context match {
            case Some(ctx) =>
              identifier.baseType match {
                // ⚠️ DEAD for every real column, and deliberately left that way. No Elasticsearch
                // mapping reports VARCHAR -- `SQLTypes(String)` yields `Text` or `Keyword` -- so
                // case-object equality never matches here. Widening it to `_: SQLVarchar` was
                // MEASURED and REVERTED: binding the parse as a parameter evaluates it EAGERLY,
                // ahead of the null guard that `FunctionN.painless` puts around the reference, so
                // a document merely MISSING the field ran `LocalDate.parse(null, ...)` and failed
                // the shard. The inlined form below keeps the parse inside the guard, and the
                // assembly binds the guarded expression itself when a later function needs to
                // chain onto it.
                case SQLTypes.Varchar =>
                  ctx.addParam(LiteralParam(s"LocalDate.parse($arg, $param)")) match {
                    case Some(p) => return p
                    case _       =>
                  }
                case _ =>
              }
            case _ =>
          }
          // 🔴 In an INGEST script the result is ASSIGNED to `ctx.<column>`, and Elasticsearch
          // refuses a `java.time.LocalDate` there — `illegal_argument_exception: unexpected value
          // type [class java.time.LocalDate]`, measured, and the computed column vanished. A
          // `ZonedDateTime` IS accepted and serialises as ISO-8601, which a `date` field parses.
          // Same collapse the operand side applies: in a processor every temporal value is a
          // ZonedDateTime, so there is one rule rather than one per position.
          if (context.exists(_.isProcessor))
            s"LocalDate.parse($arg, $param).atStartOfDay(ZoneId.of('Z'))"
          else
            s"LocalDate.parse($arg, $param)"
        case _ => throw new IllegalArgumentException("DateParse requires exactly one argument")
      }

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

    override def shouldBeScripted: Boolean = true // FIXME

    override def update(request: query.SingleSearch): DateParse = {
      this.copy(identifier = identifier.update(request))
    }
  }

  case object DateFormat extends Expr("DATE_FORMAT") with TokenRegex with PainlessScript {
    override def painless(context: Option[PainlessContext]): String = ".format"
    override lazy val words: List[String] = List(sql, "FORMAT", "DATEFORMAT")
  }

  case class DateFormat(identifier: Identifier, format: String)
      extends DateFunction
      with TransformFunction[SQLDate, SQLVarchar]
      with FunctionWithIdentifier
      with FunctionWithDateTimeFormat {
    override def fun: Option[PainlessScript] = Some(DateFormat)

    override def args: List[PainlessScript] = List(identifier)

    override def inputType: SQLDate = SQLTypes.Date
    override def outputType: SQLVarchar = SQLTypes.Varchar

    override def sql: String = DateFormat.sql
    override def toSQL(base: String): String = {
      // The pattern is re-emitted as a SQL string literal, so it needs that channel's escaper --
      // `escapeStringLiteral`, the same owner `StringValue.sql` uses. Without it the IDIOMATIC
      // java.time spelling for embedded text (`yyyy'T'MM`) rendered SQL the grammar REJECTS, and
      // this render is not decorative: `SHOW CREATE TABLE` publishes it and
      // `MaterializedViewExtension` persists it and runs `client.run(alter.sql)` (issue #383).
      s"$sql($base, '${escapeStringLiteral(format)}')"
    }

    override def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      callArgs match {
        case arg :: Nil =>
          // 🔴 A STRING operand is not a `TemporalAccessor`, and `DateTimeFormatter.format` takes
          // one. MEASURED on real Elasticsearch 8.18.3, both spellings were a script COMPILE error:
          //
          //   DATE_FORMAT('2025-01-10', '%Y')  -> Cannot cast from [java.lang.String] to
          //   DATETIME_FORMAT(<keyword col>, '%Y')   [java.time.temporal.TemporalAccessor]
          //
          // The documented spelling `DATE_FORMAT('2025-01-10'::DATE, '%Y-%m-%d')` worked only
          // because the CAST inserted the parse, which is why no published example ever failed.
          // The operand is parsed here through the SAME `coerce` arms the cast uses -- one
          // derivation, so the accepted format set cannot differ between the two spellings.
          //
          // ⚠️ The type test is `_: SQLVarchar`, not `SQLTypes.Varchar`. Case-object equality is
          // what made the old branch DEAD for every real column: `SQLTypes(String)` maps an
          // Elasticsearch string field to `Text` or `Keyword` and never to the `Varchar` object.
          // Same defect, same fix, as the `<string> -> <numeric|temporal>` arms in story 21.5.
          // ⚠️ `nullable = identifier.nullable`, not `false`: `coerce`'s temporal arms guard their
          // own parse only when told the operand can be null, and the parse is bound as its own
          // parameter, i.e. EVALUATED before the guard `FunctionN.painless` puts around the format
          // call. Passing `false` here made a document merely MISSING the field run
          // `LocalDate.parse(null, ...)` and fail the shard.
          val operand = identifier.baseType match {
            case _: SQLVarchar =>
              SQLTypeUtils
                .coerce(arg, identifier.baseType, inputType, identifier.nullable, context)
            case _ => arg
          }
          context match {
            case Some(ctx) =>
              ctx.addParam(LiteralParam(param)) match {
                case Some(p) => return s"$p.format($operand)"
                case _       =>
              }
            case _ =>
          }
          s"$param.format($operand)"
        case _ => throw new IllegalArgumentException("DateParse requires exactly one argument")
      }

    override def update(request: query.SingleSearch): DateFormat = {
      this.copy(identifier = identifier.update(request))
    }
  }

  /** `TIMESTAMPADD` is the ODBC/JDBC spelling (`{fn TIMESTAMPADD(SQL_TSI_DAY, -89, …)}`), which BI
    * tools emit directly. It is an ALIAS, not a second mechanism: the `(unit, count, base)`
    * argument order it uses is the `transactSql` form this function has always parsed, so the
    * render normalises to `DATETIME_ADD` and re-parses.
    */
  case object DateTimeAdd extends Expr("DATETIME_ADD") with TokenRegex {
    override lazy val words: List[String] = List(sql, "DATETIMEADD", "TIMESTAMPADD")
  }

  case class DateTimeAdd(
    identifier: Identifier,
    interval: TimeInterval,
    transactSql: Boolean = false
  ) extends DateTimeFunction
      with AddInterval[SQLDateTime]
      with TransformFunction[SQLDateTime, SQLDateTime]
      with FunctionWithIdentifier {
    override def inputType: SQLDateTime = SQLTypes.DateTime
    override def outputType: SQLDateTime = SQLTypes.DateTime
    override def sql: String = DateTimeAdd.sql
    override def toSQL(base: String): String = {
      if (transactSql) s"$sql(${interval.unit.sql}, ${interval.value}, $base)"
      else s"$sql($base, ${interval.sql})"
    }
    override def dateMathScript: Boolean = identifier.dateMathScript

    override def update(request: query.SingleSearch): DateTimeAdd = {
      this.copy(identifier = identifier.update(request))
    }
  }

  case object DateTimeSub extends Expr("DATETIME_SUB") with TokenRegex {
    override lazy val words: List[String] = List(sql, "DATETIMESUB")
  }

  case class DateTimeSub(
    identifier: Identifier,
    interval: TimeInterval,
    transactSql: Boolean = false
  ) extends DateTimeFunction
      with SubtractInterval[SQLDateTime]
      with TransformFunction[SQLDateTime, SQLDateTime]
      with FunctionWithIdentifier {
    override def inputType: SQLDateTime = SQLTypes.DateTime
    override def outputType: SQLDateTime = SQLTypes.DateTime
    override def sql: String = DateTimeSub.sql
    override def toSQL(base: String): String =
      if (transactSql) s"$sql(${interval.unit.sql}, ${interval.value}, $base)"
      else s"$sql($base, ${interval.sql})"
    override def dateMathScript: Boolean = identifier.dateMathScript

    override def update(request: query.SingleSearch): DateTimeSub = {
      this.copy(identifier = identifier.update(request))
    }
  }

  case object DateTimeParse extends Expr("DATETIME_PARSE") with TokenRegex with PainlessScript {
    override def painless(context: Option[PainlessContext]): String = ".parse"
    override lazy val words: List[String] =
      List(sql, "DATETIMEPARSE", "TO_TIMESTAMP", "PARSE_DATETIME")
  }

  case class DateTimeParse(identifier: Identifier, format: String)
      extends DateTimeFunction
      with TransformFunction[SQLVarchar, SQLDateTime]
      with FunctionWithIdentifier
      with FunctionWithDateTimeFormat
      with DateMathScript {
    override def fun: Option[PainlessScript] = None

    override def args: List[PainlessScript] = List(identifier)

    override def inputType: SQLVarchar = SQLTypes.Varchar
    override def outputType: SQLDateTime = SQLTypes.DateTime

    /** The `DATETIME` twin of `DateParse` above: `ZonedDateTime.parse(arg, fmt)` over a STRING, so
      * it produces rather than preserves (issue #384).
      */
    override def producesOutputJavaType: Boolean = true

    override def sql: String = DateTimeParse.sql
    override def toSQL(base: String): String = {
      // The pattern is re-emitted as a SQL string literal, so it needs that channel's escaper --
      // `escapeStringLiteral`, the same owner `StringValue.sql` uses. Without it the IDIOMATIC
      // java.time spelling for embedded text (`yyyy'T'MM`) rendered SQL the grammar REJECTS, and
      // this render is not decorative: `SHOW CREATE TABLE` publishes it and
      // `MaterializedViewExtension` persists it and runs `client.run(alter.sql)` (issue #383).
      s"$sql($base, '${escapeStringLiteral(format)}')"
    }

    override def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      callArgs match {
        case arg :: Nil =>
          context match {
            case Some(ctx) =>
              identifier.baseType match {
                // ⚠️ DEAD for every real column, and deliberately left that way. No Elasticsearch
                // mapping reports VARCHAR -- `SQLTypes(String)` yields `Text` or `Keyword` -- so
                // case-object equality never matches here. Widening it to `_: SQLVarchar` was
                // MEASURED and REVERTED: binding the parse as a parameter evaluates it EAGERLY,
                // ahead of the null guard that `FunctionN.painless` puts around the reference, so
                // a document merely MISSING the field ran `LocalDate.parse(null, ...)` and failed
                // the shard. The inlined form below keeps the parse inside the guard, and the
                // assembly binds the guarded expression itself when a later function needs to
                // chain onto it.
                case SQLTypes.Varchar =>
                  ctx.addParam(LiteralParam(s"ZonedDateTime.parse($arg, $zonedParam)")) match {
                    case Some(p) => return p
                    case _       =>
                  }
                case _ =>
              }
            case _ =>
          }
          s"ZonedDateTime.parse($arg, $zonedParam)"
        case _ => throw new IllegalArgumentException("DateParse requires exactly one argument")
      }

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

    override def update(request: query.SingleSearch): DateTimeParse = {
      this.copy(identifier = identifier.update(request))
    }
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

    override def args: List[PainlessScript] = List(identifier)

    override def inputType: SQLDateTime = SQLTypes.DateTime
    override def outputType: SQLVarchar = SQLTypes.Varchar

    override def sql: String = DateTimeFormat.sql
    override def toSQL(base: String): String = {
      // The pattern is re-emitted as a SQL string literal, so it needs that channel's escaper --
      // `escapeStringLiteral`, the same owner `StringValue.sql` uses. Without it the IDIOMATIC
      // java.time spelling for embedded text (`yyyy'T'MM`) rendered SQL the grammar REJECTS, and
      // this render is not decorative: `SHOW CREATE TABLE` publishes it and
      // `MaterializedViewExtension` persists it and runs `client.run(alter.sql)` (issue #383).
      s"$sql($base, '${escapeStringLiteral(format)}')"
    }

    override def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      callArgs match {
        case arg :: Nil =>
          // 🔴 A STRING operand is not a `TemporalAccessor`, and `DateTimeFormatter.format` takes
          // one. MEASURED on real Elasticsearch 8.18.3, both spellings were a script COMPILE error:
          //
          //   DATE_FORMAT('2025-01-10', '%Y')  -> Cannot cast from [java.lang.String] to
          //   DATETIME_FORMAT(<keyword col>, '%Y')   [java.time.temporal.TemporalAccessor]
          //
          // The documented spelling `DATE_FORMAT('2025-01-10'::DATE, '%Y-%m-%d')` worked only
          // because the CAST inserted the parse, which is why no published example ever failed.
          // The operand is parsed here through the SAME `coerce` arms the cast uses -- one
          // derivation, so the accepted format set cannot differ between the two spellings.
          //
          // ⚠️ The type test is `_: SQLVarchar`, not `SQLTypes.Varchar`. Case-object equality is
          // what made the old branch DEAD for every real column: `SQLTypes(String)` maps an
          // Elasticsearch string field to `Text` or `Keyword` and never to the `Varchar` object.
          // Same defect, same fix, as the `<string> -> <numeric|temporal>` arms in story 21.5.
          // ⚠️ `nullable = identifier.nullable`, not `false`: `coerce`'s temporal arms guard their
          // own parse only when told the operand can be null, and the parse is bound as its own
          // parameter, i.e. EVALUATED before the guard `FunctionN.painless` puts around the format
          // call. Passing `false` here made a document merely MISSING the field run
          // `LocalDate.parse(null, ...)` and fail the shard.
          val operand = identifier.baseType match {
            case _: SQLVarchar =>
              SQLTypeUtils
                .coerce(arg, identifier.baseType, inputType, identifier.nullable, context)
            case _ => arg
          }
          context match {
            case Some(ctx) =>
              ctx.addParam(LiteralParam(param)) match {
                case Some(p) => return s"$p.format($operand)"
                case _       =>
              }
            case _ =>
          }
          s"$param.format($operand)"
        case _ => throw new IllegalArgumentException("DateParse requires exactly one argument")
      }

    override def update(request: query.SingleSearch): DateTimeFormat = {
      this.copy(identifier = identifier.update(request))
    }
  }

}
