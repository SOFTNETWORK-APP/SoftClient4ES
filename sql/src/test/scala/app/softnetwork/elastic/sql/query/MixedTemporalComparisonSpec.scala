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

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

/** A comparison whose two sides render DIFFERENT `java.time` types (issue #373, item 1).
  *
  * `CAST(d AS DATE)` emits a `LocalDate`; a raw date column is a `ZonedDateTime`. Comparing them
  * was `class_cast_exception` on ES 8.18.3 for an ordering operator, and -- worse -- silently FALSE
  * for `=` on the no-schema path, where `==` on two different types is simply never true.
  *
  * 🔴 THE CAUSE WAS A TYPE THAT DID NOT DESCRIBE THE RENDERING. `Expression.leftOperand` coerces
  * the operand to the common super type of both sides, and that machinery was always correct:
  * `leastCommonSuperType(DATE, TIMESTAMP)` is TIMESTAMP and the coercion is
  * `.atStartOfDay(ZoneId.of('Z'))`, which is ANSI's DATE -> TIMESTAMP promotion. It never fired,
  * because `operandType` reported the COLUMN's type for any temporal chain over a temporal column,
  * so the coercion was computed as TIMESTAMP -> TIMESTAMP: a no-op, against a chain that had
  * already produced a `LocalDate`.
  *
  * 🔴 AND `chainType` (#367) CANNOT BE THE DISCRIMINATOR, which is why this outlived that fix.
  * `chainType` is a SQL type: `DATE_ADD(ts, INTERVAL 1 DAY)` reports DATE while still rendering a
  * `ZonedDateTime`, and `DATE_TRUNC(d, MONTH)` reports TEMPORAL for the same reason -- both keep
  * the receiver's Java type, which is exactly what the override is right about. Only a `Conversion`
  * to a date- or time-only type changes it, and the conversion is the thing that knows.
  *
  * 🔴 NOR `rendersLocalDate`, measured false here: it reads `painlessMethods` on the parameter, and
  * with a schema attached the conversion emits the narrowing INLINE rather than registering it (it
  * registers only on the `inputType == Any` no-schema path). The same CAST is therefore visible to
  * that predicate on one path and invisible on the other.
  */
class MixedTemporalComparisonSpec extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  // `SchemaTable`, because `TableDrivenPropertyChecks` shadows the name `Table`.
  private val schema = SchemaTable(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("name", SQLTypes.Keyword),
      Column("n", SQLTypes.Int)
    )
  )

  private def predicateOf(sql: String): String = Parser(sql) match {
    case Right(ss: SingleSearch) =>
      val ctx = PainlessContext(PainlessContextType.Query)
      val body = ss
        .update(Some(schema))
        .where
        .flatMap(_.criteria)
        .map(_.painless(Some(ctx)))
        .getOrElse(fail(s"[$sql] no WHERE criteria"))
      s"$ctx$body"
    case other => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  /** The two sides of a temporal comparison must end up the same Java type. Asserted as the
    * MECHANISM -- a promotion is present on the converted side -- rather than by byte pin, so the
    * rule survives a change in how the promotion is spelled. The byte pins live in
    * `ParameterIdentitySpec`, whose expectations these shapes moved.
    */
  /** 🔴 WHAT THIS FIX DOES NOT COVER, each MEASURED on ES 8.18.3 and identical before and after.
    * Recorded here because the rule above reads more general than it is: only the LEFT operand is
    * coerced -- `painlessValue` renders the right one raw -- so the reconciliation is one-sided.
    *
    *   - REVERSED ORDER. `ts > CAST(d AS DATE)`, `ts = CAST(d AS DATE)`, `d = CAST(ts AS DATE)`:
    *     `Cannot cast java.time.LocalDate to java.time.chrono.ChronoZonedDateTime`.
    *   - A RIGHT side that is not TIMESTAMP/DATETIME. `CAST(d AS DATE) > DATE_TRUNC(ts, MONTH)`
    *     gives `lcs(DATE, TEMPORAL) = DATE`, which short-circuits the override before this
    *     predicate is consulted.
    *   - A `LocalDate` produced WITHOUT a `Conversion`. `DATE_PARSE(DATE_FORMAT(d, 'yyyy-MM-dd'),
    *     'yyyy-MM-dd') > ts` renders `LocalDate.parse` over a temporal base column, and `DateParse`
    *     is a second `LocalDate` producer this predicate cannot see.
    *   - `BETWEEN`, which fails differently again: the from/to pair's type is not temporal, so
    *     `check` emits a raw `>=` / `<=` and answers `Cannot apply [>] operation to types
    *     [java.time.LocalDate] and [java.time.ZonedDateTime]`.
    *   - The TIME twin, `CAST(ts AS TIME) > ts`: `coerce` has no `(Time, _)` SOURCE arm, so nothing
    *     is emitted and it stays `Cannot cast java.time.ZonedDateTime to java.time.LocalTime`.
    *
    * All five are the same one-sided reconciliation seen from different angles. They are left for a
    * follow-up rather than widened into here, where each would need its own measured promotion.
    */
  "a date-only chain compared with a raw timestamp column" should "promote to a common type" in {
    forAll(
      Table(
        ("sql", "why"),
        (
          "SELECT name FROM t WHERE CAST(d AS DATE) > ts",
          "two DIFFERENT columns, so no shared parameter is involved at all"
        ),
        (
          "SELECT name FROM t WHERE CAST(d AS DATE) - INTERVAL 3 DAY > d",
          "the interval keeps the LocalDate, and the right side is the raw column"
        ),
        (
          "SELECT name FROM t WHERE CAST(d AS DATE) = d",
          "the `=` spelling -- this one was SILENT, never true for any document"
        ),
        (
          "SELECT name FROM t WHERE CAST(d AS DATE) < ts",
          "the ordering twin"
        )
      )
    ) { (sql, why) =>
      val emitted = predicateOf(sql)
      withClue(s"[$sql] -- $why ->\n$emitted\n") {
        // 🔴 On the OPERAND, not merely somewhere in the script: a bare `include("atStartOfDay")`
        // is satisfied by the substring appearing on the right-hand side or on an unrelated
        // parameter, and two of these rows have no byte pin anywhere else (review).
        emitted should include regex """def left\d+ = \(param\d+ != null \? param\d+\.atStartOfDay\("""
      }
    }
  }

  /** 🔴 The half that protects the override this fix narrowed. Every one of these keeps the
    * receiver's `ZonedDateTime`, so promoting them would be wrong -- and a `.atStartOfDay` on a
    * `ZonedDateTime` does not exist, so it would fail the shard rather than fail quietly.
    */
  "a chain that keeps the receiver's type" should "not be promoted" in {
    forAll(
      Table(
        ("sql", "why"),
        (
          "SELECT name FROM t WHERE DATE_ADD(ts, INTERVAL 1 DAY) > ts",
          "chainType says DATE, the rendering is a ZonedDateTime"
        ),
        ("SELECT name FROM t WHERE DATE_TRUNC(d, MONTH) < d", "truncation preserves the type"),
        ("SELECT name FROM t WHERE DATE_TRUNC(d, QUARTER) < d", "the binding variant of the same"),
        ("SELECT name FROM t WHERE d > ts", "no chain at all"),
        ("SELECT name FROM t WHERE YEAR(d) = 2025", "an extraction leaves a number"),
        ("SELECT name FROM t WHERE LAST_DAY(d) = d", "an adjuster preserves the type"),
        // 🔴 These four carry a `Conversion` and must still NOT promote. They do NOT bind the
        // `targetType == Date` clause, and saying otherwise would be false: relaxing it to
        // `case _: Conversion => true` leaves them -- and the whole suite -- green. That is
        // because the clause is consulted only when `chainType` is TEMPORAL, and for a temporal
        // conversion that is not to DATE the chain type and the column type agree, so both
        // branches answer the same. The restriction is kept for correctness rather than coverage;
        // these rows guard the surrounding rule, not that clause.
        (
          "SELECT name FROM t WHERE CAST(d AS TIMESTAMP) > ts",
          "a CAST to TIMESTAMP keeps the type"
        ),
        ("SELECT name FROM t WHERE CAST(d AS DATETIME) > ts", "and to DATETIME"),
        ("SELECT name FROM t WHERE CAST(d AS VARCHAR) = name", "a CAST to VARCHAR is a String"),
        ("SELECT name FROM t WHERE CAST(d AS BIGINT) > n", "a CAST to BIGINT is a number")
      )
    ) { (sql, why) =>
      val emitted = predicateOf(sql)
      withClue(s"[$sql] -- $why ->\n$emitted\n")(emitted should not include "atStartOfDay")
    }
  }

  /** A comparison of two date-only renderings needs no promotion either: both sides are already a
    * `LocalDate`, and widening one of them would reintroduce the mismatch from the other side.
    */
  "two date-only operands" should "stay date-only" in {
    val emitted =
      predicateOf("SELECT name FROM t WHERE CAST(d AS DATE) = CAST('2025-01-05' AS DATE)")
    withClue(s"$emitted\n") {
      emitted should not include "atStartOfDay"
      emitted should include("isEqual")
    }
  }

  /** 🔴 TWO SHAPES REMAIN BROKEN, recorded so they are visible rather than rediscovered. Neither is
    * owned by this fix and both are measured:
    *
    *   - `CAST(d AS DATE) = d` with NO schema attached. There the right operand's type is `ANY`, so
    *     the common super type is DATE and nothing tells the raw side to narrow. Emitting a
    *     narrowing on an operand of unknown type is what #367 measured failing the shard for a
    *     `keyword` column, so it is deliberately not forced here.
    *   - `DATE_TRUNC(d, MONTH) = CAST('2025-01-01' AS DATE) AND DATE_TRUNC(d, MONTH) < d`, where a
    *     DATE-literal comparison narrows a parameter the second predicate SHARES. That is issue
    *     #373 item 8 -- parameter identity, not comparison reconciliation -- and is filed
    *     separately.
    */
  "the one-sided reconciliation" should "be characterised, not silently accepted" in {
    // The reversed order is the same defect and is NOT fixed: asserted so the gap is visible and
    // cannot be closed by accident. Measured on 8.18.3 as a class_cast_exception.
    forAll(
      Table(
        "sql",
        "SELECT name FROM t WHERE ts > CAST(d AS DATE)",
        "SELECT name FROM t WHERE d = CAST(ts AS DATE)",
        "SELECT name FROM t WHERE CAST(ts AS TIME) > ts"
      )
    ) { sql =>
      val emitted = predicateOf(sql)
      withClue(s"[$sql] ->\n$emitted\n")(emitted should not include "atStartOfDay")
    }
  }

  "the residual shapes" should "be characterised, not silently accepted" in {
    val noSchema = Parser("SELECT name FROM t WHERE CAST(d AS DATE) = d") match {
      case Right(ss: SingleSearch) =>
        val ctx = PainlessContext(PainlessContextType.Query)
        val body = ss.update().where.flatMap(_.criteria).map(_.painless(Some(ctx))).getOrElse("")
        s"$ctx$body"
      case other => fail(s"unexpected: $other")
    }
    withClue(s"no-schema ->\n$noSchema\n")(noSchema should not include "atStartOfDay")

    val shared = predicateOf(
      "SELECT name FROM t WHERE DATE_TRUNC(d, MONTH) = CAST('2025-01-01' AS DATE) " +
      "AND DATE_TRUNC(d, MONTH) < d"
    )
    // the shared parameter still carries the DATE-literal narrowing (item 8)
    withClue(s"shared ->\n$shared\n")(shared should include(".toLocalDate().withDayOfMonth(1)"))
  }
}
