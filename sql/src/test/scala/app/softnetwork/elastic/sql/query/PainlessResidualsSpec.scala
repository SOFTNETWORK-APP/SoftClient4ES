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

/** Issue #373 — the Painless-emission defects surfaced by #370, fixed in one PR, one commit each.
  *
  * All nine are PRE-EXISTING: each was found by EXECUTING the emission of a #370 shape on real
  * Elasticsearch and comparing it with a control at `main`. They share one theme — the emitted
  * script must match the RECEIVER the pipeline really hands it, and the chain that produced it —
  * but each has its own decision point, so each gets its own commit and its own test here.
  *
  * 🔴 What a byte pin in this file can and cannot say: it fixes the emission, not its validity.
  * Every expectation below names the Elasticsearch behaviour it was measured against, and the
  * executed row lives in `PredicateFunctionResultSpec`.
  */
class PainlessResidualsSpec extends AnyFlatSpec with Matchers {

  private val schema: SchemaTable = SchemaTable(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("name", SQLTypes.Keyword),
      Column("n", SQLTypes.Int)
    )
  )

  private def single(sql: String, withSchema: Boolean = true): SingleSearch =
    Parser(sql) match {
      case Right(ss: SingleSearch) => if (withSchema) ss.update(Some(schema)) else ss.update()
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  /** The WHERE criteria rendered as ONE script, prologue included. */
  protected def predicateOf(sql: String, withSchema: Boolean = true): String = {
    val ctx = PainlessContext(PainlessContextType.Query)
    val body = single(sql, withSchema).where
      .flatMap(_.criteria)
      .map(_.painless(Some(ctx)))
      .getOrElse(fail(s"[$sql] has no WHERE"))
    s"$ctx$body"
  }

  /** The first SELECT field rendered as one script. */
  protected def fieldOf(sql: String, withSchema: Boolean = true): String = {
    val ctx = PainlessContext(PainlessContextType.Query)
    // 🔴 The body is rendered FIRST and bound: it is what REGISTERS the parameters, and a string
    // interpolation evaluates `ctx.toString` before the call beside it, so an inline
    // `s"$ctx${…painless(ctx)}"` prints an EMPTY prologue. Caught by this spec's own NULLIF pin.
    val body = single(sql, withSchema).select.fields.head.painless(Some(ctx))
    s"$ctx$body"
  }

  // -- item 2: the comparison spelling must exist on the receiver --------------------------------

  /** `java.time.LocalTime` is the only temporal without `isEqual`: `LocalDate` and `LocalDateTime`
    * declare it and `ZonedDateTime` inherits it from `ChronoZonedDateTime` (checked with `javap`).
    * MEASURED on ES 8.18 before this commit: `dynamic method [java.time.LocalTime, isEqual/1] not
    * found` — a shard failure on `main`, and the reason #370 could pin the TIME identity but not
    * execute it.
    */
  "a TIME comparison" should "use a spelling LocalTime has" in {
    predicateOf("SELECT name FROM t WHERE CAST(d AS TIME) = CAST('00:00:00' AS TIME)") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value.toLocalTime()); " +
    "def param2 = LocalTime.parse(\"00:00:00\", DateTimeFormatter.ISO_LOCAL_TIME); " +
    "param1 == null ? false : (param1.compareTo(param2) == 0)"
    predicateOf("SELECT name FROM t WHERE CAST(d AS TIME) <> CAST('00:00:00' AS TIME)") should
    endWith("param1 == null ? false : (param1.compareTo(param2) != 0)")
  }

  it should "leave every other temporal on isEqual" in {
    // `isEqual` is instant equality for a `ZonedDateTime` and is NOT `equals` (which compares the
    // zone too), so this arm must not be widened: DATE and TIMESTAMP keep their spelling.
    predicateOf("SELECT name FROM t WHERE CAST(d AS DATE) = CAST('2025-01-05' AS DATE)") should
    endWith("param1 == null ? false : (param1.isEqual(param2))")
    predicateOf("SELECT name FROM t WHERE ts = CAST('2025-01-05T00:00:00' AS TIMESTAMP)") should
    endWith("param1 == null ? false : (param1.isEqual(param2))")
  }

  /** 🔴 The dispatch reads `valueType` -- the type of the RIGHT operand -- while the receiver is
    * the LEFT one, so a MISMATCHED pair reaches this arm too (`name` is a keyword column; the
    * statement is accepted because a bare column is `Any` at parse time). `equals` takes an
    * `Object` and would answer FALSE for that, turning a loud shard failure into a silent wrong
    * answer -- and `<>` into "every document". `compareTo` keeps it loud. MEASURED on ES 8.18, all
    * three on a `String` receiver: `equals` -> `[false,false,false]`; `compareTo` ->
    * `class_cast_exception`; `isEqual` (main) -> `dynamic method … not found`.
    *
    * Found by review of this commit; the shape is reachable today through a CASE, and the item-4
    * commit routes the bare `WHERE name = CAST(… AS TIME)` here too.
    */
  it should "stay LOUD when the receiver is not a LocalTime" in {
    predicateOf(
      "SELECT name FROM t WHERE CASE WHEN name = CAST('00:00:00' AS TIME) THEN 1 ELSE 0 END = 1"
    ) should include("param1.compareTo(param2) == 0")
  }

  /** `NULLIF` had the same hole at its own site. 🔴 Neither `out` nor `expr1.out` could key it:
    * both report TIMESTAMP, the COLUMN's type, even though both arguments are TIME --
    * `Identifier.chainType` (#367) is the derivation that describes the RENDERING, and two wrong
    * keys were tried before that was measured. Executed on ES 8.18 over midnight dates: before,
    * `dynamic method [java.time.LocalTime, isEqual/1] not found`; after, `null` on every row (they
    * all equal 00:00:00, which is what NULLIF returns null for).
    */
  "NULLIF over a TIME receiver" should "use the same spelling" in {
    fieldOf("SELECT NULLIF(CAST(d AS TIME), CAST('00:00:00' AS TIME)) AS c FROM t") should
    include("param2 == null || param2.compareTo(param3) == 0 ? null : param2")
  }

  it should "leave the ordering comparisons alone" in {
    // `isBefore` / `isAfter` DO exist on `LocalTime`, so only `=` and `<>` moved.
    predicateOf("SELECT name FROM t WHERE CAST(d AS TIME) > CAST('00:30:00' AS TIME)") should
    endWith("(param1.isAfter(param2))")
  }
}
