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

  /** The ingest-processor source of a computed column, through the production seam
    * (`Table.update()` resolves the processors; the raw parsed column list does not).
    */
  protected def processorOf(sql: String, column: String = "c"): String =
    Parser(sql) match {
      case Right(ct: CreateTable) =>
        ct.schema.columns
          .find(_.name == column)
          .flatMap(_.script)
          .map(_.source)
          .getOrElse(fail(s"no script processor for [$column] in [$sql]"))
      case other => fail(s"[$sql] expected a CreateTable, got $other")
    }

  /** How many times `needle` occurs in `haystack` -- the shape of an "evaluated once" assertion. */
  protected def countOf(haystack: String, needle: String): Int =
    haystack.sliding(needle.length).count(_ == needle)

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
    include("param2 == null || (param3 != null && param2.compareTo(param3) == 0) ? null : param2")
  }

  it should "leave the ordering comparisons alone" in {
    // `isBefore` / `isAfter` DO exist on `LocalTime`, so only `=` and `<>` moved.
    predicateOf("SELECT name FROM t WHERE CAST(d AS TIME) > CAST('00:30:00' AS TIME)") should
    endWith("(param1.isAfter(param2))")
  }

  // -- item 3: a CASE candidate that can be null must be guarded ---------------------------------

  /** `checkCase` emitted `e != null && e.isEqual(c) ? v` and called the comparison with a `null`
    * candidate for any document missing the WHEN column. MEASURED on ES 8.18 over the very shape
    * the bridge fixture pins, with a document lacking `lastSeen`:
    * {{{
    * before: null_pointer_exception: Cannot invoke "ChronoLocalDate.toEpochDay()" because "other" is null
    * after : d -> 2025-01-03 (lastSeen + 2)   e -> 2025-01-04 (lastUpdated)   g -> 2024-10-01 (ELSE)
    * }}}
    * So the published `CASE … WHEN <column> …` failed the whole query on any index where that
    * column is not set on every document.
    */
  "a CASE candidate that can be null" should "be guarded before the comparison" in {
    fieldOf("SELECT CASE ts WHEN d THEN d ELSE ts END AS c FROM t") shouldBe
    "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "param1 != null && param2 != null && param1.isEqual(param2) ? param2 : param1"
  }

  /** 🔴 The TIME spelling must follow the RECEIVER, not `out`. Found by review: an arm keyed on
    * `out` -- the CASE's RESULT type, the least common supertype of expression, conditions and
    * results -- was DEAD, because `out` reports TIMESTAMP for a CASE whose operands are both
    * `LocalTime`. MEASURED on ES 8.18 before this: `dynamic method [java.time.LocalTime, isEqual/1]
    * not found`, the very failure this PR's first commit exists to remove. `Identifier.chainType`
    * (#367) is the derivation that describes the rendering -- the same key `NULLIF` needed, and not
    * carrying it across is what left the arm dead.
    */
  it should "spell a TIME comparison from the receiver, not from the CASE's result type" in {
    val emitted = fieldOf(
      "SELECT CASE CAST(d AS TIME) WHEN CAST(ts AS TIME) " +
      "THEN CAST('01:00:00' AS TIME) ELSE CAST('02:00:00' AS TIME) END AS c FROM t"
    )
    withClue(emitted) {
      emitted should include("param2 != null && param5 != null && param2.compareTo(param5) == 0")
      emitted should not include "isEqual"
    }
  }

  /** 🔴 `NULLIF` had the same hole one method up in the same file, and shipping the CASE half alone
    * would have made this commit's own claim -- that the sites cannot drift -- false again by the
    * next release (found by review). MEASURED on ES 8.18 over a document with no `ts`: before,
    * `null_pointer_exception: Cannot read field "hour" because "other" is null`; after, that
    * document yields its OWN time.
    *
    * 🔴 The guard means NOT-EQUAL, not null: SQL says `NULLIF(a, b)` is NULL when `a = b`, and with
    * `b` NULL the comparison is UNKNOWN, so the answer is `a`. Short-circuiting `b == null` to the
    * NULL branch would have inverted it -- executed, the document without `ts` correctly returns
    * `00:00:00` rather than null.
    */
  it should "guard NULLIF's second argument, without inverting its semantics" in {
    fieldOf("SELECT NULLIF(CAST(d AS TIME), CAST(ts AS TIME)) AS c FROM t") should include(
      "def param5 = param3 == null || (param4 != null && param3.compareTo(param4) == 0) ? null : param3"
    )
  }

  it should "guard a candidate whose NULLABILITY the AST does not report" in {
    // 🔴 Found by review. The first version asked `cond.nullable`, and MEASURED that is FALSE for
    // a function-wrapped column: `UPPER(other)` reports `nullable = false` while rendering
    // `(param3 == null) ? null : param3.toUpperCase()`, so the candidate was neither bound nor
    // guarded and `CASE UPPER(name) WHEN UPPER(other) …` threw
    // `null_pointer_exception: Cannot read field "value"` on ES 8.18 for a document missing the
    // column. None of the three structural predicates I measured can see it -- the wrapper's
    // `name` is EMPTY, its `nullable` is false, and its function is not a `FunctionWithIdentifier`
    // -- so the candidate is now ALWAYS bound and ALWAYS guarded, which is provably safe:
    // binding is a no-op when it is already a name, and a redundant `!= null` cannot change an
    // answer.
    val emitted = fieldOf("SELECT CASE UPPER(name) WHEN UPPER(other) THEN 1 ELSE 0 END AS c FROM t")
    withClue(emitted) {
      emitted should include("param2 != null && param4 != null && param2.compareTo(param4) == 0")
    }
  }

  it should "leave a non-temporal CASE on the null-safe ==" in {
    // `==` is null-safe in Painless, so a numeric candidate needs no `!= null` guard -- only the
    // binding, which is what keeps a chain from being rendered twice.
    fieldOf("SELECT CASE n WHEN 1 THEN 1 ELSE 0 END AS c FROM t") should endWith(
      "def param3 = 1; param2 == param3 ? 1 : 0"
    )
  }

  // -- item 6: the PROCESSOR and TRANSFORM contexts key on the chain too ------------------------

  /** #370 made one parameter per (column, folded chain) -- but only in a QUERY context. An
    * identifier registers as a `LiteralParam` in a PROCESSOR (`ctx.<field>`) or TRANSFORM
    * (`doc['<alias>'].value`) context, so `Identifier.contextKey` never applied and two chains over
    * one column collapsed onto a single parameter, both folds landing on it:
    * `….get(ChronoField.YEAR).get(ChronoField.MONTH_OF_YEAR)`, an `int.get(…)`.
    *
    * ⚠️ This commit fixes the IDENTITY, and the ingest script still does not RUN: the extracted
    * `int` is handed back to the processor's temporal parse (`param2 instanceof String ? … :
    * Instant.ofEpochMilli(param2)…`), which is issue #373's item 7 and its own commit. MEASURED on
    * ES 8.18 as an ingest pipeline over `{"d":"2025-01-10"}`: the computed column is ABSENT before
    * AND after this commit -- `ignore_failure: true` swallows the throw, which is exactly why the
    * defect was invisible. The executed proof of the pair lives with the item-7 commit; what is
    * asserted here is the parameter split, which is what this commit owns.
    */
  "two different chains over one column in a PROCESSOR" should "be two parameters" in {
    val emitted = processorOf(
      "CREATE TABLE t (d DATE, c INTEGER SCRIPT AS (CASE WHEN YEAR(d) > MONTH(d) THEN 1 ELSE 0 END))"
    )
    withClue(emitted) {
      emitted should not include ".get(ChronoField.YEAR).get(ChronoField.MONTH_OF_YEAR)"
      emitted should include(".get(ChronoField.YEAR); ")
      emitted should include(".get(ChronoField.MONTH_OF_YEAR); ")
    }
  }

  it should "keep ONE parameter where the chain is the same" in {
    // The single-chain processor emission must not move: one column, one chain, one parameter.
    processorOf("CREATE TABLE t (d DATE, c INTEGER SCRIPT AS (YEAR(d)))") shouldBe
    "def param1 = (ctx.d instanceof String ? " +
    "LocalDate.parse((ctx.d).replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\"))" +
    ".atStartOfDay(ZoneId.of('Z')) : Instant.ofEpochMilli(ctx.d).atZone(ZoneId.of('Z')))" +
    ".get(ChronoField.YEAR); ctx.c = param1"
  }

  // -- item 9: a guarded operand is bound once, not rendered twice -------------------------------

  /** `coerce` builds its result by interpolating the operand and then wraps it in a null guard that
    * tests the operand AGAIN, so a chained operand was rendered TWICE and every function in it ran
    * twice per document:
    * {{{
    * ((param2 == null) ? null : (def)(param2.plus(1, …)) != null
    *   ? (param2 == null) ? null : (def)(param2.plus(1, …)).atStartOfDay(…) : null)
    * }}}
    * 🔴 It is CORRECT -- executed on ES 8.18 before and after, both return the same single row --
    * so this is COST, not a wrong answer, and the pin is on the count rather than on a value.
    */
  "a chained operand under a null guard" should "be rendered once" in {
    val emitted =
      predicateOf(
        "SELECT name FROM t WHERE DATE_ADD(DATE_PARSE(name, 'yyyy-MM-dd'), INTERVAL 1 DAY) > d"
      )
    withClue(emitted) {
      countOf(emitted, "param2.plus(1, ChronoUnit.DAYS)") shouldBe 1
    }
  }

  it should "bind the operand of a chained conversion once too" in {
    val emitted = fieldOf(
      "SELECT CASE CAST(d AS DATE) WHEN CAST('2025-01-01' AS DATE) THEN YEAR(CAST(d AS DATE)) ELSE 0 END AS c FROM t"
    )
    withClue(emitted) {
      countOf(emitted, "param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate()") shouldBe 1
    }
  }

  it should "reuse an existing parameter instead of declaring an alias" in {
    // 🔴 Two earlier versions of this test were VACUOUS -- neither reddened when the binding was
    // made unconditional -- and that is the finding, not a detail: `addParam` returns the EXISTING
    // name when the literal it is handed IS one, so a bare-name operand was never going to produce
    // a duplicate. The redundant guard was removed rather than left as unfalsifiable code, and
    // what is asserted here is the property that actually holds: no parameter is declared as a
    // bare alias of another.
    val emitted =
      fieldOf("SELECT CASE CAST(d AS DATE) WHEN CAST(ts AS DATE) THEN 1 ELSE 0 END AS c FROM t")
    withClue(emitted) {
      """def \w+ = (?:param|left|arg)\d+;""".r.findFirstIn(emitted) shouldBe None
    }
  }
}
