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
import app.softnetwork.elastic.sql.{Identifier, PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Issue #370 -- two uses of one column with DIFFERENT chained transforms must be two Painless
  * parameters.
  *
  * A `PainlessParam`'s identity was the doc-value STRING (`doc['d'].value`), while a folding
  * function attaches its method to the parameter OBJECT (`addPainlessMethod`). So the second
  * appearance of the same column in one script resolved, through `PainlessContext.get`, to the
  * first object -- which by then carried the first chain. `DATE_TRUNC(d, MONTH) < d` compared the
  * truncated value with ITSELF (always false, HTTP 200); `YEAR(d) = 2025 AND MONTH(d) = 1` folded
  * BOTH extractions onto one parameter and failed the shard with `int.get(ChronoField)`.
  *
  * The repair is `PainlessParam.contextKey`: the context deduplicates on the column PLUS the
  * chained rendering, and `equals` is deliberately left alone (a case-class identifier inherits it,
  * so the whole AST would move). Every emission pinned below is EXECUTED over a real index by
  * `PredicateFunctionResultSpec`, which asserts the ROW SETS -- a byte pin proves the bytes did not
  * move, not that the rows are right.
  */
class ParameterIdentitySpec extends AnyFlatSpec with Matchers {

  private val schema: SchemaTable = SchemaTable(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("name", SQLTypes.Keyword)
    )
  )

  private def single(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss.update(Some(schema))
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  private def predicateOf(sql: String): String = {
    val ctx = PainlessContext(PainlessContextType.Query)
    val body = single(sql).where
      .flatMap(_.criteria)
      .map(_.painless(Some(ctx)))
      .getOrElse(fail(s"[$sql] has no WHERE"))
    s"$ctx$body"
  }

  private def fieldOf(sql: String): String = {
    val ctx = PainlessContext(PainlessContextType.Query)
    val body = single(sql).select.fields.head.painless(Some(ctx))
    s"$ctx$body"
  }

  /** The NO-schema path (what the bridge fixtures exercise, and a live query whose schema lookup
    * missed): every identifier's `originalType` stays `Any`, so the UTC normalisation and the
    * no-schema CAST fold are reachable here and nowhere else.
    */
  private def fieldOfNoSchema(sql: String): String =
    Parser(sql) match {
      case Right(ss: SingleSearch) =>
        val ctx = PainlessContext(PainlessContextType.Query)
        val body = ss.update().select.fields.head.painless(Some(ctx))
        s"$ctx$body"
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  private val guardedDoc = "(doc['d'].size() == 0 ? null : doc['d'].value"
  private val utc = ".toInstant().atZone(ZoneId.of('Z'))"

  // -- the mechanism ------------------------------------------------------------------------------

  private def fieldIdentifiers(sql: String): List[Identifier] =
    single(sql).select.fields.map(_.identifier).toList

  "two identifiers of one column" should "get distinct parameters iff their chained renderings differ" in {
    val List(truncated, bare, year1, year2, formatted) = fieldIdentifiers(
      "SELECT DATE_TRUNC(d, MONTH), d, YEAR(d), YEAR(d), DATE_FORMAT(d, 'yyyy') FROM t"
    )
    // Same doc-value string throughout: this is what identity USED to be, and it never changes.
    Set(truncated, bare, year1, year2, formatted).map(_.param) should have size 1

    truncated.contextKey should not be bare.contextKey
    year1.contextKey shouldBe year2.contextKey
    // DATE_FORMAT holds `d` as an ARGUMENT, it does not fold onto it -- so the outer identifier
    // keeps sharing the bare column's parameter, and those emissions keep their bytes.
    formatted.functions should not be empty
    formatted.foldedFunctions shouldBe empty
    formatted.contextKey shouldBe bare.contextKey

    val ctx = PainlessContext(PainlessContextType.Query)
    val p1 = ctx.addParam(truncated)
    val p2 = ctx.addParam(bare)
    val p3 = ctx.addParam(year1)
    val p4 = ctx.addParam(year2)
    val p5 = ctx.addParam(formatted)
    p1 should not be p2
    p3 shouldBe p4
    p3 should not be p1
    p5 shouldBe p2
  }

  // -- the emissions (executed, row sets asserted, in PredicateFunctionResultSpec) ---------------

  "DATE_TRUNC(d, MONTH) < d" should "compare the truncation with the COLUMN, not with itself" in {
    predicateOf("SELECT name FROM t WHERE DATE_TRUNC(d, MONTH) < d") shouldBe
    s"def param1 = $guardedDoc$utc.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS)); " +
    s"def param2 = $guardedDoc); " +
    "def left1 = param1; (left1 == null || param2 == null ? false : ((left1.isBefore(param2))))"
  }

  "DATE_ADD(d, INTERVAL 1 DAY) = d" should "not be a tautology" in {
    predicateOf("SELECT name FROM t WHERE DATE_ADD(d, INTERVAL 1 DAY) = d") shouldBe
    s"def param1 = $guardedDoc.plus(1, ChronoUnit.DAYS)); " +
    s"def param2 = $guardedDoc); " +
    "def left1 = param1; (left1 == null || param2 == null ? false : ((left1.isEqual(param2))))"
  }

  "YEAR(d) > MONTH(d)" should "extract each field from its own parameter" in {
    // Before: ONE parameter carrying `.get(ChronoField.YEAR).get(ChronoField.MONTH_OF_YEAR)` -- an
    // `int.get(...)` that fails the shard. This is the LIVE loud face of #370: one expression, one
    // script (executed in `PredicateFunctionResultSpec`).
    predicateOf("SELECT name FROM t WHERE YEAR(d) > MONTH(d)") shouldBe
    s"def param1 = $guardedDoc$utc.get(ChronoField.YEAR)); " +
    s"def param2 = $guardedDoc$utc.get(ChronoField.MONTH_OF_YEAR)); " +
    "def left1 = param1; (left1 == null || param2 == null ? false : ((left1 > param2)))"
  }

  "YEAR(d) = 2025 AND MONTH(d) = 1 rendered as one script" should "extract each field from its own parameter" in {
    // ⚠️ `Criteria.painless` renders the conjunction into ONE script, and there the two extractions
    // collapsed exactly as above. The WHERE translation does NOT take this rendering -- it emits
    // each AND branch as its own filter with its own context -- so the live query was never wrong
    // for this shape; the pin guards the rendering itself.
    predicateOf("SELECT name FROM t WHERE YEAR(d) = 2025 AND MONTH(d) = 1") shouldBe
    s"def param1 = $guardedDoc$utc.get(ChronoField.YEAR)); " +
    s"def param2 = $guardedDoc$utc.get(ChronoField.MONTH_OF_YEAR)); " +
    "(param1 == null ? false : (param1 == 2025)) && (param2 == null ? false : (param2 == 1))"
  }

  "DATE_FORMAT(d, 'yyyy') = '2025' AND YEAR(d) = 2025 rendered as one script" should "format the column, not the extracted year" in {
    // Before: YEAR folded `.get(ChronoField.YEAR)` onto the parameter DATE_FORMAT was formatting,
    // so `format(param1)` received an `int` -- `class_cast_exception: Cannot cast java.lang.Integer
    // to java.time.temporal.TemporalAccessor`, executed on ES 8.18 as one script. Same caveat as
    // above: the WHERE translation splits the conjunction, so this guards the rendering.
    predicateOf(
      "SELECT name FROM t WHERE DATE_FORMAT(d, 'yyyy') = '2025' AND YEAR(d) = 2025"
    ) shouldBe
    s"def param1 = $guardedDoc); " +
    "def param2 = DateTimeFormatter.ofPattern(\"yyyy\"); " +
    "def left1 = (param1 == null) ? null : param2.format(param1); " +
    s"def param3 = $guardedDoc$utc.get(ChronoField.YEAR)); " +
    "((left1 == null ? false : ((left1.compareTo(\"2025\") == 0)))) && " +
    "(param3 == null ? false : (param3 == 2025))"
  }

  // -- what must NOT move --------------------------------------------------------------------------

  "the same chain twice" should "still share one parameter" in {
    predicateOf("SELECT name FROM t WHERE YEAR(d) = 2025 AND YEAR(d) = 2024") shouldBe
    s"def param1 = $guardedDoc$utc.get(ChronoField.YEAR)); " +
    "(param1 == null ? false : (param1 == 2025)) && (param1 == null ? false : (param1 == 2024))"
  }

  "an argument-holding function over a chained one" should "share the chained parameter" in {
    // Byte-identical to before: the outer DATE_FORMAT identifier and the DATE_TRUNC it wraps fold
    // the same chain, so they are one parameter.
    predicateOf(
      "SELECT name FROM t WHERE DATE_FORMAT(DATE_TRUNC(d, MONTH), 'yyyy') = '2025'"
    ) should
    startWith(
      s"def param1 = $guardedDoc$utc.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS)); " +
      "def param2 = (param1 != null ? param1" + utc + ".toLocalDate() : null); " +
      "def param3 = DateTimeFormatter.ofPattern(\"yyyy\"); "
    )
  }

  "a function that BINDS rather than folds" should "keep sharing the raw parameter" in {
    // 🔴 `DATE_TRUNC(…, QUARTER)` needs its operand three times, so in context it binds a parameter
    // of its own instead of folding a method -- while its context-free rendering still starts with
    // `.`. Keyed on that rendering alone it split from the bare column and declared the raw
    // doc-value TWICE, one of them dead (measured). `DateTrunc.foldsOntoOperand` is the override
    // that says so, and this pin is what makes it falsifiable: remove it and `param3` appears.
    predicateOf("SELECT name FROM t WHERE DATE_TRUNC(d, QUARTER) < d") shouldBe
    s"def param1 = $guardedDoc$utc); " +
    "def param2 = param1 != null ? param1.withMonth((((param1.getMonthValue() - 1) / 3) * 3) + 1)" +
    ".withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS) : null; " +
    "def left1 = param2; (left1 == null || param1 == null ? false : ((left1.isBefore(param1))))"
    // A CAST binds too (`left1`), so the column it converts and the bare column stay one parameter.
    predicateOf("SELECT name FROM t WHERE CAST(d AS DATE) = d") shouldBe
    s"def param1 = $guardedDoc); " +
    s"def left1 = (param1 != null ? param1$utc.toLocalDate() : null); " +
    "(left1 == null || param1 == null ? false : ((left1.isEqual(param1))))"
  }

  "a fold applied AFTER an expression-rendering function" should "not key the raw parameter" in {
    // The leading-run rule: `YEAR` is read off the PARSED value, so the raw `name` is the same
    // parameter the `DATE_PARSE` argument resolves to. Keyed on `YEAR` it was declared twice.
    predicateOf("SELECT name FROM t WHERE YEAR(DATE_PARSE(name, 'yyyy-MM-dd')) = 2025") shouldBe
    "def param1 = (doc['name'].size() == 0 ? null : doc['name'].value); " +
    "def param2 = (param1 == null) ? null : LocalDate.parse(param1, DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); " +
    "def left1 = (param2 == null) ? null : (def)(param2.get(ChronoField.YEAR)); " +
    "(left1 == null ? false : ((left1 == 2025)))"
  }

  // -- the interval family folds onto ITS operand's parameter (review H1) -----------------------

  /** `IntervalFunction.toPainless` folded `.plus(…)` onto `ctx.last` -- whatever parameter was
    * created LAST -- so the second occurrence of a chain landed on someone else's parameter. All
    * three executed on ES 8.18 over `a(d=01-01,ts=01-01) b(01-05,01-04) c(01-05,01-07)`; the
    * comment on each row is the value ES returned, equal to the truth. Before the fix the first
    * returned `ts + 1 day`, the second folded the day onto the boolean CONDITION (compile error),
    * and the third's fallback read `ts + 1`.
    */
  "an interval applied twice to one column" should "fold onto that column's parameter both times" in {
    // a: 2025-01-02, b: 2025-01-06, c: 2025-01-05
    fieldOf(
      "SELECT CASE WHEN DATE_ADD(d, INTERVAL 1 DAY) > ts THEN DATE_ADD(d, INTERVAL 1 DAY) ELSE d END AS c FROM t"
    ) shouldBe
    s"def param1 = $guardedDoc.plus(1, ChronoUnit.DAYS)); def param2 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "def left1 = param1; def param3 = (left1 == null || param2 == null ? false : ((left1.isAfter(param2)))); " +
    s"def param4 = $guardedDoc); param3 ? param1 : param4"
    // a: 2025-01-01, b: 2025-01-05, c: 2025-01-06
    fieldOf(
      "SELECT CASE WHEN DATE_ADD(d, INTERVAL 1 DAY) > ts THEN d ELSE DATE_ADD(d, INTERVAL 1 DAY) END AS c FROM t"
    ) shouldBe
    s"def param1 = $guardedDoc.plus(1, ChronoUnit.DAYS)); def param2 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    s"def left1 = param1; def param3 = $guardedDoc); " +
    "def param4 = (left1 == null || param2 == null ? false : ((left1.isAfter(param2)))); param4 ? param3 : param1"
    // = d + 1 on every document
    fieldOf(
      "SELECT COALESCE(DATE_ADD(d, INTERVAL 1 DAY), DATE_ADD(ts, INTERVAL 1 DAY), DATE_ADD(d, INTERVAL 1 DAY)) AS c FROM t"
    ) shouldBe
    s"def param1 = $guardedDoc.plus(1, ChronoUnit.DAYS)); " +
    "def param2 = (doc['ts'].size() == 0 ? null : doc['ts'].value.plus(1, ChronoUnit.DAYS)); " +
    "(param1 != null ? param1 : (param2 != null ? param2 : param1))"
  }

  "a CAST chain compared with its own column" should "be two parameters" in {
    // ⚠️ Pinned for the IDENTITY, with the rest stated plainly: before the fix this was
    // `left1.isAfter(param1)` with the `.minus(3, …)` folded INTO `param1` -- `x > x`, always false,
    // silently. Now the two sides are distinct, and the comparison FAILS LOUDLY on ES 8.18
    // (`Cannot cast java.time.ZonedDateTime to java.time.chrono.ChronoLocalDate`): the right-hand
    // column is not narrowed to the left chain's DATE, which is #367's rule 8 for a COLUMN right
    // operand -- a pre-existing gap this fix exposes rather than creates, and does not own.
    predicateOf("SELECT name FROM t WHERE CAST(d AS DATE) - INTERVAL 3 DAY > d") shouldBe
    s"def param1 = $guardedDoc); " +
    s"def param2 = (param1 != null ? param1$utc.toLocalDate() : null); " +
    "def left1 = (param2 == null) ? null : (def)(param2.minus(3, ChronoUnit.DAYS)); " +
    "(left1 == null || param1 == null ? false : ((left1.isAfter(param1))))"
  }

  // -- the no-schema CAST fold, and the ordering rule (review H2, rule 2) --------------------------

  "a no-schema CAST narrowing folded onto a shared column" should "not reach the bare column" in {
    // `Conversion.toPainless` FOLDS `.toLocalDate()` onto an un-typed identifier's parameter, so
    // `CAST(lastUpdated AS date) - INTERVAL 3 DAY` must key apart from the bare `lastUpdated` in
    // THEN, else THEN returns `lastUpdated - 3 days`. The interval lands on the CAST's parameter.
    // Executed on ES 8.18 (NOW = 2025-01-08): a -> createdAt 2025-01-01, d -> lastSeen + 2 =
    // 2025-01-03, e -> lastUpdated = 2025-01-04 (the old pin answered 2025-01-01 for e).
    // ⚠️ That is the NO-schema rendering, the one the bridge fixtures pin. The SCHEMA rendering of
    // this same statement fails the shard on `main` AND here (`Class.cast` at
    // `param2.isEqual(param5)`: `CURRENT_DATE - 7` is coerced to a `ZonedDateTime` against a
    // `LocalDate` chain -- #367's `chainType` gap), where `main` returned the silently wrong
    // `e -> 2025-01-01`. Not owned by this fix; stated so nobody reads "executed" as "live".
    val emitted = fieldOfNoSchema(
      "SELECT CASE CURRENT_DATE - INTERVAL 7 DAY WHEN CAST(lastUpdated AS date) - INTERVAL 3 DAY THEN lastUpdated " +
      "WHEN lastSeen THEN lastSeen + INTERVAL 2 DAY ELSE createdAt END AS c, identifier FROM Table"
    )
    emitted should include(
      "def param2 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.toLocalDate().minus(3, ChronoUnit.DAYS)); " +
      "def param3 = (doc['lastUpdated'].size() == 0 ? null : doc['lastUpdated'].value.toLocalDate()); "
    )
    emitted should endWith("? param3 : param1 != null && param1.isEqual(param4) ? param5 : param6")
    // Rule 2, normalisation FIRST: the branch's DATE narrowing was appended to this object before
    // the chain rendered; the UTC normalisation must still come first (`LocalDate` has no
    // `toInstant()`).
    emitted should include(
      "doc['lastSeen'].value.toInstant().atZone(ZoneId.of('Z')).toLocalDate().plus(2, ChronoUnit.DAYS)"
    )
  }

  // -- rule 3 outside the survival spec, and the spelling-independent key (review M1, L1) --------

  "DATE_TRUNC against a DATE literal" should "truncate a LocalDate receiver without truncatedTo" in {
    // Executed on ES 8.18: every January document (the truncation of 2025-01-05 IS 2025-01-01),
    // and only that one on a single-document index. The pre-#370 pin certified
    // `…toLocalDate().toInstant()…`, which ES rejects.
    predicateOf("SELECT name FROM t WHERE DATE_TRUNC(d, MONTH) = CAST('2025-01-01' AS DATE)") should
    startWith(s"def param1 = $guardedDoc$utc.toLocalDate().withDayOfMonth(1)); ")
  }

  "two spellings of one fold" should "share one parameter" in {
    predicateOf("SELECT name FROM t WHERE YEAR(d) = EXTRACT(YEAR FROM d)") shouldBe
    s"def param1 = $guardedDoc$utc.get(ChronoField.YEAR)); " +
    "def left1 = param1; (left1 == null || param1 == null ? false : ((left1 == param1)))"
  }

  // -- a CAST never re-narrows a LocalDate receiver (review, introduced regression + main bug) --

  /** Executed on ES 8.18 over `a(d=01-01) b(01-05) c(01-05)`. `CAST(d AS DATE) - INTERVAL 3 DAY =
    * CAST('2025-01-02' AS DATE)` is `[b, c]`, and it was `[b, c]` on `main` ONLY because the old
    * `ctx.last` fold discarded the CAST's coercion; keeping it re-narrowed a `LocalDate` receiver
    * (`param1.toInstant()…`) and failed the shard. The plain BI filter `CAST(d AS DATE) =
    * CAST('2025-01-05' AS DATE)` failed the same way on `main` itself -- one rule fixes both.
    */
  "a CAST over a receiver already narrowed to a LocalDate" should "be the identity" in {
    // Both take #367's parameter shortcut (the chain folds entirely). Executed on ES 8.18: [b, c].
    predicateOf("SELECT name FROM t WHERE CAST(d AS DATE) = CAST('2025-01-05' AS DATE)") shouldBe
    s"def param1 = $guardedDoc.toLocalDate()); " +
    "def param2 = LocalDate.parse((\"2025-01-05\").replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); " +
    "param1 == null ? false : (param1.isEqual(param2))"
    predicateOf(
      "SELECT name FROM t WHERE CAST(d AS DATE) - INTERVAL 3 DAY = CAST('2025-01-02' AS DATE)"
    ) shouldBe
    s"def param1 = $guardedDoc.toLocalDate().minus(3, ChronoUnit.DAYS)); " +
    "def param2 = LocalDate.parse((\"2025-01-02\").replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); " +
    "param1 == null ? false : (param1.isEqual(param2))"
  }

  "a CAST over a receiver already narrowed to a LocalTime" should "be the identity too" in {
    // The TIME twin (review): `CAST(ts AS TIME) + INTERVAL 1 HOUR > …` was right on `main` for the
    // same accidental reason, and re-narrowed a `LocalTime` once the interval fold kept the
    // coercion. Executed on ES 8.18 over midnight dates: the first selects every document. The
    // second now EXECUTES: `=` over a TIME receiver is spelled `equals`, which `LocalTime` has
    // (issue #373) -- it was pinned for its identity only while it still emitted `isEqual`.
    predicateOf(
      "SELECT name FROM t WHERE CAST(d AS TIME) + INTERVAL 1 HOUR > CAST('00:30:00' AS TIME)"
    ) shouldBe
    s"def param1 = $guardedDoc.toLocalTime().plus(1, ChronoUnit.HOURS)); " +
    "def param2 = LocalTime.parse(\"00:30:00\", DateTimeFormatter.ISO_LOCAL_TIME); " +
    "param1 == null ? false : (param1.isAfter(param2))"
    predicateOf("SELECT name FROM t WHERE CAST(d AS TIME) = CAST('00:00:00' AS TIME)") shouldBe
    s"def param1 = $guardedDoc.toLocalTime()); " +
    "def param2 = LocalTime.parse(\"00:00:00\", DateTimeFormatter.ISO_LOCAL_TIME); " +
    // `compareTo`, not `isEqual`: `LocalTime` is the one temporal without it (issue #373).
    "param1 == null ? false : (param1.compareTo(param2) == 0)"
  }

  "the identity key" should "not read the narrowing off the object" in {
    // `DateTrunc`'s context-free rendering -- what the key is built from -- used to read
    // `rendersLocalDate` off the object, so the key changed with whether a DATE-literal narrowing
    // had ALREADY landed on it, i.e. with the ORDER of the predicates. Now both orders key the
    // same: the truncation and the bare column are exactly two doc-value parameters, in either
    // order. Asserted on the IDENTITY only.
    //
    // ⚠️ NOT executable, and pinned as such: on ES 8.18 BOTH orders fail the shard
    // (`Cannot cast ZonedDateTime to ChronoLocalDate` / the reverse), because the DATE-literal
    // narrowing makes the shared parameter a `LocalDate` on one side of `< d` and the raw column a
    // `ZonedDateTime` on the other. On `main` the same statement was silently `x < x`. A mixed
    // receiver comparison is #367's rule-8 family, not identity, and is not owned here.
    Seq(
      "SELECT name FROM t WHERE DATE_TRUNC(d, MONTH) = CAST('2025-01-01' AS DATE) AND DATE_TRUNC(d, MONTH) < d",
      "SELECT name FROM t WHERE DATE_TRUNC(d, MONTH) < d AND DATE_TRUNC(d, MONTH) = CAST('2025-01-01' AS DATE)"
    ).foreach { sql =>
      val emitted = predicateOf(sql)
      withClue(s"$sql ->\n$emitted\n") {
        "doc\\['d'\\]\\.size\\(\\) == 0".r.findAllIn(emitted).size shouldBe 2
        emitted should not include "def param4 = "
      }
    }
  }

  "an expression-rendering function compared with its column" should "keep one parameter" in {
    predicateOf("SELECT name FROM t WHERE LAST_DAY(d) = d") shouldBe
    s"def param1 = $guardedDoc); " +
    "def left1 = (param1 == null) ? null : param1.with(TemporalAdjusters.lastDayOfMonth()); " +
    "(left1 == null || param1 == null ? false : ((left1.isEqual(param1))))"
    predicateOf("SELECT name FROM t WHERE UPPER(name) = name") shouldBe
    "def param1 = (doc['name'].size() == 0 ? null : doc['name'].value); " +
    "def left1 = (param1 == null) ? null : param1.toUpperCase(); " +
    "(left1 == null || param1 == null ? false : ((left1 == param1)))"
  }
}
