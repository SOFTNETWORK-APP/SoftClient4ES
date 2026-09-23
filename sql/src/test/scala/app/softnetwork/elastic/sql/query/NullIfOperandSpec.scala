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
import app.softnetwork.elastic.sql.`type`.{SQLTypes, SQLVarchar}
import app.softnetwork.elastic.sql.function.cond
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType, PainlessOperandForm}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

/** Issue #382, the NULLIF addendum: `NULLIF` emitted two DIFFERENT scripts Elasticsearch refuses,
  * and a third that it accepts and answers WRONGLY. All three are pre-existing on `main`; all three
  * are one question asked of two operands — what does this argument RENDER, and may it be spliced
  * where it is spliced?
  *
  * '''N1 — the TYPE.''' `NullIf.argTypes` answered `_.out`, and a schema-resolved `Identifier.out`
  * reports the COLUMN's type, so `NULLIF(CAST(s AS BIGINT), 0)` over a KEYWORD column reported
  * `List(KEYWORD, BIGINT)`, `leastCommonSuperType` came out VARCHAR and the string arm's guard
  * reached the cluster:
  * {{{
  * def param3 = param2 == null || (0 != null && param2.compareTo(0) == 0) ? null : param2;
  *   class_cast_exception: Cannot cast from [int] to [java.lang.Object].   (ES 8.18.3)
  * }}}
  * Identical for `CAST`, `TRY_CAST` and `::`, and in the DDL, projection and predicate venues.
  *
  * '''N2 — the FORM.''' `arg0` is spliced RAW three times into `$arg0 == null || ($arg1 != null &&
  * $comparison) ? null : $arg0` and `arg1` twice, and `&&`, `||` and `==` all bind tighter than
  * `?:`. A compound rendering therefore re-associates across the template. MEASURED on ES 8.18.3:
  * {{{
  * NULLIF(UPPER(s), 'X')                          Cannot cast null to a primitive type [boolean].
  * NULLIF(CASE WHEN n > 1 THEN 1 ELSE 2 END, 1)   n = 5 stored c = 1 where SQL says NULL, HTTP 200
  * }}}
  * 🔴 The second is why the repair cannot be narrowed to the arm that fails LOUDLY. The numeric arm
  * `$arg0 == $arg1 ? null : $arg0` re-associates just as badly; it merely happens to come out right
  * when the compound rendering is a `guard ? null : value`, which is luck, not a rule — and the
  * price of trusting that luck is a silent wrong answer in the #205 family. Both templates now read
  * their operands from ONE place, so they cannot disagree.
  *
  * ⚠️ Two shapes that already WORKED change bytes as a consequence, and both were executed on a
  * cluster before and after (`GatewayApiIntegrationSpec`):
  *   - `NULLIF(LENGTH(s), 0)` gains the bound local (it was correct, and evaluated twice);
  *   - `NULLIF(CAST(s AS BIGINT), n)` moves from the string arm to the numeric one, which is the
  *     arm its corrected types name and the one `NULLIF(n, 0)` has always used.
  *
  * `ScriptProcessor.source` is persisted in `_meta` and `IngestPipeline.diff` compares it, so a
  * stored computed column of either shape reports `ProcessorChanged` on the next ALTER.
  */
class NullIfOperandSpec extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  private val schema: SchemaTable = SchemaTable(
    "t",
    columns = List(
      Column("s", SQLTypes.Keyword),
      Column("s2", SQLTypes.Keyword),
      Column("status", SQLTypes.Keyword),
      Column("n", SQLTypes.BigInt),
      Column("i", SQLTypes.Int),
      Column("dbl", SQLTypes.Double),
      Column("b", SQLTypes.Boolean),
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp)
    )
  )

  private val ddlColumns =
    "s KEYWORD, s2 KEYWORD, status KEYWORD, n BIGINT, i INTEGER, dbl DOUBLE, b BOOLEAN, " +
    "d DATE, ts TIMESTAMP"

  private def single(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss.update(Some(schema))
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  /** The DDL venue as a client runs it: `Parser` -> `CreateTable.validate()` ->
    * `validateScriptReferences` on the RESOLVED table. `Left` is the rejection a `CREATE TABLE`
    * earns, `Right` the ingest-processor source it would deploy.
    */
  private def ddlVerdict(expr: String, dataType: String = "KEYWORD"): Either[String, String] =
    Parser(s"CREATE TABLE t ($ddlColumns, c $dataType SCRIPT AS ($expr))") match {
      case Right(ct: CreateTable) =>
        Right(
          ct.schema.columns
            .find(_.name == "c")
            .flatMap(_.script)
            .map(_.source)
            .getOrElse(fail(s"no script processor for [$expr]"))
        )
      case Right(other) => fail(s"[$expr] expected a CreateTable, got $other")
      case Left(err)    => Left(err.msg)
    }

  /** The query venue as core runs it (`SearchApi.resolveWithSchema`): parse, attach the schema,
    * then [[SingleSearch.validateResolved]] -- the ONE seam that holds a resolved statement.
    */
  private def queryVerdict(expr: String): Either[String, SingleSearch] =
    Parser(s"SELECT $expr AS c FROM t") match {
      case Right(ss: SingleSearch) =>
        val resolved = ss.update(Some(schema))
        resolved.validateResolved().map(_ => resolved)
      case Right(other) => fail(s"[$expr] expected a SingleSearch, got $other")
      case Left(err)    => Left(err.msg)
    }

  private def predicateVerdict(expr: String): Either[String, SingleSearch] =
    Parser(s"SELECT s FROM t WHERE ISNOTNULL($expr)") match {
      case Right(ss: SingleSearch) =>
        val resolved = ss.update(Some(schema))
        resolved.validateResolved().map(_ => resolved)
      case Right(other) => fail(s"[$expr] expected a SingleSearch, got $other")
      case Left(err)    => Left(err.msg)
    }

  private def everyVenueVerdict(
    expr: String,
    dataType: String = "KEYWORD"
  ): Seq[(String, Either[String, Unit])] =
    Seq(
      "computed column" -> ddlVerdict(expr, dataType).map(_ => ()),
      "projection"      -> queryVerdict(expr).map(_ => ()),
      "predicate"       -> predicateVerdict(expr).map(_ => ())
    )

  /** The ingest-processor source of a computed column, through the production seam. */
  private def processorOf(expr: String, dataType: String = "KEYWORD"): String =
    Parser(s"CREATE TABLE t ($ddlColumns, c $dataType SCRIPT AS ($expr))") match {
      case Right(ct: CreateTable) =>
        ct.schema.columns
          .find(_.name == "c")
          .flatMap(_.script)
          .map(_.source)
          .getOrElse(fail(s"no script processor for [$expr]"))
      case other => fail(s"[$expr] expected a CreateTable, got $other")
    }

  /** The first SELECT field rendered as one script. The body is rendered FIRST: it is what
    * registers the parameters, so an inline `s"$ctx${…painless(ctx)}"` prints an EMPTY prologue.
    */
  private def fieldOf(expr: String): String = {
    val ctx = PainlessContext(PainlessContextType.Query)
    val body = single(s"SELECT $expr AS c FROM t").select.fields.head.painless(Some(ctx))
    s"$ctx$body"
  }

  /** The WHERE criteria rendered as ONE script, prologue included. */
  private def predicateOf(expr: String): String = {
    val ctx = PainlessContext(PainlessContextType.Query)
    val body = single(s"SELECT s FROM t WHERE ISNOTNULL($expr)").where
      .flatMap(_.criteria)
      .map(_.painless(Some(ctx)))
      .getOrElse(fail(s"[$expr] has no WHERE"))
    s"$ctx$body"
  }

  private def everyVenue(expr: String, dataType: String = "KEYWORD"): Seq[(String, String)] =
    Seq(
      "computed column" -> processorOf(expr, dataType),
      "projection"      -> fieldOf(expr),
      "predicate"       -> predicateOf(expr)
    )

  /** How many times `needle` occurs in `haystack` -- the shape of an "evaluated once" assertion. */
  private def countOf(haystack: String, needle: String): Int =
    haystack.sliding(needle.length).count(_ == needle)

  // -- the corpus the invariants below are asserted over --------------------------------------

  /** One operand per way an argument can RENDER: a bare column of each mapped type, every spelling
    * of a cast, a function, and a literal of each kind. The invariants are properties of the
    * EMISSION, so the population has to be every pair that reaches it -- a hand-picked table is
    * exactly what let six `0 != null` shapes ship green through this file.
    */
  private val operands: Seq[String] = Seq(
    "n",
    "i",
    "dbl",
    "ABS(n)",
    "CAST(s AS BIGINT)",
    "TRY_CAST(s AS BIGINT)",
    "s::BIGINT",
    "CAST(s AS DOUBLE)",
    "CAST(i AS BIGINT)",
    "LENGTH(s)",
    "0",
    "-1",
    "1.5",
    "s",
    "status",
    "UPPER(s)",
    "CAST(n AS KEYWORD)",
    "CAST(i AS VARCHAR)",
    "'x'",
    "SUBSTRING(s, 1, 2)",
    "d",
    "CAST(d AS DATE)",
    "CAST(d AS TIME)",
    "DATE_TRUNC(d, MONTH)",
    "b",
    "true",
    "NULL"
  )

  /** Every ordered pair, declared as a `KEYWORD` column so the target type never decides the arm.
    */
  private lazy val corpus: Seq[(String, String)] =
    for { a <- operands; b <- operands } yield (s"NULLIF($a, $b)", "KEYWORD")

  /** What each venue EMITS, skipping the pairs the engine refuses -- an invariant about emitted
    * Painless says nothing about a statement that emits none.
    */
  private def everyVenueEmission(expr: String, dataType: String): Seq[(String, String)] =
    ddlVerdict(expr, dataType).toSeq.map("computed column" -> _) ++
    queryVerdict(expr).toSeq.map { ss =>
      val ctx = PainlessContext(PainlessContextType.Query)
      val body = ss.select.fields.head.painless(Some(ctx))
      "projection" -> s"$ctx$body"
    } ++
    predicateVerdict(expr).toSeq.map { ss =>
      val ctx = PainlessContext(PainlessContextType.Query)
      val body = ss.where.flatMap(_.criteria).map(_.painless(Some(ctx))).getOrElse("")
      "predicate" -> s"$ctx$body"
    }

  /** A numeric literal GUARDED against null -- `(0 != null && ...)`, the exact shape ES refuses
    * with `class_cast_exception: Cannot cast from [int] to [java.lang.Object]`. A pattern over ANY
    * numeric literal, not the three that were filed.
    *
    * ⚠️ Deliberately the GUARD and not every `<literal> == null`: `NULLIF(0, NULL)` renders `0 ==
    * null` and fails to compile for the same reason, but it does so byte-identically on
    * `origin/main` -- a pre-existing degenerate shape (a literal NULLIF a literal), recorded below,
    * not this issue's to change.
    */
  private val NumericNullGuard = """(?<![A-Za-z0-9_."])-?\d+(?:\.\d+)?\s*!=\s*null""".r

  /** The two types a refusal NAMES, captured from its own clause rather than searched for anywhere
    * in the message -- see the note at the reject-list assertion.
    */
  private val NamedTypes = """compares ([A-Z_]+) with ([A-Z_]+)""".r

  /** The PRE-EXISTING parse-time refusal from `NullIf.validate()`, which the query venues reach
    * before the resolved rule can run. Its wording predates #382 and names the pair its own way.
    * Matched explicitly so that "some Left" still cannot satisfy the reject list.
    */
  private val NamedTypesAtParse =
    """output '([A-Z_]+)' is not compatible with input '([A-Z_]+)'""".r

  /** The rendering a `NULLIF` hoisted into the prologue: `def nif<N> = <rendering>;`. */
  private val BoundOperand = """def nif\d+ = (.*?);""".r

  /** NOT both of this expression's `NULLIF` operands render text -- the pair on which
    * `String.compareTo` is a per-document throw, whatever the other side is.
    *
    * 🔴 `!forall(isText)`, not `count == 1` (issue #382, found by review). The old spelling fired
    * only when EXACTLY ONE side was text, so the receiver-throw family this file documents as
    * MEASURED -- a `Boolean` or `ZonedDateTime` receiver, where NEITHER side is text -- was never
    * an offence, and the assertion's own title was not what it checked.
    *
    * ⚠️ TWO arms emit `compareTo`, and only one of them is the defect: the `Varchar` arm, where
    * `String.compareTo` demands a `String` receiver, and the TEMPORAL arm, where `compareTo` on a
    * `LocalTime` receiver is correct and deliberate (issue #373's `receiverIsTime`). Widening this
    * predicate to "not both text" flagged `NULLIF(CAST(d AS TIME), d)` -- a legitimate temporal
    * comparison -- so both homogeneous families are excluded, which is what "BOTH operands render
    * text" meant all along.
    *
    * ⚠️ It reads `argTypes`, the derivation under repair, so it cannot be fully independent of the
    * emission. That is why it is not the only guard: the pairs it describes are now REFUSED
    * outright (see "refuse a NULLIF whose operands cannot be compared at all"), and this invariant
    * covers what survives refusal.
    */
  private def mixesTextWithNonText(expr: String): Boolean =
    queryVerdict(expr).toOption.toSeq
      .flatMap(_.select.fields.map(_.identifier))
      .flatMap(cond.functionsOf)
      .collect { case n: cond.NullIf => n }
      .exists { n =>
        val homogeneous = n.argTypes.forall(_.isText) || n.argTypes.forall(_.isTemporal)
        // an UNKNOWN operand is comparable with anything by the engine's own rule, and its guard
        // (`null != null`) short-circuits before `compareTo` can run -- `NULLIF(x, NULL)` is `x`
        val unknown = n.argTypes.exists(_.isUnknown)
        !homogeneous && !unknown
      }

  // -- N1: the type an operand RENDERS decides the arm ---------------------------------------

  /** Every spelling of a cast that RENDERS a number, against a numeric literal. The guard the
    * string arm puts on that literal — `0 != null` — is what Elasticsearch refuses, so the
    * assertion is that the numeric arm is taken in EVERY venue, not just in DDL where it was filed.
    */
  it should "take the numeric arm when the operand RENDERS a number" in {
    forAll(
      Table(
        ("expr", "dataType"),
        ("NULLIF(CAST(s AS BIGINT), 0)", "BIGINT"),
        ("NULLIF(TRY_CAST(s AS BIGINT), 0)", "BIGINT"),
        ("NULLIF(s::BIGINT, 0)", "BIGINT"),
        ("NULLIF(CAST(s AS DOUBLE), 0)", "DOUBLE"),
        ("NULLIF(CAST(s AS INTEGER), -1)", "INTEGER"),
        ("NULLIF(CAST(s AS BIGINT), n)", "BIGINT")
      )
    ) { (expr, dataType) =>
      everyVenue(expr, dataType).foreach { case (venue, emitted) =>
        withClue(s"[$venue] [$expr] -> [$emitted] ") {
          emitted should not include "compareTo"
          emitted should not include "!= null &&"
        }
      }
    }
  }

  /** 🔴 THE CANARY for the three corpus invariants below (issue #382, found by review).
    *
    * Each of them is `offenders shouldBe empty` over `everyVenueEmission`, which contributes
    * NOTHING for a pair the engine refuses. So all three would go green if `NullIf` started
    * rejecting the corpus wholesale -- the project's own "everything is rejected satisfies an
    * assertion that everything is rejected", applied to emitted Painless.
    *
    * The population is DERIVED, not written down: every operand compared with ITSELF is comparable
    * by construction, whatever the comparability rule becomes, so each diagonal pair must still
    * emit. A count would have been a literal asserting itself.
    */
  it should "still EMIT for the whole diagonal, or the invariants below prove nothing" in {
    val silent = operands.filter(a => everyVenueEmission(s"NULLIF($a, $a)", "KEYWORD").isEmpty)
    withClue(s"operands whose self-comparison emits nothing: $silent ")(silent shouldBe empty)
  }

  /** 🔴 The literal guard Painless refuses, asserted over the WHOLE operand corpus rather than over
    * the three shapes that motivated it (issue #382, delta review MEDIUM-1).
    *
    * The invariant is "no emission ever guards a numeric literal against null", and a table of
    * hand-picked rows cannot establish it: the six shapes that still emitted `0 != null` after the
    * type fix (`NULLIF(CAST(n AS KEYWORD), {0, -1, 1.5})` and the reversed orientation) were absent
    * from every table in this file, so every assertion here was green while the defect shipped.
    * They are refused outright now; this walks the cross product and proves that NOTHING that
    * survives carries the guard.
    */
  it should "never guard a numeric literal against null, over the whole corpus" in {
    val offenders = corpus.flatMap { case (expr, dataType) =>
      everyVenueEmission(expr, dataType).collect {
        case (venue, emitted) if NumericNullGuard.findFirstIn(emitted).isDefined =>
          s"[$venue] [$expr] -> ${NumericNullGuard.findFirstIn(emitted).get} in [$emitted]"
      }
    }
    withClue(offenders.take(10).mkString("\n", "\n", "\n"))(offenders shouldBe empty)
  }

  /** 🔴 `String.compareTo` takes a `String` and nothing else, so the string arm is right only when
    * BOTH operands render text -- and `leastCommonSuperType` answers VARCHAR for a MIXED pair too
    * (`[BOOLEAN, KEYWORD]`, `[TIMESTAMP, KEYWORD]`). MEASURED on ES 8.18.3: without the guard,
    * `NULLIF(b, CAST(n AS KEYWORD))` and `NULLIF(d, CAST(i AS VARCHAR))` called `compareTo` on a
    * `Boolean` / `ZonedDateTime` receiver and threw PER DOCUMENT -- 32 corpus rows, every one of
    * them a shape that answered a VALUE before this issue's type fix moved the supertype from the
    * column's type to the chain's, and every one of them silent in a computed column because
    * `ScriptProcessor.ignoreFailure` defaults to `true`.
    *
    * Asserted over the corpus and not over those 32: the rule is about the ARM, so the population
    * is every expression that reaches it.
    */
  it should "emit compareTo only when BOTH operands render text, over the whole corpus" in {
    val offenders = corpus.flatMap { case (expr, dataType) =>
      everyVenueEmission(expr, dataType).collect {
        case (venue, emitted) if emitted.contains(".compareTo(") && mixesTextWithNonText(expr) =>
          s"[$venue] [$expr] -> [$emitted]"
      }
    }
    withClue(offenders.take(10).mkString("\n", "\n", "\n"))(offenders shouldBe empty)
  }

  /** The other direction of the same derivation, and the one the corpus invariants above do NOT
    * see: a NUMERIC column a cast turned into TEXT must take the STRING arm, because what it
    * RENDERS is a `String`. `out` reports the COLUMN's type and would send it to `==`, which in
    * Painless compares a `String` by reference-or-equals rather than by `compareTo`.
    *
    * 🔴 Found by the falsification matrix: restoring `argTypes = args.map(_.out)` left all sixteen
    * other assertions GREEN while moving 57 corpus rows, because the both-text guard on the string
    * arm happens to give the same answer for the headline shape. A mutation that comes back GREEN
    * is a claim about the TESTS first.
    */
  it should "take the string arm when a numeric column RENDERS text" in {
    forAll(
      Table(
        "expr",
        "NULLIF(CAST(n AS KEYWORD), 'x')",
        "NULLIF(CAST(i AS VARCHAR), 'x')",
        "NULLIF('x', CAST(n AS KEYWORD))",
        "NULLIF(CAST(i AS VARCHAR), CAST(n AS KEYWORD))"
      )
    ) { expr =>
      everyVenueEmission(expr, "KEYWORD").foreach { case (venue, emitted) =>
        withClue(s"[$venue] [$expr] -> [$emitted] ")(emitted should include(".compareTo("))
      }
    }
  }

  /** A FUNCTION-wrapped temporal operand is outside the temporal-literal edge: the literal is then
    * compared against what the CHAIN renders (a `LocalTime`), not against the mapped `date` field,
    * so asking the resolver about the field's format would refuse a shape the engine emits.
    * `'00:00:00'` is not an ISO date and the resolver WOULD reject it.
    *
    * 🔴 Found by the falsification matrix: relaxing `mappedColumn` to accept a function-wrapped
    * operand was GREEN until this row existed. (The query venues reject this shape at PARSE, on the
    * pre-existing `NullIf.validate()`; the DDL venue is where it reaches emission.)
    */
  it should "not ask the temporal resolver about a literal beside a CAST operand" in {
    // 🔴 AMENDED by the #382 review. The claim above -- that the engine EMITS this shape, so the
    // resolver must not refuse it -- was never asserted: the row checked acceptance only. It does
    // not emit it. `CAST(d AS TIME)` renders a `LocalTime`, the literal renders a `String`, the
    // pair folds to VARCHAR, misses both typed arms and lands on `==`, which is `false` for every
    // document and never throws. The resolver is still not asked (a function-wrapped operand is
    // outside `mappedColumn`, which is what this row exists to pin) -- but the answer is now a
    // refusal from the type rule, carrying the "not supported yet" wording.
    val verdict = ddlVerdict("NULLIF(CAST(d AS TIME), '00:00:00')", "TIME")
    val reason = verdict.left.getOrElse(fail(s"expected a refusal, got $verdict"))
    reason should include("TIME")
    reason should include("VARCHAR")
    reason should include("not supported yet")
    // 🔴 The resolver did NOT judge it. Keyed on the resolver's OWN wording, not on the literal:
    // the message echoes the whole expression, so `not include "00:00:00'"` matched the echo and
    // failed -- a probe's own predicate is part of what has to be falsified.
    reason should not include "as a date/time value for date field"
  }

  /** 🔴 RECORDED, NOT FIXED, and BYTE-IDENTICAL to `origin/main` -- found by widening the guard
    * above to the whole corpus. A numeric LITERAL against the NULL literal renders `0 == null`,
    * which Elasticsearch refuses with the very same `class_cast_exception: Cannot cast from [int]
    * to [java.lang.Object]` (measured on ES 8.18.3). It is degenerate SQL -- a constant NULLIF a
    * constant, with no column on either side -- it predates #382 in both directions, and the ruling
    * explicitly never rejects on a NULL operand. This pin reddens when it is fixed.
    */
  it should "still emit an uncompilable literal-against-NULL comparison (recorded, #382)" in {
    ddlVerdict("NULLIF(0, NULL)", "BIGINT") shouldBe
    Right("def param1 = 0 == null ? null : 0; ctx.c = param1")
    ddlVerdict("NULLIF(NULL, 1.5)", "DOUBLE") shouldBe
    Right("def param1 = null == 1.5 ? null : null; ctx.c = param1")
  }

  // -- the LEAD RULING of 2026-09-23: a type mismatch is refused, loudly, in every venue --------

  /** The ACCEPT list, verbatim from the ruling. Each row is a pair whose two operands RENDER
    * comparable types, whatever the COLUMNS were declared: `NULLIF(CAST(s AS BIGINT), 0)` is the
    * headline fix and `s` being a KEYWORD is irrelevant to it.
    */
  it should "accept a NULLIF whose operands RENDER comparable types" in {
    forAll(
      Table(
        ("expr", "dataType"),
        ("NULLIF(CAST(s AS BIGINT), 0)", "BIGINT"),
        ("NULLIF(n, 0)", "BIGINT"),
        ("NULLIF(i, n)", "BIGINT"),
        ("NULLIF(LENGTH(s), 0)", "BIGINT"),
        ("NULLIF(CAST(s AS BIGINT), n)", "BIGINT"),
        ("NULLIF(s, 'unknown')", "KEYWORD"),
        ("NULLIF(UPPER(s), 'X')", "KEYWORD"),
        ("NULLIF(CAST(d AS DATE), CAST('2025-01-01' AS DATE))", "DATE"),
        ("NULLIF(d, d)", "DATE"),
        // NULL compares with anything: `NULLIF(a, NULL)` is UNKNOWN, so the answer is `a`.
        ("NULLIF(s, NULL)", "KEYWORD"),
        ("NULLIF(NULL, 0)", "BIGINT"),
        // 🔴 The boundary the ruling used to leave alone, and no longer does. `NULLIF(d, 0)`,
        // `NULLIF(b, 0)`, `NULLIF(b, 'x')` and `NULLIF(s, d)` all fell to the `==` arm, which
        // Painless evaluates to `false` for a `ZonedDateTime` against a `Long`, a `Boolean`
        // against a `Long` and a `ZonedDateTime` against a `String` -- WITHOUT throwing. So the
        // NULLIF returned its first argument for every document, HTTP 200, and the shapes were
        // pinned here as "accepted" with no assertion on what they emitted. They are refused
        // below; see `refuse a NULLIF whose operands cannot be compared at all`.
        ("NULLIF(n, 1.5)", "BIGINT"),
        ("NULLIF(d, CAST('2025-01-01' AS DATE))", "DATE")
      )
    ) { (expr, dataType) =>
      everyVenueVerdict(expr, dataType).foreach { case (venue, verdict) =>
        withClue(s"[$venue] [$expr] ")(verdict shouldBe Right(()))
      }
    }
  }

  /** The REJECT list, verbatim from the ruling: exactly a TEXT-rendering operand against a
    * NUMBER-rendering one. Three of the four families were SILENT before -- the emitted `compareTo`
    * threw per document and a computed column's `ignore_failure: true` swallowed it, so the column
    * disappeared with HTTP 200.
    *
    * 🔴 `NULLIF(CAST(n AS KEYWORD), 0)` is the one place the ruling takes a WORKING shape away: a
    * numeric column a cast turned into TEXT. It stored the value on `main`. A cast can CREATE the
    * mismatch, not only cure it. (MEDIUM-1's six shapes; see the release note.)
    */
  it should "refuse a NULLIF that compares TEXT with a NUMBER, in every venue" in {
    forAll(
      Table(
        ("expr", "dataType", "left", "right"),
        ("NULLIF(s, 0)", "KEYWORD", "KEYWORD", "BIGINT"),
        ("NULLIF(n, 'x')", "BIGINT", "BIGINT", "VARCHAR"),
        ("NULLIF(CAST(s AS BIGINT), s)", "BIGINT", "BIGINT", "KEYWORD"),
        ("NULLIF(s, CAST(s AS BIGINT))", "KEYWORD", "KEYWORD", "BIGINT"),
        ("NULLIF(CAST(n AS KEYWORD), 0)", "KEYWORD", "KEYWORD", "BIGINT"),
        ("NULLIF(CAST(n AS KEYWORD), -1)", "KEYWORD", "KEYWORD", "BIGINT"),
        ("NULLIF(CAST(n AS KEYWORD), 1.5)", "KEYWORD", "KEYWORD", "DOUBLE"),
        ("NULLIF(0, CAST(n AS KEYWORD))", "KEYWORD", "BIGINT", "KEYWORD"),
        ("NULLIF(-1, CAST(n AS KEYWORD))", "KEYWORD", "BIGINT", "KEYWORD"),
        ("NULLIF(1.5, CAST(n AS KEYWORD))", "KEYWORD", "DOUBLE", "KEYWORD")
      )
    ) { (expr, dataType, left, right) =>
      everyVenueVerdict(expr, dataType).foreach { case (venue, verdict) =>
        withClue(s"[$venue] [$expr] -> $verdict ") {
          val reason = verdict.left.getOrElse(fail(s"[$venue] [$expr] was ACCEPTED"))
          // 🔴 Story 21.4's lesson: a rejection test is UNFALSIFIABLE while any `Left` passes it.
          // The message must NAME BOTH rendered types and must not be an internal-error label --
          // restoring the defect yields a `Left` too.
          //
          // 🔴 Read from the TYPE clause, not with `include` (issue #382, found by review). The
          // message echoes the whole expression AND, in the DDL venue, up to 200 characters of the
          // caller's SQL (#262) -- which declares `s KEYWORD, n BIGINT`. So `include("KEYWORD")`
          // and `include("BIGINT")` were free, and a message naming only ONE type, or the WRONG
          // pair, passed.
          val named = NamedTypes
            .findFirstMatchIn(reason)
            .orElse(NamedTypesAtParse.findFirstMatchIn(reason))
            .getOrElse(fail(s"[$venue] [$expr] message names no type pair: $reason"))
          (named.group(1), named.group(2)) shouldBe ((left, right))
          reason should not include "Internal"
          reason should not include "Operation failed"
          // ...and the DDL venue, which is the one the ruling repairs, must NAME THE EXPRESSION
          // and the column. (A `CAST(<numeric> AS <text>)` pair is already refused at PARSE in the
          // query venues by the pre-existing `NullIf.validate()`, whose message predates #382 and
          // names only the two types -- see the release note.)
          if (venue == "computed column") {
            reason should include(expr.substring(expr.indexOf("NULLIF")))
            reason should include("Column 'c'")
          }
        }
      }
    }
  }

  /** 🔴 The edge no type rule can decide. `NULLIF(d, '2025-01-01')` and `NULLIF(d, 'x')` present
    * IDENTICALLY as TIMESTAMP against VARCHAR, so a text-vs-non-text rule would refuse legitimate
    * SQL and a permissive one would ship the nonsense. The verdict comes from #276's
    * `TemporalLiterals` -- the SAME machinery a WHERE clause asks, against the column's own mapping
    * `format` -- so the engine has ONE date-literal grammar, not two.
    */
  it should "ask the temporal-literal resolver about a string literal over a date column" in {
    forAll(
      Table(
        // 🔴 AMENDED by the #382 review: EVERY row is refused now, and the column records which
        // JUDGE refused it, because that is what the wording has to come from.
        //
        // A temporal operand against a text one cannot be EMITTED -- the pair folds to VARCHAR,
        // misses both typed arms and lands on `==`, which is silently `false` for every document.
        // So a well-formed literal is refused too, with a message that says the engine cannot do
        // it yet rather than pretending the SQL is wrong. A MALFORMED literal is still refused by
        // the resolver, which names the literal and the field -- a strictly better message, and
        // the reason the resolver stays reachable and goes first.
        //
        // Making the well-formed half WORK is its own story (a `String -> temporal` coercion per
        // subtype, three venues, four ES majors; and `now-1d` is Elasticsearch date math, which
        // has no Painless equivalent at all and must stay refused by name).
        ("expr", "judge"),
        ("NULLIF(d, '2025-01-01')", "engine"),
        ("NULLIF(d, '2025-01-01 10:00:00')", "engine"),
        ("NULLIF(ts, '2025-01-01T10:00:00Z')", "engine"),
        ("NULLIF('2025-01-01', d)", "engine"),
        ("NULLIF(d, 'now-1d')", "engine"),
        ("NULLIF(d, '1735689600000')", "engine"),
        ("NULLIF(d, 'x')", "resolver"),
        ("NULLIF(d, '2026-02-30')", "resolver"),
        ("NULLIF('x', d)", "resolver"),
        ("NULLIF(ts, 'nope')", "resolver")
      )
    ) { (expr, judge) =>
      everyVenueVerdict(expr, "DATE").foreach { case (venue, verdict) =>
        withClue(s"[$venue] [$expr] -> $verdict ") {
          val reason = verdict.left.getOrElse(fail(s"[$venue] [$expr] was ACCEPTED"))
          reason should not include "Internal"
          // 🔴 The two judges must be TOLD APART, or this table proves only that everything is
          // refused -- which it would still do with the resolver deleted.
          if (judge == "engine") {
            reason should include("not supported yet")
            reason should not include "is not a valid"
          } else {
            reason should not include "not supported yet"
          }
          if (judge == "resolver") {
            // The resolver's own message, which names the LITERAL and the FIELD -- the wording the
            // type rule could never produce, and the reason it is asked FIRST.
            reason should include("as a date/time value for date field")
            reason should include(if (expr.contains("ts")) "'ts'" else "'d'")
            reason should not include "requires two arguments of comparable types"
          }
        }
      }
    }
  }

  /** 🔴 The population the #382 review found: pairs `toPainlessCall` cannot compare AT ALL, which
    * therefore reached its final `==` arm -- and Painless applies `==` to any two references
    * without complaining, answering `false`. So the NULLIF returned its first argument for EVERY
    * document, HTTP 200, in every venue. They were all pinned as "accepted" beforehand, asserting
    * the verdict and never the emission, which is exactly how they survived.
    *
    * The refusal is DERIVED from the arms (`NullIf.comparable`), not written beside them: both
    * text, both temporal, both numeric, both boolean, or one side UNKNOWN. So a new arm cannot
    * leave this list stale, and an unresolved column is never falsely refused.
    */
  it should "refuse a NULLIF whose operands cannot be compared at all" in {
    forAll(
      Table(
        ("expr", "dataType", "left", "right"),
        ("NULLIF(d, 0)", "DATE", "TIMESTAMP", "BIGINT"),
        ("NULLIF(0, d)", "DATE", "BIGINT", "TIMESTAMP"),
        ("NULLIF(b, 0)", "BOOLEAN", "BOOLEAN", "BIGINT"),
        ("NULLIF(b, 'x')", "BOOLEAN", "BOOLEAN", "VARCHAR"),
        ("NULLIF(s, d)", "KEYWORD", "KEYWORD", "TIMESTAMP"),
        ("NULLIF(d, b)", "DATE", "TIMESTAMP", "BOOLEAN")
      )
    ) { (expr, dataType, left, right) =>
      everyVenueVerdict(expr, dataType).foreach { case (venue, verdict) =>
        withClue(s"[$venue] [$expr] -> $verdict ") {
          val reason = verdict.left.getOrElse(fail(s"[$venue] [$expr] was ACCEPTED"))
          reason should include(left)
          reason should include(right)
          reason should not include "Internal"
          reason should not include "Operation failed"
        }
      }
    }
  }

  /** The other half of the same rule: a pair the arms CAN express must still be accepted, or
    * `comparable` would be satisfied by refusing everything. Each row exercises one arm.
    */
  it should "keep accepting every pair an arm can express" in {
    forAll(
      Table(
        ("expr", "dataType"),
        ("NULLIF(s, 'x')", "KEYWORD"), // both text
        ("NULLIF(n, 0)", "BIGINT"), // both numeric
        ("NULLIF(b, false)", "BOOLEAN"), // both boolean
        ("NULLIF(d, d)", "DATE"), // both temporal
        ("NULLIF(CAST(d AS DATE), CAST('2025-01-01' AS DATE))", "DATE")
      )
    ) { (expr, dataType) =>
      everyVenueVerdict(expr, dataType).foreach { case (venue, verdict) =>
        withClue(s"[$venue] [$expr] ")(verdict shouldBe Right(()))
      }
    }
  }

  /** A `NULLIF` is refused wherever it is WRITTEN, not only at the top of a SELECT field: the walk
    * is the one `Case.conditionsOf` uses, so an argument, a `CASE` branch and a nested `NULLIF` are
    * all reached. Each row was a COMPILE ERROR or a silent absence on `main`.
    */
  it should "find a mismatched NULLIF nested inside another expression" in {
    forAll(
      Table(
        ("expr", "dataType"),
        ("ABS(NULLIF(CAST(s AS BIGINT), s))", "DOUBLE"),
        ("COALESCE(NULLIF(s, 0), 'a')", "KEYWORD"),
        ("CASE WHEN n > 1 THEN NULLIF(s, 0) ELSE 'a' END", "KEYWORD"),
        ("NULLIF(NULLIF(s, status), 0)", "KEYWORD")
      )
    ) { (expr, dataType) =>
      everyVenueVerdict(expr, dataType).foreach { case (venue, verdict) =>
        withClue(s"[$venue] [$expr] -> $verdict ") {
          verdict.left.getOrElse(fail(s"[$venue] [$expr] was ACCEPTED")) should
          include("requires two arguments of comparable types")
        }
      }
    }
  }

  /** 🔴 The invariant `NullIf.operand`'s scaladoc states, asserted rather than read: with a context
    * every operand rendering is PLACEABLE, because a safe cast has already hoisted itself into the
    * prologue and what arrives is its parameter NAME. That is why the `Some(ctx)` branch may bind
    * unconditionally where `ArithmeticExpression.operandOf` must ask first.
    */
  it should "only ever bind an operand rendering that is a Painless EXPRESSION" in {
    val offenders = corpus.flatMap { case (expr, dataType) =>
      everyVenueEmission(expr, dataType).flatMap { case (venue, emitted) =>
        BoundOperand.findAllMatchIn(emitted).collect {
          case m if !PainlessOperandForm.placeable(m.group(1)) =>
            s"[$venue] [$expr] bound a statement: [${m.group(1)}]"
        }
      }
    }
    withClue(offenders.take(10).mkString("\n", "\n", "\n"))(offenders shouldBe empty)
  }

  // -- N2: a compound operand is bound, once -------------------------------------------------

  /** The LOUD half. `UPPER(s)` renders `(param1 == null) ? null : param1.toUpperCase()`, which
    * re-associates in all three of the template's slots.
    */
  it should "bind a nullable function operand and evaluate it ONCE" in {
    everyVenue("NULLIF(UPPER(s), 'X')").foreach { case (venue, emitted) =>
      withClue(s"[$venue] -> [$emitted] ") {
        countOf(emitted, "toUpperCase()") shouldBe 1
        emitted should include("def nif1 = (param1 == null) ? null : param1.toUpperCase(); ")
        emitted should include(
          "nif1 == null || (\"X\" != null && nif1.compareTo(\"X\") == 0) ? null : nif1"
        )
      }
    }
  }

  /** The SILENT half, and the reason the repair reaches the numeric arm too. `NULLIF(CASE WHEN n >
    * 1 THEN 1 ELSE 2 END, 1)` emitted `param2 ? 1 : 2 == 1 ? null : param2 ? 1 : 2`, which Painless
    * reads as `param2 ? 1 : ((2 == 1) ? null : …)`: MEASURED on ES 8.18.3, `n = 5` stored `c = 1`
    * where SQL says NULL, HTTP 200.
    */
  it should "bind a compound operand on the NUMERIC arm too" in {
    val expr = "NULLIF(CASE WHEN n > 1 THEN 1 ELSE 2 END, 1)"
    everyVenue(expr, "BIGINT").foreach { case (venue, emitted) =>
      withClue(s"[$venue] -> [$emitted] ") {
        countOf(emitted, "? 1 : 2") shouldBe 1
        emitted should include("def nif1 = param2 ? 1 : 2; ")
        emitted should include("nif1 == 1 ? null : nif1")
      }
    }
  }

  /** `arg1` is spliced twice and mis-associates the same way — `NULLIF(s, UPPER(s2))` emitted
    * `((param1 == null) ? null : param1.toUpperCase() != null && …)`, a null in a boolean slot.
    */
  it should "bind a compound SECOND argument" in {
    everyVenue("NULLIF(s, UPPER(s2))").foreach { case (venue, emitted) =>
      withClue(s"[$venue] -> [$emitted] ") {
        countOf(emitted, "toUpperCase()") shouldBe 1
        emitted should include("nif1 != null && ")
      }
    }
  }

  /** A NAME and a LITERAL are already safe operands, so neither is bound: the predicate is narrow
    * on purpose, and this is what keeps the working population byte-identical below.
    */
  it should "bind neither a name nor a literal" in {
    forAll(
      Table(
        ("expr", "dataType"),
        ("NULLIF(n, 0)", "BIGINT"),
        ("NULLIF(status, 'unknown')", "KEYWORD"),
        ("NULLIF(s, s2)", "KEYWORD"),
        ("NULLIF('a', 'b')", "KEYWORD")
      )
    ) { (expr, dataType) =>
      val emitted = processorOf(expr, dataType)
      withClue(s"[$expr] -> [$emitted] ")(emitted should not include "nif")
    }
  }

  // -- the shape of the whole script ----------------------------------------------------------

  /** Every `def` opens a STATEMENT, so it may only start the script or follow one; a hoisted local
    * that landed inside the expression would be a compile error, which is the failure mode the
    * `ArithmeticExpression` binding had twice. And a name declared twice is one Painless rejects.
    */
  it should "keep every declaration in the prologue, declared once" in {
    forAll(
      Table(
        ("expr", "dataType"),
        ("NULLIF(UPPER(s), 'X')", "KEYWORD"),
        ("NULLIF(TRIM(s), '')", "KEYWORD"),
        ("NULLIF(LENGTH(s), 0)", "BIGINT"),
        ("NULLIF(s, UPPER(s2))", "KEYWORD"),
        ("NULLIF(UPPER(s), LOWER(s2))", "KEYWORD"),
        ("NULLIF(TRY_CAST(s AS BIGINT), 0)", "BIGINT"),
        ("NULLIF(CASE WHEN n > 1 THEN 1 ELSE 2 END, 1)", "BIGINT")
      )
    ) { (expr, dataType) =>
      everyVenue(expr, dataType).foreach { case (venue, emitted) =>
        val offences = "def ".r
          .findAllMatchIn(emitted)
          .map(_.start)
          .filterNot(i =>
            i == 0 || emitted.startsWith("; ", i - 2) || emitted.startsWith("} ", i - 2)
          )
          .map(i => emitted.substring(math.max(0, i - 24), math.min(emitted.length, i + 12)))
          .toSeq
        val names = "def ([A-Za-z_][A-Za-z0-9_]*) =".r.findAllMatchIn(emitted).map(_.group(1)).toSeq
        withClue(s"[$venue] [$expr] -> [$emitted] ") {
          offences shouldBe empty
          names.groupBy(identity).collect { case (n, xs) if xs.size > 1 => n } shouldBe empty
        }
      }
    }
  }

  // -- byte stability of the population that already worked -----------------------------------

  /** The shapes the addendum listed as working, pinned BYTE-EXACT so the repair cannot move them
    * quietly. Measured against a `git clone --local` control at this branch's previous HEAD over a
    * 618-expression / 2,460-cell corpus; in the three venues production uses, the ONLY working
    * shapes that move are the two named in the class scaladoc.
    */
  it should "leave the working population byte-identical" in {
    forAll(
      Table(
        ("expr", "dataType", "expected"),
        (
          "NULLIF(n, 0)",
          "BIGINT",
          "def param1 = ctx.n; def param2 = param1 == 0 ? null : param1; ctx.c = param2"
        ),
        (
          "NULLIF(CAST(i AS BIGINT), 0)",
          "BIGINT",
          "def param1 = ctx.i; def param2 = (param1 != null ? (def)(((long) param1)) : null); " +
          "def param3 = param2 == 0 ? null : param2; ctx.c = param3"
        ),
        (
          "NULLIF(status, 'unknown')",
          "KEYWORD",
          "def param1 = ctx.status; def param2 = param1 == null || (\"unknown\" != null && " +
          "param1.compareTo(\"unknown\") == 0) ? null : param1; ctx.c = param2"
        )
      )
    ) { (expr, dataType, expected) =>
      withClue(s"[$expr] ")(processorOf(expr, dataType) shouldBe expected)
    }
  }

  /** The TEMPORAL pin the addendum singled out: `isEqual` is #373 item 2's fix and a `LocalTime`
    * has no such method, so this spelling must not drift with the type derivation.
    */
  it should "leave the temporal spellings byte-identical" in {
    fieldOf("NULLIF(CAST(d AS DATE), CAST('2025-01-01' AS DATE))") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param2 = (param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
    "def param3 = LocalDate.parse((\"2025-01-01\").replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); " +
    "def param4 = param2 == null || (param3 != null && param2.isEqual(param3)) ? null : param2; param4"

    fieldOf("NULLIF(CAST(d AS TIME), CAST('00:00:00' AS TIME))") should
    include("param2 == null || (param3 != null && param2.compareTo(param3) == 0) ? null : param2")
  }
}
