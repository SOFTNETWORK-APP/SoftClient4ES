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

package app.softnetwork.elastic.sql.census

import app.softnetwork.elastic.sql.SQLKeywords
import app.softnetwork.elastic.sql.function.FunctionN
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{Field, SingleSearch}
import app.softnetwork.elastic.sql.time.{IsoField, TimeField}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.regex.Pattern
import scala.io.Source
import scala.util.control.NonFatal
import scala.util.matching.Regex

/** Epic 19 Story 19.1 - the dialect census is DERIVED here, not typed.
  *
  * Three layers (AD-2): anchor (static scan -> file:line) / parse (Parser -> the example is real) /
  * reflect (AST + token registry -> aliases, types, ES construct).
  *
  * NOTHING in this suite is `assume`-guarded (AD-9). `SQLKeywordsSpec` cancels when the source tree
  * is absent because its scan is an auxiliary guard; here the scan IS the deliverable, and a
  * cancelled test would satisfy AC-4 and AC-5 vacuously.
  */
class DialectCensusSpec extends AnyFlatSpec with Matchers {

  // --- source location (AD-6: `read` + candidate roots copied from SQLKeywordsSpec) --------
  private val repoRootCandidates = Seq(new File("."), new File(".."), new File("../.."))
  private def repoRoot: Option[File] =
    repoRootCandidates.find(r => new File(r, "sql/src/main/scala").isDirectory)

  private def read(f: File): String = {
    val src = Source.fromFile(f, "UTF-8")
    try src.mkString
    finally src.close()
  }

  /** Report EVERY offending row, not just the first.
    *
    * A ~250-row census authored against a fail-fast `foreach` costs one sbt round-trip per bad row.
    * AD-2's promise is "fails the build with the anchor named" - naming one of forty is compliance,
    * not usefulness.
    */
  private def checkAll[A](items: Seq[A], what: String)(label: A => String)(
    check: A => Unit
  ): Unit = {
    val failures = items.flatMap { a =>
      try { check(a); None }
      catch { case NonFatal(t) => Some(s"  - ${label(a)}: ${t.getMessage}") }
    }
    withClue(
      s"$what: ${failures.size} of ${items.size} rows failed\n${failures.mkString("\n")}\n"
    ) {
      failures shouldBe empty
    }
  }

  /** Wrap-tolerant anchor match (AD-10).
    *
    * A literal `indexOf` has ZERO tolerance for re-wrapping, and `sbt scalafmtAll` (maxColumn 100)
    * runs in this very story - the same reason REPL.1's AD-7 made its scan regexes `\s+`-tolerant.
    * `Pattern.quote` also makes the author's backslashes literal, so an anchor for `Expr("\\:\\:")`
    * needs no regex escaping of its own - just a triple-quoted Scala string.
    */
  private def anchorRegex(anchor: String): Regex =
    anchor.trim.split("\\s+").map(Pattern.quote).mkString("\\s+").r

  /** Resolve an entry's anchor to a 1-based line number; fail loudly if absent or ambiguous. */
  private def resolveLine(e: CensusEntry): Int = {
    withClue(s"[${e.id}] ownerAnchor is blank\n") { e.ownerAnchor.trim should not be empty }
    val f = new File(repoRoot.get, e.ownerFile)
    withClue(s"[${e.id}] ownerFile does not exist: ${e.ownerFile}\n") { f.isFile shouldBe true }
    val text = read(f)
    val hits = anchorRegex(e.ownerAnchor).findAllMatchIn(text).map(_.start).toList
    withClue(
      s"[${e.id}] expected the anchor exactly once in ${e.ownerFile}, found ${hits.size}.\n" +
      s"  anchor: ${e.ownerAnchor}\n" +
      "  0 hits  -> the declaration moved or was renamed. Re-target on the SHORTEST unique\n" +
      "             substring of the declaration head (AD-10); do not lengthen it.\n" +
      "  >1 hits -> the text is ambiguous. NOTE: the file is read raw, so a COMMENTED-OUT copy\n" +
      "             counts (function/cond/package.scala's Exists is exactly that). Shorten or\n" +
      "             re-target; never relax this assertion.\n"
    ) { hits.size shouldBe 1 }
    // 2.12-safe: count newlines before the hit; no linesIterator arithmetic.
    text.substring(0, hits.head).count(_ == '\n') + 1
  }

  // --- layer 0: the tree must be there (AD-9) ----------------------------------------------
  "the census" should "locate the sql source tree it anchors against" in {
    withClue(
      "sql/src/main/scala not found from the working directory " +
      s"(${new File(".").getAbsolutePath}). The census is DERIVED from source and cannot be " +
      "produced without it, so this is a failure, not a skip. sbt runs tests in-process from " +
      "the repo root (build.sbt sets no `fork` key); if that ever changes, set " +
      "`Test / baseDirectory := (ThisBuild / baseDirectory).value` for the sql project.\n"
    ) { repoRoot shouldBe defined }
  }

  // --- layer 1: anchors --------------------------------------------------------------------
  it should "anchor every entry to exactly one real declaration" in {
    checkAll(DialectCensus.entries, "anchor resolution")(_.id)(e => resolveLine(e) should be > 0)
  }

  it should "assign every entry a unique id" in {
    val dupes = DialectCensus.entries.groupBy(_.id).collect { case (id, es) if es.size > 1 => id }
    withClue(s"duplicate census ids: $dupes\n") { dupes shouldBe empty }
  }

  /** No non-ASCII anywhere in an authored string.
    *
    * `scalacCompilerOptions` carries NO `-encoding` (build.sbt). CI sets `-Dfile.encoding=UTF-8` on
    * the compile/test jobs but NOT on the headerCheck/scalafmt job, and a local run sets nothing. A
    * non-ASCII character in an `ownerAnchor` breaks the match outright; in `evidence`/`notes` it
    * mojibakes the emitted CSV that 19.2/19.3 consume. Write `Clause 6.30`, not a section sign.
    */
  it should "keep every authored string ASCII-only" in {
    checkAll(DialectCensus.entries, "ASCII check")(_.id) { e =>
      val bad = Seq(
        e.id,
        e.token,
        e.spelling,
        e.ownerFile,
        e.ownerAnchor,
        e.exampleSql,
        e.arity,
        e.evidence,
        e.notes
      ).filter(_.exists(_ > 127))
      withClue(s"non-ASCII in: ${bad.mkString(" | ")}\n") { bad shouldBe empty }
    }
  }

  // --- layer 2: every example is real SQL --------------------------------------------------
  it should "parse every example statement through the real parser" in {
    checkAll(DialectCensus.entries, "example parse")(e => s"${e.id} :: ${e.exampleSql}") { e =>
      withClue(
        "`Parser` is `phrase`-anchored (Parser.scala) - the WHOLE statement must be consumed. " +
        "Parser.apply also prints the combinator message to stderr; with parallel suites that " +
        "message is detached from this failure, so read stderr as a whole.\n"
      ) { Parser(e.exampleSql).isRight shouldBe true }
    }
  }

  /** A `Kind.Fn` row's ES construct is derived from the single projected field, so the example must
    * BE that projection. Rows whose function cannot be projected are listed, with a reason, in
    * `DialectCensus.unprojectable` and are pinned instead of derived.
    */
  it should "shape every function example as a single-field SELECT" in {
    checkAll(
      DialectCensus.entries.filter(e =>
        e.kind == Kind.Fn && !DialectCensus.unprojectable.contains(e.id)
      ),
      "example shape"
    )(_.id) { e =>
      Parser(e.exampleSql) match {
        case Right(s: SingleSearch) =>
          withClue(s"projects ${s.select.fields.size} fields: ${e.exampleSql}\n") {
            s.select.fields.size shouldBe 1
          }
        case other =>
          fail(s"a projectable Kind.Fn example must be a single SELECT, got: $other")
      }
    }
  }

  // --- Rule R is enforced, not merely declared (PD-2) --------------------------------------
  it should "carry a Rule-R-shaped citation on every row" in {
    checkAll(DialectCensus.entries, "evidence shape")(e => s"${e.id} [${e.standard.label}]") { e =>
      withClue(
        s"evidence=${e.evidence}\n" +
        "  ansi -> must cite SQL:2016 plus a Feature id; " +
        "ansi_adjacent -> at least TWO fetched engine-doc URLs (PD-3's trio); " +
        "es_specific -> must name the Elasticsearch concept it reaches.\n"
      ) { e.standard.evidenceOk(e.evidence) shouldBe true }
    }
  }

  it should "pin an ES construct on every row" in {
    // 19.3 classifies on `esConstruct`; a None here is a hole in its input, and it would also
    // make AC-7's derivation check opt-out by the person it polices.
    checkAll(DialectCensus.entries, "expectEs present")(_.id)(_.expectEs shouldBe defined)
  }

  // --- layer 3: AST-derived columns (Task 4, AC-7) -----------------------------------------

  /** 2.12 + 2.13 safe: a plain match, not `.right.toOption.collect` (AD-7 - `Either.toOption`
    * exists on both legs, and `.right` is DEPRECATED on 2.13).
    *
    * Returns None for a row in `DialectCensus.unprojectable`, so the emitter records it as `pinned`
    * rather than deriving an ES construct from an unrelated projection.
    */
  private def projectedField(e: CensusEntry): Option[Field] =
    if (e.kind != Kind.Fn || DialectCensus.unprojectable.contains(e.id)) None
    else
      Parser(e.exampleSql) match {
        case Right(s: SingleSearch) if s.select.fields.size == 1 => s.select.fields.headOption
        case _                                                   => None
      }

  private def derivedEsConstruct(f: Field): EsConstruct =
    if (f.isAggregation) EsConstruct.NativeAgg // FunctionChain.isAggregation
    else if (f.isBucketScript) EsConstruct.BucketScript // query/Select.scala
    else if (f.isScriptField) EsConstruct.PainlessField // Identifier.painlessScriptRequired
    // painlessScriptRequired = functions.nonEmpty && !hasAggregation && bucket.isEmpty, so a
    // FUNCTION ON A GROUP-BY KEY (`SELECT DATE_TRUNC(d, MONTH) ... GROUP BY 1`) satisfies none
    // of the three above and would silently report `doc_field`. It is a bucket-key script.
    else if (f.identifier.bucket.nonEmpty && f.functions.nonEmpty) EsConstruct.BucketKey
    else EsConstruct.DocField

  /** Only these are asserted against the pin. `WindowTopHits`, `QueryClause`, `SortClause`,
    * `RequestShape` and `ClientSide` have no AST predicate today, so those rows are pinned and the
    * derived value is recorded in `derivedNotes` rather than asserted.
    */
  private val derivable: Set[EsConstruct] = Set(
    EsConstruct.NativeAgg,
    EsConstruct.BucketScript,
    EsConstruct.PainlessField,
    EsConstruct.BucketKey,
    EsConstruct.DocField
  )

  /** First `FunctionN` in the field's chain. `Field.functions` delegates to `identifier.functions`;
    * not every `Function` is a `FunctionN`, so this is an Option.
    */
  private def functionNode(f: Field): Option[FunctionN[_, _]] =
    f.functions.collectFirst { case fn: FunctionN[_, _] => fn }

  /** `SQLType.typeId` is the stable rendering; `toString` delegates to it. */
  private def argTypesFor(f: Option[Field]): List[String] =
    f.flatMap(functionNode).map(_.argTypes.map(_.typeId)).getOrElse(Nil)

  private def returnTypeFor(f: Option[Field]): String =
    f.flatMap(functionNode).map(_.baseType.typeId).getOrElse("")

  /** Why a derived column is blank - recorded at DERIVE time, because `notes` is authored. */
  private def derivedNotesFor(e: CensusEntry, f: Option[Field]): List[String] =
    List(
      if (f.isEmpty && e.kind == Kind.Fn)
        Some(DialectCensus.unprojectable.getOrElse(e.id, "example is not a single-field SELECT"))
      else None,
      f.flatMap(functionNode) match {
        case Some(fn) if fn.args.isEmpty =>
          Some(
            "node declares args = Nil, so arg_types is empty by construction (geo.Distance " +
            "does this); arity comes from the parser production"
          )
        case None if f.isDefined => Some("projected field carries no FunctionN node")
        case _                   => None
      }
    ).flatten

  /** THE column no scraper can produce (F-2). `Kind.Fn` rows read the live registry; every other
    * kind supplies its own list, because its token object is not in `SQLKeywords.functionTokens`.
    */
  private lazy val wordsByToken: Map[String, List[String]] =
    SQLKeywords.functionTokens.map(t => t.sql.toUpperCase -> SQLKeywords.wordsOf(t)).toMap

  private def aliasesFor(e: CensusEntry): List[String] =
    if (e.kind == Kind.Fn)
      wordsByToken.getOrElse(e.token.toUpperCase, Nil).filterNot(_ == e.token.toUpperCase)
    else e.aliasesOverride.getOrElse(Nil)

  it should "derive the ES construct of every derivable function row from the parsed AST" in {
    checkAll(
      DialectCensus.entries.filter(e =>
        e.kind == Kind.Fn &&
        !DialectCensus.unprojectable.contains(e.id) &&
        derivable.contains(e.expectEs.get)
      ),
      "ES construct derivation"
    )(e => s"${e.id} :: ${e.exampleSql}") { e =>
      val f = projectedField(e)
        .getOrElse(
          fail(
            "no projected field - list the row in DialectCensus.unprojectable, with a reason, " +
            "if it genuinely cannot be projected"
          )
        )
      derivedEsConstruct(f) shouldBe e.expectEs.get
    }
  }

  it should "record every alias the runtime token registry accepts" in {
    // F-5. These are the assertions that prove the generator RAN. Nine of the 149 spellings
    // exist in NO source literal at all (TimeField.words' underscore-stripped forms), so a
    // scraped census cannot reach 149 - which makes this number the tooling-choice guard as
    // well as the alias one.
    withClue("SQLKeywords.functionTokens changed size - 19.3's estimate base moved (F-1)\n") {
      SQLKeywords.functionTokens.size shouldBe 95
    }
    withClue(
      "two function tokens now share a `sql` literal. AC-1's coverage diff is SET-based and " +
      "would silently stop detecting one of the pair. NB the codebase ALREADY has " +
      "cross-category `sql` collisions (TimeField.HOUR_OF_DAY vs TimeUnit.HOURS, MONTH_OF_YEAR " +
      "vs MONTHS, DAY_OF_MONTH vs DAYS, string LeftOp vs query LeftJoin - all " +
      "Expr(\"HOUR\"/\"MONTH\"/\"DAY\"/\"LEFT\")); one such pair landing INSIDE functionTokens " +
      "breaks the diff.\n"
    ) { SQLKeywords.functionTokens.map(_.sql.toUpperCase).toSet.size shouldBe 95 }

    val registryWords: Set[String] =
      SQLKeywords.functionTokens.flatMap(SQLKeywords.wordsOf).toSet
    withClue(s"accepted function spellings moved from 149 to ${registryWords.size} (F-2/F-5)\n") {
      registryWords.size shouldBe 149
    }

    // Three of the nine genuinely un-greppable spellings, named, so a failure says WHAT
    // regressed.
    SQLKeywords.wordsOf(TimeField.MONTH_OF_YEAR) should contain("MONTHOFYEAR")
    SQLKeywords.wordsOf(TimeField.HOUR_OF_DAY) should contain("HOUROFDAY")
    SQLKeywords.wordsOf(IsoField.QUARTER_OF_YEAR) should contain("QUARTEROFYEAR")

    // And the emitted column really is derived: every token that HAS aliases must yield some.
    val aliasBearing = SQLKeywords.functionTokens
      .filter(t => SQLKeywords.wordsOf(t).size > 1)
      .map(_.sql.toUpperCase)
      .toSet
    checkAll(
      DialectCensus.entries
        .filter(r => r.kind == Kind.Fn && aliasBearing.contains(r.token.toUpperCase)),
      "alias derivation"
    )(r => s"${r.id} (token ${r.token})")(r => aliasesFor(r) should not be empty)

    // Every alias spelling must also be SOMEONE's `spelling` (T1 - an alias gets its own row).
    val censusSpellings = DialectCensus.entries.map(_.spelling.toUpperCase).toSet
    val canonical = SQLKeywords.functionTokens.map(_.sql.toUpperCase).toSet
    val missedAliases = registryWords.diff(canonical).diff(censusSpellings)
    withClue(s"accepted spellings with no census row of their own (T1): $missedAliases\n") {
      missedAliases shouldBe empty
    }
  }

  // --- coverage + phantom assertions (Task 5, AC-1/2/3) ------------------------------------

  /** Census words the REGISTRY deliberately does not carry, each with a written rationale.
    *
    * The spec's create-time draft carried only the 8 geo units (the measured Expr-scrape residue).
    * Implementation extends it in the OTHER direction - census->registry - with the 8 plural
    * time-unit spellings (accepted by the TimeUnit regex, curated in
    * `SQLKeywords.timeUnitPluralWords`, carried by no token's words) and TRUE/FALSE (parsed by
    * `TypeParser.boolean`, curated in `SQLKeywords.literalWords`, not TokenRegex-backed). Recorded
    * as a deviation in the 19.1 findings log.
    */
  private val nonRegistryWords: Map[String, String] = Map(
    "KM" -> ("geo distance unit; excluded from SQLKeywords because units are word-shaped and " +
    "would colourise table aliases in the REPL (REPL.1 PD)"),
    "M"        -> "geo distance unit (1 letter)",
    "CM"       -> "geo distance unit",
    "MM"       -> "geo distance unit",
    "MI"       -> "geo distance unit",
    "YD"       -> "geo distance unit",
    "FT"       -> "geo distance unit",
    "NMI"      -> "geo distance unit",
    "YEARS"    -> "plural time unit (TimeUnit regex optional s; SQLKeywords.timeUnitPluralWords)",
    "MONTHS"   -> "plural time unit",
    "QUARTERS" -> "plural time unit",
    "WEEKS"    -> "plural time unit",
    "DAYS"     -> "plural time unit",
    "HOURS"    -> "plural time unit",
    "MINUTES"  -> "plural time unit",
    "SECONDS"  -> "plural time unit",
    "BIGINT" -> ("SQL type name parsed by TypeParser.sql_type; curated in " +
    "SQLKeywords.typeWords (the census carries one representative type-name row for AC-3)"),
    "TRUE"  -> "boolean literal parsed by TypeParser.boolean; SQLKeywords.literalWords",
    "FALSE" -> "boolean literal parsed by TypeParser.boolean; SQLKeywords.literalWords"
  )

  /** Pure-symbol operators. `wordsOf` filters to `[A-Z][A-Z0-9_]*`, so NOTHING in the word-set
    * assertions can see these - without this list, deleting the `%`, `::` or `||` row breaks no
    * test and AC-3 is unenforced for the whole arithmetic/operator surface.
    */
  private val symbolSpellings: Set[String] =
    Set("=", "<>", "!=", ">=", ">", "<=", "<", "+", "-", "*", "/", "%", "||", "::")

  /** Same normalisation SQLKeywords.wordsOf applies. Replace the literal `\s+` separator BEFORE
    * uppercasing - uppercasing `\s+` yields `\S+`, a negated class.
    */
  private def normWords(ss: Seq[String]): List[String] =
    ss.toList
      .map(_.replaceAll("\\\\s\\+", " "))
      .flatMap(_.split("\\s+").toList)
      .map(_.toUpperCase)
      .filter(_.matches("[A-Z][A-Z0-9_]*"))

  private lazy val censusWords: Set[String] =
    DialectCensus.entries
      .flatMap(e => normWords(Seq(e.token, e.spelling) ++ e.aliasesOverride.getOrElse(Nil)))
      .toSet

  it should "cover every SQLKeywords function token, and invent none" in {
    val registryTokens = SQLKeywords.functionTokens.map(_.sql.toUpperCase).toSet
    val censusTokens =
      DialectCensus.entries.filter(_.kind == Kind.Fn).map(_.token.toUpperCase).toSet

    val missing = registryTokens.diff(censusTokens)
    withClue(
      s"function tokens absent from the census: $missing\n" +
      "  TRAP: a Kind.Fn row's `token` is the token's `sql`, NOT its object name. The 15 " +
      "temporal extractors differ: MONTH_OF_YEAR.sql = \"MONTH\", DAY_OF_MONTH.sql = \"DAY\", " +
      "DAY_OF_WEEK.sql = \"WEEKDAY\", DAY_OF_YEAR.sql = \"YEARDAY\", NANO_OF_SECOND.sql = " +
      "\"NANOSECOND\", EPOCH_DAY.sql = \"EPOCHDAY\", QUARTER_OF_YEAR.sql = \"QUARTER\", " +
      "WEEK_OF_WEEK_BASED_YEAR.sql = \"WEEK\".\n"
    ) { missing shouldBe empty }

    val invented = censusTokens.diff(registryTokens)
    withClue(
      s"census function tokens with no SQLKeywords backing: $invented\n" +
      "  An ALIAS row keeps the canonical token in `token` and the alias in `spelling` (T1) - " +
      "`SUBSTR` is a WORD of the `Substring` token, not a token.\n"
    ) { invented shouldBe empty }
  }

  it should "cover every word-bearing token the registry knows, and invent none" in {
    val registryWords: Set[String] = SQLKeywords.tokens.flatMap(SQLKeywords.wordsOf).toSet

    val uncovered = registryWords.diff(censusWords)
    withClue(
      s"tokens the registry accepts but the census does not describe: $uncovered\n" +
      "  Every clauseToken word counts - SELECT, FROM, WHERE, AS, ASC, DESC, ORDER, PARTITION, " +
      "AGAINST, WHEN, THEN, ELSE, END, and the OUTER spellings of LEFT/RIGHT/FULL. Author the " +
      "clause block FROM SQLKeywords.clauseTokens (47), not by hand. Or move the word into " +
      "`nonRegistryWords` with a written rationale.\n"
    ) { uncovered shouldBe empty }

    val notInRegistry = censusWords.diff(registryWords).diff(nonRegistryWords.keySet)
    withClue(
      s"census words with no registry backing and no documented exclusion: $notInRegistry\n"
    ) {
      notInRegistry shouldBe empty
    }
  }

  it should "describe every symbol operator" in {
    val spelled = DialectCensus.entries.map(_.spelling).toSet
    withClue(
      s"symbol operators absent from the census: ${symbolSpellings.diff(spelled)}\n" +
      "  `spelling` carries the SQL SURFACE (`||`, `::`), not the regex-escaped token literal - " +
      "`Pipe.sql` is `\\|\\|` and `CastOperator.sql` is `\\:\\:`.\n"
    ) { symbolSpellings.diff(spelled) shouldBe empty }
  }

  /** AD-3 has no other gate: every set-wise assertion is keyed on tokens, so a census with exactly
    * one row per token passes them all while silently deleting ~13 forms of Epic 22 work - the
    * precise deflating error PD-1 and epic framing point 3 exist to prevent.
    */
  it should "keep one row per syntax FORM, not one per token" in {
    val minRows = Map(
      "SUBSTRING"       -> 3, // FROM/FOR form, comma form, SUBSTR alias
      "LEFT"            -> 2, // FOR form, comma form
      "RIGHT"           -> 2,
      "DATE_DIFF"       -> 2, // BigQuery order + T-SQL order (+ DATEDIFF alias)
      "DATE_TRUNC"      -> 2, // date-first + part-first (+ DATETRUNC alias)
      "PERCENTILE_CONT" -> 5,
      "PERCENTILE_DISC" -> 5
    )
    val counts = DialectCensus.entries.filter(_.kind == Kind.Fn).groupBy(_.token.toUpperCase)
    checkAll(minRows.toSeq, "form coverage")(_._1) { case (tok, min) =>
      withClue(
        s"$tok has ${counts.getOrElse(tok, Nil).size} rows, expected at least $min (AD-3)\n"
      ) {
        counts.getOrElse(tok, Nil).size should be >= min
      }
    }
  }

  it should "never document a function the engine does not implement" in {
    // F-3 TRAP: check against the FUNCTION token words only. `IF` and `TRUNCATE` are present in
    // SQLKeywords.allWords as DDL statementWords (IF NOT EXISTS, TRUNCATE TABLE) and would
    // falsely clear two phantoms if allWords were used here.
    val functionWords: Set[String] =
      SQLKeywords.functionTokens.flatMap(SQLKeywords.wordsOf).toSet
    val leaked = DialectCensus.phantomFunctionNames.intersect(functionWords)
    withClue(s"names documented as phantom are now real function tokens - update PD-4: $leaked\n") {
      leaked shouldBe empty
    }
    val censusTokens =
      DialectCensus.entries.filter(_.kind == Kind.Fn).map(_.token.toUpperCase).toSet
    val classified = DialectCensus.phantomFunctionNames.intersect(censusTokens)
    withClue(s"phantom names classified as census functions: $classified\n") {
      classified shouldBe empty
    }

    // The anti-drift the phantom problem ACTUALLY needed. A hard-coded list of 13 is exactly
    // the artefact that let phantoms 11-13 appear unnoticed; walk the corpus that is on disk
    // instead. NB: all 13 phantoms AND json/_index.json are UNTRACKED, so a clean checkout has
    // 105 tracked files / 7 indices / 98 docs and this walk simply sees fewer. Asserting that a
    // phantom FILE exists would fail every fresh clone and break AC-4 - so we never assert that.
    def jsonDocs(dir: File): Seq[File] = {
      val (dirs, files) =
        Option(dir.listFiles).getOrElse(Array.empty[File]).toSeq.partition(_.isDirectory)
      files.filter(f => f.getName.endsWith(".json") && f.getName != "_index.json") ++
      dirs.flatMap(jsonDocs)
    }
    val helpDir = new File(repoRoot.get, "core/src/main/resources/help/functions")
    val docNames =
      jsonDocs(helpDir).map(f => f.getName.stripSuffix(".json").toUpperCase).toSet
    val unaccounted = docNames
      .diff(functionWords)
      .diff(DialectCensus.phantomFunctionNames)
      .diff(DialectCensus.nonFunctionHelpNames)
    withClue(
      s"help documents that are neither a function spelling, a recorded phantom, nor a " +
      s"recorded non-function token: $unaccounted\n" +
      "  Either the engine gained the function (move it to a census row), or this is a NEW " +
      "phantom - add it to phantomFunctionNames AND phantomHelpFiles with its proof.\n"
    ) { unaccounted shouldBe empty }
  }

  // --- emit the machine-readable companions (Task 6, AC-4/5/10) ----------------------------
  it should "emit the machine-readable companions" in {
    val rows = DialectCensus.entries.map { e =>
      val f = projectedField(e)
      val derived = f.map(derivedEsConstruct)
      CensusEmitter.Row(
        entry = e,
        line = resolveLine(e),
        aliases = aliasesFor(e),
        argTypes = argTypesFor(f),
        returnType = returnTypeFor(f),
        // The PIN wins for a non-derivable construct; the derived value is still recorded, so
        // a disagreement is visible in the artefact rather than silently resolved.
        esConstruct =
          if (derived.exists(_ => derivable.contains(e.expectEs.get))) derived.get.label
          else e.expectEs.get.label,
        esConstructSource =
          if (derived.isDefined && derivable.contains(e.expectEs.get)) "derived"
          else "pinned",
        derivedNotes = derivedNotesFor(e, f) ++
          derived
            .filterNot(_ => derivable.contains(e.expectEs.get))
            .map(d => s"AST reports ${d.label}; row pins ${e.expectEs.get.label}")
      )
    }
    val out = new File(repoRoot.get, "sql/target/epic-19")
    out.mkdirs()
    withClue(s"could not create ${out.getPath}\n") { out.isDirectory shouldBe true }

    // Provenance: `sbt "+ sql/test"` runs both legs and they share this directory, so the file
    // must say which one wrote it. Nothing in the derivation is version-dependent, so they
    // agree.
    val stamp = "generated by DialectCensusSpec on Scala " +
      s"${scala.util.Properties.versionNumberString}; anchors resolved against the working tree"

    Files.write(
      Paths.get(out.getPath, "dialect-census.csv"),
      CensusEmitter.toCsv(rows).getBytes(StandardCharsets.UTF_8)
    )
    Files.write(
      Paths.get(out.getPath, "dialect-census.json"),
      CensusEmitter.toJson(rows, stamp).getBytes(StandardCharsets.UTF_8)
    )
    Files.write(
      Paths.get(out.getPath, "dialect-census.md"),
      CensusEmitter.toMarkdown(rows, stamp).getBytes(StandardCharsets.UTF_8)
    )

    // Content assertions - `rows.size shouldBe entries.size` is a tautology (rows =
    // entries.map).
    withClue("a row resolved to line 0 - resolveLine regressed\n") {
      rows.count(_.line <= 0) shouldBe 0
    }
    withClue(
      "no row's ES construct was derived from a parsed AST - the reflect layer regressed\n"
    ) {
      rows.count(_.esConstructSource == "derived") should be > 0
    }
    withClue("CSV row count does not match the census - a cell contained an unquoted newline\n") {
      CensusEmitter.toCsv(rows).count(_ == '\n') should be >= rows.size + 1
    }
  }
}
