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

package app.softnetwork.elastic.sql.doc

import app.softnetwork.elastic.sql.SQLKeywords
import app.softnetwork.elastic.sql.parser.Parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.util.Locale

/** Guard for the generated half of `documentation/sql/keywords.md`.
  *
  * "Generated" degrades to "generated once" without a test that reddens when the committed page and
  * the engine disagree, so this suite owns four separable failure modes. Each needs its own
  * assertion, because none of them can see the others:
  *
  *   1. the committed page is STALE - the generated region differs from what the engine renders
  *      today. The only failure a regeneration fixes, and the message says so. 2. the GENERATOR is
  *      short, or invented - it renders fewer words than the registry and the reserved list hold,
  *      or MORE. A comparison of the page against the generator cannot see either, because both
  *      sides would be wrong together. Both directions are asserted: a hand-written word list
  *      relocated INTO the generator is exactly as bad as one on the page. 3. the RENDER is wrong -
  *      the model is right and the markdown misrepresents it. Asserted by parsing the rows back OUT
  *      of the rendered section and comparing them to the model, which is the only thing that can
  *      catch a dropped column or a mislabelled cell. 4. the PAGE makes a claim the parser
  *      contradicts. The `Reserved` column is an oracle about parsing, so it is executed here
  *      against the parser, independently of how the generator derived it; likewise every SQL
  *      sentence the prose states.
  *
  * Deliberately NO `assume`: an `assume`-cancelled test exits 0, and this suite IS the mechanism
  * that keeps the page honest. The `sql` tests are not forked, so the working directory is the
  * repository root.
  */
class KeywordsPageSpec extends AnyFlatSpec with Matchers {

  private val page: File = KeywordsPage.pageFile()

  private def content: String = KeywordsPage.read(page)

  private def staleClue: String =
    s"\n\n${KeywordsPage.PagePath} is out of date with the engine.\n" +
    s"Regenerate it with:  ${KeywordsPage.RegenerateCommand}\n" +
    s"and commit the result. Do NOT edit the generated region by hand.\n"

  // --- reading the rendered markdown back -------------------------------------------------

  private val rowPattern = """^\| `([^`]+)` \| (yes|no|no \(shadowed\)) \| (.+) \|$""".r
  private val phrasePattern = """^\| `([^`]+)` \|$""".r
  private val countPattern = """^(\d+) keywords, of which (\d+) are reserved\.$""".r

  /** (word, reserved cell, kind) triples parsed out of the rendered keyword table. */
  private def renderedRows: Seq[(String, String, String)] =
    KeywordsPage.generatedSection.linesIterator.collect { case rowPattern(w, r, k) =>
      (w, r, k)
    }.toList

  private def renderedPhrases: Seq[String] = {
    val lines = KeywordsPage.generatedSection.linesIterator.toList
    val start = lines.indexWhere(_.startsWith("## Compound phrases"))
    val end = lines.indexWhere(_.startsWith("## Symbolic operators"))
    start should be >= 0
    end should be > start
    lines.slice(start, end).collect { case phrasePattern(p) => p }
  }

  "the keywords page" should "exist at the path the generator writes" in {
    withClue(
      s"${KeywordsPage.PagePath} not found; working directory is " +
      s"${new File(".").getAbsolutePath}. The sql tests must run from the repository root.\n"
    ) {
      page.isFile shouldBe true
    }
  }

  it should "carry the two generated-region markers exactly once, in order" in {
    val text = content
    text.indexOf(KeywordsPage.BeginMarker) should be >= 0
    text.indexOf(KeywordsPage.EndMarker) should be > text.indexOf(KeywordsPage.BeginMarker)
    text.split(java.util.regex.Pattern.quote(KeywordsPage.BeginMarker), -1).length shouldBe 2
    text.split(java.util.regex.Pattern.quote(KeywordsPage.EndMarker), -1).length shouldBe 2
    // The prose is preserved verbatim while the generated region always emits LF, so a CRLF page
    // regenerates into a permanently mixed-EOL file that then passes every other test. The
    // `.gitattributes -text` rule blocks git's half of that; this blocks an editor's half.
    withClue("the page must be LF-only - see the -text rule in .gitattributes: ") {
      text should not include "\r"
    }
  }

  it should "read a reserved-word list with no duplicate in it" in {
    // Harmless as a regex alternative, but this list is a DOCUMENTATION source now, and a
    // duplicate is invisible once it reaches a Set.
    val reserved = Parser.reservedKeywords
    withClue(s"duplicated entries: ${reserved.diff(reserved.distinct).distinct}\n") {
      reserved.distinct.size shouldBe reserved.size
    }
  }

  // --- (1) the committed page matches what the engine renders ------------------------------

  it should "match the generated region byte for byte" in {
    // Compare ONLY the generated region: a whole-page comparison prints a 20 KB diff in which the
    // one changed cell is unfindable.
    val current = content
    withClue(staleClue) {
      KeywordsPage.committedSection(current) shouldBe KeywordsPage.generatedSection
    }
    withClue(staleClue)(KeywordsPage.render(current) shouldBe current)
  }

  it should "regenerate idempotently" in {
    val once = KeywordsPage.render(content)
    KeywordsPage.render(once) shouldBe once
  }

  // --- (2) the generator is neither short nor inventive ------------------------------------

  it should "emit a row for every word in SQLKeywords" in {
    val emitted = KeywordsPage.entries.map(_.word).toSet
    withClue("words in SQLKeywords.allWords with no row on the page: ") {
      SQLKeywords.allWords.diff(emitted) shouldBe empty
    }
  }

  it should "emit no row that no source of truth backs" in {
    // The reverse containment. Without it, a hand-written word list moved INTO the generator - the
    // defect this change removes, relocated one file - ships green.
    val emitted = KeywordsPage.entries.map(_.word).toSet
    withClue("rows backed by neither SQLKeywords nor Parser.reservedKeywords: ") {
      emitted.diff(SQLKeywords.allWords ++ KeywordsPage.reservedWords) shouldBe empty
    }
  }

  it should "emit a row for every reserved word, flagged reserved" in {
    val reserved = Parser.reservedKeywords.map(_.toUpperCase(Locale.ROOT)).toSet
    val flagged = KeywordsPage.entries.filter(_.reserved).map(_.word).toSet
    // Both directions: a missing row loses a reservation the user needs to know about, and a
    // spurious one tells them to quote a name that does not need quoting.
    withClue("reserved words missing from the page: ")(reserved.diff(flagged) shouldBe empty)
    withClue("words flagged reserved that the parser does not reserve: ")(
      flagged.diff(reserved) shouldBe empty
    )
  }

  it should "emit no row twice, and exactly the known kinds" in {
    val words = KeywordsPage.entries.map(_.word)
    words.distinct.size shouldBe words.size
    // SET EQUALITY, not a one-way diff. Both sides of this page derive from `SQLKeywords`, so if a
    // whole surface were dropped from the registry the page would regenerate ~200 rows shorter with
    // every containment test still green. Equality here means a lost surface takes its kind label
    // with it, and the floor below means a lost surface cannot hide inside a surviving one.
    KeywordsPage.entries.flatMap(_.kinds).toSet shouldBe Set(
      "clause",
      "operator",
      "literal",
      "statement",
      "type",
      "time unit",
      "reserved only",
      "aggregate function",
      "conditional function",
      "conversion function",
      "geo function",
      "math function",
      "string function",
      "temporal function"
    )
    withClue("the engine recognised 331 keywords when this page was generated: ") {
      KeywordsPage.entries.size should be >= 300
    }
    // The per-row kind list is the one sequence no other assertion orders.
    KeywordsPage.entries.foreach(e => e.kinds shouldBe e.kinds.sorted)
  }

  it should "label every row with the kinds its registry surfaces imply" in {
    // Found by falsification: the kind-VOCABULARY assertion above stays green when a whole surface
    // is relabelled (moving the literal tokens onto "statement" leaves the label set intact,
    // because `literalWords` still contributes "literal"). So the mapping itself is pinned here,
    // in BOTH directions, entirely from the registry - no word list, nothing to hand-maintain.
    //
    // Deliberately NOT pinned, because it would only restate the generator's own derivation: the
    // clause/operator split inside `clauseTokens`, and which function FAMILY a function word lands
    // in. Both are decided by the token's package, and the page says so.
    val byWord: Map[String, Set[String]] =
      KeywordsPage.entries.map(e => e.word -> e.kinds.toSet).toMap

    def exactly(label: String, expected: Set[String]): Unit = {
      val missing = expected.filterNot(w => byWord.get(w).exists(_.contains(label)))
      withClue(s"words that should carry kind '$label' and do not: $missing\n")(
        missing shouldBe empty
      )
      val extra = byWord.collect { case (w, ks) if ks.contains(label) => w }.toSet.diff(expected)
      withClue(s"words carrying kind '$label' that no surface backs: $extra\n")(
        extra shouldBe empty
      )
    }

    exactly("statement", SQLKeywords.statementWords)
    exactly("type", SQLKeywords.typeWords)
    exactly("time unit", SQLKeywords.timeUnitPluralWords)
    exactly(
      "literal",
      SQLKeywords.literalWords ++ SQLKeywords.literalTokens.flatMap(SQLKeywords.wordsOf).toSet
    )

    val functionWords = SQLKeywords.functionTokens.flatMap(SQLKeywords.wordsOf).toSet
    val labelledFunction =
      byWord.collect { case (w, ks) if ks.exists(_.endsWith("function")) => w }.toSet
    withClue("function words with no '... function' kind: ")(
      functionWords.diff(labelledFunction) shouldBe empty
    )
    withClue("rows labelled a function that no function token backs: ")(
      labelledFunction.diff(functionWords) shouldBe empty
    )

    val clauseWords = SQLKeywords.clauseTokens.flatMap(SQLKeywords.wordsOf).toSet
    val labelledClause =
      byWord.collect {
        case (w, ks) if ks.contains("clause") || ks.contains("operator") => w
      }.toSet
    withClue("clause words labelled neither 'clause' nor 'operator': ")(
      clauseWords.diff(labelledClause) shouldBe empty
    )
    withClue("rows labelled clause/operator that no clause token backs: ")(
      labelledClause.diff(clauseWords) shouldBe empty
    )

    exactly("reserved only", KeywordsPage.reservedWords.diff(SQLKeywords.allWords))
  }

  // --- (3) the rendered markdown says what the model says ----------------------------------

  it should "render every row of the model, and only those, in sorted order" in {
    val model = KeywordsPage.entries
    val rendered = renderedRows
    withClue("rows parsed back out of the rendered table: ") {
      rendered.size shouldBe model.size
    }
    rendered.map(_._1) shouldBe model.map(_.word)
    rendered.map(_._1) shouldBe model.map(_.word).sorted
    rendered.map(_._2) shouldBe model.map(_.reservedCell)
    rendered.map(_._3) shouldBe model.map(_.kind)
  }

  it should "render header counts that match the rows it renders" in {
    val counts = KeywordsPage.generatedSection.linesIterator.collect { case countPattern(t, r) =>
      (t.toInt, r.toInt)
    }.toList
    counts.size shouldBe 1
    val rendered = renderedRows
    counts.head._1 shouldBe rendered.size
    counts.head._2 shouldBe rendered.count(_._2 == "yes")
  }

  it should "render every compound phrase the registry knows, and only those" in {
    renderedPhrases shouldBe SQLKeywords.compoundPhrases.toSeq.sorted
  }

  it should "render the symbolic operators the registry cannot carry" in {
    // `::` is a token whose spelling is punctuation, so `wordsOf` filters it out by design and it
    // can never reach the keyword table. It was on the hand-maintained page and must stay.
    KeywordsPage.generatedSection should include("| `::` |")
    Parser("SELECT '125'::BIGINT FROM t").isRight shouldBe true
    Parser("SELECT a::BIGINT FROM t").isRight shouldBe true
  }

  it should "keep `||` off the page for as long as no production accepts it" in {
    // `function.string.Pipe` is a declared token with no production behind it. Listing it would be
    // a wrong claim; dropping it silently would be the defect this page exists to end. So the
    // exclusion is pinned to the measurement: when `||` starts working, this reddens.
    withClue("`||` now parses - add it to KeywordsPage.symbolicTokens and update this test: ") {
      Parser("SELECT a || b FROM t").isLeft shouldBe true
    }
    KeywordsPage.generatedSection should not include "| `||` |"
  }

  // --- (4) the page's claims hold against the parser ---------------------------------------

  it should "agree with the parser about every Reserved cell it prints" in {
    // The RESERVED half of the oracle, recomputed here from the parser and compared against what
    // the PAGE prints, rather than restating the list the generator derived it from.
    //
    // It runs in the ALIAS position, not the select-item position, and that is a measured choice:
    // `SELECT true FROM t` parses and RENDERS as `SELECT true FROM t` whether `true` is a boolean
    // literal or a column, so a round-trip test in the value position cannot tell the two apart and
    // reports `TRUE`/`FALSE` as usable bare names. `AS <word>` goes through `regexAlias`, which is
    // fed by the same `reservedKeywords` list and admits no literal reading, so it discriminates
    // cleanly for every row.
    val wrong = renderedRows.flatMap { case (word, cell, _) =>
      val bare = word.toLowerCase(Locale.ROOT)
      val accepted = Parser(s"SELECT c AS $bare FROM t").isRight
      val agrees = cell match {
        case "yes"                  => !accepted
        case "no" | "no (shadowed)" => accepted
        case _                      => false
      }
      if (agrees) None
      else
        Some(
          s"$word: page says '$cell' but `SELECT c AS $bare FROM t` " +
          s"${if (accepted) "parses" else "is rejected"}"
        )
    }
    withClue(s"rows whose Reserved cell the parser contradicts: ${wrong.mkString("; ")}\n") {
      wrong shouldBe empty
    }
  }

  it should "agree with the parser about every shadowed cell it prints" in {
    // The other half, and the one that can only be answered by EXECUTION: a word that is not
    // reserved and still is not your column. This is what caught CURDATE / CURTIME / NULL / RANDOM.
    // Here the select-item position IS the right one - shadowing is what happens there - and the
    // round-trip is case-sensitive on purpose: `SELECT curdate FROM t` coming back as
    // `SELECT CURRENT_DATE FROM t` is precisely the failure being detected.
    val wrong = renderedRows.filter(_._2.startsWith("no")).flatMap { case (word, cell, _) =>
      val bare = word.toLowerCase(Locale.ROOT)
      val statement = s"SELECT $bare FROM t"
      val readsAsColumn = Parser(statement) match {
        case Right(parsed) => parsed.sql == statement
        case Left(_)       => false
      }
      val agrees = if (cell == "no") readsAsColumn else !readsAsColumn
      if (agrees) None
      else
        Some(
          s"$word: page says '$cell' but `$statement` " +
          s"${if (readsAsColumn) "is the column" else "reads as something else"}"
        )
    }
    withClue(s"rows whose shadowing the parser contradicts: ${wrong.mkString("; ")}\n") {
      wrong shouldBe empty
    }
  }

  it should "accept no reserved word as a bare alias" in {
    // The strong half stated separately, so it cannot be lost inside the cell comparison above.
    val accepted = KeywordsPage.entries
      .filter(_.reserved)
      .map(_.word)
      .filter(w => Parser(s"SELECT c AS ${w.toLowerCase(Locale.ROOT)} FROM t").isRight)
    withClue(s"reserved words still accepted as a bare alias: $accepted\n") {
      accepted shouldBe empty
    }
  }

  // --- the hand-written prose --------------------------------------------------------------

  it should "keep the hand-written prose out of the generated region" in {
    val (before, after) = KeywordsPage.split(content)
    before should include("# Keywords")
    before should include("cannot be used as a bare identifier")
    after should include("[Back to index]")
  }

  it should "not grow a hand-maintained keyword list outside the generated region" in {
    // The page this replaces WAS such a list - one bare keyword per line. If one comes back, the
    // two halves drift again and nothing else here would notice. Both the bare and the backticked
    // spelling count; a keyword inside a sentence does not.
    val (before, after) = KeywordsPage.split(content)
    val strays = (before + after).linesIterator
      // Strip markdown decoration first: a restored list written as `- SELECT`, `**SELECT**`,
      // `` `SELECT` ``, `SELECT,` or `| SELECT |` is the same defect as a bare `SELECT` line, and
      // matching only the bare shape catches a verbatim restoration and essentially nothing else.
      .map(_.trim.replaceAll("^[-*+>|`\\s]+", "").replaceAll("[`*|,.\\s]+$", ""))
      .filter(l => SQLKeywords.allWords.contains(l) || SQLKeywords.compoundPhrases.contains(l))
      .toList
    withClue(
      s"hand-written keyword lines found outside the generated region: $strays\n" +
      s"Keyword lists belong in the generated region only.\n"
    )(strays shouldBe empty)
  }

  it should "hold to what the prose claims about EXISTS, ANY/SOME and quoting" in {
    // Each of these is asserted because the page states it in words. A prose claim nothing executes
    // is how the previous page came to describe a reserved-word list it did not contain.
    Parser("SELECT exists FROM t").isLeft shouldBe true
    Parser("SELECT any, some FROM t WHERE any = 1").isRight shouldBe true
    Parser("SELECT \"exists\" FROM t").isRight shouldBe true
    Parser("SELECT `exists` FROM t").isRight shouldBe true
    // ... and the dotted-name caveat the "Reserved" column description makes.
    Parser("SELECT t.from FROM t").isRight shouldBe true
    Parser("SELECT doc.count FROM t").isRight shouldBe true
    // ... and the hyphen caveat. The reserved-word lookahead ends at a word boundary and `-` is a
    // legal name character, so a bare name that merely STARTS with a reserved word is rejected too
    // - ordinary for Elasticsearch field names, and unguessable from the table alone.
    Parser("SELECT in-stock FROM t").isLeft shouldBe true
    Parser("SELECT foo-bar FROM t").isRight shouldBe true
  }

  it should "hold to what the prose claims about ANY / SOME / ALL normalisation" in {
    // Asserted on the WHOLE rendered statement, never with `include`: `include("IN (SELECT")` is
    // satisfied by `NOT IN (SELECT`, i.e. by the inverse of the claim it is supposed to pin.
    parsed("SELECT id FROM orders WHERE customer_id = ANY (SELECT id FROM customers)") shouldBe
    "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers)"
    parsed("SELECT id FROM orders WHERE customer_id = SOME (SELECT id FROM customers)") shouldBe
    "SELECT id FROM orders WHERE customer_id IN (SELECT id FROM customers)"
    parsed("SELECT id FROM orders WHERE customer_id <> ALL (SELECT id FROM customers)") shouldBe
    "SELECT id FROM orders WHERE customer_id NOT IN (SELECT id FROM customers)"
    // ... while an ordering quantifier keeps its own spelling.
    parsed("SELECT id FROM orders WHERE total > ALL (SELECT amount FROM refunds)") shouldBe
    "SELECT id FROM orders WHERE total > ALL (SELECT amount FROM refunds)"
  }

  it should "hold to what the prose claims about multi-keyword constructions" in {
    // These four are assembled from two separate tokens, so `SQLKeywords.compoundPhrases` cannot
    // see them and the generated phrase table structurally cannot list them. They are in the prose
    // instead, which means the prose is the only thing that can be wrong about them.
    Parser("SELECT a FROM t WHERE a NOT IN (1, 2)").isRight shouldBe true
    Parser("SELECT a FROM t WHERE a NOT BETWEEN 1 AND 2").isRight shouldBe true
    Parser("SELECT a FROM t WHERE MATCH(a) AGAINST ('x')").isRight shouldBe true
    Parser(
      "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY a) FROM t"
    ).isRight shouldBe true
  }

  // --- the migration itself ------------------------------------------------------------------

  it should "account for every entry the hand-maintained page listed" in {
    // A ONE-TIME migration check, not a permanent pin: the set below is the keyword inventory of
    // `documentation/sql/keywords.md` at the revision this change replaces (145 line entries plus
    // `::`). It lives in compiled source, not in a data file, so silencing it means editing code
    // that says what it is. Nothing else in this suite compares the new page to the old one, and
    // without it five constructs vanished from the documentation with every test green.
    //
    // It may SHRINK only when the parser genuinely stops accepting something; it must never be
    // trimmed to make a red go away.
    val previous: Set[String] = Set(
      "::",
      "ABS",
      "ACOS",
      "ALTER",
      "ALWAYS",
      "AND",
      "ARRAY_AGG",
      "AS",
      "ASIN",
      "AT",
      "ATAN",
      "ATAN2",
      "AVG",
      "BETWEEN",
      "CASE",
      "CAST",
      "CEIL",
      "CEILING",
      "COALESCE",
      "CONCAT",
      "CONFLICT",
      "CONVERT",
      "COPY",
      "COS",
      "COUNT",
      "CREATE",
      "CURDATE",
      "CURRENT_DATE",
      "CURRENT_DATETIME",
      "CURRENT_TIME",
      "CURRENT_TIMESTAMP",
      "CURTIME",
      "DATEADD",
      "DATEDIFF",
      "DATESUB",
      "DATETIMEADD",
      "DATETIMESUB",
      "DATETIME_ADD",
      "DATETIME_FORMAT",
      "DATETIME_PARSE",
      "DATETIME_SUB",
      "DATE_ADD",
      "DATE_DIFF",
      "DATE_FORMAT",
      "DATE_PARSE",
      "DATE_SUB",
      "DATE_TRUNC",
      "DELETE",
      "DENSE_RANK",
      "DESCRIBE",
      "DISTANCE",
      "DISTINCT",
      "DO",
      "DROP",
      "ELSE",
      "END",
      "EVERY",
      "EXP",
      "EXTRACT",
      "FIRST_VALUE",
      "FLOOR",
      "FOREACH",
      "FROM",
      "GREATEST",
      "GROUP BY",
      "HAVING",
      "IN",
      "INSERT",
      "INTERVAL",
      "IS NOT NULL",
      "IS NULL",
      "ISNOTNULL",
      "ISNULL",
      "JOIN",
      "LAST_VALUE",
      "LCASE",
      "LEAST",
      "LENGTH",
      "LIKE",
      "LIMIT",
      "LOG",
      "LOG10",
      "LOWER",
      "LTRIM",
      "MATCH ... AGAINST",
      "MAX",
      "MIN",
      "NEVER",
      "NOT",
      "NOT BETWEEN",
      "NOT IN",
      "NOW",
      "NULLIF",
      "NULLS FIRST",
      "NULLS LAST",
      "OFFSET",
      "ON",
      "OR",
      "ORDER BY",
      "OVER",
      "PARTITION BY",
      "PERCENTILE_CONT",
      "PERCENTILE_DISC",
      "POINT",
      "POSITION",
      "POW",
      "POWER",
      "RANK",
      "REGEXP",
      "REGEXP_LIKE",
      "REPLACE",
      "REVERSE",
      "RLIKE",
      "ROUND",
      "ROW_NUMBER",
      "RTRIM",
      "SAFE_CAST",
      "SELECT",
      "SHOW",
      "SIGN",
      "SIN",
      "SQRT",
      "STDDEV",
      "STDDEV_POP",
      "STDDEV_SAMP",
      "ST_DISTANCE",
      "SUBSTR",
      "SUBSTRING",
      "SUM",
      "TAN",
      "THEN",
      "TODAY",
      "TRIM",
      "TRUNCATE",
      "TRY_CAST",
      "UCASE",
      "UNNEST",
      "UPDATE",
      "UPPER",
      "VARIANCE",
      "VAR_POP",
      "VAR_SAMP",
      "WHEN",
      "WHERE",
      "WITHIN",
      "WITHIN GROUP"
    )
    previous.size shouldBe 146

    val text = content
    val rows = KeywordsPage.entries.map(_.word).toSet
    val missing = previous.filterNot { entry =>
      if (entry.matches("[A-Z][A-Z0-9_]*")) rows.contains(entry)
      // Anything else - a phrase, a construction, a symbol - must appear backticked somewhere on
      // the page. Bare containment would be satisfied by an accidental substring.
      else text.contains(s"`$entry`")
    }
    withClue(
      s"entries the hand-maintained page carried that this page dropped: $missing\n" +
      s"Each one is live grammar the user can no longer look up.\n"
    )(missing shouldBe empty)
  }

  // --- the regeneration instruction is real ---------------------------------------------------

  it should "name a regeneration command that build.sbt actually declares" in {
    // The failure message and the page both tell the reader to run this. If the alias is renamed or
    // removed, the instruction becomes a lie that nothing else would catch.
    KeywordsPage.RegenerateCommand should startWith("sbt ")
    val alias = KeywordsPage.RegenerateCommand.stripPrefix("sbt ").trim
    val build = KeywordsPage.read(new File(KeywordsPage.repoRoot, "build.sbt"))
    build should include(s""""$alias"""")
    build should include("app.softnetwork.elastic.sql.doc.KeywordsPageGenerator")
    KeywordsPage.generatedSection should include(KeywordsPage.RegenerateCommand)
    staleClue should include(KeywordsPage.RegenerateCommand)
  }

  private def parsed(sql: String): String = Parser(sql) match {
    // `Either.toOption` is 2.13-only and `.right` is deprecated there, so match instead.
    case Right(statement) => statement.sql
    case Left(error)      => fail(s"expected [$sql] to parse, got: ${error.msg}")
  }
}
