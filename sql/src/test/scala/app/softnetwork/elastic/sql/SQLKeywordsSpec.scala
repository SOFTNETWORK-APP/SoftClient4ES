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

package app.softnetwork.elastic.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.io.Source

/** Anti-drift guard for #161: the registry must track every keyword surface of the parser.
  * Source-scan tests locate the sql module sources relative to the working directory (sbt runs
  * tests from the repo root; module-local runs are also handled) and are cancelled (not failed)
  * when the source tree is absent, e.g. when tests run against a published jar.
  */
class SQLKeywordsSpec extends AnyFlatSpec with Matchers {

  // --- source location helpers -------------------------------------------------------

  private val sourceRootCandidates = Seq(
    new File("sql/src/main/scala/app/softnetwork/elastic/sql"),
    new File("src/main/scala/app/softnetwork/elastic/sql")
  )

  private def sourceRoot: Option[File] = sourceRootCandidates.find(_.isDirectory)

  private def scalaFiles(dir: File): Seq[File] = {
    val (dirs, files) =
      Option(dir.listFiles).getOrElse(Array.empty[File]).toSeq.partition(_.isDirectory)
    files.filter(_.getName.endsWith(".scala")) ++ dirs.flatMap(scalaFiles)
  }

  private def read(f: File): String = {
    val src = Source.fromFile(f, "UTF-8")
    try src.mkString
    finally src.close()
  }

  // --- registry invariants -----------------------------------------------------------

  "SQLKeywords" should "expose non-empty, normalized uppercase word sets" in {
    SQLKeywords.allWords should not be empty
    SQLKeywords.highlightedWords should not be empty
    all(SQLKeywords.allWords) should fullyMatch regex "[A-Z][A-Z0-9_]*"
    SQLKeywords.highlightedWords.foreach(_.length should be >= 2)
  }

  it should "split multi-word tokens into their component words" in {
    SQLKeywords.wordsOf(app.softnetwork.elastic.sql.query.OrderBy) shouldBe List("ORDER", "BY")
    SQLKeywords.wordsOf(app.softnetwork.elastic.sql.query.GroupBy) shouldBe List("GROUP", "BY")
    SQLKeywords.wordsOf(app.softnetwork.elastic.sql.operator.UNION) shouldBe List("UNION", "ALL")
    // regex-alternate words: LEFT\s+OUTER | LEFT (From.scala:44-46)
    SQLKeywords.wordsOf(
      app.softnetwork.elastic.sql.query.LeftJoin
    ) should contain allOf ("LEFT", "OUTER")
    // lowercase sql forms are uppercased (Select.scala:118)
    SQLKeywords.wordsOf(app.softnetwork.elastic.sql.query.Except) shouldBe List("EXCEPT")
  }

  it should "expose compound phrases for completer use" in {
    SQLKeywords.compoundPhrases should contain allOf (
      "ORDER BY",
      "GROUP BY",
      "PARTITION BY",
      "UNION ALL",
      "IS NULL",
      "IS NOT NULL",
      "NULLS FIRST",
      "NULLS LAST",
      "LEFT OUTER",
      "RIGHT OUTER",
      "FULL OUTER"
    )
  }

  it should "cover the core clause keywords from issue #161" in {
    SQLKeywords.highlightedWords should contain allOf (
      "LIMIT",
      "ORDER",
      "BY",
      "GROUP",
      "HAVING",
      "OFFSET",
      "AS",
      "ON",
      "DISTINCT",
      "UNION",
      "INTO",
      "VALUES",
      "SET"
    )
  }

  it should "cover pluralised time units (TimeUnit's regex accepts a trailing s)" in {
    SQLKeywords.highlightedWords should contain allOf ("YEARS", "DAYS", "HOURS", "MINUTES")
  }

  it should "never colourise geo distance-unit codes or 1-letter words" in {
    val banned = Set("KM", "CM", "MM", "MI", "YD", "FT", "NMI", "E", "M")
    SQLKeywords.highlightedWords.intersect(banned) shouldBe empty
  }

  // --- anti-drift: keyword("…") literals ---------------------------------------------

  it should "cover every keyword(\"…\") literal used by the parser (anti-drift)" in {
    assume(sourceRoot.isDefined, "sql source tree not on disk - source-scan skipped")
    // \s* tolerates scalafmt-wrapped calls — Parser.scala:832/850 really do contain
    // `(keyword(` with the literal on the next line; without it a wrapped literal
    // would silently escape the scan.
    // digit-tolerant char class: a future keyword("LOG10")-style literal must not escape
    val keywordLiteral = """keyword\(\s*"([A-Za-z_][A-Za-z0-9_]*)"\s*\)""".r
    val found: Set[String] =
      scalaFiles(sourceRoot.get)
        .flatMap(f => keywordLiteral.findAllMatchIn(read(f)).map(_.group(1).toUpperCase))
        .toSet
    found should not be empty
    val missing = found.diff(SQLKeywords.allWords)
    withClue(
      s"parser keyword(...) literals missing from SQLKeywords (add to statementWords): $missing\n"
    ) { missing shouldBe empty }
    // reverse direction: statementWords is documented as an exact curated copy of the
    // keyword("…") literals — a literal removed from the parser must be removed here too,
    // or the registry (and the REPL highlighting derived from it) silently rots.
    val stale = SQLKeywords.statementWords.diff(found)
    withClue(
      s"SQLKeywords.statementWords entries with no keyword(...) literal backing (remove them): $stale\n"
    ) { stale shouldBe empty }
  }

  // --- anti-drift: new Expr("...") keyword tokens -------------------------------------

  it should "cover every word-like Expr token declared in the sql module (anti-drift)" in {
    assume(sourceRoot.isDefined, "sql source tree not on disk - source-scan skipped")
    // NOTE: literal-value tokens declared `extends Value[...] with TokenRegex`
    // (Null, PiValue, RandomValue, EValue, ParamValue, IdValue, IngestTimestampValue)
    // do NOT match this pattern (their `sql` is an override, not an Expr argument) —
    // a NEW token of that shape must be added to SQLKeywords.literalTokens by hand.
    // Tokens that are deliberately NOT keywords (see SQLKeywords scaladoc).
    val excluded: Set[String] = Set(
      // geo distance units (function/geo/package.scala:61-72)
      "KM",
      "M",
      "CM",
      "MM",
      "MI",
      "YD",
      "FT",
      "IN",
      "NMI",
      // parser delimiters (parser/Delimiter.scala:25-34) - lowercase case/when/then/end
      // duplicates of the cond tokens; symbols are filtered by the word pattern anyway
      "CASE",
      "WHEN",
      "THEN",
      "END",
      "E"
    )
    // \s+/\s* tolerate scalafmt wraps around `extends Expr("…")`, mirroring the
    // keyword("…") scan above (AD-7) — a wrapped declaration must not escape the scan.
    val exprDecl = """case object \w+\s+extends\s+Expr\(\s*"([^"]+)"\s*\)""".r
    val declaredWords: Set[String] =
      scalaFiles(sourceRoot.get)
        .flatMap(f => exprDecl.findAllMatchIn(read(f)).map(_.group(1)))
        .map(_.replaceAll("\\\\s\\+", " "))
        .flatMap(_.split("\\s+"))
        .map(_.toUpperCase)
        .filter(_.matches("[A-Z][A-Z0-9_]*"))
        .toSet
    declaredWords should not be empty
    val missing = declaredWords.diff(SQLKeywords.allWords ++ excluded)
    withClue(
      s"Expr(...) token words missing from SQLKeywords (add the token object to " +
      s"clauseTokens/functionTokens/literalTokens, or to the exclusion list above " +
      s"with a rationale): $missing\n"
    ) { missing shouldBe empty }
  }

  // --- anti-drift: Parser.reservedKeywords (read-only tie-in, AD-5) -------------------

  it should "cover Parser.reservedKeywords (scraped - the val is private)" in {
    assume(sourceRoot.isDefined, "sql source tree not on disk - source-scan skipped")
    val parserFile = new File(sourceRoot.get, "parser/Parser.scala")
    assume(parserFile.isFile, "parser/Parser.scala not found - source-scan skipped")
    val text = read(parserFile)
    val start = text.indexOf("reservedKeywords = Seq(")
    start should be >= 0
    val block = text.substring(start, text.indexOf(")", start))
    val entry = """"([a-z_0-9]+)"""".r
    val reserved: Set[String] =
      block.linesIterator
        .filterNot(_.trim.startsWith("//")) // skip commented-out entries
        .flatMap(l => entry.findAllMatchIn(l).map(_.group(1).toUpperCase))
        .toSet
    reserved should not be empty
    // Reserved words with no parseable token/keyword backing at this baseline —
    // reserved-only entries, pinned here; shrink this set, never grow it silently.
    val reservedOnly = Set("FORMAT_DATE", "FORMAT_DATETIME", "CURRENT_DATETIME")
    val missing = reserved.diff(SQLKeywords.allWords ++ reservedOnly)
    withClue(s"Parser.reservedKeywords entries missing from SQLKeywords: $missing\n") {
      missing shouldBe empty
    }
  }
}
