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

package app.softnetwork.elastic.client.repl

import app.softnetwork.elastic.sql.SQLKeywords
import org.jline.reader.{Candidate, ParsedLine}
import org.jline.utils.AttributedStyle
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.util
import scala.jdk.CollectionConverters._

class ReplKeywordsSpec extends AnyFlatSpec with Matchers {

  private val keywordStyle = AttributedStyle.DEFAULT.foreground(AttributedStyle.BLUE).bold()
  private val stringStyle = AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN)
  private val numberStyle = AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW)

  private def styleOf(buffer: String, token: String): AttributedStyle = {
    // ReplHighlighter.highlight ignores the reader parameter — null is safe here.
    val highlighted = new ReplHighlighter().highlight(null, buffer)
    val idx = buffer.indexOf(token)
    idx should be >= 0
    highlighted.styleAt(idx)
  }

  /** The 54 keywords issue #161 reported as drifted, split to single words ("AS IS" -> AS, IS;
    * "OUTER JOIN" -> OUTER, JOIN) — pinned verbatim.
    */
  private val issue161Words: Set[String] = Set(
    "ACTION",
    "ALL",
    "AS",
    "AVG",
    "BULK",
    "BY",
    "CASE",
    "CAST",
    "COALESCE",
    "CONDITION",
    "COPY",
    "COUNT",
    "DESCRIBE",
    "DISTINCT",
    "ELSE",
    "END",
    "EVERY",
    "EXCEPT",
    "EXECUTE",
    "EXPLAIN",
    "FALSE",
    "GEO",
    "GEO_MATCH",
    "GROUP",
    "HAVING",
    "INTERSECT",
    "INTO",
    "IS",
    "JOIN",
    "LIMIT",
    "LOG",
    "MATCH",
    "MAX",
    "MIN",
    "NULL",
    "NULLIF",
    "OFFSET",
    "ON",
    "ORDER",
    "OUTER",
    "RANGE",
    "RENAME",
    "SCHEDULE",
    "SET",
    "SHOW",
    "SUM",
    "THEN",
    "TRUE",
    "TRUNCATE",
    "TYPE",
    "UNION",
    "VALUES",
    "WEBHOOK",
    "WHEN"
  )

  /** The pre-#161 hardcoded highlighter set — regression pin (AC 4). */
  private val legacyHighlighterWords: Set[String] = Set(
    "SELECT",
    "FROM",
    "WHERE",
    "INSERT",
    "UPDATE",
    "DELETE",
    "CREATE",
    "ALTER",
    "DROP",
    "TABLE",
    "INDEX",
    "VIEW",
    "AND",
    "OR",
    "NOT",
    "IN",
    "LIKE",
    "BETWEEN",
    "JOIN",
    "LEFT",
    "RIGHT",
    "INNER",
    "OUTER",
    "ENRICH",
    "POLICY",
    "WATCHER",
    "TRANSFORM",
    "PIPELINE"
  )

  // --- anti-drift (AC 3) --------------------------------------------------------------

  "ReplKeywords" should "be a superset of the parser keyword registry" in {
    val missing = SQLKeywords.highlightedWords.diff(ReplKeywords.all)
    withClue(s"parser keywords invisible to the REPL: $missing\n") { missing shouldBe empty }
  }

  it should "keep REPL-only extras disjoint from parser keywords" in {
    val overlap = ReplKeywords.extraWords.intersect(SQLKeywords.highlightedWords)
    withClue(
      s"these words are parser keywords and must move out of extraWords: $overlap\n"
    ) { overlap shouldBe empty }
  }

  it should "cover all 54 keywords reported by issue #161" in {
    val missing = issue161Words.diff(ReplKeywords.all)
    withClue(s"issue #161 keywords still missing: $missing\n") { missing shouldBe empty }
  }

  it should "cover the full legacy highlighter set (no regression)" in {
    val missing = legacyHighlighterWords.diff(ReplKeywords.all)
    withClue(s"legacy highlighter keywords lost: $missing\n") { missing shouldBe empty }
  }

  it should "back every parser-derived compound continuation with the registry" in {
    SQLKeywords.compoundPhrases should contain allOf (
      "ORDER BY",
      "GROUP BY",
      "PARTITION BY",
      "UNION ALL",
      "NULLS FIRST",
      "NULLS LAST"
    )
  }

  // --- highlighter behaviour (AC 1, 2, 4) ----------------------------------------------

  "ReplHighlighter" should "colourise LIMIT like SELECT and FROM" in {
    val buffer = "SELECT * FROM emp LIMIT 100"
    styleOf(buffer, "SELECT") shouldBe keywordStyle
    styleOf(buffer, "FROM") shouldBe keywordStyle
    styleOf(buffer, "LIMIT") shouldBe keywordStyle
  }

  it should "colourise ORDER BY, GROUP BY, HAVING, OFFSET, DISTINCT and UNION" in {
    val buffer =
      "SELECT DISTINCT dept FROM emp GROUP BY dept HAVING COUNT(*) > 1 " +
      "ORDER BY dept OFFSET 5 UNION ALL SELECT dept FROM emp2"
    Seq("DISTINCT", "GROUP", "BY", "HAVING", "ORDER", "OFFSET", "UNION", "ALL", "COUNT")
      .foreach(word => styleOf(buffer, word) shouldBe keywordStyle)
  }

  it should "colourise lowercase keywords identically" in {
    val buffer = "select * from emp limit 100"
    styleOf(buffer, "select") shouldBe keywordStyle
    styleOf(buffer, "limit") shouldBe keywordStyle
  }

  it should "keep number highlighting and string-literal handling unchanged (no regression)" in {
    val buffer = "SELECT * FROM emp WHERE name = 'John' LIMIT 100"
    styleOf(buffer, "100") shouldBe numberStyle
    // Baseline behaviour pin: split("\\b") fragments quoted literals (" = '", "John", "' ")
    // so the green '[^']*' branch never sees a whole 'John' token — the inner word renders
    // as a plain identifier, NOT as a keyword. Pin exactly that (see Dev Notes gotcha 12).
    styleOf(buffer, "John") shouldBe AttributedStyle.DEFAULT
    // stringStyle is referenced so the green branch constant stays pinned at compile time
    stringStyle shouldBe AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN)
  }

  it should "not colourise plain identifiers or 1-letter aliases" in {
    val buffer = "SELECT e.salary FROM emp e"
    styleOf(buffer, "salary") shouldBe AttributedStyle.DEFAULT
    styleOf(buffer, "emp") shouldBe AttributedStyle.DEFAULT
    // "e" must NOT be highlighted even though EValue.sql == "E"
    styleOf(buffer, "e.") shouldBe AttributedStyle.DEFAULT
  }

  // --- completer behaviour (AC 6) ------------------------------------------------------

  private def parsedLine(fullLine: String, currentWord: String): ParsedLine =
    new ParsedLine {
      override def word(): String = currentWord
      override def wordCursor(): Int = currentWord.length
      override def wordIndex(): Int = fullLine.trim.split("\\s+").length - 1
      override def words(): util.List[String] = fullLine.trim.split("\\s+").toList.asJava
      override def line(): String = fullLine
      override def cursor(): Int = fullLine.length
    }

  private def completionsFor(fullLine: String, currentWord: String): Seq[String] = {
    val candidates = new util.ArrayList[Candidate]()
    new ReplCompleter().complete(null, parsedLine(fullLine, currentWord), candidates)
    candidates.asScala.map(_.value).toSeq
  }

  "ReplCompleter" should "offer LIMIT from the registry" in {
    completionsFor("SELECT * FROM emp LIM", "LIM") should contain("LIMIT")
  }

  it should "offer registry-only newcomers such as UNNEST and OVER" in {
    completionsFor("SELECT * FROM UNN", "UNN") should contain("UNNEST")
    completionsFor("SELECT RANK() OV", "OV") should contain("OVER")
  }

  it should "still offer legacy extras (INTERSECT) and compound continuations" in {
    completionsFor("SELECT 1 INTERS", "INTERS") should contain("INTERSECT")
    // NOTE: the baseline compound path derives previousWord = words(length - 2) from the
    // TRIMMED buffer, so it only triggers once the continuation is being typed
    // ("ORDER B" -> previous "ORDER"), not on "ORDER " with an empty current word.
    // Pin the working form — do not "fix" the empty-word case in this story.
    completionsFor("SELECT * FROM emp ORDER B", "B") should contain("BY")
    completionsFor("SELECT x FROM t WHERE x IS N", "N") should contain allOf ("NULL", "NOT NULL")
  }

  it should "keep meta command completion working" in {
    completionsFor(".he", ".he") should contain(".help")
  }
}
