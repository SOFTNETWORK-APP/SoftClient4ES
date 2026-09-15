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

package app.softnetwork.elastic.sql.perf

import app.softnetwork.elastic.sql.parser.Parser
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path, Paths}
import scala.collection.mutable
import scala.jdk.CollectionConverters._

/** Differential probe: feed ONE frozen corpus to the parser built from two different git trees and
  * compare, per input, the VERDICT and the AST RENDER.
  *
  * Why it exists: a branch-only test suite is structurally blind to a narrowing, because the inputs
  * a grammar change breaks are by construction the ones nobody wrote a test for - story 21.7
  * shipped a real narrowing past 912 green tests. The only instrument that can establish "no
  * behaviour change" is a parser built from the OTHER tree, fed the same bytes.
  *
  * ==Two modes, and the corpus is FROZEN on purpose==
  *
  * {{{
  * # 1. once, on either tree - derive the corpus from disk and freeze it
  * sbt "sql/Test/runMain app.softnetwork.elastic.sql.perf.GrammarDiffProbe --emit-corpus corpus.txt"
  *
  * # 2. once per tree - replay the frozen corpus
  * sbt "sql/Test/runMain app.softnetwork.elastic.sql.perf.GrammarDiffProbe corpus.txt a.tsv"
  * git checkout <other-tree> -- sql/src/main
  * sbt "sql/Test/runMain app.softnetwork.elastic.sql.perf.GrammarDiffProbe corpus.txt b.tsv"
  * git checkout HEAD -- sql/src/main
  * diff a.tsv b.tsv
  * }}}
  *
  * Freezing matters: part of the corpus is scraped from the test sources themselves, and those
  * differ between the two trees. A corpus re-derived per run would silently compare two different
  * input sets and report a difference that is an artefact of the harness.
  *
  * 🔴 `git checkout <tree> -- <path>` DESTROYS uncommitted work at that path. Commit first. 🔴
  * Never point this at a `sql/target/<scala>/classes` produced by `clean` + `compile` in ONE sbt
  * invocation: that skips `copyResources`, `softnetwork-sql.conf` is then absent and EVERY `CREATE
  * TABLE` fails to parse - which looks exactly like the narrowing the probe exists to rule out. A
  * plain `sql/Test/runMain` depends on `Test/compile` and is safe.
  *
  * ==Corpus==
  * Five layers, deliberately weighted towards inputs nobody would write a test for:
  *   1. every SQL-shaped cell of every the CSVs under `sql/src/test/resources/corpus` (the BI
  *      census, the Tableau live capture, the pre-epic-21 baseline, the attribution set); 2. every
  *      `examples[].sql` and every `syntax[]` line of the help corpus (under
  *      `core/src/main/resources/help`); 3. every string literal in the `sql` test sources that
  *      starts with a SQL verb - malformed extractions included, since for a DIFFERENTIAL probe any
  *      byte string is a valid input; 4. every whitespace-boundary PREFIX of layers 1-3 (an
  *      unterminated statement is where a changed alternation shows first); 5. targeted mutations -
  *      stray `(` / `)`, dangling `AND` / `OR`, a misspelt keyword, a trailing comma, a doubled
  *      quote.
  *
  * ==Output==
  * One TSV row per input: index, verdict (`right` | `left` | `throw`), and the detail - the AST
  * render (`Statement.sql`) plus its `toString` for a success, the `ParserError` message for a
  * rejection, the throwable class and message for a throw. Tabs, CRs and LFs are escaped so a row
  * is one line. Comparing the two files with `diff` is the whole verdict: any differing line is a
  * behaviour change.
  */
object GrammarDiffProbe {

  private val SqlVerb =
    "(?is)^\\s*(SELECT|WITH|CREATE|ALTER|DROP|INSERT|UPDATE|DELETE|SHOW|TRUNCATE|COPY|REFRESH|EXPLAIN|DESCRIBE|SET|GRANT|REVOKE|CALL|USE)\\b.*"

  private def looksLikeSql(s: String): Boolean =
    s.length > 5 && s.matches(SqlVerb)

  // ---------------------------------------------------------------- corpus layers

  /** RFC-4180: a quoted field may hold commas, CRs and LFs; `""` is a literal quote. */
  private[perf] def parseCsv(content: String): List[List[String]] = {
    val rows = mutable.ListBuffer.empty[List[String]]
    val row = mutable.ListBuffer.empty[String]
    val cell = new StringBuilder
    var inQuotes = false
    var i = 0
    while (i < content.length) {
      val c = content.charAt(i)
      if (inQuotes) {
        if (c == '"') {
          if (i + 1 < content.length && content.charAt(i + 1) == '"') { cell.append('"'); i += 1 }
          else inQuotes = false
        } else cell.append(c)
      } else {
        c match {
          case '"'   => inQuotes = true
          case ','   => row += cell.toString; cell.setLength(0)
          case '\r'  => ()
          case '\n'  => row += cell.toString; cell.setLength(0); rows += row.toList; row.clear()
          case other => cell.append(other)
        }
      }
      i += 1
    }
    if (cell.nonEmpty || row.nonEmpty) { row += cell.toString; rows += row.toList }
    rows.toList
  }

  private def fromCsvCorpora(root: Path): List[String] = {
    val dir = root.resolve("sql/src/test/resources/corpus")
    require(Files.isDirectory(dir), s"GrammarDiffProbe: corpus directory not found at $dir")
    val files = Files
      .list(dir)
      .iterator()
      .asScala
      .filter(_.toString.endsWith(".csv"))
      .toList
      .sortBy(_.toString)
    require(files.nonEmpty, s"GrammarDiffProbe: no CSV under $dir")
    files.flatMap { f =>
      parseCsv(new String(Files.readAllBytes(f), StandardCharsets.UTF_8)).flatten
        .filter(looksLikeSql)
    }
  }

  private def fromHelpCorpus(root: Path): List[String] = {
    val dir = root.resolve("core/src/main/resources/help")
    require(Files.isDirectory(dir), s"GrammarDiffProbe: help corpus not found at $dir")
    val mapper = new ObjectMapper()
    val files = Files
      .walk(dir)
      .iterator()
      .asScala
      .filter(p => Files.isRegularFile(p) && p.toString.endsWith(".json"))
      .toList
      .sortBy(_.toString)
    require(files.nonEmpty, s"GrammarDiffProbe: no help JSON under $dir")
    files.flatMap { f =>
      val node: JsonNode = mapper.readTree(Files.readAllBytes(f))
      val examples =
        Option(node.get("examples")).toList.flatMap(_.iterator().asScala.toList).flatMap { e =>
          Option(e.get("sql")).map(_.asText()).toList
        }
      val syntax =
        Option(node.get("syntax")).toList.flatMap(_.iterator().asScala.toList).map(_.asText())
      (examples ++ syntax).filter(looksLikeSql)
    }
  }

  /** Every double-quoted (or triple-quoted) literal in the sql test sources that starts with a SQL
    * verb. The scanner is deliberately simple; a mis-split literal is still a legitimate probe
    * input, it just is not the statement its author wrote.
    */
  private def fromTestSources(root: Path): List[String] = {
    val dir = root.resolve("sql/src/test/scala")
    require(Files.isDirectory(dir), s"GrammarDiffProbe: test sources not found at $dir")
    val out = mutable.ListBuffer.empty[String]
    Files
      .walk(dir)
      .iterator()
      .asScala
      .filter(p => Files.isRegularFile(p) && p.toString.endsWith(".scala"))
      .toList
      .sortBy(_.toString)
      .foreach { f =>
        val src = new String(Files.readAllBytes(f), StandardCharsets.UTF_8)
        var i = 0
        while (i < src.length) {
          if (src.startsWith("\"\"\"", i)) {
            val end = src.indexOf("\"\"\"", i + 3)
            if (end < 0) i = src.length
            else { out += src.substring(i + 3, end); i = end + 3 }
          } else if (src.charAt(i) == '"') {
            val sb = new StringBuilder
            var j = i + 1
            var closed = false
            while (j < src.length && !closed) {
              val c = src.charAt(j)
              if (c == '\\' && j + 1 < src.length) {
                src.charAt(j + 1) match {
                  case 'n'   => sb.append('\n')
                  case 't'   => sb.append('\t')
                  case 'r'   => sb.append('\r')
                  case other => sb.append(other)
                }
                j += 2
              } else if (c == '"') { closed = true; j += 1 }
              else if (c == '\n') { j = src.length }
              else { sb.append(c); j += 1 }
            }
            out += sb.toString
            i = j
          } else i += 1
        }
      }
    out.toList.map(_.replace("|", "")).filter(looksLikeSql)
  }

  private def prefixes(s: String): List[String] = {
    val out = mutable.ListBuffer.empty[String]
    var i = 1
    while (i < s.length) {
      if (s.charAt(i).isWhitespace && !s.charAt(i - 1).isWhitespace) out += s.substring(0, i)
      i += 1
    }
    out.toList
  }

  private def mutations(s: String): List[String] = List(
    s + " (",
    s + ")",
    s + " AND",
    s + " OR",
    s + ",",
    s + " '",
    "(" + s,
    s.replaceFirst("(?i)\\bWHERE\\b", "WHEREE"),
    s.replaceFirst("(?i)\\bFROM\\b", "FROMM"),
    s.replaceFirst("(?i)\\bSELECT\\b", "SELEC")
  )

  private[perf] def buildCorpus(root: Path): List[String] = {
    val base = (fromCsvCorpora(root) ++ fromHelpCorpus(root) ++ fromTestSources(root))
      .map(_.trim)
      .filter(_.nonEmpty)
      .distinct
    val withPrefixes = base.flatMap(prefixes)
    // Mutating every base statement would be ~10x the base for very little extra discrimination;
    // a deterministic stride keeps the set large without making the run unbounded.
    val mutated = base.zipWithIndex.collect { case (s, i) if i % 3 == 0 => mutations(s) }.flatten
    (base ++ withPrefixes ++ mutated).map(_.trim).filter(_.nonEmpty).distinct
  }

  // ---------------------------------------------------------------- replay

  private def escape(s: String): String =
    s.replace("\\", "\\\\").replace("\t", "\\t").replace("\r", "\\r").replace("\n", "\\n")

  private[perf] def verdictOf(sql: String): String =
    try {
      Parser(sql) match {
        case Right(stmt) =>
          val rendered =
            try escape(stmt.sql)
            catch { case t: Throwable => s"render-threw:${t.getClass.getName}:${t.getMessage}" }
          s"right\t$rendered\t${escape(String.valueOf(stmt))}"
        case Left(err) => s"left\t${escape(String.valueOf(err.msg))}\t"
      }
    } catch {
      case t: Throwable => s"throw\t${t.getClass.getName}\t${escape(String.valueOf(t.getMessage))}"
    }

  def main(args: Array[String]): Unit = {
    val root = Paths.get("").toAbsolutePath
    if (args.length == 2 && args(0) == "--emit-corpus") {
      val corpus = buildCorpus(root)
      Files.write(
        Paths.get(args(1)),
        corpus.map(escape).mkString("\n").getBytes(StandardCharsets.UTF_8)
      )
      println(s"GrammarDiffProbe: wrote ${corpus.size} inputs to ${args(1)}")
      return
    }
    if (args.length != 2) {
      throw new RuntimeException(
        "usage: GrammarDiffProbe --emit-corpus <corpus.txt> | GrammarDiffProbe <corpus.txt> <out.tsv>"
      )
    }
    val corpusPath = Paths.get(args(0))
    if (!Files.isRegularFile(corpusPath)) {
      throw new RuntimeException(
        s"GrammarDiffProbe: corpus not found at ${corpusPath.toAbsolutePath}"
      )
    }
    val inputs = new String(Files.readAllBytes(corpusPath), StandardCharsets.UTF_8)
      .split("\n")
      .toList
      .filter(_.nonEmpty)
      .map(unescape)
    if (inputs.isEmpty) {
      throw new RuntimeException(s"GrammarDiffProbe: corpus at $corpusPath has 0 inputs")
    }
    val sb = new StringBuilder
    inputs.zipWithIndex.foreach { case (sql, i) =>
      sb.append(s"$i\t${escape(sql)}\t${verdictOf(sql)}\n")
    }
    Files.write(Paths.get(args(1)), sb.toString.getBytes(StandardCharsets.UTF_8))
    println(s"GrammarDiffProbe: replayed ${inputs.size} inputs -> ${args(1)}")
  }

  private[perf] def unescape(s: String): String = {
    val sb = new StringBuilder
    var i = 0
    while (i < s.length) {
      if (s.charAt(i) == '\\' && i + 1 < s.length) {
        s.charAt(i + 1) match {
          case 'n'   => sb.append('\n')
          case 't'   => sb.append('\t')
          case 'r'   => sb.append('\r')
          case '\\'  => sb.append('\\')
          case other => sb.append('\\').append(other)
        }
        i += 2
      } else { sb.append(s.charAt(i)); i += 1 }
    }
    sb.toString
  }
}
