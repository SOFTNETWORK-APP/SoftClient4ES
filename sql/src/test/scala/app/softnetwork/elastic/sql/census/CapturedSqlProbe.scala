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
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{MultiSearch, SingleSearch, Statement}

import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import scala.collection.mutable.ListBuffer

/** Story 19.4 layer 2 (AD-4/AD-7): the authoritative parse of every captured BI statement.
  *
  * A `main`-bearing object, deliberately NOT a ScalaTest suite: its input is a gitignored capture
  * artefact that does not exist on a clean checkout, so a suite would either pass vacuously
  * (`assume`) or stay permanently red for everyone (unconditional assert). `sbt test` never
  * collects this object; run it with
  *
  * {{{
  * sbt "sql/Test/runMain app.softnetwork.elastic.sql.census.CapturedSqlProbe \
  *       <corpus.csv> <out.csv> [<census.csv>]"
  * }}}
  *
  * When the census path is omitted it is resolved as `epic-19-function-census.csv` beside the
  * corpus. A missing input is a loud failed task naming the absolute path (a thrown exception, not
  * `sys.exit`: this build never forks, so `run` executes in the sbt JVM where `sys.exit` would go
  * through sbt's TrapExit).
  *
  * Per corpus row it emits: the `Parser` verdict (`parses` | `parse_error`) with the verbatim
  * `ParserError.msg`; the debound verdict for `?`-bearing rows that fail (AD-4 branch 1); one
  * sub-row per statement of a multi-statement capture, split the way the product splits (AD-4
  * branch 2, semantics copied from `GatewayApi.splitStatements`); the projected-field function
  * class names; the census ids resolved from the observed spellings (`census_ids_for`, AD-5 - a
  * list, ALL candidates of a token when the form is undecidable, tie-breaker T4); and the
  * `unmatched_spellings` scan backed by the real `SQLKeywords` registry.
  *
  * `Parser.apply` can THROW instead of returning Left (issue #250, three WhereParser sites); every
  * parse here is wrapped in a Throwable catch and recorded as a verdict, never a crash.
  */
object CapturedSqlProbe {

  private val FormKeywords: List[String] =
    List("WITHIN GROUP", "PARTITION BY", "ORDER BY", "FROM", "FOR", "IN")

  final case class CensusRow(
    id: String,
    kind: String,
    token: String,
    spelling: String,
    aliases: List[String],
    arity: String,
    exampleSql: String
  ) {
    def spellingsUpper: List[String] = (spelling :: aliases).map(_.toUpperCase)
  }

  final case class Verdict(
    captureId: String,
    part: String,
    statementPart: String,
    verdict: String,
    error: String,
    verdictDebound: String,
    parsedFunctions: List[String],
    resolvedCensusIds: List[String],
    resolvedTokenOnlyIds: List[String],
    unmatchedSpellings: List[String],
    resolutionReason: String,
    multiStatement: Boolean
  )

  private val outHeader: List[String] = List(
    "capture_id",
    "part",
    "statement_part",
    "verdict",
    "error",
    "verdict_debound",
    "parsed_functions",
    "resolved_census_ids",
    "resolved_token_only_ids",
    "unmatched_spellings",
    "resolution_reason",
    "multi_statement"
  )

  def main(args: Array[String]): Unit = {
    if (args.length < 2) {
      throw new RuntimeException(
        "usage: CapturedSqlProbe <corpus.csv> <out.csv> [<census.csv>]"
      )
    }
    val corpusPath = Paths.get(args(0)).toAbsolutePath
    val outPath = Paths.get(args(1)).toAbsolutePath
    val censusPath =
      (if (args.length > 2) Paths.get(args(2))
       else corpusPath.getParent.resolve("epic-19-function-census.csv")).toAbsolutePath

    if (!Files.isRegularFile(corpusPath)) {
      throw new RuntimeException(s"CapturedSqlProbe: corpus not found at $corpusPath")
    }
    if (!Files.isRegularFile(censusPath)) {
      throw new RuntimeException(s"CapturedSqlProbe: census not found at $censusPath")
    }

    selfCheck()

    val corpus = readCsv(corpusPath.toString)
    val census = readCensus(censusPath.toString)
    val bySpelling: Map[String, List[CensusRow]] =
      census
        .flatMap(r => r.spellingsUpper.map(sp => sp -> r))
        .groupBy(_._1)
        .map { case (k, v) => k -> v.map(_._2) }

    val exclusions: Set[String] = keywordExclusions()

    val verdicts = ListBuffer.empty[Verdict]
    var parses = 0
    var errors = 0
    corpus.foreach { row =>
      val cid = row.getOrElse("capture_id", "")
      val stmt = row.getOrElse("captured_statement", "")
      val spellings = row.getOrElse("spellings", "").split(";").map(_.trim).filter(_.nonEmpty)
      val parts = splitStatements(stmt)
      val multi = parts.size > 1
      val whole =
        probeOne(cid, "", stmt, spellings.toList, bySpelling, exclusions, multi)
      verdicts += whole
      if (whole.verdict == "parses") parses += 1 else errors += 1
      if (multi) {
        parts.zipWithIndex.foreach { case (part, i) =>
          val sub = ('a' + i).toChar.toString
          verdicts += probeOne(cid, sub, part, spellings.toList, bySpelling, exclusions, multi)
        }
      }
    }

    writeCsv(outPath.toString, verdicts.toList)

    println(s"CapturedSqlProbe: read corpus $corpusPath (${corpus.size} rows)")
    println(s"CapturedSqlProbe: read census $censusPath (${census.size} rows)")
    println(
      s"CapturedSqlProbe: wrote $outPath (${verdicts.size} verdict rows: " +
      s"$parses parses, $errors parse_error on whole statements)"
    )
    if (corpus.isEmpty) {
      throw new RuntimeException(
        s"CapturedSqlProbe: corpus at $corpusPath has 0 rows - nothing was probed"
      )
    }
  }

  private def probeOne(
    cid: String,
    part: String,
    stmt: String,
    spellings: List[String],
    bySpelling: Map[String, List[CensusRow]],
    exclusions: Set[String],
    multi: Boolean
  ): Verdict = {
    val (verdict, error, statement) = parse(stmt)
    val parameterised = hasPlaceholder(stmt)
    val debound =
      if (parameterised && verdict == "parse_error") {
        val (v2, e2, _) = parse(debind(stmt))
        if (v2 == "parses") "parses" else s"parse_error: $e2"
      } else ""
    val parsedFunctions = statement.map(functionsOf).getOrElse(Nil)

    val stripped = maskStringLiterals(stripComments(stmt))
    val unmatched = unmatchedSpellings(stripped, bySpelling.keySet, exclusions)

    val (resolved, tokenOnly, reason) =
      if (verdict == "parses") resolveAll(stmt, spellings, bySpelling)
      else (Nil, Nil, "")
    val censusGaps =
      if (verdict == "parses" && unmatched.nonEmpty)
        unmatched.map(u => s"census_gap:$u").mkString(" ")
      else ""
    val fullReason = List(reason, censusGaps).filter(_.nonEmpty).mkString("; ")

    Verdict(
      captureId = cid,
      part = part,
      statementPart = if (part.isEmpty) "" else stmt,
      verdict = verdict,
      error = error,
      verdictDebound = debound,
      parsedFunctions = parsedFunctions,
      resolvedCensusIds = resolved,
      resolvedTokenOnlyIds = tokenOnly,
      unmatchedSpellings = unmatched,
      resolutionReason = fullReason,
      multiStatement = multi
    )
  }

  /** Issue #250: Parser.apply can throw ValidationError instead of returning Left. Catch everything
    * and keep the run alive - a crash here is a verdict, not a failure of the probe.
    */
  private def parse(sql: String): (String, String, Option[Statement]) =
    try {
      Parser(sql) match {
        case Right(stmt) => ("parses", "", Some(stmt))
        case Left(err)   => ("parse_error", err.msg, None)
      }
    } catch {
      case t: Throwable =>
        (
          "parse_error",
          s"Parser threw ${t.getClass.getSimpleName} instead of Left (#250): ${t.getMessage}",
          None
        )
    }

  private def functionsOf(stmt: Statement): List[String] =
    stmt match {
      case s: SingleSearch =>
        s.select.fields.flatMap(_.functions).map(_.getClass.getSimpleName).toList.distinct
      case m: MultiSearch =>
        m.requests
          .flatMap(_.select.fields.flatMap(_.functions))
          .map(_.getClass.getSimpleName)
          .toList
          .distinct
      case _ => Nil
    }

  /** AD-5's census_ids_for: resolve every observed spelling to census rows. Returns (all resolved
    * ids, the subset resolved only at token level, reason-when-empty).
    */
  private def resolveAll(
    stmt: String,
    spellings: List[String],
    bySpelling: Map[String, List[CensusRow]]
  ): (List[String], List[String], String) = {
    if (spellings.isEmpty) {
      return (Nil, Nil, "no census spelling matched (layer 1)")
    }
    val resolved = ListBuffer.empty[String]
    val tokenOnly = ListBuffer.empty[String]
    spellings.foreach { sp =>
      bySpelling.get(sp.toUpperCase) match {
        case None => // spelling from layer 1 must exist in the census; ignore defensively
        case Some(candidates) =>
          val (ids, undecided) = censusIdsFor(sp, stmt, candidates)
          resolved ++= ids
          if (undecided) tokenOnly ++= ids
      }
    }
    (resolved.toList.distinct, tokenOnly.toList.distinct, "")
  }

  /** Resolve one observed spelling against its candidate census rows (one row per FORM). Decidable
    * discriminator: the set of form keywords (FROM/FOR/IN/WITHIN GROUP/ORDER BY/ PARTITION BY)
    * present inside the observed call's parentheses, compared against the same set computed from
    * each candidate's own example_sql. Exactly one candidate agreeing -> form-level. Anything else
    * -> ALL candidates, token-only (T4: inflating is the safe error). Returns (ids,
    * undecidedAtTokenLevel).
    */
  private[census] def censusIdsFor(
    spelling: String,
    stmt: String,
    candidates: List[CensusRow]
  ): (List[String], Boolean) = {
    if (candidates.size == 1) return (candidates.map(_.id), false)
    val observedArgs = callArgs(stmt, spelling)
    observedArgs match {
      case None => (candidates.map(_.id), true)
      case Some(args) =>
        val observedSet = formKeywordSet(args)
        val agreeing = candidates.filter { c =>
          callArgs(c.exampleSql, c.spelling)
            .map(formKeywordSet)
            .contains(observedSet)
        }
        agreeing match {
          case one :: Nil => (List(one.id), false)
          case Nil        => (candidates.map(_.id), true)
          case several    => (several.map(_.id), true)
        }
    }
  }

  private def formKeywordSet(argsUpper: String): Set[String] = {
    val padded = " " + argsUpper.toUpperCase + " "
    FormKeywords.filter { kw =>
      val pattern = ("(?i)(?<![A-Za-z0-9_])" + kw.replace(" ", "\\s+") + "(?![A-Za-z0-9_])").r
      pattern.findFirstIn(padded).isDefined
    }.toSet
  }

  /** Extract the argument text of the FIRST call of `spelling` in `sql`, parenthesis-balanced
    * (balancing IS the fix - project_function_paren_greed). None when no call is found.
    */
  private[census] def callArgs(sql: String, spelling: String): Option[String] = {
    val pattern = ("(?i)(?<![A-Za-z0-9_])" + java.util.regex.Pattern.quote(spelling) + "\\s*\\(").r
    pattern.findFirstMatchIn(sql).map { m =>
      val start = m.end // just past the opening paren
      var depth = 1
      var i = start
      while (i < sql.length && depth > 0) {
        val c = sql.charAt(i)
        if (c == '(') depth += 1
        else if (c == ')') depth -= 1
        i += 1
      }
      sql.substring(start, if (depth == 0) i - 1 else sql.length)
    }
  }

  /** unmatched_spellings (corpus schema): call-shaped names in the comment-stripped, string-masked
    * statement that are (a) not census spellings/aliases, (b) not registry clause/statement/literal
    * words, and (c) not preceded by FROM/JOIN (table functions).
    */
  private def unmatchedSpellings(
    strippedMasked: String,
    censusSpellings: Set[String],
    exclusions: Set[String]
  ): List[String] = {
    val call = "(?i)(?<![A-Za-z0-9_])([A-Za-z_][A-Za-z0-9_]*)\\s*\\(".r
    val out = ListBuffer.empty[String]
    call.findAllMatchIn(strippedMasked).foreach { m =>
      val name = m.group(1).toUpperCase
      val before = strippedMasked.substring(0, m.start).trim
      val prevWord = before.split("[^A-Za-z0-9_]+").lastOption.getOrElse("").toUpperCase
      val tableFn = prevWord == "FROM" || prevWord == "JOIN"
      if (!censusSpellings.contains(name) && !exclusions.contains(name) && !tableFn) {
        out += name
      }
    }
    out.toList.distinct
  }

  private def keywordExclusions(): Set[String] = {
    val clauseWords = SQLKeywords.clauseTokens.flatMap(SQLKeywords.wordsOf).map(_.toUpperCase)
    (clauseWords.toSet ++ SQLKeywords.statementWords.map(_.toUpperCase)
    ++ SQLKeywords.literalWords.map(_.toUpperCase) ++ Set("VALUES", "CAST", "IF"))
  }

  /** Statement split with the product's semantics - copied from GatewayApi.splitStatements (core
    * module, not on the sql classpath; ~30 lines duplicated deliberately, Rule of Three).
    */
  private[census] def splitStatements(sql: String): List[String] = {
    val statements = ListBuffer.empty[String]
    val current = new StringBuilder
    var quote: Char = 0
    var i = 0
    while (i < sql.length) {
      val c = sql.charAt(i)
      if (quote != 0) {
        current.append(c)
        if (c == '\\' && i + 1 < sql.length) {
          current.append(sql.charAt(i + 1))
          i += 1
        } else if (c == quote) {
          quote = 0
        }
      } else if (c == '-' && i + 1 < sql.length && sql.charAt(i + 1) == '-') {
        while (i < sql.length && sql.charAt(i) != '\n') {
          i += 1
        }
        current.append(' ')
      } else {
        c match {
          case '\'' | '"' =>
            quote = c
            current.append(c)
          case ';' =>
            statements += current.toString
            current.clear()
          case _ =>
            current.append(c)
        }
      }
      i += 1
    }
    statements += current.toString
    statements.toList.map(_.trim).filter(_.nonEmpty)
  }

  private[census] def hasPlaceholder(sql: String): Boolean = {
    var quote: Char = 0
    var i = 0
    while (i < sql.length) {
      val c = sql.charAt(i)
      if (quote != 0) {
        if (c == '\\' && i + 1 < sql.length) i += 1
        else if (c == quote) quote = 0
      } else if (c == '\'' || c == '"') {
        quote = c
      } else if (c == '?') {
        return true
      }
      i += 1
    }
    false
  }

  /** AD-4 branch 1: substitute a neutral literal for every placeholder, quote-aware. */
  private[census] def debind(sql: String): String = {
    val out = new StringBuilder
    var quote: Char = 0
    var i = 0
    while (i < sql.length) {
      val c = sql.charAt(i)
      if (quote != 0) {
        out.append(c)
        if (c == '\\' && i + 1 < sql.length) {
          out.append(sql.charAt(i + 1))
          i += 1
        } else if (c == quote) quote = 0
      } else if (c == '\'' || c == '"') {
        quote = c
        out.append(c)
      } else if (c == '?') {
        out.append('0')
      } else {
        out.append(c)
      }
      i += 1
    }
    out.toString
  }

  private[census] def stripComments(sql: String): String = {
    val out = sql.toCharArray
    var quote: Char = 0
    var i = 0
    val n = sql.length
    while (i < n) {
      val c = sql.charAt(i)
      if (quote != 0) {
        if (c == '\\' && i + 1 < n) i += 1
        else if (c == quote) quote = 0
        i += 1
      } else if (c == '\'' || c == '"') {
        quote = c
        i += 1
      } else if (c == '-' && i + 1 < n && sql.charAt(i + 1) == '-') {
        while (i < n && sql.charAt(i) != '\n') {
          out(i) = ' '
          i += 1
        }
      } else if (c == '/' && i + 1 < n && sql.charAt(i + 1) == '*') {
        out(i) = ' '
        out(i + 1) = ' '
        i += 2
        while (i < n && !(sql.charAt(i) == '*' && i + 1 < n && sql.charAt(i + 1) == '/')) {
          out(i) = ' '
          i += 1
        }
        if (i < n) {
          out(i) = ' '
          if (i + 1 < n) out(i + 1) = ' '
          i += 2
        }
      } else {
        i += 1
      }
    }
    new String(out)
  }

  private[census] def maskStringLiterals(sql: String): String = {
    val out = sql.toCharArray
    var quote: Char = 0
    var i = 0
    val n = sql.length
    while (i < n) {
      val c = sql.charAt(i)
      if (quote != 0) {
        if (c == '\\' && i + 1 < n) {
          out(i) = ' '
          out(i + 1) = ' '
          i += 2
        } else if (c == quote) {
          quote = 0
          i += 1
        } else {
          out(i) = ' '
          i += 1
        }
      } else {
        if (c == '\'' || c == '"') quote = c
        i += 1
      }
    }
    new String(out)
  }

  // --------------------------------------------------------------------------
  // CSV: RFC-4180 both directions. The reader accepts quoted cells containing
  // commas, doubled quotes and embedded newlines (non-JDBC seams keep their
  // line breaks); the writer quotes EVERY cell unconditionally - copied from
  // 19.1's CensusEmitter, "do NOT optimise this to quote-only-when-needed".
  // --------------------------------------------------------------------------

  private[census] def parseCsv(content: String): List[List[String]] = {
    val rows = ListBuffer.empty[List[String]]
    val row = ListBuffer.empty[String]
    val cell = new StringBuilder
    var inQuotes = false
    var i = 0
    val n = content.length
    def endCell(): Unit = { row += cell.toString; cell.clear() }
    def endRow(): Unit = { endCell(); rows += row.toList; row.clear() }
    while (i < n) {
      val c = content.charAt(i)
      if (inQuotes) {
        if (c == '"') {
          if (i + 1 < n && content.charAt(i + 1) == '"') {
            cell.append('"')
            i += 1
          } else {
            inQuotes = false
          }
        } else {
          cell.append(c)
        }
      } else {
        c match {
          case '"'  => inQuotes = true
          case ','  => endCell()
          case '\r' => // swallow; \n handles the row break
          case '\n' => endRow()
          case _    => cell.append(c)
        }
      }
      i += 1
    }
    if (cell.nonEmpty || row.nonEmpty) endRow()
    rows.toList.filter(r => !(r.size == 1 && r.head.isEmpty))
  }

  private def readCsv(path: String): List[Map[String, String]] = {
    val content = new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
    parseCsv(content) match {
      case Nil => Nil
      case header :: data =>
        data.map { cells =>
          if (cells.size != header.size) {
            throw new RuntimeException(
              s"CapturedSqlProbe: $path: row with ${cells.size} cells vs ${header.size} headers " +
              s"(starts: ${cells.headOption.getOrElse("")})"
            )
          }
          header.zip(cells).toMap
        }
    }
  }

  private def readCensus(path: String): List[CensusRow] =
    readCsv(path).map { m =>
      CensusRow(
        id = m.getOrElse("id", ""),
        kind = m.getOrElse("kind", ""),
        token = m.getOrElse("token", ""),
        spelling = m.getOrElse("spelling", ""),
        aliases = m.getOrElse("aliases", "").split("\\s+").toList.filter(_.nonEmpty),
        arity = m.getOrElse("arity", ""),
        exampleSql = m.getOrElse("example_sql", "")
      )
    }

  private def csvCell(s: String): String = "\"" + s.replace("\"", "\"\"") + "\""

  private def writeCsv(path: String, verdicts: List[Verdict]): Unit = {
    val target = Paths.get(path)
    if (target.getParent != null) Files.createDirectories(target.getParent)
    val lines = outHeader.map(csvCell).mkString(",") :: verdicts.map { v =>
      List(
        v.captureId,
        v.part,
        v.statementPart,
        v.verdict,
        v.error,
        v.verdictDebound,
        v.parsedFunctions.mkString(";"),
        v.resolvedCensusIds.mkString(";"),
        v.resolvedTokenOnlyIds.mkString(";"),
        v.unmatchedSpellings.mkString(";"),
        v.resolutionReason,
        if (v.multiStatement) "true" else "false"
      ).map(csvCell).mkString(",")
    }
    Files.write(target, (lines.mkString("\n") + "\n").getBytes(StandardCharsets.UTF_8))
  }

  /** Deterministic resolver/scanner assertions on synthetic inputs, run before every probe so a
    * broken discriminator fails loudly instead of writing plausible wrong resolutions.
    */
  private def selfCheck(): Unit = {
    val fromFor = CensusRow(
      id = "syn.substring.from-for",
      kind = "function",
      token = "SUBSTRING",
      spelling = "SUBSTRING",
      aliases = Nil,
      arity = "3",
      exampleSql = "SELECT SUBSTRING(name FROM 1 FOR 3) FROM emp"
    )
    val comma = fromFor.copy(
      id = "syn.substring.comma",
      exampleSql = "SELECT SUBSTRING(name, 1, 3) FROM emp"
    )
    val cands = List(fromFor, comma)
    val r1 = censusIdsFor("SUBSTRING", "SELECT SUBSTRING(x FROM 2 FOR 5) FROM t", cands)
    require(
      r1 == ((List("syn.substring.from-for"), false)),
      s"selfCheck: FROM/FOR form resolution failed: $r1"
    )
    val r2 = censusIdsFor("SUBSTRING", "SELECT SUBSTRING(x, 2, 5) FROM t", cands)
    require(
      r2 == ((List("syn.substring.comma"), false)),
      s"selfCheck: comma form resolution failed: $r2"
    )
    val r3 = censusIdsFor("SUBSTRING", "SELECT LENGTH(x) FROM t", cands)
    require(r3._2, s"selfCheck: no-call resolution must be token-only: $r3")

    require(splitStatements("a; b").size == 2, "selfCheck: splitStatements")
    require(
      splitStatements("SELECT ';' FROM t").size == 1,
      "selfCheck: splitStatements must be quote-aware"
    )
    require(hasPlaceholder("SELECT * FROM t WHERE a = ?"), "selfCheck: hasPlaceholder")
    require(!hasPlaceholder("SELECT '?' FROM t"), "selfCheck: '?' in a literal is not a param")
    require(
      debind(
        "SELECT * FROM t WHERE a = ? AND b = '?'"
      ) == "SELECT * FROM t WHERE a = 0 AND b = '?'",
      "selfCheck: debind"
    )
    require(
      stripComments("SELECT a -- c\nFROM t").contains("SELECT a"),
      "selfCheck: stripComments keeps code"
    )
    require(
      !stripComments("SELECT a /* UPPER(x) */ FROM t").contains("UPPER"),
      "selfCheck: stripComments removes block comments"
    )
    require(
      stripComments("SELECT '--x' FROM t").contains("'--x'"),
      "selfCheck: stripComments is quote-aware"
    )
    val csv = parseCsv("\"a\",\"b\"\r\n\"1,x\",\"say \"\"hi\"\"\nline2\"\r\n")
    require(
      csv == List(List("a", "b"), List("1,x", "say \"hi\"\nline2")),
      s"selfCheck: parseCsv: $csv"
    )
    println("CapturedSqlProbe: selfCheck OK (resolver, splitter, codec)")
  }
}
