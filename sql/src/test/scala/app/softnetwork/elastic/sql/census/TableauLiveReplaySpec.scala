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

import app.softnetwork.elastic.sql.parser.Parser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.io.Source
import scala.util.control.NonFatal

/** The SECOND corpus: what a real Tableau Desktop emitted, replayable without Tableau.
  *
  * A live Tableau Desktop 2026 for macOS was driven against a real Elasticsearch 8.18.3 through the
  * JDBC driver, and every statement it sent was captured with p6spy. Those statements existed only
  * as log files on one laptop, so Tableau support could not be regression-tested without Tableau.
  * This suite closes that gap: it replays one verbatim representative of each distinct statement
  * shape through the real `Parser` and asserts every verdict against a DECLARED table.
  *
  * It is a SIBLING of `CorpusReplaySpec`, never a merge into it. The Epic 19 corpus is a fixed
  * baseline that the published `N/99` and `N/75` figures and the Epic 23 series are anchored to;
  * appending to it would silently move denominators that have already been published. This corpus
  * has its own file, its own ids and its own gates, and contributes to no `N/99`.
  *
  * It reuses `CorpusReplay`'s codec (`CapturedSqlProbe.parseCsv`), its replay loop
  * (`CorpusReplay.replayAll`) and its `Outcome`, so there is exactly one definition of "what the
  * parser did with this statement" in the repository.
  *
  * ⚠️ What this corpus is NOT: it is not a baseline, it is not part of any `N/99` denominator, and
  * it is not a substitute for running Tableau. It records what Tableau emitted for THOSE
  * interactions on THAT version, against that fixture. See the corpus README.
  */
class TableauLiveReplaySpec extends AnyFlatSpec with Matchers {

  import TableauLiveReplay._

  private val corpus: List[LiveRow] = loadCorpus()
  private val outcomes: List[Measured] = measureAll(corpus)
  private val byId: Map[String, Measured] = outcomes.map(m => m.row.captureId -> m).toMap

  // Emit the artefact BEFORE any assertion: a failing gate must not leave the operator blind.
  writeArtefact(outcomes)
  println(summaryLine(outcomes))

  /** Report EVERY offending row, not just the first (the `DialectCensusSpec` house idiom). */
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

  // ---- G1: non-vacuity (assert the input is real BEFORE trusting any other gate) -------------

  "the live Tableau corpus" should "hold exactly the captured statement shapes" in {
    corpus should have size ShapeCount.toLong
    corpus.map(_.captureId).distinct should have size ShapeCount.toLong
    corpus.count(_.statement.trim.nonEmpty) shouldBe ShapeCount
  }

  it should "account for every captured statement exactly once" in {
    // The projection is one row per distinct NORMALISED shape, carrying its occurrence count. If
    // the counts no longer sum to what the capture held, the projection lost or double-counted a
    // statement - which a per-row gate cannot see.
    corpus.map(_.occurrences).sum shouldBe CapturedStatementCount
    checkAll(corpus, "occurrence counts")(_.captureId) { r =>
      if (r.occurrences < 1) sys.error(s"occurrences = ${r.occurrences}")
    }
  }

  it should "carry a declared attribution for every row" in {
    checkAll(corpus, "declared attribution")(_.captureId) { r =>
      if (r.expected.isEmpty) sys.error("blank `expected`")
      if (!Verdicts.contains(r.expected)) {
        sys.error(
          s"`expected` = '${r.expected}', not one of ${Verdicts.toList.sorted.mkString(", ")}"
        )
      }
      if (r.owner.isEmpty) sys.error("blank `owner`")
      if (!ownerIsValid(r.owner)) {
        sys.error(
          s"`owner` = '${r.owner}' is outside the closed vocabulary " +
          s"(${Owners.toList.sorted.mkString(", ")}, issue:<N>, local:<slug>)"
        )
      }
      if (r.note.trim.isEmpty) sys.error("blank `note`")
    }
  }

  it should "exercise more than one attribution outcome (a gate over one value proves nothing)" in {
    val owners = corpus.map(_.owner).toSet
    owners.size should be > 1
    owners should contain("works")
    corpus.map(_.expected).toSet shouldBe Verdicts
  }

  // ---- G2: the measured verdict is the declared verdict, for every row ----------------------

  "the measured parse count" should "be the pinned one, whatever the table says" in {
    // The row-by-row gate below compares the measurement against a cell in the file it guards, so
    // on its own a regression could be hidden by editing that cell. This total lives in compiled
    // code: flipping `expected` to absorb a new rejection moves the count and reddens HERE. It is
    // the cheap half of "never let the artefact under test define the expectation that guards it".
    withClue(
      outcomes.filter(_.outcome.verdict == "rejected").map(_.row.captureId).sorted.mkString(", ") +
      " are the rejected rows: "
    ) {
      outcomes.count(_.outcome.verdict == "parses") shouldBe ParsingShapeCount
    }
  }

  "every captured statement" should "parse (or not) exactly as the table declares" in {
    checkAll(corpus, "declared verdict vs measured verdict")(_.captureId) { r =>
      val m = byId(r.captureId)
      if (m.outcome.verdict != r.expected) {
        sys.error(
          s"declared '${r.expected}', measured '${m.outcome.verdict}' " +
          s"[owner ${r.owner}] ${CorpusReplay.firstLine(m.outcome.message)}"
        )
      }
    }
  }

  // ---- G3: the parser never throws and never reports an internal fault ----------------------

  it should "never throw out of Parser.apply" in {
    val threw = outcomes.filter(_.outcome.threw)
    withClue(threw.map(m => s"${m.row.captureId}: ${m.outcome.message}").mkString("\n")) {
      threw shouldBe empty
    }
  }

  it should "never come back labelled an internal parser error" in {
    // Story 21.4 wrapped `Parser.apply` in a `NonFatal` boundary catch, so a crash now arrives as a
    // `Left` that a declared `expected = rejected` would quietly absorb. Without this assertion the
    // "never throws" gate above is unfalsifiable.
    val internal = outcomes.filter(_.outcome.message.startsWith(Parser.InternalParseFailure))
    withClue(internal.map(m => s"${m.row.captureId}: ${m.outcome.message}").mkString("\n")) {
      internal shouldBe empty
    }
  }

  // ---- G4: the three load-bearing id sets are PINNED IN COMPILED CODE -----------------------
  //
  // Never let the artefact under test define the expectation that guards it. Each set below is
  // asserted for set equality in BOTH directions against the CSV, so relabelling a row cannot move
  // it and the failure message says what you are doing.

  private def pinned(owner: String, ids: Set[String], what: String): List[LiveRow] = {
    val declared = corpus.filter(_.owner == owner).map(_.captureId).toSet
    withClue(s"$what: rows declared `$owner` vs the pinned set: ") { declared shouldBe ids }
    ids should not be empty
    corpus.filter(r => ids.contains(r.captureId))
  }

  "the row-existence check Tableau issues per data source" should "keep parsing (issue #328)" in {
    // `SELECT SUM(1) AS `cnt_..._ok` FROM t HAVING (COUNT(1) > 0)` is what broke the Tableau data
    // source preview: `COUNT(<literal>)` emitted a fieldless aggregation, and a whole-table HAVING
    // was silently discarded. Both were fixed on main by PR #327 / issue #328, so on today's tree
    // these must PARSE - that is the regression this suite exists to catch.
    checkAll(pinned("issue:328", Issue328Ids, "issue #328"), "issue #328")(_.captureId) { r =>
      val m = byId(r.captureId)
      if (m.outcome.verdict != "parses") {
        sys.error(
          s"regressed: ${CorpusReplay.firstLine(m.outcome.message)}. Tableau issues this " +
          "statement several times per data source; rejecting it breaks the preview."
        )
      }
    }
  }

  "the temp-table capability probes" should "stay labelled an open product decision" in {
    // MEASURED, and it corrects a natural assumption: these are PLAIN `CREATE TABLE` / `DROP TABLE`
    // in Tableau's MySQL dialect, so issue #326 - which recognises-to-reject `CREATE [LOCAL |
    // GLOBAL] TEMPORARY TABLE` - never reaches them. They parse. Two things must not drift: the
    // LABEL (parsing a probe is not a feature, and whether a cluster should honour it is the lead's
    // open product decision) and the verdict itself, since refusing them would be a product change.
    checkAll(pinned("capability_open", TempTableProbeIds, "temp-table probes"), "probe verdict")(
      _.captureId
    ) { r =>
      val m = byId(r.captureId)
      if (m.outcome.verdict != "parses") {
        sys.error(
          s"now refused: ${CorpusReplay.firstLine(m.outcome.message)}. Tableau emits this pair on " +
          "EVERY connection; refusing it is a product decision, not a side effect."
        )
      }
    }
  }

  "the derived-table shapes" should "keep parsing now that Epic 22 story 22.1 has landed" in {
    // They are 23 of the 127 captured statements, and the connect-time one fires on every single
    // connection. Until PR #331 this asserted they stay REJECTED and owned by `epic22a_derived
    // _table`; 22.1 made them parse, so the gate flips rather than disappears.
    Epic22DerivedTableIds should not be empty
    checkAll(corpus.filter(r => Epic22DerivedTableIds.contains(r.captureId)), "Epic 22")(
      _.captureId
    ) { r =>
      val m = byId(r.captureId)
      if (m.outcome.verdict != "parses") {
        sys.error(
          "a derived table stopped parsing. Story 22.1 (PR #331) made this shape parse, so this " +
          "is a REGRESSION in relational closure, not a corpus bookkeeping problem."
        )
      }
      if (r.owner != "works") {
        sys.error(s"expected `owner` = 'works' since 22.1 landed, found '${r.owner}'")
      }
    }
  }
}

/** Pure half of [[TableauLiveReplaySpec]] - loading, measuring and reporting, with no assertions.
  */
object TableauLiveReplay {

  val Resource = "corpus/tableau-live-2026-09-13.csv"

  /** One row per distinct normalised shape; pinned so a short projection cannot pass quietly. */
  val ShapeCount = 26

  /** Every statement the three p6spy logs held, which the `occurrences` column must account for. */
  val CapturedStatementCount = 127

  /** How many of the shapes parse on this tree, PINNED so no cell edit can absorb a regression. */
  /** 21 -> 24 on 2026-09-14: story 22.1 (PR #331) made the three `epic22a_derived_table` rows
    * parse. This constant lives in compiled code precisely so that flip could not be absorbed by
    * editing the CSV alone.
    */
  val ParsingShapeCount = 24

  val Verdicts: Set[String] = Set("parses", "rejected")

  /** The closed `owner` vocabulary for THIS corpus.
    *
    *   - `works` - parses today, with no known defect behind it.
    *   - `capability_open` - a probe that parses, whose ACCEPTANCE is an open product decision.
    *   - `rejected_by_design` - the rejection is the correct answer, and Tableau reads it as one.
    *
    * `issue:<N>` and `local:<slug>` are accepted in addition, exactly as in `CorpusReplay`, whose
    * predicates are reused so the two corpora cannot drift on what an owner may look like.
    */
  val Owners: Set[String] =
    // Story 22.7 removed `epic22a_derived_table`: Epic 22 SHIPPED, this corpus has zero rows
    // under that owner, and an owner kept alive with no rows is an allow-list nobody exercises
    // -- the same reason `CorpusReplay.Owners` retired it.
    Set("works", "capability_open", "rejected_by_design")

  def ownerIsValid(owner: String): Boolean =
    Owners.contains(owner) || CorpusReplay.isIssueOwner(owner) || CorpusReplay.isLocalOwner(owner)

  /** Tableau's per-data-source row-existence check, fixed by issue #328 / PR #327.
    *
    * PINNED HERE and not in the CSV: a gate whose expectation lives in the file it guards can be
    * silenced by editing that file. These rows must PARSE; a rejection is the regression that broke
    * the data source preview coming back.
    */
  val Issue328Ids: Set[String] = Set(
    "tableau.mysql.live.009",
    "tableau.mysql.live.010",
    "tableau.mysql.live.012",
    "tableau.mysql.live.013",
    "tableau.mysql.live.015"
  )

  /** Tableau's temp-table capability probe pair, emitted on EVERY connection.
    *
    * PINNED HERE for the same reason. Measured: they PARSE - they are plain `CREATE TABLE` / `DROP
    * TABLE` in Tableau's MySQL dialect, which issue #326's `CREATE [LOCAL | GLOBAL] TEMPORARY
    * TABLE` refusal does not cover. Parsing a probe is not a feature; whether a cluster should
    * honour it is the lead's open product decision, so the LABEL is what is guarded.
    */
  val TempTableProbeIds: Set[String] = Set(
    "tableau.mysql.live.001",
    "tableau.mysql.live.002"
  )

  /** A SELECT in FROM position - Epic 22 owns relational closure.
    *
    * PINNED HERE for the same reason. When Epic 22 lands these flip to `parses`, and that must be a
    * deliberate edit to this set AND to the CSV rather than a quiet one.
    *
    * 🔴 **FLIPPED 2026-09-14 — story 22.1 landed (PR #331).** All three rows now PARSE, which is
    * the gate doing its job: Epic 22 succeeding, not a corpus defect. Their CSV `owner` moves to
    * `works` (the closed vocabulary has no "used to be blocked" value) and the ids stay pinned
    * HERE, because these are the highest-frequency statements in the whole capture — `.003` is the
    * connect-time probe Tableau issues on EVERY connection, 17 of 127 captures — and a regression
    * that stopped them parsing must redden by name, not merely move a total.
    *
    * The assertion below therefore flips with them: it no longer says "these stay rejected", it
    * says "these keep parsing". It deliberately does NOT go through `pinned`, whose contract is
    * set-equality against ONE owner label — these rows are now three of the fourteen `works` rows.
    *
    * NOTE they PARSE but do not yet EXECUTE: without the relational engine they are refused with
    * HTTP 400 (story 22.1's loud guard); story 22.4 makes them run.
    */
  val Epic22DerivedTableIds: Set[String] = Set(
    "tableau.mysql.live.003",
    "tableau.mysql.live.005",
    "tableau.mysql.live.020"
  )

  final case class LiveRow(
    captureId: String,
    tool: String,
    dialect: String,
    occurrences: Int,
    expected: String,
    owner: String,
    note: String,
    statement: String
  )

  final case class Measured(row: LiveRow, outcome: CorpusReplay.Outcome)

  // ---- loading -----------------------------------------------------------------------------

  private def resource(name: String): String = {
    val url = Option(getClass.getClassLoader.getResource(name)).getOrElse(
      sys.error(
        s"TableauLiveReplay: resource '$name' is not on the test classpath (working directory " +
        s"${new File(".").getAbsolutePath})"
      )
    )
    val src = Source.fromURL(url, "UTF-8")
    try src.mkString
    finally src.close()
  }

  def loadCorpus(): List[LiveRow] =
    // `CapturedSqlProbe.parseCsv` is `private[census]` and RFC-4180 in both directions: it
    // unescapes the doubled quotes this corpus uses, and it tolerates the TAB characters Tableau
    // emits inside `CREATE TABLE`. Do not write a second codec.
    CapturedSqlProbe.parseCsv(resource(Resource)) match {
      case Nil => Nil
      case header :: data =>
        data.map { cells =>
          if (cells.size != header.size) {
            sys.error(s"$Resource: row with ${cells.size} cells vs ${header.size} headers")
          }
          val m = header.zip(cells).toMap
          val id = m.getOrElse("capture_id", "")
          val occurrences = m.getOrElse("occurrences", "").trim
          LiveRow(
            id,
            m.getOrElse("tool", ""),
            m.getOrElse("dialect", ""),
            try occurrences.toInt
            catch {
              case _: NumberFormatException =>
                sys.error(s"$Resource: $id: `occurrences` = '$occurrences' is not a number")
            },
            m.getOrElse("expected", "").trim,
            m.getOrElse("owner", "").trim,
            m.getOrElse("note", ""),
            m.getOrElse("captured_statement", "")
          )
        }
    }

  // ---- measuring ---------------------------------------------------------------------------

  /** Reuse `CorpusReplay.replayAll` verbatim: one replay loop in the repository, one definition of
    * a verdict, one place where `Console.err` is muted and a throw is turned into an `Outcome`.
    */
  def measureAll(rows: List[LiveRow]): List[Measured] = {
    val asCorpusRows = rows.map { r =>
      CorpusReplay.CorpusRow(r.captureId, r.tool, r.dialect, "live", "tool", r.statement)
    }
    rows.zip(CorpusReplay.replayAll(asCorpusRows)).map { case (r, o) => Measured(r, o) }
  }

  // ---- reporting ---------------------------------------------------------------------------

  def summaryLine(outcomes: List[Measured]): String = {
    val parses = outcomes.count(_.outcome.verdict == "parses")
    val shapes = outcomes.size
    val statements = outcomes.map(_.row.occurrences).sum
    val parsingStatements =
      outcomes.filter(_.outcome.verdict == "parses").map(_.row.occurrences).sum
    s"[tableau-live] $parses/$shapes distinct shapes parse " +
    s"($parsingStatements of $statements captured statements); " +
    "this corpus is a regression record, NOT a baseline and NOT part of any N/99."
  }

  /** The house idiom, copied from `CorpusReplay`. */
  private def repoRoot: File =
    Seq(new File("."), new File(".."), new File("../.."))
      .find(r => new File(r, "sql/src/main/scala").isDirectory)
      .getOrElse(new File("."))

  def writeArtefact(outcomes: List[Measured]): Unit = {
    val dir = new File(repoRoot, "sql/target/tableau-live")
    dir.mkdirs()

    def cell(s: String) = "\"" + s.replace("\"", "\"\"") + "\""
    val header =
      List("capture_id", "occurrences", "expected", "measured", "owner", "message", "statement")
    val csv = (header.map(cell).mkString(",") :: outcomes.map { m =>
      List(
        m.row.captureId,
        m.row.occurrences.toString,
        m.row.expected,
        m.outcome.verdict,
        m.row.owner,
        CorpusReplay.firstLine(m.outcome.message),
        CorpusReplay.firstLine(m.row.statement)
      ).map(cell).mkString(",")
    }).mkString("\n") + "\n"

    Files.write(
      new File(dir, "tableau-live-replay.csv").toPath,
      csv.getBytes(StandardCharsets.UTF_8)
    )
    ()
  }
}
