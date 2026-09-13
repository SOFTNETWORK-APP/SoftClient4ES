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
import scala.collection.immutable.ListMap
import scala.io.Source
import scala.util.control.NonFatal

/** Story 21.6 - the Epic 21 scoreboard.
  *
  * Replays the 99 BI-emitted statements captured in Epic 19 (Tableau 2026.2.2 over JDBC, both its
  * MySQL and SQL-92 dialects; Apache Superset 6.0.1 over Flight SQL) against the real `Parser` and
  * asserts every verdict against a DECLARED attribution table. This is a regression test that
  * happens to print a number, not a report: a statement that stops parsing, a policy-pending probe
  * that STARTS parsing, and a temp-table probe scored as a win all redden the build.
  *
  * Unlike `CapturedSqlProbe` (a `main`, because its input is a gitignored capture artefact) every
  * input here is a tracked test resource, so the suite asserts unconditionally and can never pass
  * vacuously.
  *
  * Attribution is declared, never inferred: `Parser.apply` reports ONE combinator failure, and a
  * statement carrying several blockers reports whichever alternative got furthest. Classifying by
  * message is unsound AND unstable across releases - story 20.9 moved 26 rows between message
  * families without changing a single verdict. The message is recorded in the artefact for a human
  * to read and is never asserted.
  */
class CorpusReplaySpec extends AnyFlatSpec with Matchers {

  import CorpusReplay._

  private val corpus: List[CorpusRow] = loadCorpus()
  private val baseline: Map[String, String] = loadBaseline()
  private val attribution: Map[String, Attribution] = loadAttribution()
  private val outcomes: List[Outcome] = replayAll(corpus)
  private val byId: Map[String, Outcome] = outcomes.map(o => o.row.captureId -> o).toMap

  // Emit the artefacts BEFORE any assertion: a failing gate must not leave the operator blind.
  writeArtefacts(outcomes, attribution, baseline)
  println(summaryLine(outcomes, attribution, baseline))

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

  // ---- G5: non-vacuity (assert the inputs are real BEFORE trusting any other gate) -----------

  "the corpus" should "hold exactly the 99 captured statements" in {
    corpus should have size 99
    corpus.map(_.captureId).distinct should have size 99
    corpus.count(_.statement.trim.nonEmpty) shouldBe 99
  }

  it should "carry a baseline and an attribution row for every capture id" in {
    baseline should have size 99
    attribution should have size 99
  }

  it should "exercise every attribution outcome (a gate over an empty set proves nothing)" in {
    val owners = attribution.values.map(_.owner).toSet
    owners should contain("epic21")
    owners should contain("pre_epic21")
    owners should contain("rejected_pending_policy")
    owners should contain("capability_open")
    owners should contain("epic22a_derived_table")
    val scores = attribution.values.map(_.scored).toSet
    scores should contain("fixed")
    scores should contain("pre_epic21")
    scores should contain("residual")
    scores should contain("rejected_pending_policy")
    scores should contain("capability_open")
  }

  // ---- G1: two-way coverage, corpus vs attribution vs baseline ------------------------------

  "the attribution table" should "cover the corpus exactly, in both directions" in {
    val corpusIds = corpus.map(_.captureId).toSet
    withClue("capture ids in the corpus with no attribution row: ") {
      (corpusIds -- attribution.keySet) shouldBe empty
    }
    withClue("attribution rows naming a capture id the corpus does not hold: ") {
      (attribution.keySet -- corpusIds) shouldBe empty
    }
    withClue("capture ids in the corpus with no baseline row: ") {
      (corpusIds -- baseline.keySet) shouldBe empty
    }
    withClue("baseline rows naming a capture id the corpus does not hold: ") {
      (baseline.keySet -- corpusIds) shouldBe empty
    }
  }

  // ---- G3: every owner is drawn from the closed vocabulary, and agrees with `expected` ------

  it should "name a valid owner for every row, with no TBD and no blank" in {
    checkAll(attribution.values.toList.sortBy(_.captureId), "attribution owners")(_.captureId) {
      a =>
        if (!ownerIsValid(a.owner)) {
          sys.error(
            s"owner '${a.owner}' is not in the closed vocabulary ${Owners.toList.sorted
              .mkString(", ")} and does not match issue:<number> or local:<slug>"
          )
        }
        if (a.expected != "parses" && a.expected != "rejected") {
          sys.error(s"expected '${a.expected}' is neither 'parses' nor 'rejected'")
        }
        // `issue:<N>` / `local:<slug>` / `capability_open` imply NOTHING -- see `expectedFor`.
        expectedFor(a.owner).foreach { implied =>
          if (a.expected != implied) {
            sys.error(
              s"expected '${a.expected}' contradicts owner '${a.owner}' (which implies '$implied')"
            )
          }
        }
    }
  }

  // ---- G2: the scoreboard itself ------------------------------------------------------------

  "the replayed corpus" should "match the declared attribution for every statement" in {
    checkAll(corpus, "corpus replay")(_.captureId) { row =>
      // G1 has already reported a missing row; fail loudly here rather than throwing a bare
      // NoSuchElementException that names no capture id.
      val a = attribution.getOrElse(row.captureId, sys.error("no attribution row"))
      val o = byId(row.captureId)
      if (o.verdict != a.expected) {
        sys.error(
          s"expected ${a.expected} (owner ${a.owner}) but measured ${o.verdict}" +
          (if (o.message.nonEmpty) s" -- ${firstLine(o.message)}" else "") +
          s"\n      SQL: ${firstLine(row.statement)}"
        )
      }
    }
  }

  // ---- G7: `scored` agrees with `expected` and `owner` (the PD-3 interlock) -----------------

  it should "score no statement as fixed unless Epic 21 fixed it AND it parses" in {
    checkAll(attribution.values.toList.sortBy(_.captureId), "scored")(_.captureId) { a =>
      if (!Scores.contains(a.scored)) {
        sys.error(s"scored '${a.scored}' is not one of ${Scores.toList.sorted.mkString(", ")}")
      }
      if (a.scored == "fixed" && a.owner != "epic21") {
        sys.error(
          s"scored=fixed requires owner=epic21, not '${a.owner}' -- an issue-owned or " +
          "capability-open row that merely PARSES is not a fix (PD-3)"
        )
      }
      if (a.scored == "fixed" && a.expected != "parses") {
        sys.error("scored=fixed but expected=rejected")
      }
      if ((a.scored == "pre_epic21") != (a.owner == "pre_epic21")) {
        sys.error(s"scored '${a.scored}' and owner '${a.owner}' disagree about pre_epic21")
      }
      if ((a.scored == "rejected_pending_policy") != (a.owner == "rejected_pending_policy")) {
        sys.error(s"scored '${a.scored}' and owner '${a.owner}' disagree about pending policy")
      }
      if ((a.scored == "capability_open") != (a.owner == "capability_open")) {
        sys.error(s"scored '${a.scored}' and owner '${a.owner}' disagree about capability_open")
      }
      // The remaining two implications, stated as BICONDITIONALS on purpose. Story 22.7's review
      // found that adding a value to one vocabulary and not to the other clause set leaves it
      // settable under an owner no gate can see, so every owner here names its score and vice
      // versa. `epic21 <=> fixed` closes the one direction the rest of this test left open: without
      // it an `epic21` row could be `residual`, which cannot INFLATE the headline (it counts
      // `scored`) but can silently DEFLATE it, and a number that is quietly too low is still a
      // number nobody can reproduce.
      if ((a.owner == "epic21") != (a.scored == "fixed")) {
        sys.error(
          s"owner '${a.owner}' and scored '${a.scored}' disagree: owner=epic21 and scored=fixed " +
          "imply each other. A row Epic 21 fixed is scored; a row it did not fix takes an " +
          "epic22*/issue:/local: owner and scores residual."
        )
      }
      if (a.owner.startsWith("epic22") && a.scored != "residual") {
        sys.error(s"owner '${a.owner}' must score residual, not '${a.scored}'")
      }
    }
  }

  // ---- G4: THE SAFETY GATE ------------------------------------------------------------------
  // The id sets are PINNED IN CODE, not read from the table. A gate whose expectations live in the
  // file it guards can be silenced by editing that file; this one cannot.

  it should "hold the temp-table probe invariants (pinned ids, policy rows rejected, none scored)" in {
    val pendingDeclared =
      attribution.values.filter(_.owner == "rejected_pending_policy").map(_.captureId).toSet
    val openDeclared =
      attribution.values.filter(_.owner == "capability_open").map(_.captureId).toSet
    withClue("rows the table calls rejected_pending_policy that the CODE does not: ") {
      (pendingDeclared -- RejectedPendingPolicyIds) shouldBe empty
    }
    withClue("policy-pending probes the CODE pins that the table no longer holds: ") {
      (RejectedPendingPolicyIds -- pendingDeclared) shouldBe empty
    }
    withClue("rows the table calls capability_open that the CODE does not: ") {
      (openDeclared -- CapabilityOpenIds) shouldBe empty
    }
    withClue(
      "capability probes the CODE pins that the table no longer holds -- " +
      "someone re-labelled a capability probe as a fix: "
    ) {
      (CapabilityOpenIds -- openDeclared) shouldBe empty
    }
    val corpusIds = corpus.map(_.captureId).toSet
    withClue("pinned probe ids absent from the corpus: ") {
      (TempTableProbeIds -- corpusIds) shouldBe empty
    }
    TempTableProbeIds should have size 24
    RejectedPendingPolicyIds should have size 3
    CapabilityOpenIds should have size 21
    val pending = corpus.filter(r => RejectedPendingPolicyIds.contains(r.captureId))
    checkAll(pending, "policy-pending DDL probes (must STAY rejected)")(_.captureId) { row =>
      if (byId(row.captureId).verdict == "parses") {
        sys.error(
          "this CREATE LOCAL TEMPORARY TABLE statement now PARSES -- the LOCAL TEMPORARY " +
          "grammar was added without the temp-table product decision. A fix went too far. " +
          "Do NOT resolve this by editing the attribution table.\n" +
          s"      SQL: ${firstLine(row.statement)}"
        )
      }
    }
    val probes = corpus.filter(r => TempTableProbeIds.contains(r.captureId))
    checkAll(probes, "temp-table probes (never scored as wins)")(_.captureId) { row =>
      val a = attributionOf(attribution, row.captureId)
      if (a.scored == "fixed" || a.scored == "pre_epic21") {
        sys.error(
          "this Tableau temp-table capability probe was SCORED as a win. Parsing it (expected " +
          "post-21.7 for the capability_open set) answers Tableau's capability question; " +
          "honouring temp tables is an OPEN product decision tracked outside Epic 21. " +
          "It must never enter the headline numerator.\n" +
          s"      SQL: ${firstLine(row.statement)}"
        )
      }
    }
  }

  // ---- G6: #250 totality, measured over real BI traffic -------------------------------------

  it should "never throw out of Parser.apply" in {
    val threw = outcomes.filter(_.threw)
    withClue(s"statements that threw: ${threw.map(_.row.captureId).mkString(", ")}\n") {
      threw shouldBe empty
    }
  }

  it should "never report an internal parser fault for any captured statement" in {
    // #250's other half: `Parser.apply`'s NonFatal boundary catch turns an AST-surface crash into a
    // `Left` labelled `Internal parser error`. Without this assertion G6 is UNFALSIFIABLE -- a
    // restored crash still comes back as a rejection the attribution table can declare.
    val internal = outcomes.filter(o => o.message.startsWith(Parser.InternalParseFailure))
    withClue(
      "statements whose rejection came from the boundary catch rather than the grammar: " +
      s"${internal.map(o => s"${o.row.captureId} (${firstLine(o.message)})").mkString("; ")}\n"
    ) {
      internal shouldBe empty
    }
  }

  // ---- AC-5: no regression against the pre-epic baseline ------------------------------------

  it should "not regress any statement that parsed before Epic 21" in {
    // The set this gate runs over is PINNED IN CODE and cross-checked against the committed baseline
    // in BOTH directions. Filtering the corpus by the CSV alone would let one cell edit
    // (`parses` -> `rejected`) REMOVE a row from the gate, so a real pre-epic regression would go
    // green. That is the defect the 24 probe ids are pinned to prevent, in the last place it was
    // still open.
    val declaredParsing = baseline.collect { case (id, v) if v == "parses" => id }.toSet
    withClue(
      s"baseline says parses, the CODE does not: ${(declaredParsing -- PreEpicParsesIds).toList.sorted}: "
    ) {
      (declaredParsing -- PreEpicParsesIds) shouldBe empty
    }
    withClue(
      "the CODE pins these as pre-epic and the baseline no longer says parses -- someone edited the " +
      s"baseline to hide a regression: ${(PreEpicParsesIds -- declaredParsing).toList.sorted}: "
    ) {
      (PreEpicParsesIds -- declaredParsing) shouldBe empty
    }
    PreEpicParsesIds should have size 12
    val wereParsing = corpus.filter(r => PreEpicParsesIds.contains(r.captureId))
    withClue("the pinned pre-epic set did not match the corpus - write capture ids in FULL: ") {
      wereParsing should have size 12
    }
    checkAll(wereParsing, "pre-epic parses")(_.captureId) { row =>
      if (byId(row.captureId).verdict != "parses") {
        sys.error(s"parsed before Epic 21, rejected now: ${firstLine(byId(row.captureId).message)}")
      }
    }
    checkAll(wereParsing, "pre-epic render fixed points")(_.captureId) { row =>
      if (!byId(row.captureId).renderFixedPoint.contains(true)) {
        sys.error(
          "parsed before Epic 21 and is no longer a render fixed point: " +
          firstLine(byId(row.captureId).rendered)
        )
      }
    }
  }

  // ---- AD-6: rendered-TEXT fixed point -- BANNER, NOT a gate ---------------------------------
  // Winston: "I would rather have no check than a check people believe." This REPORTS and does not
  // fail. `alert` is available on AnyFlatSpecLike; `println` guarantees the line reaches a plain
  // sbt log. The per-row verdict is in corpus-replay.csv's `render_fixed_point` column; the PR body
  // carries the count.
  //
  // AC-5 above DOES gate the fixed point, but only over the 12 statements that parsed pre-epic --
  // a no-regression claim, not a claim about the newly-accepted ones.

  it should "report (not gate) any accepted statement that is not a render fixed point" in {
    val drifted = outcomes.filter(o => o.verdict == "parses" && o.renderFixedPoint.contains(false))
    val banner =
      if (drifted.isEmpty) "[21.6] render fixed point: clean over all accepted statements."
      else
        s"[21.6] BANNER -- render fixed point lost on ${drifted.size} accepted statement(s). " +
        "The engine re-reads its own render as a DIFFERENT statement, and " +
        "MaterializedViewExtension persists that text (21.2 finding 5). This is DRIFT INSURANCE, " +
        "NOT PROOF -- it cannot see a loss both sides suffer. Rows:\n" +
        drifted.map(o => s"  - ${o.row.captureId}: ${firstLine(o.rendered)}").mkString("\n")
    println(banner)
    alert(banner)
    succeed
  }
}

/** Pure logic, separated from the suite so Epic 23 can call it without ScalaTest. */
object CorpusReplay {

  val CorpusResource = "corpus/epic-19-bi-corpus.csv"
  val BaselineResource = "corpus/baseline-pre-epic21.csv"
  val AttributionResource = "corpus/epic-21-attribution.csv"

  val Owners: Set[String] = Set(
    "epic21",
    "pre_epic21",
    "rejected_pending_policy",
    "capability_open",
    "epic22a_derived_table",
    "epic22b_cte"
  )

  val Scores: Set[String] =
    Set("fixed", "pre_epic21", "residual", "rejected_pending_policy", "capability_open")

  private val IssueOwner = "^issue:[0-9]+$".r
  private val LocalOwner = "^local:[a-z0-9-]+$".r

  def ownerIsValid(owner: String): Boolean =
    Owners.contains(owner) || isIssueOwner(owner) || isLocalOwner(owner)

  def isIssueOwner(owner: String): Boolean = IssueOwner.pattern.matcher(owner).matches()
  def isLocalOwner(owner: String): Boolean = LocalOwner.pattern.matcher(owner).matches()

  /** The parser verdict `owner` IMPLIES, where an implication exists.
    *
    * `issue:<N>` / `local:<slug>` imply NOTHING and return None on purpose: they cover BOTH "still
    * rejected" and PD-3's "parses, but we are not calling that a fix". `capability_open` implies
    * nothing either - those rows legitimately flip to `parses` now that story 21.7's uniform
    * `Parser.ident` quoting has landed, and their invariant is carried by `scored` (G4/G7), never
    * by `expected`. Deriving `expected` from `owner` for EVERY owner is what made PD-3
    * unrepresentable in the first draft of this story.
    */
  def expectedFor(owner: String): Option[String] =
    if (owner == "epic21" || owner == "pre_epic21") Some("parses")
    else if (isIssueOwner(owner) || isLocalOwner(owner) || owner == "capability_open") None
    else Some("rejected")

  /** The 24 Tableau temp-table capability probes, PINNED HERE and not in the CSV.
    *
    * Parsing one of these answers Tableau's "temp tables supported?" probe - honouring temp tables
    * is an OPEN product decision (lead, 2026-09-04), tracked outside Epic 21. Under the lead's
    * uniform DQL/DML/DDL quoting ruling the 21 `CapabilityOpenIds` are EXPECTED to parse now that
    * story 21.7 has landed: they are excluded from the headline numerator on BOTH denominators,
    * never kept as parse errors, and never executed against a cluster (the replay is parse-only by
    * construction - the `sql` module's test classpath carries no Elasticsearch client).
    *
    * The 3 `RejectedPendingPolicyIds` still fail on the absent LOCAL TEMPORARY grammar and must
    * STAY rejected. If you are editing these sets to make a build go green, stop: the build is
    * telling you a probe is being scored as a win, or that a fix went too far.
    */
  val CapabilityOpenIds: Set[String] = Set(
    // CREATE TABLE `#Tableau..._Connect_Chec` (`COL` INTEGER) -- MySQL dialect, backticked; the
    // bare-name twin parses today, so the quoted name was the only blocker
    "tableau.mysql.wx.001",
    "tableau.mysql.wx.005",
    "tableau.mysql.wx.007",
    "tableau.mysql.wx.009",
    "tableau.mysql.wx.011",
    "tableau.mysql.wx.013",
    "tableau.mysql.w1.016",
    "tableau.mysql.w1.025",
    "tableau.mysql.w7.041",
    // DROP TABLE IF EXISTS `#Tableau...`
    "tableau.mysql.wx.002",
    "tableau.mysql.wx.006",
    "tableau.mysql.wx.008",
    "tableau.mysql.wx.010",
    "tableau.mysql.wx.012",
    "tableau.mysql.wx.014",
    "tableau.mysql.w1.017",
    "tableau.mysql.w1.026",
    "tableau.mysql.w7.042",
    // DROP TABLE "XT__..." -- SQL-92, double-quoted; the quoted name is the only blocker
    "tableau.sql92.wx.002",
    "tableau.sql92.wx.006",
    "tableau.sql92.wx.008"
  )

  /** The 12 statements that parsed BEFORE Epic 21, PINNED HERE and not only in the baseline CSV.
    *
    * Same principle as the probe sets above, and it closes the one-sided hole they left: AC-5's
    * no-regression gate filters the corpus by `baseline == "parses"`, so with the expectation
    * living ONLY in an editable file, flipping a baseline cell to `rejected` silently REMOVES that
    * row from the gate -- a genuine pre-epic regression then goes green and the summary prints
    * "(was 11 before Epic 21)" as though the baseline had always said so.
    *
    * Measured 2026-09-13 against the real `Parser` compiled from `ac54a079` (the epic's merge
    * base): 12 parses / 87 rejected / 0 threw. The last seven are story 20.9 / #251's FROM-less
    * SELECT family, which is exactly why the widely-quoted `5/94` baseline is stale -- it was
    * captured 2026-08-31, before that merge, and diffing against it would bank Epic 20's +7 as Epic
    * 21's.
    *
    * This set is HISTORY: it does not move when the parser moves. If a re-measurement of `ac54a079`
    * ever disagrees with it, the re-measurement is wrong or the commit is not `ac54a079`.
    */
  val PreEpicParsesIds: Set[String] = Set(
    "superset.flightsql.w1.001",
    "superset.flightsql.w2.002",
    "superset.flightsql.w3.003",
    "superset.flightsql.w4.004",
    "superset.flightsql.w8.008",
    // the seven below are story 20.9 / #251 (FROM-less SELECT), merged 2026-09-02
    "superset.flightsql.w1.009",
    "superset.flightsql.w1.010",
    "tableau.mysql.wx.004",
    "tableau.mysql.w1.015",
    "tableau.mysql.w7.040",
    "tableau.sql92.wx.004",
    "tableau.sql92.w4.024"
  )

  /** Still rejected on grammar after Epic 21: `LOCAL TEMPORARY` is added by no story. */
  val RejectedPendingPolicyIds: Set[String] = Set(
    // CREATE LOCAL TEMPORARY TABLE "XT__..._CheckCreateTempTableCap" (...) ON COMMIT PRESERVE ROWS
    "tableau.sql92.wx.001",
    "tableau.sql92.wx.005",
    "tableau.sql92.wx.007"
  )

  val TempTableProbeIds: Set[String] = CapabilityOpenIds ++ RejectedPendingPolicyIds

  final case class CorpusRow(
    captureId: String,
    tool: String,
    dialect: String,
    workloadStep: String,
    authorship: String,
    statement: String
  ) {

    /** 30 of the 99 rows have a blank step (the `wx.*` connection probes) - render, do not drop. */
    def step: String = if (workloadStep.trim.isEmpty) "wx" else workloadStep.trim
  }

  final case class Attribution(
    captureId: String,
    expected: String, // what the PARSER does     (G2)
    scored: String, // what the HEADLINE counts (G7) -- see AD-3; NOT derivable from `owner`
    owner: String,
    note: String
  )

  final case class Outcome(
    row: CorpusRow,
    verdict: String, // "parses" | "rejected"
    message: String,
    rendered: String,
    renderFixedPoint: Option[Boolean],
    threw: Boolean
  )

  def firstLine(s: String): String = {
    val one = s.replaceAll("\\s+", " ").trim
    if (one.length <= 160) one else one.substring(0, 157) + "..."
  }

  /** Replay every row with `Console.err` muted.
    *
    * Cheap insurance only: `Parser.apply`'s unconditional `Console.err.println` on every rejection
    * was DELETED by story 21.4 (PR #282), so there is nothing to mute today. Keeping the wrapper
    * costs one object and makes the suite's log immune if such a print ever returns.
    * `Console.withErr` exists in 2.12 and 2.13, is thread-scoped, and restores on exit including on
    * a throw. Every rejection message is captured in `Outcome.message` and written to the artefact
    * regardless.
    */
  def replayAll(rows: List[CorpusRow]): List[Outcome] = {
    val sink = new java.io.PrintStream(new java.io.OutputStream {
      override def write(b: Int): Unit = ()
    })
    try Console.withErr(sink)(rows.map(replay))
    finally sink.close()
  }

  def replay(row: CorpusRow): Outcome =
    try {
      Parser(row.statement) match {
        case Right(stmt) =>
          val rendered = stmt.sql
          // Render-to-render, NOT parse-to-render: ORDER BY legitimately gains an explicit ASC on
          // the first render (21.5 finding 7), so only the second render is a fixed point.
          val fp =
            try Parser(rendered) match {
              case Right(again) => again.sql == rendered
              case Left(_)      => false
            } catch { case NonFatal(_) => false }
          Outcome(row, "parses", "", rendered, Some(fp), threw = false)
        case Left(err) =>
          Outcome(row, "rejected", err.msg, "", None, threw = false)
      }
    } catch {
      case NonFatal(t) =>
        Outcome(row, "rejected", s"${t.getClass.getSimpleName}: ${t.getMessage}", "", None, true)
    }

  // ---- resources ---------------------------------------------------------------------------

  private def resource(name: String): String = {
    val url = Option(getClass.getClassLoader.getResource(name)).getOrElse(
      sys.error(
        s"CorpusReplay: resource '$name' is not on the test classpath (working directory " +
        s"${new File(".").getAbsolutePath})"
      )
    )
    val src = Source.fromURL(url, "UTF-8")
    try src.mkString
    finally src.close()
  }

  private def rows(name: String): List[Map[String, String]] =
    // `CapturedSqlProbe.parseCsv` is `private[census]` and RFC-4180 in both directions: it
    // unescapes the DOUBLED quotes this corpus uses and tolerates the embedded newlines 7 of the 99
    // statements carry (126 physical lines for 99 records). Do not write a second codec.
    CapturedSqlProbe.parseCsv(resource(name)) match {
      case Nil => Nil
      case header :: data =>
        data.map { cells =>
          if (cells.size != header.size) {
            sys.error(s"$name: row with ${cells.size} cells vs ${header.size} headers")
          }
          header.zip(cells).toMap
        }
    }

  def loadCorpus(): List[CorpusRow] =
    rows(CorpusResource).map { m =>
      CorpusRow(
        m.getOrElse("capture_id", ""),
        m.getOrElse("tool", ""),
        m.getOrElse("dialect", ""),
        m.getOrElse("workload_step", ""),
        m.getOrElse("authorship", ""),
        m.getOrElse("captured_statement", "")
      )
    }

  /** `.toMap` silently keeps the LAST of a duplicated key, which would surface only as a size
    * assertion failing by one - a diagnosis nobody reads correctly. Name the duplicate instead.
    */
  private def indexBy[A](pairs: List[(String, A)], what: String): Map[String, A] = {
    val dupes = pairs.map(_._1).groupBy(identity).collect { case (k, vs) if vs.size > 1 => k }
    if (dupes.nonEmpty) {
      sys.error(s"$what: duplicate capture_id(s) ${dupes.toList.sorted.mkString(", ")}")
    }
    pairs.toMap
  }

  def loadBaseline(): Map[String, String] =
    indexBy(
      rows(BaselineResource).map(m => m.getOrElse("capture_id", "") -> m.getOrElse("verdict", "")),
      BaselineResource
    )

  def loadAttribution(): Map[String, Attribution] =
    indexBy(
      rows(AttributionResource).map { m =>
        val a = Attribution(
          m.getOrElse("capture_id", ""),
          m.getOrElse("expected", "").trim,
          m.getOrElse("scored", "").trim,
          m.getOrElse("owner", "").trim,
          m.getOrElse("note", "")
        )
        a.captureId -> a
      },
      AttributionResource
    )

  // ---- scoreboard --------------------------------------------------------------------------

  /** The emitter runs BEFORE any gate (a failing gate must not leave the operator blind), so it
    * must tolerate an attribution table that G1 is about to reject. Never `attribution(id)` here.
    */
  private[census] def attributionOf(m: Map[String, Attribution], id: String): Attribution =
    m.getOrElse(id, Attribution(id, "", "", "", "MISSING attribution row"))

  /** ORDER MATTERS. The temp-table-probe tests come FIRST, so a probe that parses lands in its own
    * bucket instead of being counted as a win in the artefact a human actually reads. (An earlier
    * draft tested `verdict == "parses"` first, so G4 reddened while the scoreboard printed +1 parse
    * and no sign of the probe event.) A parsing `capability_open` probe is EXPECTED post-21.7 and
    * gets a calm bucket; a parsing `rejected_pending_policy` probe is the fix-went-too-far event
    * and gets the loud one. The later test separates PD-3's "parses but is not a fix" from a real
    * fix.
    */
  private[census] def bucket(o: Outcome, a: Attribution): String =
    if (a.owner == "rejected_pending_policy") {
      if (o.verdict == "parses") "UNEXPECTED_parses_pending_policy" else "rejected_pending_policy"
    } else if (a.owner == "capability_open") {
      if (o.verdict == "parses") "parses_capability_open" else "rejected_capability_open"
    } else if (o.verdict == "parses") {
      if (a.scored == "residual") "parses_not_scored" else "parses"
    } else if (a.owner.startsWith("epic22")) "rejected_epic22"
    else "rejected_other"

  private[census] val BucketOrder =
    List(
      "parses",
      "parses_not_scored",
      "parses_capability_open",
      "rejected_capability_open",
      "rejected_pending_policy",
      "rejected_epic22",
      "rejected_other",
      "UNEXPECTED_parses_pending_policy"
    )

  private[census] def tally(
    outcomes: List[Outcome],
    attribution: Map[String, Attribution],
    key: Outcome => String
  ): ListMap[String, ListMap[String, Int]] = {
    val grouped = outcomes.groupBy(key)
    ListMap(grouped.keys.toList.sorted.map { k =>
      val counts =
        grouped(k).groupBy(o => bucket(o, attributionOf(attribution, o.row.captureId))).map {
          case (b, os) => b -> os.size
        }
      k -> ListMap(BucketOrder.map(b => b -> counts.getOrElse(b, 0)): _*)
    }: _*)
  }

  def summaryLine(
    outcomes: List[Outcome],
    attribution: Map[String, Attribution],
    baseline: Map[String, String]
  ): String = {
    val parses = outcomes.count(_.verdict == "parses")
    // Exclude the probes from the BASELINE count too, or the two sides of "N (was B)" are not the
    // same measurement. Honest today only because none of the pinned 24 parsed at `ac54a079`; a
    // future baseline holding a parsing probe would diverge silently and no gate would notice.
    val before =
      baseline.count { case (id, v) => v == "parses" && !TempTableProbeIds.contains(id) }
    val probes = TempTableProbeIds.size
    val intended = outcomes.size - probes
    // PD-3: the PUBLISHED number counts what is SCORED, never the raw parse verdict. A statement
    // that parses and then answers wrong is `scored = residual`, and a temp-table probe is
    // `capability_open` / `rejected_pending_policy` -- none of them may inflate the headline.
    val scoredOk = outcomes.count { o =>
      val s = attributionOf(attribution, o.row.captureId).scored
      o.verdict == "parses" && (s == "fixed" || s == "pre_epic21")
    }
    val probesParsing = outcomes.count { o =>
      o.verdict == "parses" && TempTableProbeIds.contains(o.row.captureId)
    }
    val scoredOfIntended = outcomes.count { o =>
      val a = attributionOf(attribution, o.row.captureId)
      o.verdict == "parses" && (a.scored == "fixed" || a.scored == "pre_epic21") &&
      !TempTableProbeIds.contains(o.row.captureId)
    }
    // Lead ruling 2 (2026-09-13): the verb is SCORES, not "parses". PD-1 mandates that `N` counts
    // `scored` and then writes "parses"; the lead ruled the verb is the side that is wrong, because a
    // statement that parses and is not counted makes "parses N" false on its face. The raw parse count
    // and the difference follow immediately, so neither number can be quoted without the other.
    val gap =
      if (parses == scoredOk) ""
      else
        s" $parses PARSE -- the ${parses - scoredOk}-row difference is never counted " +
        s"($probesParsing capability probes + ${parses - scoredOk - probesParsing} that parse but " +
        "answer wrongly or incompletely, PD-3/G4); see the artefact."
    s"[21.6] corpus replay: SCORES $scoredOk/${outcomes.size} (was $before before Epic 21); " +
    s"$scoredOfIntended/$intended of the statements we intend to answer; " +
    s"$probes capability probes excluded from scoring.$gap"
  }

  /** The house idiom, copied from `DialectCensusSpec` (`repoRootCandidates` + a
    * `sql/src/main/scala` probe). Do NOT anchor on `sql/src/test/resources/corpus` - a directory
    * this very story creates is a fragile anchor, and it resolves differently before and after Task
    * 1.
    */
  private def repoRoot: File =
    Seq(new File("."), new File(".."), new File("../.."))
      .find(r => new File(r, "sql/src/main/scala").isDirectory)
      .getOrElse(new File("."))

  def writeArtefacts(
    outcomes: List[Outcome],
    attribution: Map[String, Attribution],
    baseline: Map[String, String]
  ): Unit = {
    val dir = new File(repoRoot, "sql/target/epic-21")
    dir.mkdirs()

    def cell(s: String) = "\"" + s.replace("\"", "\"\"") + "\""
    val csvHeader = List(
      "capture_id",
      "tool",
      "dialect",
      "workload_step",
      "authorship",
      "baseline",
      "measured",
      "expected",
      "scored",
      "owner",
      "bucket",
      "render_fixed_point",
      "scala_version",
      "error",
      "rendered",
      "statement"
    )
    val csv = (csvHeader.map(cell).mkString(",") :: outcomes.map { o =>
      val a = attributionOf(attribution, o.row.captureId)
      List(
        o.row.captureId,
        o.row.tool,
        o.row.dialect,
        o.row.step,
        o.row.authorship,
        baseline.getOrElse(o.row.captureId, ""),
        o.verdict,
        a.expected,
        a.scored,
        a.owner,
        bucket(o, a),
        o.renderFixedPoint.map(_.toString).getOrElse(""),
        scala.util.Properties.versionNumberString,
        o.message,
        o.rendered,
        o.row.statement
      ).map(cell).mkString(",")
    }).mkString("\n") + "\n"
    Files.write(new File(dir, "corpus-replay.csv").toPath, csv.getBytes(StandardCharsets.UTF_8))

    def table(title: String, t: ListMap[String, ListMap[String, Int]]): String = {
      val head = s"| $title | " + BucketOrder.mkString(" | ") + " | total |"
      val sep = "|---" * (BucketOrder.size + 2) + "|"
      val body = t.map { case (k, counts) =>
        s"| $k | " + BucketOrder.map(counts).mkString(" | ") + s" | ${counts.values.sum} |"
      }
      (head :: sep :: body.toList).mkString("\n")
    }

    val md = new StringBuilder
    md ++= "# Epic 21 - corpus replay scoreboard\n\n"
    md ++= summaryLine(outcomes, attribution, baseline) + "\n\n"
    // Provenance, because `<module>/target/<dir>` is SHARED across the 2.12 and 2.13 legs of
    // `+ sql/test` and the last leg silently wins. Stamp which one produced the file rather than
    // letting a reader assume.
    md ++= s"Produced by `CorpusReplaySpec` on Scala ${scala.util.Properties.versionNumberString}, " +
    s"JVM ${System.getProperty("java.version")}, at ${java.time.Instant.now()}.\n\n"
    md ++= "The verb is **SCORES**, not \"parses\" (lead ruling 2, 2026-09-13): `N` counts what is\n"
    md ++= "SCORED, and a statement that parses without being counted would make \"parses N\" false on\n"
    md ++= "its face. The raw parse count and the difference are stated in the same breath above.\n\n"
    md ++= "Denominators are published in PAIRS. `scored/99` is everything captured; the other is\n"
    md ++= "`99 - 24 temp-table capability probes` (excluded from scoring pending the product\n"
    md ++= "decision), the statements we intend to answer. Neither is quoted alone. Story\n"
    md ++= "attribution is NOT in this table: 21.4 and 21.5 both move it by a\n"
    md ++= "measured 0 because this corpus does not sample their defects.\n\n"
    md ++= "The render fixed point recorded per row is DRIFT INSURANCE, NOT PROOF: it cannot see a\n"
    md ++= "loss both the render and the re-parse suffer.\n\n"
    md ++= table("tool", tally(outcomes, attribution, _.row.tool)) + "\n\n"
    md ++= table("dialect", tally(outcomes, attribution, _.row.dialect)) + "\n\n"
    md ++= table("workload", tally(outcomes, attribution, _.row.step)) + "\n\n"
    md ++= table("authorship", tally(outcomes, attribution, _.row.authorship)) + "\n\n"
    md ++= "The `authorship` split is 19.4 capture metadata, carried here because the lead ruled\n"
    md ++= "(2026-09-13) that the 3 non-`tool` rows stay in both denominators. 🔴 19.4's G9\n"
    md ++= "caution binds any reader: an `analyst` or `tool_wrapping_analyst` row is evidence about\n"
    md ++= "the VENUE - SQL that venue really executed - and NEVER evidence that the tool emits\n"
    md ++= "that shape. `superset.flightsql.w6.006` (the CTE) is `analyst` by design and\n"
    md ++= "`superset.flightsql.w7.007` is `tool_wrapping_analyst`; neither may be cited as\n"
    md ++= "\"Superset emits this\".\n\n"
    md ++= "## Residual rejections, by owner\n\n"
    outcomes
      .filter(_.verdict == "rejected")
      .groupBy(o => attributionOf(attribution, o.row.captureId).owner)
      .toList
      .sortBy(_._1)
      .foreach { case (owner, os) =>
        md ++= s"### $owner (${os.size})\n\n"
        os.sortBy(_.row.captureId).foreach { o =>
          md ++= s"- `${o.row.captureId}` - ${firstLine(o.message)}\n"
        }
        md ++= "\n"
      }
    md ++= "## Error-message families (INFORMATIONAL - never asserted, never tracked)\n\n"
    md ++= "A message names ONE blocker; a statement can carry several, and the family a\n"
    md ++= "rejection lands in changes when an unrelated alternative is added to the grammar.\n"
    md ++= "Story 20.9 moved 26 rows between families without changing a single verdict.\n\n"
    outcomes
      .filter(_.verdict == "rejected")
      .groupBy(o => firstLine(o.message).take(60))
      .toList
      .sortBy(-_._2.size)
      .foreach { case (m, os) => md ++= s"- ${os.size} x `$m`\n" }
    Files.write(
      new File(dir, "corpus-replay.md").toPath,
      md.toString.getBytes(StandardCharsets.UTF_8)
    )
  }
}
