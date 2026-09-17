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

import app.softnetwork.elastic.sql.parser.{DerivedTableCorpusSpec, Parser}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.collection.immutable.ListMap
import scala.io.Source
import scala.util.control.NonFatal

/** Stories 21.6 and 22.7 - the corpus scoreboard.
  *
  * Story 21.6 built it for Epic 21; story 22.7 extended it (it did not fork it) into a SERIES that
  * spans epics: a second baseline (`baseline-pre-epic22.csv`), the `epic22` / `rejected_by_design`
  * owners, the code-pinned Epic-22 scoring partition, and `series.csv` with one row per measured
  * tree. Epic 23 appends a row; it does not write a second scoreboard.
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
  private val baseline: Map[String, String] = loadBaseline(BaselinePre21)
  private val pre22: Map[String, String] = loadBaseline(BaselinePre22)
  private val attribution: Map[String, Attribution] = loadAttribution()
  private val series: List[SeriesRow] = loadSeries()
  private val outcomes: List[Outcome] = replayAll(corpus)
  private val byId: Map[String, Outcome] = outcomes.map(o => o.row.captureId -> o).toMap

  // Emit the artefacts BEFORE any assertion: a failing gate must not leave the operator blind.
  writeArtefacts(outcomes, attribution, baseline, pre22, series)
  println(summaryLine(outcomes, attribution, series))

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

  it should "carry both baselines and an attribution row for every capture id" in {
    baseline should have size 99
    pre22 should have size 99
    attribution should have size 99
  }

  it should "exercise every attribution outcome (a gate over an empty set proves nothing)" in {
    val owners = attribution.values.map(_.owner).toSet
    owners should contain("epic21")
    owners should contain("pre_epic21")
    owners should contain("rejected_pending_policy")
    owners should contain("capability_open")
    // Story 22.7 RETARGETED this line from `epic22a_derived_table`, which now owns no row. A gate
    // that never observes a vocabulary member does not guard it.
    owners should contain("epic22")
    owners should contain("rejected_by_design")
    val scores = attribution.values.map(_.scored).toSet
    scores should contain("fixed")
    scores should contain("pre_epic21")
    scores should contain("residual")
    scores should contain("rejected_pending_policy")
    scores should contain("capability_open")
    scores should contain("rejected_by_design")
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
    withClue("capture ids in the corpus with no pre-epic22 baseline row: ") {
      (corpusIds -- pre22.keySet) shouldBe empty
    }
    withClue("pre-epic22 baseline rows naming a capture id the corpus does not hold: ") {
      (pre22.keySet -- corpusIds) shouldBe empty
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
      // Story 22.7 widened this from `owner == "epic21"` to the two SHIPPED epics. It stays an
      // IMPLICATION, not a biconditional: `epic22` legitimately owns a row that parses and is NOT
      // scored (see `Epic22UnmeasuredIds`), which is the state PD-3 exists to make representable.
      if (a.scored == "fixed" && !Set("epic21", "epic22").contains(a.owner)) {
        sys.error(
          s"scored=fixed requires owner=epic21 or owner=epic22, not '${a.owner}' -- an " +
          "issue-owned or capability-open row that merely PARSES is not a fix (PD-3)"
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
      // Story 22.7 added the sixth member to BOTH vocabularies, so it gets its biconditional in the
      // SAME commit. Without it a row could be `scored = rejected_by_design` under owner `epic21`,
      // where G4b -- which filters on the OWNER -- would never see it. State it as the rule: every
      // value added to `Scores` needs its biconditional here.
      if ((a.scored == "rejected_by_design") != (a.owner == "rejected_by_design")) {
        sys.error(
          s"scored '${a.scored}' and owner '${a.owner}' disagree about rejected_by_design"
        )
      }
      // The `epic21` half of 21.6's biconditional, kept as an IMPLICATION. Its reverse direction is
      // now carried by the widened `fixed => owner in {epic21, epic22}` clause above. Dropping it
      // would let an `epic21` row score `residual`, which cannot INFLATE the headline but can
      // silently DEFLATE it, and a number that is quietly too low is still irreproducible.
      if (a.owner == "epic21" && a.scored != "fixed") {
        sys.error(
          s"owner=epic21 must score fixed, not '${a.scored}'. A row Epic 21 fixed is scored; a row " +
          "it did not fix takes an epic22/issue:/local: owner."
        )
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
    // Stories 22.1 / 22.5 — the SHAPE partition of the twelve Epic-22 statements, pinned in code.
    // Story 22.7 kept the VERDICT pins here (the safety half) and moved the owner/score checks into
    // the scoring gate below, because the two owners these were keyed on (`epic22a_derived_table`,
    // `epic22b_cte`) are retired with the epic.
    DerivedTableParsesIds should have size 9
    DerivedTableRejectedIds should have size 2
    CteParsesIds should have size 1
    Epic22Ids should have size 12
    checkAll(
      Epic22Ids.toList.sorted,
      "Epic 22 verdicts (pinned in code, never in the table)"
    )(identity) { id =>
      val want = if (DerivedTableRejectedIds.contains(id)) "rejected" else "parses"
      if (byId(id).verdict != want) {
        sys.error(s"expected $want, measured ${byId(id).verdict}")
      }
    }
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

  // ---- G4b: rejected BY DESIGN — pinned in code, must STAY rejected (story 22.7) -------------

  it should "keep the by-design rejections rejected (pinned ids, never the table)" in {
    val declared =
      attribution.values.filter(_.owner == "rejected_by_design").map(_.captureId).toSet
    withClue("rows the table calls rejected_by_design that the CODE does not: ") {
      (declared -- RejectedByDesignIds) shouldBe empty
    }
    withClue("by-design ids the CODE pins that the table no longer holds: ") {
      (RejectedByDesignIds -- declared) shouldBe empty
    }
    RejectedByDesignIds should have size 1
    checkAll(
      corpus.filter(r => RejectedByDesignIds.contains(r.captureId)),
      "rejected-by-design rows"
    )(_.captureId) { row =>
      if (byId(row.captureId).verdict == "parses") {
        sys.error(
          "this statement now PARSES: a derived table with NO CORRELATION NAME was accepted " +
          "without the product decision (SQL-92 clause 7.6, story 22.1 PD-1). Note what that " +
          "costs here: ROWNUM is not a blocker -- it resolves as an ordinary identifier -- so this " +
          "statement would now range-filter a field that does not exist and return ZERO rows with " +
          "HTTP 200. Do NOT resolve this by editing the attribution table.\n" +
          s"      SQL: ${firstLine(row.statement)}"
        )
      }
    }
  }

  // ---- G4c: the Epic-22 SCORING partition — pinned in code, checked both ways (story 22.7) ---

  it should "partition the twelve Epic 22 rows by score, exactly, in code" in {
    val parts = List(
      ("epic22/fixed", Epic22FixedIds, "epic22", "fixed"),
      ("epic22/residual (UNMEASURED)", Epic22UnmeasuredIds, "epic22", "residual"),
      ("rejected_by_design", RejectedByDesignIds, "rejected_by_design", "rejected_by_design"),
      (
        "local:mysql-null-safe-equality",
        NullSafeEqualityIds,
        "local:mysql-null-safe-equality",
        "residual"
      )
    )
    // The partition is EXACT: disjoint, and its union is the twelve. Without both halves a row
    // could be dropped from every set and asserted by nothing.
    val union = parts.flatMap(_._2).toList
    withClue(s"the four scoring sets overlap: ${union.diff(union.distinct).sorted}: ") {
      union.distinct should have size union.size.toLong
    }
    withClue("Epic 22 rows in no scoring set: ") { (Epic22Ids -- union.toSet) shouldBe empty }
    withClue("scoring-set ids that are not Epic 22 rows: ") {
      (union.toSet -- Epic22Ids) shouldBe empty
    }
    // 🔴 The literal count pin story BIDC-10a proved is worth having on top of a cross-repo
    // convention: owned by NEITHER side, so a coordinated add/delete still reddens locally.
    Epic22FixedIds should have size 8
    Epic22UnmeasuredIds should have size 2
    NullSafeEqualityIds should have size 1
    // 🔴 The PUBLISHED TOTAL, pinned HERE and not in the CSV. Without this, 50 of the 99 rows (the
    // 44 `epic21` and the 6 issue:/local: ones) appear in no compiled set, so re-owning one of them
    // to `epic21`/`fixed` is a two-cell edit that G2/G3/G4/G4b/G4c/G7 all pass -- leaving only G8,
    // which the README correctly says can never be the guard. Now the headline is owned by code.
    withClue(
      "the number of SCORED FIXES moved -- if that is intended, move this pin too, and say " +
      "so in the PR: it is the published headline. "
    ) {
      attribution.values.count(_.scored == "fixed") shouldBe 52
    }
    withClue("the published headline moved: ") {
      tallyOf(outcomes, attribution).scoredOf99 shouldBe 64
    }
    // The owner check is done per OWNER, not per set: `epic22` owns two sets (fixed + unmeasured),
    // so comparing the table's `epic22` rows against either set alone reports the other set's rows
    // as unpinned.
    parts.groupBy(_._3).foreach { case (owner, group) =>
      val pinned = group.flatMap(_._2).toSet
      val declared = attribution.values.filter(_.owner == owner).map(_.captureId).toSet
      withClue(s"[$owner] rows the table owns that the CODE does not pin: ") {
        (declared -- pinned) shouldBe empty
      }
      withClue(s"[$owner] ids the CODE pins that the table no longer owns: ") {
        (pinned -- declared) shouldBe empty
      }
    }
    parts.foreach { case (what, ids, owner, scored) =>
      checkAll(ids.toList.sorted, s"$what rows")(identity) { id =>
        val a = attributionOf(attribution, id)
        if (a.scored != scored) {
          sys.error(
            s"expected scored=$scored under owner=$owner, found '${a.scored}'. If you are here " +
            "because the E5 execution measurement finally landed: move the two ids out of " +
            "Epic22UnmeasuredIds into Epic22FixedIds (green) or to a local: owner with its record " +
            "(red). Do not edit the CSV to match."
          )
        }
      }
    }
  }

  // ---- G9: two authors, one TEXT — the corpus resource vs story 22.1's literals --------------

  it should "hold every capture id story 22.1 pins, with the same statement after whitespace collapse" in {
    val ids = corpus.map(_.captureId).toSet
    val texts = epic22Texts()
    withClue("22.1 / 22.5 ids absent from the corpus resource: ") {
      (Epic22Ids -- ids) shouldBe empty
    }
    withClue("ids story 22.1 pins that are not Epic 22 rows here: ") {
      (texts.keySet -- Epic22Ids) shouldBe empty
    }
    withClue("Epic 22 rows story 22.1 does not pin: ") {
      (Epic22Ids -- texts.keySet) shouldBe empty
    }
    // An id-membership check plus a size cannot see the drift this gate exists to catch: two
    // authors typing the same statement two ways. Compare the TEXT, per id, through `checkAll`.
    checkAll(corpus.filter(r => Epic22Ids.contains(r.captureId)), "Epic 22 statement texts")(
      _.captureId
    ) { row =>
      val mine = collapse(row.statement)
      val theirs = collapse(texts(row.captureId))
      if (mine != theirs) {
        sys.error(
          "the corpus resource and story 22.1's literal disagree.\n" +
          s"      corpus: $mine\n      22.1   : $theirs"
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

  // ---- AC-5, second baseline: nothing that parsed after Epic 21 regresses (story 22.7) -------

  it should "not regress any statement that parsed before Epic 22" in {
    val were = corpus.filter(r => pre22.get(r.captureId).contains("parses"))
    withClue("the pre-epic22 baseline holds no parse - the wrong file was committed: ") {
      were should not be empty
    }
    // The pre-Epic-22 tree parsed 81 of 99. A COUNT alone is not enough: two cell edits (flip the
    // regressed row to `rejected`, flip an Epic-22 row to `parses`) keep it at 81 and hide the
    // regression -- and G8's own cross-check counts the SAME edited file. So the real defence is a
    // CROSS-FILE invariant: every row the attribution table owns `epic21` or `pre_epic21` parsed
    // before Epic 22 by construction, and those owners are themselves gated by G3 and G7.
    were should have size 81
    val settledBefore =
      attribution.collect { case (id, a) if Set("epic21", "pre_epic21")(a.owner) => id }.toSet
    withClue(
      "rows owned epic21/pre_epic21 that the pre-epic22 baseline does not call `parses` -- either " +
      "the baseline was edited to hide a regression, or an owner is wrong: "
    ) {
      (settledBefore -- pre22.collect { case (id, "parses") => id }.toSet) shouldBe empty
    }
    settledBefore should have size 56
    checkAll(were, "pre-epic22 parses")(_.captureId) { row =>
      if (byId(row.captureId).verdict != "parses") {
        sys.error(s"parsed before Epic 22, rejected now: ${firstLine(byId(row.captureId).message)}")
      }
    }
  }

  // ---- G8: the series head IS the live run — BOOKKEEPING, not safety (story 22.7 AD-3) -------
  // 🔴 Read this before trusting it. Ask `feedback_gate_integrity`'s question: what is the
  // smallest edit to a data file that makes this pass while the thing it guards is broken? Answer:
  // EDIT THE HEAD ROW. The expectation lives in the very file the dev maintains, so this can never
  // be the guard. It is acceptable only because the verdicts themselves are pinned independently
  // by G2 (against the attribution table) and by G4/G4b/G4c (against COMPILED ids), so a silent
  // scoreboard move is impossible without also moving an attribution row those gates police. G8's
  // whole job is to stop the series and the run DRIFTING APART.

  "the series" should "end with a row equal to the live run (append a row, never edit the head)" in {
    series should not be empty
    val head = series.last
    val live = tallyOf(outcomes, attribution)
    withClue(
      s"series head '${head.label}'@${head.commit} vs the live run -- the scoreboard moved and " +
      "the series was not extended. APPEND a row; do not edit the head. "
    ) {
      (head.scoredOf99, head.scoredOf75, head.parsesRaw, head.rejected) shouldBe
      (live.scoredOf99, live.scoredOf75, live.parsesRaw, live.rejected)
    }
    withClue(
      s"duplicate series labels: ${series.map(_.label).diff(series.map(_.label).distinct)} "
    ) {
      series.map(_.label).distinct should have size series.size.toLong
    }
    series.head.label shouldBe "pre-epic21"
    // The two historical rows are cross-checked against the committed baselines on the two counters
    // a baseline file can actually produce, so the series cannot be re-written to tell a different
    // story about the past either. Their `scored_*` columns are the PUBLISHED headline of their
    // epoch -- a fact of record, not a re-derivation, because a baseline carries verdicts and no
    // verdict file can say what was scored (the README says so, and the summary line names the
    // series as its source for exactly this reason).
    List("pre-epic21" -> baseline, "pre-epic22" -> pre22).foreach { case (label, b) =>
      val row = series.find(_.label == label).getOrElse(fail(s"the series lost its '$label' row"))
      withClue(s"[$label] parses_raw vs the committed baseline: ") {
        row.parsesRaw shouldBe b.values.count(_ == "parses")
      }
      withClue(s"[$label] rejected vs the committed baseline: ") {
        row.rejected shouldBe b.values.count(_ == "rejected")
      }
      withClue(s"[$label] scored more statements than parsed: ") {
        row.scoredOf99 should be <= row.parsesRaw
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
  val BaselinePre21 = "corpus/baseline-pre-epic21.csv"

  /** Story 22.7 - the POST-Epic-21 tree, measured in a separate checkout of `40c8c63e` (the first
    * parent of the first Epic-22 merge, PR #331 / `feature/22.1`). 81 parses / 18 rejected. The
    * same "do not refresh it" rule as `BaselinePre21`: it is history, and a re-measurement on a
    * later tree is a new row of the SERIES, never an edit to this file.
    */
  val BaselinePre22 = "corpus/baseline-pre-epic22.csv"
  val AttributionResource = "corpus/epic-21-attribution.csv"

  /** Story 22.7 - the scoreboard SERIES. One row per measured tree, append-only. */
  val SeriesResource = "corpus/series.csv"

  /** Story 22.7: `epic22a_derived_table` / `epic22b_cte` are GONE. Epic 22 shipped, so every row
    * they owned is now either `epic22` (it parses, and a MERGED suite executes its shape against
    * real Elasticsearch with an exact oracle) or a residual with a named owner. An owner kept alive
    * with zero rows is an allow-list nobody exercises, and G5 would go vacuous on it.
    */
  val Owners: Set[String] = Set(
    "epic21",
    "epic22",
    "pre_epic21",
    "rejected_pending_policy",
    "capability_open",
    "rejected_by_design"
  )

  val Scores: Set[String] =
    Set(
      "fixed",
      "pre_epic21",
      "residual",
      "rejected_pending_policy",
      "capability_open",
      "rejected_by_design"
    )

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
    // Story 22.7: `epic22` implies `parses` for the same reason `epic21` does - an owner that names
    // a SHIPPED epic is a claim the grammar accepts the statement, and the ten rows it owns were
    // measured parsing on this tree. The two `epic22*` placeholders 21.6 carried (which implied
    // NOTHING while their stories were in flight) are retired with the epic.
    if (owner == "epic21" || owner == "epic22" || owner == "pre_epic21") Some("parses")
    else if (isIssueOwner(owner) || isLocalOwner(owner) || owner == "capability_open") None
    else Some("rejected")

  /** Story 22.1 — the derived-table rows, PINNED IN CODE for the same reason the temp-table probe
    * ids are (G4): a gate whose expectations live in the file it guards can be silenced by editing
    * that file.
    *
    * An `epic22*` owner stopped implying `rejected` when story 22.1 landed — an epic in flight may
    * have shipped, and the verdict is MEASURED, not assumed. What replaces the implication is this
    * explicit partition of the eleven derived-table statements, asserted against the attribution
    * table in BOTH directions.
    *
    * 🔴 Story 22.7 keeps these three sets as the SHAPE partition (which statement needs what) and
    * adds a separate SCORING partition below (what the headline counts). They are different
    * questions and 21.6's own `expected`/`scored` split is the precedent: conflating them is how
    * "it parses" became "it is fixed" in the first draft of the Epic-21 headline.
    */
  val DerivedTableParsesIds: Set[String] = Set(
    "tableau.mysql.wx.003",
    "tableau.mysql.w1.018",
    "tableau.mysql.w7.043",
    "tableau.sql92.wx.003",
    "tableau.mysql.w1.019",
    "tableau.mysql.w7.044",
    "tableau.sql92.wx.009",
    "superset.flightsql.w5.005",
    "superset.flightsql.w7.007"
  )

  /** The two declared residuals, with their SECOND blocker — `wx.012` has no correlation name (the
    * alias is mandatory, SQL-92 §7.6) AND carries Oracle `ROWNUM`; `w8.054` carries the MySQL
    * null-safe `<=>` in its ON. Both are out of epic 22's scope and must STAY rejected.
    */
  val DerivedTableRejectedIds: Set[String] = Set(
    "tableau.sql92.wx.012",
    "tableau.mysql.w8.054"
  )

  /** Story 22.5 — the CTE partition, the exact shape story 22.1 gave the derived-table rows and for
    * the identical reason: once `epic22b_cte` stops implying `rejected` (the story landed), the
    * only thing left asserting this row is a pin that lives in CODE, where editing the CSV cannot
    * silence it.
    *
    * The corpus carries exactly ONE `WITH` statement, which is also story 22.5's entire corpus
    * credit.
    */
  val CteParsesIds: Set[String] = Set(
    "superset.flightsql.w6.006"
  )

  /** The twelve statements Epic 22 owns: eleven derived tables + one CTE.
    *
    * 🔴 TWELVE, not the TEN story 21.6's shape census reported. `superset.flightsql.w5.005` and
    * `tableau.mysql.w8.054` are derived tables in JOIN position, and that census classified with a
    * first-match-wins regex testing the `FROM ( SELECT` shape ONLY, so it filed both under
    * "quoting". Epic 21's arithmetic ceiling is therefore `12 + 51 = 63/99`, never 65. *Never
    * classify a corpus shape with a regex that tests one syntactic position.*
    */
  val Epic22Ids: Set[String] = DerivedTableParsesIds ++ DerivedTableRejectedIds ++ CteParsesIds

  // ---- Story 22.7 - the SCORING partition of the twelve, pinned in CODE ----------------------
  // Each of the four sets below names a (owner, scored) pair, and G4c asserts the partition is
  // exact (disjoint, union == Epic22Ids) AND agrees with the attribution table in BOTH directions.
  // The reason is 21.6 G4's: an expectation that lives only in the file it guards can be silenced
  // by editing that file.

  /** Scored `fixed` under owner `epic22`: the statement parses AND a MERGED sibling suite executes
    * its shape against real Elasticsearch 6.8 / 7.17 / 8.18 / 9.0 with an exact oracle.
    *
    * 🔴 The evidence rule is story 22.7 AD-10's, applied to Epic 22: *a `fixed` row NAMES the
    * merged suite that asserts its CORRECTNESS, not merely its parse; a row with no such suite
    * takes `residual`.* The witnesses live in another repository (`softclient4es-arrow`,
    * `JoinExtensionIntegrationSpec` behaviour *"JoinExtension (derived tables - story 22.4)"* and
    * story 22.5's CTE rows) and each row's `note` cell in the attribution table names the one that
    * discharges it. This is a CONVENTION checked by review, never a build interlock - the `sql`
    * test classpath carries no Elasticsearch client and cannot run a Docker leg. What the
    * convention is worth is the literal count pin below, owned by neither side, which reddens when
    * either half is edited without thought.
    */
  val Epic22FixedIds: Set[String] = Set(
    "tableau.mysql.wx.003",
    "tableau.mysql.w1.018",
    "tableau.mysql.w7.043",
    "tableau.sql92.wx.003",
    "tableau.mysql.w7.044",
    "superset.flightsql.w5.005",
    "superset.flightsql.w7.007",
    "superset.flightsql.w6.006"
  )

  /** Parses, owned by `epic22`, scored `residual` - because NOBODY HAS RUN IT YET.
    *
    * Both statements wrap a FULLY-QUOTED qualifier inside the derived body (`"<cluster>"."<index>"`
    * / the backticked twin). The grammar keeps the qualifier in `Table.parts` and the bare last
    * part as `Table.name` (`FromParser`), so the read is EXPECTED to reach the bare index - but
    * "expected" is a prediction, and the merged sibling suites all execute a BARE index name, so no
    * measurement anywhere covers this spelling.
    *
    * 🔴 They are NOT `fixed` (that would be a declaration whose only evidence is an unrun test) and
    * NOT a `local:` defect slug (that would publish a failure nobody has seen). `residual` under
    * `epic22` says exactly what is true: Epic 22 owns them, they parse, and the headline does not
    * count them. The `parses - scored` gap in the summary states it out loud, which is what that
    * gap is for.
    *
    * WHEN THE MEASUREMENT LANDS: move these two to `Epic22FixedIds` if they execute, or to a
    * `local:` owner with the record if they are refused. Do NOT resolve a red here by editing the
    * attribution table.
    */
  val Epic22UnmeasuredIds: Set[String] = Set(
    "tableau.mysql.w1.019",
    "tableau.sql92.wx.009"
  )

  /** Story 22.7 G4b - rejected ON PURPOSE by the epic's own scope, permanently, pinned in CODE.
    *
    * `tableau.sql92.wx.012` is `SELECT * FROM (SELECT * FROM "c"."bi_events") WHERE ROWNUM <= 1`.
    * The blocker is that the derived table carries NO CORRELATION NAME: the alias is mandatory
    * (SQL-92 clause 7.6, story 22.1 PD-1).
    *
    * 🔴 `ROWNUM` is NOT the blocker, and saying so is the trap this pin exists to avoid.
    * Re-measured 2026-09-13 (recorded in the attribution `note`): `ROWNUM` resolves as an ORDINARY
    * IDENTIFIER and round-trips. That is the HAZARD rather than the refusal - if the
    * mandatory-alias rule were ever relaxed, this statement would parse and range-filter a field
    * that does not exist, returning ZERO rows with HTTP 200 (the #205/#209/#224/#253
    * silent-wrong-answer family). Which is precisely why the row must STAY rejected and why a parse
    * here is a loud failure.
    */
  val RejectedByDesignIds: Set[String] = Set("tableau.sql92.wx.012")

  /** Rejected by a blocker Epic 22 does not own: the MySQL null-safe equality operator `<=>` in the
    * JOIN `ON`. The derived table itself parses (story 22.1's control asserts the `=` spelling of
    * the same statement parses, so `<=>` is the only variable).
    *
    * Count: 1 of 99, and there is NO SQL-92 twin - the `tableau.sql92.w8.*` slice carries no JOIN
    * and no derived table at all. So `<=>` blocks the only captured statement of its shape in
    * EITHER dialect; nothing here may be cited as a cross-dialect witness.
    */
  val NullSafeEqualityIds: Set[String] = Set("tableau.mysql.w8.054")

  /** Story 22.7 G9 - the twelve statements, read from the copy that OWNS them.
    *
    * Eleven come from story 22.1's `DerivedTableCorpusSpec.rows` (widened to `private[sql]` for
    * this gate), so G9 really is "two files, two authors, one text". A third hand transcription
    * living beside the gate would agree with itself and see nothing.
    *
    * The twelfth - the CTE witness - is NOT in 22.1's table (that story owns the eleven derived
    * rows; story 22.5 owns this one and pins it in `CorpusReplay.CteParsesIds` rather than in a
    * statement table), so it is the one literal written here, and it is written here for that
    * reason and no other.
    */
  val CteWitnessText: String =
    "WITH monthly AS (SELECT category, SUM(amount) AS total FROM bi_events GROUP BY category) " +
    "SELECT * FROM monthly"

  def epic22Texts(): Map[String, String] = {
    // Constructing the spec only builds its `rows` and registers its tests; nothing runs them.
    val declared = new DerivedTableCorpusSpec().rows.map { case (id, sql, _) => id -> sql }.toMap
    declared + (CteParsesIds.head -> CteWitnessText)
  }

  /** The collapse both this suite and story 22.1's spec apply before comparing a statement. */
  def collapse(s: String): String =
    s.replaceAll("\\s+", " ").trim.stripSuffix(";").trim

  /** The 24 Tableau temp-table capability probes, PINNED HERE and not in the CSV.
    *
    * Parsing one of these answers Tableau's "temp tables supported?" probe - honouring temp tables
    * is an OPEN product decision (lead, 2026-09-04), tracked outside Epic 21. Under the lead's
    * uniform DQL/DML/DDL quoting ruling the 21 `CapabilityOpenIds` are EXPECTED to parse now that
    * story 21.7 has landed: they are excluded from the headline numerator on BOTH denominators,
    * never kept as parse errors, and never executed against a cluster (the replay is parse-only by
    * construction - the `sql` module's test classpath carries no Elasticsearch client).
    *
    * The 3 `RejectedPendingPolicyIds` must STAY rejected. They are refused by INTENT since 0.23.0:
    * the grammar now RECOGNISES `CREATE [LOCAL | GLOBAL] TEMPORARY TABLE` and raises an `err`
    * naming the construct, rather than falling through to an unrelated combinator's failure. The
    * verdict is unchanged and so is the obligation. If you are editing these sets to make a build
    * go green, stop: the build is telling you a probe is being scored as a win, or that a fix went
    * too far.
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

  /** Rejected on grammar, BY INTENT since 0.23.0: `CREATE [LOCAL | GLOBAL] TEMPORARY TABLE` is
    * recognised so the refusal can name the construct, and refused with an `err`. Whether the
    * engine should ever honour a temporary table stays an open product decision; the parse verdict
    * does not.
    */
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

  /** Story 22.7 widened 21.6's no-argument `loadBaseline()`: there are two baselines now (the
    * pre-Epic-21 and the pre-Epic-22 trees) and Epic 23 will add more.
    */
  def loadBaseline(name: String): Map[String, String] =
    indexBy(
      rows(name).map(m => m.getOrElse("capture_id", "") -> m.getOrElse("verdict", "")),
      name
    )

  /** Story 22.7 AD-3 - one row per measured tree, append-only. */
  final case class SeriesRow(
    label: String,
    commit: String,
    measuredAt: String,
    scoredOf99: Int,
    scoredOf75: Int,
    parsesRaw: Int,
    rejected: Int
  )

  def loadSeries(): List[SeriesRow] =
    rows(SeriesResource).map { m =>
      def int(k: String): Int =
        m.getOrElse(k, "").trim match {
          case "" => sys.error(s"$SeriesResource: column '$k' is blank")
          case v =>
            try v.toInt
            catch { case _: NumberFormatException => sys.error(s"$SeriesResource: '$k' = '$v'") }
        }
      SeriesRow(
        m.getOrElse("label", "").trim,
        m.getOrElse("commit", "").trim,
        m.getOrElse("measured_at", "").trim,
        int("scored_of_99"),
        int("scored_of_75"),
        int("parses_raw"),
        int("rejected")
      )
    }

  /** The four counters G8 compares against the series head. ONE derivation, shared with
    * `summaryLine` - a second copy is the "one key, two derivations" class story 21.3 paid for.
    */
  final case class Tally(scoredOf99: Int, scoredOf75: Int, parsesRaw: Int, rejected: Int)

  def tallyOf(outcomes: List[Outcome], attribution: Map[String, Attribution]): Tally = {
    def scored(o: Outcome): Boolean = {
      val s = attributionOf(attribution, o.row.captureId).scored
      o.verdict == "parses" && (s == "fixed" || s == "pre_epic21")
    }
    Tally(
      scoredOf99 = outcomes.count(scored),
      scoredOf75 = outcomes.count(o => scored(o) && !TempTableProbeIds.contains(o.row.captureId)),
      parsesRaw = outcomes.count(_.verdict == "parses"),
      rejected = outcomes.count(_.verdict == "rejected")
    )
  }

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
    } else if (a.owner == "rejected_by_design") {
      // Story 22.7: same placement, same reason. A by-design rejection that starts PARSING is an
      // event a human must see in the artefact, not a silent +1 in the `parses` column.
      if (o.verdict == "parses") "UNEXPECTED_parses_by_design" else "rejected_by_design"
    } else if (o.verdict == "parses") {
      if (a.scored == "residual") "parses_not_scored" else "parses"
    } else "rejected_other"

  private[census] val BucketOrder =
    List(
      "parses",
      "parses_not_scored",
      "parses_capability_open",
      "rejected_capability_open",
      "rejected_pending_policy",
      "rejected_by_design",
      "rejected_other",
      "UNEXPECTED_parses_pending_policy",
      "UNEXPECTED_parses_by_design"
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
      // A bucket absent from `BucketOrder` would be silently ZEROED in the artefact below. Story
      // 22.7 removed one name and added two, which is exactly the edit that gets this wrong.
      require(
        (counts.keySet -- BucketOrder.toSet).isEmpty,
        s"bucket(s) missing from BucketOrder: ${(counts.keySet -- BucketOrder.toSet).mkString(", ")}"
      )
      k -> ListMap(BucketOrder.map(b => b -> counts.getOrElse(b, 0)): _*)
    }: _*)
  }

  /** The published scoreboard line.
    *
    * Story 22.7 added the second baseline and the Epic-22 split; everything else is 21.6's, lead
    * rulings included.
    *
    * The `[22.7]` tag names the story that last MOVED the scoreboard, not the suite: whoever moves
    * it next re-stamps it (Epic 23 appends a series row and takes the tag with it).
    *
    * `scoredAt` DEGRADES rather than throwing when a series row is missing, because the artefacts
    * are written before any gate and a failing gate must not leave the operator blind. The `??`
    * string therefore reaches `corpus-replay.md` too -- it is a defect marker, never a measurement,
    * and G8 reddens in the same run.
    */
  def summaryLine(
    outcomes: List[Outcome],
    attribution: Map[String, Attribution],
    series: List[SeriesRow]
  ): String = {
    val t = tallyOf(outcomes, attribution)
    // 🔴 The historical numbers come from the SERIES, never from a baseline file.
    //
    // Story 21.6 derived "was B before Epic 21" by counting non-probe `parses` rows in the baseline
    // CSV, and its own comment recorded why that was fragile: a baseline holding a statement that
    // parses WITHOUT being scored makes the two sides of "N (was B)" different measurements. It has
    // since diverged -- at the pre-Epic-22 tree 81 statements parsed, 60 of them non-probe, but the
    // published Epic-21 headline was 56, because four parse and answer wrongly. A baseline file
    // carries VERDICTS and cannot produce a SCORED figure at all. The series row can, which is one
    // of the reasons it exists; G8 cross-checks each historical row's `parses_raw` against the
    // baseline it belongs to, so the series cannot be re-written to tell a different story either.
    def scoredAt(label: String): String =
      series.find(_.label == label).map(_.scoredOf99.toString).getOrElse(s"?? no '$label' row")
    val probes = TempTableProbeIds.size
    val intended = outcomes.size - probes
    val probesParsing = outcomes.count { o =>
      o.verdict == "parses" && TempTableProbeIds.contains(o.row.captureId)
    }
    val epic22Fixed = outcomes.count { o =>
      Epic22Ids.contains(o.row.captureId) &&
      attributionOf(attribution, o.row.captureId).scored == "fixed"
    }
    // Lead ruling 2 (2026-09-13): the verb is SCORES, not "parses". PD-1 mandates that `N` counts
    // `scored` and then writes "parses"; the lead ruled the verb is the side that is wrong, because a
    // statement that parses and is not counted makes "parses N" false on its face. The raw parse count
    // and the difference follow immediately, so neither number can be quoted without the other.
    val gap =
      if (t.parsesRaw == t.scoredOf99) ""
      else
        s" ${t.parsesRaw} PARSE -- the ${t.parsesRaw - t.scoredOf99}-row difference is never " +
        s"counted ($probesParsing capability probes + " +
        s"${t.parsesRaw - t.scoredOf99 - probesParsing} that parse but answer wrongly, " +
        "incompletely or UNMEASURED, PD-3/G4/G4c); see the artefact."
    s"[22.7] corpus replay: SCORES ${t.scoredOf99}/${outcomes.size} " +
    s"(was ${scoredAt("pre-epic22")} after Epic 21, ${scoredAt("pre-epic21")} before it); " +
    s"${t.scoredOf75}/$intended of the statements we intend to answer; " +
    s"$probes capability probes excluded from scoring; " +
    s"Epic 22 rows: $epic22Fixed of ${Epic22Ids.size} scored fixed; " +
    "stories 22.2 / 22.3 / 22.6 corpus credit: 0 / 0 / 0 (no captured statement uses their " +
    s"constructs).$gap"
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
    pre21: Map[String, String],
    pre22: Map[String, String],
    series: List[SeriesRow]
  ): Unit = {
    // Story 22.7 renamed the directory: the scoreboard is a SERIES now, not one epic's report.
    val dir = new File(repoRoot, "sql/target/corpus-replay")
    dir.mkdirs()

    def cell(s: String) = "\"" + s.replace("\"", "\"\"") + "\""
    // `baseline` keeps its 21.6 meaning (the pre-Epic-21 tree) - a column a downstream reader may
    // already consume is never re-pointed. `baseline_pre22` is appended beside it.
    val csvHeader = List(
      "capture_id",
      "tool",
      "dialect",
      "workload_step",
      "authorship",
      "baseline",
      "baseline_pre22",
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
        pre21.getOrElse(o.row.captureId, ""),
        pre22.getOrElse(o.row.captureId, ""),
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
    md ++= "# Corpus replay scoreboard\n\n"
    md ++= summaryLine(outcomes, attribution, series) + "\n\n"
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
