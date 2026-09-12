package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime

/** Story BIDC-8, AD-9 (review H-3) — WHERE it is right to collapse an absent field to `false`, and
  * WHERE `null` must survive.
  *
  * The audit asked for a "context discriminator" that would keep the `false` collapse to filter
  * scripts only. MEASURED, that discriminator is not needed, because it already exists
  * STRUCTURALLY: the collapse lives in `Expression.painless` / `Criteria.painless`, and an
  * `Expression` IS a boolean — it is consumed as a condition, never projected as a value. A
  * projected value goes through a different renderer entirely, `Identifier.painless`, which still
  * emits `(paramN == null) ? null : …`.
  *
  * POSITIVE PROOF OF TOTALITY — every consumer of a `Criteria` rendering in this repository,
  * enumerated (`grep -rn "criteria.painless\|\.painless(Some(" sql/src/main core/src/main
  * bridge/src/main`), and what each does with it:
  *
  *   1. `bridge/package.scala:680` `scriptQueryOf` -> `scriptQuery` — Elasticsearch requires a
  *      BOOLEAN; a `null` is a runtime `class_cast_exception`.
  *   1. `Criteria.painless`'s own `Predicate` arm (`Where.scala:236-250`) — joins two renderings
  *      with `&&` / `||`. Painless refuses `null && x`.
  *   1. `function/cond/package.scala:360` — a `CASE` condition, coerced to `SQLTypes.Boolean` and
  *      spliced as `$c ? $r`. A `null` there does not even compile.
  *   1. `Expression.bucketPipelinePainless` (HAVING `bucket_selector`) — Elasticsearch requires a
  *      boolean.
  *
  * There is no fifth. Every one is a boolean position, so the collapse is right in all of them, and
  * a context flag would be a parameter with one legal value.
  *
  * This spec pins the OTHER half — the three value-projecting renderers, which must keep `null` —
  * so a future "simplification" that routes them through the criteria renderer reddens here rather
  * than silently turning every absent field into `false` in a projection.
  */
class PainlessNullSurvivalSpec extends AnyFlatSpec with Matchers {

  import scala.language.implicitConversions

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  private def bodyOf(sql: String): String =
    Parser(sql) match {
      case Right(ss: SingleSearch) => requestToElasticSearchRequest(ss).query
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  "a projected function (script_fields)" should "keep null for an absent field" in {
    val body = bodyOf("SELECT id, UPPER(status) AS u FROM probe")
    body should include("script_fields")
    body should include("? null :")
    body should not include "? false :"
  }

  /** ⚠️ ROUND 10, MEDIUM-2 — READ THIS BEFORE READING THE ASSERTION AS AN ENDORSEMENT.
    *
    * The EMISSION below is correct: the sort script preserves `null`, which is what this story
    * owns. Elasticsearch nevertheless REFUSES to run it when any document in the index lacks the
    * field — MEASURED live on 8.18.3: `SELECT id FROM probe ORDER BY UPPER(status)` answers `HTTP
    * 500 null_pointer_exception: Cannot invoke "java.lang.CharSequence.length()" because "text" is
    * null`, so an ORDER BY over a function of a sparse column is unusable. That is PRE-EXISTING,
    * downstream of this emission (the sort path builds the comparator), and NOT fixed here — a
    * null-safe sort emission would change ordering semantics for present values, which is a product
    * decision, not a dev one. It is disclosed in the PR body and in both doc twins.
    *
    * This pin therefore says "the emission still carries `null`", never "the sort path works".
    */
  "a sort key (script sort)" should "keep null for an absent field" in {
    val body = bodyOf("SELECT id FROM probe ORDER BY UPPER(status)")
    body should include("_script")
    body should include("? null :")
    body should not include "? false :"
  }

  "a grouping key (terms script)" should "keep null for an absent field" in {
    val body = bodyOf("SELECT UPPER(status) AS u, COUNT(*) AS n FROM probe GROUP BY UPPER(status)")
    body should include("? null :")
    body should not include "? false :"
  }

  "a WHERE predicate (filter script)" should "collapse an absent field to false, not null" in {
    val body = bodyOf("SELECT id FROM probe WHERE UPPER(status) = 'A'")
    body should include("? false :")
    // The PARAMETER binding still carries its own `? null :` — that is the operand's rendering.
    // What must not survive is a `null` in the CONDITION, i.e. as the last thing the script yields.
    body should include("""left1 == null ? false : ((left1.compareTo(\"A\") == 0))""")
  }

  /** The B-1 regression, byte-pinned in both bridge copies. A `CASE` over a function used to emit
    * `cannot resolve symbol [left1]`: the condition's local was bound on a THROWAWAY context while
    * the prologue came from another, so the script referenced a name it never declared. Executed on
    * live Elasticsearch 8.18.3 over docs A / B / field-absent / A, this returns 1 / 0 / 0 / 1.
    */
  "a CASE over a function" should "declare every local it references" in {
    val body = bodyOf("SELECT id, CASE WHEN UPPER(status) = 'A' THEN 1 ELSE 0 END AS c FROM probe")
    val declared =
      """def (left\d+|param\d+|right\d+)""".r.findAllMatchIn(body).map(_.group(1)).toSet
    val referenced = """\b(left\d+|right\d+)\b""".r.findAllIn(body).toSet
    withClue(s"declared=$declared referenced=$referenced in\n$body\n") {
      (referenced -- declared) shouldBe empty
    }
  }

  /** 🔴 Round 10, LOW-1 — a SOURCE SCAN, with no allow-list, over every consumer of
    * `Predicate.not`.
    *
    * The fold of a predicate's `NOT` into its right criterion has FOUR consumers (enumerated on
    * `Predicate.emittedRight`). Two of this story's defects were a consumer that did not take part:
    * BLOCKING-1 was a criteria class with no `negated` override, LOW-1 a site still reading
    * `rightCriteria` + `not` directly. Nothing listed them, so nothing noticed. This fails if a
    * source file reads a predicate's `not` without also consulting `notConsumed` — the only honest
    * way to use it.
    */
  "every consumer of a predicate's NOT" should "say which side of the fold it is on" in {
    def root: java.io.File = {
      var d = new java.io.File(".").getAbsoluteFile
      while (d != null && !new java.io.File(d, "build.sbt").isFile) d = d.getParentFile
      if (d == null) fail("could not locate the build root") else d
    }
    def scalaFilesUnder(dir: java.io.File): Seq[java.io.File] =
      if (!dir.isDirectory) Nil
      else
        Option(dir.listFiles).toSeq.flatten.flatMap { f =>
          if (f.isDirectory) scalaFilesUnder(f)
          else if (f.getName.endsWith(".scala")) Seq(f)
          else Nil
        }
    val sources = Seq("sql/src/main", "core/src/main", "bridge/src/main", "es6/bridge/src/main")
      .map(new java.io.File(root, _))
      .flatMap(scalaFilesUnder)
    sources should not be empty
    val offenders = sources.flatMap { f =>
      // 🔴 COMMENTS STRIPPED FIRST. Measured: without this the gate is defeated by the very comment
      // that explains it — the file naming `notConsumed` in prose passes while the CODE below has
      // stopped consulting it. A guard that its own documentation satisfies is not a guard.
      val raw = new String(java.nio.file.Files.readAllBytes(f.toPath), "UTF-8")
      val code = raw.replaceAll("(?s)/\\*.*?\\*/", " ").replaceAll("(?m)//.*$", " ")
      val readsNot = """(?<![A-Za-z0-9_])(p|predicate)\.not(?![A-Za-z0-9_])""".r
        .findFirstIn(code)
        .isDefined
      // The FOLD side must prove itself in CODE — a comment mentioning `notConsumed` must not
      // satisfy the gate, which is why comments are stripped first. The NO-FOLD side can only ever
      // be a written decision, so its marker is looked for in the raw text: the gate cannot verify
      // that reason, it forces someone to state one where silence used to pass.
      val classified = code.contains("notConsumed") || raw.contains("NOT-FOLD:")
      if (readsNot && !classified) Some(f.getPath.substring(root.getPath.length))
      else None
    }
    withClue(
      "these files read a predicate's `not` without saying which side of the fold they are on. " +
      "Consult `notConsumed` (fold: the criterion is evaluated over the SAME document), or write " +
      "a `NOT-FOLD:` comment saying why it must not (round 10 measured one such case: under a " +
      "nested relation the negation is outside an EXISTENTIAL and folding it changes the " +
      "question). Silence is the bug this gate exists for:\n  " + offenders.mkString("\n  ") + "\n"
    )(offenders shouldBe empty)
  }

}
