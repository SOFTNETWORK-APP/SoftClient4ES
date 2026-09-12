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
}
