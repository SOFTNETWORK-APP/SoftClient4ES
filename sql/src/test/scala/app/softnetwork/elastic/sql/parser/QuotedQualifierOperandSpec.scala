package app.softnetwork.elastic.sql.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Issue #332 — a double-quoted QUALIFIER on the right-hand side of a comparison.
  *
  * Story 21.1 AD-13 lists `literal` before any identifier alternative in a value position, so
  * `WHERE a = "x"` is the string `x`. That stays true. What it also swallowed was the QUALIFIED
  * form: `"t0"."k"` consumed `"t0"` as a literal and left `."k"` as trailing input, so the JOIN
  * predicate Tableau's SQL-92 dialect emits was rejected while its backtick twin parsed.
  *
  * The two halves are pinned together on purpose: the fix is only correct if the newly accepted
  * shapes parse AND every AD-13 value position keeps reading a lone quoted lexeme as a string.
  */
class QuotedQualifierOperandSpec extends AnyFlatSpec with Matchers {

  private def reasonOf(sql: String): String =
    Parser(sql).swap.toOption.map(_.msg).getOrElse("")

  private def accepts(sql: String, renders: String): Unit = {
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    val stmt = Parser(sql).toOption.getOrElse(fail(s"[$sql] rejected: ${reasonOf(sql)}"))
    withClue(s"[$sql] ") { stmt.sql shouldBe renders }
    // the render must re-parse to an EQUAL AST: MaterializedViewExtension persists renders
    withClue(s"[$sql] round-trip ") { Parser(stmt.sql) shouldBe Right(stmt) }
    ()
  }

  // ── the defect: a quoted qualifier on the RIGHT of a comparison ──────────────────────────────

  "A double-quoted qualifier on the right of an ON comparison" should "be a column, not a literal" in {
    accepts(
      """SELECT k FROM b JOIN u AS t0 ON b.k = "t0"."k"""",
      """SELECT k FROM b   JOIN u AS t0 ON b.k = "t0"."k""""
    )
    // MIXED quoting re-emits fully quoted: story 21.1 AD-1 keeps ONE bit ("any part was quoted")
    // rather than per-part style, so `"t0".k` canonicalises to `"t0"."k"`. Pre-existing, and the
    // round-trip below still holds because both spellings parse to the same name and bit.
    accepts(
      """SELECT k FROM b JOIN u AS t0 ON b.k = "t0".k""",
      """SELECT k FROM b   JOIN u AS t0 ON b.k = "t0"."k""""
    )
  }

  it should "work in WHERE too — ON and WHERE share whereCriteria" in {
    accepts("""SELECT k FROM b WHERE b.k = "t0"."k"""", """SELECT k FROM b WHERE b.k = "t0"."k"""")
  }

  it should "work for the ordering comparisons, not only equality" in {
    accepts("""SELECT k FROM b WHERE b.k > "t0"."k"""", """SELECT k FROM b WHERE b.k > "t0"."k"""")
  }

  it should "keep the shapes that already parsed" in {
    // the mirrored form, which parsed before the fix — the asymmetry that proved it was a defect
    accepts(
      """SELECT k FROM b JOIN u AS t0 ON "b"."k" = t0.k""",
      """SELECT k FROM b   JOIN u AS t0 ON "b"."k" = t0.k"""
    )
    // backticks were never ambiguous with a string literal, and canonicalise to ANSI quotes
    accepts(
      "SELECT k FROM b JOIN u AS t0 ON b.k = `t0`.`k`",
      """SELECT k FROM b   JOIN u AS t0 ON b.k = "t0"."k""""
    )
    accepts(
      "SELECT k FROM b JOIN u AS t0 ON b.k = t0.k",
      "SELECT k FROM b   JOIN u AS t0 ON b.k = t0.k"
    )
  }

  // ── AD-13 must not move: a LONE quoted lexeme in a value position is a STRING ────────────────

  "A lone double-quoted lexeme in a value position" should "still be a string literal (AD-13)" in {
    accepts("""SELECT k FROM b WHERE a = "x"""", "SELECT k FROM b WHERE a = 'x'")
    accepts("""SELECT k FROM b WHERE a > "x"""", "SELECT k FROM b WHERE a > 'x'")
    accepts("""SELECT k FROM b WHERE a IN ("x", "y")""", "SELECT k FROM b WHERE a IN ('x','y')")
    accepts("""SELECT k FROM b WHERE a LIKE "x%"""", "SELECT k FROM b WHERE a LIKE 'x%'")
    accepts(
      """SELECT k FROM b WHERE a BETWEEN "x" AND "y"""",
      "SELECT k FROM b WHERE a BETWEEN 'x' AND 'y'"
    )
  }

  /** 🔴 The reason the discriminator is STRUCTURAL (`rep1(nameTailPart)` — a dot BETWEEN parts)
    * rather than a test on the parsed name. `"a.b"` is ONE quoted part whose CONTENT holds a dot; a
    * `name.contains(".")` filter would turn this string into a column reference.
    */
  it should "treat a quoted lexeme whose CONTENT contains a dot as a string, not a qualifier" in {
    accepts("""SELECT k FROM b WHERE a = "a.b"""", "SELECT k FROM b WHERE a = 'a.b'")
  }
}
