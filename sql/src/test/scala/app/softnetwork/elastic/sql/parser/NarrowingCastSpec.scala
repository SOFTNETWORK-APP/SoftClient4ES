package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 21.5 part C2 — a NARROWING numeric cast was a silent no-op.
  *
  * `SQLTypeUtils.coerce` carried WIDENING arms only (`Int -> BigInt`, `Int -> Double`, `BigInt ->
  * Double`), so every narrowing pair fell through to the identity fallback: `CAST(1.9 AS INT)`
  * emitted `1.9` and `CAST(<double column> AS BIGINT)` emitted the raw doc value. Same family as C1
  * (`CastConversionSpec`) — a cast that silently does nothing — different arms, and therefore its
  * own revert boundary.
  *
  * 🔴 `canConvert` is deliberately NOT touched. Its only production consumer is
  * `schema/TableDiff.scala`, where its "expansion only" numeric-rank rule gates SCHEMA EVOLUTION;
  * loosening it would silently legalise narrowing COLUMN TYPE changes in an ALTER diff — a
  * different contract with a different blast radius.
  *
  * Never pin the BEFORE strings (`1.9`, `300`) — they are the defect.
  */
class NarrowingCastSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("amount", SQLTypes.Double),
      Column("ratio", SQLTypes.Real),
      Column("big", SQLTypes.BigInt),
      Column("n", SQLTypes.Int),
      Column("small", SQLTypes.SmallInt)
    )
  )

  private def painlessOf(sql: String): String =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss.update(Some(schema)).select.fields.head.painless(None)
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  "a narrowing numeric cast" should "narrow instead of silently no-opping, over a literal" in {
    // Truncation toward zero — Java/Painless cast semantics, and consistent with the existing
    // widening arms' style. MySQL rounds; the divergence is documented, not hidden.
    painlessOf("SELECT CAST(1.9 AS INT) FROM t") shouldBe "((int) 1.9)"
    painlessOf("SELECT CAST(1.9 AS BIGINT) FROM t") shouldBe "((long) 1.9)"
    painlessOf("SELECT CAST(300 AS TINYINT) FROM t") shouldBe "((byte) 300)"
    painlessOf("SELECT CAST(300 AS SMALLINT) FROM t") shouldBe "((short) 300)"
  }

  it should "narrow over a COLUMN, across the whole numeric lattice" in {
    // Every strictly-lower target for every numeric source the schema carries. The emitted cast
    // wraps the null-guarded doc read, exactly as the existing WIDENING arms already do — the
    // shape is precedented in-repo, and the integration leg proves Elasticsearch accepts it.
    val rows: Seq[(String, String)] = Seq(
      "SELECT CAST(amount AS REAL) FROM t"     -> "(float)",
      "SELECT CAST(amount AS BIGINT) FROM t"   -> "(long)",
      "SELECT CAST(amount AS INT) FROM t"      -> "(int)",
      "SELECT CAST(amount AS SMALLINT) FROM t" -> "(short)",
      "SELECT CAST(amount AS TINYINT) FROM t"  -> "(byte)",
      "SELECT CAST(ratio AS BIGINT) FROM t"    -> "(long)",
      "SELECT CAST(ratio AS INT) FROM t"       -> "(int)",
      "SELECT CAST(big AS INT) FROM t"         -> "(int)",
      "SELECT CAST(big AS SMALLINT) FROM t"    -> "(short)",
      "SELECT CAST(big AS TINYINT) FROM t"     -> "(byte)",
      "SELECT CAST(n AS SMALLINT) FROM t"      -> "(short)",
      "SELECT CAST(n AS TINYINT) FROM t"       -> "(byte)",
      "SELECT CAST(small AS TINYINT) FROM t"   -> "(byte)"
    )
    rows.foreach { case (sql, cast) =>
      withClue(s"[$sql] ") { painlessOf(sql) should include(cast) }
    }
  }

  it should "leave widening casts exactly as they were" in {
    painlessOf("SELECT CAST(n AS BIGINT) FROM t") should include("(long)")
    painlessOf("SELECT CAST(n AS DOUBLE) FROM t") should include("(double)")
    painlessOf("SELECT CAST(big AS DOUBLE) FROM t") should include("(double)")
    // An identity is still an identity — no cast is emitted at all.
    painlessOf("SELECT CAST(amount AS DOUBLE) FROM t") should not include "(double)"
  }

  it should "leave TRY_CAST's safe wrapper in place around a narrowing" in {
    painlessOf("SELECT TRY_CAST(amount AS INT) FROM t") should startWith("try {")
    painlessOf("SELECT TRY_CAST(amount AS INT) FROM t") should include("(int)")
  }
}
