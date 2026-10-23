package app.softnetwork.elastic.sql.function.math

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `ROUND(x, d)` to `d` decimal places is DuckDB 1.5.5's double rounding, `std::round(x * 10^d) /
  * 10^d`, a half rounded away from zero: `ROUND(2.46, 1)` is `2.5`, `ROUND(-1.25, 1)` is `-1.3`.
  *
  * The script was `Math.round((x * 10^d) / 10^d)`, which IS `Math.round(x)`: `ROUND(2.46, 1)`
  * answered `2` in every context. `ROUND(x)` keeps its bytes.
  */
class RoundScaleSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(Column("x", SQLTypes.Double), Column("n", SQLTypes.Int))
  )

  private def scriptOf(
    sql: String,
    contextType: PainlessContextType = PainlessContextType.Query
  ): String = Parser(sql) match {
    case Right(ss: SingleSearch) =>
      val ctx = PainlessContext(contextType)
      val body = ss.update(Some(schema)).select.fields.head.painless(Some(ctx))
      s"$ctx$body"
    case other => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  /** The rounding the script computes, in the JVM's own `Math`: what Painless runs. */
  private def rounded(x: Double, d: Int): Double = {
    val p = Math.pow(10, d)
    Math.signum(x) * Math.round(Math.abs(x) * p) / p
  }

  "ROUND to decimal places" should "render DuckDB's rounding, a half away from zero" in {
    scriptOf("SELECT ROUND(x, 1) FROM t") shouldBe
    "def param1 = (doc['x'].size() == 0 ? null : doc['x'].value); def param2 = Math.pow(10, 1); " +
    "(param1 == null || param2 == null) ? null : " +
    "Double.valueOf(Math.signum(param1) * Math.round(Math.abs(param1) * param2) / param2)"
    // the same in an ingest processor: its old script was wrong there too
    scriptOf("SELECT ROUND(x, 2) FROM t", PainlessContextType.Processor) should include(
      "Double.valueOf(Math.signum(param1) * Math.round(Math.abs(param1) * param2) / param2)"
    )
  }

  it should "compute the values DuckDB answers" in {
    // DuckDB 1.5.5: round(x, 1) over these doubles
    Seq(
      2.46  -> 2.5,
      -2.46 -> -2.5,
      2.44  -> 2.4,
      1.25  -> 1.3,
      -1.25 -> -1.3,
      2.55  -> 2.6
    ).foreach { case (x, duck) => withClue(x)(rounded(x, 1) shouldBe duck) }
    // and to a negative scale: round(1234.5, -2) is 1200
    rounded(1234.5, -2) shouldBe 1200.0
  }

  it should "bind an operand that is no name once, and answer NULL for its NULL" in {
    scriptOf("SELECT ROUND(ABS(x), 1) FROM t") should endWith(
      "def param3 = ((param2 == null) ? null : Double.valueOf(Math.abs(param2))); " +
      "(param1 == null) ? null : (param3 == null ? null : " +
      "Double.valueOf(Math.signum(param3) * Math.round(Math.abs(param3) * param1) / param1))"
    )
  }

  "ROUND to no decimal place" should "keep its bytes" in {
    scriptOf("SELECT ROUND(x) FROM t") shouldBe
    "def param1 = (doc['x'].size() == 0 ? null : doc['x'].value); def param2 = Math.pow(10, 0); " +
    "(param1 == null || param2 == null) ? null : Long.valueOf(Math.round((param1 * param2) / param2))"
  }
}
