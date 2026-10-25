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
  * answered `2` in every context. Without decimal places (`ROUND(x)`, `ROUND(x, 0)`) the same
  * rounding answers a whole number: `ROUND(-2.5)` is `-3`, where Java's half-up answered `-2`.
  */
class RoundScaleSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns =
      List(Column("x", SQLTypes.Double), Column("n", SQLTypes.Int), Column("r", SQLTypes.Real))
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

  /** DuckDB 1.5.5 rounds a FLOAT in double and narrows the result to a float: `ROUND(0.25::FLOAT,
    * 1)` is the float `0.3`, whose double is `0.30000001192092896`.
    */
  private def roundedReal(x: Float, d: Int): Double = rounded(x.toDouble, d).toFloat.toDouble

  it should "round a REAL in float, as DuckDB rounds a FLOAT" in {
    scriptOf("SELECT ROUND(r, 1) FROM t") should endWith(
      "Double.valueOf((double) (float) " +
      "(Math.signum(param1) * Math.round(Math.abs(param1) * param2) / param2))"
    )
    // DuckDB 1.5.5: round(r, 1)::DOUBLE over these floats
    Seq(
      0.25f -> 0.30000001192092896,
      0.1f  -> 0.10000000149011612,
      7.75f -> 7.800000190734863,
      0.5f  -> 0.5
    ).foreach { case (r, duck) => withClue(r)(roundedReal(r, 1) shouldBe duck) }
    // in double, 7.8 is below DuckDB's float: `ROUND(r, 1) < 7.800000190734863` kept r = 7.75
    rounded(7.75f.toDouble, 1) should be < 7.800000190734863
    // the controls: a DOUBLE operand, a cast to DOUBLE, and a REAL to no decimal place
    scriptOf("SELECT ROUND(x, 1) FROM t") should not include "(float)"
    scriptOf("SELECT ROUND(CAST(r AS DOUBLE), 1) FROM t") should not include "(float)"
    scriptOf("SELECT ROUND(r) FROM t") should endWith(
      "Long.valueOf((long) (Math.signum(param1) * Math.round(Math.abs(param1) * param2) / param2))"
    )
  }

  "ROUND to no decimal place" should "round a half away from zero, as DuckDB does" in {
    val whole =
      "def param1 = (doc['x'].size() == 0 ? null : doc['x'].value); def param2 = Math.pow(10, 0); " +
      "(param1 == null || param2 == null) ? null : " +
      "Long.valueOf((long) (Math.signum(param1) * Math.round(Math.abs(param1) * param2) / param2))"
    scriptOf("SELECT ROUND(x) FROM t") shouldBe whole
    scriptOf("SELECT ROUND(x, 0) FROM t") shouldBe whole
    // DuckDB 1.5.5: round(2.5) = 3, round(-2.5) = -3, round(-0.5) = -1, round(1.5) = 2
    Seq(2.5 -> 3L, -2.5 -> -3L, -0.5 -> -1L, 1.5 -> 2L, -1.5 -> -2L, 2.46 -> 2L, -2.46 -> -2L)
      .foreach { case (x, duck) => withClue(x)(rounded(x, 0).toLong shouldBe duck) }
  }

  it should "answer NULL for a NULL operand that is no name, never a throw" in {
    // `ROUND(ABS(x))` multiplied the NULL `ABS` of a document missing `x`: a NullPointerException
    scriptOf("SELECT ROUND(ABS(x)) FROM t") should endWith(
      "def param3 = ((param2 == null) ? null : Double.valueOf(Math.abs(param2))); " +
      "(param1 == null) ? null : (param3 == null ? null : " +
      "Long.valueOf((long) (Math.signum(param3) * Math.round(Math.abs(param3) * param1) / param1)))"
    )
    // an ingest processor and a transform bind it too
    Seq(PainlessContextType.Processor, PainlessContextType.Transform).foreach { context =>
      withClue(context)(
        scriptOf("SELECT ROUND(ABS(x), 1) FROM t", context) should include(
          "(param3 == null ? null : " +
          "Double.valueOf(Math.signum(param3) * Math.round(Math.abs(param3) * param1) / param1))"
        )
      )
    }
  }
}
