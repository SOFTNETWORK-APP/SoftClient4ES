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

package app.softnetwork.elastic.client

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A `DOUBLE` or a `REAL` inside a `VARCHAR` column of a set operation is spelled as DuckDB 1.5.5
  * casts it to `VARCHAR` (the lead's ruling of 2026-10-06). Every expectation below is DuckDB's
  * answer to `CAST(<value> AS VARCHAR)`, MEASURED value by value -- the layout (`250000000000.0`,
  * `1e-05`, `1e+16`), the digits, and the values where DuckDB's own algorithm does not find the
  * shortest digits (`7.1680000000000004e+25`, the float `4073260.75`), which are its answer too.
  */
class FloatingPointTextSpec extends AnyFlatSpec with Matchers {

  /** The value (Python's shortest repr, read back exactly by `parseDouble`) -> DuckDB's text. */
  private val doubles: Seq[(String, String)] = Seq(
    "250000000000.0"          -> "250000000000.0",
    "1e-05"                   -> "1e-05",
    "1e+16"                   -> "1e+16",
    "1000000000000000.0"      -> "1000000000000000.0",
    "0.0001"                  -> "0.0001",
    "-2.5"                    -> "-2.5",
    "-0.0"                    -> "-0.0",
    "0.0"                     -> "0.0",
    "1.0"                     -> "1.0",
    "100.0"                   -> "100.0",
    "123.456"                 -> "123.456",
    "0.30000000000000004"     -> "0.30000000000000004",
    "0.3333333333333333"      -> "0.3333333333333333",
    "1e+22"                   -> "1e+22",
    "1e+23"                   -> "1e+23",
    "9007199254740992.0"      -> "9007199254740992.0",
    "9999999999999998.0"      -> "9999999999999998.0",
    "5e-324"                  -> "5e-324",
    "2.2250738585072014e-308" -> "2.2250738585072014e-308",
    "1.7976931348623157e+308" -> "1.7976931348623157e+308",
    "7.168e+25"               -> "7.1680000000000004e+25",
    "3.584e+25"               -> "3.5840000000000002e+25",
    "-250000000000.0"         -> "-250000000000.0",
    "-1e-05"                  -> "-1e-05",
    "-1e+16"                  -> "-1e+16",
    "1.5e+300"                -> "1.5e+300",
    "2.82879384806159e+17"    -> "2.82879384806159e+17",
    "8.41e+21"                -> "8.41e+21",
    "0.002"                   -> "0.002",
    "4.35"                    -> "4.35",
    "12345.678"               -> "12345.678",
    "2.5e-05"                 -> "2.5e-05",
    "1.2345e-07"              -> "1.2345e-07",
    "3.14159"                 -> "3.14159",
    "-123456.789"             -> "-123456.789",
    "1.2345678901234568e+17"  -> "1.2345678901234568e+17",
    "5.0"                     -> "5.0",
    "0.1"                     -> "0.1",
    "1e-300"                  -> "1e-300",
    "3e-310"                  -> "3e-310"
  )

  /** The float (its shortest float32 repr, read back exactly by `parseFloat`) -> DuckDB's text. */
  private val floats: Seq[(String, String)] = Seq(
    "1.e-01"         -> "0.1",
    "2.5e+00"        -> "2.5",
    "1.e-05"         -> "1e-05",
    "1.6777216e+07"  -> "16777216.0",
    "3.4028235e+38"  -> "3.4028235e+38",
    "1.e-45"         -> "1e-45",
    "-0.0"           -> "-0.0",
    "0.0"            -> "0.0",
    "3.e-01"         -> "0.3",
    "3.3333334e-01"  -> "0.33333334",
    "1.e+02"         -> "100.0",
    "1.e+16"         -> "1e+16",
    "1.e+15"         -> "1000000000000000.0",
    "1.e-04"         -> "0.0001",
    "1.23456e+02"    -> "123.456",
    "-2.5e+00"       -> "-2.5",
    "2.5e+11"        -> "250000000000.0",
    "1.2345679e-01"  -> "0.12345679",
    "1.1754944e-38"  -> "1.1754944e-38",
    "7.168e+25"      -> "7.168e+25",
    "4.0732608e+06"  -> "4073260.75",
    "-3.614988e+08"  -> "-361498816.0",
    "-3.9489588e+06" -> "-3948958.75",
    "3.2423772e+06"  -> "3242377.25",
    "5.641915e+07"   -> "56419152.0",
    "-7.5e-01"       -> "-0.75",
    "1.25e+00"       -> "1.25",
    "5.e-01"         -> "0.5"
  )

  "A DOUBLE" should "be spelled as DuckDB 1.5.5 casts it to VARCHAR" in {
    val wrong = doubles.flatMap { case (value, duck) =>
      val got = FloatingPointText.double(java.lang.Double.parseDouble(value))
      if (got == duck) None else Some(s"$value: $got instead of $duck")
    }
    wrong shouldBe empty
  }

  it should "spell the values no column stores as DuckDB does" in {
    FloatingPointText.double(Double.NaN) shouldBe "nan"
    FloatingPointText.double(Double.PositiveInfinity) shouldBe "inf"
    FloatingPointText.double(Double.NegativeInfinity) shouldBe "-inf"
  }

  "A REAL" should "be spelled as DuckDB 1.5.5 casts it to VARCHAR, from the float's own value" in {
    val wrong = floats.flatMap { case (value, duck) =>
      val got = FloatingPointText.real(java.lang.Float.parseFloat(value))
      if (got == duck) None else Some(s"$value: $got instead of $duck")
    }
    wrong shouldBe empty
  }
}
