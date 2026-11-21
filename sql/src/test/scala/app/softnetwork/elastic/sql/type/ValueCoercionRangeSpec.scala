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

package app.softnetwork.elastic.sql.`type`

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.math.{BigDecimal => JBigDecimal, BigInteger}

/** SCHEMA-17 fix R-177 -- the numeric coercions JDBC's getters and arrow's cells go through never
  * narrow a number silently: a value outside the target type's range throws `SQLException("Value
  * <v> is out of range for <TYPE>")`, where `intValue()` & co. wrapped it (MEASURED on c167e07b,
  * Elasticsearch 8.18.3: an `integer` holding 3000000000 read -1294967296 through `coerceToInt`, a
  * `long` holding 1e20 read 7766279631452241920 through `coerceToLong`). A value within the range
  * converts byte for byte as before -- its canonical class as it is --, a fraction truncated toward
  * zero.
  */
class ValueCoercionRangeSpec extends AnyFlatSpec with Matchers {

  /** The failure of `f`, which must be the out-of-range one for `sqlType`. */
  private def outOfRange(sqlType: String)(f: => Any): String = {
    val error = the[java.sql.SQLException] thrownBy f
    error.getMessage should (startWith("Value ") and endWith(s" is out of range for $sqlType"))
    error.getMessage
  }

  /** One integral target: its coercion, its SQL name, its bounds. */
  private final case class Target(name: String, coerce: Any => Any, min: Long, max: Long)

  private val targets: Seq[Target] = Seq(
    Target("TINYINT", ValueCoercion.coerceToByte, Byte.MinValue, Byte.MaxValue),
    Target("SMALLINT", ValueCoercion.coerceToShort, Short.MinValue, Short.MaxValue),
    Target("INT", ValueCoercion.coerceToInt, Int.MinValue, Int.MaxValue),
    Target("BIGINT", ValueCoercion.coerceToLong, Long.MinValue, Long.MaxValue)
  )

  private def big(l: Long): BigInteger = BigInteger.valueOf(l)

  /** What the coercion answered before R-177, for a value within the target's range. */
  private def legacy(t: Target, n: Number): Any = t.name match {
    case "TINYINT"  => java.lang.Byte.valueOf(n.byteValue())
    case "SMALLINT" => java.lang.Short.valueOf(n.shortValue())
    case "INT"      => java.lang.Integer.valueOf(n.intValue())
    case _          => java.lang.Long.valueOf(n.longValue())
  }

  "An integral coercion" should "fail on every bound's outer neighbour, whatever the number's class" in {
    targets.foreach { t =>
      withClue(s"${t.name}: ") {
        val below = big(t.min).subtract(BigInteger.ONE)
        val above = big(t.max).add(BigInteger.ONE)
        // the whole numbers of the JDK, where the neighbour fits a Long
        if (t.name != "BIGINT") {
          outOfRange(t.name)(t.coerce(java.lang.Long.valueOf(t.min - 1))) shouldBe
          s"Value ${t.min - 1} is out of range for ${t.name}"
          outOfRange(t.name)(t.coerce(java.lang.Long.valueOf(t.max + 1))) shouldBe
          s"Value ${t.max + 1} is out of range for ${t.name}"
          outOfRange(t.name)(t.coerce((t.min - 1).toDouble))
          outOfRange(t.name)(t.coerce((t.max + 1).toDouble))
          outOfRange(t.name)(t.coerce((t.max + 1).toFloat))
        } else {
          // 2^63 and the next double below -2^63
          outOfRange(t.name)(t.coerce(9.223372036854775808e18))
          outOfRange(t.name)(t.coerce(Math.nextDown(-9.223372036854775808e18)))
          outOfRange(t.name)(t.coerce(9.223372036854775808e18f))
        }
        outOfRange(t.name)(t.coerce(below)) shouldBe s"Value $below is out of range for ${t.name}"
        outOfRange(t.name)(t.coerce(above))
        outOfRange(t.name)(t.coerce(new JBigDecimal(below)))
        outOfRange(t.name)(t.coerce(new JBigDecimal(above)))
        outOfRange(t.name)(t.coerce(scala.math.BigInt(above)))
        outOfRange(t.name)(t.coerce(scala.math.BigDecimal(new JBigDecimal(below))))
        // a BigInteger beyond a Long, which `longValue()` wrapped
        outOfRange(t.name)(t.coerce(new BigInteger("100000000000000000000")))
        outOfRange(t.name)(t.coerce(new JBigDecimal("1E+1000000000")))
        // NaN and the infinities have no integral value
        Seq[Any](
          Double.NaN,
          Double.PositiveInfinity,
          Double.NegativeInfinity,
          Float.NaN,
          Float.PositiveInfinity,
          Float.NegativeInfinity
        ).foreach(v => outOfRange(t.name)(t.coerce(v)))
      }
    }
  }

  it should "convert each bound itself, byte for byte as before, in its canonical class" in {
    targets.foreach { t =>
      withClue(s"${t.name}: ") {
        val population: Seq[Number] = Seq[Number](
          java.lang.Long.valueOf(t.min),
          java.lang.Long.valueOf(t.max),
          java.lang.Long.valueOf(0L),
          java.lang.Long.valueOf(-1L),
          java.lang.Integer.valueOf(42),
          java.lang.Short.valueOf(7.toShort),
          java.lang.Byte.valueOf(-7.toByte),
          big(t.min),
          big(t.max),
          new JBigDecimal(big(t.max)),
          new JBigDecimal(big(t.min)),
          scala.math.BigInt(t.max),
          scala.math.BigDecimal(t.min),
          java.lang.Double.valueOf(42.9),
          java.lang.Double.valueOf(-42.9),
          java.lang.Float.valueOf(1.5f),
          java.lang.Double.valueOf(-0.0)
        ) ++ (
          // a whole double at each bound, where the double holds it exactly
          if (t.name != "BIGINT")
            Seq[Number](
              java.lang.Double.valueOf(t.min.toDouble),
              java.lang.Double.valueOf(t.max.toDouble)
            )
          else Seq[Number](java.lang.Double.valueOf(-9.223372036854775808e18))
        )
        population.foreach { n =>
          withClue(s"$n (${n.getClass.getName}): ") {
            val answer = t.coerce(n)
            answer shouldBe legacy(t, n)
            answer.getClass shouldBe legacy(t, n).getClass
          }
        }
      }
    }
  }

  it should "keep the truncation toward zero, the truncated value judged against the range" in {
    ValueCoercion.coerceToInt(2147483647.9) shouldBe Int.MaxValue
    ValueCoercion.coerceToInt(-2147483648.9) shouldBe Int.MinValue
    ValueCoercion.coerceToInt(new JBigDecimal("2147483647.9")) shouldBe Int.MaxValue
    ValueCoercion.coerceToInt(new JBigDecimal("-2147483648.9")) shouldBe Int.MinValue
    outOfRange("INT")(ValueCoercion.coerceToInt(2147483648.0))
    outOfRange("INT")(ValueCoercion.coerceToInt(-2147483649.0))
    outOfRange("INT")(ValueCoercion.coerceToInt(new JBigDecimal("2147483648")))
    ValueCoercion.coerceToByte(127.99) shouldBe 127.toByte
    outOfRange("TINYINT")(ValueCoercion.coerceToByte(128.0))
    ValueCoercion.coerceToShort(-32768.5) shouldBe -32768.toShort
    outOfRange("SMALLINT")(ValueCoercion.coerceToShort(-32769.0))
    ValueCoercion.coerceToLong(9.2233720368547748e18) shouldBe 9223372036854774784L
    // a fraction of magnitude below 1 is 0, without dividing by ten to the power of its scale
    ValueCoercion.coerceToInt(new JBigDecimal("1E-1000000000")) shouldBe 0
    ValueCoercion.coerceToLong(new JBigDecimal("-0.999")) shouldBe 0L
  }

  it should "answer its canonical class as it is, with no new instance" in {
    // values outside the JDK's box caches: a re-boxed answer would be another instance
    val i = java.lang.Integer.valueOf(1000000)
    (ValueCoercion.coerceToInt(i) eq i) shouldBe true
    val l = java.lang.Long.valueOf(3000000000L)
    (ValueCoercion.coerceToLong(l) eq l) shouldBe true
    val s = java.lang.Short.valueOf(1000.toShort)
    (ValueCoercion.coerceToShort(s) eq s) shouldBe true
    val f = java.lang.Float.valueOf(1e30f)
    (ValueCoercion.coerceToFloat(f) eq f) shouldBe true
  }

  it should "keep every non-number arm as before" in {
    targets.foreach(t => withClue(s"${t.name}: ")(t.coerce(null) shouldBe (null: Any)))
    ValueCoercion.coerceToFloat(null) shouldBe (null: java.lang.Float)
    ValueCoercion.coerceToInt("42") shouldBe 42
    ValueCoercion.coerceToLong("3000000000") shouldBe 3000000000L
    ValueCoercion.coerceToInt(true) shouldBe 1
    ValueCoercion.coerceToLong(false) shouldBe 0L
    a[NumberFormatException] should be thrownBy ValueCoercion.coerceToInt("3000000000")
    a[java.sql.SQLException] should be thrownBy ValueCoercion.coerceToInt(new Object)
  }

  it should "fail on the cells T0-1 measured wrapping (an integer of 3000000000, a long of 1e20)" in {
    outOfRange("INT")(ValueCoercion.coerceToInt(java.lang.Long.valueOf(3000000000L))) shouldBe
    "Value 3000000000 is out of range for INT"
    outOfRange("BIGINT")(
      ValueCoercion.coerceToLong(new BigInteger("100000000000000000000"))
    ) shouldBe "Value 100000000000000000000 is out of range for BIGINT"
    // an unsigned_long above 2^63 - 1, which `getLong` wrapped negative
    outOfRange("BIGINT")(ValueCoercion.coerceToLong(new BigInteger("18446744073709551615")))
  }

  "coerceToFloat" should "fail on a finite number beyond the largest REAL" in {
    val beyond = Math.nextUp(Float.MaxValue.toDouble)
    outOfRange("REAL")(ValueCoercion.coerceToFloat(beyond))
    outOfRange("REAL")(ValueCoercion.coerceToFloat(-beyond))
    outOfRange("REAL")(
      ValueCoercion.coerceToFloat(1e39)
    ) shouldBe "Value 1.0E39 is out of range for REAL"
    outOfRange("REAL")(ValueCoercion.coerceToFloat(Double.MaxValue))
    outOfRange("REAL")(ValueCoercion.coerceToFloat(BigInteger.ONE.shiftLeft(128)))
    outOfRange("REAL")(ValueCoercion.coerceToFloat(BigInteger.ONE.shiftLeft(128).negate()))
    outOfRange("REAL")(ValueCoercion.coerceToFloat(new JBigDecimal("1E+39")))
    outOfRange("REAL")(ValueCoercion.coerceToFloat(new JBigDecimal("-1E+1000000000")))
    outOfRange("REAL")(ValueCoercion.coerceToFloat(scala.math.BigDecimal("1E+39")))
  }

  it should "convert the largest REAL itself, NaN and the infinities, byte for byte as before" in {
    val population: Seq[Number] = Seq[Number](
      java.lang.Double.valueOf(Float.MaxValue.toDouble),
      java.lang.Double.valueOf(-Float.MaxValue.toDouble),
      java.lang.Double.valueOf(Double.NaN),
      java.lang.Double.valueOf(Double.PositiveInfinity),
      java.lang.Double.valueOf(Double.NegativeInfinity),
      java.lang.Double.valueOf(1.5),
      java.lang.Double.valueOf(1e-50),
      java.lang.Long.valueOf(Long.MaxValue),
      java.lang.Integer.valueOf(Int.MinValue),
      new JBigDecimal(Float.MaxValue.toDouble),
      new JBigDecimal("1E-1000000000"),
      new BigInteger("340282346638528859811704183484516925440"),
      BigInteger.valueOf(42L),
      scala.math.BigInt(42)
    )
    population.foreach { n =>
      withClue(s"$n (${n.getClass.getName}): ") {
        val answer = ValueCoercion.coerceToFloat(n)
        java.lang.Float.floatToRawIntBits(answer) shouldBe java.lang.Float.floatToRawIntBits(
          n.floatValue()
        )
      }
    }
  }
}
