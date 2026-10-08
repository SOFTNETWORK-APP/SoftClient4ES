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

/*
 * This file contains a Scala port of portions of the {fmt} library, version 6.1.2
 * (https://github.com/fmtlib/fmt/tree/6.1.2):
 *
 *   - include/fmt/format-inl.h: the `fp` class (assign, assign_with_boundaries,
 *     assign_float_with_boundaries, normalize), `multiply`, `get_cached_power` and its tables of
 *     cached powers of 10, `bigint`, `grisu_gen_digits`, `grisu_shortest_handler`,
 *     `fallback_format` and the shortest-representation branch of `format_float`;
 *   - include/fmt/format.h: the `float_writer` layout of the `{}` specifier and
 *     `write_exponent`.
 *
 * The ported parts were read from the copy of {fmt} that DuckDB 1.5.5 embeds (third_party/fmt),
 * where they are identical to upstream {fmt} 6.1.2 (DuckDB's own additions to that copy, the
 * thousand separators, are not ported). {fmt} is distributed under the following license:
 *
 * Copyright (c) 2012 - present, Victor Zverovich
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
 * LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
 * OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
 * WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 *
 * --- Optional exception to the license ---
 *
 * As an exception, if, as a result of your compiling your source code, portions
 * of this Software are embedded into a machine-executable object form of such
 * source code, you may redistribute such embedded portions in such object form
 * without including the above copyright and permission notices.
 */

package app.softnetwork.elastic.client

/** A binary floating-point value as TEXT, spelled as DuckDB 1.5.5 spells it when it casts a
  * `DOUBLE` or a `REAL` to `VARCHAR` (`250000000000.0`, `1e-05`, `1e+16`, `-0.0`, `nan`, `inf`).
  *
  * DuckDB writes such a value with the `{}` format of the fmt library it embeds (fmt 6.1.2): the
  * shortest digits the Grisu algorithm finds, fmt's own bignum fallback where Grisu cannot decide,
  * then fmt's layout -- plain digits for a decimal exponent in [-4, 16), `d.ddde±XX` otherwise, and
  * always a fractional part. This object is a PORT of exactly that code, not an approximation of
  * it: the fallback does not always find the shortest digits (DuckDB spells the double `7.168e25`
  * `7.1680000000000004e+25`, and the float `4073260.75` with all of its digits, where the shortest
  * float would be `4073260.8`), and those spellings are DuckDB's answer too. Java's own
  * `Double.toString` differs from it on layout (`2.5E11`) and, before JDK 19, on digits. MEASURED
  * against DuckDB 1.5.5 on 3.2 million values (2.08 million doubles, 1.12 million floats): every
  * spelling identical.
  *
  * A call allocates about 250 to 290 bytes -- its digit buffer, the Grisu handler, the builder and
  * the result -- and about 1.1 KB for a value the bignum fallback spells, a small fraction of
  * values (Grisu decides the others). MEASURED over the same values.
  */
private[client] object FloatingPointText {

  /** `CAST(<DOUBLE> AS VARCHAR)`. */
  def double(value: Double): String = spell(value, binary32 = false)

  /** `CAST(<REAL> AS VARCHAR)`: the float's own boundaries for Grisu, as fmt formats a `float`. */
  def real(value: Float): String = spell(value.toDouble, binary32 = true)

  private def spell(value: Double, binary32: Boolean): String = {
    val negative = java.lang.Double.doubleToRawLongBits(value) < 0
    val magnitude = if (negative) -value else value
    if (java.lang.Double.isNaN(magnitude)) { if (negative) "-nan" else "nan" }
    else if (java.lang.Double.isInfinite(magnitude)) { if (negative) "-inf" else "inf" }
    else {
      val digits = new Array[Char](DigitCapacity)
      val exponent = new Array[Int](1)
      val count = shortestDigits(magnitude, binary32, digits, exponent)
      val text = new java.lang.StringBuilder(DigitCapacity)
      if (negative) text.append('-')
      layout(text, digits, count, exponent(0))
      text.toString
    }
  }

  private final val DigitCapacity = 40

  // ---------------------------------------------------------------------------------------------
  // fmt's layout of `{}` (float_writer: general format, precision -1, trailing zeros on)
  // ---------------------------------------------------------------------------------------------

  /** `digits` (`count` of them) times 10 ^ `exp`, laid out as fmt's `{}` lays it out. */
  private def layout(
    text: java.lang.StringBuilder,
    digits: Array[Char],
    count: Int,
    exp: Int
  ): Unit = {
    val point = count + exp // the decimal point sits after this many digits
    val scientific = point - 1
    if (scientific < -4 || scientific >= 16) {
      text.append(digits(0))
      if (count > 1) text.append('.').append(digits, 1, count - 1)
      text.append('e')
      var e = scientific
      if (e < 0) { text.append('-'); e = -e }
      else text.append('+')
      if (e >= 100) {
        val hundreds = e / 100
        if (e >= 1000) text.append(('0' + hundreds / 10).toChar)
        text.append(('0' + hundreds % 10).toChar)
        e %= 100
      }
      text.append(('0' + e / 10).toChar).append(('0' + e % 10).toChar)
    } else if (count <= point) {
      text.append(digits, 0, count)
      var zeros = point - count
      while (zeros > 0) { text.append('0'); zeros -= 1 }
      text.append(".0")
    } else if (point > 0) {
      text.append(digits, 0, point).append('.').append(digits, point, count - point)
    } else {
      text.append('0')
      if (point != 0 || count != 0) {
        text.append('.')
        var zeros = -point
        while (zeros > 0) { text.append('0'); zeros -= 1 }
        text.append(digits, 0, count)
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // fmt's format_float (shortest): Grisu, then fallback_format
  // ---------------------------------------------------------------------------------------------

  /** Unsigned 64-bit comparisons: fmt's significands are `uint64_t`. */
  @inline private def ult(a: Long, b: Long): Boolean = (a ^ Long.MinValue) < (b ^ Long.MinValue)
  @inline private def ule(a: Long, b: Long): Boolean = (a ^ Long.MinValue) <= (b ^ Long.MinValue)

  private final val ImplicitBit = 1L << 52
  private final val SignificandMask = ImplicitBit - 1
  private final val ExponentMask = 0x7ff0000000000000L

  /** A handmade floating-point number `f * 2 ^ e` (fmt's `fp`). */
  private final class Fp(var f: Long, var e: Int)

  /** `fp::assign`: the significand and exponent of a POSITIVE double; true when its predecessor is
    * closer than its successor (a normal power of two other than the smallest).
    */
  private def assign(d: Double, fp: Fp): Boolean = {
    val u = java.lang.Double.doubleToRawLongBits(d)
    var f = u & SignificandMask
    var biased = ((u & ExponentMask) >>> 52).toInt
    val predecessorCloser = f == 0 && biased > 1
    if (biased != 0) f += ImplicitBit else biased = 1
    fp.f = f
    fp.e = biased - 1023 - 52
    predecessorCloser
  }

  /** `normalize<SHIFT>`. */
  private def normalize(fp: Fp, shift: Int): Unit = {
    val shiftedImplicitBit = ImplicitBit << shift
    while ((fp.f & shiftedImplicitBit) == 0) {
      fp.f <<= 1
      fp.e -= 1
    }
    val offset = 64 - 52 - shift - 1
    fp.f <<= offset
    fp.e -= offset
  }

  /** `multiply`: `lhs * rhs / 2 ^ 64`, rounded half up (unsigned). */
  private def multiply(lhs: Long, rhs: Long): Long = {
    val mask = 0xffffffffL
    val a = lhs >>> 32
    val b = lhs & mask
    val c = rhs >>> 32
    val d = rhs & mask
    val ac = a * c
    val bc = b * c
    val ad = a * d
    val bd = b * d
    val mid = (bd >>> 32) + (ad & mask) + (bc & mask) + (1L << 31)
    ac + (ad >>> 32) + (bc >>> 32) + (mid >>> 32)
  }

  /** The decimal digits of `value` (> 0 or +0.0) into `digits`, their count returned and the
    * decimal exponent of the LAST digit in `exponent(0)`.
    */
  private def shortestDigits(
    value: Double,
    binary32: Boolean,
    digits: Array[Char],
    exponent: Array[Int]
  ): Int =
    if (value <= 0) {
      digits(0) = '0'
      exponent(0) = 0
      1
    } else {
      val fp = new Fp(0L, 0)
      var lower = 0L
      var upper = 0L
      if (binary32) {
        // assign_float_with_boundaries
        assign(value, fp)
        val minNormalE = -125 - 53
        var halfUlp = 1L << (53 - 24 - 1)
        if (minNormalE > fp.e) halfUlp <<= (minNormalE - fp.e)
        val up = new Fp(fp.f + halfUlp, fp.e)
        normalize(up, 0)
        val low = fp.f - (halfUlp >>> (if (fp.f == ImplicitBit && fp.e > minNormalE) 1 else 0))
        lower = low << (fp.e - up.e)
        upper = up.f
      } else {
        // assign_with_boundaries
        val lowerCloser = assign(value, fp)
        val lowF = if (lowerCloser) (fp.f << 2) - 1 else (fp.f << 1) - 1
        val lowE = if (lowerCloser) fp.e - 2 else fp.e - 1
        val up = new Fp((fp.f << 1) + 1, fp.e - 1)
        normalize(up, 1)
        lower = lowF << (lowE - up.e)
        upper = up.f
      }
      normalize(fp, 0)
      // a cached power of 10 bringing the exponent into [-60, -32]
      val index0 = (((-60 - (fp.e + 64)) + 64 - 1).toLong * 0x4d104d42L + 0xffffffffL) >> 32
      val index = (index0.toInt - -348 - 1) / 8 + 1
      val cachedExp10 = -348 + index * 8
      val cachedF = Pow10Significands(index)
      val valueF = multiply(fp.f, cachedF)
      val valueE = fp.e + Pow10Exponents(index) + 64
      lower = multiply(lower, cachedF) - 1
      upper = multiply(upper, cachedF) + 1
      val handler = new ShortestHandler(digits, upper - valueF)
      val kappa = new Array[Int](1)
      if (grisuGenDigits(upper, valueE, upper - lower, kappa, handler) == Error) {
        val exp10 = kappa(0) + handler.size - cachedExp10 - 1
        fallbackFormat(value, digits, exp10, exponent)
      } else {
        exponent(0) = kappa(0) - cachedExp10
        handler.size
      }
    }

  private final val More = 0
  private final val Done = 1
  private final val Error = 2

  /** `grisu_shortest_handler`: Grisu's digit-by-digit `round_weed`. */
  private final class ShortestHandler(val buf: Array[Char], val diff: Long) {
    var size = 0

    private def round(d: Long, divisor: Long, remainder0: Long, error: Long): Long = {
      var remainder = remainder0
      while (
        ult(remainder, d) && !ult(error - remainder, divisor) &&
        (ult(remainder + divisor, d) || !ult(d - remainder, remainder + divisor - d))
      ) {
        buf(size - 1) = (buf(size - 1) - 1).toChar
        remainder += divisor
      }
      remainder
    }

    def onDigit(
      digit: Char,
      divisor: Long,
      remainder0: Long,
      error: Long,
      exp: Int,
      integral: Boolean
    ): Int = {
      buf(size) = digit
      size += 1
      if (!ult(remainder0, error)) More
      else {
        val unit = if (integral) 1L else PowersOf10(-exp)
        val up = (diff - 1) * unit
        val remainder = round(up, divisor, remainder0, error)
        val down = (diff + 1) * unit
        if (
          ult(remainder, down) && !ult(error - remainder, divisor) &&
          (ult(remainder + divisor, down) || ult(remainder + divisor - down, down - remainder))
        ) Error
        else if (ule(2 * unit, remainder) && ule(remainder, error - 4 * unit)) Done
        else Error
      }
    }
  }

  private def countDigits(n: Long): Int = {
    var count = 1
    var rest = n
    while (rest >= 10) {
      rest /= 10
      count += 1
    }
    count
  }

  /** `grisu_gen_digits` for the scaled upper boundary `valueF * 2 ^ valueE`; the decimal exponent
    * (kappa) it ends on in `kappa(0)`.
    */
  private def grisuGenDigits(
    valueF: Long,
    valueE: Int,
    error0: Long,
    kappa: Array[Int],
    handler: ShortestHandler
  ): Int = {
    val shift = -valueE
    val one = 1L << shift
    var integral = valueF >>> shift
    var fractional = valueF & (one - 1)
    var error = error0
    var exp = countDigits(integral)
    var result = More
    // the integral part: up to 10 digits
    while (result == More && exp > 0) {
      val divisor = PowersOf10(exp - 1)
      val digit = integral / divisor
      integral = integral % divisor
      exp -= 1
      val remainder = (integral << shift) + fractional
      result = handler.onDigit(
        ('0' + digit).toChar,
        PowersOf10(exp) << shift,
        remainder,
        error,
        exp,
        integral = true
      )
    }
    // the fractional part
    while (result == More) {
      fractional *= 10
      error *= 10
      val digit = ('0' + (fractional >>> shift)).toChar
      fractional &= one - 1
      exp -= 1
      result = handler.onDigit(digit, one, fractional, error, exp, integral = false)
    }
    kappa(0) = exp
    result
  }

  /** `fallback_format`: Steele & White's (FPP)² over fmt's bignum, double boundaries (fmt's "TODO:
    * handle float") -- the digits into `digits`, their count returned, the exponent of the last one
    * in `exponent(0)`.
    */
  private def fallbackFormat(
    d: Double,
    digits: Array[Char],
    exp10: Int,
    exponent: Array[Int]
  ): Int = {
    val numerator = new Bignum
    val denominator = new Bignum
    val lower = new Bignum
    val upperStore = new Bignum
    var upper: Bignum = null
    val value = new Fp(0L, 0)
    val shift = if (assign(d, value)) 2 else 1
    val significand = value.f << shift
    if (value.e >= 0) {
      numerator.assign(significand)
      numerator.shiftLeft(value.e)
      lower.assign(1L)
      lower.shiftLeft(value.e)
      if (shift != 1) {
        upperStore.assign(1L)
        upperStore.shiftLeft(value.e + 1)
        upper = upperStore
      }
      denominator.assignPow10(exp10)
      denominator.shiftLeft(1)
    } else if (exp10 < 0) {
      numerator.assignPow10(-exp10)
      lower.assign(numerator)
      if (shift != 1) {
        upperStore.assign(numerator)
        upperStore.shiftLeft(1)
        upper = upperStore
      }
      numerator.multiply64(significand)
      denominator.assign(1L)
      denominator.shiftLeft(shift - value.e)
    } else {
      numerator.assign(significand)
      denominator.assignPow10(exp10)
      denominator.shiftLeft(shift - value.e)
      lower.assign(1L)
      if (shift != 1) {
        upperStore.assign(1L << 1)
        upper = upperStore
      }
    }
    if (upper == null) upper = lower
    val even = if ((value.f & 1) == 0) 1 else 0
    var count = 0
    var finished = false
    while (!finished) {
      val digit = numerator.divmodAssign(denominator)
      val low = Bignum.compare(numerator, lower) - even < 0
      val high = Bignum.addCompare(numerator, upper, denominator) + even > 0
      digits(count) = ('0' + digit).toChar
      count += 1
      if (low || high) {
        if (!low) digits(count - 1) = (digits(count - 1) + 1).toChar
        else if (high) {
          val result = Bignum.addCompare(numerator, numerator, denominator)
          if (result > 0 || (result == 0 && digit % 2 != 0))
            digits(count - 1) = (digits(count - 1) + 1).toChar
        }
        finished = true
      } else {
        numerator.multiply32(10)
        lower.multiply32(10)
        if (upper ne lower) upper.multiply32(10)
      }
    }
    exponent(0) = exp10 - (count - 1)
    count
  }

  /** fmt's `bigint`, bit for bit: 32-bit "bigits", least significant first, scaled by 2 ^ (32 *
    * `exp`). Its representation is part of the answer -- `compare` reads it, not only the value --
    * so it is reproduced operation by operation.
    */
  private final class Bignum {
    var bigits: Array[Int] = new Array[Int](40)
    var size: Int = 0
    var exp: Int = 0

    private def reserve(n: Int): Unit =
      if (n > bigits.length) {
        val grown = new Array[Int](math.max(n, bigits.length * 2))
        System.arraycopy(bigits, 0, grown, 0, size)
        bigits = grown
      }

    def resize(n: Int): Unit = {
      reserve(n)
      size = n
    }

    def pushBack(bigit: Int): Unit = {
      reserve(size + 1)
      bigits(size) = bigit
      size += 1
    }

    def numBigits: Int = size + exp

    def assign(n0: Long): Unit = {
      var n = n0
      var count = 0
      // fmt's do-while: at least one bigit, also for 0
      var more = true
      while (more) {
        reserve(count + 1)
        bigits(count) = n.toInt
        count += 1
        n >>>= 32
        more = n != 0
      }
      size = count
      exp = 0
    }

    def assign(other: Bignum): Unit = {
      resize(other.size)
      System.arraycopy(other.bigits, 0, bigits, 0, other.size)
      exp = other.exp
    }

    def shiftLeft(shift0: Int): Unit = {
      exp += shift0 / 32
      val shift = shift0 % 32
      if (shift != 0) {
        var carry = 0
        var i = 0
        val n = size
        while (i < n) {
          val c = bigits(i) >>> (32 - shift)
          bigits(i) = (bigits(i) << shift) + carry
          carry = c
          i += 1
        }
        if (carry != 0) pushBack(carry)
      }
    }

    def multiply32(value: Int): Unit = {
      val wide = value & 0xffffffffL
      var carry = 0L
      var i = 0
      val n = size
      while (i < n) {
        val result = (bigits(i) & 0xffffffffL) * wide + carry
        bigits(i) = result.toInt
        carry = result >>> 32
        i += 1
      }
      if (carry != 0) pushBack(carry.toInt)
    }

    def multiply64(value: Long): Unit = {
      val mask = 0xffffffffL
      val lowerHalf = value & mask
      val upperHalf = value >>> 32
      var carry = 0L
      var i = 0
      val n = size
      while (i < n) {
        val bigit = bigits(i) & mask
        val result = bigit * lowerHalf + (carry & mask)
        carry = bigit * upperHalf + (result >>> 32) + (carry >>> 32)
        bigits(i) = result.toInt
        i += 1
      }
      while (carry != 0) {
        pushBack((carry & mask).toInt)
        carry >>>= 32
      }
    }

    private def removeLeadingZeros(): Unit = {
      var top = size - 1
      while (top > 0 && bigits(top) == 0) top -= 1
      size = top + 1
    }

    /** `subtract_bigits`: the new borrow. */
    private def subtractBigits(index: Int, other: Int, borrow: Int): Int = {
      reserve(index + 1)
      val result = (bigits(index) & 0xffffffffL) - (other & 0xffffffffL) - (borrow & 0xffffffffL)
      bigits(index) = result.toInt
      (result >>> 63).toInt
    }

    /** `subtract_aligned`, including its last loop, which keeps subtracting at ONE index. */
    private def subtractAligned(other: Bignum): Unit = {
      var borrow = 0
      var i = other.exp - exp
      var j = 0
      while (j != other.size) {
        borrow = subtractBigits(i, other.bigits(j), borrow)
        i += 1
        j += 1
      }
      while (borrow > 0) borrow = subtractBigits(i, 0, borrow)
      removeLeadingZeros()
    }

    def assignPow10(exp10: Int): Unit =
      if (exp10 == 0) assign(1L)
      else {
        var bitmask = 1
        while (exp10 >= bitmask) bitmask <<= 1
        bitmask >>= 1
        assign(5L)
        bitmask >>= 1
        while (bitmask != 0) {
          square()
          if ((exp10 & bitmask) != 0) multiply32(5)
          bitmask >>= 1
        }
        shiftLeft(exp10)
      }

    /** `square`, its 128-bit accumulator as two words (fmt's `accumulator`). */
    private def square(): Unit = {
      val n = java.util.Arrays.copyOf(bigits, size)
      val count = size
      val resultCount = 2 * count
      resize(resultCount)
      var sumLow = 0L
      var sumHigh = 0L
      def add(term: Long): Unit = {
        sumLow += term
        if (ult(sumLow, term)) sumHigh += 1
      }
      def shiftSum(): Unit = {
        sumLow = (sumHigh << 32) | (sumLow >>> 32)
        sumHigh >>>= 32
      }
      var index = 0
      while (index < count) {
        var i = 0
        var j = index
        while (j >= 0) {
          add((n(i) & 0xffffffffL) * (n(j) & 0xffffffffL))
          i += 1
          j -= 1
        }
        bigits(index) = sumLow.toInt
        shiftSum()
        index += 1
      }
      index = count
      while (index < resultCount) {
        var j = count - 1
        var i = index - j
        while (i < count) {
          add((n(i) & 0xffffffffL) * (n(j) & 0xffffffffL))
          i += 1
          j -= 1
        }
        bigits(index) = sumLow.toInt
        shiftSum()
        index += 1
      }
      removeLeadingZeros()
      exp *= 2
    }

    /** `divmod_assign`: this becomes this % divisor; the quotient is returned. */
    def divmodAssign(divisor: Bignum): Int =
      if (Bignum.compare(this, divisor) < 0) 0
      else {
        val count = size
        val expDifference = exp - divisor.exp
        if (expDifference > 0) {
          resize(count + expDifference)
          var i = count - 1
          var j = i + expDifference
          while (i >= 0) {
            bigits(j) = bigits(i)
            i -= 1
            j -= 1
          }
          java.util.Arrays.fill(bigits, 0, expDifference, 0)
          exp -= expDifference
        }
        var quotient = 0
        // fmt's do-while: this >= divisor here, so it subtracts at least once
        var more = true
        while (more) {
          subtractAligned(divisor)
          quotient += 1
          more = Bignum.compare(this, divisor) >= 0
        }
        quotient
      }
  }

  private object Bignum {

    /** `compare`: reads the representation (a lower bigit run decides, zero or not). */
    def compare(lhs: Bignum, rhs: Bignum): Int = {
      val lhsBigits = lhs.numBigits
      val rhsBigits = rhs.numBigits
      if (lhsBigits != rhsBigits) { if (lhsBigits > rhsBigits) 1 else -1 }
      else {
        var i = lhs.size - 1
        var j = rhs.size - 1
        var end = i - j
        if (end < 0) end = 0
        var answer = 0
        var decided = false
        while (!decided && i >= end) {
          val a = lhs.bigits(i) & 0xffffffffL
          val b = rhs.bigits(j) & 0xffffffffL
          if (a != b) {
            answer = if (a > b) 1 else -1
            decided = true
          } else {
            i -= 1
            j -= 1
          }
        }
        if (decided) answer
        else if (i != j) { if (i > j) 1 else -1 }
        else 0
      }
    }

    /** `add_compare`: the sign of `lhs1 + lhs2 - rhs`. */
    def addCompare(lhs1: Bignum, lhs2: Bignum, rhs: Bignum): Int = {
      val maxLhsBigits = math.max(lhs1.numBigits, lhs2.numBigits)
      val rhsBigits = rhs.numBigits
      if (maxLhsBigits + 1 < rhsBigits) -1
      else if (maxLhsBigits > rhsBigits) 1
      else {
        def bigitAt(n: Bignum, i: Int): Long =
          if (i >= n.exp && i < n.numBigits) n.bigits(i - n.exp) & 0xffffffffL else 0L
        var borrow = 0L
        val minExp = math.min(math.min(lhs1.exp, lhs2.exp), rhs.exp)
        var i = rhsBigits - 1
        var answer = 0
        var decided = false
        while (!decided && i >= minExp) {
          val sum = bigitAt(lhs1, i) + bigitAt(lhs2, i)
          val rhsBigit = bigitAt(rhs, i)
          if (sum > rhsBigit + borrow) {
            answer = 1
            decided = true
          } else {
            borrow = rhsBigit + borrow - sum
            if (borrow > 1) {
              answer = -1
              decided = true
            } else {
              borrow <<= 32
              i -= 1
            }
          }
        }
        if (decided) answer else if (borrow != 0) -1 else 0
      }
    }
  }

  /** `powers_of_10_64`: 10 ^ 0 to 10 ^ 19, unsigned. */
  private val PowersOf10: Array[Long] = {
    val powers = new Array[Long](20)
    var p = 1L
    var i = 0
    while (i < 20) {
      powers(i) = p
      p *= 10
      i += 1
    }
    powers
  }

  /** Normalized 64-bit significands of 10 ^ k, k = -348, -340, ..., 340 (fmt's table). */
  private val Pow10Significands: Array[Long] = Array(
    0xfa8fd5a0081c0288L, 0xbaaee17fa23ebf76L, 0x8b16fb203055ac76L, 0xcf42894a5dce35eaL,
    0x9a6bb0aa55653b2dL, 0xe61acf033d1a45dfL, 0xab70fe17c79ac6caL, 0xff77b1fcbebcdc4fL,
    0xbe5691ef416bd60cL, 0x8dd01fad907ffc3cL, 0xd3515c2831559a83L, 0x9d71ac8fada6c9b5L,
    0xea9c227723ee8bcbL, 0xaecc49914078536dL, 0x823c12795db6ce57L, 0xc21094364dfb5637L,
    0x9096ea6f3848984fL, 0xd77485cb25823ac7L, 0xa086cfcd97bf97f4L, 0xef340a98172aace5L,
    0xb23867fb2a35b28eL, 0x84c8d4dfd2c63f3bL, 0xc5dd44271ad3cdbaL, 0x936b9fcebb25c996L,
    0xdbac6c247d62a584L, 0xa3ab66580d5fdaf6L, 0xf3e2f893dec3f126L, 0xb5b5ada8aaff80b8L,
    0x87625f056c7c4a8bL, 0xc9bcff6034c13053L, 0x964e858c91ba2655L, 0xdff9772470297ebdL,
    0xa6dfbd9fb8e5b88fL, 0xf8a95fcf88747d94L, 0xb94470938fa89bcfL, 0x8a08f0f8bf0f156bL,
    0xcdb02555653131b6L, 0x993fe2c6d07b7facL, 0xe45c10c42a2b3b06L, 0xaa242499697392d3L,
    0xfd87b5f28300ca0eL, 0xbce5086492111aebL, 0x8cbccc096f5088ccL, 0xd1b71758e219652cL,
    0x9c40000000000000L, 0xe8d4a51000000000L, 0xad78ebc5ac620000L, 0x813f3978f8940984L,
    0xc097ce7bc90715b3L, 0x8f7e32ce7bea5c70L, 0xd5d238a4abe98068L, 0x9f4f2726179a2245L,
    0xed63a231d4c4fb27L, 0xb0de65388cc8ada8L, 0x83c7088e1aab65dbL, 0xc45d1df942711d9aL,
    0x924d692ca61be758L, 0xda01ee641a708deaL, 0xa26da3999aef774aL, 0xf209787bb47d6b85L,
    0xb454e4a179dd1877L, 0x865b86925b9bc5c2L, 0xc83553c5c8965d3dL, 0x952ab45cfa97a0b3L,
    0xde469fbd99a05fe3L, 0xa59bc234db398c25L, 0xf6c69a72a3989f5cL, 0xb7dcbf5354e9beceL,
    0x88fcf317f22241e2L, 0xcc20ce9bd35c78a5L, 0x98165af37b2153dfL, 0xe2a0b5dc971f303aL,
    0xa8d9d1535ce3b396L, 0xfb9b7cd9a4a7443cL, 0xbb764c4ca7a44410L, 0x8bab8eefb6409c1aL,
    0xd01fef10a657842cL, 0x9b10a4e5e9913129L, 0xe7109bfba19c0c9dL, 0xac2820d9623bf429L,
    0x80444b5e7aa7cf85L, 0xbf21e44003acdd2dL, 0x8e679c2f5e44ff8fL, 0xd433179d9c8cb841L,
    0x9e19db92b4e31ba9L, 0xeb96bf6ebadf77d9L, 0xaf87023b9bf0ee6bL
  )

  /** Binary exponents of the same powers of 10 (fmt's table). */
  private val Pow10Exponents: Array[Int] = Array(
    -1220, -1193, -1166, -1140, -1113, -1087, -1060, -1034, -1007, -980, -954, -927, -901, -874,
    -847, -821, -794, -768, -741, -715, -688, -661, -635, -608, -582, -555, -529, -502, -475, -449,
    -422, -396, -369, -343, -316, -289, -263, -236, -210, -183, -157, -130, -103, -77, -50, -24, 3,
    30, 56, 83, 109, 136, 162, 189, 216, 242, 269, 295, 322, 348, 375, 402, 428, 455, 481, 508, 534,
    561, 588, 614, 641, 667, 694, 720, 747, 774, 800, 827, 853, 880, 907, 933, 960, 986, 1013, 1039,
    1066
  )
}
