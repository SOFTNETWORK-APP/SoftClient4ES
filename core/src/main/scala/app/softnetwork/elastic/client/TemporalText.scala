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
 * TemporalText follows the text layout of DuckDB 1.5.5
 * (https://github.com/duckdb/duckdb/tree/v1.5.5) for a DATE, a TIME and a TIMESTAMP cast to
 * VARCHAR, whose logic it translates: `DateToStringCast` (YearLength, Format) and
 * `TimeToStringCast` (FormatMicros, MicrosLength, Format) of
 * src/include/duckdb/common/types/cast_helpers.hpp, and `StringFromTimestamp` / `StringAsTime` of
 * src/common/operator/string_cast.cpp -- the year on at least four digits, a year before 1 written
 * as its BC year followed by " (BC)", and the fraction of a second without its trailing zeros.
 * DuckDB is distributed under the following license:
 *
 * Copyright 2018-2025 Stichting DuckDB Foundation
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and
 * associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT
 * NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */

package app.softnetwork.elastic.client

import java.time.{LocalDate, LocalDateTime, LocalTime}

/** A `DATE`, a `TIMESTAMP` and a `TIME` as TEXT, spelled as DuckDB 1.5.5 casts them to `VARCHAR`
  * (`DateToStringCast` / `TimeToStringCast`): the year on at least four digits, `(BC)` after the
  * date of a year before 1 (`0044-03-15 (BC)`), a timestamp's date and time separated by a space,
  * and the fraction of a second only when there is one, without its trailing zeros (`23:59:59.999`,
  * `10:30:00.5`). MEASURED on DuckDB 1.5.5, every case.
  */
private[client] object TemporalText {

  def date(d: LocalDate): String = {
    val text = new java.lang.StringBuilder(16)
    appendDate(text, d.getYear, d.getMonthValue, d.getDayOfMonth)
    text.toString
  }

  def timestamp(t: LocalDateTime): String = {
    val text = new java.lang.StringBuilder(32)
    appendDate(text, t.getYear, t.getMonthValue, t.getDayOfMonth)
    text.append(' ')
    appendTime(text, t.getHour, t.getMinute, t.getSecond, t.getNano)
    text.toString
  }

  def time(t: LocalTime): String = {
    val text = new java.lang.StringBuilder(18)
    appendTime(text, t.getHour, t.getMinute, t.getSecond, t.getNano)
    text.toString
  }

  private def appendDate(text: java.lang.StringBuilder, year: Int, month: Int, day: Int): Unit = {
    // DuckDB's year is proleptic, as java.time's: year 0 is 1 BC
    val bc = year <= 0
    val digits = Integer.toString(if (bc) 1 - year else year)
    var padding = 4 - digits.length
    while (padding > 0) {
      text.append('0')
      padding -= 1
    }
    text.append(digits).append('-')
    appendTwoDigits(text, month)
    text.append('-')
    appendTwoDigits(text, day)
    if (bc) text.append(" (BC)")
  }

  private def appendTime(
    text: java.lang.StringBuilder,
    hour: Int,
    minute: Int,
    second: Int,
    nano: Int
  ): Unit = {
    appendTwoDigits(text, hour)
    text.append(':')
    appendTwoDigits(text, minute)
    text.append(':')
    appendTwoDigits(text, second)
    if (nano != 0) {
      // nine digits, the trailing zeros dropped: `.5`, `.999`, `.123456789`
      val fraction = Integer.toString(nano)
      var last = fraction.length
      while (fraction.charAt(last - 1) == '0') last -= 1
      text.append('.')
      var padding = 9 - fraction.length
      while (padding > 0) {
        text.append('0')
        padding -= 1
      }
      text.append(fraction, 0, last)
    }
  }

  private def appendTwoDigits(text: java.lang.StringBuilder, value: Int): Unit =
    text.append(('0' + value / 10).toChar).append(('0' + value % 10).toChar)
}
