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

import java.time.temporal.{ChronoUnit, IsoFields}
import java.time.{Clock, DayOfWeek, Instant, LocalDate, LocalDateTime, ZoneOffset}
import java.util.Locale
import scala.util.Try

/** A custom Joda-Time date pattern, read as Elasticsearch 6 reads it. Every reading below is
  * MEASURED on 6.8.23 (each letter `G C Y x w e E y D M d a K h H k m s S z Z` at each width, alone
  * and combined, with edge values; the instant read back from the doc value) and written from that
  * behaviour and Java's own API, never from Joda-Time's code:
  *
  *   - first, the text is read left to right, each letter at its place: a number of one digit up to
  *     a maximum (2 for `M d H h K k m s w`, 3 for `D`, 1 for `e`, the letter count for `S`, `C`
  *     and a repeated letter; 9 for a year, or its letter count when a number follows), a year
  *     signed (not a year of era), `yy` `YY` `xx` two digits around the current year minus 30 (any
  *     other count of digits as written); a name in any case, short or full (`MMM`, `E`, `G`, `a`);
  *     an offset (`Z`, `ZZ`: `Z`, `z`, `+HH`, `+HHmm`, `+HH:mm`, with seconds, up to 19 hours); a
  *     zone id (`ZZZ`); one of eleven zone names (`z`: `UT`, `UTC`, `GMT` and the North American
  *     `EST` ... `PDT`); literal text in any case;
  *   - then the values are applied to 1970-01-01T00:00, largest range first (the year, the week
  *     year and the era before the month, the week, the day of year; the day of month; the day of
  *     week; then the time), each one resetting the smaller fields to their first value, and
  *     applied a second time, the last one resetting; when the first is a month, a week or a day,
  *     the year is 1970; a value out of its field's range there REJECTS the text;
  *   - last, the zone: an offset as given; `UTC` and `GMT` (`ZZZ`), `UT`, `UTC` and `GMT` (`z`) as
  *     UTC.
  *
  * A letter or a width no measurement covers, a combination of letter classes outside the measured
  * ones, two zone letters, and any other zone id or name -- a region (`Europe/Paris`), an
  * abbreviation (`EST`, `PDT`), whose offset comes from zone data, the JVM core runs on's and not
  * Elasticsearch's -- are [[MappedDateFormat.Unmeasured]].
  */
private[client] object JodaDatePattern {

  import MappedDateFormat._

  // ---------------------------------------------------------------------------------------------
  // The fields, ordered as Elasticsearch 6 applies them (MEASURED): by the size of their range,
  // then of their unit
  // ---------------------------------------------------------------------------------------------

  private sealed abstract class Field(val letter: Char, val range: Int, val unit: Int) {

    /** The local date-time with this field set to `v`; throws when `v` is out of its range. */
    def set(t: LocalDateTime, v: Int): LocalDateTime

    /** The start of this field's unit. */
    def floor(t: LocalDateTime): LocalDateTime
  }

  // range and unit sizes: no range (a year) 100, eras 100, centuries 90, years 80, months 70,
  // weeks 60, days 50, half days 40, hours 30, minutes 20, seconds 10, millis 0
  private def check(v: Int, min: Int, max: Int): Int =
    if (v < min || v > max) throw new IllegalArgumentException(s"$v out of [$min, $max]")
    else v

  private def startOfYear(y: Int): LocalDateTime = LocalDateTime.of(y, 1, 1, 0, 0)

  private def weekYear(t: LocalDateTime): Int = t.get(IsoFields.WEEK_BASED_YEAR)
  private def week(t: LocalDateTime): Int = t.get(IsoFields.WEEK_OF_WEEK_BASED_YEAR)

  private def weeksIn(weekYear: Int): Int =
    IsoFields.WEEK_OF_WEEK_BASED_YEAR.rangeRefinedBy(LocalDate.of(weekYear, 1, 4)).getMaximum.toInt

  private def firstWeekDay(weekYear: Int): LocalDate =
    LocalDate
      .of(weekYear, 1, 4)
      .`with`(IsoFields.WEEK_OF_WEEK_BASED_YEAR, 1L)
      .`with`(DayOfWeek.MONDAY)

  private case object Era extends Field('G', 100, 100) {
    def set(t: LocalDateTime, v: Int): LocalDateTime = {
      check(v, 0, 1)
      if ((t.getYear <= 0) != (v == 0)) t.withYear(1 - t.getYear) else t
    }
    // the first instant of the era: AD 1 for AD, the first representable instant for BC
    def floor(t: LocalDateTime): LocalDateTime =
      if (t.getYear >= 1) startOfYear(1)
      else LocalDateTime.ofInstant(Instant.ofEpochMilli(Long.MinValue), ZoneOffset.UTC)
  }

  /** The century of the year's absolute value, its sign kept (MEASURED: a century 0 reads years 0
    * to 99).
    */
  private case object CenturyOfEra extends Field('C', 100, 90) {
    private def withAbsoluteYear(t: LocalDateTime, v: Int) =
      t.withYear(if (t.getYear < 0) -v else v)
    def set(t: LocalDateTime, v: Int): LocalDateTime =
      withAbsoluteYear(t, check(v, 0, 2922789) * 100 + Math.abs(t.getYear) % 100)
    def floor(t: LocalDateTime): LocalDateTime =
      startOfYear(withAbsoluteYear(t, (Math.abs(t.getYear) / 100) * 100).getYear)
  }

  private case object YearOfEra extends Field('Y', 100, 80) {
    def set(t: LocalDateTime, v: Int): LocalDateTime = {
      check(v, 1, 292278993)
      t.withYear(if (t.getYear <= 0) 1 - v else v)
    }
    def floor(t: LocalDateTime): LocalDateTime = startOfYear(t.getYear)
  }

  private case object Year extends Field('y', 100, 80) {
    def set(t: LocalDateTime, v: Int): LocalDateTime = t.withYear(check(v, -292275054, 292278993))
    def floor(t: LocalDateTime): LocalDateTime = startOfYear(t.getYear)
  }

  private case object WeekYear extends Field('x', 100, 80) {
    def set(t: LocalDateTime, v: Int): LocalDateTime = {
      check(v, -292275054, 292278993)
      if (v == weekYear(t)) t
      else {
        val w = Math.min(week(t), weeksIn(v))
        LocalDateTime.of(
          firstWeekDay(v).plusWeeks(w - 1L).plusDays(t.getDayOfWeek.getValue - 1L),
          t.toLocalTime
        )
      }
    }
    def floor(t: LocalDateTime): LocalDateTime = firstWeekDay(weekYear(t)).atStartOfDay()
  }

  private case object MonthOfYear extends Field('M', 80, 70) {
    def set(t: LocalDateTime, v: Int): LocalDateTime = t.withMonth(check(v, 1, 12))
    def floor(t: LocalDateTime): LocalDateTime = t.toLocalDate.withDayOfMonth(1).atStartOfDay()
  }

  private case object WeekOfWeekYear extends Field('w', 80, 60) {
    def set(t: LocalDateTime, v: Int): LocalDateTime =
      t.plusWeeks(check(v, 1, weeksIn(weekYear(t))).toLong - week(t))
    def floor(t: LocalDateTime): LocalDateTime =
      t.toLocalDate.`with`(DayOfWeek.MONDAY).atStartOfDay()
  }

  private case object DayOfYear extends Field('D', 80, 50) {
    def set(t: LocalDateTime, v: Int): LocalDateTime =
      t.withDayOfYear(check(v, 1, t.toLocalDate.lengthOfYear))
    def floor(t: LocalDateTime): LocalDateTime = t.toLocalDate.atStartOfDay()
  }

  private case object DayOfMonth extends Field('d', 70, 50) {
    def set(t: LocalDateTime, v: Int): LocalDateTime =
      t.withDayOfMonth(check(v, 1, t.toLocalDate.lengthOfMonth))
    def floor(t: LocalDateTime): LocalDateTime = t.toLocalDate.atStartOfDay()
  }

  private case object DayOfWeekField extends Field('e', 60, 50) {
    def set(t: LocalDateTime, v: Int): LocalDateTime =
      t.plusDays(check(v, 1, 7).toLong - t.getDayOfWeek.getValue)
    def floor(t: LocalDateTime): LocalDateTime = t.toLocalDate.atStartOfDay()
  }

  private case object HalfDay extends Field('a', 50, 40) {
    def set(t: LocalDateTime, v: Int): LocalDateTime =
      t.withHour(t.getHour % 12 + 12 * check(v, 0, 1))
    def floor(t: LocalDateTime): LocalDateTime =
      t.truncatedTo(ChronoUnit.DAYS).withHour(if (t.getHour >= 12) 12 else 0)
  }

  private case object HourOfHalfDay extends Field('K', 40, 30) {
    def set(t: LocalDateTime, v: Int): LocalDateTime =
      t.withHour((t.getHour / 12) * 12 + check(v, 0, 11))
    def floor(t: LocalDateTime): LocalDateTime = t.truncatedTo(ChronoUnit.HOURS)
  }

  private case object ClockHourOfHalfDay extends Field('h', 40, 30) {
    def set(t: LocalDateTime, v: Int): LocalDateTime = HourOfHalfDay.set(t, check(v, 1, 12) % 12)
    def floor(t: LocalDateTime): LocalDateTime = t.truncatedTo(ChronoUnit.HOURS)
  }

  private case object HourOfDay extends Field('H', 50, 30) {
    def set(t: LocalDateTime, v: Int): LocalDateTime = t.withHour(check(v, 0, 23))
    def floor(t: LocalDateTime): LocalDateTime = t.truncatedTo(ChronoUnit.HOURS)
  }

  private case object ClockHourOfDay extends Field('k', 50, 30) {
    def set(t: LocalDateTime, v: Int): LocalDateTime = t.withHour(check(v, 1, 24) % 24)
    def floor(t: LocalDateTime): LocalDateTime = t.truncatedTo(ChronoUnit.HOURS)
  }

  private case object MinuteOfHour extends Field('m', 30, 20) {
    def set(t: LocalDateTime, v: Int): LocalDateTime = t.withMinute(check(v, 0, 59))
    def floor(t: LocalDateTime): LocalDateTime = t.truncatedTo(ChronoUnit.MINUTES)
  }

  private case object SecondOfMinute extends Field('s', 20, 10) {
    def set(t: LocalDateTime, v: Int): LocalDateTime = t.withSecond(check(v, 0, 59))
    def floor(t: LocalDateTime): LocalDateTime = t.truncatedTo(ChronoUnit.SECONDS)
  }

  private case object MillisOfSecond extends Field('S', 10, 0) {
    def set(t: LocalDateTime, v: Int): LocalDateTime = t.withNano(check(v, 0, 999) * 1000000)
    def floor(t: LocalDateTime): LocalDateTime = t
  }

  /** The local date-time the values give (MEASURED): see the object's documentation. */
  private def resolve(saved: Seq[(Field, Int)]): LocalDateTime = {
    def order(fields: Seq[(Field, Int)]) = fields.sortBy { case (f, _) => (-f.range, -f.unit) }
    val sorted0 = order(saved)
    val sorted =
      if (sorted0.nonEmpty && sorted0.head._1.unit >= 50 && sorted0.head._1.unit <= 70)
        order(sorted0 :+ (Year -> 1970))
      else sorted0
    var t = LocalDateTime.of(1970, 1, 1, 0, 0)
    sorted.foreach { case (f, v) => t = f.floor(f.set(t, v)) }
    sorted.zipWithIndex.foreach { case ((f, v), i) =>
      t = f.set(t, v)
      if (i == sorted.size - 1) t = f.floor(t)
    }
    t
  }

  // ---------------------------------------------------------------------------------------------
  // The pattern
  // ---------------------------------------------------------------------------------------------

  private sealed trait Element
  private final case class Literal(text: String) extends Element
  private final case class Number(field: Field, maxDigits: Int, signed: Boolean) extends Element
  private final case class TwoDigits(field: Field, lenient: Boolean) extends Element
  private final case class Fraction(maxDigits: Int) extends Element
  private final case class Text(field: Field, texts: Map[String, Int]) extends Element
  private case object Offset extends Element
  private case object RegionId extends Element
  private case object ZoneName extends Element

  private val Months = Seq(
    "january",
    "february",
    "march",
    "april",
    "may",
    "june",
    "july",
    "august",
    "september",
    "october",
    "november",
    "december"
  )
  private val MonthTexts: Map[String, Int] =
    Months.zipWithIndex.flatMap { case (m, i) => Seq(m -> (i + 1), m.take(3) -> (i + 1)) }.toMap
  private val DayTexts: Map[String, Int] =
    Seq(
      "monday",
      "tuesday",
      "wednesday",
      "thursday",
      "friday",
      "saturday",
      "sunday"
    ).zipWithIndex.flatMap { case (d, i) => Seq(d -> (i + 1), d.take(3) -> (i + 1)) }.toMap
  private val EraTexts: Map[String, Int] = Map("bc" -> 0, "ad" -> 1)
  private val HalfDayTexts: Map[String, Int] = Map("am" -> 0, "pm" -> 1)

  /** The zone names Elasticsearch 6 reads with `z`, `zz`, `zzz` (MEASURED, winter and summer: every
    * other name is rejected): core reads `UT`, `UTC` and `GMT`, UTC; the North American names take
    * their offset from zone data, and are not read.
    */
  private val ZoneNames: Seq[String] =
    Seq("UT", "UTC", "GMT", "EST", "EDT", "CST", "CDT", "MST", "MDT", "PST", "PDT")

  private val FixedNames: Set[String] = Set("UT", "UTC", "GMT")

  /** A text's values by the elements, its offset in seconds, and why core does not read its zone,
    * if it does not.
    */
  private final case class Lexed(
    saved: Seq[(Field, Int)],
    offsetSeconds: Int,
    unread: Option[String]
  )

  /** The widths of each letter whose values were read back on 6.8.23. */
  private val Widths: Map[Char, Set[Int]] = Map(
    'G' -> (1 to 5).toSet,
    'C' -> (1 to 4).toSet,
    'Y' -> (1 to 5).toSet,
    'x' -> (1 to 5).toSet,
    'w' -> (1 to 2).toSet,
    'e' -> (1 to 5).toSet,
    'E' -> (1 to 5).toSet,
    'y' -> (1 to 5).toSet,
    'D' -> (1 to 3).toSet,
    'M' -> (1 to 5).toSet,
    'd' -> (1 to 3).toSet,
    'a' -> Set(1, 2, 3),
    'K' -> (1 to 2).toSet,
    'h' -> (1 to 2).toSet,
    'H' -> (1 to 2).toSet,
    'k' -> (1 to 2).toSet,
    'm' -> (1 to 2).toSet,
    's' -> (1 to 2).toSet,
    'S' -> (1 to 9).toSet,
    'z' -> (1 to 3).toSet,
    'Z' -> (1 to 5).toSet
  )

  private val Numeric = "CYxwyDdKhHkmsSe"

  /** The elements of a Joda pattern, or why core does not read it. */
  private def elements(pattern: String): Either[String, Vector[Element]] = {
    sealed trait Token
    final case class Letters(c: Char, n: Int) extends Token
    final case class Quoted(text: String) extends Token
    val tokens = Vector.newBuilder[Token]
    var i = 0
    var error: Option[String] = None
    while (error.isEmpty && i < pattern.length) {
      val c = pattern.charAt(i)
      if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
        var j = i
        while (j < pattern.length && pattern.charAt(j) == c) j += 1
        tokens += Letters(c, j - i)
        i = j
      } else if (c == '\'') {
        val sb = new StringBuilder
        var j = i + 1
        var closed = false
        while (!closed && j < pattern.length) {
          if (pattern.charAt(j) == '\'') {
            if (j + 1 < pattern.length && pattern.charAt(j + 1) == '\'') {
              sb.append('\''); j += 2
            } else { closed = true; j += 1 }
          } else { sb.append(pattern.charAt(j)); j += 1 }
        }
        if (!closed) error = Some("an unclosed quote")
        else tokens += Quoted(if (j == i + 2) "'" else sb.toString)
        i = j
      } else { tokens += Quoted(c.toString); i += 1 }
    }
    val ts = tokens.result()
    def numericNext(k: Int): Boolean =
      k + 1 < ts.size && (ts(k + 1) match {
        case Letters(c, n) => Numeric.indexOf(c.toInt) >= 0 || (c == 'M' && n <= 2)
        case _             => false
      })
    error.toLeft(()).flatMap { _ =>
      val out = Vector.newBuilder[Element]
      var failure: Option[String] = None
      ts.indices.foreach { k =>
        ts(k) match {
          case Quoted(t) => out += Literal(t)
          case Letters(c, n) if !Widths.get(c).exists(_.contains(n)) =>
            failure = failure.orElse(
              Some(s"the letter '${String.valueOf(c) * n}' is not one core has measured")
            )
          case Letters(c, n) =>
            val year: Option[Field] = c match {
              case 'y' => Some(Year)
              case 'Y' => Some(YearOfEra)
              case 'x' => Some(WeekYear)
              case _   => None
            }
            def upTo(default: Int) = Math.max(default, n)
            out += (c match {
              // `YY` is the year, as `yy` (MEASURED: `-2024` and `0` are read)
              case 'y' | 'Y' if n == 2 => TwoDigits(Year, lenient = !numericNext(k))
              case 'x' if n == 2       => TwoDigits(WeekYear, lenient = !numericNext(k))
              case 'y' | 'Y' | 'x' =>
                Number(year.get, if (numericNext(k)) n else 9, signed = c != 'Y')
              case 'C'           => Number(CenturyOfEra, n, signed = false)
              case 'G'           => Text(Era, EraTexts)
              case 'w'           => Number(WeekOfWeekYear, upTo(2), signed = false)
              case 'e'           => Number(DayOfWeekField, n, signed = false)
              case 'E'           => Text(DayOfWeekField, DayTexts)
              case 'D'           => Number(DayOfYear, upTo(3), signed = false)
              case 'M' if n >= 3 => Text(MonthOfYear, MonthTexts)
              case 'M'           => Number(MonthOfYear, 2, signed = false)
              case 'd'           => Number(DayOfMonth, upTo(2), signed = false)
              case 'a'           => Text(HalfDay, HalfDayTexts)
              case 'K'           => Number(HourOfHalfDay, upTo(2), signed = false)
              case 'h'           => Number(ClockHourOfHalfDay, upTo(2), signed = false)
              case 'H'           => Number(HourOfDay, upTo(2), signed = false)
              case 'k'           => Number(ClockHourOfDay, upTo(2), signed = false)
              case 'm'           => Number(MinuteOfHour, upTo(2), signed = false)
              case 's'           => Number(SecondOfMinute, upTo(2), signed = false)
              case 'S'           => Fraction(n)
              case 'Z' if n <= 2 => Offset
              case 'Z'           => RegionId
              case _             => ZoneName
            })
        }
      }
      failure.toLeft(out.result())
    }
  }

  /** The class a field stands for in a shape. */
  private def classOf(e: Element): Option[Char] =
    e match {
      case Number(f, _, _)         => Some(f.letter)
      case TwoDigits(f, _)         => Some(f.letter)
      case Fraction(_)             => Some('S')
      case Text(DayOfWeekField, _) => Some('E')
      case Text(f, _)              => Some(f.letter)
      case _                       => None
    }

  /** The classes of date letters read in combination -- each with every class of time letters
    * below, and every zone (MEASURED on 6.8.23): `G` era, `C` century of era, `Y` year of era, `y`
    * year, `x` week year, `M` month, `w` week, `D` day of year, `d` day of month, `e` day of week,
    * `E` its name.
    */
  private val DateShapes: Set[String] = Set(
    "",
    "C",
    "D",
    "E",
    "G",
    "M",
    "Y",
    "d",
    "e",
    "w",
    "x",
    "y",
    "Cy",
    "Gy",
    "Md",
    "YD",
    "YM",
    "Ye",
    "Yw",
    "we",
    "xD",
    "xM",
    "xe",
    "xw",
    "yD",
    "yE",
    "yM",
    "ye",
    "yw",
    "MdE",
    "Mde",
    "YMd",
    "YwE",
    "Ywe",
    "xMd",
    "xwE",
    "xwe",
    "yDe",
    "yMD",
    "yME",
    "yMd",
    "yMe",
    "ywE",
    "ywe",
    "CyMd",
    "GYMd",
    "GYwe",
    "Gxwe",
    "GyMD",
    "GyMd",
    "YMde",
    "YyMd",
    "xMde",
    "yMDd",
    "yMdE",
    "yMde",
    "yMwd",
    "yxMd",
    "GyMdE",
    "GyMde"
  )

  /** The classes of time letters read in combination (MEASURED): `a` half day, `K` `h` its hours,
    * `H` `k` the hours of the day, `m`, `s`, `S` fraction.
    */
  private val TimeShapes: Set[String] = Set(
    "",
    "H",
    "K",
    "S",
    "a",
    "h",
    "k",
    "m",
    "s",
    "Hm",
    "Km",
    "ah",
    "hm",
    "km",
    "sS",
    "Hms",
    "aHm",
    "aKm",
    "ahm",
    "akm",
    "HmsS",
    "ahms"
  )

  private val DateOrder = "GCYyxMwDdeE"
  private val TimeOrder = "aKhHkmsS"

  /** A custom Joda pattern of Elasticsearch 6. */
  final case class Pattern(pattern: String, clock: Clock) extends Alternative {
    override def isName: Boolean = false

    private val compiled: Either[String, (Vector[Element], String)] =
      elements(pattern).flatMap { es =>
        val classes = es.flatMap(classOf).toSet
        val date = DateOrder.filter(classes.contains)
        val time = TimeOrder.filter(classes.contains)
        val zone = es
          .collectFirst {
            case Offset   => "offset"
            case RegionId => "id"
            case ZoneName => "name"
          }
          .getOrElse("")
        val shape = s"$date|$time|$zone"
        // two zone letters (`ZZ ZZZ`, `ZZ z`): a combination core has not measured
        val zones = es.count {
          case Offset | RegionId | ZoneName => true
          case _                            => false
        }
        if (zones > 1)
          Left(s"its letters combine two zones, which core has not measured")
        else if (DateShapes.contains(date) && TimeShapes.contains(time)) Right((es, zone))
        else
          Left(s"its letters combine as '$shape', which core has not measured")
      }

    /** Why the pattern is not read, if it is not. */
    def unread: Option[String] = compiled.left.toOption

    def parse(text: String): Parse =
      compiled match {
        case Left(reason) => Unmeasured(s"'$pattern' is not a date format core reads: $reason")
        case Right((es, _)) =>
          lex(es, text) match {
            case None                            => NotParsed
            case Some(Lexed(_, _, Some(reason))) => Unmeasured(reason)
            case Some(Lexed(saved, offsetSeconds, None)) =>
              Try {
                resolve(saved).toInstant(ZoneOffset.UTC).minusSeconds(offsetSeconds.toLong)
              }.fold(_ => Invalid, Parsed)
          }
      }

    /** The values of `text` by the elements, read left to right; `None` when it does not match. */
    private def lex(es: Vector[Element], text: String): Option[Lexed] = {
      var pos = 0
      val saved = scala.collection.mutable.ArrayBuffer.empty[(Field, Int)]
      var offset = 0
      var unread: Option[String] = None
      var ok = true
      def digits(from: Int, max: Int): Int = {
        var j = from
        while (j < text.length && j - from < max && Character.isDigit(text.charAt(j))) j += 1
        j - from
      }
      val it = es.iterator
      while (ok && it.hasNext) {
        it.next() match {
          case Literal(t) =>
            if (text.regionMatches(true, pos, t, 0, t.length)) pos += t.length else ok = false
          case Number(f, max, signed) =>
            val sign =
              if (
                signed && pos < text.length && (text.charAt(pos) == '-' || text.charAt(pos) == '+')
              )
                1
              else 0
            val n = digits(pos + sign, max)
            if (n == 0) ok = false
            else {
              saved += (f -> Integer.parseInt(text.substring(pos, pos + sign + n).replace("+", "")))
              pos += sign + n
            }
          case TwoDigits(f, lenient) =>
            val sign =
              if (
                lenient && pos < text.length && (text.charAt(pos) == '-' || text.charAt(pos) == '+')
              )
                1
              else 0
            val n = digits(pos + sign, if (lenient) 9 else 2)
            if (n == 0) ok = false
            else {
              val t = text.substring(pos, pos + sign + n)
              saved += (f -> (if (sign == 0 && n == 2) twoDigitYear(t, clock)
                              else Integer.parseInt(t.replace("+", ""))))
              pos += sign + n
            }
          case Fraction(max) =>
            val n = digits(pos, max)
            if (n == 0) ok = false
            else {
              saved += (MillisOfSecond -> Integer.parseInt(
                (text.substring(pos, pos + n) + "00").take(3)
              ))
              pos += n
            }
          case Text(f, texts) =>
            // the longest name that matches, in any case
            val rest = text.substring(pos).toLowerCase(Locale.ROOT)
            texts.keys.filter(rest.startsWith).toSeq.sortBy(-_.length).headOption match {
              case Some(name) => saved += (f -> texts(name)); pos += name.length
              case None       => ok = false
            }
          case Offset =>
            OffsetText.findPrefixMatchOf(text.substring(pos)) match {
              case Some(m) =>
                offset = offsetSeconds(m.matched)
                pos += m.matched.length
              case None => ok = false
            }
          case RegionId =>
            // `UTC` and `GMT` (MEASURED: `UT` is rejected); any other id takes its offset from
            // zone data, not read
            RegionText.findPrefixMatchOf(text.substring(pos)) match {
              case Some(m) =>
                if (m.matched != "UTC" && m.matched != "GMT")
                  unread = unread.orElse(Some(zoneNotRead(m.matched)))
                pos += m.matched.length
              case None => ok = false
            }
          case ZoneName =>
            ZoneNames
              .filter(text.startsWith(_, pos))
              .sortBy(-_.length)
              .headOption match {
              case Some(name) =>
                if (!FixedNames.contains(name)) unread = unread.orElse(Some(zoneNotRead(name)))
                pos += name.length
              case None => ok = false
            }
        }
      }
      if (ok && pos == text.length) Some(Lexed(saved.toList, offset, unread)) else None
    }
  }

  private val OffsetText = "[Zz]|[+-]\\d{2}(?::?\\d{2}(?::?\\d{2})?)?".r
  private val RegionText = "[A-Za-z][A-Za-z0-9_/+-]*".r

  private def offsetSeconds(t: String): Int =
    if (t == "Z" || t == "z") 0
    else {
      val sign = if (t.charAt(0) == '-') -1 else 1
      val d = t.substring(1).replace(":", "")
      def part(i: Int) = if (d.length >= i + 2) Integer.parseInt(d.substring(i, i + 2)) else 0
      sign * (part(0) * 3600 + part(2) * 60 + part(4))
    }
}
