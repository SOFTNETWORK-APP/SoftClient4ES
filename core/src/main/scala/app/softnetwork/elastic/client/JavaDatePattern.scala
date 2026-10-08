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

import java.text.ParsePosition
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder, ResolverStyle, SignStyle}
import java.time.temporal.{
  ChronoField,
  IsoFields,
  Temporal,
  TemporalAccessor,
  TemporalField,
  TemporalQueries,
  TemporalUnit,
  ValueRange,
  WeekFields
}
import java.time.{DayOfWeek, Instant, LocalDate, LocalTime, ZoneId, ZoneOffset, ZonedDateTime}
import java.util.Locale
import scala.util.Try

/** A custom `java.time` date pattern, read as ONE Elasticsearch major reads it -- Elasticsearch 7
  * and 8, whose custom patterns are `java.time` patterns, and Elasticsearch 6.8 behind a leading
  * `8`. Every reading below is MEASURED, major by major (each pattern letter at each width, alone
  * and combined, with edge values: week-year boundaries, names, 12-hour clocks, fractions, zones;
  * the instant read back from the doc value), and written from that behaviour and Java's own API:
  *
  *   - first, the text is parsed by `java.time` with the pattern's letters, built field by field
  *     from `DateTimeFormatterBuilder` with the texts and the week rules THAT major's JVM gives
  *     them
  * -- never the JVM core runs on -- and resolved as Elasticsearch does (STRICTLY on 7 and 8): a
  * value it does not parse or resolve is not this alternative's, and the next one is tried;
  *   - then Elasticsearch completes what `java.time` resolved: a resolved date as is; else a year
  *     (of era) with its month and day, or its day of year, or its month, else -- with no resolved
  *     time -- January the first, any other field ignored; else a week-based year: the first day of
  *     its week (week 1 when none, a week past the year's last rolling over); else a month: of
  *     1970, its day or the first; the time `java.time` resolved, or midnight -- an unresolved time
  *     ignored when there is a date, a resolved one with no date on 1970-01-01; no date and no
  *     time: the value is REJECTED, as is a date that does not exist (the next alternative never
  *     tried).
  *
  * A letter or a width no measurement covers, a combination of letter classes outside the measured
  * ones (a zone id beside another zone included), a zone other than an offset, `UTC`, `GMT` and
  * `UT` (a region id, an abbreviation, a long name: its offset comes from the zone data of the JVM
  * reading it -- `IST` is UTC on 8.18.3 and Israel time on 7.17.29), and a reading that would
  * depend on the Java version core runs on (a localized offset, a fraction after digits of a
  * variable width; on 6.8, an AM/PM with no hour, a week past its week-based year's last) are not
  * guessed: they are [[MappedDateFormat.Unmeasured]], and the statement fails, naming the field and
  * the format.
  */
private[client] object JavaDatePattern {

  import MappedDateFormat._

  // ---------------------------------------------------------------------------------------------
  // The measured rules of a major
  // ---------------------------------------------------------------------------------------------

  /** What a major's `java.time` reading takes from its JVM, MEASURED: the week rules, the texts of
    * each text letter (a text two values share -- the narrow `J` -- is the LAST one's; none: the
    * letter reads a number), the letters and widths whose values were read back, and the classes of
    * letters read in combination.
    */
  final case class Rules(
    label: String,
    weeks: WeekFields,
    texts: Map[(Char, Int), Seq[(Long, String)]],
    letters: Map[Char, Set[Int]],
    dateShapes: Set[String],
    timeShapes: Set[String],
    zoneKinds: Set[String],
    resolver: ResolverStyle = ResolverStyle.STRICT,
    alignedField: TemporalField = ChronoField.ALIGNED_WEEK_OF_MONTH,
    strictWeeks: Boolean = true
  )

  private def numbered(texts: Seq[String], from: Long = 1L): Seq[(Long, String)] =
    texts.zipWithIndex.map { case (t, i) => (i + from, t) }

  private val MonthsShort =
    numbered(
      Seq("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
    )
  private val MonthsFull = numbered(
    Seq(
      "January",
      "February",
      "March",
      "April",
      "May",
      "June",
      "July",
      "August",
      "September",
      "October",
      "November",
      "December"
    )
  )
  private val MonthsNarrow =
    numbered(Seq("J", "F", "M", "A", "M", "J", "J", "A", "S", "O", "N", "D"))
  private val MonthsDigits = numbered((1 to 12).map(_.toString))
  // ISO order: Monday is 1
  private val DaysShort = numbered(Seq("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"))
  private val DaysFull =
    numbered(Seq("Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"))
  private val DaysNarrow = numbered(Seq("M", "T", "W", "T", "F", "S", "S"))
  private val QuartersShort = numbered(Seq("Q1", "Q2", "Q3", "Q4"))
  private val QuartersFull =
    numbered(Seq("1st quarter", "2nd quarter", "3rd quarter", "4th quarter"))
  private val QuartersDigits = numbered(Seq("1", "2", "3", "4"))
  private val AmPm = numbered(Seq("AM", "PM"), from = 0L)

  private def eras(bc: String, ad: String): Seq[(Long, String)] = Seq(0L -> bc, 1L -> ad)

  private def textsOf(
    eraFull: Seq[(Long, String)],
    monthsNarrow: Seq[(Long, String)],
    quarters: Map[(Char, Int), Seq[(Long, String)]]
  ): Map[(Char, Int), Seq[(Long, String)]] = {
    val era = Map(
      ('G', 1) -> eras("BC", "AD"),
      ('G', 2) -> eras("BC", "AD"),
      ('G', 3) -> eras("BC", "AD"),
      ('G', 4) -> eraFull,
      ('G', 5) -> eras("B", "A")
    )
    val months = for {
      m      <- Seq('M', 'L')
      (n, t) <- Seq(3 -> MonthsShort, 4 -> MonthsFull, 5 -> monthsNarrow)
    } yield (m, n) -> t
    val days = for {
      e <- Seq('E', 'e', 'c')
      (n, t) <- Seq(3 -> DaysShort, 4 -> DaysFull, 5 -> DaysNarrow) ++
      (if (e == 'E') Seq(1 -> DaysShort, 2 -> DaysShort) else Nil)
    } yield (e, n) -> t
    era ++ months ++ days ++ quarters + (('a', 1) -> AmPm)
  }

  private val AllWidths: Map[Char, Set[Int]] = Map(
    'G' -> (1 to 5).toSet,
    'u' -> (1 to 5).toSet,
    'y' -> (1 to 5).toSet,
    'Y' -> (1 to 5).toSet,
    'D' -> (1 to 3).toSet,
    'M' -> (1 to 5).toSet,
    'L' -> (1 to 5).toSet,
    'd' -> Set(1, 2),
    'Q' -> (1 to 5).toSet,
    'q' -> (1 to 5).toSet,
    'w' -> Set(1, 2),
    'W' -> Set(1),
    'E' -> (1 to 5).toSet,
    'e' -> (1 to 5).toSet,
    'c' -> Set(1, 3, 4, 5),
    'F' -> Set(1),
    'a' -> Set(1),
    'h' -> Set(1, 2),
    'K' -> Set(1, 2),
    'k' -> Set(1, 2),
    'H' -> Set(1, 2),
    'm' -> Set(1, 2),
    's' -> Set(1, 2),
    'S' -> (1 to 9).toSet,
    'A' -> (1 to 4).toSet,
    'n' -> (1 to 4).toSet,
    'N' -> (1 to 4).toSet,
    'V' -> Set(2),
    'z' -> (1 to 4).toSet,
    'v' -> Set(1, 4),
    'O' -> Set(1, 4),
    'X' -> (1 to 5).toSet,
    'x' -> (1 to 5).toSet,
    'Z' -> (1 to 5).toSet
  )

  /** The classes of date letters read in combination -- each with every class of time letters below
    * (MEASURED on 8.18.3 and 7.17.29: the full product, with every zone): `G` era, `y` year of era,
    * `u` year, `Y` week-based year, `M` month (`L`), `d` day, `D` day of year, `Q` quarter (`q`),
    * `w` week of week-based year, `W` week of month, `E` day-of-week text (`e`, `c` at 3 and more),
    * `e` localized day-of-week number (`c` at 1), `F` aligned week of month.
    */
  private val DateShapes: Set[String] = Set(
    "",
    "D",
    "E",
    "F",
    "G",
    "M",
    "Q",
    "W",
    "Y",
    "d",
    "e",
    "u",
    "w",
    "y",
    "Gy",
    "MW",
    "Md",
    "YD",
    "YM",
    "Ye",
    "Yw",
    "dD",
    "uD",
    "uE",
    "uM",
    "uQ",
    "uw",
    "wE",
    "yD",
    "yE",
    "yM",
    "yQ",
    "ye",
    "yw",
    "MWe",
    "MdD",
    "MdE",
    "Mde",
    "YMW",
    "YMd",
    "YwE",
    "Ywe",
    "uDE",
    "uDe",
    "uMD",
    "uME",
    "uMF",
    "uMQ",
    "uMW",
    "uMd",
    "uMw",
    "uWe",
    "udQ",
    "uwe",
    "yDe",
    "yMD",
    "yME",
    "yMF",
    "yMW",
    "yMd",
    "yMe",
    "yQw",
    "ywE",
    "ywe",
    "GYwe",
    "GuMd",
    "GyMD",
    "GyMd",
    "YMWe",
    "YMde",
    "uMEF",
    "uMWE",
    "uMWe",
    "uMdD",
    "uMdE",
    "uMdQ",
    "uMde",
    "uMeF",
    "uYMd",
    "yMWE",
    "yMWe",
    "yMdD",
    "yMdE",
    "yMdQ",
    "yMdW",
    "yMde",
    "yMdw",
    "yMeF",
    "yYMd",
    "GyMdE",
    "GyMde"
  )

  /** The classes of time letters read in combination (MEASURED): `a` AM/PM, `h` clock hour of
    * AM/PM, `K` hour of AM/PM, `k` clock hour of day, `H` hour of day, `m`, `s`, `S` fraction, `n`
    * nano of second, `A` milli of day, `N` nano of day.
    */
  private val TimeShapes: Set[String] = Set(
    "",
    "A",
    "H",
    "K",
    "N",
    "S",
    "a",
    "h",
    "k",
    "m",
    "n",
    "s",
    "HA",
    "Hm",
    "Km",
    "ah",
    "hm",
    "km",
    "HmN",
    "Hms",
    "Kms",
    "aHm",
    "aKm",
    "ahm",
    "akm",
    "kms",
    "HmsA",
    "HmsS",
    "Hmsn",
    "ahms",
    "HmsSN",
    "HmsSn",
    "ahmsS"
  )

  private val ZoneKinds: Set[String] = Set("", "offset", "id", "name")

  /** Elasticsearch 8.18.3: weeks start on Sunday, week 1 holds January the first; CLDR English. */
  val Es8: Rules = Rules(
    "Elasticsearch 8",
    WeekFields.of(DayOfWeek.SUNDAY, 1),
    textsOf(
      eras("Before Christ", "Anno Domini"),
      MonthsNarrow,
      Map(
        ('Q', 3) -> QuartersShort,
        ('Q', 4) -> QuartersFull,
        ('Q', 5) -> QuartersDigits,
        ('q', 3) -> QuartersShort,
        ('q', 4) -> QuartersFull,
        ('q', 5) -> QuartersDigits
      )
    ),
    AllWidths,
    DateShapes,
    TimeShapes,
    ZoneKinds
  )

  /** Elasticsearch 7.17.29: ISO weeks (Monday, 4 days); the full era `CE`, the narrow month and
    * quarter their number's text (`1`, not `01`), the full quarter `Q1`, the stand-alone quarter a
    * NUMBER (`01` and `5` are read).
    */
  val Es7: Rules = Rules(
    "Elasticsearch 7",
    WeekFields.ISO,
    textsOf(
      eras("BCE", "CE"),
      MonthsDigits,
      Map(
        ('Q', 3) -> QuartersShort,
        ('Q', 4) -> QuartersShort,
        ('Q', 5) -> QuartersDigits,
        // no stand-alone quarter texts: a number, as `java.time` reads a text it has none for
        ('q', 3) -> Nil,
        ('q', 4) -> Nil,
        ('q', 5) -> Nil
      )
    ),
    AllWidths,
    DateShapes,
    TimeShapes,
    ZoneKinds
  )

  /** Elasticsearch 6.8's `java.time` mode, selected by a leading `8` (MEASURED on 6.8.23): weeks
    * starting on Sunday, Elasticsearch 7's texts, a SMART resolution, `F` the aligned day of week
    * in month, a week 53 of a year of 52 read into the next year.
    */
  val Es6: Rules = Es7.copy(
    label = "Elasticsearch 6",
    weeks = WeekFields.of(DayOfWeek.SUNDAY, 1),
    resolver = ResolverStyle.SMART,
    alignedField = ChronoField.ALIGNED_DAY_OF_WEEK_IN_MONTH,
    strictWeeks = false
  )

  // ---------------------------------------------------------------------------------------------
  // The pattern
  // ---------------------------------------------------------------------------------------------

  private sealed trait Token
  private final case class Letters(c: Char, n: Int) extends Token
  private final case class Literal(text: String) extends Token
  private case object OptionalStart extends Token
  private case object OptionalEnd extends Token

  /** The tokens of a `java.time` pattern, or why it is not one core reads. */
  private def tokens(pattern: String): Either[String, Vector[Token]] = {
    val out = Vector.newBuilder[Token]
    var i = 0
    var depth = 0
    var error: Option[String] = None
    while (error.isEmpty && i < pattern.length) {
      val c = pattern.charAt(i)
      if ((c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z')) {
        var j = i
        while (j < pattern.length && pattern.charAt(j) == c) j += 1
        out += Letters(c, j - i)
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
        else out += Literal(if (j == i + 2) "'" else sb.toString)
        i = j
      } else if (c == '[') { depth += 1; out += OptionalStart; i += 1 }
      else if (c == ']') {
        if (depth == 0) error = Some("an unopened ']'")
        depth -= 1; out += OptionalEnd; i += 1
      } else if (c == '{' || c == '}' || c == '#') error = Some(s"the reserved '$c'")
      else { out += Literal(c.toString); i += 1 }
    }
    error.toLeft(out.result())
  }

  /** A field holding a letter's value in the lexical pass, which says which letters a text carried
    * (an optional section may not); its range and its values are the real field's.
    */
  private final class Carrier(val letter: Char, val real: TemporalField) extends TemporalField {
    def getBaseUnit: TemporalUnit = real.getBaseUnit
    def getRangeUnit: TemporalUnit = real.getRangeUnit
    def range(): ValueRange = real.range()
    def isDateBased: Boolean = real.isDateBased
    def isTimeBased: Boolean = real.isTimeBased
    def isSupportedBy(temporal: TemporalAccessor): Boolean = false
    def rangeRefinedBy(temporal: TemporalAccessor): ValueRange = real.rangeRefinedBy(temporal)
    def getFrom(temporal: TemporalAccessor): Long = real.getFrom(temporal)
    def adjustInto[R <: Temporal](temporal: R, newValue: Long): R =
      real.adjustInto(temporal, newValue)
    override def toString: String = s"Carrier($letter)"
  }

  /** The class a letter stands for in a shape. */
  private def letterClass(c: Char, n: Int): Char =
    c match {
      case 'L'                 => 'M'
      case 'q'                 => 'Q'
      case 'e' | 'c' if n >= 3 => 'E'
      case 'c'                 => 'e'
      case other               => other
    }

  private val DateOrder = "GyuYMdDQwWEeF"
  private val TimeOrder = "ahKkHmsSnAN"

  private def zoneKind(c: Char): String =
    c match {
      case 'V'       => "id"
      case 'z' | 'v' => "name"
      case _         => "offset"
    }

  /** The formatter of the tokens: with `carriers`, each letter class in its [[Carrier]] (the
    * lexical pass), else in its real field (the resolving pass). Built ONLY from
    * `DateTimeFormatterBuilder` calls of Java 8, with the major's texts and week rules.
    */
  private def formatter(
    tokens: Vector[Token],
    rules: Rules,
    carriers: Option[scala.collection.mutable.Map[Char, Carrier]]
  ): DateTimeFormatter = {
    val b = new DateTimeFormatterBuilder()
    val weeks = rules.weeks
    val base2000 = LocalDate.of(2000, 1, 1)
    def field(c: Char, n: Int, real: TemporalField): TemporalField =
      carriers.fold(real) { registry =>
        val cls = letterClass(c, n)
        registry.getOrElseUpdate(cls, new Carrier(cls, real))
      }
    def text(c: Char, n: Int, real: TemporalField): Unit =
      rules.texts((c, n)) match {
        // no texts: a number, as `java.time` reads a text its locale data has none for
        case Nil => b.appendValue(field(c, n, real), 1, 19, SignStyle.NORMAL)
        case texts =>
          val map = new java.util.LinkedHashMap[java.lang.Long, String]()
          texts.foreach { case (v, t) => map.put(java.lang.Long.valueOf(v), t) }
          b.appendText(field(c, n, real), map)
      }
    def year(c: Char, n: Int, real: TemporalField): Unit = {
      val f = field(c, n, real)
      n match {
        case 2 => b.appendValueReduced(f, 2, 2, base2000)
        case 1 => b.appendValue(f, 1, 19, SignStyle.NORMAL)
        case 3 => b.appendValue(f, 3, 19, SignStyle.NORMAL)
        case _ => b.appendValue(f, n, 19, SignStyle.EXCEEDS_PAD)
      }
    }
    def number(c: Char, n: Int, real: TemporalField): Unit =
      if (n == 1) b.appendValue(field(c, n, real)) else b.appendValue(field(c, n, real), n)
    tokens.foreach {
      case Literal(t)    => b.appendLiteral(t)
      case OptionalStart => b.optionalStart()
      case OptionalEnd   => b.optionalEnd()
      case Letters(c, n) =>
        c match {
          case 'G' => text(c, n, ChronoField.ERA)
          case 'u' => year(c, n, ChronoField.YEAR)
          case 'y' => year(c, n, ChronoField.YEAR_OF_ERA)
          case 'Y' => year(c, n, weeks.weekBasedYear())
          case 'M' | 'L' =>
            if (n <= 2) number(c, n, ChronoField.MONTH_OF_YEAR)
            else text(c, n, ChronoField.MONTH_OF_YEAR)
          case 'd' => number(c, n, ChronoField.DAY_OF_MONTH)
          case 'D' =>
            n match {
              case 1 => b.appendValue(field(c, n, ChronoField.DAY_OF_YEAR))
              case 2 =>
                b.appendValue(field(c, n, ChronoField.DAY_OF_YEAR), 2, 3, SignStyle.NOT_NEGATIVE)
              case _ => b.appendValue(field(c, n, ChronoField.DAY_OF_YEAR), 3)
            }
          case 'Q' | 'q' =>
            if (n <= 2) number(c, n, IsoFields.QUARTER_OF_YEAR)
            else text(c, n, IsoFields.QUARTER_OF_YEAR)
          case 'w' =>
            b.appendValue(field(c, n, weeks.weekOfWeekBasedYear()), n, 2, SignStyle.NOT_NEGATIVE)
          case 'W' => b.appendValue(field(c, n, weeks.weekOfMonth()), 1, 1, SignStyle.NOT_NEGATIVE)
          case 'E' => text(c, n, ChronoField.DAY_OF_WEEK)
          case 'e' | 'c' =>
            if (n <= 2)
              b.appendValue(field(c, n, weeks.dayOfWeek()), n, n, SignStyle.NOT_NEGATIVE)
            else text(c, n, ChronoField.DAY_OF_WEEK)
          case 'F' => b.appendValue(field(c, n, rules.alignedField))
          case 'a' => text(c, n, ChronoField.AMPM_OF_DAY)
          case 'h' => number(c, n, ChronoField.CLOCK_HOUR_OF_AMPM)
          case 'K' => number(c, n, ChronoField.HOUR_OF_AMPM)
          case 'k' => number(c, n, ChronoField.CLOCK_HOUR_OF_DAY)
          case 'H' => number(c, n, ChronoField.HOUR_OF_DAY)
          case 'm' => number(c, n, ChronoField.MINUTE_OF_HOUR)
          case 's' => number(c, n, ChronoField.SECOND_OF_MINUTE)
          case 'S' => b.appendFraction(field(c, n, ChronoField.NANO_OF_SECOND), n, n, false)
          case 'A' =>
            b.appendValue(field(c, n, ChronoField.MILLI_OF_DAY), n, 19, SignStyle.NOT_NEGATIVE)
          case 'n' =>
            b.appendValue(field(c, n, ChronoField.NANO_OF_SECOND), n, 19, SignStyle.NOT_NEGATIVE)
          case 'N' =>
            b.appendValue(field(c, n, ChronoField.NANO_OF_DAY), n, 19, SignStyle.NOT_NEGATIVE)
          case 'V' | 'z' | 'v' => b.appendZoneId()
          case 'O' =>
            b.appendLocalizedOffset(
              if (n == 1) java.time.format.TextStyle.SHORT else java.time.format.TextStyle.FULL
            )
          case 'X' =>
            b.appendOffset(Seq("+HHmm", "+HHMM", "+HH:MM", "+HHMMss", "+HH:MM:ss")(n - 1), "Z")
          case 'x' =>
            b.appendOffset(
              Seq("+HHmm", "+HHMM", "+HH:MM", "+HHMMss", "+HH:MM:ss")(n - 1),
              Seq("+00", "+0000", "+00:00", "+0000", "+00:00")(n - 1)
            )
          case 'Z' =>
            n match {
              case 4 => b.appendLocalizedOffset(java.time.format.TextStyle.FULL)
              case 5 => b.appendOffset("+HH:MM:ss", "Z")
              case _ => b.appendOffset("+HHMM", "+0000")
            }
          case other => throw new IllegalArgumentException(s"letter $other")
        }
    }
    b.toFormatter(Locale.ROOT).withResolverStyle(rules.resolver)
  }

  /** A pattern's two formatters -- the lexical pass's and the resolving pass's -- its letters'
    * [[Carrier]] s, and the kind of its zone letter.
    */
  private final case class Compiled(
    lexical: DateTimeFormatter,
    resolving: DateTimeFormatter,
    carriers: Map[Char, Carrier],
    zone: String,
    lexicalPass: Boolean
  )

  /** A custom pattern of one major. */
  final case class Pattern(pattern: String, rules: Rules) extends Alternative {
    override def isName: Boolean = false

    private val compiled: Either[String, Compiled] =
      tokens(pattern).flatMap { ts =>
        val letters = ts.collect { case l: Letters => l }
        letters.find(l => !rules.letters.get(l.c).exists(_.contains(l.n))) match {
          case Some(Letters(c, n)) =>
            Left(s"the letter '${String.valueOf(c) * n}' is not one core has measured")
          case _ if clientDependent(ts, rules).isDefined => Left(clientDependent(ts, rules).get)
          case _ =>
            val registry = scala.collection.mutable.Map.empty[Char, Carrier]
            val zones = letters.filter(l => ZoneLetters.indexOf(l.c.toInt) >= 0)
            val zone = zones.headOption.fold("")(l => zoneKind(l.c))
            val classes = letters.map(l => letterClass(l.c, l.n)).toSet
            // the lexical pass says which letters a text carried (an optional section), checks
            // the values older Java resolvers let through (an hour of AM/PM, a week of a week
            // year beside a day of week, a week past the week year's last on 6.8) and tells a zone
            // name core does not read from a text the format rejects; any other pattern carries
            // all its letters, in one parse
            val lexicalPass = ts.contains(OptionalStart) || zone == "name" || classes('K') ||
              (classes('Y') && classes('w') &&
              (!rules.strictWeeks || classes('e') || classes('E')))
            Try(
              Compiled(
                formatter(ts, rules, Some(registry)),
                formatter(ts, rules, None),
                registry.toMap,
                zone,
                lexicalPass
              )
            ).toEither.left
              .map(e => s"its letters do not compile: ${e.getMessage}")
              .flatMap(c =>
                // several zone letters: measured only as offsets (`[XXX][X]`)
                if (zones.size > 1 && zones.exists(l => zoneKind(l.c) != "offset"))
                  Left(
                    s"its zone letters ${zones.map(l => s"'${String.valueOf(l.c) * l.n}'").mkString(", ")}" +
                    " combine a zone id or name with another zone, which core has not measured"
                  )
                else if (c.lexicalPass) Right(c)
                else shape(c.carriers.keySet, zone).toLeft(c)
              )
        }
      }

    /** Why the pattern is not read, if it is not. */
    def unread: Option[String] = compiled.left.toOption

    def parse(text: String): Parse =
      compiled match {
        case Left(reason) => Unmeasured(s"'$pattern' is not a date format core reads: $reason")
        case Right(c) if !c.lexicalPass =>
          // a text the pattern does not parse, told without an exception (a value the response
          // parser read as a date is tried in many spellings)
          val pos = new ParsePosition(0)
          val raw = Try(c.resolving.parseUnresolved(text, pos)).toOption.orNull
          if (raw == null || pos.getErrorIndex >= 0 || pos.getIndex != text.length)
            unknownZone(text, pos, c).fold[Parse](NotParsed)(Unmeasured)
          else resolve(text, c)
        case Right(c) =>
          val pos = new ParsePosition(0)
          val raw = Try(c.lexical.parseUnresolved(text, pos)).toOption.orNull
          if (raw == null || pos.getErrorIndex >= 0 || pos.getIndex != text.length)
            // a zone name is open-ended: a text not read may hold a name Elasticsearch reads
            if (c.zone == "name")
              Unmeasured(s"a zone in '$text' is not one core reads with '$pattern'")
            else unknownZone(text, pos, c).fold[Parse](NotParsed)(Unmeasured)
          else
            shapeOf(raw, c).orElse(weekPastYear(raw, c)) match {
              case Some(reason)           => Unmeasured(reason)
              case None if !valid(raw, c) => NotParsed
              case None                   => resolve(text, c)
            }
      }

    /** Why a text a zone id pattern (`VV`) did not parse is not read, when it failed at a zone id:
      * whether the JVM core runs on knows a region (`America/Ciudad_Juarez`) depends on its zone
      * data, and core reads no region.
      */
    private def unknownZone(text: String, pos: ParsePosition, c: Compiled): Option[String] =
      if (c.zone != "id" || pos.getErrorIndex < 0 || pos.getErrorIndex >= text.length) None
      else {
        val at = pos.getErrorIndex
        val m = ZoneToken.matcher(text).region(at, text.length)
        // the zone id is what failed when `UTC` in its place parses
        lazy val asUtc = {
          val probe = new ParsePosition(0)
          val utc = text.substring(0, at) + "UTC" + text.substring(m.end())
          Try(c.lexical.parseUnresolved(utc, probe)).toOption.exists(_ != null) &&
          probe.getErrorIndex < 0 && probe.getIndex == utc.length
        }
        if (m.lookingAt() && fixedId(m.group(), prefixed = true).isEmpty && asUtc)
          Some(zoneNotRead(m.group()))
        else None
      }

    /** The text resolved by `java.time`, then completed as Elasticsearch completes it. */
    private def resolve(text: String, c: Compiled): Parse =
      Try(c.resolving.parse(text)).toOption match {
        case None => NotParsed
        case Some(resolved) =>
          zoneOf(resolved) match {
            case Left(reason) => Unmeasured(reason)
            case Right(zone)  => completed(resolved, zone, rules).fold[Parse](Invalid)(Parsed)
          }
      }

    /** Whether every TIME value a text carried is in its field's range, as Elasticsearch's
      * `java.time` checks it (MEASURED: an hour of AM/PM 12, a clock hour 0, a milli of day of
      * 86400000 are rejected, where a date value its completion ignores is not checked -- a quarter
      * 5 in `yyyy-Q`, a month 53 in `YYYY-MM`), and a week of week-based year with a day of week is
      * a week of THAT year (week 53 of a year of 52 is rejected, where without a day of week it
      * rolls over into the next year) -- checked here rather than left to the `java.time` core runs
      * on, whose STRICT resolution of these differs across Java versions.
      */
    private def valid(raw: TemporalAccessor, c: Compiled): Boolean =
      Try {
        // STRICT: every time value; SMART (6.8): the hour of AM/PM, which older Java resolvers
        // do not check (the other SMART checks are the same in every Java version measured)
        val checked: Carrier => Boolean =
          if (rules.resolver == ResolverStyle.STRICT) _.isTimeBased
          else _.real == ChronoField.HOUR_OF_AMPM
        val inRange = c.carriers.forall { case (_, f) =>
          !checked(f) || !raw.isSupported(f) || f.range().isValidValue(raw.getLong(f))
        }
        def value(cls: Char): Option[Long] =
          c.carriers.get(cls).filter(raw.isSupported).map(raw.getLong)
        val weekOfYear = (value('Y'), value('w')) match {
          case (Some(wby), Some(w))
              if rules.strictWeeks && (value('e').isDefined || value('E').isDefined) =>
            val first = LocalDate.of(Math.toIntExact(wby), 1, rules.weeks.getMinimalDaysInFirstWeek)
            rules.weeks.weekOfWeekBasedYear().rangeRefinedBy(first).isValidValue(w)
          case _ => true
        }
        inRange && weekOfYear
      }.getOrElse(false)

    /** Why the letters a text carried are a combination core has not measured, if they are. */
    private def shapeOf(raw: TemporalAccessor, c: Compiled): Option[String] =
      shape(
        c.carriers.collect { case (cls, f) if raw.isSupported(f) => cls }.toSet,
        if (raw.query(TemporalQueries.zone()) == null) "" else c.zone
      )

    /** On Elasticsearch 6.8's java.time mode (SMART), why a week of a week-based year past that
      * year's last is not read, if it is: Elasticsearch reads it into the next year (6.8.23, Java
      * 15), which Java 21's resolution rejects -- a reading that would depend on the JVM core runs
      * on.
      */
    private def weekPastYear(raw: TemporalAccessor, c: Compiled): Option[String] =
      if (rules.strictWeeks) None
      else
        (
          c.carriers.get('Y').filter(raw.isSupported).map(raw.getLong),
          c.carriers.get('w').filter(raw.isSupported).map(raw.getLong)
        ) match {
          case (Some(wby), Some(w)) =>
            val past = Try {
              val first =
                LocalDate.of(Math.toIntExact(wby), 1, rules.weeks.getMinimalDaysInFirstWeek)
              !rules.weeks.weekOfWeekBasedYear().rangeRefinedBy(first).isValidValue(w)
            }.getOrElse(false)
            if (past)
              Some(
                s"'$pattern' reads a week past the last of its week-based year, which " +
                s"${rules.label}'s java.time mode resolves differently from one Java version to " +
                "another"
              )
            else None
          case _ => None
        }

    /** Why `carried` letter classes and a `zone` kind are a combination core has not measured. */
    private def shape(carried: scala.collection.Set[Char], zone: String): Option[String] = {
      val date = DateOrder.filter(carried.contains)
      val time = TimeOrder.filter(carried.contains)
      if (!rules.dateShapes.contains(date) || !rules.timeShapes.contains(time))
        Some(
          s"the letters of '$pattern' combine as '$date${if (time.nonEmpty) "|" + time else ""}'," +
          " which core has not measured"
        )
      // Elasticsearch 6.8 resolves SMARTLY, with its own JVM: an AM/PM with no hour is ignored
      // there (6.8.23 bundles Java 15), set to 06:00 or 18:00 from Java 16 on -- a reading that
      // would depend on the JVM core runs on
      else if (rules.resolver == ResolverStyle.SMART && carried('a') && !"hKkH".exists(carried))
        Some(
          s"'$pattern' reads an AM/PM with no hour, which ${rules.label}'s java.time mode resolves " +
          "differently from one Java version to another"
        )
      else if (!rules.zoneKinds.contains(zone))
        Some(s"the zone of '$pattern' is not one core has measured")
      else None
    }
  }

  private val ZoneLetters = "VzvOXxZ"

  private val ZoneToken = java.util.regex.Pattern.compile("[A-Za-z][A-Za-z0-9_/+~-]*+")

  /** The zone a resolved text gives -- `None` for no zone -- or why core does not read it: only an
    * offset, `UTC`, `GMT`, `UT` and their offsets (`GMT+01:00`) are read, the same on every JVM. A
    * region id (`Europe/Paris`) takes its offset from the zone data of the JVM reading it, an
    * abbreviation or a long name (`CET`, `PST`, `Pacific Time`) from its name data (MEASURED: `IST`
    * is UTC on 8.18.3, Israel time on 7.17.29; `America/Mexico_City` is 17:00Z on 8.18.3 where an
    * older zone data gives 18:00Z): never Elasticsearch's.
    */
  private def zoneOf(resolved: TemporalAccessor): Either[String, Option[ZoneId]] =
    Option(resolved.query(TemporalQueries.zone())) match {
      case None                => Right(None)
      case Some(o: ZoneOffset) => Right(Some(o))
      case Some(z) =>
        fixedId(z.getId, prefixed = true)
          .map(o => Some(o: ZoneId))
          .toRight(zoneNotRead(z.getId))
    }

  /** Why a pattern reads differently from one Java version core runs on to another, if it does
    * (MEASURED: the corpora read with Java 8, 11, 17 and 21):
    *   - a localized offset (`O`, `OOOO`, `ZZZZ`: `GMT+1`), which Java 8 does not parse as later
    *     versions do;
    *   - a fraction (`S`) right after digits of a variable width (`yyyyMMddHHmmssSSS`): later
    *     versions keep the fraction's digits apart from the variable width's, Java 8 does not.
    */
  private def clientDependent(tokens: Vector[Token], rules: Rules): Option[String] = {
    // the adjacent values `java.time` parses together: none, led by a value of a fixed width, or
    // led by a value of a variable width -- any other element ends them
    var chain = 0
    var reason: Option[String] = None
    def fixed(c: Char, n: Int): Option[Boolean] =
      c match {
        // a text letter with no texts on this major reads a number of a variable width
        case _ if rules.texts.get((c, n)).exists(_.isEmpty) => Some(false)
        case 'u' | 'y' | 'Y'                                => Some(n == 2)
        case 'M' | 'L' | 'Q' | 'q' if n >= 3                => None
        case 'e' | 'c' if n >= 3                            => None
        case 'e' | 'c' | 'W'                                => Some(true)
        case 'D'                                            => Some(n == 3)
        case 'F' | 'A' | 'n' | 'N'                          => Some(false)
        case 'M' | 'L' | 'Q' | 'q' | 'd' | 'w'              => Some(n == 2)
        case 'h' | 'K' | 'k' | 'H' | 'm' | 's'              => Some(n == 2)
        case _                                              => None
      }
    tokens.foreach {
      case Letters(c, n) if c == 'O' || (c == 'Z' && n == 4) =>
        reason = reason.orElse(
          Some(
            s"the letter '${String.valueOf(c) * n}' is a localized offset (`GMT+1`), which Java 8 " +
            "parses differently from later Java versions"
          )
        )
      case Letters('S', n) =>
        if (chain == 2)
          reason = reason.orElse(
            Some(
              s"the fraction '${"S" * n}' follows digits of a variable width with no separator, " +
              "which Java 8 parses differently from later Java versions"
            )
          )
        if (chain == 0) chain = 1
      case Letters(c, n) =>
        chain = fixed(c, n) match {
          case Some(true)  => if (chain == 0) 1 else chain
          case Some(false) => 2
          case None        => 0
        }
      case _ => chain = 0
    }
    reason
  }

  /** Elasticsearch's completion of what `java.time` resolved (MEASURED, 7.17.29 and 8.18.3): see
    * the object's documentation. `None` when the value is rejected.
    */
  private def completed(
    t: TemporalAccessor,
    zone: Option[ZoneId],
    rules: Rules
  ): Option[Instant] =
    Try {
      def has(f: TemporalField) = t.isSupported(f)
      def int(f: TemporalField) = Math.toIntExact(t.getLong(f))
      val weeks = rules.weeks
      val time = Option(t.query(TemporalQueries.localTime()))
      val resolvedDate = Option(t.query(TemporalQueries.localDate()))
      // a year with a day of week and no date, with a resolved time: REJECTED (MEASURED,
      // `Unsupported field: DayOfYear`), where it is the year's first day with no time
      if (
        resolvedDate.isEmpty && time.isDefined && has(ChronoField.YEAR) &&
        has(ChronoField.DAY_OF_WEEK)
      )
        throw new IllegalArgumentException("a year and a day of week with a time")
      val date: Option[LocalDate] = resolvedDate.orElse {
        val year =
          if (has(ChronoField.YEAR)) Some(int(ChronoField.YEAR))
          else if (has(ChronoField.YEAR_OF_ERA)) Some(int(ChronoField.YEAR_OF_ERA))
          else None
        year
          .flatMap { y =>
            if (has(ChronoField.MONTH_OF_YEAR) && has(ChronoField.DAY_OF_MONTH))
              Some(LocalDate.of(y, int(ChronoField.MONTH_OF_YEAR), int(ChronoField.DAY_OF_MONTH)))
            else if (has(ChronoField.DAY_OF_YEAR))
              Some(LocalDate.ofYearDay(y, int(ChronoField.DAY_OF_YEAR)))
            else if (has(ChronoField.MONTH_OF_YEAR))
              Some(LocalDate.of(y, int(ChronoField.MONTH_OF_YEAR), 1))
            // a year alone, with a resolved time, gives no date: 1970-01-01 (MEASURED)
            else if (time.isDefined) None
            else Some(LocalDate.of(y, 1, 1))
          }
          .orElse {
            if (has(weeks.weekBasedYear())) {
              val first = LocalDate.of(
                int(weeks.weekBasedYear()),
                1,
                weeks.getMinimalDaysInFirstWeek
              )
              val week =
                if (has(weeks.weekOfWeekBasedYear())) t.getLong(weeks.weekOfWeekBasedYear())
                else 1L
              Some(
                first.`with`(weeks.weekOfWeekBasedYear(), week).`with`(weeks.dayOfWeek(), 1L)
              )
            } else None
          }
          .orElse {
            if (has(ChronoField.MONTH_OF_YEAR))
              Some(
                LocalDate.of(
                  1970,
                  int(ChronoField.MONTH_OF_YEAR),
                  if (has(ChronoField.DAY_OF_MONTH)) int(ChronoField.DAY_OF_MONTH) else 1
                )
              )
            else None
          }
      }
      val at = zone.getOrElse(ZoneOffset.UTC)
      (date, time) match {
        case (Some(d), tm)    => ZonedDateTime.of(d, tm.getOrElse(LocalTime.MIDNIGHT), at).toInstant
        case (None, Some(tm)) => ZonedDateTime.of(LocalDate.of(1970, 1, 1), tm, at).toInstant
        case _                => throw new IllegalArgumentException("no date and no time")
      }
    }.toOption
}
