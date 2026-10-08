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

import java.time.format.DateTimeFormatter
import java.time.temporal.{ChronoField, IsoFields}
import java.time.{
  Clock,
  Instant,
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  Year,
  ZoneId,
  ZoneOffset,
  ZonedDateTime
}
import java.util.Locale
import java.util.regex.{Matcher, Pattern => Regex}
import scala.util.Try

/** A date field's mapping `format`, read as ONE Elasticsearch major reads it: the instant a value
  * of the field denotes -- the one that major INDEXED, which its queries and aggregations see, at a
  * millisecond's precision. The `||` alternatives are tried in order, and the FIRST one that parses
  * a value decides: its instant, or, when the date it parses is not a valid one (a month 31), the
  * value's rejection, the next alternative never tried -- as Elasticsearch does.
  *
  * Elasticsearch 6 parses with Joda-Time, Elasticsearch 7 and 8 with `java.time`, and the same
  * mapping reads differently across them ([[MappedDateFormat.Dialect]]). Every reading is MEASURED,
  * major by major -- every format of each major's documentation, every pattern letter at each
  * width, alone and combined, epochs and JSON numbers, written to a field of the format one value
  * at a time and the instant read back from its doc value, on Elasticsearch 6.8.23, 7.17.29 and
  * 8.18.3
  * -- and written from that behaviour and Java's own API, never from Elasticsearch's or Joda's
  * code:
  *
  *   - `epoch_millis`, `epoch_second` ([[MappedDateFormat.Epoch]]): an integer, a fraction rounded
  *     down to the millisecond, each major's rules for a negative one;
  *   - the built-in names, as grammars of their digits, separators, fractions and zones, per major;
  *   - a custom pattern: a `java.time` pattern on Elasticsearch 7 and 8, and on 6.8 behind a
  *     leading `8` ([[JavaDatePattern]]), a Joda pattern on Elasticsearch 6 ([[JodaDatePattern]]);
  *   - a field with no `format`: Elasticsearch's default,
  *     `strict_date_optional_time||epoch_millis`, which reads `"2024"` and `2024` as the year 2024,
  *     not 2024 milliseconds.
  *
  * A value is read only where its reading is the same on every Java version core runs on (8, 11,
  * 17, 21) and equal to the instant Elasticsearch indexed. A name, a pattern letter, a width or a
  * combination of letters no measurement covers, a zone other than `Z`, an offset, `UTC`, `GMT` and
  * `UT` (a region id or a zone name, whose offset comes from zone data -- the JVM core runs on's,
  * not Elasticsearch's), a reading that depends on the Java version core runs on, and a value core
  * cannot read without guessing -- a spelling or an offset the response parser no longer gives, a
  * JSON number whose digits its `double` no longer holds -- are NOT guessed: such a value is
  * [[MappedDateFormat.Undecidable]], and the statement reading it fails, naming the field and the
  * format.
  */
private[client] final case class MappedDateFormat(
  spec: String,
  dialect: MappedDateFormat.Dialect,
  alternatives: Seq[MappedDateFormat.Alternative]
) {
  import MappedDateFormat._

  /** Every alternative is a name core reads: each reads an ISO text by its ISO fields, so a value
    * the response parser already read as an ISO date denotes the same instant whatever the way it
    * was spelled, and is read as that instant.
    */
  private val namesOnly: Boolean = alternatives.forall(_.isName)

  /** The instant `text` denotes, by the first alternative that parses it. */
  def read(text: String): Outcome = {
    val it = alternatives.iterator
    var outcome: Outcome = Rejected
    var done = false
    while (!done && it.hasNext)
      it.next().parse(text) match {
        case NotParsed => ()
        case Parsed(instant) =>
          outcome = Try(Instant.ofEpochMilli(instant.toEpochMilli)).toOption.fold[Outcome](
            Rejected
          )(Read)
          done = true
        case Invalid => done = true
        case Unmeasured(reason) =>
          outcome = Undecidable(reason)
          done = true
      }
    outcome
  }

  /** The instant a value of the field denotes, whatever the class the response parser gave it: a
    * text as written; a number as its digits; a value it already read as an ISO date or time --
    * which no longer says how it was spelled -- by the spellings it may have had: the first one the
    * format reads when every alternative is a name core reads (they read every spelling as the same
    * instant), else all of them, which must agree.
    */
  def readValue(value: Any): Outcome =
    value match {
      case s: String               => read(s)
      case b: java.math.BigDecimal => read(b.toPlainString)
      case d: java.lang.Double     => byDigits(d.doubleValue(), Math.ulp(d.doubleValue()))
      case f: java.lang.Float      => byDigits(f.doubleValue(), Math.ulp(f.floatValue()).toDouble)
      case n: java.lang.Number     => read(n.toString)
      // a text with a region (`2024-07-31T10:30:00+05:00[Europe/Paris]`), which the response
      // parser read with the zone data of the JVM core runs on -- its written local time and
      // offset no longer known (Elasticsearch 8 reads the local time in the region, MEASURED)
      case z: ZonedDateTime if !z.getZone.isInstanceOf[ZoneOffset] =>
        Undecidable(zoneNotRead(z.getZone.getId))
      case _: LocalDate | _: LocalDateTime | _: ZonedDateTime | _: OffsetDateTime | _: LocalTime =>
        if (namesOnly && !value.isInstanceOf[LocalTime]) firstRead(value)
        else bySpellings(value)
      case i: Instant => Read(millis(i))
      case other      => read(other.toString)
    }

  /** A fractional JSON number, which the response parser read as a `double`: Elasticsearch read the
    * DIGITS written in the document (MEASURED: `1706697000.5` is 1706697000 milliseconds in an
    * `epoch_millis` field, 1706697000500 in an `epoch_second` one, on 6.8.23, 7.17.29 and 8.18.3),
    * which the `double` no longer says. Every number written as digits that read as this `double`
    * lies within half its unit in the last place: the value is read when every such number its
    * format reads gives the same instant -- else it is undecidable (`1706697000000.0` may have been
    * written `1706696999999.9999`, a millisecond earlier).
    */
  private def byDigits(value: Double, ulp: Double): Outcome =
    if (java.lang.Double.isNaN(value) || java.lang.Double.isInfinite(value)) Rejected
    else {
      val exact = new java.math.BigDecimal(value)
      val half = new java.math.BigDecimal(ulp).divide(java.math.BigDecimal.valueOf(2L))
      val low = exact.subtract(half)
      val high = exact.add(half)
      // every number of 0 to 9 fraction digits that reads as this double: the lowest and the
      // highest of each width (a value read with more digits than its format takes is rejected),
      // the shortest digits and both ends
      val candidates = (0 to 9).flatMap { digits =>
        Seq(
          low.setScale(digits, java.math.RoundingMode.CEILING),
          high.setScale(digits, java.math.RoundingMode.FLOOR)
        ).filter(d => d.compareTo(low) >= 0 && d.compareTo(high) <= 0)
      } ++ Seq(java.math.BigDecimal.valueOf(value).stripTrailingZeros, low, high)
      val outcomes = candidates.map(d => read(d.toPlainString))
      outcomes.collect { case Read(i) => i }.distinct match {
        case Seq(one) if !outcomes.exists(_.isInstanceOf[Undecidable]) => Read(one)
        case Seq() if !outcomes.exists(_.isInstanceOf[Undecidable])    => Rejected
        case _ =>
          Undecidable(
            "a fractional JSON number: the response gives it as a double, whose digits read " +
            "differently within its precision"
          )
      }
    }

  /** A value the response parser read as an ISO date, at a format of names only: its first spelling
    * the format reads -- the one `java.time` prints, almost always -- gives its instant. None read:
    * no alternative reads the value, which Elasticsearch could not have indexed.
    */
  private def firstRead(value: Any): Outcome = {
    // PER ROW on the extraction path: whether the format reads a value depends only on its SHAPE
    // (the digits of its year, whether it has a time, seconds, a fraction of how many digits, an
    // offset), so the spellings are read once per shape, and every other value of that shape is its
    // own instant
    val key = shapeOf(value)
    readsShape.get(key) match {
      case java.lang.Boolean.TRUE =>
        Try(millis(instantOf(value))).toOption.fold[Outcome](Rejected)(Read)
      case java.lang.Boolean.FALSE => Rejected
      case _ =>
        val it = spellings(value)
        var outcome: Outcome = Rejected
        while (outcome == Rejected && it.hasNext)
          outcome = read(it.next()) match {
            case r: Read => r
            case _       => Rejected
          }
        readsShape.put(key, java.lang.Boolean.valueOf(outcome != Rejected))
        outcome
    }
  }

  /** Whether the format reads a value of a shape ([[shapeOf]]), as it is first met. */
  private val readsShape = new java.util.concurrent.ConcurrentHashMap[Long, java.lang.Boolean]()

  /** A value the response parser read as an ISO date, read back by each ISO spelling it may have
    * had ([[spellings]]): they must agree. A date or a time whose text may have carried an offset
    * the response parser dropped (`2024-01-31+02:00` is read as the date alone) is undecidable
    * where an alternative reads such an offset.
    */
  private def bySpellings(value: Any): Outcome = {
    val texts = spellings(value).toList
    val lostOffset = value match {
      case d: LocalDate =>
        val t = DateTimeFormatter.ISO_LOCAL_DATE.format(d)
        Seq(t + "Z", t + "+01:00").exists(parses)
      case t: LocalTime => texts.exists(s => Seq(s + "Z", s + "+01:00").exists(parses))
      case _            => false
    }
    if (lostOffset)
      Undecidable(
        "the response gives it without the offset it may have been written with, which the " +
        "format reads"
      )
    else {
      val outcomes = texts.map(read)
      outcomes.collectFirst { case u: Undecidable => u } match {
        case Some(undecidable) => undecidable
        case None =>
          outcomes.collect { case Read(instant) => instant }.distinct match {
            case Seq(instant) => Read(instant)
            case Seq()        => Rejected
            case _ =>
              Undecidable(
                "the format reads it differently depending on how it was spelled, which the " +
                "response no longer says"
              )
          }
      }
    }
  }

  private def parses(text: String): Boolean =
    alternatives.exists(_.parse(text) != NotParsed)
}

private[client] object MappedDateFormat {

  /** How one Elasticsearch major reads a date: ES 6 with Joda-Time, ES 7 and ES 8 with `java.time`,
    * with their own week rules, texts and epochs (MEASURED on 6.8.23, 7.17.29 and 8.18.3). The
    * other minor versions of a major read as its measured one, and Elasticsearch 9 as Elasticsearch
    * 8 (ASSUMED: measured by the CI's legs, not locally).
    */
  sealed abstract class Dialect(val major: Int) {
    def label: String = s"Elasticsearch $major"
  }

  object Dialect {
    case object Joda extends Dialect(6)
    case object Java7 extends Dialect(7)
    case object Java8 extends Dialect(8)

    /** The dialect of an Elasticsearch `version` (`VersionApi.version`), by the predicates of
      * [[ElasticsearchVersion]]; `None` for a version before 6 or not a version.
      */
    def of(version: String): Option[Dialect] =
      Try {
        if (ElasticsearchVersion.isEs6(version)) Some(Joda)
        else if (ElasticsearchVersion.isEs7(version)) Some(Java7)
        else if (ElasticsearchVersion.isEs8OrHigher(version)) Some(Java8)
        else None
      }.toOption.flatten
  }

  /** What a format makes of a value. */
  sealed trait Outcome

  /** The instant, at a millisecond's precision. */
  final case class Read(instant: Instant) extends Outcome

  /** No alternative reads it, or the first that parses it finds no valid date in it. */
  case object Rejected extends Outcome

  /** Core cannot tell the instant without guessing: `reason` says why. */
  final case class Undecidable(reason: String) extends Outcome

  /** What ONE alternative makes of a text. */
  sealed trait Parse
  case object NotParsed extends Parse
  case object Invalid extends Parse
  final case class Parsed(instant: Instant) extends Parse
  final case class Unmeasured(reason: String) extends Parse

  trait Alternative {
    def parse(text: String): Parse

    /** A name core reads, as opposed to a custom pattern or an unknown name. */
    def isName: Boolean = true
  }

  /** A name or a pattern core does not read on this major: a value it decides is undecidable. */
  final case class Unknown(name: String, dialect: Dialect, reason: String = "")
      extends Alternative {
    def parse(text: String): Parse =
      Unmeasured(
        s"'$name' is not a date format core reads as ${dialect.label} reads it" +
        (if (reason.isEmpty) "" else s": $reason")
      )
    override def isName: Boolean = false
  }

  /** `epoch_millis` (`unitMillis` 1) or `epoch_second` (`unitMillis` 1000). */
  final case class Epoch(name: String, unitMillis: Long, dialect: Dialect) extends Alternative {
    private val shape: Regex = dialect match {
      // Joda: a sign, a fraction and an exponent, the value truncated toward zero to the unit
      case Dialect.Joda => Regex.compile("[+-]?\\d+(?:\\.\\d+)?(?:[eE][+-]?\\d+)?")
      // Elasticsearch 7 and 8: a fraction of 6 digits at most in `epoch_millis`, 9 in `epoch_second`
      // (MEASURED)
      case _ if unitMillis == 1L => Regex.compile("-?\\d++(?:\\.\\d{1,6}+)?")
      case _                     => Regex.compile("-?\\d++(?:\\.\\d{1,9}+)?")
    }

    def parse(text: String): Parse =
      if (!shape.matcher(text).matches()) NotParsed
      else if (dialect == Dialect.Joda)
        // a value past the instants Elasticsearch 6 holds is not this alternative's (MEASURED:
        // `epoch_second||epoch_millis` reads 9223372036854775807 as milliseconds)
        Try {
          val value = new java.math.BigDecimal(text)
          Instant.ofEpochMilli(
            java.lang.Math.multiplyExact(
              value.setScale(0, java.math.RoundingMode.DOWN).longValueExact(),
              unitMillis
            )
          )
        }.fold(_ => NotParsed, Parsed)
      else if (text.charAt(0) == '-') negative(this, text)
      else if (text.indexOf('.') < 0)
        // an integer, the common case: no decimal arithmetic
        Try(
          Instant.ofEpochMilli(
            java.lang.Math.multiplyExact(java.lang.Long.parseLong(text), unitMillis)
          )
        ).fold(_ => Invalid, Parsed)
      else
        Try(
          Instant.ofEpochMilli(
            new java.math.BigDecimal(text)
              .multiply(java.math.BigDecimal.valueOf(unitMillis))
              .setScale(0, java.math.RoundingMode.FLOOR)
              .longValueExact()
          )
        ).fold(_ => Invalid, Parsed)
  }

  /** A negative epoch, as each major reads it (MEASURED, every fraction width):
    *   - Elasticsearch 8 rounds it down to the millisecond (`-1.5` milliseconds is -2, `-1.5`
    *     seconds -1500 milliseconds), and rejects a negative `epoch_second` whose fraction is zero
    *     (`-1.0`);
    *   - Elasticsearch 7 reads a negative `epoch_millis` only when its integer part is a multiple
    *     of 1000 other than 0, as that integer part (`-1000.5` is -1000; `-86400` and `-1` are
    *     rejected), and a negative `epoch_second` as its integer part PLUS its fraction, rounded
    *     down to the millisecond (`-1.5` is -500 milliseconds), an integer part 0 rejected.
    */
  private def negative(epoch: Epoch, text: String): Parse = {
    val point = text.indexOf('.')
    val whole = new java.math.BigDecimal(if (point < 0) text else text.substring(0, point))
    val fraction =
      if (point < 0) java.math.BigDecimal.ZERO
      else new java.math.BigDecimal("0" + text.substring(point))
    val unit = java.math.BigDecimal.valueOf(epoch.unitMillis)
    def floor(d: java.math.BigDecimal): Parse =
      Try(
        Instant.ofEpochMilli(d.setScale(0, java.math.RoundingMode.FLOOR).longValueExact())
      ).fold(_ => Invalid, Parsed)
    epoch.dialect match {
      case Dialect.Java7 if whole.signum == 0 => NotParsed
      case Dialect.Java7 if epoch.unitMillis == 1L =>
        if (whole.remainder(java.math.BigDecimal.valueOf(1000L)).signum != 0) NotParsed
        else floor(whole)
      case Dialect.Java7 => floor(whole.add(fraction).multiply(unit))
      // Elasticsearch 8 rejects a negative `epoch_second` whose fraction is zero (`-1.0`, `-0.0`)
      case _ if epoch.unitMillis == 1000L && point >= 0 && fraction.signum == 0 => NotParsed
      case _ => floor(new java.math.BigDecimal(text).multiply(unit))
    }
  }

  /** How a parsed date is validated: STRICT (`2024-02-30` is no date) or LENIENT (`2024-02-30` is
    * `2024-03-01`, Elasticsearch 8's `strict_date`).
    */
  private[client] sealed trait Days
  private[client] case object StrictDays extends Days
  private[client] case object LenientDays extends Days

  /** A day past the month's last is its last (`2024-02-30` is `2024-02-29`: Elasticsearch 7's
    * `strict_date_hour_minute_second`, MEASURED).
    */
  private[client] case object LastDays extends Days

  private val GroupName = Regex.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>")

  /** The groups a grammar's regex declares. */
  private def groupsOf(regex: Regex): Set[String] = {
    val m = GroupName.matcher(regex.pattern())
    var names = Set.empty[String]
    while (m.find()) names += m.group(1)
    names
  }

  /** A text's fields by a grammar: `y`, `mo`, `d`, `doy` (a day of year), `wy`, `ww`, `wd` (an ISO
    * week date), `h`, `mi`, `s`, `f` (a fraction's digits) and `z` (a zone), `oy` the year of an
    * ordinal date in an alternation; a field the grammar does not hold takes January, the first
    * day, midnight, UTC, and a time with no date 1970-01-01 -- as each name was MEASURED to.
    */
  private[client] final case class Grammar(
    name: String,
    regex: Regex,
    zone: String => ZoneRead,
    days: Days = StrictDays,
    endOfDay: Boolean = false,
    override val isName: Boolean = true,
    timeDropsYear: Boolean = false,
    lastDayAt24: Boolean = false
  ) extends Alternative {
    private val groups = groupsOf(regex)

    private def group(m: Matcher, g: String): Option[String] =
      if (groups.contains(g)) Option(m.group(g)).filter(_.nonEmpty) else None

    def parse(text: String): Parse = {
      val m = regex.matcher(text)
      if (!m.matches()) NotParsed
      else
        group(m, "z").fold[ZoneRead](Some(Right(ZoneOffset.UTC)))(zone) match {
          // a zone the alternative does not read: the text is not this alternative's
          case None               => NotParsed
          case Some(Left(reason)) => Unmeasured(reason)
          case Some(Right(zoneId)) =>
            Try {
              // Elasticsearch 7 and 8 read a year with no month, day of year or week, and an hour,
              // as a time on 1970-01-01 (MEASURED)
              val yearAlone = timeDropsYear && group(m, "h").isDefined &&
                Seq("mo", "doy", "wy").forall(group(m, _).isEmpty)
              // `oy`: the year of an ordinal date in an alternation (a group name is used once)
              val year =
                if (yearAlone) 1970
                else group(m, "y").orElse(group(m, "oy")).fold(1970)(y => Integer.parseInt(y))
              val month = group(m, "mo").fold(1)(Integer.parseInt)
              val day = group(m, "d").fold(1)(Integer.parseInt)
              val at24 = group(m, "h").contains("24") &&
                Seq("mi", "s", "f").forall(g => group(m, g).forall(_.forall(_ == '0')))
              val date = (group(m, "wy"), group(m, "doy")) match {
                // an ISO week date: the week (1 when none) of the week-based year, its day (Monday
                // when none), the week a week of THAT year
                case (Some(wy), _) =>
                  val first = LocalDate.of(Integer.parseInt(wy), 1, 4)
                  val week = group(m, "ww").fold(1L)(java.lang.Long.parseLong)
                  IsoFields.WEEK_OF_WEEK_BASED_YEAR
                    .rangeRefinedBy(first)
                    .checkValidValue(week, IsoFields.WEEK_OF_WEEK_BASED_YEAR)
                  first
                    .`with`(IsoFields.WEEK_OF_WEEK_BASED_YEAR, week)
                    .`with`(
                      ChronoField.DAY_OF_WEEK,
                      group(m, "wd").fold(1L)(java.lang.Long.parseLong)
                    )
                // an ordinal date
                case (None, Some(doy)) => LocalDate.ofYearDay(year, Integer.parseInt(doy))
                case _ =>
                  (if (lastDayAt24 && at24) LastDays else days) match {
                    case StrictDays => LocalDate.of(year, month, day)
                    case LenientDays =>
                      LocalDate.of(year, 1, 1).plusMonths(month - 1L).plusDays(day - 1L)
                    case LastDays =>
                      val first = LocalDate.of(year, month, 1)
                      first.withDayOfMonth(Math.min(day, first.lengthOfMonth()))
                  }
              }
              val hour = group(m, "h").fold(0)(Integer.parseInt)
              val minute = group(m, "mi").fold(0)(Integer.parseInt)
              val second = group(m, "s").fold(0)(Integer.parseInt)
              val nano = group(m, "f").fold(0)(f => Integer.parseInt((f + "00000000").take(9)))
              val atEnd = endOfDay && hour == 24 && minute == 0 && second == 0 && nano == 0
              // 24:00 is the next day's midnight, the same day's when there is no date
              val dated = Seq("y", "oy", "wy").exists(groups.contains)
              ZonedDateTime
                .of(
                  if (atEnd && dated) date.plusDays(1) else date,
                  if (atEnd) LocalTime.MIDNIGHT else LocalTime.of(hour, minute, second, nano),
                  zoneId
                )
                .toInstant
            }.fold(_ => Invalid, Parsed)
        }
    }
  }

  /** Joda's two-digit year (MEASURED on 6.8.23, 2026-10-07: `00`..`45` are 2000..2045, `46`..`99`
    * 1946..1999): the year of those digits within 50 years of the current year minus 30. Any other
    * digit count, or a sign, is the year as written.
    */
  private[client] def twoDigitYear(text: String, clock: Clock): Int =
    if (text.length == 2 && text.forall(Character.isDigit)) {
      val low = Year.now(clock).getValue - 30 - 50
      low + (((Integer.parseInt(text) - low) % 100) + 100) % 100
    } else Integer.parseInt(text)

  // ---------------------------------------------------------------------------------------------
  // Zones
  // ---------------------------------------------------------------------------------------------

  /** An offset: hours, hours and minutes, or hours, minutes and seconds, with colons all or none.
    */
  private val OffsetText = "[+-]\\d{2}(?::\\d{2}(?::\\d{2})?|\\d{2}(?:\\d{2})?)?"
  private val Offset = Regex.compile("([+-])(\\d{2})(?::?(\\d{2})(?::?(\\d{2}))?)?")

  private def offsetOf(text: String): Option[ZoneId] = {
    val m = Offset.matcher(text)
    if (!m.matches()) None
    else
      Try {
        val sign = if (m.group(1) == "-") -1 else 1
        def part(i: Int) = Option(m.group(i)).fold(0)(Integer.parseInt)
        ZoneOffset.ofHoursMinutesSeconds(sign * part(2), sign * part(3), sign * part(4)): ZoneId
      }.toOption
  }

  /** What an alternative makes of the text of a zone: `None`, no zone it reads (the text is not the
    * alternative's); a zone; or why core does not read it (`Left`): the value is [[Unmeasured]].
    */
  private[client] type ZoneRead = Option[Either[String, ZoneId]]

  private def zoneRead(zone: Option[ZoneId]): ZoneRead = zone.map(Right(_))

  /** The fixed ids, which every JVM and every Elasticsearch major read as UTC. */
  private val FixedIds: Set[String] = Set("UTC", "GMT", "UT")

  /** A fixed id followed by an offset, in one of `ZoneOffset.of`'s forms: a sign, then 1 or 2
    * digits of hours, or 2 with 2 of minutes and 2 of seconds, colons all or none (`GMT+1`,
    * `UTC+01:00`).
    */
  private val PrefixedOffset = Regex.compile(
    "(?:UTC|GMT|UT)([+-])(?:(\\d{1,2}+)|(\\d{2}):(\\d{2})(?::(\\d{2}))?|(\\d{2})(\\d{2})(\\d{2})?)"
  )

  /** A fixed id (`UTC`, `GMT`, `UT`) and, where `prefixed`, a fixed id followed by an offset
    * (`GMT+1`): a fixed offset, read here -- never through the zone data of the JVM core runs on.
    * `None` for any other text.
    */
  private[client] def fixedId(text: String, prefixed: Boolean): Option[ZoneOffset] =
    if (FixedIds.contains(text)) Some(ZoneOffset.UTC)
    else if (!prefixed) None
    else {
      val m = PrefixedOffset.matcher(text)
      if (!m.matches()) None
      else
        Try {
          val sign = if (m.group(1) == "-") -1 else 1
          def part(groups: Int*) =
            groups.flatMap(g => Option(m.group(g))).headOption.fold(0)(Integer.parseInt)
          ZoneOffset.ofHoursMinutesSeconds(
            sign * part(2, 3, 6),
            sign * part(4, 7),
            sign * part(5, 8)
          )
        }.toOption
    }

  private val PrefixedOffsetId = Regex.compile("(?:UTC|GMT|UT)[+-].*")

  /** Why core does not read a zone id or a zone name other than `UTC`, `GMT`, `UT` and their
    * offsets: its offset comes from zone data -- the JVM core runs on would give it, not the one
    * Elasticsearch indexed it with (MEASURED: `America/Mexico_City`, `Asia/Tehran`, `Asia/Amman`,
    * `America/Asuncion` and others read differently on 6.8.23, 8.18.3 and core's JVMs).
    */
  private[client] def zoneNotRead(text: String): String =
    s"the zone '$text' is a region id or a zone name, whose offset comes from the zone data of " +
    "the JVM reading it, not Elasticsearch's: core reads only Z, an offset, UTC, GMT and UT"

  /** `Z` or an offset. */
  private def utcOrOffset(text: String): ZoneRead =
    zoneRead(if (text == "Z") Some(ZoneOffset.UTC) else offsetOf(text))

  /** Joda: `Z`, `z` or an offset. */
  private def jodaOffset(text: String): ZoneRead =
    zoneRead(if (text == "Z" || text == "z") Some(ZoneOffset.UTC) else offsetOf(text))

  /** `Z`, an offset, `UTC`, `GMT` or `UT`; an id holding an offset (`GMT+1`) is not read by the
    * names using this (MEASURED: rejected), and any other id is not read by core ([[zoneNotRead]]).
    */
  private def regionZone(text: String): ZoneRead =
    utcOrOffset(text).orElse {
      if (PrefixedOffsetId.matcher(text).matches()) None
      else Some(fixedId(text, prefixed = false).toRight(zoneNotRead(text)))
    }

  /** `Z`, an offset, `UTC`, `GMT`, `UT` and an id holding an offset (`GMT+1`, as `ZoneOffset.of`
    * reads it, else not a zone); any other id is not read by core ([[zoneNotRead]]).
    */
  private def anyZone(text: String): ZoneRead =
    utcOrOffset(text).orElse {
      if (PrefixedOffsetId.matcher(text).matches()) zoneRead(fixedId(text, prefixed = true))
      else Some(fixedId(text, prefixed = false).toRight(zoneNotRead(text)))
    }

  private val ZoneIdText = "[A-Za-z][A-Za-z0-9_/+~-]*+"

  // ---------------------------------------------------------------------------------------------
  // The names, per major (MEASURED)
  // ---------------------------------------------------------------------------------------------

  private def g(text: String): Regex = Regex.compile(text)

  /** Elasticsearch 7 and 8 (`java.time`): every name of their documentation, as MEASURED on 7.17.29
    * and 8.18.3. `es8` is false for Elasticsearch 7, which reads no zone id holding an offset
    * (`GMT+1`) where Elasticsearch 8 does, and reads a day past the month's last in
    * `strict_date_hour_minute_second` as the month's last.
    */
  private def javaNames(es8: Boolean): Map[String, Alternative] = {
    // years: strict (4 digits, a sign), strict with 5 and more digits signed, any
    val y4 = "(?<y>-?\\d{4})"
    val y4p = "(?<y>-?\\d{4}|[+-]\\d{5,9}+)"
    val y9 = "(?<y>-?\\d{1,9}+)"
    val m2 = "(?<mo>\\d{2})"
    val m12 = "(?<mo>\\d{1,2}+)"
    val d2 = "(?<d>\\d{2})"
    val d12 = "(?<d>\\d{1,2}+)"
    val h2 = "(?<h>\\d{2})"
    val h12 = "(?<h>\\d{1,2}+)"
    val mi2 = "(?<mi>\\d{2})"
    val mi12 = "(?<mi>\\d{1,2}+)"
    val s2 = "(?<s>\\d{2})"
    val s12 = "(?<s>\\d{1,2}+)"
    val f = "(?<f>\\d{1,9}+)"
    val f3 = "(?<f>\\d{1,3}+)"
    // a zone: Z, an offset or a zone id, read by `wideZone` (any id `java.time` reads on ES 8, a
    // region on ES 7)
    val zone = s"(?<z>Z|$OffsetText|$ZoneIdText)"
    val wideZone: String => ZoneRead = if (es8) anyZone else regionZone
    // the strict week names also read a lower-case `z` (MEASURED, 8.18.3 and 7.17.29)
    val zoneOrLowerZ = s"(?<z>Z|z|$OffsetText|$ZoneIdText)"
    val zOrUtc: String => ZoneRead = t => if (t == "z") Some(Right(ZoneOffset.UTC)) else wideZone(t)
    // date_time: Z or an offset with no seconds
    val shortOffset = "(?<z>Z|[+-]\\d{2}(?::?\\d{2})?)"
    // the date part of the strict and the lenient date-time names: the month and the day optional
    val strictDay = s"$y4(?:-$m2(?:-$d2)?)?"
    val anyDay = s"$y9(?:-$m12(?:-$d12)?)?"
    val strictDate = s"$y4p-$m2-$d2"
    val date = s"$y9-$m12-$d12"
    val ordinal = s"$y4-(?<doy>\\d{1,3}+)"
    val strictOrdinal = s"$y4-(?<doy>\\d{3})"
    val basicTime = "(?<h>\\d{2})(?<mi>\\d{2})(?<s>\\d{2})"
    def name(
      n: String,
      regex: String,
      z: String => ZoneRead = utcOrOffset,
      days: Days = StrictDays,
      endOfDay: Boolean = false,
      lastDayAt24: Boolean = false
    ): (String, Alternative) =
      n -> Grammar(n, g(regex), z, days, endOfDay, timeDropsYear = true, lastDayAt24 = lastDayAt24)
    Map(
      name(
        "strict_date_optional_time",
        s"$strictDay(?:T$h2(?::$mi2(?::$s2(?:[.,]$f)?)?)?$zone?)?",
        wideZone
      ),
      name("iso8601", s"$strictDay(?:T$h2(?::$mi2(?::$s2(?:[.,]$f)?)?)?$zone?)?", wideZone),
      name(
        "strict_date_optional_time_nanos",
        s"$strictDay(?:T$h2:$mi2:$s2(?:[.,]$f)?$zone?)?",
        wideZone
      ),
      name(
        "date_optional_time",
        s"$anyDay(?:T$h12(?::$mi12(?::$s12(?:[.,]$f)?)?$zone?)?)?",
        regionZone
      ),
      name("strict_date_time", s"${strictDay}T$h2:$mi2:$s2(?:\\.$f)?$zone", wideZone),
      name("date_time", s"${anyDay}T$h12:$mi12(?::$s12\\.$f)?$shortOffset"),
      name("strict_date_time_no_millis", s"${strictDay}T$h2:$mi2:$s2$zone", wideZone),
      name("date_time_no_millis", s"${anyDay}T$h12:$mi12(?::$s12)?$zone?", regionZone),
      name("strict_date", strictDate, days = LenientDays),
      name("date", s"$y9(?:-$m12(?:-$d12)?)?"),
      name("strict_date_hour", s"${strictDate}T$h2", days = LastDays, endOfDay = true),
      name("date_hour", s"${anyDay}T$h12"),
      name("strict_date_hour_minute", s"${strictDate}T$h2:$mi2", days = LastDays, endOfDay = true),
      name("date_hour_minute", s"${anyDay}T$h12:$mi12"),
      name(
        "strict_date_hour_minute_second",
        s"${strictDate}T$h2:$mi2:$s2",
        days = if (es8) StrictDays else LastDays,
        endOfDay = true,
        lastDayAt24 = true
      ),
      name("date_hour_minute_second", s"${anyDay}T$h12:$mi12:$s12"),
      name("strict_date_hour_minute_second_fraction", s"${strictDay}T$h2:$mi2:$s2\\.$f"),
      name("date_hour_minute_second_fraction", s"${anyDay}T$h2:$mi2:$s2\\.$f"),
      name("strict_date_hour_minute_second_millis", s"${strictDay}T$h2:$mi2:$s2\\.$f"),
      name("date_hour_minute_second_millis", s"${anyDay}T$h2:$mi2:$s2\\.$f3"),
      name("strict_hour", h2, endOfDay = true),
      name("hour", h12),
      name("strict_hour_minute", s"$h2:$mi2"),
      name("hour_minute", s"$h12:$mi12"),
      name("strict_hour_minute_second", s"$h2:$mi2:$s2"),
      name("hour_minute_second", s"$h12:$mi12:$s12"),
      name("strict_hour_minute_second_fraction", s"$h2:$mi2:$s2\\.$f"),
      name("hour_minute_second_fraction", s"$h2:$mi2:$s2\\.$f"),
      name("strict_hour_minute_second_millis", s"$h2:$mi2:$s2\\.$f"),
      name("hour_minute_second_millis", s"$h2:$mi2:$s2\\.$f3"),
      name("strict_time", s"$h2:$mi2:$s2\\.$f$zone", wideZone),
      name("time", s"$h2:$mi2:$s2\\.$f$zone", wideZone),
      name("strict_time_no_millis", s"$h2:$mi2:$s2$zone", wideZone),
      name("time_no_millis", s"$h12:$mi12:$s12$zone", wideZone),
      name("strict_t_time", s"T$h2:$mi2:$s2\\.$f$zone", wideZone),
      name("t_time", s"T$h2:$mi2:$s2\\.$f$zone", wideZone),
      name("strict_t_time_no_millis", s"T$h2:$mi2:$s2$zone", wideZone),
      name("t_time_no_millis", s"T$h12:$mi12:$s12$zone", wideZone),
      name("strict_ordinal_date", strictOrdinal),
      name("ordinal_date", ordinal),
      name(
        "strict_ordinal_date_time",
        s"${strictOrdinal}T$h2:$mi2(?::$s2\\.$f)?$zone",
        wideZone
      ),
      name("ordinal_date_time", s"${ordinal}T$h12:$mi12(?::$s2\\.$f)?$zone", wideZone),
      name("strict_ordinal_date_time_no_millis", s"${strictOrdinal}T$h2:$mi2:$s2$zone", wideZone),
      name("ordinal_date_time_no_millis", s"${ordinal}T$h12:$mi12:$s12$zone", wideZone),
      name("basic_date", "(?<y>\\d{4})(?<mo>\\d{2})(?<d>\\d{1,2})"),
      name(
        "basic_date_time",
        s"(?<y>\\d{4})(?<mo>\\d{2})(?<d>\\d{2})T$basicTime\\.$f$zone",
        wideZone
      ),
      name(
        "basic_date_time_no_millis",
        s"(?<y>\\d{4})(?<mo>\\d{2})(?<d>\\d{2})T$basicTime$zone",
        wideZone
      ),
      name("basic_ordinal_date", "(?<y>\\d{4})(?<doy>\\d{3})"),
      name("basic_ordinal_date_time", s"(?<y>\\d{4})(?<doy>\\d{3})T$basicTime\\.$f$zone", wideZone),
      name(
        "basic_ordinal_date_time_no_millis",
        s"(?<y>\\d{4})(?<doy>\\d{3})T$basicTime$zone",
        wideZone
      ),
      name("basic_time", s"$basicTime\\.$f$zone", wideZone),
      name("basic_time_no_millis", s"$basicTime$zone", wideZone),
      name("basic_t_time", s"T$basicTime\\.$f$zone", wideZone),
      name("basic_t_time_no_millis", s"T$basicTime$zone", wideZone),
      name("basic_week_date", "(?<wy>\\d{4})W(?<ww>\\d{2})(?<wd>\\d)"),
      name("strict_basic_week_date", "(?<wy>\\d{4})W(?<ww>\\d{2})(?<wd>\\d)"),
      name(
        "basic_week_date_time",
        s"(?<wy>\\d{4})W(?<ww>\\d{2})(?<wd>\\d)T$basicTime\\.$f$zone",
        wideZone
      ),
      name(
        "strict_basic_week_date_time",
        s"(?<wy>\\d{4})W(?<ww>\\d{2})(?<wd>\\d)T$basicTime\\.$f$zone",
        wideZone
      ),
      name(
        "basic_week_date_time_no_millis",
        s"(?<wy>\\d{4})W(?<ww>\\d{2})(?<wd>\\d)T$basicTime$zone",
        wideZone
      ),
      name(
        "strict_basic_week_date_time_no_millis",
        s"(?<wy>\\d{4})W(?<ww>\\d{2})(?<wd>\\d)T$basicTime$zone",
        wideZone
      ),
      name("strict_week_date", "(?<wy>-?\\d{4})-W(?<ww>\\d{2})-(?<wd>\\d)"),
      name("week_date", "(?<wy>-?\\d{4})-W(?<ww>\\d{1,2}+)-(?<wd>\\d)"),
      name(
        "strict_week_date_time",
        s"(?<wy>-?\\d{4})-W(?<ww>\\d{2})-(?<wd>\\d)T$h2:$mi2:$s2\\.$f$zoneOrLowerZ",
        zOrUtc
      ),
      name(
        "week_date_time",
        s"(?<wy>-?\\d{4})-W(?<ww>\\d{1,2}+)-(?<wd>\\d)T$h2:$mi2:$s2\\.$f$zone",
        wideZone
      ),
      name(
        "strict_week_date_time_no_millis",
        s"(?<wy>-?\\d{4})-W(?<ww>\\d{2})-(?<wd>\\d)T$h2:$mi2:$s2$zoneOrLowerZ",
        zOrUtc
      ),
      name(
        "week_date_time_no_millis",
        s"(?<wy>-?\\d{4})-W(?<ww>\\d{1,2}+)-(?<wd>\\d)T$h12:$mi12:$s12$zone",
        wideZone
      ),
      name("strict_weekyear", "(?<wy>-?\\d{4}|[+-]\\d{5,9}+)"),
      name("weekyear", "(?<wy>-?\\d{1,9}+)"),
      name("strict_weekyear_week", "(?<wy>-?\\d{4})-W(?<ww>\\d{2})"),
      name("weekyear_week", "(?<wy>-?\\d{1,9}+)-W(?<ww>\\d{1,2}+)"),
      name("strict_weekyear_week_day", "(?<wy>-?\\d{4})-W(?<ww>\\d{2})-(?<wd>\\d{1,2}+)"),
      name("weekyear_week_day", "(?<wy>-?\\d{1,9}+)-W(?<ww>\\d{1,2}+)-(?<wd>\\d{1,2}+)"),
      name("strict_year_month_day", s"$y4(?:-$m2(?:-$d2)?)?"),
      name("year_month_day", date),
      name("strict_year_month", s"$y4-$m2"),
      name("year_month", s"$y9-$m12"),
      name("strict_year", y4),
      name("year", y9)
    )
  }

  /** Elasticsearch 6 (Joda-Time): every name of its documentation, as MEASURED on 6.8.23. */
  private val jodaNames: Map[String, Alternative] = {
    val y4 = "(?<y>[+-]?\\d{4})"
    val y9 = "(?<y>[+-]?\\d{1,9}+)"
    val m2 = "(?<mo>\\d{2})"
    val m12 = "(?<mo>\\d{1,2}+)"
    val d2 = "(?<d>\\d{2})"
    val d12 = "(?<d>\\d{1,2}+)"
    val h2 = "(?<h>\\d{2})"
    val h12 = "(?<h>\\d{1,2}+)"
    val mi2 = "(?<mi>\\d{2})"
    val mi12 = "(?<mi>\\d{1,2}+)"
    val s2 = "(?<s>\\d{2})"
    val s12 = "(?<s>\\d{1,2}+)"
    val f = "(?<f>\\d{1,9}+)"
    val f3 = "(?<f>\\d{1,3}+)"
    val zone = s"(?<z>[Zz]|$OffsetText)"
    val strictDate = s"$y4-$m2-$d2"
    val date = s"$y9-$m12-$d12"
    val ordinal = s"$y9-(?<doy>\\d{1,3}+)"
    val strictOrdinal = s"$y4-(?<doy>\\d{3})"
    val week = "(?<wy>[+-]?\\d{1,9}+)-W(?<ww>\\d{1,2}+)-(?<wd>\\d)"
    val strictWeek = "(?<wy>[+-]?\\d{4})-W(?<ww>\\d{2})-(?<wd>\\d)"
    val basicTime = "(?<h>\\d{2})(?<mi>\\d{2})(?<s>\\d{2})"
    val basicWeek = "(?<wy>\\d{4})W(?<ww>\\d{2})(?<wd>\\d)"
    def name(n: String, regex: String): (String, Alternative) =
      n -> Grammar(n, g(regex), jodaOffset)
    Map(
      // a calendar, a week or an ordinal date, then a time (MEASURED)
      name(
        "strict_date_optional_time",
        s"(?:$y4(?:-$m2(?:-$d2)?)?|(?<wy>[+-]?\\d{4})-W(?<ww>\\d{2})(?:-(?<wd>\\d))?|" +
        s"(?<oy>[+-]?\\d{4})-(?<doy>\\d{3}))(?:T$h2(?::$mi2(?::$s2(?:[.,]$f)?)?)?$zone?)?"
      ),
      name(
        "date_optional_time",
        s"(?:$y9(?:-$m12(?:-$d12)?)?|(?<wy>[+-]?\\d{1,9}+)-W(?<ww>\\d{1,2}+)(?:-(?<wd>\\d))?|" +
        s"(?<oy>[+-]?\\d{1,9}+)-(?<doy>\\d{3}))(?:T$h12(?::$mi12(?::$s12(?:[.,]$f)?)?)?$zone?)?"
      ),
      name("strict_date_time", s"${strictDate}T$h2:$mi2:$s2\\.$f$zone"),
      name("date_time", s"${date}T$h12:$mi12:$s12\\.$f$zone"),
      name("strict_date_time_no_millis", s"${strictDate}T$h2:$mi2:$s2$zone"),
      name("date_time_no_millis", s"${date}T$h12:$mi12:$s12$zone"),
      name("strict_date", strictDate),
      name("date", date),
      name("strict_date_hour", s"${strictDate}T$h2"),
      name("date_hour", s"${date}T$h12"),
      name("strict_date_hour_minute", s"${strictDate}T$h2:$mi2"),
      name("date_hour_minute", s"${date}T$h12:$mi12"),
      name("strict_date_hour_minute_second", s"${strictDate}T$h2:$mi2:$s2"),
      name("date_hour_minute_second", s"${date}T$h12:$mi12:$s12"),
      name("strict_date_hour_minute_second_fraction", s"${strictDate}T$h2:$mi2:$s2\\.$f"),
      name("date_hour_minute_second_fraction", s"${date}T$h2:$mi2:$s2\\.$f"),
      name("strict_date_hour_minute_second_millis", s"${strictDate}T$h2:$mi2:$s2\\.$f3"),
      name("date_hour_minute_second_millis", s"${date}T$h2:$mi2:$s2\\.$f3"),
      name("strict_hour", "(?<h>\\d{2}|\\+\\d)"),
      name("hour", h12),
      name("strict_hour_minute", s"$h2:$mi2"),
      name("hour_minute", s"$h12:$mi12"),
      name("strict_hour_minute_second", s"$h2:$mi2:$s2"),
      name("hour_minute_second", s"$h12:$mi12:$s12"),
      name("strict_hour_minute_second_fraction", s"$h2:$mi2:$s2\\.$f"),
      name("hour_minute_second_fraction", s"$h2:$mi2:$s2\\.$f"),
      name("strict_hour_minute_second_millis", s"$h2:$mi2:$s2\\.$f3"),
      name("hour_minute_second_millis", s"$h2:$mi2:$s2\\.$f3"),
      name("strict_time", s"$h2:$mi2:$s2\\.$f$zone"),
      name("time", s"$h2:$mi2:$s2\\.$f$zone"),
      name("strict_time_no_millis", s"$h2:$mi2:$s2$zone"),
      name("time_no_millis", s"$h12:$mi12:$s12$zone"),
      name("strict_t_time", s"T$h2:$mi2:$s2\\.$f$zone"),
      name("t_time", s"T$h2:$mi2:$s2\\.$f$zone"),
      name("strict_t_time_no_millis", s"T$h2:$mi2:$s2$zone"),
      name("t_time_no_millis", s"T$h12:$mi12:$s12$zone"),
      name("strict_ordinal_date", strictOrdinal),
      name("ordinal_date", ordinal),
      name("strict_ordinal_date_time", s"${strictOrdinal}T$h2:$mi2:$s2\\.$f$zone"),
      name("ordinal_date_time", s"${ordinal}T$h2:$mi2:$s2\\.$f$zone"),
      name("strict_ordinal_date_time_no_millis", s"${strictOrdinal}T$h2:$mi2:$s2$zone"),
      name("ordinal_date_time_no_millis", s"${ordinal}T$h12:$mi12:$s12$zone"),
      name("basic_date", "(?<y>\\d{4})(?<mo>\\d{2})(?<d>\\d{2})"),
      name("basic_date_time", s"(?<y>\\d{4})(?<mo>\\d{2})(?<d>\\d{2})T$basicTime\\.$f$zone"),
      name("basic_date_time_no_millis", s"(?<y>\\d{4})(?<mo>\\d{2})(?<d>\\d{2})T$basicTime$zone"),
      name("basic_ordinal_date", "(?<y>\\d{4})(?<doy>\\d{3})"),
      name("basic_ordinal_date_time", s"(?<y>\\d{4})(?<doy>\\d{3})T$basicTime\\.$f$zone"),
      name("basic_ordinal_date_time_no_millis", s"(?<y>\\d{4})(?<doy>\\d{3})T$basicTime$zone"),
      name("basic_time", s"$basicTime\\.$f$zone"),
      name("basic_time_no_millis", s"$basicTime$zone"),
      name("basic_t_time", s"T$basicTime\\.$f$zone"),
      name("basic_t_time_no_millis", s"T$basicTime$zone"),
      name("basic_week_date", basicWeek),
      name("strict_basic_week_date", basicWeek),
      name("basic_week_date_time", s"${basicWeek}T$basicTime\\.$f$zone"),
      name("strict_basic_week_date_time", s"${basicWeek}T$basicTime\\.$f$zone"),
      name("basic_week_date_time_no_millis", s"${basicWeek}T$basicTime$zone"),
      name("strict_basic_week_date_time_no_millis", s"${basicWeek}T$basicTime$zone"),
      name("strict_week_date", strictWeek),
      name("week_date", week),
      name("strict_week_date_time", s"${strictWeek}T$h2:$mi2:$s2\\.$f$zone"),
      name("week_date_time", s"${week}T$h2:$mi2:$s2\\.$f$zone"),
      name("strict_week_date_time_no_millis", s"${strictWeek}T$h2:$mi2:$s2$zone"),
      name("week_date_time_no_millis", s"${week}T$h12:$mi12:$s12$zone"),
      name("strict_weekyear", "(?<wy>[+-]?\\d{4})"),
      name("weekyear", "(?<wy>[+-]?\\d{1,9}+)"),
      name("strict_weekyear_week", "(?<wy>[+-]?\\d{4})-W(?<ww>\\d{2})"),
      name("weekyear_week", "(?<wy>[+-]?\\d{1,9}+)-W(?<ww>\\d{1,2}+)"),
      name("strict_weekyear_week_day", strictWeek),
      name("weekyear_week_day", week),
      name("strict_year_month_day", strictDate),
      name("year_month_day", date),
      name("strict_year_month", s"$y4-$m2"),
      name("year_month", s"$y9-$m12"),
      name("strict_year", y4),
      name("year", y9)
    )
  }

  private lazy val java7Names = javaNames(es8 = false)
  private lazy val java8Names = javaNames(es8 = true)

  /** Elasticsearch's built-in format names (its documentation's list): a name core does not read is
    * [[Unknown]], never taken for a pattern. Elasticsearch 6 and 7 also take them in camel case
    * (`strictDateOptionalTime`).
    */
  private val BuiltInNames: Set[String] = snakeNames.toSet ++ camelNames.keySet

  /** Elasticsearch 6 and 7 also read a name in camel case (`strictDateOptionalTime`) as the name
    * (MEASURED: every name but `epochMillis` and `epochSecond`, which they refuse); Elasticsearch 8
    * reads it as a pattern.
    */
  private lazy val camelNames: Map[String, String] =
    snakeNames
      .filterNot(n => n == "epoch_millis" || n == "epoch_second")
      .map { name =>
        val parts = name.split('_')
        (parts.head + parts.tail.map(_.capitalize).mkString) -> name
      }
      .filter { case (camel, snake) => camel != snake }
      .toMap

  private lazy val snakeNames: Seq[String] = {
    val snake = Seq(
      "epoch_millis",
      "epoch_second",
      "date_optional_time",
      "strict_date_optional_time",
      "strict_date_optional_time_nanos",
      "basic_date",
      "basic_date_time",
      "basic_date_time_no_millis",
      "basic_ordinal_date",
      "basic_ordinal_date_time",
      "basic_ordinal_date_time_no_millis",
      "basic_time",
      "basic_time_no_millis",
      "basic_t_time",
      "basic_t_time_no_millis",
      "basic_week_date",
      "strict_basic_week_date",
      "basic_week_date_time",
      "strict_basic_week_date_time",
      "basic_week_date_time_no_millis",
      "strict_basic_week_date_time_no_millis",
      "date",
      "strict_date",
      "date_hour",
      "strict_date_hour",
      "date_hour_minute",
      "strict_date_hour_minute",
      "date_hour_minute_second",
      "strict_date_hour_minute_second",
      "date_hour_minute_second_fraction",
      "strict_date_hour_minute_second_fraction",
      "date_hour_minute_second_millis",
      "strict_date_hour_minute_second_millis",
      "date_time",
      "strict_date_time",
      "date_time_no_millis",
      "strict_date_time_no_millis",
      "hour",
      "strict_hour",
      "hour_minute",
      "strict_hour_minute",
      "hour_minute_second",
      "strict_hour_minute_second",
      "hour_minute_second_fraction",
      "strict_hour_minute_second_fraction",
      "hour_minute_second_millis",
      "strict_hour_minute_second_millis",
      "ordinal_date",
      "strict_ordinal_date",
      "ordinal_date_time",
      "strict_ordinal_date_time",
      "ordinal_date_time_no_millis",
      "strict_ordinal_date_time_no_millis",
      "time",
      "strict_time",
      "time_no_millis",
      "strict_time_no_millis",
      "t_time",
      "strict_t_time",
      "t_time_no_millis",
      "strict_t_time_no_millis",
      "week_date",
      "strict_week_date",
      "week_date_time",
      "strict_week_date_time",
      "week_date_time_no_millis",
      "strict_week_date_time_no_millis",
      "weekyear",
      "strict_weekyear",
      "weekyear_week",
      "strict_weekyear_week",
      "weekyear_week_day",
      "strict_weekyear_week_day",
      "year",
      "strict_year",
      "year_month",
      "strict_year_month",
      "year_month_day",
      "strict_year_month_day",
      "iso8601"
    )
    snake
  }

  /** The format of a field, as `dialect` reads it: its `||` alternatives, each a built-in name or a
    * custom pattern. `clock` dates Joda's two-digit years.
    */
  def apply(spec: String, dialect: Dialect, clock: Clock = Clock.systemUTC()): MappedDateFormat = {
    // Elasticsearch 6.8 reads a format with a leading `8` -- every alternative -- with `java.time`
    val java6 = dialect == Dialect.Joda && spec.length > 1 && spec.charAt(0) == '8'
    MappedDateFormat(
      spec,
      dialect,
      (if (java6) spec.substring(1) else spec)
        .split("\\|\\|")
        .toSeq
        .map(_.trim)
        .filter(_.nonEmpty)
        .map(alternative(_, dialect, clock, java6))
    )
  }

  private def alternative(
    spelled: String,
    dialect: Dialect,
    clock: Clock,
    java6: Boolean
  ): Alternative = {
    // Elasticsearch 7 and 8 read a format with a leading `8` -- the `java.time` selector of 6.8 --
    // as the format without it (MEASURED: every name and pattern, 0 difference); 6 and 7 read a name
    // in camel case as the name
    val unprefixed =
      if (dialect != Dialect.Joda && spelled.length > 1 && spelled.charAt(0) == '8')
        spelled.substring(1)
      else spelled
    val text =
      if (dialect == Dialect.Java8 || java6) unprefixed
      else camelNames.getOrElse(unprefixed, unprefixed)
    val names = dialect match {
      case Dialect.Joda  => jodaNames
      case Dialect.Java7 => java7Names
      case Dialect.Java8 => java8Names
    }
    // Elasticsearch 6.8's java.time epochs read as Elasticsearch 7's (MEASURED, 0 difference)
    val epochs = if (java6) Dialect.Java7 else dialect
    text match {
      case "epoch_millis" => Epoch(text, 1L, epochs)
      case "epoch_second" => Epoch(text, 1000L, epochs)
      // the names of Elasticsearch 6.8's java.time mode read their own way, which core does not
      case _ if java6 && BuiltInNames.contains(text) =>
        Unknown(spelled, dialect, "a name of Elasticsearch 6.8's java.time mode ('8')")
      case _ if java6 =>
        val pattern = JavaDatePattern.Pattern(text, JavaDatePattern.Es6)
        pattern.unread.fold[Alternative](pattern)(Unknown(spelled, dialect, _))
      case _ =>
        names.get(text) match {
          case Some(name)                          => name
          case None if BuiltInNames.contains(text) => Unknown(spelled, dialect)
          case None =>
            dialect match {
              case Dialect.Joda =>
                val pattern = JodaDatePattern.Pattern(text, clock)
                pattern.unread.fold[Alternative](pattern)(Unknown(spelled, dialect, _))
              case Dialect.Java7 | Dialect.Java8 =>
                val pattern = JavaDatePattern.Pattern(
                  text,
                  if (dialect == Dialect.Java7) JavaDatePattern.Es7 else JavaDatePattern.Es8
                )
                pattern.unread.fold[Alternative](pattern)(Unknown(spelled, dialect, _))
            }
        }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Values the response parser already read as ISO dates
  // ---------------------------------------------------------------------------------------------

  private def instantOf(value: Any): Instant =
    value match {
      case z: ZonedDateTime  => z.toInstant
      case o: OffsetDateTime => o.toInstant
      case l: LocalDateTime  => l.toInstant(ZoneOffset.UTC)
      case d: LocalDate      => d.atStartOfDay(ZoneOffset.UTC).toInstant
      case other             => throw new IllegalArgumentException(s"not a date: $other")
    }

  /** What decides whether a name reads a value the response parser read as an ISO date -- the shape
    * of its [[spellings]]: its class, its year's sign and digit count, whether its seconds and its
    * fraction are zero, its fraction's significant digits, and its offset's (zero, with seconds)
    * presence -- a value with a region is never read (`readValue`). Two values of one shape are
    * read alike.
    */
  private def shapeOf(value: Any): Long = {
    def year(y: Int): Long = {
      val digits = Math.max(4, String.valueOf(Math.abs(y.toLong)).length).toLong
      (if (y < 0) 1L else if (y > 9999) 2L else 0L) | (digits << 2)
    }
    def time(t: LocalTime): Long = {
      val nano = t.getNano
      val significant =
        if (nano == 0) 0
        else {
          var n = nano; var d = 9
          while (n % 10 == 0) { n /= 10; d -= 1 }
          d
        }
      (if (t.getSecond == 0) 1L else 0L) | (if (nano == 0) 2L else 0L) | (significant.toLong << 2)
    }
    def offset(o: ZoneOffset): Long =
      (if (o.getTotalSeconds == 0) 1L else 0L) | (if (o.getTotalSeconds % 60 != 0) 2L else 0L)
    value match {
      case d: LocalDate     => 1L | (year(d.getYear) << 4)
      case l: LocalDateTime => 2L | (year(l.getYear) << 4) | (time(l.toLocalTime) << 12)
      case o: OffsetDateTime =>
        3L | (year(o.getYear) << 4) | (time(o.toLocalTime) << 12) | (offset(o.getOffset) << 20)
      case z: ZonedDateTime =>
        4L | (year(z.getYear) << 4) | (time(z.toLocalTime) << 12) | (offset(z.getOffset) << 20)
      case _ => 0L
    }
  }

  /** Elasticsearch's `date` keeps milliseconds. */
  private def millis(instant: Instant): Instant = Instant.ofEpochMilli(instant.toEpochMilli)

  /** Every text the response parser reads as `value` (MEASURED against `java.time`'s ISO
    * formatters, which it uses): the date as `yyyy-MM-dd`; the time with or without its zero
    * seconds, a fraction in any width from its significant digits to nine, or a bare `.`; an offset
    * as `Z`, `+00:00`, `-00:00` and with zero seconds (a value with a region is never read,
    * `readValue`).
    */
  private[client] def spellings(value: Any): Iterator[String] = {
    // LAZY: a format of names only reads the first spelling, java.time's own, almost always
    def date(d: LocalDate) = DateTimeFormatter.ISO_LOCAL_DATE.format(d)
    def times(t: LocalTime): Iterator[String] = {
      val hm = f"${t.getHour}%02d:${t.getMinute}%02d"
      lazy val hms = f"$hm:${t.getSecond}%02d"
      val fractions: Iterator[String] =
        if (t.getNano == 0) Iterator("", ".") ++ (1 to 9).iterator.map(w => "." + ("0" * w))
        else {
          val nine = f"${t.getNano}%09d"
          val significant = nine.reverse.dropWhile(_ == '0').length
          (significant to 9).iterator.map(w => "." + nine.take(w))
        }
      (if (t.getSecond == 0 && t.getNano == 0) Iterator(hm) else Iterator.empty) ++
      fractions.map(hms + _)
    }
    def offsets(o: ZoneOffset): Seq[String] =
      if (o.getTotalSeconds == 0) Seq("Z", "+00:00", "-00:00", "+00:00:00", "-00:00:00")
      else if (o.getTotalSeconds % 60 == 0) Seq(o.getId, o.getId + ":00")
      else Seq(o.getId)
    def dateTimes(l: LocalDateTime, zones: Seq[String]): Iterator[String] = {
      val day = date(l.toLocalDate)
      times(l.toLocalTime).flatMap(t => zones.iterator.map(z => s"${day}T$t$z"))
    }
    value match {
      case d: LocalDate      => Iterator(date(d))
      case t: LocalTime      => times(t)
      case l: LocalDateTime  => dateTimes(l, Seq(""))
      case o: OffsetDateTime => dateTimes(o.toLocalDateTime, offsets(o.getOffset))
      case z: ZonedDateTime  => dateTimes(z.toLocalDateTime, offsets(z.getOffset))
      case other             => Iterator(other.toString)
    }
  }
}
