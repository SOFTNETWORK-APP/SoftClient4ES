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

import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.query.TemporalLiterals
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Clock, Instant, ZoneOffset, ZonedDateTime}
import java.util.zip.GZIPInputStream
import scala.collection.immutable.ListMap
import scala.io.Source
import scala.util.{Failure, Success, Try}

/** A date field's values read with its mapping format, as each Elasticsearch major reads it --
  * driven by the corpora MEASURED on Elasticsearch 6.8.23, 7.17.29 and 8.18.3
  * (`date-formats/es<major>.tsv.gz`): every value the major ACCEPTED into a field of each format,
  * as written in the document, and the instant it indexed -- each pattern letter at each width,
  * alone and combined, every built-in name, epochs and JSON numbers. Each value goes the way core
  * receives it: the JSON read by the response parser (an ISO text already a date there), then the
  * converter of a converting `TIMESTAMP` position. A value core cannot read without guessing (the
  * row's 4th column says why) must FAIL its statement, naming the field and the format.
  */
class MappedDateFormatSpec extends AnyFlatSpec with Matchers {

  /** The day the corpora were measured: Joda's two-digit years depend on it. */
  private val measuredOn = Clock.fixed(Instant.parse("2026-10-07T12:00:00Z"), ZoneOffset.UTC)

  /** A row: the format (empty: none), the value as JSON, the instant indexed, and why core does not
    * read it, if it does not.
    */
  private def corpus(major: String): Seq[(String, String, Long, Option[String])] = {
    val source = Source.fromInputStream(
      new GZIPInputStream(getClass.getResourceAsStream(s"/date-formats/$major.tsv.gz")),
      "UTF-8"
    )
    try source
      .getLines()
      .filterNot(_.startsWith("#"))
      .map { line =>
        line.split("\t", -1) match {
          case Array(format, json, millis)        => (format, json, millis.toLong, None)
          case Array(format, json, millis, cause) => (format, json, millis.toLong, Some(cause))
          case other => throw new IllegalStateException(s"a corpus row: ${other.mkString("|")}")
        }
      }
      .toList
    finally source.close()
  }

  /** A document's value as core receives it: its JSON, read by the response parser. */
  private def received(json: String): Any =
    ElasticConversion.jsonNodeToAny(JacksonConfig.objectMapper.readTree(json), ListMap.empty)

  private def readsEveryAcceptedValue(
    major: String,
    version: String,
    size: Int,
    unread: Int
  ): Unit = {
    val rows = corpus(major)
    rows.size shouldBe size
    rows.count(_._4.isDefined) shouldBe unread
    val dates = new SetOperationValues.DateReading(Some(version), measuredOn)
    val converters = scala.collection.mutable.Map.empty[String, Any => Any]
    val wrong = rows.flatMap { case (format, json, millis, cause) =>
      val spec = if (format.isEmpty) TemporalLiterals.DefaultDateFormat else format
      val convert = converters.getOrElseUpdate(
        spec,
        SetOperationValues.converter(
          SQLTypes.Timestamp,
          SQLTypes.Timestamp,
          "multi",
          Some(spec),
          "v",
          "k",
          dates
        )
      )
      (cause, Try(convert(received(json)))) match {
        case (None, Success(z: ZonedDateTime)) if z.toInstant.toEpochMilli == millis => None
        case (Some(_), Failure(e: IllegalArgumentException))
            if e.getMessage.contains("field v") && e.getMessage.contains(s"'$spec'") =>
          None
        case (_, other) =>
          Some(
            s"[$format] $json: indexed ${Instant.ofEpochMilli(millis)}" +
            cause.fold("")(c => s" (expected to fail: $c)") + s", read $other"
          )
      }
    }
    withClue(wrong.take(50).mkString("\n", "\n", "\n"))(wrong shouldBe empty)
  }

  "Elasticsearch 8.18.3's date formats" should
  "read every value it accepted as the instant it indexed" in
  readsEveryAcceptedValue("es8", "8.18.3", 36109, 3512)

  "Elasticsearch 7.17.29's date formats" should
  "read every value it accepted as the instant it indexed" in
  readsEveryAcceptedValue("es7", "7.17.29", 47065, 5351)

  "Elasticsearch 6.8.23's date formats" should
  "read every value it accepted as the instant it indexed" in
  readsEveryAcceptedValue("es6", "6.8.23", 51515, 5755)

  private def read(spec: String, version: String, value: Any): Try[Any] =
    Try(
      SetOperationValues.converter(
        SQLTypes.Timestamp,
        SQLTypes.Timestamp,
        "multi",
        Some(spec),
        "v",
        "k",
        new SetOperationValues.DateReading(Some(version), measuredOn)
      )(value)
    )

  private def instant(t: Try[Any]): Instant =
    t match {
      case Success(z: ZonedDateTime) => z.toInstant
      case other                     => fail(s"not a date: $other")
    }

  "A week-based year (`YYYY`)" should "be read as Elasticsearch 8 indexed it, not as 1970" in {
    // MEASURED on 8.18.3: the week-based year's first week, starting on Sunday -- the month and the
    // day ignored -- where round 11 read 1970-01-31, 1970-12-31, 1970-01-01, 1970-12-30
    Seq(
      "2024-01-31" -> "2023-12-31T00:00:00Z",
      "2020-12-31" -> "2019-12-29T00:00:00Z",
      "2021-01-01" -> "2020-12-27T00:00:00Z",
      "2019-12-30" -> "2018-12-30T00:00:00Z"
    ).foreach { case (value, indexed) =>
      instant(read("YYYY-MM-dd", "8.18.3", value)) shouldBe Instant.parse(indexed)
    }
    instant(read("YYYY-MM-dd'T'HH:mm:ss", "8.18.3", "2024-06-15T10:30:00")) shouldBe
    Instant.parse("2023-12-31T10:30:00Z")
    instant(read("YYYY-MM", "8.18.3", "2024-01")) shouldBe Instant.parse("2023-12-31T00:00:00Z")
    // Elasticsearch 7 starts the week on Monday (ISO); Elasticsearch 6's `YYYY` is the year of era
    instant(read("YYYY-MM-dd", "7.17.29", "2024-01-31")) shouldBe
    Instant.parse("2024-01-01T00:00:00Z")
    instant(read("YYYY-MM-dd", "6.8.23", "2024-01-31")) shouldBe
    Instant.parse("2024-01-31T00:00:00Z")
  }

  "A pattern letter, or a combination of letters, no measurement covers" should
  "fail the statement, naming the field and the format" in {
    def failure(spec: String, version: String, value: Any): String =
      read(spec, version, value) match {
        case Failure(e: IllegalArgumentException) => e.getMessage
        case other                                => fail(s"not a failure: $other")
      }
    // a letter (`B`, day periods) no measurement covers
    failure("yyyy-MM-dd B", "8.18.3", "2024-01-31 in the morning") should endWith(
      "core cannot read it with the date format 'yyyy-MM-dd B' of field v as Elasticsearch 8 " +
      "does: 'yyyy-MM-dd B' is not a date format core reads as Elasticsearch 8 reads it: the " +
      "letter 'B' is not one core has measured"
    )
    // a width no measurement covers
    failure("yyyyyyyy-MM-dd", "7.17.29", "2024-01-31") should endWith(
      "the letter 'yyyyyyyy' is not one core has measured"
    )
    failure("yyyy-MM-dd zzzzz", "6.8.23", "2024-01-31 UTC") should endWith(
      "the letter 'zzzzz' is not one core has measured"
    )
    // a field no measured completion places: an hour of the day with a clock hour of AM/PM
    failure("yyyy-MM-dd HH hh", "8.18.3", "2024-01-31 10 10") should endWith(
      "the letters of 'yyyy-MM-dd HH hh' combine as 'yMd|hH', which core has not measured"
    )
    failure("yyyy-MM-dd HH hh", "6.8.23", "2024-01-31 10 10") should endWith(
      "its letters combine as 'yMd|hH|', which core has not measured"
    )
    // a zone name, whose offset comes from the zone data of the JVM reading it
    failure("yyyy-MM-dd'T'HH:mm:ss z", "8.18.3", "2024-01-31T10:30:00 CET") should endWith(
      "the zone 'CET' is a region id or a zone name, whose offset comes from the zone data of the " +
      "JVM reading it, not Elasticsearch's: core reads only Z, an offset, UTC, GMT and UT"
    )
  }

  private def failure(spec: String, version: String, value: Any): String =
    read(spec, version, value) match {
      case Failure(e: IllegalArgumentException)
          if e.getMessage.contains("field v") && e.getMessage.contains(s"'$spec'") =>
        e.getMessage
      case other => fail(s"[$spec] $value on $version: not a failure naming the field: $other")
    }

  private def received(json: String, spec: String, version: String): Try[Any] =
    read(spec, version, received(json))

  "A zone id or a zone name other than UTC, GMT and UT" should
  "fail the statement on every major, naming the field and the format" in {
    // a zone's offset comes from zone data, the reading JVM's: MEASURED, these read a different
    // instant on the JVMs core runs on than on the Elasticsearch that indexed them (the Elasticsearch
    // 7 one, which no measurement covers, fails as well)
    val regions = Seq(
      // a `java.time` id (`VV`): 2023-07-01T17:00:00Z on 8.18.3, 18:00Z with an older zone data
      ("yyyy-MM-dd'T'HH:mm:ss VV", "8.18.3", "\"2023-07-01T12:00:00 America/Mexico_City\""),
      ("yyyy-MM-dd'T'HH:mm:ss VV", "7.17.29", "\"2023-07-01T12:00:00 Asia/Tehran\""),
      // the default format: 2025-07-01T16:00:00Z on 8.18.3, 15:00Z with Java 17's zone data
      ("", "8.18.3", "\"2025-07-01T12:00:00America/Asuncion\""),
      // a region in brackets, which the response parser reads with the JVM's zone data: 8.18.3
      // indexed the local time in the region (08:30Z, 10:30Z), the parser gives the offset's
      ("yyyy-MM-dd'T'HH:mm:ssXXX'['VV']'", "8.18.3", "\"2024-07-31T10:30:00+05:00[Europe/Paris]\""),
      ("yyyy-MM-dd'T'HH:mm:ssXXX'['VV']'", "8.18.3", "\"2024-07-31T10:30:00+05:00[UTC]\""),
      // Joda: a zone id (`ZZZ`), an abbreviation (`z`) -- 17:00Z and 14:00Z on 6.8.23
      ("yyyy-MM-dd HH:mm:ss ZZZ", "6.8.23", "\"2023-07-01 12:00:00 America/Mexico_City\""),
      ("yyyy-MM-dd HH:mm:ss ZZZ", "6.8.23", "\"2024-01-15 12:00:00 America/Nuuk\""),
      ("yyyy-MM-dd HH:mm:ss ZZZ", "6.8.23", "\"2024-01-31 10:30:00 Etc/GMT-3\""),
      ("yyyy-MM-dd HH:mm:ss z", "6.8.23", "\"2024-07-31 10:30:00 PDT\"")
    )
    regions.foreach { case (format, version, json) =>
      val spec = if (format.isEmpty) TemporalLiterals.DefaultDateFormat else format
      received(json, spec, version) match {
        case Failure(e: IllegalArgumentException)
            if e.getMessage.contains("field v") && e.getMessage.contains(s"'$spec'") &&
              e.getMessage.contains("is a region id or a zone name") =>
          ()
        case other => fail(s"[$spec] $json on $version: $other")
      }
    }
    // `UTC`, `GMT`, `UT` and an offset are read, the same on every JVM (MEASURED)
    instant(received("\"2024-01-31 10:30:00 UTC\"", "yyyy-MM-dd HH:mm:ss VV", "8.18.3")) shouldBe
    Instant.parse("2024-01-31T10:30:00Z")
    instant(received("\"2024-01-31 10:30:00 UTC\"", "yyyy-MM-dd HH:mm:ss ZZZ", "6.8.23")) shouldBe
    Instant.parse("2024-01-31T10:30:00Z")
    instant(received("\"2024-01-31 10:30:00 UTC\"", "yyyy-MM-dd HH:mm:ss z", "6.8.23")) shouldBe
    Instant.parse("2024-01-31T10:30:00Z")
    instant(
      received("\"2025-07-01T12:00:00-04:00\"", TemporalLiterals.DefaultDateFormat, "8.18.3")
    ) shouldBe Instant.parse("2025-07-01T16:00:00Z")
  }

  "A pattern whose reading depends on the Java version core runs on" should
  "fail the statement on every Java version, naming the field and the format" in {
    // MEASURED on Java 8, 11, 17 and 21: a localized offset is not parsed by Java 8, a fraction
    // after a variable width keeps its digits from Java 9 on
    failure("yyyy-MM-dd'T'HH:mm:ss O", "8.18.3", "2024-01-31T10:30:00 GMT+1") should endWith(
      "the letter 'O' is a localized offset (`GMT+1`), which Java 8 parses differently from later " +
      "Java versions"
    )
    failure("yyyy-MM-dd ZZZZ", "7.17.29", "2018-12-30 GMT+01:00") should include(
      "the letter 'ZZZZ' is a localized offset"
    )
    failure("yyyyMMddHHmmssSSS", "8.18.3", "20181230103000000") should endWith(
      "the fraction 'SSS' follows digits of a variable width with no separator, which Java 8 " +
      "parses differently from later Java versions"
    )
    failure("8yyyyMMddHHmmssSSS", "6.8.23", "20181230103000000") should include(
      "the fraction 'SSS' follows digits of a variable width"
    )
    // Elasticsearch 6.8's java.time mode resolves SMARTLY with its own JVM (Java 15): an AM/PM
    // with no hour is ignored there -- 06:00 / 18:00 from Java 16 on (MEASURED: `8yyyy a` "2025 PM"
    // indexed 2025-01-01T00:00Z) -- and a week past its week-based year's last is read into the
    // next year, which Java 21 rejects (`8YYYY-ww-e` "2020-53-7" indexed 2020-12-26T00:00Z)
    failure("8yyyy a", "6.8.23", "2025 PM") should endWith(
      "'yyyy a' reads an AM/PM with no hour, which Elasticsearch 6's java.time mode resolves " +
      "differently from one Java version to another"
    )
    failure("8dd.MM.yyyy a", "6.8.23", "04.07.2024 PM") should include("an AM/PM with no hour")
    failure("8YYYY-ww-e", "6.8.23", "2020-53-7") should endWith(
      "'YYYY-ww-e' reads a week past the last of its week-based year, which Elasticsearch 6's " +
      "java.time mode resolves differently from one Java version to another"
    )
    // with an hour, and within the year, they read as Elasticsearch 6.8 indexed them (MEASURED)
    instant(read("8yyyy-MM-dd hh a", "6.8.23", "2024-07-04 10 PM")) shouldBe
    Instant.parse("2024-07-04T22:00:00Z")
    instant(read("8YYYY-ww-e", "6.8.23", "2024-01-1")) shouldBe
    Instant.parse("2023-12-31T00:00:00Z")
  }

  "A Joda pattern with two zone letters" should "fail the statement, naming the field and the format" in {
    // MEASURED on 6.8.23: the offset wins (05:30Z) -- a combination outside the measured product
    Seq(
      "yyyy-MM-dd HH:mm:ss ZZ ZZZ" -> "2024-07-31 10:30:00 +05:00 Europe/Paris",
      "yyyy-MM-dd HH:mm:ss Z ZZZ"  -> "2024-07-31 10:30:00 +0500 Europe/Paris",
      "yyyy-MM-dd HH:mm:ss ZZ z"   -> "2024-07-31 10:30:00 +05:00 PDT"
    ).foreach { case (spec, value) =>
      failure(spec, "6.8.23", value) should endWith(
        "its letters combine two zones, which core has not measured"
      )
    }
  }
}
