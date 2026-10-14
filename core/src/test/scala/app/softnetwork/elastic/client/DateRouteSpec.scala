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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Clock, Instant, ZoneOffset}
import java.util.zip.GZIPInputStream
import scala.io.Source

/** The route of a date position, decided from its format before any value ([[DateRoute.of]]),
  * driven by the corpora MEASURED on Elasticsearch 6.8.23, 7.17.29 and 8.18.3
  * (`date-formats/es<major>.tsv.gz`): a format is answered as stored EXACTLY when one of its values
  * cannot be read by core for a reason the format alone gives (the row's 4th column: `jdk-offset`,
  * `jdk-fraction`, `java6-name`, `jdk-ampm`, `zone-name`, `zone-id`); every other format -- the
  * default included -- is typed, its values read by the format (a value-level reason,
  * `json-double`, `erased-*`, `jdk-week`, fails at the value).
  */
class DateRouteSpec extends AnyFlatSpec with Matchers {

  /** The day the corpora were measured: Joda's two-digit years depend on it. */
  private val measuredOn = Clock.fixed(Instant.parse("2026-10-07T12:00:00Z"), ZoneOffset.UTC)

  /** The reasons the format alone gives. */
  private val formatCauses =
    Set("jdk-offset", "jdk-fraction", "java6-name", "jdk-ampm", "zone-name", "zone-id")

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

  private def dates(version: Option[String]): SetOperationValues.DateReading =
    new SetOperationValues.DateReading(version, measuredOn)

  private def route(spec: String, version: String): DateRoute =
    DateRoute.of(SQLTypes.Timestamp, Some(spec), dates(Some(version)))

  private def routesEveryFormat(major: String, version: String, formats: Int): Unit = {
    val rows = corpus(major)
    val byFormat = rows.groupBy(_._1)
    byFormat.size shouldBe formats
    val wrong = byFormat.toSeq.sortBy(_._1).flatMap { case (format, formatRows) =>
      val spec = if (format.isEmpty) TemporalLiterals.DefaultDateFormat else format
      val causes = formatRows.flatMap(_._4).distinct.sorted
      val expected = causes.exists(formatCauses)
      route(spec, version) match {
        case DateRoute.AsStored(reason) if expected =>
          if (reason.contains(s"'$spec'")) None
          else Some(s"[$format] AsStored, its reason not naming the format: $reason")
        case DateRoute.Typed if !expected => None
        case other =>
          Some(s"[$format] ${causes.mkString("causes {", ", ", "}")} routed $other")
      }
    }
    val asStored = byFormat.keys.count { format =>
      route(if (format.isEmpty) TemporalLiterals.DefaultDateFormat else format, version)
        .isInstanceOf[DateRoute.AsStored]
    }
    info(s"$major on $version: ${byFormat.size} formats, $asStored as stored")
    withClue(wrong.take(50).mkString("\n", "\n", "\n"))(wrong shouldBe empty)
  }

  "Elasticsearch 8.18.3's date formats" should
  "be answered as stored exactly when the format says a value is not read" in
  routesEveryFormat("es8", "8.18.3", 4300)

  "Elasticsearch 7.17.29's date formats" should
  "be answered as stored exactly when the format says a value is not read" in
  routesEveryFormat("es7", "7.17.29", 4721)

  "Elasticsearch 6.8.23's date formats" should
  "be answered as stored exactly when the format says a value is not read" in
  routesEveryFormat("es6", "6.8.23", 6323)

  "A format" should "be answered as stored as a whole when ONE alternative is" in {
    // a zone name after a name core reads
    route("strict_date_optional_time||yyyy-MM-dd HH:mm z", "8.18.3") match {
      case DateRoute.AsStored(reason) =>
        reason shouldBe
          "core cannot read every value of the date format " +
          "'strict_date_optional_time||yyyy-MM-dd HH:mm z' as Elasticsearch 8 does: its zone letter " +
          "reads zone names, whose offset comes from the zone data of the JVM reading it, not " +
          "Elasticsearch's: core reads only Z, an offset, UTC, GMT and UT"
      case other => fail(s"$other")
    }
    // a letter no measurement covers after an epoch
    route("epoch_millis||yyyy-MM-dd B", "8.18.3") match {
      case DateRoute.AsStored(reason) =>
        reason should include("'epoch_millis||yyyy-MM-dd B'")
        reason should include("'yyyy-MM-dd B' is not a date format core reads")
      case other => fail(s"$other")
    }
    // a region id
    route("yyyy-MM-dd'T'HH:mm VV", "8.18.3") match {
      case DateRoute.AsStored(reason) => reason should include("reads region ids")
      case other                      => fail(s"$other")
    }
    // Joda-Time's zone name and region id
    route("yyyy-MM-dd HH:mm z", "6.8.23") match {
      case DateRoute.AsStored(reason) => reason should include("reads zone names")
      case other                      => fail(s"$other")
    }
    route("yyyy-MM-dd HH:mm ZZZ", "6.8.23") match {
      case DateRoute.AsStored(reason) => reason should include("reads region ids")
      case other                      => fail(s"$other")
    }
    // 6.8's `8` mode: an AM/PM with no hour, known from the letters (quoted text aside)
    route("8YYYY-ww a", "6.8.23") match {
      case DateRoute.AsStored(reason) =>
        reason should include("reads an AM/PM with no hour")
      case other => fail(s"$other")
    }
    route("8YYYY-ww hh a", "6.8.23") shouldBe DateRoute.Typed
    route("8YYYY-ww 'h' a", "6.8.23") shouldBe a[DateRoute.AsStored]
    // ...a reading of 6.8 only
    route("YYYY-ww a", "7.17.29") shouldBe DateRoute.Typed
    // an offset, `UTC` and the built-in names are read for sure
    Seq("yyyy-MM-dd HH:mm XXX", "yyyy-MM-dd HH:mm Z", "epoch_second", "strict_date_time")
      .foreach(spec => route(spec, "8.18.3") shouldBe DateRoute.Typed)
    Seq("8.18.3", "7.17.29", "6.8.23").foreach { v =>
      route(TemporalLiterals.DefaultDateFormat, v) shouldBe DateRoute.Typed
    }
  }

  it should "be answered as stored, every one, when the major is not known" in {
    DateRoute.of(SQLTypes.Timestamp, Some(TemporalLiterals.DefaultDateFormat), dates(None)) shouldBe
    DateRoute.AsStored(
      "the date format 'strict_date_optional_time||epoch_millis' is read as the Elasticsearch " +
      "version does, which is not known"
    )
    Seq("0.0.0", "5.6.16").foreach { v =>
      DateRoute.of(SQLTypes.Date, Some("epoch_second"), dates(Some(v))) shouldBe
      DateRoute.AsStored(
        s"the date format 'epoch_second' is read as the Elasticsearch version does, and core " +
        s"reads dates of no $v"
      )
    }
  }

  "A position that is not a date read by its format" should "be typed, the version not asked" in {
    var asked = 0
    def version: Option[String] = { asked += 1; Some("8.18.3") }
    val reading = SetOperationValues.DateReading(version)
    // a computed date, a number, a text: no format
    DateRoute.of(SQLTypes.Timestamp, None, reading) shouldBe DateRoute.Typed
    DateRoute.of(SQLTypes.Int, None, reading) shouldBe DateRoute.Typed
    DateRoute.of(SQLTypes.Varchar, Some("yyyy-MM-dd HH:mm z"), reading) shouldBe DateRoute.Typed
    asked shouldBe 0
    // a date read by its format: asked once for the statement
    DateRoute.of(SQLTypes.Time, Some("HH:mm z"), reading) shouldBe a[DateRoute.AsStored]
    DateRoute.of(SQLTypes.Date, Some("epoch_second"), reading) shouldBe DateRoute.Typed
    asked shouldBe 1
  }
}
