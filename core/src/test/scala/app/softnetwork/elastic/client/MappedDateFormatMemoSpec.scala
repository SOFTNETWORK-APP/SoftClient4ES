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

import app.softnetwork.elastic.client.MappedDateFormat.Dialect
import app.softnetwork.elastic.sql.query.TemporalLiterals
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{
  Clock,
  Instant,
  LocalDate,
  LocalDateTime,
  OffsetDateTime,
  ZoneId,
  ZoneOffset,
  ZonedDateTime
}
import java.util.concurrent.{Callable, CountDownLatch, Executors, TimeUnit}

/** SCHEMA-17 fix R-159 (A-193) -- a mapping date format is parsed ONCE per (format, dialect) at the
  * default clock: every caller shares the instance, which reads every value as a fresh one does. A
  * format built with an injected clock is never shared, in either direction (Joda's two-digit years
  * read the clock).
  */
class MappedDateFormatMemoSpec extends AnyFlatSpec with Matchers {

  /** Population P2's formats: the defaults, epochs, a custom pattern, Joda's two-digit year,
    * Elasticsearch 6.8's `8`-prefixed pattern and name, a zone name core reads as stored.
    */
  private val formats: Seq[String] = Seq(
    TemporalLiterals.DefaultDateFormat,
    TemporalLiterals.DefaultDateNanosFormat,
    TemporalLiterals.FieldCapsDateFormat,
    "epoch_millis",
    "epoch_second",
    "yyyy/MM/dd",
    "yy-MM-dd",
    "8yyyy-MM-dd HH:mm",
    "8strict_date_optional_time",
    "yyyy-MM-dd HH:mm z"
  )

  private val dialects: Seq[Dialect] = Seq(Dialect.Joda, Dialect.Java7, Dialect.Java8)

  /** The default clock's time, but not equal to it: what an unshared instance is built with. */
  private val sameTimeOtherClock: Clock = new Clock {
    override def getZone(): ZoneId = ZoneOffset.UTC
    override def withZone(zone: ZoneId): Clock = Clock.system(zone)
    override def instant(): Instant = Clock.systemUTC().instant()
  }

  private val year2100 = Clock.fixed(Instant.parse("2100-06-01T00:00:00Z"), ZoneOffset.UTC)

  /** Values of every class the response parser gives, of every shape the formats above read. */
  private val values: Seq[Any] = Seq(
    "2024-01-31",
    "2024-01-31T10:30:00Z",
    "2024-01-31T10:30:00.123Z",
    "2024-01-31T10:30:00.123456789Z",
    "2024-01-31T10:30:00+02:00",
    "2024/01/31",
    "24-01-31",
    "60-01-31",
    "2024-01-31 10:30",
    "2024-01-31 10:30 UTC",
    "2024-01-31 10:30 CET",
    "1706697000000",
    "1706697000",
    "not a date",
    java.lang.Long.valueOf(1706697000000L),
    java.lang.Integer.valueOf(2024),
    java.lang.Double.valueOf(1706697000.5),
    new java.math.BigDecimal("1706697000.123"),
    LocalDate.of(2024, 1, 31),
    LocalDateTime.of(2024, 1, 31, 10, 30),
    LocalDateTime.of(2024, 1, 31, 10, 30, 15, 120000000),
    OffsetDateTime.of(2024, 1, 31, 10, 30, 0, 0, ZoneOffset.UTC),
    OffsetDateTime.of(2024, 1, 31, 10, 30, 0, 0, ZoneOffset.ofHours(2)),
    ZonedDateTime.of(2024, 1, 31, 10, 30, 0, 0, ZoneId.of("Europe/Paris")),
    Instant.parse("2024-01-31T10:30:00Z")
  )

  "MappedDateFormat" should "share ONE instance per format and dialect at the default clock" in {
    for {
      spec    <- formats
      dialect <- dialects
    } withClue(s"[$spec | $dialect] ") {
      val shared = MappedDateFormat(spec, dialect)
      (MappedDateFormat(spec, dialect) eq shared) shouldBe true
      (MappedDateFormat(spec, dialect, Clock.systemUTC()) eq shared) shouldBe true
      (MappedDateFormat(spec, dialect, Clock.system(ZoneOffset.UTC)) eq shared) shouldBe true
    }
    (MappedDateFormat("yyyy/MM/dd", Dialect.Joda) eq MappedDateFormat(
      "yyyy/MM/dd",
      Dialect.Java8
    )) shouldBe false
    (MappedDateFormat("yyyy/MM/dd", Dialect.Java7) eq MappedDateFormat(
      "yyyy/MM/dd",
      Dialect.Java8
    )) shouldBe false
  }

  it should "never share an instance built with an injected clock, nor hand one to the default path" in {
    for {
      spec    <- formats
      dialect <- dialects
    } withClue(s"[$spec | $dialect] ") {
      val shared = MappedDateFormat(spec, dialect)
      val fixed = MappedDateFormat(spec, dialect, year2100)
      (MappedDateFormat(spec, dialect, year2100) eq fixed) shouldBe false
      (fixed eq shared) shouldBe false
      (MappedDateFormat(spec, dialect, sameTimeOtherClock) eq shared) shouldBe false
      (MappedDateFormat(spec, dialect) eq shared) shouldBe true
    }
  }

  it should "read Joda's two-digit years by the clock it was built with, whatever the order of use" in {
    def year(format: MappedDateFormat): Int =
      format.read("60-01-31") match {
        case MappedDateFormat.Read(instant) => instant.atZone(ZoneOffset.UTC).getYear
        case other                          => fail(s"'60-01-31' read as $other")
      }
    val now = MappedDateFormat.twoDigitYear("60", Clock.systemUTC())
    now should not be 2060
    year(MappedDateFormat("yy-MM-dd", Dialect.Joda)) shouldBe now
    year(MappedDateFormat("yy-MM-dd", Dialect.Joda, year2100)) shouldBe 2060
    year(MappedDateFormat("yy-MM-dd", Dialect.Joda)) shouldBe now
    year(MappedDateFormat("yy-MM-dd", Dialect.Joda, year2100)) shouldBe 2060
  }

  it should "read every value as a fresh instance of the same format reads it" in {
    for {
      spec    <- formats
      dialect <- dialects
    } withClue(s"[$spec | $dialect] ") {
      val shared = MappedDateFormat(spec, dialect)
      val fresh = MappedDateFormat(spec, dialect, sameTimeOtherClock)
      shared.unread shouldBe fresh.unread
      shared.alternatives.map(_.getClass.getName) shouldBe fresh.alternatives.map(
        _.getClass.getName
      )
      values.foreach { value =>
        withClue(s"[$value: ${value.getClass.getSimpleName}] ") {
          // twice on the shared instance: its per-shape memo answers the second read
          shared.readValue(value) shouldBe fresh.readValue(value)
          shared.readValue(value) shouldBe fresh.readValue(value)
          shared.readValue(value, nanos = true) shouldBe fresh.readValue(value, nanos = true)
          value match {
            case text: String =>
              shared.read(text) shouldBe fresh.read(text)
              shared.read(text, nanos = true) shouldBe fresh.read(text, nanos = true)
            case _ =>
          }
        }
      }
    }
  }

  it should "hand every thread the same instance on a format's first use" in {
    // a format no other test uses: its first use is this one
    val spec = "dd.MM.yyyy'T'HH:mm:ss.SSS||epoch_second"
    val threads = 8
    val pool = Executors.newFixedThreadPool(threads)
    val start = new CountDownLatch(1)
    try {
      val tasks = (1 to threads).map { _ =>
        pool.submit(new Callable[MappedDateFormat] {
          override def call(): MappedDateFormat = {
            start.await(10, TimeUnit.SECONDS)
            MappedDateFormat(spec, Dialect.Java8)
          }
        })
      }
      start.countDown()
      val built = tasks.map(_.get(30, TimeUnit.SECONDS))
      built.forall(_ eq built.head) shouldBe true
      (MappedDateFormat(spec, Dialect.Java8) eq built.head) shouldBe true
    } finally pool.shutdownNow()
  }
}
