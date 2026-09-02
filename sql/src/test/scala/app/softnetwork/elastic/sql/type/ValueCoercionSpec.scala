package app.softnetwork.elastic.sql.`type`

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Instant, LocalDate, ZoneOffset}

/** softclient4es-arrow#168 — `coerceToDate` had no `Number` arm while `coerceToTimestamp` did, so
  * an Elasticsearch `date` field stored as epoch millis threw `Cannot convert java.lang.Long to
  * DATE` and took down every cross-index JOIN leg that projected it (and `ResultSet.getDate` with
  * it).
  */
class ValueCoercionSpec extends AnyFlatSpec with Matchers {

  // 2026-01-01T00:00:00Z plus 13h 45m — a value with a real time-of-day, so a naive
  // implementation that truncated (or failed to) is visible.
  private val EpochMillis: Long = 1767225600000L + (13L * 3600 + 45L * 60) * 1000L
  private val ExpectedDate: LocalDate =
    Instant.ofEpochMilli(EpochMillis).atZone(ZoneOffset.UTC).toLocalDate

  "coerceToDate" should "read a java.lang.Long as epoch milliseconds (arrow#168)" in {
    val d = ValueCoercion.coerceToDate(java.lang.Long.valueOf(EpochMillis))
    d should not be null
    d.getTime shouldBe EpochMillis
  }

  it should "read every other Number shape as epoch milliseconds" in {
    ValueCoercion.coerceToDate(java.lang.Integer.valueOf(0)).getTime shouldBe 0L
    ValueCoercion
      .coerceToDate(java.math.BigDecimal.valueOf(EpochMillis))
      .getTime shouldBe EpochMillis
    ValueCoercion.coerceToDate(EpochMillis.toDouble).getTime shouldBe EpochMillis
  }

  it should "keep every pre-existing conversion working" in {
    ValueCoercion.coerceToDate(null) shouldBe null
    val ld = LocalDate.of(2026, 1, 2)
    ValueCoercion.coerceToDate(ld) shouldBe java.sql.Date.valueOf(ld)
    ValueCoercion.coerceToDate("2026-01-02") shouldBe java.sql.Date.valueOf("2026-01-02")
    ValueCoercion.coerceToDate(Instant.ofEpochMilli(EpochMillis)).getTime shouldBe EpochMillis
  }

  it should "still reject a value with no date meaning" in {
    a[java.sql.SQLException] should be thrownBy ValueCoercion.coerceToDate(new Object)
  }

  // Negative control for the DELIBERATE asymmetry recorded in coerceToDate's scaladoc: a bare
  // number in a TIME position is ambiguous, so coerceToTime must NOT grow a Number arm by
  // symmetry. If someone "fixes" that for consistency, this test tells them it was a decision.
  "coerceToTime" should "deliberately reject a Number" in {
    a[java.sql.SQLException] should be thrownBy ValueCoercion.coerceToTime(
      java.lang.Long.valueOf(EpochMillis)
    )
  }

  "coerceToTimestamp" should "remain the reference for the Number arm" in {
    ValueCoercion
      .coerceToTimestamp(java.lang.Long.valueOf(EpochMillis))
      .getTime shouldBe EpochMillis
  }

  // Documents the ES-mapping consequence that made arrow#168 reachable at all: an ES `date`
  // mapping resolves to SQLTypes.Date (typeId "DATE"), which arrow-core turns into a Date64
  // vector — while the _source value may be a plain number.
  "SQLTypes" should "resolve the ES 'date' mapping token to SQLTypes.Date" in {
    SQLTypes("date") shouldBe SQLTypes.Date
    SQLTypes.Date.typeId shouldBe "DATE"
  }

  // Subject restated on purpose — this is about coerceToDate, not about SQLTypes.
  "coerceToDate" should "map an epoch-millis Long to the same UTC calendar date in every JVM zone" in {
    // Derived from Instant + ZoneOffset.UTC, so the expectation carries no dependency on the
    // machine's default zone — the assertion holds in every CI timezone.
    Instant
      .ofEpochMilli(ValueCoercion.coerceToDate(java.lang.Long.valueOf(EpochMillis)).getTime)
      .atZone(ZoneOffset.UTC)
      .toLocalDate shouldBe ExpectedDate
  }
}
