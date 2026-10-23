package app.softnetwork.elastic.sql.function.time

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `CAST(<TEMPORAL> AS TIME)`: an operand that only adjusts its receiver (`DATE_TRUNC`) over a CASE
  * whose arms are a DATE and a date-time renders whichever class its row's arm has, so the
  * conversion is decided per row from the runtime class, as `LAST_DAY` decides its receiver's date:
  * a `LocalDate` is its UTC midnight, a `LocalDateTime` its time, a zoned doc value its UTC time of
  * day. DuckDB 1.5.5: `CAST(date_trunc('month', …) AS TIME)` is `00:00:00`.
  *
  * Before, no conversion arm matched a TEMPORAL source and the cast answered the timestamp itself
  * under the TIME label (`2024-03-01T00:00Z`). An ingest processor keeps its bytes.
  */
class TemporalCastToTimeSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("n", SQLTypes.Int),
      Column("d", SQLTypes.Timestamp),
      Column("ts", SQLTypes.Timestamp)
    )
  )

  private def scriptOf(
    sql: String,
    contextType: PainlessContextType = PainlessContextType.Query
  ): String = Parser(sql) match {
    case Right(ss: SingleSearch) =>
      val ctx = PainlessContext(contextType)
      val body = ss.update(Some(schema)).select.fields.head.painless(Some(ctx))
      s"$ctx$body"
    case other => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private val dispatch: String => String = x =>
    s"($x == null ? null : $x instanceof LocalDate ? " +
    s"$x.atStartOfDay(ZoneId.of('Z')).toLocalTime() : $x instanceof LocalDateTime ? " +
    s"$x.toLocalTime() : $x.toInstant().atZone(ZoneId.of('Z')).toLocalTime())"

  "a TEMPORAL operand cast to TIME" should "be converted per row, from its runtime class" in {
    val sql = "SELECT CAST(DATE_TRUNC(CASE WHEN n > 1 THEN CAST(d AS DATE) ELSE " +
      "CAST(d AS DATETIME) END, MONTH) AS TIME) FROM t"
    val script = scriptOf(sql)
    script should endWith(dispatch("param6"))
    // bound once: the dispatch reads one parameter
    script should include(
      "def param6 = ((param5 == null) ? null : (def)(param5.withDayOfMonth(1)" +
      ".truncatedTo(ChronoUnit.DAYS))); "
    )
    scriptOf(
      "SELECT CAST(DATE_TRUNC(CASE WHEN n > 1 THEN ts ELSE CAST(d AS DATETIME) END, MONTH) " +
      "AS TIME) FROM t"
    ) should endWith(dispatch("param6"))
  }

  "a typed operand cast to TIME" should "keep its own conversion" in {
    // control: DATE_TRUNC over a TIMESTAMP column is a TIMESTAMP, converted as before
    scriptOf("SELECT CAST(DATE_TRUNC(ts, MONTH) AS TIME) FROM t") shouldBe
    "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value.toInstant().atZone(ZoneId.of('Z'))" +
    ".withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS)); " +
    "(param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalTime() : null)"
  }

  "an ingest processor" should "keep its bytes" in {
    scriptOf(
      "SELECT CAST(DATE_TRUNC(CASE WHEN n > 1 THEN CAST(d AS DATE) ELSE CAST(d AS DATETIME) " +
      "END, MONTH) AS TIME) FROM t",
      PainlessContextType.Processor
    ) should not include "instanceof LocalDate"
  }

  private val toDate: String => String = x =>
    s"($x == null ? null : $x instanceof LocalDate ? $x : " +
    s"$x instanceof LocalDateTime ? $x.toLocalDate() : $x.toInstant().atZone(ZoneId.of('Z')).toLocalDate())"

  private val toEpochMillis: String => String = x =>
    s"($x == null ? null : (def) ($x instanceof LocalDate ? " +
    s"$x.atStartOfDay(ZoneId.of('Z')).toInstant().toEpochMilli() : " +
    s"$x instanceof LocalDateTime ? $x.atZone(ZoneId.of('Z')).toInstant().toEpochMilli() : " +
    s"$x.toInstant().toEpochMilli()))"

  /** The same dispatch to a DATE (the value's date part, as DuckDB's `CAST(<timestamp> AS DATE)`)
    * and to a BIGINT (the epoch milliseconds of the instant, a DATE's of its UTC midnight). Both
    * answered the truncated timestamp itself under their label.
    */
  "a TEMPORAL operand cast to DATE or BIGINT" should "be converted per row, from its runtime class" in {
    Seq(
      "CASE WHEN n > 1 THEN CAST(d AS DATE) ELSE CAST(d AS DATETIME) END",
      "CASE WHEN n > 1 THEN ts ELSE CAST(d AS DATETIME) END"
    ).foreach { operand =>
      withClue(operand) {
        scriptOf(s"SELECT CAST(DATE_TRUNC($operand, MONTH) AS DATE) FROM t") should endWith(
          toDate("param6")
        )
        scriptOf(s"SELECT CAST(DATE_TRUNC($operand, MONTH) AS BIGINT) FROM t") should endWith(
          toEpochMillis("param6")
        )
      }
    }
  }

  "a typed operand cast to DATE or BIGINT" should "keep its own conversion" in {
    scriptOf("SELECT CAST(DATE_TRUNC(ts, MONTH) AS BIGINT) FROM t") shouldBe
    "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value.toInstant().atZone(ZoneId.of('Z'))" +
    ".withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS)); " +
    "(param1 != null ? (def)(param1.toInstant().toEpochMilli()) : null)"
    scriptOf("SELECT CAST(ts AS DATE) FROM t") should not include "instanceof"
    scriptOf(
      "SELECT CAST(DATE_TRUNC(CASE WHEN n > 1 THEN ts ELSE CAST(d AS DATETIME) END, MONTH) " +
      "AS BIGINT) FROM t",
      PainlessContextType.Processor
    ) should not include "instanceof LocalDate"
  }
}
