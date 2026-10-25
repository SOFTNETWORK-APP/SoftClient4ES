package app.softnetwork.elastic.sql.function.time

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A DATE operand -- a `LocalDate`: a cast to DATE, `LAST_DAY`, a conditional that answers a DATE
  * -- under a time-of-day part and an interval, as DuckDB 1.5.5 answers them: `hour(DATE)` is 0
  * (the date is its UTC midnight), `DATE + INTERVAL 1 HOUR` is a TIMESTAMP, and a DATE plus whole
  * days is a DATE (the lead's date-arithmetic ruling).
  */
class DateOperandSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("d2", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("n", SQLTypes.Int)
    )
  )

  private def field(sql: String) = Parser(sql) match {
    case Right(ss: SingleSearch) => ss.update(Some(schema)).select.fields.head
    case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def scriptOf(
    sql: String,
    contextType: PainlessContextType = PainlessContextType.Query
  ): String = {
    val ctx = PainlessContext(contextType)
    val body = field(sql).painless(Some(ctx))
    s"$ctx$body"
  }

  private def reported(sql: String): SQLType = field(sql).identifier.reportedType

  "a time of day of a DATE" should "be read at its UTC midnight" in {
    Seq(
      "HOUR(COALESCE(d, d2))"                                 -> ".get(ChronoField.HOUR_OF_DAY)",
      "MINUTE(GREATEST(d, CAST(ts AS DATE)))"                 -> ".get(ChronoField.MINUTE_OF_HOUR)",
      "OFFSET_SECONDS(LAST_DAY(ts))"                          -> ".get(ChronoField.OFFSET_SECONDS)",
      "HOUR(CAST(d AS DATE))"                                 -> ".get(ChronoField.HOUR_OF_DAY)",
      "EXTRACT(HOUR FROM CASE WHEN n > 0 THEN d ELSE d2 END)" -> ".get(ChronoField.HOUR_OF_DAY)"
    ).foreach { case (expr, part) =>
      val script = scriptOf(s"SELECT $expr FROM t")
      withClue(s"[$expr] $script ") {
        script should include(s".atStartOfDay(ZoneId.of('Z'))$part")
        reported(s"SELECT $expr FROM t") shouldBe SQLTypes.BigInt
      }
    }
  }

  it should "leave a date part, and a TIMESTAMP, as they render" in {
    // a LocalDate has its YEAR; a DATE column's doc value is an instant
    scriptOf("SELECT YEAR(COALESCE(d, d2)) FROM t") should not include "atStartOfDay"
    scriptOf("SELECT HOUR(d) FROM t") should not include "atStartOfDay"
    scriptOf("SELECT HOUR(ts) FROM t") should not include "atStartOfDay"
  }

  "a sub-day interval over a DATE" should "answer a TIMESTAMP from its UTC midnight" in {
    Seq(
      "DATETIME_ADD(COALESCE(d, d2), INTERVAL 1 HOUR)",
      "DATETIME_ADD(GREATEST(CAST(d AS DATE), CAST(ts AS DATE)), INTERVAL 1 HOUR)",
      "DATETIME_ADD(CAST(ts AS DATE), INTERVAL 1 HOUR)",
      "DATE_ADD(CAST(d AS DATE), INTERVAL 30 MINUTE)"
    ).foreach { expr =>
      val script = scriptOf(s"SELECT $expr FROM t")
      withClue(s"[$expr] $script ") {
        script should include(".atStartOfDay(ZoneId.of('Z')).plus(")
        reported(s"SELECT $expr FROM t") shouldBe SQLTypes.Timestamp
      }
    }
  }

  "an interval over an operand that reports a DATE" should "report a DATE for whole days" in {
    Seq(
      "DATE_ADD(d, INTERVAL 1 DAY)"                -> SQLTypes.Date,
      "DATE_SUB(d, INTERVAL 2 MONTH)"              -> SQLTypes.Date,
      "DATE_ADD(COALESCE(d, d2), INTERVAL 1 WEEK)" -> SQLTypes.Date,
      "DATE_ADD(d, INTERVAL 1 HOUR)"               -> SQLTypes.Timestamp,
      "DATE_ADD(ts, INTERVAL 1 DAY)"               -> SQLTypes.Timestamp
    ).foreach { case (expr, t) =>
      withClue(s"[$expr] ")(reported(s"SELECT $expr FROM t") shouldBe t)
    }
  }

  it should "make a COALESCE beside it answer a DATE in every arm" in {
    // the doc value plus a day is converted once to the DATE the COALESCE answers
    val script = scriptOf("SELECT COALESCE(CAST(d AS DATE), DATE_ADD(d, INTERVAL 1 DAY)) FROM t")
    reported("SELECT COALESCE(CAST(d AS DATE), DATE_ADD(d, INTERVAL 1 DAY)) FROM t") shouldBe
    SQLTypes.Date
    script should not include "atStartOfDay"
    script should endWith(
      "(param3 != null ? param3 : " +
      "(param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null))"
    )
  }

  "an ingest processor" should "render these as it did" in {
    scriptOf(
      "SELECT HOUR(CAST(d AS DATE)) FROM t",
      PainlessContextType.Processor
    ) should not include
    "atStartOfDay(ZoneId.of('Z')).get"
    scriptOf(
      "SELECT DATETIME_ADD(COALESCE(d, d2), INTERVAL 1 HOUR) FROM t",
      PainlessContextType.Processor
    ) should not include "atStartOfDay(ZoneId.of('Z')).plus"
  }
}
