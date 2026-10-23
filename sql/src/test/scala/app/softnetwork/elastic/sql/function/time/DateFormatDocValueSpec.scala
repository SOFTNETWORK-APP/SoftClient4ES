package app.softnetwork.elastic.sql.function.time

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType, PainlessTarget}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `DATETIME_FORMAT` and `DATE_FORMAT` over a raw `date` column on Elasticsearch 6.8, which hands a
  * query script the doc value as a `JodaCompatibleZonedDateTime`: no `TemporalAccessor`, so the
  * formatter failed every shard holding such a document and the search answered HTTP 500. The doc
  * value is first made the UTC `ZonedDateTime` it holds. 7 and up, an ingest processor and an
  * operand that is no raw doc value keep their bytes.
  */
class DateFormatDocValueSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(Column("ts", SQLTypes.Timestamp), Column("d", SQLTypes.Date))
  )

  private def scriptOf(
    sql: String,
    contextType: PainlessContextType = PainlessContextType.Query,
    target: PainlessTarget = PainlessTarget.Default
  ): String = Parser(sql) match {
    case Right(ss: SingleSearch) =>
      val ctx = PainlessContext(contextType, target)
      val body = ss.update(Some(schema)).select.fields.head.painless(Some(ctx))
      s"$ctx$body"
    case other => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private val es68 = PainlessTarget(6)

  "a format of a raw date column on Elasticsearch 6.8" should "format the UTC ZonedDateTime" in {
    scriptOf("SELECT DATETIME_FORMAT(ts, '%Y') FROM t", target = es68) shouldBe
    "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "def param2 = DateTimeFormatter.ofPattern(\"yyyy\"); " +
    "(param1 == null) ? null : param2.format(param1.toInstant().atZone(ZoneId.of('Z')))"
    scriptOf("SELECT DATE_FORMAT(d, '%Y-%m-%d') FROM t", target = es68) should endWith(
      "param2.format(param1.toInstant().atZone(ZoneId.of('Z')))"
    )
  }

  "the same format" should "keep its bytes on 7 and up, in a processor and over a function" in {
    scriptOf("SELECT DATETIME_FORMAT(ts, '%Y') FROM t") should endWith(
      "(param1 == null) ? null : param2.format(param1)"
    )
    scriptOf("SELECT DATETIME_FORMAT(ts, '%Y') FROM t", target = PainlessTarget(8)) should endWith(
      "(param1 == null) ? null : param2.format(param1)"
    )
    scriptOf(
      "SELECT DATETIME_FORMAT(ts, '%Y') FROM t",
      PainlessContextType.Processor,
      es68
    ) should not include "toInstant().atZone(ZoneId.of('Z')))"
    // a truncated column is a genuine ZonedDateTime already
    scriptOf("SELECT DATETIME_FORMAT(DATE_TRUNC(ts, MONTH), '%Y') FROM t", target = es68) should
    not include "format(param1.toInstant()"
  }
}
