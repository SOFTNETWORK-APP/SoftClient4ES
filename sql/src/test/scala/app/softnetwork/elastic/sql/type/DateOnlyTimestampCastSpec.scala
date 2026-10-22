package app.softnetwork.elastic.sql.`type`

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A date-only string cast to TIMESTAMP or DATETIME is the midnight it starts, as DuckDB and
  * PostgreSQL cast `'2024-01-03'`: `ISO_DATE_TIME` requires a time, so `CAST(kd AS TIMESTAMP)` over
  * a keyword holding `2024-01-03` threw on every document and failed the statement. Per document it
  * is ONE length test on the operand, bound once; a literal is decided when the script renders. An
  * ingest processor keeps its bytes.
  */
class DateOnlyTimestampCastSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table("t", columns = List(Column("kd", SQLTypes.Keyword)))

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

  "a keyword cast to TIMESTAMP" should "read a date-only value as its midnight" in {
    scriptOf("SELECT CAST(kd AS TIMESTAMP) FROM t") shouldBe
    "def param1 = (doc['kd'].size() == 0 ? null : doc['kd'].value); " +
    "def param2 = (param1 != null ? ZonedDateTime.parse(((param1.length() == 10 ? param1 + 'T00:00:00' : param1)).replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z'))) : null); " +
    "param2"
  }

  "a keyword cast to DATETIME" should "read a date-only value as its midnight" in {
    scriptOf("SELECT CAST(kd AS DATETIME) FROM t") shouldBe
    "def param1 = (doc['kd'].size() == 0 ? null : doc['kd'].value); " +
    "def param2 = (param1 != null ? LocalDateTime.parse(((param1.length() == 10 ? param1 + 'T00:00:00' : param1)).replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME) : null); " +
    "param2"
  }

  "a date-only literal" should "be completed when the script renders" in {
    scriptOf("SELECT CAST('2024-01-03' AS TIMESTAMP) FROM t") shouldBe
    "def param1 = ZonedDateTime.parse((\"2024-01-03T00:00:00\").replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z'))); " +
    "param1"
    // a literal with a time renders as it did
    scriptOf("SELECT CAST('2024-01-03 10:00:00' AS TIMESTAMP) FROM t") shouldBe
    "def param1 = ZonedDateTime.parse((\"2024-01-03 10:00:00\").replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z'))); " +
    "param1"
  }

  /** The completion parses: `'2024-01-03'` + `T00:00:00` is what `ISO_DATE_TIME` reads, and a
    * date-time is untouched.
    */
  "the completed text" should "parse with the formatter the script uses" in {
    val f = java.time.format.DateTimeFormatter.ISO_DATE_TIME.withZone(java.time.ZoneId.of("Z"))
    def complete(s: String): String = if (s.length == 10) s + "T00:00:00" else s
    java.time.ZonedDateTime.parse(complete("2024-01-03").replace(" ", "T"), f).toString shouldBe
    "2024-01-03T00:00Z"
    java.time.ZonedDateTime
      .parse(complete("2024-01-03 10:15:00").replace(" ", "T"), f)
      .toString shouldBe "2024-01-03T10:15Z"
  }

  "an ingest processor" should "render the cast as it did" in {
    scriptOf("SELECT CAST(kd AS TIMESTAMP) FROM t", PainlessContextType.Processor) shouldBe
    "def param1 = ctx.kd; " +
    "def param2 = (param1 != null ? ZonedDateTime.parse((param1).replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z'))) : null); " +
    "param2"
  }
}
