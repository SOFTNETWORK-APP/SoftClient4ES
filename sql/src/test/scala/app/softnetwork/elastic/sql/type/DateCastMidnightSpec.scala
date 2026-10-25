package app.softnetwork.elastic.sql.`type`

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A DATE cast to BIGINT is the epoch milliseconds of its UTC midnight, and to TIME that midnight
  * (`00:00:00`), whatever renders the DATE: a cast to DATE, `CURRENT_DATE`, a DATE literal,
  * `LAST_DAY`, or a CASE, COALESCE, NULLIF, GREATEST or LEAST that answers a DATE. Before, the
  * `LocalDate` answered itself under the BIGINT / TIME label, or a conversion from its column's
  * type called `toInstant()` on it and failed the shard.
  *
  * A DATE column renders its doc value, an instant, and keeps the conversions of that instant. An
  * ingest processor and a transform keep their bytes.
  */
class DateCastMidnightSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("n", SQLTypes.Int)
    )
  )

  /** The script of the statement's only field, and the type that field reports. */
  private def rendered(
    sql: String,
    contextType: PainlessContextType = PainlessContextType.Query
  ): (String, SQLType) = Parser(sql) match {
    case Right(ss: SingleSearch) =>
      val field = ss.update(Some(schema)).select.fields.head
      val ctx = PainlessContext(contextType)
      val body = field.painless(Some(ctx))
      (s"$ctx$body", field.identifier.reportedType)
    case other => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  /** The field's script is `script` and it reports `reported`. */
  private def pin(sql: String, script: String, reported: SQLType): Unit = {
    val (body, reportedType) = rendered(sql)
    body shouldBe script
    reportedType shouldBe reported
  }

  private val d = "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); "

  private val ts = "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); "

  private val dateOfD =
    "def param2 = (param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); "

  private val literal =
    "LocalDate.parse((\"2024-01-01\").replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\"))"

  /** The DATE bound as `date`, converted to BIGINT: its UTC midnight's epoch milliseconds. */
  private def epochOf(date: String): String =
    s"($date != null ? (def)($date.atStartOfDay(ZoneId.of('Z')).toInstant().toEpochMilli()) : null)"

  /** The DATE bound as `date`, converted to TIME: its UTC midnight. */
  private def midnightOf(date: String): String =
    s"($date != null ? $date.atStartOfDay(ZoneId.of('Z')).toLocalTime() : null)"

  "a DATE column" should "keep converting the instant it holds" in {
    pin(
      "SELECT CAST(d AS BIGINT) FROM t",
      d + "(param1 != null ? (def)(param1.toInstant().toEpochMilli()) : null)",
      SQLTypes.BigInt
    )
    pin(
      "SELECT CAST(d AS TIME) FROM t",
      d + "(param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalTime() : null)",
      SQLTypes.Time
    )
  }

  "a cast to DATE" should "be converted through its UTC midnight" in {
    pin(
      "SELECT CAST(CAST(ts AS DATE) AS BIGINT) FROM t",
      ts + dateOfD + epochOf("param2"),
      SQLTypes.BigInt
    )
    pin(
      "SELECT CAST(CAST(ts AS DATE) AS TIME) FROM t",
      ts + dateOfD + midnightOf("param2"),
      SQLTypes.Time
    )
  }

  "CURRENT_DATE" should "be converted through its UTC midnight" in {
    val today =
      "def param1 = ZonedDateTime.ofInstant(Instant.ofEpochMilli(params.__now__), ZoneId.of('Z')).toLocalDate(); "
    pin(
      "SELECT CAST(CURRENT_DATE AS BIGINT) FROM t",
      today + epochOf("param1"),
      SQLTypes.BigInt
    )
    pin(
      "SELECT CAST(CURRENT_DATE AS TIME) FROM t",
      today + midnightOf("param1"),
      SQLTypes.Time
    )
  }

  "a DATE literal" should "be converted through its UTC midnight" in {
    val date =
      "def param1 = LocalDate.parse((\"2024-01-03\").replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); "
    pin(
      "SELECT CAST(CAST('2024-01-03' AS DATE) AS BIGINT) FROM t",
      date + epochOf("param1"),
      SQLTypes.BigInt
    )
    pin(
      "SELECT CAST(CAST('2024-01-03' AS DATE) AS TIME) FROM t",
      date + midnightOf("param1"),
      SQLTypes.Time
    )
  }

  /** The CASE answers NULL: it is bound once, whole, and the conversion is guarded. */
  "a DATE CASE" should "be converted through its UTC midnight" in {
    val caseOf =
      "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
      "def param2 = param1 == null ? false : (param1 > 1); " +
      "def param3 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
      "def param4 = (param3 != null ? param3.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
      "def param5 = (param2 ? null : param4); "
    pin(
      "SELECT CAST(CASE WHEN n > 1 THEN NULL ELSE CAST(d AS DATE) END AS BIGINT) FROM t",
      caseOf + epochOf("param5"),
      SQLTypes.BigInt
    )
    pin(
      "SELECT CAST(CASE WHEN n > 1 THEN NULL ELSE CAST(d AS DATE) END AS TIME) FROM t",
      caseOf + midnightOf("param5"),
      SQLTypes.Time
    )
  }

  "a DATE COALESCE" should "be converted through its UTC midnight" in {
    val coalesce =
      d + dateOfD + s"def param3 = $literal; " + "def param4 = (param2 != null ? param2 : param3); "
    pin(
      "SELECT CAST(COALESCE(CAST(d AS DATE), CAST('2024-01-01' AS DATE)) AS BIGINT) FROM t",
      coalesce + epochOf("param4"),
      SQLTypes.BigInt
    )
    pin(
      "SELECT CAST(COALESCE(CAST(d AS DATE), CAST('2024-01-01' AS DATE)) AS TIME) FROM t",
      coalesce + midnightOf("param4"),
      SQLTypes.Time
    )
  }

  "a DATE NULLIF" should "be converted through its UTC midnight" in {
    val nullIf =
      ts + "def param2 = (param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
      "def param3 = param2 == null || (param1 != null && Math.floor((param2 == null ? null : param2 instanceof LocalDate ? param2.atStartOfDay(ZoneId.of('Z')) : param2 instanceof LocalDateTime ? param2.atZone(ZoneId.of('Z')) : param2.withZoneSameInstant(ZoneId.of('Z'))).toInstant().toEpochMilli() / 86400000.0) * 86400000.0 == param1.toInstant().toEpochMilli()) ? null : param2; "
    pin(
      "SELECT CAST(NULLIF(CAST(ts AS DATE), ts) AS BIGINT) FROM t",
      nullIf + epochOf("param3"),
      SQLTypes.BigInt
    )
    pin(
      "SELECT CAST(NULLIF(CAST(ts AS DATE), ts) AS TIME) FROM t",
      nullIf + midnightOf("param3"),
      SQLTypes.Time
    )
  }

  "GREATEST and LEAST over dates" should "be converted through their UTC midnight" in {
    def reduced(reducer: String): String =
      d + dateOfD + s"def param3 = $literal; " +
      "def lv1 = (param2 == null ? null : (def)(Math.floor((param2 == null ? null : param2 instanceof LocalDate ? param2.atStartOfDay(ZoneId.of('Z')) : param2 instanceof LocalDateTime ? param2.atZone(ZoneId.of('Z')) : param2.withZoneSameInstant(ZoneId.of('Z'))).toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
      "def lv2 = Math.floor((param3 == null ? null : param3 instanceof LocalDate ? param3.atStartOfDay(ZoneId.of('Z')) : param3 instanceof LocalDateTime ? param3.atZone(ZoneId.of('Z')) : param3.withZoneSameInstant(ZoneId.of('Z'))).toInstant().toEpochMilli() / 86400000.0) * 86400000.0; " +
      s"def param4 = Instant.ofEpochMilli((long) ((lv1 == null ? lv2 : Math.$reducer(lv1, lv2)))).atZone(ZoneId.of('Z')).toInstant().atZone(ZoneId.of('Z')).toLocalDate(); "
    for ((function, reducer) <- Seq("GREATEST" -> "max", "LEAST" -> "min")) {
      pin(
        s"SELECT CAST($function(CAST(d AS DATE), CAST('2024-01-01' AS DATE)) AS BIGINT) FROM t",
        reduced(reducer) + epochOf("param4"),
        SQLTypes.BigInt
      )
      pin(
        s"SELECT CAST($function(CAST(d AS DATE), CAST('2024-01-01' AS DATE)) AS TIME) FROM t",
        reduced(reducer) + midnightOf("param4"),
        SQLTypes.Time
      )
    }
  }

  "LAST_DAY" should "be converted through its UTC midnight" in {
    val lastDay =
      ts + "def param2 = ((param1 == null) ? null : param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate().with(TemporalAdjusters.lastDayOfMonth())); "
    pin(
      "SELECT CAST(LAST_DAY(ts) AS BIGINT) FROM t",
      lastDay + epochOf("param2"),
      SQLTypes.BigInt
    )
    pin(
      "SELECT CAST(LAST_DAY(ts) AS TIME) FROM t",
      lastDay + midnightOf("param2"),
      SQLTypes.Time
    )
  }

  /** `DATE_ADD` adjusts its receiver and renders its class; `DATETIME_ADD` renders a date-time. */
  "a date function over a DATE" should "be converted as what it renders" in {
    pin(
      "SELECT CAST(DATE_ADD(CAST(d AS DATE), INTERVAL 1 DAY) AS BIGINT) FROM t",
      d + dateOfD + "def param3 = ((param2 == null) ? null : (def)(param2.plus(1, ChronoUnit.DAYS))); " +
      epochOf("param3"),
      SQLTypes.BigInt
    )
    // a TIMESTAMP DATE_ADD and a DATETIME_ADD keep the TIMESTAMP conversion
    pin(
      "SELECT CAST(DATE_ADD(ts, INTERVAL 1 DAY) AS BIGINT) FROM t",
      "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value.plus(1, ChronoUnit.DAYS)); " +
      "(param1 != null ? (def)(param1.toInstant().toEpochMilli()) : null)",
      SQLTypes.BigInt
    )
    pin(
      "SELECT CAST(DATETIME_ADD(CAST(d AS DATE), INTERVAL 1 HOUR) AS BIGINT) FROM t",
      d + dateOfD +
      "def param3 = (param2 == null) ? null : (def)(param2.atStartOfDay(ZoneId.of('Z')).plus(1, ChronoUnit.HOURS)); " +
      "(param3 != null ? (def)(param3.toInstant().toEpochMilli()) : null)",
      SQLTypes.BigInt
    )
  }

  /** The conversion's own calls answer the UTC midnight of the day. */
  "the conversion" should "answer the epoch milliseconds of the UTC midnight and 00:00:00" in {
    val midnight = java.time.LocalDate.of(2024, 1, 3).atStartOfDay(java.time.ZoneId.of("Z"))
    midnight.toInstant.toEpochMilli shouldBe 1704240000000L
    midnight.toLocalTime.toString shouldBe "00:00"
  }

  "an ingest processor and a transform" should "render the casts as they did" in {
    val date =
      "def param1 = LocalDate.parse((\"2024-01-03\").replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); param1"
    for (contextType <- Seq(PainlessContextType.Processor, PainlessContextType.Transform))
      rendered(
        "SELECT CAST(CAST('2024-01-03' AS DATE) AS BIGINT) FROM t",
        contextType
      )._1 shouldBe date
    rendered(
      "SELECT CAST(CURRENT_DATE AS BIGINT) FROM t",
      PainlessContextType.Processor
    )._1 shouldBe
    "def param1 = ZonedDateTime.ofInstant(Instant.ofEpochMilli(System.currentTimeMillis()), ZoneId.of('Z')); param1"
    rendered(
      "SELECT CAST(CAST(ts AS DATE) AS BIGINT) FROM t",
      PainlessContextType.Processor
    )._1 shouldBe
    "def param1 = (ctx.ts instanceof String ? ZonedDateTime.parse((ctx.ts).replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z'))) : Instant.ofEpochMilli(ctx.ts).atZone(ZoneId.of('Z'))); " +
    "(param1 != null ? (def)(param1.toInstant().toEpochMilli()) : null)"
    rendered(
      "SELECT CAST(COALESCE(CAST(d AS DATE), CAST('2024-01-01' AS DATE)) AS BIGINT) FROM t",
      PainlessContextType.Transform
    )._1 shouldBe
    d + dateOfD + s"def param3 = $literal; " +
    "(param2 != null ? param2 : param3).toInstant().toEpochMilli()"
  }
}
