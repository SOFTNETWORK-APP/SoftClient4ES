package app.softnetwork.elastic.sql.function.cond

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A `CASE`, a `COALESCE`, a `NULLIF`, a `GREATEST` / `LEAST` over dates and `LAST_DAY`'s receiver
  * answer ONE class: the type the node REPORTS -- DuckDB's common type of what its arms report,
  * DATE + DATE = DATE, DATE + TIMESTAMP = TIMESTAMP, INT + DOUBLE = DOUBLE -- each arm converted to
  * it once, FROM what the arm renders (`SQLTypeUtils.convertArm`).
  *
  * 🔴 Before, an arm was converted from its column's type (a schema-resolved identifier's
  * `baseType`, TIMESTAMP for every `date` field whatever function it wears) to the arms' common
  * `out`: a CASE of two DATE arms was a TIMESTAMP, its `CAST(… AS DATE)` arm stayed a `LocalDate`,
  * its DATE literal was widened to a `ZonedDateTime` beside it and a bare DATE column stayed its
  * doc value -- two classes in a column reported DATE, and a consumer (`DATE_TRUNC`) called
  * `truncatedTo` on the `LocalDate` rows. A date doc value converts through the UTC normalisation
  * (`….toInstant().atZone(ZoneId.of('Z')).toLocalDate()`), which Elasticsearch 6.8 needs.
  *
  * Arms with no common type (a DATE beside a DOUBLE: DuckDB refuses it), an arm whose type is still
  * unknown (no schema, a `date_nanos` column) and an ingest processor render as they did: the
  * processor's source is persisted, and `ALTER` must not see it change.
  */
class ConditionalArmClassSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("name", SQLTypes.Keyword),
      Column("kd", SQLTypes.Keyword),
      Column("d", SQLTypes.Date),
      Column("d2", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("n", SQLTypes.Int),
      Column("x", SQLTypes.Double),
      Column("dn", SQLTypes.Any)
    )
  )

  private def field(sql: String) = Parser(sql) match {
    case Right(ss: SingleSearch) => ss.update(Some(schema)).select.fields.head
    case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  /** The first SELECT field rendered as one script; the body first, since it registers the
    * parameters.
    */
  private def scriptOf(
    sql: String,
    contextType: PainlessContextType = PainlessContextType.Query
  ): String = {
    val ctx = PainlessContext(contextType)
    val body = field(sql).painless(Some(ctx))
    s"$ctx$body"
  }

  /** The WHERE criterion rendered as one script; the body first, since it registers the parameters.
    */
  private def whereOf(
    sql: String,
    contextType: PainlessContextType = PainlessContextType.Query
  ): String = Parser(sql) match {
    case Right(ss: SingleSearch) =>
      val criteria = ss.update(Some(schema)).where.flatMap(_.criteria).getOrElse(fail(sql))
      val ctx = PainlessContext(contextType)
      val body = criteria.painless(Some(ctx))
      s"$ctx$body"
    case other => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def typeOf(sql: String): (SQLType, SQLType) = {
    val f = field(sql)
    (f.identifier.baseType, f.identifier.reportedType)
  }

  private def count(haystack: String, needle: String): Int =
    haystack.sliding(needle.length).count(_ == needle)

  "a CASE of two DATE arms" should "answer a DATE, each arm converted once from what it renders" in {
    val sql = "SELECT CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE d2 END FROM t"
    typeOf(sql) shouldBe ((SQLTypes.Date, SQLTypes.Date))
    // the cast is a LocalDate already; the bare DATE column's doc value is converted, once
    scriptOf(sql) shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param3 = param1 == null ? false : (param1 > 0); " +
    "def param4 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
    "def param5 = (param4 != null ? param4.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
    "param3 ? (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null) : param5"
  }

  "a CASE of a DATE and a TIMESTAMP arm" should "answer a TIMESTAMP" in {
    val sql = "SELECT CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE ts END FROM t"
    typeOf(sql) shouldBe ((SQLTypes.Timestamp, SQLTypes.Timestamp))
    // the cast's LocalDate is the start of its UTC day; the timestamp is its doc value
    scriptOf(sql) shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param3 = (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
    "def param4 = param1 == null ? false : (param1 > 0); " +
    "def param5 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "param4 ? (param3 != null ? param3.atStartOfDay(ZoneId.of('Z')) : null) : param5"
  }

  "a DATE literal beside a DATE column" should "stay a LocalDate, the column converted to one" in {
    val sql = "SELECT CASE WHEN n > 0 THEN CAST('2024-01-01' AS DATE) ELSE d END FROM t"
    typeOf(sql) shouldBe ((SQLTypes.Date, SQLTypes.Date))
    scriptOf(sql) shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = LocalDate.parse((\"2024-01-01\").replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); " +
    "def param3 = param1 == null ? false : (param1 > 0); " +
    "def param4 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param5 = (param4 != null ? param4.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
    "param3 ? param2 : param5"

    // the simple CASE spelling, where the literal used to be widened to a ZonedDateTime
    val simple = scriptOf(
      "SELECT CASE n WHEN 1 THEN CAST(d AS DATE) ELSE CAST('2024-01-01' AS DATE) END FROM t"
    )
    simple should not include "atStartOfDay"
    count(simple, ".toInstant().atZone(ZoneId.of('Z')).toLocalDate()") shouldBe 1
  }

  "CURRENT_DATE beside a DATE column" should "stay a LocalDate" in {
    val sql = "SELECT CASE WHEN n > 0 THEN CURRENT_DATE ELSE d END FROM t"
    typeOf(sql) shouldBe ((SQLTypes.Date, SQLTypes.Date))
    val emitted = scriptOf(sql)
    emitted should not include "atStartOfDay"
    emitted should endWith("param3 ? param2 : param5")
  }

  "LAST_DAY beside a cast to DATE" should "answer a DATE, as DuckDB types last_day" in {
    val sql = "SELECT CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE LAST_DAY(ts) END FROM t"
    typeOf(sql) shouldBe ((SQLTypes.Date, SQLTypes.Date))
    scriptOf(sql) shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param3 = param1 == null ? false : (param1 > 0); " +
    "def param4 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "def param5 = (param4 == null) ? null : param4.toInstant().atZone(ZoneId.of('Z')).toLocalDate().with(TemporalAdjusters.lastDayOfMonth()); " +
    "param3 ? (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null) : param5"
  }

  "an INT and a DOUBLE arm" should "answer a DOUBLE" in {
    typeOf("SELECT CASE WHEN n > 0 THEN 1 ELSE 2.5 END FROM t") shouldBe
    ((SQLTypes.Double, SQLTypes.Double))
    scriptOf("SELECT CASE WHEN n > 0 THEN 1 ELSE 2.5 END FROM t") should endWith(
      "param2 ? ((double) 1) : 2.5"
    )
    // COALESCE converted no arm at all: the literal 2 was an Integer beside the doubles
    val sql = "SELECT COALESCE(CAST(n AS DOUBLE), 2) FROM t"
    typeOf(sql) shouldBe ((SQLTypes.Double, SQLTypes.Double))
    scriptOf(sql) shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = (param1 != null ? (def)(((double) param1)) : null); " +
    "(param2 != null ? param2 : ((double) 2))"
  }

  "COALESCE, NULLIF and GREATEST / LEAST over dates" should "answer the type they report" in {
    typeOf("SELECT COALESCE(d, d2) FROM t") shouldBe ((SQLTypes.Date, SQLTypes.Date))
    scriptOf("SELECT COALESCE(d, d2) FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param2 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
    "(param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null))"

    typeOf("SELECT COALESCE(CAST(d AS DATE), ts) FROM t") shouldBe
    ((SQLTypes.Timestamp, SQLTypes.Timestamp))
    scriptOf("SELECT COALESCE(CAST(d AS DATE), ts) FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param2 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "def param3 = (param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
    "(param3 != null ? param3.atStartOfDay(ZoneId.of('Z')) : param2)"

    // NULLIF answers its first argument, converted; the comparison is unchanged
    typeOf("SELECT NULLIF(d, d2) FROM t") shouldBe ((SQLTypes.Date, SQLTypes.Date))
    scriptOf("SELECT NULLIF(d, d2) FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param2 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
    "def param3 = param1 == null || (param2 != null && param1.isEqual(param2)) ? null : param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate(); " +
    "param3"

    // the instant that wins is the DATE the reducer reports; beside a TIMESTAMP, a TIMESTAMP
    typeOf("SELECT GREATEST(d, d2) FROM t") shouldBe ((SQLTypes.Date, SQLTypes.Date))
    scriptOf("SELECT GREATEST(d, d2) FROM t") should endWith(
      "(lv3 == null ? null : Instant.ofEpochMilli((long) lv3).atZone(ZoneId.of('Z'))" +
      ".toInstant().atZone(ZoneId.of('Z')).toLocalDate())"
    )
    typeOf("SELECT LEAST(d, ts) FROM t") shouldBe ((SQLTypes.Timestamp, SQLTypes.Timestamp))
    scriptOf("SELECT LEAST(d, ts) FROM t") should endWith(
      "(lv3 == null ? null : Instant.ofEpochMilli((long) lv3).atZone(ZoneId.of('Z')))"
    )
  }

  /** The class the CASE answers is the one its consumer reads: `DATE_TRUNC` decides on the type
    * whether to truncate the time, a `CAST` what to convert from.
    */
  "a consumer of a conditional" should "read the type the conditional answers" in {
    // DATE + TIMESTAMP: a TIMESTAMP, so the truncation keeps `truncatedTo` -- on a ZonedDateTime
    scriptOf(
      "SELECT DATE_TRUNC(CASE WHEN n > 1 THEN ts ELSE CAST(ts AS DATE) END, MONTH) FROM t"
    ) shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "def param3 = param1 == null ? false : (param1 > 1); " +
    "def param4 = (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
    "def param5 = (param4 != null ? param4.atStartOfDay(ZoneId.of('Z')) : null); " +
    "def param6 = param3 ? param2 : param5; " +
    "(param6 == null) ? null : (def)(param6.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS))"
    // two DATEs: a DATE, so no `truncatedTo`, which LocalDate does not have
    scriptOf(
      "SELECT DATE_TRUNC(COALESCE(CAST(d AS DATE), CAST(ts AS DATE)), MONTH) FROM t"
    ) should not include "truncatedTo"
    // a cast over the conditional converts it from the type it answers: once
    val cast = scriptOf("SELECT CAST(COALESCE(CAST(ts AS DATE), ts) AS DATE) FROM t")
    count(cast, ".toInstant()") shouldBe 2
    cast should not include ".toLocalDate().toInstant()"
    // LAST_DAY over a conditional converts the whole of it, not its last branch, inside a guard
    // on it: the CASE answers NULL where `ts` / `d` is missing
    scriptOf("SELECT LAST_DAY(CASE WHEN n > 0 THEN ts ELSE d END) FROM t") should endWith(
      "; def param5 = (param3 ? param2 : param4); (param5 == null) ? null : " +
      "param5.toInstant().atZone(ZoneId.of('Z')).toLocalDate()" +
      ".with(TemporalAdjusters.lastDayOfMonth())"
    )
  }

  "arms with no common type" should "render as they did" in {
    // a DATE beside a DOUBLE: DuckDB refuses it (`Cannot mix values of type DATE and DOUBLE`)
    scriptOf("SELECT CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE x END FROM t") shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param3 = param1 == null ? false : (param1 > 0); " +
    "def param4 = (doc['x'].size() == 0 ? null : doc['x'].value); " +
    "param3 ? (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null) : param4"
  }

  "an arm whose type is unknown" should "keep its conversion" in {
    // a `date_nanos` column types `Any` (BIDC-4): guessing its class would drop its time
    scriptOf("SELECT CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE dn END FROM t") shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param3 = param1 == null ? false : (param1 > 0); " +
    "def param4 = (doc['dn'].size() == 0 ? null : doc['dn'].value); " +
    "param3 ? (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null) : param4"
  }

  /** A conditional's nameless identifier answers `nullable = false`, so a conversion of what it
    * renders was appended to its own NULL guard -- and ran on its NULL -- instead of being placed
    * inside one. `GREATEST(d, d2) > d` failed the statement on every document missing both dates
    * (MEASURED on Elasticsearch 8.18.3 and 9.0.3). Each consumer that converts it now reads
    * `SQLTypeUtils.mayRenderNull`, and the operand is bound once and converted inside its guard.
    */
  "a DATE conditional compared with a timestamp" should "be promoted to its midnight inside its NULL guard" in {
    whereOf("SELECT name FROM t WHERE GREATEST(d, d2) > d") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param2 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
    "def lv1 = (param1 == null ? null : (def)(Math.floor(param1.toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
    "def lv2 = (param2 == null ? null : (def)(Math.floor(param2.toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
    "def lv3 = (lv1 == null ? lv2 : (lv2 == null ? lv1 : Math.max(lv1, lv2))); " +
    "def param3 = (lv3 == null ? null : Instant.ofEpochMilli((long) lv3).atZone(ZoneId.of('Z')).toInstant().atZone(ZoneId.of('Z')).toLocalDate()); " +
    "def left4 = (param3 != null ? param3.atStartOfDay(ZoneId.of('Z')) : null); " +
    "(left4 == null || param1 == null ? false : ((left4.isAfter(param1))))"
    // the whole CASE is converted, not its last branch, and inside its guard
    whereOf("SELECT name FROM t WHERE CASE WHEN n > 0 THEN d ELSE d2 END > ts") shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param3 = param1 == null ? false : (param1 > 0); " +
    "def param4 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
    "def param5 = (param4 != null ? param4.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
    "def param6 = param3 ? (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null) : param5; " +
    "def param7 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "def left1 = (param6 != null ? param6.atStartOfDay(ZoneId.of('Z')) : null); " +
    "(left1 == null || param7 == null ? false : ((left1.isAfter(param7))))"
    // a CASE that cannot be NULL is converted as ONE value too
    whereOf(
      "SELECT name FROM t WHERE " +
      "CASE WHEN n > 0 THEN CAST('2024-01-01' AS DATE) ELSE CAST('2024-02-01' AS DATE) END > ts"
    ) shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = LocalDate.parse((\"2024-01-01\").replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); " +
    "def param3 = param1 == null ? false : (param1 > 0); " +
    "def param4 = LocalDate.parse((\"2024-02-01\").replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); " +
    "def param5 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "(param5 == null ? false : (((param3 ? param2 : param4).atStartOfDay(ZoneId.of('Z')).isAfter(param5))))"
  }

  "a conditional arm that can be NULL" should "be converted inside its NULL guard" in {
    // a CASE arm
    scriptOf("SELECT CASE WHEN n > 0 THEN GREATEST(d, d2) ELSE ts END FROM t") shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param3 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
    "def lv1 = (param2 == null ? null : (def)(Math.floor(param2.toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
    "def lv2 = (param3 == null ? null : (def)(Math.floor(param3.toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
    "def lv3 = (lv1 == null ? lv2 : (lv2 == null ? lv1 : Math.max(lv1, lv2))); " +
    "def param4 = (lv3 == null ? null : Instant.ofEpochMilli((long) lv3).atZone(ZoneId.of('Z')).toInstant().atZone(ZoneId.of('Z')).toLocalDate()); " +
    "def param5 = param1 == null ? false : (param1 > 0); " +
    "def param6 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "param5 ? (param4 != null ? param4.atStartOfDay(ZoneId.of('Z')) : null) : param6"
    // the last COALESCE arm
    scriptOf("SELECT COALESCE(ts, GREATEST(d, d2)) FROM t") shouldBe
    "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param3 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
    "def lv1 = (param2 == null ? null : (def)(Math.floor(param2.toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
    "def lv2 = (param3 == null ? null : (def)(Math.floor(param3.toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
    "def lv3 = (lv1 == null ? lv2 : (lv2 == null ? lv1 : Math.max(lv1, lv2))); " +
    "def param4 = (lv3 == null ? null : Instant.ofEpochMilli((long) lv3).atZone(ZoneId.of('Z')).toInstant().atZone(ZoneId.of('Z')).toLocalDate()); " +
    "(param1 != null ? param1 : (param4 != null ? param4.atStartOfDay(ZoneId.of('Z')) : null))"
    // a GREATEST argument, converted to its epoch milliseconds
    scriptOf("SELECT GREATEST(COALESCE(d, d2), ts) FROM t") shouldBe
    "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param3 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
    "def lv1 = (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : (param3 != null ? param3.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null)); " +
    "def lv2 = (lv1 == null ? null : (def)(Math.floor((lv1 == null ? null : lv1 instanceof LocalDate ? lv1.atStartOfDay(ZoneId.of('Z')) : lv1 instanceof LocalDateTime ? lv1.atZone(ZoneId.of('Z')) : lv1.withZoneSameInstant(ZoneId.of('Z'))).toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
    "def lv3 = (param1 == null ? null : (def)(param1.toInstant().toEpochMilli())); " +
    "def lv4 = (lv2 == null ? lv3 : (lv3 == null ? lv2 : Math.max(lv2, lv3))); " +
    "(lv4 == null ? null : Instant.ofEpochMilli((long) lv4).atZone(ZoneId.of('Z')))"
    // a NULLIF's converted answer beside a numeric comparison
    scriptOf("SELECT NULLIF(d, n) FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param2 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param3 = param1 == null || param1 == param2 ? null : param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate(); " +
    "param3"
  }

  /** 🔴 The ALTER-churn rule: an ingest processor's source is persisted and compared, so its bytes
    * do not move, nor do those of a consumer reading the conditional's type while it renders. These
    * are the bytes the processor rendered before this fix.
    */
  "LAST_DAY over a nameless operand that can be NULL" should "adjust it inside its NULL guard" in {
    // the DATE_TRUNC guards its own NULL: it is bound, and the conversion and the adjuster apply
    // inside a guard on it, never to the guarded expression (a NULL dereference)
    val script =
      scriptOf("SELECT LAST_DAY(DATE_TRUNC(CASE WHEN n > 1 THEN ts ELSE NULL END, MONTH)) FROM t")
    script should endWith(
      "; (param5 == null) ? null : " +
      "param5.toInstant().atZone(ZoneId.of('Z')).toLocalDate().with(TemporalAdjusters.lastDayOfMonth())"
    )
    script should not include ")).with("
    script should not include ")).toInstant("
  }

  "the type an ingest processor writes" should "be read in the scope it renders in" in {
    import app.softnetwork.elastic.sql.`type`.SQLTypeUtils
    import app.softnetwork.elastic.sql.query.Update
    import app.softnetwork.elastic.sql.schema.ScriptProcessor
    // read after the render had closed, these were the QUERY's types (DATE, DOUBLE) over a script
    // writing the legacy class (a doc value, the integer 2)
    Seq(
      "CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE d2 END" -> SQLTypes.Timestamp,
      "COALESCE(CAST(d AS DATE), d2)"                    -> SQLTypes.Timestamp,
      "COALESCE(CAST(n AS DOUBLE), 2)"                   -> SQLTypes.BigInt
    ).foreach { case (expr, written) =>
      val identifier = field(s"SELECT $expr FROM t").identifier
      withClue(s"[$expr] ") {
        SQLTypeUtils.processorType(identifier) shouldBe written
        ScriptProcessor.fromScript("c", field(s"SELECT $expr FROM t").identifier).dataType shouldBe
        written
      }
    }
    // an UPDATE's one-off pipeline describes its processor as before
    Seq(
      "LEAST(NULL, n)"                 -> "c NUMERIC SCRIPT AS (LEAST(NULL, n))",
      "COALESCE(CAST(n AS DOUBLE), 2)" -> "c DOUBLE SCRIPT AS (COALESCE(CAST(n AS DOUBLE), 2))",
      "COALESCE(d, d2)"                -> "c ANY SCRIPT AS (COALESCE(d, d2))"
    ).foreach { case (expr, description) =>
      Parser(s"UPDATE t SET c = $expr WHERE n > 0") match {
        case Right(u: Update) =>
          u.customPipeline.processors.flatMap(_.properties.get("description")) shouldBe
            Seq(description)
        case other => fail(s"[$expr] expected an UPDATE, got $other")
      }
    }
  }

  "an ingest processor" should "render a conditional as it did" in {
    scriptOf(
      "SELECT CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE d2 END FROM t",
      PainlessContextType.Processor
    ) shouldBe
    "def param1 = ctx.n; " +
    "def param2 = (ctx.d instanceof String ? (ctx.d.length() > 10 ? ZonedDateTime.parse((ctx.d).replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z'))).withZoneSameInstant(ZoneId.of('Z')).truncatedTo(ChronoUnit.DAYS) : LocalDate.parse((ctx.d).replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")).atStartOfDay(ZoneId.of('Z'))) : Instant.ofEpochMilli(ctx.d).atZone(ZoneId.of('Z'))); " +
    "def param3 = param1 == null ? false : (param1 > 0); " +
    "def param4 = ctx.d2; " +
    "def param5 = (param4 instanceof String ? (param4.length() > 10 ? ZonedDateTime.parse((param4).replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z'))).withZoneSameInstant(ZoneId.of('Z')).truncatedTo(ChronoUnit.DAYS) : LocalDate.parse((param4).replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")).atStartOfDay(ZoneId.of('Z'))) : Instant.ofEpochMilli(param4).atZone(ZoneId.of('Z'))); " +
    "param3 ? (param2 instanceof String ? (param2.length() > 10 ? ZonedDateTime.parse((param2).replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z'))).withZoneSameInstant(ZoneId.of('Z')).truncatedTo(ChronoUnit.DAYS) : LocalDate.parse((param2).replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")).atStartOfDay(ZoneId.of('Z'))) : Instant.ofEpochMilli(param2).atZone(ZoneId.of('Z'))) : param5"
    scriptOf(
      "SELECT DATE_TRUNC(COALESCE(CAST(d AS DATE), CAST(ts AS DATE)), MONTH) FROM t",
      PainlessContextType.Processor
    ) shouldBe
    "def param1 = ctx.d; " +
    "def param2 = ctx.ts; " +
    "def param3 = (ctx.d instanceof String ? (ctx.d.length() > 10 ? ZonedDateTime.parse((ctx.d).replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z'))).withZoneSameInstant(ZoneId.of('Z')).truncatedTo(ChronoUnit.DAYS) : LocalDate.parse((ctx.d).replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")).atStartOfDay(ZoneId.of('Z'))) : Instant.ofEpochMilli(ctx.d).atZone(ZoneId.of('Z'))); " +
    "def param4 = (param3 instanceof String ? (param3.length() > 10 ? ZonedDateTime.parse((param3).replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z'))).withZoneSameInstant(ZoneId.of('Z')).truncatedTo(ChronoUnit.DAYS) : LocalDate.parse((param3).replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")).atStartOfDay(ZoneId.of('Z'))) : Instant.ofEpochMilli(param3).atZone(ZoneId.of('Z'))); " +
    "def param5 = (ctx.ts instanceof String ? ZonedDateTime.parse((ctx.ts).replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z'))) : Instant.ofEpochMilli(ctx.ts).atZone(ZoneId.of('Z'))); " +
    "def param6 = (param5 instanceof String ? ZonedDateTime.parse((param5).replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z'))) : Instant.ofEpochMilli(param5).atZone(ZoneId.of('Z'))); " +
    "def param7 = (param4 != null ? param4 : param6); " +
    "(param7 == null) ? null : (def)(param7.withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS))"
    scriptOf("SELECT LAST_DAY(ts) FROM t", PainlessContextType.Processor) shouldBe
    "def param1 = ctx.ts; " +
    "def param2 = (param1 instanceof String ? ZonedDateTime.parse((param1).replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z'))) : Instant.ofEpochMilli(param1).atZone(ZoneId.of('Z'))); " +
    "(param1 == null) ? null : param2.with(TemporalAdjusters.lastDayOfMonth())"
  }

  /** One TIMESTAMP-vs-DATE-conditional shape: the Painless every operator shares, its NULL guard,
    * and the two operands the operator compares.
    */
  private final case class Shape(
    conditional: String,
    tsOnTheLeft: Boolean,
    prologue: String,
    guard: String,
    left: String,
    right: String
  )

  /** How a comparison of two `java.time` values renders each operator. */
  private def compared(op: String, left: String, right: String): String = op match {
    case "<"  => s"$left.isBefore($right)"
    case "<=" => s"$left.isAfter($right) == false"
    case ">"  => s"$left.isAfter($right)"
    case ">=" => s"$left.isBefore($right) == false"
    case "="  => s"$left.isEqual($right)"
    case _    => s"$left.isEqual($right) == false"
  }

  "a TIMESTAMP compared with a DATE conditional" should "compare instants, the DATE taken at its UTC midnight, on either side" in {
    // DuckDB casts the DATE to a TIMESTAMP: `2024-01-31T10:30Z > DATE '2024-01-31'` is TRUE. The
    // TIMESTAMP is never narrowed to its day (`doc['ts'].value.toLocalDate()`), which answered FALSE.
    val shapes = Seq(
      Shape(
        "CASE WHEN n > 0 THEN d ELSE d2 END",
        true,
        "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
        "def param2 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
        "def param3 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
        "def param4 = param2 == null ? false : (param2 > 0); " +
        "def param5 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
        "def param6 = (param5 != null ? param5.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
        "def left1 = param1; " +
        "def right2 = param4 ? (param3 != null ? param3.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null) : param6; ",
        "left1 == null || right2 == null",
        "left1",
        "right2.atStartOfDay(ZoneId.of('Z'))"
      ),
      Shape(
        "CASE WHEN n > 0 THEN d ELSE d2 END",
        false,
        "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
        "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
        "def param3 = param1 == null ? false : (param1 > 0); " +
        "def param4 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
        "def param5 = (param4 != null ? param4.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
        "def param6 = param3 ? (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null) : param5; " +
        "def param7 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
        "def left1 = (param6 != null ? param6.atStartOfDay(ZoneId.of('Z')) : null); ",
        "left1 == null || param7 == null",
        "left1",
        "param7"
      ),
      Shape(
        "COALESCE(d, d2)",
        true,
        "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
        "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
        "def param3 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
        "def left1 = param1; " +
        "def right2 = (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : (param3 != null ? param3.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null)); ",
        "left1 == null || right2 == null",
        "left1",
        "right2.atStartOfDay(ZoneId.of('Z'))"
      ),
      Shape(
        "COALESCE(d, d2)",
        false,
        "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
        "def param2 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
        "def param3 = (param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null)); " +
        "def param4 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
        "def left1 = (param3 != null ? param3.atStartOfDay(ZoneId.of('Z')) : null); ",
        "left1 == null || param4 == null",
        "left1",
        "param4"
      ),
      Shape(
        "NULLIF(d, d2)",
        true,
        "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
        "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
        "def param3 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
        "def param4 = param2 == null || (param3 != null && param2.isEqual(param3)) ? null : param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate(); " +
        "def left1 = param1; ",
        "left1 == null || param4 == null",
        "left1",
        "param4.atStartOfDay(ZoneId.of('Z'))"
      ),
      Shape(
        "NULLIF(d, d2)",
        false,
        "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
        "def param2 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
        "def param3 = param1 == null || (param2 != null && param1.isEqual(param2)) ? null : param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate(); " +
        "def param4 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
        "def left1 = (param3 != null ? param3.atStartOfDay(ZoneId.of('Z')) : null); ",
        "left1 == null || param4 == null",
        "left1",
        "param4"
      ),
      Shape(
        "GREATEST(d, d2)",
        true,
        "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
        "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
        "def param3 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
        "def lv1 = (param2 == null ? null : (def)(Math.floor(param2.toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
        "def lv2 = (param3 == null ? null : (def)(Math.floor(param3.toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
        "def lv3 = (lv1 == null ? lv2 : (lv2 == null ? lv1 : Math.max(lv1, lv2))); " +
        "def left4 = param1; " +
        "def right5 = (lv3 == null ? null : Instant.ofEpochMilli((long) lv3).atZone(ZoneId.of('Z')).toInstant().atZone(ZoneId.of('Z')).toLocalDate()); ",
        "left4 == null || right5 == null",
        "left4",
        "right5.atStartOfDay(ZoneId.of('Z'))"
      ),
      Shape(
        "GREATEST(d, d2)",
        false,
        "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
        "def param2 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
        "def lv1 = (param1 == null ? null : (def)(Math.floor(param1.toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
        "def lv2 = (param2 == null ? null : (def)(Math.floor(param2.toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
        "def lv3 = (lv1 == null ? lv2 : (lv2 == null ? lv1 : Math.max(lv1, lv2))); " +
        "def param3 = (lv3 == null ? null : Instant.ofEpochMilli((long) lv3).atZone(ZoneId.of('Z')).toInstant().atZone(ZoneId.of('Z')).toLocalDate()); " +
        "def param4 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
        "def left4 = (param3 != null ? param3.atStartOfDay(ZoneId.of('Z')) : null); ",
        "left4 == null || param4 == null",
        "left4",
        "param4"
      ),
      Shape(
        "LEAST(d, d2)",
        true,
        "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
        "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
        "def param3 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
        "def lv1 = (param2 == null ? null : (def)(Math.floor(param2.toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
        "def lv2 = (param3 == null ? null : (def)(Math.floor(param3.toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
        "def lv3 = (lv1 == null ? lv2 : (lv2 == null ? lv1 : Math.min(lv1, lv2))); " +
        "def left4 = param1; " +
        "def right5 = (lv3 == null ? null : Instant.ofEpochMilli((long) lv3).atZone(ZoneId.of('Z')).toInstant().atZone(ZoneId.of('Z')).toLocalDate()); ",
        "left4 == null || right5 == null",
        "left4",
        "right5.atStartOfDay(ZoneId.of('Z'))"
      ),
      Shape(
        "LEAST(d, d2)",
        false,
        "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
        "def param2 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
        "def lv1 = (param1 == null ? null : (def)(Math.floor(param1.toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
        "def lv2 = (param2 == null ? null : (def)(Math.floor(param2.toInstant().toEpochMilli() / 86400000.0) * 86400000.0)); " +
        "def lv3 = (lv1 == null ? lv2 : (lv2 == null ? lv1 : Math.min(lv1, lv2))); " +
        "def param3 = (lv3 == null ? null : Instant.ofEpochMilli((long) lv3).atZone(ZoneId.of('Z')).toInstant().atZone(ZoneId.of('Z')).toLocalDate()); " +
        "def param4 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
        "def left4 = (param3 != null ? param3.atStartOfDay(ZoneId.of('Z')) : null); ",
        "left4 == null || param4 == null",
        "left4",
        "param4"
      ),
      Shape(
        "LAST_DAY(COALESCE(d, d2))",
        true,
        "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
        "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
        "def param3 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
        "def param4 = (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : (param3 != null ? param3.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null)); " +
        "def left1 = param1; " +
        "def right2 = (param4 == null) ? null : param4.with(TemporalAdjusters.lastDayOfMonth()); ",
        "left1 == null || right2 == null",
        "left1",
        "right2.atStartOfDay(ZoneId.of('Z'))"
      ),
      Shape(
        "LAST_DAY(COALESCE(d, d2))",
        false,
        "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
        "def param2 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
        "def param3 = (param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : (param2 != null ? param2.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null)); " +
        "def param4 = (param3 == null) ? null : param3.with(TemporalAdjusters.lastDayOfMonth()); " +
        "def param5 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
        "def left1 = (param4 != null ? param4.atStartOfDay(ZoneId.of('Z')) : null); ",
        "left1 == null || param5 == null",
        "left1",
        "param5"
      )
    )
    for (shape <- shapes; op <- Seq("<", "<=", ">", ">=", "=", "<>")) {
      val comparison =
        if (shape.tsOnTheLeft) s"ts $op ${shape.conditional}" else s"${shape.conditional} $op ts"
      withClue(comparison) {
        val script = whereOf(s"SELECT name FROM t WHERE $comparison")
        script should not include "doc['ts'].value.toLocalDate()"
        script shouldBe
        s"${shape.prologue}(${shape.guard} ? false : ((${compared(op, shape.left, shape.right)})))"
      }
    }
  }

  "a TIMESTAMP compared with a DATE literal" should "compare instants, the DATE taken at its UTC midnight" in {
    whereOf("SELECT name FROM t WHERE ts > CAST('2024-01-31' AS DATE)") shouldBe
    "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "def param2 = LocalDate.parse((\"2024-01-31\").replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); " +
    "param1 == null ? false : (param1.isAfter(param2.atStartOfDay(ZoneId.of('Z'))))"
  }

  "a comparison in a transform" should "read the types its operands render there" in {
    // a transform renders a conditional's arms as their doc values, so neither side is converted
    whereOf("SELECT name FROM t WHERE COALESCE(d2, d) > ts", PainlessContextType.Transform) shouldBe
    "def param1 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
    "def param2 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param3 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "def left1 = (param1 != null ? param1 : param2); " +
    "(left1 == null || param3 == null ? false : ((left1.isAfter(param3))))"
    whereOf(
      "SELECT name FROM t WHERE ts > CASE WHEN n > 0 THEN d ELSE d2 END",
      PainlessContextType.Transform
    ) shouldBe
    "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "def param2 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param3 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param4 = param2 == null ? false : (param2 > 0); " +
    "def param5 = (doc['d2'].size() == 0 ? null : doc['d2'].value); " +
    "def left1 = param1; " +
    "def right2 = param4 ? param3 : param5; " +
    "(left1 == null || right2 == null ? false : ((left1.isAfter(right2))))"
  }
}
