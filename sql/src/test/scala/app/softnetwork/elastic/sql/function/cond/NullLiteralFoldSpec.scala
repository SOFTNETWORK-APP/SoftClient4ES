package app.softnetwork.elastic.sql.function.cond

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A NULL literal argument of NULLIF, COALESCE, GREATEST or LEAST is FOLDED when a query renders,
  * never rendered into Painless: `NULLIF(x, NULL)` is `x`, `NULLIF(NULL, x)` is NULL, COALESCE
  * drops a NULL arm, GREATEST / LEAST skip a NULL (DuckDB and PostgreSQL do), and with no argument
  * left each is NULL.
  *
  * 🔴 Rendered, the literal was a comparison of NULL constants -- `null != null &&
  * param1.isEqual(null)`, `Math.min(null, param1)` -- which Painless refuses to compile, so the
  * statement failed; or a method on a NULL (`(param2 != null ? param2 : null).plus(…)`), a
  * NullPointerException on every document missing the column. The folded node keeps the other
  * argument's type and its NULL guard. The statement and its `.sql` are unchanged, and so are an
  * ingest processor's bytes.
  */
class NullLiteralFoldSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("n", SQLTypes.Int),
      Column("dn", SQLTypes.Any)
    )
  )

  private def single(sql: String): SingleSearch = Parser(sql) match {
    case Right(ss: SingleSearch) => ss
    case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def scriptOf(
    sql: String,
    contextType: PainlessContextType = PainlessContextType.Query
  ): String = {
    val ctx = PainlessContext(contextType)
    val body = single(sql).update(Some(schema)).select.fields.head.painless(Some(ctx))
    s"$ctx$body"
  }

  private val nullConstant = """\bnull\s*[!=]=\s*null\b|\bnull\.\w+\(|\.\w+\(null\)|\(null,""".r

  "NULLIF(x, NULL)" should "be x" in {
    scriptOf("SELECT NULLIF(d, NULL) FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param2 = (param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
    "param2"
  }

  "NULLIF(NULL, x)" should "be NULL" in {
    // bound, like every NULLIF answer: a consumer's guard tests the name
    scriptOf("SELECT NULLIF(NULL, d) FROM t") should endWith("def param2 = null; param2")
  }

  "COALESCE" should "drop a NULL literal arm" in {
    scriptOf("SELECT COALESCE(d, NULL) FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "(param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null)"
    scriptOf("SELECT COALESCE(NULL, NULL) FROM t") shouldBe "null"
  }

  "LEAST / GREATEST" should "skip a NULL literal argument" in {
    scriptOf("SELECT LEAST(NULL, n) FROM t") shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "(param1 == null ? param1 : (def) ((int) param1))"
    scriptOf("SELECT GREATEST(NULL, d) FROM t") should not include "Math.max(null"
  }

  /** 🔴 The consumer guards what the fold hands it -- a DATE_ADD receiver: the folded COALESCE is
    * the cast's bound parameter, and the interval landed outside the guard inside it.
    */
  "a consumer of a folded node" should "keep the operand's NULL guard" in {
    scriptOf("SELECT DATE_ADD(COALESCE(CAST(d AS DATE), NULL), INTERVAL 1 DAY) FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param2 = (param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
    "(param2 == null) ? null : (def)(param2.plus(1, ChronoUnit.DAYS))"
    // a column's own parameter carries the method inside its guard
    scriptOf("SELECT DATETIME_ADD(LEAST(NULL, dn), INTERVAL 1 HOUR) FROM t") shouldBe
    "def param1 = (doc['dn'].size() == 0 ? null : doc['dn'].value.plus(1, ChronoUnit.HOURS)); " +
    "param1"
    scriptOf("SELECT CAST(NULLIF(ts, NULL) AS TIME) FROM t") shouldBe
    "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "(param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalTime() : null)"
  }

  it should "render no NULL constant over the whole family" in {
    Seq(
      "NULLIF(d, NULL)",
      "NULLIF(NULL, d)",
      "NULLIF(ts, NULL)",
      "COALESCE(d, NULL)",
      "COALESCE(NULL, ts)",
      "COALESCE(NULL, NULL)",
      "LEAST(NULL, dn)",
      "LEAST(NULL, n)",
      "GREATEST(n, NULL)",
      "GREATEST(NULL, d)",
      "DATE_ADD(COALESCE(CAST(d AS DATE), NULL), INTERVAL 1 DAY)",
      "DATETIME_ADD(COALESCE(dn, NULL), INTERVAL 1 HOUR)",
      "EXTRACT(MONTH FROM NULLIF(ts, NULL))",
      "CAST(NULLIF(NULL, ts) AS TIMESTAMP)"
    ).foreach { expr =>
      val emitted = scriptOf(s"SELECT $expr FROM t")
      withClue(s"[$expr] $emitted ")(nullConstant.findFirstIn(emitted) shouldBe None)
    }
  }

  "a cast over a CASE" should "convert every branch under one NULL guard" in {
    // the CASE answers NULL where its arm does: `String.valueOf` around it answered the text
    // "null", and a conversion appended to `p ? a : b` converted the ELSE branch only
    val varchar = scriptOf(
      "SELECT CAST(CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE CAST(ts AS DATE) END AS VARCHAR) FROM t"
    )
    varchar should endWith("(param6 != null ? String.valueOf(param6) : null)")
    varchar should not include "String.valueOf(param3 ?"
    val timestamp = scriptOf(
      "SELECT CAST(CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE CAST(ts AS DATE) END AS TIMESTAMP) FROM t"
    )
    timestamp should endWith("(param6 != null ? param6.atStartOfDay(ZoneId.of('Z')) : null)")
  }

  "the statement" should "keep its SQL" in {
    val sql = "SELECT NULLIF(d, NULL) AS a, COALESCE(d, NULL) AS b, LEAST(NULL, n) AS c FROM t"
    single(sql).sql shouldBe single(single(sql).sql).sql
    single(sql).sql should include("NULLIF(d, NULL)")
  }

  /** 🔴 This pins the RECORDED processor twin, a DEFECT, not a behaviour: the script below cannot
    * compile (`null != null`, `null.length()`, `Instant.ofEpochMilli(null)`), as on `main`. The
    * fold is a query's only (ruling 3: an ingest processor keeps its bytes, so `ALTER` does not
    * churn a pipeline); the pin proves the bytes did not move, and moves the day the processor twin
    * is repaired.
    */
  "an ingest processor" should "keep the recorded twin's bytes, a script that cannot compile" in {
    scriptOf("SELECT NULLIF(d, NULL) FROM t", PainlessContextType.Processor) shouldBe
    "def param1 = ctx.d; " +
    "def param2 = param1 == null || (null != null && (param1 == null ? null : param1 instanceof String ? (param1.length() > 10 ? ZonedDateTime.parse((param1).replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z'))) : LocalDate.parse((param1).replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")).atStartOfDay(ZoneId.of('Z'))) : param1 instanceof Number ? Instant.ofEpochMilli(param1).atZone(ZoneId.of('Z')) : (param1 == null ? null : param1 instanceof LocalDate ? param1.atStartOfDay(ZoneId.of('Z')) : param1 instanceof LocalDateTime ? param1.atZone(ZoneId.of('Z')) : param1.withZoneSameInstant(ZoneId.of('Z')))).isEqual((null == null ? null : null instanceof String ? (null.length() > 10 ? ZonedDateTime.parse((null).replace(\" \", \"T\"), DateTimeFormatter.ISO_DATE_TIME.withZone(ZoneId.of('Z'))) : LocalDate.parse((null).replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")).atStartOfDay(ZoneId.of('Z'))) : null instanceof Number ? Instant.ofEpochMilli(null).atZone(ZoneId.of('Z')) : (null == null ? null : null instanceof LocalDate ? null.atStartOfDay(ZoneId.of('Z')) : null instanceof LocalDateTime ? null.atZone(ZoneId.of('Z')) : null.withZoneSameInstant(ZoneId.of('Z')))))) ? null : param1; " +
    "param2"
  }
}
