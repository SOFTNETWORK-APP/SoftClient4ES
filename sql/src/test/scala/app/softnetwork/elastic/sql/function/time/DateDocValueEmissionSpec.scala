package app.softnetwork.elastic.sql.function.time

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{CreateTable, SingleSearch}
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.time.TimeField
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.temporal.{ChronoField, IsoFields, TemporalField}
import java.time.{LocalDate, LocalDateTime, ZonedDateTime}
import scala.reflect.runtime.{universe => ru}

/** Issue #368 -- `LAST_DAY` and `EPOCHDAY` called a method that does not exist for the type a
  * `date` doc-value actually has, so neither had ever worked on any supported Elasticsearch.
  *
  * `doc['<date field>'].value` is a `java.time.ZonedDateTime`. `LAST_DAY` emitted
  * `<arg>.withDayOfMonth(<arg>.lengthOfMonth())` and `lengthOfMonth()` is declared on `LocalDate`
  * only; `EPOCHDAY` emitted `.get(ChronoField.EPOCH_DAY)` and `get` returns an `int`, which
  * `java.time` refuses for a field whose range does not fit one. Both failed the whole query as an
  * opaque `search_phase_execution_exception`.
  *
  * 🔴 Inserting a `.toLocalDate()` -- the fix the issue proposed -- is NOT enough, and this spec
  * exists partly to keep that from being tried again. The operand reaching `toPainlessCall` is a
  * `ZonedDateTime` for a bare column but ALREADY a `LocalDate` wherever a cast or a `SQLTypes.Date`
  * coercion ran first, and `LocalDate` has no `toLocalDate()`. MEASURED on real Elasticsearch
  * 8.18.3: `LAST_DAY(CURRENT_DATE)`, `LAST_DAY(CURRENT_TIMESTAMP)`, `LAST_DAY(CAST(d AS DATE))` and
  * the whole no-schema path (which is what the two `SQLQuerySpec` bridge fixtures pin) all reach it
  * with a `LocalDate` and all WORKED before the fix -- so `.toLocalDate()` would have swapped one
  * broken receiver for another rather than fixing anything.
  *
  * 🔴 Every emission pinned below was EXECUTED as the `source` of a `script_fields` entry over a
  * real index holding `d = 2025-02-10`, on real Elasticsearch **6.8.23, 7.17.29, 8.18.3 and 9.0.3**
  * -- "this is valid Painless" is a claim only Elasticsearch settles, and a byte pin proves the
  * bytes did not move, not that they RUN (story 21.8's lesson, which is how these two survived at
  * least four releases while being documented in five places). Results, identical on all four:
  * {{{
  * shape                            before                  after
  * LAST_DAY(d)                      shard failure           2025-02-28T00:00:00.000Z
  * EPOCHDAY(d)                      shard failure           20129
  * DAY(LAST_DAY(d))                 shard failure           28
  * EPOCHDAY(CAST(d AS DATE))        shard failure           20129   <- not in the issue
  * EPOCHDAY(CURRENT_DATE)           shard failure           20453   <- not in the issue
  * LAST_DAY(CURRENT_DATE)           2025-12-31              2025-12-31   (unchanged)
  * LAST_DAY(CURRENT_TIMESTAMP)      2025-12-31              2025-12-31   (unchanged)
  * LAST_DAY(DATE_TRUNC(d, MONTH))   2025-02-28              2025-02-28   (unchanged)
  * YEAR(d)                          2025                    2025         (control, unchanged)
  * }}}
  * ⚠️ `LAST_DAY(CAST(d AS DATE))` over a SCHEMA-CARRYING parse still fails, before AND after, with
  * the same root cause both times -- `dynamic method [java.time.LocalDate, toInstant/0] not found`.
  * That is a DOUBLE `SQLTypes.Date` -> `SQLTypes.Date` coercion in the conversion layer, a defect
  * of a different kind that this issue neither caused nor covers; it is recorded here so the next
  * reader does not mistake it for fallout.
  */
class DateDocValueEmissionSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("name", SQLTypes.Keyword),
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp)
    )
  )

  /** A schema-CARRYING parse: without it every identifier's type stays `Any`, a coercion arm that
    * does not run in production inserts itself, and a broken emission looks fixed (#306 / 21.5).
    */
  private def scriptOf(sql: String): String =
    Parser(sql) match {
      case Right(ss: SingleSearch) =>
        val ctx = PainlessContext(PainlessContextType.Query)
        val body = ss.update(Some(schema)).select.fields.head.painless(Some(ctx))
        s"$ctx$body"
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  // -- LAST_DAY -----------------------------------------------------------------------------------

  "LAST_DAY over a date column" should "adjust the receiver instead of calling a LocalDate-only method" in {
    scriptOf("SELECT LAST_DAY(d) FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "(param1 == null) ? null : param1.with(TemporalAdjusters.lastDayOfMonth())"
  }

  it should "emit the same adjuster for a timestamp column" in {
    scriptOf("SELECT LAST_DAY(ts) FROM t") shouldBe
    "def param1 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "(param1 == null) ? null : param1.with(TemporalAdjusters.lastDayOfMonth())"
  }

  it should "keep working where the operand is already a LocalDate" in {
    // The shapes `.toLocalDate()` would have broken. Both executed on all four majors.
    scriptOf("SELECT LAST_DAY(CURRENT_DATE) FROM t") should endWith(
      "param1.with(TemporalAdjusters.lastDayOfMonth())"
    )
    scriptOf("SELECT LAST_DAY(CURRENT_TIMESTAMP) FROM t") should endWith(
      "param1.toLocalDate().with(TemporalAdjusters.lastDayOfMonth())"
    )
  }

  /** 🔴 The MECHANISM, not the bytes: the reason `lengthOfMonth()` shipped broken is that nobody
    * asked whether the receiver has it. The pipeline hands this call a `ZonedDateTime`, a
    * `LocalDate` or a `LocalDateTime` depending on which coercions ran, so the method LAST_DAY
    * emits must resolve on ALL THREE -- asked of the JDK, which is the only authority on that.
    * Restoring `withDayOfMonth`/`lengthOfMonth` reddens this test on `ZonedDateTime`.
    */
  it should "emit only methods that exist on every receiver type the pipeline can produce" in {
    val emitted = scriptOf("SELECT LAST_DAY(d) FROM t")

    // 🔴 EVERY call on EVERY bound operand, not the first one on the first. The defect was in the
    // INNER call -- `param1.withDayOfMonth(param1.lengthOfMonth())` -- whose outer `withDayOfMonth`
    // is perfectly fine on all three receivers. A `findFirstMatchIn` here PASSED with the broken
    // emission restored, which is why this is spelled out: a mechanism test that cannot see the
    // defect it exists for is a proxy, not a guard.
    //
    // ⚠️ What it does NOT see, stated rather than implied: the method NAME is checked, not the
    // signature, and a call on a static receiver (`TemporalAdjusters.lastDayOfMonth()`) or on a
    // literal is invisible to it. Those are settled by `DateFunctionExecutionSpec`, which executes
    // the emission on a real Elasticsearch. This check is the cheap one that fails at the right
    // place with the right message.
    val methods = """param\d+\.(\w+)\(""".r.findAllMatchIn(emitted).map(_.group(1)).toList
    withClue(s"no method call on the operand in: $emitted") { methods should not be empty }

    Seq(classOf[ZonedDateTime], classOf[LocalDate], classOf[LocalDateTime]).foreach { receiver =>
      methods.foreach { method =>
        withClue(s"$method is not declared on ${receiver.getName} (emitted: $emitted): ") {
          receiver.getMethods.exists(_.getName == method) shouldBe true
        }
      }
    }
  }

  // -- EPOCHDAY, and the accessor rule behind it ---------------------------------------------------

  "EPOCHDAY" should "read a field whose range does not fit an int with getLong" in {
    scriptOf("SELECT EPOCHDAY(d) FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : " +
    "doc['d'].value.toInstant().atZone(ZoneId.of('Z')).getLong(ChronoField.EPOCH_DAY)); param1"
  }

  it should "leave every other field on get" in {
    scriptOf("SELECT YEAR(d) FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : " +
    "doc['d'].value.toInstant().atZone(ZoneId.of('Z')).get(ChronoField.YEAR)); param1"
  }

  // -- the INGEST (processor) context ------------------------------------------------------------

  /** The processor emission for the same two functions, through the production seam
    * (`CreateTable(...).schema.columns` -- `Table.update()` is what resolves them; the raw parsed
    * column list carries the UNRESOLVED processors and pinning it looks like an inconsistent fix).
    */
  private def processorSource(sql: String, column: String): String =
    Parser(sql) match {
      case Right(ct: CreateTable) =>
        ct.schema.columns
          .find(_.name == column)
          .flatMap(_.script)
          .map(_.source)
          .getOrElse(fail(s"no script processor for [$column] in [$sql]"))
      case other => fail(s"[$sql] expected a CreateTable, got $other")
    }

  /** 🔴 Both functions were broken in an INGEST script too, and this is the venue where it hurts
    * most: a processor carries `ignore_failure: true`, so the throw is SWALLOWED and the computed
    * column is simply ABSENT -- no error, anywhere. MEASURED by deploying these exact sources as
    * real ingest pipelines on ES 6.8.23 and 8.18.3 over `{"created":"2025-02-10"}`:
    * {{{
    * LAST_DAY  before: c <<absent>>    after: c = 2025-02-28T00:00:00.000Z
    * EPOCHDAY  before: c <<absent>>    after: c = 20129
    * }}}
    */
  "the ingest processor emission" should "carry the same two fixes" in {
    processorSource(
      "CREATE TABLE t (created DATE, c DATE SCRIPT AS (LAST_DAY(created)))",
      "c"
    ) should endWith(
      "ctx.c = (param1 == null) ? null : param2.with(TemporalAdjusters.lastDayOfMonth())"
    )

    processorSource(
      "CREATE TABLE t (created DATE, c BIGINT SCRIPT AS (EPOCHDAY(created)))",
      "c"
    ) should include(".getLong(ChronoField.EPOCH_DAY)")
  }

  /** 🔴 The mechanism again, and the reason `TimeField.accessor` lives on the FIELD: `get` is
    * refused for exactly those fields whose value RANGE does not fit an `int`
    * (`TemporalAccessor.get`'s contract, which `LocalDate.get0` implements by throwing
    * `UnsupportedTemporalTypeException`). So the JDK -- not a list kept here -- decides which
    * accessor each field takes, and the next field with that property is caught on arrival.
    *
    * The population is the SEALED `TimeField` hierarchy walked to its leaves, so the compiler
    * guarantees it is complete; the count is asserted so a silent empty walk cannot pass.
    */
  "every TimeField" should "declare the accessor its JDK range requires" in {
    val mirror = ru.runtimeMirror(getClass.getClassLoader)

    def leaves(sym: ru.ClassSymbol): Set[ru.ClassSymbol] = {
      val subs = sym.knownDirectSubclasses.map(_.asClass)
      if (subs.isEmpty) Set(sym) else subs.flatMap(leaves)
    }

    val fields: List[TimeField] =
      leaves(ru.typeOf[TimeField].typeSymbol.asClass).toList.map { c =>
        withClue(s"$c is not a case object: ") { c.isModuleClass shouldBe true }
        mirror.reflectModule(c.module.asModule).instance.asInstanceOf[TimeField]
      }

    fields should have size 15

    val jdkField: String => TemporalField = {
      case "QUARTER_OF_YEAR"         => IsoFields.QUARTER_OF_YEAR
      case "WEEK_OF_WEEK_BASED_YEAR" => IsoFields.WEEK_OF_WEEK_BASED_YEAR
      case name                      => ChronoField.valueOf(name)
    }

    fields.foreach { f =>
      val expected = if (jdkField(f.timeField).range().isIntValue) ".get" else ".getLong"
      withClue(s"${f.sql} (ChronoField.${f.timeField}): ") { f.accessor shouldBe expected }
    }

    // ...and the rule is not vacuous: it partitions the set rather than agreeing with everything.
    fields.count(_.accessor == ".getLong") shouldBe 1
  }
}
