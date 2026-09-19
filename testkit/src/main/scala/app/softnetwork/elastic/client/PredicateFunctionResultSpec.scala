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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.{Sink, Source}
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.query.SelectStatement
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions

/** Issue #367 — a WHERE predicate over a function-wrapped column must filter on the FUNCTION's
  * value, not on the raw column.
  *
  * The predicate rendering took a shortcut: it compared the Painless PARAMETER, which holds the
  * column's doc-value, and that is the whole operand only when the function chain folds into the
  * parameter as a method (`YEAR(d)` becomes `def param1 = doc['d']….get(ChronoField.YEAR)`).
  * `WEEKDAY(d)` cannot fold — its Painless is arithmetic — so the emitted script was `param1 ==
  * null ? false : (param1 == 0)`, a `ZonedDateTime` against an `int`, and the function had
  * VANISHED.
  *
  * 🔴 Why this spec asserts ROW SETS and not script text. Comparing a `ZonedDateTime` to an `int`
  * is not an error in Painless, it is FALSE — so `=` and `IN` answered ZERO rows with HTTP 200 and
  * `NOT IN` answered EVERY row. A script pin cannot see that; only the rows can. The emissions
  * themselves are pinned in `sql`'s `PredicateTransformSurvivalSpec`, which is blind to whether
  * Elasticsearch accepts or agrees with them.
  *
  * 🔴 Seven consecutive days, Monday through Sunday, is the fixture the defect needs: the answer to
  * `WEEKDAY(d) = 0` is exactly one document, so a predicate that matched everything and a predicate
  * that matched nothing are both visible, and every weekday ordinal is covered once. Both venues
  * are exercised: the client API (`search`) and `GatewayApi.run` (JDBC / REPL / Flight SQL).
  */
trait PredicateFunctionResultSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val context: ConversionContext = NativeContext

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val index = "predicate_function_result"

  /** id -> (date, its weekday with Monday = 0, a numeric string in `num`). 2025-01-06 is a Monday,
    * so the ordinals run 0..6 over the seven documents.
    */
  private val fixture: Seq[(String, String, Int, String)] = Seq(
    ("d6", "2025-01-06T00:00:00Z", 0, "6"),
    ("d7", "2025-01-07T00:00:00Z", 1, "7"),
    ("d8", "2025-01-08T00:00:00Z", 2, "8"),
    ("d9", "2025-01-09T00:00:00Z", 3, "9"),
    ("d10", "2025-01-10T00:00:00Z", 4, "10"),
    ("d11", "2025-01-11T00:00:00Z", 5, "11"),
    ("d12", "2025-01-12T00:00:00Z", 6, "12")
  )

  /** The same day as `d`, as a STRING, so `DATE_PARSE` has a keyword column to parse. */
  private def isoOf(d: String): String = d.take(10)

  private val allIds: Set[String] = fixture.map(_._1).toSet

  private def idsWithWeekday(p: Int => Boolean): Set[String] =
    fixture.collect { case (id, _, w, _) if p(w) => id }.toSet

  override def beforeAll(): Unit = {
    super.beforeAll()

    val mapping =
      """{
        |  "properties": {
        |    "id":  { "type": "keyword" },
        |    "d":   { "type": "date"    },
        |    "ts":  { "type": "date"    },
        |    "iso": { "type": "keyword" },
        |    "num": { "type": "keyword" },
        |    "num2":{ "type": "keyword" }
        |  }
        |}""".stripMargin

    client
      .createIndex(index, settings = """{"number_of_shards": 1, "number_of_replicas": 0}""")
      .get shouldBe true
    client.setMapping(index, mapping).get shouldBe true

    // `ts` is the SAME instant as `d` for every document except d6, whose `ts` is a year later, and
    // `num2` equals `num` except for d7 — so a predicate with a function on BOTH sides has something
    // to separate.
    val docs = fixture.map { case (id, d, _, num) =>
      val ts = if (id == "d6") d.replace("2025-", "2026-") else d
      val num2 = if (id == "d7") "99" else num
      s"""{"id":"$id","d":"$d","ts":"$ts","iso":"${isoOf(d)}","num":"$num","num2":"$num2"}"""
    }.toList

    implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = index, logEvery = 10)

    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)

    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) => // ok
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing into $index failed: ${error.message}")
    }

    client.refresh(index)
  }

  override def afterAll(): Unit = {
    client.deleteIndex(index)
    system.terminate()
    super.afterAll()
  }

  /** The cluster's major version, for the one family this spec must not assert on ES 6.
    *
    * 🔴 `DATE_FORMAT` / `DATETIME_FORMAT` cannot work on 6.8 in ANY venue, and issue #367 does not
    * change that: ES 6.8 hands Painless a `JodaCompatibleZonedDateTime` for a `date` field while
    * `DateTimeFormatter.format` takes a `java.time.TemporalAccessor`, and the operand of these two
    * functions is passed as an ARGUMENT, so it never reaches the UTC normalisation
    * (`painlessUtcZonedDateTime`) that the CHAINED date functions get. MEASURED on real 6.8.23 over
    * `d = 2025-01-06`: the shipped script fails the shard both as `SELECT DATE_FORMAT(d,
    * 'yyyy-MM-dd')` and as `WHERE DATE_FORMAT(d, 'yyyy') = '2025'`, while the same script over
    * `…toInstant().atZone(ZoneId.of('Z'))` answers `2025-01-06`.
    *
    * That one-line normalisation is NOT part of this fix: it moves the emission of the whole
    * date-format family in every venue, including bytes another story pins as "byte-identical to
    * before the fix" (`StandaloneCallAssemblySpec`). Recorded, reported, and left to its own change
    * — this spec states the boundary instead of hiding it.
    */
  private lazy val esMajor: Int =
    client.version match {
      case ElasticSuccess(v) => v.takeWhile(_ != '.').toInt
      case ElasticFailure(_) => 0
    }

  private def rowsOf(sql: String): Seq[ListMap[String, Any]] =
    client.search(SelectStatement(sql)) match {
      case ElasticSuccess(response) => response.results
      case ElasticFailure(error)    => fail(s"Query failed: ${error.message}\n$sql")
    }

  /** The set of ids a WHERE clause selects, through the client venue. */
  private def selected(where: String): Set[String] =
    rowsOf(s"SELECT id FROM $index WHERE $where")
      .map(r => r.getOrElse("id", fail(s"no id column in $r")).toString)
      .toSet

  /** The same, through `GatewayApi.run`. */
  private def gatewaySelected(where: String): Set[String] = {
    val sql = s"SELECT id FROM $index WHERE $where"
    val rows = Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(QueryRows(rows, _))           => rows
      case ElasticSuccess(QueryStructured(response, _)) => response.results
      case ElasticSuccess(QueryStream(stream, _)) =>
        Await.result(stream.map(_._1).runWith(Sink.seq), 60.seconds)
      case ElasticSuccess(other) => fail(s"Unexpected result: $other\n$sql")
      case ElasticFailure(error) => fail(s"Query failed: ${error.message}\n$sql")
    }
    rows.map(r => r.getOrElse("id", fail(s"no id column in $r")).toString).toSet
  }

  // -- WEEKDAY: the shape the issue measured ------------------------------------------------------

  "WEEKDAY in an equality predicate" should "select the documents whose weekday matches" in {
    // MEASURED before the fix, on ES 8.18: the empty set, with HTTP 200.
    selected("WEEKDAY(d) = 0") shouldBe Set("d6")
    selected("WEEKDAY(d) = 3") shouldBe Set("d9")
    selected("WEEKDAY(d) = 6") shouldBe Set("d12")
  }

  it should "select nothing for an ordinal no document carries" in {
    // The complement of the test above: it fails if the predicate matches everything, which is the
    // other half of the defect (`NOT IN` used to answer every row).
    selected("WEEKDAY(d) = 7") shouldBe Set.empty[String]
  }

  "WEEKDAY in an IN predicate" should "select exactly the listed ordinals" in {
    selected("WEEKDAY(d) IN (5, 6)") shouldBe Set("d11", "d12")
  }

  it should "select the complement under NOT IN" in {
    // MEASURED before the fix: ALL SEVEN rows — the signature of a predicate that matched no
    // document at all, inverted.
    selected("WEEKDAY(d) NOT IN (0, 1, 2, 3, 4)") shouldBe Set("d11", "d12")
  }

  "WEEKDAY in an ordering predicate" should "execute and compare the weekday" in {
    // MEASURED before the fix: `search_phase_execution_exception`, all shards failed.
    selected("WEEKDAY(d) > 4") shouldBe idsWithWeekday(_ > 4)
    selected("WEEKDAY(d) <= 1") shouldBe idsWithWeekday(_ <= 1)
    selected("WEEKDAY(d) BETWEEN 1 AND 2") shouldBe idsWithWeekday(w => w >= 1 && w <= 2)
  }

  "DAYOFWEEK" should "behave exactly as its WEEKDAY spelling" in {
    selected("DAYOFWEEK(d) = 0") shouldBe Set("d6")
  }

  // -- DATE_FORMAT / DATETIME_FORMAT --------------------------------------------------------------

  "DATE_FORMAT in a predicate" should "compare the FORMATTED value" in {
    assume(esMajor >= 7, "DATE_FORMAT cannot run on ES 6.8 in any venue — see `esMajor`")
    // MEASURED before the fix: `=` failed the shard (a `ZonedDateTime.compareTo(String)`), while
    // `IN` returned zero rows silently.
    selected("DATE_FORMAT(d, 'yyyy') = '2025'") shouldBe allIds
    selected("DATE_FORMAT(d, 'yyyy') = '2024'") shouldBe Set.empty[String]
    selected("DATE_FORMAT(d, 'MM-dd') = '01-06'") shouldBe Set("d6")
    selected("DATE_FORMAT(d, 'yyyy') IN ('2025')") shouldBe allIds
    selected("DATE_FORMAT(d, 'MM-dd') IN ('01-11', '01-12')") shouldBe Set("d11", "d12")
  }

  "DATETIME_FORMAT in a predicate" should "compare the FORMATTED value too" in {
    assume(esMajor >= 7, "DATETIME_FORMAT cannot run on ES 6.8 in any venue — see `esMajor`")
    selected("DATETIME_FORMAT(d, 'yyyy') = '2025'") shouldBe allIds
    selected("DATETIME_FORMAT(d, 'MM-dd') = '01-09'") shouldBe Set("d9")
  }

  // -- LAST_DAY ----------------------------------------------------------------------------------

  "LAST_DAY in a predicate" should "compare the adjusted date, not the column" in {
    // Every fixture date is in January 2025, so the last day of its month is the same for all seven
    // — and a predicate that had dropped `LAST_DAY` would compare the RAW dates instead and match
    // only the document dated the 31st, i.e. none of them.
    selected("LAST_DAY(d) = CAST('2025-01-31' AS DATE)") shouldBe allIds
    selected("LAST_DAY(d) = CAST('2025-01-06' AS DATE)") shouldBe Set.empty[String]
  }

  // -- CAST, which the issue does not mention ----------------------------------------------------

  "a CAST in a predicate" should "compare the CONVERTED value" in {
    // Found by the census this fix is guarded by, not by the issue: `CAST`/`CONVERT`/`TRY_CAST`/
    // `SAFE_CAST` are in exactly the same population — the conversion was dropped and the raw
    // keyword was compared to the number.
    selected("CAST(num AS INT) = 9") shouldBe Set("d9")
    selected("CAST(num AS INT) > 10") shouldBe Set("d11", "d12")
    selected("TRY_CAST(num AS INT) = 6") shouldBe Set("d6")
  }

  it should "keep working as a PROJECTION, which is where the safe cast is a whole script" in {
    // A safe cast is a `try`/`catch` STATEMENT, legal only as the tail of a script, so making it
    // usable in a predicate meant hoisting those statements into the prologue — which changes the
    // PROJECTION's emission too. This asserts the hoisted form still computes the same values.
    val got = rowsOf(s"SELECT id, TRY_CAST(num AS INT) AS v FROM $index").map { r =>
      r.getOrElse("id", fail(s"no id column in $r")).toString -> (r.getOrElse("v", "") match {
        case s: Seq[_]                  => s.headOption.map(_.toString).getOrElse("null")
        case a: java.util.Collection[_] => a.toArray.headOption.map(_.toString).getOrElse("null")
        case null                       => "null"
        case other                      => other.toString
      })
    }.toMap
    got shouldBe fixture.map { case (id, _, _, num) => id -> num }.toMap
  }

  // -- the shapes review added, each one a defect it found ---------------------------------------

  /** 🔴 A function compared with a date COLUMN, not a literal. An earlier draft of this fix coerced
    * the operand DATE -> TIMESTAMP here, which renders `.atStartOfDay(…)` — a `LocalDate` method —
    * onto the `ZonedDateTime` a date doc-value really is, and turned a WORKING query into `dynamic
    * method [java.time.ZonedDateTime, atStartOfDay/1] not found`. Found by review; this row is why
    * it cannot come back silently.
    */
  "a temporal function compared with a date column" should "execute on ES 7 and later" in {
    // 🔴 ES 6.8 cannot run ANY temporal expression compared with a raw date COLUMN, and issue #367
    // neither causes nor changes that. The right-hand side is rendered as the bare doc-value, which on
    // 6.8 is an `org.elasticsearch.script.JodaCompatibleZonedDateTime`, and the comparison the engine
    // emits takes a `java.time` one. MEASURED on real 6.8.23, verbatim:
    // `class_cast_exception: Cannot cast org.elasticsearch.script.JodaCompatibleZonedDateTime to
    // java.time.chrono.ChronoZonedDateTime` at `left1.isEqual(param1)`. On main the same statement
    // failed too — the dropped transform left `param1.isEqual(param1)`, whose ARGUMENT is the same
    // un-normalised value.
    //
    // It is the same missing UTC normalisation (`painlessUtcZonedDateTime`) that keeps `DATE_FORMAT`
    // off 6.8 (see `esMajor`), at a second site: the right-hand operand. One family, tracked as issue
    // #371 — the emission a fix moves is pinned by another story.
    assume(esMajor >= 7, "a raw date column reaches the comparison un-normalised on ES 6.8")
    // Every fixture date is in January 2025, so no document's own date IS the last day of its month,
    // and every one of them is before it.
    selected("LAST_DAY(d) = d") shouldBe Set.empty[String]
    selected("LAST_DAY(d) > d") shouldBe allIds

    // ⚠️ NOT asserted, and the reason is a defect this fix does NOT own: where the function folds into
    // the parameter as a METHOD (`DATE_TRUNC`, `YEAR`, …) and the same column is also the right-hand
    // side, both sides resolve to that ONE parameter — `PainlessParam` identity is the doc-value
    // string, while the methods live on the parameter object — so `DATE_TRUNC(d, MONTH) < d` emits
    // `left1.isBefore(param1)` with `left1 = param1` and the truncation folded INTO `param1`:
    // always false, silently. MEASURED on ES 8.18 before AND after this fix (`Set()` either way), and
    // independently predicted by review for `DATE_ADD(d, INTERVAL 1 DAY) = d`, which is always true
    // for the same reason. Repairing it means changing parameter identity, which reaches every
    // emission that reuses one — its own change, with its own evidence: issue #370. When that lands,
    // this assertion turns red and the comment says why.
    selected("DATE_TRUNC(d, MONTH) < d") shouldBe Set
      .empty[String] // records the defect, not the truth
  }

  /** 🔴 `List.contains` is Java `equals`, which is FALSE across boxed numeric types — so an `IN`
    * over anything but an `Integer` answered ZERO rows, silently. MEASURED:
    * `[9,12].contains(<Long>)` and `<Double>` both matched nothing on ES 8.18 while `== 9` matched
    * in all three. `EPOCHDAY` renders `getLong`, so it was in that population too.
    */
  "IN over a numeric operand" should "match across boxed types" in {
    selected("CAST(num AS BIGINT) IN (9, 12)") shouldBe Set("d9", "d12")
    selected("CAST(num AS DOUBLE) IN (9, 12)") shouldBe Set("d9", "d12")
    selected("CAST(num AS BIGINT) NOT IN (9, 12)") shouldBe (allIds -- Set("d9", "d12"))
    selected("EPOCHDAY(d) IN (20094)") shouldBe Set("d6") // 2025-01-06
    selected("WEEKDAY(d) IN (0)") shouldBe Set("d6")
  }

  it should "keep matching a list of strings" in {
    selected("UPPER(num) IN ('9', '12')") shouldBe Set("d9", "d12")
    selected("num IN ('9', '12')") shouldBe Set("d9", "d12")
  }

  /** 🔴 `COALESCE` opened one parenthesis per argument and closed exactly one, so at THREE
    * arguments the script did not parse — `no_viable_alt_exception` on ES 8.18, in every venue
    * including a plain `SELECT`, and nothing had ever executed it. At ONE argument the same fold
    * emitted `String.valueOf( : param1))`. Both were invisible until a predicate stopped dropping
    * the operand's transform.
    */
  "COALESCE at every arity" should "execute" in {
    selected("COALESCE(num) = '6'") shouldBe Set("d6")
    selected("COALESCE(num, id) = '6'") shouldBe Set("d6")
    selected("COALESCE(num, id, 'z') = '6'") shouldBe Set("d6")
    selected("COALESCE(num, id, 'z') = 'z'") shouldBe Set.empty[String]
  }

  "ISNULL and ISNOTNULL" should "test the field, not compare its value" in {
    selected("ISNULL(num) = true") shouldBe Set.empty[String]
    selected("ISNOTNULL(num) = true") shouldBe allIds
  }

  "DATE_PARSE over a keyword column" should "compare the parsed date" in {
    // The issue named `DATE_PARSE` and could not measure it; `iso` carries the same dates as strings.
    selected("DATE_PARSE(iso, 'yyyy-MM-dd') = CAST('2025-01-06' AS DATE)") shouldBe Set("d6")
    selected("DATE_PARSE(iso, 'yyyy-MM-dd') > CAST('2025-01-10' AS DATE)") shouldBe Set(
      "d11",
      "d12"
    )
  }

  /** 🔴 A function on BOTH sides — the second half of the same defect, and the half I first left
    * out. `check` picked the comparison spelling from the RIGHT operand's `out`, which for a
    * function-wrapped identifier is its COLUMN's type. MEASURED on ES 8.18 before the repair:
    * `YEAR(d) = YEAR(ts)` → `dynamic method [java.lang.Integer, isEqual/1] not found`;
    * `DATE_FORMAT(d,'yyyy') = DATE_FORMAT(ts,'yyyy')` → the same over `String`; `CAST(num AS INT) =
    * CAST(num2 AS INT)` → the left stringified, ZERO rows for matching documents; `LAST_DAY(d) =
    * LAST_DAY(ts)` → `Cannot cast java.time.ZonedDateTime to java.time.chrono.ChronoLocalDate`,
    * because the narrowing fired on one side only.
    */
  "a function on both sides of a comparison" should "compare like with like" in {
    // `ts` differs from `d` only for d6 (a year later).
    selected("YEAR(d) = YEAR(ts)") shouldBe (allIds - "d6")
    selected("YEAR(d) < YEAR(ts)") shouldBe Set("d6")
    if (esMajor >= 7)
      selected("DATE_FORMAT(d, 'yyyy') = DATE_FORMAT(ts, 'yyyy')") shouldBe (allIds - "d6")
    selected("WEEKDAY(d) = WEEKDAY(ts)") shouldBe (allIds - "d6")
    // `num2` differs from `num` only for d7.
    selected("CAST(num AS INT) = CAST(num2 AS INT)") shouldBe (allIds - "d7")
    selected("CAST(num AS BIGINT) < CAST(num2 AS BIGINT)") shouldBe Set("d7")
  }

  it should "compare two temporal chains without narrowing either one" in {
    assume(esMajor >= 7, "a raw date column reaches the comparison un-normalised on ES 6.8")
    selected("LAST_DAY(d) = LAST_DAY(ts)") shouldBe (allIds - "d6")
  }

  // -- the controls the issue names --------------------------------------------------------------

  "YEAR in a predicate" should "stay unchanged (the control)" in {
    // `YEAR` folds into the parameter as a method, which is why it was correct all along. It must
    // still be correct, and — asserted by the unit census — emit the same bytes as before.
    selected("YEAR(d) = 2025") shouldBe allIds
    selected("YEAR(d) = 2024") shouldBe Set.empty[String]
    selected("MONTH(d) = 1") shouldBe allIds
    selected("DAY(d) = 6") shouldBe Set("d6")
  }

  "a plain column predicate" should "stay unchanged (the second control)" in {
    selected("id = 'd6'") shouldBe Set("d6")
    selected("num = '12'") shouldBe Set("d12")
  }

  // -- the gateway venue -------------------------------------------------------------------------

  "the gateway venue" should "agree with the client venue" in {
    gatewaySelected("WEEKDAY(d) = 0") shouldBe Set("d6")
    gatewaySelected("WEEKDAY(d) IN (5, 6)") shouldBe Set("d11", "d12")
    if (esMajor >= 7) gatewaySelected("DATE_FORMAT(d, 'MM-dd') = '01-06'") shouldBe Set("d6")
    gatewaySelected("CAST(num AS INT) = 9") shouldBe Set("d9")
    gatewaySelected("YEAR(d) = 2025") shouldBe allIds
  }

  // -- composition -------------------------------------------------------------------------------

  "a repaired predicate" should "compose with AND, OR and NOT" in {
    selected("WEEKDAY(d) = 0 AND YEAR(d) = 2025") shouldBe Set("d6")
    selected("WEEKDAY(d) = 0 OR WEEKDAY(d) = 6") shouldBe Set("d6", "d12")
    selected("NOT WEEKDAY(d) = 0") shouldBe (allIds - "d6")
    if (esMajor >= 7)
      selected("WEEKDAY(d) = 0 AND DATE_FORMAT(d, 'yyyy') = '2025'") shouldBe Set("d6")
  }
}
