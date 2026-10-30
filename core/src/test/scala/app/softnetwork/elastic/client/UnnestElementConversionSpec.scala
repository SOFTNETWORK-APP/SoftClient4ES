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

import app.softnetwork.elastic.client.SetOperationValues.DateReading
import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{SingleSearch, UnnestScope}
import app.softnetwork.elastic.sql.schema.Schema
import app.softnetwork.elastic.sql.time.TimeUnit
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap
import scala.util.{Failure, Success, Try}

/** A conversion of a bare UNNEST element column -- `CAST`, `TRY_CAST` / `SAFE_CAST`, `::`,
  * `CONVERT`, `DATE_TRUNC` -- is answered by the READER: the row explosion places the element's
  * value, and `ElementConversions` converts it as DuckDB 1.5.5 converts it. Every expected value
  * and class below is DuckDB's, MEASURED (`SELECT CAST(<v> AS <t>), typeof(..)`).
  *
  * The same statements are executed on every major by `UnnestFunctionRefusalExecutionSpec`.
  */
class UnnestElementConversionSpec extends AnyFlatSpec with Matchers {

  private val schema: Schema =
    Index(
      "t",
      """{"t": {"aliases": {}, "mappings": {"properties": {"id": {"type": "keyword"},
        | "m": {"type": "integer"},
        | "items": {"type": "nested", "properties": {"sku": {"type": "keyword"},
        | "qty": {"type": "integer"}, "x": {"type": "double"}, "at": {"type": "date"}}}}},
        | "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
    ).schema

  private val u = "FROM t AS t JOIN UNNEST(t.items) AS i"

  private def statement(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(schema))
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  /** Two parents, three elements, every value the fixture of the release gate holds a kind of. */
  private val response =
    """{"hits": {"total": {"value": 2, "relation": "eq"}, "hits": [
      | {"_id": "a1", "_source": {"id": "a1"}, "inner_hits": {"i": {"hits": {"hits": [
      |   {"_source": {"sku": "s1", "qty": 3, "x": 2.5, "at": "2024-01-03"}},
      |   {"_source": {"sku": " 7 ", "qty": 900, "x": -2.5, "at": "2024-05-17T10:15:42.123Z"}}]}}}},
      | {"_id": "a2", "_source": {"id": "a2"}, "inner_hits": {"i": {"hits": {"hits": [
      |   {"_source": {"sku": null, "qty": null, "x": null, "at": null}}]}}}}
      |]}}""".stripMargin

  /** The rows of `sql` read from [[response]] as core reads a statement's rows (`SchemaColumns.of`,
    * the Elasticsearch 8 date dialect), or the failure.
    */
  private def rows(sql: String): Try[Seq[ListMap[String, Any]]] = {
    val single = statement(sql)
    single.validateResolved() match {
      case Left(reason) => Failure(new IllegalArgumentException(reason))
      case Right(_) =>
        implicit val context: ConversionContext =
          SchemaColumns.of(single, NativeContext, Some("8.18.3"))
        ElasticConversion.parseResponse(
          response,
          single.fieldAliases,
          ListMap.empty,
          Seq("id", "v"),
          UnnestScope.rowExplosion(single)
        )
    }
  }

  /** (value, class) of column `v`, row by row. */
  private def answer(sql: String): Seq[(Any, String)] =
    rows(sql) match {
      case Success(rs) =>
        rs.map(r => r("v")).map(v => v -> Option(v).fold("null")(_.getClass.getName))
      case Failure(e) => fail(s"[$sql] failed: ${e.getMessage}")
    }

  private def failure(sql: String): String =
    rows(sql) match {
      case Success(rs) => fail(s"[$sql] answered ${rs.map(_("v"))}")
      case Failure(e)  => e.getMessage
    }

  private val S = "java.lang.String"
  private val day: LocalDate = LocalDate.of(2024, 1, 3)
  private val ts: ZonedDateTime =
    ZonedDateTime.of(2024, 5, 17, 10, 15, 42, 123000000, ZoneOffset.UTC)

  "a text conversion of an element" should "answer DuckDB's text of its value, as a String" in {
    answer(s"SELECT t.id, CAST(i.qty AS VARCHAR) AS v $u") shouldBe
    Seq("3" -> S, "900" -> S, (null, "null"))
    answer(s"SELECT t.id, i.sku::CHAR AS v $u LIMIT 100") shouldBe
    Seq("s1" -> S, " 7 " -> S, (null, "null"))
    answer(s"SELECT t.id, CONVERT(TEXT, i.x) AS v $u") shouldBe
    Seq("2.5" -> S, "-2.5" -> S, (null, "null"))
    // an Elasticsearch `date` no `_meta` declares is a TIMESTAMP (`IndexField.undeclaredType`):
    // its UTC instant, as DuckDB spells a TIMESTAMP -- a date-only value read as every UNNEST
    // column is read, a DATE
    answer(s"SELECT t.id, CONVERT(VARCHAR, i.at) AS v $u") shouldBe
    Seq("2024-01-03" -> S, "2024-05-17 10:15:42.123" -> S, (null, "null"))
  }

  "a numeric conversion of an element" should "answer DuckDB's number, in the target's class" in {
    answer(s"SELECT t.id, SAFE_CAST(i.qty AS SHORT) AS v $u") shouldBe
    Seq(3.toShort -> "java.lang.Short", 900.toShort -> "java.lang.Short", (null, "null"))
    answer(s"SELECT t.id, TRY_CAST(i.qty AS SIGNED) AS v $u LIMIT 100") shouldBe
    Seq(3L -> "java.lang.Long", 900L -> "java.lang.Long", (null, "null"))
    answer(s"SELECT t.id, i.qty::DOUBLE AS v $u") shouldBe
    Seq(3.0 -> "java.lang.Double", 900.0 -> "java.lang.Double", (null, "null"))
    // DECIMAL is core's DOUBLE
    answer(s"SELECT t.id, CONVERT(i.qty, DECIMAL) AS v $u") shouldBe
    Seq(3.0 -> "java.lang.Double", 900.0 -> "java.lang.Double", (null, "null"))
    answer(s"SELECT t.id, CAST(i.qty AS REAL) AS v $u") shouldBe
    Seq(3.0f -> "java.lang.Float", 900.0f -> "java.lang.Float", (null, "null"))
    // a DOUBLE rounded half to even, as DuckDB rounds it
    answer(s"SELECT t.id, CAST(i.x AS INT) AS v $u") shouldBe
    Seq(2 -> "java.lang.Integer", -2 -> "java.lang.Integer", (null, "null"))
    // a text read as a number, trimmed; a text that is no number is NULL under TRY_CAST
    answer(s"SELECT t.id, TRY_CAST(i.sku AS INTEGER) AS v $u") shouldBe
    Seq((null, "null"), 7 -> "java.lang.Integer", (null, "null"))
    // out of the target's range: NULL under TRY_CAST
    answer(s"SELECT t.id, TRY_CAST(i.qty AS TINYINT) AS v $u") shouldBe
    Seq(3.toByte -> "java.lang.Byte", (null, "null"), (null, "null"))
  }

  it should "fail the statement where DuckDB's CAST fails on a value" in {
    failure(s"SELECT t.id, CAST(i.sku AS INTEGER) AS v $u") should include(
      "Could not convert string 's1' to INT32"
    )
    failure(s"SELECT t.id, CAST(i.qty AS BYTE) AS v $u") should include(
      "with value 900 can't be cast because the value is out of range for the destination type INT8"
    )
  }

  "a date conversion of an element" should "answer its day or its instant, exactly as its format reads it" in {
    val midnight = day.atStartOfDay(ZoneOffset.UTC)
    answer(s"SELECT t.id, i.at::DATE AS v $u LIMIT 100") shouldBe
    Seq(
      day                       -> "java.time.LocalDate",
      LocalDate.of(2024, 5, 17) -> "java.time.LocalDate",
      (null, "null")
    )
    answer(s"SELECT t.id, CAST(i.at AS TIMESTAMP) AS v $u") shouldBe
    Seq(midnight -> "java.time.ZonedDateTime", ts -> "java.time.ZonedDateTime", (null, "null"))
    // a TIMESTAMP truncates to a TIMESTAMP, in UTC; a date-only value to a day or a longer unit
    // stays a DATE
    answer(s"SELECT t.id, DATE_TRUNC(MONTH, i.at) AS v $u") shouldBe
    Seq(
      LocalDate.of(2024, 1, 1)                              -> "java.time.LocalDate",
      LocalDate.of(2024, 5, 1).atStartOfDay(ZoneOffset.UTC) -> "java.time.ZonedDateTime",
      (null, "null")
    )
    answer(s"SELECT t.id, DATE_TRUNC(HOUR, i.at) AS v $u") shouldBe
    Seq(
      midnight                                                   -> "java.time.ZonedDateTime",
      ZonedDateTime.of(2024, 5, 17, 10, 0, 0, 0, ZoneOffset.UTC) -> "java.time.ZonedDateTime",
      (null, "null")
    )
  }

  it should "refuse, before any row, a conversion DuckDB does not implement -- and answer NULL for it under TRY_CAST" in {
    failure(s"SELECT t.id, CAST(i.at AS INTEGER) AS v $u") should startWith(
      "Conversion Error: Unimplemented type for cast (TIMESTAMP -> INT) in CAST(i.at AS INT)"
    )
    // a number to a DATE: the conversion's own type rule refuses it first, as for any column
    failure(s"SELECT t.id, CAST(i.qty AS DATE) AS v $u") should include("cannot cast INT to DATE")
    failure(s"SELECT t.id, CAST(i.qty AS TIMESTAMP) AS v $u") should startWith(
      "Conversion Error: Unimplemented type for cast (INT -> TIMESTAMP)"
    )
    answer(s"SELECT t.id, TRY_CAST(i.at AS BIGINT) AS v $u") shouldBe
    Seq((null, "null"), (null, "null"), (null, "null"))
    // a text read as a date: DuckDB's text rules are not reproduced, so never answered
    failure(s"SELECT t.id, TRY_CAST(i.sku AS DATE) AS v $u") should include(
      "the way DuckDB reads it is not reproduced"
    )
    // a DATE_TRUNC of no date (refused by this rule or by the type rule that sees it first)
    failure(s"SELECT t.id, DATE_TRUNC(YEAR, i.qty) AS v $u") should not be empty
  }

  "the reader" should "convert ONE value: an empty array is NULL, several values fail" in {
    val read = ElementConversions.converter(
      SQLTypes.Int,
      Some(SQLTypes.Varchar),
      None,
      safe = false,
      "CAST(i.qty AS VARCHAR)",
      "items.qty",
      None,
      DateReading.unknown
    )
    read(null) shouldBe (null: Any)
    read(Seq.empty) shouldBe (null: Any)
    read(Seq(3)) shouldBe "3"
    the[IllegalArgumentException] thrownBy read(Seq(1, 2)) should have message
    "Conversion Error: CAST(i.qty AS VARCHAR) converts ONE value, and an element of items.qty holds several"
  }

  it should "read a value of no known type by its class" in {
    def any(target: Option[SQLType], unit: Option[TimeUnit] = None) =
      ElementConversions.converter(
        SQLTypes.Any,
        target,
        unit,
        safe = true,
        "e",
        "items.e",
        None,
        DateReading.unknown
      )
    any(Some(SQLTypes.Varchar))(Integer.valueOf(3)) shouldBe "3"
    any(Some(SQLTypes.Varchar))(day) shouldBe "2024-01-03"
    any(Some(SQLTypes.Varchar))(ts) shouldBe "2024-05-17 10:15:42.123"
    any(Some(SQLTypes.Double))(" 1e2 ") shouldBe 100.0
    any(Some(SQLTypes.Double))("inf") shouldBe Double.PositiveInfinity
    any(Some(SQLTypes.BigInt))("-2.5") shouldBe -3L
    any(Some(SQLTypes.BigInt))(day) shouldBe (null: Any)
    any(None, Some(TimeUnit.WEEKS))(ts) shouldBe ZonedDateTime.of(
      2024,
      5,
      13,
      0,
      0,
      0,
      0,
      ZoneOffset.UTC
    )
    any(None, Some(TimeUnit.QUARTERS))(ts) shouldBe ZonedDateTime.of(
      2024,
      4,
      1,
      0,
      0,
      0,
      0,
      ZoneOffset.UTC
    )
    any(None, Some(TimeUnit.MINUTES))(ts) shouldBe ZonedDateTime.of(
      2024,
      5,
      17,
      10,
      15,
      0,
      0,
      ZoneOffset.UTC
    )
    any(None, Some(TimeUnit.YEARS))(day) shouldBe LocalDate.of(2024, 1, 1)
  }
}
