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

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{MultiSearch, TemporalLiterals}
import app.softnetwork.elastic.sql.query.MultiSearch.{SetOperationType, StructField}
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{
  Instant,
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  ZoneOffset,
  ZonedDateTime
}
import scala.collection.immutable.ListMap

/** The values of a `UNION ALL` column are brought to the ONE type the column has -- DuckDB 1.5.5's,
  * or `VARCHAR` where DuckDB gives none -- as the class JDBC and Arrow read the column's type from:
  * a `Double` for a `DOUBLE`, a `Long` for a `BIGINT`, a `String` for a `VARCHAR`, a
  * `ZonedDateTime` for a `TIMESTAMP`. Only the legs of a column whose branches disagree pay for it.
  */
class SetOperationValuesSpec extends AnyFlatSpec with Matchers {

  import SetOperationValues.converter

  private def kind(v: Any): Class[_] = v.getClass

  "A value reaching a DOUBLE column" should "be a Double, whatever number it was" in {
    val f = converter(SQLTypes.Int, SQLTypes.Double)
    f(Integer.valueOf(3)) shouldBe 3.0
    kind(f(Integer.valueOf(3))) shouldBe kind(java.lang.Double.valueOf(0))
    kind(f(java.lang.Long.valueOf(3000000000L))) shouldBe kind(java.lang.Double.valueOf(0))
    // Elasticsearch's one-element `script_fields` array is ONE value
    f(List(Integer.valueOf(2))) shouldBe 2.0
    kind(f(List(Integer.valueOf(2)))) shouldBe kind(java.lang.Double.valueOf(0))
    // a BOOLEAN is 1 / 0 beside a number (DuckDB)
    converter(SQLTypes.Boolean, SQLTypes.Double)(java.lang.Boolean.TRUE) shouldBe 1.0
    f(null) shouldBe (null: Any)
    f(List(null)) shouldBe (null: Any)
  }

  "A value reaching a BIGINT column" should "be a Long, also when its JSON fitted an int" in {
    val f = converter(SQLTypes.BigInt, SQLTypes.BigInt)
    kind(f(Integer.valueOf(3))) shouldBe kind(java.lang.Long.valueOf(0))
    kind(converter(SQLTypes.Int, SQLTypes.BigInt)(Integer.valueOf(-2))) shouldBe
    kind(java.lang.Long.valueOf(0))
  }

  "A value reaching a REAL column" should "be a Float" in {
    kind(converter(SQLTypes.Int, SQLTypes.Real)(Integer.valueOf(3))) shouldBe
    kind(java.lang.Float.valueOf(0))
    converter(SQLTypes.Real, SQLTypes.Real)(java.lang.Double.valueOf(0.5)) shouldBe 0.5f
  }

  "A DATE reaching a TIMESTAMP column" should "be the instant its day starts at, in UTC" in {
    val f = converter(SQLTypes.Date, SQLTypes.Timestamp)
    f(LocalDate.parse("2024-01-31")) shouldBe ZonedDateTime.parse("2024-01-31T00:00:00Z")
    f(List(LocalDate.parse("2024-01-31"))) shouldBe ZonedDateTime.parse("2024-01-31T00:00:00Z")
    // a date written as epoch milliseconds
    f(java.lang.Long.valueOf(1706659200000L)) shouldBe
    ZonedDateTime.parse("2024-01-31T00:00:00Z").withZoneSameInstant(ZoneOffset.UTC)
    // a TIMESTAMP keeps its instant
    val ts = ZonedDateTime.parse("2024-01-31T10:30:00Z")
    converter(SQLTypes.Timestamp, SQLTypes.Timestamp)(ts) shouldBe ts
  }

  "A value reaching a VARCHAR column" should "be spelled as DuckDB spells its type" in {
    converter(SQLTypes.Int, SQLTypes.Varchar)(Integer.valueOf(3)) shouldBe "3"
    // an INT computed by an aggregation comes back as a Double from Elasticsearch
    converter(SQLTypes.Int, SQLTypes.Varchar)(java.lang.Double.valueOf(10.0)) shouldBe "10"
    converter(SQLTypes.BigInt, SQLTypes.Varchar)(List(java.lang.Long.valueOf(3000000000L))) shouldBe
    "3000000000"
    converter(SQLTypes.Date, SQLTypes.Varchar)(LocalDate.parse("2024-01-31")) shouldBe "2024-01-31"
    // a DATE column holding a date-time is its UTC day
    converter(SQLTypes.Date, SQLTypes.Varchar)(ZonedDateTime.parse("2024-01-31T22:30:00Z")) shouldBe
    "2024-01-31"
    converter(SQLTypes.Boolean, SQLTypes.Varchar)(java.lang.Boolean.FALSE) shouldBe "false"
    converter(SQLTypes.Varchar, SQLTypes.Varchar)(List("a")) shouldBe "a"
    converter(SQLTypes.Null, SQLTypes.Varchar)(List(null)) shouldBe (null: Any)
    // Elasticsearch's empty array is no value
    converter(SQLTypes.Varchar, SQLTypes.Varchar)(Nil) shouldBe (null: Any)
  }

  "A text value core re-read as a temporal (issue #418)" should "keep its seconds as text" in {
    // a keyword that LOOKS like a date or a time comes back from the response re-read as one;
    // `toString` would drop zero seconds (`10:30`, `2024-01-31T10:30Z`), the ISO formatters do not
    val f = converter(SQLTypes.Varchar, SQLTypes.Varchar)
    f(LocalTime.parse("10:30:00")) shouldBe "10:30:00"
    f(LocalTime.parse("10:30:00.5")) shouldBe "10:30:00.5"
    f(ZonedDateTime.parse("2024-01-31T10:30:00Z")) shouldBe "2024-01-31T10:30:00Z"
    f(ZonedDateTime.parse("2024-01-31T10:30:00+02:00")) shouldBe "2024-01-31T10:30:00+02:00"
    f(LocalDateTime.parse("2024-01-31T10:30:00")) shouldBe "2024-01-31T10:30:00"
    f(List(LocalDate.parse("2024-01-31"))) shouldBe "2024-01-31"
    // the same text reaching a VARBINARY column
    converter(SQLTypes.Varchar, SQLTypes.VarBinary)(LocalTime.parse("10:30:00"))
      .asInstanceOf[Array[Byte]]
      .toSeq shouldBe "10:30:00".getBytes(java.nio.charset.StandardCharsets.UTF_8).toSeq
  }

  "A value with several values in a converting column" should "fail the statement" in {
    // a multi-valued field or an object has no ONE value of the column's type to convert; the
    // message is in DuckDB's words for the same column (MEASURED on DuckDB 1.5.5)
    def failure(f: Any => Any, value: Any): String =
      intercept[IllegalArgumentException](f(value)).getMessage
    val tags = List("a", "b")
    failure(converter(SQLTypes.Varchar, SQLTypes.Varchar), tags) shouldBe
    "Conversion Error: Unimplemented type for cast (VARCHAR[] -> VARCHAR)"
    failure(converter(SQLTypes.Int, SQLTypes.Double), List(1, 2)) shouldBe
    "Conversion Error: Unimplemented type for cast (INTEGER[] -> DOUBLE)"
    failure(converter(SQLTypes.Varchar, SQLTypes.VarBinary), Map("a" -> 1)) shouldBe
    "Conversion Error: Unimplemented type for cast (VARCHAR[] -> BLOB)"
    failure(converter(SQLTypes.Timestamp, SQLTypes.Varchar), java.util.Arrays.asList(1, 2)) should
    startWith("Conversion Error: Unimplemented type for cast")
  }

  // Every spelling below is DuckDB 1.5.5's `CAST(<value> AS VARCHAR)`, MEASURED value by value.

  "A TIMESTAMP reaching a VARCHAR column" should "be spelled as DuckDB spells it, in UTC" in {
    val f = converter(SQLTypes.Timestamp, SQLTypes.Varchar)
    f(ZonedDateTime.parse("2024-01-31T10:30:00Z")) shouldBe "2024-01-31 10:30:00"
    // fractional seconds: only the digits there are
    f(ZonedDateTime.parse("2023-12-25T23:59:59.999Z")) shouldBe "2023-12-25 23:59:59.999"
    f(ZonedDateTime.parse("2024-01-31T10:30:00.5Z")) shouldBe "2024-01-31 10:30:00.5"
    f(ZonedDateTime.parse("2024-01-31T10:30:00.000001Z")) shouldBe "2024-01-31 10:30:00.000001"
    f(ZonedDateTime.parse("2024-01-31T10:30:00.123456789Z")) shouldBe
    "2024-01-31 10:30:00.123456789"
    // a date-only timestamp is its midnight
    f(LocalDate.parse("2024-01-31")) shouldBe "2024-01-31 00:00:00"
    // before the epoch
    f(ZonedDateTime.parse("1969-12-31T23:59:59.999Z")) shouldBe "1969-12-31 23:59:59.999"
    // another offset is read in UTC, as every date rule of the engine reads it
    f(ZonedDateTime.parse("2024-01-31T12:30:00+02:00")) shouldBe "2024-01-31 10:30:00"
    f(LocalDateTime.parse("2024-01-31T10:30:00")) shouldBe "2024-01-31 10:30:00"
    // epoch milliseconds, and a computed branch's one-element array
    f(java.lang.Long.valueOf(1706697000000L)) shouldBe "2024-01-31 10:30:00"
    f(List(ZonedDateTime.parse("2024-01-31T10:30:00Z"))) shouldBe "2024-01-31 10:30:00"
    // a year before 1, and one past 9999
    f(LocalDateTime.of(-43, 3, 15, 12, 0)) shouldBe "0044-03-15 (BC) 12:00:00"
    f(LocalDateTime.of(0, 12, 31, 0, 0)) shouldBe "0001-12-31 (BC) 00:00:00"
    f(LocalDateTime.of(10000, 1, 1, 0, 0)) shouldBe "10000-01-01 00:00:00"
    f(LocalDateTime.of(999, 6, 1, 1, 2, 3)) shouldBe "0999-06-01 01:02:03"
    f(null) shouldBe (null: Any)
  }

  "A DATE reaching a VARCHAR column" should "carry DuckDB's year spelling" in {
    val f = converter(SQLTypes.Date, SQLTypes.Varchar)
    f(LocalDate.of(-43, 3, 15)) shouldBe "0044-03-15 (BC)"
    f(LocalDate.of(1, 1, 1)) shouldBe "0001-01-01"
    f(LocalDate.of(10000, 1, 1)) shouldBe "10000-01-01"
  }

  "A DOUBLE reaching a VARCHAR column" should "be spelled as DuckDB spells it" in {
    val f = converter(SQLTypes.Double, SQLTypes.Varchar)
    f(java.lang.Double.valueOf(2.5e11)) shouldBe "250000000000.0"
    f(java.lang.Double.valueOf(1e-5)) shouldBe "1e-05"
    f(java.lang.Double.valueOf(1e16)) shouldBe "1e+16"
    f(java.lang.Double.valueOf(-2.5e11)) shouldBe "-250000000000.0"
    f(java.lang.Double.valueOf(-1e-5)) shouldBe "-1e-05"
    f(java.lang.Double.valueOf(-0.0)) shouldBe "-0.0"
    f(java.lang.Double.valueOf(0.1 + 0.2)) shouldBe "0.30000000000000004"
    // DuckDB's own algorithm misses the shortest digits here; its answer is the reference
    f(java.lang.Double.valueOf(7.168e25)) shouldBe "7.1680000000000004e+25"
    // a whole number written in a DOUBLE column's JSON is still a DOUBLE
    f(Integer.valueOf(5)) shouldBe "5.0"
    f(List(java.lang.Double.valueOf(1.5))) shouldBe "1.5"
    // a number of unknown width is a DOUBLE to DuckDB
    converter(SQLTypes.Numeric, SQLTypes.Varchar)(java.lang.Long.valueOf(15)) shouldBe "15.0"
  }

  "A REAL reaching a VARCHAR column" should "be spelled from the float's value, as DuckDB does" in {
    val f = converter(SQLTypes.Real, SQLTypes.Varchar)
    // the JSON of a REAL column is read as a Double; its value is the float's
    f(java.lang.Double.valueOf(0.1)) shouldBe "0.1"
    f(java.lang.Float.valueOf(-0.75f)) shouldBe "-0.75"
    f(java.lang.Double.valueOf(2.5e11)) shouldBe "250000000000.0"
    f(java.lang.Float.valueOf(4073260.75f)) shouldBe "4073260.75"
    f(Integer.valueOf(16777216)) shouldBe "16777216.0"
  }

  "A TIME reaching a VARCHAR column" should "be spelled as DuckDB spells it" in {
    val f = converter(SQLTypes.Time, SQLTypes.Varchar)
    f(LocalTime.parse("10:30")) shouldBe "10:30:00"
    f(LocalTime.parse("10:30:00.5")) shouldBe "10:30:00.5"
    f(LocalTime.parse("10:30:00.120")) shouldBe "10:30:00.12"
    f(LocalTime.parse("23:59:59.999999")) shouldBe "23:59:59.999999"
    f(LocalTime.MIDNIGHT) shouldBe "00:00:00"
    f(ZonedDateTime.parse("2024-01-31T10:30:00Z")) shouldBe "10:30:00"
  }

  "A value reaching a VARCHAR column DuckDB gives no type to" should "be spelled as text" in {
    // the lead's rule of 2026-10-07: a VARBINARY beside a number, a TIME beside a date make a
    // VARCHAR column
    converter(SQLTypes.VarBinary, SQLTypes.Varchar)("YWI=") shouldBe "YWI="
    // bytes are their base64 text, the text Elasticsearch holds for a `binary` field
    converter(SQLTypes.VarBinary, SQLTypes.Varchar)(utf8("ab")) shouldBe "YWI="
    converter(SQLTypes.Time, SQLTypes.Varchar)(LocalTime.parse("10:30")) shouldBe "10:30:00"
  }

  "A GEO_POINT in a set operation" should "be a {lat, lon}, whatever way Elasticsearch stored it" in {
    // every form Elasticsearch 8.18.3 accepts, MEASURED: `_source` keeps it as it was written
    def point(lat: Double, lon: Double) = ListMap("lat" -> lat, "lon" -> lon)
    val read = SetOperationValues.geoPoint(_: Any, "k")
    read(Map("lat" -> 48.85, "lon" -> 2.35)) shouldBe point(48.85, 2.35)
    read(Map("lon" -> "2.35", "lat" -> "48.85")) shouldBe point(48.85, 2.35)
    read("48.85,2.35") shouldBe point(48.85, 2.35)
    read("48.85, 2.35") shouldBe point(48.85, 2.35)
    read(List(2.35, 48.85)) shouldBe point(48.85, 2.35)
    read(List(2.35, 48.85, 10.0)) shouldBe point(48.85, 2.35)
    read("POINT (2.35 48.85)") shouldBe point(48.85, 2.35)
    read("POINT(2.35 48.85)") shouldBe point(48.85, 2.35)
    read(Map("type" -> "Point", "coordinates" -> List(2.35, 48.85))) shouldBe point(48.85, 2.35)
    // a geohash is the south-west corner of its cell, as Elasticsearch reads it (MEASURED)
    read("u09tv") shouldBe point(48.8232421875, 2.3291015625)
    read("u09tvw0f6szy") shouldBe point(48.85661389678717, 2.352221868932247)
    // one point wrapped in Elasticsearch's array, no point
    read(List(Map("lat" -> 1.0, "lon" -> 2.0))) shouldBe point(1.0, 2.0)
    read(Nil) shouldBe (null: Any)
    read(null) shouldBe (null: Any)
  }

  it should "fail the statement, clearly, for several points or for no point" in {
    def failure(value: Any): String =
      intercept[IllegalArgumentException](SetOperationValues.geoPoint(value, "k")).getMessage
    failure(List(List(2.35, 48.85), List(1.0, 2.0))) shouldBe
    "Conversion Error: Unimplemented type for cast (STRUCT(lat DOUBLE, lon DOUBLE)[] -> " +
    "STRUCT(lat DOUBLE, lon DOUBLE)) when casting from source column k"
    failure("nowhere!") shouldBe
    "Conversion Error: Could not read 'nowhere!' as a GEO_POINT when casting from source column " +
    "k: a point is an object {lat, lon}, a text 'lat,lon', an array [lon, lat], a geohash or a " +
    "WKT POINT (lon lat)"
    failure(Map("x" -> 1)) should startWith("Conversion Error: Could not read 'Map(x -> 1)'")
  }

  private def struct(fields: StructField*): SetOperationType =
    SetOperationType(SQLTypes.Struct, fields)

  "A STRUCT or a GEO_POINT reaching a merged STRUCT column" should "hold the column's fields, in order" in {
    // DuckDB 1.5.5's merge, MEASURED: STRUCT(lat, lon) beside STRUCT(a, b) is STRUCT(lat, lon, a, b)
    val obj = Seq(StructField("a", SQLTypes.Int), StructField("b", SQLTypes.Varchar))
    val merged = struct(StructField.GeoPoint ++ obj: _*)
    val branches = Seq(Some(SetOperationType.GeoPoint), Some(struct(obj: _*)))
    val fromPoint = SetOperationValues.structConverter(merged, branches, 0, "k")
    val fromObject = SetOperationValues.structConverter(merged, branches, 1, "k")
    val p = fromPoint("48.85,2.35").asInstanceOf[ListMap[String, Any]]
    p shouldBe ListMap("lat" -> 48.85, "lon" -> 2.35, "a" -> null, "b" -> null)
    p.keys.toSeq shouldBe Seq("lat", "lon", "a", "b")
    val o = fromObject(Map("b" -> "x", "a" -> 1)).asInstanceOf[ListMap[String, Any]]
    o shouldBe ListMap("lat" -> null, "lon" -> null, "a" -> 1, "b" -> "x")
    o.keys.toSeq shouldBe Seq("lat", "lon", "a", "b")
    fromObject(null) shouldBe (null: Any)
    // one object wrapped in Elasticsearch's array
    fromObject(List(Map("a" -> 2, "b" -> "y"))) shouldBe
    ListMap("lat" -> null, "lon" -> null, "a" -> 2, "b" -> "y")
  }

  it should "convert a field whose type the merge widened, match fields whatever their case" in {
    // an INTEGER beside a VARCHAR is a VARCHAR (DuckDB answers '1'); a point inside an object
    // is a {lat, lon} too
    val merged = struct(
      StructField("a", SQLTypes.Varchar),
      StructField("p", SetOperationType.GeoPoint)
    )
    val branches = Seq(
      Some(struct(StructField("A", SQLTypes.Int), StructField("p", SetOperationType.GeoPoint))),
      Some(struct(StructField("a", SQLTypes.Varchar)))
    )
    SetOperationValues.structConverter(merged, branches, 0, "k")(
      Map("A" -> 1, "p" -> "48.85,2.35")
    ) shouldBe ListMap("a" -> "1", "p" -> ListMap("lat" -> 48.85, "lon" -> 2.35))
  }

  it should "fail the statement for several objects, or a value that is no object" in {
    val obj = struct(StructField("a", SQLTypes.Int))
    val f = SetOperationValues.structConverter(
      struct(StructField.GeoPoint :+ StructField("a", SQLTypes.Int): _*),
      Seq(Some(SetOperationType.GeoPoint), Some(obj)),
      1,
      "k"
    )
    intercept[IllegalArgumentException](f(List(Map("a" -> 1), Map("a" -> 2)))).getMessage shouldBe
    "Conversion Error: Unimplemented type for cast (STRUCT(a INTEGER)[] -> STRUCT(lat DOUBLE, " +
    "lon DOUBLE, a INTEGER)) when casting from source column k"
    intercept[IllegalArgumentException](f("x")).getMessage shouldBe
    "Conversion Error: Could not read 'x' as a STRUCT when casting from source column k"
  }

  // ── one class per converting position, whatever form core read the value in ───────────────

  /** A value and its class, arrays by their bytes. */
  private def shown(v: Any): (Any, Class[_]) =
    v match {
      case null           => (null, classOf[Null])
      case b: Array[Byte] => (b.toSeq, classOf[Array[Byte]])
      case other          => (other, other.getClass)
    }

  "Every form core reads a value in" should "reach the position's one class" in {
    // the forms ElasticConversion.jsonNodeToAny makes of a JSON value -- an int, a long, a double
    // or a decimal; a date-only, zone-less, zoned or offset text; epoch milliseconds -- and an
    // Instant; each also in Elasticsearch's one-element array
    val zoned = ZonedDateTime.parse("2024-01-31T10:30:00Z")
    val midnight = ZonedDateTime.parse("2024-01-31T00:00:00Z")
    val cases: Seq[(SQLType, SQLType, Any, Any)] = Seq(
      (SQLTypes.BigInt, SQLTypes.BigInt, Integer.valueOf(1), java.lang.Long.valueOf(1L)),
      (SQLTypes.BigInt, SQLTypes.BigInt, java.lang.Long.valueOf(3000000000L), 3000000000L),
      (SQLTypes.BigInt, SQLTypes.BigInt, java.lang.Double.valueOf(4.0), java.lang.Long.valueOf(4L)),
      (SQLTypes.Int, SQLTypes.BigInt, Integer.valueOf(-2), java.lang.Long.valueOf(-2L)),
      (SQLTypes.Boolean, SQLTypes.BigInt, java.lang.Boolean.TRUE, java.lang.Long.valueOf(1L)),
      (SQLTypes.Double, SQLTypes.Double, Integer.valueOf(2), java.lang.Double.valueOf(2.0)),
      (SQLTypes.Double, SQLTypes.Double, BigDecimal("2.5").bigDecimal, 2.5),
      (SQLTypes.Int, SQLTypes.Double, java.lang.Long.valueOf(3L), java.lang.Double.valueOf(3.0)),
      (SQLTypes.Timestamp, SQLTypes.Timestamp, LocalDate.parse("2024-01-31"), midnight),
      (SQLTypes.Timestamp, SQLTypes.Timestamp, LocalDateTime.parse("2024-01-31T10:30:00"), zoned),
      (SQLTypes.Timestamp, SQLTypes.Timestamp, zoned, zoned),
      (
        SQLTypes.Timestamp,
        SQLTypes.Timestamp,
        OffsetDateTime.parse("2024-01-31T12:30:00+02:00"),
        ZonedDateTime.parse("2024-01-31T12:30:00+02:00")
      ),
      (SQLTypes.Timestamp, SQLTypes.Timestamp, Instant.parse("2024-01-31T10:30:00Z"), zoned),
      (SQLTypes.Timestamp, SQLTypes.Timestamp, java.lang.Long.valueOf(1706697000000L), zoned),
      (SQLTypes.Date, SQLTypes.Timestamp, LocalDate.parse("2024-01-31"), midnight),
      (SQLTypes.VarBinary, SQLTypes.VarBinary, "YWI=", utf8("YWI=")),
      (SQLTypes.VarBinary, SQLTypes.VarBinary, utf8("ab"), utf8("ab")),
      (SQLTypes.Varchar, SQLTypes.VarBinary, "x", utf8("x")),
      (SQLTypes.BigInt, SQLTypes.Varchar, Integer.valueOf(1), "1"),
      (SQLTypes.Double, SQLTypes.Varchar, Integer.valueOf(2), "2.0"),
      (SQLTypes.Timestamp, SQLTypes.Varchar, LocalDate.parse("2024-01-31"), "2024-01-31 00:00:00"),
      (
        SQLTypes.Timestamp,
        SQLTypes.Varchar,
        java.lang.Long.valueOf(1706697000000L),
        "2024-01-31 10:30:00"
      ),
      (SQLTypes.VarBinary, SQLTypes.Varchar, "YWI=", "YWI="),
      (SQLTypes.VarBinary, SQLTypes.Varchar, utf8("ab"), "YWI=")
    )
    val wrong = cases.flatMap { case (source, target, value, want) =>
      val f = converter(source, target)
      Seq(value, List(value), new java.util.ArrayList[Any](java.util.Arrays.asList(value)))
        .flatMap { form =>
          val got = shown(f(form))
          if (got == shown(want)) None
          else Some(s"$source -> $target: $form gave $got instead of ${shown(want)}")
        }
    }
    wrong shouldBe empty
    Seq(SQLTypes.BigInt, SQLTypes.Double, SQLTypes.Timestamp, SQLTypes.VarBinary, SQLTypes.Varchar)
      .foreach { target =>
        converter(SQLTypes.Int, target)(null) shouldBe (null: Any)
        converter(SQLTypes.Int, target)(List(null)) shouldBe (null: Any)
      }
  }

  "A value Elasticsearch accepted as text" should "be read as its field's type first, as ES indexed it" in {
    // MEASURED on Elasticsearch 8.18.3, field type by field type: what docvalue_fields answers
    // (what queries and aggregations see) for each text `_source` holds
    val zoned = ZonedDateTime.parse("2024-01-31T10:30:00Z")
    val cases: Seq[(SQLType, SQLType, Any, Any)] = Seq(
      // integer fields: truncated toward zero; an exponent applied
      (SQLTypes.BigInt, SQLTypes.BigInt, "5", java.lang.Long.valueOf(5L)),
      (SQLTypes.BigInt, SQLTypes.BigInt, "5.0", java.lang.Long.valueOf(5L)),
      (SQLTypes.BigInt, SQLTypes.BigInt, "5.5", java.lang.Long.valueOf(5L)),
      (SQLTypes.BigInt, SQLTypes.BigInt, "5.5e0", java.lang.Long.valueOf(5L)),
      (SQLTypes.BigInt, SQLTypes.BigInt, "-5.9", java.lang.Long.valueOf(-5L)),
      (SQLTypes.BigInt, SQLTypes.BigInt, "5e2", java.lang.Long.valueOf(500L)),
      (
        SQLTypes.BigInt,
        SQLTypes.BigInt,
        java.lang.Double.valueOf(-5.5),
        java.lang.Long.valueOf(-5L)
      ),
      (SQLTypes.Int, SQLTypes.BigInt, " 5 ", java.lang.Long.valueOf(5L)),
      (SQLTypes.SmallInt, SQLTypes.Int, "5.5", Integer.valueOf(5)),
      (SQLTypes.TinyInt, SQLTypes.Int, "5e1", Integer.valueOf(50)),
      (SQLTypes.BigInt, SQLTypes.Double, "5.5", java.lang.Double.valueOf(5.0)),
      (
        SQLTypes.BigInt,
        SQLTypes.Double,
        java.lang.Double.valueOf(5.5),
        java.lang.Double.valueOf(5.0)
      ),
      (SQLTypes.BigInt, SQLTypes.Varchar, "5.0", "5"),
      (SQLTypes.BigInt, SQLTypes.Varchar, "5e2", "500"),
      // double and float fields
      (SQLTypes.Double, SQLTypes.Double, "2.5", java.lang.Double.valueOf(2.5)),
      (SQLTypes.Double, SQLTypes.Double, " 2.5 ", java.lang.Double.valueOf(2.5)),
      (SQLTypes.Double, SQLTypes.Double, "+2.5", java.lang.Double.valueOf(2.5)),
      (SQLTypes.Double, SQLTypes.Double, "1e3", java.lang.Double.valueOf(1000.0)),
      (SQLTypes.Double, SQLTypes.Double, "1E3", java.lang.Double.valueOf(1000.0)),
      (SQLTypes.Double, SQLTypes.Varchar, "5", "5.0"),
      (SQLTypes.Double, SQLTypes.Varchar, "1e3", "1000.0"),
      (SQLTypes.Real, SQLTypes.Real, "0.1", java.lang.Float.valueOf(0.1f)),
      (SQLTypes.Real, SQLTypes.Double, "1e3", java.lang.Double.valueOf(1000.0)),
      // boolean fields: an empty text is false
      (SQLTypes.Boolean, SQLTypes.Int, "true", Integer.valueOf(1)),
      (SQLTypes.Boolean, SQLTypes.Int, "false", Integer.valueOf(0)),
      (SQLTypes.Boolean, SQLTypes.Int, "", Integer.valueOf(0)),
      (SQLTypes.Boolean, SQLTypes.Varchar, "", "false"),
      // date fields: digits are epoch milliseconds; a DATE is its day, in UTC
      (SQLTypes.Timestamp, SQLTypes.Timestamp, "1706697000000", zoned),
      (
        SQLTypes.Timestamp,
        SQLTypes.Timestamp,
        "1706697000",
        ZonedDateTime.parse("1970-01-20T18:04:57Z")
      ),
      (SQLTypes.Timestamp, SQLTypes.Varchar, "1706697000000", "2024-01-31 10:30:00"),
      (
        SQLTypes.Date,
        SQLTypes.Timestamp,
        "1706697000000",
        ZonedDateTime.parse("2024-01-31T00:00:00Z")
      ),
      (SQLTypes.Date, SQLTypes.Timestamp, "-86400000", ZonedDateTime.parse("1969-12-31T00:00:00Z")),
      (SQLTypes.Date, SQLTypes.Timestamp, zoned, ZonedDateTime.parse("2024-01-31T00:00:00Z")),
      (
        SQLTypes.Date,
        SQLTypes.Timestamp,
        LocalDateTime.parse("2024-01-31T10:30:00"),
        ZonedDateTime.parse("2024-01-31T00:00:00Z")
      ),
      (SQLTypes.Date, SQLTypes.Varchar, "1706697000000", "2024-01-31")
    )
    val wrong = cases.flatMap { case (source, target, value, want) =>
      val f = converter(source, target)
      Seq(value, List(value)).flatMap { form =>
        val got = shown(f(form))
        if (got == shown(want)) None
        else Some(s"$source -> $target: $form gave $got instead of ${shown(want)}")
      }
    }
    wrong shouldBe empty
    // an empty text in a number field: no value, as Elasticsearch indexes none
    Seq(SQLTypes.BigInt, SQLTypes.Int, SQLTypes.Double, SQLTypes.Real).foreach { source =>
      converter(source, SQLTypes.Double)("") shouldBe (null: Any)
    }
  }

  // ── a date field's values, read with its mapping format as the Elasticsearch major reads it ──
  // (every measured value of every major: MappedDateFormatSpec)

  private val measuredOn =
    java.time.Clock.fixed(Instant.parse("2026-10-07T12:00:00Z"), ZoneOffset.UTC)

  /** The version a statement runs on, by which it reads its date fields. */
  private val es8 = Some("8.18.3")

  /** The converter of a converting position whose branch reads the date field `obj.ts` of mapping
    * format `format`, on Elasticsearch `version`.
    */
  private def byFormat(
    source: SQLType,
    target: SQLType,
    format: String,
    version: Option[String] = Some("8.18.3")
  ): Any => Any =
    SetOperationValues.converter(
      source,
      target,
      "multi",
      Some(format),
      "obj.ts",
      "k",
      new SetOperationValues.DateReading(version, measuredOn)
    )

  private val at1030 = ZonedDateTime.parse("2024-01-31T10:30:00Z")

  "A date field's value" should "be read with its mapping format, as the Elasticsearch major reads it" in {
    val ts = SQLTypes.Timestamp
    // epoch_second: a number and its text are SECONDS, never 1970's milliseconds
    Seq("6.8.23", "7.17.29", "8.18.3").foreach { v =>
      shown(byFormat(ts, ts, "epoch_second", Some(v))(Integer.valueOf(1706697000))) shouldBe
      shown(at1030)
      shown(byFormat(ts, ts, "epoch_second", Some(v))("1706697000")) shouldBe shown(at1030)
    }
    // a fraction of a second: Elasticsearch 8 keeps its milliseconds, Elasticsearch 6 truncates
    // it to the second
    shown(byFormat(ts, ts, "epoch_second")("1706697000.5")) shouldBe
    shown(ZonedDateTime.parse("2024-01-31T10:30:00.500Z"))
    shown(byFormat(ts, ts, "epoch_second", Some("6.8.23"))("1706697000.5")) shouldBe shown(at1030)
    // the default format reads four digits as a YEAR, as Elasticsearch does, before epoch millis
    Seq[Any](Integer.valueOf(2024), "2024").foreach { year =>
      shown(byFormat(ts, ts, TemporalLiterals.DefaultDateFormat)(year)) shouldBe
      shown(ZonedDateTime.parse("2024-01-01T00:00:00Z"))
    }
    shown(byFormat(ts, ts, TemporalLiterals.DefaultDateFormat)("1706697000000")) shouldBe
    shown(at1030)
    // an ISO text the response parser does not read is read by the format
    shown(byFormat(ts, ts, TemporalLiterals.DefaultDateFormat)("2024-01-31T12:30:00+0200")) shouldBe
    shown(at1030)
    // Elasticsearch's date keeps milliseconds
    shown(
      byFormat(ts, ts, TemporalLiterals.DefaultDateFormat)(
        ZonedDateTime.parse("2024-01-31T10:30:00.123456789Z")
      )
    ) shouldBe shown(ZonedDateTime.parse("2024-01-31T10:30:00.123Z"))
    // a custom pattern's text: a ZonedDateTime, not the text
    shown(byFormat(ts, ts, "yyyy-MM-dd HH:mm:ss")("2024-01-31 10:30:00")) shouldBe shown(at1030)
    // Joda (Elasticsearch 6) reads one or two digits where java.time reads two, and a two-digit
    // year around the current year minus 30
    shown(byFormat(ts, ts, "yyyy/MM/dd", Some("6.8.23"))("2024/1/31")) shouldBe
    shown(ZonedDateTime.parse("2024-01-31T00:00:00Z"))
    shown(byFormat(ts, ts, "dd.MM.yy", Some("6.8.23"))("31.01.46")) shouldBe
    shown(ZonedDateTime.parse("1946-01-31T00:00:00Z"))
    shown(byFormat(ts, ts, "dd.MM.yy")("31.01.46")) shouldBe
    shown(ZonedDateTime.parse("2046-01-31T00:00:00Z"))
    // a value the response parser read as an ISO date is read again with the format
    shown(
      byFormat(ts, ts, "yyyy-dd-MM||strict_date_optional_time")(LocalDate.parse("2024-01-02"))
    ) shouldBe
    shown(ZonedDateTime.parse("2024-02-01T00:00:00Z"))
    shown(
      byFormat(ts, ts, "strict_date_hour_minute")(LocalDateTime.parse("2024-01-31T10:30"))
    ) shouldBe
    shown(at1030)
    // a DATE branch is the day of the instant, in UTC, spelled as DuckDB spells a date
    shown(byFormat(SQLTypes.Date, ts, "epoch_second")(Integer.valueOf(1706697000))) shouldBe
    shown(ZonedDateTime.parse("2024-01-31T00:00:00Z"))
    byFormat(SQLTypes.Date, SQLTypes.Varchar, "dd/MM/yyyy HH:mm")("31/01/2024 10:30") shouldBe
    "2024-01-31"
    byFormat(ts, SQLTypes.Varchar, "yyyy/MM/dd||epoch_millis")("2024/01/31") shouldBe
    "2024-01-31 00:00:00"
    // one converter reads every row of a leg: a value the response parser read as an ISO date is
    // read by its shape once (Elasticsearch 6's strict_date_time wants a fraction, so it reads
    // 10:30:00Z as 10:30:00.0Z), then each value of that shape is its own instant
    val sdt6 = byFormat(ts, ts, "strict_date_time", Some("6.8.23"))
    Seq("2024-01-31T10:30:00Z", "2024-02-01T11:00:00Z", "2024-01-31T12:30:00.123456+02:00")
      .map(ZonedDateTime.parse)
      .foreach(z =>
        shown(sdt6(z)) shouldBe
        shown(z.withNano(z.getNano / 1000000 * 1000000).withZoneSameInstant(ZoneOffset.UTC))
      )
    intercept[IllegalArgumentException](sdt6(LocalDate.parse("2024-01-31")))
    // NULL, a one-element array
    byFormat(ts, ts, "epoch_second")(null) shouldBe (null: Any)
    shown(byFormat(ts, ts, "epoch_second")(List("1706697000"))) shouldBe shown(at1030)
  }

  it should "fail the statement, naming the field and the format, rather than be guessed" in {
    val ts = SQLTypes.Timestamp
    def failure(f: Any => Any, value: Any): String =
      intercept[IllegalArgumentException](f(value)).getMessage
    // a value the format does not read
    failure(byFormat(ts, ts, "yyyy/MM/dd"), "31-01-2024") shouldBe
    "Conversion Error: Could not read '31-01-2024' as a TIMESTAMP when casting from source column " +
    "k: the date format 'yyyy/MM/dd' of field obj.ts does not read it as Elasticsearch 8 does"
    // Elasticsearch 7 reads no negative epoch_millis, Elasticsearch 8 does
    shown(byFormat(ts, ts, "epoch_millis")("-2024")) shouldBe
    shown(ZonedDateTime.parse("1969-12-31T23:59:57.976Z"))
    failure(byFormat(ts, ts, "epoch_millis", Some("7.17.29")), "-2024") shouldBe
    "Conversion Error: Could not read '-2024' as a TIMESTAMP when casting from source column k: " +
    "the date format 'epoch_millis' of field obj.ts does not read it as Elasticsearch 7 does"
    // a pattern letter no measurement covers
    failure(byFormat(ts, ts, "yyyy-MM-dd B||epoch_millis"), "1706697000000") shouldBe
    "Conversion Error: Could not read '1706697000000' as a TIMESTAMP when casting from source " +
    "column k: core cannot read it with the date format 'yyyy-MM-dd B||epoch_millis' of field " +
    "obj.ts as Elasticsearch 8 does: 'yyyy-MM-dd B' is not a date format core reads as " +
    "Elasticsearch 8 reads it: the letter 'B' is not one core has measured"
    // a name every major reads (MEASURED): a text it does not parse is the next alternative's
    shown(byFormat(ts, ts, "week_date||epoch_millis")("1706697000000")) shouldBe shown(at1030)
    shown(byFormat(ts, ts, "epoch_millis||week_date")("1706697000000")) shouldBe shown(at1030)
    // a spelling the response parser erased, read differently by the format's alternatives
    failure(
      byFormat(ts, ts, "yyyy-dd-MM'T'HH:mm:ss||strict_date_optional_time"),
      LocalDateTime.parse("2024-01-02T10:30")
    ) shouldBe
    "Conversion Error: Could not read '2024-01-02T10:30' as a TIMESTAMP when casting from source " +
    "column k: core cannot read it with the date format " +
    "'yyyy-dd-MM'T'HH:mm:ss||strict_date_optional_time' of field obj.ts as Elasticsearch 8 does: " +
    "the format reads it differently depending on how it was spelled, which the response no " +
    "longer says"
    // an offset the response parser dropped, which the format reads
    failure(byFormat(ts, ts, "yyyy-MM-dd[XXX]"), LocalDate.parse("2024-01-31")) should
    endWith(
      "the response gives it without the offset it may have been written with, which the format " +
      "reads"
    )
    // the Elasticsearch version not known
    failure(byFormat(ts, ts, "epoch_second", None), "1706697000") shouldBe
    "Conversion Error: Could not read '1706697000' as a TIMESTAMP when casting from source column " +
    "k: the date format 'epoch_second' of field obj.ts reads it as the Elasticsearch version " +
    "does, which is not known"
  }

  // DuckDB 1.5.5: a BLOB beside text is a BLOB, each text value cast to bytes (MEASURED)

  private def utf8(s: String): Array[Byte] = s.getBytes(java.nio.charset.StandardCharsets.UTF_8)

  private def bytesOf(v: Any): Seq[Int] = v.asInstanceOf[Array[Byte]].toSeq.map(_ & 0xff)

  "A VARBINARY value reaching a VARBINARY column" should "be the bytes of the text Elasticsearch holds" in {
    val f = converter(SQLTypes.VarBinary, SQLTypes.VarBinary)
    // a `binary` field's base64, read as its UTF-8 bytes -- what arrow and the JDBC driver read
    bytesOf(f("YWI=")) shouldBe utf8("YWI=").toSeq.map(_ & 0xff)
    kind(f("YWI=")) shouldBe classOf[Array[Byte]]
    val raw = Array[Byte](1, 2, -1)
    f(raw) shouldBe raw
    f(null) shouldBe (null: Any)
  }

  "A text value reaching a VARBINARY column" should "be cast to bytes as DuckDB casts a VARCHAR to a BLOB" in {
    val f = converter(SQLTypes.Varchar, SQLTypes.VarBinary)
    bytesOf(f("x")) shouldBe Seq(0x78)
    bytesOf(f("hello world")) shouldBe utf8("hello world").toSeq.map(_ & 0xff)
    bytesOf(f("\\x41\\xAA")) shouldBe Seq(0x41, 0xaa)
    bytesOf(f("a\\x41b")) shouldBe Seq(0x61, 0x41, 0x62)
    bytesOf(f("\\xaA")) shouldBe Seq(0xaa)
    bytesOf(f("\\x00")) shouldBe Seq(0x00)
    bytesOf(f("\t\u007f")) shouldBe Seq(0x09, 0x7f)
    bytesOf(f("")) shouldBe Nil
    bytesOf(f(List("x"))) shouldBe Seq(0x78)
    f(null) shouldBe (null: Any)
    kind(f("x")) shouldBe classOf[Array[Byte]]
  }

  it should "fail, as DuckDB's cast does, on a character outside ASCII or a broken escape" in {
    val f = converter(SQLTypes.Varchar, SQLTypes.VarBinary)
    def failure(text: String): String =
      intercept[IllegalArgumentException](f(text)).getMessage
    failure("\u00e9") shouldBe
    "Conversion Error: Invalid byte encountered in STRING -> BLOB conversion of string \"\u00e9\". " +
    "All non-ascii characters must be escaped with hex codes (e.g. \\xAA)"
    failure("a\\b") shouldBe
    "Conversion Error: Invalid hex escape code encountered in string -> blob conversion of string " +
    "\"a\\b\": unterminated escape code at end of blob"
    failure("\\x4") should endWith("\"\\x4\": unterminated escape code at end of blob")
    failure("ab\\") should endWith("\"ab\\\": unterminated escape code at end of blob")
    failure("\\xZZ") should endWith("\"\\xZZ\": \\xZZ")
    failure("\\XAA") should endWith("\"\\XAA\": \\XAA")
    failure("\\\\x41") should endWith("\"\\\\x41\": \\\\x4")
    // left to right: the first fault met names the failure
    failure("\\xZZ\u00e9") should endWith(": \\xZZ")
    failure("\u00e9\\xZZ") should include("Invalid byte encountered")
    failure("a\\\u00e9") should endWith("unterminated escape code at end of blob")
  }

  // ── the per-leg plan ─────────────────────────────────────────────────────────────────────

  private val schema = Table(
    "t",
    columns = List(
      Column("id", SQLTypes.Keyword),
      Column("n", SQLTypes.Int),
      Column("x", SQLTypes.Double),
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("bin", SQLTypes.VarBinary),
      Column("geo", SQLTypes.GeoPoint),
      Column(
        "obj",
        SQLTypes.Struct,
        multiFields = List(Column("a", SQLTypes.Int), Column("b", SQLTypes.Keyword))
      ),
      Column(
        "objl",
        SQLTypes.Struct,
        multiFields = List(Column("a", SQLTypes.BigInt), Column("b", SQLTypes.Keyword))
      ),
      Column("oi", SQLTypes.Struct, multiFields = List(Column("a", SQLTypes.Int))),
      Column("ol", SQLTypes.Struct, multiFields = List(Column("a", SQLTypes.BigInt))),
      Column("ox", SQLTypes.Struct, multiFields = List(Column("a", SQLTypes.Double))),
      Column("od", SQLTypes.Struct, multiFields = List(Column("a", SQLTypes.Date))),
      Column("ots", SQLTypes.Struct, multiFields = List(Column("a", SQLTypes.Timestamp))),
      Column("obin", SQLTypes.Struct, multiFields = List(Column("a", SQLTypes.VarBinary))),
      Column("ov", SQLTypes.Struct, multiFields = List(Column("a", SQLTypes.Keyword))),
      Column(
        "si",
        SQLTypes.Struct,
        multiFields =
          List(Column("p", SQLTypes.Struct, multiFields = List(Column("c", SQLTypes.Int))))
      ),
      Column(
        "sl",
        SQLTypes.Struct,
        multiFields =
          List(Column("p", SQLTypes.Struct, multiFields = List(Column("c", SQLTypes.BigInt))))
      ),
      Column("ai", SQLTypes.Array(SQLTypes.Int)),
      Column("av", SQLTypes.Array(SQLTypes.Keyword)),
      Column(
        "items",
        SQLTypes.Array(SQLTypes.Struct),
        multiFields = List(Column("p", SQLTypes.Keyword), Column("q", SQLTypes.Int))
      ),
      Column(
        "items2",
        SQLTypes.Array(SQLTypes.Struct),
        multiFields = List(Column("p", SQLTypes.Int))
      )
    )
  )

  private def resolved(sql: String): MultiSearch =
    Parser(sql) match {
      case Right(m: MultiSearch) => m.copy(requests = m.requests.map(_.update(Some(schema))))
      case other                 => fail(s"[$sql] expected a set operation, got $other")
    }

  "The per-leg plan" should "convert nothing when every column's branches agree" in {
    SetOperationValues.converters(
      resolved("SELECT id, n FROM t UNION ALL SELECT id, n FROM t"),
      es8
    ) shouldBe Seq(None, None)
    // a NULL literal beside an INT: nothing to convert either
    SetOperationValues.converters(
      resolved("SELECT n AS k FROM t UNION ALL SELECT NULL AS k FROM t"),
      es8
    ) shouldBe Seq(None, None)
  }

  it should "convert, in every leg, only the columns whose branches disagree" in {
    val plan = SetOperationValues.converters(
      resolved("SELECT id, n FROM t UNION ALL SELECT id, x FROM t"),
      es8
    )
    plan.map(_.map(_.map(_ ne null).toSeq)) shouldBe
    Seq(Some(Seq(false, true)), Some(Seq(false, true)))
    plan.head.get.apply(1)(Integer.valueOf(3)) shouldBe 3.0
  }

  it should "fail a multi-valued value with DuckDB's own words, naming the column" in {
    def failure(sql: String, leg: Int, value: Any): String = {
      val plan = SetOperationValues.converters(resolved(sql), es8)
      intercept[IllegalArgumentException](plan(leg).get.apply(0)(value)).getMessage
    }
    val tags = List("a", "b")
    // MEASURED on DuckDB 1.5.5, the same shapes over a VARCHAR[] column
    failure("SELECT id AS k FROM t UNION ALL SELECT n AS k FROM t", 0, tags) shouldBe
    "Conversion Error: Unimplemented type for cast (INTEGER -> VARCHAR[]) when casting from " +
    "source column k"
    failure("SELECT x AS k FROM t UNION ALL SELECT id AS k FROM t", 1, tags) shouldBe
    "Conversion Error: Unimplemented type for cast (DOUBLE -> VARCHAR[]) when casting from " +
    "source column k"
    failure("SELECT id AS k FROM t UNION ALL SELECT ts AS k FROM t", 0, tags) shouldBe
    "Conversion Error: Unimplemented type for cast (TIMESTAMP -> VARCHAR[]) when casting from " +
    "source column k"
    failure("SELECT id AS k FROM t UNION ALL SELECT bin AS k FROM t", 0, tags) shouldBe
    "Conversion Error: Unimplemented type for cast (BLOB -> VARCHAR[]) when casting from " +
    "source column k"
    // the first other branch is text: DuckDB names its value, which this branch cannot know
    failure(
      "SELECT id AS k FROM t UNION ALL SELECT 'c' AS k FROM t UNION ALL SELECT n AS k FROM t",
      0,
      tags
    ) shouldBe
    "Conversion Error: Type VARCHAR can't be cast to the destination type VARCHAR[] when " +
    "casting from source column k"
  }

  it should "spell every leg as text in a VARCHAR column DuckDB gives no type to" in {
    val plan = SetOperationValues.converters(
      resolved("SELECT bin AS k FROM t UNION ALL SELECT n AS k FROM t"),
      es8
    )
    plan.head.get.apply(0)("YWI=") shouldBe "YWI="
    plan(1).get.apply(0)(Integer.valueOf(3)) shouldBe "3"
    // a binary field with several values: the column holds one value per row
    intercept[IllegalArgumentException](
      plan.head.get.apply(0)(List("YWI=", "eA=="))
    ).getMessage shouldBe
    "Conversion Error: Unimplemented type for cast (INTEGER -> BLOB[]) when casting from source " +
    "column k"
  }

  it should "give every leg of a STRUCT column DuckDB's merged struct" in {
    val plan = SetOperationValues.converters(
      resolved("SELECT geo AS k FROM t UNION ALL SELECT obj AS k FROM t"),
      es8
    )
    plan.head.get.apply(0)(List(2.35, 48.85)) shouldBe
    ListMap("lat" -> 48.85, "lon" -> 2.35, "a" -> null, "b" -> null)
    plan(1).get.apply(0)(Map("a" -> 1, "b" -> "x")) shouldBe
    ListMap("lat" -> null, "lon" -> null, "a" -> 1, "b" -> "x")
    // two points stored differently: both {lat, lon}
    val points = SetOperationValues.converters(
      resolved("SELECT geo AS k FROM t UNION ALL SELECT geo AS k FROM t"),
      es8
    )
    points.head.get.apply(0)("48.85,2.35") shouldBe ListMap("lat" -> 48.85, "lon" -> 2.35)
    points(1).get.apply(0)(Map("lat" -> 48.85, "lon" -> 2.35)) shouldBe
    ListMap("lat" -> 48.85, "lon" -> 2.35)
  }

  /** The value branch `leg` of `sql` answers at column 1 for `value`. */
  private def converted(sql: String, leg: Int, value: Any): Any =
    SetOperationValues.converters(resolved(sql), es8)(leg) match {
      case Some(functions) if functions(0) ne null => functions(0)(value)
      case other => fail(s"[$sql] leg $leg converts nothing: $other")
    }

  private def union(a: String, b: String): String =
    s"SELECT $a AS k FROM t UNION ALL SELECT $b AS k FROM t"

  /** The field `a` branch `leg` of `sql` answers for the object `{a: value}`, with its class. */
  private def fieldA(sql: String, leg: Int, value: Any): (Any, Class[_]) =
    converted(sql, leg, Map("a" -> value)) match {
      case m: scala.collection.Map[_, _] =>
        shown(m.asInstanceOf[scala.collection.Map[String, Any]]("a"))
      case other => fail(s"[$sql] leg $leg answered $other")
    }

  it should "convert every branch of a converting field, the one already of its type too" in {
    // a BIGINT field beside an INT one: a Long in BOTH branches, also where the JSON fitted an int
    fieldA(union("ol", "oi"), 0, Integer.valueOf(1)) shouldBe shown(java.lang.Long.valueOf(1L))
    fieldA(union("ol", "oi"), 1, Integer.valueOf(2)) shouldBe shown(java.lang.Long.valueOf(2L))
    // a DOUBLE beside an INT: a Double in both
    fieldA(union("ox", "oi"), 0, Integer.valueOf(2)) shouldBe shown(java.lang.Double.valueOf(2.0))
    // a VARBINARY beside a VARCHAR: bytes in both
    fieldA(union("obin", "ov"), 0, "YWI=") shouldBe shown(utf8("YWI="))
    fieldA(union("obin", "ov"), 1, "x") shouldBe shown(utf8("x"))
    // a TIMESTAMP beside a DATE: a ZonedDateTime in both, whatever form the timestamp was read in
    val midnight = ZonedDateTime.parse("2024-01-31T00:00:00Z")
    val at1030 = ZonedDateTime.parse("2024-01-31T10:30:00Z")
    Seq(
      LocalDate.parse("2024-01-31")              -> midnight,
      LocalDateTime.parse("2024-01-31T10:30:00") -> at1030,
      java.lang.Long.valueOf(1706697000000L)     -> at1030,
      at1030                                     -> at1030
    ).foreach { case (value, want) =>
      fieldA(union("ots", "od"), 0, value) shouldBe shown(want)
    }
    fieldA(union("ots", "od"), 1, LocalDate.parse("2024-01-31")) shouldBe shown(midnight)
    // a field whose branches agree keeps its value as read, inside a converting struct
    converted(union("objl", "obj"), 1, Map("a" -> Integer.valueOf(1), "b" -> "x")) shouldBe
    ListMap("a" -> java.lang.Long.valueOf(1L), "b" -> "x")
    // a NULL literal beside a struct changes nothing; in a converting struct column it is NULL
    SetOperationValues.converters(resolved(union("ol", "NULL")), es8) shouldBe Seq(None, None)
    converted(union("ol", "oi") + " UNION ALL SELECT NULL AS k FROM t", 2, List(null)) shouldBe
    (null: Any)
  }

  it should "convert a struct inside a struct, and the elements of a list, the same way" in {
    converted(union("sl", "si"), 1, Map("p" -> Map("c" -> Integer.valueOf(3)))) shouldBe
    ListMap("p" -> ListMap("c" -> java.lang.Long.valueOf(3L)))
    // lists of values: every element in the element's type; a list of one stored bare
    converted(union("ai", "av"), 0, List(Integer.valueOf(1), Integer.valueOf(2))) shouldBe
    List("1", "2")
    converted(union("ai", "av"), 0, Integer.valueOf(3)) shouldBe List("3")
    converted(union("ai", "av"), 1, List("a")) shouldBe List("a")
    converted(union("ai", "av"), 0, null) shouldBe (null: Any)
    SetOperationValues.converters(resolved(union("ai", "NULL")), es8) shouldBe Seq(None, None)
    converted(union("ai", "av") + " UNION ALL SELECT NULL AS k FROM t", 2, List(null)) shouldBe
    (null: Any)
    // lists of objects: every element the merged struct (p VARCHAR, q INT)
    converted(union("items", "items2"), 1, List(Map("p" -> Integer.valueOf(1)))) shouldBe
    List(ListMap("p" -> "1", "q" -> null))
    converted(union("items", "items2"), 0, Map("p" -> "a", "q" -> Integer.valueOf(2))) shouldBe
    List(ListMap("p" -> "a", "q" -> Integer.valueOf(2)))
  }

  it should "convert nothing for a statement it refuses, or beside a SELECT * branch" in {
    SetOperationValues.converters(
      resolved("SELECT d AS k FROM t UNION ALL SELECT n AS k FROM t"),
      es8
    ) shouldBe Seq(None, None)
    SetOperationValues.converters(
      resolved("SELECT n AS k FROM t UNION ALL SELECT * FROM t"),
      es8
    ) shouldBe Seq(None, None)
  }
}
