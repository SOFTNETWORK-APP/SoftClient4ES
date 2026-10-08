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

import app.softnetwork.elastic.sql.{StringValue, Value}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.MultiSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{LocalDate, ZonedDateTime}
import scala.collection.immutable.ListMap

/** Core's own `UNION ALL` reads a date whose branches AGREE as the date Elasticsearch indexed when
  * one of its branches is a field with a mapping format of its own -- `epoch_second`, `yyyy/MM/dd`,
  * ... -- never as its JSON spells it (`1706697000`, `"2024/01/31"`), at the column and at every
  * depth inside it. A date of the default format, a value with no date, a multi-valued value and a
  * date core cannot read keep their values as stored: no statement core answered fails.
  */
class SetOperationMappedDatesSpec extends AnyFlatSpec with Matchers {

  private def format(f: String): ListMap[String, Value[_]] =
    ListMap[String, Value[_]]("format" -> StringValue(f))

  private val schema = Table(
    "t",
    columns = List(
      Column("c_es", SQLTypes.Timestamp, options = format("epoch_second")),
      Column("c_c1", SQLTypes.Timestamp, options = format("yyyy/MM/dd")),
      Column("c_def", SQLTypes.Timestamp),
      Column("c_dfmt", SQLTypes.Date, options = format("dd/MM/yyyy HH:mm")),
      Column("ts", SQLTypes.Timestamp),
      Column("d", SQLTypes.Date),
      Column("n", SQLTypes.Int),
      Column("s", SQLTypes.Keyword),
      Column("c_z", SQLTypes.Timestamp, options = format("yyyy-MM-dd VV")),
      Column(
        "obj",
        SQLTypes.Struct,
        multiFields = List(
          Column("a", SQLTypes.Timestamp, options = format("epoch_second")),
          Column("n", SQLTypes.Int),
          Column("z", SQLTypes.Timestamp, options = format("yyyy-MM-dd VV"))
        )
      ),
      // `obj`'s fields, every date of the default format
      Column(
        "objd",
        SQLTypes.Struct,
        multiFields = List(
          Column("a", SQLTypes.Timestamp),
          Column("n", SQLTypes.Int),
          Column("z", SQLTypes.Timestamp)
        )
      ),
      // a converting object beside `obj`: `n` an INT beside a BIGINT, `a` agreeing
      Column(
        "objl",
        SQLTypes.Struct,
        multiFields = List(
          Column("a", SQLTypes.Timestamp, options = format("epoch_second")),
          Column("n", SQLTypes.BigInt)
        )
      ),
      Column(
        "deep",
        SQLTypes.Struct,
        multiFields = List(
          Column(
            "p",
            SQLTypes.Struct,
            multiFields = List(Column("c", SQLTypes.Date, options = format("yyyy/MM/dd")))
          ),
          Column("q", SQLTypes.Keyword)
        )
      ),
      Column(
        "items",
        SQLTypes.Array(SQLTypes.Struct),
        multiFields = List(
          Column("d", SQLTypes.Date, options = format("dd/MM/yyyy")),
          Column("q", SQLTypes.Int)
        )
      ),
      Column(
        "itemsd",
        SQLTypes.Array(SQLTypes.Struct),
        multiFields = List(Column("d", SQLTypes.Date), Column("q", SQLTypes.Int))
      ),
      Column("stamps", SQLTypes.Array(SQLTypes.Timestamp), options = format("epoch_second"))
    )
  )

  private def resolved(sql: String): MultiSearch =
    Parser(sql) match {
      case Right(m: MultiSearch) => m.copy(requests = m.requests.map(_.update(Some(schema))))
      case other                 => fail(s"[$sql] expected a set operation, got $other")
    }

  private def plan(sql: String, version: Option[String] = Some("8.18.3")) =
    SetOperationValues.converters(resolved(sql), version)

  private def union(a: String, b: String): String =
    s"SELECT $a AS k FROM t UNION ALL SELECT $b AS k FROM t"

  /** A document's value as core receives it: its JSON, read by the response parser. */
  private def received(json: String): Any =
    ElasticConversion.jsonNodeToAny(JacksonConfig.objectMapper.readTree(json), ListMap.empty)

  /** What leg `leg` of `sql` answers at column 1 for a value written as `json`. */
  private def answered(
    sql: String,
    leg: Int,
    json: String,
    version: Option[String] = Some("8.18.3")
  ): Any = {
    val value = received(json)
    plan(sql, version)(leg) match {
      case Some(functions) if functions(0) ne null => functions(0)(value)
      case _                                       => value
    }
  }

  /** A value and its class. */
  private def shown(v: Any): (Any, Class[_]) =
    if (v == null) (null, classOf[Null]) else (v, v.getClass)

  private def utc(s: String): ZonedDateTime = ZonedDateTime.parse(s)

  "An agreeing date column of a mapping format of its own" should "answer the date Elasticsearch indexed, in every leg" in {
    // epoch_second beside a default-format TIMESTAMP: seconds, never 1970's milliseconds
    shown(answered(union("c_es", "ts"), 0, "1706697000")) shouldBe
    shown(utc("2024-01-31T10:30:00Z"))
    shown(answered(union("c_es", "ts"), 0, "\"1706697000.5\"")) shouldBe
    shown(utc("2024-01-31T10:30:00.500Z"))
    // the default-format leg too: every row a timestamp
    shown(answered(union("c_es", "ts"), 1, "\"2024-02-01T00:00:00Z\"")) shouldBe
    shown(utc("2024-02-01T00:00:00Z"))
    shown(answered(union("c_es", "ts"), 1, "\"2024\"")) shouldBe shown(utc("2024-01-01T00:00:00Z"))
    // a custom pattern
    shown(answered(union("c_c1", "ts"), 0, "\"2024/01/31\"")) shouldBe
    shown(utc("2024-01-31T00:00:00Z"))
    // a DATE field: its day
    shown(answered(union("c_dfmt", "d"), 0, "\"31/01/2024 23:30\"")) shouldBe
    shown(LocalDate.parse("2024-01-31"))
    // as the major the statement runs on reads it: Elasticsearch 6 truncates the fraction
    shown(answered(union("c_es", "ts"), 0, "\"1706697000.5\"", Some("6.8.23"))) shouldBe
    shown(utc("2024-01-31T10:30:00Z"))
    // a computed branch's one-element array is one value
    shown(answered(union("c_es", "ts"), 1, "[\"2024-02-01T00:00:00Z\"]")) shouldBe
    shown(utc("2024-02-01T00:00:00Z"))
  }

  it should "keep a multi-valued value as read, as it always was" in {
    answered(union("c_es", "ts"), 0, "[1706697000,1706697001]") shouldBe List(
      1706697000,
      1706697001
    )
    // in Elasticsearch's array (an array of arrays, which Elasticsearch accepts and flattens), or
    // an object: as stored, judged as the reader sees it -- never a failure
    answered(union("c_es", "ts"), 0, "[[1706697000,1706697001]]") shouldBe
    List(List(1706697000, 1706697001))
    answered(union("c_c1", "ts"), 0, """[["2024/01/31","2024/02/01"]]""") shouldBe
    List(List("2024/01/31", "2024/02/01"))
    answered(union("c_es", "ts"), 0, """[{"x":1}]""") shouldBe List(ListMap("x" -> 1))
    answered(union("c_es", "ts"), 0, """[[{"x":1}]]""") shouldBe List(List(ListMap("x" -> 1)))
    // one value in arrays of one: read
    shown(answered(union("c_es", "ts"), 0, "[[1706697000]]")) shouldBe
    shown(utc("2024-01-31T10:30:00Z"))
    // inside an object
    answered(union("obj", "obj"), 0, """{"a":[[1706697000,1706697001]]}""") shouldBe
    ListMap("a" -> List(List(1706697000, 1706697001)))
  }

  it should "change nothing else: the default format, a column with no date, a NULL, SELECT *" in {
    // the control: two fields of the default format keep their values as read
    plan(union("c_def", "ts")) shouldBe Seq(None, None)
    answered(union("c_def", "ts"), 0, "\"2024\"") shouldBe "2024"
    answered(union("c_def", "ts"), 0, "2024") shouldBe 2024
    // in one statement, only the date column of a format of its own is read
    plan("SELECT c_es, n, s FROM t UNION ALL SELECT ts, n, s FROM t")
      .map(_.map(_.toSeq.map(_ ne null))) shouldBe
    Seq(Some(Seq(true, false, false)), Some(Seq(true, false, false)))
    // a NULL literal reads nothing
    plan(union("c_es", "NULL")).map(_.isDefined) shouldBe Seq(true, false)
    // beside a SELECT * branch, whose values are not typed, nothing is read
    plan("SELECT c_es AS k FROM t UNION ALL SELECT * FROM t") shouldBe Seq(None, None)
    // objects and lists whose date fields all have the default format: as read, at no cost
    plan(union("objd", "objd")) shouldBe Seq(None, None)
    plan(union("itemsd", "itemsd")) shouldBe Seq(None, None)
  }

  it should "keep a date it cannot read as stored, where a converting column fails it" in {
    // a region id: its offset comes from zone data -- the value as stored, never a failure
    answered(union("c_z", "ts"), 0, "\"2024-01-31 Europe/Paris\"") shouldBe
    "2024-01-31 Europe/Paris"
    answered(union("c_z", "ts"), 0, "[\"2024-01-31 Europe/Paris\"]") shouldBe
    List("2024-01-31 Europe/Paris")
    // the readable values of the column are still read
    shown(answered(union("c_z", "ts"), 0, "\"2024-01-31 Z\"")) shouldBe
    shown(utc("2024-01-31T00:00:00Z"))
    shown(answered(union("c_z", "ts"), 1, "\"2024\"")) shouldBe
    shown(utc("2024-01-01T00:00:00Z"))
    // the Elasticsearch version not known: every date of a format as stored
    answered(union("c_es", "ts"), 0, "1706697000", version = None) shouldBe 1706697000
    // a column that converts (a TIMESTAMP beside a DATE) fails it, as before
    intercept[IllegalArgumentException](
      answered(union("c_z", "d"), 0, "\"2024-01-31 Europe/Paris\"")
    ).getMessage should include(
      "Could not read '2024-01-31 Europe/Paris' as a TIMESTAMP when casting from source column k: " +
      "core cannot read it with the date format 'yyyy-MM-dd VV' of field c_z as Elasticsearch 8 does"
    )
  }

  "An agreeing date field of a mapping format of its own" should "be read at every depth, every other entry as stored" in {
    // an object's field: read; every other entry -- one the schema does not hold too -- as stored,
    // in its order
    answered(union("obj", "obj"), 0, """{"x":"kept","n":"5.5","a":1706697000}""") shouldBe
    ListMap("x" -> "kept", "n" -> "5.5", "a" -> utc("2024-01-31T10:30:00Z"))
    // the default-format branch's field too
    answered(union("obj", "objd"), 1, """{"a":"2024","n":1}""") shouldBe
    ListMap("a" -> utc("2024-01-01T00:00:00Z"), "n" -> 1)
    // a date it cannot read inside: as stored
    answered(union("obj", "obj"), 0, """{"z":"2024-01-31 Europe/Paris","a":1706697000}""") shouldBe
    ListMap("z" -> "2024-01-31 Europe/Paris", "a" -> utc("2024-01-31T10:30:00Z"))
    // an object in an object
    answered(union("deep", "deep"), 0, """{"q":"x","p":{"c":"2024/01/31"}}""") shouldBe
    ListMap("q" -> "x", "p" -> ListMap("c" -> LocalDate.parse("2024-01-31")))
    // a list of objects, and one stored bare
    answered(union("items", "itemsd"), 0, """[{"d":"31/01/2024","q":"2"},{"q":3}]""") shouldBe
    List(ListMap("d" -> LocalDate.parse("2024-01-31"), "q" -> "2"), ListMap("q" -> 3))
    answered(union("items", "itemsd"), 1, """{"d":"2024-01-31"}""") shouldBe
    ListMap("d" -> LocalDate.parse("2024-01-31"))
    // a list of dates
    answered(union("stamps", "stamps"), 0, "[1706697000,\"1706697000.5\"]") shouldBe
    List(utc("2024-01-31T10:30:00Z"), utc("2024-01-31T10:30:00.500Z"))
    // an object wrapped in Elasticsearch's array is read in it; several objects as stored
    answered(union("obj", "obj"), 0, """[{"a":1706697000}]""") shouldBe
    List(ListMap("a" -> utc("2024-01-31T10:30:00Z")))
    answered(union("obj", "obj"), 0, """[{"a":1706697000},{"a":1}]""") shouldBe
    List(ListMap("a" -> 1706697000), ListMap("a" -> 1))
    // NULL, and a value that is no object, as stored
    answered(union("obj", "obj"), 0, "null") shouldBe (null: Any)
    answered(union("obj", "obj"), 0, "\"x\"") shouldBe "x"
  }

  it should "be read inside a converting object too, where its branches agree" in {
    // `n` converts (an INT beside a BIGINT): the object is DuckDB's; `a` agrees and is read
    answered(union("obj", "objl"), 0, """{"a":1706697000,"n":1}""") shouldBe
    ListMap("a" -> utc("2024-01-31T10:30:00Z"), "n" -> java.lang.Long.valueOf(1L), "z" -> null)
  }

  it should "ask the Elasticsearch version once, and only for a column it reads" in {
    var asked = 0
    def version: Option[String] = { asked += 1; Some("8.18.3") }
    SetOperationValues.converters(resolved(union("c_def", "ts")), version)
    asked shouldBe 0
    SetOperationValues.converters(
      resolved("SELECT c_es, c_c1 FROM t UNION ALL SELECT ts, ts FROM t"),
      version
    )
    asked shouldBe 1
  }
}
