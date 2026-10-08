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
import app.softnetwork.elastic.sql.query.{MultiSearch, SingleSearch}
import app.softnetwork.elastic.sql.operator.SetOperator
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{LocalDate, LocalTime, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap
import scala.util.Try

/** Each leg of a set operation READ AS ITS BRANCH DECLARES IT ([[SetOperationValues.legReaders]]),
  * the reader the relational engine types its legs with: each value as Elasticsearch indexed it, in
  * ONE class per type, at every depth, where the branches agree too. Every value goes the way core
  * receives it: its JSON, read by the response parser (`ElasticConversion.jsonNodeToAny`). The
  * expectations are the contract's table, MEASURED on Elasticsearch 8.18.3.
  */
class SetOperationLegReadersSpec extends AnyFlatSpec with Matchers {

  private def format(f: String): ListMap[String, Value[_]] =
    ListMap[String, Value[_]]("format" -> StringValue(f))

  private val schema = Table(
    "t",
    columns = List(
      Column("c_es", SQLTypes.Timestamp, options = format("epoch_second")),
      Column("c_em", SQLTypes.Timestamp, options = format("epoch_millis")),
      Column("c_c1", SQLTypes.Timestamp, options = format("yyyy/MM/dd")),
      Column("c_c3", SQLTypes.Timestamp, options = format("dd/MM/yyyy HH:mm")),
      Column("c_def", SQLTypes.Timestamp),
      Column("c_dfmt", SQLTypes.Date, options = format("dd/MM/yyyy HH:mm")),
      Column("c_des", SQLTypes.Date, options = format("epoch_second")),
      Column("c_z", SQLTypes.Timestamp, options = format("yyyy-MM-dd'T'HH:mm:ss z")),
      Column("ts", SQLTypes.Timestamp),
      Column("d", SQLTypes.Date),
      Column("t", SQLTypes.Time, options = format("HH:mm:ss")),
      Column("n", SQLTypes.Int),
      Column("bg", SQLTypes.BigInt),
      Column("sm", SQLTypes.SmallInt),
      Column("ti", SQLTypes.TinyInt),
      Column("x", SQLTypes.Double),
      Column("f", SQLTypes.Real),
      Column("b", SQLTypes.Boolean),
      Column("s", SQLTypes.Keyword),
      Column("bin", SQLTypes.VarBinary),
      Column("geo", SQLTypes.GeoPoint),
      Column(
        "oz",
        SQLTypes.Struct,
        multiFields =
          List(Column("z", SQLTypes.Timestamp, options = format("yyyy-MM-dd'T'HH:mm:ss z")))
      ),
      Column("arr", SQLTypes.Array(SQLTypes.Int)),
      Column(
        "obj",
        SQLTypes.Struct,
        multiFields = List(
          Column("n", SQLTypes.Int),
          Column("e", SQLTypes.Timestamp, options = format("epoch_second")),
          Column("s", SQLTypes.Keyword),
          Column("p", SQLTypes.Struct, multiFields = List(Column("c", SQLTypes.Double)))
        )
      ),
      Column(
        "obj2",
        SQLTypes.Struct,
        multiFields = List(Column("z", SQLTypes.Keyword), Column("n", SQLTypes.BigInt))
      ),
      Column(
        "items",
        SQLTypes.Array(SQLTypes.Struct),
        multiFields = List(Column("q", SQLTypes.Int), Column("d", SQLTypes.Date))
      )
    )
  )

  private val es8 = Some("8.18.3")

  private def resolved(sql: String): (Seq[SingleSearch], Seq[SetOperator]) =
    Parser(sql) match {
      case Right(m: MultiSearch) => (m.requests.map(_.update(Some(schema))), m.resolvedOperators)
      case other                 => fail(s"[$sql] expected a set operation, got $other")
    }

  private def readers(
    sql: String,
    version: => Option[String] = es8
  ): Seq[Option[Array[Any => Any]]] = {
    val (requests, operators) = resolved(sql)
    SetOperationValues.legReaders(requests, operators, version) match {
      case Right(legs)   => legs
      case Left(refusal) => fail(s"[$sql] refused: $refusal")
    }
  }

  /** A document's value as core receives it: its JSON, read by the response parser. */
  private def received(json: String): Any =
    ElasticConversion.jsonNodeToAny(JacksonConfig.objectMapper.readTree(json), ListMap.empty)

  /** `SELECT <a> AS k FROM t UNION ALL SELECT <b> AS k FROM t` */
  private def union(a: String, b: String, operator: String = "UNION ALL"): String =
    s"SELECT $a AS k FROM t $operator SELECT $b AS k FROM t"

  /** The reader of leg `leg`, column 1, of `sql`. */
  private def reader(sql: String, leg: Int = 0, version: => Option[String] = es8): Any => Any =
    readers(sql, version)(leg) match {
      case Some(functions) if functions(0) ne null => functions(0)
      case other                                   => fail(s"[$sql] leg $leg reads nothing: $other")
    }

  /** A value and its class -- bytes by their content. */
  private def shown(v: Any): (Any, Class[_]) =
    v match {
      case null           => (null, classOf[Null])
      case b: Array[Byte] => (b.toSeq, classOf[Array[Byte]])
      case other          => (other, other.getClass)
    }

  private def utc(s: String): ZonedDateTime = ZonedDateTime.parse(s)

  private def point(lat: Double, lon: Double) = ListMap("lat" -> lat, "lon" -> lon)

  "A leg's reader" should "read every value of the contract's table as Elasticsearch indexed it" in {
    // (field, the JSON written, the value read) -- each field beside itself, its branches agreeing
    val cases: Seq[(String, String, Any)] = Seq(
      ("c_es", "1706697000", utc("2024-01-31T10:30:00Z")),
      ("c_es", "\"1706697000.5\"", utc("2024-01-31T10:30:00.500Z")),
      ("c_c1", "\"2024/01/31\"", utc("2024-01-31T00:00:00Z")),
      ("c_c3", "\"31/01/2024 10:30\"", utc("2024-01-31T10:30:00Z")),
      ("c_def", "\"2024\"", utc("2024-01-01T00:00:00Z")),
      ("c_def", "2024", utc("2024-01-01T00:00:00Z")),
      ("c_dfmt", "\"31/01/2024 23:30\"", LocalDate.parse("2024-01-31")),
      ("c_des", "1706697000", LocalDate.parse("2024-01-31")),
      // a zone-less text is in UTC, whatever the JVM's zone
      ("ts", "\"2024-01-31T10:30:00\"", utc("2024-01-31T10:30:00Z")),
      ("d", "1706697000000", LocalDate.parse("2024-01-31")),
      ("n", "\"5.5\"", Integer.valueOf(5)),
      ("n", "\"-5.9\"", Integer.valueOf(-5)),
      ("x", "\"1e3\"", java.lang.Double.valueOf(1000.0)),
      ("x", "\" 2.5 \"", java.lang.Double.valueOf(2.5)),
      ("b", "\"\"", java.lang.Boolean.FALSE),
      ("geo", "\"48.85,2.35\"", point(48.85, 2.35)),
      ("geo", "[2.35,48.85]", point(48.85, 2.35)),
      ("geo", "\"POINT (2.35 48.85)\"", point(48.85, 2.35)),
      ("geo", "\"u09tv\"", point(48.8232421875, 2.3291015625))
    )
    val wrong = cases.flatMap { case (field, json, want) =>
      val got = Try(reader(union(field, field))(received(json)))
      if (got.map(shown).toOption.contains(shown(want))) None
      else Some(s"$field <- $json read $got instead of ${shown(want)}")
    }
    wrong shouldBe empty
    // a zone name: core's failure, naming the value, the field and the format
    intercept[IllegalArgumentException](
      reader(union("c_z", "c_z"))(received("\"2024-01-31T10:30:00 CET\""))
    ).getMessage shouldBe
    "Conversion Error: Could not read '2024-01-31T10:30:00 CET' as a TIMESTAMP when casting from " +
    "source column k: core cannot read it with the date format 'yyyy-MM-dd'T'HH:mm:ss z' of " +
    "field c_z as Elasticsearch 8 does: the zone 'CET' is a region id or a zone name, whose " +
    "offset comes from the zone data of the JVM reading it, not Elasticsearch's: core reads " +
    "only Z, an offset, UTC, GMT and UT"
  }

  it should "read each type in its ONE class, beside itself or not" in {
    val items =
      Seq("n", "bg", "sm", "ti", "x", "f", "b", "d", "ts", "t", "s", "bin", "obj", "geo", "arr")
    val row: Seq[String] = Seq(
      "5",
      "5",
      "5",
      "5",
      "5",
      "0.5",
      "true",
      "\"2024-01-31\"",
      "\"2024-01-31T12:30:00+02:00\"",
      "\"10:30:00\"",
      "\"a\"",
      "\"YWI=\"",
      """{"s":"x","n":1,"e":1706697000,"p":{"c":2}}""",
      """{"lat":48.85,"lon":2.35}""",
      "[1,2]"
    )
    val want: Seq[Any] = Seq(
      Integer.valueOf(5),
      java.lang.Long.valueOf(5L),
      java.lang.Short.valueOf(5.toShort),
      java.lang.Byte.valueOf(5.toByte),
      java.lang.Double.valueOf(5.0),
      java.lang.Float.valueOf(0.5f),
      java.lang.Boolean.TRUE,
      LocalDate.parse("2024-01-31"),
      utc("2024-01-31T10:30:00Z"),
      LocalTime.parse("10:30:00"),
      "a",
      "YWI=".getBytes(java.nio.charset.StandardCharsets.UTF_8),
      // the branch's fields, in its order
      ListMap(
        "e" -> utc("2024-01-31T10:30:00Z"),
        "n" -> Integer.valueOf(1),
        "p" -> ListMap("c" -> java.lang.Double.valueOf(2.0)),
        "s" -> "x"
      ),
      point(48.85, 2.35),
      List(Integer.valueOf(1), Integer.valueOf(2))
    )
    val select = items.mkString("SELECT ", ", ", " FROM t")
    val legs = readers(s"$select UNION ALL $select")
    legs should have size 2
    val wrong = for {
      leg                           <- legs
      functions                     <- leg.toSeq
      ((item, json), (read, value)) <- items.zip(row).zip(functions.toSeq.zip(want))
      got = shown(read(received(json)))
      if got != shown(value)
    } yield s"$item <- $json read $got instead of ${shown(value)}"
    wrong shouldBe empty
    // a struct's ListMap keeps its order; a TIMESTAMP is in UTC
    legs.head.get
      .apply(12)(received(row(12)))
      .asInstanceOf[ListMap[String, Any]]
      .keys
      .toSeq shouldBe
    Seq("e", "n", "p", "s")
    legs.head.get
      .apply(8)(received(row(8)))
      .asInstanceOf[ZonedDateTime]
      .getZone shouldBe ZoneOffset.UTC
    // beside another type, a leg is read as ITS branch declares it: the INT leg an Integer, the
    // DOUBLE leg a Double -- the relational engine casts them
    shown(reader(union("n", "x"), 0)(received("\"5.5\""))) shouldBe shown(Integer.valueOf(5))
    shown(reader(union("n", "x"), 1)(received("5"))) shouldBe shown(java.lang.Double.valueOf(5.0))
    shown(reader(union("n", "s"), 0)(received("5"))) shouldBe shown(Integer.valueOf(5))
    shown(reader(union("d", "ts"), 0)(received("\"2024-01-31\""))) shouldBe
    shown(LocalDate.parse("2024-01-31"))
    // a point beside an object: a point, {lat, lon}
    reader(union("geo", "obj2"), 0)(received("\"48.85,2.35\"")) shouldBe point(48.85, 2.35)
    // Elasticsearch's one-element array (`script_fields`) is ONE value, its empty array NULL
    shown(reader(union("n", "n"))(List(Integer.valueOf(3)))) shouldBe shown(Integer.valueOf(3))
    reader(union("n", "n"))(Nil) shouldBe (null: Any)
    reader(union("n", "n"))(null) shouldBe (null: Any)
  }

  it should "read a struct as its branch declares it, at every depth, where the branches agree" in {
    // obj beside obj: every field read, `e` with its own format, `n` truncated as ES indexed it
    reader(union("obj", "obj"))(
      received("""{"n":"5.5","e":"1706697000","p":{"c":"1e3"}}""")
    ) shouldBe
    ListMap(
      "e" -> utc("2024-01-31T10:30:00Z"),
      "n" -> 5,
      "p" -> ListMap("c" -> 1000.0),
      "s" -> null
    )
    // obj beside obj2, a merged struct: each leg its OWN fields, in its order, under its names
    reader(union("obj", "obj2"), 1)(received("""{"z":"a","n":"7"}""")) shouldBe
    ListMap("n" -> java.lang.Long.valueOf(7L), "z" -> "a")
    // a list of objects: each element its fields
    reader(union("items", "items"))(
      received("""[{"q":"2.5","d":"2024-01-31"},{"q":3}]""")
    ) shouldBe List(
      ListMap("d" -> LocalDate.parse("2024-01-31"), "q" -> 2),
      ListMap("d" -> null, "q"                          -> 3)
    )
    // one object stored bare is a list of one
    reader(union("items", "items"))(received("""{"q":4}""")) shouldBe
    List(ListMap("d" -> null, "q" -> 4))
  }

  it should "read a date as the Elasticsearch major the statement runs on reads it" in {
    val ts = union("c_es", "c_es")
    // a fraction of a second: Elasticsearch 8 and 7 keep its milliseconds, 6 truncates it
    reader(ts, version = Some("8.18.3"))(received("\"1706697000.5\"")) shouldBe
    utc("2024-01-31T10:30:00.500Z")
    reader(ts, version = Some("7.17.29"))(received("\"1706697000.5\"")) shouldBe
    utc("2024-01-31T10:30:00.500Z")
    reader(ts, version = Some("6.8.23"))(received("\"1706697000.5\"")) shouldBe
    utc("2024-01-31T10:30:00Z")
    // Elasticsearch 7 reads no negative epoch_millis, Elasticsearch 8 does
    reader(union("c_em", "c_em"))(received("\"-2024\"")) shouldBe utc("1969-12-31T23:59:57.976Z")
    intercept[IllegalArgumentException](
      reader(union("c_em", "c_em"), version = Some("7.17.29"))(received("\"-2024\""))
    ).getMessage shouldBe
    "Conversion Error: Could not read '-2024' as a TIMESTAMP when casting from source column k: " +
    "the date format 'epoch_millis' of field c_em does not read it as Elasticsearch 7 does"
    // the version not known: no date is guessed
    intercept[IllegalArgumentException](
      reader(ts, version = None)(received("1706697000"))
    ).getMessage shouldBe
    "Conversion Error: Could not read '1706697000' as a TIMESTAMP when casting from source column " +
    "k: the date format 'epoch_second' of field c_es reads it as the Elasticsearch version does, " +
    "which is not known"
    // the version asked ONCE per statement, and only when a reader reads a date field
    var asked = 0
    def version: Option[String] = { asked += 1; es8 }
    readers(s"SELECT c_es, ts, n FROM t UNION ALL SELECT c_c1, d, x FROM t", version)
    asked shouldBe 1
    readers(union("n", "x"), version)
    asked shouldBe 1
  }

  it should "fail, in core's words, on a value core cannot read" in {
    def failure(sql: String, json: String, leg: Int = 0): String =
      intercept[IllegalArgumentException](reader(sql, leg)(received(json))).getMessage
    failure(union("c_c1", "c_c1"), "\"31-01-2024\"") shouldBe
    "Conversion Error: Could not read '31-01-2024' as a TIMESTAMP when casting from source column " +
    "k: the date format 'yyyy/MM/dd' of field c_c1 does not read it as Elasticsearch 8 does"
    failure(union("geo", "geo"), "\"nowhere!\"") shouldBe
    "Conversion Error: Could not read 'nowhere!' as a GEO_POINT when casting from source column " +
    "k: a point is an object {lat, lon}, a text 'lat,lon', an array [lon, lat], a geohash or a " +
    "WKT POINT (lon lat)"
    failure(union("obj", "obj"), "\"x\"") shouldBe
    "Conversion Error: Could not read 'x' as a STRUCT when casting from source column k"
    // a field inside an object, named by its path
    failure(union("obj", "obj"), """{"e":"31-01-2024"}""") should include(
      "the date format 'epoch_second' of field obj.e does not read it"
    )
  }

  it should "fail a multi-valued value at a scalar position, where the branches agree too" in {
    def failure(sql: String, json: String, leg: Int = 0): String =
      intercept[IllegalArgumentException](reader(sql, leg)(received(json))).getMessage
    // where the branches agree: core's words for a converting position (DuckDB's)
    failure(union("s", "s"), """["a","b"]""") shouldBe
    "Conversion Error: Type VARCHAR can't be cast to the destination type VARCHAR[] when casting " +
    "from source column k"
    failure(union("n", "n"), "[1,2]") shouldBe
    "Conversion Error: Unimplemented type for cast (INTEGER -> INTEGER[]) when casting from " +
    "source column k"
    failure(union("c_es", "c_es"), "[1706697000,1706697001]") shouldBe
    "Conversion Error: Unimplemented type for cast (TIMESTAMP -> TIMESTAMP[]) when casting from " +
    "source column k"
    // inside an object
    failure(union("obj", "obj"), """{"n":[1,2]}""") shouldBe
    "Conversion Error: Unimplemented type for cast (INTEGER -> INTEGER[]) when casting from " +
    "source column k"
    // where they disagree: the same words as core's UNION ALL
    val (requests, _) = resolved(union("n", "x"))
    val converting = SetOperationValues.converters(MultiSearch(requests), es8)
    val core = intercept[IllegalArgumentException](converting.head.get.apply(0)(List(1, 2)))
    failure(union("n", "x"), "[1,2]") shouldBe core.getMessage
    core.getMessage shouldBe
    "Conversion Error: Unimplemented type for cast (DOUBLE -> INTEGER[]) when casting from source " +
    "column k"
    // a list position carries several values
    reader(union("arr", "arr"))(received("[1,2,3]")) shouldBe List(1, 2, 3)
  }

  it should "answer a date it cannot read as stored where told so, where the branches agree" in {
    def keeping(sql: String, leg: Int = 0, version: Option[String] = es8): Any => Any = {
      val (requests, operators) = resolved(sql)
      SetOperationValues.legReaders(
        requests,
        operators,
        version,
        SetOperationValues.Unreadable.KeepAsStored
      ) match {
        case Right(legs)   => legs(leg).get.apply(0)
        case Left(refusal) => fail(s"[$sql] refused: $refusal")
      }
    }
    val cet = "2024-01-31T10:30:00 CET"
    // a zone name: the value as stored, exactly -- Elasticsearch's array too
    keeping(union("c_z", "c_z"))(received(s"\"$cet\"")) shouldBe cet
    keeping(union("c_z", "c_z"))(received(s"[\"$cet\"]")) shouldBe List(cet)
    // a date it reads is read
    shown(keeping(union("c_z", "c_z"))(received("\"2024-01-31T10:30:00 UTC\""))) shouldBe
    shown(utc("2024-01-31T10:30:00Z"))
    // inside an object whose branches agree
    keeping(union("oz", "oz"))(received(s"""{"z":"$cet"}""")) shouldBe ListMap("z" -> cet)
    // the version not known: as stored
    keeping(union("c_es", "c_es"), version = None)(received("1706697000")) shouldBe 1706697000
    // beside a SELECT * branch the column converts nothing, as core's UNION ALL reads it
    keeping(s"${union("c_z", "c_z")} UNION ALL SELECT * FROM t")(received(s"\"$cet\"")) shouldBe
    cet
    // where the branches disagree (a TIMESTAMP beside a DATE) it fails, as core's UNION ALL does
    intercept[IllegalArgumentException](
      keeping(union("c_z", "d"))(received(s"\"$cet\""))
    ).getMessage should include("the zone 'CET' is a region id or a zone name")
    // other failures are not dates: they fail
    intercept[IllegalArgumentException](
      keeping(union("geo", "geo"))(received("\"nowhere!\""))
    ).getMessage should startWith("Conversion Error: Could not read 'nowhere!' as a GEO_POINT")
    intercept[IllegalArgumentException](keeping(union("n", "n"))(received("[1,2]")))
    // the default fails
    intercept[IllegalArgumentException](reader(union("c_z", "c_z"))(received(s"\"$cet\"")))
  }

  it should "read nothing where nothing is typed: ANY, SELECT *, a NULL literal" in {
    // a NULL literal: its slot is empty
    readers(union("n", "NULL")).map(_.map(_.toSeq.map(_ ne null))) shouldBe
    Seq(Some(Seq(true)), None)
    // SELECT *: the leg reads nothing
    readers("SELECT n FROM t UNION ALL SELECT * FROM t").map(_.isDefined) shouldBe Seq(true, false)
    // a column whose type is not known: nothing read, in any leg
    val (typed, _) = resolved(union("n", "n"))
    val unknown = Parser(union("n", "n")) match {
      case Right(m: MultiSearch) => m.requests
      case other                 => fail(s"$other")
    }
    SetOperationValues.legReaders(Seq(typed.head, unknown(1)), Nil, es8) shouldBe
    Right(Seq(None, None))
    // a refused statement: its refusal, word for word
    val (refused, operators) = resolved(union("d", "n"))
    SetOperationValues.legReaders(refused, operators, es8) shouldBe
    MultiSearch.branchTypes(refused, operators).map(_ => Nil)
  }

  it should "read every leg of INTERSECT and EXCEPT as declared, whatever the operator" in {
    Seq("UNION", "INTERSECT", "EXCEPT ALL").foreach { operator =>
      shown(reader(union("c_es", "ts", operator))(received("1706697000"))) shouldBe
      shown(utc("2024-01-31T10:30:00Z"))
      shown(reader(union("n", "x", operator))(received("\"5.5\""))) shouldBe
      shown(Integer.valueOf(5))
    }
  }
}
