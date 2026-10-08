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
import app.softnetwork.elastic.client.result.{
  ElasticFailure,
  ElasticSuccess,
  QueryRows,
  QueryStream,
  QueryStructured
}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.query.SelectStatement
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes, ValueCoercion}
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.time.{Instant, LocalDate, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.Try

/** A `UNION ALL` answers what DuckDB 1.5.5 answers where DuckDB types the column (the relational
  * engine that runs `UNION`, `INTERSECT` and `EXCEPT` is DuckDB, so its answer is the reference),
  * `VARCHAR` where DuckDB gives the column no type, and refuses only a date beside a number -- the
  * lead's rules of 2026-10-07.
  *
  * For every statement: the VERDICT (answered, or refused by name), the VALUES, the CLASS of every
  * row's value -- ONE class per column: every row of a `DOUBLE` column is a `Double`, of a `BIGINT`
  * one a `Long`, of a `VARCHAR` one a `String` -- and the type JDBC and Arrow REPORT, which they
  * read from the column's first value. Each statement runs through the three routes a `UNION ALL`
  * takes: the gateway (`run`), the client's `search` (un-LIMITed legs, executed leg by leg) and
  * branches bounded by a `LIMIT` (one `_msearch`).
  *
  * Every expectation is computed HERE, from the fixture, as DuckDB 1.5.5 computes it (MEASURED
  * statement by statement over the same rows) -- never read from the engine. PostgreSQL 16 refuses
  * most of the text pairs (`UNION types text and integer cannot be matched`); DuckDB comes first.
  *
  * A computed branch (a literal, `CURRENT_DATE`, a `CAST`) reaches the client as Elasticsearch's
  * one-element `script_fields` array, on every path and in a single statement alike; a column whose
  * branches already project one type is not converted, so the value is read here through that array
  * (`scalar`), as every other spec of this kit reads a computed column.
  */
trait SetOperationTypeSpec extends AnyFlatSpecLike with ElasticDockerTestKit with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val table = "set_operation_types"

  /** One document: `None` is a column it does not carry. */
  private final case class Doc(
    id: String,
    s: Option[String],
    txt: Option[String],
    n: Option[Int],
    x: Option[Double],
    f: Option[Float],
    bg: Option[Long],
    sm: Option[Short],
    d: Option[LocalDate],
    d2: Option[LocalDate],
    ts: Option[Instant],
    b: Option[Boolean]
  )

  private def day(s: String): Option[LocalDate] = Some(LocalDate.parse(s))
  private def at(s: String): Option[Instant] = Some(Instant.parse(s))

  private val docs: Seq[Doc] = Seq(
    Doc(
      "r1",
      Some("1"),
      Some("hello one"),
      Some(3),
      Some(0.25),
      Some(0.5f),
      Some(3000000000L),
      Some(3.toShort),
      day("2024-01-31"),
      day("2024-02-01"),
      at("2024-01-31T10:30:00Z"),
      Some(true)
    ),
    Doc(
      "r2",
      Some("5"),
      Some("hello two"),
      Some(5),
      Some(1.5),
      Some(1.25f),
      Some(5L),
      Some(5.toShort),
      day("2024-03-01"),
      day("2024-02-28"),
      at("2024-03-01T06:00:00Z"),
      Some(false)
    ),
    Doc(
      "r3",
      Some("12"),
      None,
      Some(-2),
      None,
      None,
      Some(-2L),
      Some(-2.toShort),
      day("2023-12-25"),
      None,
      at("2023-12-25T23:59:59.999Z"),
      None
    ),
    Doc(
      "r4",
      None,
      Some("hello four"),
      None,
      Some(2.0),
      Some(2.0f),
      None,
      None,
      None,
      day("2024-01-31"),
      None,
      Some(true)
    ),
    Doc(
      "r5",
      Some("3"),
      Some("hello five"),
      Some(10),
      Some(-1.25),
      Some(-0.75f),
      Some(10L),
      Some(10.toShort),
      day("2024-02-29"),
      day("2024-02-29"),
      at("2024-02-29T00:00:00Z"),
      Some(false)
    )
  )

  private def json(d: Doc): String = {
    def q(s: String) = "\"" + s + "\""
    val fields = Seq(
      Some("id" -> q(d.id)),
      d.s.map(v => "s" -> q(v)),
      d.txt.map(v => "txt" -> q(v)),
      d.n.map(v => "n" -> v.toString),
      d.x.map(v => "x" -> v.toString),
      d.f.map(v => "f" -> v.toString),
      d.bg.map(v => "bg" -> v.toString),
      d.sm.map(v => "sm" -> v.toString),
      d.d.map(v => "d" -> q(v.toString)),
      d.d2.map(v => "d2" -> q(v.toString)),
      d.ts.map(v => "ts" -> q(v.toString)),
      d.b.map(v => "b" -> v.toString)
    ).flatten
    fields.map { case (k, v) => s""""$k":$v""" }.mkString("{", ",", "}")
  }

  /** The values a `VARCHAR` column spells: each as its JSON, and DuckDB 1.5.5's text for it --
    * `CAST(<value> AS VARCHAR)`, MEASURED value by value, the timestamp read in UTC as the
    * relational engine loads it. `ts` holds a date-only value, one before the epoch, fractional
    * seconds and an offset; `x` a whole number written without a fraction.
    */
  private final case class Spelled(json: String, text: String)

  private final case class TextDoc(
    id: String,
    s: Option[String],
    x: Option[Spelled],
    f: Option[Spelled],
    ts: Option[Spelled],
    time: Option[String],
    date: Option[String]
  )

  private val textTable = "set_operation_text"

  private def sp(json: String, text: String): Option[Spelled] = Some(Spelled(json, text))

  private val textDocs: Seq[TextDoc] = Seq(
    TextDoc(
      "t1",
      Some("a"),
      sp("2.5E11", "250000000000.0"),
      sp("0.1", "0.1"),
      sp("\"2024-01-31T10:30:00Z\"", "2024-01-31 10:30:00"),
      Some("10:30:00"),
      Some("2024-01-31")
    ),
    TextDoc(
      "t2",
      Some("b"),
      sp("1.0E-5", "1e-05"),
      sp("-0.75", "-0.75"),
      sp("\"2023-12-25T23:59:59.999Z\"", "2023-12-25 23:59:59.999"),
      Some("23:59:59.999"),
      Some("2023-12-25")
    ),
    TextDoc(
      "t3",
      Some("c"),
      sp("1.0E16", "1e+16"),
      sp("2.5E11", "250000000000.0"),
      sp("\"2024-01-31\"", "2024-01-31 00:00:00"),
      Some("00:00:00"),
      Some("2024-01-31")
    ),
    TextDoc(
      "t4",
      None,
      sp("-2.5", "-2.5"),
      sp("4073260.75", "4073260.75"),
      sp("\"1969-12-31T23:59:59.999Z\"", "1969-12-31 23:59:59.999"),
      Some("23:59:59.999"),
      Some("1969-12-31")
    ),
    TextDoc("t5", Some("e"), sp("5", "5.0"), sp("16777216", "16777216.0"), None, None, None),
    TextDoc(
      "t6",
      Some("f"),
      sp("7.168E25", "7.1680000000000004e+25"),
      sp("0.5", "0.5"),
      sp("\"2024-03-01T06:00:00.5Z\"", "2024-03-01 06:00:00.5"),
      Some("06:00:00.5"),
      Some("2024-03-01")
    ),
    TextDoc(
      "t7",
      Some("g"),
      sp("-1.0E-5", "-1e-05"),
      None,
      sp("\"2024-02-29T00:00:00Z\"", "2024-02-29 00:00:00"),
      Some("00:00:00"),
      Some("2024-02-29")
    ),
    TextDoc(
      "t8",
      Some("h"),
      sp("0.30000000000000004", "0.30000000000000004"),
      sp("1.25", "1.25"),
      sp("\"2024-01-31T12:30:00+02:00\"", "2024-01-31 10:30:00"),
      Some("10:30:00"),
      Some("2024-01-31")
    )
  )

  /** `bin`, a `binary` field -- the base64 Elasticsearch holds -- by document. */
  private val binaryOf: Map[String, String] = Map(
    "t1" -> "YWI=",
    "t2" -> "eA==",
    "t4" -> "AAEC",
    "t5" -> "aGVsbG8=",
    "t7" -> "/w==",
    "t8" -> "YQ=="
  )

  /** `u`, a keyword with a character outside ASCII, which DuckDB cannot cast to a BLOB. */
  private val nonAsciiOf: Map[String, String] = Map("t1" -> "\u00e9t\u00e9", "t3" -> "a")

  /** `tags`, a keyword holding several values in one document (its JSON, an array). */
  private val tagsOf: Map[String, String] = Map("t1" -> "[\"a\",\"b\"]", "t2" -> "\"c\"")

  /** `geo`, a `geo_point` written as `"lat,lon"` text, and `items`, a list of objects (a `nested`
    * field, an `ARRAY<STRUCT>`), by document.
    */
  private val geoOf: Map[String, String] = Map("t1" -> "48.85,2.35", "t2" -> "40.71,-74.0")

  /** The same points stored as an object (`geoo`) and as an array `[lon, lat]` (`geoa`), and `obj`,
    * an object, by document: the JSON each holds.
    */
  private val geoObjectOf: Map[String, String] =
    Map("t1" -> """{"lat":48.85,"lon":2.35}""", "t2" -> """{"lat":40.71,"lon":-74.0}""")
  private val geoArrayOf: Map[String, String] =
    Map("t1" -> "[2.35,48.85]", "t2" -> "[-74.0,40.71]")
  private val objOf: Map[String, (Int, String)] = Map("t1" -> (1 -> "x"), "t3" -> (3 -> "z"))

  /** `oi`, `od` and `ov`, three objects with one field `a` -- an `INT`, a `DATE` and a `KEYWORD`
    * -- by document: the JSON each holds.
    */
  private val fieldOf: Map[String, Map[String, String]] = Map(
    "oi" -> Map("t1" -> """{"a":1}"""),
    "od" -> Map("t1" -> """{"a":"2024-01-31"}"""),
    "ov" -> Map("t1" -> """{"a":"x"}""")
  )

  /** The value generator's types on Elasticsearch, each as declared, at four depths: `c_<type>` a
    * column, `f_<type>` an object's field `a`, `n_<type>` the field `a` of a list of objects'
    * element (a list of values is read without a type today), `s_<type>` the field `c` of an
    * object's field `p`.
    */
  private val generatedTypes: Seq[(String, String)] = Seq(
    "INT"       -> "INT",
    "BIGINT"    -> "BIGINT",
    "DOUBLE"    -> "DOUBLE",
    "BOOLEAN"   -> "BOOLEAN",
    "DATE"      -> "DATE",
    "TIMESTAMP" -> "TIMESTAMP",
    "VARBINARY" -> "VARBINARY",
    "VARCHAR"   -> "KEYWORD"
  )

  /** Each type's values, by document, as the JSON written -- in the forms Elasticsearch 8.18.3
    * accepts and core reads differently (MEASURED): a `BIGINT` or a `DOUBLE` whose JSON fits an int
    * (an `Integer`); a number written as text, which Elasticsearch indexes as its field's type --
    * truncated toward zero in an integer field (`"5.5"`, `5.5` are 5), an exponent applied (`"5e2"`
    * is 500), a space trimmed in an `INT` field; `"true"`, `"false"` and an empty text (`false`) in
    * a `BOOLEAN` field; a `TIMESTAMP` written as a date (a `LocalDate`), without a zone (a
    * `LocalDateTime`), with one or an offset (a `ZonedDateTime`), or as epoch milliseconds, a
    * number (a `Long`) or a text; a `DATE` written as epoch milliseconds or with a time.
    */
  private val storedOf: Map[String, Map[String, String]] = Map(
    "INT" -> Map("t1" -> "1", "t2" -> "-2", "t3" -> "\"5\"", "t4" -> "\" 5 \"", "t5" -> "\"5.5\""),
    "BIGINT" -> Map(
      "t1" -> "1",
      "t2" -> "3000000000",
      "t3" -> "\"5\"",
      "t4" -> "\"5.0\"",
      "t5" -> "\"5e2\"",
      "t6" -> "5.5",
      "t7" -> "\"-5.9\""
    ),
    "DOUBLE" -> Map(
      "t1" -> "2",
      "t2" -> "2.5",
      "t3" -> "\"2.5\"",
      "t4" -> "\"1e3\"",
      "t5" -> "\" 2.5 \"",
      "t6" -> "\"5\""
    ),
    "BOOLEAN" -> Map(
      "t1" -> "true",
      "t2" -> "false",
      "t3" -> "\"true\"",
      "t4" -> "\"false\"",
      "t5" -> "\"\""
    ),
    "DATE" -> Map(
      "t1" -> "\"2024-01-31\"",
      "t2" -> "\"2023-12-25\"",
      "t3" -> "\"1706697000000\"",
      "t4" -> "1706697000000",
      "t5" -> "\"2024-01-31T10:30:00Z\""
    ),
    "TIMESTAMP" -> Map(
      "t1" -> "\"2024-01-31\"",
      "t2" -> "\"2024-01-31T10:30:00\"",
      "t3" -> "\"2024-01-31T10:30:00Z\"",
      "t4" -> "1706697000000",
      "t5" -> "\"2024-01-31T12:30:00+02:00\"",
      "t6" -> "\"1706697000000\""
    ),
    "VARBINARY" -> Map("t1" -> "\"YWI=\"", "t2" -> "\"eA==\""),
    "VARCHAR"   -> Map("t1" -> "\"x\"", "t2" -> "\"y\"")
  )

  private def generatedJson(id: String): Seq[(String, String)] =
    generatedTypes.flatMap { case (t, _) =>
      storedOf(t).get(id).toSeq.flatMap { v =>
        Seq(
          s"c_$t" -> v,
          s"f_$t" -> s"""{"a":$v}""",
          s"n_$t" -> s"""[{"a":$v}]""",
          s"s_$t" -> s"""{"p":{"c":$v}}"""
        )
      }
    }

  /** Each point, `{lat, lon}`, by document -- every storage holds the same two. */
  private val pointOf: Map[String, (Double, Double)] =
    Map("t1" -> (48.85 -> 2.35), "t2" -> (40.71 -> -74.0))
  private val itemsOf: Map[String, String] =
    Map("t1" -> """[{"p":"a","q":1}]""", "t2" -> """[{"p":"b","q":2},{"p":"c","q":3}]""")

  /** `kw`, keywords that LOOK like a time and a timestamp, which core re-reads as such (#418). */
  private val temporalKeywordOf: Map[String, String] =
    Map("t1" -> "10:30:00", "t2" -> "2024-01-31T10:30:00Z", "t3" -> "plain")

  private def textJson(d: TextDoc): String =
    Seq(
      Some("id" -> ("\"" + d.id + "\"")),
      d.s.map(v => "s" -> ("\"" + v + "\"")),
      d.x.map(v => "x" -> v.json),
      d.f.map(v => "f" -> v.json),
      d.ts.map(v => "ts" -> v.json),
      binaryOf.get(d.id).map(v => "bin" -> ("\"" + v + "\"")),
      nonAsciiOf.get(d.id).map(v => "u" -> ("\"" + v + "\"")),
      tagsOf.get(d.id).map(v => "tags" -> v),
      temporalKeywordOf.get(d.id).map(v => "kw" -> ("\"" + v + "\"")),
      geoOf.get(d.id).map(v => "geo" -> ("\"" + v + "\"")),
      itemsOf.get(d.id).map(v => "items" -> v),
      geoObjectOf.get(d.id).map(v => "geoo" -> v),
      geoArrayOf.get(d.id).map(v => "geoa" -> v),
      objOf.get(d.id).map { case (a, b) => "obj" -> s"""{"a":$a,"b":"$b"}""" },
      fieldOf("oi").get(d.id).map(v => "oi" -> v),
      fieldOf("od").get(d.id).map(v => "od" -> v),
      fieldOf("ov").get(d.id).map(v => "ov" -> v)
    ).flatten
      .++(generatedJson(d.id))
      .map { case (k, v) => s""""$k":$v""" }
      .mkString("{", ",", "}")

  /** `f_<key>` / `c_<key>`: a date field of a mapping format, as a column and as an object's field
    * `a`, and the values written to it by document (JSON), each with the instant Elasticsearch
    * indexed -- MEASURED on 6.8.23, 7.17.29 and 8.18.3 (core's date-format corpora); `es6` the
    * instant where Elasticsearch 6 indexes another one. `None` is Elasticsearch's default format.
    */
  private final case class Formatted(
    key: String,
    declared: String,
    format: Option[String],
    values: Map[String, (String, Instant, Map[Int, Instant])]
  )

  private val formatTable = "set_operation_formats"

  private val formatted: Seq[Formatted] = {
    val at1030 = Instant.parse("2024-01-31T10:30:00Z")
    val midnight = Instant.parse("2024-01-31T00:00:00Z")
    val year2024 = Instant.parse("2024-01-01T00:00:00Z")
    // the instant Elasticsearch 8 (and 9) indexed, and the majors that indexed another one
    def one(json: String, at: Instant): (String, Instant, Map[Int, Instant]) = (json, at, Map.empty)
    Seq(
      // seconds, not milliseconds; a fraction kept to the millisecond, truncated to the second
      // by Elasticsearch 6
      Formatted(
        "es",
        "TIMESTAMP",
        Some("epoch_second"),
        Map(
          "f1" -> one("1706697000", at1030),
          "f2" -> ("\"1706697000.5\"", Instant.parse("2024-01-31T10:30:00.500Z"), Map(6 -> at1030)),
          "f3" -> one("-86400", Instant.parse("1969-12-31T00:00:00Z"))
        )
      ),
      Formatted(
        "em",
        "TIMESTAMP",
        Some("epoch_millis"),
        Map(
          "f1" -> one("\"1706697000000\"", at1030),
          "f2" -> one("1706697000123", Instant.parse("2024-01-31T10:30:00.123Z"))
        )
      ),
      Formatted(
        "c1",
        "TIMESTAMP",
        Some("yyyy/MM/dd"),
        Map("f1" -> one("\"2024/01/31\"", midnight))
      ),
      Formatted(
        "c3",
        "TIMESTAMP",
        Some("dd/MM/yyyy HH:mm"),
        Map("f1" -> one("\"31/01/2024 10:30\"", at1030))
      ),
      Formatted(
        "c4",
        "TIMESTAMP",
        Some("yyyy-MM-dd HH:mm:ss Z"),
        Map("f1" -> one("\"2024-01-31 12:30:00 +0200\"", at1030))
      ),
      Formatted(
        "c7",
        "TIMESTAMP",
        Some("MMM dd, yyyy"),
        Map("f1" -> one("\"Jan 31, 2024\"", midnight))
      ),
      Formatted(
        "bd",
        "TIMESTAMP",
        Some("basic_date"),
        Map("f1" -> one("\"20240131\"", midnight), "f2" -> one("20240131", midnight))
      ),
      Formatted(
        "sdt",
        "TIMESTAMP",
        Some("strict_date_time"),
        Map(
          "f1" -> one(
            "\"2024-01-31T12:30:00.123+02:00\"",
            Instant.parse("2024-01-31T10:30:00.123Z")
          )
        )
      ),
      // a year of one to nine digits; a fraction kept to the millisecond
      Formatted(
        "dot",
        "TIMESTAMP",
        Some("date_optional_time"),
        Map(
          "f1" -> one("\"2024-01-31T10:30:00.123456Z\"", Instant.parse("2024-01-31T10:30:00.123Z")),
          "f2" -> one("\"24\"", Instant.parse("0024-01-01T00:00:00Z"))
        )
      ),
      // the first alternative that parses decides
      Formatted(
        "l3",
        "TIMESTAMP",
        Some("yyyy-dd-MM||strict_date_optional_time"),
        Map(
          "f1" -> one("\"2024-01-02\"", Instant.parse("2024-02-01T00:00:00Z")),
          "f2" -> one("\"2024-01-31T10:30:00Z\"", at1030)
        )
      ),
      Formatted(
        "l2",
        "TIMESTAMP",
        Some("epoch_second||yyyy-MM-dd"),
        Map("f1" -> one("\"2024-01-31\"", midnight), "f2" -> one("1706697000", at1030))
      ),
      // Elasticsearch's default format reads four digits as a year, before epoch milliseconds
      Formatted(
        "def",
        "TIMESTAMP",
        None,
        Map(
          "f1" -> one("\"2024\"", year2024),
          "f2" -> one("2024", year2024),
          "f3" -> one("\"2024-01-31T12:30:00+0200\"", at1030),
          "f4" -> one("\"2024-01-31T10:30:00.123456Z\"", Instant.parse("2024-01-31T10:30:00.123Z"))
        )
      ),
      // a week-based year (MEASURED): Elasticsearch 8 reads the first day of its first week, weeks
      // starting on Sunday -- the month and the day ignored; Elasticsearch 7 the same with ISO weeks;
      // Elasticsearch 6's (Joda) `Y` is the year of era
      Formatted(
        "wy",
        "TIMESTAMP",
        Some("YYYY-MM-dd"),
        Map(
          "f1" -> (
            "\"2024-01-31\"",
            Instant.parse("2023-12-31T00:00:00Z"),
            Map(7 -> Instant.parse("2024-01-01T00:00:00Z"), 6 -> midnight)
          ),
          "f2" -> (
            "\"2021-01-01\"",
            Instant.parse("2020-12-27T00:00:00Z"),
            Map(
              7 -> Instant.parse("2021-01-04T00:00:00Z"),
              6 -> Instant.parse("2021-01-01T00:00:00Z")
            )
          )
        )
      ),
      // a day's name (MEASURED): Elasticsearch 7 and 8 ignore it beside a year of era, Elasticsearch 6
      // moves the date to that day of its week
      Formatted(
        "dn",
        "TIMESTAMP",
        Some("yyyy-MM-dd EEE"),
        Map(
          "f1" -> one("\"2024-01-31 Wed\"", midnight),
          "f2" -> (
            "\"2024-01-31 Mon\"",
            midnight,
            Map(6 -> Instant.parse("2024-01-29T00:00:00Z"))
          )
        )
      ),
      // a DATE field is the day of the instant, in UTC
      Formatted(
        "dfmt",
        "DATE",
        Some("dd/MM/yyyy HH:mm"),
        Map("f1" -> one("\"31/01/2024 23:30\"", Instant.parse("2024-01-31T23:30:00Z")))
      ),
      Formatted(
        "des",
        "DATE",
        Some("epoch_second"),
        Map("f1" -> one("1706697000", at1030))
      )
    )
  }

  /** The documents of [[formatTable]]: `f1`..`f4` carry the formatted fields, `p1` the partners --
    * `pd` a `DATE`, `pts` a `TIMESTAMP`, `s` a keyword, as columns and as objects' field `a`.
    */
  private val formatDocs: Seq[String] = Seq("f1", "f2", "f3", "f4", "p1")

  private def formatJson(id: String): String =
    (Seq("id" -> ("\"" + id + "\"")) ++
      formatted.flatMap(f =>
        f.values.get(id).toSeq.flatMap { case (v, _, _) =>
          Seq(s"c_${f.key}" -> v, s"f_${f.key}" -> s"""{"a":$v}""")
        }
      ) ++ (if (id == "p1")
              Seq(
                "pd"  -> "\"2024-02-01\"",
                "pts" -> "\"2024-02-01T00:00:00Z\"",
                "s"   -> "\"x\"",
                "fd"  -> """{"a":"2024-02-01"}""",
                "fts" -> """{"a":"2024-02-01T00:00:00Z"}""",
                "fs"  -> """{"a":"x"}"""
              )
            else Nil))
      .map { case (k, v) => s""""$k":$v""" }
      .mkString("{", ",", "}")

  override def beforeAll(): Unit = {
    super.beforeAll()
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $table"), 60.seconds))
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $textTable"), 60.seconds))
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $formatTable"), 60.seconds))
    run(
      s"CREATE TABLE $table (id KEYWORD, s KEYWORD, txt TEXT, n INT, x DOUBLE, f REAL, " +
      "bg BIGINT, sm SMALLINT, d DATE, d2 DATE, ts TIMESTAMP, b BOOLEAN, PRIMARY KEY (id))"
    )
    run(
      s"CREATE TABLE $textTable (id KEYWORD, s KEYWORD, x DOUBLE, f REAL, ts TIMESTAMP, " +
      "bin VARBINARY, u KEYWORD, tags KEYWORD, kw KEYWORD, geo GEO_POINT, " +
      "items ARRAY<STRUCT> FIELDS(p KEYWORD, q INT), geoo GEO_POINT, geoa GEO_POINT, " +
      "obj STRUCT FIELDS(a INT, b KEYWORD), oi STRUCT FIELDS(a INT), od STRUCT FIELDS(a DATE), " +
      "ov STRUCT FIELDS(a KEYWORD), " +
      generatedTypes
        .map { case (t, declared) =>
          s"c_$t $declared, f_$t STRUCT FIELDS(a $declared), " +
            s"n_$t ARRAY<STRUCT> FIELDS(a $declared), s_$t STRUCT FIELDS(p STRUCT FIELDS(c $declared))"
        }
        .mkString(", ") +
      ", PRIMARY KEY (id))"
    )
    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    def load(index: String, rows: List[String]): Unit = {
      implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = index, logEvery = 100)
      client.bulk[String](rows, identity, idKey = Some(Set("id"))) match {
        case ElasticSuccess(r) if r.failedCount == 0 => client.refresh(index)
        case other                                   => fail(s"bulk failed: $other")
      }
    }
    load(table, docs.map(json).toList)
    load(textTable, textDocs.map(textJson).toList)
    run(
      s"CREATE TABLE $formatTable (id KEYWORD, pd DATE, pts TIMESTAMP, s KEYWORD, " +
      "fd STRUCT FIELDS(a DATE), fts STRUCT FIELDS(a TIMESTAMP), fs STRUCT FIELDS(a KEYWORD), " +
      formatted
        .map { f =>
          val options = f.format.fold("")(spec => s" OPTIONS (format = '$spec')")
          s"c_${f.key} ${f.declared}$options, f_${f.key} STRUCT FIELDS(a ${f.declared}$options)"
        }
        .mkString(", ") +
      ", PRIMARY KEY (id))"
    )
    load(formatTable, formatDocs.map(formatJson).toList)
    ()
  }

  override def afterAll(): Unit = {
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $table"), 60.seconds))
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $textTable"), 60.seconds))
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $formatTable"), 60.seconds))
    super.afterAll()
  }

  private def run(sql: String): Unit =
    Await.result(client.run(sql), 60.seconds) match {
      case ElasticSuccess(_)     => ()
      case ElasticFailure(error) => fail(s"[$sql] failed: ${error.message}")
    }

  // -------------------------------------------------------------------------------------------
  // The oracle
  // -------------------------------------------------------------------------------------------

  /** The class DuckDB's type is read as, and the type JDBC / Arrow report for it. */
  private final case class ColumnType(name: String, cls: Class[_], reported: SQLType)

  private val INTEGER = ColumnType("INTEGER", classOf[java.lang.Integer], SQLTypes.Int)
  private val BIGINT = ColumnType("BIGINT", classOf[java.lang.Long], SQLTypes.BigInt)
  private val DOUBLE = ColumnType("DOUBLE", classOf[java.lang.Double], SQLTypes.Double)
  private val FLOAT = ColumnType("FLOAT", classOf[java.lang.Float], SQLTypes.Real)
  private val VARCHAR = ColumnType("VARCHAR", classOf[String], SQLTypes.Varchar)
  private val DATE = ColumnType("DATE", classOf[LocalDate], SQLTypes.Date)
  private val TIMESTAMP = ColumnType("TIMESTAMP", classOf[ZonedDateTime], SQLTypes.Timestamp)
  private val BOOLEAN = ColumnType("BOOLEAN", classOf[java.lang.Boolean], SQLTypes.Boolean)
  private val VARBINARY = ColumnType("BLOB", classOf[Array[Byte]], SQLTypes.VarBinary)

  /** A branch: its SELECT item, and its value for each document in the COLUMN's type. */
  private final case class Branch(item: String, value: Doc => Any)

  private def branch(item: String)(value: Doc => Any): Branch = Branch(item, value)

  private def opt(o: Option[Any]): Any = o.orNull

  private def midnight(d: LocalDate): ZonedDateTime = d.atStartOfDay(ZoneOffset.UTC)

  private def utc(i: Instant): ZonedDateTime = i.atZone(ZoneOffset.UTC)

  private def today: LocalDate = LocalDate.now(ZoneOffset.UTC)

  /** One statement: its branches, and the type DuckDB gives its column. */
  private final case class Case(id: String, branches: Seq[Branch], column: ColumnType) {
    def sql(limit: Boolean): String =
      branches
        .map(b => s"SELECT ${b.item} AS k FROM $table" + (if (limit) " LIMIT 100" else ""))
        .mkString(" UNION ALL ")
    def expected: Seq[Any] = branches.flatMap(b => docs.map(b.value))
  }

  private val cases: Seq[Case] = Seq(
    // the brief's pairs DuckDB answers
    Case(
      "d, CURRENT_DATE",
      Seq(branch("d")(r => opt(r.d)), branch("CURRENT_DATE")(_ => today)),
      DATE
    ),
    Case(
      "n, '1'",
      Seq(branch("n")(r => opt(r.n.map(_.toString))), branch("'1'")(_ => "1")),
      VARCHAR
    ),
    Case(
      "d, '2024-01-31'",
      Seq(branch("d")(r => opt(r.d.map(_.toString))), branch("'2024-01-31'")(_ => "2024-01-31")),
      VARCHAR
    ),
    Case("id, n", Seq(branch("id")(_.id), branch("n")(r => opt(r.n.map(_.toString)))), VARCHAR),
    Case("1, 'a'", Seq(branch("1")(_ => "1"), branch("'a'")(_ => "a")), VARCHAR),
    Case(
      "d, ts",
      Seq(branch("d")(r => opt(r.d.map(midnight))), branch("ts")(r => opt(r.ts.map(utc)))),
      TIMESTAMP
    ),
    Case(
      "ts, CAST(d AS DATE)",
      Seq(
        branch("ts")(r => opt(r.ts.map(utc))),
        branch("CAST(d AS DATE)")(r => opt(r.d.map(midnight)))
      ),
      TIMESTAMP
    ),
    Case("d, d2", Seq(branch("d")(r => opt(r.d)), branch("d2")(r => opt(r.d2))), DATE),
    Case(
      "n, x",
      Seq(branch("n")(r => opt(r.n.map(_.toDouble))), branch("x")(r => opt(r.x))),
      DOUBLE
    ),
    Case(
      "n, CAST(n AS BIGINT)",
      Seq(
        branch("n")(r => opt(r.n.map(_.toLong))),
        branch("CAST(n AS BIGINT)")(r => opt(r.n.map(_.toLong)))
      ),
      BIGINT
    ),
    Case("n, 2", Seq(branch("n")(r => opt(r.n)), branch("2")(_ => 2)), INTEGER),
    Case("s, id", Seq(branch("s")(r => opt(r.s)), branch("id")(_.id)), VARCHAR),
    // the other way round
    Case(
      "'1', n",
      Seq(branch("'1'")(_ => "1"), branch("n")(r => opt(r.n.map(_.toString)))),
      VARCHAR
    ),
    Case(
      "ts, d",
      Seq(branch("ts")(r => opt(r.ts.map(utc))), branch("d")(r => opt(r.d.map(midnight)))),
      TIMESTAMP
    ),
    Case(
      "x, n",
      Seq(branch("x")(r => opt(r.x)), branch("n")(r => opt(r.n.map(_.toDouble)))),
      DOUBLE
    ),
    Case(
      "CAST(n AS BIGINT), n",
      Seq(
        branch("CAST(n AS BIGINT)")(r => opt(r.n.map(_.toLong))),
        branch("n")(r => opt(r.n.map(_.toLong)))
      ),
      BIGINT
    ),
    // a NULL literal takes the other branch's type
    Case("n, NULL", Seq(branch("n")(r => opt(r.n)), branch("NULL")(_ => null)), INTEGER),
    Case(
      "n, NULL, x",
      Seq(
        branch("n")(r => opt(r.n.map(_.toDouble))),
        branch("NULL")(_ => null),
        branch("x")(r => opt(r.x))
      ),
      DOUBLE
    ),
    // three branches: the widest type; a text branch makes the column text
    Case(
      "n, x, CAST(n AS BIGINT)",
      Seq(
        branch("n")(r => opt(r.n.map(_.toDouble))),
        branch("x")(r => opt(r.x)),
        branch("CAST(n AS BIGINT)")(r => opt(r.n.map(_.toDouble)))
      ),
      DOUBLE
    ),
    Case(
      "d, ts, d2",
      Seq(
        branch("d")(r => opt(r.d.map(midnight))),
        branch("ts")(r => opt(r.ts.map(utc))),
        branch("d2")(r => opt(r.d2.map(midnight)))
      ),
      TIMESTAMP
    ),
    Case(
      "d, n, '1'",
      Seq(
        branch("d")(r => opt(r.d.map(_.toString))),
        branch("n")(r => opt(r.n.map(_.toString))),
        branch("'1'")(_ => "1")
      ),
      VARCHAR
    ),
    Case(
      "s, n, d",
      Seq(
        branch("s")(r => opt(r.s)),
        branch("n")(r => opt(r.n.map(_.toString))),
        branch("d")(r => opt(r.d.map(_.toString)))
      ),
      VARCHAR
    ),
    // a KEYWORD beside a TEXT column, and a TEXT column beside a number
    Case("id, txt", Seq(branch("id")(_.id), branch("txt")(r => opt(r.txt))), VARCHAR),
    Case(
      "txt, n",
      Seq(branch("txt")(r => opt(r.txt)), branch("n")(r => opt(r.n.map(_.toString)))),
      VARCHAR
    ),
    // a BOOLEAN reads as 1 / 0 beside a number, and as `true` / `false` beside text
    Case(
      "b, n",
      Seq(branch("b")(r => opt(r.b.map(v => if (v) 1 else 0))), branch("n")(r => opt(r.n))),
      INTEGER
    ),
    Case(
      "b, x",
      Seq(branch("b")(r => opt(r.b.map(v => if (v) 1.0 else 0.0))), branch("x")(r => opt(r.x))),
      DOUBLE
    ),
    Case(
      "b, 'a'",
      Seq(branch("b")(r => opt(r.b.map(_.toString))), branch("'a'")(_ => "a")),
      VARCHAR
    ),
    // the numeric lattice
    Case(
      "n, f",
      Seq(branch("n")(r => opt(r.n.map(_.toFloat))), branch("f")(r => opt(r.f))),
      FLOAT
    ),
    Case(
      "sm, bg",
      Seq(branch("sm")(r => opt(r.sm.map(_.toLong))), branch("bg")(r => opt(r.bg))),
      BIGINT
    ),
    Case(
      "bg, x",
      Seq(branch("bg")(r => opt(r.bg.map(_.toDouble))), branch("x")(r => opt(r.x))),
      DOUBLE
    )
  )

  /** The refusals of rule 1 -- a date beside a number, the pair the relational engine fails on --
    * by name with core's message.
    */
  private val refused: Seq[(String, String)] = Seq(
    s"SELECT d AS k FROM $table UNION ALL SELECT n AS k FROM $table" ->
    "column 1: branch 1 'k' is DATE, branch 2 'k' is INT",
    s"SELECT ts AS k FROM $table UNION ALL SELECT n AS k FROM $table" ->
    "column 1: branch 1 'k' is TIMESTAMP, branch 2 'k' is INT",
    s"SELECT b AS k FROM $table UNION ALL SELECT d AS k FROM $table" ->
    "column 1: branch 1 'k' is BOOLEAN, branch 2 'k' is DATE",
    s"SELECT id, d FROM $table UNION ALL SELECT s, n FROM $table" ->
    "column 2: branch 1 'd' is DATE, branch 2 'n' is INT",
    // a TIME does not make the node text: no text, no VARBINARY
    s"SELECT CAST(ts AS TIME) AS k FROM $textTable UNION ALL SELECT ts AS k FROM $textTable " +
    s"UNION ALL SELECT x AS k FROM $textTable" ->
    "column 1: branch 2 'k' is TIMESTAMP, branch 3 'k' is DOUBLE"
  )

  /** Elasticsearch wraps a `script_fields` value in a one-element array. */
  private def scalar(v: Any): Any = v match {
    case s: Seq[_] if s.size == 1                  => scalar(s.head)
    case c: java.util.Collection[_] if c.size == 1 => scalar(c.iterator().next())
    case other                                     => other
  }

  /** The same VALUE (the class is asserted on its own): an instant for a timestamp, the number for
    * a number.
    */
  private def same(expected: Any, actual: Any): Boolean =
    (expected, actual) match {
      case (null, null)                               => true
      case (null, _) | (_, null)                      => false
      case (e: ZonedDateTime, a: ZonedDateTime)       => e.toInstant == a.toInstant
      case (e: java.lang.Number, a: java.lang.Number) => e.doubleValue() == a.doubleValue()
      case (e: Array[Byte], a: Array[Byte])           => java.util.Arrays.equals(e, a)
      case (e, a)                                     => e == a
    }

  /** A value as a failure message shows it: bytes in hexadecimal. */
  private def show(v: Any): String = v match {
    case b: Array[Byte] => b.map(x => f"${x & 0xff}%02x").mkString("0x", "", "")
    case other          => String.valueOf(other)
  }

  private def rowsOf(sql: String, route: String): Either[String, Seq[ListMap[String, Any]]] =
    Try {
      route match {
        case "search" =>
          implicit val ctx: ConversionContext = NativeContext
          client.search(SelectStatement(sql)) match {
            case ElasticSuccess(response) => Right(response.results)
            case ElasticFailure(error)    => Left(error.message)
          }
        case _ =>
          Await.result(client.run(sql), 60.seconds) match {
            case ElasticSuccess(QueryRows(rs, _))             => Right(rs)
            case ElasticSuccess(QueryStructured(response, _)) => Right(response.results)
            case ElasticSuccess(QueryStream(stream, _)) =>
              Right(Await.result(stream.map(_._1).runWith(Sink.seq), 60.seconds))
            case ElasticSuccess(other) => Left(s"unexpected result $other")
            case ElasticFailure(error) => Left(error.message)
          }
      }
    }.toEither.left.map(t => s"${t.getClass.getSimpleName}: ${t.getMessage}").joinRight

  /** What differs from DuckDB's answer, if anything. */
  private def differences(c: Case, route: String, limit: Boolean): Seq[String] =
    differencesOf(c.sql(limit), c.expected, c.column, route)

  private def differencesOf(
    sql: String,
    want: Seq[Any],
    column: ColumnType,
    route: String
  ): Seq[String] =
    rowsOf(sql, route) match {
      case Left(error) => Seq(s"[$route] [$sql] failed: $error")
      case Right(rows) =>
        val values = rows.map(r => r.getOrElse("k", null))
        val got = values.map(scalar)
        // the values, as a multiset
        var unmatched = got
        val missing = want.filterNot { w =>
          unmatched.indexWhere(g => same(w, g)) match {
            case -1 => false
            case i  => unmatched = unmatched.patch(i, Nil, 1); true
          }
        }
        val valueErrors =
          if (missing.isEmpty && unmatched.isEmpty) Nil
          else
            Seq(
              s"values: missing ${missing.map(show).mkString(", ")} / unexpected " +
              unmatched.map(show).mkString(", ")
            )
        // ONE class: the column's
        val classes = got.filter(_ != null).map(_.getClass).distinct
        val classErrors =
          if (classes.forall(_ == column.cls)) Nil
          else
            Seq(
              s"classes ${classes.map(_.getSimpleName).mkString(", ")} instead of " +
              column.cls.getSimpleName
            )
        // what JDBC and Arrow report: the type of the column's first value
        val reported = ValueCoercion.inferType(values.find(_ != null).orNull)
        val reportErrors =
          if (reported == column.reported) Nil
          else Seq(s"reported $reported instead of ${column.reported}")
        (valueErrors ++ classErrors ++ reportErrors).map(e => s"[$route] [$sql] $e")
    }

  "A UNION ALL" should "answer DuckDB's column type, values and one class per column, on every route" in {
    val wrong = for {
      c                <- cases
      (route, limited) <- Seq(("run", false), ("search", false), ("search", true))
      error            <- differences(c, route, limited)
    } yield error
    wrong shouldBe empty
  }

  /** A `TIMESTAMP`, a `DOUBLE`, a `REAL` or a `TIME` beside text: a `VARCHAR` column, each value
    * spelled as DuckDB 1.5.5 casts its own type to `VARCHAR` (the lead's ruling of 2026-10-06).
    */
  private val textCases: Seq[(Seq[(String, TextDoc => Any)], String)] = {
    val ts: TextDoc => Any = d => d.ts.map(_.text).orNull
    val x: TextDoc => Any = d => d.x.map(_.text).orNull
    val f: TextDoc => Any = d => d.f.map(_.text).orNull
    val s: TextDoc => Any = d => d.s.orNull
    Seq(
      Seq("ts" -> ts, "s" -> s) -> "ts, s",
      Seq("s" -> s, "ts" -> ts) -> "s, ts",
      Seq("ts" -> ts, "'x'" -> ((_: TextDoc) => "x")) -> "ts, 'x'",
      Seq("x" -> x, "s" -> s) -> "x, s",
      Seq("'a'" -> ((_: TextDoc) => "a"), "x" -> x) -> "'a', x",
      Seq("f" -> f, "s" -> s) -> "f, s",
      Seq("CAST(ts AS TIME)" -> ((d: TextDoc) => d.time.orNull), "s" -> s) -> "TIME, s",
      Seq("x" -> x, "ts" -> ts, "s" -> s) -> "x, ts, s",
      Seq(
        "CAST(ts AS DATE)" -> ((d: TextDoc) => d.date.orNull),
        "ts"               -> ts,
        "'x'"              -> ((_: TextDoc) => "x")
      ) -> "DATE, ts, 'x'"
    )
  }

  it should "spell a TIMESTAMP, a DOUBLE, a REAL or a TIME beside text as DuckDB does, on every route" in {
    val wrong = for {
      (branches, _)    <- textCases
      (route, limited) <- Seq(("run", false), ("search", false), ("search", true))
      sql = branches
        .map { case (item, _) =>
          s"SELECT $item AS k FROM $textTable" + (if (limited) " LIMIT 100" else "")
        }
        .mkString(" UNION ALL ")
      want = branches.flatMap { case (_, value) => textDocs.map(value) }
      error <- differencesOf(sql, want, VARCHAR, route)
    } yield error
    wrong shouldBe empty
  }

  private def utf8(s: String): Array[Byte] = s.getBytes(java.nio.charset.StandardCharsets.UTF_8)

  private val routes = Seq(("run", false), ("search", false), ("search", true))

  private def textUnion(items: Seq[String], limited: Boolean): String =
    items
      .map(item => s"SELECT $item AS k FROM $textTable" + (if (limited) " LIMIT 100" else ""))
      .mkString(" UNION ALL ")

  /** A `VARBINARY` beside text: DuckDB's `BLOB` (the lead's ruling of 2026-10-07). Every row is
    * BYTES (`Array[Byte]`, core's `VARBINARY` cell): a `binary` field's base64 read as its UTF-8
    * bytes, as the relational engine and the JDBC driver read it, and a text value cast to bytes as
    * DuckDB 1.5.5 casts a `VARCHAR` to a `BLOB` (MEASURED: an ASCII character is its byte).
    */
  it should "answer a VARBINARY beside text as DuckDB's BLOB, on every route" in {
    val bin: TextDoc => Any = d => binaryOf.get(d.id).map(utf8).orNull
    val s: TextDoc => Any = d => d.s.map(utf8).orNull
    val x: TextDoc => Any = _ => utf8("x")
    val cases: Seq[Seq[(String, TextDoc => Any)]] = Seq(
      Seq("bin" -> bin, "s"    -> s),
      Seq("s"   -> s, "bin"    -> bin),
      Seq("bin" -> bin, "'x'"  -> x),
      Seq("bin" -> bin, "NULL" -> ((_: TextDoc) => null), "s" -> s)
    )
    val wrong = for {
      branches         <- cases
      (route, limited) <- routes
      sql = textUnion(branches.map(_._1), limited)
      want = branches.flatMap { case (_, value) => textDocs.map(value) }
      error <- differencesOf(sql, want, VARBINARY, route)
    } yield error
    wrong shouldBe empty
  }

  it should "fail a VARBINARY beside a text DuckDB cannot cast to bytes, as DuckDB fails it" in {
    // `u` holds a character outside ASCII: DuckDB's cast refuses it, at run time
    val wrong = routes.flatMap { case (route, limited) =>
      val sql = textUnion(Seq("bin", "u"), limited)
      rowsOf(sql, route) match {
        case Left(msg) if msg.contains("Invalid byte encountered in STRING -> BLOB conversion") =>
          None
        case other => Some(s"[$route] [$sql] answered $other instead of failing the cast")
      }
    }
    wrong shouldBe empty
  }

  /** A value with SEVERAL values -- a multi-valued keyword -- in a column that converts its values
    * fails the statement: the column holds one value of its type per row, and such a value has no
    * one value to convert. The message is in DuckDB 1.5.5's words for the same column (MEASURED,
    * each shape below). The CONTROL: the same value in a column whose branches have one type, which
    * converts nothing, answers.
    */
  it should "fail a multi-valued value in a column that converts, on every route" in {
    def failure(other: String): String =
      s"Conversion Error: Unimplemented type for cast ($other -> VARCHAR[]) when casting from " +
      "source column k"
    def limitedIf(limited: Boolean, statement: String): String =
      if (limited) statement.replace(" UNION ALL ", " LIMIT 100 UNION ALL ") + " LIMIT 100"
      else statement
    def union(left: String, right: String, rightTable: String = textTable): String =
      s"SELECT $left AS k FROM $textTable UNION ALL SELECT $right AS k FROM $rightTable"
    val shapes: Seq[(String, String)] = Seq(
      union("tags", "n", table) -> failure("INTEGER"),
      union("tags", "ts")       -> failure("TIMESTAMP"),
      union("tags", "bin")      -> failure("BLOB"),
      union("x", "tags")        -> failure("DOUBLE")
    )
    val wrong = for {
      (statement, message) <- shapes
      (route, limited)     <- routes
      sql = limitedIf(limited, statement)
      error <- rowsOf(sql, route) match {
        case Left(msg) if msg.contains(message) => None
        case other => Some(s"[$route] [$sql] answered $other instead of failing with $message")
      }
    } yield error
    // the control: `tags` beside another KEYWORD, every row answered, the two values of t1 as the
    // branch reads them
    def bothTags(row: ListMap[String, Any]): Boolean = row.getOrElse("k", null) match {
      case s: Seq[_]            => s.map(e => String.valueOf(e)) == Seq("a", "b")
      case l: java.util.List[_] => l.size == 2 && l.get(0) == "a" && l.get(1) == "b"
      case _                    => false
    }
    val unconverted = routes.flatMap { case (route, limited) =>
      val sql = limitedIf(limited, union("tags", "kw"))
      rowsOf(sql, route) match {
        case Right(rows) if rows.size == 2 * textDocs.size && rows.exists(bothTags) => None
        case other => Some(s"[$route] [$sql] answered $other instead of every row, unconverted")
      }
    }
    (wrong ++ unconverted) shouldBe empty
  }

  /** A keyword core re-reads as a temporal when the response is parsed (issue #418) is spelled back
    * with its seconds -- `10:30:00`, `2024-01-31T10:30:00Z` -- its own text, which DuckDB answers
    * since it reads the keyword as text.
    */
  it should "spell a keyword that looks like a time or a timestamp as its text, on every route" in {
    val kw: TextDoc => Any = d => temporalKeywordOf.get(d.id).orNull
    val textOfN: Doc => Any = r => opt(r.n.map(_.toString))
    val bytesOfKw: TextDoc => Any = d => temporalKeywordOf.get(d.id).map(utf8).orNull
    val bin: TextDoc => Any = d => binaryOf.get(d.id).map(utf8).orNull
    val wrong = routes.flatMap { case (route, limited) =>
      val limit = if (limited) " LIMIT 100" else ""
      val asText =
        s"SELECT kw AS k FROM $textTable$limit UNION ALL SELECT n AS k FROM $table$limit"
      val asBytes =
        s"SELECT kw AS k FROM $textTable$limit UNION ALL SELECT bin AS k FROM $textTable$limit"
      differencesOf(asText, textDocs.map(kw) ++ docs.map(textOfN), VARCHAR, route) ++
      differencesOf(asBytes, textDocs.map(bytesOfKw) ++ textDocs.map(bin), VARBINARY, route)
    }
    wrong shouldBe empty
  }

  /** Where DuckDB gives the column no type and rule 1 does not refuse it (the lead's rule 2 of
    * 2026-10-07): a `VARCHAR` column, every value spelled as text -- a `binary` field its base64,
    * the text Elasticsearch holds; a `TIME`, a `DOUBLE`, a `TIMESTAMP` and a `DATE` as DuckDB
    * spells them in a `VARCHAR` column.
    */
  it should "answer VARCHAR where DuckDB gives the column no type, on every route" in {
    val bin: TextDoc => Any = d => binaryOf.get(d.id).orNull
    val x: TextDoc => Any = d => d.x.map(_.text).orNull
    val ts: TextDoc => Any = d => d.ts.map(_.text).orNull
    val s: TextDoc => Any = d => d.s.orNull
    val time: TextDoc => Any = d => d.time.orNull
    val date: TextDoc => Any = d => d.date.orNull
    val cases: Seq[Seq[(String, TextDoc => Any)]] = Seq(
      // a VARBINARY beside a number, a temporal: DuckDB has no cast to BLOB
      Seq("bin" -> bin, "x"                -> x),
      Seq("bin" -> bin, "s"                -> s, "ts" -> ts),
      Seq("bin" -> bin, "CAST(ts AS TIME)" -> time),
      // ... beside a TIMESTAMP AND a number: text to the relational engine, so never refused
      Seq("bin" -> bin, "ts" -> ts, "x"   -> x),
      Seq("x"   -> x, "bin"  -> bin, "ts" -> ts),
      Seq("ts"  -> ts, "x"   -> x, "bin"  -> bin),
      // a TIME beside a TIMESTAMP, a DATE, a number
      Seq("CAST(ts AS TIME)" -> time, "ts"               -> ts),
      Seq("CAST(ts AS TIME)" -> time, "CAST(ts AS DATE)" -> date),
      Seq("x"                -> x, "CAST(ts AS TIME)"    -> time)
    )
    val wrong = for {
      branches         <- cases
      (route, limited) <- routes
      sql = textUnion(branches.map(_._1), limited)
      want = branches.flatMap { case (_, value) => textDocs.map(value) }
      error <- differencesOf(sql, want, VARCHAR, route)
    } yield error
    wrong shouldBe empty
  }

  /** A `GEO_POINT`, a `STRUCT` or an `ARRAY` (here a `nested` field) beside a type of another
    * family is refused, text included -- the relational engine casts nothing to a list or a struct
    * (MEASURED through arrow) -- and a list of objects beside itself answered as each branch reads
    * it.
    */
  it should "refuse an object or a list beside another type, answer it beside itself" in {
    def limitedIf(limited: Boolean, statement: String): String =
      if (limited) statement.replace(" UNION ALL ", " LIMIT 100 UNION ALL ") + " LIMIT 100"
      else statement
    def union(left: String, right: String): String =
      s"SELECT $left AS k FROM $textTable UNION ALL SELECT $right AS k FROM $textTable"
    val refusedShapes: Seq[(String, String)] = Seq(
      union("geo", "s")    -> "column 1: branch 1 'k' is GEO_POINT, branch 2 'k' is KEYWORD",
      union("x", "geo")    -> "column 1: branch 1 'k' is DOUBLE, branch 2 'k' is GEO_POINT",
      union("geo", "bin")  -> "column 1: branch 1 'k' is GEO_POINT, branch 2 'k' is VARBINARY",
      union("items", "s")  -> "column 1: branch 1 'k' is ARRAY<STRUCT>, branch 2 'k' is KEYWORD",
      union("ts", "items") -> "column 1: branch 1 'k' is TIMESTAMP, branch 2 'k' is ARRAY<STRUCT>",
      // an object beside text, a number, a date or bytes: the relational engine fails on each
      union("obj", "s")   -> "column 1: branch 1 'k' is STRUCT, branch 2 'k' is KEYWORD",
      union("x", "obj")   -> "column 1: branch 1 'k' is DOUBLE, branch 2 'k' is STRUCT",
      union("obj", "ts")  -> "column 1: branch 1 'k' is STRUCT, branch 2 'k' is TIMESTAMP",
      union("bin", "obj") -> "column 1: branch 1 'k' is VARBINARY, branch 2 'k' is STRUCT"
    )
    val notRefused = for {
      (statement, reason) <- refusedShapes
      (route, limited)    <- routes
      sql = limitedIf(limited, statement)
      error <- rowsOf(sql, route) match {
        case Left(msg)
            if msg.contains(s"Set operation branches must project compatible types at $reason") =>
          None
        case other => Some(s"[$route] [$sql] answered $other instead of refusing at $reason")
      }
    } yield error
    // beside its own type: every row -- a list of objects as the branch reads it
    val sameType = routes.flatMap { case (route, limited) =>
      (rowsOf(limitedIf(limited, union("items", "items")), route) match {
        case Right(rows) if rows.size == 2 * textDocs.size => Nil
        case other => Seq(s"[$route] items beside items answered $other instead of every row")
      })
    }
    (notRefused ++ sameType) shouldBe empty
  }

  /** What a STRUCT column holds, compared as DuckDB's: each map's fields IN ORDER, every value with
    * its class.
    */
  private def structDifferencesOf(sql: String, want: Seq[Any], route: String): Seq[String] = {
    def norm(v: Any): Any = v match {
      case m: scala.collection.Map[_, _] =>
        m.toSeq.map { case (k, x) => String.valueOf(k) -> norm(x) }
      case l: Seq[_] => l.map(norm)
      case other     => shown(other)
    }
    rowsOf(sql, route) match {
      case Left(error) => Seq(s"[$route] [$sql] failed: $error")
      case Right(rows) =>
        val got = rows.map(r => norm(r.getOrElse("k", null)))
        var unmatched = got
        val missing = want.map(norm).filterNot { w =>
          unmatched.indexOf(w) match {
            case -1 => false
            case i  => unmatched = unmatched.patch(i, Nil, 1); true
          }
        }
        if (missing.isEmpty && unmatched.isEmpty) Nil
        else
          Seq(
            s"[$route] [$sql] values: missing ${missing.mkString(", ")} / unexpected ${unmatched.mkString(", ")}"
          )
    }
  }

  /** A `GEO_POINT` beside a `STRUCT`: DuckDB 1.5.5's merged struct (the lead's ruling of
    * 2026-10-07), MEASURED -- every field of every branch in the order it first appears, a field a
    * branch does not have NULL -- and every point a `{lat, lon}`, whether Elasticsearch stored it
    * as an object, a `"lat,lon"` text or an array `[lon, lat]`; two points stored differently, both
    * `{lat, lon}`.
    */
  it should "answer a GEO_POINT beside a STRUCT as DuckDB's merged STRUCT, on every route" in {
    def merged(fields: Seq[String])(values: Map[String, Any]): Any =
      ListMap(fields.map(f => f -> values.getOrElse(f, null)): _*)
    val pointFirst = merged(Seq("lat", "lon", "a", "b")) _
    val objectFirst = merged(Seq("a", "b", "lat", "lon")) _
    def points(shape: Map[String, Any] => Any): Seq[Any] =
      textDocs.map(d =>
        pointOf.get(d.id).map { case (lat, lon) => shape(Map("lat" -> lat, "lon" -> lon)) }.orNull
      )
    def objects(shape: Map[String, Any] => Any): Seq[Any] =
      textDocs.map(d =>
        objOf.get(d.id).map { case (a, b) => shape(Map("a" -> a, "b" -> b)) }.orNull
      )
    val point = merged(Seq("lat", "lon")) _
    val cases: Seq[(Seq[String], Seq[Any])] = Seq(
      Seq("geoo", "obj") -> (points(pointFirst) ++ objects(pointFirst)),
      Seq("geo", "obj")  -> (points(pointFirst) ++ objects(pointFirst)),
      Seq("geoa", "obj") -> (points(pointFirst) ++ objects(pointFirst)),
      Seq("obj", "geoo") -> (objects(objectFirst) ++ points(objectFirst)),
      Seq("obj", "geo")  -> (objects(objectFirst) ++ points(objectFirst)),
      // two points stored differently: one GEO_POINT, every point {lat, lon}
      Seq("geo", "geoa")         -> (points(point) ++ points(point)),
      Seq("geoo", "geo", "geoa") -> (points(point) ++ points(point) ++ points(point))
    )
    val wrong = for {
      (branches, want) <- cases
      (route, limited) <- routes
      error            <- structDifferencesOf(textUnion(branches, limited), want, route)
    } yield error
    wrong shouldBe empty
  }

  /** A field several objects have is typed over every branch at once, as DuckDB 1.5.5 types it
    * (MEASURED, every order): an `INT`, a `DATE` and a `KEYWORD` field `a` make a `VARCHAR` field,
    * `{a: '1'}`, `{a: '2024-01-31'}`, `{a: 'x'}`, whatever the order of the branches -- although an
    * `INT` beside a `DATE` alone fails.
    */
  it should "type a field the objects share over every branch at once, on every route" in {
    val want: Seq[Any] =
      Seq("oi" -> "1", "od" -> "2024-01-31", "ov" -> "x").flatMap { case (column, a) =>
        textDocs.map(d => if (fieldOf(column).contains(d.id)) ListMap("a" -> a) else null)
      }
    val wrong = for {
      order            <- Seq("oi", "od", "ov").permutations.toSeq
      (route, limited) <- routes
      error            <- structDifferencesOf(textUnion(order, limited), want, route)
    } yield error
    wrong shouldBe empty
    rowsOf(textUnion(Seq("oi", "od"), limited = false), "run") match {
      case Left(msg) =>
        msg should include(
          "Set operation branches must project compatible types at column 1: branch 1 'k' field " +
          "'a' is INT, branch 2 'k' field 'a' is DATE"
        )
      case other => fail(s"an INT beside a DATE answered $other")
    }
  }

  /** A value and its class -- a timestamp by its instant, bytes by their content. */
  private def shown(v: Any): (Any, String) =
    v match {
      case null             => (null, "null")
      case z: ZonedDateTime => (z.toInstant, "ZonedDateTime")
      case b: Array[Byte]   => (b.toSeq, "bytes")
      case other            => (other, other.getClass.getSimpleName)
    }

  /** The column rule's type for two of the generated types -- the sql spec's table
    * (`SetOperationTypeRuleSpec`), the same at every depth -- or REFUSED.
    */
  private val pairType: Map[Set[String], String] = Map(
    Set("INT", "BIGINT")          -> "BIGINT",
    Set("INT", "DOUBLE")          -> "DOUBLE",
    Set("INT", "BOOLEAN")         -> "INT",
    Set("INT", "DATE")            -> "REFUSED",
    Set("INT", "TIMESTAMP")       -> "REFUSED",
    Set("INT", "VARBINARY")       -> "VARCHAR",
    Set("INT", "VARCHAR")         -> "VARCHAR",
    Set("BIGINT", "DOUBLE")       -> "DOUBLE",
    Set("BIGINT", "BOOLEAN")      -> "BIGINT",
    Set("BIGINT", "DATE")         -> "REFUSED",
    Set("BIGINT", "TIMESTAMP")    -> "REFUSED",
    Set("BIGINT", "VARBINARY")    -> "VARCHAR",
    Set("BIGINT", "VARCHAR")      -> "VARCHAR",
    Set("DOUBLE", "BOOLEAN")      -> "DOUBLE",
    Set("DOUBLE", "DATE")         -> "REFUSED",
    Set("DOUBLE", "TIMESTAMP")    -> "REFUSED",
    Set("DOUBLE", "VARBINARY")    -> "VARCHAR",
    Set("DOUBLE", "VARCHAR")      -> "VARCHAR",
    Set("BOOLEAN", "DATE")        -> "REFUSED",
    Set("BOOLEAN", "TIMESTAMP")   -> "REFUSED",
    Set("BOOLEAN", "VARBINARY")   -> "VARCHAR",
    Set("BOOLEAN", "VARCHAR")     -> "VARCHAR",
    Set("DATE", "TIMESTAMP")      -> "TIMESTAMP",
    Set("DATE", "VARBINARY")      -> "VARCHAR",
    Set("DATE", "VARCHAR")        -> "VARCHAR",
    Set("TIMESTAMP", "VARBINARY") -> "VARCHAR",
    Set("TIMESTAMP", "VARCHAR")   -> "VARCHAR",
    Set("VARBINARY", "VARCHAR")   -> "VARBINARY"
  )

  /** What each stored value becomes at a converting position of each type: its value AND its class,
    * written here -- DuckDB's casts and spellings, MEASURED in the rounds before.
    */
  private val convertedOf: Map[(String, String), Map[String, Any]] = {
    val at1030 = ZonedDateTime.parse("2024-01-31T10:30:00Z")
    def midnight(day: String) = ZonedDateTime.parse(s"${day}T00:00:00Z")
    // a whole number at a BIGINT, DOUBLE, VARCHAR (and INT) position
    def whole(n: Long, withInt: Boolean = false): Map[String, Any] =
      Map[String, Any](
        "BIGINT"  -> java.lang.Long.valueOf(n),
        "DOUBLE"  -> java.lang.Double.valueOf(n.toDouble),
        "VARCHAR" -> n.toString
      ) ++ (if (withInt) Map("INT" -> Integer.valueOf(n.toInt)) else Map.empty)
    def truth(b: Boolean): Map[String, Any] = {
      val n = if (b) 1 else 0
      Map(
        "INT"     -> Integer.valueOf(n),
        "BIGINT"  -> java.lang.Long.valueOf(n.toLong),
        "DOUBLE"  -> java.lang.Double.valueOf(n.toDouble),
        "VARCHAR" -> b.toString
      )
    }
    Map(
      ("INT", "t1") -> Map(
        "BIGINT"  -> java.lang.Long.valueOf(1L),
        "DOUBLE"  -> java.lang.Double.valueOf(1.0),
        "INT"     -> Integer.valueOf(1),
        "VARCHAR" -> "1"
      ),
      ("INT", "t2") -> Map(
        "BIGINT"  -> java.lang.Long.valueOf(-2L),
        "DOUBLE"  -> java.lang.Double.valueOf(-2.0),
        "INT"     -> Integer.valueOf(-2),
        "VARCHAR" -> "-2"
      ),
      ("BIGINT", "t1") -> Map(
        "BIGINT"  -> java.lang.Long.valueOf(1L),
        "DOUBLE"  -> java.lang.Double.valueOf(1.0),
        "VARCHAR" -> "1"
      ),
      ("BIGINT", "t2") -> Map(
        "BIGINT"  -> java.lang.Long.valueOf(3000000000L),
        "DOUBLE"  -> java.lang.Double.valueOf(3.0e9),
        "VARCHAR" -> "3000000000"
      ),
      ("INT", "t3")     -> whole(5L, withInt = true),
      ("INT", "t4")     -> whole(5L, withInt = true),
      ("INT", "t5")     -> whole(5L, withInt = true),
      ("BIGINT", "t3")  -> whole(5L),
      ("BIGINT", "t4")  -> whole(5L),
      ("BIGINT", "t5")  -> whole(500L),
      ("BIGINT", "t6")  -> whole(5L),
      ("BIGINT", "t7")  -> whole(-5L),
      ("DOUBLE", "t1")  -> Map("DOUBLE" -> java.lang.Double.valueOf(2.0), "VARCHAR" -> "2.0"),
      ("DOUBLE", "t2")  -> Map("DOUBLE" -> java.lang.Double.valueOf(2.5), "VARCHAR" -> "2.5"),
      ("DOUBLE", "t3")  -> Map("DOUBLE" -> java.lang.Double.valueOf(2.5), "VARCHAR" -> "2.5"),
      ("DOUBLE", "t4")  -> Map("DOUBLE" -> java.lang.Double.valueOf(1000.0), "VARCHAR" -> "1000.0"),
      ("DOUBLE", "t5")  -> Map("DOUBLE" -> java.lang.Double.valueOf(2.5), "VARCHAR" -> "2.5"),
      ("DOUBLE", "t6")  -> Map("DOUBLE" -> java.lang.Double.valueOf(5.0), "VARCHAR" -> "5.0"),
      ("BOOLEAN", "t3") -> truth(true),
      ("BOOLEAN", "t4") -> truth(false),
      ("BOOLEAN", "t5") -> truth(false),
      ("DATE", "t3")    -> Map("TIMESTAMP" -> midnight("2024-01-31"), "VARCHAR" -> "2024-01-31"),
      ("DATE", "t4")    -> Map("TIMESTAMP" -> midnight("2024-01-31"), "VARCHAR" -> "2024-01-31"),
      ("DATE", "t5")    -> Map("TIMESTAMP" -> midnight("2024-01-31"), "VARCHAR" -> "2024-01-31"),
      ("TIMESTAMP", "t6") -> Map("TIMESTAMP" -> at1030, "VARCHAR" -> "2024-01-31 10:30:00"),
      ("BOOLEAN", "t1") -> Map(
        "INT"     -> Integer.valueOf(1),
        "BIGINT"  -> java.lang.Long.valueOf(1L),
        "DOUBLE"  -> java.lang.Double.valueOf(1.0),
        "VARCHAR" -> "true"
      ),
      ("BOOLEAN", "t2") -> Map(
        "INT"     -> Integer.valueOf(0),
        "BIGINT"  -> java.lang.Long.valueOf(0L),
        "DOUBLE"  -> java.lang.Double.valueOf(0.0),
        "VARCHAR" -> "false"
      ),
      ("DATE", "t1") -> Map("TIMESTAMP" -> midnight("2024-01-31"), "VARCHAR" -> "2024-01-31"),
      ("DATE", "t2") -> Map("TIMESTAMP" -> midnight("2023-12-25"), "VARCHAR" -> "2023-12-25"),
      ("TIMESTAMP", "t1") ->
      Map("TIMESTAMP" -> midnight("2024-01-31"), "VARCHAR" -> "2024-01-31 00:00:00"),
      ("TIMESTAMP", "t2") -> Map("TIMESTAMP" -> at1030, "VARCHAR" -> "2024-01-31 10:30:00"),
      ("TIMESTAMP", "t3") -> Map("TIMESTAMP" -> at1030, "VARCHAR" -> "2024-01-31 10:30:00"),
      ("TIMESTAMP", "t4") -> Map("TIMESTAMP" -> at1030, "VARCHAR" -> "2024-01-31 10:30:00"),
      ("TIMESTAMP", "t5") -> Map("TIMESTAMP" -> at1030, "VARCHAR" -> "2024-01-31 10:30:00"),
      ("VARBINARY", "t1") -> Map("VARBINARY" -> utf8("YWI="), "VARCHAR" -> "YWI="),
      ("VARBINARY", "t2") -> Map("VARBINARY" -> utf8("eA=="), "VARCHAR" -> "eA=="),
      ("VARCHAR", "t1")   -> Map("VARBINARY" -> utf8("x"), "VARCHAR" -> "x"),
      ("VARCHAR", "t2")   -> Map("VARBINARY" -> utf8("y"), "VARCHAR" -> "y")
    )
  }

  /** ONE class per converting position, at every depth -- a column, an object's field, the field of
    * a list of objects' element, the field of an object inside an object -- on every route: every
    * pair of two different types, in both orders; every value checked with its class, in every form
    * core reads it in. A date beside a number is refused, the field named by its path.
    */
  it should "hold one class at every converting position, at every depth, on every route" in {
    val depths = Seq("column" -> "c", "field" -> "f", "element" -> "n", "nested" -> "s")
    def at(depth: String, k: Any): Seq[Any] =
      (depth, k) match {
        case (_, null)     => Seq(null)
        case ("column", v) => Seq(v)
        case ("field", m: scala.collection.Map[_, _]) =>
          Seq(m.asInstanceOf[scala.collection.Map[String, Any]].getOrElse("a", null))
        case ("element", l: Seq[_]) =>
          l.map {
            case m: scala.collection.Map[_, _] =>
              m.asInstanceOf[scala.collection.Map[String, Any]].getOrElse("a", null)
            case other => ("not an object", other)
          }
        case ("nested", m: scala.collection.Map[_, _]) =>
          m.asInstanceOf[scala.collection.Map[String, Any]].get("p") match {
            case Some(p: scala.collection.Map[_, _]) =>
              Seq(p.asInstanceOf[scala.collection.Map[String, Any]].getOrElse("c", null))
            case other => Seq(("no p", other))
          }
        case (_, other) => Seq(("unexpected", other))
      }
    val pairs = generatedTypes.map(_._1).combinations(2).flatMap(_.permutations).toSeq
    pairs should have size 56
    val wrong = for {
      (depth, prefix)  <- depths
      pair             <- pairs
      (route, limited) <- routes
      sql = textUnion(pair.map(t => s"${prefix}_$t"), limited)
      error <- (pairType(pair.toSet), rowsOf(sql, route)) match {
        case ("REFUSED", Left(msg)) =>
          val named = depth match {
            case "column" => s"branch 1 'k' is "
            case "nested" => "field 'p.c' is "
            case _        => "field 'a' is "
          }
          if (msg.contains("must project compatible types") && msg.contains(named)) None
          else Some(s"[$route] [$sql] refused as $msg")
        case ("REFUSED", other) => Some(s"[$route] [$sql] answered $other instead of refusing")
        case (_, Left(error))   => Some(s"[$route] [$sql] failed: $error")
        case (target, Right(rows)) =>
          val got = rows.flatMap(r => at(depth, r.getOrElse("k", null))).map(shown)
          val want = pair.flatMap(t =>
            textDocs.map(d =>
              if (storedOf(t).contains(d.id)) shown(convertedOf((t, d.id))(target)) else shown(null)
            )
          )
          var unmatched = got
          val missing = want.filterNot { w =>
            unmatched.indexOf(w) match {
              case -1 => false
              case i  => unmatched = unmatched.patch(i, Nil, 1); true
            }
          }
          if (missing.isEmpty && unmatched.isEmpty) None
          else
            Some(
              s"[$route] [$sql] as $target: missing ${missing.mkString(", ")} / unexpected " +
              unmatched.mkString(", ")
            )
      }
    } yield error
    wrong shouldBe empty
  }

  private lazy val esMajor: Int =
    client.version match {
      case ElasticSuccess(v) => v.takeWhile(_ != '.').toInt
      case ElasticFailure(_) => 0
    }

  /** DuckDB's spelling of a `TIMESTAMP`, in UTC: `2024-01-31 10:30:00`, `2024-01-31 10:30:00.123`.
    */
  private def duckTimestamp(i: Instant): String = {
    val l = java.time.LocalDateTime.ofInstant(i, ZoneOffset.UTC)
    val base = f"${l.toLocalDate} ${l.getHour}%02d:${l.getMinute}%02d:${l.getSecond}%02d"
    if (l.getNano == 0) base
    else base + "." + f"${l.getNano}%09d".reverse.dropWhile(_ == '0').reverse
  }

  /** A date field's value is read with ITS mapping format, as the Elasticsearch major the statement
    * runs on reads it -- seconds, custom patterns, `||` lists, a DATE's day, the default format's
    * years -- at every converting position: a column and an object's field, beside a type that
    * makes it convert to a `TIMESTAMP` and to a `VARCHAR`, on every route; every value checked with
    * its class.
    */
  it should "read a date field with its mapping format, as the major reads it, at every depth, on every route" in {
    def depthValue(depth: String, k: Any): Any =
      (depth, k) match {
        case (_, null)     => null
        case ("column", v) => v
        case ("field", m: scala.collection.Map[_, _]) =>
          m.asInstanceOf[scala.collection.Map[String, Any]].getOrElse("a", null)
        case (_, other) => ("unexpected", other)
      }
    val wrong = for {
      f <- formatted
      (partner, target) <- (if (f.declared == "DATE") Seq("pts" -> "TIMESTAMP")
                            else Seq("pd" -> "TIMESTAMP")) :+ ("s" -> "VARCHAR")
      (depth, item, other) <- Seq(
        ("column", s"c_${f.key}", partner),
        ("field", s"f_${f.key}", "f" + partner.stripPrefix("p"))
      )
      (route, limited) <- routes
      sql = Seq(item, other)
        .map(i => s"SELECT $i AS k FROM $formatTable" + (if (limited) " LIMIT 100" else ""))
        .mkString(" UNION ALL ")
      error <- rowsOf(sql, route) match {
        case Left(e) => Some(s"[$route] [$sql] failed: $e")
        case Right(rows) =>
          def converted(at: Instant): Any = {
            val read =
              if (f.declared == "DATE")
                at.atZone(ZoneOffset.UTC).toLocalDate.atStartOfDay.toInstant(ZoneOffset.UTC)
              else at
            target match {
              case "TIMESTAMP" => read.atZone(ZoneOffset.UTC)
              case _ =>
                if (f.declared == "DATE") read.atZone(ZoneOffset.UTC).toLocalDate.toString
                else duckTimestamp(read)
            }
          }
          val partnerValue: Any = (partner, target) match {
            case (_, "VARCHAR") => "x"
            case _              => ZonedDateTime.parse("2024-02-01T00:00:00Z")
          }
          val want = formatDocs.map(id =>
            f.values.get(id).fold[Any](null) { case (_, at, byMajor) =>
              converted(byMajor.getOrElse(esMajor, at))
            }
          ) ++ formatDocs.map(id => if (id == "p1") partnerValue else null)
          val got = rows.map(r => depthValue(depth, r.getOrElse("k", null)))
          var unmatched = got.map(shown)
          val missing = want.map(shown).filterNot { w =>
            unmatched.indexOf(w) match {
              case -1 => false
              case i  => unmatched = unmatched.patch(i, Nil, 1); true
            }
          }
          if (missing.isEmpty && unmatched.isEmpty) None
          else
            Some(
              s"[$route] [$sql] as $target: missing ${missing.mkString(", ")} / unexpected " +
              unmatched.mkString(", ")
            )
      }
    } yield error
    wrong shouldBe empty
  }

  it should "refuse by name only a date beside a number, before anything runs" in {
    val wrong = refused.flatMap { case (sql, reason) =>
      rowsOf(sql, "run") match {
        case Left(msg)
            if msg.contains(s"Set operation branches must project compatible types at $reason") =>
          None
        case other => Some(s"[$sql] answered $other instead of refusing at $reason")
      }
    }
    wrong shouldBe empty
  }
}
