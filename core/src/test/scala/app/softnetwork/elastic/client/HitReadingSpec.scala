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
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.time.{LocalDate, ZoneOffset, ZonedDateTime}
import scala.collection.immutable.ListMap

/** SCHEMA-2 (D2, D4-D9) -- every value core reads from a hit is read by its column's declared type
  * where it is first read: the class oracle is D2's table, never a value equality alone.
  */
class HitReadingSpec extends AnyFlatSpec with Matchers {

  private val mapping: String =
    """{"s2_t": {"aliases": {}, "mappings": {
      | "_meta": {"columns": {"c_des": {"data_type": "DATE"}}},
      | "properties": {
      |  "id": {"type": "keyword"},
      |  "ts": {"type": "date"},
      |  "c_des": {"type": "date", "format": "epoch_second"},
      |  "c_c1": {"type": "date", "format": "yyyy/MM/dd"},
      |  "tsz": {"type": "date", "format": "yyyy-MM-dd HH:mm z"},
      |  "dm": {"type": "date", "ignore_malformed": true},
      |  "n": {"type": "integer"}, "x": {"type": "double"}, "r": {"type": "float"},
      |  "l": {"type": "long"}, "s": {"type": "short"}, "tb": {"type": "byte"},
      |  "b": {"type": "boolean"}, "g": {"type": "geo_point"}, "bin": {"type": "binary"},
      |  "k": {"type": "keyword"}, "kw": {"type": "keyword", "ignore_above": 5},
      |  "nul": {"type": "keyword"},
      |  "obj": {"properties": {"a": {"type": "integer"}, "d": {"type": "date"}}}
      | }}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin

  private val schema: Schema = Index("s2_t", mapping).schema

  private def statement(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(schema))
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def outputs(single: SingleSearch): Seq[String] = {
    val fields = single.select.fieldsWithComputedAliases
    if (fields.size == 1 && fields.head.identifier.identifierName == "*") Seq.empty
    else fields.map(_.outputName)
  }

  private def contextOf(
    single: SingleSearch,
    version: String = "8.18.3",
    multiValued: MultiValued = HitReading.Default
  ): ConversionContext = {
    val elements = ElementConversions.of(single, Some(version))
    SchemaColumns(
      HitReading.of(single, DateReading(Some(version)), multiValued, elements.keySet),
      single.doubleKeyLevels
    )(elements)
  }

  /** The rows `sql` answers for the hits `hits` (a JSON array body). */
  private def rows(
    sql: String,
    hits: String,
    version: String = "8.18.3",
    multiValued: MultiValued = HitReading.Default
  ): Seq[ListMap[String, Any]] = {
    val single = statement(sql)
    implicit val context: ConversionContext = contextOf(single, version, multiValued)
    val response = s"""{"hits": {"total": {"value": 1, "relation": "eq"}, "hits": [$hits]}}"""
    ElasticConversion
      .parseResponse(response, single.fieldAliases, ListMap.empty, outputs(single))
      .get
  }

  private def row(sql: String, source: String, extra: String = ""): ListMap[String, Any] =
    rows(sql, s"""{"_id": "a1", "_source": $source$extra}""").head

  private def utc(y: Int, mo: Int, d: Int, h: Int = 0, mi: Int = 0): ZonedDateTime =
    ZonedDateTime.of(y, mo, d, h, mi, 0, 0, ZoneOffset.UTC)

  "A hit read by its plan" should "answer each column in D2's class (types)" in {
    val r = row(
      "SELECT id, ts, c_des, c_c1, n, x, r, l, s, tb, b, k, bin FROM s2_t",
      """{"id": "1", "ts": "2024-01-31T10:30:00", "c_des": 1706697000, "c_c1": "2024/01/31",
        | "n": "5.5", "x": " 2.5 ", "r": 1.5, "l": 7, "s": 3, "tb": 2, "b": "", "k": 10,
        | "bin": "aGVsbG8="}""".stripMargin
    )
    r.keys.toSeq shouldBe Seq(
      "id",
      "ts",
      "c_des",
      "c_c1",
      "n",
      "x",
      "r",
      "l",
      "s",
      "tb",
      "b",
      "k",
      "bin"
    )
    r("id") shouldBe "1"
    r("ts") shouldBe utc(2024, 1, 31, 10, 30)
    r("ts").asInstanceOf[ZonedDateTime].getZone shouldBe ZoneOffset.UTC
    r("c_des") shouldBe LocalDate.of(2024, 1, 31)
    r("c_c1") shouldBe utc(2024, 1, 31)
    r("n") shouldBe java.lang.Integer.valueOf(5)
    r("x") shouldBe java.lang.Double.valueOf(2.5)
    r("r") shouldBe java.lang.Float.valueOf(1.5f)
    r("l") shouldBe java.lang.Long.valueOf(7L)
    r("s") shouldBe a[java.lang.Integer]
    r("tb") shouldBe a[java.lang.Integer]
    r("b") shouldBe java.lang.Boolean.FALSE
    r("k") shouldBe "10"
    new String(r("bin").asInstanceOf[Array[Byte]], StandardCharsets.US_ASCII) shouldBe "hello"
  }

  it should "answer a date its format cannot read for sure as its text as stored (AsStored)" in {
    row("SELECT tsz FROM s2_t", """{"tsz": "2024-01-31 10:30 CET"}""")(
      "tsz"
    ) shouldBe "2024-01-31 10:30 CET"
  }

  it should "read every spelling of a point as {lat, lon} (geo)" in {
    val point =
      ListMap("lat" -> java.lang.Double.valueOf(1.5), "lon" -> java.lang.Double.valueOf(2.5))
    Seq(
      """{"lat": 1.5, "lon": 2.5}""",
      "\"1.5,2.5\"",
      "[2.5, 1.5]",
      "\"POINT (2.5 1.5)\""
    ).foreach { g =>
      withClue(g)(row("SELECT g FROM s2_t", s"""{"g": $g}""")("g") shouldBe point)
    }
    // a geohash: its cell's south-west corner, the one rule (`SetOperationValues.geoPoint`)
    row("SELECT g FROM s2_t", """{"g": "s0"}""")("g") shouldBe
    SetOperationValues.geoPoint("s0", "g")
  }

  it should "unwrap a one-element array and answer an empty one NULL (unwrap)" in {
    row("SELECT n FROM s2_t", """{"n": [5]}""")("n") shouldBe java.lang.Integer.valueOf(5)
    row("SELECT n FROM s2_t", """{"n": []}""")("n") shouldBe (null: Any)
    // ONE value in arrays of one is that value, at the column and at a struct's field
    row("SELECT n FROM s2_t", """{"n": [[5]]}""")("n") shouldBe java.lang.Integer.valueOf(5)
    row("SELECT c_des FROM s2_t", """{"c_des": [[1706697000]]}""")("c_des") shouldBe
    LocalDate.of(2024, 1, 31)
    // several values in an array of one: as stored, its nesting kept (D6), never a failure
    row("SELECT c_des FROM s2_t", """{"c_des": [[1706697000, 1706697001]]}""")("c_des") shouldBe
    List(List(1706697000, 1706697001))
    row("SELECT obj FROM s2_t", """{"obj": {"a": [[1, 2]], "d": [["2024-01-31"]]}}""")(
      "obj"
    ) shouldBe ListMap("a" -> List(List(1, 2)), "d" -> utc(2024, 1, 31))
    // ...a statement failing several values fails it too (D7)
    intercept[Exception](
      rows(
        "SELECT n FROM s2_t",
        """{"_id": "doc8", "_source": {"n": [[1, 2]]}}""",
        multiValued = MultiValued.Fail
      )
    )
    // a computed column: Elasticsearch's array is ONE value
    val computed =
      rows(
        "SELECT id, n + 1 AS v FROM s2_t",
        """{"_id": "a1", "_source": {"id": "1"}, "fields": {"v": [4]}}"""
      ).head
    computed("v") shouldBe 4
    val upper = rows(
      "SELECT id, UPPER(k) AS u FROM s2_t",
      """{"_id": "a1", "_source": {"id": "1"}, "fields": {"u": ["X"]}}"""
    ).head
    upper("u") shouldBe "X"
  }

  it should "read several values per MultiValued (multi)" in {
    row("SELECT n FROM s2_t", """{"n": [1, "2"]}""")("n") shouldBe List(1, 2)
    val failure = intercept[Exception] {
      rows(
        "SELECT n FROM s2_t",
        """{"_id": "doc7", "_source": {"n": [1, 2]}}""",
        multiValued = MultiValued.Fail
      )
    }
    val message = Option(failure.getCause).fold(failure.getMessage)(_.getMessage)
    message should include("Column n")
    message should include("doc7")
    message should include("ALTER TABLE s2_t ALTER COLUMN n TYPE ARRAY")
  }

  it should "answer a date Elasticsearch ignored NULL, a keyword it ignored as stored (ignored)" in {
    val r = row(
      "SELECT dm, kw FROM s2_t",
      """{"dm": "not-a-date", "kw": "abcdefgh"}""",
      """, "_ignored": ["dm", "kw"]"""
    )
    r("dm") shouldBe (null: Any)
    r("kw") shouldBe "abcdefgh"
    // without `_ignored`, an unreadable date is as stored (D6), never a failure
    row("SELECT dm FROM s2_t", """{"dm": "not-a-date"}""")("dm") shouldBe "not-a-date"
    // Elasticsearch's verdict is per VALUE: in an array, the values it indexed keep theirs and only
    // the ones it refused are NULL
    row("SELECT dm FROM s2_t", """{"dm": ["2024-01-01", "bad"]}""", """, "_ignored": ["dm"]""")(
      "dm"
    ) shouldBe List(utc(2024, 1, 1), null)
    row("SELECT dm FROM s2_t", """{"dm": ["bad"]}""", """, "_ignored": ["dm"]""")("dm") shouldBe
    (null: Any)
    // ...and a date `_ignored` does not name is read as usual
    row(
      "SELECT dm, ts FROM s2_t",
      """{"dm": "bad", "ts": "2024-01-31"}""",
      """, "_ignored": ["dm"]"""
    )(
      "ts"
    ) shouldBe utc(2024, 1, 31)
  }

  it should "project strictly: no undeclared key, `_id` and the nested-hit keys kept (strict)" in {
    row(
      "SELECT id, n FROM s2_t",
      """{"id": "1", "extra": 42, "n": 3, "k": "x"}"""
    ).keys.toSeq shouldBe
    Seq("id", "n")
    // a computed-only projection returns only its columns (#418 part 2)
    rows(
      "SELECT n + 1 AS v FROM s2_t",
      """{"_id": "a1", "_source": {"n": 3, "k": "x"}, "fields": {"v": [4]}}"""
    ).head.keys.toSeq shouldBe Seq("v")
    val reading = HitReading.of(statement("SELECT id FROM s2_t"), DateReading(Some("8.18.3")))
    reading.strict shouldBe true
    reading.keep should contain(ElasticConversion.DocumentIdField)
    // no strict projection for a statement whose rows core reorders or aggregates
    HitReading
      .of(statement("SELECT k, COUNT(*) FROM s2_t GROUP BY k"), DateReading(None))
      .strict shouldBe false
  }

  it should "read a flat dotted key as its path, and fold it under * (flat)" in {
    row("SELECT obj.a FROM s2_t", """{"obj.a": 5}""")("obj.a") shouldBe 5
    val star = row("SELECT * FROM s2_t", """{"id": "1", "obj.a": 5, "extra": 1}""")
    star("obj") shouldBe ListMap("a" -> 5)
    star.contains("extra") shouldBe false
    star.contains("obj.a") shouldBe false
  }

  it should "read a STRUCT's declared fields by name, each by its type (struct)" in {
    val obj =
      row("SELECT obj FROM s2_t", """{"obj": {"zz": 1, "a": "3", "d": "2024-01-31"}}""")("obj")
    obj shouldBe ListMap("a" -> 3, "d" -> utc(2024, 1, 31))
    obj.asInstanceOf[ListMap[String, Any]]("a") shouldBe a[java.lang.Integer]
    // an undeclared key spelling a selected path is kept, as read
    val both = row("SELECT obj, obj.zz FROM s2_t", """{"obj": {"zz": 1, "a": 2}}""")
    both("obj.zz") shouldBe 1
  }

  it should "answer a value its typed reader cannot read as stored, never a failure (unreadable)" in {
    row("SELECT n FROM s2_t", """{"n": "abc"}""")("n") shouldBe "abc"
    row("SELECT g FROM s2_t", """{"g": "not a point"}""")("g") shouldBe "not a point"
    row("SELECT c_c1 FROM s2_t", """{"c_c1": "31-01-2024"}""")("c_c1") shouldBe "31-01-2024"
    // several points, read one by one (D7's AsRead)
    row("SELECT g FROM s2_t", """{"g": ["1.5,2.5", "3.5,4.5"]}""")("g") shouldBe List(
      ListMap("lat" -> 1.5, "lon" -> 2.5),
      ListMap("lat" -> 3.5, "lon" -> 4.5)
    )
    // a number outside its type's range is no value of it -- Elasticsearch refused it
    // (`ignore_malformed`) and holds it in `_source` only: as stored, never narrowed (wrapped)
    row("SELECT n FROM s2_t", """{"n": 3000000000}""")("n") shouldBe java.lang.Long.valueOf(
      3000000000L
    )
    row("SELECT n FROM s2_t", """{"n": "3000000000"}""")("n") shouldBe "3000000000"
    row("SELECT n FROM s2_t", """{"n": 2147483647.5}""")("n") shouldBe java.lang.Double.valueOf(
      2147483647.5
    )
    row("SELECT l FROM s2_t", """{"l": 12345678901234567890}""")("l") shouldBe
    new java.math.BigInteger("12345678901234567890")
    row("SELECT l FROM s2_t", """{"l": "1e30"}""")("l") shouldBe "1e30"
    row("SELECT s FROM s2_t", """{"s": 40000}""")("s") shouldBe java.lang.Integer.valueOf(40000)
    row("SELECT tb FROM s2_t", """{"tb": "200"}""")("tb") shouldBe "200"
    row("SELECT n FROM s2_t", """{"n": "1e1000000000"}""")("n") shouldBe "1e1000000000"
    // ...the bounds themselves are values, checked as Elasticsearch checks them: `byte`, `short`
    // and `integer` on the value, `long` on the truncated value
    row("SELECT n FROM s2_t", """{"n": -2147483648}""")("n") shouldBe
    java.lang.Integer.valueOf(Int.MinValue)
    row("SELECT tb FROM s2_t", """{"tb": "-128"}""")("tb") shouldBe java.lang.Integer.valueOf(-128)
    row("SELECT tb FROM s2_t", """{"tb": "127.9"}""")("tb") shouldBe "127.9"
    row("SELECT l FROM s2_t", """{"l": "9223372036854775807.5"}""")("l") shouldBe
    java.lang.Long.valueOf(Long.MaxValue)
    row("SELECT l FROM s2_t", """{"l": "0.5e-1000000000"}""")("l") shouldBe java.lang.Long.valueOf(
      0L
    )
  }

  it should "answer a page the same rows and classes on every parse route (pages)" in {
    val fixture = SchemaPageFixture
    val outputs = fixture.statement.select.fieldsWithComputedAliases.map(_.outputName)
    // each client's page extractor reads its page through one of these two routes, in this context:
    // the client modules' page specs assert their extractors against the same rows
    Seq("6.8.23", "7.17.29", "8.18.3").foreach { version =>
      withClue(s"[$version] ") {
        implicit val context: ConversionContext = fixture.context(version)
        val oneShot = ElasticConversion
          .parseResponse(fixture.page, fixture.fieldAliases, ListMap.empty, outputs)
          .get
        val tree = ElasticConversion
          .parseSingleSearchResponse(
            app.softnetwork.elastic.sql.serialization.JacksonConfig.objectMapper
              .readTree(fixture.page),
            fixture.fieldAliases,
            ListMap.empty
          )
          .get
        Seq(oneShot, tree).foreach { parsed =>
          parsed shouldBe fixture.expected
          fixture.classes(parsed) shouldBe fixture.classes(fixture.expected)
        }
      }
    }
  }

  it should "read a column qualified by its table alias by its type (qualified)" in {
    val r = row("SELECT t.id, t.n FROM s2_t AS t", """{"id": "1", "n": "7", "k": "x"}""")
    r.values.toSeq shouldBe Seq("1", 7)
    r.values.toSeq(1) shouldBe a[java.lang.Integer]
  }

  it should "leave a column the statement types as it comes as read (ANY)" in {
    // `extra` is not mapped: ANY, as read today
    row("SELECT id, extra FROM s2_t", """{"id": "1", "extra": [1, 2]}""")("extra") shouldBe List(
      1,
      2
    )
  }

  it should "read a NULL as NULL and a never-set keyword as an absent value" in {
    val r = row("SELECT id, nul FROM s2_t", """{"id": "1", "ts": null}""")
    r("nul") shouldBe (null: Any)
  }

  "Elasticsearch's default date format" should "be read by hand exactly as MappedDateFormat reads it (default date)" in {
    val corpus = Seq(
      "2024-01-31",
      "2024-01-31T10:30:00",
      "2024-01-31T10:30:00Z",
      "2024-01-31T10:30:00.5",
      "2024-01-31T10:30:00.55Z",
      "2024-01-31T10:30:00.555",
      "2024-01-31T10:30:00.5555",
      "2024-02-29T23:59:59.999Z",
      "2023-02-29",
      "2024-02-30T10:00:00",
      "2024-13-01",
      "2024-01-31T24:00:00",
      "2024-01-31T10:30",
      "2024-01-31T10:30:00+01:00",
      "0001-01-01T00:00:00",
      "9999-12-31T23:59:59.999Z",
      "2024-1-31",
      "1706697000000",
      "2024-01-31 10:30:00",
      "20240131"
    )
    Seq("6.8.23", "7.17.29", "8.18.3").foreach { version =>
      val dialect = MappedDateFormat.Dialect.of(version).get
      val format = MappedDateFormat(
        app.softnetwork.elastic.sql.query.TemporalLiterals.DefaultDateFormat,
        dialect,
        java.time.Clock.systemUTC()
      )
      corpus.foreach { text =>
        Option(HitReading.DefaultDates.read(text)).foreach { fast =>
          withClue(s"[$version] $text: ") {
            format.read(text) shouldBe MappedDateFormat.Read(fast.toInstant)
          }
        }
      }
    }
    // the shapes the hand reader takes
    HitReading.DefaultDates.read("2024-01-31T10:30:00.555Z") shouldBe
    ZonedDateTime.of(2024, 1, 31, 10, 30, 0, 555000000, ZoneOffset.UTC)
    HitReading.DefaultDates.read("2024-01-31T10:30:00.5555") shouldBe null
    HitReading.DefaultDates.read("2024-02-30T10:00:00") shouldBe null
  }

  "The strict projection" should "hand an in-order row back as the same instance (identity)" in {
    val single = statement("SELECT id, n FROM s2_t")
    implicit val context: ConversionContext = contextOf(single)
    val project = Projector.normalizer(Seq("id", "n"))
    val inOrder = ListMap[String, Any]("id" -> "1", "n" -> 3)
    (project(inOrder) eq inOrder) shouldBe true
    // ...a trailing `_id` is kept, as the same instance
    val withId = ListMap[String, Any]("id" -> "1", "n" -> 3, "_id" -> "a1")
    (project(withId) eq withId) shouldBe true
    // ...a trailing undeclared key is dropped
    project(ListMap[String, Any]("id" -> "1", "n" -> 3, "k" -> "x")) shouldBe
    ListMap[String, Any]("id" -> "1", "n" -> 3)
  }

  "A set operation's date reader" should "pass a canonical value through (D12)" in {
    val dates = DateReading(Some("8.18.3"))
    def read(source: app.softnetwork.elastic.sql.`type`.SQLType, format: String) =
      SetOperationValues.converter(source, source, "x", Some(format), "c", "c", dates)
    read(SQLTypes.Date, "epoch_second")(LocalDate.of(2024, 1, 31)) shouldBe LocalDate.of(
      2024,
      1,
      31
    )
    read(SQLTypes.Timestamp, "yyyy/MM/dd")(utc(2024, 1, 31)) shouldBe utc(2024, 1, 31)
    read(SQLTypes.Time, "HH:mm:ss.SSS")(java.time.LocalTime.of(10, 30, 0, 123000000)) shouldBe
    java.time.LocalTime.of(10, 30, 0, 123000000)
    // the raw forms still read by their format
    read(SQLTypes.Date, "epoch_second")(1706697000) shouldBe LocalDate.of(2024, 1, 31)
    // ...and so is a temporal the response parser made of a text, which is NOT that one form: an
    // offset other than UTC (read by its format: Elasticsearch 8.18.3's week-based year here,
    // MEASURED by `MappedDateFormatSpec`'s corpus), a region (never read without guessing: it
    // fails, R-11 / R-15)
    read(SQLTypes.Timestamp, "strict_date_optional_time||epoch_millis")(
      ZonedDateTime.of(2024, 1, 31, 11, 30, 0, 0, java.time.ZoneOffset.ofHours(1))
    ) shouldBe utc(2024, 1, 31, 10, 30)
    read(SQLTypes.Timestamp, "YYYY-MM-dd'T'HH:mm:ssXXX")(
      ZonedDateTime.parse("2024-01-31T10:30:00+02:00")
    ) shouldBe utc(2023, 12, 31, 8, 30)
    an[IllegalArgumentException] should be thrownBy read(
      SQLTypes.Timestamp,
      "yyyy-MM-dd'T'HH:mm:ssXXX'['VV']'"
    )(ZonedDateTime.parse("2024-07-31T10:30:00+02:00[Europe/Paris]"))
  }

  it should "answer a SMALLINT and a TINYINT as Integers, a binary's base64 as its bytes (D2, Q5)" in {
    SetOperationValues.converter(SQLTypes.Int, SQLTypes.SmallInt)(5) shouldBe
    java.lang.Integer.valueOf(5)
    SetOperationValues.converter(SQLTypes.Int, SQLTypes.TinyInt)(5) shouldBe
    java.lang.Integer.valueOf(5)
    new String(
      SetOperationValues
        .converter(SQLTypes.VarBinary, SQLTypes.VarBinary)("aGVsbG8=")
        .asInstanceOf[Array[Byte]],
      StandardCharsets.US_ASCII
    ) shouldBe "hello"
  }
}

/** The protected projector, for the identity check. */
private object Projector extends ElasticConversion {
  def normalizer(fields: Seq[String])(implicit
    context: ConversionContext
  ): ListMap[String, Any] => ListMap[String, Any] = rowNormalizer(fields)
}
