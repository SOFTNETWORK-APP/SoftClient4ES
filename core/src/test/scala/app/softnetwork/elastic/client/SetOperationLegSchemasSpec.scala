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

import app.softnetwork.elastic.client.SetOperationValues.{LegSchema, LegSlot, Reading, Unreadable}
import app.softnetwork.elastic.sql.{StringValue, Value}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{MultiSearch, SingleSearch, TemporalLiterals}
import app.softnetwork.elastic.sql.query.MultiSearch.{
  SetOperationColumn,
  SetOperationType,
  StructField
}
import app.softnetwork.elastic.sql.operator.SetOperator
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.{Clock, Instant, LocalDate, ZoneOffset, ZonedDateTime}
import java.util.zip.GZIPInputStream
import scala.collection.immutable.ListMap
import scala.io.Source
import scala.util.{Failure, Success, Try}

/** Each leg of a set operation TYPED BEFORE ANY VALUE ([[SetOperationValues.legSchemas]]): per
  * column, the leg's type at every depth and the reader of its values -- a date whose format core
  * cannot read for sure `VARCHAR`, the text as stored, every row, whatever their order; every other
  * date read as [[SetOperationValues.legReaders]] reads it. Every value goes the way core receives
  * it: its JSON, read by the response parser (`ElasticConversion.jsonNodeToAny`).
  */
class SetOperationLegSchemasSpec extends AnyFlatSpec with Matchers {

  private def format(f: String): ListMap[String, Value[_]] =
    ListMap[String, Value[_]]("format" -> StringValue(f))

  private val zoneName = "yyyy-MM-dd HH:mm z"

  private val schema = Table(
    "t",
    columns = List(
      Column("c_es", SQLTypes.Timestamp, options = format("epoch_second")),
      Column("c_em", SQLTypes.Timestamp, options = format("epoch_millis")),
      Column("c_c1", SQLTypes.Timestamp, options = format("yyyy/MM/dd")),
      Column("c_def", SQLTypes.Timestamp),
      Column("tsz", SQLTypes.Timestamp, options = format(zoneName)),
      Column("ts", SQLTypes.Timestamp),
      Column("d", SQLTypes.Date),
      Column("n", SQLTypes.Int),
      Column("x", SQLTypes.Double),
      Column("f", SQLTypes.Real),
      Column("s", SQLTypes.Keyword),
      Column("bin", SQLTypes.VarBinary),
      Column("meta", SQLTypes.Any),
      Column("o", SQLTypes.Struct, multiFields = List(Column("meta", SQLTypes.Any))),
      Column("fl", SQLTypes.Struct),
      Column(
        "oz",
        SQLTypes.Struct,
        multiFields = List(Column("z", SQLTypes.Timestamp, options = format(zoneName)))
      ),
      Column("ozs", SQLTypes.Struct, multiFields = List(Column("z", SQLTypes.Keyword))),
      Column("od", SQLTypes.Struct, multiFields = List(Column("t", SQLTypes.Timestamp))),
      Column(
        "obj",
        SQLTypes.Struct,
        multiFields = List(
          Column("n", SQLTypes.Int),
          Column("e", SQLTypes.Timestamp, options = format("epoch_second"))
        )
      ),
      Column("ofr", SQLTypes.Struct, multiFields = List(Column("f", SQLTypes.Real))),
      Column("o2", SQLTypes.Struct, multiFields = List(Column("n", SQLTypes.Int))),
      Column(
        "p",
        SQLTypes.Struct,
        multiFields = List(
          Column(
            "m",
            SQLTypes.Struct,
            multiFields = List(Column("e", SQLTypes.Timestamp, options = format("epoch_second")))
          )
        )
      ),
      Column("q", SQLTypes.Struct, multiFields = List(Column("m", SQLTypes.Any))),
      Column("r", SQLTypes.Struct, multiFields = List(Column("m", SQLTypes.Struct))),
      Column("az", SQLTypes.Array(SQLTypes.Timestamp), options = format(zoneName)),
      Column("ae", SQLTypes.Array(SQLTypes.Timestamp), options = format("epoch_second")),
      Column("ats", SQLTypes.Array(SQLTypes.Timestamp)),
      Column("ar", SQLTypes.Array(SQLTypes.Real))
    )
  )

  private val es8 = Some("8.18.3")

  private val varchar = SetOperationType(SQLTypes.Varchar)

  private def resolved(sql: String): (Seq[SingleSearch], Seq[SetOperator]) =
    Parser(sql) match {
      case Right(m: MultiSearch) => (m.requests.map(_.update(Some(schema))), m.resolvedOperators)
      case other                 => fail(s"[$sql] expected a set operation, got $other")
    }

  private def schemas(sql: String, version: => Option[String] = es8): Seq[Option[LegSchema]] = {
    val (requests, operators) = resolved(sql)
    SetOperationValues.legSchemas(requests, operators, version) match {
      case Right(legs)   => legs
      case Left(refusal) => fail(s"[$sql] refused: $refusal")
    }
  }

  /** Leg `leg`'s slot at `position`. */
  private def slot(
    sql: String,
    leg: Int = 0,
    position: Int = 0,
    version: => Option[String] = es8
  ): LegSlot =
    schemas(sql, version)(leg).flatMap(_.slots(position)) match {
      case Some(s) => s
      case None    => fail(s"[$sql] leg $leg has no slot at $position")
    }

  /** The slots of every leg: `None` for a `*` leg, else each position's slot or `None`. */
  private def slots(sql: String): Seq[Option[Seq[Option[LegSlot]]]] =
    schemas(sql).map(_.map(_.slots))

  /** The reader [[SetOperationValues.legReaders]] gives leg `leg` at `position` -- `null` where it
    * reads nothing.
    */
  private def legReader(sql: String, leg: Int = 0, position: Int = 0): Any => Any = {
    val (requests, operators) = resolved(sql)
    SetOperationValues.legReaders(requests, operators, es8) match {
      case Right(legs)   => legs(leg).map(_(position)).orNull
      case Left(refusal) => fail(s"[$sql] refused: $refusal")
    }
  }

  /** A document's value as core receives it: its JSON, read by the response parser. */
  private def received(json: String): Any =
    ElasticConversion.jsonNodeToAny(JacksonConfig.objectMapper.readTree(json), ListMap.empty)

  private def union(a: String, b: String, operator: String = "UNION"): String =
    s"SELECT $a AS k FROM t $operator SELECT $b AS k FROM t"

  /** A value and its class. */
  private def shown(v: Any): (Any, Class[_]) =
    v match {
      case null  => (null, classOf[Null])
      case other => (other, other.getClass)
    }

  private def utc(s: String): ZonedDateTime = ZonedDateTime.parse(s)

  private val at1030 = utc("2024-01-31T10:30:00Z")

  // ---------------------------------------------------------------------------------------------
  // Order-independent VARCHAR
  // ---------------------------------------------------------------------------------------------

  "A zone-name date" should "be VARCHAR before any value, every row as stored, in any order" in {
    Seq("8.18.3", "7.17.29", "6.8.23").foreach { v =>
      var asked = 0
      def version: Option[String] = { asked += 1; Some(v) }
      val legs = schemas(union("tsz", "ts"), version)
      // decided with no value read: the version asked once
      asked shouldBe 1
      val tsz = legs.head.flatMap(_.slots.head).getOrElse(fail(s"$v: no slot"))
      tsz.sqlType shouldBe varchar
      tsz.asStored.map(_.path) shouldBe Seq("tsz")
      tsz.asStored.head.reason should include(s"'$zoneName'")
      tsz.asStored.head.reason should include("zone names")
      legs(1).flatMap(_.slots.head).map(_.sqlType) shouldBe
      Some(SetOperationType(SQLTypes.Timestamp, format = Some(TemporalLiterals.DefaultDateFormat)))
      val utcText = "2024-01-31 10:30 UTC"
      val cetText = "2024-01-31 10:30 CET"
      val rows = Seq.fill(1000)(utcText) :+ cetText
      Seq(rows, rows.reverse, Seq.fill(10)(cetText), Seq.fill(10)(utcText)).foreach { texts =>
        val read = texts.map(text => tsz.read(received(s"\"$text\"")))
        withClue(s"$v: ")(read shouldBe texts)
        read.map(_.getClass).distinct shouldBe Seq(classOf[String])
      }
      asked shouldBe 1
    }
  }

  // ---------------------------------------------------------------------------------------------
  // Typed readers are legReaders'; the last release's forms
  // ---------------------------------------------------------------------------------------------

  "A date read by its format" should "be its declared type, read as legReaders reads it" in {
    Seq(
      ("c_es", "c_es", "1706697000"),
      ("c_es", "ts", "1706697000"),
      ("c_em", "c_em", "1706697000000"),
      ("c_def", "ts", "1706697000000")
    ).foreach { case (a, b, json) =>
      val sql = union(a, b)
      val s = slot(sql)
      s.sqlType.sqlType shouldBe SQLTypes.Timestamp
      s.asStored shouldBe empty
      shown(s.read(received(json))) shouldBe shown(at1030)
      shown(s.read(received(json))) shouldBe shown(legReader(sql)(received(json)))
    }
    // a zone name stays VARCHAR, agreeing or converting
    Seq("tsz", "s", "ts").foreach(b => slot(union("tsz", b)).sqlType shouldBe varchar)
    slot(union("tsz", "s")).read(received("\"2024-01-31 10:30 UTC\"")) shouldBe
    "2024-01-31 10:30 UTC"
  }

  // ---------------------------------------------------------------------------------------------
  // Every corpus row
  // ---------------------------------------------------------------------------------------------

  /** The day the corpora were measured: Joda's two-digit years depend on it. */
  private val measuredOn = Clock.fixed(Instant.parse("2026-10-07T12:00:00Z"), ZoneOffset.UTC)

  private val valueCauses = Set("json-double", "erased-offset", "erased-spelling", "jdk-week")

  private def corpus(major: String): Seq[(String, String, Long, Option[String])] = {
    val source = Source.fromInputStream(
      new GZIPInputStream(getClass.getResourceAsStream(s"/date-formats/$major.tsv.gz")),
      "UTF-8"
    )
    try source
      .getLines()
      .filterNot(_.startsWith("#"))
      .map { line =>
        line.split("\t", -1) match {
          case Array(f, json, millis)        => (f, json, millis.toLong, None)
          case Array(f, json, millis, cause) => (f, json, millis.toLong, Some(cause))
          case other => throw new IllegalStateException(s"a corpus row: ${other.mkString("|")}")
        }
      }
      .toList
    finally source.close()
  }

  private def readsEveryRow(major: String, version: String, size: Int): Unit = {
    val rows = corpus(major)
    rows.size shouldBe size
    val dates = new SetOperationValues.DateReading(Some(version), measuredOn)
    // the text leg's reader: a KEYWORD beside a KEYWORD
    val keyword = SetOperationValues.converter(SQLTypes.Varchar, SQLTypes.Varchar)
    var typedRead, typedFailed, storedRead = 0
    val wrong = rows.groupBy(_._1).toSeq.sortBy(_._1).flatMap { case (f, formatRows) =>
      val spec = if (f.isEmpty) TemporalLiterals.DefaultDateFormat else f
      val declared = SetOperationType(SQLTypes.Timestamp, format = Some(spec))
      val column =
        SetOperationColumn(
          SetOperationType(SQLTypes.Timestamp),
          Seq(Some(declared), Some(declared))
        )
      val legSlot = SetOperationValues.legSlot(column, 0, "k", "v", dates)
      // legReaders' reader of the same column (`legReader`'s own call)
      val reference = SetOperationValues.positionConverter(
        column.result,
        column.branches,
        0,
        "k",
        "v",
        dates,
        Reading.Declared(Unreadable.Fail),
        agrees = Some(!column.converts)
      )
      (DateRoute.of(SQLTypes.Timestamp, Some(spec), dates), legSlot) match {
        case (_, None)                  => Seq(s"[$f] no slot")
        case (DateRoute.Typed, Some(s)) =>
          // SCHEMA-2 (D4, D12) -- a leg's row holds the value core's hit reader gives the column:
          // the date its format reads (canonical), or, where it reads none, the value as stored
          val canonical = new HitReading.Readers(ListMap.empty, dates, MultiValued.AsRead, "t")
            .column("v", declared, Set.empty)
            ._1
          (if (s.sqlType != declared || s.asStored.nonEmpty) Seq(s"[$f] typed as $s") else Nil) ++
          formatRows.flatMap { case (_, json, millis, cause) =>
            val value = canonical(JacksonConfig.objectMapper.readTree(json))
            (cause, Try(s.read(value)), Try(reference(value))) match {
              case (None, Success(z: ZonedDateTime), Success(r))
                  if shown(z) == shown(r) && z.toInstant.toEpochMilli == millis =>
                typedRead += 1
                None
              // SCHEMA-2 (D4): a value whose spelling or offset the response parser erased is
              // read from its text now, before that parser: the instant Elasticsearch indexed
              case (Some(c), Success(z: ZonedDateTime), Success(r))
                  if c.startsWith("erased-") && shown(z) == shown(r) &&
                    z.toInstant.toEpochMilli == millis =>
                typedRead += 1
                None
              case (Some(c), Failure(e: IllegalArgumentException), Failure(r))
                  if valueCauses(c) && e.getMessage == r.getMessage &&
                    e.getMessage.contains("field v") && e.getMessage.contains(s"'$spec'") =>
                typedFailed += 1
                None
              case (c, got, want) => Some(s"[$f] $json ($c): read $got, legReaders $want")
            }
          }
        case (DateRoute.AsStored(reason), Some(s)) =>
          (if (s.sqlType != varchar || s.asStored != Seq(LegSlot.AsStored("v", reason)))
             Seq(s"[$f] as stored, typed as $s")
           else Nil) ++
            formatRows.flatMap { case (_, json, _, cause) =>
              val value = received(json)
              (Try(s.read(value)), Try(keyword(value))) match {
                case (Success(text: String), Success(k)) if text == k =>
                  storedRead += 1
                  None
                case (got, want) => Some(s"[$f] $json ($cause): read $got, the text leg $want")
              }
            }
      }
    }
    info(
      s"$major on $version: ${rows.size} rows -- $typedRead read by their format, $typedFailed " +
      s"failing at the value, $storedRead as stored"
    )
    withClue(wrong.take(50).mkString("\n", "\n", "\n"))(wrong shouldBe empty)
    (typedRead + typedFailed + storedRead) shouldBe size
  }

  "Every value of Elasticsearch 8.18.3's corpus" should "be read by its slot's reader" in
  readsEveryRow("es8", "8.18.3", 36109)

  "Every value of Elasticsearch 7.17.29's corpus" should "be read by its slot's reader" in
  readsEveryRow("es7", "7.17.29", 47065)

  "Every value of Elasticsearch 6.8.23's corpus" should "be read by its slot's reader" in
  readsEveryRow("es6", "6.8.23", 51515)

  // ---------------------------------------------------------------------------------------------
  // Positions and depth
  // ---------------------------------------------------------------------------------------------

  "A date answered as stored" should "be VARCHAR at every position and depth" in {
    val cet = "2024-01-31 10:30 CET"
    // agreeing, converting to VARCHAR and to TIMESTAMP, multi-step
    Seq(union("tsz", "tsz"), union("tsz", "s"), union("tsz", "d"), union("tsz", "ts")).foreach {
      sql =>
        val s = slot(sql)
        s.sqlType shouldBe varchar
        s.asStored.map(_.path) shouldBe Seq("tsz")
        s.read(received(s"\"$cet\"")) shouldBe cet
    }
    slot(union("tsz", "tsz"), 1).sqlType shouldBe varchar
    slot(union("tsz", "d"), 1).sqlType.sqlType shouldBe SQLTypes.Date
    val multiStep =
      "SELECT tsz AS k FROM t UNION SELECT ts AS k FROM t INTERSECT SELECT tsz AS k FROM t"
    Seq(0, 2).foreach(leg => slot(multiStep, leg).sqlType shouldBe varchar)
    slot(multiStep, 1).sqlType.sqlType shouldBe SQLTypes.Timestamp
    // a struct's field: STRUCT(z VARCHAR), its path `oz.z`
    Seq(union("oz", "oz"), union("oz", "ozs")).foreach { sql =>
      val s = slot(sql)
      s.sqlType shouldBe SetOperationType(SQLTypes.Struct, Seq(StructField("z", varchar)))
      s.asStored.map(_.path) shouldBe Seq("oz.z")
      s.read(received(s"""{"z":"$cet"}""")) shouldBe ListMap("z" -> cet)
    }
    // a list's element: ARRAY<VARCHAR>
    val az = slot(union("az", "az"))
    az.sqlType shouldBe
    SetOperationType(SQLTypes.Array(SQLTypes.Varchar), element = Some(varchar))
    az.asStored.map(_.path) shouldBe Seq("az")
    az.read(received(s"""["2024-01-31 10:30 UTC","$cet"]""")) shouldBe
    List("2024-01-31 10:30 UTC", cet)
    az.read(received(s"\"$cet\"")) shouldBe List(cet)
  }

  "A date read by its format" should "keep its type at every position and depth" in {
    // (statement, leg, JSON): each read as legReaders reads it
    val cases = Seq(
      (union("c_es", "c_es"), 0, "1706697000"),
      (union("c_es", "s"), 0, "1706697000"),
      (union("c_es", "d"), 0, "1706697000"),
      (union("ts", "ts"), 0, "\"2024-01-31T10:30:00Z\""),
      (union("ts", "s"), 0, "\"2024-01-31T10:30:00Z\""),
      (union("ts", "d"), 1, "\"2024-01-31\""),
      (
        "SELECT c_es AS k FROM t UNION SELECT ts AS k FROM t INTERSECT SELECT c_es AS k FROM t",
        2,
        "1706697000"
      ),
      (union("obj", "obj"), 0, """{"n":1,"e":1706697000}"""),
      (union("od", "od"), 0, """{"t":"2024-01-31T10:30:00Z"}"""),
      (union("ae", "ae"), 0, "[1706697000,1706697001]"),
      (union("ats", "ats"), 0, "[\"2024-01-31T10:30:00Z\"]")
    )
    val wrong = cases.flatMap { case (sql, leg, json) =>
      val s = slot(sql, leg)
      val got = Try(s.read(received(json))).map(shown)
      val want = Try(legReader(sql, leg)(received(json))).map(shown)
      if (s.asStored.isEmpty && got.isSuccess && got == want) None
      else Some(s"[$sql] leg $leg <- $json: $s read $got, legReaders $want")
    }
    wrong shouldBe empty
    slot(union("obj", "obj")).read(received("""{"n":1,"e":1706697000}""")) shouldBe
    ListMap("e" -> at1030, "n" -> 1)
    slot(union("obj", "obj")).sqlType.fields.map(f => f.name -> f.valueType) shouldBe Seq(
      "e" -> SetOperationType(SQLTypes.Timestamp, format = Some("epoch_second")),
      "n" -> SetOperationType(SQLTypes.Int)
    )
    slot(union("ae", "ae")).sqlType.element shouldBe
    Some(SetOperationType(SQLTypes.Timestamp, format = Some("epoch_second")))
  }

  // ---------------------------------------------------------------------------------------------
  // Several values
  // ---------------------------------------------------------------------------------------------

  "A value of several values" should "fail in core's words, typed or as stored" in {
    def failure(read: Any => Any, json: String): String =
      intercept[IllegalArgumentException](read(received(json))).getMessage
    val typed = slot(union("c_es", "c_es")).read
    val stored = slot(union("tsz", "tsz")).read
    Seq("""["a","b"]""", "[[1706697000,1706697001]]").foreach { json =>
      failure(typed, json) shouldBe failure(legReader(union("c_es", "c_es")), json)
      failure(typed, json) shouldBe
      "Conversion Error: Unimplemented type for cast (TIMESTAMP -> TIMESTAMP[]) when casting " +
      "from source column k"
      failure(stored, json) shouldBe
      "Conversion Error: Unimplemented type for cast (TIMESTAMP -> VARCHAR[]) when casting from " +
      "source column k"
    }
    // Elasticsearch's one-element array is ONE value
    stored(received("""["c"]""")) shouldBe "c"
  }

  // ---------------------------------------------------------------------------------------------
  // Value-typed positions
  // ---------------------------------------------------------------------------------------------

  "A position typed as its values come" should "have no slot where legReaders reads nothing" in {
    // a column whose type is not known: a WITH aliasing inside its CTE, every derived table, in
    // every leg; a column of type ANY; a NULL literal
    val anyColumns = Seq(
      "WITH a AS (SELECT tsz AS k FROM t) SELECT k FROM a UNION SELECT ts AS k FROM t",
      "SELECT k FROM (SELECT tsz AS k FROM t) dd UNION SELECT ts AS k FROM t",
      "SELECT dd.k FROM (SELECT tsz AS k FROM t) dd UNION SELECT ts AS k FROM t",
      "SELECT ts AS k FROM t UNION SELECT k FROM (SELECT tsz AS k FROM t) dd",
      union("meta", "meta")
    )
    anyColumns.foreach { sql =>
      val (requests, operators) = resolved(sql)
      MultiSearch.columnTypes(requests, operators).map(_.map(_.resultType)) shouldBe
      Right(Seq(SQLTypes.Any))
      slots(sql) shouldBe Seq(Some(Seq(None)), Some(Seq(None)))
      Seq(0, 1).foreach(leg => legReader(sql, leg) shouldBe null)
    }
    slots(union("n", "NULL")).map(_.map(_.map(_.map(_.sqlType)))) shouldBe
    Seq(Some(Seq(Some(SetOperationType(SQLTypes.Int)))), Some(Seq(None)))
    legReader(union("n", "NULL"), 1) shouldBe null
    // a SELECT * leg: no schema, whatever the width of the others
    slots("SELECT n FROM t UNION ALL SELECT * FROM t").map(_.isDefined) shouldBe Seq(true, false)
    slots("SELECT n, x FROM t UNION ALL SELECT * FROM t").map(_.isDefined) shouldBe
    Seq(true, false)
  }

  it should "keep the plain column's slot in a WITH that passes it through" in {
    val passThrough =
      "WITH a AS (SELECT tsz FROM t) SELECT tsz AS k FROM a UNION SELECT ts AS k FROM t"
    slots(passThrough).map(_.map(_.map(_.map(s => (s.sqlType, s.asStored))))) shouldBe
    slots(union("tsz", "ts")).map(_.map(_.map(_.map(s => (s.sqlType, s.asStored)))))
    slot(passThrough).sqlType shouldBe varchar
    val epochs =
      "WITH a AS (SELECT c_es FROM t) SELECT c_es AS k FROM a UNION SELECT ts AS k FROM t"
    shown(slot(epochs).read(received("1706697000"))) shouldBe shown(at1030)
  }

  it should "have no slot at a type not known at any depth" in {
    // STRUCT(meta ANY), a STRUCT whose fields are not known: legReaders reads them, arrow types
    // their values as they come
    Seq(union("o", "o"), union("fl", "fl")).foreach { sql =>
      slots(sql) shouldBe Seq(Some(Seq(None)), Some(Seq(None)))
    }
  }

  it should "have no slot where ANOTHER leg leaves the column's type not known at some depth" in {
    var asked = 0
    def version: Option[String] = { asked += 1; es8 }
    // the column is a STRUCT whose fields are not known (`fl`), or holds one (`r.m`) or an `ANY`
    // (`q.m`) where this leg declares its fields: legReader reads the leg's value there as it
    // comes, not as the leg's type -- no slot for any leg, and no version asked for `p`'s date
    Seq(
      union("obj", "fl"),
      union("fl", "obj"),
      union("obj", "fl", "UNION ALL"),
      union("oz", "fl"),
      union("p", "q"),
      union("p", "r")
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        schemas(sql, version).map(_.map(_.slots)) shouldBe Seq(Some(Seq(None)), Some(Seq(None)))
        asked shouldBe 0
      }
    }
    // the column's type known at every depth: every leg keeps its slot
    Seq(union("obj", "o2"), union("obj", "obj"), union("oz", "oz"), union("p", "p")).foreach {
      sql =>
        withClue(s"[$sql] ")(
          slots(sql).map(_.map(_.map(_.isDefined))) shouldBe Seq(Some(Seq(true)), Some(Seq(true)))
        )
    }
    slot(union("p", "p")).read(received("""{"m":{"e":1706697000}}""")) shouldBe
    ListMap("m" -> ListMap("e" -> at1030))
  }

  /** One value per column of [[schema]], its JSON as Elasticsearch stores it -- every object
    * holding, at every depth, a key its mapping does not declare (`zz`), which a reader typing the
    * object leaves out.
    */
  private val stored: ListMap[String, String] = ListMap(
    "c_es"  -> "1706697000",
    "c_em"  -> "1706697000000",
    "c_c1"  -> "\"2024/01/31\"",
    "c_def" -> "\"2024-01-31T10:30:00Z\"",
    "tsz"   -> "\"2024-01-31 10:30 CET\"",
    "ts"    -> "\"2024-01-31T10:30:00Z\"",
    "d"     -> "\"2024-01-31\"",
    "n"     -> "1",
    "x"     -> "1.5",
    "f"     -> "1.5",
    "s"     -> "\"a\"",
    "bin"   -> "\"aGk=\"",
    "meta"  -> """{"a":1}""",
    "o"     -> """{"meta":{"a":1},"zz":1}""",
    "fl"    -> """{"a":1,"zz":1}""",
    "oz"    -> """{"z":"2024-01-31 10:30 CET","zz":1}""",
    "ozs"   -> """{"z":"a","zz":1}""",
    "od"    -> """{"t":"2024-01-31T10:30:00Z","zz":1}""",
    "obj"   -> """{"n":1,"e":1706697000,"zz":1}""",
    "ofr"   -> """{"f":1.5,"zz":1}""",
    "o2"    -> """{"n":1,"zz":1}""",
    "p"     -> """{"m":{"e":1706697000,"zz":1},"zz":1}""",
    "q"     -> """{"m":{"a":1},"zz":1}""",
    "r"     -> """{"m":{"a":1},"zz":1}""",
    "az"    -> """["2024-01-31 10:30 UTC","2024-01-31 10:30 CET"]""",
    "ae"    -> "[1706697000,1706697001]",
    "ats"   -> """["2024-01-31T10:30:00Z","2024-01-31T11:30:00Z"]""",
    "ar"    -> "[1.5,2.5]"
  )

  /** The class of each scalar type a slot of [[schema]] holds. */
  private val scalarClass: Map[SQLType, Class[_]] = Map(
    SQLTypes.Varchar   -> classOf[String],
    SQLTypes.Int       -> classOf[java.lang.Integer],
    SQLTypes.Double    -> classOf[java.lang.Double],
    SQLTypes.Date      -> classOf[LocalDate],
    SQLTypes.Timestamp -> classOf[ZonedDateTime]
  )

  /** Whether `v` is of `t`'s class at every depth: a `STRUCT` a `ListMap` of EXACTLY its fields, in
    * its order; a list a `List` of its element's class; `null` of any type.
    */
  private def conforms(t: SetOperationType, v: Any): Boolean =
    v == null || (t.sqlType match {
      case SQLTypes.Struct =>
        v match {
          case m: ListMap[_, _] =>
            val fields = m.asInstanceOf[ListMap[String, Any]]
            fields.keys.toSeq == t.fields.map(_.name) &&
            t.fields.forall(f => conforms(f.valueType, fields(f.name)))
          case _ => false
        }
      case SQLTypes.Array(_) =>
        v match {
          case l: List[_] => t.element.exists(e => l.forall(conforms(e, _)))
          case _          => false
        }
      case scalar => scalarClass.get(scalar).exists(_.isInstance(v))
    })

  "Every slot" should "answer its type's class at every depth, over every pair of columns" in {
    val names = stored.keys.toSeq
    names.toSet shouldBe schema.columns.map(_.name).toSet
    val mismatches = Seq.newBuilder[String]
    var statements = 0
    var refused = 0
    var slotsRead = 0
    for (a <- names; b <- names; operator <- Seq("UNION", "UNION ALL")) {
      val sql = union(a, b, operator)
      val (requests, operators) = resolved(sql)
      statements += 1
      SetOperationValues.legSchemas(requests, operators, es8) match {
        case Left(_) => refused += 1
        case Right(legs) =>
          legs.zip(Seq(a, b)).foreach { case (leg, column) =>
            leg.flatMap(_.slots.head).foreach { s =>
              slotsRead += 1
              Try(s.read(received(stored(column)))) match {
                case Success(v) if conforms(s.sqlType, v) =>
                case outcome => mismatches += s"[$sql] $column ${s.sqlType}: ${outcome.map(shown)}"
              }
            }
          }
      }
    }
    mismatches.result() shouldBe empty
    // the population: 28 x 28 columns x 2 operators, 992 statements refused (`branchTypes`); 544
    // slots read -- 640 before no slot was given where ANOTHER leg leaves the column's type not
    // known inside, 32 of which answered another class than their type's
    (statements, refused, slotsRead) shouldBe ((1568, 992, 544))
  }

  it should "have no slot at NUMERIC or TEMPORAL, the other legs keeping theirs" in {
    // ROUND and DATEDIFF report NUMERIC; a date part reports BIGINT (the next row)
    Seq("ROUND(x, 2)", "DATEDIFF(ts, d)").foreach { computed =>
      val legs = slots(union(computed, "x"))
      legs.head shouldBe Some(Seq(None))
      legs(1).flatMap(_.head).map(_.sqlType) shouldBe Some(SetOperationType(SQLTypes.Double))
    }
    // a constructed TEMPORAL leg (no SQL form yields one)
    val temporal = SetOperationColumn(
      SetOperationType(SQLTypes.Temporal),
      Seq(
        Some(SetOperationType(SQLTypes.Temporal)),
        Some(SetOperationType(SQLTypes.Timestamp, format = Some("epoch_second")))
      )
    )
    val dates = SetOperationValues.DateReading(es8)
    SetOperationValues.legSlot(temporal, 0, "k", "", dates) shouldBe None
    SetOperationValues.legSlot(temporal, 1, "k", "c_es", dates).map(_.sqlType.sqlType) shouldBe
    Some(SQLTypes.Timestamp)
    // DATE_TRUNC is a TIMESTAMP, computed: no format, read as legReaders reads it
    val truncated = union("DATE_TRUNC(ts, DAY)", "ts")
    slot(truncated).sqlType shouldBe SetOperationType(SQLTypes.Timestamp)
    shown(slot(truncated).read(received("\"2024-01-31T00:00:00Z\""))) shouldBe
    shown(legReader(truncated)(received("\"2024-01-31T00:00:00Z\"")))
  }

  // A date part is a whole number: BIGINT, as DuckDB 1.5.5 types it (`typeof(year(d))`,
  // `typeof(month(ts))`, `typeof(extract(year FROM ts))`). Its leg has a slot, which reads every
  // value as a Long, and the other leg keeps its own.
  it should "give a date-part leg a BIGINT slot, reading every value as a Long" in {
    Seq("YEAR(d)", "MONTH(ts)", "EXTRACT(YEAR FROM ts)").foreach { part =>
      val sql = union(part, "x")
      val legs = slots(sql)
      withClue(s"[$sql] ") {
        legs.head.flatMap(_.head).map(_.sqlType) shouldBe Some(SetOperationType(SQLTypes.BigInt))
        legs(1).flatMap(_.head).map(_.sqlType) shouldBe Some(SetOperationType(SQLTypes.Double))
        shown(slot(sql).read(received("2024"))) shouldBe ((2024L, classOf[java.lang.Long]))
      }
    }
  }

  it should "have no slot at VARBINARY or REAL at any depth, the other legs keeping theirs" in {
    Seq(union("bin", "bin"), union("f", "f"), union("ofr", "ofr"), union("ar", "ar")).foreach {
      sql => slots(sql) shouldBe Seq(Some(Seq(None)), Some(Seq(None)))
    }
    val text = slots(union("s", "bin"))
    text.head.flatMap(_.head).map(_.sqlType) shouldBe Some(varchar)
    text(1) shouldBe Some(Seq(None))
    val double = slots(union("x", "f"))
    double.head.flatMap(_.head).map(_.sqlType) shouldBe Some(SetOperationType(SQLTypes.Double))
    double(1) shouldBe Some(Seq(None))
  }

  // ---------------------------------------------------------------------------------------------
  // The version
  // ---------------------------------------------------------------------------------------------

  "A leg's schema" should "read its dates as the major the statement runs on" in {
    val sql = union("c_es", "c_es")
    slot(sql, version = Some("8.18.3")).read(received("\"1706697000.5\"")) shouldBe
    utc("2024-01-31T10:30:00.500Z")
    slot(sql, version = Some("7.17.29")).read(received("\"1706697000.5\"")) shouldBe
    utc("2024-01-31T10:30:00.500Z")
    slot(sql, version = Some("6.8.23")).read(received("\"1706697000.5\"")) shouldBe at1030
  }

  it should "answer every date of a format as stored when the major is not known" in {
    val sql =
      "SELECT c_es AS a, ts AS b, tsz AS c, DATE_TRUNC(ts, DAY) AS e FROM t UNION " +
      "SELECT c_es AS a, ts AS b, tsz AS c, DATE_TRUNC(ts, DAY) AS e FROM t"
    Seq(None -> "which is not known", Some("0.0.0") -> "no 0.0.0", Some("5.6.16") -> "no 5.6.16")
      .foreach { case (version, named) =>
        val leg = schemas(sql, version).head.getOrElse(fail("no schema")).slots
        leg.take(3).map(_.map(_.sqlType)) shouldBe Seq.fill(3)(Some(varchar))
        leg.take(3).flatMap(_.toSeq.flatMap(_.asStored)).map(_.path) shouldBe
        Seq("c_es", "ts", "tsz")
        leg.take(3).flatMap(_.toSeq.flatMap(_.asStored)).foreach(_.reason should include(named))
        leg.head.map(_.read(received("1706697000"))) shouldBe Some("1706697000")
        // a computed date: typed
        leg(3).map(_.sqlType) shouldBe Some(SetOperationType(SQLTypes.Timestamp))
        leg(3).map(s => shown(s.read(received("\"2024-01-31T00:00:00Z\"")))) shouldBe
        Some(shown(utc("2024-01-31T00:00:00Z")))
      }
  }

  it should "ask the version at most once, never for a statement with no formatted date slot" in {
    var asked = 0
    def version: Option[String] = { asked += 1; es8 }
    schemas("SELECT c_es, ts, n FROM t UNION ALL SELECT c_c1, d, x FROM t", version)
    asked shouldBe 1
    schemas(union("n", "x"), version)
    schemas(union("s", "s"), version)
    schemas(union("DATE_TRUNC(ts, DAY)", "DATE_TRUNC(d, MONTH)"), version)
    asked shouldBe 1
    // a date read by its format where no slot is: a column whose type is not known (`ANY`) -- a
    // WITH aliasing inside its CTE, a derived table, in any leg -- which legReaders reads nothing at
    Seq(
      "SELECT k FROM (SELECT tsz AS k FROM t) dd UNION SELECT ts AS k FROM t",
      "SELECT dd.k FROM (SELECT tsz AS k FROM t) dd UNION SELECT ts AS k FROM t",
      "WITH a AS (SELECT tsz AS k FROM t) SELECT k FROM a UNION SELECT ts AS k FROM t",
      "SELECT ts AS k FROM t UNION SELECT k FROM (SELECT tsz AS k FROM t) dd",
      "SELECT k FROM (SELECT c_c1 AS k FROM t) dd UNION SELECT c_c1 AS k FROM t"
    ).foreach { sql =>
      val before = asked
      schemas(sql, version).map(_.map(_.slots)) shouldBe Seq(Some(Seq(None)), Some(Seq(None)))
      withClue(s"[$sql] ")(asked shouldBe before)
    }
  }

  it should "answer a refused statement with its refusal, word for word" in {
    val (refused, operators) = resolved(union("d", "n"))
    SetOperationValues.legSchemas(refused, operators, es8) shouldBe
    MultiSearch.branchTypes(refused, operators).map(_ => Nil)
  }
}
