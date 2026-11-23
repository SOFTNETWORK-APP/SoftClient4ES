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

import app.softnetwork.elastic.client.SetOperationValues.{LegSchema, LegSlot}
import app.softnetwork.elastic.sql.{StringValue, Value}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{MultiSearch, SingleSearch}
import app.softnetwork.elastic.sql.query.MultiSearch.{
  SetOperationColumn,
  SetOperationType,
  StructField
}
import app.softnetwork.elastic.sql.operator.SetOperator
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

/** A `NULL` literal in a set-operation branch declares its column's type for that leg
  * ([[SetOperationValues.legSchemas]]): the column's type as the relational engine (DuckDB 1.5.5)
  * types it -- [[MultiSearch.columnTypes]] ' result, THE function core's `describe` reads -- read
  * as NULL every row. No slot where that type is NULL (every branch a NULL), not known at some
  * depth, or of unknown width (`NUMERIC`, `TEMPORAL`). Every other leg keeps its slot.
  */
class SetOperationNullSlotSpec extends AnyFlatSpec with Matchers {

  private def format(f: String): ListMap[String, Value[_]] =
    ListMap[String, Value[_]]("format" -> StringValue(f))

  private val zoneName = "yyyy-MM-dd HH:mm z"

  private val t = Table(
    "t",
    columns = List(
      Column("n", SQLTypes.Int),
      Column("l", SQLTypes.BigInt),
      Column("x", SQLTypes.Double),
      Column("f", SQLTypes.Real),
      Column("b", SQLTypes.Boolean),
      Column("s", SQLTypes.Keyword),
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("c_es", SQLTypes.Timestamp, options = format("epoch_second")),
      Column("tsz", SQLTypes.Timestamp, options = format(zoneName)),
      Column("bin", SQLTypes.VarBinary),
      Column(
        "o",
        SQLTypes.Struct,
        multiFields = List(
          Column("a", SQLTypes.Int),
          Column("e", SQLTypes.Timestamp, options = format("epoch_second"))
        )
      ),
      Column("o2", SQLTypes.Struct, multiFields = List(Column("b", SQLTypes.Keyword))),
      Column(
        "oz",
        SQLTypes.Struct,
        multiFields = List(Column("z", SQLTypes.Timestamp, options = format(zoneName)))
      ),
      Column("g", SQLTypes.GeoPoint),
      Column("ai", SQLTypes.Array(SQLTypes.Int)),
      Column("ao", SQLTypes.Array(SQLTypes.Struct), multiFields = List(Column("a", SQLTypes.Int))),
      Column("fl", SQLTypes.Struct),
      Column("om", SQLTypes.Struct, multiFields = List(Column("meta", SQLTypes.Any))),
      Column("meta", SQLTypes.Any)
    )
  )

  /** A one-column index, for a `SELECT *` branch. */
  private val t1 = Table("t1", columns = List(Column("n", SQLTypes.Int)))

  private val es8 = Some("8.18.3")

  private def resolved(sql: String): (Seq[SingleSearch], Seq[SetOperator]) =
    Parser(sql) match {
      case Right(m: MultiSearch) =>
        (
          m.requests.map(r => r.update(Some(if (r.sources.contains("t1")) t1 else t))),
          m.resolvedOperators
        )
      case other => fail(s"[$sql] expected a set operation, got $other")
    }

  private def columns(sql: String): Seq[SetOperationColumn] = {
    val (requests, operators) = resolved(sql)
    MultiSearch.columnTypes(requests, operators).fold(e => fail(s"[$sql] refused: $e"), identity)
  }

  private def schemas(sql: String, version: => Option[String] = es8): Seq[Option[LegSchema]] = {
    val (requests, operators) = resolved(sql)
    SetOperationValues.legSchemas(requests, operators, version) match {
      case Right(legs)   => legs
      case Left(refusal) => fail(s"[$sql] refused: $refusal")
    }
  }

  private def slots(sql: String): Seq[Option[Seq[Option[LegSlot]]]] =
    schemas(sql).map(_.map(_.slots))

  private def slotAt(sql: String, leg: Int, position: Int = 0): Option[LegSlot] =
    schemas(sql)(leg).flatMap(_.slots(position))

  /** A document's value as core receives it: its JSON, read by the response parser. */
  private def received(json: String): Any =
    ElasticConversion.jsonNodeToAny(JacksonConfig.objectMapper.readTree(json), ListMap.empty)

  /** A type as DuckDB names it: `INT`, `STRUCT(a INT, e TIMESTAMP)`, `INT[]`, `GEO_POINT`. */
  private def render(t: SetOperationType): String =
    t.sqlType match {
      case SQLTypes.Struct =>
        t.fields.map(f => s"${f.name} ${render(f.valueType)}").mkString("STRUCT(", ", ", ")")
      case SQLTypes.Array(elementType) =>
        t.element.map(render).getOrElse(elementType.typeId) + "[]"
      case scalar => scalar.typeId
    }

  // ---------------------------------------------------------------------------------------------
  // The population (spec §4): every cell, NULL first or second, each operator
  // ---------------------------------------------------------------------------------------------

  /** One statement: its SQL, the leg holding the NULL literal, its position, the expected type. */
  private final case class Cell(
    group: String,
    sql: String,
    leg: Int,
    position: Int,
    expected: String
  )

  private val operators = Seq("UNION ALL", "UNION", "INTERSECT", "EXCEPT")

  private def branch(item: String): String = s"SELECT $item AS k FROM t"

  /** Each order of `items` (as listed, and with the NULL one place over) under each operator. */
  private def uniform(group: String, items: Seq[String], expected: String): Seq[Cell] = {
    val i = items.indexOf("NULL")
    val j = if (i == 0) 1 else i - 1
    val swapped = items.updated(i, items(j)).updated(j, "NULL")
    for {
      order    <- Seq(items, swapped)
      operator <- operators
    } yield Cell(
      group,
      order.map(branch).mkString(s" $operator "),
      order.indexOf("NULL"),
      0,
      expected
    )
  }

  /** `items` under the operators given, as listed and with the NULL one place over. */
  private def fixed(
    group: String,
    items: Seq[String],
    ops: Seq[String],
    expected: String
  ): Seq[Cell] = {
    val i = items.indexOf("NULL")
    val j = if (i == 0) 1 else i - 1
    val swapped = items.updated(i, items(j)).updated(j, "NULL")
    Seq(items, swapped).map { order =>
      val sql = order.map(branch).head + order.tail
        .map(branch)
        .zip(ops)
        .map { case (b, op) => s" $op $b" }
        .mkString
      Cell(group, sql, order.indexOf("NULL"), 0, expected)
    }
  }

  private val g1: Seq[Cell] = Seq(
    "n"    -> "INT",
    "l"    -> "BIGINT",
    "x"    -> "DOUBLE",
    "f"    -> "REAL",
    "b"    -> "BOOLEAN",
    "s"    -> "VARCHAR",
    "d"    -> "DATE",
    "ts"   -> "TIMESTAMP",
    "c_es" -> "TIMESTAMP",
    "bin"  -> "VARBINARY",
    "o.a"  -> "INT",
    "o.e"  -> "TIMESTAMP"
  ).flatMap { case (sibling, expected) => uniform("G1", Seq("NULL", sibling), expected) }

  private val g2: Seq[Cell] = Seq(
    "tsz"  -> "TIMESTAMP",
    "oz"   -> "STRUCT(z TIMESTAMP)",
    "oz.z" -> "TIMESTAMP"
  ).flatMap { case (sibling, expected) => uniform("G2", Seq("NULL", sibling), expected) }

  private val g3: Seq[Cell] = Seq(
    Seq("NULL", "o")       -> "STRUCT(a INT, e TIMESTAMP)",
    Seq("NULL", "g")       -> "GEO_POINT",
    Seq("NULL", "ai")      -> "INT[]",
    Seq("NULL", "ao")      -> "STRUCT(a INT)[]",
    Seq("NULL", "o", "o2") -> "STRUCT(a INT, e TIMESTAMP, b VARCHAR)",
    Seq("NULL", "g", "o")  -> "STRUCT(lat DOUBLE, lon DOUBLE, a INT, e TIMESTAMP)"
  ).flatMap { case (items, expected) => uniform("G3", items, expected) }

  private val g4: Seq[Cell] =
    uniform("G4", Seq("NULL", "n", "x"), "DOUBLE") ++
    uniform("G4", Seq("bin", "NULL", "s"), "VARBINARY") ++
    uniform("G4", Seq("bin", "NULL", "n"), "VARCHAR") ++
    fixed("G4", Seq("NULL", "d", "ts"), Seq("INTERSECT", "UNION"), "TIMESTAMP") ++
    fixed(
      "G4",
      Seq("NULL", "o", "o2"),
      Seq("INTERSECT", "UNION ALL"),
      "STRUCT(a INT, e TIMESTAMP, b VARCHAR)"
    ) ++
    fixed("G4", Seq("NULL", "n", "s"), Seq("UNION", "EXCEPT"), "VARCHAR")

  private val aggregationNull = "SELECT NULL AS k, COUNT(*) AS c FROM t"
  private val aggregationN = "SELECT n AS k, COUNT(*) AS c FROM t GROUP BY n"

  private val g5: Seq[Cell] = operators.flatMap { operator =>
    Seq(
      Cell("G5", s"$aggregationNull $operator $aggregationN", 0, 0, "INT"),
      Cell("G5", s"$aggregationN $operator $aggregationNull", 1, 0, "INT"),
      Cell("G5", s"SELECT NULL FROM t $operator SELECT n FROM t", 0, 0, "INT"),
      Cell("G5", s"SELECT n FROM t $operator SELECT NULL FROM t", 1, 0, "INT")
    )
  }

  private val population: Seq[Cell] = g1 ++ g2 ++ g3 ++ g4 ++ g5

  /** The NULL leg's slot of a population cell, or a failure naming the cell. */
  private def nullSlotOf(cell: Cell): LegSlot =
    slotAt(cell.sql, cell.leg, cell.position).getOrElse(fail(s"[${cell.sql}] no NULL slot"))

  "A NULL literal's slot" should "be its column's DuckDB type, every population cell, NULL first or second, each operator" in {
    val wrong = population.flatMap { cell =>
      val result = columns(cell.sql)(cell.position).result
      slotAt(cell.sql, cell.leg, cell.position) match {
        case Some(slot)
            if render(slot.sqlType) == cell.expected && slot.sqlType == result &&
              slot.asStored.isEmpty =>
          None
        case other =>
          Some(
            s"[${cell.sql}] ${cell.group} expected ${cell.expected} (= columnTypes ${render(result)}), " +
            s"got ${other.map(s => s"${render(s.sqlType)} ${s.asStored}")}"
          )
      }
    }
    wrong shouldBe empty
    // the population: G1 12 x 8, G2 3 x 8, G3 6 x 8, G4 3 x 8 + 3 x 2, G5 4 x 4
    population.size shouldBe 214
  }

  it should "read every value as NULL, with nothing as stored" in {
    val values = Seq(
      "null",
      "[null]",
      "3",
      "10000000000",
      "1.5",
      "true",
      "\"a\"",
      "\"2024-01-31\"",
      "\"2024-01-31T10:30:00Z\"",
      "1706697000",
      "\"2024-01-31 10:30 CET\"",
      "\"aGk=\"",
      """{"a":1,"e":1706697000}""",
      """{"e":1706697000}""",
      """{"b":"x"}""",
      """{"z":"2024-01-31 10:30 CET"}""",
      """{"lat":1.0,"lon":2.0}""",
      "[1,2]",
      """[{"a":1},{"a":2}]""",
      """["a","b"]"""
    ).map(received)
    val wrong = population.flatMap { cell =>
      val slot = nullSlotOf(cell)
      val read = (null +: values).map(v => scala.util.Try(slot.read(v)))
      if (read.forall(r => r.isSuccess && r.get == null) && slot.asStored.isEmpty) None
      else Some(s"[${cell.sql}] read ${read.filterNot(r => r.isSuccess && r.get == null)}")
    }
    wrong shouldBe empty
  }

  it should "type a routed date's NULL as the column declares it, the routed sibling as stored (D4)" in {
    Seq(
      ("tsz", "TIMESTAMP", "VARCHAR"),
      ("oz", "STRUCT(z TIMESTAMP)", "STRUCT(z VARCHAR)"),
      ("oz.z", "TIMESTAMP", "VARCHAR")
    ).foreach { case (sibling, nullType, siblingType) =>
      val sql = s"SELECT NULL AS k FROM t UNION SELECT $sibling AS k FROM t"
      withClue(s"[$sql] ") {
        slotAt(sql, 0).map(s => render(s.sqlType)) shouldBe Some(nullType)
        val routed = slotAt(sql, 1).getOrElse(fail("no sibling slot"))
        render(routed.sqlType) shouldBe siblingType
        routed.asStored should not be empty
      }
    }
  }

  it should "have no slot where the column is NULL, not known inside, or of unknown width (D3)" in {
    // every branch a NULL: the column's type is NULL
    operators.foreach { operator =>
      val sql = s"SELECT NULL AS k FROM t $operator SELECT NULL AS k FROM t"
      columns(sql).map(_.resultType) shouldBe Seq(SQLTypes.Null)
      slots(sql) shouldBe Seq(Some(Seq(None)), Some(Seq(None)))
    }
    // a field-less STRUCT, a STRUCT holding ANY, ANY: no leg has a slot, as for every leg today
    for {
      sibling  <- Seq("fl", "om", "meta")
      operator <- operators
      sql <- Seq(
        s"SELECT NULL AS k FROM t $operator SELECT $sibling AS k FROM t",
        s"SELECT $sibling AS k FROM t $operator SELECT NULL AS k FROM t"
      )
    } withClue(s"[$sql] ")(slots(sql) shouldBe Seq(Some(Seq(None)), Some(Seq(None))))
    // a column of unknown width, constructed (no SQL form yields one since A-175), at the column
    // and in a struct's field; REAL and VARBINARY are of known width and DO get one
    val dates = SetOperationValues.DateReading(es8)
    val nul = Some(SetOperationType(SQLTypes.Null))
    def column(result: SetOperationType): SetOperationColumn =
      SetOperationColumn(result, Seq(nul, Some(result)))
    Seq(
      SetOperationType(SQLTypes.Numeric),
      SetOperationType(SQLTypes.Temporal),
      SetOperationType(SQLTypes.Struct, fields = Seq(StructField("t", SQLTypes.Temporal))),
      SetOperationType(
        SQLTypes.Array(SQLTypes.Numeric),
        element = Some(SetOperationType(SQLTypes.Numeric))
      )
    ).foreach { result =>
      withClue(s"[$result] ")(
        SetOperationValues.legSlot(column(result), 0, "k", "", dates) shouldBe None
      )
    }
  }

  it should "give a REAL or a VARBINARY column a slot: their width is known (D3)" in {
    val dates = SetOperationValues.DateReading(es8)
    Seq(SetOperationType(SQLTypes.Real), SetOperationType(SQLTypes.VarBinary)).foreach { result =>
      val column =
        SetOperationColumn(result, Seq(Some(SetOperationType(SQLTypes.Null)), Some(result)))
      SetOperationValues.legSlot(column, 0, "k", "", dates).map(_.sqlType) shouldBe Some(result)
      // the sibling of that type is read as it comes, as before (valueTyped): no slot
      SetOperationValues.legSlot(column, 1, "k", "f", dates) shouldBe None
    }
  }

  it should "ask the version as main does, never for the NULL leg itself (P3)" in {
    def hasDate(t: SetOperationType): Boolean =
      t.sqlType match {
        case SQLTypes.Struct   => t.fields.exists(f => hasDate(f.valueType))
        case SQLTypes.Array(_) => t.element.exists(hasDate)
        case SQLTypes.Date | SQLTypes.Timestamp | SQLTypes.Time | SQLTypes.DateTime => true
        case _                                                                      => false
      }
    val wrong = population.flatMap { cell =>
      var asked = 0
      def version: Option[String] = { asked += 1; es8 }
      schemas(cell.sql, version)
      // main's count: the non-NULL legs' slots alone, over one statement's date reading
      var mainAsked = 0
      def mainVersion: Option[String] = { mainAsked += 1; es8 }
      val (requests, ops) = resolved(cell.sql)
      val cs = MultiSearch.columnTypes(requests, ops).fold(e => fail(e), identity)
      val mainDates = SetOperationValues.DateReading(mainVersion)
      for {
        leg      <- requests.indices if leg != cell.leg
        position <- cs.indices
      } {
        val item = requests(leg).select.fieldsWithComputedAliases(position)
        val field = Option(item.identifier.name).filter(_.nonEmpty).getOrElse(item.outputName)
        SetOperationValues.legSlot(cs(position), leg, item.outputName, field, mainDates)
      }
      // the NULL slot alone asks nothing
      var nullAsked = 0
      def nullVersion: Option[String] = { nullAsked += 1; es8 }
      SetOperationValues.legSlot(
        cs(cell.position),
        cell.leg,
        "k",
        "",
        SetOperationValues.DateReading(nullVersion)
      )
      val dateFree = !hasDate(cs(cell.position).result)
      if (asked == mainAsked && asked <= 1 && nullAsked == 0 && (!dateFree || asked == 0)) None
      else
        Some(
          s"[${cell.sql}] asked $asked, main $mainAsked, NULL slot $nullAsked, date-free $dateFree"
        )
    }
    wrong shouldBe empty
  }

  it should "keep two equal slots equal (one shared reader)" in {
    population.foreach { cell =>
      withClue(s"[${cell.sql}] ")(nullSlotOf(cell) shouldBe nullSlotOf(cell))
    }
  }

  it should "mark a row-query NULL item a script field, not an aggregation's" in {
    Seq(
      "SELECT NULL AS k FROM t UNION SELECT n AS k FROM t",
      "SELECT NULL FROM t UNION SELECT n FROM t"
    ).foreach { sql =>
      val (requests, _) = resolved(sql)
      val item = requests.head.select.fieldsWithComputedAliases.head
      withClue(s"[$sql] ")(requests.head.scriptFields should contain(item))
    }
    val (aggregation, _) = resolved(s"$aggregationNull UNION $aggregationN")
    aggregation.head.scriptFields shouldBe empty
  }

  it should "leave CAST(NULL AS ...) on its own path, typed as cast (G5 control)" in {
    val sql = "SELECT CAST(NULL AS BIGINT) AS k FROM t UNION SELECT n AS k FROM t"
    slotAt(sql, 0).map(s => render(s.sqlType)) shouldBe Some("BIGINT")
    slotAt(sql, 1).map(s => render(s.sqlType)) shouldBe Some("INT")
  }

  // ---------------------------------------------------------------------------------------------
  // G7: forms the engine does not read the slot of (recorded, not consumed)
  // ---------------------------------------------------------------------------------------------

  it should "type the recorded forms as core types their column (G7)" in {
    // a derived branch aliasing the column: the column is ANY, no slot anywhere
    val derived = "SELECT k FROM (SELECT NULL AS k FROM t) dd UNION SELECT n AS k FROM t"
    columns(derived).map(_.resultType) shouldBe Seq(SQLTypes.Any)
    slots(derived) shouldBe Seq(Some(Seq(None)), Some(Seq(None)))
    // beside a SELECT * branch: the declared column is the NULL alone, no slot (D3 a)
    val star = "SELECT NULL AS k FROM t UNION ALL SELECT * FROM t1"
    columns(star).map(_.resultType) shouldBe Seq(SQLTypes.Null)
    slots(star) shouldBe Seq(Some(Seq(None)), None)
    // a WITH host: the NULL leg's slot is its column's (the engine runs that branch nested)
    val withHost = "WITH a AS (SELECT n FROM t) SELECT NULL AS k FROM a UNION SELECT n AS k FROM t"
    slotAt(withHost, 0).map(s => render(s.sqlType)) shouldBe Some("INT")
  }
}
