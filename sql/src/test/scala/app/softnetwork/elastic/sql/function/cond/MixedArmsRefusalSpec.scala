package app.softnetwork.elastic.sql.function.cond

import app.softnetwork.elastic.schema.{FieldCaps, Index}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{mergedSchema, Column, Table, TableType}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A CASE or a COALESCE whose arms have no common type is refused once the types are read, with
  * DuckDB 1.5.5's reason (measured: `Cannot mix values of type INTEGER and DATE in CASE expression
  *   - an explicit cast is required`).
  */
class MixedArmsRefusalSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("kd", SQLTypes.Keyword),
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("n", SQLTypes.Int),
      Column("x", SQLTypes.Double),
      Column("b", SQLTypes.Boolean),
      // a column whose type is not known (a mapping type core reads as ANY, `flattened` among them)
      Column("un", SQLTypes.Any)
    )
  )

  private def refusals(sql: String): Seq[String] = Parser(sql) match {
    case Right(ss: SingleSearch) => ss.update(Some(schema)).typeErrors
    case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  "arms with no common type" should "be refused with DuckDB's reason" in {
    Seq(
      "CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE n END" ->
      "Cannot mix values of type INT and DATE in CASE expression - an explicit cast is required",
      "CASE WHEN n > 0 THEN ts ELSE x END" ->
      "Cannot mix values of type DOUBLE and TIMESTAMP in CASE expression - an explicit cast is required",
      "CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE CAST(ts AS TIME) END" ->
      "Cannot mix values of type TIME and DATE in CASE expression - an explicit cast is required",
      "CASE WHEN n > 0 THEN kd ELSE n END" ->
      "Cannot mix values of type INT and KEYWORD in CASE expression - an explicit cast is required",
      "CASE WHEN n > 0 THEN b ELSE CAST(d AS DATE) END" ->
      "Cannot mix values of type DATE and BOOLEAN in CASE expression - an explicit cast is required",
      "COALESCE(CAST(d AS DATE), n)" ->
      "Cannot mix values of type DATE and INT in COALESCE operator - an explicit cast is required",
      "COALESCE(ts, 1.5)" ->
      "Cannot mix values of type TIMESTAMP and DOUBLE in COALESCE operator - an explicit cast is required"
    ).foreach { case (expr, reason) =>
      withClue(s"[$expr] ")(refusals(s"SELECT $expr AS v FROM t") should contain(reason))
    }
  }

  // -- SCHEMA-7 (A-68) ----------------------------------------------------------------------------

  private def field(tpe: String, indices: Seq[String] = Nil): String =
    s"""{"type":"$tpe","metadata_field":false,"searchable":true,"aggregatable":true""" +
    (if (indices.isEmpty) ""
     else indices.map(i => s""""$i"""").mkString(""","indices":[""", ",", "]")) +
    "}"

  /** The fields of the A-68 fixture and their mapping types: a native column of each SQL type a D1
    * type reads as (`kd`, `nb`, `xd`, `rf`, `ts`), a text, a boolean and a second date (`d2`, the
    * TIME arm's), and the D1 columns.
    */
  private val a68Fields: Seq[(String, String)] = Seq(
    "id" -> "keyword",
    "kd" -> "keyword",
    "nb" -> "long",
    "xd" -> "double",
    "rf" -> "float",
    "ts" -> "date",
    "d2" -> "date",
    "tx" -> "text",
    "bo" -> "boolean",
    "sf" -> "scaled_float",
    "hf" -> "half_float",
    "u"  -> "unsigned_long",
    "dn" -> "date_nanos",
    "ip" -> "ip",
    "ck" -> "constant_keyword"
  )

  private def mappingOf(index: String, fields: Seq[(String, String)]): Table =
    Index(
      index,
      fields
        .map {
          case (c, "constant_keyword") => s""""$c":{"type":"constant_keyword","value":"C"}"""
          case (c, "scaled_float")     => s""""$c":{"type":"scaled_float","scaling_factor":100}"""
          case (c, t)                  => s""""$c":{"type":"$t"}"""
        }
        .mkString("""{"mappings":{"properties":{""", ",", """}},"settings":{}}""")
    ).schema

  /** What the client's `_field_caps` fallback answers for `body`
    * (`IndicesApi.fetchFieldCapsSchema`).
    */
  private def fieldCapsOf(source: String, body: String): Table =
    mergedSchema(
      source,
      FieldCaps.members(source, new ObjectMapper().readTree(body)).map { case (name, document) =>
        name -> Index(name, document).schema
      }
    )._1.copy(tableType = TableType.FieldCapabilities)

  /** `s7_t` on both origins; `_field_caps` reports `constant_keyword` as `keyword` (A-67). */
  private lazy val a68Tables: Seq[(String, Table)] = Seq(
    "mapping" -> mappingOf("s7_t", a68Fields),
    "field caps" -> fieldCapsOf(
      "s7_t",
      a68Fields
        .map { case (c, t) =>
          s""""$c":{"${if (t == "constant_keyword") "keyword" else t}":""" +
            field(if (t == "constant_keyword") "keyword" else t) + "}"
        }
        .mkString("""{"indices":["s7_t"],"fields":{""", ",", "}}")
    )
  )

  /** `s7_p*`, whose `lv` is a `keyword` in `s7_p1` and a `long` in `s7_p2`: the lattice types it
    * VARCHAR, as the keyword index's KEYWORD column (A-66).
    */
  private lazy val a68Patterns: Seq[(String, Table)] = {
    val common =
      Seq("id" -> "keyword", "kd" -> "keyword", "nb" -> "long", "ts" -> "date", "bo" -> "boolean")
    Seq(
      "mapping" -> mergedSchema(
        "s7_p*",
        Seq(
          "s7_p1" -> mappingOf("s7_p1", common :+ ("lv" -> "keyword")),
          "s7_p2" -> mappingOf("s7_p2", common :+ ("lv" -> "long"))
        )
      )._1,
      "field caps" -> fieldCapsOf(
        "s7_p*",
        (common.map { case (c, t) => s""""$c":{"$t":${field(t)}}""" } :+
        s""""lv":{"keyword":${field("keyword", Seq("s7_p1"))},"long":${field(
          "long",
          Seq("s7_p2")
        )}}""")
          .mkString("""{"indices":["s7_p1","s7_p2"],"fields":{""", ",", "}}")
      )
    )
  }

  /** The five conditionals, over the arm `%c` and the other arm `%o`. */
  private val conditionals: Seq[String] = Seq(
    "CASE WHEN id = 'r1' THEN %c ELSE %o END",
    "COALESCE(%c, %o)",
    "NULLIF(%c, %o)",
    "GREATEST(%c, %o)",
    "LEAST(%o, %c)"
  )

  /** The four places a conditional is read: a projection, a GROUP BY key, WHERE, ORDER BY. */
  private def positions(source: String): Seq[String] = Seq(
    s"SELECT id, %e AS v FROM $source",
    s"SELECT %e AS g, COUNT(*) AS cnt FROM $source GROUP BY g",
    // `= 1`, not `IS NOT NULL`: the parser does not read `CASE ... END IS NOT NULL` (as on main);
    // the native control gets the same comparison, so the two verdicts still compare one rule
    s"SELECT id FROM $source WHERE %e = 1",
    s"SELECT id FROM $source ORDER BY %e"
  )

  /** A conditional over a D1 or a lattice-VARCHAR column is refused exactly as over a native column
    * of its SQL type -- the same rule, the same reason -- on the mapping origin and the
    * `_field_caps` one: `ip` / `ck` / `lv` beside a number, a date or a BOOLEAN; `sf` / `hf` / `u`
    * beside text or a date; `dn` beside a number, text or a TIME.
    */
  "a conditional over a D1 or a lattice-VARCHAR arm" should "be refused as over a native column of its type (A-68)" in {
    // (the column, the native column of its SQL type, the other arms)
    val keywordArms = Seq("nb", "ts", "bo")
    val onTable: Seq[(String, String, Seq[String])] = Seq(
      ("ip", "kd", keywordArms),
      ("ck", "kd", keywordArms),
      ("sf", "xd", Seq("tx", "ts")),
      ("hf", "rf", Seq("tx", "ts")),
      ("u", "nb", Seq("tx", "ts")),
      ("dn", "ts", Seq("nb", "tx", "CAST(d2 AS TIME)"))
    )
    val cells =
      a68Tables.flatMap { case (origin, table) =>
        onTable.map { case (c, native, others) => (origin, table, "s7_t", c, native, others) }
      } ++ a68Patterns.map { case (origin, table) =>
        (origin, table, "s7_p*", "lv", "kd", keywordArms)
      }
    def errors(sql: String, table: Table): Seq[String] = Parser(sql) match {
      case Right(ss: SingleSearch) => ss.update(Some(table)).typeErrors
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }
    var count = 0
    for {
      (origin, table, source, c, native, others) <- cells
      other                                      <- others
      conditional                                <- conditionals
      position                                   <- positions(source)
    } {
      def statement(column: String): String =
        position.replace("%e", conditional.replace("%c", column).replace("%o", other))
      val sql = statement(c)
      withClue(s"[$origin] $sql: ") {
        val control = errors(statement(native), table)
        control should not be empty
        errors(sql, table) shouldBe control.map(_.replaceAll(s"\\b$native\\b", c))
        count += 1
      }
    }
    // two origins x (15 pairs over s7_t + 3 over s7_p*) x 5 conditionals x 4 places
    count shouldBe 2 * (15 + 3) * 5 * 4
  }

  "arms DuckDB reads together" should "be accepted" in {
    Seq(
      "CASE WHEN n > 0 THEN b ELSE n END", // INTEGER
      "CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE ts END", // TIMESTAMP
      "CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE '2024-01-01' END", // the literal is read as a DATE
      "CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE NULL END",
      "COALESCE(n, x)",
      "COALESCE(un, n)", // a type still unknown: Elasticsearch answers
      "CASE WHEN n > 0 THEN 1 ELSE 2.5 END"
    ).foreach { expr =>
      withClue(s"[$expr] ")(
        refusals(s"SELECT $expr AS v FROM t").filter(_.contains("Cannot mix")) shouldBe empty
      )
    }
  }
}
