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

import app.softnetwork.elastic.client.result.{
  DdlResult,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess,
  QueryResult,
  QueryRows,
  QueryStream
}
import app.softnetwork.elastic.scalatest.ElasticTestKit
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{CreateTable, MultiSearch, SingleSearch}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}

import scala.collection.immutable.ListMap
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util.Try

/** SCHEMA-6 on a real cluster (AC0, AC1, AC5, AC6):
  *
  *   - "round trip": a column declared `ARRAY<X>`, for every X the DDL accepts, reads back as
  *     `ARRAY<X>` (DESCRIBE TABLE), maps to X's Elasticsearch type, re-parses from SHOW CREATE
  *     TABLE, and reads its values as a `List` of the class its scalar twin reads;
  *   - "declaration": on `zs_ext`, created by a raw mapping, several values in a column fail the
  *     statement naming the ALTER that declares it a list; that ALTER runs IN PLACE (index uuid and
  *     mapping unchanged, `_meta` declared), and the column then reads its list;
  *   - "derived": a derived-table / CTE column typed from its body by `attachDerived` (Case 14,
  *     INT), core-alone execution still refused;
  *   - "struct order": sub-fields in their declared order (DESCRIBE TABLE, SHOW CREATE TABLE, the
  *     result schema), a raw mapping's in its order, an ALTER on it in place;
  *   - "array_agg": ARRAY_AGG answers `ARRAY<elem>`, every cell a list.
  */
trait ArrayRoundTripSpec extends GatewayIntegrationTestKit {
  self: ElasticTestKit =>

  private val mapper = new ObjectMapper()

  private lazy val api: ElasticClientApi = client.asInstanceOf[ElasticClientApi]

  private def run(sql: String): ElasticResult[QueryResult] =
    Await.result(client.run(sql), 60.seconds)

  private def runOk(sql: String): QueryResult =
    run(sql) match {
      case ElasticSuccess(result) => result
      case ElasticFailure(error)  => fail(s"[$sql] failed: ${error.message}")
    }

  private def rows(sql: String): Seq[Map[String, Any]] = collectRows(System.nanoTime(), run(sql))

  private def failureOf(sql: String): String =
    run(sql) match {
      case ElasticFailure(error) => error.message
      case ElasticSuccess(result) =>
        Try(collectRows(System.nanoTime(), ElasticSuccess(result))).failed.toOption
          .map(messageOf)
          .getOrElse(fail(s"[$sql] answered $result instead of failing"))
    }

  /** The failure in core's own words: a streamed result's failure reaches the test wrapped by
    * `futureValue` ("The future returned an exception of type: …, with message: ….").
    */
  private def messageOf(e: Throwable): String = e match {
    case wrapped: org.scalatest.exceptions.TestFailedException if wrapped.getCause != null =>
      messageOf(wrapped.getCause)
    case other => Option(other.getMessage).getOrElse(other.toString)
  }

  private def drop(index: String): Unit = {
    Try(Await.result(client.run(s"DROP TABLE IF EXISTS $index"), 60.seconds))
    Try(api.deleteIndex(index))
  }

  private def mappingOf(index: String): JsonNode =
    api.getMapping(index) match {
      case ElasticSuccess(json) =>
        val root = mapper.readTree(json)
        // `{index: {mappings: …}}`, `{mappings: …}` or the mapping itself, per client
        Option(root.get(index))
          .map(_.path("mappings"))
          .filterNot(_.isMissingNode)
          .orElse(Option(root.get("mappings")))
          .getOrElse(root)
      case other => fail(s"getMapping($index): $other")
    }

  private def uuidOf(index: String): String =
    api.getIndex(index) match {
      case ElasticSuccess(Some(i)) =>
        val settings = i.settings
        Seq(
          settings.path("settings").path("index").path("uuid"),
          settings.path("index").path("uuid"),
          settings.path(index).path("settings").path("index").path("uuid")
        ).find(n => !n.isMissingNode && n.asText().nonEmpty)
          .map(_.asText())
          .getOrElse(
            fail(s"no uuid in the settings of $index: $settings")
          )
      case other => fail(s"getIndex($index): $other")
    }

  private def load(index: String, mapping: String, docs: Seq[(String, String)]): Unit = {
    api
      .createIndex(index, settings = """{"number_of_shards": 1, "number_of_replicas": 0}""")
      .get shouldBe true
    api.setMapping(index, mapping).get shouldBe true
    docs.foreach { case (id, source) =>
      api.index(index, id, source) match {
        case ElasticSuccess(true) =>
        case other                => fail(s"indexing $id into $index: $other")
      }
    }
    api.refresh(index)
  }

  private def byId(rs: Seq[Map[String, Any]]): Map[String, Map[String, Any]] =
    rs.map(r => String.valueOf(r.getOrElse("id", null)) -> r).toMap

  private def cls(v: Any): String = if (v == null) "null" else v.getClass.getName

  // -----------------------------------------------------------------------------------------------
  // AC0 -- round trip of every X
  // -----------------------------------------------------------------------------------------------

  /** X, its Elasticsearch type, two values as JSON. */
  private val population: Seq[(SQLType, String, String, String)] = Seq(
    (SQLTypes.TinyInt, "byte", "1", "2"),
    (SQLTypes.SmallInt, "short", "1", "2"),
    (SQLTypes.Int, "integer", "1", "2"),
    (SQLTypes.BigInt, "long", "1", "2"),
    (SQLTypes.Real, "float", "1.5", "2.5"),
    (SQLTypes.Double, "double", "1.5", "2.5"),
    (SQLTypes.Boolean, "boolean", "true", "false"),
    (SQLTypes.Keyword, "keyword", "\"a\"", "\"b\""),
    (SQLTypes.Text, "text", "\"a\"", "\"b\""),
    (SQLTypes.Varchar, "text", "\"a\"", "\"b\""),
    (SQLTypes.Char, "text", "\"a\"", "\"b\""),
    (SQLTypes.Date, "date", "\"2024-01-01\"", "\"2024-01-02\""),
    (SQLTypes.Time, "date", "37800000", "41400000"),
    (SQLTypes.DateTime, "date", "\"2024-01-01T10:00:00\"", "\"2024-01-02T11:00:00\""),
    (SQLTypes.Timestamp, "date", "\"2024-01-01T10:00:00Z\"", "\"2024-01-02T11:00:00Z\""),
    (SQLTypes.VarBinary, "binary", "\"aGVsbG8=\"", "\"d29ybGQ=\""),
    (SQLTypes.GeoPoint, "geo_point", "[2.0, 1.0]", "[4.0, 3.0]"),
    (SQLTypes.Struct, "nested", """{"q": 1, "p": "a"}""", """{"q": 2, "p": "b"}""")
  )

  private def tableOf(x: SQLType): String = s"ar_${x.typeId.toLowerCase}"

  behavior of "an ARRAY<X> column (AC0)"

  it should "read back as ARRAY<X>: DESCRIBE TABLE, mapping, SHOW CREATE TABLE, rows" in {
    population.foreach { case (x, es, v1, v2) =>
      val table = tableOf(x)
      drop(table)
      val struct = x == SQLTypes.Struct
      val fields = if (struct) " FIELDS (q INT, p KEYWORD)" else ""
      val twin = if (struct) "" else s", s ${x.typeId}"
      runOk(s"CREATE TABLE $table (id KEYWORD, c ARRAY<${x.typeId}>$fields$twin, PRIMARY KEY (id))")
      withClue(s"ARRAY<${x.typeId}>: ") {
        // DESCRIBE TABLE
        val describe = rows(s"DESCRIBE TABLE $table")
        describe.find(_("Field") == "c").map(_("Type")) shouldBe Some(s"ARRAY<${x.typeId}>")
        // the mapping
        mappingOf(table).path("properties").path("c").path("type").asText() shouldBe es
        // SHOW CREATE TABLE re-parses to the same column
        val ddl = assertShowCreate(System.nanoTime(), run(s"SHOW CREATE TABLE $table"))
        Parser(ddl) match {
          case Right(ct: CreateTable) =>
            ct.schema.find("c").map(_.dataType) shouldBe Some(SQLTypes.Array(x))
          case other => fail(s"SHOW CREATE TABLE $table does not re-parse: $other\n$ddl")
        }
        // rows: [v1, v2], [v1], v1, null, absent
        val twinOf = if (struct) "" else s""", "s": $v1"""
        Seq(
          "r1" -> s"""{"id": "r1", "c": [$v1, $v2]$twinOf}""",
          "r2" -> s"""{"id": "r2", "c": [$v1]$twinOf}""",
          "r3" -> s"""{"id": "r3", "c": $v1$twinOf}""",
          "r4" -> s"""{"id": "r4", "c": null}""",
          "r5" -> s"""{"id": "r5"}"""
        ).foreach { case (id, source) => api.index(table, id, source).get shouldBe true }
        api.refresh(table)
        val read = byId(rows(s"SELECT id, c${if (struct) "" else ", s"} FROM $table"))
        def list(id: String): Seq[Any] = read(id)("c") match {
          case l: Seq[_] => l
          case other     => fail(s"$id: c = $other (${cls(other)}), not a List")
        }
        list("r1").size shouldBe 2
        list("r2").size shouldBe 1
        list("r3").size shouldBe 1
        read("r4")("c") shouldBe (null: Any)
        read("r5").getOrElse("c", null) shouldBe (null: Any)
        // every element in the class its scalar twin reads
        if (!struct) {
          val twinClass = cls(read("r1")("s"))
          (list("r1") ++ list("r2") ++ list("r3")).foreach(e => cls(e) shouldBe twinClass)
          list("r1").head shouldBe read("r1")("s")
        } else
          list("r1").foreach(e => e shouldBe a[scala.collection.Map[_, _]])
      }
      drop(table)
    }
  }

  it should "refuse a list of lists in every DDL statement, naming the column" in {
    val refusal = "ARRAY<ARRAY<INT>> is not supported"
    Seq(
      "CREATE TABLE ar_nested (id KEYWORD, c ARRAY<ARRAY<INT>>)",
      "ALTER TABLE ar_any ADD COLUMN c ARRAY<ARRAY<INT>>",
      "ALTER TABLE ar_any ALTER COLUMN c SET DATA TYPE ARRAY<ARRAY<INT>>",
      "ALTER TABLE ar_any ALTER COLUMN obj SET FIELD z ARRAY<ARRAY<INT>>"
    ).foreach { sql =>
      run(sql) match {
        case ElasticFailure(error) => withClue(s"$sql: ")(error.message should include(refusal))
        case other                 => fail(s"$sql answered $other")
      }
    }
  }

  // -----------------------------------------------------------------------------------------------
  // AC0 -- zs_ext: Q1's failure, its meta-only remedy, the list after
  // -----------------------------------------------------------------------------------------------

  private val ext = "zs_ext"

  private val extMapping =
    """{"properties": {
      |  "id": {"type": "keyword"}, "tags": {"type": "keyword"}, "n": {"type": "integer"},
      |  "s": {"type": "text", "fields": {"keyword": {"type": "keyword", "ignore_above": 256}}},
      |  "ts": {"type": "date"}, "geo": {"type": "geo_point"},
      |  "obj": {"properties": {"a": {"type": "integer"}}}
      |}}""".stripMargin

  private val extDocs = Seq(
    "e1" -> """{"id": "e1", "tags": ["a", "b"], "n": [1, 2], "s": ["x", "y"],
      | "ts": ["2024-01-01T10:00:00Z", "2024-01-02T10:00:00Z"],
      | "geo": [[2.0, 1.0], [4.0, 3.0]], "obj": [{"a": 5}, {"a": 50}]}""".stripMargin,
    "e2" -> """{"id": "e2", "tags": "c", "n": 3, "s": "z", "ts": "2024-01-03T10:00:00Z",
      | "geo": [6.0, 5.0], "obj": {"a": 7}}""".stripMargin,
    "e3" -> """{"id": "e3"}"""
  )

  behavior of "a field holding several values, not declared a list (AC0, Q1)"

  it should "fail naming the ALTER, which runs in place, then read the list" in {
    drop(ext)
    load(ext, extMapping, extDocs)
    // the STRUCT column holding a list of objects answers it (A-26)
    byId(rows(s"SELECT id, obj FROM $ext"))("e1")("obj") shouldBe List(
      ListMap("a" -> 5),
      ListMap("a" -> 50)
    )
    Seq(
      ("tags", "KEYWORD", "keyword", List("a", "b")),
      ("n", "INT", "integer", List(1, 2)),
      ("s", "TEXT", "text", List("x", "y")),
      ("ts", "TIMESTAMP", "date", null),
      ("geo", "GEO_POINT", "geo_point", null)
    ).foreach { case (c, x, es, expected) =>
      withClue(s"$c: ") {
        val message = failureOf(s"SELECT id, $c FROM $ext")
        message should include(s"Column $c holds several values in document e1")
        message should include(s"index $ext")
        val remedy = message.substring(message.indexOf("ALTER TABLE"))
        remedy shouldBe s"ALTER TABLE $ext ALTER COLUMN $c SET DATA TYPE ARRAY<$x>"
        val uuid = uuidOf(ext)
        runOk(remedy) shouldBe DdlResult(true)
        uuidOf(ext) shouldBe uuid // in place: no reindex
        val mapping = mappingOf(ext)
        mapping.path("properties").path(c).path("type").asText() shouldBe es
        mapping.path("_meta").path("columns").path(c).path("data_type").asText() shouldBe
        s"ARRAY<$x>"
        rows(s"DESCRIBE TABLE $ext").find(_("Field") == c).map(_("Type")) shouldBe
        Some(s"ARRAY<$x>")
        val read = byId(rows(s"SELECT id, $c FROM $ext"))
        read("e1")(c) shouldBe a[Seq[_]]
        read("e1")(c).asInstanceOf[Seq[_]].size shouldBe 2
        if (expected != null) read("e1")(c) shouldBe expected
        read("e2")(c).asInstanceOf[Seq[_]].size shouldBe 1
        read("e3").getOrElse(c, null) shouldBe (null: Any)
      }
    }
    // a scalar sub-field under the list of objects: SET FIELD
    val message = failureOf(s"SELECT id, obj.a FROM $ext")
    message should include("Column obj.a holds several values in document e1")
    val remedy = message.substring(message.indexOf("ALTER TABLE"))
    remedy shouldBe s"ALTER TABLE $ext ALTER COLUMN obj SET FIELD a ARRAY<INT>"
    val uuid = uuidOf(ext)
    runOk(remedy) shouldBe DdlResult(true)
    uuidOf(ext) shouldBe uuid
    val paths = byId(rows(s"SELECT id, obj.a FROM $ext"))
    paths("e1")("obj.a") shouldBe List(5, 50)
    paths("e2")("obj.a") shouldBe List(7)
    byId(rows(s"SELECT id, obj FROM $ext"))("e1")("obj") shouldBe List(
      ListMap("a" -> List(5)),
      ListMap("a" -> List(50))
    )
    drop(ext)
  }

  /** Under an index PATTERN the failure names the index the hit came from (`_index`), never the
    * pattern: `ALTER TABLE "zs_ext_*" …` is refused by Elasticsearch (`Invalid index`), so naming
    * the pattern shipped a remedy that cannot run on the commonest time-series shape. Each client
    * must carry the hit's `_index` into the rows it parses (the ES 8 / 9 Java clients build their
    * own hit envelope), on a plain search and on a sorted one alike.
    */
  it should "name the hit's own index under an index pattern, and that ALTER runs in place" in {
    val concrete = s"${ext}_p1"
    val pattern = s"${ext}_p*"
    drop(concrete)
    load(concrete, extMapping, extDocs)
    Seq(s"SELECT id, tags FROM $pattern", s"SELECT id, tags FROM $pattern ORDER BY id").foreach {
      sql =>
        withClue(s"[$sql] ") {
          val message = failureOf(sql)
          message should include(
            s"Column tags holds several values in document e1 of index $concrete"
          )
          message.substring(message.indexOf("ALTER TABLE")) shouldBe
          s"ALTER TABLE $concrete ALTER COLUMN tags SET DATA TYPE ARRAY<KEYWORD>"
        }
    }
    val remedy = s"ALTER TABLE $concrete ALTER COLUMN tags SET DATA TYPE ARRAY<KEYWORD>"
    val uuid = uuidOf(concrete)
    runOk(remedy) shouldBe DdlResult(true)
    uuidOf(concrete) shouldBe uuid // in place: no reindex
    val read = byId(rows(s"SELECT id, tags FROM $pattern"))
    read("e1")("tags") shouldBe List("a", "b")
    read("e2")("tags") shouldBe List("c")
    drop(concrete)
  }

  // -----------------------------------------------------------------------------------------------
  // AC1 -- a derived / CTE column typed from its body
  // -----------------------------------------------------------------------------------------------

  private val zs = "zs_t"

  behavior of "a derived-table or CTE column (AC1)"

  it should "be typed INT from its body through attachDerived, while core alone still refuses it" in {
    drop(zs)
    runOk(s"CREATE TABLE $zs (id KEYWORD, n INT, d DATE, PRIMARY KEY (id))")
    Seq(
      s"SELECT q.c1 FROM (SELECT n AS c1 FROM $zs) q",
      s"SELECT c1 FROM (SELECT n AS c1 FROM $zs) q",
      s"SELECT q2.c1 FROM (SELECT q.c1 FROM (SELECT n AS c1 FROM $zs) q) q2",
      s"WITH q AS (SELECT n AS c1 FROM $zs) SELECT q.c1 FROM q"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val single = Parser(sql) match {
          case Right(s: SingleSearch) => s
          case other                  => fail(s"$other")
        }
        api.attachDerived(single) match {
          case ElasticSuccess(attached) =>
            MultiSearch.projectionTypes(attached).map(_.map(_._2.sqlType)) shouldBe Some(
              Seq(SQLTypes.Int)
            )
          case other => fail(s"attachDerived: $other")
        }
        // core alone keeps 0.24.0's closure refusal (D11)
        run(sql) match {
          case ElasticFailure(_) =>
          case other             => fail(s"core ran a derived statement: $other")
        }
      }
    }
    val star = Parser(s"SELECT q.* FROM (SELECT n AS c1 FROM $zs) q") match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"$other")
    }
    api.attachDerived(star) match {
      case ElasticSuccess(attached) =>
        attached.schema.flatMap(_.find("c1")).map(_.dataType) shouldBe Some(SQLTypes.Int)
      case other => fail(s"attachDerived: $other")
    }
    // Case 15: a DATE beside the derived INT is #420 rule 1's refusal
    Parser(s"SELECT d AS k FROM $zs UNION ALL SELECT q.c1 FROM (SELECT n AS c1 FROM $zs) q") match {
      case Right(m: MultiSearch) =>
        api.attachDerived(m) match {
          case ElasticSuccess(attached) =>
            MultiSearch.branchTypes(attached.requests, attached.resolvedOperators) shouldBe Left(
              "Set operation branches must project compatible types at column 1: branch 1 'k' " +
              "is DATE, branch 2 'c1' is INT"
            )
          case other => fail(s"attachDerived: $other")
        }
      case other => fail(s"$other")
    }
    drop(zs)
  }

  // -----------------------------------------------------------------------------------------------
  // AC5 -- struct order
  // -----------------------------------------------------------------------------------------------

  private val so = "so_t"
  private val soRaw = "so_raw"

  behavior of "a struct's sub-fields (AC5)"

  it should "keep their declared order: DESCRIBE TABLE, SHOW CREATE TABLE, the result schema" in {
    drop(so)
    runOk(
      s"CREATE TABLE $so (id KEYWORD, obj STRUCT FIELDS (y INT, b KEYWORD), " +
      "items ARRAY<STRUCT> FIELDS (q INT, p KEYWORD), PRIMARY KEY (id))"
    )
    api.index(
      so,
      "s1",
      """{"id": "s1", "obj": {"b": "x", "y": 1}, "items": [{"p": "a", "q": 2}]}"""
    )
    api.refresh(so)
    // each struct's sub-fields in their declared order (D9); the columns themselves keep the
    // mapping's order, which D9 does not touch
    val described = rows(s"DESCRIBE TABLE $so").map(_("Field").toString)
    described.filter(_.startsWith("obj.")) shouldBe Seq("obj.y", "obj.b")
    described.filter(_.startsWith("items.")) shouldBe Seq("items.q", "items.p")
    val ddl = assertShowCreate(System.nanoTime(), run(s"SHOW CREATE TABLE $so"))
    ddl.indexOf("y INT") should be < ddl.indexOf("b KEYWORD")
    ddl.indexOf("q INT") should be < ddl.indexOf("p KEYWORD")
    // a statement with no LIMIT streams its rows: its schema is the stream's
    val result = runOk(s"SELECT obj, items FROM $so")
    val schema = result match {
      case q: QueryRows   => q.schema
      case q: QueryStream => q.schema
      case other          => fail(s"$other")
    }
    val columns = schema.getOrElse(fail("no schema")).columns
    columns.head.fields.map(_.name) shouldBe Seq("y", "b")
    columns(1).element.map(_.fields.map(_.name)) shouldBe Some(Seq("q", "p"))
    // the rows keep `_source`'s key order (A-14)
    collectRows(System.nanoTime(), ElasticSuccess(result))
      .head("obj")
      .asInstanceOf[scala.collection.Map[String, Any]]
      .keys
      .toSeq shouldBe Seq("b", "y")
    drop(so)
  }

  it should "keep a raw mapping's order without field_order, and ALTER it in place" in {
    drop(soRaw)
    load(
      soRaw,
      """{"properties": {"id": {"type": "keyword"},
        | "obj": {"properties": {"y": {"type": "integer"}, "b": {"type": "keyword"}}}}}""".stripMargin,
      Seq("r1" -> """{"id": "r1", "obj": {"y": 1, "b": "x"}}""")
    )
    rows(s"DESCRIBE TABLE $soRaw")
      .map(_("Field").toString)
      .filter(_.startsWith("obj.")) shouldBe Seq("obj.b", "obj.y")
    val uuid = uuidOf(soRaw)
    runOk(
      s"ALTER TABLE $soRaw ALTER COLUMN obj SET FIELD b KEYWORD OPTIONS (ignore_above = 10)"
    ) shouldBe
    DdlResult(true)
    uuidOf(soRaw) shouldBe uuid
    drop(soRaw)
  }

  // -----------------------------------------------------------------------------------------------
  // AC6 -- ARRAY_AGG declared ARRAY<elem>
  // -----------------------------------------------------------------------------------------------

  private val agg = "ar_agg"

  behavior of "ARRAY_AGG (AC6)"

  it should "answer ARRAY<elem>, every cell a list, a one-element group too" in {
    drop(agg)
    load(
      agg,
      """{"properties": {"id": {"type": "keyword"}, "g": {"type": "keyword"},
        | "k": {"type": "keyword"}}}""".stripMargin,
      Seq(
        "a1" -> """{"id": "a1", "g": "x", "k": "a"}""",
        "a2" -> """{"id": "a2", "g": "x", "k": "b"}""",
        "a3" -> """{"id": "a3", "g": "y", "k": "c"}"""
      )
    )
    run(s"SELECT g, ARRAY_AGG(k) AS a FROM $agg GROUP BY g") match {
      case ElasticSuccess(result) =>
        val rs = collectRows(System.nanoTime(), ElasticSuccess(result))
        rs.foreach(r => r("a") shouldBe a[Seq[_]])
        rs.find(_("g") == "y").map(_("a")) shouldBe Some(List("c"))
      case other => fail(s"$other")
    }
    drop(agg)
  }

  private val perElement = "ar_elem"

  behavior of "an ARRAY<X> column read per element beside its scalar twin (D7)"

  // a quoted number beside a whole column is the whole number it rounds to, sent to Elasticsearch
  // as that number, and ROUND takes its operand's type: an `ARRAY<INT>` column is matched and
  // rounded per element, so it answers what its INT twin `n` (each document's first element)
  // answers
  it should "match a quoted number and round to tens as its twin does" in {
    drop(perElement)
    runOk(s"CREATE TABLE $perElement (id KEYWORD, arr ARRAY<INT>, n INT, PRIMARY KEY (id))")
    Seq(
      "e1" -> """{"id": "e1", "arr": [2, 9], "n": 2}""",
      "e2" -> """{"id": "e2", "arr": [1], "n": 1}""",
      "e3" -> """{"id": "e3", "arr": 3, "n": 3}""",
      "e4" -> """{"id": "e4", "arr": [14, 20], "n": 14}"""
    ).foreach { case (id, source) => api.index(perElement, id, source).get shouldBe true }
    api.refresh(perElement)
    def ids(where: String): Seq[Any] =
      rows(s"SELECT id FROM $perElement WHERE $where").map(_("id")).sortBy(String.valueOf)
    // '12.5' is 13 (half away from zero), so `> '12.5'` keeps 14 only
    Seq("= '1.5'", "IN ('1.5', '3')", "BETWEEN '1.5' AND '2.5'", "> '12.5'").foreach { predicate =>
      withClue(s"[c $predicate] ") {
        val twin = ids(s"n $predicate")
        twin should not be empty
        ids(s"arr $predicate") shouldBe twin
      }
    }
    ids("arr = '1.5'") shouldBe Seq("e1")
    ids("arr > '12.5'") shouldBe Seq("e4")
    // a HAVING key filter on the element groups: '1.5' keeps the group 2, never the group 1
    def groups(c: String): Seq[(Any, Any)] =
      rows(s"SELECT $c, COUNT(*) AS k FROM $perElement GROUP BY $c HAVING $c = '1.5'")
        .map(r => (r(c), r("k")))
    groups("arr").map { case (key, count) => (String.valueOf(key), count) } shouldBe
    groups("n").map { case (key, count) => (String.valueOf(key), count) }
    groups("arr").map(g => String.valueOf(g._1)) shouldBe Seq("2")
    // ROUND to tens: a BIGINT, a Long per row, the twin's value
    val rounded = byId(rows(s"SELECT id, ROUND(arr, -1) AS r, ROUND(n, -1) AS t FROM $perElement"))
    rounded.values.foreach { row =>
      withClue(s"[${row("id")}] ") {
        row("r") shouldBe row("t")
        cls(row("r")) shouldBe classOf[java.lang.Long].getName
      }
    }
    drop(perElement)
  }
}
