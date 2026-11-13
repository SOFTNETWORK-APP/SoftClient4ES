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

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.`type`.{SQLTypeUtils, SQLTypes}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{DerivedTable, MultiSearch, SingleSearch, Statement}
import app.softnetwork.elastic.sql.schema.Schema
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.typesafe.config.ConfigFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

/** SCHEMA-2 (D14, AC1) -- `describe(sql)` answers `run(sql)`'s schema, running nothing: no search
  * (one-shot, `_msearch`, async twins, scroll), no subquery; and refuses iff `run`'s resolve
  * refuses, with the same status and message.
  */
class DescribeSpec extends AnyFlatSpec with Matchers with ScalaFutures with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem("describe-spec")
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(20.seconds))

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private val mapper = new ObjectMapper()

  private def schemaOf(index: String, properties: String, meta: String = "{}"): Schema =
    Index(
      index,
      s"""{"$index": {"aliases": {}, "mappings": {"_meta": {"columns": $meta},
         | "properties": $properties}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
    ).schema

  private val s2: Schema = schemaOf(
    "s2_t",
    """{"id": {"type": "keyword"}, "k": {"type": "keyword"}, "ts": {"type": "date"},
      | "c_des": {"type": "date", "format": "epoch_second"},
      | "c_c1": {"type": "date", "format": "yyyy/MM/dd"},
      | "tsz": {"type": "date", "format": "yyyy-MM-dd HH:mm z"},
      | "n": {"type": "integer"}, "x": {"type": "double"}, "b": {"type": "boolean"},
      | "g": {"type": "geo_point"}, "bin": {"type": "binary"},
      | "obj": {"properties": {"a": {"type": "integer"}}}}""".stripMargin,
    """{"c_des": {"data_type": "DATE"}}"""
  )

  private val other: Schema =
    schemaOf("other_t", """{"oid": {"type": "keyword"}, "m": {"type": "integer"}}""")

  private val emptyHits: JsonNode =
    mapper.readTree("""{"hits": {"total": {"value": 0, "relation": "eq"}, "hits": []}}""")

  /** Counts every search a statement could send; seeded schemas, a known version. */
  private class CountingClient extends NopeClientApi {
    override protected def logger: Logger = LoggerFactory.getLogger(getClass)

    val searches = new AtomicInteger(0)

    /** Every schema lookup a statement makes, a cache hit included (SCHEMA-15 D5). */
    val reads = new AtomicInteger(0)

    override private[client] def loadQuerySchema(source: String): ElasticResult[Schema] = {
      reads.incrementAndGet()
      super.loadQuerySchema(source)
    }

    override private[client] def executeVersion(): ElasticResult[String] =
      ElasticResult.success("8.18.3")

    override private[client] def executeSingleSearch(
      elasticQuery: ElasticQuery
    ): ElasticResult[Option[JsonNode]] = {
      searches.incrementAndGet()
      ElasticResult.success(Some(emptyHits))
    }

    override private[client] def executeMultiSearch(
      elasticQueries: ElasticQueries
    ): ElasticResult[Option[JsonNode]] = {
      searches.incrementAndGet()
      ElasticResult.success(Some(responses(elasticQueries)))
    }

    override private[client] def executeSingleSearchAsync(elasticQuery: ElasticQuery)(implicit
      ec: ExecutionContext
    ): Future[ElasticResult[Option[JsonNode]]] = {
      searches.incrementAndGet()
      Future.successful(ElasticResult.success(Some(emptyHits)))
    }

    override private[client] def executeMultiSearchAsync(elasticQueries: ElasticQueries)(implicit
      ec: ExecutionContext
    ): Future[ElasticResult[Option[JsonNode]]] = {
      searches.incrementAndGet()
      Future.successful(ElasticResult.success(Some(responses(elasticQueries))))
    }

    /** A scroll is OPENED here: counted even when nobody materialises it. */
    override private[client] def scrollWithSchema(
      statement: app.softnetwork.elastic.sql.query.SearchStatement,
      config: app.softnetwork.elastic.client.scroll.ScrollConfig
    )(implicit
      system: ActorSystem,
      context: ConversionContext
    ) = {
      searches.incrementAndGet()
      super.scrollWithSchema(statement, config)
    }

    private def responses(elasticQueries: ElasticQueries): JsonNode = {
      val root = mapper.createObjectNode()
      val array = root.putArray("responses")
      elasticQueries.queries.foreach(_ => array.add(emptyHits.deepCopy[JsonNode]()))
      root
    }
  }

  private def client(): CountingClient = {
    val c = new CountingClient
    c.updateSchema("s2_t", s2)
    c.updateSchema("other_t", other)
    c
  }

  private def runSchema(c: CountingClient, sql: String): ElasticResult[Option[ResultSchema]] =
    c.run(sql).futureValue match {
      case ElasticSuccess(q: QueryRows)       => ElasticSuccess(q.schema)
      case ElasticSuccess(q: QueryStream)     => ElasticSuccess(q.schema)
      case ElasticSuccess(q: QueryStructured) => ElasticSuccess(q.schema)
      case ElasticSuccess(other: QueryResult) => fail(s"[$sql] answered $other")
      case ElasticFailure(error)              => ElasticFailure(error)
    }

  /** P1, generated over the fixture's columns, not hand-picked. */
  private val p1: Seq[String] = {
    val columns = Seq("id", "k", "ts", "c_des", "c_c1", "tsz", "n", "x", "b", "g", "bin", "obj")
    columns.flatMap { c =>
      Seq(
        s"SELECT $c FROM s2_t",
        s"SELECT $c AS kk FROM s2_t LIMIT 10",
        s"SELECT CAST($c AS VARCHAR) AS s FROM s2_t LIMIT 10"
      )
    } ++ Seq(
      "SELECT * FROM s2_t",
      "SELECT * FROM s2_t LIMIT 10",
      "SELECT k, COUNT(*) AS c, MIN(n) AS mi, MAX(n) AS ma FROM s2_t GROUP BY k",
      "SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM s2_t LIMIT 10",
      "SELECT id, n + 1 AS v FROM s2_t LIMIT 10",
      "SELECT DATE_FORMAT(ts, '%Y') AS y FROM s2_t LIMIT 10",
      "SELECT UPPER(k) AS u, n FROM s2_t",
      "SELECT n AS v FROM s2_t LIMIT 5 UNION ALL SELECT n AS v FROM s2_t LIMIT 5",
      "SELECT n AS v FROM s2_t LIMIT 5 UNION ALL SELECT x AS v FROM s2_t LIMIT 5",
      "SELECT ts AS v FROM s2_t LIMIT 5 UNION ALL SELECT c_des AS v FROM s2_t LIMIT 5",
      "SELECT tsz AS v FROM s2_t LIMIT 5 UNION ALL SELECT ts AS v FROM s2_t LIMIT 5",
      "SELECT id FROM s2_t UNION ALL SELECT oid AS id FROM other_t"
    )
  }

  "describe" should "answer run's schema over P1, running no search (AC1)" in {
    p1.foreach { sql =>
      withClue(s"[$sql] ") {
        val c = client()
        val described = c.describe(sql)
        c.searches.get() shouldBe 0
        val ran = runSchema(client(), sql)
        (described, ran) match {
          case (ElasticSuccess(d), ElasticSuccess(r)) => d shouldBe r
          case (ElasticFailure(d), ElasticFailure(r)) =>
            d.statusCode shouldBe r.statusCode
            d.message shouldBe r.message
          case other => fail(s"describe and run disagree: $other")
        }
      }
    }
    // the cells answer a schema at all
    client().describe("SELECT n, ts FROM s2_t") match {
      case ElasticSuccess(Some(schema)) =>
        schema.columns.map(c => c.name -> c.sqlType) shouldBe Seq(
          "n"  -> SQLTypes.Int,
          "ts" -> SQLTypes.Timestamp
        )
      case other => fail(s"$other")
    }
  }

  it should "run no subquery, and refuse a bare-name correlation as run does" in {
    val c = client()
    val uncorrelated = "SELECT id FROM s2_t WHERE n IN (SELECT m FROM other_t) LIMIT 10"
    c.describe(uncorrelated) match {
      case ElasticSuccess(Some(schema)) => schema.names shouldBe Seq("id")
      case other                        => fail(s"$other")
    }
    c.searches.get() shouldBe 0
    // a column of the outer index named bare in the inner statement: refused before execution
    val correlated = "SELECT id FROM s2_t WHERE n IN (SELECT m FROM other_t WHERE m = x) LIMIT 10"
    val described = client().describe(correlated)
    val ran = runSchema(client(), correlated)
    (described, ran) match {
      case (ElasticFailure(d), ElasticFailure(r)) =>
        d.statusCode shouldBe r.statusCode
        d.message shouldBe r.message
      case (ElasticSuccess(d), ElasticSuccess(r)) => d shouldBe r
      case other                                  => fail(s"describe and run disagree: $other")
    }
  }

  it should "refuse a JOIN as run does, and answer None under another extension" in {
    val join = "SELECT a.id FROM s2_t a JOIN other_t b ON a.id = b.oid"
    (client().describe(join), runSchema(client(), join)) match {
      case (ElasticFailure(d), ElasticFailure(r)) =>
        d.statusCode shouldBe r.statusCode
        d.message shouldBe r.message
      case other => fail(s"describe and run disagree on a JOIN: $other")
    }
    val foreign = new ExtensionSpi {
      override def extensionId: String = "describe-foreign-test"
      override def extensionName: String = "Foreign handler (test double)"
      override def version: String = "test"
      override def priority: Int = 1
      override def initialize(
        config: com.typesafe.config.Config,
        licenseRefreshStrategy: app.softnetwork.elastic.licensing.LicenseRefreshStrategy
      ): Either[String, Unit] = Right(())
      override def canHandle(statement: Statement): Boolean = true
      override def execute(statement: Statement, client: ElasticClientApi)(implicit
        system: ActorSystem
      ): Future[ElasticResult[QueryResult]] = Future.successful(ElasticSuccess(QueryRows(Nil)))
      override def supportedSyntax: Seq[String] = Seq.empty
    }
    val c = new CountingClient {
      override lazy val extensionRegistry: ExtensionRegistry =
        new ExtensionRegistry(ConfigFactory.load(), licenseRefreshStrategy) {
          override lazy val extensions: Seq[ExtensionSpi] = Seq(foreign)
        }
    }
    c.describe(join) shouldBe ElasticSuccess(None)
    c.searches.get() shouldBe 0
  }

  it should "answer a FROM-less statement, and refuse what is not a query (400)" in {
    client().describe("SELECT 1 AS one") match {
      case ElasticSuccess(Some(schema)) =>
        schema.columns.map(c => c.name -> c.sqlType) shouldBe Seq("one" -> SQLTypes.Int)
      case other => fail(s"$other")
    }
    Seq("CREATE TABLE zz (a INT)", "SELECT 1; SELECT 2", "SELEC nothing").foreach { sql =>
      client().describe(sql) match {
        case ElasticFailure(error) => error.statusCode shouldBe Some(400)
        case other                 => fail(s"[$sql] answered $other")
      }
    }
  }

  it should "answer None where run carries no schema (no mapping)" in {
    val c = new CountingClient // no seed: every lookup misses
    c.describe("SELECT id FROM nowhere_t") shouldBe ElasticSuccess(None)
    c.searches.get() shouldBe 0
  }

  // ---------------------------------------------------------------------------------------------
  // SCHEMA-15 -- `describePerTable`: a JOIN body typed per table, by describe's builder, running
  // nothing; every other statement answered as `describe` answers it
  // ---------------------------------------------------------------------------------------------

  private def mappingOf(columns: Seq[(String, String)]): String =
    columns.map { case (c, m) => s""""$c": $m""" }.mkString("{", ", ", "}")

  private val ptAColumns: Seq[(String, String)] = Seq(
    "aid"   -> """{"type": "keyword"}""",
    "bid"   -> """{"type": "keyword"}""",
    "k"     -> """{"type": "keyword"}""",
    "t"     -> """{"type": "text"}""",
    "n"     -> """{"type": "integer"}""",
    "big_a" -> """{"type": "long"}""",
    "x"     -> """{"type": "double"}""",
    "r"     -> """{"type": "float"}""",
    "ts"    -> """{"type": "date"}""",
    "dc"    -> """{"type": "date", "format": "yyyy/MM/dd"}""",
    "de"    -> """{"type": "date", "format": "epoch_second"}""",
    "b"     -> """{"type": "boolean"}""",
    "g"     -> """{"type": "geo_point"}""",
    "obj"   -> """{"properties": {"a": {"type": "integer"}}}""",
    "items" -> """{"type": "nested", "properties": {"p": {"type": "integer"}}}""",
    "tags"  -> """{"type": "integer"}""",
    "pz"    -> """{"type": "date", "format": "yyyy-MM-dd HH:mm z"}""",
    "dn"    -> """{"type": "date_nanos"}"""
  )

  /** `k` and `n` collide with `pt_a`'s, `n` with another type: a wrong attribution is visible. */
  private val ptBColumns: Seq[(String, String)] = Seq(
    "bid"   -> """{"type": "keyword"}""",
    "label" -> """{"type": "keyword"}""",
    "name"  -> """{"type": "text"}""",
    "m"     -> """{"type": "integer"}""",
    "big"   -> """{"type": "long"}""",
    "d2"    -> """{"type": "date"}""",
    "addr"  -> """{"properties": {"zip": {"type": "keyword"}}}""",
    "k"     -> """{"type": "keyword"}""",
    "n"     -> """{"type": "long"}"""
  )

  private val ptMeta = """{"de": {"data_type": "DATE"}, "tags": {"data_type": "ARRAY<INT>"}}"""

  private val ptA: Schema = schemaOf("pt_a", mappingOf(ptAColumns), ptMeta)
  private val ptB: Schema = schemaOf("pt_b", mappingOf(ptBColumns))

  /** The twin: every column of both tables (`pt_b`'s `bid`, `k`, `n` are `pt_a`'s). */
  private val ptAB: Schema = schemaOf(
    "pt_ab",
    mappingOf(ptAColumns ++ ptBColumns.filterNot(c => Set("bid", "k", "n").contains(c._1))),
    ptMeta
  )

  private def ptClient(dialect: String = "8.18.3"): CountingClient = {
    val c = new CountingClient {
      override private[client] def executeVersion(): ElasticResult[String] =
        ElasticResult.success(dialect)
    }
    c.updateSchema("pt_a", ptA)
    c.updateSchema("pt_b", ptB)
    c.updateSchema("pt_ab", ptAB)
    c
  }

  private def parsed(sql: String): Statement =
    Parser(sql) match {
      case Right(statement) => statement
      case Left(error)      => fail(s"[$sql] does not parse: ${error.msg}")
    }

  private def single(sql: String): SingleSearch =
    parsed(sql) match {
      case s: SingleSearch => s
      case other           => fail(s"[$sql] is not a single search: $other")
    }

  /** The item over its own table: the qualifier dropped. */
  private def unqualified(sql: String): String =
    sql.replaceAll("\\b(?:a2|b2|a|b|pt_a|pt_b)\\.", "")

  private val onBid = "ON a.bid = b.bid"

  private val joinKinds: Seq[String] =
    Seq("JOIN", "INNER JOIN", "LEFT JOIN", "RIGHT JOIN", "FULL JOIN")

  private def oneTable(q: String, c: String): Seq[String] =
    Seq(s"$q.$c", s"$q.$c AS v", s"CAST($q.$c AS VARCHAR) AS s")

  /** G1 -- (item, the table its oracle reads). */
  private val g1: Seq[(String, String)] =
    ptAColumns.map(_._1).flatMap(c => oneTable("a", c).map(_ -> "pt_a")) ++
    ptBColumns.map(_._1).flatMap(c => oneTable("b", c).map(_ -> "pt_b")) ++
    Seq("a.obj.a" -> "pt_a", "b.addr.zip AS zip" -> "pt_b")

  /** G2 -- items over both tables, typed as over the twin. */
  private val g2: Seq[(String, String)] = Seq(
    "COALESCE(a.n, b.m) AS v",
    "a.n + b.big AS v",
    "GREATEST(a.x, b.m) AS v",
    "CASE WHEN a.b THEN a.ts ELSE b.d2 END AS v",
    "NULLIF(a.k, b.label) AS v",
    "CONCAT(a.k, b.label) AS v",
    "UPPER(b.name) AS v",
    "a.ts - b.d2 AS v",
    "DATEDIFF(a.ts, b.d2) AS v"
  ).map(_ -> "pt_ab")

  /** G3 / B8 -- (select list, group by). */
  private val g3: Seq[(String, String)] = Seq(
    "b.label, COUNT(*) AS c, COUNT(DISTINCT a.k) AS dk, SUM(a.n) AS s, AVG(a.x) AS av, " +
    "MIN(a.ts) AS mi, MAX(a.dc) AS ma"           -> "b.label",
    "b.label, a.k, COUNT(*) AS c, SUM(a.n) AS s" -> "b.label, a.k"
  )

  private val b2 =
    "a.aid, b.label, b.addr.zip AS customer_zip, UPPER(b.name) AS uname"

  /** B9 -- every B1 type of `pt_a` (keyword, text, int, long, double, date, timestamp, boolean). */
  private val b9 = "a.aid, a.t, a.n, a.big_a, a.x, a.de, a.ts, a.b"

  /** A cell: the JOIN body, and the single-table statement whose answer is its oracle. */
  private case class Cell(body: String, oracle: String)

  private def joinCells(from: String, items: Seq[(String, String)]): Seq[Cell] =
    items.map { case (item, table) =>
      Cell(s"SELECT $item FROM $from", s"SELECT ${unqualified(item)} FROM $table")
    }

  /** Every G1 / G2 cell under every join form, the unaliased, self-join and three-leg forms, G3. */
  private lazy val parityCells: Seq[Cell] = {
    val kinds = joinKinds.flatMap(kind => joinCells(s"pt_a a $kind pt_b b $onBid", g1 ++ g2))
    val unaliased = (g1 ++ g2).map { case (item, table) =>
      val written = item.replaceAll("\\ba\\.", "pt_a.").replaceAll("\\bb\\.", "pt_b.")
      Cell(
        s"SELECT $written FROM pt_a JOIN pt_b ON pt_a.bid = pt_b.bid",
        s"SELECT ${unqualified(item)} FROM $table"
      )
    }
    val ptAItems = g1.filter(_._2 == "pt_a")
    val selfJoin = joinCells(
      "pt_a a JOIN pt_a a2 ON a.bid = a2.bid",
      ptAItems ++ ptAItems.map { case (item, t) => item.replaceAll("\\ba\\.", "a2.") -> t }
    )
    val ptBItems = g1.filter(_._2 == "pt_b")
    val threeLegs = joinCells(
      s"pt_a a JOIN pt_b b $onBid JOIN pt_b b2 ON a.bid = b2.bid",
      g1 ++ g2 ++ ptBItems.map { case (item, t) => item.replaceAll("\\bb\\.", "b2.") -> t }
    )
    val aggregates = g3.flatMap { case (items, keys) =>
      Seq("JOIN", "LEFT JOIN").map { kind =>
        Cell(
          s"SELECT $items FROM pt_a a $kind pt_b b $onBid GROUP BY $keys",
          s"SELECT ${unqualified(items)} FROM pt_ab GROUP BY ${unqualified(keys)}"
        )
      }
    }
    val consumer = Seq(
      Cell(s"SELECT $b2 FROM pt_a a JOIN pt_b b $onBid", s"SELECT ${unqualified(b2)} FROM pt_ab"),
      Cell(s"SELECT $b9 FROM pt_a a JOIN pt_b b $onBid", s"SELECT ${unqualified(b9)} FROM pt_a")
    )
    kinds ++ unaliased ++ selfJoin ++ threeLegs ++ aggregates ++ consumer
  }

  /** Both answers equal: the same schema, or the same status and message. */
  private def sameAnswer(
    perTable: ElasticResult[Option[ResultSchema]],
    oracle: ElasticResult[Option[ResultSchema]]
  ): Unit =
    (perTable, oracle) match {
      case (ElasticSuccess(p), ElasticSuccess(o)) => p shouldBe o
      case (ElasticFailure(p), ElasticFailure(o)) =>
        p.statusCode shouldBe o.statusCode
        p.message shouldBe o.message
      case other => fail(s"describePerTable and its oracle disagree: $other")
    }

  private def columnsOf(answer: ElasticResult[Option[ResultSchema]]): Seq[(String, String)] =
    answer match {
      case ElasticSuccess(Some(schema)) => schema.columns.map(c => c.name -> c.sqlType.typeId)
      case other                        => fail(s"no schema: $other")
    }

  "T0-2 (SCHEMA-15 premises)" should "attach both legs, and fall back to the main table for a leg the attach misses" in {
    val byName = Map("pt_a" -> ptA, "pt_b" -> ptB)
    // (i) both legs registered under their bare names; a joined column typed from its own leg
    val s = single(s"SELECT a.aid, b.big FROM pt_a a JOIN pt_b b $onBid")
    DerivedTable.attach(s, sources => byName.get(sources.mkString(","))) match {
      case Right(attached) =>
        attached.schemas.keySet shouldBe Set("pt_a", "pt_b")
        MultiSearch.fieldType(attached.select.fields(1)).sqlType shouldBe SQLTypes.BigInt
      case Left(refusal) => fail(s"(i) attach refused: $refusal")
    }
    // (ii) an index pattern as a JOIN leg is a leg, keyed by the pattern (MappingTypeReadingSpec
    // types both sides)
    val pattern = single("SELECT t.k, p.k FROM s7_t t JOIN s7_p* p ON t.k = p.k")
    pattern.from.aliasesToTable.toSeq shouldBe Seq("t" -> "s7_t", "p" -> "s7_p*")
    pattern.from.joinedTables shouldBe Seq("s7_p*")
    // (iii) a qualified star parses as a star: `HitReading.isStar` is D7's whole test
    def starForm(sql: String): Seq[(String, Option[String], Option[String], Boolean)] =
      single(sql).select.fields.map { f =>
        (
          f.identifier.identifierName,
          f.identifier.tableAlias,
          f.identifier.table,
          HitReading.isStar(f)
        )
      }
    starForm(s"SELECT a.* FROM pt_a a JOIN pt_b b $onBid") shouldBe
    Seq(("*", Some("a"), Some("pt_a"), true))
    starForm(s"SELECT a.aid, b.* FROM pt_a a JOIN pt_b b $onBid") shouldBe
    Seq(("aid", Some("a"), Some("pt_a"), false), ("*", Some("b"), Some("pt_b"), true))
    // (iv) G3 / B8 (and B2, B9) pass the resolved-type rules once attached
    val bodies = g3.map { case (items, keys) =>
      s"SELECT $items FROM pt_a a JOIN pt_b b $onBid GROUP BY $keys"
    } ++ Seq(
      s"SELECT $b2 FROM pt_a a JOIN pt_b b $onBid",
      s"SELECT $b9 FROM pt_a a JOIN pt_b b $onBid"
    )
    bodies.foreach { sql =>
      withClue(s"(iv) [$sql] ") {
        DerivedTable.attach(single(sql), sources => byName.get(sources.mkString(","))) match {
          case Right(attached) => attached.validateResolved() shouldBe Right(())
          case Left(refusal)   => fail(s"attach refused: $refusal")
        }
      }
    }
    // (v) the main table's schema answers for a leg the attach did not register (D5's reason)
    val missing = single(s"SELECT b.n FROM pt_a a JOIN pt_b b $onBid")
    DerivedTable.attach(missing, sources => Map("pt_a" -> ptA).get(sources.mkString(","))) match {
      case Right(attached) =>
        attached.schemas.keySet shouldBe Set("pt_a")
        MultiSearch.fieldType(attached.select.fields.head).sqlType shouldBe SQLTypes.Int
      case Left(refusal) => fail(s"(v) attach refused: $refusal")
    }
  }

  "describePerTable (SCHEMA-15)" should "type every JOIN body per table as describe types its single-table oracle, searching nothing (AC1)" in {
    val c = ptClient()
    val oracles = parityCells.map(cell => cell -> c.describe(cell.oracle))
    // a refused oracle is compared by status and message; a schema-less one would prove nothing
    oracles
      .collect { case (cell, o) if !o.toOption.exists(_.isDefined) => cell -> o }
      .distinct
      .foreach { case (cell, o) => println(s"[SCHEMA-15 oracle not typed] ${cell.oracle}: $o") }
    oracles.collect { case (cell, ElasticSuccess(None)) => cell.oracle } shouldBe empty
    val statements = oracles.map { case (cell, o) => (cell, parsed(cell.body), o) }
    statements.foreach { case (cell, statement, oracle) =>
      withClue(s"[${cell.body}] vs [${cell.oracle}] ") {
        sameAnswer(c.describePerTable(statement), oracle)
      }
    }
    c.searches.get() shouldBe 0
  }

  it should "attribute a bare name to the main table and keep repeated names, as the view does (A-110)" in {
    val oracles = ptClient()
    val mainN = columnsOf(oracles.describe("SELECT n FROM pt_a"))
    val joinedN = columnsOf(oracles.describe("SELECT n FROM pt_b"))
    mainN should not be joinedN
    // a fresh client per cell: a path no mapping holds reloads (and, in this test double, drops)
    // the cached mapping of its leg (D10), as single-table describe does
    Seq(
      "aid"      -> columnsOf(oracles.describe("SELECT aid FROM pt_a")),
      "k"        -> columnsOf(oracles.describe("SELECT k FROM pt_a")),
      "n"        -> mainN,
      "label"    -> Seq("label" -> "ANY"),
      "b.aid"    -> Seq("aid" -> "ANY"),
      "a.n, b.n" -> (mainN ++ joinedN),
      "a.nope"   -> Seq("nope" -> "ANY")
    ).foreach { case (items, expected) =>
      withClue(s"[$items] ") {
        val c = ptClient()
        columnsOf(
          c.describePerTable(parsed(s"SELECT $items FROM pt_a a JOIN pt_b b $onBid"))
        ) shouldBe
        expected
        c.searches.get() shouldBe 0
      }
    }
  }

  it should "answer None for an unknown leg, an unregistered key or a star, never the main table's column (AC3)" in {
    // an unknown leg, joined or main, and a same-named column of the main table
    Seq(
      s"SELECT a.aid, z.q FROM pt_a a JOIN pt_zz z ON a.bid = z.bid",
      s"SELECT z.n FROM pt_a a JOIN pt_zz z ON a.bid = z.bid",
      s"SELECT z.q, b.label FROM pt_zz z JOIN pt_b b ON z.bid = b.bid"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val c = ptClient()
        c.describePerTable(parsed(sql)) shouldBe ElasticSuccess(None)
        // remembered: answered the same way again
        c.describePerTable(parsed(sql)) shouldBe ElasticSuccess(None)
        c.searches.get() shouldBe 0
      }
    }
    // a failed read (not a 404)
    val failing = new CountingClient {
      override private[client] def loadQuerySchema(source: String): ElasticResult[Schema] =
        if (source == "pt_b")
          ElasticResult.failure(ElasticError(message = "boom", statusCode = Some(500)))
        else super.loadQuerySchema(source)
    }
    failing.updateSchema("pt_a", ptA)
    failing.describePerTable(parsed(s"SELECT a.n, b.m FROM pt_a a JOIN pt_b b $onBid")) shouldBe
    ElasticSuccess(None)
    // a key no leg registers: one index under two qualifiers (a fallback would type `p.n` BIGINT
    // from `pt_b`), an UNNEST alias -- no mapping read
    Seq(
      """SELECT p.n FROM pt_b x JOIN "c1".pt_a p ON x.bid = p.bid JOIN "c2".pt_a q ON x.bid = q.bid""",
      s"SELECT a.aid, i.p FROM pt_a a JOIN UNNEST(a.items) AS i JOIN pt_b b $onBid"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val c = ptClient()
        val statement = parsed(sql)
        c.reads.set(0)
        c.describePerTable(statement) shouldBe ElasticSuccess(None)
        c.reads.get() shouldBe 0
        c.searches.get() shouldBe 0
      }
    }
    // a star: the main table's columns only would be a short schema
    Seq(
      s"SELECT * FROM pt_a a JOIN pt_b b $onBid",
      s"SELECT a.* FROM pt_a a JOIN pt_b b $onBid",
      s"SELECT a.aid, b.* FROM pt_a a JOIN pt_b b $onBid"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        ptClient().describePerTable(parsed(sql)) shouldBe ElasticSuccess(None)
      }
    }
  }

  it should "answer describe's answer for every other shape, and refuse a type error as over the twin (AC2, AC3)" in {
    val shapes = Seq(
      s"SELECT a.aid, d.label FROM pt_a a JOIN (SELECT bid, label FROM pt_b) d ON a.bid = d.bid",
      "WITH d AS (SELECT bid, label FROM pt_b) SELECT a.aid, d.label FROM pt_a a JOIN d ON a.bid = d.bid",
      s"SELECT a.aid, b.label FROM pt_a a JOIN pt_b b $onBid WHERE a.k IN (SELECT k FROM pt_b)",
      s"SELECT a.aid FROM pt_a a JOIN pt_b b $onBid WHERE a.n IN (SELECT c.m FROM pt_ab c WHERE c.aid = a.aid)",
      "SELECT a.aid FROM pt_a a, pt_b b JOIN pt_ab c ON b.bid = c.bid",
      s"SELECT a.aid FROM pt_a a JOIN pt_b b $onBid UNION SELECT a.aid FROM pt_a a JOIN pt_b b $onBid"
    )
    val statements = shapes.map(sql => sql -> parsed(sql))
    statements.foreach { case (sql, statement) =>
      withClue(s"[$sql] ") {
        val c = ptClient()
        sameAnswer(c.describePerTable(statement), c.describe(statement))
        c.searches.get() shouldBe 0
      }
    }
    // P1: a non-JOIN statement is describe's, verbatim
    p1.foreach { sql =>
      withClue(s"[$sql] ") {
        val c = client()
        val statement = parsed(sql)
        c.describePerTable(statement) shouldBe c.describe(statement)
      }
    }
    // a type error: validateResolved's reason, as single-table describe gives it over the twin;
    // the reason quotes the expression as written, so the body's qualifiers are dropped to compare
    val twin = ptClient().describe("SELECT aid FROM pt_ab WHERE CAST(ts AS TIME) = d2")
    val typeError =
      parsed(s"SELECT a.aid FROM pt_a a JOIN pt_b b $onBid WHERE CAST(a.ts AS TIME) = b.d2")
    (ptClient().describePerTable(typeError), twin) match {
      case (ElasticFailure(p), ElasticFailure(o)) =>
        p.statusCode shouldBe Some(400)
        p.operation shouldBe Some("describe")
        unqualified(p.message) shouldBe o.message
      case other => fail(s"a type error: $other")
    }
  }

  it should "read dates as single-table describe does under 6.8.23 and 8.18.3 (AC4)" in {
    Seq("6.8.23", "8.18.3").foreach { dialect =>
      withClue(s"[$dialect] ") {
        val c = ptClient(dialect)
        val oracle = c.describe("SELECT dc, de, pz, ts, dn FROM pt_a")
        columnsOf(oracle) should not be empty
        sameAnswer(
          c.describePerTable(
            parsed(s"SELECT a.dc, a.de, a.pz, a.ts, a.dn FROM pt_a a JOIN pt_b b $onBid")
          ),
          oracle
        )
        sameAnswer(
          c.describePerTable(parsed(s"SELECT b.d2 FROM pt_a a JOIN pt_b b $onBid")),
          c.describe("SELECT d2 FROM pt_b")
        )
      }
    }
  }

  it should "ignore a foreign extension for a JOIN body only (D4)" in {
    val foreign = new ExtensionSpi {
      override def extensionId: String = "describe-per-table-foreign-test"
      override def extensionName: String = "Foreign handler (test double)"
      override def version: String = "test"
      override def priority: Int = 1
      override def initialize(
        config: com.typesafe.config.Config,
        licenseRefreshStrategy: app.softnetwork.elastic.licensing.LicenseRefreshStrategy
      ): Either[String, Unit] = Right(())
      override def canHandle(statement: Statement): Boolean = true
      override def execute(statement: Statement, client: ElasticClientApi)(implicit
        system: ActorSystem
      ): Future[ElasticResult[QueryResult]] = Future.successful(ElasticSuccess(QueryRows(Nil)))
      override def supportedSyntax: Seq[String] = Seq.empty
    }
    val withForeign = new CountingClient {
      override lazy val extensionRegistry: ExtensionRegistry =
        new ExtensionRegistry(ConfigFactory.load(), licenseRefreshStrategy) {
          override lazy val extensions: Seq[ExtensionSpi] = Seq(foreign)
        }
    }
    Seq("pt_a" -> ptA, "pt_b" -> ptB).foreach { case (i, s) => withForeign.updateSchema(i, s) }
    val join = parsed(s"SELECT $b9 FROM pt_a a JOIN pt_b b $onBid")
    withForeign.describe(join) shouldBe ElasticSuccess(None)
    val typed = withForeign.describePerTable(join)
    typed shouldBe ptClient().describePerTable(join)
    typed.toOption.flatten should not be empty
    // a single-table statement: describe's answer under that extension
    withForeign.describePerTable(parsed("SELECT aid FROM pt_a")) shouldBe ElasticSuccess(None)
    withForeign.searches.get() shouldBe 0
  }

  it should "answer each scope as a fresh parse: after a validation pass on the same AST (AC6)" in {
    val schemas = Map("pt_a" -> ptA, "pt_b" -> ptB)
    Seq(
      s"SELECT $b9, a.tags FROM pt_a a JOIN pt_b b $onBid",
      s"SELECT a.tags, COALESCE(a.dc, b.d2) AS v, CASE WHEN a.b THEN a.ts ELSE b.d2 END AS w " +
      s"FROM pt_a a JOIN pt_b b $onBid",
      s"SELECT ${g3.head._1} FROM pt_a a JOIN pt_b b $onBid GROUP BY ${g3.head._2}"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val c = ptClient()
        val fresh = c.describePerTable(single(sql))
        val shared = single(sql)
        val verdict =
          SQLTypeUtils.validatingComputedColumns(shared.updateAll(schemas).validateResolved())
        c.describePerTable(shared) shouldBe fresh
        // and the validation pass, after describePerTable, answers as on a fresh parse
        SQLTypeUtils.validatingComputedColumns(
          shared.updateAll(schemas).validateResolved()
        ) shouldBe verdict
        SQLTypeUtils.validatingComputedColumns(
          single(sql).updateAll(schemas).validateResolved()
        ) shouldBe verdict
      }
    }
  }

  it should "forward through a delegator: the delegate's answer, its member run once (D10)" in {
    val calls = new AtomicInteger(0)
    val recording = new CountingClient {
      override private[elastic] def describePerTable(
        statement: Statement
      ): ElasticResult[Option[ResultSchema]] = {
        calls.incrementAndGet()
        super.describePerTable(statement)
      }
    }
    Seq("pt_a" -> ptA, "pt_b" -> ptB).foreach { case (i, s) => recording.updateSchema(i, s) }
    val wrapper = new ElasticClientDelegator { override val delegate: ElasticClientApi = recording }
    val join = parsed(s"SELECT $b9 FROM pt_a a JOIN pt_b b $onBid")
    val answer = wrapper.describePerTable(join)
    calls.get() shouldBe 1
    answer shouldBe ptClient().describePerTable(join)
    answer.toOption.flatten should not be empty
    recording.searches.get() shouldBe 0
  }
}
