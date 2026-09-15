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
import akka.stream.scaladsl.Source
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticResult, ElasticSuccess}
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import app.softnetwork.elastic.sql.query.SelectStatement
import app.softnetwork.persistence.generateUUID
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.language.implicitConversions

case class WsqId(id: String)
case class WsqCount(n: Long)

/** Story 22.2 — uncorrelated WHERE subqueries, executed ES-natively against a REAL cluster.
  *
  * Two-index, MULTI-SHARD fixture with CLOSED-FORM oracles: every expectation below is computed
  * from the fixture rule, never from a document total, so a wrong answer cannot coincide with the
  * right count (the story 21.3 lesson). The failure modes this guards are silent ones — an
  * unresolved predicate answering `match_all`, a truncated `terms` list, an ANSI NULL rule inverted
  * — so the assertions are exact row sets or exact sizes, never "non-empty".
  */
trait WhereSubqueryCompletenessSpec
    extends AnyFlatSpecLike
    with ElasticDockerTestKit
    with Matchers {

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  lazy val client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  private val customers = "wsq_customers"
  private val orders = "wsq_orders"
  private val narrow = "wsq_narrow"
  private val wide = "wsq_wide"

  /** 40 customers, 200 orders: customer `cNN` owns exactly 5 orders, so a set of `k` customers
    * selects exactly `5 * k` orders.
    */
  private val customerCount = 40
  private val orderCount = 200
  private val euCustomers = 15 // c01..c15
  private val vipCustomers = 5 // c01..c05

  private val threeShards = """{"number_of_shards": 3, "number_of_replicas": 0}"""

  private def customerId(n: Int): String = f"c${(n - 1) % customerCount + 1}%02d"

  override def beforeAll(): Unit = {
    super.beforeAll()

    client.createIndex(customers, settings = threeShards).get shouldBe true
    client
      .setMapping(
        customers,
        """{
          |  "properties": {
          |    "id":     { "type": "keyword" },
          |    "region": { "type": "keyword" },
          |    "vip":    { "type": "boolean" },
          |    "since":  { "type": "date" },
          |    "tier":   { "type": "text" }
          |  }
          |}""".stripMargin
      )
      .get shouldBe true

    client.createIndex(orders, settings = threeShards).get shouldBe true
    client
      .setMapping(
        orders,
        """{
          |  "properties": {
          |    "id":          { "type": "keyword" },
          |    "customer_id": { "type": "keyword" },
          |    "amount":      { "type": "integer" },
          |    "placed":      { "type": "date", "format": "yyyy-MM-dd" }
          |  }
          |}""".stripMargin
      )
      .get shouldBe true

    // `max_terms_count` tuned BELOW the default so a 150-value terms query is refused (AD-9).
    client
      .createIndex(
        narrow,
        settings = """{"number_of_shards": 1, "number_of_replicas": 0, "max_terms_count": 100}"""
      )
      .get shouldBe true
    client
      .setMapping(
        narrow,
        """{"properties": {"id": {"type": "keyword"}, "k": {"type": "integer"}}}"""
      )
      .get shouldBe true
    client.createIndex(wide, settings = threeShards).get shouldBe true
    client
      .setMapping(wide, """{"properties": {"id": {"type": "keyword"}, "k": {"type": "integer"}}}""")
      .get shouldBe true

    // c01..c15 = EU, c16..c39 = US, c40 has NO region at all (the ANSI NULL of the NOT IN rule).
    val customerDocs = (1 to customerCount).map { c =>
      val id = f"c$c%02d"
      val region =
        if (c <= euCustomers) """"region":"EU","""
        else if (c < customerCount) """"region":"US","""
        else ""
      val since = if (c <= euCustomers) "2024-01-01" else "2024-06-01"
      val tier = if (c <= vipCustomers) "gold" else "silver"
      s"""{"id":"$id",$region"vip":${c <= vipCustomers},"since":"$since","tier":"$tier"}"""
    }.toList

    val orderDocs = (1 to orderCount).map { n =>
      val cid = customerId(n)
      val placed = if (cid.drop(1).toInt <= euCustomers) "2024-01-01" else "2024-06-01"
      f"""{"id":"o$n%03d","customer_id":"$cid","amount":$n,"placed":"$placed"}"""
    }.toList

    val narrowDocs = (1 to 10).map(k => s"""{"id":"n$k","k":$k}""").toList
    val wideDocs = (1 to 150).map(k => s"""{"id":"w$k","k":$k}""").toList

    index(customers, customerDocs)
    index(orders, orderDocs)
    index(narrow, narrowDocs)
    index(wide, wideDocs)
  }

  private def index(name: String, docs: List[String]): Unit = {
    implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = name, logEvery = 1000)
    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    client.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_) =>
      case ElasticFailure(error) =>
        error.cause.foreach(_.printStackTrace())
        fail(s"Bulk indexing $name failed: ${error.message}")
    }
    client.refresh(name)
    ()
  }

  override def afterAll(): Unit = {
    Seq(customers, orders, narrow, wide).foreach(client.deleteIndex)
    super.afterAll()
  }

  /** 🔴 `searchAs` is a MACRO that validates the statement at COMPILE time, so every query below is
    * an inline string LITERAL — a `val` holding the same text is rejected by the macro. That is
    * also why the correlated-subquery rejection (a parse-time `Left`) lives in the REPL integration
    * spec, which dispatches at run time, and not here.
    */
  private def idsOf(result: ElasticResult[Seq[WsqId]]): Seq[String] = result match {
    case ElasticSuccess(rows)  => rows.map(_.id)
    case ElasticFailure(error) => fail(s"failed: ${error.message}")
  }

  // ── IN / NOT IN ──────────────────────────────────────────────────────────────────────────────

  "IN (SELECT ...)" should "return exactly the orders whose customer is in the inner set" in {
    val ids = idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE customer_id IN (SELECT id FROM wsq_customers WHERE region = 'EU') ORDER BY id"
      )
    )
    // the exact SET, not merely its size: a wrong answer of the right cardinality fails here
    ids shouldBe (1 to orderCount)
      .filter(n => customerId(n).drop(1).toInt <= euCustomers)
      .map(n => f"o$n%03d")
    ids should have size (5L * euCustomers) // 75
  }

  it should "read the column the converter actually produces for a QUALIFIED or ALIASED projection" in {
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE customer_id IN (SELECT c.id FROM wsq_customers c WHERE c.region = 'EU')"
      )
    ) should have size 75L
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE customer_id IN (SELECT id AS cid FROM wsq_customers WHERE region = 'EU')"
      )
    ) should have size 75L
  }

  it should "normalise a DATE inner projection against the OUTER column's own format" in {
    // `since` is a default-format `date` on wsq_customers while `placed` is `yyyy-MM-dd` on
    // wsq_orders: the inner terms key arrives as a java.time value, is rendered as an ISO-8601
    // instant, and TemporalLiterals then normalises it against `placed`'s own format.
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE placed IN (SELECT since FROM wsq_customers WHERE region = 'EU')"
      )
    ) should have size 75L
  }

  it should "run a `text` inner column in mode W (a terms aggregation on text is an ES 400)" in {
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_customers WHERE tier IN (SELECT tier FROM wsq_customers WHERE vip = true)"
      )
    ) should have size vipCustomers.toLong
  }

  "NOT IN" should "be the exact complement over a NULL-free inner set" in {
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE customer_id NOT IN (SELECT id FROM wsq_customers WHERE region = 'EU')"
      )
    ) should have size (orderCount - 5L * euCustomers) // 125
  }

  /** 🔴 ANSI (PD-5), and the two rows are DIFFERENT rules. `c40` carries no `region`, so the inner
    * set contains a NULL: `IN` ignores it (and matches nothing, since no `customer_id` is a region
    * name) while `NOT IN` is UNKNOWN for EVERY row and matches NOTHING — not "everything except".
    */
  it should "return NO row when the inner set contains a NULL" in {
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE customer_id NOT IN (SELECT region FROM wsq_customers)"
      )
    ) shouldBe empty
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE customer_id IN (SELECT region FROM wsq_customers)"
      )
    ) shouldBe empty
  }

  "The quantified spellings" should "agree with IN / NOT IN" in {
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE customer_id = ANY (SELECT id FROM wsq_customers WHERE region = 'EU')"
      )
    ) should have size 75L
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE customer_id <> ALL (SELECT id FROM wsq_customers WHERE region = 'EU')"
      )
    ) should have size 125L
  }

  /** 🔴 Lead ruling OQ-4 — the ordering quantifiers EXECUTE, reduced from the resolved value list.
    * `amount` runs 1..200 and the inner set is `{1..5}`, so `> ANY` is `> 1` (199 rows) while `>
    * ALL` is `> 5` (195 rows): a test that confused the two ends would be off by four.
    */
  it should "reduce an ordering quantifier to the right end of the inner set" in {
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE amount > ANY (SELECT amount FROM wsq_orders WHERE amount <= 5)"
      )
    ) should have size 199L
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE amount > ALL (SELECT amount FROM wsq_orders WHERE amount <= 5)"
      )
    ) should have size 195L
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE amount <= ALL (SELECT amount FROM wsq_orders WHERE amount <= 5)"
      )
    ) should have size 1L // <= 1
  }

  /** 🔴 The two empty-set rules are OPPOSITE: an existential over nothing is FALSE, a universal
    * over nothing is TRUE. The single most likely thing to ship backwards.
    */
  it should "answer no rows for ANY and every row for ALL over an EMPTY subquery" in {
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE amount > ANY (SELECT amount FROM wsq_orders WHERE amount > 100000)"
      )
    ) shouldBe empty
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE amount > ALL (SELECT amount FROM wsq_orders WHERE amount > 100000)"
      )
    ) should have size orderCount.toLong
  }

  // ── EXISTS ───────────────────────────────────────────────────────────────────────────────────

  "EXISTS" should "be true or false on a row-shaped body and true on an aggregate body" in {
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE EXISTS (SELECT 1 FROM wsq_customers WHERE vip = true)"
      )
    ) should have size orderCount.toLong
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE EXISTS (SELECT 1 FROM wsq_customers WHERE region = 'MARS')"
      )
    ) shouldBe empty
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE NOT EXISTS (SELECT 1 FROM wsq_customers WHERE region = 'MARS')"
      )
    ) should have size orderCount.toLong
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE EXISTS (SELECT id FROM wsq_customers LIMIT 0)"
      )
    ) shouldBe empty
    // a metric-only SELECT always yields exactly one row, so EXISTS is true even over no documents
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE EXISTS (SELECT COUNT(*) AS n FROM wsq_customers WHERE region = 'MARS')"
      )
    ) should have size orderCount.toLong
  }

  // ── scalar ───────────────────────────────────────────────────────────────────────────────────

  "A scalar subquery" should "compare against the aggregate and against a LIMIT 1 row" in {
    // amounts are 1..200, so AVG = 100.5 and exactly 100 orders are above it.
    // 🔴 The UN-ALIASED shape is the headline one: under NativeContext its requested output name
    // would be NULL-FILLED and the predicate would silently collapse to match_none.
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE amount > (SELECT AVG(amount) FROM wsq_orders)"
      )
    ) should have size 100L
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE amount > (SELECT AVG(amount) AS a FROM wsq_orders)"
      )
    ) should have size 100L
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE amount = (SELECT MAX(amount) AS m FROM wsq_orders)"
      )
    ) shouldBe Seq("o200")
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE amount >= (SELECT amount FROM wsq_orders ORDER BY amount DESC LIMIT 1)"
      )
    ) shouldBe Seq("o200")
    // 🔴 ANSI: an aggregate over ZERO matching documents is NULL, so the comparison is UNKNOWN and
    // NO row matches. This is the statement the empty-aggregate defect broke: before the fix it
    // reduced to `amount > 0` and returned EVERY one of the 200 orders on ES 8.18 (which reported
    // `0.0`), while ES 7.17 answered a 400. An exact-count assertion, on a 3-shard index, is the
    // guard — an execution-success assertion would have passed throughout.
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE amount > (SELECT MAX(amount) AS m FROM wsq_orders WHERE amount > 100000)"
      )
    ) shouldBe empty
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE amount < (SELECT MIN(amount) AS m FROM wsq_orders WHERE amount > 100000)"
      )
    ) shouldBe empty
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE amount > (SELECT AVG(amount) AS a FROM wsq_orders WHERE amount > 100000)"
      )
    ) shouldBe empty
  }

  /** 🔴 The empty-aggregate rule itself, asserted on the VALUES rather than through a subquery, so
    * a regression is attributed to the conversion layer and not to the subquery resolver.
    *
    * MEASURED before the fix, same request on every major: Elasticsearch answers identically on the
    * wire (`{"value":null}` for max/min/avg, `0.0` for sum, `0` for value_count), but ES 8.18 alone
    * converted the nulls to `0.0` — its module reads the TYPED response, where a primitive `double`
    * has already swallowed the null before any of our code runs.
    */
  it should "answer NULL for MAX / MIN / AVG over zero documents, 0 for COUNT, and keep SUM at 0" in {
    def cell(sql: String, column: String): Option[Any] =
      client.search(
        app.softnetwork.elastic.sql.parser
          .Parser(sql)
          .toOption
          .collect { case s: app.softnetwork.elastic.sql.query.SearchStatement => s }
          .getOrElse(fail(s"[$sql] did not parse"))
      )(NativeContext) match {
        case ElasticSuccess(r) =>
          r.results.headOption.getOrElse(fail(s"[$sql] returned no row")).get(column)
        case ElasticFailure(e) => fail(s"[$sql] ${e.message}")
      }
    val empty = "FROM wsq_orders WHERE amount > 100000"
    // ANSI NULL — the aggregate had no input
    cell(s"SELECT MAX(amount) AS m $empty", "m") shouldBe Some(null)
    cell(s"SELECT MIN(amount) AS m $empty", "m") shouldBe Some(null)
    cell(s"SELECT AVG(amount) AS m $empty", "m") shouldBe Some(null)
    // 🔴 COUNT is 0, NEVER null — ANSI's own exception, and the single most damaging thing to get
    // backwards here.
    cell(s"SELECT COUNT(*) AS c $empty", "c").map(_.toString.toDouble) shouldBe Some(0.0d)
    cell(s"SELECT COUNT(amount) AS c $empty", "c").map(_.toString.toDouble) shouldBe Some(0.0d)
    // SUM keeps Elasticsearch's own answer on every major (recorded decision, NOT ANSI's NULL)
    cell(s"SELECT SUM(amount) AS s $empty", "s").map(_.toString.toDouble) shouldBe Some(0.0d)
    // and a NON-empty aggregate is untouched: amounts are 1..200
    cell("SELECT MAX(amount) AS m FROM wsq_orders", "m").map(_.toString.toDouble) shouldBe
    Some(200.0d)
    cell("SELECT COUNT(*) AS c FROM wsq_orders", "c").map(_.toString.toDouble) shouldBe Some(200.0d)
  }

  // ── recursion, correlation, bounds ───────────────────────────────────────────────────────────

  "Nested subqueries" should "resolve recursively" in {
    // orders o001..o005 belong to c01..c05, which own 25 orders in total
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE customer_id IN (SELECT id FROM wsq_customers WHERE id IN (SELECT customer_id FROM wsq_orders WHERE amount <= 5))"
      )
    ) should have size 25L
  }

  "A bare outer column inside a subquery" should "be refused by the MAPPINGS at the seam (PD-2)" in {
    // `vip` is a wsq_customers column, NOT a wsq_orders one, so the inner statement is reading the
    // OUTER row. Executed as uncorrelated it would send an unknown field to Elasticsearch and
    // answer zero rows with HTTP 200 — the silent mode epic 22 exists to close.
    client.searchAs[WsqId](
      "SELECT id FROM wsq_customers WHERE id IN (SELECT customer_id FROM wsq_orders WHERE vip = true)"
    ) match {
      case ElasticFailure(error) => error.message should include("Correlated subquery")
      case ElasticSuccess(rows)  => fail(s"a bare outer column was read as an inner one: $rows")
    }
  }

  /** The false-positive guard for the rule above: `cid` is an alias the INNER SELECT defines and no
    * mapping carries, while the outer index HAS a `customer_id`. Without the alias filter in
    * `bareNameCorrelation` a self-contained subquery would be rejected.
    */
  it should "NOT refuse an inner SELECT alias that no mapping carries" in {
    idsOf(
      client.searchAs[WsqId](
        "SELECT id FROM wsq_orders WHERE customer_id IN (SELECT id AS cid FROM wsq_customers WHERE region = 'EU' ORDER BY cid)"
      )
    ) should have size 75L
  }

  /** 🔴 ES 6.8 does NOT enforce a per-index `max_terms_count` set at create time — MEASURED: the
    * 150-value terms query is accepted there, on both the REST and the Jest client, while 7.17 /
    * 8.18 / 9.0 refuse it. The bound itself is enforced on EVERY major by the resolver's own count
    * check (`SubqueryResolver.MaxTerms`, pinned Docker-free); what is version-dependent is only
    * whether ELASTICSEARCH ALSO refuses a smaller, index-tuned limit — which is what the error
    * translation exists for. Gated on the measured version rather than skipped, so the translation
    * stays asserted wherever it can fire.
    */
  private lazy val enforcesMaxTermsCount: Boolean =
    client.version match {
      case ElasticSuccess(v) => !v.trim.startsWith("6.")
      case ElasticFailure(_) => true
    }

  "The terms bound" should "surface the index's own max_terms_count loudly, with the remedy" in {
    assume(enforcesMaxTermsCount)
    client.searchAs[WsqCount](
      "SELECT COUNT(*) AS n FROM wsq_narrow WHERE k IN (SELECT k FROM wsq_wide)"
    ) match {
      case ElasticFailure(error) =>
        error.message should include("max_terms_count")
        error.message should include("JOIN")
      case ElasticSuccess(_) =>
        fail("a 150-value terms query was accepted by an index capped at 100")
    }
  }
}
