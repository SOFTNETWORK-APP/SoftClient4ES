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

import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{MultiSearch, SelectStatement, SingleSearch, Statement}
import app.softnetwork.elastic.sql.schema.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.Logger
import org.slf4j.helpers.NOPLogger

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

/** SCHEMA-17 fix R-169 -- `describe` looks a remembered missing source up again, so it answers what
  * `run` answers once the mapping is read: an index another client creates after a 404 was
  * remembered (an ingest that auto-creates it, a raw `PUT`) is typed by the next describe, whose
  * successful lookup forgets the miss, so the next `run` is typed too. `run` keeps its negative
  * cache: no lookup while the miss stands.
  *
  * Population (spec section 4): source {one index, an alias, a pattern, a list, a pattern with a
  * concrete part} x {created after the miss, still absent} x entry {describe(sql),
  * describe(SingleSearch) reused, describe(SelectStatement) reused} x memo {cold, warm}; the other
  * readers of the miss: `describePerTable`'s legs, `attachDerived`'s sources, a WHERE subquery's
  * scope check, the legs of a UNION ALL.
  *
  * Docker-free: Elasticsearch is canned (`GET <source>`, `_field_caps`, version 8.18.3), and an
  * index is created "by another client" by adding it to [[CannedClient.existing]]: no DDL through
  * the client, so no miss is forgotten by it. Every lookup the client makes is counted
  * ([[CannedClient.lookupsOf]]), a cache hit included.
  */
class DescribeRememberedMissSpec extends AnyFlatSpec with Matchers {

  private val LongTtlMs = 3600000L

  private val tFields: Seq[(String, String)] = Seq(
    "id" -> """{"type":"keyword"}""",
    "k"  -> """{"type":"keyword"}""",
    "n"  -> """{"type":"integer"}""",
    "ts" -> """{"type":"date"}"""
  )

  /** `t` re-created by another client with another mapping. */
  private val tFieldsB: Seq[(String, String)] = Seq(
    "id"    -> """{"type":"keyword"}""",
    "k"     -> """{"type":"keyword"}""",
    "n"     -> """{"type":"long"}""",
    "ts"    -> """{"type":"date"}""",
    "extra" -> """{"type":"double"}"""
  )

  private val uFields: Seq[(String, String)] = Seq(
    "id" -> """{"type":"keyword"}""",
    "m"  -> """{"type":"integer"}"""
  )

  private val everything: Set[String] = Set("t", "t2", "u")

  private def properties(fields: Seq[(String, String)]): String =
    fields.map { case (name, mapping) => s""""$name":$mapping""" }.mkString("{", ",", "}")

  private def doc(fields: Seq[(String, String)], aliases: String = "{}"): String =
    s"""{"aliases":$aliases,"mappings":{"properties":${properties(fields)}},""" +
    """"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}"""

  /** Canned Elasticsearch over the indices that [[existing]] holds now:
    *   - `t` (its mapping [[fieldsOfT]]), `t2` (the same columns), `u` (another table);
    *   - `al` an alias over `t`, present with it;
    *   - a pattern matches the existing indices it prefixes (`t*`: `t`, `t2`), a comma list answers
    *     its members, and a concrete part that does not exist is a 404, as Elasticsearch answers;
    *   - `zz_*` matches nothing.
    */
  final class CannedClient extends NopeClientApi {
    @volatile var existing: Set[String] = Set.empty
    @volatile var fieldsOfT: Seq[(String, String)] = tFields
    @volatile var missTtlMs: Long = LongTtlMs

    private val lookups = new ConcurrentHashMap[String, AtomicInteger]()

    /** The lookups of `source` -- `SearchApi.lookupSchema`'s key -- made so far, cache hits
      * included.
      */
    def lookupsOf(source: String): Int = Option(lookups.get(source)).map(_.get()).getOrElse(0)

    override protected def logger: Logger = NOPLogger.NOP_LOGGER

    override protected def schemaCacheTtlMs: Long = LongTtlMs

    override protected def schemaMissTtlMs: Long = missTtlMs

    override private[client] def executeVersion(): ElasticResult[String] =
      ElasticResult.success("8.18.3")

    override private[client] def loadQuerySchema(source: String): ElasticResult[Schema] = {
      lookups.computeIfAbsent(source, _ => new AtomicInteger()).incrementAndGet()
      super.loadQuerySchema(source)
    }

    private def fieldsOf(index: String): Seq[(String, String)] =
      index match {
        case "t" => fieldsOfT
        case "u" => uFields
        case _   => tFields
      }

    override private[client] def executeGetIndex(source: String): ElasticResult[Option[String]] = {
      val now = existing
      val (patterns, concrete) = source.split(",").map(_.trim).toSeq.partition(_.contains("*"))
      val named = concrete.flatMap {
        case "al" => if (now.contains("t")) Seq("t" -> """{"al":{}}""") else Nil
        case name => if (now.contains(name)) Seq(name -> "{}") else Nil
      }
      val matched = patterns.flatMap { p =>
        val prefix = p.takeWhile(_ != '*')
        now.toSeq.sorted.filter(_.startsWith(prefix)).map(_ -> "{}")
      }
      val members = (named ++ matched).distinct
      if (named.size < concrete.size || members.isEmpty) ElasticResult.success(None)
      else
        ElasticResult.success(
          Some(
            members
              .map { case (index, aliases) => s""""$index":${doc(fieldsOf(index), aliases)}""" }
              .mkString("{", ",", "}")
          )
        )
    }

    override private[client] def executeFieldCaps(source: String): ElasticResult[Option[String]] =
      ElasticResult.success(None)
  }

  private def client(
    existing: Set[String] = Set.empty,
    fieldsOfT: Seq[(String, String)] = tFields
  ): CannedClient = {
    val c = new CannedClient
    c.existing = existing
    c.fieldsOfT = fieldsOfT
    c
  }

  type Answer = ElasticResult[Option[ResultSchema]]

  private def parse(sql: String): Statement =
    Parser(sql) match {
      case Right(statement) => statement
      case Left(error)      => fail(s"[$sql] does not parse: ${error.msg}")
    }

  private def single(sql: String): SingleSearch =
    parse(sql) match {
      case s: SingleSearch => s
      case s: SelectStatement =>
        s.statement match {
          case Some(inner: SingleSearch) => inner
          case other                     => fail(s"[$sql] is not a single search: $other")
        }
      case other => fail(s"[$sql] is not a single search: $other")
    }

  /** How a consumer calls describe; `memoized`: the memo reaches it (A-190). */
  final case class Entry(
    name: String,
    memoized: Boolean,
    caller: (CannedClient, String) => () => Answer
  )

  private val bySql = Entry("describe(sql)", memoized = false, (c, sql) => () => c.describe(sql))

  private val reusedSingle =
    Entry(
      "describe(SingleSearch) reused",
      memoized = true,
      (c, sql) => {
        val statement = single(sql)
        () => c.describe(statement)
      }
    )

  private val reusedSelect =
    Entry(
      "describe(SelectStatement) reused",
      memoized = true,
      (c, sql) => {
        val statement = SelectStatement(sql)
        () => c.describe(statement)
      }
    )

  private val entries = Seq(bySql, reusedSingle, reusedSelect)

  /** (label, source): every source shape, keyed as the miss is (the sources joined by commas). */
  private val sources: Seq[(String, String)] = Seq(
    "one index"                      -> "t",
    "an alias"                       -> "al",
    "a pattern"                      -> "t*",
    "a list"                         -> "t,t2",
    "a pattern with a concrete part" -> "zz_*,t"
  )

  private def statements(source: String): Seq[String] = Seq(
    s"SELECT * FROM $source",
    s"SELECT id, k, n, ts FROM $source"
  )

  private def renderColumn(c: ResultColumn): String = {
    val fields = if (c.fields.isEmpty) "" else c.fields.map(renderColumn).mkString("{", ",", "}")
    val element = c.element.fold("")(e => "[" + renderColumn(e) + "]")
    s"${c.name}:${c.sqlType.typeId}${if (c.nullable) "" else " NOT NULL"}$fields$element"
  }

  /** An answer, compared across clients: the schema, or the refusal with its status. */
  private def render(answer: Answer): String =
    answer match {
      case ElasticSuccess(Some(schema)) =>
        schema.columns.map(renderColumn).mkString("SOME(", ",", ")")
      case ElasticSuccess(None) => "NONE"
      case ElasticFailure(error) =>
        s"FAILURE(${error.statusCode.getOrElse(-1)}|${error.operation.getOrElse("")}|" +
          s"${error.index.getOrElse("")}|${error.message})"
    }

  /** The answer a FRESH client -- no miss remembered -- gives `sql` over `existing`. */
  private def fresh(
    sql: String,
    existing: Set[String],
    fieldsOfT: Seq[(String, String)] = tFields
  ): String =
    render(client(existing, fieldsOfT).describe(sql))

  /** Both successes, and the very same answer instance: answered from memory. */
  private def sameAnswer(a: Answer, b: Answer): Boolean =
    (a, b) match {
      case (ElasticSuccess(x), ElasticSuccess(y)) =>
        x.asInstanceOf[AnyRef] eq y.asInstanceOf[AnyRef]
      case _ => false
    }

  /** Whether `run`'s resolution (`executeSubqueries = true`) attaches a schema with columns. */
  private def typedByRun(c: CannedClient, sql: String): Boolean =
    c.resolveWithSchema(single(sql)) match {
      case ElasticSuccess(sent) => sent.schema.exists(_.columns.nonEmpty)
      case other                => fail(s"run's resolution of [$sql]: $other")
    }

  // ---------------------------------------------------------------------------------------------
  // AC1 / AC3 -- created after the miss: describe types it, run is typed after describe
  // ---------------------------------------------------------------------------------------------

  "describe" should "type a source another client created after its 404 was remembered, and run after it (AC1, AC3)" in {
    for {
      entry           <- entries
      (label, source) <- sources
      sql             <- statements(source)
    } withClue(s"[${entry.name} | $label | $sql] ") {
      val c = client()
      val call = entry.caller(c, sql)
      // the 404 -- or, for a pattern of `*` parts, the empty table -- remembered by the lookup
      val before = call()
      render(before) shouldBe fresh(sql, Set.empty)
      c.schemaMissCount shouldBe 1
      c.existing = everything // created by another client: the miss stands
      // AC3 -- run keeps its negative cache: no lookup, untyped
      val looked = c.lookupsOf(source)
      typedByRun(c, sql) shouldBe false
      c.lookupsOf(source) shouldBe looked
      // AC1 -- describe looks it up again: typed, as a fresh client types it
      val after = call()
      render(after) should startWith("SOME(")
      render(after) should not be render(before)
      render(after) shouldBe fresh(sql, everything)
      c.lookupsOf(source) shouldBe looked + 1
      // the success forgot the miss: the next run is typed
      c.schemaMissCount shouldBe 0
      typedByRun(c, sql) shouldBe true
      // AC5 -- memo warm after the transition: the typed answer from memory
      if (entry.memoized) {
        sameAnswer(after, call()) shouldBe true
        c.describeAnswerCount shouldBe 1
      } else c.describeAnswerCount shouldBe 0
    }
  }

  it should "type the legs of a UNION ALL created after the first leg's 404 was remembered (AC1)" in {
    val sql = "SELECT id, n FROM t UNION ALL SELECT id, n FROM t2"
    val reusedStatement =
      Entry(
        "describe(Statement) reused",
        memoized = false,
        (c, s) => {
          val statement = parse(s)
          () => c.describe(statement)
        }
      )
    for (entry <- Seq(bySql, reusedStatement)) withClue(s"[${entry.name}] ") {
      val c = client()
      val call = entry.caller(c, sql)
      render(call()) should startWith("FAILURE(404|describe|")
      c.existing = everything
      val after = call()
      render(after) should startWith("SOME(")
      render(after) shouldBe fresh(sql, everything)
      c.schemaMissCount shouldBe 0
    }
  }

  // ---------------------------------------------------------------------------------------------
  // AC2 / AC6 -- still absent: the 404, looked up on every describe, the miss re-armed
  // ---------------------------------------------------------------------------------------------

  it should "answer a source still absent with its 404, looking it up on every describe (AC2, AC6)" in {
    for {
      entry  <- entries
      source <- Seq("t", "al", "t,t2", "zz_*,t")
    } withClue(s"[${entry.name} | $source] ") {
      val existing = Set("t2", "u") // `t` absent
      val c = client(existing)
      val sql = s"SELECT id FROM $source"
      val call = entry.caller(c, sql)
      (1 to 3).foreach { i =>
        val answer = call()
        render(answer) shouldBe fresh(sql, existing)
        render(answer) should startWith("FAILURE(404|describe|")
        render(answer) should endWith(s"|no such index [$source])")
        // one lookup per describe: no negative caching on describe's path
        c.lookupsOf(source) shouldBe i
      }
      // never memoized (A-192), the miss remembered for run
      c.describeAnswerCount shouldBe 0
      c.schemaMissCount shouldBe 1
      typedByRun(c, sql) shouldBe false
      c.lookupsOf(source) shouldBe 3
    }
  }

  it should "type a pattern of `*` parts matching nothing as an empty table on every describe (AC2, A-163 b)" in {
    for {
      entry <- entries
      sql   <- Seq("SELECT * FROM zz_*", "SELECT id FROM zz_*")
    } withClue(s"[${entry.name} | $sql] ") {
      val c = client(everything)
      val call = entry.caller(c, sql)
      (1 to 3).foreach { i =>
        val answer = call()
        render(answer) should startWith("SOME(")
        render(answer) shouldBe fresh(sql, everything)
        // one lookup per describe, the 404 behind the empty table remembered for run
        c.lookupsOf("zz_*") shouldBe i
        c.schemaMissCount shouldBe 1
      }
      if (sql.startsWith("SELECT *")) render(call()) shouldBe "SOME()"
    }
  }

  it should "remember the miss afresh on each describe of an absent source (AC2)" in {
    val sql = "SELECT id FROM t"
    val c = client()
    render(c.describe(sql)) should startWith("FAILURE(404|describe|")
    Thread.sleep(1500)
    render(c.describe(sql)) should startWith("FAILURE(404|describe|")
    // a miss 1 s old stands, one 1.5 s old would not: the second describe re-armed it
    c.missTtlMs = 1000L
    val looked = c.lookupsOf("t")
    typedByRun(c, sql) shouldBe false
    c.lookupsOf("t") shouldBe looked
    // the control: once that miss is older than the TTL too, run looks the source up again
    Thread.sleep(1100)
    typedByRun(c, sql) shouldBe false
    c.lookupsOf("t") shouldBe looked + 1
  }

  // ---------------------------------------------------------------------------------------------
  // AC5 -- the memo across the transition
  // ---------------------------------------------------------------------------------------------

  it should "answer a miss remembered over a cached schema from that schema, from memory when warm (AC5)" in {
    for (entry <- entries) withClue(s"[${entry.name}] ") {
      val sql = "SELECT * FROM t"
      val c = client(everything)
      val call = entry.caller(c, sql)
      val first = call()
      render(first) shouldBe fresh(sql, everything)
      // the testkit's seam: a miss remembered while the schema is cached
      c.rememberSchemaMiss("t")
      val second = call()
      render(second) shouldBe render(first)
      sameAnswer(first, second) shouldBe entry.memoized
      c.schemaMissCount shouldBe 0
      typedByRun(c, sql) shouldBe true
    }
  }

  it should "never store a 404, and store the typed answer under the new schema once the index appears (AC5)" in {
    for (entry <- entries.filter(_.memoized)) withClue(s"[${entry.name}] ") {
      val sql = "SELECT * FROM t"
      val c = client()
      val call = entry.caller(c, sql)
      render(call()) should startWith("FAILURE(404|")
      render(call()) should startWith("FAILURE(404|")
      c.describeAnswerCount shouldBe 0
      // created by another client
      c.existing = everything
      val typedA = call()
      render(typedA) shouldBe fresh(sql, everything)
      sameAnswer(typedA, call()) shouldBe true
      c.describeAnswerCount shouldBe 1
      // dropped through this client, then re-created by another one with another mapping
      c.existing = Set("t2", "u")
      c.invalidateSchema("t")
      render(call()) should startWith("FAILURE(404|")
      c.fieldsOfT = tFieldsB
      c.existing = everything
      val typedB = call()
      render(typedB) shouldBe fresh(sql, everything, tFieldsB)
      render(typedB) should include("extra:")
      render(typedB) should not be render(typedA)
      sameAnswer(typedA, typedB) shouldBe false
      sameAnswer(typedB, call()) shouldBe true
      c.describeAnswerCount shouldBe 1
    }
  }

  // ---------------------------------------------------------------------------------------------
  // AC4 -- the other readers of the miss, in describe's mode
  // ---------------------------------------------------------------------------------------------

  it should "type a JOIN body per table once its remembered missing leg exists, None while absent (describePerTable)" in {
    val sql = "SELECT a.id, a.n, b.m FROM t a JOIN u b ON a.id = b.id"
    val statement = parse(sql)
    val c = client(Set("t", "t2"))
    c.describePerTable(statement) shouldBe ElasticSuccess(None)
    c.lookupsOf("u") shouldBe 1
    c.schemaMissCount shouldBe 1
    // still absent: read again, still None
    c.describePerTable(statement) shouldBe ElasticSuccess(None)
    c.lookupsOf("u") shouldBe 2
    c.schemaMissCount shouldBe 1
    // created by another client
    c.existing = everything
    val typed = c.describePerTable(statement)
    render(typed) should startWith("SOME(")
    render(typed) shouldBe render(client(everything).describePerTable(parse(sql)))
    c.schemaMissCount shouldBe 0
  }

  it should "type a derived body over an index created after its 404 was remembered (attachDerived)" in {
    val sql = "SELECT q.c1 FROM (SELECT n AS c1 FROM t) q"
    val c = client()
    render(c.describe("SELECT n FROM t")) should startWith("FAILURE(404|")
    c.schemaMissCount shouldBe 1
    c.existing = everything
    c.attachDerived(single(sql)) match {
      case ElasticSuccess(attached) =>
        MultiSearch.projectionTypes(attached).map(_.map(_._2.sqlType)) shouldBe Some(
          Seq(SQLTypes.Int)
        )
      case other => fail(s"attachDerived: $other")
    }
    c.schemaMissCount shouldBe 0
  }

  it should "scope a WHERE subquery by an inner index created after its 404 was remembered, as run then does" in {
    val sql = "SELECT id FROM t WHERE id IN (SELECT id FROM u WHERE k = 'x')"
    val oracle = fresh(sql, everything)
    // the oracle: with both mappings read, `k` -- mapped by `t`, not by `u` -- is a correlation
    oracle should startWith("FAILURE(400|")
    oracle should include("'k' is not a column of 'u'")
    val c = client(everything)
    c.rememberSchemaMiss("u")
    val outer = single(sql)
    val inner = outer.whereSubqueries.head.inner.getOrElse(fail("no body"))
    // run's scope check keeps the miss: no lookup, no refusal
    c.scopeCorrelation(outer, inner) shouldBe None
    c.lookupsOf("u") shouldBe 0
    // describe's scope check reads `u` again: run's refusal
    render(c.describe(sql)) shouldBe oracle
    c.schemaMissCount shouldBe 0
    // the success forgot the miss: run's scope check refuses too
    c.scopeCorrelation(outer, inner).getOrElse(fail("expected run's refusal")) should include(
      "'k' is not a column of 'u'"
    )
  }
}
