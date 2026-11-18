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
import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{SelectStatement, SingleSearch, Statement}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.Logger
import org.slf4j.helpers.NOPLogger

import java.util.concurrent.{Callable, Executors, TimeUnit}
import scala.collection.mutable

/** SCHEMA-17 fix R-159 (A-189..A-192) -- a cached `describe` of the SAME statement over the SAME
  * cached schema answers from memory, and every answer equals the one a fresh client gives.
  *
  * Population P1 (spec 4.1): entry x source x cache event on a plain statement; each non-plain
  * statement shape x reused entry; parallel describes; an invalidation racing describes.
  *
  * A HIT is observed as the very answer instance the first call built (`sameAnswer`); a MISS as a
  * new one. The memo reaches a reused SingleSearch or an unscored SelectStatement over a typed
  * source, on a cache hit, for a plain statement -- every other cell recomputes. Docker-free:
  * Elasticsearch is canned (`GET <source>`, `_field_caps`, version 8.18.3).
  */
class DescribeMemoSpec extends AnyFlatSpec with Matchers {

  private val LongTtlMs = 3600000L

  /** Every field kind the answer types: dates of each format route (default, custom, epoch, zone
    * name read as stored, `date_nanos`), a STRUCT, a nested list of objects.
    */
  private val baseFields: Seq[(String, String)] = Seq(
    "id"    -> """{"type":"keyword"}""",
    "k"     -> """{"type":"keyword"}""",
    "n"     -> """{"type":"integer"}""",
    "x"     -> """{"type":"double"}""",
    "ts"    -> """{"type":"date"}""",
    "ts2"   -> """{"type":"date"}""",
    "dc"    -> """{"type":"date","format":"yyyy/MM/dd"}""",
    "de"    -> """{"type":"date","format":"epoch_millis"}""",
    "dz"    -> """{"type":"date","format":"yyyy-MM-dd HH:mm z"}""",
    "dn"    -> """{"type":"date_nanos"}""",
    "obj"   -> """{"properties":{"a":{"type":"integer"},"b":{"type":"keyword"}}}""",
    "items" -> """{"type":"nested","properties":{"p":{"type":"integer"}}}"""
  )

  /** A field a DDL elsewhere adds. */
  private val extraField: (String, String) = "extra" -> """{"type":"long"}"""

  private def properties(fields: Seq[(String, String)]): String =
    fields.map { case (name, mapping) => s""""$name":$mapping""" }.mkString("{", ",", "}")

  private def doc(fields: Seq[(String, String)], aliases: String = "{}"): String =
    s"""{"aliases":$aliases,"mappings":{"properties":${properties(fields)}},""" +
    """"settings":{"index":{"number_of_shards":"1","number_of_replicas":"0"}}}"""

  /** `GET <index>/_field_caps?fields=*` (ES 8 shape) over the index `ro`: the same leaves. */
  private def fieldCaps(fields: Seq[(String, String)]): String = {
    def entry(path: String, es: String, metadata: Boolean = false): String =
      s""""$path":{"$es":{"type":"$es","metadata_field":$metadata,"searchable":true,""" +
      s""""aggregatable":${es != "text" && es != "object" && es != "nested"}}}"""
    val typeOf = """"type":"([a-z_]+)"""".r
    val leaves = fields.flatMap {
      case ("obj", _) =>
        Seq(entry("obj", "object"), entry("obj.a", "integer"), entry("obj.b", "keyword"))
      case ("items", _) => Seq(entry("items", "nested"), entry("items.p", "integer"))
      case (name, mapping) =>
        Seq(entry(name, typeOf.findFirstMatchIn(mapping).map(_.group(1)).getOrElse("keyword")))
    }
    val metaFields = Seq("_id", "_index", "_source").map(m => entry(m, m, metadata = true))
    s"""{"indices":["ro"],"fields":${(leaves ++ metaFields).mkString("{", ",", "}")}}"""
  }

  /** Canned Elasticsearch over [[fields]], which a test changes as a DDL elsewhere would:
    *   - `t`, `m1`, `m2` one index each; `al` an alias over `m1` and `m2`, which agree; `m1,m2` the
    *     same indices as a comma list; `m*` as a pattern;
    *   - `ro` refuses its mapping (403), typed from `_field_caps`;
    *   - anything else is unknown (`nope` a 404, `zz_none_*` a pattern matching no index).
    */
  final class CannedClient(ttlMs: Long) extends NopeClientApi {
    @volatile var fields: Seq[(String, String)] = baseFields

    override protected def logger: Logger = NOPLogger.NOP_LOGGER

    override protected def schemaCacheTtlMs: Long = ttlMs

    override private[client] def executeVersion(): ElasticResult[String] =
      ElasticResult.success("8.18.3")

    override private[client] def executeGetIndex(index: String): ElasticResult[Option[String]] = {
      val now = fields
      val member = doc(now, """{"al":{}}""")
      index match {
        case "ro" =>
          ElasticFailure(
            ElasticError(
              message = s"action [indices:admin/get] is unauthorized for user [ro] on [$index]",
              statusCode = Some(403),
              index = Some(index)
            )
          )
        case "t" | "m1" | "m2" => ElasticResult.success(Some(s"""{"$index":${doc(now)}}"""))
        case "al" | "m1,m2" | "m*" =>
          ElasticResult.success(Some(s"""{"m1":$member,"m2":$member}"""))
        case _ => ElasticResult.success(None)
      }
    }

    override private[client] def executeFieldCaps(source: String): ElasticResult[Option[String]] =
      ElasticResult.success(if (source == "ro") Some(fieldCaps(fields)) else None)
  }

  private def client(ttlMs: Long = LongTtlMs): CannedClient = new CannedClient(ttlMs)

  type Answer = ElasticResult[Option[ResultSchema]]

  private def parse(sql: String): Statement =
    Parser(sql) match {
      case Right(statement) => statement
      case Left(error)      => fail(s"[$sql] does not parse: ${error.msg}")
    }

  /** The SingleSearch `sql` parses to (a plain SELECT). */
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

  /** How a consumer calls describe: the statement it holds, built ONCE and re-used, or the SQL text
    * (parsed again on each call). `memoized`: the memo reaches it (A-190).
    */
  final case class Entry(
    name: String,
    memoized: Boolean,
    caller: (CannedClient, String) => () => Answer
  )

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
      "describe(SelectStatement) reused, no score",
      memoized = true,
      (c, sql) => {
        val statement = SelectStatement(sql)
        () => c.describe(statement)
      }
    )

  private val reusedScored =
    Entry(
      "describe(SelectStatement) reused, a score",
      memoized = false,
      (c, sql) => {
        val statement = SelectStatement(sql, score = Some(1.0))
        () => c.describe(statement)
      }
    )

  private val bySql =
    Entry("describe(sql)", memoized = false, (c, sql) => () => c.describe(sql))

  private val entries = Seq(reusedSingle, reusedSelect, reusedScored, bySql)

  /** The sources a schema types (a cache entry): the memo's reach. */
  private val typedSources: Seq[(String, String)] = Seq(
    "one index"      -> "t",
    "alias over 2"   -> "al",
    "comma list"     -> "m1,m2",
    "pattern"        -> "m*",
    "read-only user" -> "ro"
  )

  /** A star and a projection of every field kind, per source. */
  private def plainStatements(source: String): Seq[String] = Seq(
    s"SELECT * FROM $source",
    s"SELECT id, n, x, ts, dc, de, dz, dn, obj FROM $source"
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

  /** The answer a FRESH client gives `sql` over `fields`. */
  private def fresh(sql: String, fields: Seq[(String, String)] = baseFields): String = {
    val c = client()
    c.fields = fields
    render(c.describe(sql))
  }

  /** Both successes, and the very same answer instance: the second call was answered from memory.
    */
  private def sameAnswer(a: Answer, b: Answer): Boolean =
    (a, b) match {
      case (ElasticSuccess(x), ElasticSuccess(y)) =>
        x.asInstanceOf[AnyRef] eq y.asInstanceOf[AnyRef]
      case _ => false
    }

  // ---------------------------------------------------------------------------------------------
  // entry x source x event, on a plain statement
  // ---------------------------------------------------------------------------------------------

  "describe" should "answer a re-used statement over a cached schema from memory, as a fresh client answers it" in {
    for {
      entry           <- entries.filter(_.memoized)
      (label, source) <- typedSources
      sql             <- plainStatements(source)
    } withClue(s"[${entry.name} | $label | $sql] ") {
      val c = client()
      val call = entry.caller(c, sql)
      val first = call()
      val second = call()
      val third = call()
      render(first) should startWith("SOME(")
      render(first) shouldBe fresh(sql)
      render(second) shouldBe render(first)
      sameAnswer(first, second) shouldBe true
      sameAnswer(second, third) shouldBe true
      c.describeAnswerCount shouldBe 1
    }
  }

  it should "recompute a scored SelectStatement and the SQL text, answering as a fresh client does" in {
    for {
      entry           <- entries.filterNot(_.memoized)
      (label, source) <- typedSources
      sql             <- plainStatements(source)
    } withClue(s"[${entry.name} | $label | $sql] ") {
      val c = client()
      val call = entry.caller(c, sql)
      val first = call()
      val second = call()
      render(first) shouldBe fresh(sql)
      render(second) shouldBe render(first)
      sameAnswer(first, second) shouldBe false
      c.describeAnswerCount shouldBe 0
    }
  }

  it should "recompute a source no cached schema types: a pattern matching no index, an unknown index (404)" in {
    for {
      entry <- entries
      sql   <- Seq("SELECT * FROM zz_none_*", "SELECT id FROM zz_none_*", "SELECT id FROM nope")
    } withClue(s"[${entry.name} | $sql] ") {
      val c = client()
      val call = entry.caller(c, sql)
      val first = call()
      val second = call()
      render(first) shouldBe fresh(sql)
      render(second) shouldBe render(first)
      sameAnswer(first, second) shouldBe false
      c.describeAnswerCount shouldBe 0
    }
    render(client().describe("SELECT id FROM nope")) should startWith("FAILURE(404|describe|")
  }

  it should "recompute under a TTL of 0, every lookup reading a new schema" in {
    for {
      entry           <- entries.filter(_.memoized)
      (label, source) <- typedSources
      sql             <- plainStatements(source)
    } withClue(s"[${entry.name} | $label | $sql] ") {
      val c = client(ttlMs = 0L)
      val call = entry.caller(c, sql)
      val first = call()
      val second = call()
      render(first) shouldBe fresh(sql)
      render(second) shouldBe render(first)
      sameAnswer(first, second) shouldBe false
      // each lookup reads a new schema: an answer stored under it could never hit
      c.describeAnswerCount shouldBe 0
    }
  }

  it should "answer the new schema after invalidateSchema, then from memory again" in {
    val withExtra = baseFields :+ extraField
    for {
      entry           <- entries.filter(_.memoized)
      (label, source) <- typedSources
    } withClue(s"[${entry.name} | $label] ") {
      val sql = s"SELECT * FROM $source"
      val c = client()
      val call = entry.caller(c, sql)
      val before = call()
      sameAnswer(before, call()) shouldBe true
      c.fields = withExtra
      c.invalidateSchema(source)
      val after = call()
      render(after) shouldBe fresh(sql, withExtra)
      render(after) should not be render(before)
      render(after) should include("extra:")
      sameAnswer(before, after) shouldBe false
      sameAnswer(after, call()) shouldBe true
      // the answer over the new schema replaced the statement's entry
      c.describeAnswerCount shouldBe 1
    }
  }

  it should "answer the new schema after a DDL replaced the cache entry (updateSchema)" in {
    val withExtra = baseFields :+ extraField
    for (entry <- entries.filter(_.memoized)) withClue(s"[${entry.name}] ") {
      val sql = "SELECT * FROM t"
      val c = client()
      val call = entry.caller(c, sql)
      val before = call()
      sameAnswer(before, call()) shouldBe true
      c.fields = withExtra
      c.updateSchema("t", Index("t", s"""{"t":${doc(withExtra)}}""").schema)
      val after = call()
      render(after) shouldBe fresh(sql, withExtra)
      render(after) should include("extra:")
      sameAnswer(after, call()) shouldBe true
      c.describeAnswerCount shouldBe 1
    }
  }

  it should "answer the new schema after a D10 reload of an explicit path the cached mapping lacked" in {
    val withExtra = baseFields :+ extraField
    for (entry <- entries.filter(_.memoized)) withClue(s"[${entry.name}] ") {
      val c = client()
      val star = entry.caller(c, "SELECT * FROM t")
      val before = star()
      sameAnswer(before, star()) shouldBe true
      // a field mapped since: the statement selecting it reloads the mapping (D10)
      c.fields = withExtra
      val selecting = entry.caller(c, "SELECT id, extra FROM t")
      val reloaded = selecting()
      render(reloaded) shouldBe fresh("SELECT id, extra FROM t", withExtra)
      sameAnswer(reloaded, selecting()) shouldBe true
      // the star, stored over the schema the reload replaced, is a miss with the new answer
      val after = star()
      render(after) shouldBe fresh("SELECT * FROM t", withExtra)
      render(after) should include("extra:")
      sameAnswer(before, after) shouldBe false
      sameAnswer(after, star()) shouldBe true
      // two statements, one entry each
      c.describeAnswerCount shouldBe 2
    }
  }

  it should "answer an explicit path no reload maps as a fresh client does, on every call" in {
    for (entry <- entries) withClue(s"[${entry.name}] ") {
      val sql = "SELECT id, ghost FROM t"
      val call = entry.caller(client(), sql)
      val first = call()
      val second = call()
      render(first) shouldBe fresh(sql)
      render(second) shouldBe render(first)
    }
  }

  // ---------------------------------------------------------------------------------------------
  // non-plain statement shapes: never answered from memory
  // ---------------------------------------------------------------------------------------------

  it should "recompute a statement with a WHERE subquery, a UNION ALL and a resolved-arm refusal (400)" in {
    val shapes = Seq(
      "a WHERE subquery" -> "SELECT id FROM t WHERE n IN (SELECT n FROM m1)",
      "a UNION ALL"      -> "SELECT id, n FROM t UNION ALL SELECT id, n FROM al",
      "a type error"     -> "SELECT id FROM t WHERE CAST(ts AS TIME) = ts2"
    )
    val reusedStatement =
      Entry(
        "describe(Statement) reused",
        memoized = false,
        (c, sql) => {
          val statement = parse(sql)
          () => c.describe(statement)
        }
      )
    for {
      entry        <- Seq(reusedStatement, reusedSelect)
      (label, sql) <- shapes
    } withClue(s"[${entry.name} | $label] ") {
      val c = client()
      val call = entry.caller(c, sql)
      val first = call()
      val second = call()
      render(first) shouldBe fresh(sql)
      render(second) shouldBe render(first)
      sameAnswer(first, second) shouldBe false
      // never stored: a WHERE subquery, a UNION ALL's legs, a refusal (A-192)
      c.describeAnswerCount shouldBe 0
    }
    render(client().describe(shapes(2)._2)) should startWith("FAILURE(400|")
  }

  // ---------------------------------------------------------------------------------------------
  // concurrency
  // ---------------------------------------------------------------------------------------------

  it should "answer parallel describes of one statement over one schema alike" in {
    val sql = "SELECT id, n, x, ts, dc, de, dz, dn, obj FROM al"
    val expected = fresh(sql)
    val c = client()
    val statement = single(sql)
    val pool = Executors.newFixedThreadPool(8)
    try {
      val tasks = (1 to 8).map { _ =>
        pool.submit(new Callable[Seq[String]] {
          override def call(): Seq[String] = (1 to 200).map(_ => render(c.describe(statement)))
        })
      }
      val answers = tasks.flatMap(_.get(60, TimeUnit.SECONDS)).distinct
      answers shouldBe Seq(expected)
    } finally pool.shutdownNow()
    sameAnswer(c.describe(statement), c.describe(statement)) shouldBe true
    c.describeAnswerCount shouldBe 1
  }

  it should "answer each describe racing an invalidation by one of the schemas, and the last schema once it settles" in {
    val sql = "SELECT * FROM t"
    val withExtra = baseFields :+ extraField
    val valid = Set(fresh(sql), fresh(sql, withExtra))
    val c = client()
    val statement = single(sql)
    val pool = Executors.newFixedThreadPool(5)
    try {
      val flipper = pool.submit(new Callable[Unit] {
        override def call(): Unit = (1 to 300).foreach { i =>
          c.fields = if (i % 2 == 0) withExtra else baseFields
          c.invalidateSchema("t")
        }
      })
      val describers = (1 to 4).map { _ =>
        pool.submit(new Callable[Seq[String]] {
          override def call(): Seq[String] = (1 to 300).map(_ => render(c.describe(statement)))
        })
      }
      flipper.get(60, TimeUnit.SECONDS)
      val seen = mutable.LinkedHashSet.empty[String]
      describers.foreach(d => seen ++= d.get(60, TimeUnit.SECONDS))
      seen.toSet.diff(valid) shouldBe empty
    } finally pool.shutdownNow()
    c.fields = withExtra
    c.invalidateSchema("t")
    render(c.describe(statement)) shouldBe fresh(sql, withExtra)
    c.fields = baseFields
    c.invalidateSchema("t")
    render(c.describe(statement)) shouldBe fresh(sql)
    c.describeAnswerCount shouldBe 1
  }

  // ---------------------------------------------------------------------------------------------
  // the memo itself: identity keys, schema instances, the cap
  // ---------------------------------------------------------------------------------------------

  private def schemaOf(fields: Seq[(String, String)] = baseFields) =
    Index("t", s"""{"t":${doc(fields)}}""").schema

  "StatementAnswers" should "answer the very statement over the very schema, and nothing else" in {
    val memo = new StatementAnswers[String](cap = 2)
    val statement = single("SELECT id, ts FROM t")
    val twin = single("SELECT id, ts FROM t") // the same SQL, another instance
    val schema = schemaOf()
    val sameMapping = schemaOf() // the same mapping, another instance (a reload)
    memo.put(statement, schema, "answer")
    memo.get(statement, schema) shouldBe Some("answer")
    memo.get(twin, schema) shouldBe None
    memo.get(statement, sameMapping) shouldBe None
    // the answer over the new schema replaces the statement's entry
    memo.put(statement, sameMapping, "new answer")
    memo.size shouldBe 1
    memo.get(statement, schema) shouldBe None
    memo.get(statement, sameMapping) shouldBe Some("new answer")
  }

  it should "replace a statement's entry at the cap, and clear the map for a new statement" in {
    val memo = new StatementAnswers[String](cap = 2)
    val (a, b, c) =
      (single("SELECT id FROM t"), single("SELECT n FROM t"), single("SELECT x FROM t"))
    val schema = schemaOf()
    memo.put(a, schema, "a")
    memo.put(b, schema, "b")
    memo.size shouldBe 2
    // a known statement at the cap: replaced in place, the other kept
    memo.put(a, schema, "a2")
    memo.size shouldBe 2
    memo.get(a, schema) shouldBe Some("a2")
    memo.get(b, schema) shouldBe Some("b")
    // a new statement at the cap: the map is cleared first
    memo.put(c, schema, "c")
    memo.size shouldBe 1
    memo.get(c, schema) shouldBe Some("c")
    memo.get(a, schema) shouldBe None
    memo.get(b, schema) shouldBe None
  }
}
