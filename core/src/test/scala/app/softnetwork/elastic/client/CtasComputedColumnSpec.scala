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
import app.softnetwork.elastic.licensing.LogbackCapture
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.CreateTable
import app.softnetwork.elastic.sql.schema.{
  validateScriptReferences,
  Column,
  Schema,
  ScriptProcessor,
  SetProcessor,
  Table,
  TableAlias
}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import ch.qos.logback.classic.Level
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicInteger
import scala.concurrent.duration._

/** Issue #385, the `core` half — `CREATE TABLE … AS SELECT` validates the MERGED table BEFORE it
  * touches Elasticsearch, and the two WARNs the `sql` schema emits when it degrades a computed
  * column are asserted here.
  *
  * `CreateTable.validate()` takes `case Left(select) => select.validate()`: it validates the QUERY,
  * not the scripts the query's columns carry, so nothing between the parser and index creation ever
  * looked at them. The `sql` module now drops a script (and a table-level processor) whose operands
  * the projection cannot carry; this hook is the BACKSTOP for what that rule cannot repair —
  * chiefly the TYPE half of #376's rule, which a projection cannot fix by dropping a column.
  *
  * 🔴 The WARN assertions live in THIS module, not in `sql`. `sql` carries no logging backend, so a
  * capture there would pass vacuously under slf4j's NOP logger — and giving it one through a
  * `logback-test.xml` would override `persistence-core-testkit`'s own `logback.xml` on the test
  * classpath of all five ES client modules, via `test->test` (`licensing/build.sbt` records exactly
  * that trap). `core` already has the backend, the `TestLoggingConfigurator` SPI and
  * [[LogbackCapture]].
  *
  * Docker-free: a `NopeClientApi` seeded with the source schema, counting the writes — and the
  * DELETES — it is asked to perform.
  */
class CtasComputedColumnSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem("ctas-computed-column")
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(5.seconds))

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private val schemaLoggerName = "app.softnetwork.elastic.sql.schema"

  private def create(sql: String): Schema = Parser(sql) match {
    case Right(ct: CreateTable) => ct.schema
    case other                  => fail(s"[$sql] expected a CreateTable, got $other")
  }

  /** An ordinary source table. No legacy shape, nothing hand-built: `src` is exactly what `CREATE
    * TABLE src (d DATE, c INTEGER SCRIPT AS (YEAR(d)))` produces.
    */
  private val src: Schema = create("CREATE TABLE src (d DATE, c INTEGER SCRIPT AS (YEAR(d)))")

  /** The statement the rejection tests use, and it is the reviewer's: re-typing the operand IN the
    * projection (`UPPER(d) AS d` is a VARCHAR) makes the copied script type-invalid while `d` is
    * still projected — so the drop rule cannot repair it and the backstop must. It SUCCEEDED before
    * this rule existed, which is what makes a destructive route unacceptable for it.
    */
  private val rejected = "SELECT UPPER(d) AS d, c FROM src"

  /** Counts what reached Elasticsearch. A rejection that still deleted the index, or still created
    * a pipeline, would be the same defect one step later — so the counters, not only the message,
    * are what this asserts.
    */
  private class CountingClient extends NopeClientApi {
    val pipelinesCreated = new AtomicInteger(0)
    val indicesCreated = new AtomicInteger(0)
    val indicesDeleted = new AtomicInteger(0)

    override protected def logger: Logger = LoggerFactory.getLogger(getClass)

    override private[client] def executeCreatePipeline(
      pipelineName: String,
      pipelineDefinition: String
    ): ElasticResult[Boolean] = {
      pipelinesCreated.incrementAndGet()
      ElasticResult.success(true)
    }

    override private[client] def executeCreateIndex(
      index: String,
      settings: String,
      mappings: Option[String],
      aliases: Seq[TableAlias]
    ): ElasticResult[Boolean] = {
      indicesCreated.incrementAndGet()
      ElasticResult.success(true)
    }

    override private[client] def executeDeleteIndex(index: String): ElasticResult[Boolean] = {
      indicesDeleted.incrementAndGet()
      ElasticResult.success(true)
    }

    /** The CTAS population step (`INSERT INTO … AS SELECT`), which a stub cannot really perform —
      * without it every accepted CTAS ends in a `Resource not found` from the search it runs, and
      * the acceptance assertions below could only ever be "not THIS failure", which tolerates any
      * other. Stubbed so a success is assertable as a success.
      */
    override def insertByQuery(index: String, query: String, refresh: Boolean)(implicit
      system: ActorSystem
    ): scala.concurrent.Future[ElasticResult[DmlResult]] =
      scala.concurrent.Future.successful(ElasticResult.success(DmlResult(inserted = 1L)))
  }

  /** The same client, for the `OR REPLACE` route: the target index already exists. */
  private class ExistingTargetClient extends CountingClient {
    override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
      ElasticResult.success(index == "t2")
  }

  private def seeded[C <: CountingClient](client: C, schema: Schema = src): C = {
    client.updateSchema(schema.name, schema)
    client
  }

  private def assertComputedColumnRejection(result: ElasticResult[QueryResult]): Unit =
    result match {
      case ElasticFailure(error) =>
        error.message should startWith("Invalid computed column in CREATE TABLE t2: ")
        error.message should include("'c'")
        error.message should include("'d'")
        error.message should include("VARCHAR")
        error.statusCode shouldBe Some(400)
        error.operation shouldBe Some("ddl")
      case other => fail(s"expected a 400, got $other")
    }

  // ── the rejection, and what it must NOT have done first ───────────────────────────────────────

  "CREATE TABLE … AS SELECT" should "reject a copied computed column the merged table cannot honour" in {
    val client = seeded(new CountingClient)
    assertComputedColumnRejection(client.run(s"CREATE TABLE t2 AS $rejected").futureValue)
    withClue("the rejection must happen BEFORE anything is created: ") {
      client.pipelinesCreated.get() shouldBe 0
      client.indicesCreated.get() shouldBe 0
    }
  }

  it should "reject it on the OR REPLACE route WITHOUT deleting the existing index" in {
    // 🔴 B-1, and the reason the validation was hoisted out of `createNonExistentIndex`:
    // `replaceExistingIndex` DELETES the target first and only then delegates. A rejection raised
    // downstream of that destroys the user's index and puts nothing back — measured, on a
    // statement that succeeded before this rule existed. The delete counter is the assertion that
    // says so; the message alone cannot.
    val client = seeded(new ExistingTargetClient)
    assertComputedColumnRejection(
      client.run(s"CREATE OR REPLACE TABLE t2 AS $rejected").futureValue
    )
    withClue("a refused CREATE OR REPLACE must not have destroyed the existing index: ") {
      client.indicesDeleted.get() shouldBe 0
    }
    client.pipelinesCreated.get() shouldBe 0
    client.indicesCreated.get() shouldBe 0
  }

  /** 🔴 The control, and it is not decoration: without it every CTAS could be rejected and the two
    * tests above would still pass.
    */
  it should "create the index when the projection drops the unsatisfiable script" in {
    val client = seeded(new CountingClient)
    val result = client.run("CREATE TABLE t3 AS SELECT c FROM src").futureValue
    withClue(s"got $result: ") {
      result shouldBe a[ElasticSuccess[_]]
    }
    client.indicesCreated.get() shouldBe 1
    withClue("the dropped script must leave NO processor behind: ") {
      client.pipelinesCreated.get() shouldBe 0
    }
  }

  it should "still create it on the OR REPLACE route, deleting exactly once" in {
    val client = seeded(new ExistingTargetClient)
    client.run("CREATE OR REPLACE TABLE t2 AS SELECT c FROM src").futureValue shouldBe a[
      ElasticSuccess[_]
    ]
    client.indicesDeleted.get() shouldBe 1
    client.indicesCreated.get() shouldBe 1
  }

  /** 🔴 C-1, a defect the HOIST itself introduced: with the target table built before the delete,
    * `CREATE OR REPLACE TABLE src AS SELECT c FROM src` stopped failing — it deleted `src`,
    * recreated it EMPTY and answered 200 (`0 documents indexed`). It failed on `main` (after
    * destroying the index) and it must keep failing; that is the whole point of the guard.
    */
  "a CTAS that selects from itself" should "fail, without deleting the index" in {
    val client = new CountingClient {
      override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
        ElasticResult.success(index == "src")
    }
    client.updateSchema("src", src)
    client.run("CREATE OR REPLACE TABLE src AS SELECT c FROM src").futureValue match {
      case ElasticFailure(error) =>
        error.message shouldBe "CREATE TABLE src cannot select from itself."
        error.statusCode shouldBe Some(400)
        error.operation shouldBe Some("schema")
      case other => fail(s"expected a 400, got $other")
    }
    withClue("the self-referential statement must not have destroyed the index: ") {
      client.indicesDeleted.get() shouldBe 0
    }
    client.indicesCreated.get() shouldBe 0
  }

  /** The target named ONLY as a JOIN leg empties the index just as surely, and `SingleSearch
    * .sources` is `from.tables` alone — it does not see a join. Measured: without the join half of
    * the guard this statement deletes `src`, recreates it and answers 200 on an empty index.
    */
  it should "be refused when the target is only a JOIN leg" in {
    val client = new CountingClient {
      override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
        ElasticResult.success(index == "src")
    }
    client.updateSchema("src", src)
    client
      .run("CREATE OR REPLACE TABLE src AS SELECT o.c FROM other o INNER JOIN src s ON o.c = s.c")
      .futureValue match {
      case ElasticFailure(error) =>
        error.message shouldBe "CREATE TABLE src cannot select from itself."
        error.statusCode shouldBe Some(400)
      case other => fail(s"expected a 400, got $other")
    }
    client.indicesDeleted.get() shouldBe 0
  }

  /** 🔴 C-2 — a SECOND data-loss fix the hoist delivered, previously unclaimed and unguarded:
    * `CREATE OR REPLACE TABLE t2 AS SELECT c FROM missing` DESTROYED `t2` on `main` (delete first,
    * fail to read the source second). The source is now read before the delete, so a failure to
    * read it costs nothing.
    */
  it should "not delete the target when the SOURCE schema cannot be read" in {
    val client = seeded(new ExistingTargetClient) // seeds `src` only; `missing` is unknown
    client.run("CREATE OR REPLACE TABLE t2 AS SELECT c FROM missing").futureValue match {
      case ElasticFailure(error) =>
        error.message should startWith("Error retrieving source schema missing")
        error.operation shouldBe Some("schema")
      case other => fail(s"expected a failure, got $other")
    }
    withClue("the target must survive a source that cannot be read: ") {
      client.indicesDeleted.get() shouldBe 0
    }
    client.indicesCreated.get() shouldBe 0
  }

  // ── the ALTER route, whose message has to carry a remedy it did not need before ───────────────

  /** 🔴 M-3. Making a LOADED table's scripts visible means a table with ONE invalid computed column
    * refuses EVERY alteration of that table, with a message naming a column the user did not touch.
    * The rejection is right; a message that does not say what to do about it is not. The remedy is
    * phrased to be true in both cases — the offending column may well be the one being altered.
    */
  "an ALTER refused by a pre-existing computed column" should "name the remedy, not just the column" in {
    // The legacy shape: `YEAR(k)` over a KEYWORD is what an index created before #376 can carry,
    // with `expr = None` because it was read back out of Elasticsearch.
    val legacy = Table(
      name = "legacy",
      columns = List(
        Column("k", SQLTypes.Keyword),
        Column(
          "c",
          SQLTypes.Int,
          script = Some(
            ScriptProcessor(
              script = "YEAR(k)",
              column = "c",
              dataType = SQLTypes.Int,
              source = "def param1 = ctx.k.get(ChronoField.YEAR); ctx.c = param1"
            )
          )
        )
      )
    )
    val client = new CountingClient {
      override private[client] def executeIndexExists(index: String): ElasticResult[Boolean] =
        ElasticResult.success(index == "legacy")
    }
    client.updateSchema("legacy", legacy)

    // an alteration that has NOTHING to do with the offending column
    client.run("ALTER TABLE legacy ADD COLUMN x INTEGER").futureValue match {
      case ElasticFailure(error) =>
        error.message should startWith("Invalid computed column in ALTER TABLE legacy: ")
        error.message should include("'c'")
        error.message should include("'k'")
        withClue(s"[${error.message}] ") {
          error.message should include("drop or redefine")
          error.message should include("if it is not the one you are altering")
        }
        error.statusCode shouldBe Some(400)
        error.operation shouldBe Some("ddl")
      case other => fail(s"expected a 400, got $other")
    }
  }

  // ── the receipts: every degradation the sql schema performs silently is logged ─────────────────

  "the dropped script" should "be reported at WARN, naming the table, the column and the operand" in {
    val client = seeded(new CountingClient)
    val (result, events) = LogbackCapture.capture(schemaLoggerName, Level.WARN) {
      client.run("CREATE TABLE t3 AS SELECT c FROM src").futureValue
    }
    result shouldBe a[ElasticSuccess[_]]
    val dropped = events.filter(_.message.contains("WITHOUT its script"))
    withClue(s"captured: $events ") {
      dropped.size shouldBe 1
      dropped.head.level shouldBe "WARN"
      dropped.head.message should include("'c'")
      dropped.head.message should include("'src'")
      dropped.head.message should include("'d'")
    }
  }

  "a projection that carries every operand" should "emit no WARN at all" in {
    val client = seeded(new CountingClient)
    val (_, events) = LogbackCapture.capture(schemaLoggerName, Level.WARN) {
      client.run("CREATE TABLE t3 AS SELECT d, c FROM src").futureValue
    }
    events shouldBe empty
  }

  "a dropped table-level processor" should "be reported at WARN too" in {
    // The pipeline half of the same rule: `mergeWithSearch` inherited `Table.processors` wholesale,
    // so a `set { field: c, copy_from: d }` reached a target with no `d`.
    val withProcessor = create("CREATE TABLE src (d DATE, c INTEGER)").copy(processors =
      Seq(
        SetProcessor(column = "c", value = app.softnetwork.elastic.sql.Null, copyFrom = Some("d"))
      )
    )
    val client = seeded(new CountingClient, withProcessor)
    val (result, events) = LogbackCapture.capture(schemaLoggerName, Level.WARN) {
      client.run("CREATE TABLE t3 AS SELECT c FROM src").futureValue
    }
    result shouldBe a[ElasticSuccess[_]]
    val dropped = events.filter(_.message.contains("is NOT carried"))
    withClue(s"captured: $events ") {
      dropped.size shouldBe 1
      dropped.head.message should include("c COPY FROM d")
      dropped.head.message should include("'src'")
      dropped.head.message should include("'d'")
    }
    withClue("and nothing was left to create a pipeline for: ") {
      client.pipelinesCreated.get() shouldBe 0
    }
  }

  "a stored expression this grammar cannot parse" should "be skipped with a WARN, never rejected" in {
    // The fail-open, and its receipt. A legacy or hand-written `_meta.script.sql` must not block
    // every future ALTER on that table; the cost is that its references go unchecked, which is
    // exactly the pre-existing behaviour for it.
    val legacy = Table(
      name = "legacy",
      columns = List(
        Column("n", SQLTypes.Int),
        Column(
          "c",
          SQLTypes.Int,
          script = Some(
            ScriptProcessor(
              script = "### not an expression this grammar accepts ###",
              column = "c",
              dataType = SQLTypes.Int,
              source = "ctx.c = 1"
            )
          )
        )
      )
    )
    val (result, events) = LogbackCapture.capture(schemaLoggerName, Level.WARN) {
      validateScriptReferences(legacy)
    }
    result shouldBe Right(())
    val skipped = events.filter(_.message.contains("cannot be parsed"))
    withClue(s"captured: $events ") {
      skipped.size shouldBe 1
      skipped.head.message should include("'c'")
    }
  }
}
