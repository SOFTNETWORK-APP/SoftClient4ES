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
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.query.Statement
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
}
