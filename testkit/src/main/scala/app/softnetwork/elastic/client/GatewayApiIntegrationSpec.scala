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
import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.client.result.{
  DdlResult,
  DmlResult,
  ElasticResult,
  ElasticSuccess,
  QueryPipeline,
  QueryResult,
  QueryRows,
  QueryStream,
  QueryStructured,
  QueryTable
}
import app.softnetwork.elastic.client.scroll.ScrollMetrics
import app.softnetwork.elastic.scalatest.ElasticTestKit
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.schema.{IngestPipeline, Table}
import app.softnetwork.persistence.generateUUID
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDate
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import java.util.concurrent.TimeUnit

// ---------------------------------------------------------------------------
// Base test trait — to be mixed with ElasticDockerTestKit
// ---------------------------------------------------------------------------

trait GatewayApiIntegrationSpec extends AnyFlatSpecLike with Matchers with ScalaFutures {
  self: ElasticTestKit =>

  lazy val log: Logger = LoggerFactory getLogger getClass.getName

  implicit val system: ActorSystem = ActorSystem(generateUUID())
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(30, Seconds))

  // Provided by concrete test class
  def client: GatewayApi

  override def beforeAll(): Unit = {
    self.beforeAll()
  }

  override def afterAll(): Unit = {
    Await.result(system.terminate(), Duration(30, TimeUnit.SECONDS))
    self.afterAll()
  }

  // -------------------------------------------------------------------------
  // Helper: assert SELECT result type
  // -------------------------------------------------------------------------

  private def normalizeRow(row: Map[String, Any]): Map[String, Any] = {
    val updated = row - "_id" - "_index" - "_score" - "_version" - "_sort"
    updated.map(entry =>
      entry._2 match {
        case m: Map[_, _] =>
          entry._1 -> normalizeRow(m.asInstanceOf[Map[String, Any]])
        case seq: Seq[_] if seq.nonEmpty && seq.head.isInstanceOf[Map[_, _]] =>
          entry._1 -> seq
            .asInstanceOf[Seq[Map[String, Any]]]
            .map(m => normalizeRow(m))
        case other => entry._1 -> other
      }
    )
  }

  def assertSelectResult(
    res: ElasticResult[QueryResult],
    rows: Seq[Map[String, Any]] = Seq.empty
  ): Unit = {
    res.isSuccess shouldBe true
    res.toOption.get match {
      case QueryStream(stream) =>
        val sink = Sink.fold[Seq[Map[String, Any]], (Map[String, Any], ScrollMetrics)](Seq.empty) {
          case (acc, (row, _)) =>
            acc :+ normalizeRow(row)
        }
        val results = stream.runWith(sink).futureValue
        if (rows.nonEmpty) {
          results.size shouldBe rows.size
          results should contain theSameElementsAs rows
        } else {
          log.info(s"Rows: $results")
        }
      case QueryStructured(response) =>
        val results =
          response.results.map(normalizeRow)
        if (rows.nonEmpty) {
          results.size shouldBe rows.size
          results should contain theSameElementsAs rows
        } else {
          log.info(s"Rows: $results")
        }
      case q: QueryRows =>
        val results = q.rows.map(normalizeRow)
        if (rows.nonEmpty) {
          results.size shouldBe rows.size
          results should contain theSameElementsAs rows
        } else {
          log.info(s"Rows: $results")
        }
      case other => fail(s"Unexpected QueryResult type for SELECT: $other")
    }
  }

  // -------------------------------------------------------------------------
  // Helper: assert DDL result type
  // -------------------------------------------------------------------------

  def assertDdl(res: ElasticResult[QueryResult]): Unit = {
    res.isSuccess shouldBe true
    res.toOption.get shouldBe a[DdlResult]
  }

  // -------------------------------------------------------------------------
  // Helper: assert DML result type
  // -------------------------------------------------------------------------

  def assertDml(res: ElasticResult[QueryResult], result: Option[DmlResult] = None): Unit = {
    res.isSuccess shouldBe true
    res.toOption.get shouldBe a[DmlResult]
    result match {
      case Some(expected) =>
        val dml = res.toOption.get.asInstanceOf[DmlResult]
        dml.inserted shouldBe expected.inserted
        dml.updated shouldBe expected.updated
        dml.deleted shouldBe expected.deleted
      case None => // do nothing
    }
  }

  // -------------------------------------------------------------------------
  // Helper: assert SHOW TABLE result type
  // -------------------------------------------------------------------------

  def assertShowTable(res: ElasticResult[QueryResult]): Table = {
    res.isSuccess shouldBe true
    res.toOption.get shouldBe a[QueryTable]
    res.toOption.get.asInstanceOf[QueryTable].table
  }

  // -------------------------------------------------------------------------
  // Helper: assert SHOW PIPELINE result type
  // -------------------------------------------------------------------------

  def assertShowPipeline(res: ElasticResult[QueryResult]): IngestPipeline = {
    res.isSuccess shouldBe true
    res.toOption.get shouldBe a[QueryPipeline]
    res.toOption.get.asInstanceOf[QueryPipeline].pipeline
  }

  // -------------------------------------------------------------------------
  // SHOW / DESCRIBE TABLE tests
  // -------------------------------------------------------------------------

  behavior of "SHOW / DESCRIBE TABLE"

  it should "return the table schema using SHOW TABLE" in {
    val create =
      """CREATE TABLE IF NOT EXISTS show_users (
        |  id INT NOT NULL,
        |  name VARCHAR,
        |  age INT DEFAULT 0
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val show = client.run("SHOW TABLE show_users").futureValue
    val table = assertShowTable(show)

    val ddl = table.ddl
    ddl should include("CREATE OR REPLACE TABLE show_users")
    ddl should include("id INT NOT NULL")
    ddl should include("name VARCHAR")
    ddl should include("age INT DEFAULT 0")
  }

  it should "describe a table using DESCRIBE TABLE" in {
    val create =
      """CREATE TABLE IF NOT EXISTS desc_users (
        |  id INT NOT NULL,
        |  name VARCHAR DEFAULT 'anonymous',
        |  age INT,
        |  profile STRUCT FIELDS(
        |    city VARCHAR,
        |    followers INT
        |  )
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val res = client.run("DESCRIBE TABLE desc_users").futureValue
    res.isSuccess shouldBe true
    res.toOption.get shouldBe a[QueryRows]

    val rows = res.toOption.get.asInstanceOf[QueryRows].rows

    rows.exists(_("name") == "id") shouldBe true
    rows.exists(_("name") == "name") shouldBe true
    rows.exists(_("name") == "profile.city") shouldBe true
  }

  // ===========================================================================
  // 2. DDL — CREATE TABLE, ALTER TABLE, DROP TABLE, TRUNCATE TABLE
  // ===========================================================================

  behavior of "DDL statements"

  // ---------------------------------------------------------------------------
  // CREATE TABLE — full complex schema
  // ---------------------------------------------------------------------------

  it should "create a table with complex columns, defaults, scripts, struct, PK, partitioning and options" in {
    val sql =
      """CREATE TABLE IF NOT EXISTS users (
        |  id INT NOT NULL COMMENT 'user identifier',
        |  name VARCHAR FIELDS(raw Keyword COMMENT 'sortable') DEFAULT 'anonymous' OPTIONS (analyzer = 'french', search_analyzer = 'french'),
        |  birthdate DATE,
        |  age INT SCRIPT AS (DATEDIFF(birthdate, CURRENT_DATE, YEAR)),
        |  ingested_at TIMESTAMP DEFAULT _ingest.timestamp,
        |  profile STRUCT FIELDS(
        |    bio VARCHAR,
        |    followers INT,
        |    join_date DATE,
        |    seniority INT SCRIPT AS (DATEDIFF(profile.join_date, CURRENT_DATE, DAY))
        |  ) COMMENT 'user profile',
        |  PRIMARY KEY (id)
        |) PARTITION BY birthdate (MONTH), OPTIONS (mappings = (dynamic = false));""".stripMargin

    assertDdl(client.run(sql).futureValue)

    // Vérification via SHOW TABLE
    val table = assertShowTable(client.run("SHOW TABLE users").futureValue)
    val ddl = table.ddl.replaceAll("\\s+", " ")

    ddl should include("CREATE OR REPLACE TABLE users")
    ddl should include("id INT NOT NULL COMMENT 'user identifier'")
    ddl should include(
      """name VARCHAR FIELDS ( raw KEYWORD COMMENT 'sortable' ) DEFAULT 'anonymous' OPTIONS (analyzer = "french", search_analyzer = "french")"""
    )
    ddl should include("birthdate DATE")
    ddl should include("age INT SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR))")
    ddl should include("ingested_at TIMESTAMP DEFAULT _ingest.timestamp")
    ddl should include("profile STRUCT FIELDS (")
    ddl should include("bio VARCHAR")
    ddl should include("followers INT")
    ddl should include("join_date DATE")
    ddl should include("seniority INT SCRIPT AS (DATE_DIFF(profile.join_date, CURRENT_DATE, DAY))")
    ddl should include("PRIMARY KEY (id)")
    ddl should include("PARTITION BY birthdate (MONTH)")
  }

  // ---------------------------------------------------------------------------
  // CREATE TABLE IF NOT EXISTS
  // ---------------------------------------------------------------------------

  it should "create a simple table and allow re-creation with IF NOT EXISTS" in {
    val sql =
      """CREATE TABLE IF NOT EXISTS accounts (
        |  id INT NOT NULL,
        |  owner VARCHAR,
        |  balance DOUBLE
        |) OPTIONS (
        |    settings = (
        |      number_of_shards = 1,
        |      number_of_replicas = 0
        |    )
        |);""".stripMargin

    assertDdl(client.run(sql).futureValue)
    assertDdl(client.run(sql).futureValue) // second call should succeed
  }

  // ---------------------------------------------------------------------------
  // CREATE OR REPLACE TABLE AS SELECT
  // ---------------------------------------------------------------------------

  it should "create or replace a table from a SELECT query" in {
    val createSource =
      """CREATE TABLE IF NOT EXISTS accounts_src (
        |  id INT NOT NULL,
        |  name VARCHAR,
        |  active BOOLEAN,
        |  PRIMARY KEY (id)
        |);""".stripMargin

    assertDdl(client.run(createSource).futureValue)

    val insertSource =
      """INSERT INTO accounts_src (id, name, active) VALUES
        | (1, 'Alice', true),
        | (2, 'Bob',   false),
        | (3, 'Chloe', true);""".stripMargin

    assertDml(client.run(insertSource).futureValue)

    val createOrReplace =
      "CREATE OR REPLACE TABLE users_cr AS SELECT id, name FROM accounts_src WHERE active = true;"

    assertDml(client.run(createOrReplace).futureValue)

    // Vérification via SHOW TABLE
    val table = assertShowTable(client.run("SHOW TABLE users_cr").futureValue)
    table.ddl should include("CREATE OR REPLACE TABLE users_cr")
    table.ddl should include("id INT")
    table.ddl should include("name VARCHAR")
  }

  // ---------------------------------------------------------------------------
  // DROP TABLE
  // ---------------------------------------------------------------------------

  it should "drop a table if it exists" in {
    val create =
      """CREATE TABLE IF NOT EXISTS tmp_drop (
        |  id INT NOT NULL,
        |  value VARCHAR
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val drop =
      """DROP TABLE IF EXISTS tmp_drop;""".stripMargin

    assertDdl(client.run(drop).futureValue)
  }

  // ---------------------------------------------------------------------------
  // TRUNCATE TABLE
  // ---------------------------------------------------------------------------

  it should "truncate a table" in {
    val create =
      """CREATE TABLE IF NOT EXISTS tmp_truncate (
        |  id INT NOT NULL,
        |  value VARCHAR
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val insert =
      """INSERT INTO tmp_truncate (id, value) VALUES
        |  (1, 'a'),
        |  (2, 'b'),
        |  (3, 'c');""".stripMargin

    assertDml(client.run(insert).futureValue)

    val truncate = "TRUNCATE TABLE tmp_truncate;"
    assertDdl(client.run(truncate).futureValue)

    // Vérification : SELECT doit renvoyer 0 lignes
    val select = client.run("SELECT * FROM tmp_truncate").futureValue
    assertSelectResult(select)
  }

  // ---------------------------------------------------------------------------
  // ALTER TABLE — ADD COLUMN
  // ---------------------------------------------------------------------------

  it should "add a column IF NOT EXISTS with DEFAULT" in {
    val create =
      """CREATE TABLE IF NOT EXISTS users_alter1 (
        |  id INT NOT NULL,
        |  status VARCHAR
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val alter =
      """ALTER TABLE users_alter1
        |  ADD COLUMN IF NOT EXISTS age INT DEFAULT 0;""".stripMargin

    assertDdl(client.run(alter).futureValue)

    val table = assertShowTable(client.run("SHOW TABLE users_alter1").futureValue)
    table.ddl should include("age INT DEFAULT 0")
  }

  // ---------------------------------------------------------------------------
  // ALTER TABLE — RENAME COLUMN
  // ---------------------------------------------------------------------------

  it should "rename a column" in {
    val create =
      """CREATE TABLE IF NOT EXISTS users_alter2 (
        |  id INT NOT NULL,
        |  name VARCHAR
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val alter =
      """ALTER TABLE users_alter2
        |  RENAME COLUMN name TO full_name;""".stripMargin

    assertDdl(client.run(alter).futureValue)

    val table = assertShowTable(client.run("SHOW TABLE users_alter2").futureValue)
    table.ddl should include("full_name VARCHAR")
  }

  // ---------------------------------------------------------------------------
  // ALTER TABLE — COMMENT
  // ---------------------------------------------------------------------------

  it should "set and drop a column COMMENT" in {
    val create =
      """CREATE TABLE IF NOT EXISTS users_alter3 (
        |  id INT NOT NULL,
        |  status VARCHAR
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val setComment =
      """ALTER TABLE users_alter3
        |  ALTER COLUMN IF EXISTS status SET COMMENT 'a description';""".stripMargin

    val dropComment =
      """ALTER TABLE users_alter3
        |  ALTER COLUMN IF EXISTS status DROP COMMENT;""".stripMargin

    assertDdl(client.run(setComment).futureValue)
    assertDdl(client.run(dropComment).futureValue)

    val table = assertShowTable(client.run("SHOW TABLE users_alter3").futureValue)
    table.ddl should not include "COMMENT 'a description'"
  }

  // ---------------------------------------------------------------------------
  // ALTER TABLE — DEFAULT
  // ---------------------------------------------------------------------------

  it should "set and drop a DEFAULT value on a column" in {
    val create =
      """CREATE TABLE IF NOT EXISTS users_alter4 (
        |  id INT NOT NULL,
        |  status VARCHAR
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val setDefault =
      """ALTER TABLE users_alter4
        |  ALTER COLUMN status SET DEFAULT 'active';""".stripMargin

    val dropDefault =
      """ALTER TABLE users_alter4
        |  ALTER COLUMN status DROP DEFAULT;""".stripMargin

    assertDdl(client.run(setDefault).futureValue)
    assertDdl(client.run(dropDefault).futureValue)

    val table = assertShowTable(client.run("SHOW TABLE users_alter4").futureValue)
    table.ddl should not include "DEFAULT 'active'"
  }

  // ---------------------------------------------------------------------------
  // ALTER TABLE — STRUCT SET FIELDS
  // ---------------------------------------------------------------------------

  it should "alter a STRUCT column with SET FIELDS" in {
    val create =
      """CREATE TABLE IF NOT EXISTS users_alter5 (
        |  id INT NOT NULL,
        |  profile STRUCT FIELDS(
        |    bio VARCHAR,
        |    followers INT,
        |    join_date DATE,
        |    seniority INT SCRIPT AS (DATEDIFF(profile.join_date, CURRENT_DATE, DAY))
        |  )
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val alter =
      """ALTER TABLE users_alter5
        |  ALTER COLUMN profile SET FIELDS (
        |    bio VARCHAR,
        |    followers INT,
        |    join_date DATE,
        |    seniority INT SCRIPT AS (DATEDIFF(profile.join_date, CURRENT_DATE, DAY)),
        |    reputation DOUBLE DEFAULT 0.0
        |  );""".stripMargin

    assertDdl(client.run(alter).futureValue)

    val table = assertShowTable(client.run("SHOW TABLE users_alter5").futureValue)
    table.ddl should include("reputation DOUBLE DEFAULT 0.0")
    table.defaultPipeline.processors.size shouldBe 2
  }

  // ---------------------------------------------------------------------------
  // ALTER TABLE — NOT NULL
  // ---------------------------------------------------------------------------

  it should "set and drop NOT NULL on a column" in {
    val create =
      """CREATE TABLE IF NOT EXISTS users_alter6 (
        |  id INT,
        |  status VARCHAR
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val setNotNull =
      """ALTER TABLE users_alter6
        |  ALTER COLUMN status SET NOT NULL;""".stripMargin

    val dropNotNull =
      """ALTER TABLE users_alter6
        |  ALTER COLUMN status DROP NOT NULL;""".stripMargin

    assertDdl(client.run(setNotNull).futureValue)
    assertDdl(client.run(dropNotNull).futureValue)

    val table = assertShowTable(client.run("SHOW TABLE users_alter6").futureValue)
    table.ddl should not include "NOT NULL"
  }

  // ---------------------------------------------------------------------------
  // ALTER TABLE — TYPE CHANGE (UNSAFE → migration)
  // ---------------------------------------------------------------------------

  it should "change a column data type (UNSAFE → migration)" in {
    val create =
      """CREATE TABLE IF NOT EXISTS users_alter7 (
        |  id INT NOT NULL,
        |  status VARCHAR
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val alter =
      """ALTER TABLE users_alter7
        |  ALTER COLUMN id SET DATA TYPE BIGINT;""".stripMargin

    assertDdl(client.run(alter).futureValue)

    val table = assertShowTable(client.run("SHOW TABLE users_alter7").futureValue)
    table.ddl should include("id BIGINT NOT NULL")
  }

  // ---------------------------------------------------------------------------
  // ALTER TABLE — multi-alteration
  // ---------------------------------------------------------------------------

  it should "apply multiple alterations in a single ALTER TABLE statement" in {
    val create =
      """CREATE TABLE IF NOT EXISTS users_alter8 (
        |  id INT NOT NULL,
        |  name VARCHAR,
        |  status VARCHAR,
        |  profile STRUCT FIELDS(
        |    description VARCHAR,
        |    visibility BOOLEAN
        |  )
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val alter =
      """ALTER TABLE users_alter8 (
        |  ADD COLUMN IF NOT EXISTS age INT DEFAULT 0,
        |  RENAME COLUMN name TO full_name,
        |  ALTER COLUMN IF EXISTS status SET DEFAULT 'active',
        |  ALTER COLUMN IF EXISTS profile SET FIELDS (
        |    description VARCHAR DEFAULT 'N/A',
        |    visibility BOOLEAN DEFAULT true
        |  )
        |);""".stripMargin

    assertDdl(client.run(alter).futureValue)

    val table = assertShowTable(client.run("SHOW TABLE users_alter8").futureValue)

    table.ddl should include("age INT DEFAULT 0")
    table.ddl should include("full_name VARCHAR")
    table.ddl should include("status VARCHAR DEFAULT 'active'")
    table.ddl should include("description VARCHAR DEFAULT 'N/A'")
    table.ddl should include("visibility BOOLEAN DEFAULT true")
  }

  // ===========================================================================
  // 3. DML — INSERT / UPDATE / DELETE
  // ===========================================================================

  behavior of "DML statements"

  // ---------------------------------------------------------------------------
  // INSERT
  // ---------------------------------------------------------------------------

  it should "insert rows into a table and return a DmlResult" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dml_users (
        |  id INT NOT NULL,
        |  name VARCHAR,
        |  age INT
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val insert =
      """INSERT INTO dml_users (id, name, age) VALUES
        |  (1, 'Alice', 30),
        |  (2, 'Bob',   40),
        |  (3, 'Chloe', 25);""".stripMargin

    val res = client.run(insert).futureValue
    assertDml(res)

    val dml = res.toOption.get.asInstanceOf[DmlResult]
    dml.inserted shouldBe 3

    // Vérification via SELECT
    val select = client.run("SELECT * FROM dml_users ORDER BY id ASC").futureValue
    assertSelectResult(select)
  }

  // ---------------------------------------------------------------------------
  // UPDATE
  // ---------------------------------------------------------------------------

  it should "update rows using UPDATE ... WHERE and return a DmlResult" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dml_accounts (
        |  id INT NOT NULL,
        |  owner KEYWORD,
        |  balance DOUBLE
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val insert =
      """INSERT INTO dml_accounts (id, owner, balance) VALUES
        |  (1, 'Alice', 100.00),
        |  (2, 'Bob',   50.00),
        |  (3, 'Chloe', 75.00);""".stripMargin

    assertDml(client.run(insert).futureValue)

    val update =
      """UPDATE dml_accounts
        |SET balance = 125
        |WHERE owner = 'Alice';""".stripMargin

    val res = client.run(update).futureValue
    assertDml(res)

    val dml = res.toOption.get.asInstanceOf[DmlResult]
    dml.updated should be >= 1L

    // Vérification via SELECT
    val select =
      """SELECT owner, balance
        |FROM dml_accounts
        |WHERE owner = 'Alice';""".stripMargin

    val q = client.run(select).futureValue
    assertSelectResult(q)
  }

  // ---------------------------------------------------------------------------
  // DELETE
  // ---------------------------------------------------------------------------

  it should "delete rows using DELETE ... WHERE and return a DmlResult" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dml_logs (
        |  id INT NOT NULL,
        |  level KEYWORD,
        |  message VARCHAR
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val insert =
      """INSERT INTO dml_logs (id, level, message) VALUES
        |  (1, 'INFO',  'started'),
        |  (2, 'ERROR', 'failed'),
        |  (3, 'INFO',  'running');""".stripMargin

    assertDml(client.run(insert).futureValue)

    val delete =
      """DELETE FROM dml_logs
        |WHERE level = 'ERROR';""".stripMargin

    val res = client.run(delete).futureValue
    assertDml(res)

    val dml = res.toOption.get.asInstanceOf[DmlResult]
    dml.deleted shouldBe 1L

    // Vérification via SELECT
    val select =
      """SELECT id, level
        |FROM dml_logs
        |ORDER BY id ASC;""".stripMargin

    val q = client.run(select).futureValue
    assertSelectResult(q)
  }

  // ---------------------------------------------------------------------------
  // INSERT + UPDATE + DELETE chain
  // ---------------------------------------------------------------------------

  it should "support a full DML lifecycle: INSERT → UPDATE → DELETE" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dml_chain (
        |  id INT NOT NULL,
        |  value INT
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val insert =
      """INSERT INTO dml_chain (id, value) VALUES
        |  (1, 10),
        |  (2, 20),
        |  (3, 30);""".stripMargin

    assertDml(client.run(insert).futureValue)

    val update =
      """UPDATE dml_chain
        |SET value = 50
        |WHERE id IN (1, 3);""".stripMargin

    assertDml(client.run(update).futureValue)

    val delete =
      """DELETE FROM dml_chain
        |WHERE value > 40;""".stripMargin

    assertDml(client.run(delete).futureValue)

    // Vérification finale
    val select = client.run("SELECT * FROM dml_chain ORDER BY id ASC").futureValue
    assertSelectResult(select)
  }

  // ---------------------------------------------------------------------------
  // COPY INTO integration test
  // ---------------------------------------------------------------------------

  it should "support COPY INTO for JSONL and JSON_ARRAY formats" in {
    // 1. Create table with primary key
    val create =
      """CREATE TABLE IF NOT EXISTS copy_into_test (
        |  uuid KEYWORD NOT NULL,
        |  name VARCHAR,
        |  birthDate DATE,
        |  childrenCount INT,
        |  PRIMARY KEY (uuid)
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    // 2. Prepare sample documents
    val persons = List(
      """{"uuid": "A12", "name": "Homer Simpson", "birthDate": "1967-11-21", "childrenCount": 0}""",
      """{"uuid": "A14", "name": "Moe Szyslak", "birthDate": "1967-11-21", "childrenCount": 0}""",
      """{"uuid": "A16", "name": "Barney Gumble", "birthDate": "1969-05-09", "childrenCount": 2}"""
    )

    // 3. Create a temporary JSONL file
    val jsonlFile = java.io.File.createTempFile("copy_into_jsonl", ".jsonl")
    jsonlFile.deleteOnExit()
    val writer1 = new java.io.PrintWriter(jsonlFile)
    persons.foreach(writer1.println)
    writer1.close()

    // 4. COPY INTO using JSONL (format inferred)
    val copyJsonl =
      s"""COPY INTO copy_into_test FROM "${jsonlFile.getAbsolutePath}";"""

    val jsonlResult = client.run(copyJsonl).futureValue
    assertDml(jsonlResult, Some(DmlResult(inserted = persons.size)))

    // 5. Create a temporary JSON_ARRAY file
    val jsonArrayFile = java.io.File.createTempFile("copy_into_array", ".json")
    jsonArrayFile.deleteOnExit()
    val writer2 = new java.io.PrintWriter(jsonArrayFile)
    writer2.println("[")
    writer2.println(persons.mkString(",\n"))
    writer2.println("]")
    writer2.close()

    // 6. COPY INTO using JSON_ARRAY
    val copyArray =
      s"""COPY INTO copy_into_test FROM "${jsonArrayFile.getAbsolutePath}" FILE_FORMAT = JSON_ARRAY ON CONFLICT DO UPDATE;"""

    val arrayResult = client.run(copyArray).futureValue
    assertDml(arrayResult, Some(DmlResult(inserted = persons.size)))

    // 7. Final verification: SELECT all documents
    val select = client.run("SELECT * FROM copy_into_test ORDER BY uuid ASC").futureValue
    assertSelectResult(
      select,
      Seq(
        Map(
          "uuid"          -> "A12",
          "name"          -> "Homer Simpson",
          "birthDate"     -> LocalDate.parse("1967-11-21"),
          "childrenCount" -> 0
        ),
        Map(
          "uuid"          -> "A14",
          "name"          -> "Moe Szyslak",
          "birthDate"     -> LocalDate.parse("1967-11-21"),
          "childrenCount" -> 0
        ),
        Map(
          "uuid"          -> "A16",
          "name"          -> "Barney Gumble",
          "birthDate"     -> LocalDate.parse("1969-05-09"),
          "childrenCount" -> 2
        )
      )
    )
  }

  // ===========================================================================
  // 4. DQL — SELECT, JOIN, UNNEST, GROUP BY, HAVING, ORDER BY, LIMIT/OFFSET,
  //          functions, window functions, nested fields, UNION ALL
  // ===========================================================================

  behavior of "DQL statements"

  // ---------------------------------------------------------------------------
  // Setup for DQL tests: base table with nested fields
  // ---------------------------------------------------------------------------

  it should "prepare DQL test data" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dql_users (
        |  id INT NOT NULL,
        |  name VARCHAR FIELDS(
        |    raw KEYWORD
        |  ) OPTIONS (fielddata = true),
        |  age INT,
        |  birthdate DATE,
        |  profile STRUCT FIELDS(
        |    city VARCHAR OPTIONS (fielddata = true),
        |    followers INT
        |  )
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val insert =
      """INSERT INTO dql_users (id, name, age, birthdate, profile) VALUES
        |  (1, 'Alice', 30, '1994-01-01', {city = "Paris", followers = 100}),
        |  (2, 'Bob',   40, '1984-05-10', {city = "Lyon",  followers = 50}),
        |  (3, 'Chloe', 25, '1999-07-20', {city = "Paris", followers = 200}),
        |  (4, 'David', 50, '1974-03-15', {city = "Marseille", followers = 10});
        |""".stripMargin

    assertDml(client.run(insert).futureValue, Some(DmlResult(inserted = 4)))
  }

  // ---------------------------------------------------------------------------
  // SELECT simple + alias + nested fields
  // ---------------------------------------------------------------------------

  it should "execute a simple SELECT with aliases and nested fields" in {
    val sql =
      """SELECT id,
        |       name AS full_name,
        |       profile.city AS city,
        |       profile.followers AS followers
        |FROM dql_users
        |ORDER BY id ASC;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(
      res,
      Seq(
        Map(
          "id"        -> 1,
          "full_name" -> "Alice",
          "city"      -> "Paris",
          "followers" -> 100,
          "profile"   -> Map("city" -> "Paris", "followers" -> 100)
        ),
        Map(
          "id"        -> 2,
          "full_name" -> "Bob",
          "city"      -> "Lyon",
          "followers" -> 50,
          "profile"   -> Map("city" -> "Lyon", "followers" -> 50)
        ),
        Map(
          "id"        -> 3,
          "full_name" -> "Chloe",
          "city"      -> "Paris",
          "followers" -> 200,
          "profile"   -> Map("city" -> "Paris", "followers" -> 200)
        ),
        Map(
          "id"        -> 4,
          "full_name" -> "David",
          "city"      -> "Marseille",
          "followers" -> 10,
          "profile"   -> Map("city" -> "Marseille", "followers" -> 10)
        )
      )
    )
  }

  // ---------------------------------------------------------------------------
  // UNION ALL
  // ---------------------------------------------------------------------------

  it should "execute a UNION ALL query" in {
    val sql =
      """SELECT id, name FROM dql_users WHERE age > 30
        |UNION ALL
        |SELECT id, name FROM dql_users WHERE age <= 30;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(
      res,
      Seq(
        Map("id" -> 2, "name" -> "Bob"),
        Map("id" -> 4, "name" -> "David"),
        Map("id" -> 1, "name" -> "Alice"),
        Map("id" -> 3, "name" -> "Chloe")
      )
    )
  }

  // ---------------------------------------------------------------------------
  // JOIN + UNNEST
  // ---------------------------------------------------------------------------

  it should "execute a JOIN with UNNEST on array of structs" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dql_orders (
        |  id INT NOT NULL,
        |  customer_id INT,
        |  items ARRAY<STRUCT> FIELDS(
        |    product VARCHAR OPTIONS (fielddata = true),
        |    quantity INT,
        |    price DOUBLE
        |  ) OPTIONS (include_in_parent = false)
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val table = assertShowTable(client.run("SHOW TABLE dql_orders").futureValue)
    table.ddl should include("items ARRAY<STRUCT> FIELDS")

    val insert =
      """INSERT INTO dql_orders (id, customer_id, items) VALUES
        |  (1, 1, [ { product = "A", quantity = 2, price = 10.0 },
        |           { product = "B", quantity = 1, price = 20.0 } ]),
        |  (2, 2, [ { product = "C", quantity = 3, price = 5.0 } ]);""".stripMargin

    assertDml(client.run(insert).futureValue, Some(DmlResult(inserted = 2)))

    val sql =
      """SELECT
        | o.id,
        | items.product,
        | items.quantity,
        | SUM(items.price * items.quantity) OVER (PARTITION BY o.id) AS total_price
        |FROM dql_orders o
        |JOIN UNNEST(o.items) AS items
        |WHERE items.quantity >= 1
        |ORDER BY o.id ASC;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(
      res,
      Seq(
        Map(
          "id" -> 1,
          "items" -> Seq(
            Map("product" -> "A", "quantity" -> 2, "price" -> 10.0),
            Map("product" -> "B", "quantity" -> 1, "price" -> 20.0)
          ),
          "total_price" -> 40.0
        ),
        Map(
          "id"          -> 2,
          "items"       -> Seq(Map("product" -> "C", "quantity" -> 3, "price" -> 5.0)),
          "total_price" -> 15.0
        )
      )
    )
  }

  // ---------------------------------------------------------------------------
  // WHERE — complex conditions
  // ---------------------------------------------------------------------------

  it should "support WHERE with complex logical and comparison operators" in {
    val sql =
      """SELECT id, name, age
        |FROM dql_users
        |WHERE (age > 20 AND profile.followers >= 100)
        |   OR (profile.city = 'Lyon' AND age < 50)
        |ORDER BY age DESC;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(
      res,
      Seq(
        Map("id" -> 1, "name" -> "Alice", "age" -> 30),
        Map("id" -> 3, "name" -> "Chloe", "age" -> 25)
      )
    )
  }

  // ---------------------------------------------------------------------------
  // ORDER BY + LIMIT + OFFSET
  // ---------------------------------------------------------------------------

  it should "support ORDER BY with multiple fields, LIMIT and OFFSET" in {
    val sql =
      """SELECT id, name, age
        |FROM dql_users
        |ORDER BY age DESC, name ASC
        |LIMIT 2 OFFSET 1;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(res)
  }

  // ---------------------------------------------------------------------------
  // GROUP BY + HAVING
  // ---------------------------------------------------------------------------

  it should "support GROUP BY with aggregations and HAVING" in {
    val sql =
      """SELECT profile.city AS city,
        |       COUNT(*) AS cnt,
        |       AVG(age) AS avg_age
        |FROM dql_users
        |GROUP BY profile.city
        |HAVING COUNT(*) >= 1
        |ORDER BY COUNT(*) DESC;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(res)
  }

  // ---------------------------------------------------------------------------
  // Arithmetic, IN, BETWEEN, IS NULL, LIKE, RLIKE
  // ---------------------------------------------------------------------------

  it should "support arithmetic, IN, BETWEEN, IS NULL, LIKE, RLIKE" in {
    val sql =
      """SELECT id,
        |       age + 10 AS age_plus_10,
        |       name
        |FROM dql_users
        |WHERE age BETWEEN 20 AND 50
        |  AND name IN ('Alice', 'Bob', 'Chloe')
        |  AND name IS NOT NULL
        |  AND (name LIKE 'A%' OR name RLIKE '.*o.*');""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(res)
  }

  // ---------------------------------------------------------------------------
  // CAST, TRY_CAST, SAFE_CAST, operator ::
  // ---------------------------------------------------------------------------

  it should "support CAST, TRY_CAST, SAFE_CAST and the :: operator" in {
    val sql =
      """SELECT id,
        |       age::BIGINT AS age_bigint,
        |       CAST(age AS DOUBLE) AS age_double,
        |       TRY_CAST('123' AS INT) AS try_cast_ok,
        |       SAFE_CAST('abc' AS INT) AS safe_cast_null
        |FROM dql_users;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(res)
  }

  // ---------------------------------------------------------------------------
  // Numeric + Trigonometric functions
  // ---------------------------------------------------------------------------

  it should "support numeric and trigonometric functions" in {
    val sql =
      """SELECT id,
        |       ABS(age) AS abs_age,
        |       CEIL(age) AS ceil_div,
        |       FLOOR(age) AS floor_div,
        |       ROUND(age, 2) AS round_div,
        |       SQRT(age) AS sqrt_age,
        |       POW(age, 2) AS pow_age,
        |       LOG(age) AS log_age,
        |       LOG10(age) AS log10_age,
        |       EXP(age) AS exp_age,
        |       SIN(age) AS sin_age,
        |       COS(age) AS cos_age,
        |       TAN(age) AS tan_age
        |FROM dql_users;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(res)
  }

  // ---------------------------------------------------------------------------
  // String functions
  // ---------------------------------------------------------------------------

  it should "support string functions" in {
    val sql =
      """SELECT id,
        |       CONCAT(name.raw, '_suffix') AS name_concat,
        |       SUBSTRING(name.raw, 1, 2) AS name_sub,
        |       LOWER(name.raw) AS name_lower,
        |       UPPER(name.raw) AS name_upper,
        |       TRIM(name.raw) AS name_trim,
        |       LENGTH(name.raw) AS name_len,
        |       REPLACE(name.raw, 'A', 'X') AS name_repl,
        |       LEFT(name.raw, 1) AS name_left,
        |       RIGHT(name.raw, 1) AS name_right,
        |       REVERSE(name.raw) AS name_rev
        |FROM dql_users
        |ORDER BY id ASC;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(
      res,
      Seq(
        Map(
          "id"          -> 1,
          "name_concat" -> Seq("Alice_suffix"),
          "name_sub"    -> Seq("Al"),
          "name_lower"  -> Seq("alice"),
          "name_upper"  -> Seq("ALICE"),
          "name_trim"   -> Seq("Alice"),
          "name_len"    -> Seq(5),
          "name_repl"   -> Seq("Xlice"),
          "name_left"   -> Seq("A"),
          "name_right"  -> Seq("e"),
          "name_rev"    -> Seq("ecilA")
        ),
        Map(
          "id"          -> 2,
          "name_concat" -> Seq("Bob_suffix"),
          "name_sub"    -> Seq("Bo"),
          "name_lower"  -> Seq("bob"),
          "name_upper"  -> Seq("BOB"),
          "name_trim"   -> Seq("Bob"),
          "name_len"    -> Seq(3),
          "name_repl"   -> Seq("Bob"),
          "name_left"   -> Seq("B"),
          "name_right"  -> Seq("b"),
          "name_rev"    -> Seq("boB")
        ),
        Map(
          "id"          -> 3,
          "name_concat" -> Seq("Chloe_suffix"),
          "name_sub"    -> Seq("Ch"),
          "name_lower"  -> Seq("chloe"),
          "name_upper"  -> Seq("CHLOE"),
          "name_trim"   -> Seq("Chloe"),
          "name_len"    -> Seq(5),
          "name_repl"   -> Seq("Chloe"),
          "name_left"   -> Seq("C"),
          "name_right"  -> Seq("e"),
          "name_rev"    -> Seq("eolhC")
        ),
        Map(
          "id"          -> 4,
          "name_concat" -> Seq("David_suffix"),
          "name_sub"    -> Seq("Da"),
          "name_lower"  -> Seq("david"),
          "name_upper"  -> Seq("DAVID"),
          "name_trim"   -> Seq("David"),
          "name_len"    -> Seq(5),
          "name_repl"   -> Seq("David"),
          "name_left"   -> Seq("D"),
          "name_right"  -> Seq("d"),
          "name_rev"    -> Seq("divaD")
        )
      )
    )
  }

  // ---------------------------------------------------------------------------
  // Date / Time functions
  // ---------------------------------------------------------------------------

  it should "support date and time functions" in {
    val sql =
      """SELECT id,
        |       YEAR(CURRENT_DATE) AS current_year,
        |       MONTH(CURRENT_DATE) AS current_month,
        |       DAY(CURRENT_DATE) AS current_day,
        |       WEEKDAY(CURRENT_DATE) AS current_weekday,
        |       YEARDAY(CURRENT_DATE) AS current_yearday,
        |       HOUR(CURRENT_TIMESTAMP) AS current_hour,
        |       MINUTE(CURRENT_TIMESTAMP) AS current_minute,
        |       SECOND(CURRENT_TIMESTAMP) AS current_second,
        |       NANOSECOND(CURRENT_TIMESTAMP) AS current_nano,
        |       MICROSECOND(CURRENT_TIMESTAMP) AS current_micro,
        |       MILLISECOND(CURRENT_TIMESTAMP) AS current_milli,
        |       YEAR(birthdate) AS year_b,
        |       MONTH(birthdate) AS month_b,
        |       DAY(birthdate) AS day_b,
        |       WEEKDAY(birthdate) AS weekday_b,
        |       YEARDAY(birthdate) AS yearday_b,
        |       HOUR(birthdate) AS hour_b,
        |       MINUTE(birthdate) AS minute_b,
        |       SECOND(birthdate) AS second_b,
        |       NANOSECOND(birthdate) AS nano_b,
        |       MICROSECOND(birthdate) AS micro_b,
        |       MILLISECOND(birthdate) AS milli_b,
        |       OFFSET_SECONDS(birthdate) AS epoch_day,
        |       DATE_TRUNC(birthdate, MONTH) AS trunc_month,
        |       DATE_ADD(birthdate, INTERVAL 1 DAY) AS plus_one_day,
        |       DATE_SUB(birthdate, INTERVAL 1 DAY) AS minus_one_day,
        |       DATE_DIFF(CURRENT_DATE, birthdate, YEAR) AS diff_years
        |FROM dql_users;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(res)
  }

  // ---------------------------------------------------------------------------
  // Geospatial functions
  // ---------------------------------------------------------------------------

  it should "support geospatial functions POINT and ST_DISTANCE" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dql_geo (
        |  id INT NOT NULL,
        |  location GEO_POINT,
        |  PRIMARY KEY (id)
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val table = assertShowTable(client.run("SHOW TABLE dql_geo").futureValue)
    table.ddl should include("location GEO_POINT")
    table.find("location").exists(_.dataType == SQLTypes.GeoPoint) shouldBe true

    val insert =
      """INSERT INTO dql_geo (id, location) VALUES
        |  (1, {lon = 2.3522, lat = 48.8566}),
        |  (2, {lon = 4.8357, lat = 45.7640});""".stripMargin

    assertDml(client.run(insert).futureValue, Some(DmlResult(inserted = 2)))

    val sql =
      """SELECT id,
        |       ST_DISTANCE(location, POINT(2.3522, 48.8566)) AS dist_paris
        |FROM dql_geo;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(res)
  }

  // ---------------------------------------------------------------------------
  // Window functions
  // ---------------------------------------------------------------------------

  it should "support window functions with OVER clause" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dql_sales (
        |  id INT NOT NULL,
        |  product KEYWORD,
        |  customer VARCHAR,
        |  amount DOUBLE,
        |  ts TIMESTAMP
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val insert =
      """INSERT INTO dql_sales (id, product, customer, amount, ts) VALUES
        |  (1, 'A', 'C1', 10.0, '2024-01-01T10:00:00Z'),
        |  (2, 'A', 'C2', 20.0, '2024-01-01T11:00:00Z'),
        |  (3, 'B', 'C1', 30.0, '2024-01-01T12:00:00Z'),
        |  (4, 'A', 'C3', 40.0, '2024-01-01T13:00:00Z');""".stripMargin

    assertDml(client.run(insert).futureValue, Some(DmlResult(inserted = 4)))

    val sql =
      """SELECT
        |  product,
        |  customer,
        |  amount,
        |  SUM(amount) OVER (PARTITION BY product) AS sum_per_product,
        |  COUNT(_id) OVER (PARTITION BY product) AS cnt_per_product,
        |  FIRST_VALUE(amount) OVER (PARTITION BY product ORDER BY ts ASC) AS first_amount,
        |  LAST_VALUE(amount) OVER (PARTITION BY product ORDER BY ts ASC) AS last_amount,
        |  ARRAY_AGG(amount) OVER (PARTITION BY product ORDER BY ts ASC LIMIT 10) AS amounts_array
        |FROM dql_sales
        |ORDER BY product, ts;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(
      res,
      Seq(
        Map(
          "product"         -> "A",
          "customer"        -> "C1",
          "amount"          -> 10.0,
          "sum_per_product" -> 70.0,
          "cnt_per_product" -> 3,
          "first_amount"    -> 10.0,
          "last_amount"     -> 40.0,
          "amounts_array"   -> Seq(10.0, 20.0, 40.0)
        ),
        Map(
          "product"         -> "A",
          "customer"        -> "C2",
          "amount"          -> 20.0,
          "sum_per_product" -> 70.0,
          "cnt_per_product" -> 3,
          "first_amount"    -> 10.0,
          "last_amount"     -> 40.0,
          "amounts_array"   -> Seq(10.0, 20.0, 40.0)
        ),
        Map(
          "product"         -> "A",
          "customer"        -> "C3",
          "amount"          -> 40.0,
          "sum_per_product" -> 70.0,
          "cnt_per_product" -> 3,
          "first_amount"    -> 10.0,
          "last_amount"     -> 40.0,
          "amounts_array"   -> Seq(10.0, 20.0, 40.0)
        ),
        Map(
          "product"         -> "B",
          "customer"        -> "C1",
          "amount"          -> 30.0,
          "sum_per_product" -> 30.0,
          "cnt_per_product" -> 1,
          "first_amount"    -> 30.0,
          "last_amount"     -> 30.0,
          "amounts_array"   -> Seq(30.0)
        )
      )
    )
  }

  // ===========================================================================
  // 5. PIPELINES — CREATE / ALTER / DROP / SHOW
  // ===========================================================================

  behavior of "PIPELINE statements"

  // ---------------------------------------------------------------------------
  // CREATE OR REPLACE PIPELINE with full processor set
  // ---------------------------------------------------------------------------

  it should "create or replace a pipeline with SET, SCRIPT, DATE_INDEX_NAME processors" in {
    val sql =
      """CREATE OR REPLACE PIPELINE user_pipeline WITH PROCESSORS (
        |    SET (
        |        field = "name",
        |        if = "ctx.name == null",
        |        description = "DEFAULT 'anonymous'",
        |        ignore_failure = true,
        |        value = "anonymous"
        |    ),
        |    SCRIPT (
        |        description = "age INT SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR))",
        |        lang = "painless",
        |        source = "def param1 = ctx.birthdate; def param2 = ZonedDateTime.ofInstant(Instant.ofEpochMilli(ctx['_ingest']['timestamp']), ZoneId.of('Z')).toLocalDate(); ctx.age = (param1 == null) ? null : Long.valueOf(ChronoUnit.YEARS.between(param1, param2))",
        |        ignore_failure = true
        |    ),
        |    SET (
        |        field = "ingested_at",
        |        if = "ctx.ingested_at == null",
        |        description = "DEFAULT _ingest.timestamp",
        |        ignore_failure = true,
        |        value = "_ingest.timestamp"
        |    ),
        |    SCRIPT (
        |        description = "profile.seniority INT SCRIPT AS (DATE_DIFF(profile.join_date, CURRENT_DATE, DAY))",
        |        lang = "painless",
        |        source = "def param1 = ctx.profile?.join_date; def param2 = ZonedDateTime.ofInstant(Instant.ofEpochMilli(ctx['_ingest']['timestamp']), ZoneId.of('Z')).toLocalDate(); ctx.profile.seniority = (param1 == null) ? null : Long.valueOf(ChronoUnit.DAYS.between(param1, param2))",
        |        ignore_failure = true
        |    ),
        |    DATE_INDEX_NAME (
        |        field = "birthdate",
        |        index_name_prefix = "users-",
        |        date_formats = ["yyyy-MM"],
        |        ignore_failure = true,
        |        date_rounding = "M",
        |        description = "PARTITION BY birthdate (MONTH)",
        |        separator = "-"
        |    ),
        |    SET (
        |        field = "_id",
        |        description = "PRIMARY KEY (id)",
        |        ignore_failure = false,
        |        ignore_empty_value = false,
        |        value = "{{id}}"
        |    )
        |);""".stripMargin

    assertDdl(client.run(sql).futureValue)

    val pipeline = assertShowPipeline(client.run("SHOW PIPELINE user_pipeline").futureValue)
    pipeline.name shouldBe "user_pipeline"
    pipeline.processors.size shouldBe 6
  }

  // ---------------------------------------------------------------------------
  // ALTER PIPELINE — ADD PROCESSOR, DROP PROCESSOR
  // ---------------------------------------------------------------------------

  it should "alter an existing pipeline by adding and dropping processors" in {
    val sql =
      """ALTER PIPELINE IF EXISTS user_pipeline (
        |    ADD PROCESSOR SET (
        |        field = "status",
        |        if = "ctx.status == null",
        |        description = "status DEFAULT 'active'",
        |        ignore_failure = true,
        |        value = "active"
        |    ),
        |    DROP PROCESSOR SET (_id)
        |);""".stripMargin

    assertDdl(client.run(sql).futureValue)
  }

  // ---------------------------------------------------------------------------
  // DROP PIPELINE
  // ---------------------------------------------------------------------------

  it should "drop a pipeline" in {
    val sql = "DROP PIPELINE IF EXISTS user_pipeline;"
    assertDdl(client.run(sql).futureValue)
  }

  // ===========================================================================
  // 6. ERRORS — parsing errors, unsupported SQL
  // ===========================================================================

  behavior of "SQL error handling"

  // ---------------------------------------------------------------------------
  // Parsing error
  // ---------------------------------------------------------------------------

  it should "return a parsing error for invalid SQL" in {
    val invalidSql = "CREAT TABL missing_keyword"
    val res = client.run(invalidSql).futureValue

    res.isFailure shouldBe true
    res.toEither.left.get.message should include("Error parsing schema DDL statement")
  }

  // ---------------------------------------------------------------------------
  // Unsupported SQL
  // ---------------------------------------------------------------------------

  it should "return an error for unsupported SQL statements" in {
    val unsupportedSql = "GRANT SELECT ON users TO user1"
    val res = client.run(unsupportedSql).futureValue

    res.isFailure shouldBe true
    res.toEither.left.get.message should include("Error parsing schema DDL statement")
  }

}
