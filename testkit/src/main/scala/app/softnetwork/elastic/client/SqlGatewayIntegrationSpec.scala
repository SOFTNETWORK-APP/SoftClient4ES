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
import app.softnetwork.elastic.client.result.{
  DdlResult,
  DmlResult,
  ElasticResult,
  QueryResult,
  QueryRows,
  QueryStream,
  QueryStructured,
  QueryTable
}
import app.softnetwork.elastic.scalatest.ElasticTestKit
import app.softnetwork.elastic.sql.schema.Table
import app.softnetwork.persistence.generateUUID
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}
import java.util.concurrent.TimeUnit

// ---------------------------------------------------------------------------
// Base test trait — to be mixed with ElasticDockerTestKit
// ---------------------------------------------------------------------------

trait SqlGatewayIntegrationSpec extends AnyFlatSpecLike with Matchers with ScalaFutures {
  self: ElasticTestKit =>

  lazy val log: Logger = LoggerFactory getLogger getClass.getName

  implicit val system: ActorSystem = ActorSystem(generateUUID())
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(30, Seconds))

  // Provided by concrete test class
  def client: SqlGateway

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

  def assertSelectResult(res: ElasticResult[QueryResult]): Unit = {
    res.isSuccess shouldBe true
    res.toOption.get match {
      case QueryStream(_)     => succeed
      case QueryStructured(_) => succeed
      case QueryRows(_)       => succeed
      case other              => fail(s"Unexpected QueryResult type for SELECT: $other")
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

  def assertDml(res: ElasticResult[QueryResult]): Unit = {
    res.isSuccess shouldBe true
    res.toOption.get shouldBe a[DmlResult]
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
    ddl should include("CREATE TABLE show_users")
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
    val ddl = table.ddl

    ddl should include("CREATE TABLE users")
    ddl should include("id INT NOT NULL")
    ddl should include("name VARCHAR")
    ddl should include("DEFAULT 'anonymous'")
    ddl should include("SCRIPT AS (DATEDIFF")
    ddl should include("STRUCT FIELDS")
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
        |  balance DECIMAL(10,2)
        |) WITH (
        |  number_of_shards = 1,
        |  number_of_replicas = 0
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
        |  active BOOLEAN
        |);""".stripMargin

    assertDdl(client.run(createSource).futureValue)

    val insertSource =
      """INSERT INTO accounts_src (id, name, active) VALUES
        |  (1, 'Alice', true),
        |  (2, 'Bob',   false),
        |  (3, 'Chloe', true);""".stripMargin

    assertDml(client.run(insertSource).futureValue)

    val createOrReplace =
      """CREATE OR REPLACE TABLE users_cr AS
        |SELECT id, name FROM accounts_src WHERE active = true;""".stripMargin

    assertDdl(client.run(createOrReplace).futureValue)

    // Vérification via SHOW TABLE
    val table = assertShowTable(client.run("SHOW TABLE users_cr").futureValue)
    table.ddl should include("CREATE TABLE users_cr")
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
  }

  // ---------------------------------------------------------------------------
  // ALTER TABLE — NOT NULL
  // ---------------------------------------------------------------------------

  it should "set and drop NOT NULL on a column" in {
    val create =
      """CREATE TABLE IF NOT EXISTS users_alter6 (
        |  id INT NOT NULL,
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
        |  ALTER COLUMN status SET DATA TYPE BIGINT;""".stripMargin

    assertDdl(client.run(alter).futureValue)

    val table = assertShowTable(client.run("SHOW TABLE users_alter7").futureValue)
    table.ddl should include("status BIGINT")
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
        |  owner VARCHAR,
        |  balance DECIMAL(10,2)
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
        |SET balance = balance + 25
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
        |  level VARCHAR,
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
        |SET value = value * 2
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
        |  name VARCHAR,
        |  age INT,
        |  birthdate DATE,
        |  profile STRUCT FIELDS(
        |    city VARCHAR,
        |    followers INT
        |  )
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val insert =
      """INSERT INTO dql_users (id, name, age, birthdate, profile) VALUES
        |  (1, 'Alice', 30, '1994-01-01', { "city": "Paris", "followers": 100 }),
        |  (2, 'Bob',   40, '1984-05-10', { "city": "Lyon",  "followers": 50  }),
        |  (3, 'Chloe', 25, '1999-07-20', { "city": "Paris", "followers": 200 }),
        |  (4, 'David', 50, '1974-03-15', { "city": "Marseille", "followers": 10 });
        |""".stripMargin

    assertDml(client.run(insert).futureValue)
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
    assertSelectResult(res)
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
    assertSelectResult(res)
  }

  // ---------------------------------------------------------------------------
  // JOIN + UNNEST
  // ---------------------------------------------------------------------------

  it should "execute a JOIN with UNNEST on array of structs" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dql_orders (
        |  id INT NOT NULL,
        |  customer_id INT,
        |  items ARRAY<STRUCT FIELDS(
        |    product VARCHAR,
        |    quantity INT,
        |    price DOUBLE
        |  )>
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val insert =
      """INSERT INTO dql_orders (id, customer_id, items) VALUES
        |  (1, 1, [ { "product": "A", "quantity": 2, "price": 10.0 },
        |           { "product": "B", "quantity": 1, "price": 20.0 } ]),
        |  (2, 2, [ { "product": "C", "quantity": 3, "price": 5.0 } ]);""".stripMargin

    assertDml(client.run(insert).futureValue)

    val sql =
      """SELECT o.id, i.product, i.quantity
        |FROM dql_orders o
        |JOIN UNNEST(o.items) AS i
        |ORDER BY o.id ASC;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(res)
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
    assertSelectResult(res)
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
        |ORDER BY cnt DESC;""".stripMargin

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
        |       CEIL(age / 3.0) AS ceil_div,
        |       FLOOR(age / 3.0) AS floor_div,
        |       ROUND(age / 3.0, 2) AS round_div,
        |       SQRT(age) AS sqrt_age,
        |       POWER(age, 2) AS pow_age,
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
        |       CONCAT(name, '_suffix') AS name_concat,
        |       SUBSTRING(name, 1, 2) AS name_sub,
        |       LOWER(name) AS name_lower,
        |       UPPER(name) AS name_upper,
        |       TRIM(name) AS name_trim,
        |       LENGTH(name) AS name_len,
        |       REPLACE(name, 'A', 'X') AS name_repl,
        |       LEFT(name, 1) AS name_left,
        |       RIGHT(name, 1) AS name_right,
        |       REVERSE(name) AS name_rev
        |FROM dql_users;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(res)
  }

  // ---------------------------------------------------------------------------
  // Date / Time functions
  // ---------------------------------------------------------------------------

  it should "support date and time functions" in {
    val sql =
      """SELECT id,
        |       YEAR(birthdate) AS year_b,
        |       MONTH(birthdate) AS month_b,
        |       DAY(birthdate) AS day_b,
        |       DATE_ADD(birthdate, INTERVAL 1 DAY) AS plus_one_day,
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
        |  lon DOUBLE,
        |  lat DOUBLE
        |);""".stripMargin

    assertDdl(client.run(create).futureValue)

    val insert =
      """INSERT INTO dql_geo (id, lon, lat) VALUES
        |  (1, 2.3522, 48.8566),
        |  (2, 4.8357, 45.7640);""".stripMargin

    assertDml(client.run(insert).futureValue)

    val sql =
      """SELECT id,
        |       POINT(lon, lat) AS point,
        |       ST_DISTANCE(POINT(lon, lat), POINT(2.3522, 48.8566)) AS dist_paris
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
        |  product VARCHAR,
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

    assertDml(client.run(insert).futureValue)

    val sql =
      """SELECT
        |  product,
        |  customer,
        |  amount,
        |  SUM(amount) OVER (PARTITION BY product) AS sum_per_product,
        |  COUNT(*) OVER (PARTITION BY product) AS cnt_per_product,
        |  FIRST_VALUE(amount) OVER (PARTITION BY product ORDER BY ts ASC) AS first_amount,
        |  LAST_VALUE(amount) OVER (PARTITION BY product ORDER BY ts ASC) AS last_amount,
        |  ARRAY_AGG(amount) OVER (PARTITION BY product ORDER BY ts ASC LIMIT 10) AS amounts_array
        |FROM dql_sales
        |ORDER BY product, ts;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(res)
  }

  // ===========================================================================
  // 5. PIPELINES — CREATE / ALTER / DROP
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
        |        source = "def p1 = ctx.birthdate; def p2 = ZonedDateTime.now(ZoneId.of('Z')).toLocalDate(); ctx.age = (p1 == null) ? null : ChronoUnit.YEARS.between(p1, p2)",
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
        |        source = "def p1 = ctx.profile?.join_date; def p2 = ZonedDateTime.now(ZoneId.of('Z')).toLocalDate(); ctx.profile.seniority = (p1 == null) ? null : ChronoUnit.DAYS.between(p1, p2)",
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
  // 6. TEMPLATES — CREATE / ALTER / DROP
  // ===========================================================================

  behavior of "TEMPLATE statements"

  it should "create, alter and drop index templates via SQL if supported" in {
    val create =
      """CREATE TEMPLATE logs_template
        |PATTERN 'logs-*'
        |WITH (
        |  number_of_shards = 1,
        |  number_of_replicas = 1
        |);""".stripMargin

    val createRes = client.run(create).futureValue

    // Certains backends Elasticsearch ne supportent pas les templates via SQL
    if (createRes.isSuccess) {
      assertDdl(createRes)

      val alter =
        """ALTER TEMPLATE logs_template
          |SET ( number_of_replicas = 2 );""".stripMargin

      assertDdl(client.run(alter).futureValue)

      val drop = "DROP TEMPLATE logs_template;"
      assertDdl(client.run(drop).futureValue)

    } else {
      log.warn("Templates not supported by this backend, skipping template tests.")
    }
  }

  // ===========================================================================
  // 7. ERRORS — parsing errors, unsupported SQL
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
    res.toEither.left.get.message should include("Unsupported SQL statement")
  }

}
