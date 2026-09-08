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

import app.softnetwork.elastic.client.result.{DmlResult, ElasticSuccess}
import app.softnetwork.elastic.scalatest.ElasticTestKit
import app.softnetwork.elastic.sql.{DoubleValue, IdValue}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.health.HealthStatus
import app.softnetwork.elastic.sql.policy.EnrichPolicyTaskStatus

import java.time.LocalDate

// ---------------------------------------------------------------------------
// Base test trait — to be mixed with ElasticDockerTestKit
// ---------------------------------------------------------------------------

trait GatewayApiIntegrationSpec extends GatewayIntegrationTestKit {
  self: ElasticTestKit =>

  // -------------------------------------------------------------------------
  // SHOW / DESCRIBE TABLE tests
  // -------------------------------------------------------------------------

  behavior of "SHOW / DESCRIBE TABLE"

  it should "return the table schema using SHOW TABLE" in {
    val create =
      """CREATE TABLE IF NOT EXISTS show_users (
        |  id INT NOT NULL,
        |  name VARCHAR FIELDS(
        |    raw KEYWORD
        |  ) OPTIONS (fielddata = true),
        |  age INT DEFAULT 0,
        |  PRIMARY KEY (id)
        |);""".stripMargin

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val show = client.run("SHOW TABLE show_users").futureValue
    val table = assertShowTable(System.nanoTime(), show)

    val showCreate = client.run("SHOW CREATE TABLE show_users").futureValue
    val sql = assertShowCreate(System.nanoTime(), showCreate)
    sql should include("CREATE OR REPLACE TABLE show_users")

    val ddl = table.ddl
    ddl should include("CREATE OR REPLACE TABLE show_users")
    ddl should include("id INT NOT NULL")
    ddl should include("name VARCHAR")
    ddl should include("age INT DEFAULT 0")
    ddl should include("PRIMARY KEY (id)")

    var rows =
      assertQueryRows(System.nanoTime(), client.run("SHOW TABLES LIKE 'show_%'").futureValue)
    rows.size should be >= 1
    rows.exists(_("name") == "show_users") shouldBe true

    rows = assertQueryRows(System.nanoTime(), client.run("SHOW TABLES LIKE '.%'").futureValue)
    rows.size shouldBe 0
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
        |  ),
        |  PRIMARY KEY (id)
        |);""".stripMargin

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val rows =
      assertQueryRows(System.nanoTime(), client.run("DESCRIBE TABLE desc_users").futureValue)
    rows.size shouldBe 6
    rows.exists(row =>
      row("Field") == "id" &&
      row("Null") == "no" &&
      row("Key") == "PRI"
    ) shouldBe true
    rows.exists(_("Field") == "name") shouldBe true
    rows.exists(_("Field") == "profile.city") shouldBe true
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

    assertDdl(System.nanoTime(), client.run(sql).futureValue)

    // Vérification via SHOW TABLE
    val table = assertShowTable(System.nanoTime(), client.run("SHOW TABLE users").futureValue)
    val ddl = table.ddl.replaceAll("\\s+", " ")

    ddl should include("CREATE OR REPLACE TABLE users")
    ddl should include("id INT NOT NULL COMMENT 'user identifier'")
    ddl should include(
      """name VARCHAR FIELDS ( raw KEYWORD COMMENT 'sortable' ) DEFAULT 'anonymous' OPTIONS (analyzer = "french", search_analyzer = "french")"""
    )
    // 🔴 DATE — what the user declared. `SHOW TABLE` reports the DECLARED type, not the runtime
    // one; story 21.5 briefly reported TIMESTAMP here and that user-visible change was a symptom
    // of conflating the two facts. The runtime type now lives in `SQLTypeUtils.runtimeType`,
    // reached only through `GenericIdentifier.baseType`, so no release note is owed.
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

    assertDdl(System.nanoTime(), client.run(sql).futureValue)
    assertDdl(System.nanoTime(), client.run(sql).futureValue) // second call should succeed
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

    assertDdl(System.nanoTime(), client.run(createSource).futureValue)

    val insertSource =
      """INSERT INTO accounts_src (id, name, active) VALUES
        | (1, 'Alice', true),
        | (2, 'Bob',   false),
        | (3, 'Chloe', true);""".stripMargin

    assertDml(System.nanoTime(), client.run(insertSource).futureValue)

    val createOrReplace =
      "CREATE OR REPLACE TABLE users_cr AS SELECT id, name FROM accounts_src WHERE active = true;"

    assertDml(System.nanoTime(), client.run(createOrReplace).futureValue)

    // Vérification via SHOW TABLE
    val table = assertShowTable(System.nanoTime(), client.run("SHOW TABLE users_cr").futureValue)
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val drop =
      """DROP TABLE IF EXISTS tmp_drop;""".stripMargin

    assertDdl(System.nanoTime(), client.run(drop).futureValue)
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val insert =
      """INSERT INTO tmp_truncate (id, value) VALUES
        |  (1, 'a'),
        |  (2, 'b'),
        |  (3, 'c');""".stripMargin

    assertDml(System.nanoTime(), client.run(insert).futureValue)

    val truncate = "TRUNCATE TABLE tmp_truncate;"
    assertDdl(System.nanoTime(), client.run(truncate).futureValue)

    // Vérification : SELECT doit renvoyer 0 lignes
    val select = client.run("SELECT * FROM tmp_truncate").futureValue
    assertSelectResult(System.nanoTime(), select)
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val alter =
      """ALTER TABLE users_alter1
        |  ADD COLUMN IF NOT EXISTS age INT DEFAULT 0;""".stripMargin

    assertDdl(System.nanoTime(), client.run(alter).futureValue)

    val table =
      assertShowTable(System.nanoTime(), client.run("SHOW TABLE users_alter1").futureValue)
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val alter =
      """ALTER TABLE users_alter2
        |  RENAME COLUMN name TO full_name;""".stripMargin

    assertDdl(System.nanoTime(), client.run(alter).futureValue)

    val table =
      assertShowTable(System.nanoTime(), client.run("SHOW TABLE users_alter2").futureValue)
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val setComment =
      """ALTER TABLE users_alter3
        |  ALTER COLUMN IF EXISTS status SET COMMENT 'a description';""".stripMargin

    val dropComment =
      """ALTER TABLE users_alter3
        |  ALTER COLUMN IF EXISTS status DROP COMMENT;""".stripMargin

    assertDdl(System.nanoTime(), client.run(setComment).futureValue)
    assertDdl(System.nanoTime(), client.run(dropComment).futureValue)

    val table =
      assertShowTable(System.nanoTime(), client.run("SHOW TABLE users_alter3").futureValue)
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val setDefault =
      """ALTER TABLE users_alter4
        |  ALTER COLUMN status SET DEFAULT 'active';""".stripMargin

    val dropDefault =
      """ALTER TABLE users_alter4
        |  ALTER COLUMN status DROP DEFAULT;""".stripMargin

    assertDdl(System.nanoTime(), client.run(setDefault).futureValue)
    assertDdl(System.nanoTime(), client.run(dropDefault).futureValue)

    val table =
      assertShowTable(System.nanoTime(), client.run("SHOW TABLE users_alter4").futureValue)
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val alter =
      """ALTER TABLE users_alter5
        |  ALTER COLUMN profile SET FIELDS (
        |    bio VARCHAR,
        |    followers INT,
        |    join_date DATE,
        |    seniority INT SCRIPT AS (DATEDIFF(profile.join_date, CURRENT_DATE, DAY)),
        |    reputation DOUBLE DEFAULT 0.0
        |  );""".stripMargin

    assertDdl(System.nanoTime(), client.run(alter).futureValue)

    val table =
      assertShowTable(System.nanoTime(), client.run("SHOW TABLE users_alter5").futureValue)
    table.find("profile.reputation") match {
      case Some(col) =>
        col.dataType shouldBe SQLTypes.Double
        col.defaultValue shouldBe Some(DoubleValue(0.0))
      case _ => fail("Column 'profile.reputation' not found")
    }
    table.defaultPipeline.processors.size shouldBe 3 // added processor to create an artificial primary key
    table.find("users_alter5_id") match {
      case Some(col) =>
        col.dataType shouldBe SQLTypes.Keyword
        col.defaultValue shouldBe Some(IdValue)
        col.processors.size shouldBe 1
      case _ => fail("Column 'users_alter5_id' not found")
    }
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val setNotNull =
      """ALTER TABLE users_alter6
        |  ALTER COLUMN status SET NOT NULL;""".stripMargin

    val dropNotNull =
      """ALTER TABLE users_alter6
        |  ALTER COLUMN status DROP NOT NULL;""".stripMargin

    assertDdl(System.nanoTime(), client.run(setNotNull).futureValue)
    assertDdl(System.nanoTime(), client.run(dropNotNull).futureValue)

    val table =
      assertShowTable(System.nanoTime(), client.run("SHOW TABLE users_alter6").futureValue)
    table.find("status") match {
      case Some(col) => col.nullable shouldBe true
      case _         => fail("Column 'status' not found")
    }
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val alter =
      """ALTER TABLE users_alter7
        |  ALTER COLUMN id SET DATA TYPE BIGINT;""".stripMargin

    assertDdl(System.nanoTime(), client.run(alter).futureValue)

    val table =
      assertShowTable(System.nanoTime(), client.run("SHOW TABLE users_alter7").futureValue)
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

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

    assertDdl(System.nanoTime(), client.run(alter).futureValue)

    val table =
      assertShowTable(System.nanoTime(), client.run("SHOW TABLE users_alter8").futureValue)

    table.ddl should include("age INT DEFAULT 0")
    table.ddl should include("full_name VARCHAR")
    table.ddl should include("status VARCHAR DEFAULT 'active'")
    table.ddl should include("description VARCHAR DEFAULT 'N/A'")
    table.ddl should include("visibility BOOLEAN DEFAULT true")
  }

  it should "list all tables" in {
    val tables = assertQueryRows(System.nanoTime(), client.run("SHOW TABLES").futureValue)
    tables should not be empty
    for {
      table <- tables
    } {
      table should contain key "name"
      table should contain key "type"
    }
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val insert =
      """INSERT INTO dml_users (id, name, age) VALUES
        |  (1, 'Alice', 30),
        |  (2, 'Bob',   40),
        |  (3, 'Chloe', 25);""".stripMargin

    val res = client.run(insert).futureValue
    assertDml(System.nanoTime(), res)

    val dml = res.toOption.get.asInstanceOf[DmlResult]
    dml.inserted shouldBe 3

    // Vérification via SELECT
    val select = client.run("SELECT * FROM dml_users ORDER BY id ASC").futureValue
    assertSelectResult(System.nanoTime(), select)
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val insert =
      """INSERT INTO dml_accounts (id, owner, balance) VALUES
        |  (1, 'Alice', 100.00),
        |  (2, 'Bob',   50.00),
        |  (3, 'Chloe', 75.00);""".stripMargin

    assertDml(System.nanoTime(), client.run(insert).futureValue)

    val update =
      """UPDATE dml_accounts
        |SET balance = 125
        |WHERE owner = 'Alice';""".stripMargin

    val res = client.run(update).futureValue
    assertDml(System.nanoTime(), res)

    val dml = res.toOption.get.asInstanceOf[DmlResult]
    dml.updated should be >= 1L

    // Vérification via SELECT
    val select =
      """SELECT owner, balance
        |FROM dml_accounts
        |WHERE owner = 'Alice';""".stripMargin

    val q = client.run(select).futureValue
    assertSelectResult(System.nanoTime(), q)
  }

  // ---------------------------------------------------------------------------
  // UPDATE with scripts
  // ---------------------------------------------------------------------------

  it should "update rows using scripted SET clauses and return a DmlResult" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dml_products (
        |  id INT NOT NULL,
        |  name KEYWORD,
        |  price DOUBLE,
        |  category KEYWORD,
        |  updated_at DATETIME
        |);""".stripMargin

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val insert =
      """INSERT INTO dml_products (id, name, price, category) VALUES
        |  (1, 'Laptop',  1000.00, 'Electronics'),
        |  (2, 'T-Shirt',   25.00, 'Clothing'),
        |  (3, 'Phone',    800.00, 'Electronics');""".stripMargin

    assertDml(System.nanoTime(), client.run(insert).futureValue)

    val update =
      """UPDATE dml_products
        |SET price = price * 1.1, updated_at = CURRENT_TIMESTAMP
        |WHERE category = 'Electronics';""".stripMargin

    val res = client.run(update).futureValue
    assertDml(System.nanoTime(), res)

    val dml = res.toOption.get.asInstanceOf[DmlResult]
    dml.updated should be >= 1L

    // Verify via SELECT that prices were increased and updated_at is set
    val select =
      """SELECT id, price, updated_at
        |FROM dml_products
        |WHERE category = 'Electronics'
        |ORDER BY id ASC;""".stripMargin

    val rows = collectRows(System.nanoTime(), client.run(select).futureValue)
    rows should have size 2

    // id=1 Laptop: 1000.0 * 1.1 = 1100.0 — id=3 Phone: 800.0 * 1.1 = 880.0
    rows.zipWithIndex.foreach { case (row, i) =>
      val price = row("price") match {
        case d: Double => d
        case n: Number => n.doubleValue()
      }
      val originalPrice = if (i == 0) 1000.0 else 800.0
      price shouldBe (originalPrice * 1.1) +- 0.01

      // updated_at must be set by the CURRENT_TIMESTAMP script
      row.get("updated_at") shouldBe defined
    }
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val insert =
      """INSERT INTO dml_logs (id, level, message) VALUES
        |  (1, 'INFO',  'started'),
        |  (2, 'ERROR', 'failed'),
        |  (3, 'INFO',  'running');""".stripMargin

    assertDml(System.nanoTime(), client.run(insert).futureValue)

    val delete =
      """DELETE FROM dml_logs
        |WHERE level = 'ERROR';""".stripMargin

    val res = client.run(delete).futureValue
    assertDml(System.nanoTime(), res)

    val dml = res.toOption.get.asInstanceOf[DmlResult]
    dml.deleted shouldBe 1L

    // Vérification via SELECT
    val select =
      """SELECT id, level
        |FROM dml_logs
        |ORDER BY id ASC;""".stripMargin

    val q = client.run(select).futureValue
    assertSelectResult(System.nanoTime(), q)
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val insert =
      """INSERT INTO dml_chain (id, value) VALUES
        |  (1, 10),
        |  (2, 20),
        |  (3, 30);""".stripMargin

    assertDml(System.nanoTime(), client.run(insert).futureValue)

    val update =
      """UPDATE dml_chain
        |SET value = 50
        |WHERE id IN (1, 3);""".stripMargin

    assertDml(System.nanoTime(), client.run(update).futureValue)

    val delete =
      """DELETE FROM dml_chain
        |WHERE value > 40;""".stripMargin

    assertDml(System.nanoTime(), client.run(delete).futureValue)

    // Vérification finale
    val select = client.run("SELECT * FROM dml_chain ORDER BY id ASC").futureValue
    assertSelectResult(System.nanoTime(), select)
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

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
    assertDml(System.nanoTime(), jsonlResult, Some(DmlResult(inserted = persons.size)))

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
    assertDml(System.nanoTime(), arrayResult, Some(DmlResult(inserted = persons.size)))

    // 7. Final verification: SELECT all documents
    val select = client.run("SELECT * FROM copy_into_test ORDER BY uuid ASC").futureValue
    assertSelectResult(
      System.nanoTime(),
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val insert =
      """INSERT INTO dql_users (id, name, age, birthdate, profile) VALUES
        |  (1, 'Alice', 30, '1994-01-01', {city = "Paris", followers = 100}),
        |  (2, 'Bob',   40, '1984-05-10', {city = "Lyon",  followers = 50}),
        |  (3, 'Chloe', 25, '1999-07-20', {city = "Paris", followers = 200}),
        |  (4, 'David', 50, '1974-03-15', {city = "Marseille", followers = 10});
        |""".stripMargin

    assertDml(System.nanoTime(), client.run(insert).futureValue, Some(DmlResult(inserted = 4)))
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
      System.nanoTime(),
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
      System.nanoTime(),
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val table = assertShowTable(System.nanoTime(), client.run("SHOW TABLE dql_orders").futureValue)
    table.ddl should include("items ARRAY<STRUCT> FIELDS")

    val insert =
      """INSERT INTO dql_orders (id, customer_id, items) VALUES
        |  (1, 1, [ { product = "A", quantity = 2, price = 10.0 },
        |           { product = "B", quantity = 1, price = 20.0 } ]),
        |  (2, 2, [ { product = "C", quantity = 3, price = 5.0 } ]);""".stripMargin

    assertDml(System.nanoTime(), client.run(insert).futureValue, Some(DmlResult(inserted = 2)))

    val sql =
      """SELECT
        | o.id,
        | oi.product,
        | oi.quantity,
        | SUM(oi.price * oi.quantity) OVER (PARTITION BY o.id) AS total_price
        |FROM dql_orders o
        |JOIN UNNEST(o.items) AS oi
        |WHERE oi.quantity >= 1
        |ORDER BY o.id ASC;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(
      System.nanoTime(),
      res,
      Seq(
        Map(
          "id"          -> 1,
          "oi.product"  -> "A",
          "oi.quantity" -> 2,
          "total_price" -> 40.0
        ),
        Map(
          "id"          -> 1,
          "oi.product"  -> "B",
          "oi.quantity" -> 1,
          "total_price" -> 40.0
        ),
        Map(
          "id"          -> 2,
          "oi.product"  -> "C",
          "oi.quantity" -> 3,
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
      System.nanoTime(),
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
    assertSelectResult(System.nanoTime(), res)
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
    assertSelectResult(System.nanoTime(), res)
  }

  // ---------------------------------------------------------------------------
  // Issues #54 / #223 (BIDC-2) — buckets_path and aggregation naming, bucket-pipeline scripts.
  // A dedicated table: `Nice` has a single user with NO age, so every MAX/MIN over `age` is
  // missing for that bucket — the null-metric contract (AC 4b) needs such a bucket, and adding it
  // to `dql_users` would move every 4-row oracle above.
  // ---------------------------------------------------------------------------

  it should "prepare the aggregation-naming test data (issues #54 / #223)" in {
    val create =
      """CREATE TABLE IF NOT EXISTS having_naming (
        |  id INT NOT NULL,
        |  city KEYWORD,
        |  name KEYWORD,
        |  age INT,
        |  birthdate DATE
        |);""".stripMargin
    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val insert =
      """INSERT INTO having_naming (id, city, name, age, birthdate) VALUES
        |  (1, 'Paris',     'Alice', 30, '1994-01-01'),
        |  (2, 'Lyon',      'Bob',   40, '1984-05-10'),
        |  (3, 'Paris',     'Chloe', 25, '1999-07-20'),
        |  (4, 'Marseille', 'David', 50, '1974-03-15');""".stripMargin
    assertDml(System.nanoTime(), client.run(insert).futureValue, Some(DmlResult(inserted = 4)))

    val insertNoAge =
      """INSERT INTO having_naming (id, city, name, birthdate) VALUES
        |  (5, 'Nice', 'Eve', '2000-01-01');""".stripMargin
    assertDml(System.nanoTime(), client.run(insertNoAge).futureValue, Some(DmlResult(inserted = 1)))
  }

  it should "filter on an un-aliased HAVING COUNT(field) exactly like the aliased form (issue #54)" in {
    val expected = Seq(Map("city" -> "Paris", "cnt" -> 2))
    val aliased =
      """SELECT city, COUNT(name) AS cnt FROM having_naming
        |GROUP BY city HAVING cnt > 1;""".stripMargin
    assertSelectResult(System.nanoTime(), client.run(aliased).futureValue, expected)

    val unaliased =
      """SELECT city, COUNT(name) AS cnt FROM having_naming
        |GROUP BY city HAVING COUNT(name) > 1;""".stripMargin
    assertSelectResult(System.nanoTime(), client.run(unaliased).futureValue, expected)

    // The aggregate exists only in HAVING: it is created for the filter and hidden from the rows.
    val havingOnly =
      """SELECT city FROM having_naming
        |GROUP BY city HAVING COUNT(name) > 1;""".stripMargin
    assertSelectResult(
      System.nanoTime(),
      client.run(havingOnly).futureValue,
      Seq(Map("city" -> "Paris"))
    )
  }

  it should "keep two aggregates over the same field apart in HAVING (issue #54)" in {
    // COUNT(age) >= 1 holds for every city but Nice; only Marseille has MAX(age) > 45. Before the
    // fix both aggregates were named `age`, the second was dropped, and the count stood in for the
    // max — `1 >= 1 && 1 > 45` — so no city qualified.
    val sql =
      """SELECT city FROM having_naming
        |GROUP BY city HAVING COUNT(age) >= 1 AND MAX(age) > 45;""".stripMargin
    assertSelectResult(
      System.nanoTime(),
      client.run(sql).futureValue,
      Seq(Map("city" -> "Marseille"))
    )
  }

  it should "filter on HAVING over a transformed aggregate (issue #223)" in {
    // MAX(YEAR(birthdate)): Paris 1999, Lyon 1984, Marseille 1974, Nice 2000.
    val year =
      """SELECT city, COUNT(*) AS cnt FROM having_naming
        |GROUP BY city HAVING MAX(YEAR(birthdate)) > 1990;""".stripMargin
    assertSelectResult(
      System.nanoTime(),
      client.run(year).futureValue,
      Seq(Map("city" -> "Paris", "cnt" -> 2), Map("city" -> "Nice", "cnt" -> 1))
    )

    // OR across a transformed aggregate and a count: Paris by count, Marseille by MAX(ABS(age));
    // Nice has no age, so its MAX(ABS(age)) is missing and its count is 1 — it must NOT pass.
    val or =
      """SELECT city FROM having_naming
        |GROUP BY city HAVING MAX(ABS(age)) > 45 OR COUNT(*) > 1;""".stripMargin
    assertSelectResult(
      System.nanoTime(),
      client.run(or).futureValue,
      Seq(Map("city" -> "Paris"), Map("city" -> "Marseille"))
    )
  }

  it should "sort buckets by an aggregate over a transform (issue #223)" in {
    // MAX(ABS(age)): Marseille 50, Lyon 40, Paris 30 — distinct values, so the order is exact;
    // Nice's is missing and sorts last.
    val sql =
      """SELECT city FROM having_naming
        |GROUP BY city ORDER BY MAX(ABS(age)) DESC;""".stripMargin
    val rows = collectRows(System.nanoTime(), client.run(sql).futureValue)
    rows.map(_("city")) shouldBe Seq("Marseille", "Lyon", "Paris", "Nice")
  }

  it should "never let a bucket whose compared metric is missing pass a HAVING comparison (AC 4b)" in {
    // Nice's MAX(age) has no value. Whatever the runtime hands the bucket_selector for it, the
    // guarded script yields false: the bucket is dropped by BOTH directions of the comparison, and
    // the request does not fail.
    val expected = Seq(Map("city" -> "Paris"), Map("city" -> "Lyon"), Map("city" -> "Marseille"))
    val above =
      """SELECT city FROM having_naming
        |GROUP BY city HAVING MAX(age) > 0;""".stripMargin
    assertSelectResult(System.nanoTime(), client.run(above).futureValue, expected)
    val below =
      """SELECT city FROM having_naming
        |GROUP BY city HAVING MAX(age) < 1000;""".stripMargin
    assertSelectResult(System.nanoTime(), client.run(below).futureValue, expected)
  }

  it should "compute arithmetic over aggregates as a bucket_script (issue #54)" in {
    // The operands are created as their own (hidden) aggregations and read as params; the value is
    // a double, as every bucket_script result is.
    val range =
      """SELECT city, MAX(age) - MIN(age) AS age_range FROM having_naming
        |WHERE age IS NOT NULL
        |GROUP BY city ORDER BY city ASC;""".stripMargin
    assertSelectResult(
      System.nanoTime(),
      client.run(range).futureValue,
      Seq(
        Map("city" -> "Lyon", "age_range"      -> 0.0),
        Map("city" -> "Marseille", "age_range" -> 0.0),
        Map("city" -> "Paris", "age_range"     -> 5.0)
      )
    )

    // An operand that is also a SELECT item resolves to that item's alias — one aggregation, two uses.
    val shared =
      """SELECT city, MAX(age) AS oldest, MAX(age) - MIN(age) AS age_range FROM having_naming
        |WHERE age IS NOT NULL
        |GROUP BY city ORDER BY city ASC;""".stripMargin
    assertSelectResult(
      System.nanoTime(),
      client.run(shared).futureValue,
      Seq(
        Map("city" -> "Lyon", "oldest"      -> 40.0, "age_range" -> 0.0),
        Map("city" -> "Marseille", "oldest" -> 50.0, "age_range" -> 0.0),
        Map("city" -> "Paris", "oldest"     -> 30.0, "age_range" -> 5.0)
      )
    )
  }

  it should "filter on a bucket_script through its alias (R2-1)" in {
    // The bucket_selector reads the sibling pipeline aggregation by name; only Paris (30 - 25 = 5)
    // clears 3. Before the fix the alias was a bare name, the selector was dropped, all came back.
    val sql =
      """SELECT city, MAX(age) - MIN(age) AS age_range FROM having_naming
        |WHERE age IS NOT NULL
        |GROUP BY city HAVING age_range > 3;""".stripMargin
    assertSelectResult(
      System.nanoTime(),
      client.run(sql).futureValue,
      Seq(Map("city" -> "Paris", "age_range" -> 5.0))
    )
  }

  it should "apply BETWEEN, IN and NOT to aggregates in HAVING (AC 4b, R2-2, R2-3)" in {
    // COUNT(name): Paris 2, others 1 (Nice included -- its NAME is set, only its age is missing).
    val between =
      """SELECT city FROM having_naming
        |GROUP BY city HAVING COUNT(name) BETWEEN 2 AND 3;""".stripMargin
    assertSelectResult(
      System.nanoTime(),
      client.run(between).futureValue,
      Seq(Map("city" -> "Paris"))
    )

    val notBetween =
      """SELECT city FROM having_naming
        |GROUP BY city HAVING COUNT(name) NOT BETWEEN 2 AND 3;""".stripMargin
    assertSelectResult(
      System.nanoTime(),
      client.run(notBetween).futureValue,
      Seq(Map("city" -> "Lyon"), Map("city" -> "Marseille"), Map("city" -> "Nice"))
    )

    // MAX(age): Paris 30, Lyon 40, Marseille 50, Nice missing.
    val in =
      """SELECT city FROM having_naming
        |GROUP BY city HAVING MAX(age) IN (40, 50);""".stripMargin
    assertSelectResult(
      System.nanoTime(),
      client.run(in).futureValue,
      Seq(Map("city" -> "Lyon"), Map("city" -> "Marseille"))
    )

    // The same over a COUNT metric, which the type validator rejected until the third look (T3-3)
    // so it had never reached a cluster: `value_count` comes back as a long on the buckets_path
    // while the literals are Integers -- the S2-1 boxing class of defect. COUNT(name): Paris 2,
    // Lyon 1, Marseille 1, Nice 1, so exactly one bucket may pass (an all-pass or an empty result
    // both fail this assertion).
    val countIn =
      """SELECT city FROM having_naming
        |GROUP BY city HAVING COUNT(name) IN (2, 3);""".stripMargin
    assertSelectResult(
      System.nanoTime(),
      client.run(countIn).futureValue,
      Seq(Map("city" -> "Paris"))
    )

    // `A AND NOT B`: the NOT belongs to the RIGHT operand (it used to negate the left one), and a
    // bucket whose metric is missing (Nice) must still fail `NOT MAX(age) > 45`, as in SQL.
    val andNot =
      """SELECT city FROM having_naming
        |GROUP BY city HAVING COUNT(*) >= 1 AND NOT MAX(age) > 45;""".stripMargin
    assertSelectResult(
      System.nanoTime(),
      client.run(andNot).futureValue,
      Seq(Map("city" -> "Paris"), Map("city" -> "Lyon"))
    )
  }

  it should "reject an aggregate in WHERE instead of silently dropping it (lead-confirmed 2026-09-06)" in {
    val sql =
      """SELECT city FROM having_naming
        |WHERE COUNT(name) > 1
        |GROUP BY city;""".stripMargin
    val res = client.run(sql).futureValue
    renderResults(System.nanoTime(), res)
    res.isSuccess shouldBe false
    res.error.map(_.message).getOrElse("") should include(
      "Aggregate functions are not allowed in WHERE"
    )
  }

  it should "reject an aggregate in a DELETE or UPDATE WHERE and touch nothing (S2-2, lead-confirmed 2026-09-06)" in {
    // Before: the predicate became `match_all` -- the DELETE wiped the index, the UPDATE hit every
    // document. The count and the ages must be exactly what they were.
    val snapshot =
      "SELECT COUNT(*) AS n, MAX(age) AS oldest, MIN(age) AS youngest FROM having_naming;"
    val before = collectRows(System.nanoTime(), client.run(snapshot).futureValue)
    before shouldBe Seq(Map("n" -> 5, "oldest" -> 50.0, "youngest" -> 25.0))

    val delete = client.run("DELETE FROM having_naming WHERE COUNT(name) > 1;").futureValue
    renderResults(System.nanoTime(), delete)
    delete.isSuccess shouldBe false
    delete.error.map(_.message).getOrElse("") should include(
      "Aggregate functions are not allowed in WHERE"
    )

    val update =
      client.run("UPDATE having_naming SET age = 0 WHERE COUNT(name) > 1;").futureValue
    renderResults(System.nanoTime(), update)
    update.isSuccess shouldBe false
    update.error.map(_.message).getOrElse("") should include(
      "Aggregate functions are not allowed in WHERE"
    )

    val after = collectRows(System.nanoTime(), client.run(snapshot).futureValue)
    after shouldBe before
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
    assertSelectResult(System.nanoTime(), res)
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
    assertSelectResult(System.nanoTime(), res)
  }

  // ---------------------------------------------------------------------------
  // Story 21.5 — the CAST conversion defect (parts C1/C2) and Painless literal
  // escaping (part D), on a REAL index.
  //
  // A dedicated table: `dql_users` has no keyword column holding a numeric string and no DOUBLE
  // column, and adding either would move every 4-row oracle above. These three defects are all
  // EXECUTION-time — a schema-less unit test cannot see C1 at all, and only Elasticsearch can
  // prove a Painless-syntax claim — so they need their own fixture and their own content
  // assertions, not `isSuccess`.
  // ---------------------------------------------------------------------------

  it should "prepare the cast-conversion test data (story 21.5)" in {
    val create =
      """CREATE TABLE IF NOT EXISTS cast_conversions (
        |  id INT NOT NULL,
        |  code KEYWORD,
        |  bad KEYWORD,
        |  amount DOUBLE,
        |  when_dt DATE
        |);""".stripMargin

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val insert =
      """INSERT INTO cast_conversions (id, code, bad, amount, when_dt) VALUES
        |  (1, '125', 'abc', 1.9, '2024-03-15T14:30:00Z');
        |""".stripMargin

    assertDml(System.nanoTime(), client.run(insert).futureValue, Some(DmlResult(inserted = 1)))
  }

  /** `script_fields` values arrive wrapped in Elasticsearch's per-field array on the row path (a
    * pre-existing divergence, recorded by story 21.3), so unwrap a single-element list before
    * asserting the VALUE. What matters here is the TYPE and the value, not the wrapper.
    */
  private def scalarOf(row: Map[String, Any], key: String): Any =
    row.getOrElse(key, fail(s"no column [$key] in $row")) match {
      case Seq(one)  => one
      case List(one) => one
      case other     => other
    }

  it should "narrow a cast over a LITERAL instead of returning the unnarrowed value (C2)" in {
    // BEFORE story 21.5 this came back 1.9: `coerce` carried widening arms only, so every
    // narrowing pair fell to the identity fallback. This is the case that proves Elasticsearch
    // accepts the emitted `((int) 1.9)` — a Painless claim only ES can settle.
    //
    // Asserted as an EXACT double, deliberately: `longValue()` alone would be satisfied by the
    // unnarrowed 1.9 too, i.e. a gate that cannot fail.
    val sql = "SELECT id, CAST(1.9 AS INT) AS n, CAST(300 AS TINYINT) AS b FROM cast_conversions;"
    val rows = collectRows(System.nanoTime(), client.run(sql).futureValue)
    rows.size shouldBe 1
    scalarOf(rows.head, "n").asInstanceOf[java.lang.Number].doubleValue() shouldBe 1.0
    // 300 does not fit in a byte. Java/Painless narrowing DISCARDS THE HIGH-ORDER BITS, so the
    // answer is deterministic: `(byte) 300` is 44. Asserted exactly — `should not be 300` would be
    // satisfied by a clamp to 127, by 0, or by any other wrong narrowing, i.e. a gate that cannot
    // fail. (It was written that way first; this is the tightened form.)
    scalarOf(rows.head, "b").asInstanceOf[java.lang.Number].intValue() shouldBe 44
  }

  it should "convert a cast over a LITERAL string exactly as documented (C1's reachable half)" in {
    val sql =
      """SELECT id,
        |       CAST('125' AS SIGNED) AS signed_n,
        |       CAST('1.5' AS DECIMAL(10,2)) AS dec_n,
        |       CAST(1 AS CHAR(10)) AS as_char,
        |       TRY_CAST('abc' AS BIGINT) AS bad_n
        |FROM cast_conversions;""".stripMargin
    val rows = collectRows(System.nanoTime(), client.run(sql).futureValue)
    rows.size shouldBe 1
    scalarOf(rows.head, "signed_n").asInstanceOf[java.lang.Number].longValue() shouldBe 125L
    scalarOf(rows.head, "dec_n").asInstanceOf[java.lang.Number].doubleValue() shouldBe 1.5
    // `CAST(<non-string> AS CHAR)` was a silent no-op before this story: it emitted the bare `1`.
    scalarOf(rows.head, "as_char") shouldBe "1"
    // `shouldBe null` does not compile on an `Any` (Cannot prove that Any <:< AnyRef); `Option(...)`
    // is the exact same assertion and does.
    Option(scalarOf(rows.head, "bad_n")) shouldBe None
  }

  /** 🔴 RETARGETED by issue #306 — this used to pin the DEFECT ("should NOT yet convert a cast over
    * a COLUMN") and the defect is gone, so the pin asserts the fixed behaviour instead of being
    * relaxed or deleted.
    *
    * `SQLTypeUtils.coerce` is indexed by the operand's `baseType`, which for an identifier is
    * `col.map(_.dataType)`, and `col` is populated only by `SingleSearch.update(Some(schema))`.
    * Until #306 that had ONE production call site — `Table.mergeWithSearch`, inferring a CTAS
    * target's columns — so no executing query carried a schema and a cast over a column emitted no
    * conversion at all, for ANY source type. `SearchApi.resolveTemporalLiterals` now attaches the
    * schema it was already loading, on every execution path.
    *
    * Values asserted EXACTLY: `should not be` here would be the fourth can't-fail gate of this
    * epic.
    */
  it should "convert a cast over a COLUMN, now that the schema reaches the execution path (#306)" in {
    val sql =
      "SELECT id, CAST(code AS BIGINT) AS n, CAST(amount AS BIGINT) AS m FROM cast_conversions;"
    val rows = collectRows(System.nanoTime(), client.run(sql).futureValue)
    rows.size shouldBe 1
    // a KEYWORD column cast to BIGINT: the STRING "125" before #306, the NUMBER 125 after (C1)
    val n = scalarOf(rows.head, "n")
    n shouldBe a[java.lang.Number]
    n.asInstanceOf[java.lang.Number].longValue() shouldBe 125L
    // a DOUBLE column narrowed to BIGINT: 1.9 before #306, 1 after (C2). Asserted as an exact
    // double, because `longValue()` alone is satisfied by the unnarrowed 1.9 too.
    val m = scalarOf(rows.head, "m")
    m shouldBe a[java.lang.Number]
    m.asInstanceOf[java.lang.Number].doubleValue() shouldBe 1.0
  }

  /** Every message a failure carries, cause chain and suppressed included. ES 6/7 nest the shard
    * root cause as SUPPRESSED under "all shards failed", so a plain `getMessage` sees none of it.
    */
  private def throwableText(t: Throwable): String = {
    val seen = scala.collection.mutable.Set.empty[Throwable]
    def walk(x: Throwable): List[String] =
      if (x == null || !seen.add(x)) Nil
      else
        Option(x.getMessage).toList ::: walk(x.getCause) ::: x.getSuppressed.toList.flatMap(walk)
    walk(t).mkString(" | ")
  }

  /** 🔴 The MEDIUM-5 replacement, and the evidence for the two HIGH findings.
    *
    * `when_dt` is declared DATE — deliberately, because that is the case both HIGHs were measured
    * on and the harder one: our own CREATE TABLE records `_meta.columns.when_dt.data_type = DATE`,
    * and `IndexField.apply` PREFERS that recorded spelling over Elasticsearch's own `date`. So it
    * is not enough to fix the ES-mapping fallback; the declared spelling has to resolve to
    * TIMESTAMP too, which is what `SQLTypes.apply(IndexField)` now does.
    *
    * It carries a TIME OF DAY (14:30Z) on purpose: with a midnight value, "truncated to the date"
    * and "not truncated at all" are nearly the same string, and the gate would barely fail.
    *
    * Every one of the four is falsifiable against the unfixed branch, where the operand resolved to
    * `Date`:
    *   - AS TIMESTAMP and AS DATETIME took `(Date, DateTime | Timestamp)` and emitted
    *     `.atStartOfDay(ZoneId.of('Z'))`, which a `ZonedDateTime` does not have ⇒ all shards
    *     failed;
    *   - AS DATE was `(Date, Date)`, the IDENTITY arm ⇒ the un-truncated timestamp came back;
    *   - AS TIME had no `(Date, Time)` arm at all ⇒ the fallback returned the timestamp whole.
    */
  /** 🔴 The INGEST path, which this suite has never exercised end to end.
    *
    * `users` is created with an ingest `DATEDIFF` column but nothing is ever inserted into it, and
    * the `age` assertions elsewhere belong to `dql_users`, which has no script column. So no test
    * has ever observed what an ingest script actually computes — which is why story 21.5's ingest
    * guard rested on inference rather than measurement.
    *
    * What is being measured: the RUNTIME TYPE of `ctx.<date field>`. `SQLTypeUtils.coerce` guards
    * its temporal arms on `isProcessorContext` because `ctx.d` is the raw JSON value of the
    * document being indexed, NOT the temporal object `doc['d'].value` hands a query. If that is
    * right, `DATEDIFF(d, CURRENT_DATE, DAY)` cannot compute at ingest — `ChronoUnit.DAYS.between`
    * gets a String — and the processor's `ignore_failure` leaves the column unset. If it is wrong,
    * `days` comes back a number and the guard is wrong; this test says which.
    */
  it should "record what an ingest script sees for a DATE column (ctx runtime type)" in {
    val create =
      """CREATE TABLE IF NOT EXISTS ingest_probe (
        |  id INT NOT NULL,
        |  d DATE,
        |  label KEYWORD,
        |  days INT SCRIPT AS (DATEDIFF(d, CURRENT_DATE, DAY))
        |);""".stripMargin
    assertDdl(System.nanoTime(), client.run(create).futureValue)

    assertDml(
      System.nanoTime(),
      client
        .run("INSERT INTO ingest_probe (id, d, label) VALUES (1, '2024-03-15', 'x');")
        .futureValue,
      Some(DmlResult(inserted = 1))
    )

    val rows = collectRows(
      System.nanoTime(),
      client.run("SELECT id, d, label, days FROM ingest_probe;").futureValue
    )
    rows.size shouldBe 1
    val row = rows.head

    // CONTROL: the document was indexed and the plain columns round-tripped, so anything observed
    // about `days` is about the ingest SCRIPT and not about a failed insert.
    String.valueOf(scalarOf(row, "d")) should startWith("2024-03-15")
    scalarOf(row, "label") shouldBe "x"

    val days = row.get("days").map(v => scalarOf(row, "days")).orNull
    withClue(s"ingest-computed days = [$days] (null/absent => ctx.d is NOT a temporal object): ") {
      // The measurement. Asserted, not merely printed, so a change in either direction is loud.
      Option(days) shouldBe None
    }
  }

  it should "convert every temporal cast over a DATE column (HIGH-1 + HIGH-2)" in {
    val sql =
      """SELECT id,
        |       CAST(when_dt AS DATE) AS d,
        |       CAST(when_dt AS TIME) AS t,
        |       CAST(when_dt AS TIMESTAMP) AS ts,
        |       CAST(when_dt AS DATETIME) AS dt
        |FROM cast_conversions;""".stripMargin
    val rows = collectRows(System.nanoTime(), client.run(sql).futureValue)
    rows.size shouldBe 1
    val row = rows.head

    // HIGH-2, and the one that earns an exact value: `CAST(ts AS DATE)` is the date-truncation
    // idiom Superset and Tableau emit constantly. The time of day must be GONE.
    val d = String.valueOf(scalarOf(row, "d"))
    withClue(s"CAST(when_dt AS DATE) returned [$d]: ") {
      d should startWith("2024-03-15")
      d should not include "14:30" // the un-truncated timestamp, i.e. the defect
    }

    // the complementary half: the time survives its own cast
    val t = String.valueOf(scalarOf(row, "t"))
    withClue(s"CAST(when_dt AS TIME) returned [$t]: ") {
      t should startWith("14:30")
    }

    // HIGH-1: these two merely have to EXECUTE — on the unfixed branch they are a compile error
    // inside Elasticsearch, so reaching a value at all is the assertion. The value is pinned too,
    // since it costs nothing.
    Seq("ts", "dt").foreach { col =>
      val v = String.valueOf(scalarOf(row, col))
      withClue(s"CAST(when_dt AS ${col.toUpperCase}) returned [$v]: ") {
        v should startWith("2024-03-15")
        v should include("14:30")
      }
    }
  }

  it should "fail a cast over a COLUMN whose value cannot be parsed, and null it under TRY_CAST" in {
    // The other half of the C1 contract, and the reason the release note leads on it: a value that
    // used to pass through raw now FAILS, and TRY_CAST is the documented escape hatch.
    //
    // `isFailure shouldBe true` on its own is NOT evidence: `collectRows` itself asserts
    // `res.isSuccess`, so a missing index, a parse rejection, a dead cluster - any breakage at all
    // - satisfies it identically, and it would keep passing if the conversion were reverted to the
    // pass-through and something ELSE broke. The failure has to be shown to be THE one under test:
    // Painless running `Long.parseLong` over the unparseable value the fixture stored.
    val bad = "SELECT id, CAST(bad AS BIGINT) AS n FROM cast_conversions;"
    val thrown = scala.util
      .Try(collectRows(System.nanoTime(), client.run(bad).futureValue))
      .failed
      .getOrElse(fail("CAST(bad AS BIGINT) over an unparseable value was expected to fail"))
    val reported = throwableText(thrown)

    // (i) CONTROL - the same index, the same column, the same value, WITHOUT the cast. It
    // succeeds and yields 'abc', so nothing upstream of the conversion is broken and the CAST is
    // the only difference between this pair.
    val control =
      collectRows(
        System.nanoTime(),
        client.run("SELECT id, bad FROM cast_conversions;").futureValue
      )
    control.size shouldBe 1
    scalarOf(control.head, "bad") shouldBe "abc"

    // (ii) and the failure is Elasticsearch rejecting the query AT EXECUTION, not the testkit
    // refusing something earlier. MEASURED on ES 8.18: the Painless NumberFormatException does NOT
    // reach any Throwable message - the client keeps the root cause in its typed ErrorCause tree,
    // outside the exception chain (the same asymmetry #224 had to work around), so `include("abc")`
    // is unassertable here and the shard-failure marker is the most specific text every client
    // surfaces. Together with (i) that is the conversion, and nothing else.
    withClue(s"reported failure text was [$reported]: ") {
      reported.toLowerCase should include("shards failed")
    }

    val safe = "SELECT id, TRY_CAST(bad AS BIGINT) AS n FROM cast_conversions;"
    val rows = collectRows(System.nanoTime(), client.run(safe).futureValue)
    rows.size shouldBe 1
    Option(scalarOf(rows.head, "n")) shouldBe None
  }

  // 🔴 REMOVED — this test could not fail, and it was the evidence for the #306 Any-branch risk.
  // It asserted `YEAR(birthdate)` / `DATE_TRUNC(birthdate, MONTH)`, which route through
  // `Identifier.painless` -> `originalType`, and `originalType` is
  // `if (name.trim.nonEmpty) SQLTypes.Any` (sql/.../package.scala) — `Any` for ANY NAMED COLUMN,
  // schema attached or not. So both shapes emit byte-identical Painless with and without the
  // attach and the test passes on unfixed code.
  //
  // The branch that actually moves is `Conversion.toPainless`'s own guard, and the shapes that
  // move through it are `CAST(<date column> AS TIMESTAMP|DATETIME|DATE|TIME)` — the subject of the
  // two HIGH findings, whose fix is with the lead. The real Any-branch evidence lands with that
  // fix; a gate that cannot fail is worse than none, so it is not left standing in the meantime.
  //
  // Epic lesson, for the fourth time: a test chosen to demonstrate a change must be SHOWN to fail
  // without it.

  // 🔴 RESTORED. These two were deleted by the #306 commit's splice and the suite count hid
  // it — two out, two in, so es8java read 70/70 on both sides. They are the ONLY real-ES
  // executions of issue #305's escaping claims; `PainlessLiteralEscapingSpec` asserts the
  // emitted string, not that Painless ACCEPTS it, and only a real index settles that.
  it should "execute a script whose string literal contains a double quote (D)" in {
    // BEFORE: `Value.painless` emitted `"a"b"` and Elasticsearch rejected the script at COMPILE
    // time. A Painless-syntax claim is only provable by Elasticsearch, which is why this case
    // cannot live in the sql module.
    val sql = """SELECT id, CONCAT('a"b', code) AS c FROM cast_conversions;"""
    val rows = collectRows(System.nanoTime(), client.run(sql).futureValue)
    rows.size shouldBe 1
    scalarOf(rows.head, "c") shouldBe """a"b125"""
  }

  it should "execute a script whose string literal ends in a backslash (D)" in {
    val sql = """SELECT id, CONCAT('C:\\', code) AS c FROM cast_conversions;"""
    val rows = collectRows(System.nanoTime(), client.run(sql).futureValue)
    rows.size shouldBe 1
    scalarOf(rows.head, "c") shouldBe """C:\125"""
  }

  it should "execute a script whose string literal contains a RAW newline (D)" in {
    // A raw line terminator DOES reach a literal: `Parser.normalize` collapses newlines only
    // OUTSIDE quoted runs, and the literal regex's `[^'\\]` matches LF. The JDBC driver inlines
    // PreparedStatement parameters as SQL literals, so any multi-line string value lands here.
    //
    // 🔴 It must be passed through RAW. Painless accepts only `\\` and `\"` as escape sequences and
    // its content rule `~[\\"]` admits a raw line terminator, so the two-character `\n` form is a
    // script COMPILE ERROR - the opposite of Java. Review proposed escaping it; this case is what
    // measured that proposal breaking, and it fails if anyone re-introduces it.
    val sql = "SELECT id, CONCAT('a\nb', code) AS c FROM cast_conversions;"
    val rows = collectRows(System.nanoTime(), client.run(sql).futureValue)
    rows.size shouldBe 1
    scalarOf(rows.head, "c") shouldBe "a\nb125"
  }

  it should "accept the SQL-standard doubled quote end to end (A)" in {
    val sql = "SELECT id, UPPER('it''s') AS s FROM cast_conversions;"
    val rows = collectRows(System.nanoTime(), client.run(sql).futureValue)
    rows.size shouldBe 1
    scalarOf(rows.head, "s") shouldBe "IT'S"
  }

  it should "accept the new CAST target types over a column end to end (B)" in {
    // 🔴 This deferred to "the known limitation above" - that a cast over a COLUMN emitted no
    // conversion - and asserted only the ROW COUNT. #306 removed the limitation, so the reference
    // was stale AND the assertion was weak: `nbResults = Some(1)` passes just as well when every
    // one of these six columns comes back as the raw keyword "125". The values are assertable now,
    // so they are asserted.
    val sql =
      """SELECT id,
        |       CAST(code AS SIGNED) AS signed_n,
        |       CAST(code AS DECIMAL(10,2)) AS dec_n,
        |       CAST(code AS TEXT) AS as_text,
        |       CAST(code AS CHAR(10)) AS as_char,
        |       CAST(code AS INT(11)) AS as_int,
        |       CONVERT(code USING utf8) AS as_utf8
        |FROM cast_conversions;""".stripMargin
    val rows = collectRows(System.nanoTime(), client.run(sql).futureValue)
    rows.size shouldBe 1
    val row = rows.head
    // SIGNED is an alias of BIGINT (documented as such, not pretended to be unsigned-aware)
    scalarOf(row, "signed_n") shouldBe a[java.lang.Number]
    scalarOf(row, "signed_n").asInstanceOf[java.lang.Number].longValue() shouldBe 125L
    // DECIMAL(p,s): OQ-7's uniform rule DISCARDS the parameters, the target is DOUBLE
    scalarOf(row, "dec_n") shouldBe a[java.lang.Number]
    scalarOf(row, "dec_n").asInstanceOf[java.lang.Number].doubleValue() shouldBe 125.0
    // INT(11): the display width is discarded, the CONVERSION is not
    scalarOf(row, "as_int") shouldBe a[java.lang.Number]
    scalarOf(row, "as_int").asInstanceOf[java.lang.Number].intValue() shouldBe 125
    // the three string targets keep the value AS A STRING - a number here would mean the cast
    // fell through to the identity fallback
    scalarOf(row, "as_text") shouldBe "125"
    scalarOf(row, "as_char") shouldBe "125"
    scalarOf(row, "as_utf8") shouldBe "125"
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
    assertSelectResult(System.nanoTime(), res)
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
      System.nanoTime(),
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
    assertSelectResult(System.nanoTime(), res)
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val table = assertShowTable(System.nanoTime(), client.run("SHOW TABLE dql_geo").futureValue)
    table.ddl should include("location GEO_POINT")
    table.find("location").exists(_.dataType == SQLTypes.GeoPoint) shouldBe true

    val insert =
      """INSERT INTO dql_geo (id, location) VALUES
        |  (1, {lon = 2.3522, lat = 48.8566}),
        |  (2, {lon = 4.8357, lat = 45.7640});""".stripMargin

    assertDml(System.nanoTime(), client.run(insert).futureValue, Some(DmlResult(inserted = 2)))

    val sql =
      """SELECT id,
        |       ST_DISTANCE(location, POINT(2.3522, 48.8566)) AS dist_paris
        |FROM dql_geo;""".stripMargin

    val res = client.run(sql).futureValue
    assertSelectResult(System.nanoTime(), res)
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

    assertDdl(System.nanoTime(), client.run(create).futureValue)

    val insert =
      """INSERT INTO dql_sales (id, product, customer, amount, ts) VALUES
        |  (1, 'A', 'C1', 10.0, '2024-01-01T10:00:00Z'),
        |  (2, 'A', 'C2', 20.0, '2024-01-01T11:00:00Z'),
        |  (3, 'B', 'C1', 30.0, '2024-01-01T12:00:00Z'),
        |  (4, 'A', 'C3', 40.0, '2024-01-01T13:00:00Z');""".stripMargin

    assertDml(System.nanoTime(), client.run(insert).futureValue, Some(DmlResult(inserted = 4)))

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
      System.nanoTime(),
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

    assertDdl(System.nanoTime(), client.run(sql).futureValue)

    val pipeline =
      assertShowPipeline(System.nanoTime(), client.run("SHOW PIPELINE user_pipeline").futureValue)
    pipeline.name shouldBe "user_pipeline"
    pipeline.processors.size shouldBe 6

    val desc =
      assertQueryRows(System.nanoTime(), client.run("DESCRIBE PIPELINE user_pipeline").futureValue)
    desc.exists(row =>
      row("processor_type") == "set" &&
      row("field") == "name" &&
      row("description").asInstanceOf[String].contains("DEFAULT 'anonymous'")
    ) shouldBe true
    desc.exists(row =>
      row("processor_type") == "set" &&
      row("field") == "_id" &&
      row("description").asInstanceOf[String].contains("PRIMARY KEY (id)")
    ) shouldBe true

    val showCreate = client.run("SHOW CREATE PIPELINE user_pipeline").futureValue
    val ddl = assertShowCreate(System.nanoTime(), showCreate)
    ddl should include("CREATE OR REPLACE PIPELINE user_pipeline")

    val pipelines = assertQueryRows(System.nanoTime(), client.run("SHOW PIPELINES").futureValue)
    pipelines should contain(Map("name" -> "user_pipeline", "processors_count" -> 6))
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

    assertDdl(System.nanoTime(), client.run(sql).futureValue)
  }

  // ---------------------------------------------------------------------------
  // DROP PIPELINE
  // ---------------------------------------------------------------------------

  it should "drop a pipeline" in {
    val sql = "DROP PIPELINE IF EXISTS user_pipeline;"
    assertDdl(System.nanoTime(), client.run(sql).futureValue)
  }

  // ===========================================================================
  // 5b. FROM-less SELECT — the connection handshake (story 20.9 / issue #251)
  // ===========================================================================

  behavior of "FROM-less SELECT handshake"

  it should "answer the FROM-less connection handshake against the live cluster" in {
    // issue #251 — the Tableau/Superset connect idiom, executed as Painless AGAINST ES
    var rows = assertQueryRows(System.nanoTime(), client.run("SELECT 1").futureValue)
    rows shouldBe Seq(Map("1" -> 1)) // Integer via Jackson — NOT List(1): AC 7's unwrap
    // PD-4: the value round-trips ES JSON as Jackson's smallest type — INTEGER, not BIGINT
    // (Scala's cooperative equality makes `shouldBe 1` pass for a Long too — pin the class)
    rows.head("1").isInstanceOf[Int] shouldBe true
    rows = assertQueryRows(System.nanoTime(), client.run("SELECT 1 LIMIT 100").futureValue)
    rows.size shouldBe 1
    rows = assertQueryRows(
      System.nanoTime(),
      client.run("SELECT 1 AS ok, UPPER('x') AS u, 1+1 AS two").futureValue
    )
    rows.head("ok") shouldBe 1
    rows.head("u") shouldBe "X"
    rows.head("two") shouldBe 2
    // constant cast — '125'::BIGINT arrives as the VALUE's own type (Jackson smallest — PD-4)
    rows = assertQueryRows(
      System.nanoTime(),
      client.run("SELECT '125'::BIGINT AS c").futureValue
    )
    rows.head("c") shouldBe 125
    // OQ-2 rows (PD-5, dev-verified): interval arithmetic and CASE over constants ride the
    // same FROM-ful painless pipeline — value truth asserted on the live cluster
    rows = assertQueryRows(
      System.nanoTime(),
      client.run("SELECT CASE WHEN 1 = 1 THEN 'a' ELSE 'b' END AS c").futureValue
    )
    rows.head("c") shouldBe "a"
    rows = assertQueryRows(
      System.nanoTime(),
      client.run("SELECT CURRENT_DATE - INTERVAL 1 DAY AS d").futureValue
    )
    Option(rows.head("d")) should not be None
  }

  it should "return NULL, fresh RANDOM values and honour LIMIT 0 on the handshake path" in {
    // NULL literal — the empty/absent-fields assembly on real ES (AC 7)
    var rows = assertQueryRows(System.nanoTime(), client.run("SELECT NULL AS n").futureValue)
    rows.size shouldBe 1
    Option(rows.head("n")) shouldBe None
    // RANDOM is painless Math.random() — fresh per execution (AD-7′)
    val r1 =
      assertQueryRows(System.nanoTime(), client.run("SELECT RANDOM AS r").futureValue).head("r")
    val r2 =
      assertQueryRows(System.nanoTime(), client.run("SELECT RANDOM AS r").futureValue).head("r")
    r1 should not be r2
    // LIMIT 0 returns zero rows but the round-trip still executed (AD-8)
    rows = assertQueryRows(System.nanoTime(), client.run("SELECT 1 LIMIT 0").futureValue)
    rows shouldBe Seq.empty
  }

  it should "latch one clock per FROM-less statement" in {
    // AD-6′ — one __now__ per search request, byte-equal items (the C7 guarantee, re-derived)
    val rows = assertQueryRows(
      System.nanoTime(),
      client.run("SELECT CURRENT_TIMESTAMP AS a, CURRENT_TIMESTAMP AS b").futureValue
    )
    rows.head("a") shouldBe rows.head("b")
  }

  it should "fail loudly at execution on a broken constant script" in {
    // SELECT 1/0 PARSES (no local evaluation any more) and fails on ES with a script error —
    // loud, inherited FROM-ful semantics. Message deliberately unpinned (grammar/ES-internal).
    val res = client.run("SELECT 1/0").futureValue
    res.isFailure shouldBe true
  }

  it should "keep the handshake index invisible to SHOW TABLES while it exists" in {
    // the handshake tests above ran => the index exists...
    client.indexExists(GatewayApi.HandshakeIndex, pattern = false) shouldBe ElasticSuccess(true)
    // ...but no SHOW TABLES pattern ever lists it (AC 6 — the ONE seam covering jdbc getTables
    // and Flight GET_TABLES; the ':74' dot-pattern assertion above is untouched: non-dot name).
    val all = assertQueryRows(System.nanoTime(), client.run("SHOW TABLES").futureValue)
    all.map(_("name")) should not contain GatewayApi.HandshakeIndex
    val like = assertQueryRows(
      System.nanoTime(),
      client.run("SHOW TABLES LIKE 'softclient4es%'").futureValue
    )
    like shouldBe Seq.empty
    // DESCRIBE TABLE deliberately still works on it (debuggability — AD-10)
    val describe = assertQueryRows(
      System.nanoTime(),
      client.run(s"DESCRIBE TABLE ${GatewayApi.HandshakeIndex}").futureValue
    )
    describe.exists(_("Field") == "dummy") shouldBe true
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
    val error = res.toEither.left.get
    // jdbc#35 — `run` is the front door for DQL, DML and DDL alike; a rejection must not claim the
    // statement was schema DDL, and it must echo what was rejected (BI tools show this verbatim).
    error.message should include("Error parsing SQL statement")
    error.message should include(invalidSql)
    error.message should not include "schema DDL"
  }

  // ---------------------------------------------------------------------------
  // Unsupported SQL
  // ---------------------------------------------------------------------------

  it should "return an error for unsupported SQL statements" in {
    val unsupportedSql = "GRANT SELECT ON users TO user1"
    val res = client.run(unsupportedSql).futureValue

    res.isFailure shouldBe true
    val error = res.toEither.left.get
    error.message should include("Error parsing SQL statement")
    error.message should include(unsupportedSql)
    error.message should not include "schema DDL"
  }

  // ---------------------------------------------------------------------------
  // jdbc#35 — a rejected DQL statement is not a schema DDL error
  // ---------------------------------------------------------------------------

  it should "not report a rejected SELECT as a schema DDL error" in {
    val invalidDql = "SELECT * FRM users"
    val res = client.run(invalidDql).futureValue

    res.isFailure shouldBe true
    val error = res.toEither.left.get
    error.message should include("Error parsing SQL statement")
    error.message should include(invalidDql)
    error.message should not include "schema DDL"
    error.operation shouldBe Some("sql")
    error.statusCode shouldBe Some(400)
  }

  // ---------------------------------------------------------------------------
  // AC-4 — the failing member of a batch is named, at position 2
  // ---------------------------------------------------------------------------

  // This case belongs HERE and not in the core unit spec: the property is "a rejection AFTER a
  // statement that actually ran is the one reported", and only a live cluster can make the first
  // statement succeed. The unit spec's batch test puts the failure at position 1, where "the
  // failing statement" and "the first statement" are indistinguishable.
  // `SHOW TABLES LIKE ...` is the cheapest statement this suite already proves works (see the
  // `SHOW TABLES LIKE 'show_%'` cases earlier in this file); the pattern is chosen to match
  // nothing so the case is independent of every fixture.
  it should "name the failing statement of a multi-statement batch" in {
    val res = client.run("SHOW TABLES LIKE 'no_such_prefix_%'; SELECT * FRM users").futureValue

    res.isFailure shouldBe true
    val error = res.toEither.left.get
    error.message should include("Error parsing SQL statement")
    error.message should include("SELECT * FRM users")
    error.message should not include "SHOW TABLES"
    error.operation shouldBe Some("sql")
  }

  // ===========================================================================
  // 7. WATCHERS — CREATE / DROP / SHOW
  // ===========================================================================

  behavior of "WATCHERS statements"

  it should "create, show and drop a watcher" in {
    val createIndex =
      """CREATE TABLE IF NOT EXISTS my_index (
        |  id INT NOT NULL,
        |  content VARCHAR,
        |  PRIMARY KEY (id)
        |);""".stripMargin

    assertDdl(System.nanoTime(), client.run(createIndex).futureValue)

    val createWatcherWithInterval =
      """CREATE OR REPLACE WATCHER my_watcher_interval AS
        | EVERY 5 SECONDS
        | FROM my_index WITHIN 1 MINUTE
        | ALWAYS DO
        | log_action AS LOG "Watcher triggered with {{ctx.payload.hits.total}} hits" AT INFO FOREACH "ctx.payload.hits.hits" LIMIT 500
        | END;""".stripMargin

    assertDdl(System.nanoTime(), client.run(createWatcherWithInterval).futureValue)

    var rows = assertQueryRows(
      System.nanoTime(),
      client.run("SHOW WATCHER STATUS my_watcher_interval").futureValue
    )
    rows.size shouldBe 1
    var row = rows.head
    row.get("id") shouldBe Some("my_watcher_interval")
    row.get("is_healthy") shouldBe Some(true)
    row.get("is_operational") shouldBe Some(true)

    val createWatcherWithCron =
      """CREATE OR REPLACE WATCHER my_watcher_cron AS
        | AT SCHEDULE '* * * * * ?'
        | WITH INPUTS search_data AS FROM my_index WITHIN 1 MINUTE, http_data AS GET "https://jsonplaceholder.typicode.com/todos/1" HEADERS ("Accept" = "application/json") TIMEOUT (connection = "5s", read = "10s")
        | WHEN SCRIPT 'ctx.payload.hits.total > params.threshold' USING LANG 'painless' WITH PARAMS (threshold = 10) RETURNS TRUE
        | DO
        | log_action AS LOG "Watcher triggered with {{ctx.payload.hits.total}} hits" AT INFO FOREACH "ctx.payload.hits.hits" LIMIT 500
        | END;""".stripMargin

    assertDdl(System.nanoTime(), client.run(createWatcherWithCron).futureValue)

    rows = assertQueryRows(
      System.nanoTime(),
      client.run("SHOW WATCHER STATUS my_watcher_cron").futureValue
    )
    rows.size shouldBe 1
    row = rows.head
    row.get("id") shouldBe Some("my_watcher_cron")
    row.get("is_healthy") shouldBe Some(true)
    row.get("is_operational") shouldBe Some(true)

    if (supportsQueryWatchers) {
      rows = assertQueryRows(System.nanoTime(), client.run("SHOW WATCHERS").futureValue)
      rows.find(row => row.get("id").contains("my_watcher_interval")) match {
        case Some(row) =>
          row.get("is_healthy") shouldBe Some(true)
          row.get("is_operational") shouldBe Some(true)
        case None => fail("Watcher my_watcher_interval not found in SHOW WATCHERS")
      }
    }

    val dropWatcher = "DROP WATCHER IF EXISTS my_watcher_interval;"
    assertDdl(System.nanoTime(), client.run(dropWatcher).futureValue)

  }

  // ===========================================================================
  // 8. POLICIES — CREATE / DROP / EXECUTE
  // ===========================================================================

  behavior of "POLICIES statements"

  it should "create, show, execute and drop a policy" in {

    assume(supportsEnrichPolicies, "Enrich policies are not supported in this environment")

    val createIndex =
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

    assertDdl(System.nanoTime(), client.run(createIndex).futureValue)

    val createPolicy =
      """CREATE OR REPLACE ENRICH POLICY my_policy
        |FROM dql_users
        |ON id
        |ENRICH name, profile.city
        |WHERE age > 10;""".stripMargin

    assertDdl(System.nanoTime(), client.run(createPolicy).futureValue)

    var rows =
      assertQueryRows(System.nanoTime(), client.run("SHOW ENRICH POLICIES").futureValue)
    rows.size shouldBe 1
    var enrichPolicy = rows.head
    enrichPolicy.get("name") shouldBe Some("my_policy")
    enrichPolicy.get("type") shouldBe Some("match")
    enrichPolicy.get("indices") shouldBe Some("dql_users")
    enrichPolicy.get("match_field") shouldBe Some("id")
    enrichPolicy.get("enrich_fields") shouldBe Some("name,profile.city")
    enrichPolicy
      .getOrElse("query", "")
      .asInstanceOf[String]
      .contains("""{"bool":{"filter":[{"range":{"age":{""") shouldBe true

    rows =
      assertQueryRows(System.nanoTime(), client.run("SHOW ENRICH POLICY my_policy").futureValue)
    rows.size shouldBe 1
    enrichPolicy = rows.head
    enrichPolicy.get("name") shouldBe Some("my_policy")
    enrichPolicy.get("type") shouldBe Some("match")
    enrichPolicy.get("indices") shouldBe Some("dql_users")
    enrichPolicy.get("match_field") shouldBe Some("id")
    enrichPolicy.get("enrich_fields") shouldBe Some("name,profile.city")
    enrichPolicy
      .getOrElse("query", "")
      .asInstanceOf[String]
      .contains("""{"bool":{"filter":[{"range":{"age":{""") shouldBe true

    val executePolicy = "EXECUTE ENRICH POLICY my_policy;"
    rows = assertQueryRows(System.nanoTime(), client.run(executePolicy).futureValue)
    rows.size shouldBe 1
    val row = rows.head
    row.getOrElse("policy_name", "") shouldBe "my_policy"
    row.getOrElse("status", "") shouldBe EnrichPolicyTaskStatus.Completed.name
    row.getOrElse("health", "") shouldBe HealthStatus.Green.name

    val dropPolicy = "DROP ENRICH POLICY IF EXISTS my_policy;"
    assertDdl(System.nanoTime(), client.run(dropPolicy).futureValue)
  }

}
