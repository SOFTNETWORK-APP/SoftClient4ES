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

package app.softnetwork.elastic.client.repl

import app.softnetwork.elastic.client.result.{DmlResult, OutputFormat, QueryRows}
import app.softnetwork.elastic.scalatest.ElasticTestKit

import java.time.LocalDate

/** Common integration tests for REPL, mirroring GatewayApiIntegrationSpec
  */
trait ReplGatewayIntegrationSpec extends ReplIntegrationTestKit {
  self: ElasticTestKit =>

  // =========================================================================
  // 1. SHOW / DESCRIBE TABLE tests
  // =========================================================================

  behavior of "REPL - SHOW / DESCRIBE TABLE"

  it should "return the table schema using SHOW TABLE" in {
    val create =
      """CREATE TABLE IF NOT EXISTS show_users (
        |  id INT NOT NULL,
        |  name VARCHAR FIELDS(
        |    raw KEYWORD
        |  ) OPTIONS (fielddata = true),
        |  age INT DEFAULT 0,
        |  PRIMARY KEY (id)
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val show = executeSync("SHOW TABLE show_users")
    val table = assertShowTable(System.nanoTime(), show)

    val showCreate = executeSync("SHOW CREATE TABLE show_users")
    val sql = assertShowCreate(System.nanoTime(), showCreate)
    sql should include("CREATE OR REPLACE TABLE show_users")

    val ddl = table.ddl
    ddl should include("CREATE OR REPLACE TABLE show_users")
    ddl should include("id INT NOT NULL")
    ddl should include("name VARCHAR")
    ddl should include("age INT DEFAULT 0")
    ddl should include("PRIMARY KEY (id)")

    var rows = assertQueryRows(System.nanoTime(), executeSync("SHOW TABLES LIKE 'show_*'"))
    rows.size should be >= 1
    rows.exists(_("name") == "show_users") shouldBe true

    rows = assertQueryRows(System.nanoTime(), executeSync("SHOW TABLES LIKE '.*'"))
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
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val rows = assertQueryRows(System.nanoTime(), executeSync("DESCRIBE TABLE desc_users"))
    rows.size shouldBe 6
    rows.exists(row =>
      row("Field") == "id" &&
      row("Null") == "no" &&
      row("Key") == "PRI"
    ) shouldBe true
    rows.exists(_("Field") == "name") shouldBe true
    rows.exists(_("Field") == "profile.city") shouldBe true
  }

  // =========================================================================
  // 2. DDL — CREATE TABLE, ALTER TABLE, DROP TABLE, TRUNCATE TABLE
  // =========================================================================

  behavior of "REPL - DDL statements"

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
        |) PARTITION BY birthdate (MONTH), OPTIONS (mappings = (dynamic = false))""".stripMargin

    assertDdl(System.nanoTime(), executeSync(sql))

    val table = assertShowTable(System.nanoTime(), executeSync("SHOW TABLE users"))
    val ddl = table.ddl.replaceAll("\\s+", " ")

    ddl should include("CREATE OR REPLACE TABLE users")
    ddl should include("id INT NOT NULL COMMENT 'user identifier'")
    ddl should include("birthdate DATE")
    ddl should include("age INT SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR))")
    ddl should include("PRIMARY KEY (id)")
    ddl should include("PARTITION BY birthdate (MONTH)")
  }

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
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(sql))
    assertDdl(System.nanoTime(), executeSync(sql)) // second call should succeed
  }

  it should "create or replace a table from a SELECT query" in {
    val createSource =
      """CREATE TABLE IF NOT EXISTS accounts_src (
        |  id INT NOT NULL,
        |  name VARCHAR,
        |  active BOOLEAN,
        |  PRIMARY KEY (id)
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(createSource))

    val insertSource =
      """INSERT INTO accounts_src (id, name, active) VALUES
        | (1, 'Alice', true),
        | (2, 'Bob',   false),
        | (3, 'Chloe', true)""".stripMargin

    assertDml(System.nanoTime(), executeSync(insertSource))

    val createOrReplace =
      "CREATE OR REPLACE TABLE users_cr AS SELECT id, name FROM accounts_src WHERE active = true"

    assertDml(System.nanoTime(), executeSync(createOrReplace))

    val table = assertShowTable(System.nanoTime(), executeSync("SHOW TABLE users_cr"))
    table.ddl should include("CREATE OR REPLACE TABLE users_cr")
    table.ddl should include("id INT")
    table.ddl should include("name VARCHAR")
  }

  it should "drop a table if it exists" in {
    val create =
      """CREATE TABLE IF NOT EXISTS tmp_drop (
        |  id INT NOT NULL,
        |  value VARCHAR
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))
    assertDdl(System.nanoTime(), executeSync("DROP TABLE IF EXISTS tmp_drop"))
  }

  it should "truncate a table" in {
    val create =
      """CREATE TABLE IF NOT EXISTS tmp_truncate (
        |  id INT NOT NULL,
        |  value VARCHAR
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val insert =
      """INSERT INTO tmp_truncate (id, value) VALUES
        |  (1, 'a'),
        |  (2, 'b'),
        |  (3, 'c')""".stripMargin

    assertDml(System.nanoTime(), executeSync(insert))
    assertDdl(System.nanoTime(), executeSync("TRUNCATE TABLE tmp_truncate"))

    val select = executeSync("SELECT * FROM tmp_truncate")
    assertSelectResult(System.nanoTime(), select)
  }

  // =========================================================================
  // 3. ALTER TABLE tests
  // =========================================================================

  it should "add a column IF NOT EXISTS with DEFAULT" in {
    val create =
      """CREATE TABLE IF NOT EXISTS users_alter1 (
        |  id INT NOT NULL,
        |  status VARCHAR
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val alter = "ALTER TABLE users_alter1 ADD COLUMN IF NOT EXISTS age INT DEFAULT 0"
    assertDdl(System.nanoTime(), executeSync(alter))

    val table = assertShowTable(System.nanoTime(), executeSync("SHOW TABLE users_alter1"))
    table.ddl should include("age INT DEFAULT 0")
  }

  it should "rename a column" in {
    val create =
      """CREATE TABLE IF NOT EXISTS users_alter2 (
        |  id INT NOT NULL,
        |  name VARCHAR
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val alter = "ALTER TABLE users_alter2 RENAME COLUMN name TO full_name"
    assertDdl(System.nanoTime(), executeSync(alter))

    val table = assertShowTable(System.nanoTime(), executeSync("SHOW TABLE users_alter2"))
    table.ddl should include("full_name VARCHAR")
  }

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
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val alter =
      """ALTER TABLE users_alter8 (
        |  ADD COLUMN IF NOT EXISTS age INT DEFAULT 0,
        |  RENAME COLUMN name TO full_name,
        |  ALTER COLUMN IF EXISTS status SET DEFAULT 'active',
        |  ALTER COLUMN IF EXISTS profile SET FIELDS (
        |    description VARCHAR DEFAULT 'N/A',
        |    visibility BOOLEAN DEFAULT true
        |  )
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(alter))

    val table = assertShowTable(System.nanoTime(), executeSync("SHOW TABLE users_alter8"))

    table.ddl should include("age INT DEFAULT 0")
    table.ddl should include("full_name VARCHAR")
    table.ddl should include("status VARCHAR DEFAULT 'active'")
    table.ddl should include("description VARCHAR DEFAULT 'N/A'")
    table.ddl should include("visibility BOOLEAN DEFAULT true")
  }

  it should "list all tables" in {
    val tables = assertQueryRows(System.nanoTime(), executeSync("SHOW TABLES"))
    tables should not be empty
    for {
      table <- tables
    } {
      table should contain key "name"
      table should contain key "type"
    }
  }

  // =========================================================================
  // 4. DML — INSERT / UPDATE / DELETE
  // =========================================================================

  behavior of "REPL - DML statements"

  it should "insert rows into a table and return a DmlResult" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dml_users (
        |  id INT NOT NULL,
        |  name VARCHAR,
        |  age INT
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val insert =
      """INSERT INTO dml_users (id, name, age) VALUES
        |  (1, 'Alice', 30),
        |  (2, 'Bob',   40),
        |  (3, 'Chloe', 25)""".stripMargin

    val res = executeSync(insert)
    assertDml(System.nanoTime(), res)

    val dml = res.asInstanceOf[ExecutionSuccess].result.asInstanceOf[DmlResult]
    dml.inserted shouldBe 3

    val select = executeSync("SELECT * FROM dml_users ORDER BY id ASC")
    assertSelectResult(System.nanoTime(), select)
  }

  it should "update rows using UPDATE ... WHERE and return a DmlResult" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dml_accounts (
        |  id INT NOT NULL,
        |  owner KEYWORD,
        |  balance DOUBLE
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val insert =
      """INSERT INTO dml_accounts (id, owner, balance) VALUES
        |  (1, 'Alice', 100.00),
        |  (2, 'Bob',   50.00),
        |  (3, 'Chloe', 75.00)""".stripMargin

    assertDml(System.nanoTime(), executeSync(insert))

    val update =
      """UPDATE dml_accounts
        |SET balance = 125
        |WHERE owner = 'Alice'""".stripMargin

    val res = executeSync(update)
    assertDml(System.nanoTime(), res)

    val dml = res.asInstanceOf[ExecutionSuccess].result.asInstanceOf[DmlResult]
    dml.updated should be >= 1L

    val select =
      """SELECT owner, balance
        |FROM dml_accounts
        |WHERE owner = 'Alice'""".stripMargin

    assertSelectResult(System.nanoTime(), executeSync(select))
  }

  it should "delete rows using DELETE ... WHERE and return a DmlResult" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dml_logs (
        |  id INT NOT NULL,
        |  level KEYWORD,
        |  message VARCHAR
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val insert =
      """INSERT INTO dml_logs (id, level, message) VALUES
        |  (1, 'INFO',  'started'),
        |  (2, 'ERROR', 'failed'),
        |  (3, 'INFO',  'running')""".stripMargin

    assertDml(System.nanoTime(), executeSync(insert))

    val delete =
      """DELETE FROM dml_logs
        |WHERE level = 'ERROR'""".stripMargin

    val res = executeSync(delete)
    assertDml(System.nanoTime(), res)

    val dml = res.asInstanceOf[ExecutionSuccess].result.asInstanceOf[DmlResult]
    dml.deleted shouldBe 1L

    val select =
      """SELECT id, level
        |FROM dml_logs
        |ORDER BY id ASC""".stripMargin

    assertSelectResult(System.nanoTime(), executeSync(select))
  }

  it should "support a full DML lifecycle: INSERT → UPDATE → DELETE" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dml_chain (
        |  id INT NOT NULL,
        |  value INT
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val insert =
      """INSERT INTO dml_chain (id, value) VALUES
        |  (1, 10),
        |  (2, 20),
        |  (3, 30)""".stripMargin

    assertDml(System.nanoTime(), executeSync(insert))

    val update =
      """UPDATE dml_chain
        |SET value = 50
        |WHERE id IN (1, 3)""".stripMargin

    assertDml(System.nanoTime(), executeSync(update))

    val delete =
      """DELETE FROM dml_chain
        |WHERE value > 40""".stripMargin

    assertDml(System.nanoTime(), executeSync(delete))

    val select = executeSync("SELECT * FROM dml_chain ORDER BY id ASC")
    assertSelectResult(System.nanoTime(), select)
  }

  it should "support COPY INTO for JSONL and JSON_ARRAY formats" in {
    val create =
      """CREATE TABLE IF NOT EXISTS copy_into_test (
        |  uuid KEYWORD NOT NULL,
        |  name VARCHAR,
        |  birthDate DATE,
        |  childrenCount INT,
        |  PRIMARY KEY (uuid)
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val persons = List(
      """{"uuid": "A12", "name": "Homer Simpson", "birthDate": "1967-11-21", "childrenCount": 0}""",
      """{"uuid": "A14", "name": "Moe Szyslak", "birthDate": "1967-11-21", "childrenCount": 0}""",
      """{"uuid": "A16", "name": "Barney Gumble", "birthDate": "1969-05-09", "childrenCount": 2}"""
    )

    // Create temporary JSONL file
    val jsonlFile = java.io.File.createTempFile("copy_into_jsonl", ".jsonl")
    jsonlFile.deleteOnExit()
    val writer1 = new java.io.PrintWriter(jsonlFile)
    persons.foreach(writer1.println)
    writer1.close()

    // COPY INTO using JSONL
    val copyJsonl = s"""COPY INTO copy_into_test FROM "${jsonlFile.getAbsolutePath}""""
    val jsonlResult = executeSync(copyJsonl)
    assertDml(System.nanoTime(), jsonlResult, Some(DmlResult(inserted = persons.size)))

    // Create temporary JSON_ARRAY file
    val jsonArrayFile = java.io.File.createTempFile("copy_into_array", ".json")
    jsonArrayFile.deleteOnExit()
    val writer2 = new java.io.PrintWriter(jsonArrayFile)
    writer2.println("[")
    writer2.println(persons.mkString(",\n"))
    writer2.println("]")
    writer2.close()

    // COPY INTO using JSON_ARRAY with ON CONFLICT
    val copyArray =
      s"""COPY INTO copy_into_test FROM "${jsonArrayFile.getAbsolutePath}" FILE_FORMAT = JSON_ARRAY ON CONFLICT DO UPDATE"""
    val arrayResult = executeSync(copyArray)
    assertDml(System.nanoTime(), arrayResult, Some(DmlResult(inserted = persons.size)))

    // Verify
    val select = executeSync("SELECT * FROM copy_into_test ORDER BY uuid ASC")
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

  // =========================================================================
  // 5. DQL — SELECT, JOIN, UNNEST, GROUP BY, etc.
  // =========================================================================

  behavior of "REPL - DQL statements"

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
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val insert =
      """INSERT INTO dql_users (id, name, age, birthdate, profile) VALUES
        |  (1, 'Alice', 30, '1994-01-01', {city = "Paris", followers = 100}),
        |  (2, 'Bob',   40, '1984-05-10', {city = "Lyon",  followers = 50}),
        |  (3, 'Chloe', 25, '1999-07-20', {city = "Paris", followers = 200}),
        |  (4, 'David', 50, '1974-03-15', {city = "Marseille", followers = 10})""".stripMargin

    assertDml(System.nanoTime(), executeSync(insert), Some(DmlResult(inserted = 4)))
  }

  it should "execute a simple SELECT with aliases and nested fields" in {
    val sql =
      """SELECT id,
        |       name AS full_name,
        |       profile.city AS city,
        |       profile.followers AS followers
        |FROM dql_users
        |ORDER BY id ASC""".stripMargin

    val res = executeSync(sql)
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

  it should "execute a UNION ALL query" in {
    val sql =
      """SELECT id, name FROM dql_users WHERE age > 30
        |UNION ALL
        |SELECT id, name FROM dql_users WHERE age <= 30""".stripMargin

    val res = executeSync(sql)
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
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val insert =
      """INSERT INTO dql_orders (id, customer_id, items) VALUES
        |  (1, 1, [ { product = "A", quantity = 2, price = 10.0 },
        |           { product = "B", quantity = 1, price = 20.0 } ]),
        |  (2, 2, [ { product = "C", quantity = 3, price = 5.0 } ])""".stripMargin

    assertDml(System.nanoTime(), executeSync(insert), Some(DmlResult(inserted = 2)))

    val sql =
      """SELECT
        | o.id,
        | items.product,
        | items.quantity,
        | SUM(items.price * items.quantity) OVER (PARTITION BY o.id) AS total_price
        |FROM dql_orders o
        |JOIN UNNEST(o.items) AS items
        |WHERE items.quantity >= 1
        |ORDER BY o.id ASC""".stripMargin

    val res = executeSync(sql)
    assertSelectResult(
      System.nanoTime(),
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

  it should "support WHERE with complex logical and comparison operators" in {
    val sql =
      """SELECT id, name, age
        |FROM dql_users
        |WHERE (age > 20 AND profile.followers >= 100)
        |   OR (profile.city = 'Lyon' AND age < 50)
        |ORDER BY age DESC""".stripMargin

    val res = executeSync(sql)
    assertSelectResult(
      System.nanoTime(),
      res,
      Seq(
        Map("id" -> 1, "name" -> "Alice", "age" -> 30),
        Map("id" -> 3, "name" -> "Chloe", "age" -> 25)
      )
    )
  }

  it should "support ORDER BY with multiple fields, LIMIT and OFFSET" in {
    val sql =
      """SELECT id, name, age
        |FROM dql_users
        |ORDER BY age DESC, name ASC
        |LIMIT 2 OFFSET 1""".stripMargin

    assertSelectResult(System.nanoTime(), executeSync(sql))
  }

  it should "support GROUP BY with aggregations and HAVING" in {
    val sql =
      """SELECT profile.city AS city,
        |       COUNT(*) AS cnt,
        |       AVG(age) AS avg_age
        |FROM dql_users
        |GROUP BY profile.city
        |HAVING COUNT(*) >= 1
        |ORDER BY COUNT(*) DESC""".stripMargin

    assertSelectResult(System.nanoTime(), executeSync(sql))
  }

  it should "support arithmetic, IN, BETWEEN, IS NULL, LIKE, RLIKE" in {
    val sql =
      """SELECT id,
        |       age + 10 AS age_plus_10,
        |       name
        |FROM dql_users
        |WHERE age BETWEEN 20 AND 50
        |  AND name IN ('Alice', 'Bob', 'Chloe')
        |  AND name IS NOT NULL
        |  AND (name LIKE 'A%' OR name RLIKE '.*o.*')""".stripMargin

    assertSelectResult(System.nanoTime(), executeSync(sql))
  }

  it should "support CAST, TRY_CAST, SAFE_CAST and the :: operator" in {
    val sql =
      """SELECT id,
        |       age::BIGINT AS age_bigint,
        |       CAST(age AS DOUBLE) AS age_double,
        |       TRY_CAST('123' AS INT) AS try_cast_ok,
        |       SAFE_CAST('abc' AS INT) AS safe_cast_null
        |FROM dql_users""".stripMargin

    assertSelectResult(System.nanoTime(), executeSync(sql))
  }

  it should "support numeric and trigonometric functions" in {
    val sql =
      """SELECT id,
        |       ABS(age) AS abs_age,
        |       CEIL(age) AS ceil_div,
        |       FLOOR(age) AS floor_div,
        |       ROUND(age, 2) AS round_div,
        |       SQRT(age) AS sqrt_age,
        |       POW(age, 2) AS pow_age
        |FROM dql_users""".stripMargin

    assertSelectResult(System.nanoTime(), executeSync(sql))
  }

  it should "support string functions" in {
    val sql =
      """SELECT id,
        |       CONCAT(name.raw, '_suffix') AS name_concat,
        |       SUBSTRING(name.raw, 1, 2) AS name_sub,
        |       LOWER(name.raw) AS name_lower,
        |       UPPER(name.raw) AS name_upper,
        |       TRIM(name.raw) AS name_trim,
        |       LENGTH(name.raw) AS name_len
        |FROM dql_users
        |ORDER BY id ASC""".stripMargin

    assertSelectResult(System.nanoTime(), executeSync(sql))
  }

  it should "support date and time functions" in {
    val sql =
      """SELECT id,
        |       YEAR(CURRENT_DATE) AS current_year,
        |       MONTH(CURRENT_DATE) AS current_month,
        |       DAY(CURRENT_DATE) AS current_day,
        |       YEAR(birthdate) AS year_b,
        |       MONTH(birthdate) AS month_b,
        |       DAY(birthdate) AS day_b,
        |       DATE_ADD(birthdate, INTERVAL 1 DAY) AS plus_one_day,
        |       DATE_SUB(birthdate, INTERVAL 1 DAY) AS minus_one_day,
        |       DATE_DIFF(CURRENT_DATE, birthdate, YEAR) AS diff_years
        |FROM dql_users""".stripMargin

    assertSelectResult(System.nanoTime(), executeSync(sql))
  }

  it should "support geospatial functions POINT and ST_DISTANCE" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dql_geo (
        |  id INT NOT NULL,
        |  location GEO_POINT,
        |  PRIMARY KEY (id)
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val insert =
      """INSERT INTO dql_geo (id, location) VALUES
        |  (1, {lon = 2.3522, lat = 48.8566}),
        |  (2, {lon = 4.8357, lat = 45.7640})""".stripMargin

    assertDml(System.nanoTime(), executeSync(insert), Some(DmlResult(inserted = 2)))

    val sql =
      """SELECT id,
        |       ST_DISTANCE(location, POINT(2.3522, 48.8566)) AS dist_paris
        |FROM dql_geo""".stripMargin

    assertSelectResult(System.nanoTime(), executeSync(sql))
  }

  it should "support window functions with OVER clause" in {
    val create =
      """CREATE TABLE IF NOT EXISTS dql_sales (
        |  id INT NOT NULL,
        |  product KEYWORD,
        |  customer VARCHAR,
        |  amount DOUBLE,
        |  ts TIMESTAMP
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val insert =
      """INSERT INTO dql_sales (id, product, customer, amount, ts) VALUES
        |  (1, 'A', 'C1', 10.0, '2024-01-01T10:00:00Z'),
        |  (2, 'A', 'C2', 20.0, '2024-01-01T11:00:00Z'),
        |  (3, 'B', 'C1', 30.0, '2024-01-01T12:00:00Z'),
        |  (4, 'A', 'C3', 40.0, '2024-01-01T13:00:00Z')""".stripMargin

    assertDml(System.nanoTime(), executeSync(insert), Some(DmlResult(inserted = 4)))

    val sql =
      """SELECT
        |  product,
        |  customer,
        |  amount,
        |  SUM(amount) OVER (PARTITION BY product) AS sum_per_product,
        |  COUNT(_id) OVER (PARTITION BY product) AS cnt_per_product
        |FROM dql_sales
        |ORDER BY product, ts""".stripMargin

    assertSelectResult(System.nanoTime(), executeSync(sql))
  }

  // =========================================================================
  // 6. PIPELINE statements
  // =========================================================================

  behavior of "REPL - PIPELINE statements"

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
        |    SET (
        |        field = "_id",
        |        description = "PRIMARY KEY (id)",
        |        ignore_failure = false,
        |        ignore_empty_value = false,
        |        value = "{{id}}"
        |    )
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(sql))

    val pipeline = assertShowPipeline(System.nanoTime(), executeSync("SHOW PIPELINE user_pipeline"))
    pipeline.name shouldBe "user_pipeline"
    pipeline.processors.size shouldBe 2

    val desc = assertQueryRows(System.nanoTime(), executeSync("DESCRIBE PIPELINE user_pipeline"))
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

    val showCreate = executeSync("SHOW CREATE PIPELINE user_pipeline")
    val ddl = assertShowCreate(System.nanoTime(), showCreate)
    ddl should include("CREATE OR REPLACE PIPELINE user_pipeline")

    val pipelines = assertQueryRows(System.nanoTime(), executeSync("SHOW PIPELINES"))
    pipelines should contain(Map("name" -> "user_pipeline", "processors_count" -> 2))
  }

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
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(sql))
  }

  it should "drop a pipeline" in {
    assertDdl(System.nanoTime(), executeSync("DROP PIPELINE IF EXISTS user_pipeline"))
  }

  // =========================================================================
  // 7. Error handling
  // =========================================================================

  behavior of "REPL - SQL error handling"

  it should "return a parsing error for invalid SQL" in {
    val invalidSql = "CREAT TABL missing_keyword"
    val res = executeSync(invalidSql)

    res shouldBe a[ExecutionFailure]
    res.asInstanceOf[ExecutionFailure].error.message should include("Error parsing")
  }

  it should "return an error for unsupported SQL statements" in {
    val unsupportedSql = "GRANT SELECT ON users TO user1"
    val res = executeSync(unsupportedSql)

    res shouldBe a[ExecutionFailure]
    res.asInstanceOf[ExecutionFailure].error.message should include("Error parsing")
  }

  // =========================================================================
  // 8. WATCHERS statements
  // =========================================================================

  behavior of "REPL - WATCHERS statements"

  it should "create, show and drop a watcher" in {
    val createIndex =
      """CREATE TABLE IF NOT EXISTS my_index (
        |  id INT NOT NULL,
        |  content VARCHAR,
        |  PRIMARY KEY (id)
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(createIndex))

    val createWatcherWithInterval =
      """CREATE OR REPLACE WATCHER my_watcher_interval AS
        | EVERY 5 SECONDS
        | FROM my_index WITHIN 1 MINUTE
        | ALWAYS DO
        | log_action LOG "Watcher triggered with {{ctx.payload.hits.total}} hits" AT INFO FOREACH "ctx.payload.hits.hits" LIMIT 500
        | END""".stripMargin

    assertDdl(System.nanoTime(), executeSync(createWatcherWithInterval))

    val watcherStatus = executeSync("SHOW WATCHER STATUS my_watcher_interval")
    watcherStatus match {
      case ExecutionSuccess(QueryRows(rows), _) =>
        rows.size shouldBe 1
        val row = rows.head
        log.info(s"Watcher Status: $row")
        row.get("id") shouldBe Some("my_watcher_interval")
        row.get("is_healthy") shouldBe Some(true)
        row.get("is_operational") shouldBe Some(true)
      case _ => fail("Expected QueryRows result")
    }

    if (supportsQueryWatchers) {
      val watchers = executeSync("SHOW WATCHERS")
      watchers match {
        case ExecutionSuccess(QueryRows(rows), _) =>
          rows.find(row => row.get("id").contains("my_watcher_interval")) match {
            case Some(row) =>
              row.get("is_healthy") shouldBe Some(true)
              row.get("is_operational") shouldBe Some(true)
            case None => fail("Watcher my_watcher_interval not found in SHOW WATCHERS")
          }
        case _ => fail("Expected QueryRows result")
      }
    }

    val createWatcherWithCron =
      """CREATE OR REPLACE WATCHER my_watcher_cron AS
        | AT SCHEDULE '* * * * * ?'
        | WITH INPUTS search_data FROM my_index WITHIN 1 MINUTE, http_data GET "https://jsonplaceholder.typicode.com/todos/1" HEADERS ("Accept" = "application/json") TIMEOUT (connection = "5s", read = "10s")
        | WHEN SCRIPT 'ctx.payload.hits.total > params.threshold' USING LANG 'painless' WITH PARAMS (threshold = 10) RETURNS TRUE
        | DO
        | log_action LOG "Watcher triggered with {{ctx.payload.hits.total}} hits" AT INFO FOREACH "ctx.payload.hits.hits" LIMIT 500
        | END""".stripMargin

    assertDdl(System.nanoTime(), executeSync(createWatcherWithCron))

    assertDdl(System.nanoTime(), executeSync("DROP WATCHER IF EXISTS my_watcher_interval"))
    assertDdl(System.nanoTime(), executeSync("DROP WATCHER IF EXISTS my_watcher_cron"))
  }

  // =========================================================================
  // 9. ENRICH POLICIES statements
  // =========================================================================

  behavior of "REPL - POLICIES statements"

  it should "create, show, execute and drop a policy" in {
    assume(supportsEnrichPolicies, "Enrich policies are not supported in this environment")

    val createIndex =
      """CREATE TABLE IF NOT EXISTS policy_users (
        |  id INT NOT NULL,
        |  name VARCHAR,
        |  age INT,
        |  city VARCHAR
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(createIndex))

    val insert =
      """INSERT INTO policy_users (id, name, age, city) VALUES
        |  (1, 'Alice', 30, 'Paris'),
        |  (2, 'Bob', 25, 'Lyon')""".stripMargin

    assertDml(System.nanoTime(), executeSync(insert))

    // Wait for indexing
    Thread.sleep(1000)

    val createPolicy =
      """CREATE OR REPLACE ENRICH POLICY my_policy
        |FROM policy_users
        |ON id
        |ENRICH name, city
        |WHERE age > 10""".stripMargin

    assertDdl(System.nanoTime(), executeSync(createPolicy))

    var rows = assertQueryRows(System.nanoTime(), executeSync("SHOW ENRICH POLICIES"))
    rows.size shouldBe 1
    var enrichPolicy = rows.head
    enrichPolicy.get("name") shouldBe Some("my_policy")
    enrichPolicy.get("type") shouldBe Some("match")
    enrichPolicy.get("indices") shouldBe Some("policy_users")
    enrichPolicy.get("match_field") shouldBe Some("id")
    enrichPolicy.get("enrich_fields") shouldBe Some("name,city")
    enrichPolicy
      .getOrElse("query", "")
      .asInstanceOf[String]
      .contains("""{"bool":{"filter":[{"range":{"age":{""") shouldBe true

    rows = assertQueryRows(System.nanoTime(), executeSync("SHOW ENRICH POLICY my_policy"))
    rows.size shouldBe 1
    enrichPolicy = rows.head
    enrichPolicy.get("name") shouldBe Some("my_policy")
    enrichPolicy.get("type") shouldBe Some("match")
    enrichPolicy.get("indices") shouldBe Some("policy_users")
    enrichPolicy.get("match_field") shouldBe Some("id")
    enrichPolicy.get("enrich_fields") shouldBe Some("name,city")
    enrichPolicy
      .getOrElse("query", "")
      .asInstanceOf[String]
      .contains("""{"bool":{"filter":[{"range":{"age":{""") shouldBe true

    val executePolicy = "EXECUTE ENRICH POLICY my_policy"
    val result = executeSync(executePolicy)
    result match {
      case ExecutionSuccess(QueryRows(rows), _) =>
        rows.size shouldBe 1
        val row = rows.head
        log.info(s"Policy Execution Result: $row")
        row.getOrElse("policy_name", "") shouldBe "my_policy"
      case _ => fail("Expected QueryRows result")
    }

    assertDdl(System.nanoTime(), executeSync("DROP ENRICH POLICY IF EXISTS my_policy"))
  }

  // =========================================================================
  // 10. Output format tests
  // =========================================================================

  behavior of "REPL - Output formats"

  it should "render results in different formats" in {
    val create =
      """CREATE TABLE IF NOT EXISTS format_test (
        |  id INT NOT NULL,
        |  name VARCHAR
        |)""".stripMargin

    assertDdl(System.nanoTime(), executeSync(create))

    val insert = "INSERT INTO format_test (id, name) VALUES (1, 'Test')"
    assertDml(System.nanoTime(), executeSync(insert))

    // Test ASCII format (default)
    testRepl.setFormat(OutputFormat.Ascii)
    val asciiResult = executeSync("SELECT * FROM format_test")
    asciiResult shouldBe a[ExecutionSuccess]

    // Test JSON format
    testRepl.setFormat(OutputFormat.Json)
    val jsonResult = executeSync("SELECT * FROM format_test")
    jsonResult shouldBe a[ExecutionSuccess]

    // Test CSV format
    testRepl.setFormat(OutputFormat.Csv)
    val csvResult = executeSync("SELECT * FROM format_test")
    csvResult shouldBe a[ExecutionSuccess]

    // Reset to ASCII
    testRepl.setFormat(OutputFormat.Ascii)
  }
}
