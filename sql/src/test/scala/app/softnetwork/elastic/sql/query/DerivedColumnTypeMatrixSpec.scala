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

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.MultiSearch.{SetOperationType, StructField}
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** SCHEMA-6 (D10, AC1-AC3, #419) -- a derived-table or CTE column is typed from its body, by the
  * rule a projection item is typed by (`MultiSearch.fieldType`, #292), once the derived tables are
  * attached (`DerivedTable.attach`, the seam `SearchApi.attachDerived` serves the relational
  * engine). Plain `update` never types one (R-6): arrow's own call stays unattached until
  * SCHEMA-10.
  *
  * 🔴 The ORACLE is DuckDB 1.5.5, MEASURED (its Python client, Python 3.12, one non-null row in a
  * twin table: INT -> INTEGER, KEYWORD -> VARCHAR, VARBINARY -> BLOB, GEO_POINT -> `STRUCT(lat
  * DOUBLE, lon DOUBLE)`, `obj` -> `STRUCT(y INTEGER, b VARCHAR)` in declared order, `ARRAY<INT>` ->
  * `INTEGER[]`, nested -> `STRUCT(q INTEGER, p VARCHAR)[]`), embedded below and NOT derived from
  * core:
  *   - P3: `DESCRIBE` of every item (a bare column of each kind, an alias, literals, casts,
  *     arithmetic, functions, aggregates under `GROUP BY g`, a list column grouped and under MIN)
  *     in every form -- F1 `q.c`, F2 unqualified `c`, F2b `q.*`, F3 WITH, F4 two levels, F5 a
  *     `UNION ALL` body;
  *   - F7: whether DuckDB runs a derived column of T1 beside a plain column of T2 (`UNION ALL`,
  *     `UNION`), and the list-of-objects merge of #419 item 2.
  *
  * Every cell equals DuckDB's answer, except the LISTED divergences, each with its rationale, and
  * the NUMERIC / TEMPORAL cells SCHEMA-5 owns (D12), excluded and counted.
  */
class DerivedColumnTypeMatrixSpec extends AnyFlatSpec with Matchers {

  /** `zs_t` as core reads it; `l BIGINT` added for F7's BIGINT plain column. */
  private val zsT: SchemaTable = SchemaTable(
    "zs_t",
    columns = List(
      Column("n", SQLTypes.Int),
      Column("x", SQLTypes.Double),
      Column("b", SQLTypes.Boolean),
      Column("k", SQLTypes.Keyword),
      Column("s", SQLTypes.Keyword),
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("bin", SQLTypes.VarBinary),
      Column("geo", SQLTypes.GeoPoint),
      Column(
        "obj",
        SQLTypes.Struct,
        multiFields = List(Column("y", SQLTypes.Int), Column("b", SQLTypes.Keyword))
      ),
      Column(
        "items",
        SQLTypes.Array(SQLTypes.Struct),
        multiFields = List(Column("q", SQLTypes.Int), Column("p", SQLTypes.Keyword))
      ),
      Column("arr", SQLTypes.Array(SQLTypes.Int)),
      Column("arrk", SQLTypes.Array(SQLTypes.Keyword)),
      Column("ad", SQLTypes.Array(SQLTypes.Date)),
      Column("g", SQLTypes.Keyword),
      Column("l", SQLTypes.BigInt)
    )
  ).update()

  private val zsU: SchemaTable = SchemaTable(
    "zs_u",
    columns = List(
      Column(
        "items",
        SQLTypes.Array(SQLTypes.Struct),
        multiFields = List(Column("r", SQLTypes.Int))
      )
    )
  ).update()

  private val lookup: Seq[String] => Option[SchemaTable] =
    sources => Map("zs_t" -> zsT, "zs_u" -> zsU).get(sources.mkString(","))

  /** MEASURED DuckDB 1.5.5: kind, form or operator, statement, `DESCRIBE` type or FAILS.
    */
  private val measured: Seq[(String, String, String, String)] =
    """
      |P3	F1	SELECT q.c FROM (SELECT n AS c FROM zs_t) q	INTEGER
      |P3	F2	SELECT c FROM (SELECT n AS c FROM zs_t) q	INTEGER
      |P3	F2b	SELECT q.* FROM (SELECT n AS c FROM zs_t) q	INTEGER
      |P3	F3	WITH q AS (SELECT n AS c FROM zs_t) SELECT q.c FROM q	INTEGER
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT n AS c FROM zs_t) q) q2	INTEGER
      |P3	F5	SELECT q.c FROM (SELECT n AS c FROM zs_t UNION ALL SELECT n AS c FROM zs_t) q	INTEGER
      |P3	F1	SELECT q.c FROM (SELECT x AS c FROM zs_t) q	DOUBLE
      |P3	F2	SELECT c FROM (SELECT x AS c FROM zs_t) q	DOUBLE
      |P3	F2b	SELECT q.* FROM (SELECT x AS c FROM zs_t) q	DOUBLE
      |P3	F3	WITH q AS (SELECT x AS c FROM zs_t) SELECT q.c FROM q	DOUBLE
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT x AS c FROM zs_t) q) q2	DOUBLE
      |P3	F5	SELECT q.c FROM (SELECT x AS c FROM zs_t UNION ALL SELECT x AS c FROM zs_t) q	DOUBLE
      |P3	F1	SELECT q.c FROM (SELECT b AS c FROM zs_t) q	BOOLEAN
      |P3	F2	SELECT c FROM (SELECT b AS c FROM zs_t) q	BOOLEAN
      |P3	F2b	SELECT q.* FROM (SELECT b AS c FROM zs_t) q	BOOLEAN
      |P3	F3	WITH q AS (SELECT b AS c FROM zs_t) SELECT q.c FROM q	BOOLEAN
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT b AS c FROM zs_t) q) q2	BOOLEAN
      |P3	F5	SELECT q.c FROM (SELECT b AS c FROM zs_t UNION ALL SELECT b AS c FROM zs_t) q	BOOLEAN
      |P3	F1	SELECT q.c FROM (SELECT k AS c FROM zs_t) q	VARCHAR
      |P3	F2	SELECT c FROM (SELECT k AS c FROM zs_t) q	VARCHAR
      |P3	F2b	SELECT q.* FROM (SELECT k AS c FROM zs_t) q	VARCHAR
      |P3	F3	WITH q AS (SELECT k AS c FROM zs_t) SELECT q.c FROM q	VARCHAR
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT k AS c FROM zs_t) q) q2	VARCHAR
      |P3	F5	SELECT q.c FROM (SELECT k AS c FROM zs_t UNION ALL SELECT k AS c FROM zs_t) q	VARCHAR
      |P3	F1	SELECT q.c FROM (SELECT s AS c FROM zs_t) q	VARCHAR
      |P3	F2	SELECT c FROM (SELECT s AS c FROM zs_t) q	VARCHAR
      |P3	F2b	SELECT q.* FROM (SELECT s AS c FROM zs_t) q	VARCHAR
      |P3	F3	WITH q AS (SELECT s AS c FROM zs_t) SELECT q.c FROM q	VARCHAR
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT s AS c FROM zs_t) q) q2	VARCHAR
      |P3	F5	SELECT q.c FROM (SELECT s AS c FROM zs_t UNION ALL SELECT s AS c FROM zs_t) q	VARCHAR
      |P3	F1	SELECT q.c FROM (SELECT d AS c FROM zs_t) q	DATE
      |P3	F2	SELECT c FROM (SELECT d AS c FROM zs_t) q	DATE
      |P3	F2b	SELECT q.* FROM (SELECT d AS c FROM zs_t) q	DATE
      |P3	F3	WITH q AS (SELECT d AS c FROM zs_t) SELECT q.c FROM q	DATE
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT d AS c FROM zs_t) q) q2	DATE
      |P3	F5	SELECT q.c FROM (SELECT d AS c FROM zs_t UNION ALL SELECT d AS c FROM zs_t) q	DATE
      |P3	F1	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q	TIMESTAMP
      |P3	F2	SELECT c FROM (SELECT ts AS c FROM zs_t) q	TIMESTAMP
      |P3	F2b	SELECT q.* FROM (SELECT ts AS c FROM zs_t) q	TIMESTAMP
      |P3	F3	WITH q AS (SELECT ts AS c FROM zs_t) SELECT q.c FROM q	TIMESTAMP
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT ts AS c FROM zs_t) q) q2	TIMESTAMP
      |P3	F5	SELECT q.c FROM (SELECT ts AS c FROM zs_t UNION ALL SELECT ts AS c FROM zs_t) q	TIMESTAMP
      |P3	F1	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q	BLOB
      |P3	F2	SELECT c FROM (SELECT bin AS c FROM zs_t) q	BLOB
      |P3	F2b	SELECT q.* FROM (SELECT bin AS c FROM zs_t) q	BLOB
      |P3	F3	WITH q AS (SELECT bin AS c FROM zs_t) SELECT q.c FROM q	BLOB
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT bin AS c FROM zs_t) q) q2	BLOB
      |P3	F5	SELECT q.c FROM (SELECT bin AS c FROM zs_t UNION ALL SELECT bin AS c FROM zs_t) q	BLOB
      |P3	F1	SELECT q.c FROM (SELECT geo AS c FROM zs_t) q	STRUCT(lat DOUBLE, lon DOUBLE)
      |P3	F2	SELECT c FROM (SELECT geo AS c FROM zs_t) q	STRUCT(lat DOUBLE, lon DOUBLE)
      |P3	F2b	SELECT q.* FROM (SELECT geo AS c FROM zs_t) q	STRUCT(lat DOUBLE, lon DOUBLE)
      |P3	F3	WITH q AS (SELECT geo AS c FROM zs_t) SELECT q.c FROM q	STRUCT(lat DOUBLE, lon DOUBLE)
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT geo AS c FROM zs_t) q) q2	STRUCT(lat DOUBLE, lon DOUBLE)
      |P3	F5	SELECT q.c FROM (SELECT geo AS c FROM zs_t UNION ALL SELECT geo AS c FROM zs_t) q	STRUCT(lat DOUBLE, lon DOUBLE)
      |P3	F1	SELECT q.c FROM (SELECT obj AS c FROM zs_t) q	STRUCT(y INTEGER, b VARCHAR)
      |P3	F2	SELECT c FROM (SELECT obj AS c FROM zs_t) q	STRUCT(y INTEGER, b VARCHAR)
      |P3	F2b	SELECT q.* FROM (SELECT obj AS c FROM zs_t) q	STRUCT(y INTEGER, b VARCHAR)
      |P3	F3	WITH q AS (SELECT obj AS c FROM zs_t) SELECT q.c FROM q	STRUCT(y INTEGER, b VARCHAR)
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT obj AS c FROM zs_t) q) q2	STRUCT(y INTEGER, b VARCHAR)
      |P3	F5	SELECT q.c FROM (SELECT obj AS c FROM zs_t UNION ALL SELECT obj AS c FROM zs_t) q	STRUCT(y INTEGER, b VARCHAR)
      |P3	F1	SELECT q.c FROM (SELECT items AS c FROM zs_t) q	STRUCT(q INTEGER, p VARCHAR)[]
      |P3	F2	SELECT c FROM (SELECT items AS c FROM zs_t) q	STRUCT(q INTEGER, p VARCHAR)[]
      |P3	F2b	SELECT q.* FROM (SELECT items AS c FROM zs_t) q	STRUCT(q INTEGER, p VARCHAR)[]
      |P3	F3	WITH q AS (SELECT items AS c FROM zs_t) SELECT q.c FROM q	STRUCT(q INTEGER, p VARCHAR)[]
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT items AS c FROM zs_t) q) q2	STRUCT(q INTEGER, p VARCHAR)[]
      |P3	F5	SELECT q.c FROM (SELECT items AS c FROM zs_t UNION ALL SELECT items AS c FROM zs_t) q	STRUCT(q INTEGER, p VARCHAR)[]
      |P3	F1	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q	INTEGER[]
      |P3	F2	SELECT c FROM (SELECT arr AS c FROM zs_t) q	INTEGER[]
      |P3	F2b	SELECT q.* FROM (SELECT arr AS c FROM zs_t) q	INTEGER[]
      |P3	F3	WITH q AS (SELECT arr AS c FROM zs_t) SELECT q.c FROM q	INTEGER[]
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT arr AS c FROM zs_t) q) q2	INTEGER[]
      |P3	F5	SELECT q.c FROM (SELECT arr AS c FROM zs_t UNION ALL SELECT arr AS c FROM zs_t) q	INTEGER[]
      |P3	F1	SELECT q.c FROM (SELECT arrk AS c FROM zs_t) q	VARCHAR[]
      |P3	F2	SELECT c FROM (SELECT arrk AS c FROM zs_t) q	VARCHAR[]
      |P3	F2b	SELECT q.* FROM (SELECT arrk AS c FROM zs_t) q	VARCHAR[]
      |P3	F3	WITH q AS (SELECT arrk AS c FROM zs_t) SELECT q.c FROM q	VARCHAR[]
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT arrk AS c FROM zs_t) q) q2	VARCHAR[]
      |P3	F5	SELECT q.c FROM (SELECT arrk AS c FROM zs_t UNION ALL SELECT arrk AS c FROM zs_t) q	VARCHAR[]
      |P3	F1	SELECT q.c FROM (SELECT ad AS c FROM zs_t) q	DATE[]
      |P3	F2	SELECT c FROM (SELECT ad AS c FROM zs_t) q	DATE[]
      |P3	F2b	SELECT q.* FROM (SELECT ad AS c FROM zs_t) q	DATE[]
      |P3	F3	WITH q AS (SELECT ad AS c FROM zs_t) SELECT q.c FROM q	DATE[]
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT ad AS c FROM zs_t) q) q2	DATE[]
      |P3	F5	SELECT q.c FROM (SELECT ad AS c FROM zs_t UNION ALL SELECT ad AS c FROM zs_t) q	DATE[]
      |P3	F1	SELECT q.c FROM (SELECT g AS c FROM zs_t) q	VARCHAR
      |P3	F2	SELECT c FROM (SELECT g AS c FROM zs_t) q	VARCHAR
      |P3	F2b	SELECT q.* FROM (SELECT g AS c FROM zs_t) q	VARCHAR
      |P3	F3	WITH q AS (SELECT g AS c FROM zs_t) SELECT q.c FROM q	VARCHAR
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT g AS c FROM zs_t) q) q2	VARCHAR
      |P3	F5	SELECT q.c FROM (SELECT g AS c FROM zs_t UNION ALL SELECT g AS c FROM zs_t) q	VARCHAR
      |P3	F1	SELECT q.c FROM (SELECT l AS c FROM zs_t) q	BIGINT
      |P3	F2	SELECT c FROM (SELECT l AS c FROM zs_t) q	BIGINT
      |P3	F2b	SELECT q.* FROM (SELECT l AS c FROM zs_t) q	BIGINT
      |P3	F3	WITH q AS (SELECT l AS c FROM zs_t) SELECT q.c FROM q	BIGINT
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT l AS c FROM zs_t) q) q2	BIGINT
      |P3	F5	SELECT q.c FROM (SELECT l AS c FROM zs_t UNION ALL SELECT l AS c FROM zs_t) q	BIGINT
      |P3	F1	SELECT q.c FROM (SELECT 1 AS c FROM zs_t) q	INTEGER
      |P3	F2	SELECT c FROM (SELECT 1 AS c FROM zs_t) q	INTEGER
      |P3	F2b	SELECT q.* FROM (SELECT 1 AS c FROM zs_t) q	INTEGER
      |P3	F3	WITH q AS (SELECT 1 AS c FROM zs_t) SELECT q.c FROM q	INTEGER
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT 1 AS c FROM zs_t) q) q2	INTEGER
      |P3	F5	SELECT q.c FROM (SELECT 1 AS c FROM zs_t UNION ALL SELECT 1 AS c FROM zs_t) q	INTEGER
      |P3	F1	SELECT q.c FROM (SELECT 'a' AS c FROM zs_t) q	VARCHAR
      |P3	F2	SELECT c FROM (SELECT 'a' AS c FROM zs_t) q	VARCHAR
      |P3	F2b	SELECT q.* FROM (SELECT 'a' AS c FROM zs_t) q	VARCHAR
      |P3	F3	WITH q AS (SELECT 'a' AS c FROM zs_t) SELECT q.c FROM q	VARCHAR
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT 'a' AS c FROM zs_t) q) q2	VARCHAR
      |P3	F5	SELECT q.c FROM (SELECT 'a' AS c FROM zs_t UNION ALL SELECT 'a' AS c FROM zs_t) q	VARCHAR
      |P3	F1	SELECT q.c FROM (SELECT 1.5 AS c FROM zs_t) q	DECIMAL(2,1)
      |P3	F2	SELECT c FROM (SELECT 1.5 AS c FROM zs_t) q	DECIMAL(2,1)
      |P3	F2b	SELECT q.* FROM (SELECT 1.5 AS c FROM zs_t) q	DECIMAL(2,1)
      |P3	F3	WITH q AS (SELECT 1.5 AS c FROM zs_t) SELECT q.c FROM q	DECIMAL(2,1)
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT 1.5 AS c FROM zs_t) q) q2	DECIMAL(2,1)
      |P3	F5	SELECT q.c FROM (SELECT 1.5 AS c FROM zs_t UNION ALL SELECT 1.5 AS c FROM zs_t) q	DECIMAL(2,1)
      |P3	F1	SELECT q.c FROM (SELECT TRUE AS c FROM zs_t) q	BOOLEAN
      |P3	F2	SELECT c FROM (SELECT TRUE AS c FROM zs_t) q	BOOLEAN
      |P3	F2b	SELECT q.* FROM (SELECT TRUE AS c FROM zs_t) q	BOOLEAN
      |P3	F3	WITH q AS (SELECT TRUE AS c FROM zs_t) SELECT q.c FROM q	BOOLEAN
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT TRUE AS c FROM zs_t) q) q2	BOOLEAN
      |P3	F5	SELECT q.c FROM (SELECT TRUE AS c FROM zs_t UNION ALL SELECT TRUE AS c FROM zs_t) q	BOOLEAN
      |P3	F1	SELECT q.c FROM (SELECT NULL AS c FROM zs_t) q	INTEGER
      |P3	F2	SELECT c FROM (SELECT NULL AS c FROM zs_t) q	INTEGER
      |P3	F2b	SELECT q.* FROM (SELECT NULL AS c FROM zs_t) q	INTEGER
      |P3	F3	WITH q AS (SELECT NULL AS c FROM zs_t) SELECT q.c FROM q	INTEGER
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT NULL AS c FROM zs_t) q) q2	INTEGER
      |P3	F5	SELECT q.c FROM (SELECT NULL AS c FROM zs_t UNION ALL SELECT NULL AS c FROM zs_t) q	INTEGER
      |P3	F1	SELECT q.c FROM (SELECT CAST(n AS BIGINT) AS c FROM zs_t) q	BIGINT
      |P3	F2	SELECT c FROM (SELECT CAST(n AS BIGINT) AS c FROM zs_t) q	BIGINT
      |P3	F2b	SELECT q.* FROM (SELECT CAST(n AS BIGINT) AS c FROM zs_t) q	BIGINT
      |P3	F3	WITH q AS (SELECT CAST(n AS BIGINT) AS c FROM zs_t) SELECT q.c FROM q	BIGINT
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT CAST(n AS BIGINT) AS c FROM zs_t) q) q2	BIGINT
      |P3	F5	SELECT q.c FROM (SELECT CAST(n AS BIGINT) AS c FROM zs_t UNION ALL SELECT CAST(n AS BIGINT) AS c FROM zs_t) q	BIGINT
      |P3	F1	SELECT q.c FROM (SELECT n + 1 AS c FROM zs_t) q	INTEGER
      |P3	F2	SELECT c FROM (SELECT n + 1 AS c FROM zs_t) q	INTEGER
      |P3	F2b	SELECT q.* FROM (SELECT n + 1 AS c FROM zs_t) q	INTEGER
      |P3	F3	WITH q AS (SELECT n + 1 AS c FROM zs_t) SELECT q.c FROM q	INTEGER
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT n + 1 AS c FROM zs_t) q) q2	INTEGER
      |P3	F5	SELECT q.c FROM (SELECT n + 1 AS c FROM zs_t UNION ALL SELECT n + 1 AS c FROM zs_t) q	INTEGER
      |P3	F1	SELECT q.c FROM (SELECT CONCAT(k, 'z') AS c FROM zs_t) q	VARCHAR
      |P3	F2	SELECT c FROM (SELECT CONCAT(k, 'z') AS c FROM zs_t) q	VARCHAR
      |P3	F2b	SELECT q.* FROM (SELECT CONCAT(k, 'z') AS c FROM zs_t) q	VARCHAR
      |P3	F3	WITH q AS (SELECT CONCAT(k, 'z') AS c FROM zs_t) SELECT q.c FROM q	VARCHAR
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT CONCAT(k, 'z') AS c FROM zs_t) q) q2	VARCHAR
      |P3	F5	SELECT q.c FROM (SELECT CONCAT(k, 'z') AS c FROM zs_t UNION ALL SELECT CONCAT(k, 'z') AS c FROM zs_t) q	VARCHAR
      |P3	F1	SELECT q.c FROM (SELECT CASE WHEN b THEN n ELSE 0 END AS c FROM zs_t) q	INTEGER
      |P3	F2	SELECT c FROM (SELECT CASE WHEN b THEN n ELSE 0 END AS c FROM zs_t) q	INTEGER
      |P3	F2b	SELECT q.* FROM (SELECT CASE WHEN b THEN n ELSE 0 END AS c FROM zs_t) q	INTEGER
      |P3	F3	WITH q AS (SELECT CASE WHEN b THEN n ELSE 0 END AS c FROM zs_t) SELECT q.c FROM q	INTEGER
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT CASE WHEN b THEN n ELSE 0 END AS c FROM zs_t) q) q2	INTEGER
      |P3	F5	SELECT q.c FROM (SELECT CASE WHEN b THEN n ELSE 0 END AS c FROM zs_t UNION ALL SELECT CASE WHEN b THEN n ELSE 0 END AS c FROM zs_t) q	INTEGER
      |P3	F1	SELECT q.c FROM (SELECT COALESCE(n, 0) AS c FROM zs_t) q	INTEGER
      |P3	F2	SELECT c FROM (SELECT COALESCE(n, 0) AS c FROM zs_t) q	INTEGER
      |P3	F2b	SELECT q.* FROM (SELECT COALESCE(n, 0) AS c FROM zs_t) q	INTEGER
      |P3	F3	WITH q AS (SELECT COALESCE(n, 0) AS c FROM zs_t) SELECT q.c FROM q	INTEGER
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT COALESCE(n, 0) AS c FROM zs_t) q) q2	INTEGER
      |P3	F5	SELECT q.c FROM (SELECT COALESCE(n, 0) AS c FROM zs_t UNION ALL SELECT COALESCE(n, 0) AS c FROM zs_t) q	INTEGER
      |P3	F1	SELECT q.c FROM (SELECT COUNT(*) AS c FROM zs_t GROUP BY g) q	BIGINT
      |P3	F2	SELECT c FROM (SELECT COUNT(*) AS c FROM zs_t GROUP BY g) q	BIGINT
      |P3	F2b	SELECT q.* FROM (SELECT COUNT(*) AS c FROM zs_t GROUP BY g) q	BIGINT
      |P3	F3	WITH q AS (SELECT COUNT(*) AS c FROM zs_t GROUP BY g) SELECT q.c FROM q	BIGINT
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT COUNT(*) AS c FROM zs_t GROUP BY g) q) q2	BIGINT
      |P3	F5	SELECT q.c FROM (SELECT COUNT(*) AS c FROM zs_t GROUP BY g UNION ALL SELECT COUNT(*) AS c FROM zs_t GROUP BY g) q	BIGINT
      |P3	F1	SELECT q.c FROM (SELECT SUM(n) AS c FROM zs_t GROUP BY g) q	HUGEINT
      |P3	F2	SELECT c FROM (SELECT SUM(n) AS c FROM zs_t GROUP BY g) q	HUGEINT
      |P3	F2b	SELECT q.* FROM (SELECT SUM(n) AS c FROM zs_t GROUP BY g) q	HUGEINT
      |P3	F3	WITH q AS (SELECT SUM(n) AS c FROM zs_t GROUP BY g) SELECT q.c FROM q	HUGEINT
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT SUM(n) AS c FROM zs_t GROUP BY g) q) q2	HUGEINT
      |P3	F5	SELECT q.c FROM (SELECT SUM(n) AS c FROM zs_t GROUP BY g UNION ALL SELECT SUM(n) AS c FROM zs_t GROUP BY g) q	HUGEINT
      |P3	F1	SELECT q.c FROM (SELECT AVG(n) AS c FROM zs_t GROUP BY g) q	DOUBLE
      |P3	F2	SELECT c FROM (SELECT AVG(n) AS c FROM zs_t GROUP BY g) q	DOUBLE
      |P3	F2b	SELECT q.* FROM (SELECT AVG(n) AS c FROM zs_t GROUP BY g) q	DOUBLE
      |P3	F3	WITH q AS (SELECT AVG(n) AS c FROM zs_t GROUP BY g) SELECT q.c FROM q	DOUBLE
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT AVG(n) AS c FROM zs_t GROUP BY g) q) q2	DOUBLE
      |P3	F5	SELECT q.c FROM (SELECT AVG(n) AS c FROM zs_t GROUP BY g UNION ALL SELECT AVG(n) AS c FROM zs_t GROUP BY g) q	DOUBLE
      |P3	F1	SELECT q.c FROM (SELECT MIN(d) AS c FROM zs_t GROUP BY g) q	DATE
      |P3	F2	SELECT c FROM (SELECT MIN(d) AS c FROM zs_t GROUP BY g) q	DATE
      |P3	F2b	SELECT q.* FROM (SELECT MIN(d) AS c FROM zs_t GROUP BY g) q	DATE
      |P3	F3	WITH q AS (SELECT MIN(d) AS c FROM zs_t GROUP BY g) SELECT q.c FROM q	DATE
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT MIN(d) AS c FROM zs_t GROUP BY g) q) q2	DATE
      |P3	F5	SELECT q.c FROM (SELECT MIN(d) AS c FROM zs_t GROUP BY g UNION ALL SELECT MIN(d) AS c FROM zs_t GROUP BY g) q	DATE
      |P3	F1	SELECT q.c FROM (SELECT MAX(k) AS c FROM zs_t GROUP BY g) q	VARCHAR
      |P3	F2	SELECT c FROM (SELECT MAX(k) AS c FROM zs_t GROUP BY g) q	VARCHAR
      |P3	F2b	SELECT q.* FROM (SELECT MAX(k) AS c FROM zs_t GROUP BY g) q	VARCHAR
      |P3	F3	WITH q AS (SELECT MAX(k) AS c FROM zs_t GROUP BY g) SELECT q.c FROM q	VARCHAR
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT MAX(k) AS c FROM zs_t GROUP BY g) q) q2	VARCHAR
      |P3	F5	SELECT q.c FROM (SELECT MAX(k) AS c FROM zs_t GROUP BY g UNION ALL SELECT MAX(k) AS c FROM zs_t GROUP BY g) q	VARCHAR
      |P3	F1	SELECT q.c FROM (SELECT ARRAY_AGG(k) AS c FROM zs_t GROUP BY g) q	VARCHAR[]
      |P3	F2	SELECT c FROM (SELECT ARRAY_AGG(k) AS c FROM zs_t GROUP BY g) q	VARCHAR[]
      |P3	F2b	SELECT q.* FROM (SELECT ARRAY_AGG(k) AS c FROM zs_t GROUP BY g) q	VARCHAR[]
      |P3	F3	WITH q AS (SELECT ARRAY_AGG(k) AS c FROM zs_t GROUP BY g) SELECT q.c FROM q	VARCHAR[]
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT ARRAY_AGG(k) AS c FROM zs_t GROUP BY g) q) q2	VARCHAR[]
      |P3	F5	SELECT q.c FROM (SELECT ARRAY_AGG(k) AS c FROM zs_t GROUP BY g UNION ALL SELECT ARRAY_AGG(k) AS c FROM zs_t GROUP BY g) q	VARCHAR[]
      |P3	F1	SELECT q.c FROM (SELECT arr AS c FROM zs_t GROUP BY arr) q	INTEGER[]
      |P3	F2	SELECT c FROM (SELECT arr AS c FROM zs_t GROUP BY arr) q	INTEGER[]
      |P3	F2b	SELECT q.* FROM (SELECT arr AS c FROM zs_t GROUP BY arr) q	INTEGER[]
      |P3	F3	WITH q AS (SELECT arr AS c FROM zs_t GROUP BY arr) SELECT q.c FROM q	INTEGER[]
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT arr AS c FROM zs_t GROUP BY arr) q) q2	INTEGER[]
      |P3	F5	SELECT q.c FROM (SELECT arr AS c FROM zs_t GROUP BY arr UNION ALL SELECT arr AS c FROM zs_t GROUP BY arr) q	INTEGER[]
      |P3	F1	SELECT q.c FROM (SELECT ad AS c FROM zs_t GROUP BY ad) q	DATE[]
      |P3	F2	SELECT c FROM (SELECT ad AS c FROM zs_t GROUP BY ad) q	DATE[]
      |P3	F2b	SELECT q.* FROM (SELECT ad AS c FROM zs_t GROUP BY ad) q	DATE[]
      |P3	F3	WITH q AS (SELECT ad AS c FROM zs_t GROUP BY ad) SELECT q.c FROM q	DATE[]
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT ad AS c FROM zs_t GROUP BY ad) q) q2	DATE[]
      |P3	F5	SELECT q.c FROM (SELECT ad AS c FROM zs_t GROUP BY ad UNION ALL SELECT ad AS c FROM zs_t GROUP BY ad) q	DATE[]
      |P3	F1	SELECT q.c FROM (SELECT MIN(arr) AS c FROM zs_t GROUP BY g) q	INTEGER[]
      |P3	F2	SELECT c FROM (SELECT MIN(arr) AS c FROM zs_t GROUP BY g) q	INTEGER[]
      |P3	F2b	SELECT q.* FROM (SELECT MIN(arr) AS c FROM zs_t GROUP BY g) q	INTEGER[]
      |P3	F3	WITH q AS (SELECT MIN(arr) AS c FROM zs_t GROUP BY g) SELECT q.c FROM q	INTEGER[]
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT MIN(arr) AS c FROM zs_t GROUP BY g) q) q2	INTEGER[]
      |P3	F5	SELECT q.c FROM (SELECT MIN(arr) AS c FROM zs_t GROUP BY g UNION ALL SELECT MIN(arr) AS c FROM zs_t GROUP BY g) q	INTEGER[]
      |P3	F1	SELECT q.c FROM (SELECT MIN(ad) AS c FROM zs_t GROUP BY g) q	DATE[]
      |P3	F2	SELECT c FROM (SELECT MIN(ad) AS c FROM zs_t GROUP BY g) q	DATE[]
      |P3	F2b	SELECT q.* FROM (SELECT MIN(ad) AS c FROM zs_t GROUP BY g) q	DATE[]
      |P3	F3	WITH q AS (SELECT MIN(ad) AS c FROM zs_t GROUP BY g) SELECT q.c FROM q	DATE[]
      |P3	F4	SELECT q2.c FROM (SELECT q.c FROM (SELECT MIN(ad) AS c FROM zs_t GROUP BY g) q) q2	DATE[]
      |P3	F5	SELECT q.c FROM (SELECT MIN(ad) AS c FROM zs_t GROUP BY g UNION ALL SELECT MIN(ad) AS c FROM zs_t GROUP BY g) q	DATE[]
      |F7	UNION ALL	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION ALL SELECT n FROM zs_t	INTEGER
      |F7	UNION ALL	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION ALL SELECT l FROM zs_t	BIGINT
      |F7	UNION ALL	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION ALL SELECT x FROM zs_t	DOUBLE
      |F7	UNION ALL	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION ALL SELECT b FROM zs_t	INTEGER
      |F7	UNION ALL	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION ALL SELECT d FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION ALL SELECT ts FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION ALL SELECT k FROM zs_t	VARCHAR
      |F7	UNION ALL	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION ALL SELECT bin FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION ALL SELECT arr FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION ALL SELECT n FROM zs_t	BIGINT
      |F7	UNION ALL	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION ALL SELECT l FROM zs_t	BIGINT
      |F7	UNION ALL	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION ALL SELECT x FROM zs_t	DOUBLE
      |F7	UNION ALL	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION ALL SELECT b FROM zs_t	BIGINT
      |F7	UNION ALL	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION ALL SELECT d FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION ALL SELECT ts FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION ALL SELECT k FROM zs_t	VARCHAR
      |F7	UNION ALL	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION ALL SELECT bin FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION ALL SELECT arr FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION ALL SELECT n FROM zs_t	DOUBLE
      |F7	UNION ALL	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION ALL SELECT l FROM zs_t	DOUBLE
      |F7	UNION ALL	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION ALL SELECT x FROM zs_t	DOUBLE
      |F7	UNION ALL	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION ALL SELECT b FROM zs_t	DOUBLE
      |F7	UNION ALL	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION ALL SELECT d FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION ALL SELECT ts FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION ALL SELECT k FROM zs_t	VARCHAR
      |F7	UNION ALL	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION ALL SELECT bin FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION ALL SELECT arr FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION ALL SELECT n FROM zs_t	INTEGER
      |F7	UNION ALL	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION ALL SELECT l FROM zs_t	BIGINT
      |F7	UNION ALL	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION ALL SELECT x FROM zs_t	DOUBLE
      |F7	UNION ALL	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION ALL SELECT b FROM zs_t	BOOLEAN
      |F7	UNION ALL	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION ALL SELECT d FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION ALL SELECT ts FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION ALL SELECT k FROM zs_t	VARCHAR
      |F7	UNION ALL	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION ALL SELECT bin FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION ALL SELECT arr FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION ALL SELECT n FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION ALL SELECT l FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION ALL SELECT x FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION ALL SELECT b FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION ALL SELECT d FROM zs_t	DATE
      |F7	UNION ALL	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION ALL SELECT ts FROM zs_t	TIMESTAMP
      |F7	UNION ALL	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION ALL SELECT k FROM zs_t	VARCHAR
      |F7	UNION ALL	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION ALL SELECT bin FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION ALL SELECT arr FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION ALL SELECT n FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION ALL SELECT l FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION ALL SELECT x FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION ALL SELECT b FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION ALL SELECT d FROM zs_t	TIMESTAMP
      |F7	UNION ALL	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION ALL SELECT ts FROM zs_t	TIMESTAMP
      |F7	UNION ALL	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION ALL SELECT k FROM zs_t	VARCHAR
      |F7	UNION ALL	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION ALL SELECT bin FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION ALL SELECT arr FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION ALL SELECT n FROM zs_t	VARCHAR
      |F7	UNION ALL	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION ALL SELECT l FROM zs_t	VARCHAR
      |F7	UNION ALL	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION ALL SELECT x FROM zs_t	VARCHAR
      |F7	UNION ALL	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION ALL SELECT b FROM zs_t	VARCHAR
      |F7	UNION ALL	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION ALL SELECT d FROM zs_t	VARCHAR
      |F7	UNION ALL	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION ALL SELECT ts FROM zs_t	VARCHAR
      |F7	UNION ALL	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION ALL SELECT k FROM zs_t	VARCHAR
      |F7	UNION ALL	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION ALL SELECT bin FROM zs_t	BLOB
      |F7	UNION ALL	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION ALL SELECT arr FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION ALL SELECT n FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION ALL SELECT l FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION ALL SELECT x FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION ALL SELECT b FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION ALL SELECT d FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION ALL SELECT ts FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION ALL SELECT k FROM zs_t	BLOB
      |F7	UNION ALL	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION ALL SELECT bin FROM zs_t	BLOB
      |F7	UNION ALL	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION ALL SELECT arr FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION ALL SELECT n FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION ALL SELECT l FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION ALL SELECT x FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION ALL SELECT b FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION ALL SELECT d FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION ALL SELECT ts FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION ALL SELECT k FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION ALL SELECT bin FROM zs_t	FAILS
      |F7	UNION ALL	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION ALL SELECT arr FROM zs_t	INTEGER[]
      |F7	UNION	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION SELECT n FROM zs_t	INTEGER
      |F7	UNION	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION SELECT l FROM zs_t	BIGINT
      |F7	UNION	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION SELECT x FROM zs_t	DOUBLE
      |F7	UNION	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION SELECT b FROM zs_t	INTEGER
      |F7	UNION	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION SELECT d FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION SELECT ts FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION SELECT k FROM zs_t	VARCHAR
      |F7	UNION	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION SELECT bin FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT n AS c FROM zs_t) q UNION SELECT arr FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION SELECT n FROM zs_t	BIGINT
      |F7	UNION	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION SELECT l FROM zs_t	BIGINT
      |F7	UNION	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION SELECT x FROM zs_t	DOUBLE
      |F7	UNION	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION SELECT b FROM zs_t	BIGINT
      |F7	UNION	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION SELECT d FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION SELECT ts FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION SELECT k FROM zs_t	VARCHAR
      |F7	UNION	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION SELECT bin FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT l AS c FROM zs_t) q UNION SELECT arr FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION SELECT n FROM zs_t	DOUBLE
      |F7	UNION	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION SELECT l FROM zs_t	DOUBLE
      |F7	UNION	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION SELECT x FROM zs_t	DOUBLE
      |F7	UNION	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION SELECT b FROM zs_t	DOUBLE
      |F7	UNION	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION SELECT d FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION SELECT ts FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION SELECT k FROM zs_t	VARCHAR
      |F7	UNION	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION SELECT bin FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT x AS c FROM zs_t) q UNION SELECT arr FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION SELECT n FROM zs_t	INTEGER
      |F7	UNION	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION SELECT l FROM zs_t	BIGINT
      |F7	UNION	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION SELECT x FROM zs_t	DOUBLE
      |F7	UNION	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION SELECT b FROM zs_t	BOOLEAN
      |F7	UNION	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION SELECT d FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION SELECT ts FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION SELECT k FROM zs_t	VARCHAR
      |F7	UNION	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION SELECT bin FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT b AS c FROM zs_t) q UNION SELECT arr FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION SELECT n FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION SELECT l FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION SELECT x FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION SELECT b FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION SELECT d FROM zs_t	DATE
      |F7	UNION	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION SELECT ts FROM zs_t	TIMESTAMP
      |F7	UNION	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION SELECT k FROM zs_t	VARCHAR
      |F7	UNION	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION SELECT bin FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT d AS c FROM zs_t) q UNION SELECT arr FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION SELECT n FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION SELECT l FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION SELECT x FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION SELECT b FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION SELECT d FROM zs_t	TIMESTAMP
      |F7	UNION	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION SELECT ts FROM zs_t	TIMESTAMP
      |F7	UNION	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION SELECT k FROM zs_t	VARCHAR
      |F7	UNION	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION SELECT bin FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT ts AS c FROM zs_t) q UNION SELECT arr FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION SELECT n FROM zs_t	VARCHAR
      |F7	UNION	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION SELECT l FROM zs_t	VARCHAR
      |F7	UNION	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION SELECT x FROM zs_t	VARCHAR
      |F7	UNION	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION SELECT b FROM zs_t	VARCHAR
      |F7	UNION	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION SELECT d FROM zs_t	VARCHAR
      |F7	UNION	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION SELECT ts FROM zs_t	VARCHAR
      |F7	UNION	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION SELECT k FROM zs_t	VARCHAR
      |F7	UNION	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION SELECT bin FROM zs_t	BLOB
      |F7	UNION	SELECT q.c FROM (SELECT k AS c FROM zs_t) q UNION SELECT arr FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION SELECT n FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION SELECT l FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION SELECT x FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION SELECT b FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION SELECT d FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION SELECT ts FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION SELECT k FROM zs_t	BLOB
      |F7	UNION	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION SELECT bin FROM zs_t	BLOB
      |F7	UNION	SELECT q.c FROM (SELECT bin AS c FROM zs_t) q UNION SELECT arr FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION SELECT n FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION SELECT l FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION SELECT x FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION SELECT b FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION SELECT d FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION SELECT ts FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION SELECT k FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION SELECT bin FROM zs_t	FAILS
      |F7	UNION	SELECT q.c FROM (SELECT arr AS c FROM zs_t) q UNION SELECT arr FROM zs_t	INTEGER[]
      |F7	UNION ALL	SELECT q.items FROM (SELECT items FROM zs_t) q UNION ALL SELECT items FROM zs_u	STRUCT(q INTEGER, p VARCHAR, r INTEGER)[]
      |""".stripMargin.split("\n").toSeq.filter(_.nonEmpty).map { line =>
      val Array(kind, form, sql, t) = line.split("\t")
      (kind, form, sql, t)
    }

  /** A core type spelled as DuckDB spells it. */
  private def duck(t: SetOperationType): String =
    t.sqlType match {
      case SQLTypes.TinyInt   => "TINYINT"
      case SQLTypes.SmallInt  => "SMALLINT"
      case SQLTypes.Int       => "INTEGER"
      case SQLTypes.BigInt    => "BIGINT"
      case SQLTypes.Real      => "FLOAT"
      case SQLTypes.Double    => "DOUBLE"
      case SQLTypes.Boolean   => "BOOLEAN"
      case SQLTypes.Varchar   => "VARCHAR"
      case SQLTypes.Date      => "DATE"
      case SQLTypes.Time      => "TIME"
      case SQLTypes.Timestamp => "TIMESTAMP"
      case SQLTypes.VarBinary => "BLOB"
      case SQLTypes.GeoPoint  => "STRUCT(lat DOUBLE, lon DOUBLE)"
      case SQLTypes.Struct =>
        t.fields.map(f => s"${f.name} ${duck(f.valueType)}").mkString("STRUCT(", ", ", ")")
      case SQLTypes.Array(_) =>
        s"${duck(t.element.getOrElse(SetOperationType(SQLTypes.Any)))}[]"
      case other => other.typeId
    }

  private def unformatted(t: SetOperationType): SetOperationType =
    t.copy(
      fields = t.fields.map(f => f.copy(valueType = unformatted(f.valueType))),
      element = t.element.map(unformatted),
      format = None
    )

  /** The core type of the statement's first column, once its derived tables are attached. */
  private def coreType(sql: String): Either[String, SetOperationType] =
    Parser(sql) match {
      case Right(single: SingleSearch) =>
        DerivedTable.attach(single, lookup).flatMap { attached =>
          if (attached.select.fields.exists(_.identifier.identifierName.endsWith("*")))
            attached.schema
              .flatMap(_.columns.headOption)
              .map(MultiSearch.columnType)
              .toRight(s"[$sql] no schema attached for its *")
          else
            MultiSearch
              .projectionTypes(attached)
              .flatMap(_.headOption.map(_._2))
              .toRight(s"[$sql] declares nothing")
        }
      case other => Left(s"[$sql] is not one SELECT: $other")
    }

  /** The listed divergences, each with its rationale: (a fragment of the statement, the core type,
    * DuckDB's type, why).
    */
  private val divergences: Seq[(String, String, String, String)] = Seq(
    // D7 / A-27: Elasticsearch buckets and aggregates a list field ELEMENT by element -- core
    // answers the element, DuckDB a list it groups or orders as a whole (core cannot answer
    // DuckDB's value)
    ("SELECT arr AS c FROM zs_t GROUP BY arr", "INTEGER", "INTEGER[]", "A-27 GROUP BY key"),
    ("SELECT ad AS c FROM zs_t GROUP BY ad", "DATE", "DATE[]", "A-27 GROUP BY key"),
    ("SELECT MIN(arr) AS c FROM zs_t GROUP BY g", "INTEGER", "INTEGER[]", "D7 metric"),
    ("SELECT MIN(ad) AS c FROM zs_t GROUP BY g", "DATE", "DATE[]", "D7 metric"),
    // AggregateReportedTypeSpec (A-20): a whole SUM is DuckDB's HUGEINT, of which core's BIGINT
    // holds every sum Elasticsearch computes below 2^63
    ("SELECT SUM(n) AS c FROM zs_t GROUP BY g", "BIGINT", "HUGEINT", "A-20 SUM"),
    // #420: a bare NULL has no type in core -- it types nothing beside other branches
    ("SELECT NULL AS c FROM zs_t", "NULL", "INTEGER", "#420 NULL"),
    // PRE-EXISTING, inherited from the projection item (one rule, #292: the plain `SELECT <item>`
    // reads the same, asserted below): a whole-number literal is a BIGINT operand in core's
    // arithmetic and conditionals, an INTEGER in DuckDB's -- recorded, not fixed here
    ("SELECT n + 1 AS c FROM zs_t", "BIGINT", "INTEGER", "literal width (pre-existing)"),
    (
      "SELECT CASE WHEN b THEN n ELSE 0 END AS c FROM zs_t",
      "BIGINT",
      "INTEGER",
      "literal width (pre-existing)"
    ),
    ("SELECT COALESCE(n, 0) AS c FROM zs_t", "BIGINT", "INTEGER", "literal width (pre-existing)")
  )

  it should "type every derived column as the plain projection item of its body (#292)" in {
    val bodies = measured.filter(_._1 == "P3").map(_._3).collect {
      case sql if sql.startsWith("SELECT q.c FROM (") && !sql.contains("UNION ALL") =>
        sql.stripPrefix("SELECT q.c FROM (").stripSuffix(") q")
    }
    bodies.size shouldBe 36
    bodies.foreach { body =>
      val plain = Parser(body) match {
        case Right(s: SingleSearch) => s.update(Some(zsT))
        case other                  => fail(s"[$body] $other")
      }
      val projected = MultiSearch.projectionTypes(plain).flatMap(_.headOption.map(_._2))
      // a derived temporal column reads Elasticsearch's default format where its item had none (an
      // aggregate): the column carries one, as every mapped date does
      withClue(s"[$body] ") {
        coreType(s"SELECT q.c FROM ($body) q").toOption.map(unformatted) shouldBe
        projected.map(unformatted)
      }
    }
  }

  /** SCHEMA-5's cells (D12): a NUMERIC / TEMPORAL answer on either side -- a DECIMAL literal, a
    * number of unknown width.
    */
  private def schema5Owned(core: String, duckdb: String): Boolean =
    duckdb.startsWith("DECIMAL") || core == "NUMERIC" || core == "TEMPORAL"

  behavior of "a derived or CTE column, typed from its body (D10, AC3)"

  it should "equal DuckDB 1.5.5's DESCRIBE on every P3 cell, but the listed divergences" in {
    val cells = measured.filter(_._1 == "P3")
    cells.size shouldBe 216
    var excluded = 0
    var diverging = 0
    val mismatches = cells.flatMap { case (_, form, sql, expected) =>
      coreType(sql) match {
        case Left(error) => Some(s"$form [$sql] refused: $error (DuckDB $expected)")
        case Right(t) =>
          val actual = duck(t)
          if (actual == expected) None
          else if (schema5Owned(actual, expected)) { excluded += 1; None }
          else
            divergences.find { case (fragment, c, d, _) =>
              sql.contains(fragment) && c == actual && d == expected
            } match {
              case Some(_) => diverging += 1; None
              case None    => Some(s"$form [$sql] core $actual, DuckDB $expected")
            }
      }
    }
    withClue(
      s"${mismatches.size} mismatches over ${cells.size} cells ($diverging listed divergences, " +
      s"$excluded SCHEMA-5 cells excluded):\n${mismatches.mkString("\n")}\n"
    ) {
      mismatches shouldBe empty
    }
    // every listed divergence is met in every form
    diverging shouldBe divergences.size * 6
  }

  it should "type Case 14 INT: qualified, unqualified, two levels, q.* and WITH (AC1)" in {
    Seq(
      "SELECT q.c1 FROM (SELECT n AS c1 FROM zs_t) q",
      "SELECT c1 FROM (SELECT n AS c1 FROM zs_t) q",
      "SELECT q2.c1 FROM (SELECT q.c1 FROM (SELECT n AS c1 FROM zs_t) q) q2",
      "SELECT q.* FROM (SELECT n AS c1 FROM zs_t) q",
      "WITH q AS (SELECT n AS c1 FROM zs_t) SELECT q.c1 FROM q"
    ).foreach { sql =>
      withClue(s"[$sql] ")(coreType(sql).map(_.sqlType) shouldBe Right(SQLTypes.Int))
    }
  }

  it should "leave an UNattached derived column ANY (R-6: plain update never types one)" in {
    Parser("SELECT q.c1 FROM (SELECT n AS c1 FROM zs_t) q") match {
      case Right(single: SingleSearch) =>
        val plain = single.update(Some(zsT))
        MultiSearch.projectionTypes(plain).map(_.map(_._2.sqlType)) shouldBe Some(
          Seq(SQLTypes.Any)
        )
      case other => fail(s"$other")
    }
  }

  behavior of "a derived column beside a plain one in a set operation (F7, AC2)"

  private def branches(
    sql: String,
    attached: Boolean
  ): (Seq[SingleSearch], Seq[app.softnetwork.elastic.sql.operator.SetOperator]) =
    Parser(sql) match {
      case Right(m: MultiSearch) =>
        val requests =
          if (attached)
            m.requests.map(r => DerivedTable.attach(r, lookup).fold(e => fail(e), identity))
          else m.requests.map(_.update(Some(zsT)))
        (requests, m.resolvedOperators)
      case other => fail(s"[$sql] is not a set operation: $other")
    }

  it should "refuse only what DuckDB fails on, and answer every statement DuckDB runs" in {
    val cells = measured.filter(_._1 == "F7")
    cells.size shouldBe 163
    val wrong = cells.flatMap { case (_, op, sql, expected) =>
      val (requests, operators) = branches(sql, attached = true)
      val verdict = MultiSearch.branchTypes(requests, operators)
      (verdict, expected) match {
        case (Left(refusal), "FAILS") => None
        case (Right(_), "FAILS")      => None // answered as VARCHAR where DuckDB gives no type
        case (Left(refusal), t)       => Some(s"$op [$sql] refused ($refusal), DuckDB $t")
        case (Right(_), _)            => None
      }
    }
    withClue(s"${wrong.size} wrong verdicts:\n${wrong.mkString("\n")}\n")(wrong shouldBe empty)
    // and every refusal core makes is one DuckDB makes, at the column it makes it
    val refused = cells.count { case (_, _, sql, _) =>
      val (requests, operators) = branches(sql, attached = true)
      MultiSearch.branchTypes(requests, operators).isLeft
    }
    refused should be > 0
  }

  it should "refuse Case 15 with #420 rule 1's message, a DATE beside an INT" in {
    val sql =
      "SELECT d AS k FROM zs_t UNION ALL SELECT q.c1 FROM (SELECT n AS c1 FROM zs_t) q"
    val (requests, operators) = branches(sql, attached = true)
    MultiSearch.branchTypes(requests, operators) shouldBe Left(
      "Set operation branches must project compatible types at column 1: branch 1 'k' is DATE, " +
      "branch 2 'c1' is INT"
    )
    // unattached, the derived branch is ANY and the statement passes (R-6)
    val (plain, plainOperators) = branches(sql, attached = false)
    MultiSearch.branchTypes(plain, plainOperators) shouldBe Right(())
    MultiSearch
      .columnTypes(plain, plainOperators)
      .map(_.head.branches.map(_.map(_.sqlType))) shouldBe Right(
      Seq(Some(SQLTypes.Date), Some(SQLTypes.Any))
    )
  }

  it should "merge lists of objects field by field, as DuckDB does (#419 item 2)" in {
    val (_, op, sql, expected) = measured.filter(_._1 == "F7").last
    op shouldBe "UNION ALL"
    expected shouldBe "STRUCT(q INTEGER, p VARCHAR, r INTEGER)[]"
    val (requests, operators) = branches(sql, attached = true)
    MultiSearch.columnTypes(requests, operators).map(columns => duck(columns.head.result)) shouldBe
    Right(expected)
  }

  behavior of "MultiSearch.columnOf"

  it should "be the inverse of columnType over every zs_t column and a derived struct" in {
    val types: Seq[SetOperationType] = zsT.columns.map(MultiSearch.columnType) ++ Seq(
      SetOperationType(SQLTypes.Null),
      SetOperationType(SQLTypes.Any),
      SetOperationType(
        SQLTypes.Struct,
        Seq(
          StructField("a", SetOperationType(SQLTypes.Int)),
          StructField("z", SetOperationType.GeoPoint)
        )
      ),
      SetOperationType(
        SQLTypes.Array(SQLTypes.Date),
        element = Some(SetOperationType(SQLTypes.Date, format = Some("yyyy-MM-dd")))
      )
    )
    types.foreach { t =>
      withClue(s"$t: ")(MultiSearch.columnType(MultiSearch.columnOf("c", t)) shouldBe t)
    }
  }
}
