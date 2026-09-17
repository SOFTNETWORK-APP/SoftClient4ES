[Back to index](README.md)

# Known Limitations & Roadmap

SoftClient4ES runs a large, practical subset of ANSI SQL on Elasticsearch — including cross-index JOINs, and, since engine `0.24.0`, subqueries, derived tables, non-recursive CTEs and the `UNION` / `INTERSECT` / `EXCEPT` set operators, none of which Elasticsearch can do itself. This page tells you exactly what works **as of this release**, what's coming, and how to get unblocked today.

> **Since engine `0.24.0`:** subqueries, derived tables, non-recursive CTEs and set operators
> (`UNION`, `UNION ALL`, `INTERSECT`, `EXCEPT`, with or without `ALL`) all work, including the nested
> SQL BI tools generate for you.

## Using a BI tool? Read this first

Two different things can stop a BI tool here, and it is worth separating them.

### One tool has a path nobody has walked yet

- **Power BI** — Power Query's generic connectors are ODBC and OData, never JDBC. The one candidate path
  is a generic Arrow Flight SQL ODBC driver pointed at the sidecar. It is **unproven**: nobody has
  connected it, so treat it as a lead to follow rather than a supported route.
  See [Power BI](../client/bi_tools.md).

### Some tools cannot connect at all — and that is on their side

- **Metabase** — no generic JDBC database type exists; anything not on Metabase's own driver list needs a
  community driver plugin, which is code nobody has written for SoftClient4ES. See [Metabase](../client/bi_tools.md).
- **Looker** — Looker connects only through drivers it maintains itself, and it allowlists JDBC parameters
  per dialect, so a customer-supplied driver cannot be introduced. This gap is **structural, not
  commercial** — a licence would not close it.

Neither is a gap we can close from our side: each needs either a change by the vendor or a driver plugin
that nobody has written.

*(Each blocker checked against the vendor's own connection documentation — Metabase, Microsoft Power Query
and Looker — on 2026-08-31 and 2026-09-01.)*

### Tools that generate nested SQL for you

Some BI tools auto-generate nested SQL (subqueries / derived tables) even when your logical query has none.
**Since engine `0.24.0` that form is accepted** — you no longer have to rewrite it as an explicit JOIN.

- **Apache Superset / DBeaver / Grafana** — you control the SQL. Subqueries, derived tables and explicit
  JOINs are all available; everything in **Works in this release** below applies.
- **Tableau** — connecting, browsing, previewing, aggregating, filtering and sorting work. Drag-and-drop
  worksheets quote and fully qualify every identifier (backticks under the MySQL dialect,
  `"schema"."table"` under Generic SQL-92) and wrap the query in a derived table; since `0.24.0` both
  forms parse. Tableau's **Custom SQL** wraps your statement too — it documents that it *"must wrap the custom
  SQL statement within a select statement"* (Tableau's Custom SQL documentation, checked 2026-09-01) — and
  that wrapper is a derived table, which now runs. **Extract** mode remains **untested** against
  SoftClient4ES. See [Tableau](../client/bi_tools.md).

> **One thing to check before you rely on it:** a derived table runs on the relational engine — **since
> engine `0.24.0` with arrow-extensions `0.3.4`** — so the venue executing your SQL must carry the
> `softclient4es-arrow-extensions` jar as well as the engine. See
> [Which forms need the relational engine](#which-forms-need-the-relational-engine) below. The JDBC driver,
> the ADBC driver and the Arrow Flight SQL sidecar ship with it; a REPL installed with `--no-extensions`
> does not.

**Apache Superset** (dedicated dialect), **DBeaver**, and **Grafana** (via Arrow Flight SQL) are **Tested**.
**Tableau** is **Compatible** — the connection path works, but it is not yet in our formal regression suite.

## Works in this release

- **Cross-index JOINs**: `INNER` / `LEFT` / `RIGHT` / `FULL` / `CROSS`, plus `JOIN UNNEST` on nested arrays — something Elasticsearch cannot do natively. (See the [JOIN matrix walkthrough](joins.md) for the per-tier rows and worked examples.)
- **Subqueries in `WHERE`** — *since engine `0.24.0`*: `IN (SELECT …)` / `NOT IN`, `EXISTS` /
  `NOT EXISTS`, a scalar comparison against `(SELECT …)`, and the quantified forms `= ANY | SOME`,
  `<> ALL`, `> ALL`, `>= ANY`, `< ALL`, … — **correlated or not**. See
  [Subqueries and derived tables](#subqueries-and-derived-tables) below.
- **Derived tables** — *since engine `0.24.0`*: `FROM (SELECT …) d` and `JOIN (SELECT …) d ON …`, nested
  to any depth — including bodies that themselves carry a JOIN or another derived table.
- **Aggregations** + `GROUP BY` / `HAVING`.
- **Analytical SQL**: `ROW_NUMBER` / `RANK` / `DENSE_RANK`; the `STDDEV` / `VARIANCE` family (`STDDEV_POP`, `STDDEV_SAMP`, `VAR_POP`, `VAR_SAMP`); `PERCENTILE_CONT` / `PERCENTILE_DISC`; window aggregates and `FIRST_VALUE` / `LAST_VALUE` / `ARRAY_AGG` over `OVER (PARTITION BY …)`.
- **Conditionals & null handling**: `CASE` / `COALESCE` / `NULLIF` / `GREATEST` / `LEAST` / `ISNULL` / `ISNOTNULL`.
- `ORDER BY … NULLS FIRST | NULLS LAST`.
- **Non-recursive CTEs** — *since engine `0.24.0`*: `WITH name AS (SELECT …)` at the top of a `SELECT`,
  chained left to right. A CTE reference *is* a derived table, so it runs where derived tables run.
- **Set operators** — *since engine `0.24.0`*: `UNION ALL`, `UNION` / `UNION DISTINCT`, `INTERSECT` /
  `INTERSECT ALL`, `EXCEPT` / `EXCEPT ALL`. See [Set operators](#set-operators) below.
- `SELECT * EXCEPT(col, …)` — drop named columns from `SELECT *`. This is the BigQuery-style **column-exclusion** clause. It removes *columns*; the `EXCEPT` **set operator** removes *rows*. Both work, and they are unrelated.

## Subqueries and derived tables

**Since engine `0.24.0`.** Earlier releases reject every form below at the parser, so check your engine
version before planning around them.

An **uncorrelated** `WHERE` subquery needs nothing but the engine: it executes on Elasticsearch itself, at
every venue. **Correlated subqueries and derived tables additionally need the relational engine — since
engine `0.24.0` with arrow-extensions `0.3.4`.** See
[Which forms need the relational engine](#which-forms-need-the-relational-engine).

Every form below **parses and executes**. The examples are literal — they are the shapes the engine
accepts.

```sql
-- IN / NOT IN over a subquery
SELECT name FROM employees
WHERE department_id IN (SELECT id FROM departments WHERE region = 'EU');

-- scalar comparison
SELECT name FROM employees
WHERE salary > (SELECT AVG(salary) FROM employees);

-- quantified comparison (= ANY | SOME, <> ALL, > ALL, >= ANY, < ALL, …)
SELECT name FROM employees
WHERE salary >= ALL (SELECT salary FROM employees WHERE department = 'IT');

-- EXISTS / NOT EXISTS, correlated against the outer row
SELECT c.name FROM customers c
WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id);

-- derived table in FROM …
SELECT d.category, d.n
FROM (SELECT category, COUNT(*) AS n FROM bi_events GROUP BY category) d
WHERE d.n > 10;

-- … and in JOIN
SELECT o.id, c.name
FROM orders o
JOIN (SELECT id, name FROM customers WHERE tier = 'gold') c ON o.customer_id = c.id;
```

### Which forms need the relational engine

This is the distinction worth knowing before you plan around it.

| Form | Runs where | Needs `softclient4es-arrow-extensions`? |
| --- | --- | --- |
| **Uncorrelated** `WHERE` subquery — `IN` / `NOT IN` / `EXISTS` / `NOT EXISTS` / scalar / quantified | Elasticsearch, in two phases: the inner statement is executed first, then the outer one is rewritten against its values | **No** — works at every venue, including a plain REPL with no extensions |
| **Correlated** `WHERE` subquery (the body reads an outer alias) | The relational engine | **Yes** — arrow-extensions `0.3.4` |
| **Derived table** in `FROM` or `JOIN` | The relational engine | **Yes** — arrow-extensions `0.3.4` |
| **Non-recursive CTE** (`WITH name AS (SELECT …)`) | The relational engine — a CTE reference *is* a derived table | **Yes** — arrow-extensions `0.3.4` |
| **`UNION ALL`** | Elasticsearch, one `_msearch`, branches concatenated in order | **No** — works at every venue |
| **`UNION` / `INTERSECT` / `EXCEPT`** (with or without `ALL`) | The relational engine | **Yes** — arrow-extensions `0.3.4` |

A venue without that jar does not guess: it refuses the statement with an HTTP 400 naming the construct and
the jar, rather than executing it against the first index the statement mentions. The JDBC driver, the ADBC
driver and the Arrow Flight SQL sidecar ship the engine; a REPL installed with `--no-extensions` does not.

### The bound on an uncorrelated subquery

The two-phase path resolves the inner statement into a set of values, so it is bounded by what an
Elasticsearch `terms` query accepts — **65,536 distinct values** (`index.max_terms_count`). Past that the
statement fails loudly, naming the limit and suggesting the JOIN rewrite; it is never silently truncated. A
plain `SELECT <column> FROM …` body is resolved with a single bounded `terms` aggregation, so the values are
already distinct and `DISTINCT` buys nothing.

`NULL` follows ANSI: `IN` ignores NULLs in the inner values, `NOT IN` over a set containing a NULL matches no
rows, and an `EXISTS` over an empty body is false while `NOT EXISTS` over one is true.

### Subquery forms that are still refused

Each of these is rejected by name, never silently mis-executed:

- **`LATERAL`** — a derived table that reads an alias from the enclosing `FROM`
  (`FROM orders o, (SELECT id FROM customers WHERE id = o.customer_id) d`). Move the condition to the outer
  `WHERE`, or write it as a correlated `WHERE` subquery.
- **A subquery in `HAVING`** — any subquery, correlated or not. Compute the value separately, or move the
  condition to `WHERE`.
- **A subquery in the `SELECT` list** — `SELECT (SELECT MAX(amount) FROM orders) AS m …` does not parse.
- **A `UNION ALL` body** — `IN (SELECT a FROM t1 UNION ALL SELECT a FROM t2)`. Write one subquery per branch.
- **A `FROM`-less body** — `IN (SELECT 1)`. Write the literal list instead.
- **More than one projected column** — an `IN` / quantified / scalar body must project exactly one column, so
  `IN (SELECT * FROM customers)` is refused.
- **An unqualified outer reference.** Inside a subquery body a bare column name is read as the body's own
  column, so a correlated reference must carry the outer alias: write
  `WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.id)`, not `… WHERE o.customer_id = id`. The
  outer reference must also be **unquoted**.
- **A correlated body that is not a single Elasticsearch source** — its own `JOIN`, comma-separated `FROM`,
  `JOIN UNNEST`, derived table or window function. Move the construct to the outer `FROM` and correlate
  against it.

### Licensing

A **correlated** subquery counts as one relational operation against your plan's `maxJoins` allowance, the
same as a JOIN clause — it is a semi-, anti- or aggregate-join the engine executes over two extracted
sources. A **derived table** costs nothing on its own; the JOINs *inside* it count, at any nesting depth.

## Set operators

**Since engine `0.24.0`.** Earlier releases accept `UNION ALL` only and reject every other spelling at the
parser.

| Spelling | Duplicates | Runs where | Needs `softclient4es-arrow-extensions`? |
| --- | --- | --- | --- |
| `UNION ALL` | kept | Elasticsearch, one `_msearch`, results concatenated in branch order | **No** — every venue, a plain REPL included |
| `UNION` / `UNION DISTINCT` | removed | The relational engine | **Yes** — arrow-extensions `0.3.4` |
| `INTERSECT` / `INTERSECT ALL` | removed / kept | The relational engine | **Yes** — arrow-extensions `0.3.4` |
| `EXCEPT` / `EXCEPT ALL` | removed / kept | The relational engine | **Yes** — arrow-extensions `0.3.4` |

Elasticsearch has no operation that de-duplicates or intersects across independent searches, so everything
but `UNION ALL` is executed by the same relational engine that runs cross-index JOINs and derived tables. A
venue without that jar refuses the statement rather than answering from one branch.

A branch may carry anything a `SELECT` can carry — `GROUP BY`, a `JOIN`, a derived table, a CTE, a
correlated subquery. A branch that needs the relational engine on its own account routes the whole
statement there.

Full syntax, precedence and the matching rules: [Set operators](dql_statements.md#set-operators).

### Columns match by position

Branches are matched **column by column**, and the result takes the **first branch's** column names — the
standard's rule (SQL-92 §7.10), and what every other SQL engine does. Column names are never compared, so
`SELECT id AS x … UNION ALL SELECT id AS y …` returns **one** column named `x` carrying both branches' ids.

> **Changed in `0.24.0`:** before this release branches were matched **by name**, so a column
> the other branch did not name came back `NULL` — including for the first branch's own rows. If you have a
> `UNION ALL` written against the old behaviour, check that its branches project their columns in the same
> order.

A branch written as a bare `SELECT *` declares no column list, so there is nothing to match positionally;
such a branch is matched by name instead and its width cannot be checked. Name the columns explicitly
whenever a branch's shape matters.

### Set-operator forms that are still refused

Each is rejected by name, never silently mis-executed:

- **A set operation as a subquery body** — `WHERE a IN (SELECT … UNION SELECT …)`. Write one subquery per
  branch.
- **A parenthesised set operation** — both `(a UNION b) INTERSECT c` and a whole statement wrapped in
  parentheses. To group against the default precedence (`INTERSECT` binds tighter than `UNION` / `EXCEPT`),
  use a derived table: `SELECT * FROM (a UNION b) AS g INTERSECT c`.
- **A trailing `ORDER BY` / `LIMIT` after the last branch** of a `UNION`, `INTERSECT` or `EXCEPT` — it would
  silently bind to that branch alone. Parenthesise the branch to keep it there, or wrap the whole operation
  in a derived table to order or limit the result. `UNION ALL` is unchanged: its `ORDER BY` / `LIMIT` have
  always applied per branch.
- **A set operation across catalogs** — mixing branches with catalog-qualified names (`` `cluster_b`.orders ``).
  Catalogs are resolved by their position in the SQL text, so a branch could run on the wrong cluster; the
  planner refuses rather than risk it. Run each branch as its own statement, or drop the catalog prefix.
- **`CORRESPONDING` / `CORRESPONDING BY`** — SQL's opt-in for name-based matching. Not implemented;
  positional matching is the only mode.

### Licensing

A set operation costs **nothing** against your plan's `maxJoins` allowance — like a derived table, it is the
JOINs and correlated subqueries *inside* the branches that count, at any nesting depth.

## Quoted identifiers — residual limits

Quoted column names, aliases and **table names** work in both spellings — see
[Quoted identifiers](dql_statements.md#quoted-identifiers) and
[Qualified and quoted table names](dql_statements.md#qualified-and-quoted-table-names). Five things
they do **not** cover yet:

- **`INSERT`, `UPDATE`, `CREATE`, `DROP` and `ALTER` names are not quotable.**
  ``INSERT INTO `prod_eu`.dest``, `INSERT INTO "prod_eu".dest`, ``UPDATE `orders` SET …`` and
  `CREATE TABLE "dest" ("c" INTEGER)` are all rejected — and so are quoted **column** names in
  those statements (`UPDATE tbl SET "a" = 1`). `SELECT` and `DELETE` are unaffected, because both
  route through the `FROM` table surface. This is the next piece of quoting work; until it lands,
  send DML/DDL names bare. The ``INSERT INTO `prod_eu`.dest`` / ``CREATE TABLE `prod_eu`.dest``
  examples in [joins.md](joins.md) belong to that gap; the ``FROM `prod_us`.orders`` ones do not —
  they work.

- **Quoting each part of a dotted index name splits it.** ``FROM `logs-2025`.`03` `` reads index
  `03` under the qualifier `logs-2025`, because a quoted part followed by a dot is a qualifier by
  definition. Write the whole name as one lexeme instead — ``FROM `logs-2025.03` `` or
  `FROM "logs-2025.03"` — or leave it bare (`FROM logs-2025.03`). All three read the index
  `logs-2025.03`.

- **A qualifier must be quoted from the FIRST part.** `FROM elastic."bi_events"` mixes the
  spellings, so the leading run of quoted parts is empty and the whole operand is read as ONE index
  name, `elastic.bi_events`. Quote the first part too (`FROM "elastic"."bi_events"`) if you meant
  `elastic` as a qualifier, or leave both bare if you meant the dotted index name.

- **A dot inside a quoted COLUMN name is still a qualifier.** `` SELECT `a.b` FROM t `` is read as
  the column `b` qualified by `a`, exactly as `SELECT a.b` is — there is no way to address an
  Elasticsearch field whose own name contains a dot. (A quoted *table* name is the opposite: its
  dots are literal.) Quoting makes it *look* as though there should be; there is not.

- **A dot and the name part after it must be adjacent — in a column name.** `SELECT a.b` is a
  qualified name; `SELECT a . b` is rejected, and so is a name left with a trailing dot
  (`ORDER BY b. DESC`). This is deliberate: when the dot was allowed to float, `ORDER BY b. DESC`
  silently parsed as a column named `b.DESC` sorted *ascending*. A **table**-name qualifier is
  deliberately more tolerant (`FROM "elastic" . bi_events` is accepted), because that spelling has
  always been accepted there and tightening it would have moved which index the statement reads.

- **A qualifier shares a namespace with a real dotted index name.** When one `FROM` names the same
  index under two different qualifiers, the engine tells the two apart by their qualified reference
  — so `SELECT a FROM a.orders q, "a".orders o, "b".orders p` uses `a.orders` both as a real index
  (what `q` reads) and as the qualified reference of `"a".orders`. Both readings of that statement
  are wrong, it was already wrong before, and it is not worth machinery: do not qualify two
  same-named indices with a name that is itself a real index.

> ⚠️ **Federation reads a qualifier differently from the engine.** A cross-cluster statement whose
> table names are FULLY quoted — ``FROM `prod_us`.`orders` `` rather than ``FROM `prod_us`.orders``
> — is not recognised by Federation's catalog pre-processor, so it is forwarded to the default
> cluster instead of the one you named. Before this release such a statement failed loudly in the
> parser; now it parses, so the mis-routing is silent. **On the federation path, leave the table
> name itself unquoted** (`` `prod_us`.orders ``) until this is fixed — see
> [joins.md](joins.md#row-2--cross-cluster-conveyor).

## Not yet supported

- **Recursive CTEs** (`WITH RECURSIVE …`) and **CTE column lists** (`WITH a (x, y) AS …`), both refused by name. Plain non-recursive CTEs work since engine `0.24.0`, with two further limits: a `WITH` clause is accepted only at the top of a `SELECT` (not inside a subquery body, CTAS, `INSERT … SELECT` or a materialized view), and a CTE body may not name the CTE itself — unlike PostgreSQL, which binds such a name to the base table, this engine rejects it.
- **Positional / tiling window functions**: `NTILE`, `LAG`, `LEAD` — not yet implemented. (Note: `PERCENTILE_CONT` / `PERCENTILE_DISC` — percentile *aggregates* — already work; the positional/tiling window functions are a different family.)

When they arrive they will be a driver-side enhancement — single-cluster customers get them by upgrading the driver (JDBC / ADBC / sidecar), with no infrastructure change and no federation server required.

### What a not-yet-supported query looks like

A **recursive** CTE is rejected by the parser today, by name:

```sql
-- Not supported: WITH RECURSIVE is refused — only non-recursive CTEs are accepted.
WITH RECURSIVE subordinates AS (
  SELECT id, manager_id FROM employees WHERE id = 1
  UNION ALL
  SELECT e.id, e.manager_id FROM employees e JOIN subordinates s ON e.manager_id = s.id
)
SELECT id FROM subordinates;
```

There is no rewrite that recovers arbitrary-depth recursion. Flatten the hierarchy at index time (store a
path or a level on each document), or run one statement per level.

The **non-recursive** CTE and the set operator below, on the other hand, both run since engine `0.24.0` —
a CTE reference is a derived table, so each executes on the relational engine and carries the same venue
requirement:

```sql
WITH eu_departments AS (SELECT id FROM departments WHERE region = 'EU')
SELECT name FROM employees WHERE department_id IN (SELECT id FROM eu_departments);

SELECT customer_id FROM orders_q1
INTERSECT
SELECT customer_id FROM orders_q2;
```

## Temporary tables are not supported

`CREATE TEMPORARY TABLE` and `CREATE [LOCAL | GLOBAL] TEMPORARY TABLE`, with or without
`ON COMMIT { PRESERVE | DELETE } ROWS`, are **refused by intent**, and the error names the
construct. Two reasons, both structural:

- **An Elasticsearch index is cluster-global and has no session scope.** It is visible to every
  client that can read the cluster, and there is no session for it to belong to or be cleaned up
  with — the JDBC and ADBC drivers have no server process at all, so nothing can be told that a
  connection ended.
- **`ON COMMIT … ROWS` is transaction semantics, and Elasticsearch has no transactions.** There is
  nothing to honour, and accepting the clause to ignore it would silently change how long your data
  lives.

Use `CREATE TABLE` for a regular index and `DROP TABLE` it when you are done.

Tableau's connection-capability probe issues a `CREATE TABLE` / `DROP TABLE` pair against a
`#`-prefixed name, and its SQL-92 dialect issues `CREATE LOCAL TEMPORARY TABLE`. Every one of those
statements is refused, and Tableau then takes its documented fallback. For what that fallback costs —
and why a `.tdc` customization file cannot skip the probe — see
[Tableau: the temp-table probe](../client/bi_tools.md#tableau-the-temp-table-probe).

A plain `CREATE TABLE` against a probe-shaped name is refused too, but on Elasticsearch's index
naming rules rather than on the temporary-table grammar: an index name must be lowercase and cannot
contain `\`, `/`, `*`, `?`, `"`, `<`, `>`, `|`, a space, a comma or `#`. The error names every rule
the name breaks.

> **Note on what "not session-scoped" does and does not imply.** It is not that Tableau requires
> session scope — Tableau's own capability `CAP_TEMP_TABLES_NOT_SESSION_SCOPED` exists precisely for
> sources that *"use regular tables to simulate temp tables"*. So a future release could choose to
> serve temporary tables with something other than a session. The reason we do not is a cost and a
> safety argument, not an impossibility: every temporary table would be a cluster-state update
> serialised through the elected master, and anything that failed to clean them up would accumulate
> indices in your production cluster until the per-node shard limit refused **all** index creation,
> yours included.

## `STDDEV` / `VARIANCE` over a transformed expression — Elasticsearch 6 refuses it

`STDDEV(YEAR(hire_date))`, `VARIANCE(ABS(salary))` and the rest of the `extended_stats` family over
a transformed operand (plain or `OVER (PARTITION BY …)`) compute correctly on **Elasticsearch 7, 8
and 9**. On **Elasticsearch 6** the query is refused with a `400` — *"STDDEV/VARIANCE over a
transformed expression is not supported on Elasticsearch 6 …"* — because the client library the
driver builds on drops the aggregation script on that line (elastic4s#4100) and the 6.x line is
unmaintained, so the fix cannot reach it; until this rule, the query silently returned the statistic
of the **raw** field. Aggregate over a raw field there, or use Elasticsearch 7+. That refusal is
permanent. See [STDDEV / VARIANCE family](functions_aggregate.md#function-stddev--variance-family).

## Coming in the upcoming release (Quarter 1 2027)

- **Heterogeneous federation**: JOIN or correlate Elasticsearch with PostgreSQL, MySQL, ClickHouse, Snowflake, and more — plus cross-cluster subqueries (e.g. correlate one cluster's data against another's).
  **Not this**: correlating one Elasticsearch index against **another Elasticsearch index** already works in this release and is single-cluster — see [Subqueries and derived tables](#subqueries-and-derived-tables) above. What lands here is correlating across *heterogeneous* sources and across *clusters*.

## Deferred (a future release, demand-driven — tell us what you need)

- `MERGE`, `RETURNING`, `INFORMATION_SCHEMA`, non-materialized `CREATE VIEW`, `TIMESTAMP WITH TIME ZONE`, `INTERVAL` as a type, and `UUID`. (`DECIMAL` / `NUMERIC` are now accepted as cast targets and column types, but **approximately** — they map to `DOUBLE`, and a precision or scale is accepted and ignored. Elasticsearch has no exact decimal type.) No committed date — these are prioritised by customer demand. (Current-release DML already supports `INSERT … ON CONFLICT` upsert — a different feature from `MERGE`.)

## Roadmap timing

We do not commit firm external dates. The **Not yet supported** list above carries no target release: those items are planned, not scheduled. The upcoming heterogeneous-federation release is targeted for **Quarter 1 2027**; the deferred items are demand-driven with no committed date.

## See also

- The [JOIN matrix walkthrough](joins.md) — how the three JOIN tiers work, with worked examples.
- The [federation operator guide](../client/federation_operator_guide.md) — multi-cluster federation deployment.

---

*This page describes SoftClient4ES **as of engine `0.24.0`**. Availability lines name the release a feature landed in; the **Not yet supported** list shrinks as items ship — verify against your installed release.*
