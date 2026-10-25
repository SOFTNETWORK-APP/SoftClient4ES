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
- **dbt** — dbt requires a dedicated adapter plugin per platform. There is no generic JDBC or ODBC adapter,
  and no SoftClient4ES adapter.

None of these is a gap we can close from our side: each one needs either a change by the vendor or a driver
or adapter plugin that nobody has written.

*(Each blocker checked against the vendor's own connection documentation — Metabase, Microsoft Power Query,
Looker and dbt — on 2026-08-31 and 2026-09-01.)*

### Tools that generate nested SQL for you

Some BI tools auto-generate nested SQL (subqueries / derived tables) even when your logical query has none.
**Since engine `0.24.0` that form is accepted** — you no longer have to rewrite it as an explicit JOIN.

- **Apache Superset / DBeaver / Grafana** — you control the SQL. Subqueries, derived tables and explicit
  JOINs are all available; everything in **Works in this release** below applies.
- **Tableau** — connecting, browsing, previewing, aggregating, filtering and sorting work. Drag-and-drop
  worksheets quote and fully qualify every identifier (backticks under the MySQL dialect,
  `"schema"."table"` under Generic SQL-92) and wrap the query in a derived table; **both quoted forms
  parse since engine `0.23.0`, and since `0.24.0` the derived-table wrapper also EXECUTES**, on the
  relational engine. Tableau's **Custom SQL** wraps your statement too — it documents that it *"must wrap the custom
  SQL statement within a select statement"* (Tableau's Custom SQL documentation, checked 2026-09-01) — and
  that wrapper is a derived table, which now runs. **Extract** mode remains **untested** against
  SoftClient4ES. See [Tableau](../client/bi_tools.md).

> **One thing to check before you rely on it:** a derived table runs on the relational engine — **since
> engine `0.24.0` with arrow-extensions `0.3.4`** — so the venue executing your SQL must carry the
> `softclient4es-arrow-extensions` jar as well as the engine. See
> [Which forms need the relational engine](#which-forms-need-the-relational-engine) below. **You almost
> certainly have it already**: the JDBC driver, the ADBC driver and the Arrow Flight SQL sidecar all ship
> with it, and `install.sh` installs it with the REPL by default. Only a REPL installed explicitly with
> `--no-extensions` lacks it.

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
- **BI dialect spellings** — *since engine `0.24.0`*: `CHAR_LENGTH` / `CHARACTER_LENGTH` (for
  `LENGTH`), `TIMESTAMPADD` (for `DATETIME_ADD`) and `TIMESTAMPDIFF` (for `DATE_DIFF`) including the
  ODBC unit-first argument order and the `SQL_TSI_*` interval names, and `SELECT TOP n` (for
  `LIMIT n`). These are spellings, not new behaviour — each renders as the canonical form.
- `SELECT * EXCEPT(col, …)` — drop named columns from `SELECT *`. This is the BigQuery-style **column-exclusion** clause. It removes *columns*; the `EXCEPT` **set operator** removes *rows*. Both work, and they are unrelated.

## Subqueries and derived tables

**Since engine `0.24.0`.** Earlier releases refuse every form below, so check your engine version
before planning around them. (Where they refuse it varies by release and by form — `0.23.0`, for
instance, parses a derived table and refuses it in the engine — so do not rely on the error you get,
only on the version.)

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
| **`UNION ALL`** | Elasticsearch: one `_msearch`, or one search per branch where a branch needs it; branches concatenated in order | **No** — works at every venue |
| **`UNION` / `INTERSECT` / `EXCEPT`** (with or without `ALL`) | The relational engine | **Yes** — arrow-extensions `0.3.4` |

A venue without that jar does not guess: it refuses the statement with an HTTP 400 naming the construct and
the jar, rather than executing it against the first index the statement mentions. **The default install has
the engine** — the JDBC driver, the ADBC driver and the Arrow Flight SQL sidecar all bundle it, and
`install.sh` installs it alongside the REPL unless you pass `--no-extensions`.

### The bound on an uncorrelated subquery

The two-phase path resolves the inner statement into a set of values, so it is bounded by what an
Elasticsearch `terms` query accepts — **65,536 distinct values** (`index.max_terms_count`). Past that the
statement fails loudly, naming the limit and suggesting the JOIN rewrite; it is never silently truncated. A
plain `SELECT <column> FROM …` body is resolved with a single bounded `terms` aggregation, so the values are
already distinct and `DISTINCT` buys nothing.

`NULL` follows ANSI: `IN` ignores NULLs in the inner values, `NOT IN` over a set containing a NULL matches no
rows, and an `EXISTS` over an empty body is false while `NOT EXISTS` over one is true.

> ⚠️ **`NOT IN` has one carve-out, and it fails the other way.** The engine detects the `NULL` by
> re-running the inner query with an `IS NULL` filter. When that inner query carries a `GROUP BY`,
> the probe is a grouped query too, and Elasticsearch's `terms` aggregation **drops the
> missing-value group** — so a `NULL` in a `GROUP BY` body is invisible and `NOT IN` returns rows
> the rule above says it should not. Filter the `NULL` out explicitly in that body
> (`… WHERE <col> IS NOT NULL GROUP BY …`) rather than relying on the detection.

`DELETE` and `UPDATE` take the same `WHERE` subqueries as `SELECT`, with the same venue rules:

```sql
DELETE FROM orders WHERE customer_id IN (SELECT id FROM customers WHERE region = 'EU');
UPDATE orders SET status = 'eu' WHERE customer_id IN (SELECT id FROM customers WHERE region = 'EU');
```

### Subquery forms that are still refused

Each of these is refused, never silently mis-executed. Most are refused **by name**, with the rewrite
in the message; where the refusal is a bare `end of input expected` instead, it is said so, because a
message that names nothing is the one you will need this page for:

- **`LATERAL`** — a derived table that reads an alias from the enclosing `FROM`
  (`FROM orders o, (SELECT id FROM customers WHERE id = o.customer_id) d`). Move the condition to the outer
  `WHERE`, or write it as a correlated `WHERE` subquery. *Named only in that comma-`FROM` spelling: the
  `LATERAL` keyword itself (`FROM o, LATERAL (…)`, `JOIN LATERAL …`) is a bare syntax error.*
- **A subquery in `HAVING`** — any subquery, correlated or not. Compute the value separately, or move the
  condition to `WHERE`.
- **A subquery in the `SELECT` list** — `SELECT (SELECT MAX(amount) FROM orders) AS m …` does not parse.
- **A `UNION ALL` body** — `IN (SELECT a FROM t1 UNION ALL SELECT a FROM t2)`. Write one subquery per branch.
- **A `FROM`-less body** — `IN (SELECT 1)`. Write the literal list instead.
- **More than one projected column** — an `IN` / quantified / scalar body must project exactly one column, so
  `IN (SELECT * FROM customers)` is refused.
- **A QUOTED outer reference** — `WHERE o.customer_id = "c"."id"`. Write it unquoted; the engine rewrites
  an outer reference onto the extracted leg, and a quoted identifier is not rewritten.
- **A correlated body that is not a single Elasticsearch source** — its own `JOIN`, comma-separated `FROM`,
  `JOIN UNNEST`, derived table or window function. Move the construct to the outer `FROM` and correlate
  against it.
- **A scalar or quantified subquery on the LEFT of the comparison.** `WHERE (SELECT COUNT(*) FROM orders o
  WHERE o.customer_id = c.id) > 5` is rejected, and the message it produces (`Unbalanced parentheses`) does
  not say why. Flip the comparison — `WHERE 5 < (SELECT COUNT(*) …)` means the same thing and is accepted.
  The subquery must be the right-hand operand.
- **A column list on the derived table's correlation name** — `FROM (SELECT id FROM orders) AS d (x)`.
  Alias the columns inside the body instead: `(SELECT id AS x FROM orders) AS d`. *Syntax error, not
  a named refusal.*
- **A subquery in a `CASE WHEN` condition** — refused by name, with the rewrite: filter in `WHERE`, or
  compute the flag in a separate query.
- **A subquery in a `JOIN … ON` clause.** ⚠️ Its message names neither subqueries nor a rewrite — it
  reads *"ON clause … must use either equality operator or AND predicate"*. Join on a plain equality
  and move the subquery to `WHERE`.
- **A `WHERE` subquery in a `CREATE MATERIALIZED VIEW`** — refused by name: an Elasticsearch transform
  cannot run the inner query. Resolve the subquery into the view's own source, or keep it in the
  queries you run against the view.
- **A `FROM`-less `SELECT` as a set-operation branch** — `SELECT 1 UNION ALL SELECT id FROM orders`.
  *Syntax error, not a named refusal.* Note this is the one place the connection-handshake idiom
  `SELECT 1` does not compose.

> ⚠️ **One mistake in this family is NOT refused, and it is the easiest one to make.** An
> **unqualified** outer reference — `WHERE EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = id)`
> instead of `… = c.id` — is a perfectly legal statement, so nothing can reject it. The bare `id`
> binds to the subquery's OWN table, the statement stops being correlated, and it runs as an
> ordinary uncorrelated subquery: **HTTP 200, and different rows from the ones you meant.** It is
> the only item here that fails silently rather than loudly.
>
> Always qualify the outer reference with the outer query's alias, and leave it unquoted.

### Licensing

A **correlated** subquery counts as one relational operation against your plan's `maxJoins` allowance, the
same as a JOIN clause — it is a semi-, anti- or aggregate-join the engine executes over two extracted
sources. A **derived table** costs nothing on its own; the JOINs *inside* it count, at any nesting depth.

## Set operators

**Since engine `0.24.0`.** Earlier releases accept `UNION ALL` only and reject every other spelling at the
parser.

| Spelling | Duplicates | Runs where | Needs `softclient4es-arrow-extensions`? |
| --- | --- | --- | --- |
| `UNION ALL` | kept | Elasticsearch: one `_msearch`, or one search per branch where a branch needs it; results concatenated in branch order | **No** — every venue, a plain REPL included |
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
[Qualified and quoted table names](dql_statements.md#qualified-and-quoted-table-names), in every
statement — `INSERT`, `UPDATE`, `CREATE`, `DROP` and `ALTER` included. Five things they do **not**
cover yet:

- **Quoting each part of a dotted index name splits it.** ``FROM `logs-2025`.`03` `` reads index
  `03` under the qualifier `logs-2025`, because a quoted part followed by a dot is a qualifier by
  definition. Write the whole name as one lexeme instead — ``FROM `logs-2025.03` `` or
  `FROM "logs-2025.03"` — or leave it bare (`FROM logs-2025.03`). All three read the index
  `logs-2025.03`.

- **A qualifier must be quoted from the FIRST part.** `FROM prod_eu."bi_events"` mixes the
  spellings, so the leading run of quoted parts is empty and the whole operand is read as ONE index
  name, `prod_eu.bi_events`. Quote the first part too (`FROM "prod_eu"."bi_events"`) if you meant
  `prod_eu` as a qualifier, or leave both bare if you meant the dotted index name.

- **A dot inside a quoted COLUMN name is still a qualifier.** `` SELECT `a.b` FROM t `` is read as
  the column `b` qualified by `a`, exactly as `SELECT a.b` is — there is no way to address an
  Elasticsearch field whose own name contains a dot. (A quoted *table* name is the opposite: its
  dots are literal.) Quoting makes it *look* as though there should be; there is not.

- **A dot and the name part after it must be adjacent — in a column name.** `SELECT a.b` is a
  qualified name; `SELECT a . b` is rejected, and so is a name left with a trailing dot
  (`ORDER BY b. DESC`). This is deliberate: when the dot was allowed to float, `ORDER BY b. DESC`
  silently parsed as a column named `b.DESC` sorted *ascending*. A **table**-name qualifier is
  deliberately more tolerant (`FROM "prod_eu" . bi_events` is accepted), because that spelling has
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
- **`SELECT TOP n PERCENT` and `SELECT TOP n WITH TIES`**, both refused *by name*. `TOP n` itself
  works and is a spelling of `LIMIT n`. Take the plain row count, or compute the percentage
  yourself. ⚠️ Because `PERCENT` is recognised in that position, a column of that name cannot be the
  sole select item directly after `TOP n` — write `SELECT TOP 5 t.percent FROM t AS t`, or quote it.
- **`SELECT DISTINCT TOP n`** — rejected. Write `SELECT DISTINCT … LIMIT n`. (The reverse order,
  `SELECT TOP n DISTINCT`, happens to parse, but it is not valid T-SQL and is not a supported
  spelling.)
- **The ODBC/JDBC escape sequences** — `{fn …}`, `{d '…'}`, `{ts '…'}`, `{oj …}`, `{escape '…'}`.
  A tool that emits `{fn TIMESTAMPADD(SQL_TSI_DAY, -89, CURRENT_DATE)}` is refused, even though the
  `TIMESTAMPADD(…)` inside it is now accepted on its own. Turn escape processing off in the client,
  or write the call without the braces.
- **MySQL's null-safe equality operator `<=>`** (`a <=> b`, i.e. `a = b OR (a IS NULL AND b IS NULL)`). A BI tool set to a MySQL dialect can emit it in a `JOIN … ON`. Write the expansion, or `=` when neither side is nullable.

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
SELECT e.name FROM employees e JOIN eu_departments d ON e.department_id = d.id;

SELECT customer_id FROM orders_q1
INTERSECT
SELECT customer_id FROM orders_q2;
```

> ⚠️ **A CTE cannot be named inside a `WHERE` subquery body.**
> `WITH eu AS (…) SELECT name FROM employees WHERE department_id IN (SELECT id FROM eu)` parses, but
> the reference inside the body is not resolved: the body reaches Elasticsearch asking for an index
> called `eu`, and the statement fails with a `404 index_not_found_exception` naming it. Loud, never
> silent. Read the CTE in `FROM` or `JOIN`, as above.

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

## A pattern and several values on one GROUP BY key — Elasticsearch 6 refuses it

A `HAVING` condition on the grouping key is applied by the `terms` filter, which holds one list of
kept terms and one of removed terms. **Elasticsearch 6** applies the two together only when both
are patterns (`LIKE` / `RLIKE`) or both are values, so a pattern in one list and **two or more**
values in the other is refused with a `400` — *"HAVING cannot filter the GROUP BY key … on
Elasticsearch 6 …"*:

```sql
-- refused on Elasticsearch 6; answered on 7.11 and later
SELECT city, COUNT(*) AS cnt FROM dql_users GROUP BY city
HAVING city LIKE 'P%' AND city NOT IN ('Pau', 'Pise');
```

Write the value list as ONE `RLIKE` pattern — a pattern too, so the pair is applied on every
release (escape any regular-expression metacharacter a value carries, e.g. `'a\.b'` for `a.b`):

```sql
SELECT city, COUNT(*) AS cnt FROM dql_users GROUP BY city
HAVING city LIKE 'P%' AND city NOT RLIKE 'Pau|Pise';
```

A single value in either list is unaffected, and so is every other combination.

## A fractional number compared with an integer GROUP BY key needs the mapping

`HAVING year = 2025.5` over an integer grouping key keeps no group, `HAVING year <> 2025.5` keeps
every group, as SQL answers: no integer equals that number, while Elasticsearch would round it down
and keep the group `2025`. The rule needs the index mapping; an index pattern or several indices
have one too, merged over the indices they match into the wider type. Where no mapping is known —
a pattern that matches no index, a mapping that cannot be read, or a key the matched indices map
with types that have no common type — the number is rounded down by Elasticsearch, without an
error. A key grouped from an `UNNEST` keeps the refusal with a `400` — *"HAVING cannot compare the
integer GROUP BY key … with 2025.5 …"*: compare it with a whole number. See
[Conditions on the GROUP BY key](dql_statements.md#conditions-on-the-group-by-key).

## `DATE_FORMAT` / `DATETIME_FORMAT` of a bare date column as a GROUP BY key — Elasticsearch 6.8 refuses it

`DATE_FORMAT(created_at, '%Y-%m-%d')` and `DATETIME_FORMAT(ts, '%Y-%m-%d %H:%i:%s')` — the operand a
**bare `date` column** — work on every release in a `SELECT` and a `WHERE`. On **Elasticsearch 6.8**
a `date` doc-value reaches Painless as a `JodaCompatibleZonedDateTime`, which the formatter cannot
read, so there the column is first made the UTC `ZonedDateTime` it holds.

As a **`GROUP BY` key**, such a format is refused by name on 6.8. Wrap the column:
`GROUP BY DATE_FORMAT(DATE_TRUNC(created_at, DAY), '%Y-%m-%d')` or
`GROUP BY DATE_FORMAT(CAST(created_at AS DATE), '%Y-%m-%d')` returns exactly what the bare column
returns on 7+. See [DATE_FORMAT](functions_date_time.md#date_format) and
[DATETIME_FORMAT](functions_date_time.md#datetime_format).

## Arithmetic is not accepted inside a function, a `CAST` or a `CASE` branch

An arithmetic expression cannot be the **operand** of a function call, of a `CAST` or of a `CASE`
branch. The restriction is not specific to division, and parenthesising does not help:

| Written | Verdict |
|---------|---------|
| `CAST(a / b AS INTEGER)`, `CAST(a + b AS INTEGER)`, `CAST((a / b) AS INTEGER)` | Parse error |
| `FLOOR(a / b)`, `ABS(a / b)`, `COALESCE(a / b, 0)` | Parse error |
| `CASE WHEN b != 0 THEN a / b ELSE 0 END` | Parse error |
| `(a / b)::INTEGER`, `a / NULLIF(b, 0)`, `FLOOR(x)`, `CAST(x AS INTEGER)` | Accepted |

The rule is **directional**: arithmetic *over* a function call is fine, a function call *over*
arithmetic is not.

**The `::` spelling works.** Since `0.24.0`, `(a / b)::INTEGER` applies the cast, truncated toward
zero: `(n / m)::INTEGER` is `3` for 7 / 2 and `-3` for -7 / 2, and NULL for a zero divisor, in a
search and in a computed column. Before `0.24.0` the conversion was discarded and the result stayed
a DOUBLE. This holds for any operator (`(a + b)::BIGINT`), so a truncated quotient is one
expression:

```sql
-- instead of CAST(n / m AS INTEGER)
SELECT (n / m)::INTEGER AS whole FROM t;
```

## `%` by zero is not guarded by itself

The `0.24.0` rule that makes `a / 0` yield NULL covers `/` only. With integer operands `a % 0`
throws — HTTP 400 in a search, and in a computed column the ingest processor's `ignore_failure`
swallows it so the column is simply absent. With **floating** operands it produces `NaN`, which
Elasticsearch refuses to index, so **the whole document is rejected**.

Guard it with `NULLIF`: `a % NULLIF(b, 0)` answers NULL on the rows where `b` is `0` or missing, in
a search and in a computed column (before `0.24.0` it threw a `null_pointer_exception` on exactly
those rows). See [operators](operators.md#-mod).

## `ORDER BY` over arithmetic on a nullable column

`ORDER BY <arithmetic over a nullable column>` fails. The bridge emits a `number`-typed script sort,
and Elasticsearch rejects a sort script that can return null. Order by a stored column instead, or
make the expression a computed column and sort on that.

## A function over an UNNEST column is refused

An `UNNEST` element is a separate (nested) Elasticsearch document, and a computed value is evaluated
against ONE document. A function **projected** over an UNNEST column — alone or beside a window
function — would be computed once per PARENT document, where the element's columns do not exist;
so would a WHERE condition whose function is not applied directly to the UNNEST column (unless the
column is only a `COALESCE` argument after a non-null literal, which `COALESCE` never returns).
Both used to answer HTTP 200 with wrong rows — the raw value, NULL, zero rows or every row — and are
now refused with a `400`: *"A function over an UNNEST column is not supported in …"*.

```sql
-- refused
SELECT o.id, UPPER(items.product) AS product FROM dql_orders o JOIN UNNEST(o.items) AS items;

-- answered: select the column, compute the value from the returned rows
SELECT o.id, items.product FROM dql_orders o JOIN UNNEST(o.items) AS items LIMIT 100;
```

Without a `LIMIT`, an UNNEST projection, and a `WHERE` over an UNNEST element, return up to 100
elements per parent, or up to the index's own `index.max_inner_result_window` when that setting is
lower. A parent holding more elements is
cut without an error: a 101-element parent returns 100 rows, its last element dropped.

Not affected by this refusal: a WHERE condition whose function is applied directly to the UNNEST
column and reads no parent column (`CAST(items.quantity AS VARCHAR) = '2'`, a date part such as
`YEAR(<column>) = 2025`), any condition without a function, aggregates over UNNEST columns, a
window function over an UNNEST column, over arithmetic of UNNEST columns or over a function applied
directly to one, and statements planned by the relational engine (cross-index JOIN, derived table,
CTE). See [JOIN UNNEST](dql_statements.md#join-unnest).

A derived table over `JOIN UNNEST` (answered by the arrow extension) names an inner column without
an alias by its short name (`items.product` is `product`), and needs no alias and no inner `LIMIT`,
within the elements an UNNEST projection returns per parent. Two inner columns whose short names
coincide (`o.id` and `items.id`) are refused: alias one of them (measured on Elasticsearch 8.18 and
6.8). See [JOIN UNNEST](dql_statements.md#join-unnest).

## `NOT` applies to one condition

`NOT` negates the one condition written after it, and only some spellings of it are accepted:

| Written | Verdict |
|---------|---------|
| `NOT price > 100`, `NOT status = 'x'` (before a comparison) | Accepted |
| `status NOT IN (...)`, `name NOT LIKE 'x%'`, `price NOT BETWEEN 1 AND 2`, `manager IS NOT NULL` | Accepted |
| `NOT (a = 1 OR b = 2)`, `NOT (price > 100)` (before a parenthesised group) | Parse error |
| `NOT is_active` (before a bare boolean column) | Parse error |
| `NOT status IN (...)`, `NOT name LIKE 'x%'`, `NOT manager IS NULL`, `NOT MATCH (...) AGAINST (...)`, `NOT ISNULL(x)` at the start of a condition | Parse error |
| `a = 1 AND NOT name LIKE 'x%'` (the same spellings right after `AND` / `OR`) | Accepted in some positions only: `a = 1 AND b = 2 AND NOT name LIKE 'x%'` is a parse error |
| `a = 1 OR NOT ISNULL(x) AND b = 2` (also with `ISNOTNULL(x)`) | Accepted, read as `a = 1 OR (ISNOTNULL(x) AND b = 2)` |
| `a = 1 OR NOT MATCH (t) AGAINST ('x') AND b = 2` | Refused: *"NOT ... cannot start conditions joined by AND after an OR"* |

Write the condition De Morgan's laws give instead of a `NOT` over a group (`NOT (a = 1 OR b = 2)` is
`NOT a = 1 AND NOT b = 2`), compare a boolean column (`is_active = false`), put the `NOT` of `IN`,
`LIKE`, `RLIKE`, `BETWEEN` and `IS NULL` after the column, write `ISNOTNULL(x)` for `NOT ISNULL(x)`
(and `ISNULL(x)` for `NOT ISNOTNULL(x)`), and move a negated `MATCH` to the end of its `AND` group
(`a = 1 OR (b = 2 AND NOT MATCH (t) AGAINST ('x'))`).

## Aggregates: what is refused, and what is not answered yet

Each of these is refused by name rather than answered wrongly, or is an answer this release does
not give yet:

- **`DISTINCT` inside an aggregate other than `COUNT` and `ARRAY_AGG` is refused** — `SUM(DISTINCT x)`,
  `AVG(DISTINCT x)`, the `STDDEV` / `VARIANCE` family and the percentiles, plain or over a window:
  Elasticsearch would compute them over every value. `COUNT(DISTINCT x)` is answered. See
  [Aggregate functions](functions_aggregate.md#function-count).
- **`GREATEST` / `LEAST` over an aggregate that can be NULL are refused in a `SELECT`**, per group
  and over the whole table (`GREATEST(MAX(n), 2)`, `GREATEST(MAX(d), MAX(d2))`): in a group with no
  value of the column Elasticsearch skips the calculation, so the group would answer NULL where SQL
  answers the largest of the other arguments. Over `COUNT` or `SUM`, and in a `HAVING`, they are
  answered. Select the aggregate on its own and apply `GREATEST` to the result.
- **A function over an aggregate in `HAVING` is refused** where the group filter cannot evaluate it —
  over a `SELECT` aggregate alias (`SELECT MAX(n) AS m … HAVING CAST(m AS VARCHAR) < '5'`), or one
  that rounds (`HAVING ROUND(AVG(x), 1) > 2`). Compare the aggregate itself. See
  [GROUP BY and HAVING](dql_statements.md#group-by-and-having).
- **`COALESCE(MAX(n), 0)` without a `GROUP BY`** fails with a script compile error. Read `MAX(n)` and
  apply the default to the result.
- **On Elasticsearch 6.8, `ORDER BY AVG(x) DESC` over several `GROUP BY` keys** places a group whose
  `AVG` is NULL first.
- **A scripted `MIN` alone** (`MIN(n + 1)`) runs its script twice per document on Elasticsearch 8: a
  cost, not a wrong answer.

## Windows: partitions that read NULL

A window answers NULL on every row when its `PARTITION BY` column is one the `SELECT` list does not
return (`MIN(n) OVER (PARTITION BY g)` without `g` in the `SELECT`), or a `BOOLEAN` column. Return the
partition column in the `SELECT` list.

## GROUP BY: the NULL group, and keys over UNNEST

- **A `GROUP BY` forms no `NULL` group.** A row whose key is NULL — a missing field, a function that
  answers NULL, a field the index does not map — belongs to no group, where SQL forms one group for
  all of them; the groups' `COUNT(*)` then do not add up to the rows. A materialized view stores no
  `NULL` group either. Count those rows apart, with `WHERE <key> IS NULL`.
- **`RLIKE` beside another condition on the same key in `HAVING` is refused** — `HAVING k = 'a' OR k
  RLIKE 'b.*'`: the engine matches a regular expression against the whole key, as Elasticsearch
  does, where DuckDB matches part of it, and which reading such a combination should follow is not
  settled. An `RLIKE` alone, or combined by `AND` with an aggregate, is answered as a whole-key match.
- **A `float` key compared with a value no float holds is refused**; a `half_float` key is not
  checked, and a comparison with such a value may keep no group. See
  [Conditions on the GROUP BY key](dql_statements.md#conditions-on-the-group-by-key).
- **A root aggregate beside an `UNNEST` key is not answered per group.** `COUNT(*)`, `COUNT(t.id)`
  or a `SUM` of a root field beside a `GROUP BY` key of an `UNNEST` element (`… JOIN UNNEST(t.items)
  AS i GROUP BY i.sku`) answers one count over the index, or NULL, instead of one per group. An
  aggregate of the array itself (`MIN(i.qty)`, `COUNT(i.sku)`) is answered per group.
- **Over two `UNNEST`s, an aggregate reads only its own array**: over `JOIN UNNEST(t.items) AS i
  JOIN UNNEST(t.tags) AS g`, `COUNT(i.qty)` counts the elements of `items`, not the rows of the join.

## Types and values

- **A `CASE` / `COALESCE` whose arguments have no common type is refused**, with DuckDB's reason. See
  [The type of a conditional](functions_conditional.md#the-type-of-a-conditional).
- **`date_nanos` columns have no SQL type** in the schema, so a conditional cannot convert them: a
  `CASE` or `COALESCE` that returns a `date_nanos` column beside a date can answer a timestamp in
  some rows and a date in others, `NULLIF(dn, x)` over a `date_nanos` column never matches, and a
  `date_nanos` argument beside a `TIME` makes a `TIME`. `LAST_DAY` over one answers a `DATE`.
- **A date cast to text prints its instant**: `CAST(d AS VARCHAR)` over a `DATE` column answers
  `'2024-01-03T00:00Z'`, where DuckDB answers `'2024-01-03'`. Format it with `DATE_FORMAT(d,
  '%Y-%m-%d')`.
- **A value that is not finite reads back as text**: a computed `DOUBLE` that is `Infinity` or `NaN`
  (`EXP(1000)`) answers the text `'Infinity'`, not a number.
- **Transcendental results can differ from DuckDB's by one unit in the last place** — `EXP(1)` is
  `2.7182818284590455` here and `2.718281828459045` in DuckDB — so an equality written with
  DuckDB's printed value can match nothing.
- **JDBC and Arrow Flight read a plain query's column types from its values**, not from the type the
  column declares: a `BIGINT` column holding small whole numbers is reported as an `INTEGER`
  (`Int32`), and a computed column whose rows carry whole and decimal numbers alike can mix
  `Long` and `Double` values. A set operation (`UNION ALL` and the others) is typed from the
  declared types.

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
