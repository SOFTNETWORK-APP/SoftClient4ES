# The Epic 19 BI corpus, as a tracked test resource

Three CSV files read by `CorpusReplaySpec`
(`sql/src/test/scala/app/softnetwork/elastic/sql/census/CorpusReplaySpec.scala`), the Epic 21
scoreboard. All three are inputs to a **gate**, not documentation: every one of the 99 statements is
replayed through the real `Parser` on every `sql/test` run and its verdict is asserted against the
declared expectation.

They are test resources only. sbt publishes no test artifacts by default and this build sets no
`Test / publishArtifact`, so nothing here reaches a published jar; the files' only reach is the test
classpath of `sql` itself and of the projects that take `sql % "...test->test"` (`macros` and the four
`es{N}bridge`s).

## `epic-19-bi-corpus.csv` — the statements

A **six-column projection** of `_bmad-output/planning-artifacts/epic-19-bi-corpus.csv`, the 31-column
capture artefact produced by story 19.4. `_bmad*` is gitignored, so the source is on nobody else's
clean checkout; without this projection the replay could not run in CI and Epic 23 would get no
series. The lead approved committing it on 2026-09-04 (epic decision 3) and approved adding
`authorship` back on 2026-09-13.

| column | meaning |
|---|---|
| `capture_id` | the join key for the other two files; `<tool>.<dialect>.<step>.<n>` |
| `tool` | `tableau` or `superset` |
| `dialect` | derived: `capture_id.split('.')(1)` — `mysql`, `sql92` or `flightsql` |
| `workload_step` | `W1`..`W8`; **blank for the 30 `wx.*` connection/capability probes**, rendered `wx` by the suite |
| `authorship` | 19.4's tool-emitted vs human-typed split — `tool` 96, `tool_wrapping_analyst` 2, `analyst` 1 |
| `captured_statement` | the statement, verbatim |

**Retained verbatim**: `captured_statement` is byte-for-byte identical to the source artefact's, and
that equality was asserted when the projection was generated.

**Dropped deliberately**, and they must not be added back: `driver_artifact` (carries a container
image sha256), `seam_file` / `seam_line` / `seam_note` (paths on one operator's laptop), the whole
Epic 19 census-resolution block, and above all `elasticsql_verdict` / `elasticsql_error` — those two
are the 2026-08-31 measurement, they were **already wrong by 7 rows** when this projection was made,
and a tracked stale verdict column is a booby trap for the next reader. The verdict lives in
`baseline-pre-epic21.csv`, which names the commit it was measured at.

Capture provenance: Tableau 2026.2.2 over JDBC (its MySQL and SQL-92 dialects) and Apache Superset
6.0.1 over Flight SQL, against a synthetic `bi_events` / `bi_category_dim` fixture, captured
2026-08-31. 99 statements is **not "BI SQL"** — it is what these two tools emitted for these
workloads.

⚠️ **Format traps.** The file is RFC-4180 with every cell quoted; `"` is escaped by **doubling**, and
7 statements carry embedded newlines, so the file is **126 physical lines for 99 records**. A
`readLine`-based reader silently feeds the parser a shape no BI tool ever emitted and the run still
looks complete. Read it with `CapturedSqlProbe.parseCsv`, which the suite does.

🔴 **All three CSVs are pinned `-text` in `.gitattributes`, and that line must stay.** Because the
newlines are INSIDE quoted fields, checkout-time eol conversion does not merely reformat the file — it
inserts CR into the **statements themselves**. On a clone with `core.autocrlf=true` (the common Windows
setting) the replay would then parse seven statements no BI tool emitted, the byte-for-byte equality
with the capture artefact would no longer hold, and **macOS and Linux CI would never reproduce it**.
Same reasoning the file already applies to `*.cmd` / `*.bat`, for the opposite line ending.

### Regenerating it

```bash
python3.12 - <<'PY'
import csv
src="_bmad-output/planning-artifacts/epic-19-bi-corpus.csv"
dst="sql/src/test/resources/corpus/epic-19-bi-corpus.csv"
cols=["capture_id","tool","dialect","workload_step","authorship","captured_statement"]
with open(src, newline='', encoding='utf-8') as f, open(dst, "w", newline='', encoding='utf-8') as g:
    w=csv.DictWriter(g, fieldnames=cols, quoting=csv.QUOTE_ALL, lineterminator="\n")
    w.writeheader()
    n=0
    for r in csv.DictReader(f):
        w.writerow({
            "capture_id":        r["capture_id"],
            "tool":              r["tool"],
            "dialect":           r["capture_id"].split(".")[1],
            "workload_step":     r["workload_step"],
            "authorship":        r["authorship"],
            "captured_statement":r["captured_statement"],
        })
        n+=1
    assert n == 99, n
PY
```

## `baseline-pre-epic21.csv` — what parsed BEFORE Epic 21

`capture_id,verdict` with `verdict` in `{parses, rejected}`, measured **12 parses / 87 rejected** by
running the real `Parser` compiled from **`ac54a079`** (the epic's merge base, PR #272) in a separate
worktree on 2026-09-13.

🔴 **Do not "refresh" this file.** It is the anchor of the epic's diff and of the Epic 23 series; a
re-measurement on a later tree is a different fact and belongs in a new row of a series, never in
this file. The widely-quoted `5/94` baseline is **stale**: it was captured 2026-08-31, before story
20.9 / #251 fixed the FROM-less `SELECT 1` family, and diffing against it would credit Epic 21 with
Epic 20's +7 (`superset.flightsql.w1.009`, `w1.010`, `tableau.mysql.wx.004`, `w1.015`, `w7.040`,
`tableau.sql92.wx.004`, `w4.024`).

## `epic-21-attribution.csv` — the declared expectation, per statement

`capture_id,expected,scored,owner,note`. **Two facts, two columns, on purpose**: `expected` says what
the PARSER does and is asserted by gate G2; `scored` says whether the statement COUNTS in the
published headline and is asserted by G7. Deriving one from the other makes the state "it parses, and
we are not calling that a fix" unrepresentable.

`expected` ∈ `parses` | `rejected`.
`scored` ∈ `fixed` | `pre_epic21` | `residual` | `rejected_pending_policy` | `capability_open`.

| `owner` | meaning | `expected` | `scored` |
|---|---|---|---|
| `epic21` | Epic 21 fixed it and its family's correctness assertion passes | `parses` | `fixed` |
| `pre_epic21` | already parsed before Epic 21 | `parses` | `pre_epic21` |
| `rejected_pending_policy` | temp-table probe still rejected on grammar; **must STAY rejected** | `rejected` | `rejected_pending_policy` |
| `capability_open` | temp-table probe whose acceptance is an open product decision | either | `capability_open` |
| `epic22a_derived_table` | needs derived tables | `rejected` | `residual` |
| `epic22b_cte` | needs CTEs | `rejected` | `residual` |
| `issue:<N>` | a remotely filed issue; still rejected, **or** parsing and answering wrong | either | `residual` |
| `local:<slug>` | a defect recorded in the team's own issue notes, which live outside this repository; the slug IS the record's identity, and it becomes `issue:<N>` when a fixing story files it remotely | either | `residual` |

Three owners leave `expected` free, and that is the point: it is how "it parses; we are not calling
that a fix" and "it parses; the capability question is not ours to answer" are recorded honestly.
`note` carries the one-line reason. `TBD`, `unknown` and a blank owner are hard failures.

🔴 **Attribution is DECLARED, never inferred from the error message.** `Parser.apply` reports ONE
combinator failure, so a statement carrying several blockers reports whichever alternative got
furthest. Counting one blocker family by message gave 9 ids, by SQL shape 27, and a third method 10.
Worse, families are unstable: story 20.9 moved 26 rows between message families without changing a
single verdict. **Every residual in this table was attributed by BISECTING the statement**, and the
`note` says what the bisection found. Track verdicts and this table; never track message histograms.

⚠️ **One `note` was wrong for exactly the reason this paragraph warns about, and the correction is in
the table.** `tableau.sql92.wx.012`'s second blocker was first recorded as "Oracle `ROWNUM`" on the
strength of a planning note rather than a measurement. Re-measured: `ROWNUM` **parses** and
round-trips — it resolves as an ordinary identifier — so it is not a blocker at all, and the real
consequence is worse than a rejection. Taking a second-hand claim on trust is the same failure as
reading the error message; a `note` that names a blocker must name one somebody ran.

🔴 **The 24 temp-table probe ids are pinned in COMPILED code**
(`CorpusReplay.RejectedPendingPolicyIds` / `CapabilityOpenIds`), not here — a gate whose
expectations live in the file it guards can be silenced by editing that file. Editing this CSV cannot
move them, and G4's failure message says what you are doing.

## Output

The suite writes `sql/target/epic-21/corpus-replay.csv` and `corpus-replay.md` (per-tool,
per-dialect, per-workload and per-authorship tallies, the residuals by owner, and the informational
message families) and prints a one-line summary. The artefacts are emitted BEFORE any assertion, so a
failing gate never leaves the operator blind.

🔴 **The published verb is "SCORES", never "parses"** (lead ruling, 2026-09-13). `N` counts `scored`,
so a sentence saying the engine *parses* `N` is false on its face whenever any statement parses without
being counted — and 25 of them do. The summary line therefore reads *"SCORES 56/99 … 81 PARSE — the
25-row difference is never counted"*, and the raw parse count is stated in the same breath so neither
number can be quoted alone. The spec's PD-1 writes the verb as "parses"; that wording is superseded and
must not be "corrected" back.

---

# `tableau-live-2026-09-13.csv` — the SECOND corpus: what a real Tableau emitted

A separate corpus with its own suite,
`sql/src/test/scala/app/softnetwork/elastic/sql/census/TableauLiveReplaySpec.scala`. It is a
**sibling** of the Epic 19 corpus above, deliberately not merged into it: the 99-statement corpus is
a fixed baseline that the published `N/99` and `N/75` figures and the Epic 23 series are anchored
to, and appending to it would silently move denominators that have already been published.

## Where it came from

On **2026-09-13** a real **Tableau Desktop 2026 for macOS** was driven by hand against a real
**Elasticsearch 8.18.3**, through the SoftClient4ES **JDBC driver `0.3.3-SNAPSHOT`** (core
`0.23.0-SNAPSHOT`), with **p6spy** in the JDBC chain recording every statement the tool sent. The
connection used Tableau's **MySQL dialect** (hence the backtick quoting throughout). The data was
the same synthetic `bi_events` / `bi_category_dim` fixture the Epic 19 capture used; the catalog
renders as the cluster name, `docker-cluster`.

Until this file existed those statements lived only as log files on one laptop, so **nothing in the
repository could regression-test Tableau support without Tableau**. That is the gap this corpus
closes, and it is the whole of its purpose.

**The capture held 127 statements across seven Tableau connections attempts:**

| capture | statements | what it was |
|---|---|---|
| aborted run | 11 | Tableau pointed at a dead port; **three** connection attempts, none reached the cluster |
| run A | 29 | **one** Tableau Desktop session: connect, browse, preview, aggregate sheet, dimension filter, sort |
| run B | 87 | **five** Tableau Desktop sessions, the same workload repeated |

Six sessions reached the cluster. The aborted run **is included**, and is marked here rather than in
the data because it changes no row: every shape it holds (the connect handshake — `CREATE TABLE` /
`DROP TABLE` / the derived-table probe / `SELECT 1`) also occurs in runs A and B, so it contributes
**11 occurrences and zero distinct shapes**. Its statements are still what Tableau *emitted*; only
the answers are missing.

## The projection: one row per distinct normalised shape

`capture_id,tool,dialect,occurrences,expected,owner,note,captured_statement` — **26 rows**, whose
`occurrences` sum to the full **127** (the suite asserts that: a projection that loses or
double-counts a statement is invisible to any per-row gate).

Each row carries **one verbatim representative** — the first occurrence in file order — and the
number of times its shape appeared. Two families of Tableau-generated names are normalised **for the
grouping key only**; the committed statement is untouched:

| regex | why |
|---|---|
| `#Tableau[A-Za-z0-9_]*` | the temp-table probe name, freshly generated per connection |
| `cnt_([A-Za-z0-9_]+?)_[0-9A-Fa-f]{32}_ok` → `cnt_\1_HASH_ok` | the row-existence check's aggregate alias, freshly generated per data source |

Nothing else is normalised. In particular the **FROM table is NOT normalised**, so the four shapes
Tableau issued against both `bi_category_dim` and `bi_events` (`.005`/`.020`, `.006`/`.011`,
`.009`/`.012`, `.010`/`.013`) keep a row each. Folding the table name as well would give **22**
rows, of which 10 rather than 14 are absent from the Epic 19 corpus; that projection is recorded
here so the two counts reconcile, and was not taken, because a verbatim statement against a
different index is a different statement and the file exists to hold statements.

**14 of the 26 shapes are absent from the Epic 19 corpus** (comparing after the same normalisation
plus a catalog-qualifier fold, since the two captures ran against differently-named clusters):
`.005`–`.010`, `.014`, `.015`, `.019`, `.022`–`.026`.

## What the rows measured

Measured on this tree, not predicted: **21 of the 26 shapes parse** (98 of the 127 captured
statements); 5 are rejected. Nothing throws, and nothing comes back labelled an internal parser
error.

| `owner` | rows | meaning |
|---|---|---|
| `works` | 14 | parses today, no known defect behind it |
| `issue:328` | 5 | Tableau's row-existence check — `COUNT(<literal>)` + whole-table `HAVING`, fixed by PR #327 / issue #328. **Must keep parsing**; this is the regression guard |
| `epic22a_derived_table` | 3 | a `SELECT` in `FROM` position; Epic 22 owns relational closure |
| `capability_open` | 2 | the temp-table probe pair; parsing it is not a feature and honouring it is an open product decision |
| `rejected_by_design` | 2 | the rejection is the correct answer to a capability probe |

Three results are worth naming because each corrects an assumption:

- **The temp-table probe is NOT refused.** Tableau's MySQL dialect emits a plain
  ``CREATE TABLE `#Tableau…` (`COL` INTEGER)`` and ``DROP TABLE IF EXISTS `#Tableau…` ``. Issue #326
  recognises-to-reject `CREATE [LOCAL | GLOBAL] TEMPORARY TABLE`, which is the **SQL-92** dialect's
  spelling; it never reaches these. They parse — exactly as the Epic 19 twins
  (`tableau.mysql.wx.001` / `wx.002`, both `capability_open`) do.
- **`GROUP BY 2` against a one-item SELECT list is a probe, and rejecting it is right.** Tableau
  issues `GROUP BY 1` and then `GROUP BY 2` over the same single-column query; #298's bounds check
  rejects the second with a message naming the valid range. What Tableau concludes from that answer
  was not measured.
- **The connect-time derived table is the most frequent shape in the whole capture** —
  ``SELECT `COL` FROM (SELECT 1 AS `COL`) AS `SUBQUERY` ``, 17 of 127 statements, once per
  connection. It is rejected and, measured live, it does not block browsing.

## Two things recorded and deliberately not explained

- 🔴 **`GROUP BY 1` vs `` GROUP BY `t`.`category` ``.** This capture has Tableau emitting the
  **ordinal** (`.023`–`.026`) where the August Epic 19 capture recorded the qualified column name
  (`tableau.mysql.w2.028` and friends) for the **same interaction** — and run A, in this very
  capture, emitted the column-name spelling (`.016`–`.019`). A control run showed the difference is
  **not** caused by a `.tdc`. Both shapes are recorded; the cause is unestablished and no
  explanation is offered here.
- **Correctness is out of scope for this file.** Every gate in `TableauLiveReplaySpec` is a PARSE
  verdict. The `sql` module has no Elasticsearch client, so it cannot ask whether a statement
  *answers* correctly — and parsing is not answering (#205/#209/#224/#253).

### Which rows already have a correctness oracle, and which do not

Oracles live in `testkit/src/main/scala/app/softnetwork/elastic/client/GroupByCompletenessSpec.scala`
(3 shards, 37 categories, 703 documents, run against real ES on all five client subclasses).

| live rows | oracle today |
|---|---|
| `.016`, `.018` | yes — *"aggregate-free GROUP BY, qualified backtick name, NO LIMIT"* and the ORDER-BY-alias test |
| `.007`, `.021`, `.023`, `.025` | yes — *"resolve an ordinal GROUP BY / ORDER BY to the n-th SELECT item"* |
| `.009`, `.012`, `.015` | yes — *"corpus shape: HAVING with no GROUP BY"*, asserted as a TRUE/FALSE predicate pair |
| every row with a `docker-cluster` prefix | yes — *"a catalog prefix is captured and IGNORED"* |
| `.010`, `.013` | partial — grouping by a row-invariant constant is covered; the same statement's `SUM`+`HAVING` combination is not |
| `.017`, `.019`, `.024`, `.026` | **no** — a per-group `SUM` (and, for `.019`/`.026`, a `WHERE … IN (…)` pushed down beside a `GROUP BY`) has no oracle |
| `.014` | **no** — the eight-column aliased preview projection with `LIMIT 100` |
| `.006`, `.011` | **no** — `SELECT * FROM t LIMIT 1`, the shape probe |
| `.004` | covered elsewhere — the FROM-less `SELECT 1` has its own integration coverage from story 20.9 |
| `.001`, `.002` | n/a — DDL, and the execution policy is an open product decision |
| `.003`, `.005`, `.008`, `.020`, `.022` | n/a — rejected, nothing executes |

A later story can pick the **no** rows up; this one deliberately does not invent oracles for shapes
nobody owns.

## What this corpus is NOT

- **Not a baseline.** It carries no `verdict` column measured at a named commit, and nothing diffs
  against it. The Epic 19 `baseline-pre-epic21.csv` is the only baseline in this directory.
- **Not part of any `N/99` or `N/75` denominator.** It scores nothing, publishes no headline, and
  contributes to no epic's arithmetic. Its summary line prints a raw parse count and says so.
- **Not a substitute for running Tableau.** It records what **one** version of Tableau emitted for
  **those** interactions against **that** fixture. A different Tableau build, a different dialect, a
  `.taco` connector, or simply a different click path emits different SQL — `.023`–`.026` above are
  the proof, from two runs a few minutes apart.
- **Not a capability claim.** `parses` is not `works`.

## Format traps

⚠️ **The statements contain literal TAB characters** (`0x09`), inside the `CREATE TABLE` probe —
Tableau emits them and p6spy's single-line rendering preserves them while replacing newlines with
spaces. That is also why the p6spy format below must be split with a **maxsplit**: the log is
TAB-delimited and the SQL, which holds tabs of its own, is **last**.

⚠️ The file is RFC-4180 with **every** cell quoted and `"` escaped by doubling, read by
`CapturedSqlProbe.parseCsv` like the other three. It happens to hold no newline inside a cell today,
but it is covered by the same `sql/src/test/resources/corpus/*.csv -text` rule in `.gitattributes`,
and that is not optional: a checkout-time eol conversion would rewrite the byte stream and reach the
statements themselves.

## Regenerating it

The capture logs live outside this repository. Point `LOGDIR` at them; declared cells
(`expected`, `owner`, `note`) are read back from the existing file and preserved, so a regeneration
refreshes the capture columns without discarding the attribution.

```bash
python3.12 - <<'PY'
import csv, os, re, collections
LOGDIR = "<the directory holding the three p6spy logs>"
FILES  = ["aborted-wrong-port.p6spy.log", "runA-no-tdc.p6spy.log", "runB-with-tdc.p6spy.log"]
DST    = "sql/src/test/resources/corpus/tableau-live-2026-09-13.csv"

TEMP = re.compile(r"#Tableau[A-Za-z0-9_]*")
CNT  = re.compile(r"cnt_([A-Za-z0-9_]+?)_[0-9A-Fa-f]{32}_ok")
norm = lambda s: CNT.sub(r"cnt_\1_HASH_ok", TEMP.sub("#Tableau_PROBE", s))

groups = collections.OrderedDict()
for fn in FILES:
    with open(os.path.join(LOGDIR, fn), newline='', encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue
            # p6spy CustomLineFormat: currentTime executionTime category connectionId sql,
            # TAB-delimited with the SQL LAST -- and the SQL contains tabs, hence the maxsplit.
            _ts, _ms, category, _conn, sql = line.rstrip("\n").split("\t", 4)
            if category == "statement":
                groups.setdefault(norm(sql), []).append(sql)
assert sum(len(v) for v in groups.values()) == 127

declared = {}
if os.path.exists(DST):
    with open(DST, newline='', encoding='utf-8') as f:
        for r in csv.DictReader(f):
            declared[norm(r["captured_statement"])] = (r["expected"], r["owner"], r["note"])

cols = ["capture_id","tool","dialect","occurrences","expected","owner","note","captured_statement"]
with open(DST, "w", newline='', encoding='utf-8') as g:
    w = csv.DictWriter(g, fieldnames=cols, quoting=csv.QUOTE_ALL, lineterminator="\n")
    w.writeheader()
    for i, (k, seen) in enumerate(groups.items(), 1):
        e, o, n = declared.get(k, ("", "", ""))
        w.writerow({"capture_id": "tableau.mysql.live.%03d" % i, "tool": "tableau",
                    "dialect": "mysql", "occurrences": str(len(seen)), "expected": e,
                    "owner": o, "note": n, "captured_statement": seen[0]})
assert len(groups) == 26
PY
```

**Every statement was read before it was committed.** A scan for host-, user- and secret-shaped
content (`http`, `localhost`, `127.0`, `:9200`, `password`, `token`, `secret`, `@`, `/Users/`,
`/home/`, `apikey`) returns **0 hits**; the only identifiers present are the synthetic fixture's
tables and columns, Tableau's own generated `#Tableau…` / `cnt_…_ok` names, the literals `'DE'`,
`'ES'`, `'FR'`, and the cluster name `docker-cluster`. There are no non-ASCII characters and the
only control character is the TAB described above.
