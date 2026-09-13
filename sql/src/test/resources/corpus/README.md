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
| `local:<slug>` | a locally recorded defect (`docs/issues/local-<story>-<slug>.md`, untracked) | either | `residual` |

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
