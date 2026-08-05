#!/usr/bin/env python3.12
"""Compose the SECOND LinkedIn demo cast from REAL captured product output.

Story (post 2 — "the whole statement surface", not just SELECT … JOIN):
    CREATE the two tables -> COPY INTO from files -> aggregate report over the
    cross-index JOIN -> CTAS the joined result into a new index -> join that new
    index straight back.

Sources — all real runs against the released 0.20.3 bundle and a live ES 8.18.3.
Committed so the cast is rebuildable from a fresh clone (named .txt, not .log, because
.gitignore excludes *.log — these are captured product output, not runtime logs):

    capture-banner.txt      interactive REPL banner (0.20.3 / ext 0.2.3 + 0.2.4)
                            re-capture: printf 'quit\\n' | script -q capture-banner.txt \\
                                          ~/softclient4es/bin/softclient4es
    capture-statements.txt  every statement's real response and real latency
                            re-capture: script -q capture-statements.txt \\
                                          ~/softclient4es/bin/softclient4es -f final.sql
                            (run it from THIS directory — final.sql loads the two JSON
                             fixtures by relative path)

Render:
    python3.12 build_cast_post2.py [--no-banner] out.cast
    agg --theme dracula --font-size 20 --fps-cap 12 --idle-time-limit 2 \\
        --last-frame-duration 4 out.cast out.gif

Only pacing and keystroke animation are authored: no recorder can drive JLine
without stalling (see the memory note), so typing is replayed rather than
captured. Keyword colours are NOT authored — every typed line goes through the
product's own ReplHighlighter from the released 0.20.3 jar.
"""
import json
import os
import re
import subprocess
import sys

SP = os.path.dirname(os.path.abspath(__file__)) + "/"
JAR = os.path.expanduser(
    "~/softclient4es/lib/softclient4es8-cli-all_2.13-0.20.3-assembly.jar"
)
# 80 cols as in the post-1 cut (the LinkedIn feed renders ~555 CSS px, so column
# count — not pixel width — is what decides legibility). 23 rows is the tallest
# scene (22: banner + 4 statements) plus one row of slack: every extra row is a
# permanent black bar that costs feed height.
# --no-banner drops the REPL version banner. The banner is the ONLY version string in
# the whole cast (`grep -c "0\.20\." post2.cast` == 1), so without it the render is
# evergreen — nothing in the SQL or in any captured result names a version. Use it for
# long-lived surfaces (README, website); the LinkedIn cut keeps the banner because a feed
# post is dated anyway and the banner is free credibility there.
# Dropping 9 rows of banner makes scene 3 the tallest (19 rows), so H shrinks with it —
# fewer dead rows means larger type at a given render width.
NO_BANNER = "--no-banner" in sys.argv
W, H = (80, 20) if NO_BANNER else (80, 23)
out, t = [], 0.0


def emit(data, dt=0.0):
    global t
    t += dt
    if data:
        out.append([round(t, 4), "o", data])


def hold(dt):
    global t
    t += dt


SGR = re.compile(r"\x1b\[[0-9;]*m")


def per_char(ansi):
    """Split an ANSI string into one chunk per printable char, escapes attached."""
    chunks, pending, i = [], "", 0
    while i < len(ansi):
        m = SGR.match(ansi, i)
        if m:
            pending += m.group(0)
            i = m.end()
            continue
        chunks.append(pending + ansi[i])
        pending = ""
        i += 1
    if pending and chunks:
        chunks[-1] += pending
    return chunks


def highlight(lines):
    """Colour lines with the REPL's own highlighter — never with hand-written codes."""
    r = subprocess.run(
        ["java", "-cp", JAR, SP + "Highlight.java"],
        input="\n".join(lines), capture_output=True, text=True, check=True,
    )
    got = r.stdout.split("\n")
    assert len(got) >= len(lines), r.stderr
    hl = dict(zip(lines, got))
    for ln in lines:                          # colour must not alter the text
        assert SGR.sub("", hl[ln]) == ln, ln
    return hl


def type_line(s, cps=0.030, pre=0.25, enter=True, ansi=None):
    hold(pre)
    for ch in per_char(ansi) if ansi else s:
        emit(ch, cps)
    if enter:
        emit("\r\n", 0.12)


def unmojibake(s):
    def fix(m):
        try:
            return m.group(0).encode("latin-1").decode("utf-8")
        except (UnicodeDecodeError, UnicodeEncodeError):
            return m.group(0)
    return re.sub("[\\u0080-\\u00ff]+", fix, s)


def keep_sgr(s):
    """Drop terminal control (cursor, modes, erase) but KEEP colour: the captured
    output carries ResultRenderer's real SGR codes and they must survive."""
    s = unmojibake(s)
    s = re.sub(r"\x1b\][^\x07\x1b]*(?:\x07|\x1b\\)", "", s)      # OSC
    s = re.sub(r"\x1b\[[0-9;?]*[a-ln-zA-LN-Z]", "", s)           # CSI except SGR (m)
    return re.sub(r"\x1b[=>]", "", s).replace("\r", "")


def read(f):
    return keep_sgr(open(SP + f, encoding="utf8", errors="replace").read())


# ── real material ────────────────────────────────────────────────────────────
if NO_BANNER:
    BANNER = None
else:
    bl = read("capture-banner.txt")
    b0, b1 = bl.find("╔"), bl.find("╝") + 1
    BANNER = bl[b0:b1]
    # Pins the captured banner to the release it was captured from. If this fires after a
    # release, re-capture capture-banner.txt rather than loosening it — a wrong version on
    # screen is exactly the drift this assert exists to catch.
    assert "0.20.3" in BANNER and "ext 0.2.3 + 0.2.4" in BANNER, BANNER

# `-f` echoes each statement as "=> <sql>"; everything up to the next echo is
# that statement's real response (table rows included) with its real latency.
blocks = [b.strip("\n") for b in read("capture-statements.txt").split("=> ")[1:]]
RESP = [b.split("\n", 1)[1].strip("\n") for b in blocks]
assert len(RESP) == 7, len(RESP)
for r in RESP:
    assert "❌" not in r, r                    # a failed run must never be filmed
CRE_EMP, CRE_DEPT, CP_EMP, CP_DEPT, AGG_OUT, CTAS_OUT, JOIN_OUT = RESP
assert "11 inserted" in CP_EMP and "4 inserted" in CP_DEPT
assert "Engineering" in AGG_OUT and "94500.0" in AGG_OUT and "\U0001F4CA" in AGG_OUT
assert "3 inserted" in CTAS_OUT
assert "Alice" in JOIN_OUT and "EU" in JOIN_OUT

PROMPT = "\x1b[38;5;39m➜\x1b[0m  \x1b[38;5;79m~\x1b[0m "

# ── the statements, wrapped to fit 80 columns with the 5-char prompt ─────────
S1 = ["CREATE TABLE jdbc_join_emp (emp_id KEYWORD, name KEYWORD,",
      "dept_id INTEGER, salary INTEGER, PRIMARY KEY (emp_id));"]
S2 = ["CREATE TABLE jdbc_join_dept (dept_id INTEGER, dept_name KEYWORD,",
      "region KEYWORD, PRIMARY KEY (dept_id));"]
S3 = ["COPY INTO jdbc_join_emp FROM 'employees.json' FILE_FORMAT = 'JSON';"]
S4 = ["COPY INTO jdbc_join_dept FROM 'departments.json' FILE_FORMAT = 'JSON';"]
S5 = ["SELECT   d.dept_name, COUNT(*) AS headcount,",
      "         AVG(e.salary) AS avg_salary, MAX(e.salary) AS top_salary",
      "FROM     jdbc_join_emp  e",
      "JOIN     jdbc_join_dept d ON e.dept_id = d.dept_id",
      "GROUP BY d.dept_name",
      "HAVING   AVG(e.salary) > 75000",
      "ORDER BY AVG(e.salary) DESC;"]
S6 = ["CREATE TABLE high_earner_report AS",
      "SELECT e.name, e.salary, d.dept_name",
      "FROM   jdbc_join_emp  e",
      "JOIN   jdbc_join_dept d ON e.dept_id = d.dept_id",
      "WHERE  e.salary > 90000;"]
S7 = ["SELECT h.name, h.salary, d.region",
      "FROM   high_earner_report h",
      "JOIN   jdbc_join_dept d ON h.dept_name = d.dept_name",
      "ORDER BY h.salary DESC;"]

ALL = S1 + S2 + S3 + S4 + S5 + S6 + S7
for ln in ALL:                                # nothing may wrap on screen
    assert len(ln) + 5 <= W, (len(ln), ln)
HL = highlight(ALL)


def statement(lines, response, cps=0.028, think=0.45, after=0.5, pre=0.3,
              line_pre=0.16):
    for n, sql in enumerate(lines):
        if n:
            emit("  -> ", 0.10)
        type_line(sql, cps=cps, pre=pre if n == 0 else line_pre, ansi=HL[sql])
    emit(response.replace("\n", "\r\n") + "\r\n", think)
    emit("sql> ", 0.15)
    hold(after)


# ── scene 1: launch, then build the two tables and bulk-load them ───────────
emit(PROMPT)
type_line("softclient4es", cps=0.045, pre=0.25)
if BANNER:
    emit("\r\n" + BANNER.replace("\n", "\r\n") + "\r\n\r\n", 0.9)
else:
    emit("\r\n", 0.9)          # same beat, no version string on screen
emit("sql> ", 0.3)
hold(0.45)

statement(S1, CRE_EMP, cps=0.010, after=0.3)
statement(S2, CRE_DEPT, cps=0.011, after=0.45)
statement(S3, CP_EMP, cps=0.020, think=0.5, after=0.35)
statement(S4, CP_DEPT, cps=0.020, think=0.5, after=1.5)

# ── scene 2: the aggregate report across two indices ────────────────────────
# 7 typed lines with no output is the likeliest scroll-away point in a feed —
# type it fast; the 3.4s hold on the result is where the time belongs.
emit("\x1b[H\x1b[2J\x1b[3Jsql> ", 0.4)
hold(0.3)
statement(S5, AGG_OUT, cps=0.013, line_pre=0.10, think=0.8, after=3.4)

# ── scene 3: materialise the joined result, then join it straight back ──────
emit("\x1b[H\x1b[2J\x1b[3Jsql> ", 0.4)
hold(0.3)
statement(S6, CTAS_OUT, cps=0.016, line_pre=0.12, think=0.7, after=1.0)
# Ends ON the result table: `quit` + "👋 Goodbye!" spent 13% of the runtime
# leaving a farewell — not the claim — as the last thing on screen.
statement(S7, JOIN_OUT, cps=0.018, line_pre=0.12, think=0.7, after=4.0)
out.append([round(t, 4), "o", ""])

paths = [a for a in sys.argv[1:] if not a.startswith("--")]
dst = paths[0] if paths else SP + "post2.cast"
with open(dst, "w") as f:
    f.write(json.dumps({"version": 2, "width": W, "height": H}) + "\n")
    for e in out:
        f.write(json.dumps(e, ensure_ascii=False) + "\n")
print(f"{dst}: {len(out)} events, {out[-1][0]:.1f}s")
