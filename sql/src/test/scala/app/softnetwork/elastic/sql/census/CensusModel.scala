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

package app.softnetwork.elastic.sql.census

/** Which surface of the dialect an entry belongs to. `function` rows are the ones that must cover
  * `SQLKeywords.functionTokens`; everything else is enumerated but excluded from that coverage
  * assertion (PD-5).
  */
sealed abstract class Kind(val label: String)
object Kind {
  case object Fn extends Kind("function") // callable NAME(...)
  case object Op extends Kind("operator") // =, MATCH ... AGAINST, +, ||, ::
  case object Clause extends Kind("clause") // SELECT * EXCEPT, NULLS FIRST, OVER(...), UNION ALL
  case object Lit extends Kind("literal") // PI, RANDOM, E, NULL, TRUE/FALSE
  case object Uom extends Kind("unit") // km, mi, nmi ... ; DAY/DAYS time units
  // NB: `Uom`, not `Unit` - a `case object Unit` inside `object Kind` shadows `scala.Unit` for
  // everything else declared in that object, which is a confusing trap for no benefit.
}

/** What the construct becomes on the Elasticsearch side. Derived from the parsed AST wherever the
  * example is a single projected field (see DialectCensusSpec); pinned by hand otherwise.
  */
sealed abstract class EsConstruct(val label: String)
object EsConstruct {
  case object NativeAgg extends EsConstruct("native_agg") // Field.isAggregation
  case object BucketScript extends EsConstruct("bucket_script") // Field.isBucketScript
  case object PainlessField extends EsConstruct("painless_script_field") // Field.isScriptField
  case object BucketKey extends EsConstruct("bucket_key_script") // identifier.bucket set
  case object WindowTopHits extends EsConstruct("window_top_hits") // ranking windows (unused pin:
  // Task 4.2's empirical check showed the AST reports native_agg for every window token, so no
  // row pins this; kept so 19.3's vocabulary is stable if a later AST predicate distinguishes it)
  case object DocField extends EsConstruct("doc_field") // plain source field
  case object QueryClause extends EsConstruct("query_clause") // WHERE -> query DSL
  case object SortClause extends EsConstruct("sort_clause") // ORDER BY -> sort
  case object RequestShape extends EsConstruct("request_shape") // size/from/msearch/PIT
  case object ClientSide extends EsConstruct("client_side") // resolved after the response
}

/** ANSI classification (Rule R, PD-3). Assigned by a human, SHAPE-ENFORCED by the spec.
  *
  * `evidenceOk` is the whole gate: "non-empty" would be satisfied by `evidence = "x"`, which is
  * exactly the theatre PD-2 refuses. Each branch mirrors one step of Rule R.
  */
sealed abstract class Standard(val label: String) {
  def evidenceOk(evidence: String): Boolean
}
object Standard {
  case object Ansi extends Standard("ansi") {
    // Rule R.1: an ISO/IEC 9075-2:2016 clause or feature id. Authored strings are ASCII-only, so
    // in practice every ansi evidence cites a Feature id (the section-sign branch is kept for
    // completeness but unreachable under the ASCII gate).
    def evidenceOk(e: String): Boolean =
      e.contains("SQL:2016") && (e.contains("§") || e.contains("Feature"))
  }
  case object AnsiAdjacent extends Standard("ansi_adjacent") {
    // Rule R.2: at least TWO fetched engine-doc URLs (PostgreSQL 16 / MySQL 8.4 / DuckDB 1.x).
    def evidenceOk(e: String): Boolean = "http".r.findAllMatchIn(e).size >= 2
  }
  case object EsSpecific extends Standard("es_specific") {
    // Rule R.3: name the Elasticsearch concept the construct exists to reach.
    def evidenceOk(e: String): Boolean =
      e.trim.nonEmpty && (e.contains("ES ") || e.contains("Elasticsearch"))
  }
}

/** One SYNTAX FORM of the dialect (AD-3 - not one token).
  *
  * @param id
  *   stable unique slug, e.g. "fn.string.substring.ansi-from-for"
  * @param token
  *   CANONICAL token word; MUST equal a `SQLKeywords` token's `sql` for Kind.Fn. An alias row keeps
  *   the canonical here - `SUBSTR` is a WORD of the `Substring` token, not a token - or AC-1's
  *   "invent none" diff rejects it.
  * @param spelling
  *   the accepted spelling THIS form uses; equals `token` for a canonical row, and is the alias for
  *   an alias row (T1). 19.4 intersects its BI corpus on `spelling` u `aliases`, never on `token`
  *   alone.
  * @param ownerFile
  *   repo-relative path, e.g. "sql/src/main/scala/.../function/string/package.scala"
  * @param ownerAnchor
  *   exact source substring declaring it; resolved to a line at emit time (AD-2). NEVER store a
  *   line number here - that is what went stale in the epic.
  * @param exampleSql
  *   one statement that MUST parse (AC-6) and that 19.2 feeds to jOOQ verbatim
  * @param arity
  *   "0" | "1" | "2" | "2..3" | "n" - from the parser production / constructor, NOT from
  *   `args.size` (geo.Distance declares args = Nil, see Dev Notes)
  * @param expectEs
  *   pinned ES construct. REQUIRED on every row (asserted): 19.3 classifies on `esConstruct`, so a
  *   blank cell is a hole in its input. On a Kind.Fn row the spec also DERIVES it from the AST and
  *   asserts equality - leaving it None would make AC-7 opt-out by the person it polices
  * @param evidence
  *   Rule-R-shaped citation, shape-checked per `Standard.evidenceOk` (PD-2)
  * @param aliasesOverride
  *   Aliases for rows whose token object is not reachable via SQLKeywords (Kind.Op / Kind.Clause
  *   symbol tokens). Kind.Fn rows leave this None and get aliases from the runtime registry.
  */
final case class CensusEntry(
  id: String,
  kind: Kind,
  token: String,
  spelling: String,
  ownerFile: String,
  ownerAnchor: String,
  exampleSql: String,
  arity: String,
  standard: Standard,
  evidence: String,
  expectEs: Option[EsConstruct],
  notes: String = "",
  aliasesOverride: Option[List[String]] = None
)

object CensusEmitter {

  /** Resolved, emit-ready row: the authored entry plus everything derived (AD-2).
    *
    * @param esConstructSource
    *   "derived" when the value came from the parsed AST, "pinned" when it was authored by hand.
    *   19.3 must be able to tell evidence from assertion (epic framing point 2), and without this
    *   column a pinned fallback is indistinguishable from a derived fact in the CSV.
    * @param derivedNotes
    *   Why a derived column is blank, recorded at derive time. `notes` is AUTHORED and cannot be
    *   written here, so without this the `ST_DISTANCE` row emits an empty `arg_types` (its
    *   `Distance` node declares `args = Nil`, function/geo/package.scala) with no reason.
    */
  final case class Row(
    entry: CensusEntry,
    line: Int,
    aliases: List[String],
    argTypes: List[String],
    returnType: String,
    esConstruct: String,
    esConstructSource: String,
    derivedNotes: List[String] = Nil
  )

  val header: List[String] = List(
    "id",
    "kind",
    "token",
    "spelling",
    "aliases",
    "file",
    "line",
    "example_sql",
    "arity",
    "arg_types",
    "return_type",
    "es_construct",
    "es_construct_source",
    "standard",
    "evidence",
    "notes",
    "derived_notes"
  )

  private def cells(r: Row): List[String] = {
    val out = List(
      r.entry.id,
      r.entry.kind.label,
      r.entry.token,
      r.entry.spelling,
      r.aliases.mkString(" "),
      r.entry.ownerFile,
      r.line.toString,
      r.entry.exampleSql,
      r.entry.arity,
      r.argTypes.mkString(" "),
      r.returnType,
      r.esConstruct,
      r.esConstructSource,
      r.entry.standard.label,
      r.entry.evidence,
      r.entry.notes,
      r.derivedNotes.mkString("; ")
    )
    // `header.zip(cells)` in toJson TRUNCATES silently on a length mismatch, so a column added to
    // one list and not the other would quietly vanish from the JSON companion 19.2 consumes.
    require(
      out.size == header.size,
      s"census row ${r.entry.id}: ${out.size} cells, ${header.size} headers"
    )
    out
  }

  // RFC 4180: quote every cell, double embedded quotes. Mandatory - tokens contain `,` `"` `|`
  // (AD-4). Do NOT "optimise" this to quote-only-when-needed.
  private def csvCell(s: String): String = "\"" + s.replace("\"", "\"\"") + "\""

  def toCsv(rows: Seq[Row]): String =
    (header.map(csvCell).mkString(",") +: rows.map(cells(_).map(csvCell).mkString(",")))
      .mkString("\n") + "\n"

  // Hand-rolled JSON string escaper - no Jackson (AD-4).
  private def jsonStr(s: String): String = {
    val b = new StringBuilder("\"")
    s.foreach {
      case '"'  => b.append("\\\"")
      case '\\' => b.append("\\\\")
      case '\n' => b.append("\\n")
      case '\r' => b.append("\\r")
      case '\t' => b.append("\\t")
      // Locale-free: `"...".format(...)` goes through Locale.getDefault(FORMAT)
      // (cf. project_locale_independent_response_keys). Never use it in an emitter.
      case c if c < ' ' =>
        b.append("\\u").append(Integer.toHexString(0x10000 | c.toInt).substring(1))
      case c => b.append(c)
    }
    b.append("\"").toString
  }

  /** DECIDED (19.1 dev, settling the Task 1 / Task 6 arity inconsistency the create-time review
    * flagged): `toJson` takes the provenance stamp and emits a top-level OBJECT of the shape
    * `{"provenance": <string>, "rows": [ {<header>: <string cell>, ...}, ... ]}`. Every cell is a
    * JSON STRING (including `line`). 19.2/19.3's loaders accept either a bare array or this object
    * shape; this is the shape they will get.
    */
  def toJson(rows: Seq[Row], provenance: String): String = {
    val body = rows
      .map(r =>
        header
          .zip(cells(r))
          .map { case (k, v) => s"${jsonStr(k)}:${jsonStr(v)}" }
          .mkString("{", ",", "}")
      )
      .mkString("[\n    ", ",\n    ", "\n  ]")
    s"""{\n  "provenance":${jsonStr(provenance)},\n  "rows":$body\n}\n"""
  }

  /** GFM tables split cells on `|` even inside backticks (project_sql_keyword_registry). A cell
    * must also never contain a raw newline, which would end the table row.
    */
  def mdCell(s: String): String =
    // BACKSLASH FIRST. `Pipe.sql` is literally `\|\|` (function/string/package.scala), so a
    // naive `|`->`\|` yields `\\|\\|`: GFM renders `\\` as one backslash and the `|` stays an
    // UNESCAPED cell separator, shredding the row. A raw newline ends a GFM row outright.
    s.replace("\\", "\\\\")
      .replace("|", "\\|")
      .replace("\r\n", " ")
      .replace("\n", " ")
      .replace("\r", " ")

  /** One GFM table per Kind, in a stable order, provenance line first. Cells are `mdCell`-escaped;
    * the columns are exactly `header`, so the markdown census cannot drift from the CSV.
    */
  def toMarkdown(rows: Seq[Row], provenance: String): String = {
    val sb = new StringBuilder
    sb.append(s"<!-- $provenance -->\n\n")
    val kinds: List[Kind] = List(Kind.Fn, Kind.Op, Kind.Clause, Kind.Lit, Kind.Uom)
    kinds.foreach { k =>
      val group = rows.filter(_.entry.kind == k)
      if (group.nonEmpty) {
        sb.append(s"## ${k.label} (${group.size} rows)\n\n")
        sb.append(header.map(mdCell).mkString("| ", " | ", " |")).append("\n")
        sb.append(header.map(_ => "---").mkString("| ", " | ", " |")).append("\n")
        group.foreach { r =>
          sb.append(cells(r).map(mdCell).mkString("| ", " | ", " |")).append("\n")
        }
        sb.append("\n")
      }
    }
    sb.toString
  }
}
