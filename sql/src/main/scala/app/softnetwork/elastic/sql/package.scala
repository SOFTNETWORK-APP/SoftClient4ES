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

package app.softnetwork.elastic

import app.softnetwork.elastic.schema.NamingUtils
import app.softnetwork.elastic.sql.function.aggregate.{
  AggregateFunction,
  COUNT,
  CountAgg,
  WindowFunction
}
import app.softnetwork.elastic.sql.function.geo.DistanceUnit
import app.softnetwork.elastic.sql.function.time.CurrentFunction
import app.softnetwork.elastic.sql.parser.{Validation, Validator}
import app.softnetwork.elastic.sql.query._
import app.softnetwork.elastic.sql.schema.{Column, Table => Schema}
import com.fasterxml.jackson.databind.JsonNode

import scala.annotation.tailrec
import scala.collection.immutable.ListMap
import scala.jdk.CollectionConverters._
import scala.reflect.runtime.universe._
import scala.util.Try
import scala.util.matching.Regex

/** Created by smanciot on 27/06/2018.
  */
package object sql {

  /** type alias for SQL query
    */
  type SQL = String

  import app.softnetwork.elastic.sql.function._
  import app.softnetwork.elastic.sql.`type`._

  import scala.language.implicitConversions

  implicit def asString(token: Option[_ <: Token]): String = token match {
    case Some(t) => t.toString
    case _       => ""
  }

  /** Escapes a bare string for a single-quoted SQL literal — backslash first, then the quote, so
    * the composition reverses. Renderers that hold a plain `String` rather than a `StringValue` (a
    * column COMMENT, for one) need it too, which is why it lives here rather than on the value.
    *
    * The grammar accepts TWO spellings of an embedded quote — the SQL-standard doubled delimiter
    * (`''`, issue #274) and this legacy backslash form — and `unescapeStringLiteral` reads both.
    * Only the backslash form is ever EMITTED, deliberately (story 21.5 AD-2): an ANSI-doubling
    * render would also have to stop escaping backslashes, and `StringValue("C:\\").sql` would then
    * emit `'C:\'`, which this grammar rejects — an un-reparseable render, the #218 class.
    */
  def escapeStringLiteral(value: String): String =
    value.replace("\\", "\\\\").replace("'", "\\'")

  /** Un-escapes a quoted string literal's interior, in ONE left-to-right scan.
    *
    * Handles both accepted forms in a single pass: the SQL-standard doubled delimiter (`''` / `""`)
    * and the grammar's legacy backslash escapes (`\'`, `\"`, `\\`). Any OTHER `\x` sequence is
    * copied VERBATIM, exactly as the two-`replace` chain this supersedes did — `'a\nb'` keeps its
    * literal backslash, and COPY INTO paths depend on that.
    *
    * Parameterised by delimiter so both branches of `TypeParser.literal` share one scan rather than
    * growing a second copy.
    *
    * 🔴 It is NOT shared with `Parser.unquoteName`, and the difference is not an oversight: an
    * IDENTIFIER un-escapes a backslash before ANY character (measured: `SELECT "a\nb" FROM t` is
    * the column `anb`), a LITERAL only before the delimiter or another backslash. The two escape
    * alphabets genuinely differ, so folding them — which a note on `unquoteName` used to prescribe
    * — would silently change what a quoted identifier means (story 21.5 AD-12).
    */
  def unescapeStringLiteral(content: String, delimiter: Char): String = {
    val out = new StringBuilder(content.length)
    var i = 0
    while (i < content.length) {
      val c = content.charAt(i)
      if (
        c == '\\' && i + 1 < content.length &&
        (content.charAt(i + 1) == '\\' || content.charAt(i + 1) == delimiter)
      ) {
        out.append(content.charAt(i + 1))
        i += 2
      } else if (c == delimiter && i + 1 < content.length && content.charAt(i + 1) == delimiter) {
        out.append(delimiter)
        i += 2
      } else {
        out.append(c)
        i += 1
      }
    }
    out.toString
  }

  /** The UTC normalisation every Painless emission over an Elasticsearch `date` VALUE must go
    * through — and the single place the rule is written down.
    *
    * 🔴 Elasticsearch does not hand Painless the same object on every major. ES 7+ gives a
    * `java.time.ZonedDateTime`; ES 6.8 gives an
    * `org.elasticsearch.script.JodaCompatibleZonedDateTime`. They overlap, but not completely, and
    * the gap is not theoretical — MEASURED on real ES 6.8.23:
    * {{{
    *   dynamic method [org.elasticsearch.script.JodaCompatibleZonedDateTime, toLocalTime/0] not found
    * }}}
    * `toLocalDate()` happens to exist there and `toLocalTime()` does not, which is exactly the kind
    * of accident that produces a rule with an "except" in it. There is no except: every temporal
    * conversion over a date VALUE normalises first.
    *
    * `toInstant()` exists on BOTH classes, so re-zoning through it yields a real `ZonedDateTime` on
    * every supported major.
    *
    * 🔴 On ES 7/8/9 this chain is an IDENTITY, and that is the point rather than an objection:
    * Elasticsearch stores and returns UTC and `'Z'` IS UTC, so
    * `x.toInstant().atZone(ZoneId.of('Z'))` denotes the same instant, the same zone and the same
    * local fields as `x`. It therefore reads as verbosity that could be "simplified" back to a bare
    * `.toLocalDate()` / `.toLocalTime()` — do not. Deleting it breaks ES 6.8 and changes nothing
    * anywhere else.
    *
    * Applies to a TIMESTAMP-typed operand, which is what an ES `date` field resolves to (story 21.5
    * / issue #306). NOT to a DATETIME-typed one: `coerce`'s `(varchar, DateTime)` arm emits
    * `LocalDateTime.parse(...)`, and `LocalDateTime` has no zero-argument `toInstant()`, so the
    * same chain there is a compile error inside Elasticsearch. (That DATETIME denotes a
    * `LocalDateTime` from one arm and a `ZonedDateTime` from another is a pre-existing
    * inconsistency, recorded not fixed here.)
    *
    * ONE derivation, shared by both emitters — `SQLTypeUtils.coerce` and the transform-function
    * chain in `function/package.scala`. It was previously four separate string literals, with the
    * whole rule carried by a three-word `// compatible ES6+` comment on one of them. Story 21.5 hit
    * "one key, two derivations" for the third time; this is the extraction rather than a fifth
    * copy.
    */
  val painlessUtcZonedDateTime: String = ".toInstant().atZone(ZoneId.of('Z'))"

  /** [[painlessUtcZonedDateTime]] followed by the calendar-date part. */
  val painlessUtcLocalDate: String = s"$painlessUtcZonedDateTime.toLocalDate()"

  /** [[painlessUtcZonedDateTime]] followed by the time-of-day part. */
  val painlessUtcLocalTime: String = s"$painlessUtcZonedDateTime.toLocalTime()"

  /** Escapes a bare string for a DOUBLE-quoted Painless string literal.
    *
    * The escaped set is ENUMERATED from Painless's own lexer rather than patched case by case,
    * because the whole justification for this function is "otherwise the literal is broken" and a
    * rule stated that way admits no "except" (`feedback_constant_uniform_justification`). Painless
    * accepts EXACTLY TWO escape sequences in a double-quoted string, and this is quoted verbatim
    * from the Elasticsearch 8.18 compile error, not inferred:
    *
    * {{{
    * unexpected character ["a\n]. The only valid escape sequences in strings
    * starting with ["] are [\\] and [\"].
    * }}}
    *
    * So the set is exactly the backslash and the double quote:
    *
    *   - the BACKSLASH — a lone one starts an escape sequence, so unescaped it either forms an
    *     INVALID sequence with whatever follows or, at the end of a value, escapes the closing
    *     quote and leaves the string unterminated;
    *   - the DOUBLE QUOTE — the delimiter itself, which otherwise ends the literal early.
    *
    * 🔴 Nothing else may be escaped, and that is a CORRECTNESS constraint, not a minimalism
    * preference. Painless's lexer content rule is `~[\\"]`, i.e. every other character — including
    * a RAW LINE FEED or CARRIAGE RETURN, a tab, non-ASCII — is legal raw inside the literal, while
    * the two-character `\n` / `\r` forms are NOT valid escapes and are a script COMPILE ERROR.
    * Painless differs from Java here. A raw line terminator genuinely reaches this function
    * (`Parser.normalize` collapses newlines only OUTSIDE quoted runs, and the JDBC driver inlines
    * `PreparedStatement` parameters as SQL literals), it passes through untouched, and it WORKS —
    * measured end to end on real Elasticsearch. Adding `\n` / `\r` escaping here was proposed in
    * review and MEASURED to break exactly those statements; `PainlessLiteralEscapingSpec` and the
    * testkit's raw-newline case now guard against re-introducing it.
    *
    * Backslash FIRST, then the quote, so the composition reverses: escaping the quote first would
    * then double the backslash it had just introduced.
    *
    * For a value containing NEITHER character the result is byte-identical to the unescaped form,
    * which is what bounds the change: only literals that were already broken alter shape.
    *
    * 🔴 A different channel from `escapeStringLiteral`, deliberately not shared with it even though
    * the rules rhyme: this escapes for PAINLESS (double-quoted), that escapes for a SQL literal
    * (single-quoted, and it must stay reversible by `unescapeStringLiteral`). They answer to
    * different grammars — the SQL literal's escape alphabet is `\'` and `\\`, this one's is `\"`
    * and `\\` — and only the transport JSON layer escapes line terminators, on its own.
    *
    * ⚠️ It IS byte-identical to `StringValue.ddl`'s inline escape today, and the twin is
    * deliberate: `ddl` renders a DOUBLE-QUOTED SQL literal that `TypeParser.literal`'s double-quote
    * branch must read back, so it is pinned to the SQL grammar, not to Painless's. Sharing them
    * would couple the DDL render to a channel whose rules can change independently.
    */
  def escapePainlessString(value: String): String =
    value
      .replace("\\", "\\\\")
      .replace("\"", "\\\"")

  /** Re-emits a name that was written quoted.
    *
    * The canonical delimiter is the ANSI SQL-92 double quote and an embedded one is DOUBLED — the
    * SQL-standard escape, and exactly the form `Parser.quotedNameRegex` reads back, which is what
    * makes `Parser(identifier.sql)` a fixed point. Backticks are ACCEPTED on input (every Tableau
    * generic-JDBC connection emits them) and deliberately never emitted: the only consumer of this
    * rendering is our own re-parse — `MaterializedViewExtension` runs `client.run(alter.sql)` and
    * writes the same string into a user-runnable artifact — so one canonical spelling beats echoing
    * whichever spelling the caller happened to use.
    *
    * NEVER use this on a path that reaches Elasticsearch: an ES field name is `Identifier.name`,
    * unquoted. Same rule as `escapeStringLiteral` above.
    */
  def quoteIdentifier(name: String): String =
    "\"" + name.replace("\"", "\"\"") + "\""

  /** Re-emits an object reference from the ordered part list the statement wrote.
    *
    * ONE derivation, shared by `query.Table` (the FROM/JOIN surface, story 21.2) and by every
    * DDL/DML statement that names a table, view, pipeline, watcher or enrich policy (story 21.7).
    * Two derivations of the same rendering is the defect story 21.3 paid for four times in one
    * story; there is deliberately no second copy.
    *
    * Two rules, both load-bearing (story 21.2 AD-1 rule 5):
    *
    *   1. 🔴 Each part is emitted as ONE lexeme, NEVER split on its dots — the OPPOSITE of
    *      `Identifier.sql`, which quotes per dot-separated part because in an identifier `a.b`
    *      means *alias a, column b*. In an object reference `a.b` is ONE Elasticsearch name whose
    *      dot is literal, so splitting would render `logs-2025.03` as `"logs-2025"."03"`, which
    *      re-parses as qualifier `logs-2025` + index `03` — an index move manufactured by a
    *      renderer. 2. 🔴 A qualifier part is ALWAYS emitted quoted (its `quoted` bit is true by
    *      construction — `Parser.qualifierPart` matches nothing else). An unquoted prefix is not a
    *      prefix.
    *
    * `parts` empty ⇒ the reference is exactly `name`, written bare: either a programmatic
    * construction (`IndicesApi`, `TableDiff`, `FromlessSelect.toSingleSearch`) or a single bare
    * part, which `Parser.identRef` normalises to `Nil` precisely so that the AST and the render of
    * every bare-spelled statement stay byte-identical to what they were before story 21.7.
    */
  def renderName(parts: Seq[NamePart], name: String): String =
    if (parts.isEmpty) name
    else parts.map(p => if (p.quoted) quoteIdentifier(p.value) else p.value).mkString(".")

  /** The bare DDL/DML name shape, and its ONE owner: `Parser.ident` is `bareNameRegex.r` and
    * `renderColumnName` below is its inverse. A second copy of this character class is how a rule
    * and its reverse drift apart.
    */
  val bareNameRegex: String = "[a-zA-Z_][a-zA-Z0-9_.]*"

  private val bareName = bareNameRegex.r.pattern

  /** Re-emits a COLUMN name, an option key or a struct-entry key in the spelling that reads back as
    * the SAME single name: bare when the DDL/DML name surface can spell it that way, ANSI-quoted
    * otherwise.
    *
    * 🔴 Why these positions decide by SHAPE while an object reference (`renderName` above) carries
    * the parts the statement wrote. A column name lives in a `List[String]` / `ListMap[String, _]`
    * (an INSERT column list, an UPDATE SET, a PRIMARY KEY, the options map) with nowhere to put a
    * per-name bit, and story 21.7 made those positions accept quoting for the first time — so
    * WITHOUT this, `INSERT INTO t ("my col") VALUES (1)` would render `INSERT INTO t (my col)
    * VALUES (1)`: the lossy render story 21.2 exists to remove, re-created on the DDL surface. The
    * predicate is exact rather than heuristic because `Parser.identName` keeps `ident` as its last
    * alternative, so "the bare spelling re-parses to this same name" IS "it matches
    * `bareNameRegex`" — no reserved-word list is involved, and none is duplicated here.
    *
    * Consequence, and it is the wanted one: a name that has always been spellable renders exactly
    * as it did before story 21.7, so no existing rendering moves.
    *
    * NEVER use this on a path that reaches Elasticsearch — an ES field name is the unquoted value.
    */
  def renderColumnName(name: String): String =
    if (bareName.matcher(name).matches()) name else quoteIdentifier(name)

  /** One part of a table reference, exactly as the statement wrote it (#85 — story 21.2 AD-1).
    *
    * `value` is the raw content (un-escaped for a quoted part); `quoted` records whether it was
    * written quoted. A quoted part is ONE lexeme whatever it contains — its internal dots are NEVER
    * split, no part is ever dropped, and no part is ever assigned a role (catalog / schema /
    * federation server alias): interpretation belongs to the resolver, which knows the venue.
    *
    * That is not a stylistic preference. The same lexeme means different things at different venues
    * — the JDBC driver and the Flight SQL producer read one qualifier as the SCHEMA (the cluster;
    * the catalog is the constant `elasticsearch`), the federation join planner reads it as a
    * CATALOG (a `servers.<name>` alias), and BigQuery writes `proj.ds.tbl` as one backticked lexeme
    * whose internal dots separate. A grammar that picked one of those readings would have to be
    * rewritten the day heterogeneous federation lands.
    */
  case class NamePart(value: String, quoted: Boolean)

  /** Base trait for all tokens
    */
  trait Token extends Serializable with Validation {
    def sql: String
    override def toString: String = sql
    def baseType: SQLType = SQLTypes.Any
    def in: SQLType = baseType
    private[this] var _out: SQLType = SQLTypes.Null
    def out: SQLType = if (_out == SQLTypes.Null) baseType else _out
    /*def out_=(t: SQLType): Unit = {
      _out = t
    }*/
    def cast(targetType: SQLType): SQLType = {
      this._out = targetType
      this.out
    }
    def system: Boolean = false
    def nullable: Boolean = !system
    def dateMathScript: Boolean = false
    def isTemporal: Boolean = out.isInstanceOf[SQLTemporal]
    def isAggregation: Boolean = false
    def hasAggregation: Boolean = isAggregation

    /** The type a consumer that DECLARES a column for this expression should use — what SQL
      * consumers are TOLD, as opposed to what Painless renders.
      *
      * 🔴 The third of three questions that had been sharing one answer, and the one nothing asked
      * until a materialized view had to name the type of a computed column:
      *
      *   - [[Token.out]] / `baseType` — the type a QUERY hands Painless. For a schema-resolved
      *     column that is `SQLTypeUtils.runtimeType(declared)`, which collapses every temporal to
      *     `Timestamp`, because Elasticsearch has no timestamp type: every temporal column is a
      *     `date` and `doc['f'].value` is a `ZonedDateTime` whatever the user wrote. Right for
      *     emission and `coerce`, which is all `GenericIdentifier.baseType`'s scaladoc claims.
      *   - [[Identifier.declaredType]] — what the LEAF column was declared as, which an INGEST
      *     script needs because it reads raw JSON (`ZonedDateTime.parse` refuses `"2025-01-10"`).
      *   - `reportedType` — this one. Declaring a column is neither emission nor coercion, so the
      *     runtime collapse has no claim on it. And because Elasticsearch has no timestamp type,
      *     DATE and TIMESTAMP map to the SAME `date` mapping: reporting TIMESTAMP changes nothing
      *     about storage or the script, and destroys the distinction in the one place still
      *     carrying it — what `SHOW MATERIALIZED VIEW`, JDBC and BI metadata report.
      *
      * Defaults to `out`, so every expression answers exactly what it answers today until a
      * function overrides it. The derivation runs in PARALLEL to `baseType` rather than replacing
      * it — story 21.5 tried to fix this at the mapping layer and the consequences cascaded.
      */
    def reportedType: SQLType = out
    def shouldBeScripted: Boolean = false
  }

  trait DdlToken extends Token {
    def ddl: String = sql
  }

  trait TokenValue extends Token {
    def value: Any
  }

  /** Trait for tokens that can be used in painless scripts
    */
  trait PainlessScript extends Token {

    /** Generate painless script for this token
      *
      * @param context
      *   the painless context
      * @return
      *   the painless script
      */
    def painless(context: Option[PainlessContext] = None): String
    def nullValue: String = "null"

  }

  /** Trait for tokens that can be used as parameters in painless scripts
    */
  trait PainlessParam extends Token {
    def param: String
    def checkNotNull: String

    /** The identity a `PainlessContext` deduplicates parameters on (issue #370).
      *
      * By default the rendered doc access itself, which is also what `equals` compares. An
      * `Identifier` widens it with its CHAINED function rendering, because the chain is folded onto
      * the parameter OBJECT (`addPainlessMethod`) while identity was the doc-value STRING alone --
      * so `DATE_TRUNC(d, MONTH) < d` resolved BOTH sides to the one parameter that by then carried
      * the truncation, and compared `x < x`. Kept apart from `equals` on purpose:
      * `GenericIdentifier` is a case class that INHERITS this trait's `equals`, so changing it
      * would move identifier equality everywhere in the AST; the context's bookkeeping is the only
      * thing that must tell two transforms of one column apart.
      */
    def contextKey: String = param

    override def hashCode(): Int = param.hashCode
    override def equals(obj: Any): Boolean = {
      obj match {
        case p: PainlessParam => p.param == param
        case _                => false
      }
    }

    private[this] var _painlessMethods: collection.mutable.Seq[String] =
      collection.mutable.Seq.empty

    def addPainlessMethod(method: String): PainlessParam = {
      if (!_painlessMethods.contains(method))
        _painlessMethods = _painlessMethods :+ method // FIXME we should apply functions only once
      this
    }

    /** Put `method` FIRST. For the UTC normalisation of a raw doc-value (`toInstant().atZone(…)`),
      * which must run before any narrowing (`.toLocalDate()`, `.toLocalTime()`) already appended to
      * this parameter: `LocalDate` has no `toInstant()`, so the other order fails the shard. The
      * narrowing is appended at coercion time and the normalisation only when the chain renders, so
      * insertion order is the wrong order -- the FIRST method is the one that decides the receiver.
      */
    def prependPainlessMethod(method: String): PainlessParam = {
      if (!_painlessMethods.contains(method))
        _painlessMethods = method +: _painlessMethods
      this
    }

    def painlessMethods: Seq[String] = _painlessMethods.toSeq

    /** Whether the methods appended so far leave this parameter a `java.time.LocalDate`.
      *
      * A narrowing (`.toLocalDate()`, alone or as the tail of `painlessUtcLocalDate`) is injected
      * on the OBJECT by a comparison against a DATE literal, a CAST or a CASE, before the chain
      * renders -- so the column's type no longer says what the receiver IS. A method that reads the
      * receiver's type off `baseType` then emits a `ZonedDateTime` call onto a `LocalDate`
      * (`truncatedTo`, MEASURED: `LocalDate.truncatedTo/1 not found`). Later re-widenings
      * (`.atZone(…)`, `.atStartOfDay(…)`) flip it back. Read by `DateTrunc` (issue #370).
      */
    def rendersLocalDate: Boolean = rendersLocal(".toLocalDate()", ".toLocalTime()")

    /** The `TIME` twin: `.toLocalTime()` leaves the receiver a `java.time.LocalTime`. */
    def rendersLocalTime: Boolean = rendersLocal(".toLocalTime()", ".toLocalDate()")

    private[this] def rendersLocal(narrowing: String, other: String): Boolean =
      _painlessMethods.foldLeft(false) { (narrowed, m) =>
        if (m.endsWith(narrowing)) true
        else if (m.contains(".atZone(") || m.contains(".atStartOfDay(") || m.endsWith(other)) false
        else narrowed
      }

  }

  /** @param key
    *   the identity this parameter is deduplicated on, when it differs from the literal.
    *
    * 🔴 Issue #373. An identifier registers as a `LiteralParam` in a PROCESSOR or TRANSFORM context
    * (its operand is `ctx.<field>` / `doc['<alias>'].value`, not a doc-value read), so
    * `Identifier.contextKey` -- #370's fix -- never applied there and two different chains over one
    * column collapsed onto a single parameter again: `SCRIPT AS (CASE WHEN YEAR(d) > MONTH(d) …)`
    * emitted `….get(ChronoField.YEAR).get(ChronoField.MONTH_OF_YEAR)` on ONE `ctx.d`.
    *
    * ⚠️ Used at ONE site -- the processor's parsed temporal base. Keying the identifier
    * registrations themselves was measured to fix nothing and to cost a DEAD duplicate declaration,
    * so it is deliberately not done; see the call site.
    */
  case class LiteralParam(
    literal: String,
    maybeCheckNotNull: Option[String] = None,
    key: Option[String] = None
  ) extends PainlessParam {
    override def param: String = literal
    override def contextKey: String = key.getOrElse(literal)
    override def sql: String = ""
    override def nullable: Boolean = maybeCheckNotNull.nonEmpty
    override def checkNotNull: String = maybeCheckNotNull.getOrElse("")
  }

  sealed trait PainlessContextType

  case object PainlessContextType {
    case object Processor extends PainlessContextType
    case object Query extends PainlessContextType
    case object Transform extends PainlessContextType
  }

  /** Is a rendered Painless fragment a single EXPRESSION, i.e. may it be placed where an operand
    * goes? [[PainlessOperandForm.placeable]] is the one owner of that rule.
    *
    * A `def x = …;` and a `try { … } catch { … }` are STATEMENTS: Painless allows them as the tail
    * of a whole script -- which is why a `script_fields` projection of a safe cast works -- and
    * NOWHERE an operand may go. [[PainlessContext.bindLocal]] hoists the first kind into the
    * prologue; the second kind cannot be hoisted as an expression at all (see
    * [[PainlessContext.bindLocalWith]]) and its caller must fall back to a parameter.
    *
    * 🔴 The scan SKIPS string literals, which is the whole reason it is not a `contains`. A `;` a
    * user put in a format or a pattern (`DATE_FORMAT(d, 'yyyy;MM')`) must not be read as a
    * statement separator: a caller that demotes on it drops the operand's function chain, a SILENT
    * wrong answer.
    *
    * 🔴 THE ASYMMETRY IS DELIBERATE -- when in doubt, prefer the LOUD direction. Answering
    * "statement" about an expression loses a chain silently; answering "expression" about a
    * statement splices it into an operand slot and Elasticsearch says `compile error`. Every
    * limitation below therefore sits on the loud side.
    *
    * Invariants the cases in `PainlessExpressionFormSpec` pin, each for a reason:
    *   - ONE `quote` character, not two booleans: an apostrophe inside a double-quoted pattern
    *     (`"it's;ok"`) or a quote inside a single-quoted one (`'say "hi";'`) would corrupt a
    *     two-flag reading. What matters is WHICH delimiter opened the literal.
    *   - `\` consumes the backslash AND the next character. An escaped BACKSLASH therefore CLOSES
    *     the literal, so `foo("a\\") ; x` really does contain a separator. A scanner that only
    *     special-cased `\"` would read the closing quote as escaped, swallow the rest of the
    *     script, miss that `;` and splice a statement.
    *   - `try` counts only on a word boundary: `entry `, `retry`, `a_try ` and `registry` all
    *     contain it.
    *
    * Accepted limitations, all LOUD-direction, pinned as characterisation so nobody "fixes" one
    * silently: the `try` arm keys on the spelling `try `, so `try{ x }` and a bare `try` are not
    * matched; and an unterminated literal swallows a following real `;` -- in a script
    * Elasticsearch rejects anyway. Bounds-safe: the escape step may run past the end, which the
    * loop guard catches.
    */
  private[sql] object PainlessOperandForm {

    /** Where the first statement boundary is, or `None` for a pure expression. Callers that only
      * decide use [[placeable]]; this variant exists so a diagnostic can NAME the offending spot.
      */
    def firstStatementAt(rendered: String): Option[Int] = {
      var found = -1
      scanOutsideLiterals(rendered) { i =>
        val c = rendered.charAt(i)
        if (c == ';') found = i
        else if (
          rendered.startsWith("try ", i) &&
          (i == 0 || !(rendered.charAt(i - 1).isLetterOrDigit || rendered.charAt(i - 1) == '_'))
        ) found = i
        found < 0 // stop at the first boundary -- the answer cannot change after it
      }
      if (found < 0) None else Some(found)
    }

    def placeable(rendered: String): Boolean = firstStatementAt(rendered).isEmpty

    /** `rendered.split(";")`, except that a `;` inside a string literal is not a separator.
      *
      * ⚠️ NO PRODUCTION CALLER SINCE ISSUE #382, and the reason is worth keeping: it existed for
      * `ScriptProcessor.fromScript`, which assembled an ingest script by splitting the rendered
      * Painless, treating the LAST part as the expression to assign and rejoining the preamble.
      * Making THAT scanner literal-aware fixed one half of the defect (below) and could not fix the
      * other: a `;` inside a BLOCK is a statement boundary to any scanner, so a `TRY_CAST`'s
      * `try`/`catch` still displaced the assignment. `fromScript` is now HANDED the prologue and
      * the expression ([[app.softnetwork.elastic.sql.schema]]'s `ScriptTarget.assemble`), so the
      * boundary is never thrown away and never has to be recovered.
      *
      * It is KEPT, not deleted, because it is the executable SPECIFICATION of the literal rule that
      * [[firstStatementAt]] and [[withoutLiterals]] still enforce: `PainlessExpressionFormSpec`
      * pins that it agrees with `String.split(";")` wherever no literal is involved, and
      * `PainlessLiteralEscapingSpec` uses it as the differential for #383's injection shape. Delete
      * it and those two proofs go with it, while the rule they describe stays live.
      *
      * 🔴 The defect it was written for, so the rule is not re-learned: a bare `split(";")` let a
      * `;` inside a literal in the last statement cut the expression in half, and the assignment
      * was written INTO the literal. `c KEYWORD SCRIPT AS (UPPER('a;b'))` emitted `"a; ctx.c =
      * b".toUpperCase()` -- valid Painless that computes a value and discards it, so Elasticsearch
      * answers 200 and the computed column is simply ABSENT (issue #373, a residual of it; measured
      * on 8.18.3). `REPLACE(name, ';', 'x')` hits the same thing with no contrived input at all.
      *
      * Mirrors `String.split(";")` including its removal of trailing empty parts, so every source
      * with no quoted `;` splits byte-identically to what it did before. Proved exhaustively over
      * the literal-free alphabet `{a, b, ;, \}` up to length 7 -- 21,845 strings, zero mismatches.
      *
      * 🔴 WHY UNDER-SPLITTING IS SAFE, and it is load-bearing. The only direction this can differ
      * from `String.split(";")` is FEWER cuts, and it does that whenever a literal is left
      * unbalanced -- which is STILL REACHABLE, so the argument below is live, not ceremonial.
      *
      * The route this paragraph used to NAME -- `function/time/package.scala` building
      * `ofPattern("<pattern>")` by raw concatenation, so `DATE_FORMAT(d, 'yyyy\')` emitted an
      * unterminated literal -- was CLOSED by issue #383: that emitter escapes its pattern through
      * `escapePainlessString`. It is replaced here by a MEASURED one rather than deleted, because a
      * paragraph that reasons about an unreachable case rots: `Identifier.paramName` builds
      * `doc['<path>']` by raw concatenation too, and a quoted identifier may carry an apostrophe
      * since story 21.1, so `SELECT UPPER("child's_name") FROM t` emits `doc['child's_name']` and
      * this scanner desyncs on it exactly as it did on the pattern, and the ingest assignment moves
      * out of last position -- the #373 failure mode. Measured, not inferred.
      *
      * No audit here enumerates every emission site, so nothing below is a proof of unreachability.
      *
      * It is harmless ONLY because this scanner's escape rule is Painless's own: for any emission
      * Painless accepts, "inside a literal" means the same to both, so a disagreement implies an
      * unbalanced literal, and Elasticsearch rejects such a script with a compile error whichever
      * way it was split. Both spellings measured as `compile error` on 8.18.3. If the escape rule
      * here is ever relaxed, that argument dies with it.
      */
    def splitStatements(rendered: String): Array[String] =
      if (rendered.isEmpty) Array(rendered)
      else {
        val parts = scala.collection.mutable.ArrayBuffer.empty[String]
        var start = 0
        scanOutsideLiterals(rendered) { i =>
          if (rendered.charAt(i) == ';') {
            parts += rendered.substring(start, i)
            start = i + 1
          }
          true
        }
        parts += rendered.substring(start)
        var n = parts.length
        while (n > 0 && parts(n - 1).isEmpty) n -= 1
        parts.take(n).toArray
      }

    /** `rendered` with every string literal AND every comment blanked out, character for character,
      * so a caller can ask a question about CODE only. Indices and length are preserved, so a match
      * on the result carries the same offsets and the same captured text as one on the original.
      *
      * ⚠️ The name is historical: comments joined the literals in issue #382, for the same reason
      * and at the same seam (see [[scanOutsideLiterals]]). Blanking a comment cannot change any
      * emission this repo produces -- no emitter writes one -- and it changes the reading of a
      * HAND-AUTHORED processor source, which is the only place a comment can arrive from.
      *
      * 🔴 `ScriptTarget.of` recovers "which column does this ingest script feed" by taking the LAST
      * `ctx.<name> = ` in the source. A literal is allowed to contain that text -- `CONCAT(name, ';
      * ctx.d = 1')` is ordinary SQL -- and the raw regex then reported the computed column of one
      * processor as another's. `IngestPipeline.diff` keys processors by it, and a `Map` silently
      * DROPS the loser: measured, the ALTER rewrote the sibling column's processor and never added
      * the new column (issue #373). Blanking the literals first is what makes the recovery read
      * code and not data.
      */
    def withoutLiterals(rendered: String): String = {
      val out = Array.fill(rendered.length)(' ')
      scanOutsideLiterals(rendered) { i =>
        out(i) = rendered.charAt(i)
        true
      }
      new String(out)
    }

    /** Walks `rendered` once and hands the caller every index that is CODE -- i.e. lies outside
      * both a string literal and a comment.
      *
      * 🔴 The literal and comment handling lives HERE and nowhere else. `firstStatementAt` and
      * `splitStatements` must agree about what a `;` inside a literal is -- one decides whether a
      * fragment may sit in an operand slot, the other decides where an ingest script's last
      * statement begins -- and two copies of this loop would be free to drift apart.
      *
      * 🔴 COMMENTS ARE NOT CODE (issue #382, found by independent review). `ScriptTarget.of` reads
      * an ingest script's identity off the last `ctx.<name> = ` that follows a statement boundary,
      * and #382 widened that boundary set with `}` because a hoisted `try { … } catch (Exception e)
      * {} ` is a statement with no `;`. A `}` inside a Painless BLOCK COMMENT was then read as a
      * boundary: `ctx.c = 1` followed by a block comment holding the text `} ctx.zzz = 9` reported
      * column **zzz** -- a processor claiming another column's identity, which is the collision
      * `IngestPipeline.diff` resolves by DROPPING one side (measured; `Some(c)` before #382,
      * `Some(zzz)` after; the line-comment spelling is identical). A comment is reachable from SQL
      * today: `ALTER PIPELINE … ADD PROCESSOR SCRIPT(source = '…')` and `CREATE PIPELINE … WITH
      * PROCESSORS (SCRIPT(…))` both take a hand-authored source.
      *
      * Fixed at the same seam that already solves it for literals rather than by narrowing the
      * regex, because it is the same question -- "is this character code?" -- and two answers to it
      * would drift. Two shapes that used to answer `None` (a comment BEFORE the assignment, so no
      * boundary was recognised at all) now answer correctly, which is a bonus, not the point.
      *
      * A `/` that opens nothing is ordinary division and is emitted; an unterminated block-comment
      * opener swallows the rest, the same LOUD-direction convention an unterminated literal already
      * follows.
      */
    private def scanOutsideLiterals(rendered: String)(at: Int => Boolean): Unit = {
      val n = rendered.length
      var i = 0
      var quote = '\u0000'
      var continue = true
      // The last non-whitespace character accepted as CODE. Only `~` can precede a regex literal
      // (`==~` / `=~`), which is what tells a regex opener apart from division and from a comment.
      var lastCode = '\u0000'
      while (i < n && continue) {
        val c = rendered.charAt(i)
        if (quote != '\u0000') {
          if (c == '\\') i += 1 else if (c == quote) quote = '\u0000'
        } else if (c == '\'' || c == '"') quote = c
        else if (c == '/' && lastCode == '~') {
          // 🔴 A Painless REGEX literal, which is neither code nor a comment (issue #382, found by
          // review). `WHERE path RLIKE 'a/*b'` emits `($p ==~ /a\/*b/)`, so the bytes `/*` appear
          // OUTSIDE every quoted literal: read as a block-comment opener they have no `*/`, and the
          // rest of the script -- the trailing `ctx.<column> = ` included -- was blanked. That lost
          // the processor's identity for `ScriptTarget.of` (the ALTER churn this scanner exists to
          // stop) and could hide a real `;` from `firstStatementAt`, declaring a STATEMENT
          // placeable. A pattern merely STARTING with `*` (`RLIKE '*b'`) does it with no escape at
          // all, so this arm has to precede both comment arms.
          //
          // `~` is the whole discriminator and it is sufficient: `==~` and `=~` are the only
          // operators Painless spells with it, an ordinary division never follows one, and a `/`
          // preceded by anything else keeps its existing reading.
          i += 1
          while (i < n && rendered.charAt(i) != '/') {
            if (rendered.charAt(i) == '\\') i += 1
            i += 1
          }
        } else if (c == '/' && i + 1 < n && rendered.charAt(i + 1) == '/') {
          // line comment: everything up to (but not including) the newline is not code
          i += 1
          while (i + 1 < n && rendered.charAt(i + 1) != '\n') i += 1
        } else if (c == '/' && i + 1 < n && rendered.charAt(i + 1) == '*') {
          // block comment: skip to the closing `*/`, or to the end if there is none
          i += 2
          while (i + 1 < n && !(rendered.charAt(i) == '*' && rendered.charAt(i + 1) == '/')) i += 1
          i += 1
        } else {
          continue = at(i)
          if (!c.isWhitespace) lastCode = c
        }
        i += 1
      }
    }
  }

  /** Context for painless scripts
    * @param context
    *   the context type
    */
  case class PainlessContext(context: PainlessContextType = PainlessContextType.Query) {
    // List of parameter keys
    private[this] var _keys: collection.mutable.Seq[PainlessParam] = collection.mutable.Seq.empty

    // List of parameter names
    private[this] var _values: collection.mutable.Seq[String] = collection.mutable.Seq.empty

    // Last parameter name added
    private[this] var _lastParam: Option[String] = None

    def isProcessor: Boolean = context == PainlessContextType.Processor

    def isTransform: Boolean = context == PainlessContextType.Transform

    lazy val timestamp: String = {
      context match {
        case PainlessContextType.Processor => CurrentFunction.processorTimestamp
        case PainlessContextType.Query | PainlessContextType.Transform =>
          CurrentFunction.queryTimestamp
      }
    }

    /** Add a token parameter to the context if not already present
      *
      * @param token
      *   the token parameter to add
      * @return
      *   the optional parameter name
      */
    def addParam(token: Token): Option[String] = {
      token match {
        case identifier: Identifier if isProcessor =>
          if (identifier.name.nonEmpty)
            addParam(
              LiteralParam(
                identifier.processParamName,
                None /*identifier.processCheckNotNull*/
              )
            )
          else
            None
        case identifier: Identifier if isTransform =>
          if (identifier.name.nonEmpty)
            addParam(
              LiteralParam(
                identifier.transformParamName,
                identifier.transformCheckNotNull
              )
            )
          else
            None
        case param: PainlessParam
            if param.param.nonEmpty && (param.isInstanceOf[LiteralParam] || param.nullable) =>
          get(param) match {
            case Some(p) => Some(p)
            case _ =>
              val index = _values.indexOf(param.param)
              if (index >= 0) {
                Some(param.param)
              } else {
                val paramName = s"param${_keys.size + 1}"
                _keys = _keys :+ param
                _values = _values :+ paramName
                // 🔴 Declaration ORDER is creation ORDER (story BIDC-8, review B-1). A param can be
                // created AFTER a local — `function/cond/package.scala` captures an already-rendered
                // CASE condition into a new `LiteralParam` — and emitting all params before all
                // locals then produced a FORWARD REFERENCE (`def param2 = (left1 …); def left1 = …`),
                // which Elasticsearch refuses with `cannot resolve symbol [left1]` on EVERY index.
                // There are 18 `addParam(LiteralParam(...))` sites, so the ordering must be
                // structural, not a special case.
                _declarations = _declarations :+ Left(param)
                _lastParam = Some(paramName)
                _lastParam
              }
          }
        case _ => None
      }
    }

    def get(token: Token): Option[String] = {
      token match {
        case identifier: Identifier if isProcessor =>
          get(
            LiteralParam(
              identifier.processParamName,
              None /*identifier.processCheckNotNull*/
            )
          )
        case identifier: Identifier if isTransform =>
          get(
            LiteralParam(
              identifier.transformParamName,
              None /*identifier.transformCheckNotNull*/
            )
          )
        case param: PainlessParam =>
          // By `contextKey`, not `equals`: see `PainlessParam.contextKey` (issue #370).
          Try(_values(_keys.indexWhere(_.contextKey == param.contextKey))).toOption
        case f: FunctionWithIdentifier => get(f.identifier)
        case _                         => None
      }
    }

    // Unique local-variable names for a script (story BIDC-8, AD-9). A guarded boolean binds its
    // operand to a local so the comparison lands INSIDE the null guard; two such operands in one
    // script must not declare the same name.
    private[this] var _locals: Int = 0

    /** Every declaration this script emits, in CREATION order: a parameter (rendered from its
      * `PainlessParam`) or a local bound by [[bindLocal]] / [[bindLocalWith]], carried as the
      * `(name, already-rendered declaration)` pair so a local that needs more than `def <name> =
      * <expr>;` can say so. See the B-1 note in `addParam`.
      */
    private[this] var _declarations
      : collection.mutable.Seq[Either[PainlessParam, (String, String)]] =
      collection.mutable.Seq.empty

    /** Bind `expr` to a fresh local declared in this script's PROLOGUE and return its name.
      *
      * A Painless `def x = …;` is a STATEMENT: it cannot sit inside a parenthesised operand, so a
      * guarded boolean cannot declare its own local inline once predicates are composed (story
      * BIDC-8, AD-9). Declaring it beside the `param` assignments keeps every operand a pure
      * expression and evaluates it once.
      */
    def bindLocal(expr: String, prefix: String = "left"): String =
      bindLocalWith(prefix)(name => s"def $name = $expr; ")

    /** Bind a local whose value cannot be WRITTEN as an expression, so the caller supplies the
      * whole declaration and is handed the name this context chose for it.
      *
      * 🔴 The one case that needs it (issue #367): Painless has no expression-level `try`/`catch`,
      * so `TRY_CAST` / `SAFE_CAST` render as a `try { … } catch …` STATEMENT. That is legal as the
      * tail of a whole script — which is why a `script_fields` projection of it works — and illegal
      * anywhere an operand goes: `def left1 = try { … };` is a compile error, and a predicate over
      * a safe cast is exactly that. Hoisted as statements into the prologue, the operand becomes a
      * name like every other.
      */
    def bindLocalWith(prefix: String)(declaration: String => String): String = {
      _locals += 1
      val name = s"$prefix${_locals}"
      _declarations = _declarations :+ Right(name -> declaration(name))
      name
    }

    def exists(token: Token): Boolean = {
      token match {
        case param: PainlessParam      => _keys.exists(_.contextKey == param.contextKey)
        case f: FunctionWithIdentifier => exists(f.identifier)
        case _                         => false
      }
    }

    def isEmpty: Boolean = _declarations.isEmpty

    // Review M-7: the complement of `isEmpty`, so a locals-only context cannot be both.
    def nonEmpty: Boolean = !isEmpty

    def last: Option[String] = _lastParam

    def find(paramName: String): Option[PainlessParam] = {
      val index = _values.indexOf(paramName)
      if (index >= 0) Some(_keys(index))
      else None
    }

    private[this] def paramValue(param: PainlessParam): String =
      if (param.nullable && param.checkNotNull.nonEmpty)
        param.checkNotNull
      else
        s"${param.param}${param.painlessMethods.mkString("")}"

    override def toString: String =
      _declarations
        .flatMap {
          case Left(param) =>
            get(param) match {
              case Some(v) => Some(s"def $v = ${paramValue(param)}; ")
              case None    => None // should not happen
            }
          // The local's declaration is already rendered (see `bindLocalWith`): a `TRY_CAST` needs
          // two statements, not one `def`.
          case Right((_, declaration)) => Some(declaration)
        }
        .mkString("")
  }

  trait PainlessParams extends PainlessScript {
    def params: Map[String, Any]
  }

  /** Trait for tokens that can be used in date math scripts
    */
  trait DateMathScript extends Token {
    def script: Option[String]
    override def dateMathScript: Boolean = true
    def formatScript: Option[String] = None
  }

  object DateMathRounding {
    def apply(out: SQLType): Option[String] =
      out match {
        case _: SQLDate => Some("/d")
        /*case _: SQLDateTime  => Some("/s")
        case _: SQLTimestamp => Some("/s")*/
        case _: SQLTime => Some("/s")
        case _          => None
      }
  }

  trait DateMathRounding {
    def roundingScript: Option[String] = None
    def hasRounding: Boolean = roundingScript.isDefined
  }

  trait Updateable extends Token {
    def update(request: SingleSearch): Updateable
  }

  abstract class Expr(override val sql: String) extends Token

  case object Distinct extends Expr("DISTINCT") with TokenRegex

  abstract class Value[+T](val value: T)
      extends DdlToken
      with PainlessScript
      with FunctionWithValue[T] {
    override def painless(context: Option[PainlessContext]): String =
      SQLTypeUtils.coerce(
        value match {
          // Escaped, since #274/story 21.5: an unescaped `"` produced a Painless SYNTAX error and a
          // trailing backslash an unterminated string. Byte-identical for a value carrying neither.
          case s: String  => s""""${escapePainlessString(s)}""""
          case b: Boolean => b.toString
          case n: Number  => n.toString
          case _          => value.toString
        },
        this.baseType,
        this.out,
        nullable = false,
        context
      )

    override def nullable: Boolean = false
  }

  object Value {
    def apply[R: TypeTag, T <: Value[R]](value: Any): Value[_] = {
      value match {
        case null       => Null
        case b: Boolean => BooleanValue(b)
        case c: Char    => CharValue(c)
        case s: String =>
          s match {
            case "null"                                        => Null
            case "_id" | "{{_id]]"                             => IdValue
            case "_ingest.timestamp" | "{{_ingest.timestamp}}" => IngestTimestampValue
            case _                                             => StringValue(s)
          }
        case b: Byte     => ByteValue(b)
        case s: Short    => ShortValue(s)
        case i: Int      => IntValue(i)
        case l: Long     => LongValue(l)
        case f: Float    => FloatValue(f)
        case d: Double   => DoubleValue(d)
        case a: Array[T] => apply(a.toSeq)
        case a: Seq[T] if a.nonEmpty =>
          val values = a.map(apply)
          values.headOption match {
            case Some(_: StringValue) =>
              StringValues(values.asInstanceOf[Seq[StringValue]]).asInstanceOf[Values[R, T]]
            case Some(_: ByteValue) =>
              ByteValues(values.asInstanceOf[Seq[ByteValue]]).asInstanceOf[Values[R, T]]
            case Some(_: ShortValue) =>
              ShortValues(values.asInstanceOf[Seq[ShortValue]]).asInstanceOf[Values[R, T]]
            case Some(_: IntValue) =>
              IntValues(values.asInstanceOf[Seq[IntValue]]).asInstanceOf[Values[R, T]]
            case Some(_: LongValue) =>
              LongValues(values.asInstanceOf[Seq[LongValue]]).asInstanceOf[Values[R, T]]
            case Some(_: FloatValue) =>
              FloatValues(values.asInstanceOf[Seq[FloatValue]]).asInstanceOf[Values[R, T]]
            case Some(_: DoubleValue) =>
              DoubleValues(values.asInstanceOf[Seq[DoubleValue]]).asInstanceOf[Values[R, T]]
            case Some(_: BooleanValue) =>
              BooleanValues(values.asInstanceOf[Seq[BooleanValue]])
                .asInstanceOf[Values[R, T]]
            case Some(_: ObjectValue) =>
              ObjectValues(
                values
                  .asInstanceOf[Seq[ObjectValue]]
              ).asInstanceOf[Values[R, T]]
            case _ => throw new IllegalArgumentException("Unsupported Values type")
          }
        case _: Seq[T] => EmptyValues().asInstanceOf[Values[R, T]]
        case o: Map[_, _] =>
          val map = o.asInstanceOf[Map[String, Any]].map { case (k, v) => k -> apply(v) }.toList
          ObjectValue(ListMap(map: _*))
        case other => StringValue(other.toString)
      }
    }

    /** The raw Scala value behind a [[Value]], all the way down.
      *
      * 🔴 21.8 Part F.1. `Value.value` is NOT this for a container: `Values.value` is the
      * `Seq[Value[_]]` of WRAPPED elements and `ObjectValue.value` is a `ListMap[String,
      * Value[_]]`. Handing either to `mapToJsonNode` serialises each element as a Jackson BEAN —
      * `{"value":"a","regex":{…},"expr":{…}}` — so a list-valued ingest processor was written back
      * to Elasticsearch in a shape Elasticsearch would store verbatim. MEASURED both ways: from
      * `.value` the JSON is that bean; from here it is `["a","b"]`.
      *
      * Recursive, because one level is not enough: `{"tags":["a","b"],"n":1}` still emitted beans
      * for `tags` after the outer map was unwrapped.
      *
      * `Values.innerValues` is the one-level cousin of this and stays as it is — it is typed
      * `Seq[R]`, which is what its callers want, and it cannot express the nested case.
      */
    def unwrap(value: Value[_]): Any = value match {
      case Null                 => null
      case values: Values[_, _] => values.values.map(unwrap)
      case obj: ObjectValue     => obj.value.map { case (k, v) => k -> unwrap(v) }
      case other                => other.value
    }

    def apply(node: JsonNode): Option[Any] = {
      node match {
        case n if n.isNull    => Some(null)
        case n if n.isTextual => Some(n.asText())
        case n if n.isBoolean => Some(n.asBoolean())
        case n if n.isShort   => Some(node.asInstanceOf[Short])
        case n if n.isInt     => Some(n.asInt())
        case n if n.isLong    => Some(n.asLong())
        case n if n.isDouble  => Some(n.asDouble())
        case n if n.isFloat   => Some(node.asInstanceOf[Float])
        case n if n.isArray =>
          import scala.jdk.CollectionConverters._
          val arr = n
            .elements()
            .asScala
            .flatMap(apply)
            .toList
          Some(arr)
        case n if n.isObject =>
          val map = n
            .properties()
            .asScala
            .flatMap { entry =>
              val key = entry.getKey
              val valueNode = entry.getValue
              apply(valueNode).map(value => key -> value)
            }
            .toMap
          Some(map)
        case _ =>
          None
      }
    }
  }

  sealed trait Diff

  case class Altered(name: String, value: Value[_]) extends Diff

  case class Removed(name: String) extends Diff

  case class ObjectValue(override val value: ListMap[String, Value[_]])
      extends Value[Map[String, Value[_]]](value) {
    override def sql: String = value
      .map { case (k, v) => s"""$k = ${v.sql}""" }
      .mkString("(", ", ", ")")
    override def baseType: SQLType = SQLTypes.Struct
    override def ddl: String = value
      .map { case (k, v) =>
        v match {
          case IdValue | IngestTimestampValue => s"""$k = "${v.ddl}""""
          case _                              => s"""$k = ${v.ddl}"""
        }
      }
      .mkString("(", ", ", ")")

    /** Recursion is on the HEAD of the path, one level at a time. Descending to the LEAF's parent
      * and then re-attaching it under `keys.head` collapsed the intermediate levels: for a path of
      * depth 3 or more, `_meta.columns.<c>.default_value` replaced the whole of `_meta` with the
      * innermost object, dropping every sibling key. Depth 1 and 2 happened to be correct, which is
      * why it survived — the metadata paths the schema writes are exactly the deeper ones.
      */
    def set(path: String, newValue: Value[_]): ObjectValue = {
      path.split("\\.").toList match {
        case Nil      => this
        case k :: Nil => ObjectValue(value + (k -> newValue))
        case k :: rest =>
          val child = value.get(k) match {
            case Some(obj: ObjectValue) => obj
            case _                      => ObjectValue.empty
          }
          ObjectValue(value + (k -> child.set(rest.mkString("."), newValue)))
      }
    }

    def remove(path: String): ObjectValue = {
      path.split("\\.").toList match {
        case Nil      => this
        case k :: Nil => ObjectValue(value - k)
        case k :: rest =>
          value.get(k) match {
            case Some(obj: ObjectValue) =>
              ObjectValue(value + (k -> obj.remove(rest.mkString("."))))
            // nothing at that path: removing is a no-op, NOT a reason to overwrite `k`
            case _ => this
          }
      }
    }

    def find(path: String): Option[Value[_]] = {
      val keys = path.split("\\.")
      @tailrec
      def loop(current: Map[String, Value[_]], remainingKeys: Seq[String]): Option[Value[_]] = {
        remainingKeys match {
          case Seq()  => None
          case Seq(k) => current.get(k)
          case k +: ks =>
            current.get(k) match {
              case Some(obj: ObjectValue) => loop(obj.value, ks)
              case _                      => None
            }
        }
      }
      loop(value, keys.toSeq)
    }

    def diff(other: ObjectValue, path: Option[String] = None): List[Diff] = {
      val diffs = scala.collection.mutable.ListBuffer[Diff]()

      val actual = this.value
      val desired = other.value

      val allKeys = actual.keySet ++ desired.keySet

      allKeys.foreach { key =>
        val computedKey = s"${path.map(p => s"$p.").getOrElse("")}$key"
        (actual.get(key), desired.get(key)) match {
          case (None, Some(v)) =>
            diffs += Altered(computedKey, v)
          case (Some(_), None) =>
            diffs += Removed(computedKey)
          case (Some(a), Some(b)) =>
            b match {
              case ObjectValue(_) =>
                a match {
                  case ObjectValue(_) =>
                    diffs ++= a
                      .asInstanceOf[ObjectValue]
                      .diff(
                        b.asInstanceOf[ObjectValue],
                        Some(computedKey)
                      )
                  case _ =>
                    diffs += Altered(computedKey, b)
                }
              case _ =>
                if (a != b) {
                  diffs += Altered(computedKey, b)
                }
            }
          case _ =>
        }
      }

      diffs.toList
    }

    import app.softnetwork.elastic.sql.serialization._

    def toJson: JsonNode = this

  }

  object ObjectValue {
    import app.softnetwork.elastic.sql.serialization._

    def empty: ObjectValue = ObjectValue(ListMap.empty)

    def fromJson(jsonNode: JsonNode): ObjectValue = jsonNode

    def parseJson(jsonString: String): ObjectValue = jsonString
  }

  case object Null extends Value[Null](null) with TokenRegex {
    override def sql: String = "NULL"
    override def painless(context: Option[PainlessContext]): String = "null"
    override def nullable: Boolean = true
    override def baseType: SQLType = SQLTypes.Null
  }

  case object ParamValue extends Value[String](null) with TokenRegex {
    override def sql: String = "?"
    override def painless(context: Option[PainlessContext]): String = "params.paramValue"
    override def nullable: Boolean = true
    override def baseType: SQLType = SQLTypes.Any
  }

  case class BooleanValue(override val value: Boolean) extends Value[Boolean](value) {
    override def sql: String = value.toString
    override def baseType: SQLType = SQLTypes.Boolean
  }

  case class CharValue(override val value: Char) extends Value[Char](value) {
    override def sql: String = s"""'$value'"""
    override def baseType: SQLType = SQLTypes.Char
  }

  case class StringValue(override val value: String) extends Value[String](value) {
    // Escaped in the backslash form the grammar's literal accepts — backslash first, then the
    // quote, so the composition reverses. Without this a value holding an apostrophe rendered
    // `'it's'`, which no longer parses. The grammar ALSO accepts the SQL-standard doubled quote on
    // input (#274); the render deliberately stays backslash-escaped (story 21.5 AD-2), so
    // `SELECT 'O''Brien'` re-renders `'O\'Brien'` and re-parses to an equal AST.
    override def sql: String = s"""'${escapeStringLiteral(value)}'"""
    override def baseType: SQLType = SQLTypes.Varchar

    override def ddl: String = s""""${value.replace("\\", "\\\\").replace("\"", "\\\"")}""""
  }

  case object IdValue extends Value[String]("_id") with TokenRegex {
    override def sql: String = value
    override def painless(context: Option[PainlessContext]): String = s"{{$value}}"
    override def baseType: SQLType = SQLTypes.Varchar
    override def ddl: String = value
  }

  case object IngestTimestampValue extends Value[String]("_ingest.timestamp") with TokenRegex {
    override def sql: String = value
    override def painless(context: Option[PainlessContext]): String =
      s"{{$value}}"
    override def baseType: SQLType = SQLTypes.Timestamp
    override def ddl: String = value
  }

  sealed abstract class NumericValue[T: Numeric](override val value: T) extends Value[T](value) {
    override def sql: String = value.toString
    private[this] val num: Numeric[T] = implicitly[Numeric[T]]
    def toDouble: Double = num.toDouble(value)
    def toEither: Either[Long, Double] = value match {
      case l: Long   => Left(l)
      case i: Int    => Left(i.toLong)
      case d: Double => Right(d)
      case f: Float  => Right(f.toDouble)
      case _         => Right(toDouble)
    }
    override def baseType: SQLNumeric = SQLTypes.Numeric
  }

  case class ByteValue(override val value: Byte) extends NumericValue[Byte](value) {
    override def baseType: SQLNumeric = SQLTypes.TinyInt
  }

  case class ShortValue(override val value: Short) extends NumericValue[Short](value) {
    override def baseType: SQLNumeric = SQLTypes.SmallInt
  }

  case class IntValue(override val value: Int) extends NumericValue[Int](value) {
    override def baseType: SQLNumeric = SQLTypes.Int
  }

  case class LongValue(override val value: Long) extends NumericValue[Long](value) {
    override def baseType: SQLNumeric = SQLTypes.BigInt
  }

  case class FloatValue(override val value: Float) extends NumericValue[Float](value) {
    override def baseType: SQLNumeric = SQLTypes.Real
  }

  case class DoubleValue(override val value: Double) extends NumericValue[Double](value) {
    override def baseType: SQLNumeric = SQLTypes.Double
  }

  case object PiValue extends Value[Double](Math.PI) with TokenRegex {
    override def sql: String = "PI"
    override def painless(context: Option[PainlessContext]): String = "Math.PI"
    override def baseType: SQLNumeric = SQLTypes.Double
  }

  case object RandomValue extends Value[Double](Math.random()) with TokenRegex {
    override def sql: String = "RANDOM"
    override def painless(context: Option[PainlessContext]): String = "Math.random()"
    override def baseType: SQLNumeric = SQLTypes.Double
  }

  case object EValue extends Value[Double](Math.E) with TokenRegex {
    override def sql: String = "E"
    override def painless(context: Option[PainlessContext]): String = "Math.E"
    override def baseType: SQLNumeric = SQLTypes.Double
  }

  case class GeoDistance(longValue: LongValue, unit: DistanceUnit)
      extends NumericValue[Double](DistanceUnit.convertToMeters(longValue.value, unit))
      with PainlessScript {
    override def baseType: SQLNumeric = SQLTypes.Double
    override def sql: String = s"$longValue $unit"
    def geoDistance: String = s"$longValue$unit"
    override def painless(context: Option[PainlessContext]): String = s"$value"
  }

  sealed abstract class FromTo(val from: TokenValue, val to: TokenValue) extends Token {
    override def sql = s"${from.sql} AND ${to.sql}"

    override def baseType: SQLType =
      SQLTypeUtils.leastCommonSuperType(List(from.baseType, to.baseType))

    override def validate(): Either[String, Unit] = {
      for {
        _ <- from.validate()
        _ <- to.validate()
        _ <- Validator.validateTypesMatching(from.out, to.out)
      } yield ()
    }
  }

  case class LiteralFromTo(override val from: StringValue, override val to: StringValue)
      extends FromTo(from, to) {
    def between: Seq[String] => Boolean = {
      _.exists { s => s >= from.value && s <= to.value }
    }
    def notBetween: Seq[String] => Boolean = {
      _.forall { s => s < from.value || s > to.value }
    }
  }

  case class LongFromTo(override val from: LongValue, override val to: LongValue)
      extends FromTo(from, to) {
    def between: Seq[Long] => Boolean = {
      _.exists { n => n >= from.value && n <= to.value }
    }
    def notBetween: Seq[Long] => Boolean = {
      _.forall { n => n < from.value || n > to.value }
    }
  }

  case class DoubleFromTo(override val from: DoubleValue, override val to: DoubleValue)
      extends FromTo(from, to) {
    def between: Seq[Double] => Boolean = {
      _.exists { n => n >= from.value && n <= to.value }
    }
    def notBetween: Seq[Double] => Boolean = {
      _.forall { n => n < from.value || n > to.value }
    }
  }

  case class GeoDistanceFromTo(override val from: GeoDistance, override val to: GeoDistance)
      extends FromTo(from, to) {
    def between: Seq[Double] => Boolean = {
      _.exists { n => n >= from.value && n <= to.value }
    }
    def notBetween: Seq[Double] => Boolean = {
      _.forall { n => n < from.value || n > to.value }
    }
  }

  case class IdentifierFromTo(override val from: Identifier, override val to: Identifier)
      extends FromTo(from, to)

  sealed abstract class Values[+R: TypeTag, +T <: Value[R]](val values: Seq[T])
      extends Value[Seq[T]](values) {
    override def sql = s"(${values.map(_.sql).mkString(",")})"
    override def painless(context: Option[PainlessContext]): String =
      s"[${values.map(_.painless(context)).mkString(",")}]"
    lazy val innerValues: Seq[R] = values.map(_.value)
    override def nullable: Boolean = values.exists(_.nullable)
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.Any)

    override def ddl: String = s"[${values.map(_.ddl).mkString(",")}]"
  }

  case class StringValues(override val values: Seq[StringValue])
      extends Values[String, Value[String]](values) {
    def eq: Seq[String] => Boolean = {
      _.exists { s => innerValues.exists(_.contentEquals(s)) }
    }
    def ne: Seq[String] => Boolean = {
      _.forall { s => innerValues.forall(!_.contentEquals(s)) }
    }
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.Varchar)
  }

  class NumericValues[R: TypeTag](override val values: Seq[NumericValue[R]])
      extends Values[R, NumericValue[R]](values) {
    def eq: Seq[R] => Boolean = {
      _.exists { n => innerValues.contains(n) }
    }
    def ne: Seq[R] => Boolean = {
      _.forall { n => !innerValues.contains(n) }
    }
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.Numeric)
  }

  case class ByteValues(override val values: Seq[ByteValue]) extends NumericValues[Byte](values) {
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.TinyInt)
  }

  case class ShortValues(override val values: Seq[ShortValue])
      extends NumericValues[Short](values) {
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.SmallInt)
  }

  case class IntValues(override val values: Seq[IntValue]) extends NumericValues[Int](values) {
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.Int)
  }

  case class LongValues(override val values: Seq[LongValue]) extends NumericValues[Long](values) {
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.BigInt)
  }

  case class FloatValues(override val values: Seq[FloatValue])
      extends NumericValues[Float](values) {
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.Real)
  }

  case class DoubleValues(override val values: Seq[DoubleValue])
      extends NumericValues[Double](values) {
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.Double)
  }

  case class BooleanValues(override val values: Seq[BooleanValue])
      extends Values[Boolean, Value[Boolean]](values) {
    def eq: Seq[Boolean] => Boolean = {
      _.exists { b => innerValues.contains(b) }
    }
    def ne: Seq[Boolean] => Boolean = {
      _.forall { b => !innerValues.contains(b) }
    }
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.Boolean)
  }

  case class ObjectValues(override val values: Seq[ObjectValue])
      extends Values[Map[String, Value[_]], ObjectValue](values) {
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.Struct)
    import app.softnetwork.elastic.sql.serialization._

    def toJson: JsonNode = this
  }

  case class EmptyValues() extends Values[Any, Null](Seq.empty) {
    override def sql: String = "()"
    override def nullable: Boolean = true
  }

  /** A SQL `LIKE` pattern as a regular expression.
    *
    * 🔴 Story BIDC-8 (round 10, HIGH-2). This used to replace `%` and `_` and NOTHING else, so
    * every other regex metacharacter in the pattern kept its REGEX meaning — and since a pattern
    * only becomes a regex on some paths, the same SQL then meant different things depending on
    * where it ran. MEASURED on live ES 8.18.3 over `A.B` and `AXB1`: `status LIKE 'A.B%'` (native
    * `regexp`) matched BOTH, `UPPER(status) LIKE 'A.B%'` (string methods) matched only `A.B`, and
    * `UPPER(status) LIKE 'A.B%1'` (Painless regex) matched only `AXB1` — three readings of one
    * pattern. In SQL only `%` and `_` are wildcards; `.` is a literal. Escaping here fixes all of
    * them in ONE place, which is the point: the native `regexp` query and the scripted forms read
    * the shared translation, so they cannot disagree.
    *
    * ⚠️ USER-VISIBLE: `LIKE 'A.B%'` no longer matches `AXB1`. That is the correct SQL reading, and
    * it is a 0.23.0 release note.
    *
    * The escaped set is the union of the Java and Lucene regex metacharacters. Every one of them is
    * a legal backslash escape in BOTH engines (Java only forbids escaping an ALPHABETIC character
    * that is not a known construct), so one escaping rule serves the query DSL and Painless alike.
    */
  def toRegex(value: String): String = {
    val metacharacters = "\\.[]{}()*+-?^$|#@&<>~\""
    val out = new StringBuilder(value.length * 2)
    value.foreach {
      case '%'                                 => out.append(".*")
      case '_'                                 => out.append('.')
      case c if metacharacters.indexOf(c) >= 0 => out.append('\\').append(c)
      case c                                   => out.append(c)
    }
    out.toString
  }

  case object Alias extends Expr("AS") with TokenRegex

  /** `quoted` mirrors `Identifier.quoted` (story 21.1 AD-1): it records that the alias was written
    * inside quotes, so `.sql` can re-emit it and re-parse to an equal AST. `alias` itself stays the
    * bare string -- it is what `Field.fieldAlias`, `scriptName` and the script_fields keys use, and
    * none of those may ever see a delimiter.
    */
  case class Alias(alias: String, quoted: Boolean = false)
      extends Expr(s" ${Alias.sql} ${if (quoted) quoteIdentifier(alias) else alias}")

  object AliasUtils {
    private val MaxAliasLength = 50

    private val opMapping = Map(
      "+" -> "plus",
      "-" -> "minus",
      "*" -> "mul",
      "/" -> "div",
      "%" -> "mod"
    )

    def normalize(expr: String): String = {
      // Remplacer les opérateurs SQL par des noms lisibles
      val replaced = opMapping.foldLeft(expr) { case (acc, (k, v)) =>
        acc.replace(k, s"_${v}_")
      }
      // Nettoyer pour obtenir un identifiant valide
      NamingUtils.normalizeObjectName(replaced, MaxAliasLength).replaceAll("\\.", "_")
    }
  }

  trait TokenRegex extends Token {
    def words: List[String] = List(sql)
    // A literal space in a multi-word token (`GROUP BY`, `UNION ALL`, `IS NOT NULL`, …) matches
    // one space and no other whitespace, so keyword-aligned SQL — `GROUP  BY`, or a newline
    // between the words — missed the token. That used to be invisible: the production simply did
    // not match and the rest of the statement was discarded. Now that `Parser.apply` requires the
    // whole input to be consumed it would be a hard rejection of valid SQL, so accept any run of
    // whitespace between the words. `\s+` cannot widen what matches otherwise: every alternative
    // still has to match the same words in the same order.
    lazy val regex: Regex =
      s"(?i)(${words.map(_.replace(" ", "\\s+")).mkString("|")})\\b".r
  }

  trait Source extends Updateable {
    def name: String
    def update(request: SingleSearch): Source
  }

  sealed trait Identifier
      extends TokenValue
      with Source
      with FunctionChain
      with PainlessScript
      with DateMathScript
      with PainlessParam {
    def name: String

    def withFunctions(functions: List[Function]): Identifier

    def update(request: SingleSearch): Identifier

    def tableAlias: Option[String]
    def table: Option[String]
    def distinct: Boolean

    /** True iff any part of this name was written inside quotes -- `"col"`, a backticked `col`, or
      * a qualified mix of the two. It records ONLY that quoting was used, never which delimiter:
      * the two spellings denote the same column, so they must produce EQUAL ASTs (story 21.1 AD-1).
      * Consumed by `sql` alone; `name`, `identifierName`, `path` and everything that reaches
      * Elasticsearch stay unquoted.
      */
    def quoted: Boolean
    def nested: Boolean
    def nestedElement: Option[NestedElement]
    def limit: Option[Limit]
    def fieldAlias: Option[String]
    def bucket: Option[Bucket]
    def hasBucket: Boolean = bucket.isDefined

    lazy val aggregations: Seq[AggregateFunction] = FunctionUtils.aggregateFunctions(this)

    def bucketPath: String

    lazy val allMetricsPath: Map[String, String] = {
      metricName match {
        case Some(name) => Map(name -> name)
        // The alias of a SELECT `bucket_script` item referenced from HAVING (`... AS d ... HAVING
        // d > 3`): the selector reads the sibling pipeline aggregation by that name.
        case _ if hasAggregation && fieldAlias.isDefined => Map(aliasOrName -> aliasOrName)
        case _                                           => Map.empty
      }
    }

    override def sql: String = {
      var parts: Seq[String] = name.split("\\.").toSeq
      tableAlias match {
        case Some(a) => parts = a +: (if (nested) parts.tail else parts)
        case _       =>
      }
      // Quote EACH dot-separated part -- `"e"."category"`, not `"e.category"`. Both re-parse to the
      // same AST as far as THIS parser is concerned, so the tie is broken downstream:
      // softclient4es-arrow's JoinPlanner builds its DuckDB SELECT list out of `identifier.sql`,
      // and DuckDB reads `"e.category"` as one column literally named `e.category` -- a binder
      // error -- where `"e"."category"` is the qualified reference the statement means.
      val rendered =
        if (quoted)
          // NOT trimmed, unlike the bare branch below: a quoted name means exactly what is inside
          // the delimiters, so `` `a ` `` is the field `a ` and trimming it would render `"a"` and
          // re-parse to a DIFFERENT field. (Measured: trimming per part broke the fixed point for
          // every name with an edge space.)
          //
          // An EMPTY part is never quoted: `""` is not an identifier in the grammar (the content
          // quantifier is `+`), so emitting one would produce a rendering that cannot be re-parsed.
          // Unreachable today -- `Parser.unquoteName` cannot return "" -- but the fixed point is
          // the whole contract, so it is a guard, not an assumption.
          parts.map(p => if (p.isEmpty) p else quoteIdentifier(p)).mkString(".")
        else parts.mkString(".").trim
      val sql = if (distinct) s"$Distinct $rendered".trim else rendered
      functions.reverse.foldLeft(sql)((expr, fun) => {
        fun.toSQL(expr)
      })
    }

    applyTo(this)

    lazy val identifierName: String =
      functions.reverse.foldLeft(name)((expr, fun) => {
        fun.toSQL(expr)
      }) // TODO use AliasUtils.normalize?

    lazy val innerHitsName: Option[String] =
      nestedElement match {
        case Some(ne) => Some(ne.innerHitsName)
        case None     => None
      }

    lazy val aliasOrName: String = fieldAlias.getOrElse(name)

    lazy val path: String =
      nestedElement match {
        case Some(ne) =>
          name.split("\\.") match {
            case Array(_, _*) => s"${ne.path}.${name.split("\\.").tail.mkString(".")}"
            case _            => s"${ne.path}.$name"
          }
        case None => name
      }

    lazy val paramName: String =
      if (isAggregation && functions.size == 1) metricParam
      else if (path.nonEmpty)
        s"doc['$path'].value"
      else ""

    /** The name an AGGREGATE identifier is addressed by in a bucket pipeline: the `buckets_path`
      * key, the `params.<name>` a `bucket_selector` / `bucket_script` reads, and -- when the
      * aggregate is not itself a SELECT item -- the name of the auxiliary aggregation created for
      * it (`Criteria.extractAggregationFields` / `FieldSort.extractAggregationFields` alias the
      * synthesised Field with it, and both `SQLAggregation.fromField` and the bridge name the
      * aggregation after that alias). One rule, one name: the path resolves by construction.
      *
      * Aliased: the alias. Un-aliased: a name derived from the WHOLE expression, never the bare
      * field name. The bare name collapsed distinct aggregates over one field (`COUNT(x)` and
      * `MAX(x)` were both `x`; `auxiliaryAggs` dedups by name, so the second silently vanished and
      * every HAVING term compared the first -- issue #54), let a dotted field leak into a `params.`
      * key (`params.emails.address` is a nested-property read Painless rejects, and the selector's
      * name scanner then dropped the condition altogether), and named an aggregate over a
      * self-contained function (`MAX(ABS(salary))`, whose identifier has no field-derived `name`)
      * `""` (issue #223). `COUNT(*)` keeps its long-standing `count_all` / `count_distinct_all`.
      */
    lazy val metricName: Option[String] =
      aggregateFunction.map { af =>
        fieldAlias.getOrElse {
          af match {
            case COUNT | _: CountAgg if name == "*" =>
              if (distinct) "count_distinct_all" else "count_all"
            case _ =>
              val operand = if (distinct) s"$Distinct $name" else name
              AliasUtils.normalize(
                functions.reverse.foldLeft(operand)((expr, fun) => fun.toSQL(expr))
              )
          }
        }
      }

    /** How a bucket pipeline script reads this aggregate: the metric Elasticsearch already
      * computed, published under `buckets_path` as [[metricName]].
      */
    lazy val metricParam: String = s"params.${metricName.getOrElse(aliasOrName)}"

    lazy val script: Option[String] =
      if (isTemporal) {
        var orderedFunctions = FunctionUtils.transformFunctions(this).reverse

        val baseOpt: Option[String] = orderedFunctions.headOption match {
          case Some(head) =>
            head match {
              case s: StringValue if s.value.nonEmpty =>
                orderedFunctions = orderedFunctions.tail
                Some(s.value + "||")
              case current: CurrentFunction =>
                orderedFunctions = orderedFunctions.tail
                current.script
              case _ => Option(name).filter(_.nonEmpty).map(_ + "||")
            }
          case _ => Option(name).filter(_.nonEmpty).map(_ + "||")
        }

        val roundingOpt: Option[String] =
          orderedFunctions
            .collectFirst {
              case r: DateMathRounding if r.hasRounding => r.roundingScript.get
            }
            .orElse(DateMathRounding(out))

        orderedFunctions.foldLeft(baseOpt) {
          case (expr, f: Function) if expr.isDefined && f.dateMathScript =>
            f match {
              case s: DateMathScript =>
                s.script match {
                  case Some(script) if script.nonEmpty =>
                    Some(s"${expr.get}$script")
                  case _ => expr
                }
              case _ => expr
            }
          case (_, _) => None // ignore non math scripts
        } match {
          case Some(s) if s.nonEmpty =>
            roundingOpt match {
              case Some(r) if r.nonEmpty => Some(s"$s$r")
              case _                     => Some(s)
            }
          case _ => None
        }
      } else
        None

    override def dateMathScript: Boolean = isTemporal

    def checkNotNull: String =
      if (path.isEmpty) ""
      else
        s"(doc['$path'].size() == 0 ? $nullValue : doc['$path'].value${painlessMethods.mkString("")})"

    lazy val processParamName: String = {
      if (path.nonEmpty) {
        if (path.contains("."))
          s"ctx.${path.split("\\.").mkString("?.")}"
        else
          s"ctx.$path"
      } else ""
    }

    lazy val processCheckNotNull: Option[String] =
      if (path.isEmpty || !nullable) None
      else
        Option(
          s"($processParamName == null ? $nullValue : $processParamName${painlessMethods.mkString("")})"
        )

    lazy val transformParamName: String =
      if (isAggregation && functions.size == 1) s"params.${metricName.getOrElse(aliasOrName)}"
      else if (aliasOrName.nonEmpty)
        s"doc['$aliasOrName'].value"
      else ""

    lazy val transformCheckNotNull: Option[String] =
      if (aliasOrName.isEmpty || !nullable) None
      else
        Option(
          s"(doc['$aliasOrName'].size() == 0 ? $nullValue : doc['$aliasOrName'].value${painlessMethods
            .mkString("")})"
        )

    def originalType: SQLType =
      if (name.trim.nonEmpty) SQLTypes.Any
      else this.baseType

    /** The type of the expression [[painless]] actually RENDERS — which is NOT what [[baseType]]
      * reports for a function-wrapped column.
      *
      * 🔴 Issue #367. A schema-resolved identifier overrides `baseType` with the COLUMN's runtime
      * type and stops there, so `WEEKDAY(d)` claims to be a `TIMESTAMP` while the string it renders
      * is the `int` the extraction produced, and `DATE_FORMAT(d, 'yyyy')` claims `TIMESTAMP` while
      * rendering a `String`. Every `SQLTypeUtils.coerce(in, to, ctx)` call reads the type of the
      * string it was just handed (see the note on `Criteria.baseType`, which fixed the same lie for
      * a comparison: *the source type has to be right before `coerce` is called*), so the coercion
      * inserted a temporal conversion on a value that is no longer temporal.
      *
      * The OUTERMOST function's own `out` is that type: the chain is applied innermost-first, so
      * the last type to be produced is the outermost function's. Reading it is also PURE, which a
      * fold over `applyType` is not — `IntervalFunction.applyType` calls `cast`, which WRITES
      * `_out`, so computing this by folding changed `DATE_ADD(d, INTERVAL 1 DAY)`'s own `out` from
      * DATE to TIMESTAMP and with it the rendering of that expression everywhere else (found by
      * review). It is also more accurate: that operand renders `…toLocalDate().plus(1,
      * ChronoUnit.DAYS)`, a `LocalDate`, which is DATE.
      *
      * ⚠️ Deliberately a SEPARATE derivation rather than a correction of `baseType`: `baseType`
      * feeds `out`, and `out` feeds `isTemporal`, which decides whether an identifier renders as
      * DATE MATH (`d||+1d`) instead of a script. Making `baseType` chain-aware sent `ORDER BY
      * DATE_PARSE(name, 'x')` down the date-math branch, which assembles its expression from the
      * COLUMN name and produced the nonsense `name||name||/d` (MEASURED). That branch's real
      * precondition is that the chain's BASE is temporal, not its output, and repairing it is a
      * separate change with its own blast radius (sorts, date math, validation, reported column
      * types) — recorded on #367 rather than smuggled in here.
      *
      * ⚠️ An AGGREGATE reports `out`, unchanged: its predicate is rendered context-free as a
      * bucket-pipeline read of the metric Elasticsearch already computed
      * (`MetricSelectorScript.metricSelector` calls `painless(None)`, which returns from
      * `bucketPipelinePainless` before any operand is coerced), so the chain type would describe a
      * rendering this identifier never produces.
      */
    def chainType: SQLType =
      functions.headOption match {
        case Some(f) if !isAggregation => f.out
        case _                         => out
      }

    /** The type the operand's rendering really HAS at run time — the JAVA type, where [[chainType]]
      * is the SQL one (issue #384).
      *
      * 🔴 The two differ for the whole temporal family and that difference is the defect. Every
      * adjuster returns its RECEIVER's type (#368), so `DATE_ADD(ts, INTERVAL 1 DAY)` reports DATE
      * while rendering a `ZonedDateTime` and `DATE_TRUNC(d, MONTH)` reports TEMPORAL for the same
      * reason. Only a function that PRODUCES its output type changes the answer, and the function
      * is what knows: `TransformFunction.producesOutputJavaType`.
      *
      * So: scan the chain OUTERMOST inward and take the first producer with a temporal output;
      * failing that, the column's own type. Three shapes PR #379 could not reach, each measured:
      *
      *   - `DATE_TRUNC(CAST(d AS DATE), MONTH)` — a preserving function ABOVE a producer, so
      *     `chainType` (TEMPORAL) described neither side;
      *   - `DATE_PARSE(DATE_FORMAT(d, …), …)` over a TEMPORAL column — a producer that is not a
      *     `Conversion`, which the old `convertsToLocal` predicate enumerated by class;
      *   - `CAST(ts AS TIME)` — a producer whose output is TIME, excluded there as dead code.
      *
      * ⚠️ Asked only of a TEMPORAL chain. For everything else the SQL type and the Java type agree
      * closely enough that `chainType` is already the answer, and widening the question would drag
      * `String.valueOf` / numeric widening into a derivation that has nothing to say about them.
      *
      * ⚠️ An AGGREGATE reports `chainType`, for the reason [[chainType]] already records: its
      * predicate is rendered context-free as a bucket-pipeline read of a metric Elasticsearch has
      * already computed, so no operand of it is ever coerced.
      */
    def renderedType: SQLType = {
      val ct = chainType
      if (isAggregation || !ct.isInstanceOf[SQLTemporal]) ct
      else
        FunctionUtils
          .transformFunctions(this)
          .collectFirst {
            case f: TransformFunction[_, _]
                if f.producesOutputJavaType && f.outputType.isInstanceOf[SQLTemporal] =>
              f.outputType
          }
          .getOrElse(if (baseType.isInstanceOf[SQLTemporal]) baseType else ct)
    }

    /** The type the column was DECLARED as, where that differs from what a query hands Painless.
      * Defaults to `baseType`; only a schema-resolved identifier can tell them apart.
      *
      * ⚠️ This is the LEAF question — what THIS column says it is — and the ingest venue depends on
      * it being exactly that (`SQLTypeUtils.processorTemporal` asks it to decide how to parse
      * `ctx.<field>`). It is deliberately NOT chain-aware; the chain question is [[reportedType]].
      */
    def declaredType: SQLType = baseType

    /** This identifier's own contribution to [[reportedType]], before any function applies. Split
      * out so `GenericIdentifier` can supply the column's DECLARATION without `reportedType` having
      * to know whether a schema was attached.
      */
    def reportedLeafType: SQLType = baseType

    /** What a consumer that DECLARES a column for this expression should report (see
      * [[PainlessScript.reportedType]]).
      *
      * The chain answers it: the outermost function decides, computing from its arguments' own
      * `reportedType`, so the leaf declarations propagate outward instead of the runtime collapse
      * doing. With no functions this IS the leaf.
      *
      * ⚠️ An AGGREGATE reports `out`, for the reason [[chainType]] already records.
      */
    override def reportedType: SQLType =
      functions.headOption match {
        case Some(f) if !isAggregation =>
          // 🔴 A function that has NOT refined the question answers `out`, and taking that verbatim
          // would be a LOSS: the outermost function's own `out` is coarser than the chain's for the
          // whole adjuster family, which declares the un-narrowed TEMPORAL (#368). So
          // `DATE_PARSE(…) - INTERVAL 2 DAY` would report TEMPORAL where the chain says DATE. Only
          // a function that actually says something different gets to override.
          //
          // 🔴 And the fallback is [[renderedType]], NOT `out`: on a schema-resolved identifier
          // `out` reports the COLUMN's type rather than the chain's, which is the very defect #382
          // repaired -- `CAST(s AS BIGINT)` over a KEYWORD column answers KEYWORD there. MEASURED:
          // falling back to `out` reported `NULLIF(CAST(s AS BIGINT), 0)` as KEYWORD.
          val refined = f.reportedType
          if (refined == f.out) renderedType else refined
        case _ => reportedLeafType
      }

    override def painless(context: Option[PainlessContext]): String = {
      // A context-free rendering of an AGGREGATE is a bucket-pipeline rendering (`bucket_selector`
      // for HAVING, `bucket_script` for arithmetic over aggregates): there is no document to read,
      // only the metric Elasticsearch already computed, published under `buckets_path`. The
      // transform chain (`MAX(YEAR(x))`) belongs to the METRIC aggregation's own script, rendered by
      // the context-bearing call below; re-applying it here produced a `doc[...]` read -- which a
      // bucket script cannot do -- and a doubled transform in the selector (issue #223).
      if (context.isEmpty && isAggregation) return metricParam
      val orderedFunctions = FunctionUtils.transformFunctions(this).reverse
      var currType = this.originalType
      currType match {
        case SQLTypes.Any =>
          orderedFunctions.headOption match {
            case Some(f: TransformFunction[_, _]) =>
              f.in match {
                case SQLTypes.Temporal => // the first function to apply required a Temporal as input type
                  context match {
                    case Some(_) =>
                      // compatible ES6+ -- see `painlessUtcZonedDateTime` for WHY. PREPENDED
                      // (issue #370): a `.toLocalDate()` narrowing may already be on this object
                      // from coercion, and the normalisation applies to the RAW value under it.
                      // Before #370 the two often sat on DIFFERENT instances of one column and
                      // the fold landed on whichever was registered, which hid the order.
                      this.prependPainlessMethod(painlessUtcZonedDateTime)
                      currType = SQLTypes.Timestamp
                    case _ => // do nothing
                  }
                case _ => // do nothing
              }
            case _ => // do nothing
          }
        case _ => // do nothing
      }

      /** 🔴 In an INGEST script the operand is `ctx.<field>` — the RAW JSON value — so a temporal
        * function applied to it ran `String.get(ChronoField)` and threw; `ignore_failure: true`
        * swallowed the throw and the computed column was silently ABSENT. Measured on real
        * Elasticsearch 8.18.3 for `YEAR`, `MONTH`, `DATE_TRUNC` and `DATE_ADD`, and
        * `DATE_DIFF(birthdate, CURRENT_DATE, YEAR)` is a PUBLISHED example.
        *
        * The value is parsed into a temporal FIRST, with the shape decided at runtime because
        * Elasticsearch accepts both an ISO string and epoch millis into a `date` field. It is the
        * base rather than a method because a processor parameter drops `painlessMethods`.
        *
        * Only when a temporal is actually required: a `keyword` is a `String` in both contexts, so
        * `UPPER(a)` and `CAST(zip AS BIGINT)` stay byte-identical.
        */
      val processorBase: Option[String] =
        context.filter(_.isProcessor).flatMap { _ =>
          orderedFunctions.headOption
            .collect {
              // ... and only when the operand is CHAINED. A function that takes this identifier as
              // an ARGUMENT (`DATE_FORMAT`, `DATE_PARSE`) coerces it on the argument path instead,
              // and doing both emits a parse nobody reads.
              case f: TransformFunction[_, _]
                  if !f.args.exists(_ == this) &&
                    (f.in == SQLTypes.Temporal || f.in == SQLTypes.Date ||
                    f.in == SQLTypes.Time || f.in == SQLTypes.DateTime ||
                    f.in == SQLTypes.Timestamp) =>
                f
            }
            .flatMap(_ => SQLTypeUtils.processorTemporal(processParamName, declaredType))
        }
      val base =
        context match {
          case Some(ctx) =>
            processorBase
              // 🔴 Keyed on the CHAIN, like every other registration (issue #373). The parsed
              // processor base is the SAME literal for every chain over one column, so `YEAR(d)`
              // and `MONTH(d)` collapsed onto one parameter and both folds landed on it:
              // `….get(ChronoField.YEAR).get(ChronoField.MONTH_OF_YEAR)`, an `int.get(…)` that
              // throws -- and in an ingest pipeline `ignore_failure: true` swallows it and the
              // computed column is simply ABSENT.
              .flatMap(e => ctx.addParam(LiteralParam(e, None, Some(contextKeyOf(e)))))
              .orElse(ctx.addParam(this))
              .getOrElse("")
          case _ =>
            if (nullable)
              checkNotNull
            else
              paramName
        }
      var expr = base
      orderedFunctions.zipWithIndex.foreach { case (f, idx) =>
        f match {
          case f: TransformFunction[_, _] => expr = f.toPainless(expr, idx, context)
          case f: PainlessScript          => expr = s"$expr${f.painless(context)}"
          case f                          => expr = f.toSQL(expr) // fallback
        }
        currType = f.out
      }
      expr
    }

    override def param: String = paramName

    /** The functions that FOLD onto this identifier's raw parameter as methods, in application
      * order (issue #370).
      *
      * A function folds when its rendering is a `.method(...)` suffix -- `DATE_TRUNC`, the interval
      * family, the extractors -- and `TransformFunction.toPainless` then appends it to the
      * parameter OBJECT. A function that renders as an EXPRESSION around its operand does not: one
      * that takes the identifier as an ARGUMENT (`DATE_FORMAT`, `DATE_PARSE`, `WEEKDAY`,
      * `LAST_DAY`, the string functions) or a `CAST`, which binds a new parameter. And the run
      * STOPS at the first of those, because everything after it applies to that expression, not to
      * the raw parameter: in `YEAR(DATE_PARSE(name, …))` the `YEAR` is read off the parsed value,
      * so the raw `name` is shared with the argument `DATE_PARSE` holds -- keying it on `YEAR`
      * split them and declared the raw doc-value twice. The test is
      * `TransformFunction.foldsOntoOperand`, the same one `toPainless` folds on.
      */
    lazy val foldedFunctions: List[Function] =
      FunctionUtils.transformFunctions(this).reverse.takeWhile {
        case f: TransformFunction[_, _] => f.foldsOntoOperand
        case _                          => false
      }

    /** Issue #370: one parameter per (column, folded rendering), not per column. Two identifiers of
      * one column share a parameter iff they fold the SAME run onto it; `d` and `DATE_TRUNC(d,
      * MONTH)` no longer collapse onto one object, and neither do `YEAR(d)` and `MONTH(d)`, which
      * used to produce `….get(ChronoField.YEAR).get(ChronoField.MONTH_OF_YEAR)`.
      */
    override lazy val contextKey: String = contextKeyOf(paramName)

    /** The same identity rule over an arbitrary operand rendering, so a PROCESSOR (`ctx.<field>`)
      * or TRANSFORM (`doc['<alias>'].value`) registration keys on its chain too (issue #373).
      */
    def contextKeyOf(base: String): String =
      // The folded RENDERINGS, not the SQL spellings: `YEAR(d)` and `EXTRACT(YEAR FROM d)` fold
      // the same `.get(ChronoField.YEAR)` and must share one parameter (review, L1).
      foldedFunctions.foldLeft(base) { (key, f) =>
        key + (f match {
          case ps: PainlessScript => Try(ps.painless(None)).getOrElse(f.toSQL(""))
          case _                  => f.toSQL("")
        })
      }

    private[this] var _nullable =
      this.name.nonEmpty && (!isAggregation || functions.size > 1)

    protected def nullable_=(b: Boolean): Unit = {
      _nullable = b
    }

    override def nullable: Boolean = _nullable

    def withNullable(b: Boolean): Identifier = {
      this.nullable = b
      this
    }

    override lazy val value: String =
      script match {
        case Some(s) => s
        case _       => painless(None)
      }

    def withNested(nested: Boolean): Identifier = this match {
      case g: GenericIdentifier => g.copy(nested = nested)
      case _                    => this
    }

    lazy val windows: Option[WindowFunction] =
      functions.collectFirst { case th: WindowFunction => th }

    def hasWindow: Boolean = windows.nonEmpty

    def isWindowing: Boolean = windows.exists(_.partitionBy.nonEmpty)

    def painlessScriptRequired: Boolean = functions.nonEmpty && !hasAggregation && bucket.isEmpty

    def isObject: Boolean = {
      out match {
        case SQLTypes.Struct => true
        case _               => name.contains(".")
      }
    }
  }

  object Identifier {
    def apply(): Identifier = GenericIdentifier("")
    def apply(function: Function): Identifier = apply(List(function))
    def apply(functions: List[Function]): Identifier = apply().withFunctions(functions)
    def apply(name: String): Identifier = GenericIdentifier(name)
    def apply(name: String, function: Function): Identifier =
      apply(name).withFunctions(List(function))
  }

  case class GenericIdentifier(
    name: String,
    tableAlias: Option[String] = None,
    distinct: Boolean = false,
    nested: Boolean = false,
    limit: Option[Limit] = None,
    functions: List[Function] = List.empty,
    fieldAlias: Option[String] = None,
    bucket: Option[Bucket] = None,
    nestedElement: Option[NestedElement] = None,
    bucketPath: String = "",
    col: Option[Column] = None,
    table: Option[String] = None,
    // Appended LAST and defaulted so every positional construction in this repo and downstream
    // keeps compiling, and so all ~20 `this.copy(...)` sites in `update` carry it through for free.
    quoted: Boolean = false
  ) extends Identifier {

    def withFunctions(functions: List[Function]): Identifier = this.copy(functions = functions)

    override def withNullable(b: Boolean): Identifier = {
      val id = this.copy()
      id.nullable = b
      id
    }

    /** The RUNTIME type of this column -- what `doc['f'].value` yields -- not the DECLARED one. See
      * `SQLTypeUtils.runtimeType`. Every consumer of `baseType` is Painless emission or `coerce`;
      * anything wanting the declared type reads `Column.dataType` instead.
      */
    override def baseType: SQLType =
      col.map(c => SQLTypeUtils.runtimeType(c.dataType)).getOrElse(super.baseType)

    /** The DECLARED type, which an INGEST script needs and `baseType` cannot give it.
      *
      * `runtimeType` collapses every temporal declaration to `Timestamp`, because that is what a
      * QUERY gets from `doc['f'].value` whatever the column was declared as. An ingest script reads
      * `ctx.<field>` — the raw JSON — so the declaration is the only thing that says which temporal
      * the value should become: a DATE column carrying `"2025-01-10"` must parse as a `LocalDate`,
      * and `ZonedDateTime.parse` REFUSES a date without a time (measured on ES 8.18.3).
      */
    override def declaredType: SQLType = col.map(_.dataType).getOrElse(baseType)

    /** The LEAF of the reported-type derivation: the column's declaration, NOT
      * `runtimeType(declaration)`. A bare `DATE` column is reported as `DATE`, which is what the
      * user wrote and what DuckDB answers for the same column.
      */
    override def reportedLeafType: SQLType = col.map(_.dataType).getOrElse(super.reportedLeafType)

    def update(request: SingleSearch): Identifier = {
      val bucketPath: String =
        request.groupBy match {
          case Some(gb) =>
            BucketPath(
              gb.buckets.map(b => request.bucketNames.getOrElse(b.identifier.identifierName, b))
            ).path
          case None /*if this.bucketPath.isEmpty*/ =>
            aggregateFunction match {
              case Some(af) => af.bucketPath
              case _        => this.bucketPath
            }
          //case _ => this.bucketPath
        }
      val parts: Seq[String] = name.split("\\.").toSeq
      val tableAlias = parts.head
      val mainTable = request.from.mainTable.name
      // A qualifier needs something to qualify: `parts.head` is only a table alias when a column
      // name follows it. Without the arity check a column that happens to share its table's name
      // — `FROM status WHERE status = 'done'` — matched `tableAliases` and was rewritten to
      // `parts.tail.mkString(".")`, i.e. the empty string, silently querying a nameless field.
      //
      // 🔴 `aliasesToTable`, never a REVERSE lookup over `tableAliases` (story BIDC-8,
      // softclient4es-arrow#144): that map is keyed by TABLE and holds one alias per table, so on a
      // self-join (`FROM idx a JOIN idx b`) the reverse lookup found `b` and never `a` — `a.id`
      // stayed a literal dotted field name with no `table`, and the ON clause lost its join key.
      // The value is still a `tableAliases` KEY (same `aliasKey`), so `table` keeps its language.
      val table =
        if (parts.size > 1) request.aliasesToTable.get(tableAlias)
        else None

      /** The schema for THIS column's own table.
        *
        * 🔴 Keyed on `table`, never on the alias. `request.schemas` is keyed the way
        * `From.tableAliases` keys it -- the index name via `aliasKey` (story 21.2 AD-6) -- which is
        * also how softclient4es-extensions builds its map. `tableAlias` is the SQL alias (`c`), a
        * different language; looking the map up with it misses for every aliased JOIN.
        *
        * 🔴 `this.table` is the fallback because `update` is not the first pass. The parser has
        * already normalised `c.zip` to `name = "zip"`, `table = Some("customers")` -- MEASURED --
        * so `parts.size` is 1, the qualified branch below is not taken, and `table` (local) is
        * None. `this.table` is then the ONLY surviving evidence of which leg the column came from.
        *
        * A known-but-absent leg deliberately does NOT fall back to the main table: attributing a
        * joined column to the main table's schema resolves it against the wrong mapping, which is
        * worse than not resolving it at all.
        */
      val columnSchema: Option[Schema] =
        table.orElse(this.table) match {
          case Some(key) => request.schemas.get(key).orElse(request.schema)
          case None      => request.schemas.get(mainTable).orElse(request.schema)
        }
      if (table.nonEmpty) { // maybe from a JOIN or a subquery, not the main table
        request.unnestAliases.find(_._1 == tableAlias) match {
          case Some(tuple) if !nested =>
            val nestedElement =
              request.unnests.get(tableAlias) match {
                case Some(unnest) => Some(request.toNestedElement(unnest))
                case None         => None
              }
            val colName = parts.tail.mkString(".")
            this
              .copy(
                tableAlias = Some(tableAlias),
                name = s"${tuple._2._1}.$colName",
                nested = true,
                limit = tuple._2._2,
                fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias),
                bucket = request.bucketNames.get(identifierName).orElse(bucket),
                nestedElement = nestedElement,
                bucketPath = bucketPath,
                col = columnSchema.flatMap(schema => schema.find(colName)),
                table = table
              )
              .withFunctions(this.updateFunctions(request))
          case Some(tuple) if nested =>
            val colName = parts.tail.mkString(".")
            this
              .copy(
                tableAlias = Some(tableAlias),
                name = s"${tuple._2._1}.$colName",
                limit = tuple._2._2,
                fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias),
                bucket = request.bucketNames.get(identifierName).orElse(bucket),
                bucketPath = bucketPath,
                col = columnSchema.flatMap(schema => schema.find(colName)),
                table = table
              )
              .withFunctions(this.updateFunctions(request))
          case None if nested =>
            this
              .copy(
                tableAlias = Some(tableAlias),
                fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias),
                bucket = request.bucketNames.get(identifierName).orElse(bucket),
                bucketPath = bucketPath,
                col = columnSchema.flatMap(schema => schema.find(name)),
                table = table
              )
              .withFunctions(this.updateFunctions(request))
          case _ =>
            val colName = parts.tail.mkString(".")
            this.copy(
              tableAlias = Some(tableAlias),
              name = colName,
              fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias),
              bucket = request.bucketNames.get(identifierName).orElse(bucket),
              bucketPath = bucketPath,
              col = columnSchema.flatMap(schema => schema.find(colName)),
              table = table
            )
        }
      } else {
        // An UN-QUALIFIED name. It resolves against the MAIN table, whatever that is — and since
        // story 22.1 the main table may itself be a DERIVED table, in which case there is no
        // schema to attach (a subquery has no mapping) and the name is checked against the derived
        // table's PROJECTION instead, by `SingleSearch.derivedScopeCheck`. Nothing to do here: a
        // derived table's `name` IS its correlation name, so every alias map already agrees.
        this
          .copy(
            fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias),
            bucket = request.bucketNames.get(identifierName).orElse(bucket),
            bucketPath = bucketPath,
            col = columnSchema.flatMap(schema => schema.find(name))
          )
          .withFunctions(this.updateFunctions(request))
      }
    }
  }
}
