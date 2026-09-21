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

package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql._
import app.softnetwork.elastic.sql.function.time.DateTimeFunction
import app.softnetwork.elastic.sql.function._
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql.parser.`type`.TypeParser
import app.softnetwork.elastic.sql.parser.function.aggregate.AggregateParser
import app.softnetwork.elastic.sql.parser.function.cond.CondParser
import app.softnetwork.elastic.sql.parser.function.convert.ConvertParser
import app.softnetwork.elastic.sql.parser.function.geo.GeoParser
import app.softnetwork.elastic.sql.parser.function.math.MathParser
import app.softnetwork.elastic.sql.parser.function.string.StringParser
import app.softnetwork.elastic.sql.parser.function.time.TemporalParser
import app.softnetwork.elastic.sql.parser.operator.math.ArithmeticParser
import app.softnetwork.elastic.sql.policy.EnrichPolicyType
import app.softnetwork.elastic.sql.query._
import app.softnetwork.elastic.sql.schema.{
  Column,
  IngestPipelineType,
  IngestProcessor,
  IngestProcessorType,
  PartitionDate,
  SchemaCacheTtl,
  ScriptProcessor
}
import app.softnetwork.elastic.sql.time.TimeUnit
import app.softnetwork.elastic.sql.transform.{Delay, Frequency, TransformTimeUnit}
import app.softnetwork.elastic.sql.parser.http.HttpParser
import app.softnetwork.elastic.sql.watcher.{
  AlwaysWatcherCondition,
  ChainInput,
  CompareWatcherCondition,
  CronWatcherTrigger,
  EmptyWatcherInput,
  HttpInput,
  IntervalWatcherTrigger,
  LoggingAction,
  LoggingActionConfig,
  LoggingLevel,
  NeverWatcherCondition,
  ScriptWatcherCondition,
  SearchWatcherInput,
  SimpleWatcherInput,
  WatcherAction,
  WatcherCondition,
  WatcherInput,
  WatcherTrigger,
  WebhookAction
}

import scala.collection.immutable.ListMap
import scala.language.implicitConversions
import scala.language.existentials
import scala.util.control.NonFatal
import scala.util.matching.Regex
import scala.util.parsing.combinator.{PackratParsers, RegexParsers}
import scala.util.parsing.input.CharSequenceReader

/** Created by smanciot on 27/06/2018.
  *
  * SQL Parser for ElasticSearch
  */
object Parser
    extends Parser
    with SelectParser
    with FromParser
    with WhereParser
    with GroupByParser
    with HavingParser
    with OrderByParser
    with LimitParser {

  /** 🔴 `TOP n` and `LIMIT n` are two spellings of ONE row bound, so carrying both would need a
    * precedence rule nobody could guess from the SQL. Combining them is refused by name instead;
    * `t.orElse(l)` then never has to choose.
    */
  lazy val single: PackratParser[SingleSearch] = {
    select ~ from ~ where.? ~ groupBy.? ~ having.? ~ orderBy.? ~ limit.? ~ onConflict.? >> {
      case (s, t) ~ f ~ w ~ g ~ h ~ o ~ l ~ oc =>
        if (t.isDefined && l.isDefined)
          err(
            "TOP and LIMIT both bound the number of rows -- use one of them, not both. TOP " +
            "carries no OFFSET, so paging needs the LIMIT n OFFSET m spelling"
          )
        else success(SingleSearch(s, f, w, g, h, o, t.orElse(l), onConflict = oc).update())
    }
  }

  /** The set operators, story 22.6.
    *
    * 🔴 Ordered: every multi-word spelling BEFORE its one-word prefix. `|` commits to the first
    * SUCCEEDING alternative and `UNION.regex` (`UNION\s+ALL`) FAILS — it does not partially succeed
    * — on `UNION SELECT`, which is what lets `UNION_DISTINCT` take it. The REVERSE order would
    * commit `UNION_DISTINCT` on the prefix of `UNION ALL` and leave `ALL` as trailing input; that
    * is the one ordering that must never ship. `UNION_DISTINCT`'s own regex is
    * `(UNION\s+DISTINCT|UNION)\b`, longest first for the same reason.
    */
  lazy val setOperator: PackratParser[SetOperator] =
    UNION.regex ^^ (_ => UNION) |
    UNION_DISTINCT.regex ^^ (_ => UNION_DISTINCT) |
    INTERSECT_ALL.regex ^^ (_ => INTERSECT_ALL) |
    INTERSECT.regex ^^ (_ => INTERSECT) |
    EXCEPT_ALL.regex ^^ (_ => EXCEPT_ALL) |
    EXCEPT.regex ^^ (_ => EXCEPT)

  /** One branch of a set operation: `(SELECT ...)` or `SELECT ...`. The `Boolean` records whether
    * it was PARENTHESISED — consumed only by the trailing-clause rule in [[searchStatement]], never
    * stored on the AST (parentheses are not recorded; the precedence tree is derived).
    *
    * The `err` inside the parenthesised alternative fires when `(` was consumed HERE and a set
    * operator follows the inner SELECT — i.e. a set operation is itself wrapped in parentheses.
    * That covers two shapes and the message names both: a parenthesised GROUP used as a branch (`(a
    * UNION b) INTERSECT c`) and a whole set operation wrapped in parentheses (`(SELECT ... UNION
    * SELECT ...)` as the statement, or as a derived-table body). It can never fire for a derived
    * table or a WHERE subquery: there the `(` belongs to `derivedTableBody` / the `IN`/`EXISTS`
    * production, and this alternative's own `start` fails at the inner `SELECT` without consuming
    * anything. Safe to raise this early (story 21.4's rule — an `err` is discarded only by a
    * sibling `Failure` that got FURTHER): the bare `single` alternative fails at the very same `(`.
    *
    * Side effect, deliberate and release-noted: a lone parenthesised SELECT — `(SELECT a FROM t)` —
    * now PARSES as that `SingleSearch` and renders bare. It was a parse error before.
    */
  lazy val setOperand: PackratParser[(SingleSearch, Boolean)] =
    (start ~> single <~ (end | (setOperator ~> err(
      // Both halves have to survive `GatewayApi.excerpt`, which caps a rejection at 200
      // characters and elides the MIDDLE (head 120 + tail 77): what was refused is in the head,
      // the remedy is in the tail.
      "A set operation wrapped in parentheses is not supported, neither as a branch nor as a " +
      "whole parenthesised statement or body. Remove the outer parentheses; to group " +
      "differently use a derived table: SELECT * FROM (a UNION b) AS g INTERSECT c"
    )))) ^^ (s => (s, true)) |
    single ^^ (s => (s, false))

  /** `SELECT ... [<set operator> SELECT ...]*`. One branch yields the `SingleSearch` exactly as
    * before; two or more yield a `MultiSearch` carrying the flat operator list.
    *
    * 🔴 Trailing-clause rule. `single` consumes `ORDER BY` / `LIMIT`, so after the LAST branch they
    * belong to THAT branch — today's documented behaviour, kept byte-for-byte for `UNION ALL`. For
    * any other operator a set result has no branch order to preserve, so a trailing `ORDER BY` the
    * analyst almost certainly meant for the WHOLE result would silently order nothing (HTTP 200,
    * rows in an order nobody asked for). It is therefore an `err` — unless the branch is
    * PARENTHESISED, which is the unambiguous spelling for "this branch only". Only the LAST branch
    * is ambiguous: a first or middle branch carrying an unparenthesised `ORDER BY`/`LIMIT` can only
    * mean that branch, and is accepted.
    *
    * 🔴 Canonical form: a `UNION ALL`-only list is stored as `Nil`, so the legacy construction
    * `MultiSearch(branches)` and this parse are `==` and the render fixed point holds for the one
    * shape that pre-dates story 22.6.
    */
  lazy val searchStatement: PackratParser[SearchStatement] =
    setOperand ~ rep(setOperator ~ setOperand) >> {
      case (first, _) ~ Nil => success(first)
      case (first, _) ~ rest =>
        val operands = first +: rest.map { case _ ~ operand => operand._1 }
        val ops = rest.map { case op ~ _ => op }
        val unionAllOnly = ops.forall(_ == UNION)
        val (last, lastParenthesised) = rest.last match { case _ ~ operand => operand }
        if (!unionAllOnly && !lastParenthesised && (last.orderBy.isDefined || last.limit.isDefined))
          err(
            // Head 120 / tail 77 again: the rule in the head, the remedy in the tail.
            "ORDER BY / LIMIT after the last branch of a UNION, INTERSECT or EXCEPT applies to " +
            "that branch only. Parenthesise the branch to keep it there, or wrap the whole set " +
            "operation in a derived table to order or limit the whole result."
          )
        else success(MultiSearch(operands, operators = if (unionAllOnly) Nil else ops))
    }

  /** FROM-less SELECT (issue #251): the same select-list grammar, no FROM, optional LIMIT. `SELECT
    * 1 LIMIT 100` must parse — it is Superset's engine probe AND what the Flight sidecar's own
    * schemaProbeSql rewrites `SELECT 1` into.
    */
  lazy val fromlessSelect: PackratParser[FromlessSelect] =
    select ~ limit.? >> { case (s, t) ~ l =>
      if (t.isDefined && l.isDefined)
        err(
          "TOP and LIMIT both bound the number of rows -- use one of them, not both. TOP " +
          "carries no OFFSET, so paging needs the LIMIT n OFFSET m spelling"
        )
      else success(FromlessSelect(s, t.orElse(l)))
    }

  lazy val row: PackratParser[List[Value[_]]] =
    lparen ~> repsep(array_of_struct | struct | value, comma) <~ rparen

  lazy val rows: PackratParser[List[List[Value[_]]]] =
    repsep(row, comma)

  /** DELIBERATELY still `ident` (story 21.7 Task 1.1). This selects a processor TYPE, not a name:
    * it never reaches Elasticsearch as an identifier, and `IngestProcessor.sql` renders
    * `processorType.name.toUpperCase`, so a quoted spelling could not round-trip even if it parsed.
    * The one `ident` position that is not a name is not converted.
    */
  lazy val processorType: PackratParser[IngestProcessorType] =
    ident ^^ { name =>
      name.toLowerCase match {
        case "set"             => IngestProcessorType.Set
        case "script"          => IngestProcessorType.Script
        case "rename"          => IngestProcessorType.Rename
        case "remove"          => IngestProcessorType.Remove
        case "date_index_name" => IngestProcessorType.DateIndexName
        case "enrich"          => IngestProcessorType.Enrich
        case other             => IngestProcessorType(other)
      }
    }

  lazy val processor: PackratParser[IngestProcessor] =
    processorType ~ objectValue ^^ { case pt ~ opts =>
      IngestProcessor(pt, opts)
    }

  lazy val createOrReplacePipeline: PackratParser[CreatePipeline] =
    (keyword("CREATE") ~ keyword("OR") ~ keyword("REPLACE") ~ keyword(
      "PIPELINE"
    )) ~ identRef ~ (keyword("WITH") ~ keyword("PROCESSORS")) ~ start ~ repsep(
      processor,
      separator
    ) ~ end ^^ { case _ ~ name ~ _ ~ _ ~ proc ~ _ =>
      CreatePipeline(
        name._1,
        IngestPipelineType.Custom,
        orReplace = true,
        processors = proc,
        parts = name._2
      )
    }

  lazy val createPipeline: PackratParser[CreatePipeline] =
    (keyword("CREATE") ~ keyword("PIPELINE")) ~ ifNotExists ~ identRef ~ (keyword(
      "WITH"
    ) ~ keyword(
      "PROCESSORS"
    ) ~ start) ~ repsep(
      processor,
      separator
    ) <~ end ^^ { case _ ~ ine ~ name ~ _ ~ proc =>
      CreatePipeline(
        name._1,
        IngestPipelineType.Custom,
        ifNotExists = ine,
        processors = proc,
        parts = name._2
      )
    }

  lazy val dropPipeline: PackratParser[DropPipeline] =
    (keyword("DROP") ~ keyword("PIPELINE")) ~ ifExists ~ identRef ^^ { case _ ~ ie ~ name =>
      DropPipeline(name._1, ifExists = ie, parts = name._2)
    }

  lazy val showPipeline: PackratParser[ShowPipeline] =
    (keyword("SHOW") ~ keyword("PIPELINE")) ~ identRef ^^ { case _ ~ pipeline =>
      ShowPipeline(pipeline._1, parts = pipeline._2)
    }

  lazy val showPipelines: PackratParser[ShowPipelines.type] =
    (keyword("SHOW") ~ keyword("PIPELINES")) ^^ { _ =>
      ShowPipelines
    }

  lazy val showCreatePipeline: PackratParser[ShowCreatePipeline] =
    (keyword("SHOW") ~ keyword("CREATE") ~ keyword("PIPELINE")) ~ identRef ^^ {
      case _ ~ _ ~ _ ~ pipeline =>
        ShowCreatePipeline(pipeline._1, parts = pipeline._2)
    }

  lazy val describePipeline: PackratParser[DescribePipeline] =
    ((keyword("DESCRIBE") | keyword("DESC")) ~ keyword("PIPELINE")) ~ identRef ^^ {
      case _ ~ pipeline =>
        DescribePipeline(pipeline._1, parts = pipeline._2)
    }

  lazy val addProcessor: PackratParser[AddPipelineProcessor] =
    (keyword("ADD") ~ keyword("PROCESSOR")) ~ processor ^^ { case _ ~ proc =>
      AddPipelineProcessor(proc)
    }

  lazy val dropProcessor: PackratParser[DropPipelineProcessor] =
    (keyword("DROP") ~ keyword("PROCESSOR")) ~ processorType ~ start ~ identName ~ end ^^ {
      case _ ~ pt ~ _ ~ name ~ _ =>
        DropPipelineProcessor(pt, name)
    }

  lazy val alterPipelineStatement: PackratParser[AlterPipelineStatement] =
    addProcessor | dropProcessor

  lazy val alterPipeline: PackratParser[AlterPipeline] =
    (keyword("ALTER") ~ keyword("PIPELINE")) ~ ifExists ~ identRef ~ start.? ~ repsep(
      alterPipelineStatement,
      separator
    ) ~ end.? >> { case _ ~ ie ~ pipeline ~ s ~ stmts ~ e =>
      if (s.isDefined && e.isEmpty) {
        err("Mismatched closing parentheses in ALTER PIPELINE statement")
      } else if (s.isEmpty && e.isDefined) {
        err("Mismatched opening parentheses in ALTER PIPELINE statement")
      } else if (s.isEmpty && e.isEmpty && stmts.size > 1) {
        err("Multiple ALTER PIPELINE statements require parentheses")
      } else
        success(AlterPipeline(pipeline._1, ie, stmts, parts = pipeline._2))
    }

  /** `FIELDS (…)` — required. The empty fallback belongs to `optionalMultiFields`, whose only
    * caller is a column definition (a column need not declare sub-fields). Folding it in here made
    * every consumer optional, and `alterColumnFields` (`ALTER COLUMN c SET <multiFields>`) then
    * matched a bare `SET` with an empty field list: `ALTER COLUMN c SET` parsed as a no-op, and
    * `ALTER COLUMN c SET FIELD raw KEYWORD` matched that same alternative and left `FIELD raw
    * KEYWORD` unconsumed — so `SET FIELD` silently did nothing before #213 made trailing input an
    * error, and could not parse at all afterwards.
    */
  lazy val multiFields: PackratParser[List[Column]] =
    keyword("FIELDS") ~ start ~> repsep(column, separator) <~ end ^^ (cols => cols)

  lazy val optionalMultiFields: PackratParser[List[Column]] = multiFields | success(Nil)

  lazy val ifExists: PackratParser[Boolean] =
    opt(keyword("IF") ~ keyword("EXISTS")) ^^ {
      case Some(_) => true
      case None    => false
    }

  lazy val ifNotExists: PackratParser[Boolean] =
    opt(keyword("IF") ~ keyword("NOT") ~ keyword("EXISTS")) ^^ {
      case Some(_) => true
      case None    => false
    }

  lazy val notNull: PackratParser[Boolean] =
    opt(keyword("NOT") ~ keyword("NULL")) ^^ {
      case Some(_) => true
      case None    => false
    }

  lazy val defaultVal: PackratParser[Option[Value[_]]] =
    opt(keyword("DEFAULT") ~ (value | ingest_id | ingest_timestamp)) ^^ {
      case Some(_ ~ v) => Some(v)
      case None        => None
    }

  lazy val comment: PackratParser[Option[String]] =
    opt(keyword("COMMENT") ~ literal) ^^ {
      case Some(_ ~ v) => Some(v.value)
      case None        => None
    }

  lazy val scriptValue: PackratParser[PainlessScript] = identifierWithArithmeticExpression |
    identifierWithTransformation |
    identifierWithIntervalFunction |
    identifierWithFunction

  /** The `SCRIPT AS ( … )` body, delimited by scanning its own balanced parentheses.
    *
    * Historically `scriptValue` could not be relied on to stop at the wrapper's closing paren: the
    * generic `identifierWithFunction` closed with `rep1(end)` — one or MORE — so a call to a
    * name-only extractor (`YEAR(x)`, `MONTH(x)`, `QUARTER(x)`, …) consumed the `)` belonging to the
    * wrapper. `script` then failed, and a column definition fell through to `optionalMultiFields`,
    * which is why `age INT SCRIPT AS (YEAR(CURRENT_DATE) - YEAR(birthdate))` — the documented
    * example — reported `')' expected but 'S' found`.
    *
    * `identifierWithFunction` has since been made balanced (issue #220), so it no longer overruns.
    * This scanner is kept because it is the stronger guarantee: the body's extent is decided here
    * rather than inferred from whichever alternative happens to win inside it, which is what makes
    * the diagnostics below — unbalanced parentheses, a missing `(`, an unparseable body — possible
    * at all. Deleting it would put the wrapper's boundary back at the mercy of the expression
    * grammar.
    */
  private def scriptBody: Parser[String] = new Parser[String] {
    def apply(in: Input): ParseResult[String] = {
      val source = in.source
      var i = in.offset
      while (i < source.length && source.charAt(i).isWhitespace) i += 1
      // `Error`, not `Failure`: `SCRIPT AS` has already been consumed, so no alternative is
      // legitimate from here. A `Failure` is discarded by both call sites for structural reasons —
      // `column`'s `script | optionalMultiFields` keeps the alternative's Success and drops the
      // deeper failure, and `alterTable`'s `repsep` accepts zero statements so `phrase` overwrites
      // the message — which left the diagnostic below unreachable and reported the very message
      // this production exists to eliminate.
      if (i >= source.length || source.charAt(i) != '(')
        Error("'(' expected after SCRIPT AS", in)
      else {
        val bodyStart = i + 1
        var depth = 0
        var quote: Char = 0
        var closing = -1
        while (i < source.length && closing < 0) {
          val c = source.charAt(i)
          if (quote != 0) {
            // As in `normalize`: a backslash is an escape inside `'...'` and `"..."` only. Inside
            // backticks it is data.
            if (c == '\\' && quote != '`') i += 1
            else if (c == quote) quote = 0
          } else
            c match {
              // Without the backtick a `)` inside a backticked identifier closes the SCRIPT AS body
              // early -- the #219/#220 paren-greed failure mode, in a new disguise.
              case '\'' | '"' | '`' => quote = c
              case '('              => depth += 1
              case ')'              => depth -= 1; if (depth == 0) closing = i
              case _                => ()
            }
          i += 1
        }
        if (closing < 0) Error("unbalanced parentheses in SCRIPT AS", in)
        else
          Success(
            source.subSequence(bodyStart, closing).toString,
            in.drop(closing + 1 - in.offset)
          )
      }
    }
  }

  lazy val script: PackratParser[PainlessScript] =
    (keyword("SCRIPT") ~ keyword("AS")) ~> scriptBody >> { body =>
      parseAll(scriptValue, body) match {
        case Success(s, _)     => success(s)
        case NoSuccess(msg, _) => err(s"Invalid SCRIPT AS expression ($body): $msg")
      }
    }

  /** Parse a bare `SCRIPT AS (…)` BODY — the expression alone, with no wrapper and no statement
    * around it. The entry point exists so a caller that already holds the stored SQL text of a
    * computed column can recover its AST without re-implementing the grammar: `_meta.script.sql`
    * survives the Elasticsearch round-trip while `ScriptProcessor.expr` does not (issue #385).
    *
    * 🔴 It runs [[scriptValue]] — the SAME production [[script]] runs — and NOT `Parser.apply`,
    * which normalises a whole statement and would reject an expression outright. One production, so
    * a body the DDL accepts is a body this accepts, by construction rather than by review.
    *
    * `None` for anything this grammar does not accept: the callers are VALIDATION-only and treat an
    * unrecognised body as "nothing to check" rather than as a rejection (see
    * `ScriptProcessor.validationExpr`, which states why that has to be a fail-open).
    */
  def parseScriptExpression(text: String): Option[PainlessScript] =
    try {
      parseAll(scriptValue, text) match {
        case Success(s, _) => Some(s)
        case _             => None
      }
    } catch {
      // Same boundary rationale as `grammar`'s (#250): every alternative of `scriptValue` ends in
      // `Identifier.update`-adjacent AST construction, and a throw from there must not turn a
      // best-effort validation walk into a crash.
      case NonFatal(_) => None
    }

  /** `SCRIPT AS (<expr>) STORED` — the column's value is already materialized in this index, so the
    * script records how it was DERIVED and is NOT executed here.
    *
    * 🔴 The marker has to live in the DDL TEXT. `_meta` cannot carry it: `Table.update()` REBUILDS
    * `_meta.columns` from the parsed columns (it drops the incoming `columns` key), so anything not
    * recoverable from the column list is erased on the first round-trip. Measured, not assumed.
    */
  lazy val storedScript: PackratParser[(PainlessScript, Boolean)] =
    script ~ opt(keyword("STORED")) ^^ { case s ~ stored => (s, stored.isDefined) }

  lazy val column: PackratParser[Column] =
    identName ~ extension_type ~ (storedScript | optionalMultiFields) ~ defaultVal ~ notNull ~ comment ~ (options | success(
      ListMap.empty[String, Value[_]]
    )) ^^ { case name ~ dt ~ mfs ~ dv ~ nn ~ ct ~ opts =>
      mfs match {
        case (script: PainlessScript, stored: Boolean) =>
          Column(
            name,
            dt,
            Some(ScriptProcessor.fromScript(name, script, Some(dt), materialized = stored)),
            Nil,
            dv,
            nn,
            ct,
            opts
          )
        case cols: List[_] =>
          Column(name, dt, None, cols.asInstanceOf[List[Column]], dv, nn, ct, opts)
      }
    }

  lazy val columns: PackratParser[List[Column]] =
    start ~ repsep(column, separator) ~ end ^^ { case _ ~ cols ~ _ => cols }

  lazy val primaryKey: PackratParser[List[String]] =
    separator ~ keyword("PRIMARY") ~ keyword("KEY") ~ start ~ repsep(
      identName,
      separator
    ) ~ end ^^ { case _ ~ _ ~ _ ~ _ ~ keys ~ _ =>
      keys
    } | success(Nil)

  lazy val granularity: PackratParser[TimeUnit] = start ~
    ((keyword("YEAR") ^^^ TimeUnit.YEARS) |
    (keyword("MONTH") ^^^ TimeUnit.MONTHS) |
    (keyword("DAY") ^^^ TimeUnit.DAYS) |
    (keyword("HOUR") ^^^ TimeUnit.HOURS) |
    (keyword("MINUTE") ^^^ TimeUnit.MINUTES) |
    (keyword("SECOND") ^^^ TimeUnit.SECONDS)) ~ end ^^ { case _ ~ gf ~ _ => gf }

  lazy val partitionBy: PackratParser[Option[PartitionDate]] =
    opt(keyword("PARTITION") ~ keyword("BY") ~ identName ~ opt(granularity)) ^^ {
      case Some(_ ~ _ ~ pb ~ gf) => Some(PartitionDate(pb, gf.getOrElse(TimeUnit.DAYS)))
      case None                  => None
    }

  lazy val columnsWithPartitionBy
    : PackratParser[(List[Column], List[String], Option[PartitionDate], ListMap[String, Any])] =
    start ~ repsep(
      column,
      separator
    ) ~ primaryKey ~ end ~ partitionBy ~ ((separator.? ~> options) | success(
      ListMap.empty[String, Value[_]]
    )) ^^ { case _ ~ cols ~ pk ~ _ ~ pb ~ opts =>
      (cols, pk, pb, opts)
    }

  lazy val createOrReplaceTable: PackratParser[CreateTable] =
    (keyword("CREATE") ~ keyword("OR") ~ keyword("REPLACE") ~ keyword(
      "TABLE"
    )) ~ identRef ~ (columnsWithPartitionBy | (keyword("AS") ~> searchStatement)) ^^ {
      case _ ~ name ~ lr =>
        lr match {
          case (
                cols: List[Column],
                pk: List[String],
                p: Option[PartitionDate],
                opts: ListMap[String, Value[_]]
              ) =>
            CreateTable(
              name._1,
              Right(cols),
              ifNotExists = false,
              orReplace = true,
              primaryKey = pk,
              partitionBy = p,
              options = opts,
              parts = name._2
            )
          case sel: SearchStatement =>
            CreateTable(
              name._1,
              Left(sel),
              ifNotExists = false,
              orReplace = true,
              parts = name._2
            )
        }
    }

  lazy val createTable: PackratParser[CreateTable] =
    (keyword("CREATE") ~ keyword(
      "TABLE"
    )) ~ ifNotExists ~ identRef ~ (columnsWithPartitionBy | (keyword(
      "AS"
    ) ~> searchStatement)) ^^ { case _ ~ ine ~ name ~ lr =>
      lr match {
        case (
              cols: List[Column],
              pk: List[String],
              p: Option[PartitionDate],
              opts: ListMap[String, Value[_]]
            ) =>
          CreateTable(
            name._1,
            Right(cols),
            ine,
            primaryKey = pk,
            partitionBy = p,
            options = opts,
            parts = name._2
          )
        case sel: SearchStatement => CreateTable(name._1, Left(sel), ine, parts = name._2)
      }
    }

  /** SQL-92 `CREATE [LOCAL | GLOBAL] TEMPORARY TABLE`, with or without its `ON COMMIT { PRESERVE |
    * DELETE } ROWS` clause, and MySQL's `CREATE TEMPORARY TABLE`.
    *
    * RECOGNISE-TO-REJECT. The shape is matched only so the refusal can NAME the construct; the
    * statement is then refused with [[temporaryTableRefusal]]. Before this production the SQL-92
    * spelling surfaced `string matching regex '(?i)OR\b' expected but 'L' found` — the failure of
    * the sibling `createOrReplaceTable` alternative, which names neither temporary tables nor
    * anything a user wrote.
    *
    * 🔴 Three properties of this design are load-bearing; do not "simplify" any of them.
    *
    * ONE — **`err`, never `failure`, and never accept-and-ignore.** A `failure` would fall through
    * an enclosing alternative and let some other production answer; accepting the statement and
    * refusing it downstream would make `Parser.apply` return a `Right`, i.e. report the capability
    * as PRESENT to every consumer that only looks at the parse — including the Epic 21 corpus
    * replay, whose three `rejected_pending_policy` rows must stay rejected. `DropTable.cascade` is
    * the standing proof that a parsed-and-ignored clause survives for years; for a clause that
    * governs data LIFETIME that would be a silent wrong answer.
    *
    * TWO — **it refuses at the HEADER, before the column list or the `AS <select>` body.** The
    * refusal is then independent of whether the body parses, so the message is deterministic: an
    * unparseable body (or an `err` raised inside an embedded SELECT) can never substitute its own
    * reason for this one. `ON COMMIT ... ROWS` is consequently refused as part of the statement it
    * can only legally appear on, rather than accepted and dropped.
    *
    * THREE — **it is the FIRST alternative of `ddlStatement`, and `private`.** First, for the
    * reason spelled out at that call site: it cannot commit to a prefix (`TEMPORARY` is mandatory
    * and no sibling accepts it there), and from the front its `Error` short-circuits the rest
    * unconditionally while its `Failure` yields every tie, so a plain `CREATE TABEL` typo is never
    * told about temporary tables. `private`, because this is a refusal and not a statement
    * production: it yields `Nothing`, has no AST node and no help document, and `HelpCorpusSpec`
    * enumerates the PUBLIC productions by result type to prove every statement a user CAN type is
    * documented.
    *
    * `TEMPORARY` / `LOCAL` / `GLOBAL` are deliberately NOT added to `reservedKeywords`: a column or
    * table called `temporary` keeps working, and `CREATE TABLE temporary (...)` still parses (the
    * discriminating token is consumed BEFORE the name).
    */
  private def temporaryTable: Parser[Nothing] =
    keyword("CREATE") ~> opt(keyword("LOCAL") | keyword("GLOBAL")) <~ keyword(
      "TEMPORARY"
    ) <~ keyword("TABLE") >> { scope => err(temporaryTableRefusal(scope)) }

  /** The refusal [[temporaryTable]] raises. It names the construct as the caller spelled it, says
    * why Elasticsearch cannot honour it, and gives the remedy — the three things the previous
    * combinator message gave none of.
    */
  private def temporaryTableRefusal(scope: Option[String]): String =
    s"CREATE ${scope.map(_ + " ").getOrElse("")}TEMPORARY TABLE is not supported: an " +
    "Elasticsearch index is cluster-global, is visible to every client that can read the cluster " +
    "and has no session scope, and ON COMMIT { PRESERVE | DELETE } ROWS is transaction semantics " +
    "that Elasticsearch does not have. Use CREATE TABLE for a regular index and DROP TABLE it " +
    "when you are done."

  lazy val patterns: PackratParser[List[String]] = keyword("LIKE") ~> repsep(literal, comma) ^^ {
    patterns =>
      patterns.map(_.value)
  }

  lazy val showTables: PackratParser[ShowTables] =
    (keyword("SHOW") ~ keyword("TABLES")) ~> opt(patterns) ^^ { indices =>
      ShowTables(indices.getOrElse(Seq.empty))
    }

  lazy val showTable: PackratParser[ShowTable] =
    (keyword("SHOW") ~ keyword("TABLE")) ~ identRef ^^ { case _ ~ table =>
      ShowTable(table._1, parts = table._2)
    }

  lazy val showCreateTable: PackratParser[ShowCreateTable] =
    (keyword("SHOW") ~ keyword("CREATE") ~ keyword("TABLE")) ~ identRef ^^ {
      case _ ~ _ ~ _ ~ table =>
        ShowCreateTable(table._1, parts = table._2)
    }

  lazy val describeTable: PackratParser[DescribeTable] =
    ((keyword("DESCRIBE") | keyword("DESC")) ~ opt(keyword("TABLE"))) ~ identRef ^^ {
      case _ ~ table =>
        DescribeTable(table._1, parts = table._2)
    }

  lazy val dropTable: PackratParser[DropTable] =
    (keyword("DROP") ~ (keyword("TABLE") | keyword("INDEX"))) ~ ifExists ~ identRef ^^ {
      case _ ~ ie ~ name =>
        DropTable(name._1, ifExists = ie, parts = name._2)
    }

  lazy val truncateTable: PackratParser[TruncateTable] =
    (keyword("TRUNCATE") ~ keyword("TABLE")) ~ identRef ^^ { case _ ~ name =>
      TruncateTable(name._1, parts = name._2)
    }

  lazy val frequency: PackratParser[Frequency] =
    (keyword("REFRESH") ~ keyword(
      "EVERY"
    )) ~> """\d+\s+(MILLISECOND|SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)S?""".r ^^ { str =>
      val parts = str.trim.split("\\s+")
      Frequency(TransformTimeUnit(parts(1)), parts(0).toLong)
    }

  lazy val withOptions: PackratParser[ListMap[String, Value[_]]] =
    (keyword("WITH") ~ lparen) ~> repsep(option, separator) <~ rparen ^^ { opts =>
      ListMap(opts: _*)
    }

  lazy val createOrReplaceMaterializedView: PackratParser[CreateMaterializedView] =
    (keyword("CREATE") ~ keyword("OR") ~ keyword("REPLACE") ~ keyword("MATERIALIZED") ~ keyword(
      "VIEW"
    )) ~ identRef ~ opt(frequency) ~ opt(
      withOptions
    ) ~ (keyword("AS") ~> searchStatement) ^^ { case _ ~ view ~ freq ~ opts ~ dql =>
      CreateMaterializedView(
        view._1,
        dql,
        ifNotExists = false,
        orReplace = true,
        frequency = freq,
        options = opts.getOrElse(ListMap.empty),
        parts = view._2
      )
    }

  lazy val createMaterializedView: PackratParser[CreateMaterializedView] =
    (keyword("CREATE") ~ keyword("MATERIALIZED") ~ keyword("VIEW")) ~ ifNotExists ~ identRef ~ opt(
      frequency
    ) ~ opt(
      withOptions
    ) ~ (keyword("AS") ~> searchStatement) ^^ { case _ ~ ine ~ view ~ freq ~ opts ~ dql =>
      CreateMaterializedView(
        view._1,
        dql,
        ifNotExists = ine,
        orReplace = false,
        frequency = freq,
        options = opts.getOrElse(ListMap.empty),
        parts = view._2
      )
    }

  lazy val dropMaterializedView: PackratParser[DropMaterializedView] =
    (keyword("DROP") ~ keyword("MATERIALIZED") ~ keyword("VIEW")) ~ ifExists ~ identRef ^^ {
      case _ ~ ie ~ name =>
        DropMaterializedView(name._1, ifExists = ie, parts = name._2)
    }

  lazy val refreshMaterializedView: PackratParser[RefreshMaterializedView] =
    (keyword("REFRESH") ~ keyword("MATERIALIZED") ~ keyword("VIEW")) ~ ifExists ~ identRef ~ opt(
      keyword("WITH") ~ keyword("SCHEDULE") ~ keyword("NOW")
    ) ^^ { case _ ~ ie ~ view ~ wn =>
      RefreshMaterializedView(view._1, ifExists = ie, scheduleNow = wn.isDefined, parts = view._2)
    }

  lazy val showMaterializedViewStatus: PackratParser[ShowMaterializedViewStatus] =
    (keyword("SHOW") ~ keyword("MATERIALIZED") ~ keyword("VIEW") ~ keyword(
      "STATUS"
    )) ~ identRef ^^ { case _ ~ _ ~ _ ~ _ ~ view =>
      ShowMaterializedViewStatus(view._1, parts = view._2)
    }

  lazy val showCreateMaterializedView: PackratParser[ShowCreateMaterializedView] =
    (keyword("SHOW") ~ keyword("CREATE") ~ keyword("MATERIALIZED") ~ keyword(
      "VIEW"
    )) ~ identRef ^^ { case _ ~ _ ~ _ ~ _ ~ view =>
      ShowCreateMaterializedView(view._1, parts = view._2)
    }

  lazy val showMaterializedView: PackratParser[ShowMaterializedView] =
    (keyword("SHOW") ~ keyword("MATERIALIZED") ~ keyword("VIEW")) ~ identRef ^^ {
      case _ ~ _ ~ view =>
        ShowMaterializedView(view._1, parts = view._2)
    }

  lazy val showMaterializedViews: PackratParser[ShowMaterializedViews.type] =
    (keyword("SHOW") ~ keyword("MATERIALIZED") ~ keyword("VIEWS")) ^^ { _ =>
      ShowMaterializedViews
    }

  lazy val describeMaterializedView: PackratParser[DescribeMaterializedView] =
    ((keyword("DESCRIBE") | keyword("DESC")) ~ keyword("MATERIALIZED") ~ keyword(
      "VIEW"
    )) ~ identRef ^^ { case _ ~ _ ~ _ ~ view =>
      DescribeMaterializedView(view._1, parts = view._2)
    }

  lazy val addColumn: PackratParser[AddColumn] =
    (keyword("ADD") ~ keyword("COLUMN")) ~ ifNotExists ~ column ^^ { case _ ~ ine ~ col =>
      AddColumn(col, ifNotExists = ine)
    }

  lazy val dropColumn: PackratParser[DropColumn] =
    (keyword("DROP") ~ keyword("COLUMN")) ~ ifExists ~ identName ^^ { case _ ~ ie ~ name =>
      DropColumn(name, ifExists = ie)
    }

  lazy val renameColumn: PackratParser[RenameColumn] =
    (keyword("RENAME") ~ keyword("COLUMN")) ~ identName ~ (keyword("TO") ~> identName) ^^ {
      case _ ~ oldName ~ newName =>
        RenameColumn(oldName, newName)
    }

  lazy val alterColumnIfExists: PackratParser[Boolean] =
    (keyword("ALTER") ~ keyword("COLUMN")) ~ ifExists ^^ { case _ ~ ie =>
      ie
    }

  lazy val alterColumnOptions: PackratParser[AlterColumnOptions] =
    alterColumnIfExists ~ identName ~ keyword("SET") ~ options ^^ { case ie ~ col ~ _ ~ opts =>
      AlterColumnOptions(col, opts, ifExists = ie)
    }

  lazy val alterColumnOption: PackratParser[AlterColumnOption] =
    alterColumnIfExists ~ identName ~ ((keyword("SET") | keyword("ADD")) ~ keyword(
      "OPTION"
    )) ~ start ~ option ~ end ^^ { case ie ~ col ~ _ ~ _ ~ opt ~ _ =>
      AlterColumnOption(col, opt._1, opt._2, ifExists = ie)
    }

  lazy val dropColumnOption: PackratParser[DropColumnOption] =
    alterColumnIfExists ~ identName ~ (keyword("DROP") ~ keyword("OPTION")) ~ identName ^^ {
      case ie ~ col ~ _ ~ optionName =>
        DropColumnOption(col, optionName, ifExists = ie)
    }

  lazy val alterColumnFields: PackratParser[AlterColumnFields] =
    alterColumnIfExists ~ identName ~ keyword("SET") ~ multiFields ^^ {
      case ie ~ col ~ _ ~ fields =>
        AlterColumnFields(col, fields, ifExists = ie)
    }

  lazy val alterColumnField: PackratParser[AlterColumnField] =
    alterColumnIfExists ~ identName ~ ((keyword("SET") | keyword("ADD")) ~ keyword(
      "FIELD"
    )) ~ column ^^ { case ie ~ col ~ _ ~ field =>
      AlterColumnField(col, field, ifExists = ie)
    }

  lazy val dropColumnField: PackratParser[DropColumnField] =
    alterColumnIfExists ~ identName ~ (keyword("DROP") ~ keyword("FIELD")) ~ identName ^^ {
      case ie ~ col ~ _ ~ fieldName =>
        DropColumnField(col, fieldName, ifExists = ie)
    }

  lazy val alterColumnType: PackratParser[AlterColumnType] =
    alterColumnIfExists ~ identName ~ (keyword("SET") ~ keyword("DATA") ~ keyword(
      "TYPE"
    )) ~ extension_type ^^ { case ie ~ name ~ _ ~ newType =>
      AlterColumnType(name, newType, ifExists = ie)
    }

  lazy val alterColumnScript: PackratParser[AlterColumnScript] =
    alterColumnIfExists ~ identName ~ keyword("SET") ~ storedScript ^^ {
      case ie ~ name ~ _ ~ ((ns, stored)) =>
        AlterColumnScript(
          name,
          ScriptProcessor.fromScript(name, ns, Some(ns.out), materialized = stored),
          ifExists = ie
        )
    }

  lazy val dropColumnScript: PackratParser[DropColumnScript] =
    alterColumnIfExists ~ identName ~ (keyword("DROP") ~ keyword("SCRIPT")) ^^ {
      case ie ~ name ~ _ =>
        DropColumnScript(name, ifExists = ie)
    }

  /** The value grammar must match `defaultVal`'s: a column declares `DEFAULT _ingest.timestamp` at
    * CREATE time, so an ALTER that sets the same default on an existing column has to accept it
    * too. It did not, which broke the `_last_updated` column the materialized-view machinery adds
    * whenever that column already exists (`TableDiff` renders `ColumnDefaultSet` and the extension
    * runs the rendered SQL).
    */
  lazy val alterColumnDefault: PackratParser[AlterColumnDefault] =
    alterColumnIfExists ~ identName ~ (keyword("SET") ~ keyword(
      "DEFAULT"
    )) ~ (value | ingest_id | ingest_timestamp) ^^ { case ie ~ name ~ _ ~ dv =>
      AlterColumnDefault(name, dv, ifExists = ie)
    }

  lazy val dropColumnDefault: PackratParser[DropColumnDefault] =
    alterColumnIfExists ~ identName ~ (keyword("DROP") ~ keyword("DEFAULT")) ^^ {
      case ie ~ name ~ _ =>
        DropColumnDefault(name, ifExists = ie)
    }

  lazy val alterColumnNotNull: PackratParser[AlterColumnNotNull] =
    alterColumnIfExists ~ identName ~ (keyword("SET") ~ keyword("NOT") ~ keyword("NULL")) ^^ {
      case ie ~ name ~ _ =>
        AlterColumnNotNull(name, ifExists = ie)
    }

  lazy val dropColumnNotNull: PackratParser[DropColumnNotNull] =
    alterColumnIfExists ~ identName ~ (keyword("DROP") ~ keyword("NOT") ~ keyword("NULL")) ^^ {
      case ie ~ name ~ _ =>
        DropColumnNotNull(name, ifExists = ie)
    }

  lazy val alterColumnComment: PackratParser[AlterColumnComment] =
    alterColumnIfExists ~ identName ~ (keyword("SET") ~ keyword("COMMENT")) ~ literal ^^ {
      case ie ~ name ~ _ ~ c =>
        AlterColumnComment(name, c.value, ifExists = ie)
    }

  lazy val dropColumnComment: PackratParser[DropColumnComment] =
    alterColumnIfExists ~ identName ~ (keyword("DROP") ~ keyword("COMMENT")) ^^ {
      case ie ~ name ~ _ =>
        DropColumnComment(name, ifExists = ie)
    }

  lazy val alterTableMapping: PackratParser[AlterTableMapping] =
    ((keyword("SET") | keyword("ADD")) ~ keyword("MAPPING")) ~ option ^^ { case _ ~ opt =>
      AlterTableMapping(opt._1, opt._2)
    }

  lazy val dropTableMapping: PackratParser[DropTableMapping] =
    (keyword("DROP") ~ keyword("MAPPING")) ~> identName ^^ { m => DropTableMapping(m) }

  lazy val alterTableSetting: PackratParser[AlterTableSetting] =
    ((keyword("SET") | keyword("ADD")) ~ keyword("SETTING")) ~ option ^^ { case _ ~ opt =>
      AlterTableSetting(opt._1, opt._2)
    }

  lazy val dropTableSetting: PackratParser[DropTableSetting] =
    (keyword("DROP") ~ keyword("SETTING")) ~> identName ^^ { m => DropTableSetting(m) }

  /** `SET SCHEMA CACHE TTL = '10m'` — sugar over the metadata write it desugars to, NOT a second
    * way of storing the same thing (story 21.8 Part D). The TTL lives at
    * `SchemaCacheTtl.MetadataPath` whichever spelling wrote it, so `SET MAPPING`, `CREATE TABLE …
    * OPTIONS` and this production produce one AST, one diff, one mapping update and one read.
    *
    * The duration is validated HERE, by the same parse the client applies, so a misspelled TTL is
    * refused at parse time rather than silently ignored for the lifetime of the index. `err`, never
    * `throw`: `Parser.apply` is typed `Either[ParserError, Statement]` (#250).
    */
  lazy val alterTableSchemaCacheTtl: PackratParser[AlterTableMapping] =
    ((keyword("SET") ~ keyword("SCHEMA") ~ keyword("CACHE") ~ keyword(
      "TTL"
    )) ~ "=".? ~ literal) >> { case _ ~ _ ~ ttl =>
      SchemaCacheTtl.parse(ttl.value) match {
        case Right(_)     => success(AlterTableMapping(SchemaCacheTtl.MetadataPath, ttl))
        case Left(reason) => err(s"Invalid ${SchemaCacheTtl.Ddl}: $reason")
      }
    }

  lazy val dropTableSchemaCacheTtl: PackratParser[DropTableMapping] =
    (keyword("DROP") ~ keyword("SCHEMA") ~ keyword("CACHE") ~ keyword("TTL")) ^^ { _ =>
      DropTableMapping(SchemaCacheTtl.MetadataPath)
    }

  lazy val alterTableAlias: PackratParser[AlterTableAlias] =
    ((keyword("SET") | keyword("ADD")) ~ keyword("ALIAS")) ~ option ^^ { case _ ~ opt =>
      AlterTableAlias(opt._1, opt._2)
    }

  lazy val dropTableAlias: PackratParser[DropTableAlias] =
    (keyword("DROP") ~ keyword("ALIAS")) ~> identName ^^ { m => DropTableAlias(m) }

  lazy val alterTableStatement: PackratParser[AlterTableStatement] =
    addColumn |
    dropColumn |
    renameColumn |
    alterColumnOptions |
    alterColumnOption |
    dropColumnOption |
    alterColumnType |
    alterColumnScript |
    dropColumnScript |
    alterColumnDefault |
    dropColumnDefault |
    alterColumnNotNull |
    dropColumnNotNull |
    alterColumnComment |
    dropColumnComment |
    alterColumnFields |
    alterColumnField |
    dropColumnField |
    alterTableSchemaCacheTtl |
    dropTableSchemaCacheTtl |
    alterTableMapping |
    dropTableMapping |
    alterTableSetting |
    dropTableSetting |
    alterTableAlias |
    dropTableAlias

  lazy val alterTable: PackratParser[AlterTable] =
    (keyword("ALTER") ~ keyword("TABLE")) ~ ifExists ~ identRef ~ start.? ~ repsep(
      alterTableStatement,
      separator
    ) ~ end.? >> { case _ ~ ie ~ table ~ s ~ stmts ~ e =>
      // `err`, not `throw`: these run inside a combinator, and `Parser.apply` is typed
      // `Either[ParserError, Statement]` — a raw exception escapes that signature, and five
      // production call sites match on that Either with no `try` of their own
      // (SQLImplicits.queryToStatement, IndicesApi x3, the searchAs macro). Since #250 the
      // rule holds for the whole package: `Parser.apply` is total for NonFatal and no `throw`
      // may return here (ParserTotalitySpec's source scan enforces it).
      if (s.isDefined && e.isEmpty) {
        err("Mismatched closing parentheses in ALTER TABLE statement")
      } else if (s.isEmpty && e.isDefined) {
        err("Mismatched opening parentheses in ALTER TABLE statement")
      } else if (s.isEmpty && e.isEmpty && stmts.size > 1) {
        err("Multiple ALTER TABLE statements require parentheses")
      } else
        success(AlterTable(table._1, ie, stmts, parts = table._2))
    }

  // Watcher parsers

  // Watcher condition parsers
  lazy val alwaysWatcherCondition: PackratParser[AlwaysWatcherCondition.type] =
    keyword("ALWAYS") ^^ { _ => AlwaysWatcherCondition }

  lazy val neverWatcherCondition: PackratParser[NeverWatcherCondition.type] =
    keyword("NEVER") ^^ { _ => NeverWatcherCondition }

  /** 🔴 Ordered LONGEST spelling first, and it is load-bearing. These are string literals, not
    * anchored tokens: `gt` (`>`) matches the first character of `>=` and succeeds, leaving `=` for
    * the value production, which then fails with *"A value or a date/datetime function must be
    * provided for comparison"*. `WHEN x >= 0` and `WHEN x <= 0` were rejected for exactly that
    * reason, while `>`, `<`, `=` and `<>` parsed. `WhereParser.comparisonOp` has always had the
    * right order; this production had not.
    */
  private lazy val comparison_operator: PackratParser[ComparisonOperator] =
    eq | ne | diff | ge | gt | le | lt

  private lazy val dateMathScript
    : PackratParser[DateTimeFunction with FunctionWithIdentifier with DateMathScript] =
    date_add | datetime_add | date_sub | datetime_sub

  lazy val compareWatcherCondition: PackratParser[CompareWatcherCondition] =
    keyword("WHEN") ~> opt(not) ~ identName ~ comparison_operator ~ opt(value) ~ opt(
      dateMathScript
    ) >> { case n ~ field ~ op ~ v ~ fun =>
      // A `NOT` is folded into the operator. `comparison_operator` admits only the six arithmetic
      // comparisons, every one of which HAS a negated spelling, so `maybeNegated` is `Some` here
      // today -- but the answer is total (story BIDC-8, review M-6: `ComparisonOperator.not` used
      // to `MatchError` on every other comparison), so a widening of that production rejects the
      // combination instead of emitting a silently un-negated condition.
      val target_op = if (n.isDefined) op.maybeNegated.getOrElse(op) else op
      if (n.isDefined && op.maybeNegated.isEmpty)
        err(s"NOT is not supported with the $op operator in a watcher condition")
      else
        v match {
          case Some(value) =>
            success(CompareWatcherCondition(field, target_op, Left(value)))
          case None =>
            fun match {
              case Some(f) if f.identifier.dependencies.isEmpty =>
                success(
                  CompareWatcherCondition(
                    field,
                    target_op,
                    Right(f.identifier.withFunctions(f +: f.identifier.functions))
                  )
                )
              case Some(_) =>
                err(
                  "Date/datetime functions with field dependencies are not supported for comparison"
                )
              case None =>
                err("A value or a date/datetime function must be provided for comparison")
            }
        }
    }

  private lazy val scriptParams: PackratParser[ListMap[String, Value[_]]] =
    (keyword("WITH") ~ keyword("PARAMS")) ~> lparen ~ repsep(option, comma) ~ rparen ^^ {
      case _ ~ opts ~ _ =>
        ListMap(opts: _*)
    }

  lazy val scriptWatcherCondition: PackratParser[ScriptWatcherCondition] =
    (keyword("WHEN") ~ keyword("SCRIPT")) ~> literal ~ opt(
      keyword("USING") ~ keyword("LANG") ~> literal
    ) ~ opt(
      scriptParams
    ) ~ opt(keyword("RETURNS") ~ keyword("TRUE")) ^^ { case scr ~ lang ~ p ~ _ =>
      ScriptWatcherCondition(
        scr.value,
        lang.map(_.value).getOrElse("painless"),
        p.getOrElse(ListMap.empty)
      )
    }

  lazy val watcherCondition: PackratParser[WatcherCondition] =
    neverWatcherCondition | alwaysWatcherCondition | compareWatcherCondition | scriptWatcherCondition

  // Watcher trigger parsers
  lazy val triggerWatcherEveryInterval: PackratParser[IntervalWatcherTrigger] =
    keyword("EVERY") ~> """\d+\s+(MILLISECOND|SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)S?""".r ^^ {
      str =>
        val parts = str.trim.split("\\s+")
        IntervalWatcherTrigger(Delay(TransformTimeUnit(parts(1)), parts(0).toLong))
    }

  lazy val triggerWatcherAtSchedule: PackratParser[CronWatcherTrigger] =
    (keyword("AT") ~ keyword("SCHEDULE")) ~> literal ^^ { cronExpr =>
      CronWatcherTrigger(cronExpr.value)
    }

  lazy val watcherTrigger: PackratParser[WatcherTrigger] =
    triggerWatcherEveryInterval | triggerWatcherAtSchedule

  // Watcher input parsers
  lazy val simpleWatcherInput: PackratParser[SimpleWatcherInput] =
    opt(keyword("WITH") ~ keyword("INPUT")) ~> start ~ repsep(option, comma) ~ end ^^ {
      case _ ~ opts ~ _ =>
        SimpleWatcherInput(payload = ObjectValue(ListMap(opts: _*)))
    }

  lazy val withinTimeout: PackratParser[Option[Delay]] =
    opt(
      keyword("WITHIN") ~> """(\d+\s+(MILLISECOND|SECOND|MINUTE|HOUR|DAY|WEEK|MONTH|YEAR)S?)""".r
    ) ^^ {
      case Some(str) =>
        val parts = str.trim.split("\\s+")
        Some(Delay(TransformTimeUnit(parts(1)), parts(0).toLong))
      case None => None
    }

  /** Resolve alias-qualified identifiers in a WHERE against its own FROM clause — `o.status`
    * becomes `status`, tagged with `table = orders` — exactly as the SELECT path does. Any
    * production that keeps the criteria while flattening the FROM to bare index names MUST run
    * this: without it the alias is dropped from the index list but kept in the field name, and the
    * query targets a field that exists in no index (#212).
    *
    * `Select(Nil)` keeps the throwaway request's SELECT pass a no-op — the default `Select()` is
    * `SELECT *` — leaving `update()` to do its FROM and WHERE passes and nothing else. `update()`
    * reaches the criteria through `map`, so a `Some` WHERE can never come back `None`; callers for
    * which a lost WHERE would be destructive assert that anyway.
    */
  private def resolveWhere(f: From, w: Option[Where]): Option[Where] =
    SingleSearch(select = Select(Nil), from = f, where = w).update().where

  /** A single Elasticsearch search applies one query to every index it names, so a table-qualified
    * predicate over a multi-index FROM asks for per-index scoping that cannot be honored — whether
    * it correlates the indices (`WHERE o.id = c.order_id`, an ANSI-89 join, #191's defect in the
    * older syntax) or merely scopes to one of them (`WHERE orders.status = 'F'`, which would
    * silently filter `refunds` too).
    *
    * Testing for "any qualifier" rather than "qualifiers from two tables" is deliberate: the
    * narrower test lets a correlation through whenever one side fails to resolve — a function
    * argument (`WHERE o.id = LOWER(c.id)`), or — before story BIDC-8 — a self-join through
    * duplicate table names, where the alias map kept only the last alias.
    *
    * ⚠️ That last example became conditional, and the guard WIDENED because of it (story 21.2
    * AD-6'): when two tables differ only by qualifier the alias map keeps BOTH entries, so both
    * qualifiers resolve and this guard fires where it used to be defeated. MEASURED: `CREATE OR
    * REPLACE WATCHER w AS EVERY 5 MINUTES FROM "a".orders o, "b".orders p WHERE o.x = 1 WITHIN 2
    * MINUTES ALWAYS DO … END` is accepted before 21.2 and rejected after — #191's guard finally
    * firing on a statement it always meant to catch. Story BIDC-8 closed the last gap: qualifiers
    * resolve through the lossless `From.aliasesToTable`, so a WHOLLY unqualified self-join (`FROM
    * orders o, orders p WHERE o.x = 1`) now resolves `o` too and is rejected here as well — a
    * qualifier over a doubled single index is still a qualifier this search cannot scope.
    */
  private def qualifiedOverManyIndices(f: From, criteria: Option[Criteria]): Boolean =
    f.tables.size > 1 && criteria.exists(_.referencedIdentifiers.exists(_.table.isDefined))

  // A watcher search input maps to Elasticsearch's `search` input, which knows nothing but a list
  // of indices — there is no join engine behind it. `from` parses `a JOIN b ON …` happily, so
  // without this guard the join is dropped and the watcher silently watches `a` alone (#191).
  // `err` (not `failure`) is deliberate: it short-circuits the enclosing alternatives instead of
  // letting `watcherInput` fall through to `success(EmptyWatcherInput)` and report a position
  // error that names neither JOIN nor the watcher.
  lazy val searchInput: PackratParser[SearchWatcherInput] =
    from ~ opt(where) ~ withinTimeout >> { case f ~ w ~ t =>
      // Story 22.1 — a watcher input keeps only `tables.map(_.name)`, and a derived table's name
      // is its ALIAS, so the watcher would silently watch an index named after the subquery. Same
      // shape, same reason, as the JOIN refusal below (#191).
      if (f.hasDerivedTables)
        err(
          "A watcher input cannot search a derived table (subquery in FROM/JOIN): a watcher " +
          "searches indices. Watch the index directly, or pre-compute the subquery as a " +
          "MATERIALIZED VIEW and watch the view."
        )
      else
        f.joins match {
          case Nil =>
            val criteria = resolveWhere(f, w).flatMap(_.criteria)
            // Story 22.2 — a watcher input renders its WHERE into a watcher search body WITHOUT
            // crossing `SearchApi.resolveWithSchema`, so the two-phase subquery rewrite never runs
            // for it and an unresolved node would reach the query builder at deployment time. Same
            // shape, same reason, as the derived-table refusal above.
            if (criteria.exists(_.subqueries.nonEmpty))
              err(
                "A watcher input cannot carry a WHERE subquery (IN (SELECT ...), EXISTS " +
                "(SELECT ...), a scalar or quantified subquery): a watcher input is a single " +
                "Elasticsearch search and cannot run the inner query. Pre-compute the subquery's " +
                "values as a MATERIALIZED VIEW and watch the view."
              )
            else
            // `FROM a, b` stays a legitimate multi-index search; only a qualifier over it is
            // unserviceable — see `qualifiedOverManyIndices`.
            if (qualifiedOverManyIndices(f, criteria))
              err(
                s"A watcher input cannot qualify a column by table when it searches several " +
                s"indices (${f.tables.map(_.name).mkString(", ")}): one Elasticsearch search " +
                "applies one query to all of them, so it can neither join them nor scope a " +
                "predicate to one. Watch a single index, drop the qualifiers, or pre-join the " +
                "sources with a MATERIALIZED VIEW and watch the view."
              )
            else
              success(
                SearchWatcherInput(
                  f.tables.map(_.name).distinct,
                  criteria,
                  t
                )
              )
          case joins =>
            err(
              s"JOIN is not supported in a watcher input (${joins.map(_.sql.trim).mkString(" ")}): " +
              "a watcher input can only search one or more indices (FROM index1, index2). " +
              "Pre-join the sources with a MATERIALIZED VIEW and have the watcher search the view."
            )
        }
    }

  lazy val httpInput: PackratParser[HttpInput] =
    opt(keyword("WITH") ~ keyword("INPUT")) ~> httpRequest ^^ { req =>
      HttpInput(req)
    }

  lazy val chainInput: PackratParser[(String, WatcherInput)] =
    identName ~ opt(keyword("AS")) ~ watcherInput ^^ { case name ~ _ ~ input =>
      (name, input)
    }

  lazy val chainInputs: PackratParser[WatcherInput] =
    (keyword("WITH") ~ keyword("INPUTS")) ~> rep1sep(
      chainInput,
      comma
    ) ^^ { inputs =>
      ChainInput(ListMap(inputs: _*))
    }

  lazy val watcherInput: PackratParser[WatcherInput] =
    chainInputs | searchInput | httpInput | simpleWatcherInput | success(EmptyWatcherInput)

  // logging action parsers
  def info: Parser[LoggingLevel] = "(?i)(INFO)\\b".r ^^ { _ => LoggingLevel.INFO }
  def debug: Parser[LoggingLevel] = "(?i)(DEBUG)\\b".r ^^ { _ => LoggingLevel.DEBUG }
  def warn: Parser[LoggingLevel] = "(?i)(WARN)\\b".r ^^ { _ => LoggingLevel.WARN }
  def error: Parser[LoggingLevel] = "(?i)(ERROR)\\b".r ^^ { _ => LoggingLevel.ERROR }

  lazy val loggingLevel: PackratParser[LoggingLevel] =
    info | debug | warn | error

  // action foreach limit parser
  lazy val foreachWithLimit: PackratParser[(String, Int)] =
    (keyword("FOREACH") ~> literal) ~ (keyword("LIMIT") ~> """\d+""".r) ^^ { case fe ~ l =>
      (fe.value, l.toInt)
    }

  // simple logging action parser
  lazy val loggingAction: PackratParser[Option[LoggingAction]] =
    (keyword("LOG") ~> literal) ~ opt(keyword("AT") ~> loggingLevel) ~ opt(foreachWithLimit) ^^ {
      case text ~ levelOpt ~ feOpt =>
        val foreach = feOpt.map(_._1)
        val limit = feOpt.map(_._2)
        Some(LoggingAction(LoggingActionConfig(text.value, levelOpt), foreach, limit))
    }

  // webhook action parser
  lazy val webhookAction: PackratParser[Option[WebhookAction]] =
    keyword("WEBHOOK") ~> httpRequest ~ opt(foreachWithLimit) ^^ { case req ~ feOpt =>
      val foreach = feOpt.map(_._1)
      val limit = feOpt.map(_._2)
      Some(WebhookAction(req, foreach, limit))
    }

  lazy val watcherAction: PackratParser[(String, WatcherAction)] =
    identName ~ opt(keyword("AS")) ~ (loggingAction | webhookAction) >> { case name ~ _ ~ wa =>
      wa match {
        case Some(wa) => success((name, wa))
        case _        => err(s"Unsupported watcher action type in action '$name'")
      }
    }

  lazy val watcherActions: PackratParser[ListMap[String, WatcherAction]] =
    rep1sep(
      watcherAction,
      separator
    ) ^^ { actions =>
      ListMap(actions: _*)
    }

  /** The two CREATE WATCHER forms take `identRef` and use only its NAME: `CreateWatcher.sql`
    * delegates to `Watcher.sql`, whose `id` is the Elasticsearch watch id, so there is nowhere to
    * put a render-only quoting bit and none is carried (story 21.7 AD-3 — `Watcher.sql` re-quotes
    * by shape instead). `identRef` rather than `identName` so that CREATE, SHOW STATUS and DROP
    * accept the SAME watcher-name spellings; the discarded qualifier run is the price of that
    * uniformity, and it is one allocation on a statement that opens a watch.
    */
  lazy val createOrReplaceWatcher: PackratParser[CreateWatcher] =
    (keyword("CREATE") ~ keyword("OR") ~ keyword("REPLACE") ~ keyword(
      "WATCHER"
    )) ~> identRef ~ opt(
      keyword("AS")
    ) ~ watcherTrigger ~ watcherInput ~ watcherCondition ~ (keyword(
      "DO"
    ) ~> watcherActions <~ keyword("END")) ^^ {
      case name ~ _ ~ trigger ~ input ~ condition ~ actions =>
        CreateWatcher(
          name = name._1,
          orReplace = true,
          ifNotExists = false,
          condition = condition,
          trigger = trigger,
          actions = actions,
          input = input
        )
    }

  lazy val createWatcher: PackratParser[CreateWatcher] =
    (keyword("CREATE") ~ keyword("WATCHER")) ~ ifNotExists ~ identRef ~ opt(
      keyword("AS")
    ) ~ watcherTrigger ~ watcherInput ~ watcherCondition ~ (keyword(
      "DO"
    ) ~> watcherActions <~ keyword("END")) ^^ {
      case _ ~ _ ~ ine ~ name ~ _ ~ trigger ~ input ~ condition ~ actions =>
        CreateWatcher(
          name = name._1,
          orReplace = false,
          ifNotExists = ine,
          condition = condition,
          trigger = trigger,
          actions = actions,
          input = input
        )
    }

  lazy val showWatcherStatus: PackratParser[ShowWatcherStatus] =
    (keyword("SHOW") ~ keyword("WATCHER") ~ keyword("STATUS")) ~> identRef ^^ { name =>
      ShowWatcherStatus(name._1, parts = name._2)
    }

  lazy val showWatchers: PackratParser[ShowWatchers.type] =
    (keyword("SHOW") ~ keyword("WATCHERS")) ^^ { _ =>
      ShowWatchers
    }

  lazy val dropWatcher: PackratParser[DropWatcher] =
    (keyword("DROP") ~ keyword("WATCHER")) ~ ifExists ~ identRef ^^ { case _ ~ ie ~ name =>
      DropWatcher(name._1, ifExists = ie, parts = name._2)
    }

  lazy val createEnrichPolicy: PackratParser[CreateEnrichPolicy] =
    (keyword("CREATE") ~ keyword("ENRICH") ~ keyword("POLICY")) ~
    ifNotExists ~
    identRef ~
    opt(keyword("TYPE") ~> (keyword("MATCH") | keyword("GEO_MATCH") | keyword("RANGE"))) ~
    (keyword("FROM") ~> repsep(identName, separator)) ~
    (keyword("ON") ~> identName) ~
    (keyword("ENRICH") ~> repsep(identName, separator)) ~
    opt(where) ^^ { case _ ~ ine ~ name ~ policyTypeOpt ~ sources ~ on ~ refreshFields ~ whereOpt =>
      val policyType = policyTypeOpt match {
        case Some(value) => EnrichPolicyType(value)
        case _           => EnrichPolicyType.Match
      }
      CreateEnrichPolicy(
        name = name._1,
        policyType = policyType,
        from = sources,
        on = on,
        refreshFields,
        whereOpt,
        ifNotExists = ine,
        parts = name._2
      )
    }

  lazy val createOrReplaceEnrichPolicy: PackratParser[CreateEnrichPolicy] =
    (keyword("CREATE") ~ keyword("OR") ~ keyword("REPLACE") ~ keyword("ENRICH") ~ keyword(
      "POLICY"
    )) ~
    identRef ~
    opt(keyword("TYPE") ~> (keyword("MATCH") | keyword("GEO_MATCH") | keyword("RANGE"))) ~
    (keyword("FROM") ~> repsep(identName, separator)) ~
    (keyword("ON") ~> identName) ~
    (keyword("ENRICH") ~> repsep(identName, separator)) ~
    opt(where) ^^ { case _ ~ name ~ policyTypeOpt ~ sources ~ on ~ refreshFields ~ whereOpt =>
      val policyType = policyTypeOpt match {
        case Some("MATCH")     => EnrichPolicyType.Match
        case Some("GEO_MATCH") => EnrichPolicyType.GeoMatch
        case Some("RANGE")     => EnrichPolicyType.Range
        case _                 => EnrichPolicyType.Match
      }
      CreateEnrichPolicy(
        name = name._1,
        policyType = policyType,
        from = sources,
        on = on,
        refreshFields,
        whereOpt,
        orReplace = true,
        parts = name._2
      )
    }

  lazy val executeEnrichPolicy: PackratParser[ExecuteEnrichPolicy] =
    (keyword("EXECUTE") ~ keyword("ENRICH") ~ keyword("POLICY")) ~> identRef ^^ { name =>
      ExecuteEnrichPolicy(name._1, parts = name._2)
    }

  lazy val dropEnrichPolicy: PackratParser[DropEnrichPolicy] =
    (keyword("DROP") ~ keyword("ENRICH") ~ keyword("POLICY")) ~ ifExists ~ identRef ^^ {
      case _ ~ ie ~ name =>
        DropEnrichPolicy(name._1, ifExists = ie, parts = name._2)
    }

  lazy val showEnrichPolicy: PackratParser[ShowEnrichPolicy] =
    (keyword("SHOW") ~ keyword("ENRICH") ~ keyword("POLICY")) ~> identRef ^^ { name =>
      ShowEnrichPolicy(name._1, parts = name._2)
    }

  lazy val showEnrichPolicies: PackratParser[ShowEnrichPolicies.type] =
    (keyword("SHOW") ~ keyword("ENRICH") ~ keyword("POLICIES")) ^^ { _ =>
      ShowEnrichPolicies
    }

  lazy val showClusterName: PackratParser[ShowClusterName.type] =
    (keyword("SHOW") ~ keyword("CLUSTER") ~ keyword("NAME")) ^^ { _ =>
      ShowClusterName
    }

  lazy val showLicense: PackratParser[ShowLicense.type] =
    (keyword("SHOW") ~ keyword("LICENSE")) ^^ { _ =>
      ShowLicense
    }

  lazy val refreshLicense: PackratParser[RefreshLicense.type] =
    (keyword("REFRESH") ~ keyword("LICENSE")) ^^ { _ =>
      RefreshLicense
    }

  /** The body of a derived table, WITHOUT its parentheses (story 22.1 AD-3).
    *
    * The SAME pair, in the SAME order, as `dqlStatement` below, for the same reason: `|` commits to
    * the first SUCCEEDING alternative and `searchStatement` FAILS (does not partially succeed) on a
    * FROM-less body because `single` requires `from`, so `(SELECT 1 AS COL)` falls through to
    * `fromlessSelect` and `(SELECT a FROM t)` never does. Putting `fromlessSelect` first would
    * commit `(SELECT a FROM t)` to the prefix `SELECT a` and then fail on `FROM`. Do not reorder.
    *
    * The ascription is needed because `Parser[+T].|[U >: T]` cannot unify `SearchStatement` with
    * `FromlessSelect`; their common supertype is `DqlStatement`.
    */
  override lazy val derivedTableBodyInner: PackratParser[DqlStatement] =
    (searchStatement: PackratParser[DqlStatement]) | fromlessSelect

  /** Story 22.5 — the NAME of a CTE: ONE part, bare or quoted (`identRef` normalises a single bare
    * part to `parts = Nil`, so `WITH "My CTE" AS (…)` and `WITH my_cte AS (…)` spell exactly what
    * `FROM "My CTE"` / `FROM my_cte` reference). A QUALIFIED spelling is an `err`: a CTE lives in
    * the statement, not in a namespace, and reading `"s"."a"` as the CTE `a` would silently accept
    * a qualifier the statement can never honour.
    */
  lazy val cteName: PackratParser[NamePart] = identRef >> { case (n, ps) =>
    if (ps.size > 1)
      err(s"A CTE name must be a single unqualified name, got ${renderName(ps, n)}")
    else success(NamePart(n, ps.headOption.exists(_.quoted)))
  }

  /** The parenthesised body of a CTE.
    *
    * 🔴 NOT `derivedTableBodyInner` alone, and NOT `FromParser.derivedTable`: the first carries no
    * `err`, so `WITH a AS (t) …` would surface a grammar-internal identifier failure; the second
    * carries a derived-table-specific message and lives behind the FROM surface. A CTE body owns
    * its OWN `err` — safe for the same reason 22.1's is: after `AS (` nothing else can match, so
    * the `err` cannot steal an input a sibling would have taken.
    */
  lazy val cteBody: PackratParser[DqlStatement] =
    start ~> (derivedTableBodyInner | err(
      "A CTE body must be a SELECT: write WITH <name> AS (SELECT ...)"
    )) <~ end

  /** `<name> [(col, …)] AS (<body>)`.
    *
    * The column list is ACCEPTED by the grammar so it can be REFUSED with a message saying what to
    * write instead; without the optional arm the rejection would be a grammar-internal `AS
    * expected` at the `(`, which names neither the construct nor the remedy.
    */
  lazy val cteDefinition: PackratParser[Cte] =
    cteName ~ opt(start ~> rep1sep(identName, separator) <~ end) ~ (keyword("AS") ~> cteBody) >> {
      case n ~ Some(cols) ~ _ =>
        err(
          s"CTE '${n.value}' declares a column list (${cols.mkString(", ")}), which is not " +
          "supported: alias the columns in the CTE's SELECT list instead " +
          s"(WITH ${n.value} AS (SELECT expr AS ${cols.head}, ...))"
        )
      case n ~ None ~ body => success(Cte(n, body))
    }

  /** `WITH [RECURSIVE] <cte> [, <cte>]*`.
    *
    * `RECURSIVE` is refused BY NAME (epic 22 out of scope) rather than left to fail as an
    * identifier, so the user is told what is unsupported instead of where the parser stopped. The
    * `err` fires only when the literal follows `WITH`, so a CTE NAMED `recursive` in FIRST position
    * is refused too — an accepted, documented cost: the same name in a later position and the
    * QUOTED spelling (`WITH "recursive" AS (…)`, which `keyword("RECURSIVE")` cannot match) are
    * both accepted, and the quoted form is the documented escape hatch. Neither `with` nor
    * `recursive` is RESERVED by this story: rejecting a table or alias named `with` that parses
    * today would be a breaking change (`feedback_alternation_order_declines`).
    */
  lazy val withClause: PackratParser[Seq[Cte]] =
    keyword("WITH") ~> (
      (keyword("RECURSIVE") ~> err(
        "WITH RECURSIVE is not supported: only non-recursive common table expressions are " +
        "accepted (a CTE may reference the CTEs defined before it, never itself)"
      )) |
      rep1sep(cteDefinition, separator)
    )

  /** Story 22.5 — a search statement prefixed by a `WITH` clause.
    *
    * Substitution runs HERE, in the parser action, on the FULLY PARSED statement — ONCE. It is not
    * in `update()` because `update()` re-runs on every executed statement
    * (`SearchApi.resolveWithSchema` is `copy(schema).update()`), so a rewrite there would need an
    * idempotency latch and the WITH list would have to be reachable from every branch and every
    * body at update time. See [[app.softnetwork.elastic.sql.query.CteSubstitution]].
    *
    * Its rejections (duplicate name, forward/self reference) are `err`s: raised at the END of a
    * fully consumed input they sit FURTHER than every sibling alternative's offset-0 failure, so
    * `Failure.append` keeps them and the user sees our message rather than the terminal
    * `dmlStatement` alternative's regex complaint.
    */
  lazy val withQuery: PackratParser[SearchStatement] =
    withClause ~ searchStatement >> { case ctes ~ body =>
      CteSubstitution(ctes, body) match {
        case Right(stmt)  => success(stmt)
        case Left(reason) => err(reason)
      }
    }

  lazy val dqlStatement: PackratParser[DqlStatement] = {
    // Story 22.5 — FIRST, and it narrows nothing: `withQuery` begins with the literal `(?i)WITH\b`
    // while every other alternative of `dqlStatement`, `ddlStatement` and `dmlStatement` begins
    // with a DIFFERENT keyword, so they are disjoint at the first token. First is chosen because a
    // `\bWITH\b` test is cheaper than trying the SELECT regex on every statement, and because it
    // keeps `searchStatement` and `fromlessSelect` adjacent (#251).
    withQuery |
    searchStatement |
    // Issue #251 — FROM-less SELECT. MUST stay immediately AFTER searchStatement: `|` commits
    // to the first SUCCEEDING alternative, and searchStatement FAILS (not partially succeeds)
    // on FROM-less input because `single` requires `from`; placing fromlessSelect first would
    // commit to the prefix parse and break every `SELECT ... FROM ...`. Do not reorder.
    fromlessSelect |
    showTables |
    showTable |
    showCreateTable |
    showPipelines |
    showPipeline |
    showCreatePipeline |
    describePipeline |
    showMaterializedViewStatus |
    showMaterializedViews |
    showMaterializedView |
    showCreateMaterializedView |
    describeMaterializedView |
    describeTable |
    showWatchers |
    showWatcherStatus |
    showEnrichPolicy |
    showEnrichPolicies |
    showLicense |
    showClusterName |
    refreshLicense
  }

  lazy val ddlStatement: PackratParser[DdlStatement] =
    // Recognise-to-reject, FIRST on purpose — measured, not assumed. `TEMPORARY` is mandatory and
    // no other alternative accepts it in that position, so this can never commit to a prefix of a
    // statement another alternative handles (proved by a 994-statement differential probe: zero
    // verdict changes against the previous tree). Placing it first also makes the diagnosis
    // strictly better in BOTH directions: its `Error` short-circuits everything to its right with
    // no position arithmetic at all, while for a non-temporary typo (`CREATE TABEL t (…)`) its own
    // `Failure` lands at the same offset as `createTable`'s and `Failure.append` keeps the LATER
    // result on a tie (scala-parser-combinators 1.1.2, `Parsers.scala:195-198`) — so from here it
    // yields the tie instead of winning it, and `CREATE TABEL` is not told about TEMPORARY.
    temporaryTable |
    createTable |
    createPipeline |
    createOrReplaceTable |
    createOrReplacePipeline |
    alterTable |
    alterPipeline |
    dropTable |
    truncateTable |
    dropPipeline |
    createMaterializedView |
    createOrReplaceMaterializedView |
    dropMaterializedView |
    refreshMaterializedView |
    createWatcher |
    createOrReplaceWatcher |
    dropWatcher |
    createEnrichPolicy |
    createOrReplaceEnrichPolicy |
    executeEnrichPolicy |
    dropEnrichPolicy

  lazy val onConflict: PackratParser[OnConflict] =
    (keyword("ON") ~ keyword("CONFLICT") ~> opt(conflictTarget) <~ keyword("DO")) ~ (keyword(
      "UPDATE"
    ) | keyword("NOTHING")) ^^ { case target ~ action =>
      OnConflict(target, action == "UPDATE")
    }

  lazy val conflictTarget: PackratParser[List[String]] =
    start ~> repsep(identName, separator) <~ end

  /** INSERT INTO table [(col1, col2, ...)] VALUES (v1, v2, ...) */
  lazy val insert: PackratParser[Insert] =
    (keyword("INSERT") ~ keyword("INTO")) ~ identRef ~ opt(
      lparen ~> repsep(identName, comma) <~ rparen
    ) ~
    ((keyword("VALUES") ~> rows) ^^ { vs => Right(vs) }
    | keyword("AS").? ~> searchStatement ^^ { q => Left(q) }) ~ opt(onConflict) ^^ {
      case _ ~ table ~ colsOpt ~ vals ~ conflict =>
        conflict match {
          case Some(c) => Insert(table._1, colsOpt.getOrElse(Nil), vals, Some(c), parts = table._2)
          case _ =>
            vals match {
              case Left(q: SingleSearch) =>
                Insert(table._1, colsOpt.getOrElse(Nil), vals, q.onConflict, parts = table._2)
              case _ => Insert(table._1, colsOpt.getOrElse(Nil), vals, parts = table._2)
            }
        }
    }

  /** FILE_FORMAT [=] {PARQUET | JSON | JSON_ARRAY | DELTA_LAKE} — bare or quoted, `=` optional.
    *
    * `FILE_FORMAT = X` is the form every published example uses (documentation, help JSON — and
    * `FileFormat.sql` itself renders it), yet the grammar only accepted the bare `FILE_FORMAT X`:
    * under the pre-#213 prefix parse, `opt(fileFormat)` backtracked on the `=` and the clause —
    * plus any ON CONFLICT after it — was discarded in silence, with format auto-detection masking
    * the loss. A FILE_FORMAT followed by anything but a known format is a hard `err` for the same
    * reason: backtracking here can only ever mean dropping what the user wrote.
    */
  lazy val fileFormat: PackratParser[FileFormat] =
    (keyword("FILE_FORMAT") ~ opt("=")) ~> (
      (keyword("PARQUET") ^^^ Parquet) |
      (keyword("JSON_ARRAY") ^^^ JsonArray) |
      (keyword("JSON") ^^^ Json) |
      (keyword("DELTA_LAKE") ^^^ Delta) |
      // Quoted format names (the form dml_statements.md documents) land here; so does anything
      // unrecognised, which must err rather than backtrack into a silent drop.
      //
      // DELIBERATELY still `ident` (story 21.7 Task 1.1): a FILE_FORMAT is an enum value, not a
      // name, and `literal` ahead of it already accepts every quoted spelling a caller writes.
      ((literal ^^ (_.value) | ident) >> { name =>
        name.toUpperCase(java.util.Locale.ROOT) match {
          case "PARQUET"    => success(Parquet)
          case "JSON_ARRAY" => success(JsonArray)
          case "JSON"       => success(Json)
          case "DELTA_LAKE" => success(Delta)
          case other =>
            err(
              s"Unsupported FILE_FORMAT '$other': expected PARQUET, JSON, JSON_ARRAY or DELTA_LAKE"
            )
        }
      })
    )

  /** COPY INTO table FROM source */
  lazy val copy: PackratParser[CopyInto] =
    (keyword("COPY") ~ keyword("INTO")) ~ identRef ~ (keyword("FROM") ~> literal) ~ opt(
      fileFormat
    ) ~ opt(onConflict) ^^ { case _ ~ table ~ source ~ format ~ conflict =>
      CopyInto(
        source.value,
        table._1,
        fileFormat = format,
        onConflict = conflict,
        parts = table._2
      )
    }

  /** UPDATE table SET col1 = v1, col2 = v2 [WHERE ...]
    *
    * UPDATE has no FROM clause. `opt(from)` is here only to catch one being written anyway —
    * `Parser.apply` runs `parse`, not `phrase`, so an unconsumed `FROM customers JOIN x ON …` used
    * to be discarded in silence and the UPDATE ran against the first table alone (#213). It catches
    * both operand orders: written before the WHERE, `where.?` yields None and this fires.
    */
  lazy val update: PackratParser[Update] =
    (keyword("UPDATE") ~> identRef) ~ (keyword("SET") ~> repsep(
      identName ~ "=" ~ (value | scriptValue),
      separator
    )) ~ where.? ~ opt(from) >> { case table ~ assigns ~ w ~ extraFrom =>
      extraFrom match {
        case Some(f) =>
          err(
            s"UPDATE does not support a FROM clause (${f.sql.trim}): " +
            "UPDATE targets exactly one table, named right after the UPDATE keyword. " +
            "Filter with WHERE, or pre-join the sources with a MATERIALIZED VIEW."
          )
        case None =>
          val values = ListMap(assigns.map { case col ~ _ ~ v => col -> v }: _*)
          // UPDATE keeps only the bare table name, so its WHERE needs the same qualifier
          // resolution DELETE and watcher inputs get — `WHERE orders.id = 1` must filter on `id`.
          // The `Table` built for that resolution deliberately carries NO parts: it exists to
          // resolve alias qualifiers against the bare index name and never reaches a render.
          success(
            Update(table._1, values, resolveWhere(From(Seq(Table(table._1))), w), parts = table._2)
          )
      }
    }

  /** DELETE FROM table [WHERE ...]
    *
    * Parses the full FROM shape — alias and joins included — rather than a bare `ident`, then
    * rejects what DELETE cannot express. `Parser.apply` runs `parse`, not `phrase`, so everything
    * past the first table name used to be discarded in silence: `DELETE FROM a, b WHERE …` became
    * `DELETE FROM a` with **no** WHERE, which the client turns into `match_all` — wiping the whole
    * index instead of the matching rows (#213).
    */
  lazy val delete: PackratParser[Delete] =
    (keyword("DELETE") ~ keyword("FROM")) ~> rep1sep(table, separator) ~ where.? >> {
      case tables ~ w =>
        tables.flatMap(_.joins) match {
          // Story 22.1 — `DELETE FROM` shares `table` with the SELECT surface, so without this arm
          // a derived table would parse and `Delete(tables.head, …)` would carry a `Table` whose
          // name is the subquery's ALIAS: the delete-by-query would target an index that does not
          // exist, or worse one that happens to. Nothing downstream re-checks it (#191's class).
          case Nil if tables.exists(_.derived.isDefined) =>
            err(
              "DELETE cannot target a derived table (subquery in FROM): Elasticsearch deletes by " +
              "query over an index. Name the index and move the subquery's predicate into the " +
              "WHERE clause."
            )
          case Nil if tables.size > 1 =>
            err(
              s"DELETE targets a single table, got ${tables.map(_.name).mkString(", ")}: " +
              "issue one DELETE per table."
            )
          case Nil =>
            resolveWhere(From(tables), w) match {
              // Fail closed. A DELETE with no WHERE is `match_all`, so a WHERE lost in resolution
              // would empty the index — the very outcome #213 is about. `update()` reaches the
              // criteria through `map` and cannot drop it, which is exactly why this must stay
              // loud rather than becoming a comment claiming it cannot happen.
              case None if w.isDefined =>
                err(
                  s"Could not resolve the WHERE clause of DELETE FROM ${tables.head.name}: " +
                  "refusing to run it as an unfiltered delete."
                )
              case resolved => success(Delete(tables.head, resolved))
            }
          case joins =>
            err(
              s"JOIN is not supported in DELETE (${joins.map(_.sql.trim).mkString(" ")}): " +
              "Elasticsearch deletes by query over a single index. Select the ids to remove with " +
              "a JOIN query first, then DELETE on that key."
            )
        }
    }

  lazy val dmlStatement: PackratParser[DmlStatement] = insert | update | delete | copy

  lazy val statement: PackratParser[Statement] = ddlStatement | dqlStatement | dmlStatement

  /** Strip `--` comments and collapse newlines OUTSIDE string literals only. The previous
    * line-based normalizer (`split("\n").map(_.split("--")(0))`) was blind to quotes: it cut `WHERE
    * code = 'AB--12'` at the `--`, severing the literal and failing a valid statement. Literal
    * interiors — including their backslash escapes — are copied verbatim.
    */
  private def normalize(query: String): String = {
    val out = new StringBuilder(query.length)
    var quote: Char = 0
    var i = 0
    while (i < query.length) {
      val c = query.charAt(i)
      if (quote != 0) {
        out.append(c)
        // A backslash escapes inside a single- or double-quoted run -- the grammar's own literal
        // and identifier productions read it that way -- but NOT inside a backticked identifier,
        // where the only escape is a doubled backtick and a backslash is an ordinary character.
        if (c == '\\' && quote != '`' && i + 1 < query.length) {
          out.append(query.charAt(i + 1))
          i += 1
        } else if (c == quote) {
          quote = 0
        }
        i += 1
      } else if (c == '-' && i + 1 < query.length && query.charAt(i + 1) == '-') {
        // comment runs to end of line; the newline itself is handled by the next iteration
        while (i < query.length && query.charAt(i) != '\n') i += 1
      } else {
        c match {
          // The backtick joins the quote set for #252: without it a `--` inside a backticked
          // identifier is read as a comment and deletes the rest of the statement -- a silent
          // truncation, not an error.
          case '\'' | '"' | '`' =>
            quote = c
            out.append(c)
          case '\n' | '\r' => out.append(' ')
          case _           => out.append(c)
        }
        i += 1
      }
    }
    out.toString
  }

  /** Prefix for the one failure mode `err(...)` cannot reach: a throw from the AST, not the
    * grammar.
    *
    * `single` calls `.update()` inside its combinator action (see `def single` above), so the whole
    * AST-update surface runs during the parse. TWO sites on that surface throw today, measured:
    * `SingleSearch.bucketNames` (query/package.scala:125) indexes `select.fields(n - 1)` with no
    * bounds check, so `GROUP BY 0` raises IndexOutOfBoundsException(-1) and `GROUP BY 9` over two
    * columns raises IndexOutOfBoundsException(8); `Bucket.update` (query/GroupBy.scala:76) raises
    * IllegalArgumentException on `GROUP BY -1`. Both are three frames below any combinator and no
    * `err` conversion reaches them, so this catch is the only thing that keeps `apply`'s
    * `Either[ParserError, Statement]` honest for the five call sites that wrap it in no `try` of
    * their own (SQLImplicits.queryToStatement, IndicesApi x3, the searchAs macro).
    *
    * Note that `bucketNames` crashes on a bare INDEX, not on a `throw` - no source scan can ever
    * see it, which is why ParserTotalitySpec covers this surface by INPUT instead.
    *
    * Totality is claimed for `NonFatal` only: `VirtualMachineError` (a StackOverflowError from a
    * pathologically nested parse) still escapes, deliberately - turning a runaway recursion into a
    * tidy 400 would make it a denial-of-service amplifier.
    *
    * The prefix keeps the 400 the caller now gets from reading as "your syntax is bad" when it is
    * not. Ordinal-bucket semantics belong to story 21.3 / issue #253.
    */
  val InternalParseFailure: String = "Internal parser error"

  /** The grammar half of [[apply]], WITHOUT `validate()` — ONE owner of the normalisation and of
    * the packrat reader construction (story 22.3, lead ruling OQ-8).
    *
    * `Parser.single`'s action still runs `.update()` on the statement it builds, so everything the
    * UPDATE pass records (`Identifier.tableAlias` / `table`, story 22.2's `correlatedRefs`, the
    * bucket names) is populated exactly as `apply` would have populated it — only the VALIDATION
    * pass is skipped. That is what a test of a rule that `validate()` itself enforces needs: it
    * must reach the AST of a statement `apply` refuses, and it must not re-implement the reader (a
    * second construction would drift from `apply`'s, and packrat memoisation keys on the parser
    * INSTANCE).
    *
    * `private[sql]` on purpose: nothing outside this module may execute an unvalidated statement.
    */
  private[sql] def parseUnvalidated(query: String): Either[ParserError, Statement] =
    grammar(query)

  /** Shared by [[apply]] and [[parseUnvalidated]]: normalise, build the packrat reader, run
    * `phrase(statement)`. The `NonFatal` boundary catch (#250) wraps the WHOLE body because the AST
    * surface `.update()` drags in can throw before `parse` even returns.
    */
  private def grammar(query: String): Either[ParserError, Statement] = {
    try {
      val normalizedQuery =
        normalize(query)
          // Trailing statement terminators are idiomatic SQL and every caller that splits on `;`
          // already drops them; keep tolerating them now that anything else left over is an error.
          // A run rather than one, because tools that append `;` to SQL a user already terminated
          // produce `;;`. Anchored, so a `;` inside a literal is out of reach — a literal always
          // ends in its closing quote.
          .replaceFirst("(?:;\\s*)+$", "")
          .trim
      val reader = new PackratReader(new CharSequenceReader(normalizedQuery))
      // `phrase`, not a bare `parse` (#213): a bare `parse` succeeds on the longest matching PREFIX
      // and silently discards the rest, so the statement that ran was not the statement written.
      // Measured before/after on this exact grammar:
      //
      //   DELETE FROM orders WHEREE id = 1     ran as DELETE FROM orders  -> emptied the index
      //   DELETE FROM orders LIMIT 10          ran as DELETE FROM orders  -> emptied the index
      //   UPDATE orders SET a = 1 LIMIT 10     updated every document
      //   SELECT a FROM x UNION SELECT b ...   ran as SELECT a FROM x     (bare UNION is not the
      //                                        UNION ALL token, so the whole second leg vanished)
      //   SELECT * FROM t WHERE a IS  NOT  NULL  ran as SELECT * FROM t   (two spaces broke the
      //                                        token match and the WHERE was dropped — fixed in
      //                                        TokenRegex with \s+, and now loud here if it recurs)
      //
      // All of those are hard errors now. The per-statement guards added for #213 cover only the
      // shapes someone enumerated; requiring the whole input to be consumed covers the rest. The
      // one shape this cannot catch: `DELETE FROM orders customers` stays a valid single-table
      // DELETE, because an alias without AS is standard SQL — pinned as such in ParserSpec.
      parse(phrase(statement), reader) match {
        case NoSuccess(msg, _)  => Left(ParserError(msg))
        case Success(result, _) => Right(result)
      }
    } catch {
      // #250. Totality is claimed for `NonFatal` only: VirtualMachineError (a StackOverflowError
      // from a pathologically nested parse), ControlThrowable, LinkageError and
      // InterruptedException still escape, deliberately - swallowing those would hide a broken JVM
      // as a SQL error. Every rejection the GRAMMAR owns is an `err(...)` and arrives above as a
      // NoSuccess; this branch is reached from the AST surface described on `InternalParseFailure`
      // and from a null input to `normalize`.
      //
      // What keeps it that way, precisely: ParserTotalitySpec's source scan is scoped to
      // `parser/**` ONLY, so it polices the GRAMMAR half and nothing else. It does NOT cover the
      // AST surface under `query/**` that `.update()` drags in - and could not, since
      // `bucketNames` crashes on a bare index with no `throw` token to find. That residual is
      // covered by INPUT instead (ParserTotalitySpec's GROUP BY cases) and is handed to story
      // 21.3 / #253.
      //
      // The exception CLASS is always in the message. Measured: `GROUP BY 0` throws an
      // IndexOutOfBoundsException whose entire message is "-1", and `Internal parser error: -1`
      // tells a reader nothing. `getMessage` can also be null - `ElasticResult.attempt` already
      // ships the "Operation failed: null" bug from exactly that.
      //
      // The cause is CARRIED, not discarded: `ElasticResult.attempt` used to supply it and `core`
      // reads `error.cause`. Dropping it would delete the only stack trace an internal parser
      // fault ever produces. A grammar rejection keeps `cause = None` - a user's syntax error has
      // no interesting stack.
      case NonFatal(e) =>
        // 🔴 The two accessors below can THEMSELVES throw, which would escape `apply` and defeat
        // the totality this branch exists to provide: `Class.getSimpleName` raises
        // `InternalError: Malformed class name` on JDK 8 for some Scala inner/anonymous classes
        // (and the drivers promise JDK 8 for ES 6/7/8), and a custom `Throwable` may override
        // `getMessage` to throw. Neither is reachable with the exceptions measured on this path -
        // all named JDK classes on JDK 11 - which is exactly why they are guarded rather than
        // trusted. `getSimpleName` also returns "" for an anonymous class, which would render
        // `Internal parser error: : -1`.
        Left(internalFailure(e))
    }
  }

  /** ONE builder for both boundary catches (#250). Two copies of this rule in one file is the
    * story-21.3 desync class, one file over.
    */
  private def internalFailure(e: Throwable): ParserError = {
    // 🔴 Both accessors can THEMSELVES throw, which would escape the boundary this exists to
    // provide: `Class.getSimpleName` raises `InternalError: Malformed class name` on JDK 8 for some
    // Scala inner/anonymous classes (and the drivers promise JDK 8 for ES 6/7/8), and a custom
    // `Throwable` may override `getMessage` to throw. `getSimpleName` also returns "" for an
    // anonymous class, which would render `Internal parser error: : -1`.
    def safely(read: => String, fallback: String): String =
      try Option(read).map(_.trim).filter(_.nonEmpty).getOrElse(fallback)
      catch { case NonFatal(_) => fallback }
    val className = safely(e.getClass.getSimpleName, safely(e.getClass.getName, "Throwable"))
    val message = safely(e.getMessage, "")
    val detail = if (message.isEmpty) "" else s": $message"
    ParserError(s"$InternalParseFailure: $className$detail", Some(e))
  }

  def apply(
    query: String
  ): Either[ParserError, Statement] =
    grammar(query) match {
      case Right(result) =>
        // 🔴 `validate()` runs INSIDE the boundary catch's sibling, not inside it: `grammar` has
        // already returned, so a `validate()` that threw would escape `apply`. It cannot today —
        // every `validate()` in the tree returns `Either` — and the AST surface that DOES throw
        // (`bucketNames`, #253) is reached from `.update()`, which runs inside `grammar`. Guarded
        // here anyway, for the same reason #250 guarded the grammar half: totality is a property
        // of the METHOD, not of today's arms.
        try result.validate() match {
          case Left(error) => Left(ParserError(error))
          case _           => Right(result)
        } catch {
          case NonFatal(e) => Left(internalFailure(e))
        }
      case left => left
    }

}

trait CompilationError

/** `cause` is set ONLY by `Parser.apply`'s NonFatal boundary catch (#250). A grammar rejection - an
  * `err(...)` or a `validate()` Left - leaves it `None`: a user's syntax error has no interesting
  * stack, and attaching one would put a parser internal in front of a BI user for no reason.
  *
  * Defaulted, so every existing `ParserError(msg)` construction compiles unchanged. It IS a
  * binary-compatibility change for the `sql` module (case-class arity); there are no destructuring
  * `case ParserError(m)` patterns anywhere in this repo or in any sibling repo.
  *
  * Two further consequences of putting a `Throwable` in a case class, stated because they are easy
  * to trip over: the generated `equals`/`hashCode` now include `cause`, and a `Throwable` compares
  * by IDENTITY - so two internal-fault results for the SAME input are never `==` to each other
  * (grammar rejections, whose cause is `None`, still compare by message as before). And `toString`
  * renders the whole throwable. Compare `.msg`, never the `ParserError` value, on the
  * internal-fault route.
  */
case class ParserError(msg: String, cause: Option[Throwable] = None) extends CompilationError

trait Parser
    extends RegexParsers
    with PackratParsers
    with AggregateParser
    with ArithmeticParser
    with CondParser
    with ConvertParser
    with GeoParser
    with MathParser
    with StringParser
    with TemporalParser
    with TypeParser
    with HttpParser { _: WhereParser with OrderByParser with LimitParser =>

  protected def keyword(word: String): Parser[String] = s"(?i)$word\\b".r ^^ (_ => word)

  /** Story 22.1 — the derived-table productions, DECLARED here because `FromParser` (which owns
    * `derivedTable`) sees this trait through its self-type while `searchStatement` /
    * `fromlessSelect` live on `object Parser`. Implemented there and in `FromParser` respectively.
    *
    * Story 22.2 reuses `derivedTableBodyInner` for `IN (SELECT …)` / `EXISTS (SELECT …)` — with its
    * OWN `start`/`end`, and WITHOUT `derivedTable`'s `err` alternative, which would fire on `WHERE
    * a = (b + 1)` before the alternation could fall back to `equality`. The paren-bearing
    * `derivedTableBody = start ~> derivedTableBodyInner <~ end` the 22.1 spec named is deliberately
    * NOT shipped here: nothing in this story calls it (`derivedTable` inlines the parentheses so
    * that an UNTERMINATED body stays a plain `Failure` instead of taking the `err` branch), and an
    * artifact ships no unreachable code. It is one line for 22.2 to add at its own call site.
    */
  def derivedTableBodyInner: PackratParser[app.softnetwork.elastic.sql.query.DqlStatement]

  def derivedTable: PackratParser[app.softnetwork.elastic.sql.query.DerivedTable]

  /** The pre-21.7 DDL/DML name regex. It is no longer used directly by any statement: story 21.7
    * routed every one of its call sites through `identRef` (object references) or `identName`
    * (columns, option keys, struct-entry keys), which accept the same bare spelling PLUS both quote
    * styles. It survives as the LAST alternative of each of those, and only for that — see
    * `identParts` for why removing it would be a customer-visible narrowing.
    */
  private val identRegex: Regex = bareNameRegex.r

  def ident: Parser[String] = identRegex

  val lparen: Parser[String] = "("
  val rparen: Parser[String] = ")"
  val comma: Parser[String] = ","
  val lbracket: Parser[String] = "["
  val rbracket: Parser[String] = "]"
  val startStruct: Parser[String] = "{"
  val endStruct: Parser[String] = "}"

  lazy val objectValue: PackratParser[ObjectValue] =
    lparen ~> repsep(option, comma) <~ rparen ^^ { opts =>
      ObjectValue(ListMap(opts: _*))
    }

  lazy val objectValues: PackratParser[ObjectValues] =
    lbracket ~> rep1sep(objectValue, comma) <~ rbracket ^^ { ovs =>
      ObjectValues(ovs)
    }

  // `ingest_id | ingest_timestamp` for the same reason as `alterColumnDefault`: the mapping
  // metadata a column's DEFAULT is mirrored into (`_meta.columns.<c>.default_value`) is written
  // through this production.
  lazy val option: PackratParser[(String, Value[_])] =
    (identName | literal) ~ "=" ~ (objectValues | objectValue | value | ingest_id | ingest_timestamp) ^^ {
      case key ~ _ ~ value =>
        key match {
          case lit: StringValue => (lit.value, value)
          case id: String       => (id, value)
        }
    }

  lazy val options: PackratParser[ListMap[String, Value[_]]] =
    keyword("OPTIONS") ~ lparen ~ repsep(option, comma) ~ rparen ^^ { case _ ~ _ ~ opts ~ _ =>
      ListMap(opts: _*)
    }

  lazy val array_of_struct: PackratParser[ObjectValues] =
    lbracket ~> repsep(struct, comma) <~ rbracket ^^ { ovs =>
      ObjectValues(ovs)
    }

  lazy val struct_entry: PackratParser[(String, Value[_])] =
    identName ~ "=" ~ (array_of_struct | struct | value) ^^ { case key ~ _ ~ v =>
      key -> v
    }

  lazy val struct: PackratParser[ObjectValue] =
    startStruct ~> repsep(struct_entry, comma) <~ endStruct ^^ { entries =>
      ObjectValue(ListMap(entries: _*))
    }

  lazy val start: PackratParser[Delimiter] = "(" ^^ (_ => StartPredicate)

  lazy val end: PackratParser[Delimiter] = ")" ^^ (_ => EndPredicate)

  lazy val separator: PackratParser[Delimiter] = "," ^^ (_ => Separator)

  lazy val valueExpr: PackratParser[PainlessScript] = {
    // the order is important here
    identifierWithWindowFunction |
    identifierWithTransformation | // transformations applied to an identifier
    identifierWithIntervalFunction |
    identifierWithFunction | // fonctions applied to an identifier
    quotedIdentifier | // double-quoted identifiers (ANSI SQL-92 delimited identifiers)
    identifierWithValue |
    identifier
  }

  implicit def functionAsIdentifier(mf: Function): Identifier = mf match {
    case id: Identifier => id
    case fid: FunctionWithIdentifier =>
      fid.identifier //.withFunctions(fid +: fid.identifier.functions)
    case _ => Identifier(mf)
  }

  lazy val sql_function: PackratParser[Function] =
    aggregate_function | time_function | conditional_function

  private val reservedKeywords = Seq(
    "select",
    "insert",
    "update",
    "copy",
    "delete",
    "create",
    "alter",
    "drop",
    "truncate",
    "column",
    "from",
    "join",
    "inner",
    "left",
    "right",
    "full",
    "cross",
    "outer",
    "on",
    "where",
    "group",
    "having",
    "order",
    "limit",
    "offset",
    "as",
    "by",
    "except",
    "unnest",
    "current_date",
    "current_time",
    "current_datetime",
    "current_timestamp",
    "now",
    "today",
    "coalesce",
    "nullif",
    "isnull",
    "isnotnull",
    "greatest",
    "least",
    "row_number",
    "rank",
    "dense_rank",
    "date_add",
    "date_sub",
    "parse_date",
    "parse_datetime",
    "format_date",
    "format_datetime",
    "date_trunc",
    "extract",
    "date_diff",
    "datetime_add",
    "datetime_sub",
    "interval",
//    "year",
//    "month",
//    "day",
//    "hour",
//    "minute",
//    "second",
//    "quarter",
//    "char",
//    "string",
//    "byte",
//    "tinyint",
//    "short",
//    "smallint",
//    "int",
//    "integer",
//    "long",
//    "bigint",
//    "real",
//    "float",
//    "double",
    "pi",
//    "boolean",
    "distance",
//    "time",
//    "date",
//    "datetime",
//    "timestamp",
    "and",
    "or",
    "not",
    "like",
    "in",
    "between",
    "distinct",
    "cast",
    "count",
    "min",
    "max",
    "avg",
    "sum",
    "stddev",
    "stddev_pop",
    "stddev_samp",
    "variance",
    "var_pop",
    "var_samp",
    "percentile_cont",
    "percentile_disc",
    "case",
    "when",
    "then",
    "else",
    "end",
    "union",
    // Story 22.6 -- SQL-92 reserves it, and without it `FROM t INTERSECT SELECT ...` reads
    // INTERSECT as `t`'s ALIAS (`regexAlias` carries the reserved-word lookahead). The story's ONE
    // narrowing: a column or alias literally named `intersect` must now be quoted, exactly as one
    // named `union` already had to be.
    "intersect",
    "all",
    "exists",
    "true",
    "false",
//    "nested",
//    "parent",
//    "child",
    "match",
    "against",
    "abs",
    "ceil",
    "floor",
    "exp",
    "log",
    "log10",
    "sqrt",
    "round",
    "pow",
    "sign",
    "sin",
    "asin",
    "cos",
    "acos",
    "tan",
    "atan",
    "atan2",
    "concat",
    "substr",
    "substring",
    "to",
    "length",
    "lower",
    "upper",
    "trim",
    "first",
    "last",
    "array_agg",
    "first_value",
    "last_value",
    "ltrim",
    "rtrim",
    "replace",
    "on",
    "conflict",
    "do",
    "show",
    "describe",
    "every",
    "at",
    "never",
    "always",
    "foreach",
    "within"
//    "protocol",
//    "http",
//    "https",
//    "host",
//    "port"
  )

  // `identifierRegexStr` / `identifierRegex` lived here until story 21.2. Story 21.1 rewrote
  // `identifier` onto `qualifiedName` and 21.2 rewrote `FromParser.table` onto `tableParts`, which
  // left them with zero callers in this repo and zero in jdbc / arrow / extensions -- so they were
  // deleted rather than kept as a second, drifting lexing surface. Their reserved-word guard and
  // character class live on in `bareFirstPartStr` / `bareNextPartStr` below.

  // -----------------------------------------------------------------------------------------------
  // Quoted identifiers (#252). ONE lexing surface, shared by `identifier`, `quotedIdentifier` and
  // `alias`, so a new production cannot forget it and the two styles cannot drift apart.
  // -----------------------------------------------------------------------------------------------

  private val doubleQuoteChar = "\""

  /** ANSI SQL-92 delimited identifier. Two escapes are accepted:
    *   - `""` -- the SQL standard, and what every generator that quotes at all emits;
    *   - a backslash before the delimiter or before itself -- non-standard, but what this
    *     production has always accepted, so nothing that parses today stops parsing. Deliberately
    *     NOT extended to the backtick style below.
    *
    * The content quantifier is `+`, not `*`: an EMPTY `""` is a valid empty STRING literal
    * (`TypeParser.literal`) and must keep falling through to it rather than becoming a nameless
    * column.
    */
  private val doubleQuotedNameStr =
    doubleQuoteChar + """(?:[^"\\]|""|\\.)+""" + doubleQuoteChar

  /** MySQL delimited identifier -- the spelling every Tableau generic-JDBC connection emits. Only
    * the standard doubled-backtick escape; a backslash is an ordinary character here.
    */
  private val backQuotedNameStr = "`(?:[^`]|``)+`"

  /** One quoted lexeme, either style, delimiters included. */
  val quotedNameRegex: Regex = s"(?:$doubleQuotedNameStr|$backQuotedNameStr)".r

  /** Strips the delimiters and un-escapes, in ONE left-to-right pass.
    *
    * `Parser.normalize` was rewritten from a line-based pass into a scan for the same reason: one
    * pass is obviously correct without a case analysis.
    *
    * 🔴 It is deliberately NOT folded onto `unescapeStringLiteral(content, delimiter)`, which story
    * 21.5 added for string LITERALS -- and a merge note here used to prescribe exactly that fold.
    * The note was wrong: the two escape ALPHABETS differ, and folding them would silently change
    * what a quoted identifier means (story 21.5 AD-12, measured both ways).
    *
    *   - An IDENTIFIER un-escapes a backslash before ANY character, so `"a\nb"` is the column
    *     `anb`. That is what this loop's `c == '\\' && q == '"'` arm does, and it has been the
    *     behaviour since story 21.1.
    *   - A LITERAL un-escapes only `\\` and a backslash before its own delimiter, so `'a\nb'` keeps
    *     the backslash -- a contract COPY INTO paths depend on.
    *
    * The backtick arm is local for a second, independent reason (story 21.1 AD-5): inside backticks
    * the ONLY escape is a doubled backtick, and a shared backslash-aware scan would collapse a
    * doubled backslash that must survive.
    */
  private def unquoteName(lexeme: String): String = {
    val q = lexeme.charAt(0)
    val last = lexeme.length - 1
    val out = new StringBuilder(lexeme.length)
    var i = 1
    while (i < last) {
      val c = lexeme.charAt(i)
      if (c == q && i + 1 < last && lexeme.charAt(i + 1) == q) {
        out.append(q)
        i += 2
      } else if (c == '\\' && q == '"' && i + 1 < last) {
        out.append(lexeme.charAt(i + 1))
        i += 2
      } else {
        out.append(c)
        i += 1
      }
    }
    out.toString
  }

  /** The characters an UNQUOTED name part may contain -- the character class of the pre-21.1
    * whole-name `identifierRegexStr` (deleted in 21.2) minus the dot, which `qualifiedName`
    * consumes as the separator between parts.
    */
  private val barePartChars = """[a-zA-Z0-9_\-\[\]\*]"""

  /** The FIRST part keeps the reserved-word guard and the "no leading digit" rule the deleted
    * whole-name regex carried. Dropping the guard here would let `SELECT FROM t` read `FROM` as a
    * column name.
    */
  private val bareFirstPartStr =
    s"""(?i)(?!(?:${reservedKeywords.mkString("|")})\\b)[\\*a-zA-Z_\\-]$barePartChars*"""

  /** Every part AFTER the first has neither guard.
    *
    * The pre-21.1 whole-name regex matched a dotted name in ONE go, so its negative lookahead only
    * ever applied at offset 0: `t.from`, `doc.count`, `order.min` and `item.first` all parse today
    * as one identifier, and `logs-2025.03` has a digit-leading second part. Re-applying the guard
    * (or the leading-digit rule) per part would newly reject every one of them.
    */
  private val bareNextPartStr = s"$barePartChars+"

  /** Compiled ONCE, as a `val`, never inside a `def`. `bareFirstPartStr` interpolates the
    * reserved-keyword alternation, and `String.r` is `Pattern.compile` -- calling it inside a `def`
    * recompiles a 131-alternative regex on every construction of the hottest production in the
    * grammar.
    *
    * `bareNextPartStr` has no regex of its own: it is interpolated into `nameTailPartStr`, which
    * owns the separator so the dot and the part cannot be split by whitespace (#285).
    */
  private val bareFirstPartRegex: Regex = bareFirstPartStr.r

  private lazy val quotedPart: PackratParser[(String, Boolean)] =
    quotedNameRegex ^^ (lexeme => (unquoteName(lexeme), true))

  private lazy val bareFirstPart: PackratParser[(String, Boolean)] =
    bareFirstPartRegex ^^ (n => (n, false))

  /** One dot-separated tail element, **separator included**, matched as a SINGLE regex so the dot
    * and the part that follows it must be ADJACENT.
    *
    * `RegexParsers` skips whitespace before every terminal, so the earlier `rep("." ~> part)`
    * spelling let a name ending in a dot swallow the next word across whitespace (#285): `ORDER BY
    * b. DESC` parsed as `ORDER BY "b.DESC" ASC` and the sort direction vanished **silently**.
    * Owning the dot closes that -- after `b` the tail cannot match `. DESC`, so `b` is the name and
    * `DESC` is the direction -- and it also restores the pre-21.1 rejection of `SELECT a . b`,
    * which the part-split had widened into an acceptance.
    *
    * Residual, deliberately accepted: whitespace BEFORE the dot is still skipped by the enclosing
    * `rep`, so `SELECT a .b` reads as `a.b`. That is a tolerance of an odd spelling, not a silent
    * reading change -- no clause is mis-parsed and nothing is lost.
    */
  private val nameTailPartStr =
    s"""\\.(?:$doubleQuotedNameStr|$backQuotedNameStr|$bareNextPartStr)"""

  /** Compiled once, for the same reason as `bareFirstPartRegex`. */
  private val nameTailPartRegex: Regex = nameTailPartStr.r

  private lazy val nameTailPart: PackratParser[(String, Boolean)] =
    nameTailPartRegex ^^ { lexeme =>
      val part = lexeme.substring(1) // drop the leading dot, which this regex owns
      part.charAt(0) match {
        case '"' | '`' => (unquoteName(part), true)
        case _         => (part, false)
      }
    }

  private lazy val nameTail: PackratParser[List[(String, Boolean)]] = rep(nameTailPart)

  private def joinNameParts(parts: List[(String, Boolean)]): (String, Boolean) =
    (parts.map(_._1).mkString("."), parts.exists(_._2))

  /** One dot-separated name; each part is independently bare or quoted. The `Boolean` is true iff
    * ANY part was quoted -- that single bit is everything `Identifier.sql` needs to re-emit the
    * quoting (story 21.1 AD-1). Style is not retained: a backticked `a` and `"a"` denote the same
    * column and must produce equal ASTs.
    *
    * PUBLIC on purpose: story 21.2 rewrites `FromParser.table` on top of this so the FROM/JOIN
    * surface cannot become a second lexer.
    */
  lazy val qualifiedName: PackratParser[(String, Boolean)] =
    (quotedPart | bareFirstPart) ~ nameTail ^^ { case h ~ t => joinNameParts(h :: t) }

  /** `qualifiedName`, but the FIRST part must be quoted -- which makes `quotedIdentifier` a strict
    * subset of `identifier` and lets every existing `quotedIdentifier | ...` alternation keep its
    * exact current behaviour.
    */
  lazy val quotedQualifiedName: PackratParser[(String, Boolean)] =
    quotedPart ~ nameTail ^^ { case h ~ t => joinNameParts(h :: t) }

  /** One QUOTED name part consumed as a leading TABLE-name qualifier: `` `prod_us`. `` or
    * `"elastic".`.
    *
    * 🔴 The quoting IS the discriminator, and that asymmetry is deliberate. An UNQUOTED dot belongs
    * to the index name: `logs-2025.03` is one Elasticsearch index, and `elastic.bi_events` has
    * always been read as one. There is nothing else in the statement that separates "qualifier"
    * from "name that contains a dot", so a design that split on dots would move 48 of the 99
    * captured BI statements onto an index that does not exist (story 21.2 AD-1, measured).
    *
    * What the qualifier MEANS is deliberately not decided here -- the parser preserves, the
    * resolver interprets (#85).
    *
    * 🔴 The dot is a SEPARATE terminal here, unlike `nameTailPart` above which owns its dot. That
    * is not an oversight and it must not be "unified": `RegexParsers` skips whitespace before every
    * terminal, so this production accepts `"elastic" . bi_events` and `"elastic" .bi_events`
    * exactly as the `quotedSchemaPrefix` it replaces did (both MEASURED parsing on `origin/main`,
    * both reading index `bi_events`). Folding the dot into one adjacency-strict regex would make
    * `"elastic" .bi_events` fall through to `qualifiedName`, whose tail WOULD match `.bi_events`,
    * and the statement would silently start reading index `elastic.bi_events` -- a renderer-free
    * index move on an input that works today. Preserving today's reading outranks symmetry.
    *
    * `quotedPart` is private to this trait; this is its one FROM-side export.
    */
  lazy val qualifierPart: PackratParser[NamePart] =
    quotedPart <~ "." ^^ (p => NamePart(p._1, quoted = true))

  /** A table reference as the ordered part list the statement wrote -- never split, never
    * role-assigned, never dropped (#85, story 21.2 AD-1).
    *
    * Shape: the maximal LEADING run of quoted-part-followed-by-a-dot qualifiers, then the name
    * (21.1's `qualifiedName`, which JOINS a mixed tail into one lexeme) -- so the name is always
    * the LAST part, and `Table.name` is `parts.last.value` structurally rather than interpretively.
    *
    * `qualifiedName` is used JOINED on purpose. `"sch".a.b` must resolve to index `a.b` (measured
    * on `origin/main`), and `elastic."bi_events"` -- whose FIRST part is bare, so the qualifier run
    * is empty -- must resolve to the single index name `elastic.bi_events`. Splitting the tail into
    * separate parts would render it `"elastic"."bi_events"`, which re-parses as qualifier `elastic`
    * + index `bi_events`: a different index, manufactured by the renderer.
    *
    * `rep`, not `opt`: story 20.3 (jdbc#34) made the JDBC driver advertise BOTH a catalog (the
    * constant `elasticsearch`) and a schema (the discovered cluster name), so a catalog-aware
    * generator can emit `"elasticsearch"."prod-cluster"."bi_events"`. No captured statement does
    * today; `rep` costs one character and removes the whole class of failure. Each level stays its
    * OWN part -- nothing is joined; the resolver assigns roles.
    *
    * `rep` is greedy and a FAILED iteration consumes nothing, which is exactly what is wanted here:
    * for `"elastic"."bi_events"` the second iteration fails at the missing dot without consuming,
    * so the qualifier run is [elastic] and `qualifiedName` gets `"bi_events"`. `rep` does NOT
    * backtrack into a SUCCESSFUL iteration, so `FROM "elastic".` fails the whole production rather
    * than re-trying with zero qualifiers -- also wanted, and pinned.
    *
    * PUBLIC because `FromParser` has `self: Parser with ... =>` and can only see public members.
    */
  lazy val tableParts: PackratParser[Seq[NamePart]] =
    rep(qualifierPart) ~ qualifiedName ^^ { case ps ~ nq => ps :+ NamePart(nq._1, nq._2) }

  // -----------------------------------------------------------------------------------------------
  // The DDL/DML name surface (story 21.7). `Parser.ident` was the THIRD name surface of this
  // dialect and the only one story 21.1/21.2 left untouched, so `INSERT`, `UPDATE`, `CREATE`,
  // `DROP`, `ALTER`, `COPY INTO` and every SHOW/DESCRIBE disagreed with `SELECT` — and with each
  // other — about what a name may look like. Lead ruling 2026-09-04: DQL, DML and DDL all respect
  // the same rules. These two productions are how: every former `ident` call site takes one of
  // them, and neither adds a lexer (both are built from 21.1's `qualifiedName` / 21.2's
  // `tableParts`).
  // -----------------------------------------------------------------------------------------------

  /** An object reference in a DDL/DML statement, as its ordered part list: 21.2's `tableParts`,
    * with the legacy `ident` regex retained as the LAST alternative.
    *
    * 🔴 The fallback is not belt-and-braces, it is what keeps this a pure widening. `tableParts`
    * reaches the name through `bareFirstPart`, which carries the reserved-word negative lookahead
    * `identifier` needs so that `SELECT FROM t` does not read `FROM` as a column. `ident` never had
    * it — so `CREATE TABLE t (count INTEGER)`, `DROP TABLE order`, `INSERT INTO t (min) …` and
    * roughly 130 further bare names parse today and would stop parsing if `tableParts` replaced
    * `ident` outright. That is a customer-visible regression with no migration path inside the
    * release, and AC-5 of this story forbids it. Keeping `ident` last is also what makes the
    * normalising render of a COLUMN name (below) a fixed point for every legacy name.
    *
    * What that leaves is a residual asymmetry — `FROM count` is rejected while `DROP TABLE count`
    * is accepted — which is PRE-EXISTING (DQL has always rejected it) and is not made worse here.
    * Narrowing DDL onto DQL's reserved-word rule is a product decision about a breaking change, not
    * a grammar clean-up, and belongs to whoever schedules that deprecation.
    *
    * Ordering: `tableParts` FIRST, because it is the one that can consume MORE — `logs-2025.03`, ``
    * `#Tableau…` ``, `"sch".tbl` are all invisible to `ident`.
    *
    * 🔴 `<~ not(".")` is what keeps that ordering from NARROWING. `|` commits to the first
    * SUCCEEDING alternative, and on a dangling dot `tableParts` succeeds on a PREFIX — `a` out of
    * `a.`, `a` out of `a..b` — leaving the rest unconsumed, so the enclosing sequence fails and a
    * name `ident` used to swallow whole stops parsing. The lookahead makes `tableParts` DECLINE
    * exactly there, handing the lexeme to `ident` unchanged. It consumes nothing and cannot alter a
    * well-formed name: every accepted shape ends at a delimiter, not at a dot.
    *
    * The story is a PURE widening by lead ruling (2026-09-08): nothing that parsed before stops
    * parsing, on any surface. `identName` carries the identical guard for the same reason — the
    * measurement that forced it was an OPTION key (`OPTIONS (a. = 1)`), and a rule justified by "no
    * regression in existing parsing" cannot hold for option keys and not for table names.
    */
  lazy val identParts: PackratParser[Seq[NamePart]] =
    (tableParts <~ not(".")) | (ident ^^ (n => Seq(NamePart(n, quoted = false))))

  /** `identParts` reduced to what an AST node carries: the name (the LAST part's value,
    * structurally — never an interpretation) and the parts, `Nil` when they hold nothing the name
    * does not.
    *
    * 🔴 The `Nil` normalisation is load-bearing. A single BARE part IS `name`, so recording it
    * would make every parsed DDL node unequal to the programmatically built one — `TableDiff`,
    * `IndicesApi` and `AlterTableRoundTripSpec` all build `AlterTable("dest", …)` with no parts and
    * compare it against a re-parse. Story 21.2 hit exactly this with
    * `FromlessSelect.toSingleSearch` and fixed it at the construction site; here the reference is
    * normalised at the ONE site that produces it, so the AST and the render of every bare-spelled
    * statement stay byte-identical to what they were before this story (AC-5).
    */
  lazy val identRef: PackratParser[(String, Seq[NamePart])] =
    identParts ^^ { ps =>
      (ps.last.value, if (ps.size == 1 && !ps.head.quoted) Nil else ps)
    }

  /** A DDL/DML name that takes NO qualifier run: a column, an option key, a struct-entry key.
    *
    * Both quote styles accepted, delimiters stripped, case preserved verbatim, the reserved-word
    * lookahead bypassed inside quotes — exactly what story 21.1 pinned for expression positions, so
    * `"c"` in `INSERT INTO t ("c") …` denotes the same column as `"c"` in `SELECT "c" FROM t`.
    *
    * `qualifiedName` JOINS its dot-separated parts, which is what a name in these positions needs:
    * an ES sub-field path (`a.b`) and a mapping metadata path (`_meta.columns.<c>.default_value`,
    * written through `option`) are ONE key, not two.
    *
    * A column list is not a table reference, so there is deliberately no `qualifierPart` run here.
    * MEASURED consequence, and it is the wanted one: `INSERT INTO t ("sch".c)` names the single
    * column `sch.c` — every part is JOINED and none can be dropped — exactly as `SELECT "e"."c"`
    * names the single identifier `e.c` on the expression surface. Adding a qualifier run instead
    * would let a DDL statement silently DISCARD a leading part of a column name.
    *
    * The render is `renderColumnName`, whose predicate is this production's own `ident` fallback:
    * bare when the name matches `bareNameRegex`, ANSI-quoted otherwise. That keeps the render a
    * fixed point for a name that needs quoting WITHOUT threading a per-name bit through the
    * `List[String]` / `ListMap[String, _]` these positions live in.
    *
    * 🔴 `<~ not(".")` for the same reason `identParts` carries it, and MEASURED on an option key:
    * `OPTIONS (a. = 1)` and `OPTIONS (a..b = 1)` parse today, `qualifiedName` succeeds on the
    * PREFIX `a`, `|` commits, and the enclosing `~ "="` then fails against the leftover dot — a key
    * that parsed would have stopped parsing. The lookahead makes `qualifiedName` decline on a
    * dangling dot and `ident` take the lexeme whole, exactly as before. Differential probe against
    * `origin/main` over 28 option/struct shapes: 25 byte-identical, 3 widened (a backtick key, a
    * backtick struct-entry key, and a hyphenated key such as `Content-Type` that `ident`'s charset
    * could never spell), 0 narrowed.
    */
  lazy val identName: PackratParser[String] =
    ((qualifiedName ^^ (_._1)) <~ not(".")) | ident

  /** Kept, and kept FIRST in `SelectParser.field`, `GroupByParser.bucketWithFunction`,
    * `OrderByParser.fieldWithFunction` and `WhereParser.any_identifier`/`isNull`/`isNotNull`, for
    * one load-bearing reason: a double-quoted lexeme also matches `TypeParser.literal`, and each of
    * those productions has a LATER alternative that reaches it
    * (`identifierWithArithmeticExpression` -> `factor` -> `valueExpr` ->
    * `identifierWithIntervalFunction` -> `identifierWithValue`). Delete this production and rely on
    * the quoting now folded into `identifier`, and `SELECT "category"` / `GROUP BY "category"` /
    * `ORDER BY "category"` all flip from a column reference to a string literal. Ordering there is
    * not style.
    *
    * It is ALSO the operand-position reading, since story 21.1 AD-13 placed it immediately before
    * `identifierWithValue` inside `identifierWithIntervalFunction` -- see that call site for why
    * the alternative had to exist for the render to be a fixed point.
    */
  lazy val quotedIdentifier: PackratParser[Identifier] =
    (Distinct.regex.? ~ quotedQualifiedName ^^ { case d ~ nq =>
      GenericIdentifier(nq._1, None, d.isDefined, quoted = nq._2)
    }) >> cast

  /** `quotedIdentifier`, but it declines to match when an arithmetic operator follows it (#284).
    *
    * The four clause-level productions -- `SelectParser.field`, `GroupByParser.bucketWithFunction`,
    * `OrderByParser.fieldWithFunction` and `WhereParser.any_identifier` -- all list
    * `quotedIdentifier` AHEAD of `identifierWithArithmeticExpression`, and `|` commits to the first
    * SUCCEEDING alternative. So a quoted lexeme at the HEAD of an arithmetic expression was
    * consumed alone and the operator was left unconsumed: ``SELECT `amount` + 1`` failed with `end
    * of input expected`, while ``SELECT (`amount` + 1)`` and ``SELECT MAX(`amount` + 1)`` both
    * worked.
    *
    * That ordering cannot simply be reversed. It is what makes `SELECT "category"` a COLUMN rather
    * than a string literal, because a double-quoted lexeme also matches `TypeParser.literal` (story
    * 21.1 AD-3). A one-token LOOKAHEAD is the narrow fix: it changes only which alternative wins
    * when an operator is genuinely next, and `not(...)` consumes nothing, so every other input
    * reaches `quotedIdentifier` exactly as before.
    *
    * Deliberately NOT applied to `quotedIdentifier` itself. In OPERAND position -- inside
    * `identifierWithIntervalFunction`, reached from `factor` -- `quotedIdentifier` matching the
    * bare name is precisely what lets `arithmeticExpressionLevel1`'s `rep` pick up the rest of the
    * expression. Guarding it there would push the operand down to `identifierWithValue` and turn it
    * back into a string, which is the AD-13 corruption in reverse.
    */
  lazy val quotedIdentifierUnlessArithmetic: PackratParser[Identifier] =
    quotedIdentifier <~ not(add | subtract | multiply | divide | modulo)

  /** A quoted lexeme that can ONLY be an identifier: its first part is quoted AND at least one
    * dot-separated part follows it (issue #332).
    *
    * Story 21.1 AD-13 deliberately lists `literal` BEFORE any identifier alternative in a VALUE
    * position, so `WHERE a = "x"` is the string `x`. That is correct and is not touched here. But
    * it also swallowed the QUALIFIED form: on the right of `=`, `"t0"."k"` consumed `"t0"` as a
    * string literal and left `."k"` as trailing input, so `ON b.k = "t0"."k"` -- the exact JOIN
    * predicate Tableau's SQL-92 dialect emits -- failed with `end of input expected`, while its
    * backtick twin parsed (a backtick is never a string literal) and the mirrored `ON "b"."k" =
    * t0.k` parsed too.
    *
    * The discriminator is STRUCTURAL, not a property of the parsed name: `rep1(nameTailPart)`
    * demands a dot BETWEEN parts in the input. A filter such as `name.contains(".")` would be
    * wrong, because `"a.b"` is ONE quoted part whose CONTENT holds a dot and must keep reading as
    * the string `a.b`. Nothing else changes: a lone `"x"` has an empty tail, fails this production,
    * and reaches `literal` exactly as before -- so every AD-13 pin holds by construction rather
    * than by case analysis.
    *
    * Used ONLY by the two `WhereParser` alternations that ALREADY accept an identifier on the right
    * (`equality`, `comparison`). `IN` / `BETWEEN` / `LIKE` take literals only; giving them an
    * identifier operand would be a new feature, not this fix.
    */
  lazy val quotedQualifiedIdentifier: PackratParser[Identifier] =
    (Distinct.regex.? ~ (quotedPart ~ rep1(nameTailPart) ^^ { case h ~ t =>
      joinNameParts(h :: t)
    }) ^^ { case d ~ nq =>
      GenericIdentifier(nq._1, None, d.isDefined, quoted = nq._2)
    }) >> cast

  /** THE identifier production. Quoting is folded in here rather than sprinkled over the ~35 sites
    * that end in `| identifier` -- the four-alternative operand idiom alone occurs 21 times -- so a
    * production added later inherits it instead of having to remember it.
    */
  lazy val identifier: PackratParser[Identifier] =
    (Distinct.regex.? ~ qualifiedName ^^ { case d ~ nq =>
      GenericIdentifier(nq._1, None, d.isDefined, quoted = nq._2)
    }) >> cast

  lazy val identifierWithTransformation: PackratParser[Identifier] =
    (mathematicalFunctionWithIdentifier |
    conversionFunctionWithIdentifier |
    conditionalFunctionWithIdentifier |
    timeFunctionWithIdentifier |
    stringFunctionWithIdentifier |
    geoFunctionWithIdentifier) >> cast

  lazy val identifierWithFunction: PackratParser[Identifier] =
    ((rep1sep(
      sql_function,
      start
    ) ~ start.? ~ (identifierWithTransformation | identifierWithIntervalFunction | identifier).?) >> {
      case f ~ s ~ i =>
        // Close exactly the parentheses this production opened — one per `rep1sep` separator, plus
        // the optional one introducing the innermost identifier. It used to close with `rep1(end)`
        // — one or MORE — which made a call to a name-only extractor (`YEAR(x)`, `MONTH(x)`, …)
        // swallow the `)` of whatever enclosed it, so `ABS(YEAR(x))` reported `')' expected but 'F'
        // found` at the FROM (issue #220), and `SCRIPT AS (YEAR(x))` blamed the SCRIPT keyword
        // (issue #219). Functions that consume their own parentheses (DATE_TRUNC, DATE_DIFF,
        // WEEKDAY, …) were never affected, which is why the failure looked arbitrary.
        val opened = f.size - 1 + (if (s.isDefined) 1 else 0)
        if (opened < 1)
          // No parenthesis was opened, so this is a bare function name rather than a call — the
          // `rep1` this replaced rejected it too, and accepting it would let `SELECT MAX FROM t`
          // parse as a function applied to nothing.
          failure("function call expected")
        else
          repN(opened, end) ^^ { _ =>
            i match {
              case None =>
                f.lastOption match {
                  case Some(fi: FunctionWithIdentifier) =>
                    fi.identifier.withFunctions(f ++ fi.identifier.functions)
                  case _ => Identifier(f)
                }
              case Some(id) => id.withFunctions(f ++ id.functions)
            }
          }
    }) >> cast

  private val regexAlias =
    s"""\\b(?i)(?!(?:${reservedKeywords.mkString("|")})\\b)[a-zA-Z0-9_.]*""".stripMargin

  /** Compiled once, for the same reason as `bareFirstPartRegex`: this string interpolates the
    * reserved-keyword alternation and `alias` is tried after every field, every JOIN source and
    * every table.
    */
  private val regexAliasRegex: Regex = regexAlias.r

  /** The quoted form comes FIRST: `regexAlias`'s class is `*`, so it matches the empty string, and
    * only its leading word boundary stops it succeeding in front of a delimiter. Relying on that is
    * one refactor away from an empty alias silently winning.
    *
    * All four alias positions ride this one production -- the SELECT list (`SelectParser.field`),
    * `UNNEST(...) alias`, a JOIN source's alias and the FROM table's alias (`FromParser`) -- so a
    * backticked table alias and `SELECT a AS "my col"` are the same fix.
    */
  lazy val alias: PackratParser[Alias] =
    Alias.regex.? ~ (quotedNameRegex ^^ (l => Alias(unquoteName(l), quoted = true)) |
    regexAliasRegex ^^ (b => Alias(b))) ^^ { case _ ~ a => a }

  /** Retained for `SelectParser.field`'s `(quotedAlias | alias)`, and now a strict subset of
    * `alias` -- same lexeme, same un-escaping, same `quoted` bit.
    */
  lazy val quotedAlias: PackratParser[Alias] =
    Alias.regex.? ~ quotedNameRegex ^^ { case _ ~ l => Alias(unquoteName(l), quoted = true) }

}
