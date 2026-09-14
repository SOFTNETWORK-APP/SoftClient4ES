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

import app.softnetwork.elastic.sql.GenericIdentifier
import app.softnetwork.elastic.sql.query.{
  CrossJoin,
  DerivedTable,
  From,
  FullJoin,
  InnerJoin,
  Join,
  JoinType,
  LeftJoin,
  On,
  RightJoin,
  StandardJoin,
  Table,
  Unnest
}

trait FromParser {
  self: Parser with WhereParser with LimitParser =>

  def unnest: PackratParser[Join] =
    Unnest.regex ~ start ~ identifier ~ end ~ alias.? ^^ { case _ ~ i ~ _ ~ a =>
      Unnest(i, None, a)
    }

  def inner_join: PackratParser[JoinType] = InnerJoin.regex ^^ { _ => InnerJoin }
  def left_join: PackratParser[JoinType] = LeftJoin.regex ^^ { _ => LeftJoin }
  def right_join: PackratParser[JoinType] = RightJoin.regex ^^ { _ => RightJoin }
  def full_join: PackratParser[JoinType] = FullJoin.regex ^^ { _ => FullJoin }
  def cross_join: PackratParser[JoinType] = CrossJoin.regex ^^ { _ => CrossJoin }
  def join_type: PackratParser[JoinType] =
    inner_join | left_join | right_join | full_join | cross_join

  def on: PackratParser[On] = On.regex ~> whereCriteria >> { rawTokens =>
    // `On(criteria: Criteria)` is not optional (query/From.scala), which is why an ON whose
    // criteria resolve to nothing used to `throw new Exception`. #250: `err`, like every other
    // rejection in this package - see `WhereParser.where` for the full reasoning.
    processTokens(rawTokens) match {
      case Right(Some(criteria)) => success(On(criteria))
      case Right(None)           => err("ON clause requires criteria")
      case Left(reason)          => err(reason)
    }
  }

  /** A JOIN's table source.
    *
    * Returns a partially-built `StandardJoin` (no join type, no ON) so that `(unnest | source)` is
    * a `PackratParser[Join]`. It used to be a `Parser[Any]` destructured with `case (i: Identifier,
    * a: Option[Alias])` — an unchecked erasure pattern. Typing it here removes the warning and the
    * class of bug it hides.
    *
    * 🔴 It takes `tableParts`, NOT the bare `identifier`. After story 21.1 `identifier` is
    * quoting-aware and JOINS dot-separated parts, which is right for a column (`alias.column`) and
    * wrong for an index. MEASURED on `origin/main` BEFORE this story: `JOIN "prod_us".customers c`
    * parses and reads index `prod_us.customers` while the FROM side of the same statement reads
    * `customers` — one statement, two rules, no error. This production is what closes that.
    *
    * `quoted = false` on the `GenericIdentifier` is deliberate, and it is the single most likely
    * mistake in this story: `Identifier.sql` quotes PER dot-separated part, which is correct for
    * `alias.column` and fatal for an index name whose dot is literal — `` JOIN `logs-2025.03` c ``
    * would render `"logs-2025"."03"` and re-parse as qualifier `logs-2025` + index `03`.
    * `StandardJoin.sql` renders each `NamePart` as ONE lexeme instead (21.2 AD-5).
    */
  def source: PackratParser[StandardJoin] =
    derivedTable ^^ { dt =>
      // A derived JOIN leg owns its alias and its parts are empty — the AD-1 invariant that
      // `StandardJoin.validate()` ENFORCES.
      StandardJoin(source = dt, joinType = None, on = None, alias = None, parts = Nil)
    } |
    tableParts ~ alias.? ^^ { case ps ~ a =>
      StandardJoin(
        source = GenericIdentifier(ps.last.value),
        joinType = None,
        on = None,
        alias = a,
        parts = ps
      )
    }

  def join: PackratParser[Join] = opt(join_type) ~ Join.regex ~ (unnest | source) ~ opt(on) ^^ {
    case _ ~ _ ~ (u: Unnest) ~ _ =>
      u // Unnest cannot have a join type or an ON clause
    case jt ~ _ ~ (sj: StandardJoin) ~ o =>
      sj.copy(joinType = jt, on = o)
  }

  /** The FROM (and DELETE, and CTAS/MV/WATCHER body) table reference.
    *
    * `Table.name` is `parts.last.value` — STRUCTURALLY the last part, never an interpretation. The
    * qualifier the leading `rep` in `tableParts` matched is preserved in `parts` instead of being
    * discarded, which is what the `quotedSchemaPrefix` this replaces used to do (#85): the render
    * no longer deletes a clause the statement carried.
    */
  def table: PackratParser[Table] =
    (derivedTable ^^ { dt => Table(dt.name, None, Nil, Nil, derived = Some(dt)) } |
    tableParts ~ alias.? ^^ { case ps ~ a => Table(ps.last.value, a, Nil, parts = ps) }) ~
    rep(join) ^^ { case t ~ js => t.copy(joins = js) }

  /** `(SELECT …) [AS] alias` — a derived table (SQL-92 §7.6 `<derived table>`).
    *
    * DISJOINT from `tableParts` at the FIRST character: this production begins with the literal
    * `(`, while `tableParts` begins with a name character or a quote (`qualifiedName` =
    * `(quotedPart | bareFirstPart) ~ nameTail`). Neither can match a prefix of an input the other
    * accepts whole, so the alternation order narrows nothing either way — `derivedTable` is listed
    * first only because a literal test fails faster than a 131-alternative reserved-word regex.
    *
    * The ALIAS is mandatory (SQL-92 requires a `<correlation name>`; MySQL 8.4 raises error 1248;
    * 10 of the 11 captured BI statements carry one) and its absence is an `err`, never a `failure`:
    * a `failure` would fall through `rep1sep(table, separator)` / `rep(join)` and report a position
    * error naming neither the derived table nor the alias (the #213 mode). The emptiness test is on
    * `alias.alias`, not on the `Option`: `regexAlias`'s character class is `*`, so `alias` can
    * succeed with an EMPTY name and `alias.?` is not a reliable absence test.
    *
    * The body alternative carries its own `err` for a parenthesised non-SELECT (`FROM (t) x`, `FROM
    * (SHOW TABLES) x`): without it the rejection is `tableParts`' identifier-regex failure, a
    * grammar-internal message this project never pins. Raising an `err` this early in the input is
    * safe: an `err` is discarded only by a SIBLING `Failure` that got FURTHER (story 21.4), and the
    * only sibling here — `tableParts` — fails at the very same `(`.
    *
    * `alias.?` declines a following keyword by construction: `regexAliasRegex` carries the
    * reserved-word negative lookahead, so `… ) WHERE ROWNUM …` yields `None` here and the alias
    * `err` fires — which is how the Oracle corpus row gets OUR message rather than a lexer's.
    *
    * Recursion (`table -> derivedTable -> searchStatement -> single -> from -> table`) runs through
    * a CONSUMED `(`, so it is not left recursion; Packrat memoises it and nesting is unbounded.
    */
  override def derivedTable: PackratParser[DerivedTable] =
    (start ~> (derivedTableBodyInner | err(
      "A derived table body must be a SELECT: write FROM (SELECT ...) AS <name>"
    )) <~ end) ~ alias.? >> {
      case body ~ Some(a) if a.alias.nonEmpty => success(DerivedTable(body, a))
      case _ =>
        err(
          "A derived table requires an alias (SQL-92 correlation name): " +
          "write FROM (SELECT ...) AS <name> or JOIN (SELECT ...) AS <name> ON ..."
        )
    }

  def from: PackratParser[From] = From.regex ~ rep1sep(table, separator) ^^ { case _ ~ tables =>
    From(tables)
  }

}
