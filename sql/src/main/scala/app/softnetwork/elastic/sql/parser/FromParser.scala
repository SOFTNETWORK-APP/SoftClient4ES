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
    tableParts ~ alias.? ~ rep(join) ^^ { case ps ~ a ~ js =>
      Table(ps.last.value, a, js, parts = ps)
    }

  def from: PackratParser[From] = From.regex ~ rep1sep(table, separator) ^^ { case _ ~ tables =>
    From(tables)
  }

}
