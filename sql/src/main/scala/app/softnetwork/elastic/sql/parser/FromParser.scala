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

import app.softnetwork.elastic.sql.{Alias, Identifier}
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

  def on: PackratParser[On] = On.regex ~> whereCriteria ^^ { rawTokens =>
    On(
      processTokens(rawTokens).getOrElse(throw new Exception("ON clause requires criteria"))
    )
  }

  def source: PackratParser[(Identifier, Option[Alias])] = identifier ~ alias.? ^^ { case i ~ a =>
    (i, a)
  }

  def join: PackratParser[Join] = opt(join_type) ~ Join.regex ~ (unnest | source) ~ opt(on) ^^ {
    case jt ~ _ ~ t ~ o =>
      t match {
        case u: Unnest =>
          u // Unnest cannot have a join type or an ON clause
        case (i: Identifier, a: Option[Alias]) =>
          StandardJoin(
            source = i,
            joinType = jt,
            on = o,
            alias = a
          )
      }
  }

  def table: PackratParser[Table] = identifierRegex ~ alias.? ~ rep(join) ^^ { case i ~ a ~ js =>
    Table(i, a, js)
  }

  def from: PackratParser[From] = From.regex ~ rep1sep(table, separator) ^^ { case _ ~ tables =>
    From(tables)
  }

}
