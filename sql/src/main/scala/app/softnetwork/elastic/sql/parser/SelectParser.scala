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

import app.softnetwork.elastic.sql.query.{Except, Field, Limit, Select, Top}

trait SelectParser {
  self: Parser with WhereParser =>

  lazy val field: PackratParser[Field] =
    // #284: decline the quoted lexeme when an arithmetic operator follows, so
    // `SELECT `amount` + 1` reaches identifierWithArithmeticExpression below.
    (quotedIdentifierUnlessArithmetic |
    identifierWithArithmeticExpression |
    identifierWithTransformation |
    identifierWithWindowFunction |
    identifierWithAggregation |
    identifierWithIntervalFunction |
    identifierWithFunction |
    identifier) ~ (quotedAlias | alias).? ^^ { case i ~ a =>
      Field(i, a)
    }

  lazy val except: PackratParser[Except] =
    Except.regex ~ start ~ rep1sep(field, separator) ~ end ^^ { case _ ~ _ ~ e ~ _ =>
      Except(e)
    }

  /** `TOP n` — T-SQL's row bound, which Tableau emits in its SQL-92 dialect. It is returned
    * ALONGSIDE the `Select` rather than stored on it, so the statement keeps exactly one owner of
    * its row bound (`Parser.single` folds it into `limit`) and the render normalises to `LIMIT n`.
    *
    * 🔴 `TOP` is NOT reserved, and must not become so: `SELECT top FROM t` selects a column named
    * `top` today. `top.?` is safe because `Top.regex ~> long` FAILS (it does not error) when no
    * number follows, and `opt` backtracks to the original position, where `field` reads `top` as
    * the identifier it is. Both readings are pinned in `ParserSpec`.
    */
  lazy val top: PackratParser[Limit] =
    Top.regex ~> (start ~> long <~ end | long) >> { l =>
      if (l.value < 0 || l.value > Int.MaxValue) {
        // 🔴 `failure`, NEVER `err`, and the distinction is the whole correctness of `top.?`.
        // `err` yields an `Error`, and `Parsers.|` does not try another alternative after an
        // Error — so `opt` could not backtrack and `SELECT top -1 AS x FROM t`, which reads
        // `top - 1` and parses on main, became a hard rejection. At this position `TOP` is
        // genuinely ambiguous between the clause and a column called `top`; a `Failure` lets
        // `field` settle it, which is the only reading that cannot regress.
        failure(s"TOP takes a row count between 0 and ${Int.MaxValue}")
      } else {
        // `TOP n PERCENT` and `TOP n WITH TIES` are real T-SQL that this engine does not
        // implement. They are refused BY NAME because the alternative is far worse: with `top`
        // having consumed `TOP 5`, `field` reads `PERCENT a` as the column `PERCENT` aliased to
        // `a`, so `SELECT TOP 5 PERCENT a FROM t` returned rows for a column the user never
        // named — a loud rejection turned into a silent wrong answer (#205/#253 family).
        // An `err` is safe HERE, unlike above: no spelling of `TOP <n> PERCENT` parsed before.
        (keyword("PERCENT") | (keyword("WITH") ~ keyword("TIES") ^^ (_ => "WITH TIES"))) >> { w =>
          err(
            s"SELECT TOP n $w is not supported -- TOP takes a row COUNT. Use TOP n, or LIMIT n"
          )
        } | success(Limit(l.value.toInt, None))
      }
    }

  lazy val select: PackratParser[(Select, Option[Limit])] =
    Select.regex ~ top.? ~ rep1sep(
      field,
      separator
    ) ~ except.? ^^ { case _ ~ t ~ fields ~ e =>
      (Select(fields, e), t)
    }

}
