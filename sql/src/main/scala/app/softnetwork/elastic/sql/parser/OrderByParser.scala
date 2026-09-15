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

import app.softnetwork.elastic.sql.Identifier
import app.softnetwork.elastic.sql.function.Function
import app.softnetwork.elastic.sql.query.{
  Asc,
  Desc,
  FieldSort,
  NullOrdering,
  NullsFirst,
  NullsLast,
  OrderBy
}

trait OrderByParser {
  self: Parser =>

  lazy val asc: PackratParser[Asc.type] = Asc.regex ^^ (_ => Asc)

  lazy val desc: PackratParser[Desc.type] = Desc.regex ^^ (_ => Desc)

  lazy val nullsFirst: PackratParser[NullsFirst.type] = NullsFirst.regex ^^ (_ => NullsFirst)

  lazy val nullsLast: PackratParser[NullsLast.type] = NullsLast.regex ^^ (_ => NullsLast)

  lazy val nullOrdering: PackratParser[NullOrdering] = nullsFirst | nullsLast

  private lazy val fieldName: PackratParser[String] =
    """\b(?!(?i)limit\b)[a-zA-Z_][a-zA-Z0-9_]*""".r ^^ (f => f)

  lazy val fieldWithFunction: PackratParser[Identifier] =
    // #284 - see quotedIdentifierUnlessArithmetic.
    quotedIdentifierUnlessArithmetic |
    identifierWithArithmeticExpression |
    identifierWithTransformation |
    identifierWithWindowFunction |
    identifierWithAggregation |
    identifierWithIntervalFunction |
    identifierWithFunction |
    identifier

  lazy val sort: PackratParser[FieldSort] =
    fieldWithFunction ~ (asc | desc).? ~ nullOrdering.? ^^ { case f ~ o ~ n =>
      FieldSort(f, o, n)
    }

  lazy val orderBy: PackratParser[OrderBy] = OrderBy.regex ~ rep1sep(sort, separator) ^^ {
    case _ ~ s =>
      OrderBy(s)
  }

}
