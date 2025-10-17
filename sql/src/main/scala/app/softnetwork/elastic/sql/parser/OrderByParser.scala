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

import app.softnetwork.elastic.sql.function.Function
import app.softnetwork.elastic.sql.query.{Asc, Desc, FieldSort, OrderBy}

trait OrderByParser {
  self: Parser =>

  def asc: PackratParser[Asc.type] = Asc.regex ^^ (_ => Asc)

  def desc: PackratParser[Desc.type] = Desc.regex ^^ (_ => Desc)

  private def fieldName: PackratParser[String] =
    """\b(?!(?i)limit\b)[a-zA-Z_][a-zA-Z0-9_]*""".r ^^ (f => f)

  def fieldWithFunction: PackratParser[(String, List[Function])] =
    rep1sep(sql_function, start) ~ start.? ~ fieldName ~ rep1(end) ^^ { case f ~ _ ~ n ~ _ =>
      (n, f)
    }

  def sort: PackratParser[FieldSort] =
    (fieldWithFunction | fieldName) ~ (asc | desc).? ^^ { case f ~ o =>
      f match {
        case i: (String, List[Function]) => FieldSort(i._1, o, i._2)
        case s: String                   => FieldSort(s, o, List.empty)
      }
    }

  def orderBy: PackratParser[OrderBy] = OrderBy.regex ~ rep1sep(sort, separator) ^^ { case _ ~ s =>
    OrderBy(s)
  }

}
