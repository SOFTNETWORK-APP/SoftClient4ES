/*
 * Copyright 2015 SOFTNETWORK
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

import app.softnetwork.elastic.sql.query.{Except, Field, Select}

trait SelectParser {
  self: Parser with WhereParser =>

  def field: PackratParser[Field] =
    (identifierWithTopHits |
    identifierWithArithmeticExpression |
    identifierWithTransformation |
    identifierWithAggregation |
    identifierWithIntervalFunction |
    identifierWithFunction |
    identifier) ~ alias.? ^^ { case i ~ a =>
      Field(i, a)
    }

  def except: PackratParser[Except] = Except.regex ~ start ~ rep1sep(field, separator) ~ end ^^ {
    case _ ~ _ ~ e ~ _ =>
      Except(e)
  }

  def select: PackratParser[Select] =
    Select.regex ~ rep1sep(
      field,
      separator
    ) ~ except.? ^^ { case _ ~ fields ~ e =>
      Select(fields, e)
    }

}
