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

package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.function.convert.{Cast, CastOperator, Convert, TryCast}
import app.softnetwork.elastic.sql.{Alias, Identifier}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.parser.Parser

package object convert {

  trait ConvertParser { self: Parser =>

    def cast_identifier: PackratParser[Identifier] =
      Cast.regex ~ start ~ (identifierWithTransformation |
      identifierWithIntervalFunction |
      identifierWithFunction |
      identifier) ~ Alias.regex.? ~ sql_type ~ end ^^ { case _ ~ _ ~ i ~ as ~ t ~ _ =>
        i.withFunctions(Cast(i, targetType = t, as = as.isDefined) +: i.functions)
      }

    def try_cast_identifier: PackratParser[Identifier] =
      TryCast.regex ~ start ~ (identifierWithTransformation |
      identifierWithIntervalFunction |
      identifierWithFunction |
      identifier) ~ Alias.regex.? ~ sql_type ~ end ^^ { case _ ~ _ ~ i ~ as ~ t ~ _ =>
        i.withFunctions(
          Cast(i, targetType = t, as = as.isDefined, safe = true) +: i.functions
        )
      }

    def convert_identifier: PackratParser[Identifier] =
      Convert.regex ~ start ~ (identifierWithTransformation |
      identifierWithIntervalFunction |
      identifierWithFunction |
      identifier) ~ separator ~ sql_type ~ end ^^ { case _ ~ _ ~ i ~ _ ~ t ~ _ =>
        i.withFunctions(Convert(i, targetType = t) +: i.functions)
      }

    /** MySQL's `CONVERT(expr USING <charset>)`.
      *
      * Elasticsearch stores UTF-8 throughout, so a charset conversion is a no-op; the charset name
      * is parsed and DISCARDED, and the statement renders as `CONVERT(expr, VARCHAR)` so the drop
      * is visible in every artifact that echoes it.
      *
      * Registered AFTER `convert_identifier`, which fails at its `separator` on this shape and lets
      * `|` move on: the non-backtracking rule bites only INSIDE a committed alternation, never
      * across sibling alternatives of the outer `|`.
      *
      * The charset is `(ident | literal)` because MySQL accepts BOTH `USING utf8` and `USING
      * 'utf8'`. Taking only the bare form would half-support a spelling the lead confirmed we keep
      * (OQ-3), and `ident` cannot express a quoted one.
      */
    def convert_using_identifier: PackratParser[Identifier] =
      Convert.regex ~ start ~ (identifierWithTransformation |
      identifierWithIntervalFunction |
      identifierWithFunction |
      identifier) ~ keyword("USING") ~ (ident | literal) ~ end ^^ { case _ ~ _ ~ i ~ _ ~ _ ~ _ =>
        i.withFunctions(Convert(i, targetType = SQLTypes.Varchar) +: i.functions)
      }

    def convert_transact_sql_identifier: PackratParser[Identifier] =
      Convert.regex ~ start ~> sql_type ~ separator ~ (identifierWithTransformation |
      identifierWithIntervalFunction |
      identifierWithFunction |
      identifier) <~ end ^^ { case t ~ _ ~ i =>
        i.withFunctions(Convert(i, targetType = t, transactSql = true) +: i.functions)
      }

    def cast: Identifier => PackratParser[Identifier] = i =>
      (CastOperator.regex ~ sql_type).? ^^ {
        case None => i
        case Some(_ ~ t) =>
          i.withFunctions(CastOperator(i, targetType = t) +: i.functions)
      }

    def conversionFunctionWithIdentifier: PackratParser[Identifier] =
      (cast_identifier |
      try_cast_identifier |
      convert_identifier |
      convert_using_identifier |
      convert_transact_sql_identifier) ~ rep(
        intervalFunction
      ) ^^ { case id ~ funcs =>
        id.withFunctions(funcs ++ id.functions)
      }

  }
}
