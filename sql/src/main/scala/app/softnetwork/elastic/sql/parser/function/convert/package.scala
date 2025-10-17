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

package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.function.convert.{Cast, CastOperator, Convert, TryCast}
import app.softnetwork.elastic.sql.{Alias, Identifier}
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

    def cast: Identifier => PackratParser[Identifier] = i =>
      (CastOperator.regex ~ sql_type).? ^^ {
        case None => i
        case Some(_ ~ t) =>
          i.withFunctions(CastOperator(i, targetType = t) +: i.functions)
      }

    def conversionFunctionWithIdentifier: PackratParser[Identifier] =
      (cast_identifier | try_cast_identifier | convert_identifier) ~ rep(
        intervalFunction
      ) ^^ { case id ~ funcs =>
        id.withFunctions(funcs ++ id.functions)
      }

  }
}
