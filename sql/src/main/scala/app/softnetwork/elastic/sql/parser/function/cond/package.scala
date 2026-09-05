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

import app.softnetwork.elastic.sql.function.{FunctionWithIdentifier, TransformFunction}
import app.softnetwork.elastic.sql.function.cond.{
  Case,
  Coalesce,
  ConditionalFunction,
  ELSE,
  END,
  Greatest,
  IsNotNull,
  IsNull,
  Least,
  NullIf,
  THEN,
  WHEN
}
import app.softnetwork.elastic.sql.{Identifier, Null, PainlessScript, Token}
import app.softnetwork.elastic.sql.parser.{
  EndCase,
  Parser,
  StartCase,
  ThenCase,
  WhenCase,
  WhereParser
}

package object cond {

  trait CondParser { self: Parser with WhereParser =>

    def is_null: PackratParser[ConditionalFunction[_]] =
      "(?i)isnull".r ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ end ^^ {
        case _ ~ _ ~ i ~ _ => IsNull(i)
      }

    def is_notnull: PackratParser[ConditionalFunction[_]] =
      "(?i)isnotnull".r ~ start ~ (identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction | identifier) ~ end ^^ {
        case _ ~ _ ~ i ~ _ => IsNotNull(i)
      }

    def coalesce: PackratParser[Coalesce] =
      Coalesce.regex ~ start ~ rep1sep(
        valueExpr,
        separator
      ) ~ end ^^ { case _ ~ _ ~ ids ~ _ =>
        Coalesce(ids)
      }

    def nullif: PackratParser[NullIf] =
      NullIf.regex ~ start ~ valueExpr ~ separator ~ valueExpr ~ end ^^ {
        case _ ~ _ ~ id1 ~ _ ~ id2 ~ _ => NullIf(id1, id2)
      }

    def greatest: PackratParser[Greatest] =
      Greatest.regex ~ start ~ rep1sep(valueExpr, separator) ~ end ^^ { case _ ~ _ ~ vs ~ _ =>
        Greatest(vs)
      }

    def least: PackratParser[Least] =
      Least.regex ~ start ~ rep1sep(valueExpr, separator) ~ end ^^ { case _ ~ _ ~ vs ~ _ =>
        Least(vs)
      }

    def start_case: PackratParser[StartCase.type] = Case.regex ^^ (_ => StartCase)

    def when_case: PackratParser[WhenCase.type] = WHEN.regex ^^ (_ => WhenCase)

    def then_case: PackratParser[ThenCase.type] = THEN.regex ^^ (_ => ThenCase)

    def else_case: PackratParser[ELSE.type] = ELSE.regex ^^ (_ => ELSE)

    def end_case: PackratParser[EndCase.type] = END.regex ^^ (_ => EndCase)

    def case_condition: Parser[(PainlessScript, PainlessScript)] =
      when_case ~ (whereCriteria | valueExpr) ~ then_case.? ~ valueExpr >> { case _ ~ c ~ _ ~ r =>
        c match {
          case p: PainlessScript => success(p -> r)
          case rawTokens: List[Token] =>
            processTokens(rawTokens) match {
              case Right(Some(criteria)) => success(criteria -> r)
              // UNCHANGED behaviour: an empty WHEN legitimately means `Null` here, so this arm is
              // deliberately NOT the `err` that `where` / `having` now emit for the same shape.
              case Right(None)  => success(Null -> r)
              case Left(reason) => err(reason)
            }
          // #250 - this match had NO default arm. A MatchError escapes `Parser.apply` exactly like
          // a `throw`, and the ParserTotalitySpec source scan cannot see it (there is no `throw`
          // token), so it would land in the boundary catch as an internal parser error. The 20.9
          // `searchAs` MatchError is the recorded precedent for this shape.
          case other => err(s"Unsupported WHEN expression: ${other.getClass.getSimpleName}")
        }
      }

    def case_else: Parser[PainlessScript] = else_case ~ valueExpr ^^ { case _ ~ r => r }

    def case_when: PackratParser[Case] =
      start_case ~ valueExpr.? ~ rep1(case_condition) ~ case_else.? ~ end_case ^^ {
        case _ ~ e ~ c ~ r ~ _ => Case(e, c, r)
      }

    def case_when_identifier: Parser[Identifier] = case_when ^^ { cw =>
      Identifier(cw)
    }

    def conditional_function: PackratParser[FunctionWithIdentifier] =
      is_null | is_notnull | coalesce | nullif | greatest | least

    def conditionalFunctionWithIdentifier: PackratParser[Identifier] =
      conditional_function ^^ { t =>
        t.identifier.withFunctions(t +: t.identifier.functions)
      } | case_when_identifier

  }
}
