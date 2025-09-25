package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.function.TransformFunction
import app.softnetwork.elastic.sql.function.cond.{
  Case,
  Coalesce,
  ConditionalFunction,
  ELSE,
  END,
  IsNotNull,
  IsNull,
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

    def start_case: PackratParser[StartCase.type] = Case.regex ^^ (_ => StartCase)

    def when_case: PackratParser[WhenCase.type] = WHEN.regex ^^ (_ => WhenCase)

    def then_case: PackratParser[ThenCase.type] = THEN.regex ^^ (_ => ThenCase)

    def else_case: PackratParser[ELSE.type] = ELSE.regex ^^ (_ => ELSE)

    def end_case: PackratParser[EndCase.type] = END.regex ^^ (_ => EndCase)

    def case_condition: Parser[(PainlessScript, PainlessScript)] =
      when_case ~ (whereCriteria | valueExpr) ~ then_case.? ~ valueExpr ^^ { case _ ~ c ~ _ ~ r =>
        c match {
          case p: PainlessScript => p -> r
          case rawTokens: List[Token] =>
            processTokens(rawTokens) match {
              case Some(criteria) => criteria -> r
              case _              => Null     -> r
            }
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

    def conditional_functions: PackratParser[TransformFunction[_, _]] =
      is_null | is_notnull | coalesce | nullif | case_when

    def conditionalFunctionWithIdentifier: PackratParser[Identifier] =
      (is_null | is_notnull | coalesce | nullif) ^^ { t =>
        t.identifier.withFunctions(t +: t.identifier.functions)
      }

  }
}
