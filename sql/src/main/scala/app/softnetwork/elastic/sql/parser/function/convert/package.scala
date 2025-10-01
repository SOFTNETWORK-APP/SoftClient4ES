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
