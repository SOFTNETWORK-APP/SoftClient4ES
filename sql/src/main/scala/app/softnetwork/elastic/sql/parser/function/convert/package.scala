package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.function.convert.{Cast, CastOperator, Convert, TryCast}
import app.softnetwork.elastic.sql.{Alias, Identifier}
import app.softnetwork.elastic.sql.parser.Parser

package object convert {

  trait ConvertParser { self: Parser =>

    def castFunctionWithIdentifier: PackratParser[Identifier] =
      Cast.regex ~ start ~ (identifierWithTransformation |
      identifierWithIntervalFunction |
      identifierWithFunction |
      identifier) ~ Alias.regex.? ~ sql_type ~ end ~ intervalFunction.? ^^ {
        case _ ~ _ ~ i ~ as ~ t ~ _ ~ a =>
          i.withFunctions(a.toList ++ (Cast(i, targetType = t, as = as.isDefined) +: i.functions))
      }

    def tryCastFunctionWithIdentifier: PackratParser[Identifier] =
      TryCast.regex ~ start ~ (identifierWithTransformation |
      identifierWithIntervalFunction |
      identifierWithFunction |
      identifier) ~ Alias.regex.? ~ sql_type ~ end ~ intervalFunction.? ^^ {
        case _ ~ _ ~ i ~ as ~ t ~ _ ~ a =>
          i.withFunctions(
            a.toList ++ (Cast(i, targetType = t, as = as.isDefined, safe = true) +: i.functions)
          )
      }

    def castOperator: Identifier => PackratParser[Identifier] = i =>
      (CastOperator.regex ~ sql_type).? ^^ {
        case None => i
        case Some(_ ~ t) =>
          i.withFunctions(CastOperator(i, targetType = t) +: i.functions)
      }

    def convertFunctionWithIdentifier: PackratParser[Identifier] =
      Convert.regex ~ start ~ (identifierWithTransformation |
      identifierWithIntervalFunction |
      identifierWithFunction |
      identifier) ~ separator ~ sql_type ~ end ~ intervalFunction.? ^^ {
        case _ ~ _ ~ i ~ _ ~ t ~ _ ~ a =>
          i.withFunctions(a.toList ++ (Convert(i, targetType = t) +: i.functions))
      }

    def conversionFunctionWithIdentifier: PackratParser[Identifier] =
      castFunctionWithIdentifier | tryCastFunctionWithIdentifier | convertFunctionWithIdentifier

  }
}
