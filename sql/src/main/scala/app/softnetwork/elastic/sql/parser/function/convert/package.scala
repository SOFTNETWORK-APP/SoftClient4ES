package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.function.convert.Cast
import app.softnetwork.elastic.sql.{Alias, Identifier}
import app.softnetwork.elastic.sql.parser.Parser

package object convert {

  trait ConvertParser { self: Parser =>

    def castFunctionWithIdentifier: PackratParser[Identifier] =
      "(?i)cast".r ~ start ~ (identifierWithTransformation |
      identifierWithSystemFunction |
      identifierWithIntervalFunction |
      identifierWithFunction |
      date_diff_identifier |
      last_day_identifier |
      extract_identifier |
      identifier) ~ Alias.regex.? ~ sql_type ~ end ~ intervalFunction.? ^^ {
        case _ ~ _ ~ i ~ as ~ t ~ _ ~ a =>
          i.withFunctions(a.toList ++ (Cast(i, targetType = t, as = as.isDefined) +: i.functions))
      }

  }
}
