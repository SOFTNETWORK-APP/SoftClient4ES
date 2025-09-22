package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.Identifier
import app.softnetwork.elastic.sql.`type`.{SQLBigInt, SQLVarchar}
import app.softnetwork.elastic.sql.function.string.{
  Concat,
  Length,
  Lower,
  SQLLength,
  StringFunction,
  StringFunctionWithOp,
  Substring,
  To,
  Trim,
  Upper
}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.From

package object string {

  trait StringParser { self: Parser =>

    def concatFunction: PackratParser[StringFunction[SQLVarchar]] =
      Concat.regex ~ start ~ rep1sep(valueExpr, separator) ~ end ^^ { case _ ~ _ ~ vs ~ _ =>
        Concat(vs)
      }

    def substringFunction: PackratParser[StringFunction[SQLVarchar]] =
      Substring.regex ~ start ~ valueExpr ~ (From.regex | separator) ~ long ~ ((To.regex | separator) ~ long).? ~ end ^^ {
        case _ ~ _ ~ v ~ _ ~ s ~ eOpt ~ _ =>
          Substring(v, s.value.toInt, eOpt.map { case _ ~ e => e.value.toInt })
      }

    def stringFunctionWithIdentifier: PackratParser[Identifier] =
      (concatFunction | substringFunction) ^^ { sf =>
        sf.identifier
      }

    def length: PackratParser[StringFunction[SQLBigInt]] =
      Length.regex ^^ { _ =>
        SQLLength
      }

    def lower: PackratParser[StringFunction[SQLVarchar]] =
      Lower.regex ^^ { _ =>
        StringFunctionWithOp(Lower)
      }

    def upper: PackratParser[StringFunction[SQLVarchar]] =
      Upper.regex ^^ { _ =>
        StringFunctionWithOp(Upper)
      }

    def trim: PackratParser[StringFunction[SQLVarchar]] =
      Trim.regex ^^ { _ =>
        StringFunctionWithOp(Trim)
      }

    def string_functions: Parser[
      StringFunction[_]
    ] = /*concatFunction | substringFunction |*/ length | lower | upper | trim

  }
}
