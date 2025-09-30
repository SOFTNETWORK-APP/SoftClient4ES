package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.Identifier
import app.softnetwork.elastic.sql.`type`.{SQLBigInt, SQLBool, SQLVarchar}
import app.softnetwork.elastic.sql.function.string._
import app.softnetwork.elastic.sql.operator.IN
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.From

package object string {

  trait StringParser { self: Parser =>

    def concat: PackratParser[StringFunction[SQLVarchar]] =
      Concat.regex ~ start ~ rep1sep(valueExpr, separator) ~ end ^^ { case _ ~ _ ~ vs ~ _ =>
        Concat(vs)
      }

    def substr: PackratParser[StringFunction[SQLVarchar]] =
      Substring.regex ~ start ~ valueExpr ~ (From.regex | separator) ~ long ~ ((For.regex | separator) ~ long).? ~ end ^^ {
        case _ ~ _ ~ v ~ _ ~ s ~ eOpt ~ _ =>
          Substring(v, s.value.toInt, eOpt.map { case _ ~ e => e.value.toInt })
      }

    def left: PackratParser[StringFunction[SQLVarchar]] =
      LeftOp.regex ~ start ~ valueExpr ~ (For.regex | separator) ~ long ~ end ^^ {
        case _ ~ _ ~ v ~ _ ~ l ~ _ =>
          LeftFunction(v, l.value.toInt)
      }

    def right: PackratParser[StringFunction[SQLVarchar]] =
      RightOp.regex ~ start ~ valueExpr ~ (For.regex | separator) ~ long ~ end ^^ {
        case _ ~ _ ~ v ~ _ ~ l ~ _ =>
          RightFunction(v, l.value.toInt)
      }

    def replace: PackratParser[StringFunction[SQLVarchar]] =
      Replace.regex ~ start ~ valueExpr ~ separator ~ valueExpr ~ separator ~ valueExpr ~ end ^^ {
        case _ ~ _ ~ v ~ _ ~ f ~ _ ~ r ~ _ =>
          Replace(v, f, r)
      }

    def reverse: PackratParser[StringFunction[SQLVarchar]] =
      Reverse.regex ~ start ~ valueExpr ~ end ^^ { case _ ~ _ ~ v ~ _ =>
        Reverse(v)
      }

    def position: PackratParser[StringFunction[SQLBigInt]] =
      Position.regex ~ start ~ valueExpr ~ (separator | IN.regex) ~ valueExpr ~ ((separator | From.regex) ~ long).? ~ end ^^ {
        case _ ~ _ ~ sub ~ _ ~ str ~ from ~ _ =>
          Position(sub, str, from.map { case _ ~ f => f.value.toInt }.getOrElse(1))
      }

    def regexp: PackratParser[StringFunction[SQLBool]] =
      RegexpLike.regex ~ start ~ valueExpr ~ separator ~ valueExpr ~ (separator ~ literal).? ~ end ^^ {
        case _ ~ _ ~ str ~ _ ~ pattern ~ flags ~ _ =>
          RegexpLike(
            str,
            pattern,
            flags match {
              case Some(_ ~ f) => Some(MatchFlags(f.value))
              case _           => None
            }
          )
      }

    def stringFunctionWithIdentifier: PackratParser[Identifier] =
      (concat | substr | left | right | replace | reverse | position | regexp) ^^ { sf =>
        sf.identifier
      }

    def length: PackratParser[StringFunction[SQLBigInt]] =
      Length.regex ^^ { _ =>
        Length()
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

    def ltrim: PackratParser[StringFunction[SQLVarchar]] =
      Ltrim.regex ^^ { _ =>
        StringFunctionWithOp(Ltrim)
      }

    def rtrim: PackratParser[StringFunction[SQLVarchar]] =
      Rtrim.regex ^^ { _ =>
        StringFunctionWithOp(Rtrim)
      }

    def string_functions: Parser[
      StringFunction[_]
    ] = /*concatFunction | substringFunction |*/ length | lower | upper | trim | ltrim | rtrim

  }
}
