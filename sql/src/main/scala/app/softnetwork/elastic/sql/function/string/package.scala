package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.{Expr, Identifier, IntValue, PainlessScript, TokenRegex}
import app.softnetwork.elastic.sql.`type`.{
  SQLBigInt,
  SQLBool,
  SQLType,
  SQLTypeUtils,
  SQLTypes,
  SQLVarchar
}

package object string {

  sealed trait StringOp extends PainlessScript with TokenRegex {
    override def painless: String = s".${sql.toLowerCase()}()"
  }

  case object Concat extends Expr("CONCAT") with StringOp {
    override def painless: String = " + "
  }
  case object Pipe extends Expr("\\|\\|") with StringOp {
    override def painless: String = " + "
  }
  case object Lower extends Expr("LOWER") with StringOp {
    override lazy val words: List[String] = List(sql, "LCASE")
  }
  case object Upper extends Expr("UPPER") with StringOp {
    override lazy val words: List[String] = List(sql, "UCASE")
  }
  case object Trim extends Expr("TRIM") with StringOp
  case object Ltrim extends Expr("LTRIM") with StringOp {
    override def painless: String = ".replaceAll(\"^\\\\s+\",\"\")"
  }
  case object Rtrim extends Expr("RTRIM") with StringOp {
    override def painless: String = ".replaceAll(\"\\\\s+$\",\"\")"
  }
  case object Substring extends Expr("SUBSTRING") with StringOp {
    override def painless: String = ".substring"
    override lazy val words: List[String] = List(sql, "SUBSTR")
  }
  case object LeftOp extends Expr("LEFT") with StringOp
  case object RightOp extends Expr("RIGHT") with StringOp
  case object For extends Expr("FOR") with TokenRegex
  case object Length extends Expr("LENGTH") with StringOp {
    override lazy val words: List[String] = List(sql, "LEN")
  }
  case object Replace extends Expr("REPLACE") with StringOp {
    override lazy val words: List[String] = List(sql, "STR_REPLACE")
    override def painless: String = ".replace"
  }
  case object Reverse extends Expr("REVERSE") with StringOp
  case object Position extends Expr("POSITION") with StringOp {
    override lazy val words: List[String] = List(sql, "STRPOS")
    override def painless: String = ".indexOf"
  }

  case object RegexpLike extends Expr("REGEXP_LIKE") with StringOp {
    override lazy val words: List[String] = List(sql, "REGEXP")
    override def painless: String = ".matches"
  }

  case class MatchFlags(flags: String) extends PainlessScript {
    override def sql: String = s"'$flags'"
    override def painless: String = flags.toCharArray
      .map {
        case 'i' => "java.util.regex.Pattern.CASE_INSENSITIVE"
        case 'c' => "0"
        case 'n' => "java.util.regex.Pattern.DOTALL"
        case 'm' => "java.util.regex.Pattern.MULTILINE"
        case _   => ""
      }
      .filter(_.nonEmpty)
      .mkString(" | ") match {
      case "" => "0"
      case s  => s
    }

    override def nullable: Boolean = false
  }

  sealed trait StringFunction[Out <: SQLType]
      extends TransformFunction[SQLVarchar, Out]
      with FunctionWithIdentifier {
    override def inputType: SQLVarchar = SQLTypes.Varchar

    override def outputType: Out

    def stringOp: StringOp

    override def fun: Option[PainlessScript] = Some(stringOp)

    override def identifier: Identifier = Identifier(this)

    override def toSQL(base: String): String = s"$sql($base)"

    override def sql: String =
      if (args.isEmpty)
        s"${fun.map(_.sql).getOrElse("")}"
      else
        super.sql
  }

  case class StringFunctionWithOp(stringOp: StringOp) extends StringFunction[SQLVarchar] {
    override def outputType: SQLVarchar = SQLTypes.Varchar
    override def args: List[PainlessScript] = List.empty
  }

  case class Substring(str: PainlessScript, start: Int, length: Option[Int])
      extends StringFunction[SQLVarchar] {
    override def outputType: SQLVarchar = SQLTypes.Varchar
    override def stringOp: StringOp = Substring

    override def args: List[PainlessScript] =
      List(str, IntValue(start)) ++ length.map(l => IntValue(l)).toList

    override def nullable: Boolean = str.nullable

    override def toPainlessCall(callArgs: List[String]): String = {
      callArgs match {
        // SUBSTRING(expr, start, length)
        case List(arg0, arg1, arg2) =>
          s"$arg0.substring($arg1 - 1, Math.min($arg1 - 1 + $arg2, $arg0.length()))"

        // SUBSTRING(expr, start)
        case List(arg0, arg1) =>
          s"$arg0.substring(Math.min($arg1 - 1, $arg0.length() - 1))"

        case _ => throw new IllegalArgumentException("SUBSTRING requires 2 or 3 arguments")
      }
    }

    override def validate(): Either[String, Unit] =
      if (start < 1)
        Left("SUBSTRING start position must be greater than or equal to 1 (SQL is 1-based)")
      else if (length.exists(_ < 0))
        Left("SUBSTRING length must be non-negative")
      else
        str.validate()

    override def toSQL(base: String): String = sql

  }

  case class Concat(values: List[PainlessScript]) extends StringFunction[SQLVarchar] {
    override def outputType: SQLVarchar = SQLTypes.Varchar
    override def stringOp: StringOp = Concat

    override def args: List[PainlessScript] = values

    override def nullable: Boolean = values.exists(_.nullable)

    override def toPainlessCall(callArgs: List[String]): String = {
      if (callArgs.isEmpty)
        throw new IllegalArgumentException("CONCAT requires at least one argument")
      else
        callArgs.zipWithIndex
          .map { case (arg, idx) =>
            SQLTypeUtils.coerce(arg, values(idx).baseType, SQLTypes.Varchar, nullable = false)
          }
          .mkString(stringOp.painless)
    }

    override def validate(): Either[String, Unit] =
      if (values.isEmpty) Left("CONCAT requires at least one argument")
      else
        values.map(_.validate()).filter(_.isLeft) match {
          case Nil    => Right(())
          case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
        }

    override def toSQL(base: String): String = sql
  }

  case class Length() extends StringFunction[SQLBigInt] {
    override def outputType: SQLBigInt = SQLTypes.BigInt
    override def stringOp: StringOp = Length
    override def args: List[PainlessScript] = List.empty
  }

  case class LeftFunction(str: PainlessScript, length: Int) extends StringFunction[SQLVarchar] {
    override def outputType: SQLVarchar = SQLTypes.Varchar
    override def stringOp: StringOp = LeftOp

    override def args: List[PainlessScript] = List(str, IntValue(length))

    override def nullable: Boolean = str.nullable

    override def toPainlessCall(callArgs: List[String]): String = {
      callArgs match {
        case List(arg0, arg1) =>
          s"$arg0.substring(0, Math.min($arg1, $arg0.length()))"
        case _ => throw new IllegalArgumentException("LEFT requires 2 arguments")
      }
    }

    override def validate(): Either[String, Unit] =
      if (length < 0)
        Left("LEFT length must be non-negative")
      else
        str.validate()

    override def toSQL(base: String): String = sql
  }

  case class RightFunction(str: PainlessScript, length: Int) extends StringFunction[SQLVarchar] {
    override def outputType: SQLVarchar = SQLTypes.Varchar
    override def stringOp: StringOp = RightOp

    override def args: List[PainlessScript] = List(str, IntValue(length))

    override def nullable: Boolean = str.nullable

    override def toPainlessCall(callArgs: List[String]): String = {
      callArgs match {
        case List(arg0, arg1) =>
          s"""$arg1 == 0 ? "" : $arg0.substring($arg0.length() - Math.min($arg1, $arg0.length()))"""
        case _ => throw new IllegalArgumentException("RIGHT requires 2 arguments")
      }
    }

    override def validate(): Either[String, Unit] =
      if (length < 0)
        Left("RIGHT length must be non-negative")
      else
        str.validate()

    override def toSQL(base: String): String = sql
  }

  case class Replace(str: PainlessScript, search: PainlessScript, replace: PainlessScript)
      extends StringFunction[SQLVarchar] {
    override def outputType: SQLVarchar = SQLTypes.Varchar
    override def stringOp: StringOp = Replace

    override def args: List[PainlessScript] = List(str, search, replace)

    override def nullable: Boolean = str.nullable || search.nullable || replace.nullable

    override def toPainlessCall(callArgs: List[String]): String = {
      callArgs match {
        case List(arg0, arg1, arg2) =>
          s"$arg0.replace($arg1, $arg2)"
        case _ => throw new IllegalArgumentException("REPLACE requires 3 arguments")
      }
    }

    override def validate(): Either[String, Unit] =
      args.map(_.validate()).filter(_.isLeft) match {
        case Nil    => Right(())
        case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
      }

    override def toSQL(base: String): String = sql
  }

  case class Reverse(str: PainlessScript) extends StringFunction[SQLVarchar] {
    override def outputType: SQLVarchar = SQLTypes.Varchar
    override def stringOp: StringOp = Reverse

    override def args: List[PainlessScript] = List(str)

    override def nullable: Boolean = str.nullable

    override def toPainlessCall(callArgs: List[String]): String = {
      callArgs match {
        case List(arg0) => s"new StringBuilder($arg0).reverse().toString()"
        case _          => throw new IllegalArgumentException("REVERSE requires 1 argument")
      }
    }

    override def validate(): Either[String, Unit] =
      str.validate()

    override def toSQL(base: String): String = sql
  }

  case class Position(substr: PainlessScript, str: PainlessScript, start: Int)
      extends StringFunction[SQLBigInt] {
    override def outputType: SQLBigInt = SQLTypes.BigInt
    override def stringOp: StringOp = Position

    override def args: List[PainlessScript] = List(substr, str, IntValue(start))

    override def nullable: Boolean = substr.nullable || str.nullable

    override def toPainlessCall(callArgs: List[String]): String = {
      callArgs match {
        case List(arg0, arg1, arg2) => s"$arg1.indexOf($arg0, $arg2 - 1) + 1"
        case _ => throw new IllegalArgumentException("POSITION requires 3 arguments")
      }
    }

    override def validate(): Either[String, Unit] =
      if (start < 1)
        Left("POSITION start must be greater than or equal to 1 (SQL is 1-based)")
      else {
        for {
          _ <- str.validate()
          _ <- substr.validate()
        } yield ()
      }

    override def toSQL(base: String): String = sql
  }

  case class RegexpLike(
    str: PainlessScript,
    pattern: PainlessScript,
    matchFlags: Option[MatchFlags] = None
  ) extends StringFunction[SQLBool] {
    override def outputType: SQLBool = SQLTypes.Boolean

    override def stringOp: StringOp = RegexpLike

    override def args: List[PainlessScript] = List(str, pattern) ++ matchFlags.toList

    override def nullable: Boolean = str.nullable || pattern.nullable

    override def toPainlessCall(callArgs: List[String]): String = {
      callArgs match {
        case List(arg0, arg1) => s"java.util.regex.Pattern.compile($arg1).matcher($arg0).find()"
        case List(arg0, arg1, arg2) =>
          s"java.util.regex.Pattern.compile($arg1, $arg2).matcher($arg0).find()"
        case _ => throw new IllegalArgumentException("REGEXP_LIKE requires 2 or 3 arguments")
      }
    }

    override def validate(): Either[String, Unit] =
      args.map(_.validate()).filter(_.isLeft) match {
        case Nil    => Right(())
        case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
      }

    override def toSQL(base: String): String = sql
  }
}
