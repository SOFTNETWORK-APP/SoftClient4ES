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

package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.{
  Expr,
  Identifier,
  IntValue,
  PainlessContext,
  PainlessScript,
  TokenRegex
}
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
    override def painless(context: Option[PainlessContext]): String = s".${sql.toLowerCase()}()"
  }

  case object Concat extends Expr("CONCAT") with StringOp {
    override def painless(context: Option[PainlessContext]): String = " + "
  }
  case object Pipe extends Expr("\\|\\|") with StringOp {
    override def painless(context: Option[PainlessContext]): String = " + "
  }
  case object Lower extends Expr("LOWER") with StringOp {
    override lazy val words: List[String] = List(sql, "LCASE")
  }
  case object Upper extends Expr("UPPER") with StringOp {
    override lazy val words: List[String] = List(sql, "UCASE")
  }
  case object Trim extends Expr("TRIM") with StringOp
  case object Ltrim extends Expr("LTRIM") with StringOp {
    override def painless(context: Option[PainlessContext]): String =
      ".replaceAll(\"^\\\\s+\",\"\")"
  }
  case object Rtrim extends Expr("RTRIM") with StringOp {
    override def painless(context: Option[PainlessContext]): String =
      ".replaceAll(\"\\\\s+$\",\"\")"
  }
  case object Substring extends Expr("SUBSTRING") with StringOp {
    override def painless(context: Option[PainlessContext]): String = ".substring"
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
    override def painless(context: Option[PainlessContext]): String = ".replace"
  }
  case object Reverse extends Expr("REVERSE") with StringOp
  case object Position extends Expr("POSITION") with StringOp {
    override lazy val words: List[String] = List(sql, "STRPOS")
    override def painless(context: Option[PainlessContext]): String = ".indexOf"
  }

  case object RegexpLike extends Expr("REGEXP_LIKE") with StringOp {
    override lazy val words: List[String] = List(sql, "REGEXP")
    override def painless(context: Option[PainlessContext]): String = ".matches"
  }

  case class MatchFlags(flags: String) extends PainlessScript {
    override def sql: String = s"'$flags'"
    override def painless(context: Option[PainlessContext]): String = flags.toCharArray
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

    override def identifier: Identifier = Identifier(this)

    override def toSQL(base: String): String =
      if (base.nonEmpty) s"$sql($base)"
      else sql

    override def sql: String =
      if (args.isEmpty)
        s"${fun.map(_.sql).getOrElse("")}"
      else
        s"$stringOp(${args.map(_.sql).mkString(argsSeparator)})"

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      callArgs match {
        case List(str) => s"$str${stringOp.painless(context)}"
        case _         => throw new IllegalArgumentException(s"${stringOp.sql} requires 1 argument")
      }
    }
  }

  case class StringFunctionWithOp(str: PainlessScript, stringOp: StringOp)
      extends StringFunction[SQLVarchar] {
    override def outputType: SQLVarchar = SQLTypes.Varchar
    override def args: List[PainlessScript] = List(str)
  }

  case class Substring(str: PainlessScript, start: Int, length: Option[Int])
      extends StringFunction[SQLVarchar] {
    override def outputType: SQLVarchar = SQLTypes.Varchar
    override def stringOp: StringOp = Substring

    override def args: List[PainlessScript] =
      List(str, IntValue(start)) ++ length.map(l => IntValue(l)).toList

    override def nullable: Boolean = str.nullable

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      callArgs match {
        // SUBSTRING(expr, start, length)
        case List(arg0, _, _) =>
          s"$arg0.substring(${start - 1}, Math.min(${start - 1 + length.get}, $arg0.length()))"

        // SUBSTRING(expr, start)
        case List(arg0, arg1) =>
          s"$arg0.substring(Math.min(${start - 1}, $arg0.length() - 1))"

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

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      if (callArgs.isEmpty)
        throw new IllegalArgumentException("CONCAT requires at least one argument")
      else
        callArgs.zipWithIndex
          .map { case (arg, idx) =>
            SQLTypeUtils.coerce(
              arg,
              values(idx).baseType,
              SQLTypes.Varchar,
              nullable = false,
              context
            )
          }
          .mkString(stringOp.painless(context))
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

  case class Length(str: PainlessScript) extends StringFunction[SQLBigInt] {
    override def outputType: SQLBigInt = SQLTypes.BigInt
    override def stringOp: StringOp = Length
    override def args: List[PainlessScript] = List(str)
  }

  case class LeftFunction(str: PainlessScript, length: Int) extends StringFunction[SQLVarchar] {
    override def outputType: SQLVarchar = SQLTypes.Varchar
    override def stringOp: StringOp = LeftOp

    override def args: List[PainlessScript] = List(str, IntValue(length))

    override def nullable: Boolean = str.nullable

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
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

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      callArgs match {
        case List(arg0, arg1) =>
          if (length == 0) ""
          else s"""$arg0.substring($arg0.length() - Math.min($arg1, $arg0.length()))"""
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

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
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

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
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

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      callArgs match {
        case List(arg0, arg1, _) => s"$arg1.indexOf($arg0, ${start - 1}) + 1"
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

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
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
