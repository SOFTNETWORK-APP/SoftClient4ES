package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.{Expr, Identifier, IntValue, PainlessScript, TokenRegex}
import app.softnetwork.elastic.sql.`type`.{SQLBigInt, SQLType, SQLTypeUtils, SQLTypes, SQLVarchar}

package object string {

  sealed trait StringOp extends PainlessScript with TokenRegex {
    override def painless: String = s".${sql.toLowerCase()}()"
  }

  case object Concat extends Expr("CONCAT") with StringOp {
    override def painless: String = " + "
  }
  case object Lower extends Expr("LOWER") with StringOp
  case object Upper extends Expr("UPPER") with StringOp
  case object Trim extends Expr("TRIM") with StringOp
  //case object LTrim extends SQLExpr("LTRIM") with SQLStringOperator
  //case object RTrim extends SQLExpr("RTRIM") with SQLStringOperator
  case object Substring extends Expr("SUBSTRING") with StringOp {
    override def painless: String = ".substring"
    override lazy val words: List[String] = List(sql, "SUBSTR")
  }
  case object To extends Expr("TO") with TokenRegex
  case object Length extends Expr("LENGTH") with StringOp

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
          s"(($arg1 - 1) < 0 || ($arg1 - 1 + $arg2) > $arg0.length()) ? null : $arg0.substring(($arg1 - 1), ($arg1 - 1 + $arg2))"

        // SUBSTRING(expr, start)
        case List(arg0, arg1) =>
          s"(($arg1 - 1) < 0 || ($arg1 - 1) >= $arg0.length()) ? null : $arg0.substring(($arg1 - 1))"

        case _ => throw new IllegalArgumentException("SUBSTRING requires 2 or 3 arguments")
      }
    }

    override def validate(): Either[String, Unit] =
      if (start < 1)
        Left("SUBSTRING start position must be greater than or equal to 1 (SQL is 1-based)")
      else if (length.exists(_ < 0))
        Left("SUBSTRING length must be non-negative")
      else Right(())

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
            SQLTypeUtils.coerce(arg, values(idx).out, SQLTypes.Varchar, nullable = false)
          }
          .mkString(stringOp.painless)
    }

    override def validate(): Either[String, Unit] =
      if (values.isEmpty) Left("CONCAT requires at least one argument")
      else Right(())

    override def toSQL(base: String): String = sql
  }

  case object SQLLength extends StringFunction[SQLBigInt] {
    override def outputType: SQLBigInt = SQLTypes.BigInt
    override def stringOp: StringOp = Length
    override def args: List[PainlessScript] = List.empty
  }
}
