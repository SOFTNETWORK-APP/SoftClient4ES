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
  LiteralParam,
  PainlessContext,
  PainlessScript,
  TokenRegex
}
import app.softnetwork.elastic.sql.`type`.{
  SQLAny,
  SQLBool,
  SQLTemporal,
  SQLType,
  SQLTypeUtils,
  SQLTypes
}
import app.softnetwork.elastic.sql.parser.Validator
import app.softnetwork.elastic.sql.query.{CriteriaWithConditionalFunction, Expression}

package object cond {

  sealed trait ConditionalOp extends PainlessScript with TokenRegex {
    override def painless(context: Option[PainlessContext] = None): String = sql
  }

  case object Coalesce extends Expr("COALESCE") with ConditionalOp
  case object IsNull extends Expr("ISNULL") with ConditionalOp
  case object IsNotNull extends Expr("ISNOTNULL") with ConditionalOp
  case object NullIf extends Expr("NULLIF") with ConditionalOp
  // case object Exists extends Expr("EXISTS") with ConditionalOp

  case object Case extends Expr("CASE") with ConditionalOp

  case object WHEN extends Expr("WHEN") with TokenRegex
  case object THEN extends Expr("THEN") with TokenRegex
  case object ELSE extends Expr("ELSE") with TokenRegex
  case object END extends Expr("END") with TokenRegex

  sealed trait ConditionalFunction[In <: SQLType]
      extends TransformFunction[In, SQLBool]
      with FunctionWithIdentifier {
    def conditionalOp: ConditionalOp

    override def fun: Option[PainlessScript] = Some(conditionalOp)

    override def outputType: SQLBool = SQLTypes.Boolean

  }

  case class IsNull(identifier: Identifier) extends ConditionalFunction[SQLAny] {
    override def conditionalOp: ConditionalOp = IsNull

    override def args: List[PainlessScript] = List(identifier)

    override def inputType: SQLAny = SQLTypes.Any

    override def toSQL(base: String): String = sql

    override def checkIfNullable: Boolean = false

    override def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      callArgs match {
        case List(arg) =>
          s"${arg.trim} == null" // TODO check when identifier is nullable and has functions
        case _ => throw new IllegalArgumentException("ISNULL requires exactly one argument")
      }
  }

  case class IsNotNull(identifier: Identifier) extends ConditionalFunction[SQLAny] {
    override def conditionalOp: ConditionalOp = IsNotNull

    override def args: List[PainlessScript] = List(identifier)

    override def inputType: SQLAny = SQLTypes.Any

    override def toSQL(base: String): String = sql

    override def checkIfNullable: Boolean = false

    override def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      callArgs match {
        case List(arg) =>
          s"${arg.trim} != null" // TODO check when identifier is nullable and has functions
        case _ => throw new IllegalArgumentException("ISNOTNULL requires exactly one argument")
      }
  }

  case class Coalesce(values: List[PainlessScript])
      extends TransformFunction[SQLAny, SQLType]
      with FunctionWithIdentifier {
    def operator: ConditionalOp = Coalesce

    override def fun: Option[ConditionalOp] = Some(operator)

    override def args: List[PainlessScript] = values

    override def outputType: SQLType =
      baseType //SQLTypeUtils.leastCommonSuperType(args.map(_.baseType))

    override def identifier: Identifier = Identifier()

    override def inputType: SQLAny = SQLTypes.Any

    override def sql: String = s"$Coalesce(${values.map(_.sql).mkString(", ")})"

    // Reprend l’idée de SQLValues mais pour n’importe quel token
    override def baseType: SQLType = SQLTypeUtils.leastCommonSuperType(argTypes)

    override def validate(): Either[String, Unit] = {
      if (values.isEmpty) Left("COALESCE requires at least one argument")
      else Right(())
    }

    override def checkIfNullable: Boolean = false

    override def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      callArgs match {
        case Nil => throw new IllegalArgumentException("COALESCE requires at least one argument")
        case _ =>
          callArgs
            .take(values.length - 1)
            .map { arg =>
              s"(${arg.trim} != null ? ${arg.trim}" // TODO check when value is nullable and has functions
            }
            .mkString(" : ") + s" : ${callArgs.last})"
      }

    override def nullable: Boolean = values.forall(_.nullable)
  }

  case class NullIf(expr1: PainlessScript, expr2: PainlessScript)
      extends ConditionalFunction[SQLAny] {
    override def conditionalOp: ConditionalOp = NullIf

    override def args: List[PainlessScript] = List(expr1, expr2)

    override def identifier: Identifier = Identifier()

    override def inputType: SQLAny = SQLTypes.Any

    override def baseType: SQLType = SQLTypeUtils.leastCommonSuperType(argTypes)

    private[this] def checkIfExpressionNullable(expr: PainlessScript): Boolean = expr match {
      case f: FunctionChain if f.functions.nonEmpty => true
      case _                                        => false
    }

    override def checkIfNullable: Boolean =
      false //checkIfExpressionNullable(expr1) || checkIfExpressionNullable(expr2)

    override def toPainlessCall(
      callArgs: List[String],
      context: Option[PainlessContext]
    ): String = {
      callArgs match {
        case List(arg0, arg1) =>
          val expr =
            out match {
              case SQLTypes.Varchar =>
                s"$arg0 == null || $arg0.compareTo($arg1) == 0 ? null : $arg0"
              case _: SQLTemporal => s"$arg0 == null || $arg0.isEqual($arg1) ? null : $arg0"
              case _              => s"$arg0 == $arg1 ? null : $arg0"
            }
          context match {
            case Some(ctx) =>
              ctx.addParam(LiteralParam(expr)) match {
                case Some(e) => return e
                case _       =>
              }
            case _ =>
          }
          expr
        case _ => throw new IllegalArgumentException("NULLIF requires exactly two arguments")
      }
    }

    override def validate(): Either[String, Unit] = {
      for {
        _ <- expr1.validate()
        _ <- expr2.validate()
        _ <- Validator.validateTypesMatching(expr1.out, expr2.out)
      } yield ()
    }
  }

  case class Case(
    expression: Option[PainlessScript],
    conditions: List[(PainlessScript, PainlessScript)],
    default: Option[PainlessScript]
  ) extends TransformFunction[SQLAny, SQLAny] {
    override def args: List[PainlessScript] = expression.toList ++
      conditions.map { case (_, res) => res } ++
      default.toList

    override def inputType: SQLAny = SQLTypes.Any
    override def outputType: SQLAny = SQLTypes.Any

    override def sql: String = {
      val exprPart = expression.map(e => s"$Case ${e.sql}").getOrElse(Case.sql)
      val whenThen = conditions
        .map { case (cond, res) => s"$WHEN ${cond.sql} $THEN ${res.sql}" }
        .mkString(" ")
      val elsePart = default.map(d => s" $ELSE ${d.sql}").getOrElse("")
      s"$exprPart $whenThen$elsePart $END"
    }

    override def baseType: SQLType = SQLTypeUtils.leastCommonSuperType(argTypes)

    override def validate(): Either[String, Unit] = {
      if (conditions.isEmpty) Left("CASE WHEN requires at least one condition")
      else if (
        expression.isEmpty && conditions.exists { case (cond, _) => cond.out != SQLTypes.Boolean }
      )
        Left("CASE WHEN conditions must be of type BOOLEAN")
      else if (
        expression.isDefined && conditions.exists { case (cond, _) =>
          !SQLTypeUtils.matches(cond.out, expression.get.out)
        }
      )
        Left("CASE WHEN conditions must be of the same type as the expression")
      else Right(())
    }

    private[this] def checkCase(e: String, c: String, v: String): String = {
      out match {
        case SQLTypes.Varchar =>
          s"$e != null && $e.compareTo($c) == 0 ? $v"
        case _: SQLTemporal =>
          s"$e != null && $e.isEqual($c) ? $v"
        case _ => s"$e == $c ? $v"
      }
    }

    override def painless(context: Option[PainlessContext] = None): String = {
      context match {
        case Some(ctx) =>
          var cases =
            expression match {
              case Some(expr) => // case with expression to evaluate
                val e = SQLTypeUtils.coerce(expr, out, context)
                val expParam = ctx.addParam(
                  LiteralParam(e)
                )
                conditions
                  .map { case (cond, res) =>
                    val name =
                      cond match {
                        case e: Expression =>
                          e.identifier.name
                        case f: FunctionWithIdentifier =>
                          f.identifier.name
                        case i: Identifier =>
                          i.name
                        case _ => ""
                      }
                    val c = SQLTypeUtils.coerce(cond, out, context)
                    val r =
                      res match {
                        case i: Identifier if i.name == name && name.nonEmpty =>
                          i.withNullable(false)
                          SQLTypeUtils.coerce(
                            i,
                            out,
                            context
                          )
                        case _ =>
                          SQLTypeUtils.coerce(res, out, context)
                      }
                    expParam match {
                      case Some(e) =>
                        if (cond.nullable) {
                          ctx.addParam(LiteralParam(c)) match {
                            case Some(c) => checkCase(e, c, r)
                            case _       => checkCase(e, c, r)
                          }
                        } else {
                          checkCase(e, c, r)
                        }
                      case _ =>
                        checkCase(e, c, r)
                    }
                  }
                  .mkString(" : ")
              case _ =>
                conditions
                  .map { case (cond, res) =>
                    val name =
                      cond match {
                        case e: Expression =>
                          e.identifier.name
                        case f: FunctionWithIdentifier =>
                          f.identifier.name
                        case i: Identifier =>
                          i.name
                        case _ => ""
                      }
                    val c = SQLTypeUtils.coerce(cond, SQLTypes.Boolean, context)
                    val r =
                      res match {
                        case i: Identifier if i.name == name && name.nonEmpty =>
                          i.withNullable(false)
                          SQLTypeUtils.coerce(
                            i,
                            out,
                            context
                          )
                        case _ =>
                          SQLTypeUtils.coerce(res, out, context)
                      }
                    if (!cond.isInstanceOf[CriteriaWithConditionalFunction[_]] && cond.nullable) {
                      ctx.addParam(LiteralParam(c)) match {
                        case Some(c) => s"$c ? $r"
                        case _       => s"$c ? $r"
                      }
                    } else {
                      s"$c ? $r"
                    }
                  }
                  .mkString(" : ")
            }
          default match {
            case Some(df) =>
              val d = SQLTypeUtils.coerce(df, out, context)
              if (df.nullable) {
                ctx.addParam(LiteralParam(d)) match {
                  case Some(d) => cases = s"$cases : $d"
                  case _       => cases = s"$cases : $d"
                }
              } else {
                cases = s"$cases : $d"
              }
            case _ =>
          }

          cases

        case _ =>
          throw new IllegalArgumentException(s"Painless context is required for $sql")
      }
    }

    override def toPainless(base: String, idx: Int, context: Option[PainlessContext]): String =
      s"$base${painless(context)}"

    override def nullable: Boolean =
      conditions.exists { case (_, res) => res.nullable } || default.forall(_.nullable)
  }

}
