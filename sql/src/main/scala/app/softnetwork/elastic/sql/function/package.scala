/*
 * Copyright 2015 SOFTNETWORK
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

package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils}
import app.softnetwork.elastic.sql.function.aggregate.AggregateFunction
import app.softnetwork.elastic.sql.operator.math.ArithmeticExpression
import app.softnetwork.elastic.sql.parser.Validator

package object function {

  trait Function extends TokenRegex {
    def toSQL(base: String): String = if (base.nonEmpty) s"$sql($base)" else sql
    def applyType(in: SQLType): SQLType = out
    private[this] var _expr: Token = Null
    def expr_=(e: Token): Unit = {
      _expr = e
    }
    def expr: Token = _expr
    override def nullable: Boolean = expr.nullable
  }

  trait FunctionWithIdentifier extends Function {
    def identifier: Identifier
  }

  trait FunctionWithValue[+T] extends Function with TokenValue {
    def value: T
  }

  object FunctionUtils {
    def aggregateAndTransformFunctions(
      chain: FunctionChain
    ): (List[Function], List[Function]) = {
      chain.functions.partition {
        case _: AggregateFunction => true
        case _                    => false
      }
    }

    def transformFunctions(chain: FunctionChain): List[Function] = {
      aggregateAndTransformFunctions(chain)._2
    }

  }

  trait FunctionChain extends Function {
    def functions: List[Function]

    override def validate(): Either[String, Unit] = {
      if (aggregations.size > 1) {
        Left("Only one aggregation function is allowed in a function chain")
      } else if (aggregations.size == 1 && !functions.head.isInstanceOf[AggregateFunction]) {
        Left("Aggregation function must be the first function in the chain")
      } else {
        Validator.validateChain(functions)
      }
    }

    override def toSQL(base: String): String =
      functions.reverse.foldLeft(base)((expr, fun) => {
        fun.toSQL(expr)
      })

    override def system: Boolean = functions.lastOption.exists(_.system)

    def applyTo(expr: Token): Unit = {
      this.expr = expr
      functions.reverse.foldLeft(expr) { (currentExpr, fun) =>
        fun.expr = currentExpr
        fun
      }
    }

    private[this] lazy val aggregations = functions.collect { case af: AggregateFunction =>
      af
    }

    lazy val aggregateFunction: Option[AggregateFunction] = aggregations.headOption

    lazy val aggregation: Boolean = aggregateFunction.isDefined

    override def in: SQLType = functions.lastOption.map(_.in).getOrElse(super.in)

    override def baseType: SQLType = {
      val baseType = functions.lastOption.map(_.in).getOrElse(super.baseType)
      functions.reverse.foldLeft(baseType) { (currentType, fun) =>
        fun.applyType(currentType)
      }
    }

    def arithmetic: Boolean = functions.nonEmpty && functions.forall {
      case _: ArithmeticExpression => true
      case _                       => false
    }

    override def cast(targetType: SQLType): SQLType = {
      functions.headOption match {
        case Some(f) =>
          f.cast(targetType)
        case None =>
          this.baseType
      }
    }

    def find(function: Function): Option[Function] = {
      functions.find(_ == function)
    }

    def contains(function: Function): Boolean = {
      functions.contains(function)
    }

    def indexOf(function: Function): Int = {
      functions.indexOf(function)
    }
  }

  trait FunctionN[In <: SQLType, Out <: SQLType] extends Function with PainlessScript {
    def fun: Option[PainlessScript] = None

    def args: List[PainlessScript]

    def argTypes: List[SQLType] = args.map(_.out)

    def argsSeparator: String = ", "

    def inputType: In
    def outputType: Out

    override def in: SQLType = inputType
    override def baseType: SQLType = outputType

    override def applyType(in: SQLType): SQLType = outputType

    override def sql: String =
      s"${fun.map(_.sql).getOrElse("")}(${args.map(_.sql).mkString(argsSeparator)})"

    override def toSQL(base: String): String = s"$base$sql"

    def checkIfNullable: Boolean = args.exists(_.nullable)

    override def painless(context: Option[PainlessContext]): String = {
      context match {
        case Some(ctx) =>
          args.foreach(arg => ctx.addParam(arg)) // ensure all args are added to the context
        case _ =>
      }

      val nullCheck =
        if (checkIfNullable) {
          args.zipWithIndex
            .filter(_._1.nullable)
            .map { case (a, i) =>
              context.flatMap(ctx => ctx.get(a)).getOrElse(s"arg$i") + " == null"
            }
            .mkString(" || ")
        } else
          ""

      val assignments =
        args.zipWithIndex
          .filter(_._1.nullable)
          .map { case (a, i) =>
            context
              .flatMap(ctx => ctx.get(a).map(_ => ""))
              .getOrElse(
                s"def arg$i = ${SQLTypeUtils
                  .coerce(a.painless(context), a.baseType, argTypes(i), nullable = false, context)};"
              )
          }
          .mkString(" ")
          .trim

      val callArgs = args.zipWithIndex
        .map { case (a, i) =>
          context.flatMap(ctx => ctx.get(a)).getOrElse {
            if (a.nullable) s"arg$i"
            else
              SQLTypeUtils
                .coerce(a.painless(context), a.baseType, argTypes(i), nullable = false, context)
          }
        }

      val painlessCall = toPainlessCall(callArgs, context)

      if (checkIfNullable) {
        if (assignments.nonEmpty)
          s"$assignments ($nullCheck) ? $nullValue : $painlessCall"
        else s"($nullCheck) ? $nullValue : $painlessCall"
      } else
        s"$painlessCall"
    }

    def toPainlessCall(callArgs: List[String], context: Option[PainlessContext]): String =
      if (callArgs.nonEmpty)
        s"${fun.map(_.painless(context)).getOrElse("")}(${callArgs.mkString(argsSeparator)})"
      else
        fun.map(_.painless(context)).getOrElse("")
  }

  trait BinaryFunction[In1 <: SQLType, In2 <: SQLType, Out <: SQLType] extends FunctionN[In2, Out] {
    self: Function =>

    def left: PainlessScript
    def right: PainlessScript

    override def args: List[PainlessScript] = List(left, right)

    override def nullable: Boolean = left.nullable || right.nullable
  }

  trait TransformFunction[In <: SQLType, Out <: SQLType] extends FunctionN[In, Out] {
    override def checkIfNullable: Boolean =
      super.checkIfNullable && (this match {
        case f: FunctionWithIdentifier
            if f.identifier.functions.size > 1 && f.identifier.functions.reverse.headOption.exists(
              !_.equals(this)
            ) =>
          false
        case _ =>
          true
      })

    def toPainless(base: String, idx: Int, context: Option[PainlessContext]): String = {
      context match {
        case Some(_) =>
          val p = painless(context)
          if (p.startsWith(".")) // method call
            s"$base$p"
          else
            p
        case None =>
          if (checkIfNullable && base.nonEmpty)
            s"(def e$idx = $base; e$idx != null ? e$idx${painless(context)} : null)"
          else
            s"$base${painless(context)}"
      }
    }
  }

}
