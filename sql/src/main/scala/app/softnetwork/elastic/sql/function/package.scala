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

    override def painless(): String = {
      val nullCheck =
        args.zipWithIndex
          .filter(_._1.nullable)
          .map { case (_, i) => s"arg$i == null" }
          .mkString(" || ")

      val assignments =
        args.zipWithIndex
          .filter(_._1.nullable)
          .map { case (a, i) =>
            s"def arg$i = ${SQLTypeUtils.coerce(a.painless(), a.baseType, argTypes(i), nullable = false)};"
          }
          .mkString(" ")

      val callArgs = args.zipWithIndex
        .map { case (a, i) =>
          if (a.nullable)
            s"arg$i"
          else
            SQLTypeUtils.coerce(a.painless(), a.baseType, argTypes(i), nullable = false)
        }

      if (args.exists(_.nullable))
        s"($assignments ($nullCheck) ? null : ${toPainlessCall(callArgs)})"
      else
        s"${toPainlessCall(callArgs)}"
    }

    def toPainlessCall(callArgs: List[String]): String =
      if (callArgs.nonEmpty)
        s"${fun.map(_.painless()).getOrElse("")}(${callArgs.mkString(argsSeparator)})"
      else
        fun.map(_.painless()).getOrElse("")
  }

  trait BinaryFunction[In1 <: SQLType, In2 <: SQLType, Out <: SQLType] extends FunctionN[In2, Out] {
    self: Function =>

    def left: PainlessScript
    def right: PainlessScript

    override def args: List[PainlessScript] = List(left, right)

    override def nullable: Boolean = left.nullable || right.nullable
  }

  trait TransformFunction[In <: SQLType, Out <: SQLType] extends FunctionN[In, Out] {
    def toPainless(base: String, idx: Int): String = {
      if (nullable && base.nonEmpty)
        s"(def e$idx = $base; e$idx != null ? e$idx${painless()} : null)"
      else
        s"$base${painless()}"
    }
  }

}
