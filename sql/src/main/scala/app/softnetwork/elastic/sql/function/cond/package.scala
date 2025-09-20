package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.{
  Expr,
  Expression,
  GenericIdentifier,
  Identifier,
  PainlessScript,
  TokenRegex
}
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql.`type`.{SQLAny, SQLBool, SQLType, SQLTypeUtils, SQLTypes}

package object cond {

  sealed trait ConditionalOp extends PainlessScript with TokenRegex {
    override def painless: String = sql
  }

  case object Coalesce extends Expr("coalesce") with ConditionalOp
  case object IsNullFunction extends Expr("isnull") with ConditionalOp
  case object IsNotNullFunction extends Expr("isnotnull") with ConditionalOp
  case object NullIf extends Expr("nullif") with ConditionalOp
  case object Exists extends Expr("exists") with ConditionalOp

  case object Case extends Expr("case") with ConditionalOp

  case object When extends Expr("when") with TokenRegex
  case object Then extends Expr("then") with TokenRegex
  case object Else extends Expr("else") with TokenRegex
  case object End extends Expr("end") with TokenRegex

  sealed trait ConditionalFunction[In <: SQLType]
      extends TransformFunction[In, SQLBool]
      with FunctionWithIdentifier {
    def conditionalOp: ConditionalOp

    override def fun: Option[PainlessScript] = Some(conditionalOp)

    override def outputType: SQLBool = SQLTypes.Boolean

    override def toPainless(base: String, idx: Int): String = s"($base$painless)"
  }

  case class IsNullFunction(identifier: GenericIdentifier) extends ConditionalFunction[SQLAny] {
    override def conditionalOp: ConditionalOp = IsNullFunction

    override def args: List[PainlessScript] = List(identifier)

    override def inputType: SQLAny = SQLTypes.Any

    override def toSQL(base: String): String = sql

    override def painless: String = s" == null"
    override def toPainless(base: String, idx: Int): String = {
      if (nullable)
        s"(def e$idx = $base; e$idx$painless)"
      else
        s"$base$painless"
    }
  }

  case class IsNotNullFunction(identifier: GenericIdentifier) extends ConditionalFunction[SQLAny] {
    override def conditionalOp: ConditionalOp = IsNotNullFunction

    override def args: List[PainlessScript] = List(identifier)

    override def inputType: SQLAny = SQLTypes.Any

    override def toSQL(base: String): String = sql

    override def painless: String = s" != null"
    override def toPainless(base: String, idx: Int): String = {
      if (nullable)
        s"(def e$idx = $base; e$idx$painless)"
      else
        s"$base$painless"
    }
  }

  case class Coalesce(values: List[PainlessScript])
      extends TransformFunction[SQLAny, SQLType]
      with FunctionWithIdentifier {
    def operator: ConditionalOp = Coalesce

    override def fun: Option[ConditionalOp] = Some(operator)

    override def args: List[PainlessScript] = values

    override def outputType: SQLType = SQLTypeUtils.leastCommonSuperType(args.map(_.out))

    override def identifier: GenericIdentifier = GenericIdentifier("")

    override def inputType: SQLAny = SQLTypes.Any

    override def sql: String = s"$Coalesce(${values.map(_.sql).mkString(", ")})"

    // Reprend l’idée de SQLValues mais pour n’importe quel token
    override def out: SQLType = SQLTypeUtils.leastCommonSuperType(values.map(_.out).distinct)

    override def applyType(in: SQLType): SQLType = out

    override def validate(): Either[String, Unit] = {
      if (values.isEmpty) Left("COALESCE requires at least one argument")
      else Right(())
    }

    override def toPainless(base: String, idx: Int): String = s"$base$painless"

    override def painless: String = {
      require(values.nonEmpty, "COALESCE requires at least one argument")

      val checks = values
        .take(values.length - 1)
        .zipWithIndex
        .map { case (v, index) =>
          var check = s"def v$index = ${SQLTypeUtils.coerce(v, out)};"
          check += s"if (v$index != null) return v$index;"
          check
        }
        .mkString(" ")
      // final fallback
      s"{ $checks return ${SQLTypeUtils.coerce(values.last, out)}; }"
    }

    override def nullable: Boolean = values.forall(_.nullable)
  }

  case class NullIf(expr1: PainlessScript, expr2: PainlessScript)
      extends ConditionalFunction[SQLAny] {
    override def conditionalOp: ConditionalOp = NullIf

    override def args: List[PainlessScript] = List(expr1, expr2)

    override def identifier: GenericIdentifier = GenericIdentifier("")

    override def inputType: SQLAny = SQLTypes.Any

    override def out: SQLType = expr1.out

    override def applyType(in: SQLType): SQLType = out

    override def toPainlessCall(callArgs: List[String]): String = {
      callArgs match {
        case List(arg0, arg1) => s"${arg0.trim} == ${arg1.trim} ? null : $arg0"
        case _ => throw new IllegalArgumentException("NULLIF requires exactly two arguments")
      }
    }
  }

  case class Case(
    expression: Option[PainlessScript],
    conditions: List[(PainlessScript, PainlessScript)],
    default: Option[PainlessScript]
  ) extends TransformFunction[SQLAny, SQLAny] {
    override def args: List[PainlessScript] = List.empty

    override def inputType: SQLAny = SQLTypes.Any
    override def outputType: SQLAny = SQLTypes.Any

    override def sql: String = {
      val exprPart = expression.map(e => s"$Case ${e.sql}").getOrElse(Case.sql)
      val whenThen = conditions
        .map { case (cond, res) => s"$When ${cond.sql} $Then ${res.sql}" }
        .mkString(" ")
      val elsePart = default.map(d => s" $Else ${d.sql}").getOrElse("")
      s"$exprPart $whenThen$elsePart $End"
    }

    override def out: SQLType =
      SQLTypeUtils.leastCommonSuperType(
        conditions.map(_._2.out) ++ default.map(_.out).toList
      )

    override def applyType(in: SQLType): SQLType = out

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

    override def painless: String = {
      val base =
        expression match {
          case Some(expr) =>
            s"def expr = ${SQLTypeUtils.coerce(expr, expr.out)}; "
          case _ => ""
        }
      val cases = conditions.zipWithIndex
        .map { case ((cond, res), idx) =>
          val name =
            cond match {
              case e: Expression =>
                e.identifier.name
              case i: Identifier =>
                i.name
              case _ => ""
            }
          expression match {
            case Some(expr) =>
              val c = SQLTypeUtils.coerce(cond, expr.out)
              if (cond.sql == res.sql) {
                s"def val$idx = $c; if (expr == val$idx) return val$idx;"
              } else {
                res match {
                  case i: Identifier if i.name == name && cond.isInstanceOf[Identifier] =>
                    i.nullable = false
                    if (cond.asInstanceOf[Identifier].functions.isEmpty)
                      s"def val$idx = $c; if (expr == val$idx) return ${SQLTypeUtils.coerce(i.toPainless(s"val$idx"), i.out, out, nullable = false)};"
                    else {
                      cond.asInstanceOf[Identifier].nullable = false
                      s"def e$idx = ${i.checkNotNull}; def val$idx = e$idx != null ? ${SQLTypeUtils
                        .coerce(cond.asInstanceOf[Identifier].toPainless(s"e$idx"), cond.out, out, nullable = false)} : null; if (expr == val$idx) return ${SQLTypeUtils
                        .coerce(i.toPainless(s"e$idx"), i.out, out, nullable = false)};"
                    }
                  case _ =>
                    s"if (expr == $c) return ${SQLTypeUtils.coerce(res, out)};"
                }
              }
            case None =>
              val c = SQLTypeUtils.coerce(cond, SQLTypes.Boolean)
              val r =
                res match {
                  case i: Identifier if i.name == name && cond.isInstanceOf[Expression] =>
                    i.nullable = false
                    SQLTypeUtils.coerce(i.toPainless("left"), i.out, out, nullable = false)
                  case _ => SQLTypeUtils.coerce(res, out)
                }
              s"if ($c) return $r;"
          }
        }
        .mkString(" ")
      val defaultCase = default
        .map(d => s"def dval = ${SQLTypeUtils.coerce(d, out)}; return dval;")
        .getOrElse("return null;")
      s"{ $base$cases $defaultCase }"
    }

    override def toPainless(base: String, idx: Int): String = s"$base$painless"

    override def nullable: Boolean =
      conditions.exists { case (_, res) => res.nullable } || default.forall(_.nullable)
  }

}
