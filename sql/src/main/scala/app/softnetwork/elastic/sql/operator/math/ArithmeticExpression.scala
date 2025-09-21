package app.softnetwork.elastic.sql.operator.math

import app.softnetwork.elastic.sql._
import app.softnetwork.elastic.sql.`type`._
import app.softnetwork.elastic.sql.function.{BinaryFunction, TransformFunction}
import app.softnetwork.elastic.sql.parser.Validator

case class ArithmeticExpression(
  left: PainlessScript,
  operator: ArithmeticOperator,
  right: PainlessScript,
  group: Boolean = false
) extends TransformFunction[SQLNumeric, SQLNumeric]
    with BinaryFunction[SQLNumeric, SQLNumeric, SQLNumeric] {

  override def fun: Option[ArithmeticOperator] = Some(operator)

  override def inputType: SQLNumeric = SQLTypes.Numeric
  override def outputType: SQLNumeric = SQLTypes.Numeric

  override def applyType(in: SQLType): SQLType = in

  override def sql: String = {
    val expr = s"${left.sql}$operator${right.sql}"
    if (group)
      s"($expr)"
    else
      expr
  }

  override def out: SQLType =
    SQLTypeUtils.leastCommonSuperType(List(left.out, right.out))

  override def validate(): Either[String, Unit] = {
    for {
      _ <- left.validate()
      _ <- right.validate()
      _ <- Validator.validateTypesMatching(left.out, right.out)
    } yield ()
  }

  override def nullable: Boolean = left.nullable || right.nullable

  override def toPainless(base: String, idx: Int): String = {
    if (nullable) {
      val l = left match {
        case t: TransformFunction[_, _] =>
          SQLTypeUtils.coerce(t.toPainless("", idx + 1), left.out, out, nullable = false)
        case _ => SQLTypeUtils.coerce(left.painless, left.out, out, nullable = false)
      }
      val r = right match {
        case t: TransformFunction[_, _] =>
          SQLTypeUtils.coerce(t.toPainless("", idx + 1), right.out, out, nullable = false)
        case _ => SQLTypeUtils.coerce(right.painless, right.out, out, nullable = false)
      }
      var expr = ""
      if (left.nullable)
        expr += s"def lv$idx = ($l); "
      if (right.nullable)
        expr += s"def rv$idx = ($r); "
      if (left.nullable && right.nullable)
        expr += s"(lv$idx == null || rv$idx == null) ? null : (lv$idx ${operator.painless} rv$idx)"
      else if (left.nullable)
        expr += s"(lv$idx == null) ? null : (lv$idx ${operator.painless} $r)"
      else
        expr += s"(rv$idx == null) ? null : ($l ${operator.painless} rv$idx)"
      if (group)
        expr = s"($expr)"
      return s"$base$expr"
    }
    s"$base$painless"
  }

  override def painless: String = {
    val l = SQLTypeUtils.coerce(left, out)
    val r = SQLTypeUtils.coerce(right, out)
    val expr = s"$l ${operator.painless} $r"
    if (group)
      s"($expr)"
    else
      expr
  }

}
