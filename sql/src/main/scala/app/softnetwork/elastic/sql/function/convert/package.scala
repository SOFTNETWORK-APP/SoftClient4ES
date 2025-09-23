package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.{Alias, Expr, PainlessScript, TokenRegex}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils}

package object convert {

  case object Cast extends Expr("CAST") with TokenRegex

  case class Cast(value: PainlessScript, targetType: SQLType, as: Boolean = true)
      extends TransformFunction[SQLType, SQLType] {
    override def inputType: SQLType = value.out
    override def outputType: SQLType = targetType

    override def args: List[PainlessScript] = List.empty

    override def sql: String =
      s"$Cast(${value.sql} ${if (as) s"$Alias " else ""}${targetType.typeId})"

    override def toSQL(base: String): String = sql

    override def painless: String =
      SQLTypeUtils.coerce(value, targetType)

    override def toPainless(base: String, idx: Int): String =
      SQLTypeUtils.coerce(base, value.out, targetType, value.nullable)
  }

}
