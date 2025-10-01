package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.{Alias, DateMathRounding, Expr, PainlessScript, TokenRegex}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils}

package object convert {

  sealed trait Conversion extends TransformFunction[SQLType, SQLType] with DateMathRounding {
    override def toSQL(base: String): String = sql

    def value: PainlessScript
    def targetType: SQLType
    def safe: Boolean

    override def inputType: SQLType = value.baseType
    override def outputType: SQLType = targetType

    override def args: List[PainlessScript] = List.empty

    //override def nullable: Boolean = value.nullable

    override def painless: String = SQLTypeUtils.coerce(value, targetType)

    override def toPainless(base: String, idx: Int): String = {
      val ret = SQLTypeUtils.coerce(base, value.baseType, targetType, value.nullable)
      val bloc = ret.startsWith("{") && ret.endsWith("}")
      val retWithBrackets = if (bloc) ret else s"{ return $ret; }"
      if (safe) s"try $retWithBrackets catch (Exception e) { return null; }"
      else ret
    }

    override def roundingScript: Option[String] = DateMathRounding(targetType)

    override def dateMathScript: Boolean = isTemporal
  }

  case object Cast extends Expr("CAST") with TokenRegex

  case object TryCast extends Expr("TRY_CAST") with TokenRegex {
    override def words: List[String] = List(sql, "SAFE_CAST")
  }

  case class Cast(
    value: PainlessScript,
    targetType: SQLType,
    as: Boolean = true,
    safe: Boolean = false
  ) extends Conversion {
    override def sql: String = {
      val ret = s"${value.sql} ${if (as) s"$Alias " else ""}$targetType"
      if (safe) s"$TryCast($ret)"
      else s"$Cast($ret)"
    }
    value.cast(targetType)
  }

  case object CastOperator extends Expr("\\:\\:") with TokenRegex

  case class CastOperator(value: PainlessScript, targetType: SQLType) extends Conversion {
    override def sql: String = s"${value.sql}::$targetType"

    override def safe: Boolean = false

    value.cast(targetType)
  }

  case object Convert extends Expr("CONVERT") with TokenRegex

  case class Convert(value: PainlessScript, targetType: SQLType) extends Conversion {
    override def sql: String = s"$Convert(${value.sql}, $targetType)"

    override def safe: Boolean = false

    value.cast(targetType)
  }
}
