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
  query,
  Alias,
  DateMathRounding,
  Expr,
  Identifier,
  PainlessContext,
  PainlessScript,
  TokenRegex,
  Updateable
}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils, SQLTypes}

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

    override def painless(context: Option[PainlessContext] = None): String =
      SQLTypeUtils.coerce(value, targetType, context)

    override def toPainless(base: String, idx: Int, context: Option[PainlessContext]): String = {
      context match {
        case Some(ctx) =>
          value match {
            case _: Identifier =>
              inputType match {
                case SQLTypes.Any =>
                  ctx.find(base) match {
                    case Some(identifier) =>
                      outputType match {
                        case SQLTypes.Date =>
                          identifier.addPainlessMethod(".toLocalDate()")
                        case SQLTypes.Time =>
                          identifier.addPainlessMethod(".toLocalTime()")
                        case _ => // do nothing
                      }
                    case _ => // do nothing
                  }
                case _ => // do nothing
              }
            case _ => // do nothing
          }
        case _ => // do nothing
      }
      val ret = SQLTypeUtils.coerce(base, value.baseType, targetType, value.nullable, context)
      val bloc = ret.startsWith("{") && ret.endsWith("}")
      val retWithBrackets = if (bloc) ret else s"{ $ret }"
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

    override def update(request: query.SingleSearch): Conversion = {
      value match {
        case updatable: Updateable =>
          this.copy(value = updatable.update(request).asInstanceOf[PainlessScript])
        case _ => this
      }
    }
  }

  case object CastOperator extends Expr("\\:\\:") with TokenRegex

  case class CastOperator(value: PainlessScript, targetType: SQLType) extends Conversion {
    override def sql: String = s"${value.sql}::$targetType"

    override def safe: Boolean = false

    value.cast(targetType)

    override def update(request: query.SingleSearch): CastOperator = {
      value match {
        case updatable: Updateable =>
          this.copy(value = updatable.update(request).asInstanceOf[PainlessScript])
        case _ => this
      }
    }
  }

  case object Convert extends Expr("CONVERT") with TokenRegex

  case class Convert(value: PainlessScript, targetType: SQLType, transactSql: Boolean = false)
      extends Conversion {
    override def sql: String = {
      if (transactSql) s"$Convert($targetType, ${value.sql})"
      else s"$Convert(${value.sql}, $targetType)"
    }

    override def safe: Boolean = false

    value.cast(targetType)

    override def update(request: query.SingleSearch): Convert = {
      value match {
        case updatable: Updateable =>
          this.copy(value = updatable.update(request).asInstanceOf[PainlessScript])
        case _ => this
      }
    }
  }
}
