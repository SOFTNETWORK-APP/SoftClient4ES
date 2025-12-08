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

package app.softnetwork.elastic

import app.softnetwork.elastic.sql.function.aggregate.{
  AggregateFunction,
  COUNT,
  MAX,
  MIN,
  WindowFunction
}
import app.softnetwork.elastic.sql.function.geo.DistanceUnit
import app.softnetwork.elastic.sql.function.time.CurrentFunction
import app.softnetwork.elastic.sql.operator._
import app.softnetwork.elastic.sql.parser.{Validation, Validator}
import app.softnetwork.elastic.sql.query._

import java.security.MessageDigest
import java.util.regex.Pattern
import scala.reflect.runtime.universe._
import scala.util.Try
import scala.util.matching.Regex

/** Created by smanciot on 27/06/2018.
  */
package object sql {

  /** type alias for SQL query
    */
  type SQL = String

  import app.softnetwork.elastic.sql.function._
  import app.softnetwork.elastic.sql.`type`._

  import scala.language.implicitConversions

  implicit def asString(token: Option[_ <: Token]): String = token match {
    case Some(t) => t.toString
    case _       => ""
  }

  /** Base trait for all tokens
    */
  trait Token extends Serializable with Validation {
    def sql: String
    override def toString: String = sql
    def baseType: SQLType = SQLTypes.Any
    def in: SQLType = baseType
    private[this] var _out: SQLType = SQLTypes.Null
    def out: SQLType = if (_out == SQLTypes.Null) baseType else _out
    /*def out_=(t: SQLType): Unit = {
      _out = t
    }*/
    def cast(targetType: SQLType): SQLType = {
      this._out = targetType
      this.out
    }
    def system: Boolean = false
    def nullable: Boolean = !system
    def dateMathScript: Boolean = false
    def isTemporal: Boolean = out.isInstanceOf[SQLTemporal]
    def isAggregation: Boolean = false
    def hasAggregation: Boolean = isAggregation
    def shouldBeScripted: Boolean = false
  }

  trait TokenValue extends Token {
    def value: Any
  }

  /** Trait for tokens that can be used in painless scripts
    */
  trait PainlessScript extends Token {

    /** Generate painless script for this token
      *
      * @param context
      *   the painless context
      * @return
      *   the painless script
      */
    def painless(context: Option[PainlessContext] = None): String
    def nullValue: String = "null"
  }

  /** Trait for tokens that can be used as parameters in painless scripts
    */
  trait PainlessParam extends Token {
    def param: String
    def checkNotNull: String
    override def hashCode(): Int = param.hashCode
    override def equals(obj: Any): Boolean = {
      obj match {
        case p: PainlessParam => p.param == param
        case _                => false
      }
    }

    def paramValue: String =
      if (nullable && checkNotNull.nonEmpty)
        checkNotNull
      else
        s"$param${painlessMethods.mkString("")}"

    private[this] var _painlessMethods: collection.mutable.Seq[String] =
      collection.mutable.Seq.empty

    def addPainlessMethod(method: String): PainlessParam = {
      if (!_painlessMethods.contains(method))
        _painlessMethods = _painlessMethods :+ method // FIXME we should apply functions only once
      this
    }

    def painlessMethods: Seq[String] = _painlessMethods.toSeq

  }

  case class LiteralParam(param: String) extends PainlessParam {
    override def sql: String = ""
    override def checkNotNull: String = ""
  }

  /** Context for painless scripts
    */
  case class PainlessContext() {
    // List of parameter keys
    private[this] var _keys: collection.mutable.Seq[PainlessParam] = collection.mutable.Seq.empty

    // List of parameter names
    private[this] var _values: collection.mutable.Seq[String] = collection.mutable.Seq.empty

    // Last parameter name added
    private[this] var _lastParam: Option[String] = None

    /** Add a token parameter to the context if not already present
      *
      * @param token
      *   the token parameter to add
      * @return
      *   the optional parameter name
      */
    def addParam(token: Token): Option[String] = {
      token match {
        case param: PainlessParam
            if param.param.nonEmpty && (param.isInstanceOf[LiteralParam] || param.nullable) =>
          get(param) match {
            case Some(p) => Some(p)
            case _ =>
              val index = _values.indexOf(param.param)
              if (index >= 0) {
                Some(param.param)
              } else {
                val paramName = s"param${_keys.size + 1}"
                _keys = _keys :+ param
                _values = _values :+ paramName
                _lastParam = Some(paramName)
                _lastParam
              }
          }
        case _ => None
      }
    }

    def get(token: Token): Option[String] = {
      token match {
        case param: PainlessParam =>
          if (exists(param)) Try(_values(_keys.indexOf(param))).toOption
          else None
        case f: FunctionWithIdentifier => get(f.identifier)
        case _                         => None
      }
    }

    def exists(token: Token): Boolean = {
      token match {
        case param: PainlessParam      => _keys.contains(param)
        case f: FunctionWithIdentifier => exists(f.identifier)
        case _                         => false
      }
    }

    def isEmpty: Boolean = _keys.isEmpty

    def nonEmpty: Boolean = _keys.nonEmpty

    def last: Option[String] = _lastParam

    def find(paramName: String): Option[PainlessParam] = {
      val index = _values.indexOf(paramName)
      if (index >= 0) Some(_keys(index))
      else None
    }

    override def toString: String = {
      if (isEmpty) ""
      else
        _keys
          .flatMap { param =>
            get(param) match {
              case Some(v) => Some(s"def $v = ${param.paramValue}; ")
              case None    => None // should not happen
            }
          }
          .mkString("")
    }
  }

  trait PainlessParams extends PainlessScript {
    def params: Map[String, Any]
  }

  /** Trait for tokens that can be used in date math scripts
    */
  trait DateMathScript extends Token {
    def script: Option[String]
    override def dateMathScript: Boolean = true
    def formatScript: Option[String] = None
  }

  object DateMathRounding {
    def apply(out: SQLType): Option[String] =
      out match {
        case _: SQLDate => Some("/d")
        /*case _: SQLDateTime  => Some("/s")
        case _: SQLTimestamp => Some("/s")*/
        case _: SQLTime => Some("/s")
        case _          => None
      }
  }

  trait DateMathRounding {
    def roundingScript: Option[String] = None
    def hasRounding: Boolean = roundingScript.isDefined
  }

  trait Updateable extends Token {
    def update(request: SingleSearch): Updateable
  }

  abstract class Expr(override val sql: String) extends Token

  case object Distinct extends Expr("DISTINCT") with TokenRegex

  abstract class Value[+T](val value: T)(implicit ev$1: T => Ordered[T])
      extends Token
      with PainlessScript
      with FunctionWithValue[T] {
    def choose[R >: T](
      values: Seq[R],
      operator: Option[ExpressionOperator],
      separator: String = "|"
    )(implicit ev: R => Ordered[R]): Option[R] = {
      if (values.isEmpty)
        None
      else
        operator match {
          case Some(EQ)        => values.find(_ == value)
          case Some(NE | DIFF) => values.find(_ != value)
          case Some(GE)        => values.filter(_ >= value).sorted.reverse.headOption
          case Some(GT)        => values.filter(_ > value).sorted.reverse.headOption
          case Some(LE)        => values.filter(_ <= value).sorted.headOption
          case Some(LT)        => values.filter(_ < value).sorted.headOption
          case _               => values.headOption
        }
    }
    override def painless(context: Option[PainlessContext]): String =
      SQLTypeUtils.coerce(
        value match {
          case s: String  => s""""$s""""
          case b: Boolean => b.toString
          case n: Number  => n.toString
          case _          => value.toString
        },
        this.baseType,
        this.out,
        nullable = false,
        context
      )

    override def nullable: Boolean = false
  }

  case object Null extends Value[Null](null) with TokenRegex {
    override def sql: String = "NULL"
    override def painless(context: Option[PainlessContext]): String = "null"
    override def nullable: Boolean = true
    override def baseType: SQLType = SQLTypes.Null
  }

  case object ParamValue extends Value[String](null) with TokenRegex {
    override def sql: String = "?"
    override def painless(context: Option[PainlessContext]): String = "params.paramValue"
    override def nullable: Boolean = true
    override def baseType: SQLType = SQLTypes.Any
  }

  case class BooleanValue(override val value: Boolean) extends Value[Boolean](value) {
    override def sql: String = value.toString
    override def baseType: SQLType = SQLTypes.Boolean
  }

  case class CharValue(override val value: Char) extends Value[Char](value) {
    override def sql: String = s"""'$value'"""
    override def baseType: SQLType = SQLTypes.Char
  }

  case class StringValue(override val value: String) extends Value[String](value) {
    override def sql: String = s"""'$value'"""
    import SQLImplicits._
    private lazy val pattern: Pattern = value.pattern
    def like: Seq[String] => Boolean = {
      _.exists { pattern.matcher(_).matches() }
    }
    def eq: Seq[String] => Boolean = {
      _.exists { _.contentEquals(value) }
    }
    def ne: Seq[String] => Boolean = {
      _.forall { !_.contentEquals(value) }
    }
    override def choose[R >: String](
      values: Seq[R],
      operator: Option[ExpressionOperator],
      separator: String = "|"
    )(implicit ev: R => Ordered[R]): Option[R] = {
      operator match {
        case Some(EQ)           => values.find(v => v.toString contentEquals value)
        case Some(NE | DIFF)    => values.find(v => !(v.toString contentEquals value))
        case Some(LIKE | RLIKE) => values.find(v => pattern.matcher(v.toString).matches())
        case None               => Some(values.mkString(separator))
        case _                  => super.choose(values, operator, separator)
      }
    }
    override def baseType: SQLType = SQLTypes.Varchar
  }

  sealed abstract class NumericValue[T: Numeric](override val value: T)(implicit
    ev$1: T => Ordered[T]
  ) extends Value[T](value) {
    override def sql: String = value.toString
    override def choose[R >: T](
      values: Seq[R],
      operator: Option[ExpressionOperator],
      separator: String = "|"
    )(implicit ev: R => Ordered[R]): Option[R] = {
      operator match {
        case None => if (values.isEmpty) None else Some(values.max)
        case _    => super.choose(values, operator, separator)
      }
    }
    private[this] val num: Numeric[T] = implicitly[Numeric[T]]
    def toDouble: Double = num.toDouble(value)
    def toEither: Either[Long, Double] = value match {
      case l: Long   => Left(l)
      case i: Int    => Left(i.toLong)
      case d: Double => Right(d)
      case f: Float  => Right(f.toDouble)
      case _         => Right(toDouble)
    }
    def max: Seq[T] => T = x => Try(x.max).getOrElse(num.zero)
    def min: Seq[T] => T = x => Try(x.min).getOrElse(num.zero)
    def eq: Seq[T] => Boolean = {
      _.exists { _ == value }
    }
    def ne: Seq[T] => Boolean = {
      _.forall { _ != value }
    }
    override def baseType: SQLNumeric = SQLTypes.Numeric
  }

  case class ByteValue(override val value: Byte) extends NumericValue[Byte](value) {
    override def baseType: SQLNumeric = SQLTypes.TinyInt
  }

  case class ShortValue(override val value: Short) extends NumericValue[Short](value) {
    override def baseType: SQLNumeric = SQLTypes.SmallInt
  }

  case class IntValue(override val value: Int) extends NumericValue[Int](value) {
    override def baseType: SQLNumeric = SQLTypes.Int
  }

  case class LongValue(override val value: Long) extends NumericValue[Long](value) {
    override def baseType: SQLNumeric = SQLTypes.BigInt
  }

  case class FloatValue(override val value: Float) extends NumericValue[Float](value) {
    override def baseType: SQLNumeric = SQLTypes.Real
  }

  case class DoubleValue(override val value: Double) extends NumericValue[Double](value) {
    override def baseType: SQLNumeric = SQLTypes.Double
  }

  case object PiValue extends Value[Double](Math.PI) with TokenRegex {
    override def sql: String = "PI"
    override def painless(context: Option[PainlessContext]): String = "Math.PI"
    override def baseType: SQLNumeric = SQLTypes.Double
  }

  case object EValue extends Value[Double](Math.E) with TokenRegex {
    override def sql: String = "E"
    override def painless(context: Option[PainlessContext]): String = "Math.E"
    override def baseType: SQLNumeric = SQLTypes.Double
  }

  case class GeoDistance(longValue: LongValue, unit: DistanceUnit)
      extends NumericValue[Double](DistanceUnit.convertToMeters(longValue.value, unit))
      with PainlessScript {
    override def baseType: SQLNumeric = SQLTypes.Double
    override def sql: String = s"$longValue $unit"
    def geoDistance: String = s"$longValue$unit"
    override def painless(context: Option[PainlessContext]): String = s"$value"
  }

  sealed abstract class FromTo(val from: TokenValue, val to: TokenValue) extends Token {
    override def sql = s"${from.sql} AND ${to.sql}"

    override def baseType: SQLType =
      SQLTypeUtils.leastCommonSuperType(List(from.baseType, to.baseType))

    override def validate(): Either[String, Unit] = {
      for {
        _ <- from.validate()
        _ <- to.validate()
        _ <- Validator.validateTypesMatching(from.out, to.out)
      } yield ()
    }
  }

  case class LiteralFromTo(override val from: StringValue, override val to: StringValue)
      extends FromTo(from, to) {
    def between: Seq[String] => Boolean = {
      _.exists { s => s >= from.value && s <= to.value }
    }
    def notBetween: Seq[String] => Boolean = {
      _.forall { s => s < from.value || s > to.value }
    }
  }

  case class LongFromTo(override val from: LongValue, override val to: LongValue)
      extends FromTo(from, to) {
    def between: Seq[Long] => Boolean = {
      _.exists { n => n >= from.value && n <= to.value }
    }
    def notBetween: Seq[Long] => Boolean = {
      _.forall { n => n < from.value || n > to.value }
    }
  }

  case class DoubleFromTo(override val from: DoubleValue, override val to: DoubleValue)
      extends FromTo(from, to) {
    def between: Seq[Double] => Boolean = {
      _.exists { n => n >= from.value && n <= to.value }
    }
    def notBetween: Seq[Double] => Boolean = {
      _.forall { n => n < from.value || n > to.value }
    }
  }

  case class GeoDistanceFromTo(override val from: GeoDistance, override val to: GeoDistance)
      extends FromTo(from, to) {
    def between: Seq[Double] => Boolean = {
      _.exists { n => n >= from.value && n <= to.value }
    }
    def notBetween: Seq[Double] => Boolean = {
      _.forall { n => n < from.value || n > to.value }
    }
  }

  case class IdentifierFromTo(override val from: Identifier, override val to: Identifier)
      extends FromTo(from, to)

  sealed abstract class Values[+R: TypeTag, +T <: Value[R]](val values: Seq[T])
      extends Token
      with PainlessScript {
    override def sql = s"(${values.map(_.sql).mkString(",")})"
    override def painless(context: Option[PainlessContext]): String =
      s"[${values.map(_.painless(context)).mkString(",")}]"
    lazy val innerValues: Seq[R] = values.map(_.value)
    override def nullable: Boolean = values.exists(_.nullable)
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.Any)
  }

  case class StringValues(override val values: Seq[StringValue])
      extends Values[String, Value[String]](values) {
    def eq: Seq[String] => Boolean = {
      _.exists { s => innerValues.exists(_.contentEquals(s)) }
    }
    def ne: Seq[String] => Boolean = {
      _.forall { s => innerValues.forall(!_.contentEquals(s)) }
    }
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.Varchar)
  }

  class NumericValues[R: TypeTag](override val values: Seq[NumericValue[R]])
      extends Values[R, NumericValue[R]](values) {
    def eq: Seq[R] => Boolean = {
      _.exists { n => innerValues.contains(n) }
    }
    def ne: Seq[R] => Boolean = {
      _.forall { n => !innerValues.contains(n) }
    }
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.Numeric)
  }

  case class ByteValues(override val values: Seq[ByteValue]) extends NumericValues[Byte](values) {
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.TinyInt)
  }

  case class ShortValues(override val values: Seq[ShortValue])
      extends NumericValues[Short](values) {
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.SmallInt)
  }

  case class IntValues(override val values: Seq[IntValue]) extends NumericValues[Int](values) {
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.Int)
  }

  case class LongValues(override val values: Seq[LongValue]) extends NumericValues[Long](values) {
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.BigInt)
  }

  case class FloatValues(override val values: Seq[FloatValue])
      extends NumericValues[Float](values) {
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.Real)
  }

  case class DoubleValues(override val values: Seq[DoubleValue])
      extends NumericValues[Double](values) {
    override def baseType: SQLArray = SQLTypes.Array(SQLTypes.Double)
  }

  def choose[T](
    values: Seq[T],
    criteria: Option[Criteria],
    function: Option[Function] = None
  )(implicit ev$1: T => Ordered[T]): Option[T] = {
    criteria match {
      case Some(GenericExpression(_, operator, value: Value[T] @unchecked, _)) =>
        value.choose[T](values, Some(operator))
      case _ =>
        function match {
          case Some(MIN) => Some(values.min)
          case Some(MAX) => Some(values.max)
          // FIXME        case Some(SQLSum) => Some(values.sum)
          // FIXME        case Some(SQLAvg) => Some(values.sum / values.length  )
          case _ => values.headOption
        }
    }
  }

  def toRegex(value: String): String = {
    value.replaceAll("%", ".*").replaceAll("_", ".")
  }

  case object Alias extends Expr("AS") with TokenRegex

  case class Alias(alias: String) extends Expr(s" ${Alias.sql} $alias")

  object AliasUtils {
    private val MaxAliasLength = 50

    private val opMapping = Map(
      "+" -> "plus",
      "-" -> "minus",
      "*" -> "mul",
      "/" -> "div",
      "%" -> "mod"
    )

    def normalize(expr: String): String = {
      // Remplacer les opérateurs SQL par des noms lisibles
      val replaced = opMapping.foldLeft(expr) { case (acc, (k, v)) =>
        acc.replace(k, s"_${v}_")
      }
      // Nettoyer pour obtenir un identifiant valide
      val normalized = replaced
        .replaceAll("[^a-zA-Z0-9_]", "_") // caractères invalides -> "_"
        .replaceAll("_+", "_") // compacter plusieurs "_"
        .stripPrefix("_")
        .stripSuffix("_")
        .toLowerCase

      // Tronquer si nécessaire
      if (normalized.length > MaxAliasLength) {
        val digest = MessageDigest.getInstance("MD5").digest(normalized.getBytes("UTF-8"))
        val hash = digest.map("%02x".format(_)).mkString.take(8) // suffix court
        normalized.take(MaxAliasLength - hash.length - 1) + "_" + hash
      } else {
        normalized
      }
    }
  }

  trait TokenRegex extends Token {
    def words: List[String] = List(sql)
    lazy val regex: Regex = s"(?i)(${words.mkString("|")})\\b".r
  }

  trait Source extends Updateable {
    def name: String
    def update(request: SingleSearch): Source
  }

  sealed trait Identifier
      extends TokenValue
      with Source
      with FunctionChain
      with PainlessScript
      with DateMathScript
      with PainlessParam {
    def name: String

    def withFunctions(functions: List[Function]): Identifier

    def update(request: SingleSearch): Identifier

    def tableAlias: Option[String]
    def distinct: Boolean
    def nested: Boolean
    def nestedElement: Option[NestedElement]
    def limit: Option[Limit]
    def fieldAlias: Option[String]
    def bucket: Option[Bucket]
    def hasBucket: Boolean = bucket.isDefined

    lazy val aggregations: Seq[AggregateFunction] = FunctionUtils.aggregateFunctions(this)

    def bucketPath: String

    lazy val allMetricsPath: Map[String, String] = {
      metricName match {
        case Some(name) => Map(name -> name)
        case _          => Map.empty
      }
    }

    override def sql: String = {
      var parts: Seq[String] = name.split("\\.").toSeq
      tableAlias match {
        case Some(a) => parts = a +: (if (nested) parts.tail else parts)
        case _       =>
      }
      val sql = {
        if (distinct) {
          s"$Distinct ${parts.mkString(".")}".trim
        } else {
          parts.mkString(".").trim
        }
      }
      functions.reverse.foldLeft(sql)((expr, fun) => {
        fun.toSQL(expr)
      })
    }

    applyTo(this)

    lazy val identifierName: String =
      functions.reverse.foldLeft(name)((expr, fun) => {
        fun.toSQL(expr)
      }) // TODO use AliasUtils.normalize?

    lazy val innerHitsName: Option[String] =
      nestedElement match {
        case Some(ne) => Some(ne.innerHitsName)
        case None     => None
      }

    lazy val aliasOrName: String = fieldAlias.getOrElse(name)

    lazy val path: String =
      nestedElement match {
        case Some(ne) =>
          name.split("\\.") match {
            case Array(_, _*) => s"${ne.path}.${name.split("\\.").tail.mkString(".")}"
            case _            => s"${ne.path}.$name"
          }
        case None => name
      }

    lazy val paramName: String =
      if (isAggregation && functions.size == 1) s"params.${metricName.getOrElse(aliasOrName)}"
      else if (path.nonEmpty)
        s"doc['$path'].value"
      else ""

    lazy val metricName: Option[String] =
      aggregateFunction match {
        case Some(af) =>
          af match {
            case COUNT =>
              aliasOrName match {
                case "*" =>
                  if (distinct) {
                    Some(s"count_distinct_all")
                  } else {
                    Some(s"count_all")
                  }
                case _ => Some(aliasOrName)
              }
            case _ => Some(aliasOrName)
          }
        case _ => None
      }

    lazy val script: Option[String] =
      if (isTemporal) {
        var orderedFunctions = FunctionUtils.transformFunctions(this).reverse

        val baseOpt: Option[String] = orderedFunctions.headOption match {
          case Some(head) =>
            head match {
              case s: StringValue if s.value.nonEmpty =>
                orderedFunctions = orderedFunctions.tail
                Some(s.value + "||")
              case current: CurrentFunction =>
                orderedFunctions = orderedFunctions.tail
                current.script
              case _ => Option(name).filter(_.nonEmpty).map(_ + "||")
            }
          case _ => Option(name).filter(_.nonEmpty).map(_ + "||")
        }

        val roundingOpt: Option[String] =
          orderedFunctions
            .collectFirst {
              case r: DateMathRounding if r.hasRounding => r.roundingScript.get
            }
            .orElse(DateMathRounding(out))

        orderedFunctions.foldLeft(baseOpt) {
          case (expr, f: Function) if expr.isDefined && f.dateMathScript =>
            f match {
              case s: DateMathScript =>
                s.script match {
                  case Some(script) if script.nonEmpty =>
                    Some(s"${expr.get}$script")
                  case _ => expr
                }
              case _ => expr
            }
          case (_, _) => None // ignore non math scripts
        } match {
          case Some(s) if s.nonEmpty =>
            roundingOpt match {
              case Some(r) if r.nonEmpty => Some(s"$s$r")
              case _                     => Some(s)
            }
          case _ => None
        }
      } else
        None

    override def dateMathScript: Boolean = isTemporal

    def checkNotNull: String =
      if (path.isEmpty) ""
      else
        s"(doc['$path'].size() == 0 ? $nullValue : doc['$path'].value${painlessMethods.mkString("")})"

    override def painless(context: Option[PainlessContext]): String = {
      val base =
        context match {
          case Some(ctx) =>
            ctx.addParam(this).getOrElse("")
          case _ =>
            if (nullable)
              checkNotNull
            else
              paramName
        }
      val orderedFunctions = FunctionUtils.transformFunctions(this).reverse
      var expr = base
      orderedFunctions.zipWithIndex.foreach { case (f, idx) =>
        f match {
          case f: TransformFunction[_, _] => expr = f.toPainless(expr, idx, context)
          case f: PainlessScript          => expr = s"$expr${f.painless(context)}"
          case f                          => expr = f.toSQL(expr) // fallback
        }
      }
      expr
    }

    override def param: String = paramName

    private[this] var _nullable =
      this.name.nonEmpty && (!isAggregation || functions.size > 1)

    protected def nullable_=(b: Boolean): Unit = {
      _nullable = b
    }

    override def nullable: Boolean = _nullable

    def withNullable(b: Boolean): Identifier = {
      this.nullable = b
      this
    }

    override lazy val value: String =
      script match {
        case Some(s) => s
        case _       => painless(None)
      }

    def withNested(nested: Boolean): Identifier = this match {
      case g: GenericIdentifier => g.copy(nested = nested)
      case _                    => this
    }

    lazy val windows: Option[WindowFunction] =
      functions.collectFirst { case th: WindowFunction => th }

    def hasWindow: Boolean = windows.nonEmpty

    def isWindowing: Boolean = windows.exists(_.partitionBy.nonEmpty)

  }

  object Identifier {
    def apply(): Identifier = GenericIdentifier("")
    def apply(function: Function): Identifier = apply(List(function))
    def apply(functions: List[Function]): Identifier = apply().withFunctions(functions)
    def apply(name: String): Identifier = GenericIdentifier(name)
    def apply(name: String, function: Function): Identifier =
      apply(name).withFunctions(List(function))
  }

  case class GenericIdentifier(
    name: String,
    tableAlias: Option[String] = None,
    distinct: Boolean = false,
    nested: Boolean = false,
    limit: Option[Limit] = None,
    functions: List[Function] = List.empty,
    fieldAlias: Option[String] = None,
    bucket: Option[Bucket] = None,
    nestedElement: Option[NestedElement] = None,
    bucketPath: String = ""
  ) extends Identifier {

    def withFunctions(functions: List[Function]): Identifier = this.copy(functions = functions)

    override def withNullable(b: Boolean): Identifier = {
      val id = this.copy()
      id.nullable = b
      id
    }

    def update(request: SingleSearch): Identifier = {
      val bucketPath: String =
        request.groupBy match {
          case Some(gb) =>
            BucketPath(
              gb.buckets.map(b => request.bucketNames.getOrElse(b.identifier.identifierName, b))
            ).path
          case None /*if this.bucketPath.isEmpty*/ =>
            aggregateFunction match {
              case Some(af) => af.bucketPath
              case _        => this.bucketPath
            }
          //case _ => this.bucketPath
        }
      val parts: Seq[String] = name.split("\\.").toSeq
      val tableAlias = parts.head
      if (request.tableAliases.values.toSeq.contains(tableAlias)) {
        request.unnestAliases.find(_._1 == tableAlias) match {
          case Some(tuple) if !nested =>
            val nestedElement =
              request.unnests.get(tableAlias) match {
                case Some(unnest) => Some(request.toNestedElement(unnest))
                case None         => None
              }
            this
              .copy(
                tableAlias = Some(tableAlias),
                name = s"${tuple._2._1}.${parts.tail.mkString(".")}",
                nested = true,
                limit = tuple._2._2,
                fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias),
                bucket = request.bucketNames.get(identifierName).orElse(bucket),
                nestedElement = nestedElement,
                bucketPath = bucketPath
              )
              .withFunctions(this.updateFunctions(request))
          case Some(tuple) if nested =>
            this
              .copy(
                tableAlias = Some(tableAlias),
                name = s"${tuple._2._1}.${parts.tail.mkString(".")}",
                limit = tuple._2._2,
                fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias),
                bucket = request.bucketNames.get(identifierName).orElse(bucket),
                bucketPath = bucketPath
              )
              .withFunctions(this.updateFunctions(request))
          case None if nested =>
            this
              .copy(
                tableAlias = Some(tableAlias),
                fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias),
                bucket = request.bucketNames.get(identifierName).orElse(bucket),
                bucketPath = bucketPath
              )
              .withFunctions(this.updateFunctions(request))
          case _ =>
            this.copy(
              tableAlias = Some(tableAlias),
              name = parts.tail.mkString("."),
              fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias),
              bucket = request.bucketNames.get(identifierName).orElse(bucket),
              bucketPath = bucketPath
            )
        }
      } else {
        this
          .copy(
            fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias),
            bucket = request.bucketNames.get(identifierName).orElse(bucket),
            bucketPath = bucketPath
          )
          .withFunctions(this.updateFunctions(request))
      }
    }
  }
}
