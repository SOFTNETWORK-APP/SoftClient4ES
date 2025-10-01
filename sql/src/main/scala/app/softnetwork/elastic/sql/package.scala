package app.softnetwork.elastic

import app.softnetwork.elastic.sql.function.aggregate.{MAX, MIN}
import app.softnetwork.elastic.sql.function.geo.DistanceUnit
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

  import app.softnetwork.elastic.sql.function._
  import app.softnetwork.elastic.sql.`type`._

  import scala.language.implicitConversions

  implicit def asString(token: Option[_ <: Token]): String = token match {
    case Some(t) => t.toString
    case _       => ""
  }

  trait Token extends Serializable with Validation {
    def sql: String
    override def toString: String = sql
    def baseType: SQLType = SQLTypes.Any
    def in: SQLType = baseType
    private[this] var _out: SQLType = SQLTypes.Null
    def out: SQLType = if (_out == SQLTypes.Null) baseType else _out
    def out_=(t: SQLType): Unit = {
      _out = t
    }
    def cast(targetType: SQLType): SQLType = {
      this.out = targetType
      this.out
    }
    def system: Boolean = false
    def nullable: Boolean = !system
    def dateMathScript: Boolean = false
  }

  trait PainlessScript extends Token {
    def painless: String
    def nullValue: String = "null"
  }

  trait PainlessParams extends PainlessScript {
    def params: Map[String, Any]
  }

  trait DateMathScript extends Token {
    def script: String
    override def dateMathScript: Boolean = true
  }

  trait Updateable extends Token {
    def update(request: SQLSearchRequest): Updateable
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
    override def painless: String =
      SQLTypeUtils.coerce(
        value match {
          case s: String  => s""""$s""""
          case b: Boolean => b.toString
          case n: Number  => n.toString
          case _          => value.toString
        },
        this.baseType,
        this.out,
        nullable = false
      )

    override def nullable: Boolean = false
  }

  case object Null extends Value[Null](null) with TokenRegex {
    override def sql: String = "NULL"
    override def painless: String = "null"
    override def nullable: Boolean = true
    override def baseType: SQLType = SQLTypes.Null
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
    override def painless: String = "Math.PI"
    override def baseType: SQLNumeric = SQLTypes.Double
  }

  case object EValue extends Value[Double](Math.E) with TokenRegex {
    override def sql: String = "E"
    override def painless: String = "Math.E"
    override def baseType: SQLNumeric = SQLTypes.Double
  }

  case class GeoDistance(longValue: LongValue, unit: DistanceUnit)
      extends NumericValue[Double](DistanceUnit.convertToMeters(longValue.value, unit))
      with PainlessScript {
    override def baseType: SQLNumeric = SQLTypes.Double
    override def sql: String = s"$longValue $unit"
    def geoDistance: String = s"$longValue$unit"
    override def painless: String = s"$value"
  }

  sealed abstract class FromTo[+T](val from: Value[T], val to: Value[T]) extends Token {
    override def sql = s"${from.sql} AND ${to.sql}"

    override def baseType: SQLType =
      SQLTypeUtils.leastCommonSuperType(List(from.baseType, to.baseType))

    override def validate(): Either[String, Unit] =
      Validator.validateTypesMatching(from.out, to.out)
  }

  case class LiteralFromTo(override val from: StringValue, override val to: StringValue)
      extends FromTo[String](from, to) {
    def between: Seq[String] => Boolean = {
      _.exists { s => s >= from.value && s <= to.value }
    }
    def notBetween: Seq[String] => Boolean = {
      _.forall { s => s < from.value || s > to.value }
    }
  }

  case class LongFromTo(override val from: LongValue, override val to: LongValue)
      extends FromTo[Long](from, to) {
    def between: Seq[Long] => Boolean = {
      _.exists { n => n >= from.value && n <= to.value }
    }
    def notBetween: Seq[Long] => Boolean = {
      _.forall { n => n < from.value || n > to.value }
    }
  }

  case class DoubleFromTo(override val from: DoubleValue, override val to: DoubleValue)
      extends FromTo[Double](from, to) {
    def between: Seq[Double] => Boolean = {
      _.exists { n => n >= from.value && n <= to.value }
    }
    def notBetween: Seq[Double] => Boolean = {
      _.forall { n => n < from.value || n > to.value }
    }
  }

  case class GeoDistanceFromTo(override val from: GeoDistance, override val to: GeoDistance)
      extends FromTo[Double](from, to) {
    def between: Seq[Double] => Boolean = {
      _.exists { n => n >= from.value && n <= to.value }
    }
    def notBetween: Seq[Double] => Boolean = {
      _.forall { n => n < from.value || n > to.value }
    }
  }

  sealed abstract class Values[+R: TypeTag, +T <: Value[R]](val values: Seq[T])
      extends Token
      with PainlessScript {
    override def sql = s"(${values.map(_.sql).mkString(",")})"
    override def painless: String = s"[${values.map(_.painless).mkString(",")}]"
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
    def update(request: SQLSearchRequest): Source
  }

  sealed trait Identifier extends Token with Source with FunctionChain with PainlessScript {
    def name: String

    def withFunctions(functions: List[Function]): Identifier

    def update(request: SQLSearchRequest): Identifier

    def tableAlias: Option[String]
    def distinct: Boolean
    def nested: Boolean
    def limit: Option[Limit]
    def fieldAlias: Option[String]
    def bucket: Option[Bucket]
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
      }) // FIXME use AliasUtils.normalize?

    lazy val nestedType: Option[String] = if (nested) Some(name.split('.').head) else None

    lazy val innerHitsName: Option[String] = if (nested) tableAlias else None

    lazy val aliasOrName: String = fieldAlias.getOrElse(name)

    def paramName: String =
      if (aggregation && functions.size == 1) s"params.$aliasOrName"
      else if (name.nonEmpty)
        s"doc['$name'].value"
      else ""

    def toPainless(base: String): String = {
      val orderedFunctions = FunctionUtils.transformFunctions(this).reverse
      var expr = base
      orderedFunctions.zipWithIndex.foreach { case (f, idx) =>
        f match {
          case f: TransformFunction[_, _] => expr = f.toPainless(expr, idx)
          case f: PainlessScript          => expr = s"$expr${f.painless}"
          case f                          => expr = f.toSQL(expr) // fallback
        }
      }
      expr
    }

    def checkNotNull: String =
      if (name.isEmpty) ""
      else
        s"(!doc.containsKey('$name') || doc['$name'].empty ? $nullValue : doc['$name'].value)"

    override def painless: String = toPainless(
      if (nullable)
        checkNotNull
      else
        paramName
    )

    private[this] var _nullable =
      this.name.nonEmpty && (!aggregation || functions.size > 1)

    def nullable_=(b: Boolean): Unit = {
      _nullable = b
    }

    override def nullable: Boolean = _nullable

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
    bucket: Option[Bucket] = None
  ) extends Identifier {

    def withFunctions(functions: List[Function]): Identifier = this.copy(functions = functions)

    def update(request: SQLSearchRequest): Identifier = {
      val parts: Seq[String] = name.split("\\.").toSeq
      if (request.tableAliases.values.toSeq.contains(parts.head)) {
        request.unnests.find(_._1 == parts.head) match {
          case Some(tuple) =>
            this.copy(
              tableAlias = Some(parts.head),
              name = s"${tuple._2}.${parts.tail.mkString(".")}",
              nested = true,
              limit = tuple._3,
              fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias),
              bucket = request.bucketNames.get(identifierName).orElse(bucket)
            )
          case _ =>
            this.copy(
              tableAlias = Some(parts.head),
              name = parts.tail.mkString("."),
              fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias),
              bucket = request.bucketNames.get(identifierName).orElse(bucket)
            )
        }
      } else {
        this.copy(
          fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias),
          bucket = request.bucketNames.get(identifierName).orElse(bucket)
        )
      }
    }
  }
}
