package app.softnetwork.elastic

import java.util.regex.Pattern
import scala.reflect.runtime.universe._
import scala.util.Try
import scala.util.matching.Regex

/** Created by smanciot on 27/06/2018.
  */
package object sql {

  import scala.language.implicitConversions

  implicit def asString(token: Option[_ <: SQLToken]): String = token match {
    case Some(t) => t.toString
    case _       => ""
  }

  trait SQLToken extends Serializable {
    def sql: String
    override def toString: String = sql
  }

  trait PainlessScript extends SQLToken {
    def painless: String
  }

  trait MathScript extends SQLToken {
    def script: String
  }

  trait SQLTokenWithFunction extends SQLToken {
    def functions: List[SQLFunction]

    lazy val aggregateFunction: Option[AggregateFunction] = functions.headOption match {
      case Some(af: AggregateFunction) => Some(af)
      case other =>
        Console.println(this)
        None
    }

    lazy val aggregation: Boolean = aggregateFunction.isDefined

    def validate(): Either[String, Unit] = {
      SQLValidator.validateChain(functions)
    }

  }

  trait Updateable extends SQLToken {
    def update(request: SQLSearchRequest): Updateable
  }

  abstract class SQLExpr(override val sql: String) extends SQLToken

  case object Distinct extends SQLExpr("distinct") with SQLRegex

  abstract class SQLValue[+T](val value: T)(implicit ev$1: T => Ordered[T])
      extends SQLToken
      with PainlessScript {
    def choose[R >: T](
      values: Seq[R],
      operator: Option[SQLExpressionOperator],
      separator: String = "|"
    )(implicit ev: R => Ordered[R]): Option[R] = {
      if (values.isEmpty)
        None
      else
        operator match {
          case Some(_: Eq.type) => values.find(_ == value)
          case Some(_: Ne.type) => values.find(_ != value)
          case Some(_: Ge.type) => values.filter(_ >= value).sorted.reverse.headOption
          case Some(_: Gt.type) => values.filter(_ > value).sorted.reverse.headOption
          case Some(_: Le.type) => values.filter(_ <= value).sorted.headOption
          case Some(_: Lt.type) => values.filter(_ < value).sorted.headOption
          case _                => values.headOption
        }
    }
    def painless: String = value match {
      case s: String  => s""""$s""""
      case b: Boolean => b.toString
      case n: Number  => n.toString
      case _          => value.toString
    }
  }

  case class SQLBoolean(override val value: Boolean) extends SQLValue[Boolean](value) {
    override def sql: String = value.toString
  }

  case class SQLLiteral(override val value: String) extends SQLValue[String](value) {
    override def sql: String = s""""$value""""
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
      operator: Option[SQLExpressionOperator],
      separator: String = "|"
    )(implicit ev: R => Ordered[R]): Option[R] = {
      operator match {
        case Some(_: Eq.type)   => values.find(v => v.toString contentEquals value)
        case Some(_: Ne.type)   => values.find(v => !(v.toString contentEquals value))
        case Some(_: Like.type) => values.find(v => pattern.matcher(v.toString).matches())
        case None               => Some(values.mkString(separator))
        case _                  => super.choose(values, operator, separator)
      }
    }
  }

  sealed abstract class SQLNumeric[T: Numeric](override val value: T)(implicit
    ev$1: T => Ordered[T]
  ) extends SQLValue[T](value) {
    override def sql: String = value.toString
    override def choose[R >: T](
      values: Seq[R],
      operator: Option[SQLExpressionOperator],
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
  }

  case class SQLLong(override val value: Long) extends SQLNumeric[Long](value)

  case class SQLDouble(override val value: Double) extends SQLNumeric[Double](value)

  sealed abstract class SQLFromTo[+T](val from: SQLValue[T], val to: SQLValue[T]) extends SQLToken {
    override def sql = s"${from.sql} and ${to.sql}"
  }

  case class SQLLiteralFromTo(override val from: SQLLiteral, override val to: SQLLiteral)
      extends SQLFromTo[String](from, to) {
    def between: Seq[String] => Boolean = {
      _.exists { s => s >= from.value && s <= to.value }
    }
    def notBetween: Seq[String] => Boolean = {
      _.forall { s => s < from.value || s > to.value }
    }
  }

  case class SQLLongFromTo(override val from: SQLLong, override val to: SQLLong)
      extends SQLFromTo[Long](from, to) {
    def between: Seq[Long] => Boolean = {
      _.exists { n => n >= from.value && n <= to.value }
    }
    def notBetween: Seq[Long] => Boolean = {
      _.forall { n => n < from.value || n > to.value }
    }
  }

  case class SQLDoubleFromTo(override val from: SQLDouble, override val to: SQLDouble)
      extends SQLFromTo[Double](from, to) {
    def between: Seq[Double] => Boolean = {
      _.exists { n => n >= from.value && n <= to.value }
    }
    def notBetween: Seq[Double] => Boolean = {
      _.forall { n => n < from.value || n > to.value }
    }
  }

  sealed abstract class SQLValues[+R: TypeTag, +T <: SQLValue[R]](val values: Seq[T])
      extends SQLToken {
    override def sql = s"(${values.map(_.sql).mkString(",")})"
    lazy val innerValues: Seq[R] = values.map(_.value)
  }

  case class SQLLiteralValues(override val values: Seq[SQLLiteral])
      extends SQLValues[String, SQLValue[String]](values) {
    def eq: Seq[String] => Boolean = {
      _.exists { s => innerValues.exists(_.contentEquals(s)) }
    }
    def ne: Seq[String] => Boolean = {
      _.forall { s => innerValues.forall(!_.contentEquals(s)) }
    }
  }

  class SQLNumericValues[R: TypeTag](override val values: Seq[SQLNumeric[R]])
      extends SQLValues[R, SQLNumeric[R]](values) {
    def eq: Seq[R] => Boolean = {
      _.exists { n => innerValues.contains(n) }
    }
    def ne: Seq[R] => Boolean = {
      _.forall { n => !innerValues.contains(n) }
    }
  }

  case class SQLLongValues(override val values: Seq[SQLLong]) extends SQLNumericValues[Long](values)

  case class SQLDoubleValues(override val values: Seq[SQLDouble])
      extends SQLNumericValues[Double](values)

  def choose[T](
    values: Seq[T],
    criteria: Option[SQLCriteria],
    function: Option[SQLFunction] = None
  )(implicit ev$1: T => Ordered[T]): Option[T] = {
    criteria match {
      case Some(SQLExpression(_, operator, value: SQLValue[T] @unchecked, _)) =>
        value.choose[T](values, Some(operator))
      case _ =>
        function match {
          case Some(_: Min.type) => Some(values.min)
          case Some(_: Max.type) => Some(values.max)
          // FIXME        case Some(_: SQLSum.type) => Some(values.sum)
          // FIXME        case Some(_: SQLAvg.type) => Some(values.sum / values.length  )
          case _ => values.headOption
        }
    }
  }

  def toRegex(value: String): String = {
    val startWith = value.startsWith("%")
    val endWith = value.endsWith("%")
    val v =
      if (startWith && endWith)
        value.substring(1, value.length - 1)
      else if (startWith)
        value.substring(1)
      else if (endWith)
        value.substring(0, value.length - 1)
      else
        value
    s"""${if (startWith) ".*"}$v${if (endWith) ".*"}"""
  }

  case object Alias extends SQLExpr("as") with SQLRegex

  case class SQLAlias(alias: String) extends SQLExpr(s" ${Alias.sql} $alias")

  trait SQLRegex extends SQLToken {
    lazy val regex: Regex = s"\\b(?i)$sql\\b".r
  }

  trait SQLSource extends Updateable {
    def name: String
    def update(request: SQLSearchRequest): SQLSource
  }

  case class SQLIdentifier(
    name: String,
    tableAlias: Option[String] = None,
    distinct: Boolean = false,
    nested: Boolean = false,
    limit: Option[SQLLimit] = None,
    functions: List[SQLFunction] = List.empty,
    fieldAlias: Option[String] = None,
    bucket: Option[SQLBucket] = None
  ) extends SQLExpr({
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
      })
      with SQLSource
      with SQLTokenWithFunction {

    lazy val identifierName: String =
      functions.reverse.foldLeft(name)((expr, fun) => {
        fun.toSQL(expr)
      })

    lazy val nestedType: Option[String] = if (nested) Some(name.split('.').head) else None

    lazy val innerHitsName: Option[String] = if (nested) tableAlias else None

    lazy val aliasOrName: String = fieldAlias.getOrElse(name)

    def update(request: SQLSearchRequest): SQLIdentifier = {
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
