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
    case Some(t) => t.sql
    case _       => ""
  }

  trait SQLToken extends Serializable {
    def sql: String
    override def toString: String = sql
  }

  trait SQLTokenWithFunction extends SQLToken {
    def function: Option[SQLFunction]

    lazy val aggregateFunction: Option[AggregateFunction] = function match {
      case Some(af: AggregateFunction) => Some(af)
      case _                           => None
    }

    lazy val aggregation: Boolean = aggregateFunction.isDefined
  }

  trait Updateable extends SQLToken {
    def update(request: SQLSearchRequest): Updateable
  }

  abstract class SQLExpr(override val sql: String) extends SQLToken

  case object Distinct extends SQLExpr("distinct") with SQLRegex

  abstract class SQLValue[+T](val value: T)(implicit ev$1: T => Ordered[T]) extends SQLToken {
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
  }

  case class SQLBoolean(value: Boolean) extends SQLToken {
    override def sql: String = s"$value"
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

  abstract class SQLNumeric[+T](override val value: T)(implicit ev$1: T => Ordered[T])
      extends SQLValue[T](value) {
    override def sql: String = s"$value"
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
  }

  case class SQLInt(override val value: Int) extends SQLNumeric[Int](value) {
    def max: Seq[Int] => Int = x => Try(x.max).getOrElse(0)
    def min: Seq[Int] => Int = x => Try(x.min).getOrElse(0)
    def eq: Seq[Int] => Boolean = {
      _.exists { _ == value }
    }
    def ne: Seq[Int] => Boolean = {
      _.forall { _ != value }
    }
  }

  case class SQLDouble(override val value: Double) extends SQLNumeric[Double](value) {
    def max: Seq[Double] => Double = x => Try(x.max).getOrElse(0)
    def min: Seq[Double] => Double = x => Try(x.min).getOrElse(0)
    def eq: Seq[Double] => Boolean = {
      _.exists { _ == value }
    }
    def ne: Seq[Double] => Boolean = {
      _.forall { _ != value }
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

  case class SQLNumericValues[R: TypeTag](override val values: Seq[SQLNumeric[R]])
      extends SQLValues[R, SQLNumeric[R]](values) {
    def eq: Seq[R] => Boolean = {
      _.exists { n => innerValues.contains(n) }
    }
    def ne: Seq[R] => Boolean = {
      _.forall { n => !innerValues.contains(n) }
    }
  }

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
    function: Option[SQLFunction] = None,
    fieldAlias: Option[String] = None
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
        function match {
          case Some(f) => s"$f($sql)"
          case _       => sql
        }
      })
      with SQLSource
      with SQLTokenWithFunction {

    lazy val aggregationName: Option[String] =
      if (aggregation) fieldAlias.orElse(Option(name)) else None

    lazy val identifierName: String =
      (function match {
        case Some(f) => s"${f.sql}($name)"
        case _       => name
      }).toLowerCase

    lazy val nestedType: Option[String] = if (nested) Some(name.split('.').head) else None

    lazy val innerHitsName: Option[String] = if (nested) tableAlias else None

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
              fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias)
            )
          case _ =>
            this.copy(
              tableAlias = Some(parts.head),
              name = parts.tail.mkString("."),
              fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias)
            )
        }
      } else {
        this.copy(
          fieldAlias = request.fieldAliases.get(identifierName).orElse(fieldAlias)
        )
      }
    }
  }
}
