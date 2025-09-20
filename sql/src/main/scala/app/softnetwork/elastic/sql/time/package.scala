package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.`type`._

import scala.util.matching.Regex

package object time {

  sealed trait TimeUnit extends PainlessScript with MathScript {
    lazy val regex: Regex = s"\\b(?i)$sql(s)?\\b".r

    override def painless: String = s"ChronoUnit.${sql.toUpperCase()}S"

    override def nullable: Boolean = false
  }

  sealed trait CalendarUnit extends TimeUnit
  sealed trait FixedUnit extends TimeUnit

  object TimeUnit {
    case object Year extends Expr("year") with CalendarUnit {
      override def script: String = "y"
    }
    case object Month extends Expr("month") with CalendarUnit {
      override def script: String = "M"
    }
    case object Quarter extends Expr("quarter") with CalendarUnit {
      override def script: String = throw new IllegalArgumentException(
        "Quarter must be converted to months (value * 3) before creating date-math"
      )
    }
    case object Week extends Expr("week") with CalendarUnit {
      override def script: String = "w"
    }

    case object Day extends Expr("day") with CalendarUnit with FixedUnit {
      override def script: String = "d"
    }

    case object Hour extends Expr("hour") with FixedUnit {
      override def script: String = "H"
    }
    case object Minute extends Expr("minute") with FixedUnit {
      override def script: String = "m"
    }
    case object Second extends Expr("second") with FixedUnit {
      override def script: String = "s"
    }

  }

  case object Interval extends Expr("interval") with TokenRegex

  sealed trait TimeInterval extends PainlessScript with MathScript {
    def value: Int
    def unit: TimeUnit
    override def sql: String = s"$Interval $value ${unit.sql}"

    override def painless: String = s"$value, ${unit.painless}"

    override def script: String = TimeInterval.script(this)

    def checkType(in: SQLType): Either[String, SQLType] = {
      import TimeUnit._
      in match {
        case SQLTypes.Date =>
          unit match {
            case Year | Month | Day     => Right(SQLTypes.Date)
            case Hour | Minute | Second => Right(SQLTypes.Timestamp)
            case _                      => Left(s"Invalid interval unit $unit for DATE")
          }
        case SQLTypes.Time =>
          unit match {
            case Hour | Minute | Second => Right(SQLTypes.Time)
            case _                      => Left(s"Invalid interval unit $unit for TIME")
          }
        case SQLTypes.DateTime =>
          Right(SQLTypes.Timestamp)
        case SQLTypes.Timestamp =>
          Right(SQLTypes.Timestamp)
        case SQLTypes.Temporal =>
          Right(SQLTypes.Timestamp)
        case _ =>
          Left(s"Intervals not supported for type $in")
      }
    }

    override def nullable: Boolean = false
  }

  import TimeUnit._

  case class CalendarInterval(value: Int, unit: CalendarUnit) extends TimeInterval
  case class FixedInterval(value: Int, unit: FixedUnit) extends TimeInterval

  object TimeInterval {
    def apply(value: Int, unit: TimeUnit): TimeInterval = unit match {
      case cu: CalendarUnit => CalendarInterval(value, cu)
      case fu: FixedUnit    => FixedInterval(value, fu)
    }
    def script(interval: TimeInterval): String = interval match {
      case CalendarInterval(v, Quarter) => s"${v * 3}M"
      case CalendarInterval(v, u)       => s"$v${u.script}"
      case FixedInterval(v, u)          => s"$v${u.script}"
    }
  }

}
