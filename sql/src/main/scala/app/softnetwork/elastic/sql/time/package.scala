package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.`type`._

import scala.util.matching.Regex

package object time {

  sealed trait TimeField extends PainlessScript with TokenRegex {
    override def painless: String = s"ChronoField.$sql"

    override def nullable: Boolean = false
  }

  object TimeField {
    case object YEAR extends Expr("YEAR") with TimeField
    case object MONTH_OF_YEAR extends Expr("MONTH_OF_YEAR") with TimeField {
      override val words: List[String] = List(sql, "MONTHOFYEAR", "MONTH")
    }
    case object DAY_OF_MONTH extends Expr("DAY_OF_MONTH") with TimeField {
      override val words: List[String] = List(sql, "DAYOFMONTH", "DAY")
    }
    case object DAY_OF_WEEK extends Expr("DAY_OF_WEEK") with TimeField {
      override val words: List[String] = List(sql, "DAYOFWEEK", "WEEKDAY")
    }
    case object DAY_OF_YEAR extends Expr("DAY_OF_YEAR") with TimeField {
      override val words: List[String] = List(sql, "DAYOFYEAR")
    }
    case object HOUR_OF_DAY extends Expr("HOUR_OF_DAY") with TimeField {
      override val words: List[String] = List(sql, "HOUROFDAY", "HOUR")
    }
    case object MINUTE_OF_HOUR extends Expr("MINUTE_OF_HOUR") with TimeField {
      override val words: List[String] = List(sql, "MINUTEOFHOUR", "MINUTE")
    }
    case object SECOND_OF_MINUTE extends Expr("SECOND_OF_MINUTE") with TimeField {
      override val words: List[String] = List(sql, "SECONDOFMINUTE", "SECOND")
    }
    case object NANO_OF_SECOND extends Expr("NANO_OF_SECOND") with TimeField {
      override val words: List[String] = List(sql, "NANOFSECOND", "NANOSECOND")
    }
    case object MICRO_OF_SECOND extends Expr("MICRO_OF_SECOND") with TimeField {
      override val words: List[String] = List(sql, "MICROOFSECOND", "MICROSECOND")
    }
    case object MILLI_OF_SECOND extends Expr("MILLI_OF_SECOND") with TimeField {
      override val words: List[String] = List(sql, "MILLIOFSECOND", "MILLISECOND")
    }
    case object EPOCH_DAY extends Expr("EPOCH_DAY") with TimeField {
      override val words: List[String] = List(sql, "EPOCHDAY")
    }
    case object OFFSET_SECONDS extends Expr("OFFSET_SECONDS") with TimeField {
      override val words: List[String] = List(sql, "OFFSETSECONDS")
    }
  }

  sealed trait TimeUnit extends PainlessScript with MathScript {
    lazy val regex: Regex = s"\\b(?i)$sql(s)?\\b".r

    override def painless: String = s"ChronoUnit.${sql.toUpperCase()}S"

    override def nullable: Boolean = false
  }

  sealed trait CalendarUnit extends TimeUnit
  sealed trait FixedUnit extends TimeUnit

  object TimeUnit {
    case object YEARS extends Expr("YEAR") with CalendarUnit {
      override def script: String = "y"
    }
    case object MONTHS extends Expr("MONTH") with CalendarUnit {
      override def script: String = "M"
    }
    case object QUARTERS extends Expr("QUARTER") with CalendarUnit {
      override def script: String = throw new IllegalArgumentException(
        "Quarter must be converted to months (value * 3) before creating date-math"
      )
    }
    case object WEEKS extends Expr("WEEK") with CalendarUnit {
      override def script: String = "w"
    }

    case object DAYS extends Expr("DAY") with CalendarUnit with FixedUnit {
      override def script: String = "d"
    }

    case object HOURS extends Expr("HOUR") with FixedUnit {
      override def script: String = "H"
    }
    case object MINUTES extends Expr("MINUTE") with FixedUnit {
      override def script: String = "m"
    }
    case object SECONDS extends Expr("SECOND") with FixedUnit {
      override def script: String = "s"
    }

  }

  case object Interval extends Expr("INTERVAL") with TokenRegex

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
            case YEARS | MONTHS | DAYS     => Right(SQLTypes.Date)
            case HOURS | MINUTES | SECONDS => Right(SQLTypes.Timestamp)
            case _                         => Left(s"Invalid interval unit $unit for DATE")
          }
        case SQLTypes.Time =>
          unit match {
            case HOURS | MINUTES | SECONDS => Right(SQLTypes.Time)
            case _                         => Left(s"Invalid interval unit $unit for TIME")
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
      case CalendarInterval(v, QUARTERS) => s"${v * 3}M"
      case CalendarInterval(v, u)        => s"$v${u.script}"
      case FixedInterval(v, u)           => s"$v${u.script}"
    }
  }

}
