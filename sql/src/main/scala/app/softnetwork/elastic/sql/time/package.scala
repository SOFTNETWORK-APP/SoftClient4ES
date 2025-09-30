package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.`type`._

import scala.util.matching.Regex

package object time {

  sealed trait TimeField extends PainlessScript with TokenRegex {
    override def painless: String = s"ChronoField.$timeField"

    override def nullable: Boolean = false

    def timeField: String

    override lazy val words: List[String] =
      List(timeField, timeField.replaceAll("_", ""), sql).distinct
  }

  object TimeField {
    case object YEAR extends Expr("YEAR") with TimeField {
      override val timeField: String = "YEAR"
    }
    case object MONTH_OF_YEAR extends Expr("MONTH") with TimeField {
      override val timeField: String = "MONTH_OF_YEAR"
    }
    case object DAY_OF_MONTH extends Expr("DAY") with TimeField {
      override val timeField: String = "DAY_OF_MONTH"
    }
    case object DAY_OF_WEEK extends Expr("WEEKDAY") with TimeField {
      override val timeField: String = "DAY_OF_WEEK"
    }
    case object DAY_OF_YEAR extends Expr("YEARDAY") with TimeField {
      override val timeField: String = "DAY_OF_YEAR"
    }
    case object HOUR_OF_DAY extends Expr("HOUR") with TimeField {
      override val timeField: String = "HOUR_OF_DAY"
    }
    case object MINUTE_OF_HOUR extends Expr("MINUTE") with TimeField {
      override val timeField: String = "MINUTE_OF_HOUR"
    }
    case object SECOND_OF_MINUTE extends Expr("SECOND") with TimeField {
      override val timeField: String = "SECOND_OF_MINUTE"
    }
    case object NANO_OF_SECOND extends Expr("NANOSECOND") with TimeField {
      override val timeField: String = "NANO_OF_SECOND"
    }
    case object MICRO_OF_SECOND extends Expr("MICROSECOND") with TimeField {
      override val timeField: String = "MICRO_OF_SECOND"
    }
    case object MILLI_OF_SECOND extends Expr("MILLISECOND") with TimeField {
      override val timeField: String = "MILLI_OF_SECOND"
    }
    case object EPOCH_DAY extends Expr("EPOCHDAY") with TimeField {
      override val timeField: String = "EPOCH_DAY"
    }
    case object OFFSET_SECONDS extends Expr("OFFSET_SECONDS") with TimeField {
      override val timeField: String = "OFFSET_SECONDS"
    }
  }

  sealed trait IsoField extends TimeField {
    def isoField: String
    def timeField: String = isoField
    override def painless: String = s"java.time.temporal.IsoFields.$isoField"
  }

  object IsoField {

    case object QUARTER_OF_YEAR extends Expr("QUARTER") with IsoField {
      override val isoField: String = "QUARTER_OF_YEAR"
    }

    case object WEEK_OF_WEEK_BASED_YEAR extends Expr("WEEK") with IsoField {
      override val isoField: String = "WEEK_OF_WEEK_BASED_YEAR"
    }

  }

  sealed trait TimeUnit extends PainlessScript with MathScript {
    lazy val regex: Regex = s"\\b(?i)$sql(s)?\\b".r

    def timeUnit: String = sql.toUpperCase() + "S"

    override def painless: String = s"ChronoUnit.$timeUnit"

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
