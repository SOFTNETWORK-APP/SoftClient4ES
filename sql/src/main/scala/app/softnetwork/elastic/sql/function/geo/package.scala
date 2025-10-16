package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.`type`.{SQLAny, SQLDouble, SQLTypes}
import app.softnetwork.elastic.sql.{
  DoubleValue,
  Expr,
  Identifier,
  PainlessContext,
  PainlessParams,
  PainlessScript,
  Token,
  TokenRegex,
  Updateable
}
import app.softnetwork.elastic.sql.operator.Operator
import app.softnetwork.elastic.sql.query.SQLSearchRequest

package object geo {

  case object Point extends Expr("POINT") with TokenRegex

  case class Point(lat: DoubleValue, lon: DoubleValue) extends Token {
    override def sql: String = s"POINT($lat, $lon)"
  }

  sealed trait DistanceUnit extends TokenRegex

  object DistanceUnit {
    def convertToMeters(value: Double, unit: DistanceUnit): Double =
      unit match {
        case Kilometers    => value * 1000.0
        case Meters        => value
        case Centimeters   => value / 100.0
        case Millimeters   => value / 1000.0
        case Miles         => value * 1609.34
        case Yards         => value * 0.9144
        case Feet          => value * 0.3048
        case Inches        => value * 0.0254
        case NauticalMiles => value * 1852.0
      }
  }

  sealed trait MetricUnit extends DistanceUnit

  case object Kilometers extends Expr("km") with MetricUnit
  case object Meters extends Expr("m") with MetricUnit
  case object Centimeters extends Expr("cm") with MetricUnit
  case object Millimeters extends Expr("mm") with MetricUnit

  sealed trait ImperialUnit extends DistanceUnit
  case object Miles extends Expr("mi") with ImperialUnit
  case object Yards extends Expr("yd") with ImperialUnit
  case object Feet extends Expr("ft") with ImperialUnit
  case object Inches extends Expr("in") with ImperialUnit

  case object NauticalMiles extends Expr("nmi") with DistanceUnit

  case object Distance extends Expr("ST_DISTANCE") with Function with Operator {
    override def words: List[String] = List(sql, "DISTANCE")

    override def painless(context: Option[PainlessContext] = None): String = ".arcDistance"

    def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
      val R = 6371e3 // Radius of the earth in meters
      val r1 = lat1.toRadians
      val r2 = lat2.toRadians
      val rlat = (lat2 - lat1).toRadians
      val rlon = (lon2 - lon1).toRadians

      val a = Math.sin(rlat / 2) * Math.sin(rlat / 2) +
        Math.cos(r1) * Math.cos(r2) *
        Math.sin(rlon / 2) * Math.sin(rlon / 2)
      val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

      (R * c).round.toDouble // in meters
    }
  }

  case class Distance(from: Either[Identifier, Point], to: Either[Identifier, Point])
      extends FunctionN[SQLAny, SQLDouble]
      with PainlessParams
      with Updateable {

    override def update(request: SQLSearchRequest): Distance = this.copy(
      from = from.fold(id => Left(id.update(request)), p => Right(p)),
      to = to.fold(id => Left(id.update(request)), p => Right(p))
    )

    override def fun: Option[PainlessScript] = Some(Distance)

    override def inputType: SQLAny = SQLTypes.Any
    override def outputType: SQLDouble = SQLTypes.Double

    override def args: List[PainlessScript] = List.empty

    override def sql: String =
      s"$Distance(${from.fold(identity, identity)}, ${to.fold(identity, identity)})"

    private[this] lazy val (fromId, toId) = {
      val fromId = from.fold(Some(_), _ => None)
      val toId = to.fold(Some(_), _ => None)
      (fromId, toId)
    }

    lazy val identifiers: List[Identifier] = List(fromId, toId).flatten

    lazy val oneIdentifier: Boolean = identifiers.size == 1

    private[this] lazy val (fromPoint, toPoint) = {
      val fromPoint = from.fold(_ => None, Some(_))
      val toPoint = to.fold(_ => None, Some(_))
      (fromPoint, toPoint)
    }

    lazy val points: List[Point] = List(fromPoint, toPoint).flatten

    override def nullable: Boolean =
      from.fold(identity, identity).nullable || to.fold(identity, identity).nullable

    override def params: Map[String, Any] =
      if (oneIdentifier)
        Map(
          "lat" -> points.head.lat.value,
          "lon" -> points.head.lon.value
        )
      else
        Map.empty

    override def painless(context: Option[PainlessContext]): String = {
      val nullCheck =
        identifiers.zipWithIndex
          .map { case (_, i) => s"arg$i == null" }
          .mkString(" || ")

      val assignments =
        identifiers.zipWithIndex
          .map { case (a, i) =>
            val name = a.name
            s"def arg$i = (!doc.containsKey('$name') || doc['$name'].empty ? ${a.nullValue} : doc['$name']);"
          }
          .mkString(" ")

      val ret =
        if (oneIdentifier) {
          s"arg0${fun.map(_.painless(context)).getOrElse("")}(params.lat, params.lon)"
        } else if (identifiers.isEmpty) {
          s"${Distance.haversine(
            fromPoint.get.lat.value,
            fromPoint.get.lon.value,
            toPoint.get.lat.value,
            toPoint.get.lon.value
          )}"
        } else {
          s"arg0${fun.map(_.painless(context)).getOrElse("")}(arg1.lat, arg1.lon)"
        }

      if (identifiers.nonEmpty)
        s"($assignments ($nullCheck) ? null : $ret)"
      else
        ret
    }
  }

}
