package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.`type`.{SQLAny, SQLDouble, SQLTypes}
import app.softnetwork.elastic.sql.{
  DoubleValue,
  Expr,
  Identifier,
  PainlessParams,
  PainlessScript,
  Token,
  TokenRegex
}
import app.softnetwork.elastic.sql.operator.Operator

package object geo {

  case object Point extends Expr("POINT") with TokenRegex

  case class Point(lat: DoubleValue, lon: DoubleValue) extends Token {
    override def sql: String = s"POINT($lat, $lon)"
  }

  case object Distance extends Expr("ST_DISTANCE") with Function with Operator {
    override def words: List[String] = List(sql, "DISTANCE")

    override def painless: String = ".arcDistance"
  }

  case class Distance(from: Either[Identifier, Point], to: Either[Identifier, Point])
      extends FunctionN[SQLAny, SQLDouble]
      with PainlessParams {

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

    private[this] lazy val identifiers: List[Identifier] = List(fromId, toId).flatten

    private[this] lazy val (fromPoint, toPoint) = {
      val fromPoint = from.fold(_ => None, Some(_))
      val toPoint = to.fold(_ => None, Some(_))
      (fromPoint, toPoint)
    }

    private[this] lazy val points: List[Point] = List(fromPoint, toPoint).flatten

    private[this] lazy val oneIdentifier: Boolean = identifiers.size == 1

    override def nullable: Boolean =
      from.fold(identity, identity).nullable || to.fold(identity, identity).nullable

    override def params: Map[String, Any] =
      if (oneIdentifier)
        Map(
          "lat" -> points.head.lat.value,
          "lon" -> points.head.lon.value
        )
      else if (identifiers.isEmpty)
        Map(
          "lat1" -> fromPoint.get.lat.value,
          "lon1" -> fromPoint.get.lon.value,
          "lat2" -> toPoint.get.lat.value,
          "lon2" -> toPoint.get.lon.value
        )
      else
        Map.empty

    override def painless: String = {
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
          s"arg0${fun.map(_.painless).getOrElse("")}(params.lat, params.lon)"
        } else if (identifiers.isEmpty) {
          s"new GeoPoint(params.lat1, params.lon1)${fun.map(_.painless).getOrElse("")}(params.lat2, params.lon2)"
        } else {
          s"arg0${fun.map(_.painless).getOrElse("")}(arg1.lat, arg1.lon)"
        }

      if (identifiers.nonEmpty)
        s"($assignments ($nullCheck) ? null : $ret)"
      else
        ret
    }
  }

}
