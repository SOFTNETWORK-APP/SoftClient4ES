package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.{GeoDistance, Identifier}
import app.softnetwork.elastic.sql.function.geo._
import app.softnetwork.elastic.sql.parser.Parser

package object geo {

  trait GeoParser { self: Parser =>

    def point: PackratParser[Point] =
      Point.regex ~> start ~> double ~ separator ~ double <~ end ^^ { case lat ~ _ ~ lon =>
        Point(lat, lon)
      }

    def pointOrIdentifier: PackratParser[Either[Identifier, Point]] =
      (point | identifier) ^^ {
        case id: Identifier => Left(id)
        case p: Point       => Right(p)
      }

    def distance: PackratParser[Distance] =
      Distance.regex ~> start ~> pointOrIdentifier ~ separator ~ pointOrIdentifier <~ end ^^ {
        case from ~ _ ~ to => Distance(from, to)
      }

    def kilometers: PackratParser[DistanceUnit] = Kilometers.regex ^^ (_ => Kilometers)
    def meters: PackratParser[DistanceUnit] = Meters.regex ^^ (_ => Meters)
    def centimeters: PackratParser[DistanceUnit] = Centimeters.regex ^^ (_ => Centimeters)
    def millimeters: PackratParser[DistanceUnit] = Millimeters.regex ^^ (_ => Millimeters)
    def miles: PackratParser[DistanceUnit] = Miles.regex ^^ (_ => Miles)
    def yards: PackratParser[DistanceUnit] = Yards.regex ^^ (_ => Yards)
    def feet: PackratParser[DistanceUnit] = Feet.regex ^^ (_ => Feet)
    def inches: PackratParser[DistanceUnit] = Inches.regex ^^ (_ => Inches)
    def nauticalMiles: PackratParser[DistanceUnit] = NauticalMiles.regex ^^ (_ => NauticalMiles)

    def distance_unit: PackratParser[DistanceUnit] =
      kilometers | meters | centimeters | millimeters | miles | yards | feet | inches | nauticalMiles

    def geo_distance: PackratParser[GeoDistance] =
      long ~ distance_unit ^^ { case value ~ unit => GeoDistance(value, unit) }

    def distance_identifier: PackratParser[Identifier] = distance ^^ functionAsIdentifier
  }
}
