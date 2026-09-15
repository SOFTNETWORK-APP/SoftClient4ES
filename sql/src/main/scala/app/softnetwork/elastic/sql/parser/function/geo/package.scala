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

package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.{GeoDistance, Identifier}
import app.softnetwork.elastic.sql.function.geo._
import app.softnetwork.elastic.sql.parser.Parser

package object geo {

  trait GeoParser { self: Parser =>

    lazy val point: PackratParser[Point] =
      Point.regex ~> start ~> double ~ separator ~ double <~ end ^^ { case lat ~ _ ~ lon =>
        Point(lat, lon)
      }

    lazy val pointOrIdentifier: PackratParser[Either[Identifier, Point]] =
      (point | identifier) ^^ {
        case id: Identifier => Left(id)
        case p: Point       => Right(p)
      }

    lazy val distance: PackratParser[Distance] =
      Distance.regex ~> start ~> pointOrIdentifier ~ separator ~ pointOrIdentifier <~ end ^^ {
        case from ~ _ ~ to => Distance(from, to)
      }

    lazy val kilometers: PackratParser[DistanceUnit] = Kilometers.regex ^^ (_ => Kilometers)
    lazy val meters: PackratParser[DistanceUnit] = Meters.regex ^^ (_ => Meters)
    lazy val centimeters: PackratParser[DistanceUnit] = Centimeters.regex ^^ (_ => Centimeters)
    lazy val millimeters: PackratParser[DistanceUnit] = Millimeters.regex ^^ (_ => Millimeters)
    lazy val miles: PackratParser[DistanceUnit] = Miles.regex ^^ (_ => Miles)
    lazy val yards: PackratParser[DistanceUnit] = Yards.regex ^^ (_ => Yards)
    lazy val feet: PackratParser[DistanceUnit] = Feet.regex ^^ (_ => Feet)
    lazy val inches: PackratParser[DistanceUnit] = Inches.regex ^^ (_ => Inches)
    lazy val nauticalMiles: PackratParser[DistanceUnit] =
      NauticalMiles.regex ^^ (_ => NauticalMiles)

    lazy val distance_unit: PackratParser[DistanceUnit] =
      kilometers | meters | centimeters | millimeters | miles | yards | feet | inches | nauticalMiles

    lazy val geo_distance: PackratParser[GeoDistance] =
      long ~ distance_unit ^^ { case value ~ unit => GeoDistance(value, unit) }

    lazy val distance_identifier: PackratParser[Identifier] = distance ^^ functionAsIdentifier

    lazy val geoFunctionWithIdentifier: PackratParser[Identifier] = distance_identifier
  }
}
