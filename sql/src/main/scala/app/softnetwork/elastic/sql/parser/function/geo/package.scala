package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.Identifier
import app.softnetwork.elastic.sql.function.Function
import app.softnetwork.elastic.sql.function.geo.{Distance, Point}
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

    def distance: PackratParser[Function] =
      Distance.regex ~> start ~> pointOrIdentifier ~ separator ~ pointOrIdentifier <~ end ^^ {
        case from ~ _ ~ to => Distance(from, to)
      }

    def distance_identifier: PackratParser[Identifier] = distance ^^ functionAsIdentifier
  }
}
