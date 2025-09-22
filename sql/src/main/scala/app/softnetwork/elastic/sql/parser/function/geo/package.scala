package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.function.Function
import app.softnetwork.elastic.sql.function.geo.Distance
import app.softnetwork.elastic.sql.parser.Parser

package object geo {

  trait GeoParser { self: Parser =>

    def distance: PackratParser[Function] = Distance.regex ^^ (_ => Distance)

  }
}
