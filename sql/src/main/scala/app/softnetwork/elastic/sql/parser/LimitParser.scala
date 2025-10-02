package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.{Limit, Offset}

trait LimitParser {
  self: Parser =>

  def offset: PackratParser[Offset] = Offset.regex ~ long ^^ { case _ ~ i =>
    Offset(i.value.toInt)
  }

  def limit: PackratParser[Limit] = Limit.regex ~ long ~ offset.? ^^ { case _ ~ i ~ o =>
    Limit(i.value.toInt, o)
  }

}
