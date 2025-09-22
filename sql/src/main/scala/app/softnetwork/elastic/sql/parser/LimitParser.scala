package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.Limit

trait LimitParser {
  self: Parser =>

  def limit: PackratParser[Limit] = Limit.regex ~ long ^^ { case _ ~ i =>
    Limit(i.value.toInt)
  }

}
