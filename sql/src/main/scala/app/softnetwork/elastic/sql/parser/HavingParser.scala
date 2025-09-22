package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.Having

trait HavingParser {
  self: Parser with WhereParser =>

  def having: PackratParser[Having] = Having.regex ~> whereCriteria ^^ { rawTokens =>
    Having(
      processTokens(rawTokens)
    )
  }

}
