package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.{From, Table, Unnest}

trait FromParser {
  self: Parser with LimitParser =>

  def unnest: PackratParser[Table] =
    Unnest.regex ~ start ~ identifier ~ limit.? ~ end ~ alias ^^ { case _ ~ _ ~ i ~ l ~ _ ~ a =>
      Table(Unnest(i, l), Some(a))
    }

  def table: PackratParser[Table] = identifier ~ alias.? ^^ { case i ~ a => Table(i, a) }

  def from: PackratParser[From] = From.regex ~ rep1sep(unnest | table, separator) ^^ {
    case _ ~ tables =>
      From(tables)
  }

}
