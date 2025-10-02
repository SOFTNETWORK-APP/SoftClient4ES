package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.{
  CrossJoin,
  From,
  FullJoin,
  InnerJoin,
  Join,
  JoinType,
  LeftJoin,
  On,
  RightJoin,
  Table,
  Unnest
}

trait FromParser {
  self: Parser with WhereParser with LimitParser =>

  def unnest: PackratParser[Unnest] =
    Unnest.regex ~ start ~ identifier ~ limit.? ~ end ~ alias.? ^^ { case _ ~ i ~ l ~ _ ~ a =>
      Unnest(i, l, a)
    }

  def inner_join: PackratParser[JoinType] = InnerJoin.regex ^^ { _ => InnerJoin }
  def left_join: PackratParser[JoinType] = LeftJoin.regex ^^ { _ => LeftJoin }
  def right_join: PackratParser[JoinType] = RightJoin.regex ^^ { _ => RightJoin }
  def full_join: PackratParser[JoinType] = FullJoin.regex ^^ { _ => FullJoin }
  def cross_join: PackratParser[JoinType] = CrossJoin.regex ^^ { _ => CrossJoin }
  def join_type: PackratParser[JoinType] =
    inner_join | left_join | right_join | full_join | cross_join

  def on: PackratParser[On] = On.regex ~> whereCriteria ^^ { rawTokens =>
    On(
      processTokens(rawTokens).getOrElse(throw new Exception("ON clause requires criteria"))
    )
  }

  def join: PackratParser[Join] = opt(join_type) ~ Join.regex ~ unnest ~ opt(on) ^^ {
    case jt ~ _ ~ t ~ o => t // Unnest cannot have a join type or an ON clause
  }

  def table: PackratParser[Table] = identifierRegex ~ alias.? ~ rep(join) ^^ { case i ~ a ~ js =>
    Table(i, a, js)
  }

  def from: PackratParser[From] = From.regex ~ rep1sep(table, separator) ^^ { case _ ~ tables =>
    From(tables)
  }

}
