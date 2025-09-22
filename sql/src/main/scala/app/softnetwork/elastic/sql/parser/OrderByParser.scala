package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.function.Function
import app.softnetwork.elastic.sql.query.{Asc, Desc, FieldSort, OrderBy}

trait OrderByParser {
  self: Parser =>

  def asc: PackratParser[Asc.type] = Asc.regex ^^ (_ => Asc)

  def desc: PackratParser[Desc.type] = Desc.regex ^^ (_ => Desc)

  private def fieldName: PackratParser[String] =
    """\b(?!(?i)limit\b)[a-zA-Z_][a-zA-Z0-9_]*""".r ^^ (f => f)

  def fieldWithFunction: PackratParser[(String, List[Function])] =
    rep1sep(sql_functions, start) ~ start.? ~ fieldName ~ rep1(end) ^^ { case f ~ _ ~ n ~ _ =>
      (n, f)
    }

  def sort: PackratParser[FieldSort] =
    (fieldWithFunction | fieldName) ~ (asc | desc).? ^^ { case f ~ o =>
      f match {
        case i: (String, List[Function]) => FieldSort(i._1, o, i._2)
        case s: String                   => FieldSort(s, o, List.empty)
      }
    }

  def orderBy: PackratParser[OrderBy] = OrderBy.regex ~ rep1sep(sort, separator) ^^ { case _ ~ s =>
    OrderBy(s)
  }

}
