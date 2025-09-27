package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.{Except, Field, Select}

trait SelectParser {
  self: Parser with WhereParser =>

  def field: PackratParser[Field] =
    (distance_identifier >> castOperator | identifierWithTopHits |
    identifierWithArithmeticExpression |
    identifierWithTransformation |
    identifierWithAggregation |
    identifierWithIntervalFunction |
    identifierWithFunction |
    identifier) ~ alias.? ^^ { case i ~ a =>
      Field(i, a)
    }

  def except: PackratParser[Except] = Except.regex ~ start ~ rep1sep(field, separator) ~ end ^^ {
    case _ ~ _ ~ e ~ _ =>
      Except(e)
  }

  def select: PackratParser[Select] =
    Select.regex ~ rep1sep(
      field,
      separator
    ) ~ except.? ^^ { case _ ~ fields ~ e =>
      Select(fields, e)
    }

}
