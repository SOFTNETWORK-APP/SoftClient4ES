package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.Identifier
import app.softnetwork.elastic.sql.function.aggregate.{AggregateFunction, Avg, Count, Max, Min, Sum}
import app.softnetwork.elastic.sql.parser.Parser

package object aggregate {

  trait AggregateParser { self: Parser =>

    def count: PackratParser[AggregateFunction] = Count.regex ^^ (_ => Count)

    def min: PackratParser[AggregateFunction] = Min.regex ^^ (_ => Min)

    def max: PackratParser[AggregateFunction] = Max.regex ^^ (_ => Max)

    def avg: PackratParser[AggregateFunction] = Avg.regex ^^ (_ => Avg)

    def sum: PackratParser[AggregateFunction] = Sum.regex ^^ (_ => Sum)

    def aggregates: PackratParser[AggregateFunction] = count | min | max | avg | sum

    def identifierWithAggregation: PackratParser[Identifier] =
      aggregates ~ start ~ (identifierWithFunction | identifierWithIntervalFunction | identifier) ~ end ^^ {
        case a ~ _ ~ i ~ _ =>
          i.withFunctions(a +: i.functions)
      }

  }

}
