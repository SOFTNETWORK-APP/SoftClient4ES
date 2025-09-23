package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.Identifier
import app.softnetwork.elastic.sql.function.aggregate.{AVG, AggregateFunction, COUNT, MAX, MIN, SUM}
import app.softnetwork.elastic.sql.parser.Parser

package object aggregate {

  trait AggregateParser { self: Parser =>

    def count: PackratParser[AggregateFunction] = COUNT.regex ^^ (_ => COUNT)

    def min: PackratParser[AggregateFunction] = MIN.regex ^^ (_ => MIN)

    def max: PackratParser[AggregateFunction] = MAX.regex ^^ (_ => MAX)

    def avg: PackratParser[AggregateFunction] = AVG.regex ^^ (_ => AVG)

    def sum: PackratParser[AggregateFunction] = SUM.regex ^^ (_ => SUM)

    def aggregates: PackratParser[AggregateFunction] = count | min | max | avg | sum

    def identifierWithAggregation: PackratParser[Identifier] =
      aggregates ~ start ~ (identifierWithFunction | identifierWithIntervalFunction | identifier) ~ end ^^ {
        case a ~ _ ~ i ~ _ =>
          i.withFunctions(a +: i.functions)
      }

  }

}
