package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.Identifier
import app.softnetwork.elastic.sql.function.aggregate._
import app.softnetwork.elastic.sql.parser.{OrderByParser, Parser}
import app.softnetwork.elastic.sql.query.OrderBy

package object aggregate {

  trait AggregateParser { self: Parser with OrderByParser =>

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

    def partition_by: PackratParser[Seq[Identifier]] =
      PARTITION_BY.regex ~> rep1sep(identifier, separator)

    private[this] def top_hits: PackratParser[(Identifier, Seq[Identifier], OrderBy)] =
      start ~ identifier ~ end ~ OVER.regex ~ start ~ partition_by.? ~ orderBy ~ end ^^ {
        case _ ~ id ~ _ ~ _ ~ _ ~ pb ~ ob ~ _ =>
          (id, pb.getOrElse(Seq.empty), ob)
      }

    def first_value: PackratParser[TopHitsAggregation] =
      FIRST_VALUE.regex ~ top_hits ^^ { case _ ~ top =>
        FirstValue(top._1, top._2, top._3)
      }

    def last_value: PackratParser[TopHitsAggregation] =
      LAST_VALUE.regex ~ top_hits ^^ { case _ ~ top =>
        LastValue(top._1, top._2, top._3)
      }

    def identifierWithTopHits: PackratParser[Identifier] = (first_value | last_value) ^^ { th =>
      th.identifier.withFunctions(List(th))
    }

  }

}
