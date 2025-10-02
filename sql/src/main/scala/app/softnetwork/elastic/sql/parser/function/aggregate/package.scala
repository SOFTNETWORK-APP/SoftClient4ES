package app.softnetwork.elastic.sql.parser.function

import app.softnetwork.elastic.sql.Identifier
import app.softnetwork.elastic.sql.function.aggregate._
import app.softnetwork.elastic.sql.parser.{LimitParser, OrderByParser, Parser}
import app.softnetwork.elastic.sql.query.{FieldSort, Limit, OrderBy}

package object aggregate {

  trait AggregateParser { self: Parser with OrderByParser with LimitParser =>

    def count: PackratParser[AggregateFunction] = COUNT.regex ^^ (_ => COUNT)

    def min: PackratParser[AggregateFunction] = MIN.regex ^^ (_ => MIN)

    def max: PackratParser[AggregateFunction] = MAX.regex ^^ (_ => MAX)

    def avg: PackratParser[AggregateFunction] = AVG.regex ^^ (_ => AVG)

    def sum: PackratParser[AggregateFunction] = SUM.regex ^^ (_ => SUM)

    def aggregate_function: PackratParser[AggregateFunction] = count | min | max | avg | sum

    def identifierWithAggregation: PackratParser[Identifier] =
      aggregate_function ~ start ~ (identifierWithFunction | identifierWithIntervalFunction | identifier) ~ end ^^ {
        case a ~ _ ~ i ~ _ =>
          i.withFunctions(a +: i.functions)
      }

    def partition_by: PackratParser[Seq[Identifier]] =
      PARTITION_BY.regex ~> rep1sep(identifier, separator)

    private[this] def over: Parser[(Seq[Identifier], OrderBy, Option[Limit])] =
      OVER.regex ~> start ~ partition_by.? ~ orderBy ~ limit.? <~ end ^^ { case _ ~ pb ~ ob ~ l =>
        (pb.getOrElse(Seq.empty), ob, l)
      }

    private[this] def top_hits
      : PackratParser[(Identifier, Seq[Identifier], OrderBy, Option[Limit])] =
      start ~ identifier ~ end ~ over.? ^^ { case _ ~ id ~ _ ~ o =>
        o match {
          case Some((pb, ob, l)) => (id, pb, ob, l)
          case None => (id, Seq.empty, OrderBy(Seq(FieldSort(id.name, order = None))), None)
        }
      }

    def first_value: PackratParser[TopHitsAggregation] =
      FIRST_VALUE.regex ~ top_hits ^^ { case _ ~ top =>
        FirstValue(top._1, top._2, top._3, limit = top._4)
      }

    def last_value: PackratParser[TopHitsAggregation] =
      LAST_VALUE.regex ~ top_hits ^^ { case _ ~ top =>
        LastValue(top._1, top._2, top._3, limit = top._4)
      }

    def array_agg: PackratParser[TopHitsAggregation] =
      ARRAY_AGG.regex ~ top_hits ^^ { case _ ~ top =>
        ArrayAgg(top._1, top._2, top._3, limit = top._4)
      }

    def identifierWithTopHits: PackratParser[Identifier] =
      (first_value | last_value | array_agg) ^^ { th =>
        th.identifier.withFunctions(th +: th.identifier.functions)
      }

  }

}
