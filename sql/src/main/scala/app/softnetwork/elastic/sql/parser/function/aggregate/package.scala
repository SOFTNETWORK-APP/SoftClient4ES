/*
 * Copyright 2025 SOFTNETWORK
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

    def stddev: PackratParser[AggregateFunction] = STDDEV.regex ^^ (_ => STDDEV)

    def stddev_pop: PackratParser[AggregateFunction] = STDDEV_POP.regex ^^ (_ => STDDEV_POP)

    def stddev_samp: PackratParser[AggregateFunction] = STDDEV_SAMP.regex ^^ (_ => STDDEV_SAMP)

    def variance: PackratParser[AggregateFunction] = VARIANCE.regex ^^ (_ => VARIANCE)

    def var_pop: PackratParser[AggregateFunction] = VAR_POP.regex ^^ (_ => VAR_POP)

    def var_samp: PackratParser[AggregateFunction] = VAR_SAMP.regex ^^ (_ => VAR_SAMP)

    // Longest-prefix alternation: STDDEV_POP / STDDEV_SAMP / VAR_POP / VAR_SAMP must be tried
    // before the bare STDDEV / VARIANCE so the suffixed forms are not shadowed.
    def aggregate_function: PackratParser[AggregateFunction] =
      count | min | max | avg | sum |
      stddev_pop | stddev_samp | stddev |
      var_pop | var_samp | variance

    def aggWithFunction: PackratParser[Identifier] =
      identifierWithArithmeticExpression |
      identifierWithTransformation |
      identifierWithIntervalFunction |
      identifierWithFunction |
      identifier

    def identifierWithAggregation: PackratParser[Identifier] =
      aggregate_function ~ start ~ aggWithFunction ~ end ^^ { case a ~ _ ~ i ~ _ =>
        i.withFunctions(a +: i.functions)
      }

    def partition_by: PackratParser[Seq[Identifier]] =
      PARTITION_BY.regex ~> rep1sep(identifierWithTransformation | identifier, separator)

    private[this] def over: Parser[(Seq[Identifier], Option[OrderBy], Option[Limit])] =
      OVER.regex ~> start ~ partition_by.? ~ orderBy.? ~ limit.? <~ end ^^ { case _ ~ pb ~ ob ~ l =>
        (pb.getOrElse(Seq.empty), ob, l)
      }

    private[this] def window_function(
      windowId: PackratParser[Identifier] = identifier
    ): PackratParser[(Identifier, Seq[Identifier], Option[OrderBy], Option[Limit])] =
      start ~ windowId ~ end ~ over.? ^^ { case _ ~ id ~ _ ~ o =>
        o match {
          case Some((pb, ob, l)) => (id, pb, ob, l)
          case None              => (id, Seq.empty, None, None)
        }
      }

    def first_value: PackratParser[WindowFunction] =
      FIRST_VALUE.regex ~ window_function() ^^ { case _ ~ top =>
        FirstValue(
          top._1,
          top._2,
          top._3.orElse(Option(OrderBy(Seq(FieldSort(top._1, order = None)))))
        )
      }

    def last_value: PackratParser[WindowFunction] =
      LAST_VALUE.regex ~ window_function() ^^ { case _ ~ top =>
        LastValue(
          top._1,
          top._2,
          top._3.orElse(Option(OrderBy(Seq(FieldSort(top._1, order = None)))))
        )
      }

    def array_agg: PackratParser[WindowFunction] =
      ARRAY_AGG.regex ~ window_function() ^^ { case _ ~ top =>
        ArrayAgg(
          top._1,
          top._2,
          top._3.orElse(Option(OrderBy(Seq(FieldSort(top._1, order = None))))),
          limit = top._4
        )
      }

    def count_agg: PackratParser[WindowFunction] =
      count ~ window_function(aggWithFunction) ^^ { case _ ~ top =>
        CountAgg(top._1, top._2)
      }

    def min_agg: PackratParser[WindowFunction] =
      min ~ window_function(aggWithFunction) ^^ { case _ ~ top =>
        MinAgg(top._1, top._2)
      }

    def max_agg: PackratParser[WindowFunction] =
      max ~ window_function(aggWithFunction) ^^ { case _ ~ top =>
        MaxAgg(top._1, top._2)
      }

    def avg_agg: PackratParser[WindowFunction] =
      avg ~ window_function(aggWithFunction) ^^ { case _ ~ top =>
        AvgAgg(top._1, top._2)
      }

    def sum_agg: PackratParser[WindowFunction] =
      sum ~ window_function(aggWithFunction) ^^ { case _ ~ top =>
        SumAgg(top._1, top._2)
      }

    def stddev_agg: PackratParser[WindowFunction] =
      (stddev_pop | stddev_samp | stddev) ~ window_function(aggWithFunction) ^^ { case fn ~ top =>
        val kind = fn match {
          case STDDEV_POP  => ExtendedStatsKind.StddevPop
          case STDDEV_SAMP => ExtendedStatsKind.StddevSamp
          case _           => ExtendedStatsKind.Stddev
        }
        ExtendedStatsAgg(top._1, kind, top._2)
      }

    def variance_agg: PackratParser[WindowFunction] =
      (var_pop | var_samp | variance) ~ window_function(aggWithFunction) ^^ { case fn ~ top =>
        val kind = fn match {
          case VAR_POP  => ExtendedStatsKind.VarPop
          case VAR_SAMP => ExtendedStatsKind.VarSamp
          case _        => ExtendedStatsKind.Variance
        }
        ExtendedStatsAgg(top._1, kind, top._2)
      }

    def percentile_cont: PackratParser[AggregateFunction] =
      PERCENTILE_CONT.regex ^^ (_ => PERCENTILE_CONT)

    def percentile_disc: PackratParser[AggregateFunction] =
      PERCENTILE_DISC.regex ^^ (_ => PERCENTILE_DISC)

    // Numeric percentile literal in [0,1] — accepts decimals (0.99) and whole 0/1.
    private[this] def percentile_literal: PackratParser[Double] =
      (double ^^ (_.value)) | (long ^^ (_.value.toDouble))

    // (col, p) shorthand  OR  (p)
    private[this] def percentile_args: PackratParser[(Option[Identifier], Double)] =
      (start ~> aggWithFunction ~ (separator ~> percentile_literal) <~ end ^^ { case id ~ p =>
        (Some(id), p)
      }) |
      (start ~> percentile_literal <~ end ^^ (p => (None, p)))

    // WITHIN GROUP ( ORDER BY <col> ) -> value column(s). A percentile takes a
    // SINGLE value column; a multi-column ORDER BY is rejected in `percentile_agg`
    // (the full sort list is surfaced here so the guard can count columns).
    private[this] def percentile_within_group: PackratParser[Seq[Identifier]] =
      """(?i)\bwithin\b""".r ~> """(?i)\bgroup\b""".r ~> start ~> orderBy <~ end ^^ (_.sorts.map(
        _.field
      ))

    // value column(s) from OVER's ORDER BY, if present
    private[this] def percentileOverValueCols(
      ov: Option[(Seq[Identifier], Option[OrderBy], Option[Limit])]
    ): Seq[Identifier] =
      ov.flatMap(_._2).map(_.sorts.map(_.field)).getOrElse(Seq.empty)

    /** PERCENTILE_CONT / PERCENTILE_DISC — five forms, all normalizing to
      * `PercentileAgg(valueColumn, cont, p, partitionBy)`. The value column comes from exactly one
      * of: the `(col, p)` shorthand, `WITHIN GROUP (ORDER BY col)`, or `OVER (... ORDER BY col)`.
      * The `^?` guard rejects (parse failure) when there is no value column, more than one source,
      * or `p` outside `[0,1]`.
      */
    def percentile_agg: PackratParser[WindowFunction] =
      ((percentile_cont | percentile_disc) ~ percentile_args ~
      percentile_within_group.? ~ over.?) ^? ({
        case fn ~ ((shorthandCol, p)) ~ wg ~ ov if {
              // exactly one value-column SOURCE, and that source names exactly ONE
              // column (rejects no source, conflicting sources, and a multi-column
              // WITHIN GROUP / OVER ORDER BY value list).
              val sources =
                Seq(shorthandCol.toSeq, wg.getOrElse(Seq.empty), percentileOverValueCols(ov))
                  .filter(_.nonEmpty)
              sources.size == 1 && sources.head.size == 1 && p >= 0.0 && p <= 1.0
            } =>
          val valueCol =
            Seq(shorthandCol.toSeq, wg.getOrElse(Seq.empty), percentileOverValueCols(ov))
              .filter(_.nonEmpty)
              .head
              .head
          val partitionBy = ov.map(_._1).getOrElse(Seq.empty)
          PercentileAgg(valueCol, cont = fn == PERCENTILE_CONT, p, partitionBy)
      }, { _ =>
        "PERCENTILE_CONT/DISC requires a literal percentile in [0,1] and exactly one value " +
        "column (a single column via (column, p), WITHIN GROUP (ORDER BY col), or " +
        "OVER (... ORDER BY col))"
      })

    /** OVER clause variant used by ranking windows: ORDER BY is REQUIRED (ANSI). Falling through to
      * the optional-orderBy parser would let `ROW_NUMBER() OVER (PARTITION BY d)` parse and then
      * break at execution; rejecting at parse time is preferable.
      */
    private[this] def ranking_over: Parser[(Seq[Identifier], OrderBy, Option[Limit])] =
      OVER.regex ~> start ~ partition_by.? ~ orderBy ~ limit.? <~ end ^^ { case _ ~ pb ~ ob ~ l =>
        (pb.getOrElse(Seq.empty), ob, l)
      }

    def row_number: PackratParser[WindowFunction] =
      ROW_NUMBER.regex ~ start ~ end ~ ranking_over ^^ { case _ ~ _ ~ _ ~ ((pb, ob, l)) =>
        RowNumber(partitionBy = pb, orderBy = Some(ob), limit = l)
      }

    def rank: PackratParser[WindowFunction] =
      RANK.regex ~ start ~ end ~ ranking_over ^^ { case _ ~ _ ~ _ ~ ((pb, ob, l)) =>
        Ranking(partitionBy = pb, orderBy = Some(ob), limit = l)
      }

    def dense_rank: PackratParser[WindowFunction] =
      DENSE_RANK.regex ~ start ~ end ~ ranking_over ^^ { case _ ~ _ ~ _ ~ ((pb, ob, l)) =>
        DenseRank(partitionBy = pb, orderBy = Some(ob), limit = l)
      }

    def identifierWithWindowFunction: PackratParser[Identifier] =
      (first_value | last_value | array_agg | count_agg | min_agg | max_agg | avg_agg | sum_agg |
      stddev_agg | variance_agg | percentile_agg |
      row_number | rank | dense_rank) ^^ { th =>
        th.identifier.withFunctions(th +: th.identifier.functions)
      }

  }

}
