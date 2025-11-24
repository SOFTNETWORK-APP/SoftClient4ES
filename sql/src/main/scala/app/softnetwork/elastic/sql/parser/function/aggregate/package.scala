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

    def aggregate_function: PackratParser[AggregateFunction] = count | min | max | avg | sum

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
      PARTITION_BY.regex ~> rep1sep(identifier, separator)

    private[this] def over: Parser[(Seq[Identifier], OrderBy)] =
      OVER.regex ~> start ~ partition_by.? ~ orderBy <~ end ^^ { case _ ~ pb ~ ob =>
        (pb.getOrElse(Seq.empty), ob)
      }

    private[this] def top_hits: PackratParser[(Identifier, Seq[Identifier], OrderBy)] =
      start ~ identifier ~ end ~ over.? ^^ { case _ ~ id ~ _ ~ o =>
        o match {
          case Some((pb, ob)) => (id, pb, ob)
          case None           => (id, Seq.empty, OrderBy(Seq(FieldSort(id, order = None))))
        }
      }

    def first_value: PackratParser[WindowFunction] =
      FIRST_VALUE.regex ~ top_hits ^^ { case _ ~ top =>
        FirstValue(top._1, top._2, top._3)
      }

    def last_value: PackratParser[WindowFunction] =
      LAST_VALUE.regex ~ top_hits ^^ { case _ ~ top =>
        LastValue(top._1, top._2, top._3)
      }

    def array_agg: PackratParser[WindowFunction] =
      ARRAY_AGG.regex ~ top_hits ^^ { case _ ~ top =>
        ArrayAgg(top._1, top._2, top._3, limit = None)
      }

    def identifierWithWindowFunction: PackratParser[Identifier] =
      (first_value | last_value | array_agg) ^^ { th =>
        th.identifier.withFunctions(th +: th.identifier.functions)
      }

  }

}
