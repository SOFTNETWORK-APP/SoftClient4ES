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

package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.function.aggregate.{
  AggregateFunction,
  AvgAgg,
  CountAgg,
  MaxAgg,
  MinAgg,
  SumAgg
}

package object transform {

  /** Extension methods for AggregateFunction
    */
  implicit class AggregateConversion(agg: AggregateFunction) {

    /** Converts SQL aggregate function to Elasticsearch aggregation
      */
    def toTransformAggregation: Option[TransformAggregation] = agg match {
      case ma: MaxAgg =>
        Some(MaxTransformAggregation(ma.identifier.name))

      case ma: MinAgg =>
        Some(MinTransformAggregation(ma.identifier.name))

      case sa: SumAgg =>
        Some(SumTransformAggregation(sa.identifier.name))

      case aa: AvgAgg =>
        Some(AvgTransformAggregation(aa.identifier.name))

      case ca: CountAgg =>
        val field = ca.identifier.name
        Some(
          if (field == "*" || field.isEmpty)
            if (ca.isCardinality) CardinalityTransformAggregation("_id")
            else CountTransformAggregation("_id")
          else if (ca.isCardinality) CardinalityTransformAggregation(field)
          else CountTransformAggregation(field)
        )

      case _ =>
        // For other aggregate functions, default to none
        None
    }
  }

}
