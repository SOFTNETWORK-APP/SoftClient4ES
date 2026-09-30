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

  /** The field a transform's `value_count` reads for `COUNT(*)`: `_index`, which every document
    * carries exactly once and which needs no field data -- the field the search bridge already
    * counts for `COUNT(*)`.
    *
    * 🔴 It was `_id`, and Elasticsearch 8 disallows field data on `_id`
    * (`indices.id_field_data.enabled` is false by default), so a view with `COUNT(*)` was refused
    * at deploy: "Fielddata access on the _id field is disallowed". MEASURED on 7.17.29 and 8.18.3
    * against a pivot on two keys, one document with a multi-valued key and one missing a key:
    * `value_count` on `_index`, `value_count` with a constant script and a `match_all` `filter` are
    * exact on both (all typed `long`); a `bucket_script` over `_count` is exact but typed `float`;
    * `value_count` on a GROUP BY key OVER-counts (it counts values, not documents). `_index` is the
    * one that needs no script and no new aggregation type.
    */
  val CountAllDocumentsField: String = "_index"

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
          // `COUNT(DISTINCT *)` too: it was the `cardinality` of `_id`, i.e. the number of distinct
          // documents, which IS the number of documents -- the exact count, where `cardinality`
          // only estimated it. MEASURED equal (7.17.29, where the `_id` form still ran). The
          // `cardinality` of `_index` is NOT a form of it: it counts indices, 1 per bucket.
          if (field == "*" || field.isEmpty) CountTransformAggregation(CountAllDocumentsField)
          else if (ca.isCardinality) CardinalityTransformAggregation(field)
          else CountTransformAggregation(field)
        )

      case _ =>
        // For other aggregate functions, default to none
        None
    }
  }

}
