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

package app.softnetwork.elastic.sql.transform

import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}

object ChangelogAggregationStrategy {

  /** Selects the appropriate aggregation for a field in a changelog based on its data type
    *
    * @param field
    *   The field to aggregate
    * @param dataType
    *   The SQL data type of the field
    * @return
    *   The transform aggregation to use
    */
  def selectAggregation(field: String, dataType: SQLType): TransformAggregation = {
    dataType match {
      // Numeric types: Use MAX directly
      case SQLTypes.Int | SQLTypes.BigInt | SQLTypes.Double | SQLTypes.Real =>
        MaxTransformAggregation(field)

      // Date/Timestamp: Use MAX directly
      case SQLTypes.Date | SQLTypes.Timestamp =>
        MaxTransformAggregation(field)

      // Boolean: Use Top Hits
      case SQLTypes.Boolean =>
        TopHitsTransformAggregation(Seq(field))

      // Keyword: Already a keyword type, use MAX directly
      case SQLTypes.Keyword =>
        TopHitsTransformAggregation(Seq(field))

      // Text/Varchar: Analyzed field, must use .keyword multi-field
      case SQLTypes.Text | SQLTypes.Varchar =>
        TopHitsTransformAggregation(Seq(s"$field.keyword")) // ✅ Use .keyword multi-field

      // For complex types, use top_hits as fallback
      case _ =>
        TopHitsTransformAggregation(Seq(field))
    }
  }

  /** Determines if a field can use simple MAX aggregation (without needing a multi-field)
    */
  def canUseMaxDirectly(dataType: SQLType): Boolean = dataType match {
    case SQLTypes.Int | SQLTypes.BigInt | SQLTypes.Double | SQLTypes.Real | SQLTypes.Date |
        SQLTypes.Timestamp | SQLTypes.Keyword | SQLTypes.Boolean =>
      true
    case _ => false
  }

  /** Determines if a field needs a .keyword multi-field for aggregation
    */
  def needsKeywordMultiField(dataType: SQLType): Boolean = dataType match {
    case SQLTypes.Text | SQLTypes.Varchar => true
    case _                                => false
  }
}
