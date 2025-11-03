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

package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticResult, ElasticSuccess}
import app.softnetwork.elastic.sql.query.{SQLAggregation, SQLQuery}

import java.time.temporal.Temporal
import scala.annotation.tailrec
import scala.concurrent.{ExecutionContext, Future}

/** Aggregate API for Elasticsearch clients.
  *
  * @tparam T
  *   - the type of aggregate result
  */
trait AggregateApi[T <: AggregateResult] {

  /** Aggregate the results of the given SQL query.
    *
    * @param sqlQuery
    *   - the query to aggregate the results for
    * @return
    *   a sequence of aggregated results
    */
  def aggregate(sqlQuery: SQLQuery)(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[_root_.scala.collection.Seq[T]]]
}

/** Aggregate API for single value aggregate results.
  */
trait SingleValueAggregateApi
    extends AggregateApi[SingleValueAggregateResult]
    with ElasticConversion {
  _: SearchApi =>

  /** Aggregate the results of the given SQL query.
    *
    * @param sqlQuery
    *   - the query to aggregate the results for
    * @return
    *   a sequence of aggregated results
    */
  override def aggregate(
    sqlQuery: SQLQuery
  )(implicit
    ec: ExecutionContext
  ): Future[ElasticResult[_root_.scala.collection.Seq[SingleValueAggregateResult]]] = {
    Future {
      @tailrec
      def findAggregation(
        name: String,
        aggregation: SQLAggregation,
        results: Map[String, Any]
      ): Option[Any] = {
        name.split("\\.") match {
          case Array(_, tail @ _*) if tail.nonEmpty =>
            findAggregation(
              tail.mkString("."),
              aggregation,
              results
            )
          case _ => results.get(name)
        }
      }

      @tailrec
      def getAggregateValue(s: Seq[_], distinct: Boolean): AggregateValue = {
        if (s.isEmpty) return EmptyValue

        s.headOption match {
          case Some(_: Boolean) =>
            val values = s.asInstanceOf[Seq[Boolean]]
            ArrayOfBooleanValue(if (distinct) values.distinct else values)

          case Some(_: Number) =>
            val values = s.asInstanceOf[Seq[Number]]
            ArrayOfNumericValue(if (distinct) values.distinct else values)

          case Some(_: Temporal) =>
            val values = s.asInstanceOf[Seq[Temporal]]
            ArrayOfTemporalValue(if (distinct) values.distinct else values)

          case Some(_: String) =>
            val values = s.map(_.toString)
            ArrayOfStringValue(if (distinct) values.distinct else values)

          case Some(_: Map[_, _]) =>
            val typedMaps = s.asInstanceOf[Seq[Map[String, Any]]]
            val metadataKeys = Set("_id", "_index", "_score", "_sort")

            // Check if all maps have the same single non-metadata key
            val nonMetadataKeys = typedMaps.flatMap(_.keys.filterNot(metadataKeys.contains))
            val uniqueKeys = nonMetadataKeys.distinct

            if (uniqueKeys.size == 1) {
              // Extract values from the single key
              val key = uniqueKeys.head
              val extractedValues = typedMaps.flatMap(_.get(key))
              getAggregateValue(extractedValues, distinct)
            } else {
              // Multiple keys: return as object array
              val cleanMaps = typedMaps.map(m =>
                m.filterNot(k => metadataKeys.contains(k.toString))
                  .map(kv => kv._1 -> kv._2)
              )
              ArrayOfObjectValue(if (distinct) cleanMaps.distinct else cleanMaps)
            }

          case Some(_: Seq[_]) =>
            // Handle nested sequences (flatten them)
            getAggregateValue(s.asInstanceOf[Seq[Seq[_]]].flatten, distinct)

          case _ => EmptyValue
        }
      }
      // Execute the search
      search(sqlQuery)
        .flatMap { response =>
          // Parse the response
          val parseResult = ElasticResult.fromTry(parseResponse(response))

          parseResult match {
            // Case 1: Parse successful - process the results
            case ElasticSuccess(results) =>
              val aggregationResults = results.flatMap { result =>
                response.aggregations.map { case (name, aggregation) =>
                  // Attempt to process each aggregation
                  val aggregationResult = ElasticResult.attempt {
                    val value = findAggregation(name, aggregation, result).orNull match {
                      case b: Boolean     => BooleanValue(b)
                      case n: Number      => NumericValue(n)
                      case s: String      => StringValue(s)
                      case t: Temporal    => TemporalValue(t)
                      case m: Map[_, Any] => ObjectValue(m.map(kv => kv._1.toString -> kv._2))
                      case s: Seq[_] if aggregation.multivalued =>
                        getAggregateValue(s, aggregation.distinct)
                      case _ => EmptyValue
                    }

                    SingleValueAggregateResult(name, aggregation.aggType, value)
                  }

                  // Convert failures to results with errors
                  aggregationResult match {
                    case ElasticSuccess(result) => result
                    case ElasticFailure(error) =>
                      SingleValueAggregateResult(
                        name,
                        aggregation.aggType,
                        EmptyValue,
                        error = Some(s"Failed to process aggregation: ${error.message}")
                      )
                  }
                }.toSeq
              }

              ElasticResult.success(aggregationResults)

            // Case 2: Parse failed - returning empty results with errors
            case ElasticFailure(error) =>
              val errorResults = response.aggregations.map { case (name, aggregation) =>
                SingleValueAggregateResult(
                  name,
                  aggregation.aggType,
                  EmptyValue,
                  error = Some(s"Parse error: ${error.message}")
                )
              }.toSeq

              ElasticResult.success(errorResults)
          }
        }
        .fold(
          // If search() fails, throw an exception
          onFailure = error => {
            throw new IllegalArgumentException(
              s"Failed to execute search for SQL query: ${sqlQuery.query}",
              error.cause.orNull
            )
          },
          onSuccess = results => ElasticResult.success(results)
        )
    }
  }
}
