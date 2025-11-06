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

package app.softnetwork.elastic.client.jest

import app.softnetwork.elastic.client.{CountApi, ElasticQuery}
import app.softnetwork.elastic.client.result.ElasticResult
import io.searchbox.core.Count

import scala.concurrent.{ExecutionContext, Future}

/** Count API for Jest (Elasticsearch HTTP Client).
  * @see
  *   [[CountApi]] for generic API documentation
  */
trait JestCountApi extends CountApi with JestClientHelpers { _: JestClientCompanion =>

  /** Count documents matching a query.
    * @see
    *   [[CountApi.count]]
    */
  override private[client] def executeCount(query: ElasticQuery): ElasticResult[Option[Double]] =
    executeJestAction(
      operation = "count",
      index = Some(query.indices.mkString(","))
    ) {
      val count = new Count.Builder().query(query.query)
      import query._
      for (indice <- indices) count.addIndex(indice)
      for (t      <- types) count.addType(t)
      count.build()
    } { result =>
      if (result.isSucceeded) {
        Some(result.getCount)
      } else {
        None
      }
    }

  /** Count documents matching a query asynchronously.
    * @see
    *   [[CountApi.countAsync]]
    */
  override private[client] def executeCountAsync(
    elasticQuery: ElasticQuery
  )(implicit ec: ExecutionContext): Future[ElasticResult[Option[Double]]] = {
    executeAsyncJestAction(
      operation = "countAsync",
      index = Some(elasticQuery.indices.mkString(","))
    ) {
      import elasticQuery._
      val count = new Count.Builder().query(query)
      for (indice <- indices) count.addIndex(indice)
      for (t      <- types) count.addType(t)
      count.build()
    } { result =>
      if (result.isSucceeded) {
        Some(result.getCount)
      } else {
        None
      }
    }
  }

}
