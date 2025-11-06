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

import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess
}

import scala.concurrent.{ExecutionContext, Future}

trait CountApi extends ElasticClientHelpers {

  // ========================================================================
  // PUBLIC METHODS
  // ========================================================================

  /** Count the number of documents matching the given JSON query.
    *
    * @param query
    *   - the query to count the documents for
    * @return
    *   the number of documents matching the query, or None if the count could not be determined
    */
  def count(query: ElasticQuery): ElasticResult[Option[Double]] = {
    query.indices.flatMap { index =>
      validateIndexName(index).map(error => index -> error.message)
    } match {
      case errors if errors.nonEmpty =>
        return ElasticResult.failure(
          ElasticError(
            message = s"Invalid indices: ${errors.map(_._2).mkString(",")}",
            statusCode = Some(400),
            index = Some(errors.map(_._1).mkString(",")),
            operation = Some("count")
          )
        )
      case _ => // continue
    }

    validateJson("count", query.query) match {
      case Some(error) =>
        return ElasticResult.failure(
          error.copy(
            message = s"Invalid query: ${error.message}",
            statusCode = Some(400),
            index = Some(query.indices.mkString(",")),
            operation = Some("count")
          )
        )
      case None => // continue
    }

    logger.debug(
      s"Counting documents matching query '${query.query}' in indices '${query.indices.mkString(",")}'"
    )

    executeCount(query) match {
      case success @ ElasticSuccess(Some(count)) =>
        logger.info(
          s"✅ Successfully counted $count documents matching query in indices '${query.indices.mkString(",")}'"
        )
        success
      case _ @ElasticSuccess(None) =>
        val error =
          ElasticError(
            message =
              s"Could not determine count of documents matching query in indices '${query.indices.mkString(",")}'",
            index = Some(query.indices.mkString(",")),
            operation = Some("count")
          )
        logger.error(s"❌ ${error.message}")
        ElasticResult.failure(error)
      case failure @ ElasticFailure(error) =>
        logger.error(
          s"❌ Failed to count documents matching query in indices '${query.indices.mkString(",")}': ${error.message}"
        )
        failure
    }
  }

  /** Count the number of documents matching the given JSON query asynchronously.
    *
    * @param query
    *   - the query to count the documents for
    * @return
    *   the number of documents matching the query, or None if the count could not be determined
    */
  def countAsync(
    query: ElasticQuery
  )(implicit ec: ExecutionContext): Future[ElasticResult[Option[Double]]] = {
    Future {
      this.count(query)
    }
  }

  // ========================================================================
  // METHODS TO IMPLEMENT
  // ========================================================================

  private[client] def executeCount(
    query: ElasticQuery
  ): ElasticResult[Option[Double]]

  private[client] def executeCountAsync(
    query: ElasticQuery
  )(implicit ec: ExecutionContext): Future[ElasticResult[Option[Double]]]
}
