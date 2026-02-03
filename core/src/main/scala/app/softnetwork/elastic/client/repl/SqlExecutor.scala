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

package app.softnetwork.elastic.client.repl

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.GatewayApi
import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticSuccess,
  QueryResult
}

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}

class SqlExecutor(gateway: GatewayApi)(implicit system: ActorSystem, ec: ExecutionContext) {

  /** Execute SQL and return formatted result */
  def execute(sql: String): Future[ExecutionResult] = {
    val startTime = System.nanoTime()

    gateway
      .run(sql)
      .map { elasticResult =>
        val executionTime = (System.nanoTime() - startTime).nanos

        elasticResult match {
          case ElasticSuccess(queryResult) =>
            ExecutionSuccess(queryResult, executionTime)

          case ElasticFailure(error) =>
            ExecutionFailure(error, executionTime)
        }
      }
      .recover { case ex: Throwable =>
        val executionTime = (System.nanoTime() - startTime).nanos
        ExecutionFailure(ElasticError.fromThrowable(ex), executionTime)
      }
  }
}

sealed trait ExecutionResult {
  def executionTime: Duration
}

case class ExecutionSuccess(
  result: QueryResult,
  executionTime: Duration
) extends ExecutionResult

case class ExecutionFailure(
  error: ElasticError,
  executionTime: Duration
) extends ExecutionResult
