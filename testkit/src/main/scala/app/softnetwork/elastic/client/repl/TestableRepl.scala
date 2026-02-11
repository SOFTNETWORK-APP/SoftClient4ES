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
import app.softnetwork.elastic.client.result.OutputFormat

import scala.collection.immutable.ListMap
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._

/** Testable REPL for integration tests
  */
class TestableRepl(
  executor: StreamingReplExecutor,
  config: ReplConfig = ReplConfig.default
)(implicit system: ActorSystem, ec: ExecutionContextExecutor) {

  def executeSync(sql: String, timeout: Duration = 30.seconds): ExecutionResult = {
    Await.result(executor.execute(sql), timeout)
  }

  def consumeStreamSync(
    batchSize: Int = 100,
    maxRows: Option[Int] = None,
    timeout: Duration = 30.seconds
  ): Seq[ListMap[String, Any]] = {
    var displayedRows = 0
    val allRows = scala.collection.mutable.ListBuffer[ListMap[String, Any]]()

    Await.result(
      executor.consumeStream(
        batchSize = batchSize,
        maxRows = maxRows,
        onBatch = { batch =>
          // Accumulate rows for display
          allRows ++= batch
          displayedRows += batch.size

          // Show progress
          print(s"\r📊 Received $displayedRows rows...")
        }
      ),
      timeout
    )
    println() // New line after progress

    allRows.toSeq
  }

  def setFormat(format: OutputFormat): Unit = {
    config.format = format
  }
}
