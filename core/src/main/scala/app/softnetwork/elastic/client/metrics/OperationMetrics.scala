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

package app.softnetwork.elastic.client.metrics

case class OperationMetrics(
  operation: String,
  totalOperations: Long,
  successCount: Long,
  failureCount: Long,
  totalDuration: Long,
  minDuration: Long,
  maxDuration: Long,
  lastExecutionTime: Long
) {
  def averageDuration: Double =
    if (totalOperations > 0) totalDuration.toDouble / totalOperations else 0.0

  def successRate: Double =
    if (totalOperations > 0) (successCount.toDouble / totalOperations) * 100 else 0.0

  def failureRate: Double = 100.0 - successRate
}
