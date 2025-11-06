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

package app.softnetwork.elastic.client.monitoring

import app.softnetwork.elastic.client.metrics.MetricsElasticClient

trait PrometheusExporter { self: MetricsElasticClient =>

  def exportPrometheusMetrics: String = {
    val aggregated = getAggregatedMetrics
    val sb = new StringBuilder()

    // Global metrics
    sb.append(s"elasticsearch_operations_total ${aggregated.totalOperations}\n")
    sb.append(s"elasticsearch_operations_success ${aggregated.successCount}\n")
    sb.append(s"elasticsearch_operations_failure ${aggregated.failureCount}\n")
    sb.append(s"elasticsearch_operations_duration_ms ${aggregated.totalDuration}\n")

    // Per-operation metrics
    aggregated.operationMetrics.foreach { case (op, m) =>
      sb.append(s"""elasticsearch_operation_total{operation="$op"} ${m.totalOperations}""" + "\n")
      sb.append(
        s"""elasticsearch_operation_duration_ms{operation="$op"} ${m.totalDuration}""" + "\n"
      )
      sb.append(
        s"""elasticsearch_operation_success_rate{operation="$op"} ${m.successRate}""" + "\n"
      )
    }

    // Per-index metrics
    aggregated.indexMetrics.foreach { case (idx, m) =>
      sb.append(s"""elasticsearch_index_operations{index="$idx"} ${m.totalOperations}""" + "\n")
      sb.append(s"""elasticsearch_index_duration_ms{index="$idx"} ${m.totalDuration}""" + "\n")
    }

    sb.toString()
  }
}
