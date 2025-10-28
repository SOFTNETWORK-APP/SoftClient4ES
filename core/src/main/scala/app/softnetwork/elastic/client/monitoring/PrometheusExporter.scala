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
      sb.append(s"""elasticsearch_operation_duration_ms{operation="$op"} ${m.totalDuration}""" + "\n")
      sb.append(s"""elasticsearch_operation_success_rate{operation="$op"} ${m.successRate}""" + "\n")
    }

    // Per-index metrics
    aggregated.indexMetrics.foreach { case (idx, m) =>
      sb.append(s"""elasticsearch_index_operations{index="$idx"} ${m.totalOperations}""" + "\n")
      sb.append(s"""elasticsearch_index_duration_ms{index="$idx"} ${m.totalDuration}""" + "\n")
    }

    sb.toString()
  }
}

