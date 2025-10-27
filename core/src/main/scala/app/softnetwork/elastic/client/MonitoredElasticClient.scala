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

import akka.actor.{ActorSystem, Cancellable}

class MonitoredElasticClient(
  delegate: ElasticClientApi,
  metricsCollector: MetricsCollector,
  monitoringConfig: MonitoringConfig
)(implicit system: ActorSystem)
    extends MetricsElasticClient(delegate, metricsCollector) {

  import system.dispatcher

  private val cancellable: Cancellable = system.scheduler.scheduleAtFixedRate(
    monitoringConfig.interval.asInstanceOf,
    monitoringConfig.interval.asInstanceOf
  ) { () =>
    logMetrics()
    checkAlerts()
  }

  private def logMetrics(): Unit = {
    val metrics = getMetrics
    logger.info(
      s"""
         |=== Elasticsearch Metrics ===
         |Total Operations: ${metrics.totalOperations}
         |Success Rate: ${metrics.successRate}%
         |Failure Rate: ${metrics.failureRate}%
         |Average Duration: ${metrics.averageDuration}ms
         |Min Duration: ${metrics.minDuration}ms
         |Max Duration: ${metrics.maxDuration}ms
         |=============================
       """.stripMargin
    )

    // Log par opération
    val aggregated = getAggregatedMetrics
    aggregated.operationMetrics.foreach { case (op, m) =>
      if (m.totalOperations > 0) {
        logger.debug(
          s"[$op] ops=${m.totalOperations}, success=${m.successRate}%, avg=${m.averageDuration}ms"
        )
      }
    }
  }

  private def checkAlerts(): Unit = {
    val metrics = getMetrics

    // Alert sur taux d'échec élevé
    if (metrics.failureRate > monitoringConfig.failureRateThreshold) {
      logger.warn(s"⚠️  HIGH FAILURE RATE: ${metrics.failureRate}%")
    }

    // Alert sur latence élevée
    if (metrics.averageDuration > monitoringConfig.latencyThreshold) {
      logger.warn(s"⚠️  HIGH LATENCY: ${metrics.averageDuration}ms")
    }

    // Alerts par opération
    val aggregated = getAggregatedMetrics
    aggregated.operationMetrics.foreach { case (op, m) =>
      if (m.failureRate > monitoringConfig.failureRateThreshold) {
        logger.warn(s"⚠️  HIGH FAILURE RATE for [$op]: ${m.failureRate}%")
      }
      if (m.averageDuration > monitoringConfig.latencyThreshold) {
        logger.warn(s"⚠️  HIGH LATENCY for [$op]: ${m.averageDuration}ms")
      }
    }
  }

  def shutdown(): Unit = {
    cancellable.cancel()
  }
}
