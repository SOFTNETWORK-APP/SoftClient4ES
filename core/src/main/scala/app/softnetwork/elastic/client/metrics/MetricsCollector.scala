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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

import scala.jdk.CollectionConverters._

class MetricsCollector extends MetricsApi {

  private val metrics = new ConcurrentHashMap[String, MetricAccumulator]()
  private val indexMetrics = new ConcurrentHashMap[String, MetricAccumulator]()

  private class MetricAccumulator {
    val totalOps = new AtomicLong(0)
    val successOps = new AtomicLong(0)
    val failureOps = new AtomicLong(0)
    val totalDuration = new AtomicLong(0)
    val minDuration = new AtomicLong(Long.MaxValue)
    val maxDuration = new AtomicLong(Long.MinValue)
    val lastExecution = new AtomicLong(0)

    /** Records an operation with its duration and success status. Thread-safe implementation using
      * atomic operations.
      *
      * @param duration
      *   Operation duration in milliseconds
      * @param success
      *   Whether the operation succeeded
      */
    def record(duration: Long, success: Boolean): Unit = {
      // Update counters
      totalOps.incrementAndGet()
      if (success) successOps.incrementAndGet() else failureOps.incrementAndGet()
      totalDuration.addAndGet(duration)
      lastExecution.set(System.currentTimeMillis())

      // Update min duration using atomic operation
      minDuration.updateAndGet(current => Math.min(current, duration))

      // Update max duration using atomic operation
      maxDuration.updateAndGet(current => Math.max(current, duration))
    }

    /** Converts accumulated metrics to an OperationMetrics object.
      *
      * @param operation
      *   The operation name
      * @return
      *   OperationMetrics snapshot
      */
    def toMetrics(operation: String): OperationMetrics = {
      val min = minDuration.get()
      val max = maxDuration.get()

      OperationMetrics(
        operation = operation,
        totalOperations = totalOps.get(),
        successCount = successOps.get(),
        failureCount = failureOps.get(),
        totalDuration = totalDuration.get(),
        minDuration = if (min == Long.MaxValue) 0 else min,
        maxDuration = if (max == Long.MinValue) 0 else max,
        lastExecutionTime = lastExecution.get()
      )
    }

    /** Resets all metrics to initial values. Useful for testing or periodic metric resets.
      */
    def reset(): Unit = {
      totalOps.set(0)
      successOps.set(0)
      failureOps.set(0)
      totalDuration.set(0)
      minDuration.set(Long.MaxValue)
      maxDuration.set(Long.MinValue)
      lastExecution.set(0)
    }
  }

  override def recordOperation(
    operation: String,
    duration: Long,
    success: Boolean,
    index: Option[String] = None
  ): Unit = {
    // Record operation metrics
    val accumulator = metrics.computeIfAbsent(operation, _ => new MetricAccumulator())
    accumulator.record(duration, success)

    // Record index metrics if provided
    index.foreach { idx =>
      val idxAccumulator = indexMetrics.computeIfAbsent(idx, _ => new MetricAccumulator())
      idxAccumulator.record(duration, success)
    }
  }

  override def getMetrics: OperationMetrics = {
    val allMetrics = metrics.asScala.values.toSeq

    if (allMetrics.isEmpty) {
      OperationMetrics("all", 0, 0, 0, 0, 0, 0, 0)
    } else {
      OperationMetrics(
        operation = "all",
        totalOperations = allMetrics.map(_.totalOps.get()).sum,
        successCount = allMetrics.map(_.successOps.get()).sum,
        failureCount = allMetrics.map(_.failureOps.get()).sum,
        totalDuration = allMetrics.map(_.totalDuration.get()).sum,
        minDuration = allMetrics
          .map(_.minDuration.get())
          .filter(_ != Long.MaxValue)
          .reduceOption(_ min _)
          .getOrElse(0),
        maxDuration = allMetrics.map(_.maxDuration.get()).max,
        lastExecutionTime = allMetrics.map(_.lastExecution.get()).max
      )
    }
  }

  override def getMetricsByOperation(operation: String): Option[OperationMetrics] = {
    Option(metrics.get(operation)).map(_.toMetrics(operation))
  }

  override def getMetricsByIndex(index: String): Option[OperationMetrics] = {
    Option(indexMetrics.get(index)).map(_.toMetrics(index))
  }

  override def getAggregatedMetrics: AggregatedMetrics = {
    val globalMetrics = getMetrics
    AggregatedMetrics(
      totalOperations = globalMetrics.totalOperations,
      successCount = globalMetrics.successCount,
      failureCount = globalMetrics.failureCount,
      totalDuration = globalMetrics.totalDuration,
      operationMetrics = metrics.asScala.map { case (op, acc) =>
        op -> acc.toMetrics(op)
      }.toMap,
      indexMetrics = indexMetrics.asScala.map { case (idx, acc) =>
        idx -> acc.toMetrics(idx)
      }.toMap
    )
  }

  override def resetMetrics(): Unit = {
    metrics.clear()
    indexMetrics.clear()
  }
}
