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

package app.softnetwork.elastic.licensing

import app.softnetwork.elastic.licensing.metrics.{AggregatedMetrics, MetricsApi, OperationMetrics}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TelemetryCollectorSpec extends AnyFlatSpec with Matchers {

  private val noop = MetricsApi.Noop

  // --- Noop collector ---

  "TelemetryCollector.Noop" should "return zero-valued TelemetryData" in {
    val data = TelemetryCollector.Noop.collect(noop)
    data.queriesTotal shouldBe 0
    data.mvsActive shouldBe 0
    data.clustersConnected shouldBe 0
    data.clusterId shouldBe None
    data.clusterName shouldBe None
    data.clusterVersion shouldBe None
    data.aggregatedMetrics shouldBe AggregatedMetrics.empty
  }

  // --- incrementQueries ---

  "TelemetryCollector" should "increment queries counter atomically" in {
    val collector = new TelemetryCollector
    collector.incrementQueries()
    collector.incrementQueries()
    collector.incrementQueries()
    collector.collect(noop).queriesTotal shouldBe 3
  }

  // --- setMvsActive ---

  it should "set MVs active gauge" in {
    val collector = new TelemetryCollector
    collector.setMvsActive(5)
    collector.collect(noop).mvsActive shouldBe 5
    collector.setMvsActive(3)
    collector.collect(noop).mvsActive shouldBe 3
  }

  // --- setClustersConnected ---

  it should "set clusters connected gauge" in {
    val collector = new TelemetryCollector
    collector.setClustersConnected(2)
    collector.collect(noop).clustersConnected shouldBe 2
  }

  // --- setClusterInfo ---

  it should "set cluster info" in {
    val collector = new TelemetryCollector
    collector.setClusterInfo("my-cluster", Some("prod-cluster-1"), Some("8.18.3"))
    val data = collector.collect(noop)
    data.clusterId shouldBe Some("my-cluster")
    data.clusterName shouldBe Some("prod-cluster-1")
    data.clusterVersion shouldBe Some("8.18.3")
  }

  // --- collect returns consistent snapshot ---

  it should "return a consistent snapshot combining all counters" in {
    val collector = new TelemetryCollector
    collector.incrementQueries()
    collector.incrementQueries()
    collector.setMvsActive(4)
    collector.setClustersConnected(1)
    collector.setClusterInfo("test-cluster", Some("test"), Some("9.0.0"))

    val data = collector.collect(noop)
    data.queriesTotal shouldBe 2
    data.mvsActive shouldBe 4
    data.clustersConnected shouldBe 1
    data.clusterId shouldBe Some("test-cluster")
    data.clusterName shouldBe Some("test")
    data.clusterVersion shouldBe Some("9.0.0")
    data.aggregatedMetrics shouldBe AggregatedMetrics.empty
  }

  // --- collectAndReset ---

  it should "atomically collect and reset aggregated metrics" in {
    val collector = new TelemetryCollector
    collector.incrementQueries()
    collector.incrementQueries()
    collector.setMvsActive(3)
    collector.setClusterInfo("uuid-1", Some("cluster-1"), Some("8.0.0"))

    // Create a mock MetricsApi that tracks reset calls
    var resetCalled = false
    val mockMetrics = new MetricsApi {
      override def recordOperation(
        operation: String,
        duration: Long,
        success: Boolean,
        index: Option[String]
      ): Unit = ()
      override def getMetrics: OperationMetrics = OperationMetrics.empty
      override def getMetricsByOperation(operation: String): Option[OperationMetrics] = None
      override def getMetricsByIndex(index: String): Option[OperationMetrics] = None
      override def getAggregatedMetrics: AggregatedMetrics = AggregatedMetrics.empty
      override def resetMetrics(): Unit = { resetCalled = true }
      override def collectAndResetAggregatedMetrics: AggregatedMetrics = {
        resetCalled = true
        AggregatedMetrics.empty
      }
    }

    val data = collector.collectAndReset(mockMetrics)
    data.queriesTotal shouldBe 2
    data.mvsActive shouldBe 3
    data.clusterId shouldBe Some("uuid-1")
    resetCalled shouldBe true
  }

  // --- concurrent access ---

  it should "handle concurrent increments safely" in {
    val collector = new TelemetryCollector
    val threads = (1 to 10).map { _ =>
      new Thread(() => {
        (1 to 1000).foreach { _ =>
          collector.incrementQueries()
        }
      })
    }
    threads.foreach(_.start())
    threads.foreach(_.join())

    val data = collector.collect(noop)
    data.queriesTotal shouldBe 10000
  }

  // --- Story 15.3: JOIN query counter ---

  "TelemetryCollector" should "increment the JOIN counter per row and sum to the total" in {
    val collector = new TelemetryCollector
    collector.incrementJoin(TelemetryCollector.JoinRow.Passthrough)
    collector.incrementJoin(TelemetryCollector.JoinRow.CrossCluster)
    collector.incrementJoin(TelemetryCollector.JoinRow.CrossCluster)
    collector.incrementJoin(TelemetryCollector.JoinRow.Coordinator)
    val data = collector.collect(noop)
    data.joinQueryByRow("passthrough") shouldBe 1
    data.joinQueryByRow("cross_cluster") shouldBe 2
    data.joinQueryByRow("coordinator") shouldBe 1
    data.joinQueryCount shouldBe 4
  }

  it should "ALWAYS carry all three JOIN row keys even when their buckets are 0" in {
    val collector = new TelemetryCollector
    val data = collector.collect(noop)
    data.joinQueryByRow.keySet shouldBe Set("passthrough", "cross_cluster", "coordinator")
    data.joinQueryByRow.values.toSet shouldBe Set(0L)
    data.joinQueryCount shouldBe 0
    // and the same on a reset read
    val reset = collector.collectAndReset(noop)
    reset.joinQueryByRow.keySet shouldBe Set("passthrough", "cross_cluster", "coordinator")
    reset.joinQueryCount shouldBe 0
  }

  it should "NOT reset the JOIN counters on collect (interval read without flush)" in {
    val collector = new TelemetryCollector
    collector.incrementJoin(TelemetryCollector.JoinRow.Passthrough)
    collector.collect(noop).joinQueryCount shouldBe 1
    collector.collect(noop).joinQueryCount shouldBe 1 // still 1 -- collect never resets
  }

  it should "reset the JOIN counters on collectAndReset (per-interval delta; shutdown flush)" in {
    val collector = new TelemetryCollector
    collector.incrementJoin(TelemetryCollector.JoinRow.Coordinator)
    collector.collectAndReset(noop).joinQueryCount shouldBe 1
    collector.collectAndReset(noop).joinQueryCount shouldBe 0 // delta reset after the interval
  }

  it should "snapshot+reset only the JOIN delta via collectAndResetJoinCounts (all 3 keys)" in {
    val collector = new TelemetryCollector
    collector.incrementJoin(TelemetryCollector.JoinRow.Passthrough)
    collector.incrementJoin(TelemetryCollector.JoinRow.Coordinator)
    val (total, byRow) = collector.collectAndResetJoinCounts()
    total shouldBe 2
    byRow shouldBe Map("passthrough" -> 1L, "cross_cluster" -> 0L, "coordinator" -> 1L)
    // reset: a second snapshot returns zeros, all 3 keys present
    val (total2, byRow2) = collector.collectAndResetJoinCounts()
    total2 shouldBe 0
    byRow2 shouldBe Map("passthrough" -> 0L, "cross_cluster" -> 0L, "coordinator" -> 0L)
    // and queriesTotal is NOT affected by the JOIN flush
    collector.incrementQueries()
    collector.collect(noop).queriesTotal shouldBe 1
  }

  it should "increment the JOIN counters concurrently without loss" in {
    val collector = new TelemetryCollector
    val threads = (1 to 10).map { _ =>
      new Thread(() => {
        (1 to 1000).foreach { _ =>
          collector.incrementJoin(TelemetryCollector.JoinRow.Passthrough)
        }
      })
    }
    threads.foreach(_.start())
    threads.foreach(_.join())
    collector.collect(noop).joinQueryCount shouldBe 10000
  }

  it should "record ONLY integer JOIN counts (no SQL text / identifiers -- AC 7)" in {
    val collector = new TelemetryCollector
    collector.incrementJoin(TelemetryCollector.JoinRow.CrossCluster)
    val data = collector.collect(noop)
    // The carrier exposes a Long total and a Map[String, Long] -- no String payload derived from
    // any SQL. The only String keys are the fixed row labels, never query-derived.
    data.joinQueryByRow.keySet shouldBe Set("passthrough", "cross_cluster", "coordinator")
    data.joinQueryCount shouldBe 1
  }

  "TelemetryCollector.Noop" should "no-op incrementJoin and zero-out collectAndResetJoinCounts" in {
    TelemetryCollector.Noop.incrementJoin(TelemetryCollector.JoinRow.Coordinator)
    TelemetryCollector.Noop.collect(noop).joinQueryCount shouldBe 0
    val (total, byRow) = TelemetryCollector.Noop.collectAndResetJoinCounts()
    total shouldBe 0
    byRow shouldBe Map("passthrough" -> 0L, "cross_cluster" -> 0L, "coordinator" -> 0L)
  }
}
