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

import app.softnetwork.elastic.licensing.metrics.{AggregatedMetrics, MetricsApi}
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
}
