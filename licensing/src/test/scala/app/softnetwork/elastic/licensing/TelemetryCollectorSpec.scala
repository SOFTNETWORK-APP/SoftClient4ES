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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TelemetryCollectorSpec extends AnyFlatSpec with Matchers {

  // --- Noop collector ---

  "TelemetryCollector.Noop" should "return zero-valued TelemetryData" in {
    val data = TelemetryCollector.Noop.collect()
    data shouldBe TelemetryData(0, 0, 0, 0)
  }

  // --- incrementQueries ---

  "TelemetryCollector" should "increment queries counter atomically" in {
    val collector = new TelemetryCollector
    collector.incrementQueries()
    collector.incrementQueries()
    collector.incrementQueries()
    collector.collect().queriesTotal shouldBe 3
  }

  // --- incrementJoins ---

  it should "increment joins counter atomically" in {
    val collector = new TelemetryCollector
    collector.incrementJoins()
    collector.incrementJoins()
    collector.collect().joinsTotal shouldBe 2
  }

  // --- setMvsActive ---

  it should "set MVs active gauge" in {
    val collector = new TelemetryCollector
    collector.setMvsActive(5)
    collector.collect().mvsActive shouldBe 5
    collector.setMvsActive(3)
    collector.collect().mvsActive shouldBe 3
  }

  // --- setClustersConnected ---

  it should "set clusters connected gauge" in {
    val collector = new TelemetryCollector
    collector.setClustersConnected(2)
    collector.collect().clustersConnected shouldBe 2
  }

  // --- collect returns consistent snapshot ---

  it should "return a consistent snapshot combining all counters" in {
    val collector = new TelemetryCollector
    collector.incrementQueries()
    collector.incrementQueries()
    collector.incrementJoins()
    collector.setMvsActive(4)
    collector.setClustersConnected(1)

    val data = collector.collect()
    data.queriesTotal shouldBe 2
    data.joinsTotal shouldBe 1
    data.mvsActive shouldBe 4
    data.clustersConnected shouldBe 1
  }

  // --- concurrent access ---

  it should "handle concurrent increments safely" in {
    val collector = new TelemetryCollector
    val threads = (1 to 10).map { _ =>
      new Thread(() => {
        (1 to 1000).foreach { _ =>
          collector.incrementQueries()
          collector.incrementJoins()
        }
      })
    }
    threads.foreach(_.start())
    threads.foreach(_.join())

    val data = collector.collect()
    data.queriesTotal shouldBe 10000
    data.joinsTotal shouldBe 10000
  }
}
