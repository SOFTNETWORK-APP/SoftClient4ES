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

package app.softnetwork.elastic.scalatest

import app.softnetwork.elastic.SoftClient4esCoreTestkitBuildInfo
import app.softnetwork.elastic.client.ElasticRestClientTestKit
import org.scalatest.matchers.{MatchResult, Matcher}
import org.scalatest.{BeforeAndAfterAll, Suite}
import org.slf4j.Logger

import java.util.UUID

/** Created by smanciot on 18/05/2021.
  */
trait ElasticTestKit extends ElasticRestClientTestKit with BeforeAndAfterAll { _: Suite =>

  def log: Logger

  def elasticVersion: String = SoftClient4esCoreTestkitBuildInfo.elasticVersion

  lazy val clusterName: String = s"test-${UUID.randomUUID()}"

  def start(): Unit = ()

  def stop(): Unit = ()

  override def beforeAll(): Unit = {
    start()
    assert(
      createIndexTemplate(
        "all_templates",
        List("*"),
        Map("number_of_shards" -> 1, "number_of_replicas" -> 0)
      ).getStatusLine.getStatusCode < 400
    )
  }

  override def afterAll(): Unit = {
    restClient.close()
    stop()
  }

  def haveCount(expectedCount: Int): Matcher[String] =
    (index: String) => {
      val count = searchCount(index)
      MatchResult(
        count == expectedCount,
        s"Index $index had count $count but expected $expectedCount",
        s"Index $index had document count $expectedCount"
      )
    }

  def containDoc(expectedId: String): Matcher[String] =
    (index: String) => {
      MatchResult(
        documentExists(index, expectedId),
        s"Index $index did not contain expected document $expectedId",
        s"Index $index contained document $expectedId"
      )
    }

  def beCreated(): Matcher[String] =
    (index: String) => {
      MatchResult(
        doesIndexExists(index),
        s"Index $index did not exist",
        s"Index $index exists"
      )
    }

  def beEmpty(): Matcher[String] =
    (index: String) => {
      val count = searchCount(index)
      MatchResult(
        count == 0,
        s"Index $index was not empty",
        s"Index $index was empty"
      )
    }

  // Copy/paste methos HttpElasticSugar as it is not available yet

  // refresh all indexes
  def refreshAll(): Boolean =
    refresh().getStatusLine.getStatusCode < 400

  def ensureIndexExists(index: String): Unit = {
    createIndex(index)
  }

}
