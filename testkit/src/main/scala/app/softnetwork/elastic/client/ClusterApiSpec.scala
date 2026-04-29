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

import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticSuccess}
import app.softnetwork.elastic.scalatest.ElasticTestKit
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

trait ClusterApiSpec extends AnyFlatSpecLike with Matchers {
  self: ElasticTestKit =>

  lazy val log: Logger = LoggerFactory getLogger getClass.getName

  def client: ClusterApi

  "clusterName" should "return a non-empty cluster name" in {
    client.clusterName match {
      case ElasticSuccess(name) =>
        name should not be empty
      case ElasticFailure(error) =>
        fail(s"Expected cluster name but got failure: ${error.message}")
    }
  }

  it should "return a cached result on subsequent calls" in {
    val first = client.clusterName
    val second = client.clusterName
    first shouldBe second
  }

  "clusterUuid" should "return a non-empty cluster UUID" in {
    client.clusterUuid match {
      case ElasticSuccess(uuid) =>
        uuid should not be empty
      case ElasticFailure(error) =>
        fail(s"Expected cluster UUID but got failure: ${error.message}")
    }
  }

  it should "return a cached result on subsequent calls" in {
    val first = client.clusterUuid
    val second = client.clusterUuid
    first shouldBe second
  }

  it should "return a UUID different from the cluster name" in {
    (client.clusterName, client.clusterUuid) match {
      case (ElasticSuccess(name), ElasticSuccess(uuid)) =>
        uuid should not equal name
      case _ =>
      // If either fails, skip this assertion (tested individually above)
    }
  }
}
