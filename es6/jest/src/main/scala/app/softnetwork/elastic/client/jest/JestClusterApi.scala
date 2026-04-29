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

package app.softnetwork.elastic.client.jest

import app.softnetwork.elastic.client.{result, ClusterApi}
import app.softnetwork.elastic.client.jest.actions.GetClusterInfo
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

trait JestClusterApi extends ClusterApi with JestClientHelpers {
  _: JestClientCompanion =>
  override private[client] def executeGetClusterName(): result.ElasticResult[String] =
    executeJestAction(
      "cluster_name",
      retryable = true
    )(
      new GetClusterInfo.Builder().build()
    ) { result =>
      val jsonString = result.getJsonString
      implicit val formats: DefaultFormats.type = DefaultFormats
      val json = JsonMethods.parse(jsonString)
      (json \ "cluster_name").extract[String]
    }

  override private[client] def executeGetClusterUuid(): result.ElasticResult[String] =
    executeJestAction(
      "cluster_uuid",
      retryable = true
    )(
      new GetClusterInfo.Builder().build()
    ) { result =>
      val jsonString = result.getJsonString
      implicit val formats: DefaultFormats.type = DefaultFormats
      val json = JsonMethods.parse(jsonString)
      (json \ "cluster_uuid").extract[String]
    }
}
