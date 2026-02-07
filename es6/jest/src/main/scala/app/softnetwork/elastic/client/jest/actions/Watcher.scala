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

package app.softnetwork.elastic.client.jest.actions

import io.searchbox.client.config.ElasticsearchVersion

object Watcher {

  import io.searchbox.action.AbstractAction
  import io.searchbox.client.JestResult
  import com.google.gson.Gson

  case class Create(watcherId: String, json: String) extends AbstractAction[JestResult] {

    payload = json

    override def getRestMethodName: String = "PUT"

    override def getURI(elasticsearchVersion: ElasticsearchVersion): String =
      s"/_watcher/watch/$watcherId"

    override def createNewElasticSearchResult(
      json: String,
      statusCode: Int,
      reasonPhrase: String,
      gson: Gson
    ): JestResult = {
      val result = new JestResult(gson)
      result.setResponseCode(statusCode)
      result.setSucceeded(statusCode == 200 || statusCode == 201)
      Option(json).foreach(result.setJsonString)
      Option(reasonPhrase).foreach(result.setErrorMessage)
      result
    }
  }

  case class Get(watcherId: String) extends AbstractAction[JestResult] {

    override def getRestMethodName: String = "GET"

    override def getURI(elasticsearchVersion: ElasticsearchVersion): String =
      s"/_watcher/watch/$watcherId"

    override def createNewElasticSearchResult(
      json: String,
      statusCode: Int,
      reasonPhrase: String,
      gson: Gson
    ): JestResult = {
      val result = new JestResult(gson)
      result.setResponseCode(statusCode)
      result.setSucceeded(statusCode == 200 || statusCode == 201 || statusCode == 404)
      if (statusCode != 404) {
        Option(json).foreach(result.setJsonString)
      }
      Option(reasonPhrase).foreach(result.setErrorMessage)
      result
    }
  }

  case class Delete(watcherId: String) extends AbstractAction[JestResult] {

    override def getRestMethodName: String = "DELETE"

    override def getURI(elasticsearchVersion: ElasticsearchVersion): String =
      s"/_watcher/watch/$watcherId"

    override def createNewElasticSearchResult(
      json: String,
      statusCode: Int,
      reasonPhrase: String,
      gson: Gson
    ): JestResult = {
      val result = new JestResult(gson)
      result.setResponseCode(statusCode)
      result.setSucceeded(statusCode == 200 || statusCode == 201)
      Option(json).foreach(result.setJsonString)
      Option(reasonPhrase).foreach(result.setErrorMessage)
      result
    }
  }
}
