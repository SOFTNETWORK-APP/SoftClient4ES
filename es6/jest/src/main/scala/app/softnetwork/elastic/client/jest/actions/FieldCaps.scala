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

import com.google.gson.Gson
import io.searchbox.action.AbstractAction
import io.searchbox.client.JestResult
import io.searchbox.client.config.ElasticsearchVersion

/** SCHEMA-7 (D7) -- `GET <source>/_field_caps?fields=*`: Jest 6.3.1 has no action for it. A non-2xx
  * answer is a failed result carrying its status (403 for a user who may not read it).
  */
case class FieldCaps(source: String) extends AbstractAction[JestResult] {

  override def getRestMethodName: String = "GET"

  override def getURI(elasticsearchVersion: ElasticsearchVersion): String =
    s"/$source/_field_caps?fields=*"

  override def createNewElasticSearchResult(
    json: String,
    statusCode: Int,
    reasonPhrase: String,
    gson: Gson
  ): JestResult = {
    val result = new JestResult(gson)
    result.setResponseCode(statusCode)
    result.setSucceeded(statusCode >= 200 && statusCode < 300)
    Option(json).foreach(result.setJsonString)
    Option(reasonPhrase).foreach(result.setErrorMessage)
    result
  }
}
