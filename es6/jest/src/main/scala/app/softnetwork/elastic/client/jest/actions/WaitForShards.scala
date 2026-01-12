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

import io.searchbox.action.{AbstractAction, GenericResultAbstractAction}
import io.searchbox.client.config.ElasticsearchVersion

object WaitForShards {
  class Builder(var index: String, var status: String = "yellow", var timeout: Int = 30)
      extends AbstractAction.Builder[WaitForShards, WaitForShards.Builder] {
    override def build = new WaitForShards(this)
  }
}

class WaitForShards protected (builder: WaitForShards.Builder)
    extends GenericResultAbstractAction(builder) {
  override def getRestMethodName = "GET"

  override def buildURI(elasticsearchVersion: ElasticsearchVersion): String =
    s"/_cluster/health/${builder.index}?wait_for_status=${builder.status}&timeout=${builder.timeout}s"

}
