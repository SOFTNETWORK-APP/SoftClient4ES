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

package app.softnetwork.elastic.sql.transform

import app.softnetwork.elastic.sql.DdlToken
import app.softnetwork.elastic.sql.schema.mapper
import com.fasterxml.jackson.databind.JsonNode

case class TransformTimeSync(field: String, delay: Delay) extends DdlToken {
  override def sql: String = s"TIME FIELD $field ${delay.sql}"

  def node: JsonNode = {
    val node = mapper.createObjectNode()
    node.put("field", field)
    node.put("delay", delay.toTransformFormat)
    node
  }
}
