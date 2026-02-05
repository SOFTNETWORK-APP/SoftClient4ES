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
import app.softnetwork.elastic.sql.query.Criteria
import app.softnetwork.elastic.sql.schema.mapper
import com.fasterxml.jackson.databind.JsonNode

/** Transform configuration models with built-in JSON serialization
  */
case class TransformConfig(
  id: String,
  viewName: String,
  source: TransformSource,
  dest: TransformDest,
  pivot: Option[TransformPivot] = None,
  latest: Option[TransformLatest] = None,
  sync: Option[TransformSync] = None,
  delay: Delay,
  frequency: Frequency,
  metadata: Map[String, AnyRef] = Map.empty
) extends DdlToken {

  /** Delay in seconds for display */
  lazy val delaySeconds: Long = delay.toSeconds

  /** Frequency in seconds for display */
  lazy val frequencySeconds: Long = frequency.toSeconds

  /** Converts to Elasticsearch JSON format
    */
  def node(implicit criteriaToNode: Criteria => JsonNode): JsonNode = {
    val node = mapper.createObjectNode()
    node.set("source", source.node)
    node.set("dest", dest.node)
    node.put("frequency", frequency.toTransformFormat)
    sync.foreach { s =>
      node.set("sync", s.node)
      ()
    }
    pivot.foreach { p =>
      node.set("pivot", p.node)
      ()
    }
    latest.foreach { l =>
      node.set("latest", l.node)
      ()
    }
    node
  }

  /** SQL DDL representation
    */
  override def sql: String = {
    val sb = new StringBuilder()

    sb.append(s"CREATE TRANSFORM $id\n")
    sb.append(s"  SOURCE ${source.sql}\n")
    sb.append(s"  DEST ${dest.sql}\n")

    pivot.foreach { p =>
      sb.append(s"  ${p.sql}\n")
    }

    sync.foreach { s =>
      sb.append(s"  ${s.sql}\n")
    }

    sb.append(s"  ${frequency.sql}\n")
    sb.append(s"  ${delay.sql}")

    sb.toString()
  }

  /** Human-readable description
    */
  def description: String = {
    s"Transform $id: ${source.index.mkString(", ")} → ${dest.index} " +
    s"(every ${frequencySeconds}s, delay ${delaySeconds}s) for view $viewName"
  }
}
