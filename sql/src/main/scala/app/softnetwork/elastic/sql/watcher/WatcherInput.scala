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

package app.softnetwork.elastic.sql.watcher

import app.softnetwork.elastic.sql.http.HttpRequest
import app.softnetwork.elastic.sql.query.Criteria
import app.softnetwork.elastic.sql.schema.mapper
import app.softnetwork.elastic.sql.transform.TransformTimeInterval
import app.softnetwork.elastic.sql.{DdlToken, ObjectValue}
import com.fasterxml.jackson.databind.JsonNode

import scala.collection.immutable.ListMap

sealed trait WatcherInput extends DdlToken {
  def node(implicit criteriaToNode: Criteria => JsonNode): JsonNode
}

case object EmptyWatcherInput extends WatcherInput {
  override def sql: String = " WITH NO INPUT"
  override def node(implicit criteriaToNode: Criteria => JsonNode): JsonNode = {
    val node = mapper.createObjectNode()
    node
  }
}

case class SimpleWatcherInput(payload: ObjectValue) extends WatcherInput {
  override def sql: String = s" WITH INPUT ${payload.ddl}"
  override def node(implicit criteriaToNode: Criteria => JsonNode): JsonNode = {
    val node = mapper.createObjectNode()
    val payloadNode = payload.toJson
    node.set("simple", payloadNode.asInstanceOf[JsonNode])
    node
  }
}

case class SearchWatcherInput(
  index: Seq[String],
  query: Option[Criteria] = None,
  timeout: Option[TransformTimeInterval] = None
) extends WatcherInput {
  override def sql: String = {
    val queryClause = query.map(q => s" WHERE ${q.sql}").getOrElse("")
    val timeoutClause = timeout.map(t => s" WITHIN ${t.interval} ${t.timeUnit}").getOrElse("")
    s" FROM ${index.mkString(", ")}$queryClause$timeoutClause"
  }

  override def node(implicit criteriaToNode: Criteria => JsonNode): JsonNode = {
    val node = mapper.createObjectNode()
    val searchNode = mapper.createObjectNode()
    val requestNode = mapper.createObjectNode()

    val indicesNode = mapper.createArrayNode()
    index.foreach { idx =>
      indicesNode.add(idx)
      ()
    }
    requestNode.set("indices", indicesNode)

    val body = mapper.createObjectNode()
    query match {
      case Some(q) => body.set("query", implicitly[JsonNode](q))
      case None =>
        val q = mapper.createObjectNode()
        val matchAll = mapper.createObjectNode()
        q.set("match_all", matchAll)
        body.set("query", q)
        ()
    }
    requestNode.set("body", body)

    searchNode.set("request", requestNode)

    timeout.foreach { t =>
      searchNode.put("timeout", t.toTransformFormat)
      ()
    }

    node.set("search", searchNode)

    node
  }
}

case class HttpInput(request: HttpRequest) extends WatcherInput {
  override def sql: String = s" WITH INPUT ${request.sql}"
  override def node(implicit criteriaToNode: Criteria => JsonNode): JsonNode = {
    val node = mapper.createObjectNode()
    val req = mapper.createObjectNode()
    req.set("request", request.node)
    node.set("http", req)
    node
  }
}

case class ChainInput(inputs: ListMap[String, WatcherInput]) extends WatcherInput {
  override def sql: String = {
    val inputsSql = inputs.map { case (name, input) => s"$name AS ${input.sql}" }.mkString(", ")
    s" WITH INPUTS $inputsSql"
  }

  override def node(implicit criteriaToNode: Criteria => JsonNode): JsonNode = {
    val node = mapper.createObjectNode()
    val inputsNode = mapper.createObjectNode()
    val inputsArrayNode = mapper.createArrayNode()
    inputs.foreach { case (name, input) =>
      val inputNode = mapper.createObjectNode()
      inputNode.set(name, input.node)
      inputsArrayNode.add(inputNode)
      ()
    }
    inputsNode.set("inputs", inputsArrayNode)
    node.set("chain", inputsNode)
    node
  }
}
