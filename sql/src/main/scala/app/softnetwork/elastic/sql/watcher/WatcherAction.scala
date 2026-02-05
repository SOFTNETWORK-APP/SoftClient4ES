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

import app.softnetwork.elastic.sql.http._
import app.softnetwork.elastic.sql.schema.mapper
import app.softnetwork.elastic.sql.transform.{Delay, TransformTimeInterval, TransformTimeUnit}
import app.softnetwork.elastic.sql.{DdlToken, IntValue, ObjectValue, StringValue, Value}
import com.fasterxml.jackson.databind.JsonNode

import java.net.URLEncoder

sealed trait WatcherAction extends DdlToken {
  def node: JsonNode
  def foreach: Option[String]
  def limit: Option[Int]
}

/** Configuration for logging action
  *
  * @param text
  *   Log message text
  * @param level
  *   Optional log level (default: info)
  */
case class LoggingActionConfig(
  text: String,
  level: Option[LoggingLevel] = Some(LoggingLevel.INFO)
) extends DdlToken {

  def sql: String = s"\"$text\"" + level.map(l => s" AT $l").getOrElse("")

  def node: JsonNode = {
    val node = mapper.createObjectNode()
    node.put("text", text)
    level.foreach { lvl =>
      node.put("level", lvl.name)
      ()
    }
    node
  }

  def options: Map[String, Value[_]] = {
    Map("text" -> StringValue(text)) ++
    level.map(l => "level" -> StringValue(l.name))
  }
}

sealed trait LoggingLevel extends DdlToken {
  def name: String
  override def sql: String = name.toUpperCase
}

object LoggingLevel {
  case object DEBUG extends LoggingLevel {
    val name: String = "debug"
  }
  case object INFO extends LoggingLevel {
    val name: String = "info"
  }
  case object WARN extends LoggingLevel {
    val name: String = "warn"
  }
  case object ERROR extends LoggingLevel {
    val name: String = "error"
  }

  def apply(name: String): LoggingLevel = name.trim.toLowerCase match {
    case "debug" => DEBUG
    case "info"  => INFO
    case "warn"  => WARN
    case "error" => ERROR
    case other   => throw new IllegalArgumentException(s"Invalid logging level: $other")
  }
}

/** Logging action
  *
  * @param logging
  *   Logging configuration
  */
case class LoggingAction(
  logging: LoggingActionConfig,
  foreach: Option[String] = None,
  limit: Option[Int] = None
) extends WatcherAction {

  def sql: String = s"LOG $logging" +
    foreach.map(f => s" FOREACH \"$f\"").getOrElse("") +
    limit.map(mi => s" LIMIT $mi").getOrElse("")

  override def node: JsonNode = {
    val node = mapper.createObjectNode()
    foreach.foreach { fe =>
      node.put("foreach", fe)
      ()
    }
    limit.foreach { mi =>
      node.put("max_iterations", mi)
      ()
    }
    val loggingNode = logging.node
    node.set("logging", loggingNode)
    node
  }
}

/** Configuration for webhook action
  *
  * @param protocol
  *   URL scheme (http or https)
  * @param host
  *   Hostname
  * @param port
  *   Port number
  * @param method
  *   HTTP method (GET, POST, etc.)
  * @param path
  *   URL path
  * @param headers
  *   Optional HTTP headers
  * @param body
  *   Optional request body
  */
case class WebhookActionConfig(
  protocol: Protocol = Protocol.Http,
  host: String,
  port: Int,
  path: String,
  params: Option[Map[String, String]] = None,
  method: Method = Method.Get,
  headers: Option[Map[String, String]] = None,
  body: Option[String] = None,
  connectionTimeout: Option[TransformTimeInterval] = Some(Delay(TransformTimeUnit.Seconds, 5)),
  readTimeout: Option[TransformTimeInterval] = Some(Delay(TransformTimeUnit.Seconds, 30))
) extends DdlToken {
  private lazy val encodedParams: String = params match {
    case Some(ps) if ps.nonEmpty =>
      val params = ps
        .map { case (k, v) => s"${URLEncoder.encode(k, "UTF-8")}=${URLEncoder.encode(v, "UTF-8")}" }
        .mkString("&")
      s"?$params"
    case _ => ""
  }

  lazy val url: String = "%s://%s:%d%s%s".format(protocol, host, port, path, encodedParams)

  def node: JsonNode = {
    val node = mapper.createObjectNode()
    node.put("scheme", protocol.name)
    node.put("host", host)
    node.put("port", port)
    node.put("method", method.name)
    node.put("path", path)

    headers.foreach { hdrs =>
      val headersNode = mapper.createObjectNode()
      hdrs.foreach { case (k, v) =>
        headersNode.put(k, v)
        ()
      }
      node.set("headers", headersNode)
      ()
    }

    params.foreach { prs =>
      val paramsNode = mapper.createObjectNode()
      prs.foreach { case (k, v) =>
        paramsNode.put(k, v)
        ()
      }
      node.set("params", paramsNode)
      ()
    }

    body.foreach { b =>
      node.put("body", b)
      ()
    }

    connectionTimeout.foreach { t =>
      node.put("connection_timeout", t.toTransformFormat)
      ()
    }

    readTimeout.foreach { t =>
      node.put("read_timeout", t.toTransformFormat)
      ()
    }

    node
  }

  def sql: String = {
    val sb = new StringBuilder
    sb.append(s"$url WITH")
    sb.append(s" METHOD ${method.name.toUpperCase}")
    headers.foreach { hdrs =>
      val headersSql = hdrs.map { case (k, v) => s""""$k":"$v"""" }.mkString(", ")
      sb.append(s" HEADERS { $headersSql }")
    }
    body.foreach { b =>
      sb.append(s" BODY '$b'")
    }
    connectionTimeout.foreach { t =>
      sb.append(s" CONNECTION_TIMEOUT '${t.toTransformFormat}'")
    }
    readTimeout.foreach { t =>
      sb.append(s" READ_TIMEOUT '${t.toTransformFormat}'")
    }
    sb.toString()
  }

  def options: Map[String, Value[_]] = {
    Map(
      "scheme" -> StringValue(protocol.name),
      "host"   -> StringValue(host),
      "port"   -> IntValue(port),
      "method" -> StringValue(method.name),
      "path"   -> StringValue(path)
    ) ++
    headers.map(h => "headers" -> ObjectValue(h.map { case (k, v) => k -> StringValue(v) })) ++
    params.map(p => "params" -> ObjectValue(p.map { case (k, v) => k -> StringValue(v) })) ++
    body.map("body" -> StringValue(_)) ++
    connectionTimeout.map(t => "connection_timeout" -> StringValue(t.toTransformFormat)) ++
    readTimeout.map(t => "read_timeout" -> StringValue(t.toTransformFormat))
  }
}

/** Webhook action
  *
  * @param webhook
  *   Webhook configuration
  */
case class WebhookAction(
  webhook: HttpRequest,
  foreach: Option[String] = None,
  limit: Option[Int] = None
) extends WatcherAction {
  def sql: String = s"WEBHOOK $webhook" +
    foreach.map(f => s" FOREACH \"$f\"").getOrElse("") +
    limit.map(mi => s" LIMIT $mi").getOrElse("")

  override def node: JsonNode = {
    val node = mapper.createObjectNode()
    val webhookNode = webhook.node
    foreach.foreach { fe =>
      node.put("foreach", fe)
      ()
    }
    limit.foreach { mi =>
      node.put("max_iterations", mi)
      ()
    }
    node.set("webhook", webhookNode)
    node
  }
}
