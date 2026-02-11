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

package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.schema.mapper
import app.softnetwork.elastic.sql.transform.TransformTimeInterval
import com.fasterxml.jackson.databind.JsonNode

import java.net.{URLDecoder, URLEncoder}
import scala.collection.immutable.ListMap
import scala.util.matching.Regex

package object http {
  sealed trait HttpPart extends DdlToken

  sealed trait UrlPart extends HttpPart

  sealed trait Protocol extends UrlPart {
    def name: String
    override def sql: String = name.toUpperCase
  }

  object Protocol {
    case object Http extends Protocol {
      val name: String = "http"
    }
    case object Https extends Protocol {
      val name: String = "https"
    }

    def apply(name: String): Protocol = name.trim.toLowerCase match {
      case "http"  => Http
      case "https" => Https
      case other   => throw new IllegalArgumentException(s"Invalid HTTP scheme: $other")
    }

    def apply(port: Port): Protocol = port match {
      case Port.DefaultHttpPort  => Http
      case Port.DefaultHttpsPort => Https
    }
  }

  case class Host(host: StringValue) extends UrlPart {
    override def sql: String = host.sql
  }

  sealed trait Port extends UrlPart {
    def port: IntValue
    def sql: String = s"PORT $port"
  }

  object Port {
    case object DefaultHttpPort extends Port {
      override val port: IntValue = IntValue(80)
    }
    case object DefaultHttpsPort extends Port {
      override val port: IntValue = IntValue(443)
    }
    case class CustomPort(override val port: IntValue) extends Port

    def apply(protocol: Protocol): Option[Port] = protocol match {
      case Protocol.Http  => Some(DefaultHttpPort)
      case Protocol.Https => Some(DefaultHttpsPort)
      case _              => None
    }
  }

  case class Path(path: StringValue) extends UrlPart {
    override def sql: String = path.sql
  }

  case class QueryParams(params: ListMap[String, Value[_]]) extends UrlPart {
    override def sql: String = s"PARAMS $params"
    lazy val encoded: String = {
      params
        .map { case (k, v) =>
          val value =
            v match {
              case StringValue(s)  => s
              case ShortValue(i)   => i.toString
              case IntValue(i)     => i.toString
              case LongValue(i)    => i.toString
              case BooleanValue(b) => b.toString
              case other =>
                throw new IllegalArgumentException(
                  s"Unsupported query parameter value type: ${other.getClass}"
                )
            }
          s"${URLEncoder.encode(k, "UTF-8")}=${URLEncoder.encode(value, "UTF-8")}"
        }
        .mkString("&")
    }
  }

  object QueryParams {
    def apply(query: String): QueryParams = {
      lazy val decoded: ListMap[String, Value[_]] = {
        ListMap(
          URLDecoder
            .decode(query, "UTF-8")
            .split("&")
            .map { param =>
              val parts = param.split("=")
              if (parts.length == 2) {
                parts(0) -> StringValue(parts(1))
              } else {
                parts(0) -> StringValue("")
              }
            }: _*
        )
      }
      QueryParams(decoded)
    }
  }

  case class Url(
    protocol: Protocol,
    host: Host,
    port: Option[Port],
    path: Option[Path],
    query: Option[QueryParams]
  ) extends DdlToken
      with UrlPart {
    override def sql: String = {
      val portPart = port.map(p => s":${p.port.value}").getOrElse("")
      val pathPart = path.map(p => p.path.value).getOrElse("/")
      val queryPart = query.map(q => s"?${q.encoded}").getOrElse("")
      s""""${protocol.name}://${host.host.value}$portPart$pathPart$queryPart""""
    }
  }

  object Url {
    val urlPattern: Regex = """^(https?)://([^:/]+)(?::(\d+))?(/[^?]*)?(?:\?(.*))?$""".r

    def apply(url: String): Url = {
      url match {
        case urlPattern(protocol, host, port, path, query) =>
          Url(
            Protocol(protocol),
            Host(StringValue(host)),
            Option(port)
              .map(p => Port.CustomPort(IntValue(p.toInt)))
              .orElse(Port(Protocol(protocol))),
            Option(path).map(p => Path(StringValue(p))),
            Option(query).map(q => QueryParams(q))
          )
        case _ => throw new IllegalArgumentException(s"Invalid URL format: $url")
      }
    }

    def apply(parts: Seq[UrlPart]): Url = {
      val protocol = parts
        .collectFirst { case p: Protocol => p }
        .getOrElse(Protocol.Http)
      val host = parts
        .collectFirst { case h: Host => h }
        .getOrElse(throw new IllegalArgumentException("URL must include a host"))
      val port = parts.collectFirst { case p: Port => p }.orElse(Port(protocol))
      val path = parts.collectFirst { case p: Path => p }
      val query = parts.collectFirst { case q: QueryParams => q }
      Url(protocol, host, port, path, query)
    }
  }

  sealed trait Method extends HttpPart {
    def name: String
    override def sql: String = name.toUpperCase
  }

  object Method {
    case object Get extends Method {
      val name: String = "get"
    }
    case object Head extends Method {
      val name: String = "head"
    }
    case object Post extends Method {
      val name: String = "post"
    }
    case object Put extends Method {
      val name: String = "put"
    }
    case object Delete extends Method {
      val name: String = "delete"
    }
    case object Trace extends Method {
      val name: String = "trace"
    }
    case object Patch extends Method {
      val name: String = "patch"
    }

    def apply(name: String): Method = name.trim.toLowerCase match {
      case "get"    => Get
      case "head"   => Head
      case "post"   => Post
      case "put"    => Put
      case "delete" => Delete
      case "trace"  => Trace
      case "patch"  => Patch
      case other    => throw new IllegalArgumentException(s"Invalid HTTP method: $other")
    }
  }

  case class Headers(headers: ListMap[String, Value[_]]) extends HttpPart {
    override def sql: String = s"HEADERS ${ObjectValue(headers).ddl}"
  }

  case class Body(body: StringValue) extends HttpPart {
    private[http] lazy val normalizedBody: String = {
      val trimmed = body.value.trim
      val out = trimmed.replaceAll("\\\\\"", """"""").replaceAll("\"", "\\\\\"")
      out
    }

    override def sql: String = s"""BODY "$normalizedBody""""
  }

  case class Timeout(connection: Option[TransformTimeInterval], read: Option[TransformTimeInterval])
      extends DdlToken
      with HttpPart {
    override def sql: String = {
      val map: ListMap[String, StringValue] = ListMap.empty ++
        connection.map(c => "connection" -> StringValue(c.toTransformFormat)) ++
        read.map(r => "read" -> StringValue(r.toTransformFormat))
      s"TIMEOUT ${ObjectValue(map).ddl}".trim
    }
  }

  object Timeout {
    def apply(timeout: Map[String, Value[_]]): Option[Timeout] = {
      val connection = timeout.get("connection").flatMap {
        case StringValue(s) => TransformTimeInterval(s)
        case _              => None
      }
      val read = timeout.get("read").flatMap {
        case StringValue(s) => TransformTimeInterval(s)
        case _              => None
      }
      (connection, read) match {
        case (None, None) => None
        case _            => Some(Timeout(connection, read))
      }
    }
  }

  case class HttpRequest(
    method: Method,
    url: Url,
    headers: Option[Headers] = None,
    body: Option[Body] = None,
    timeout: Option[Timeout] = None
  ) extends DdlToken {
    override def sql: String = {
      val headersPart = headers.map(h => s"\n${h.sql}").getOrElse("")
      val bodyPart = body.map(b => s"\n${b.sql}").getOrElse("")
      val timeoutPart = timeout.map(t => s"\n${t.sql}").getOrElse("")
      s"$method $url$headersPart$bodyPart$timeoutPart"
    }

    def node: JsonNode = {
      val node = mapper.createObjectNode()
      import url._

      node.put("scheme", protocol.name)
      node.put("host", host.host.value)
      node.put("port", port.map(_.port.value).getOrElse(80))

      node.put("method", method.name)

      path.foreach { p =>
        node.put("path", p.path.value)
        ()
      }

      headers.foreach { hdrs =>
        val headersNode = ObjectValue(hdrs.headers).toJson
        node.set("headers", headersNode)
        ()
      }

      query.foreach { prs =>
        val paramsNode = ObjectValue(prs.params).toJson
        node.set("params", paramsNode)
        ()
      }

      body.foreach { b =>
        node.put("body", b.body.value)
        ()
      }

      timeout.foreach { t =>
        t.connection.foreach { t =>
          node.put("connection_timeout", t.toTransformFormat)
          ()
        }
        t.read.foreach { t =>
          node.put("read_timeout", t.toTransformFormat)
          ()
        }
        ()
      }

      node
    }

  }
}
