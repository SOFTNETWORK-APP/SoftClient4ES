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

package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.IntValue
import app.softnetwork.elastic.sql.http.{
  Body,
  Headers,
  Host,
  HttpRequest,
  Method,
  Path,
  Port,
  Protocol,
  QueryParams,
  Timeout,
  Url,
  UrlPart
}

import scala.collection.immutable.ListMap

package object http {

  trait HttpParser { self: Parser =>

    // URL parser
    lazy val url: PackratParser[Url] = literal ^^ { urlStr =>
      Url(urlStr.value)
    }

    // url protocol parser
    lazy val http: PackratParser[Protocol.Http.type] =
      "(?i)(HTTP)\\b".r ^^ { _ => Protocol.Http }
    lazy val https: PackratParser[Protocol.Https.type] =
      "(?i)(HTTPS)\\b".r ^^ { _ => Protocol.Https }

    lazy val urlProtocol: PackratParser[Protocol] =
      "PROTOCOL" ~> (https | http)

    // url host parser
    lazy val urlHost: PackratParser[Host] =
      "HOST" ~> literal ^^ { hostStr =>
        Host(hostStr)
      }

    // url port parser
    lazy val urlPort: PackratParser[Port] =
      "PORT" ~> long ^^ { l =>
        Port.CustomPort(IntValue(l.value.toInt))
      }

    // url path parser
    lazy val urlPath: PackratParser[Path] =
      "PATH" ~> literal ^^ { pathStr =>
        Path(pathStr)
      }

    // url query parameters parser
    lazy val urlQueryParams: PackratParser[QueryParams] =
      "PARAMS" ~> start ~ repsep(option, separator) ~ end ^^ { case _ ~ opts ~ _ =>
        QueryParams(ListMap(opts: _*))
      }

    // url part parser
    lazy val urlPart: PackratParser[UrlPart] =
      urlProtocol | urlHost | urlPort | urlPath | urlQueryParams

    // combined url parts parser
    lazy val urlParts: PackratParser[Url] =
      rep(urlPart) ^^ { parts =>
        Url(parts)
      }

    // method parser
    lazy val get: PackratParser[Method.Get.type] =
      "(?i)(GET)\\b".r ^^ { _ => Method.Get }
    lazy val post: PackratParser[Method.Post.type] =
      "(?i)(POST)\\b".r ^^ { _ => Method.Post }
    lazy val put: PackratParser[Method.Put.type] =
      "(?i)(PUT)\\b".r ^^ { _ => Method.Put }
    lazy val del: PackratParser[Method.Delete.type] =
      "(?i)(DELETE)\\b".r ^^ { _ => Method.Delete }

    lazy val httpMethod: PackratParser[Method] = get | post | put | del

    // headers parser
    lazy val headers: PackratParser[Headers] =
      "HEADERS" ~> start ~ repsep(option, separator) ~ end ^^ { case _ ~ opts ~ _ =>
        Headers(ListMap(opts: _*))
      }

    // body parser
    lazy val body: PackratParser[Body] =
      "BODY" ~> literal ^^ { body =>
        Body(body)
      }

    lazy val timeout: PackratParser[Option[Timeout]] =
      "TIMEOUT" ~> start ~ repsep(option, separator) <~ end ^^ { case _ ~ t =>
        Timeout(t.toMap)
      }

    lazy val httpRequest: PackratParser[HttpRequest] =
      httpMethod ~ (url | urlParts) ~ opt(headers) ~ opt(body) ~ opt(timeout) ^^ {
        case method ~ url ~ headersOpt ~ bodyOpt ~ timeoutOpt =>
          HttpRequest(
            method = method,
            url = url,
            headers = headersOpt,
            body = bodyOpt,
            timeout = timeoutOpt.flatten
          )
      }

  }
}
