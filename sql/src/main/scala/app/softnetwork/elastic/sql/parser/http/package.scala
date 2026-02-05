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

package object http {

  trait HttpParser { self: Parser =>

    // URL parser
    def url: PackratParser[Url] = literal ^^ { urlStr =>
      Url(urlStr.value)
    }

    // url protocol parser
    def http: PackratParser[Protocol.Http.type] =
      "(?i)(HTTP)\\b".r ^^ { _ => Protocol.Http }
    def https: PackratParser[Protocol.Https.type] =
      "(?i)(HTTPS)\\b".r ^^ { _ => Protocol.Https }

    def urlProtocol: PackratParser[Protocol] =
      "PROTOCOL" ~> (https | http)

    // url host parser
    def urlHost: PackratParser[Host] =
      "HOST" ~> literal ^^ { hostStr =>
        Host(hostStr)
      }

    // url port parser
    def urlPort: PackratParser[Port] =
      "PORT" ~> long ^^ { l =>
        Port.CustomPort(IntValue(l.value.toInt))
      }

    // url path parser
    def urlPath: PackratParser[Path] =
      "PATH" ~> literal ^^ { pathStr =>
        Path(pathStr)
      }

    // url query parameters parser
    def urlQueryParams: PackratParser[QueryParams] =
      "PARAMS" ~> start ~ repsep(option, separator) ~ end ^^ { case _ ~ opts ~ _ =>
        QueryParams(opts.toMap)
      }

    // url part parser
    def urlPart: PackratParser[UrlPart] = urlProtocol | urlHost | urlPort | urlPath | urlQueryParams

    // combined url parts parser
    def urlParts: PackratParser[Url] =
      rep(urlPart) ^^ { parts =>
        Url(parts)
      }

    // method parser
    def get: PackratParser[Method.Get.type] =
      "(?i)(GET)\\b".r ^^ { _ => Method.Get }
    def post: PackratParser[Method.Post.type] =
      "(?i)(POST)\\b".r ^^ { _ => Method.Post }
    def put: PackratParser[Method.Put.type] =
      "(?i)(PUT)\\b".r ^^ { _ => Method.Put }
    def del: PackratParser[Method.Delete.type] =
      "(?i)(DELETE)\\b".r ^^ { _ => Method.Delete }

    def httpMethod: PackratParser[Method] = get | post | put | del

    // headers parser
    def headers: PackratParser[Headers] =
      "HEADERS" ~> start ~ repsep(option, separator) ~ end ^^ { case _ ~ opts ~ _ =>
        Headers(opts.toMap)
      }

    // body parser
    def body: PackratParser[Body] =
      "BODY" ~> literal ^^ { body =>
        Body(body)
      }

    def timeout: PackratParser[Option[Timeout]] =
      "TIMEOUT" ~> start ~ repsep(option, separator) <~ end ^^ { case _ ~ t =>
        Timeout(t.toMap)
      }

    def httpRequest: PackratParser[HttpRequest] =
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
