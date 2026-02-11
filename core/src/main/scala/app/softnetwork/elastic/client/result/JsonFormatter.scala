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

package app.softnetwork.elastic.client.result

import org.json4s._
import org.json4s.native.JsonMethods._

import scala.concurrent.duration.Duration

object JsonFormatter {

  implicit val formats: Formats = DefaultFormats

  def format(result: QueryResult, executionTime: Duration): String = {
    result match {
      case QueryRows(rows) =>
        val json = JObject(
          "rows"              -> JArray(rows.map(rowToJson).toList),
          "count"             -> JInt(rows.size),
          "execution_time_ms" -> JInt(executionTime.toMillis)
        )
        pretty(render(json))

      case QueryStructured(response) =>
        val json = JObject(
          "rows"              -> JArray(response.results.map(rowToJson).toList),
          "count"             -> JInt(response.results.size),
          "execution_time_ms" -> JInt(executionTime.toMillis)
        )
        pretty(render(json))

      case DmlResult(inserted, updated, deleted, rejected) =>
        val json = JObject(
          "inserted"          -> JInt(inserted),
          "updated"           -> JInt(updated),
          "deleted"           -> JInt(deleted),
          "rejected"          -> JInt(rejected),
          "execution_time_ms" -> JInt(executionTime.toMillis)
        )
        pretty(render(json))

      case DdlResult(success) =>
        val json = JObject(
          "success"           -> JBool(success),
          "execution_time_ms" -> JInt(executionTime.toMillis)
        )
        pretty(render(json))

      case _ =>
        compact(render(JObject("result" -> JString(result.toString))))
    }
  }

  private def rowToJson(row: Map[String, Any]): JValue = {
    JObject(row.map { case (k, v) => k -> valueToJson(v) }.toList)
  }

  private def valueToJson(value: Any): JValue = value match {
    case null           => JNull
    case s: String      => JString(s)
    case i: Short       => JInt(i)
    case i: Int         => JInt(i)
    case l: Long        => JLong(l)
    case d: Double      => JDouble(d)
    case b: Boolean     => JBool(b)
    case seq: Seq[_]    => JArray(seq.map(valueToJson).toList)
    case map: Map[_, _] => JObject(map.map { case (k, v) => k.toString -> valueToJson(v) }.toList)
    case other          => JString(other.toString)
  }
}
