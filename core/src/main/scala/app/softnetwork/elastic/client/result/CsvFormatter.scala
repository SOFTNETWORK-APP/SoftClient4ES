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

import scala.annotation.tailrec

object CsvFormatter {

  @tailrec
  def format(result: QueryResult): String = {
    result match {
      case QueryRows(rows) if rows.nonEmpty =>
        val columns = rows.head.keys.toSeq
        val header = columns.mkString(",")
        val body = rows
          .map { row =>
            columns.map(col => escapeCsv(row.getOrElse(col, ""))).mkString(",")
          }
          .mkString("\n")
        s"$header\n$body"

      case QueryStructured(response) if response.results.nonEmpty =>
        format(QueryRows(response.results))

      case _ =>
        "No data"
    }
  }

  private def escapeCsv(value: Any): String = {
    val str = value match {
      case null  => ""
      case other => other.toString
    }

    if (str.contains(",") || str.contains("\"") || str.contains("\n")) {
      s""""${str.replace("\"", "\"\"")}""""
    } else {
      str
    }
  }
}
