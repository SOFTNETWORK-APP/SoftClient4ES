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

package app.softnetwork.elastic.client.repl

import org.jline.reader.{Candidate, Completer, LineReader, ParsedLine}

import java.util

class SqlCompleter extends Completer {

  private val keywords = Set(
    // DQL
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP BY",
    "ORDER BY",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "JOIN",
    "LEFT JOIN",
    "RIGHT JOIN",
    "INNER JOIN",
    "OUTER JOIN",
    "UNION",
    "UNION ALL",
    "INTERSECT",
    "EXCEPT",

    // DML
    "INSERT",
    "INTO",
    "VALUES",
    "UPDATE",
    "SET",
    "DELETE",
    "COPY",
    "BULK",

    // DDL
    "CREATE",
    "ALTER",
    "DROP",
    "TRUNCATE",
    "RENAME",
    "TABLE",
    "INDEX",
    "VIEW",
    "PIPELINE",
    "WATCHER",
    "ENRICH",
    "POLICY",
    "TRANSFORM",

    // Functions
    "COUNT",
    "SUM",
    "AVG",
    "MIN",
    "MAX",
    "DISTINCT",
    "CAST",
    "COALESCE",
    "NULLIF",
    "CASE",
    "WHEN",
    "THEN",
    "ELSE",
    "END",

    // Operators
    "AND",
    "OR",
    "NOT",
    "IN",
    "LIKE",
    "BETWEEN",
    "IS",
    "NULL",
    "TRUE",
    "FALSE",

    // Enrich Policy
    "TYPE",
    "MATCH",
    "GEO_MATCH",
    "RANGE",
    "ON",
    "EXECUTE",

    // Watcher
    "SCHEDULE",
    "EVERY",
    "CONDITION",
    "ACTION",
    "WEBHOOK",
    "LOGGING",
    //"EMAIL",

    // Meta
    "SHOW",
    "DESCRIBE",
    "EXPLAIN"
  )

  override def complete(
    reader: LineReader,
    line: ParsedLine,
    candidates: util.List[Candidate]
  ): Unit = {
    val word = line.word().toUpperCase

    keywords
      .filter(_.startsWith(word))
      .foreach { keyword =>
        candidates.add(
          new Candidate(
            keyword,
            keyword,
            null, // group
            null, // description
            null, // suffix
            null, // key
            true // complete
          )
        )
      }
  }
}
