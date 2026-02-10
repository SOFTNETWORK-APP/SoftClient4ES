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

import app.softnetwork.elastic.client.help.HelpRegistry
import org.jline.reader.{Candidate, Completer, LineReader, ParsedLine}

import java.util

class ReplCompleter extends Completer {

  private val simpleKeywords = Set(
    // DQL
    "SELECT",
    "FROM",
    "WHERE",
    "GROUP",
    "ORDER",
    "HAVING",
    "LIMIT",
    "OFFSET",
    "JOIN",
    "LEFT",
    "RIGHT",
    "INNER",
    "OUTER",
    "UNION",
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

    // Enrich/Watcher
    "TYPE",
    "MATCH",
    "GEO_MATCH",
    "RANGE",
    "ON",
    "EXECUTE",
    "SCHEDULE",
    "EVERY",
    "CONDITION",
    "ACTION",
    "WEBHOOK",
    "LOG",

    // Meta
    "SHOW",
    "DESCRIBE",
    "EXPLAIN",
    "BY",
    "ALL",
    "AS"
  )

  // Context-aware compound keywords
  private val compoundKeywords = Map(
    "GROUP"  -> List("BY"),
    "ORDER"  -> List("BY"),
    "LEFT"   -> List("JOIN", "OUTER JOIN"),
    "RIGHT"  -> List("JOIN", "OUTER JOIN"),
    "INNER"  -> List("JOIN"),
    "OUTER"  -> List("JOIN"),
    "UNION"  -> List("ALL"),
    "ENRICH" -> List("POLICY"),
    "GEO"    -> List("MATCH")
  )

  private val metaCommands = Set(
    ".help",
    ".h",
    ".quit",
    ".exit",
    ".q",
    ".tables",
    ".t",
    ".describe",
    ".desc",
    ".d",
    ".pipelines",
    ".p",
    ".watchers",
    ".w",
    ".policies",
    ".pol",
    ".history",
    ".clear",
    ".timing",
    ".format",
    ".timeout"
  )

  override def complete(
    reader: LineReader,
    line: ParsedLine,
    candidates: util.List[Candidate]
  ): Unit = {
    val buffer = line.line()
    val word = line.word()
    val wordUpper = word.toUpperCase

    // Meta commands (start with .)
    if (buffer.trim.toLowerCase.startsWith(".help ")) {
      val helpArg = buffer.trim.drop(6).toUpperCase
      HelpRegistry.allTopics
        .filter(_.startsWith(helpArg))
        .foreach { topic =>
          candidates.add(
            new Candidate(
              topic,
              topic,
              "help",
              "Help topic",
              null,
              null,
              true
            )
          )
        }
    } else if (buffer.trim.startsWith(".")) {
      metaCommands
        .filter(_.startsWith(word.toLowerCase))
        .foreach { cmd =>
          candidates.add(
            new Candidate(
              cmd,
              cmd,
              "meta", // group
              s"Meta command", // description
              null,
              null,
              true
            )
          )
        }
    }
    // SQL keywords
    else {
      // Get previous word for compound keyword detection
      val words = buffer.trim.split("\\s+")
      val previousWord = if (words.length > 1) words(words.length - 2).toUpperCase else ""

      // Check for compound keywords first
      if (compoundKeywords.contains(previousWord)) {
        compoundKeywords(previousWord)
          .filter(_.startsWith(wordUpper))
          .foreach { continuation =>
            candidates.add(
              new Candidate(
                continuation,
                continuation,
                "keyword",
                s"$previousWord $continuation",
                null,
                null,
                true
              )
            )
          }
      }

      // Add simple keywords
      simpleKeywords
        .filter(_.startsWith(wordUpper))
        .foreach { keyword =>
          candidates.add(
            new Candidate(
              keyword,
              keyword,
              "keyword",
              "SQL keyword",
              null,
              null,
              true
            )
          )
        }
    }
  }
}
