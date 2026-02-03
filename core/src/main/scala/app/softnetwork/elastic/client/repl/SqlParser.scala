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

import org.jline.reader.{EOFError, ParsedLine, Parser}

// ==================== SQL Parser (for multiline) ====================

class SqlParser extends Parser {
  override def parse(line: String, cursor: Int, context: Parser.ParseContext): ParsedLine = {
    val trimmed = line.trim

    // Meta commands are always complete (no ; needed)
    if (trimmed.startsWith(".")) {
      new SqlParsedLine(line, trimmed.split("\\s+").toList)
    }
    // SQL statements require semicolon
    else if (trimmed.endsWith(";")) {
      new SqlParsedLine(line, List(trimmed))
    }
    // Incomplete statement - ask for more input
    else if (context == Parser.ParseContext.ACCEPT_LINE) {
      throw new EOFError(-1, -1, "Statement not terminated with ;")
    }
    // In other contexts (e.g., completion), just parse as-is
    else {
      new SqlParsedLine(line, List(trimmed))
    }
  }
}

class SqlParsedLine(val _line: String, val _words: List[String]) extends ParsedLine {
  override def word(): String = _words.headOption.getOrElse("")
  override def wordCursor(): Int = 0
  override def wordIndex(): Int = 0
  override def words(): java.util.List[String] = {
    import scala.jdk.CollectionConverters._
    _words.asJava
  }
  override def line(): String = _line
  override def cursor(): Int = _line.length
}
