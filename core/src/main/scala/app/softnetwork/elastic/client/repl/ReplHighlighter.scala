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

import org.jline.reader.{Highlighter, LineReader}
import org.jline.utils.{AttributedString, AttributedStringBuilder, AttributedStyle}

class ReplHighlighter extends Highlighter {

  private val keywords = Set(
    "SELECT",
    "FROM",
    "WHERE",
    "INSERT",
    "UPDATE",
    "DELETE",
    "CREATE",
    "ALTER",
    "DROP",
    "TABLE",
    "INDEX",
    "VIEW",
    "AND",
    "OR",
    "NOT",
    "IN",
    "LIKE",
    "BETWEEN",
    "JOIN",
    "LEFT",
    "RIGHT",
    "INNER",
    "OUTER",
    "ENRICH",
    "POLICY",
    "WATCHER",
    "TRANSFORM",
    "PIPELINE"
  )

  override def highlight(reader: LineReader, buffer: String): AttributedString = {
    val builder = new AttributedStringBuilder()

    buffer.split("\\b").foreach { token =>
      val style = if (keywords.contains(token.toUpperCase)) {
        AttributedStyle.DEFAULT.foreground(AttributedStyle.BLUE).bold()
      } else if (token.matches("'[^']*'")) {
        AttributedStyle.DEFAULT.foreground(AttributedStyle.GREEN)
      } else if (token.matches("\\d+")) {
        AttributedStyle.DEFAULT.foreground(AttributedStyle.YELLOW)
      } else {
        AttributedStyle.DEFAULT
      }

      builder.styled(style, token)
    }

    builder.toAttributedString
  }
}
