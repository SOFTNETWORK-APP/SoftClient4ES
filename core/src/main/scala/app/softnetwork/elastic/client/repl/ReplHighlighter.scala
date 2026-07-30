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

  // Single source of truth for SQL keywords (#161): parser-derived registry + REPL extras.
  private val keywords: Set[String] = ReplKeywords.all

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
