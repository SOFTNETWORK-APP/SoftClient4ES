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

import org.jline.reader.{CompletingParsedLine, ParsedLine, Parser}
import org.jline.reader.Parser.ParseContext

import scala.collection.mutable.ListBuffer
import scala.jdk.CollectionConverters._

// ==================== SQL Parser (for multiline) ====================

class ReplParser extends Parser {

  override def parse(line: String, cursor: Int, context: ParseContext): ParsedLine = {
    val trimmed = line.trim
    val isMetaCommand = trimmed.startsWith(".")

    new SqlParsedLine(line, cursor, isMetaCommand)
  }

  // Classe interne qui implémente CompletingParsedLine
  private class SqlParsedLine(
    val aline: String,
    val acursor: Int,
    val isMetaCommand: Boolean
  ) extends CompletingParsedLine {

    private val parsedWords: List[String] = parseWords(aline)
    private val (wordIdx, wordCurs) = findWordPosition(aline, acursor, parsedWords)

    // Parsing des mots avec support des quotes
    private def parseWords(line: String): List[String] = {
      val result = ListBuffer[String]()
      val word = new StringBuilder
      var inQuote = false
      var quoteChar: Char = 0

      line.foreach { c =>
        if (!inQuote && (c == '\'' || c == '"')) {
          inQuote = true
          quoteChar = c
          word.append(c)
        } else if (inQuote && c == quoteChar) {
          inQuote = false
          word.append(c)
        } else if (!inQuote && c.isWhitespace) {
          if (word.nonEmpty) {
            result += word.toString()
            word.clear()
          }
        } else {
          word.append(c)
        }
      }

      if (word.nonEmpty) {
        result += word.toString()
      }

      result.toList
    }

    // Trouver la position du mot sous le curseur
    private def findWordPosition(line: String, cursor: Int, words: List[String]): (Int, Int) = {
      var currentPos = 0

      words.zipWithIndex.foreach { case (word, idx) =>
        val wordStart = line.indexOf(word, currentPos)
        val wordEnd = wordStart + word.length

        if (cursor >= wordStart && cursor <= wordEnd) {
          return (idx, cursor - wordStart)
        }

        currentPos = wordEnd
      }

      // Si le curseur est après tous les mots
      (words.length, 0)
    }

    // Implémentation de ParsedLine
    override def word(): String =
      if (wordIdx < parsedWords.length) parsedWords(wordIdx) else ""

    override def wordCursor(): Int = wordCurs

    override def wordIndex(): Int = wordIdx

    override def words(): java.util.List[String] = parsedWords.asJava

    override def line(): String = aline

    override def cursor(): Int = acursor

    // Implémentation de CompletingParsedLine
    override def escape(candidate: CharSequence, complete: Boolean): CharSequence = {
      val str = candidate.toString
      // Échapper si contient des espaces ou quotes
      if (str.contains(" ") || str.contains("'") || str.contains("\"")) {
        s""""${str.replace("\"", "\\\"")}""""
      } else {
        candidate
      }
    }

    override def rawWordCursor(): Int = wordCurs

    override def rawWordLength(): Int = word().length
  }
}
