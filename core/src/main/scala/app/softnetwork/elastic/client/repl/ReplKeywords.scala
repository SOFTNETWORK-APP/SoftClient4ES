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

import app.softnetwork.elastic.sql.SQLKeywords

/** The single keyword set both REPL components (highlighter + completer) consume (#161).
  *
  * `sqlWords` is the parser-derived truth (SQLKeywords registry, sql module). `extraWords` are
  * REPL-only entries the parser does NOT accept today (legacy/roadmap completer entries kept for
  * continuity - PD-2). A word the parser actually accepts must live in SQLKeywords, never here
  * (guarded by ReplKeywordsSpec).
  */
object ReplKeywords {

  /** Parser-backed keywords (single uppercase words). */
  val sqlWords: Set[String] = SQLKeywords.highlightedWords

  /** REPL-only entries — NOT parser keywords at this baseline:
    * INTERSECT/EXPLAIN/BULK/CONDITION/ACTION/TRANSFORM were advertised by the pre-#161
    * completer/highlighter; GEO is the compound trigger for "GEO MATCH".
    */
  val extraWords: Set[String] =
    Set("ACTION", "BULK", "CONDITION", "EXPLAIN", "GEO", "INTERSECT", "TRANSFORM")

  /** Everything the REPL highlights and completes. */
  val all: Set[String] = sqlWords ++ extraWords
}
