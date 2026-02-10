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

package app.softnetwork.elastic.client.help

/** In-memory database of help entries
  */
case class HelpDatabase(
  commands: Map[String, SqlCommandHelp],
  functions: Map[String, FunctionHelp],
  aliases: Map[String, String]
) {

  def getHelp(topic: String): Option[HelpEntry] = {
    val normalized = topic.toUpperCase.trim
    commands
      .get(normalized)
      .orElse(functions.get(normalized))
      .orElse(aliases.get(normalized).flatMap(t => commands.get(t).orElse(functions.get(t))))
  }

  def search(query: String): Seq[HelpEntry] = {
    val q = query.toLowerCase
    val cmdResults = commands.values.filter { cmd =>
      cmd.name.toLowerCase.contains(q) ||
      cmd.shortDescription.toLowerCase.contains(q) ||
      cmd.description.toLowerCase.contains(q)
    }
    val fnResults = functions.values.filter { fn =>
      fn.name.toLowerCase.contains(q) ||
      fn.shortDescription.toLowerCase.contains(q) ||
      fn.description.toLowerCase.contains(q)
    }
    (cmdResults ++ fnResults).toSeq.sortBy(_.name)
  }

  def allTopics: Seq[String] = {
    (commands.keys ++ functions.keys ++ aliases.keys).toSeq.sorted
  }

  def byCategory(category: HelpCategory): Seq[HelpEntry] = {
    val cmdResults = commands.values.filter(_.category == category)
    val fnResults = if (category == HelpCategory.Functions) functions.values else Seq.empty
    (cmdResults ++ fnResults).toSeq.sortBy(_.name)
  }
}
