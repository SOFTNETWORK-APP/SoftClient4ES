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

/** Singleton registry that loads from JSON
  */
object HelpRegistry {

  private lazy val database: HelpDatabase = HelpJsonLoader.loadAll()

  def getHelp(topic: String): Option[HelpEntry] = database.getHelp(topic)

  def search(query: String): Seq[HelpEntry] = database.search(query)

  def allTopics: Seq[String] = database.allTopics

  def byCategory(category: HelpCategory): Seq[HelpEntry] = database.byCategory(category)

  def allByCategory: Map[HelpCategory, Seq[HelpEntry]] = {
    HelpCategory.all.map { cat =>
      cat -> byCategory(cat)
    }.toMap
  }

  // For commands map access (used by existing code)
  def commands: Map[String, HelpEntry] = database.commands ++ database.functions
}
