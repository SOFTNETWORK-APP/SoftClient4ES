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

import java.io.{File, PrintWriter}
import scala.util.Using

/** Generator for Markdown documentation from JSON help files
  */
object MarkdownGenerator {

  /** Generate complete documentation
    */
  def generateAll(outputDir: String): Unit = {
    val dir = new File(outputDir)
    if (!dir.exists()) dir.mkdirs()

    // Generate index
    generateIndex(outputDir)

    // Generate category pages
    generateDdlDocumentation(outputDir)
    generateDmlDocumentation(outputDir)
    generateDqlDocumentation(outputDir)
    generateFunctionsDocumentation(outputDir)
  }

  /** Generate index page
    */
  private def generateIndex(outputDir: String): Unit = {
    val content = new StringBuilder()

    content.append("# SQL Gateway Documentation\n\n")
    content.append("Welcome to the SQL Gateway for Elasticsearch documentation.\n\n")

    content.append("## Quick Links\n\n")
    content.append("- [DDL Statements](ddl_statements.md) - CREATE, ALTER, DROP tables and more\n")
    content.append("- [DML Statements](dml_statements.md) - INSERT, UPDATE, DELETE data\n")
    content.append("- [DQL Statements](dql_statements.md) - SELECT queries\n")
    content.append("- [Functions Reference](functions.md) - All supported functions\n\n")

    content.append("## Command Reference\n\n")

    HelpCategory.all.foreach { category =>
      val entries = HelpRegistry.byCategory(category)
      if (entries.nonEmpty) {
        content.append(s"### ${category.name}\n\n")
        content.append("| Command | Description |\n")
        content.append("|---------|-------------|\n")
        entries.foreach { entry =>
          content.append(
            s"| [${entry.name}](#${anchor(entry.name)}) | ${entry.shortDescription} |\n"
          )
        }
        content.append("\n")
      }
    }

    writeFile(s"$outputDir/README.md", content.toString())
  }

  /** Generate DDL documentation
    */
  private def generateDdlDocumentation(outputDir: String): Unit = {
    val entries = HelpRegistry.byCategory(HelpCategory.DDL)
    val content = generateCategoryDoc("DDL Statements", "Data Definition Language", entries)
    writeFile(s"$outputDir/ddl_statements.md", content)
  }

  /** Generate DML documentation
    */
  private def generateDmlDocumentation(outputDir: String): Unit = {
    val entries = HelpRegistry.byCategory(HelpCategory.DML)
    val content = generateCategoryDoc("DML Statements", "Data Manipulation Language", entries)
    writeFile(s"$outputDir/dml_statements.md", content)
  }

  /** Generate DQL documentation
    */
  private def generateDqlDocumentation(outputDir: String): Unit = {
    val entries = HelpRegistry.byCategory(HelpCategory.DQL)
    val content = generateCategoryDoc("DQL Statements", "Data Query Language", entries)
    writeFile(s"$outputDir/dql_statements.md", content)
  }

  /** Generate functions documentation
    */
  private def generateFunctionsDocumentation(outputDir: String): Unit = {
    val entries = HelpRegistry.byCategory(HelpCategory.Functions)

    val content = new StringBuilder()
    content.append("# Functions Reference\n\n")
    content.append("This document lists all supported SQL functions.\n\n")

    content.append("## Table of Contents\n\n")

    // Group by subcategory
    val bySubcategory = entries
      .collect { case f: FunctionHelp => f }
      .groupBy(_.notes.headOption.map(extractCategory).getOrElse("Other"))

    bySubcategory.keys.toSeq.sorted.foreach { subcat =>
      content.append(s"- [$subcat](#${anchor(subcat)})\n")
    }
    content.append("\n---\n\n")

    // Generate each function
    entries.foreach { entry =>
      content.append(renderEntryMarkdown(entry))
      content.append("\n---\n\n")
    }

    writeFile(s"$outputDir/functions.md", content.toString())
  }

  /** Generate category documentation
    */
  private def generateCategoryDoc(
    title: String,
    description: String,
    entries: Seq[HelpEntry]
  ): String = {
    val content = new StringBuilder()

    content.append(s"# $title\n\n")
    content.append(s"$description statements for SQL Gateway.\n\n")

    content.append("## Table of Contents\n\n")
    entries.foreach { entry =>
      content.append(s"- [${entry.name}](#${anchor(entry.name)})\n")
    }
    content.append("\n---\n\n")

    entries.foreach { entry =>
      content.append(renderEntryMarkdown(entry))
      content.append("\n---\n\n")
    }

    content.toString()
  }

  /** Render a single help entry as Markdown
    */
  private def renderEntryMarkdown(entry: HelpEntry): String = {
    val content = new StringBuilder()

    entry match {
      case cmd: SqlCommandHelp =>
        content.append(s"## ${cmd.name}\n\n")
        content.append(s"${cmd.shortDescription}\n\n")

        // Syntax
        content.append("### Syntax\n\n")
        content.append("```sql\n")
        content.append(cmd.syntax)
        content.append("\n```\n\n")

        // Description
        content.append("### Description\n\n")
        content.append(s"${cmd.description}\n\n")

        // Clauses
        if (cmd.clauses.nonEmpty) {
          content.append("### Clauses\n\n")
          content.append("| Clause | Description | Required |\n")
          content.append("|--------|-------------|----------|\n")
          cmd.clauses.foreach { clause =>
            val required = if (clause.optional) "Optional" else "Required"
            content.append(s"| `${clause.name}` | ${clause.description} | $required |\n")
          }
          content.append("\n")
        }

        // Examples
        if (cmd.examples.nonEmpty) {
          content.append("### Examples\n\n")
          cmd.examples.foreach { ex =>
            content.append(s"**${ex.description}**\n\n")
            content.append("```sql\n")
            content.append(ex.sql)
            content.append("\n```\n\n")
          }
        }

        // Notes
        if (cmd.notes.nonEmpty) {
          content.append("### Notes\n\n")
          cmd.notes.foreach { note =>
            content.append(s"- $note\n")
          }
          content.append("\n")
        }

        // Limitations
        if (cmd.limitations.nonEmpty) {
          content.append("### Limitations\n\n")
          content.append("> ⚠️ **Warning**\n\n")
          cmd.limitations.foreach { limit =>
            content.append(s"- $limit\n")
          }
          content.append("\n")
        }

        // See Also
        if (cmd.seeAlso.nonEmpty) {
          content.append("### See Also\n\n")
          content.append(cmd.seeAlso.map(s => s"[${s}](#${anchor(s)})").mkString(", "))
          content.append("\n\n")
        }

      case fn: FunctionHelp =>
        content.append(s"## ${fn.name}\n\n")
        content.append(s"${fn.shortDescription}\n\n")

        // Syntax
        content.append("### Syntax\n\n")
        content.append("```sql\n")
        content.append(fn.syntax)
        content.append("\n```\n\n")

        // Parameters
        if (fn.parameters.nonEmpty) {
          content.append("### Parameters\n\n")
          content.append("| Parameter | Type | Description | Required |\n")
          content.append("|-----------|------|-------------|----------|\n")
          fn.parameters.foreach { param =>
            val required = if (param.optional) "Optional" else "Required"
            val default = param.defaultValue.map(d => s" (default: $d)").getOrElse("")
            content.append(
              s"| `${param.name}` | ${param.dataType} | ${param.description}$default | $required |\n"
            )
          }
          content.append("\n")
        }

        // Return type
        content.append(s"**Returns:** `${fn.returnType}`\n\n")

        // Description
        content.append("### Description\n\n")
        content.append(s"${fn.description}\n\n")

        // Examples
        if (fn.examples.nonEmpty) {
          content.append("### Examples\n\n")
          fn.examples.foreach { ex =>
            content.append(s"**${ex.description}**\n\n")
            content.append("```sql\n")
            content.append(ex.sql)
            content.append("\n```\n\n")
          }
        }

        // Notes
        if (fn.notes.nonEmpty) {
          content.append("### Notes\n\n")
          fn.notes.foreach { note =>
            content.append(s"- $note\n")
          }
          content.append("\n")
        }

        // See Also
        if (fn.seeAlso.nonEmpty) {
          content.append("### See Also\n\n")
          content.append(fn.seeAlso.map(s => s"[${s}](#${anchor(s)})").mkString(", "))
          content.append("\n\n")
        }
    }

    content.toString()
  }

  /** Generate anchor from title
    */
  private def anchor(title: String): String = {
    title.toLowerCase
      .replaceAll("[^a-z0-9\\s-]", "")
      .replaceAll("\\s+", "-")
  }

  /** Extract category from note (heuristic)
    */
  private def extractCategory(note: String): String = {
    if (note.toLowerCase.contains("aggregate")) "Aggregate"
    else if (note.toLowerCase.contains("string")) "String"
    else if (note.toLowerCase.contains("date") || note.toLowerCase.contains("time")) "Date/Time"
    else if (note.toLowerCase.contains("numeric") || note.toLowerCase.contains("math")) "Numeric"
    else if (note.toLowerCase.contains("geo")) "Geospatial"
    else "Other"
  }

  /** Write content to file
    */
  private def writeFile(path: String, content: String): Unit = {
    Using(new PrintWriter(new File(path), "UTF-8")) { writer =>
      writer.write(content)
    }
  }
}
