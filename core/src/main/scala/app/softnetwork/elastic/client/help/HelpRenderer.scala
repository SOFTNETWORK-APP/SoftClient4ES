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

object HelpRenderer {

  def render(entry: HelpEntry): String = {
    entry match {
      case cmd: SqlCommandHelp => renderCommand(cmd)
      case fn: FunctionHelp    => renderFunction(fn)
    }
  }

  private def renderCommand(cmd: SqlCommandHelp): String = {
    val sb = new StringBuilder()

    // Header
    sb.append(s"\n${bold(cyan(cmd.name))} ${gray(s"[${cmd.category.name}]")}\n")
    sb.append(s"${cmd.shortDescription}\n\n")

    // Syntax
    sb.append(s"${bold("Syntax:")}\n")
    sb.append(s"${highlightSql(cmd.syntax)}\n\n")

    // Description
    sb.append(s"${bold("Description:")}\n")
    sb.append(s"${cmd.description}\n\n")

    // Clauses
    if (cmd.clauses.nonEmpty) {
      sb.append(s"${bold("Clauses:")}\n")
      cmd.clauses.foreach { clause =>
        val opt = if (clause.optional) gray(" (optional)") else ""
        sb.append(s"  ${yellow(clause.name)}$opt\n")
        sb.append(s"    ${clause.description}\n")
      }
      sb.append("\n")
    }

    // Examples
    if (cmd.examples.nonEmpty) {
      sb.append(s"${bold("Examples:")}\n")
      cmd.examples.foreach { ex =>
        sb.append(s"  ${gray(s"-- ${ex.description}")}\n")
        sb.append(s"  ${highlightSql(ex.sql)}\n\n")
      }
    }

    // Notes
    if (cmd.notes.nonEmpty) {
      sb.append(s"${bold("Notes:")}\n")
      cmd.notes.foreach { note =>
        sb.append(s"  ${emoji("💡")} $note\n")
      }
      sb.append("\n")
    }

    // Limitations
    if (cmd.limitations.nonEmpty) {
      sb.append(s"${bold(red("Limitations:"))}\n")
      cmd.limitations.foreach { limit =>
        sb.append(s"  ${emoji("⚠️")} $limit\n")
      }
      sb.append("\n")
    }

    // See Also
    if (cmd.seeAlso.nonEmpty) {
      sb.append(
        s"${bold("See Also:")} ${cmd.seeAlso.map(s => yellow(s"help $s")).mkString(", ")}\n"
      )
    }

    sb.toString()
  }

  private def renderFunction(fn: FunctionHelp): String = {
    val sb = new StringBuilder()

    // Header
    sb.append(s"\n${bold(green(fn.name))}${gray("()")} ${gray(s"[${fn.category.name}]")}\n")
    sb.append(s"${fn.shortDescription}\n\n")

    // Syntax
    sb.append(s"${bold("Syntax:")}\n")
    sb.append(s"  ${cyan(fn.syntax)}\n\n")

    // Parameters
    if (fn.parameters.nonEmpty) {
      sb.append(s"${bold("Parameters:")}\n")
      fn.parameters.foreach { param =>
        val opt = if (param.optional) gray(" (optional)") else ""
        val default = param.defaultValue.map(d => gray(s" [default: $d]")).getOrElse("")
        sb.append(s"  ${yellow(param.name)} ${gray(param.dataType)}$opt$default\n")
        sb.append(s"    ${param.description}\n")
      }
      sb.append("\n")
    }

    // Return type
    sb.append(s"${bold("Returns:")} ${cyan(fn.returnType)}\n\n")

    // Description
    sb.append(s"${bold("Description:")}\n")
    sb.append(s"${fn.description}\n\n")

    // Examples
    if (fn.examples.nonEmpty) {
      sb.append(s"${bold("Examples:")}\n")
      fn.examples.foreach { ex =>
        sb.append(s"  ${gray(s"-- ${ex.description}")}\n")
        sb.append(s"  ${highlightSql(ex.sql)}\n\n")
      }
    }

    // Notes
    if (fn.notes.nonEmpty) {
      sb.append(s"${bold("Notes:")}\n")
      fn.notes.foreach { note =>
        sb.append(s"  ${emoji("💡")} $note\n")
      }
      sb.append("\n")
    }

    // See Also
    if (fn.seeAlso.nonEmpty) {
      sb.append(
        s"${bold("See Also:")} ${fn.seeAlso.map(s => yellow(s"help $s")).mkString(", ")}\n"
      )
    }

    sb.toString()
  }

  def renderTopicList(category: Option[HelpCategory] = None): String = {
    val sb = new StringBuilder()

    val entries = category match {
      case Some(cat) =>
        sb.append(s"\n${bold(cyan(s"${cat.name} Commands/Functions:"))}\n\n")
        HelpRegistry.byCategory(cat)
      case None =>
        sb.append(s"\n${bold(cyan("Available Help Topics:"))}\n\n")
        HelpRegistry.commands.values.toSeq
    }

    // Group by category
    entries.groupBy(_.category).toSeq.sortBy(_._1.order).foreach { case (cat, catEntries) =>
      sb.append(s"${bold(cat.name)}:\n")
      catEntries.sortBy(_.name).grouped(4).foreach { row =>
        sb.append("  ")
        row.foreach { entry =>
          val colored = entry match {
            case _: SqlCommandHelp => yellow(entry.name.padTo(20, ' '))
            case _: FunctionHelp   => green(entry.name.padTo(20, ' '))
          }
          sb.append(colored)
        }
        sb.append("\n")
      }
      sb.append("\n")
    }

    sb.append(
      s"${gray("Type")} ${yellow("help <topic>")} ${gray("for detailed help on a specific topic.")}\n"
    )
    sb.append(s"${gray("Example:")} ${yellow("help SELECT")}  ${yellow("help CURRENT_DATE")}\n")

    sb.toString()
  }

  def renderSearchResults(query: String, results: Seq[HelpEntry]): String = {
    val sb = new StringBuilder()

    sb.append(s"\n${bold(cyan(s"Search results for '$query':"))}\n\n")

    if (results.isEmpty) {
      sb.append(s"${gray("No results found.")}\n")
      sb.append(s"${gray("Try")} ${yellow("help")} ${gray("to see all available topics.")}\n")
    } else {
      results.foreach { entry =>
        val typeColor = entry match {
          case _: SqlCommandHelp => yellow(entry.name)
          case _: FunctionHelp   => green(entry.name)
        }
        sb.append(s"  $typeColor ${gray(s"[${entry.category.name}]")}\n")
        sb.append(s"    ${entry.shortDescription}\n\n")
      }
    }

    sb.toString()
  }

  // Color helpers
  private def emoji(s: String): String = s
  private def bold(s: String): String = fansi.Bold.On(s).render
  private def red(s: String): String = fansi.Color.Red(s).render
  private def green(s: String): String = fansi.Color.Green(s).render
  private def yellow(s: String): String = fansi.Color.Yellow(s).render
  private def cyan(s: String): String = fansi.Color.Cyan(s).render
  private def gray(s: String): String = fansi.Color.DarkGray(s).render

  private def highlightSql(sql: String): String = {
    val keywords = Set(
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
      "AND",
      "OR",
      "NOT",
      "IN",
      "LIKE",
      "JOIN",
      "LEFT",
      "RIGHT",
      "INNER",
      "OUTER",
      "GROUP",
      "BY",
      "ORDER",
      "HAVING",
      "LIMIT",
      "OFFSET",
      "ENRICH",
      "POLICY",
      "WATCHER",
      "AS",
      "ON",
      "INTO",
      "VALUES",
      "SET",
      "WHEN",
      "THEN",
      "ELSE",
      "END",
      "CASE",
      "DISTINCT",
      "PRIMARY",
      "KEY",
      "PARTITIONED",
      "WITH",
      "DO",
      "EVERY",
      "INTERVAL",
      "DAY",
      "MONTH",
      "YEAR",
      "HOUR",
      "MINUTE",
      "SECOND"
    )

    sql
      .split("(\\b)")
      .map { token =>
        if (keywords.contains(token.toUpperCase)) {
          fansi.Color.Blue(token).render
        } else if (token.matches("'[^']*'")) {
          fansi.Color.Green(token).render
        } else if (token.matches("\\d+")) {
          fansi.Color.Yellow(token).render
        } else {
          token
        }
      }
      .mkString
  }
}
