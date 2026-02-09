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

package app.softnetwork.elastic.client.result

import app.softnetwork.elastic.sql.schema.{IngestPipeline, Table}
import fansi.Color

import scala.collection.immutable.ListMap
import scala.concurrent.duration.Duration

object ResultRenderer {

  /** Render a QueryResult with pretty formatting */
  def render(
    result: QueryResult,
    executionTime: Duration,
    format: OutputFormat = OutputFormat.Ascii
  ): String = {
    format match {
      case OutputFormat.Ascii =>
        renderAscii(result, executionTime)

      case OutputFormat.Json =>
        JsonFormatter.format(result, executionTime)

      case OutputFormat.Csv =>
        CsvFormatter.format(result)
    }
  }

  /** Render a QueryResult with ascii formatting */
  def renderAscii(result: QueryResult, executionTime: Duration): String = {
    result match {
      case EmptyResult =>
        renderEmpty()

      case QueryRows(rows) =>
        renderTable(rows, executionTime)

      case QueryStructured(response) =>
        renderTable(response.results, executionTime)

      case DmlResult(inserted, updated, deleted, rejected) =>
        renderDml(inserted, updated, deleted, rejected, executionTime)

      case DdlResult(success) =>
        renderDdl(success, executionTime)

      case TableResult(table) =>
        renderTableDefinition(table)

      case PipelineResult(pipeline) =>
        renderPipelineDefinition(pipeline)

      case SQLResult(sql) =>
        renderSql(sql)

      case QueryStream(_) =>
        renderStreamInfo()

      case StreamResult(estimatedSize, _) =>
        val sizeInfo = estimatedSize.map(s => s" (~$s rows)").getOrElse("")
        s"""${emoji("🌊")} ${cyan(s"Streaming result$sizeInfo")}
           |
           |${bold("Commands:")}
           |  ${yellow(".consume [batch] [max]")} - Fetch results (default batch: 100)
           |  ${yellow(".consume 50 1000")}       - Fetch max 1000 rows in batches of 50
           |  ${yellow(".cancel")}                - Cancel the stream
           |
           |${gray(s"(${executionTime.toMillis}ms to initialize)")}""".stripMargin
    }
  }

  // ==================== Table Rendering ====================

  private def renderTable(rows: Seq[Map[String, Any]], executionTime: Duration): String = {
    if (rows.isEmpty) {
      return s"${emoji("✅")} ${green("No rows returned")} ${gray(s"(${executionTime.toMillis}ms)")}"
    }

    // Extract columns
    val columnNames: Seq[String] = rows.headOption.map(_.keys.toSeq).getOrElse(Seq.empty)

    // Prepare headers with colors
    val headers = columnNames.map(col => bold(cyan(col)))

    // Prepare data with colors
    val dataRows = rows.map { row =>
      columnNames.map(col => formatValue(row.getOrElse(col, null)))
    }

    // Render custom table with ANSI support
    val table = renderCustomTable(headers, dataRows)

    val output = new StringBuilder()
    output.append(table)
    output.append("\n")
    output.append(s"${emoji("📊")} ${green(s"${rows.size} row(s)")} ")
    output.append(gray(s"(${executionTime.toMillis}ms)"))

    output.toString()
  }

  // Custom table renderer with ANSI support
  private def renderCustomTable(headers: Seq[String], rows: Seq[Seq[String]]): String = {
    if (rows.isEmpty) return ""

    val allRows = headers +: rows

    // Calculate column widths (strip ANSI codes)
    val columnWidths = headers.indices.map { colIndex =>
      allRows.map(row => stripAnsi(row(colIndex)).length).max + 2
    }

    def separator = "+" + columnWidths.map("-" * _).mkString("+") + "+"

    def renderRow(row: Seq[String]) = {
      "|" + row
        .zip(columnWidths)
        .map { case (cell, width) =>
          val visible = stripAnsi(cell)
          val padding = width - visible.length - 1
          s" $cell${" " * padding}"
        }
        .mkString("|") + "|"
    }

    val output = new StringBuilder()
    output.append(separator + "\n")
    output.append(renderRow(headers) + "\n")
    output.append(separator + "\n")
    rows.foreach { row =>
      output.append(renderRow(row) + "\n")
    }
    output.append(separator)

    output.toString()
  }

  // Strip ANSI escape codes for width calculation
  private def stripAnsi(s: String): String = {
    s.replaceAll("\u001B\\[[;\\d]*m", "")
  }

  private def formatValue(value: Any): String = {
    value match {
      case null                                   => gray("NULL")
      case s: String                              => s
      case n: Number                              => yellow(n.toString)
      case b: Boolean                             => if (b) green("true") else red("false")
      case d: java.time.temporal.TemporalAccessor => magenta(d.toString)
      case seq: Seq[_]                            => s"[${seq.map(formatValue).mkString(", ")}]"
      case map: Map[_, _] =>
        s"{${map.map { case (k, v) => s"$k: ${formatValue(v)}" }.mkString(", ")}}"
      case other => other.toString
    }
  }

  // ==================== DML Rendering ====================

  private def renderDml(
    inserted: Long,
    updated: Long,
    deleted: Long,
    rejected: Long,
    executionTime: Duration
  ): String = {
    val parts = Seq(
      if (inserted > 0) Some(s"${green(s"$inserted inserted")}") else None,
      if (updated > 0) Some(s"${yellow(s"$updated updated")}") else None,
      if (deleted > 0) Some(s"${red(s"$deleted deleted")}") else None,
      if (rejected > 0) Some(s"${red(s"$rejected rejected")}") else None
    ).flatten

    if (parts.isEmpty) {
      s"${emoji("✅")} ${gray("No changes")} ${gray(s"(${executionTime.toMillis}ms)")}"
    } else {
      s"${emoji("✅")} ${parts.mkString(", ")} ${gray(s"(${executionTime.toMillis}ms)")}"
    }
  }

  // ==================== DDL Rendering ====================

  private def renderDdl(success: Boolean, executionTime: Duration): String = {
    if (success) {
      s"${emoji("✅")} ${green("Success")} ${gray(s"(${executionTime.toMillis}ms)")}"
    } else {
      s"${emoji("❌")} ${red("Failed")} ${gray(s"(${executionTime.toMillis}ms)")}"
    }
  }

  private def renderEmpty(): String = {
    s"${emoji("ℹ️")} ${gray("Empty result")}"
  }

  // ==================== Table Definition Rendering ====================

  private def renderTableDefinition(table: Table): String = {
    val output = new StringBuilder()

    // Table header
    output.append(
      s"${emoji("📋")} ${bold(cyan(s"Table: ${table.name}"))} " +
      gray(s"[${table.tableType}]") +
      "\n\n"
    )

    val rows: List[ListMap[String, Any]] = table.columns.flatMap(_.asMap(table))

    // Extract columns
    val columnNames: Seq[String] = rows.headOption.map(_.keys.toSeq).getOrElse(Seq.empty)

    // Prepare headers and data with colors
    val headers = columnNames.map(col => bold(cyan(col)))
    val dataRows = rows.map { row =>
      columnNames.map(col => formatValue(row.getOrElse(col, null)))
    }

    output.append(renderCustomTable(headers, dataRows))

    // Primary Key and Partition
    val properties = Seq(
      table.primaryKey.nonEmpty -> s"${emoji("🔑")} ${bold("PRIMARY KEY")} ${yellow(table.primaryKey.mkString(", "))}",
      table.partitionBy.isDefined -> s"${emoji("📅")} ${bold("PARTITION BY")} ${yellow(
        table.partitionBy.map(p => s"${p.column} (${p.granularity})").getOrElse("")
      )}"
    ).collect { case (true, line) => line }

    if (properties.nonEmpty) {
      output.append("\n\n")
      output.append(properties.mkString("\n"))
    }

    // Settings
    if (table.settings.nonEmpty) {
      output.append(s"\n\n${emoji("⚙️")} ${bold("Settings:")}\n")
      table.settings.foreach { case (k, v) =>
        output.append(s"  ${cyan(k)}: ${yellow(v.toString)}\n")
      }
    }

    // Mappings
    if (table.mappings.nonEmpty) {
      output.append(s"\n\n${emoji("🗺️")} ${bold("Mappings:")}\n")
      table.mappings.foreach { case (k, v) =>
        output.append(s"  ${cyan(k)}: ${yellow(v.toString)}\n")
      }
    }

    // Aliases
    if (table.aliases.nonEmpty) {
      output.append(s"\n\n${emoji("🔗")} ${bold("Aliases:")}\n")
      table.aliases.foreach { case (k, v) =>
        output.append(s"  ${cyan(k)}: ${yellow(v.toString)}\n")
      }
    }

    // DDL
    if (table.ddl.nonEmpty) {
      output.append(s"\n\n${emoji("📝")} ${bold("DDL:")}\n")
      output.append(highlightSql(table.ddl))
      output.append("\n")
    }

    output.toString()
  }

  // ==================== Pipeline Definition Rendering ====================

  private def renderPipelineDefinition(pipeline: IngestPipeline): String = {
    val output = new StringBuilder()
    output.append(s"${emoji("🔄")} ${bold(cyan(s"Pipeline: ${pipeline.name}"))}\n\n")

    output.append(s"${bold("Processors:")} (${pipeline.processors.size})\n")

    val rows: Seq[ListMap[String, Any]] = pipeline.describe

    // Extract columns
    val columnNames: Seq[String] = rows.headOption.map(_.keys.toSeq).getOrElse(Seq.empty)

    // Prepare headers and data with colors
    val headers = columnNames.map(col => bold(cyan(col)))
    val dataRows = rows.map { row =>
      columnNames.map(col => formatValue(row.getOrElse(col, null)))
    }

    output.append(renderCustomTable(headers, dataRows))

    // DDL
    if (pipeline.sql.nonEmpty) {
      output.append(s"\n\n${emoji("📝")} ${bold("DDL:")}\n")
      output.append(highlightSql(pipeline.ddl))
      output.append("\n")
    }

    output.toString()
  }

  // ==================== SQL Rendering ====================

  private def renderSql(sql: String): String = {
    s"${emoji("📝")} ${bold("Generated SQL:")}\n\n${highlightSql(sql)}"
  }

  private def renderStreamInfo(): String = {
    s"${emoji("🌊")} ${cyan("Streaming result")} ${gray("(use .consume to fetch)")}"
  }

  // ==================== Syntax Highlighting ====================

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
      "GROUP",
      "BY",
      "ORDER",
      "HAVING",
      "LIMIT",
      "OFFSET",
      "ENRICH",
      "POLICY",
      "TYPE",
      "MATCH",
      "ON",
      "WATCHER",
      "TRANSFORM",
      "PIPELINE",
      "PROCESSOR"
    )

    sql
      .split("\\b")
      .map { word =>
        if (keywords.contains(word.toUpperCase())) {
          blue(word)
        } else if (word.matches("'[^']*'")) {
          green(word)
        } else if (word.matches("\\d+")) {
          yellow(word)
        } else {
          word
        }
      }
      .mkString
  }

  // ==================== ANSI Colors ====================

  private def emoji(s: String): String = s

  private def bold(s: String): String = fansi.Bold.On(s).render

  private def red(s: String): String = Color.Red(s).render
  private def green(s: String): String = Color.Green(s).render
  private def yellow(s: String): String = Color.Yellow(s).render
  private def blue(s: String): String = Color.Blue(s).render
  private def magenta(s: String): String = Color.Magenta(s).render
  private def cyan(s: String): String = Color.Cyan(s).render
  private def gray(s: String): String = Color.DarkGray(s).render
}
