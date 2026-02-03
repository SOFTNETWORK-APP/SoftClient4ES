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
import com.github.freva.asciitable.AsciiTable
import fansi.Color

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
        renderAscii(result, executionTime) // Existing

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
    }
  }

  // ==================== Table Rendering ====================

  private def renderTable(rows: Seq[Map[String, Any]], executionTime: Duration): String = {
    if (rows.isEmpty) {
      return s"${emoji("✅")} ${green("No rows returned")} ${gray(s"(${executionTime.toMillis}ms)")}"
    }

    // Extract columns (preserve insertion order)
    val columnNames: Seq[String] = rows.headOption.map(_.keys.toSeq).getOrElse(Seq.empty)

    // Prepare headers with colors
    val headers: Array[String] = columnNames.map(col => bold(cyan(col))).toArray

    // Prepare data using ORIGINAL column names (not colored)
    val dataArray: Array[Array[Object]] = rows.map { row =>
      columnNames.map(col => formatValue(row.getOrElse(col, null)).asInstanceOf[Object]).toArray
    }.toArray

    // Create table
    val table = AsciiTable.getTable(headers, dataArray)

    val output = new StringBuilder()
    output.append(table)
    output.append("\n")
    output.append(s"${emoji("📊")} ${green(s"${rows.size} row(s)")} ")
    output.append(gray(s"(${executionTime.toMillis}ms)"))

    output.toString()
  }

  private def formatValue(value: Any): String = {
    value match {
      case null       => gray("NULL")
      case s: String  => s
      case n: Number  => yellow(n.toString)
      case b: Boolean => if (b) green("true") else red("false")
      case d: java.time.temporal.TemporalAccessor =>
        magenta(d.toString)
      case seq: Seq[_] =>
        s"[${seq.map(formatValue).mkString(", ")}]"
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

    val rows: List[Map[String, Any]] = table.columns.flatMap(_.asMap)

    // Extract columns (preserve insertion order)
    val columnNames: Seq[String] = rows.headOption.map(_.keys.toSeq).getOrElse(Seq.empty)

    // Prepare headers with colors
    val headers: Array[String] = columnNames.map(col => bold(cyan(col))).toArray

    // Prepare data using ORIGINAL column names (not colored)
    val dataArray: Array[Array[Object]] = rows.map { row =>
      columnNames.map(col => formatValue(row.getOrElse(col, null)).asInstanceOf[Object]).toArray
    }.toArray

    output.append(AsciiTable.getTable(headers, dataArray))

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

    output.toString()
  }

  // ==================== Pipeline Definition Rendering ====================

  private def renderPipelineDefinition(pipeline: IngestPipeline): String = {
    val output = new StringBuilder()
    output.append(s"${emoji("🔄")} ${bold(cyan(s"Pipeline: ${pipeline.name}"))}\n\n")

    if (pipeline.describe.nonEmpty) {
      output.append(s"${bold("Description:")} ${pipeline.describe}\n\n")
    }

    output.append(s"${bold("Processors:")} (${pipeline.processors.size})\n")
    pipeline.processors.zipWithIndex.foreach { case (processor, i) =>
      output.append(s"\n  ${i + 1}. ${cyan(processor.processorType.name)}\n")
    // Add processor details if needed
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
