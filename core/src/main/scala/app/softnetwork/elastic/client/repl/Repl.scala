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

import akka.actor.ActorSystem
import app.softnetwork.elastic.SoftClient4esCoreBuildInfo
import app.softnetwork.elastic.client.help.{HelpCategory, HelpRegistry, HelpRenderer}
import app.softnetwork.elastic.client.result.{OutputFormat, QueryRows, ResultRenderer}
import org.jline.reader._
import org.jline.reader.impl.history.DefaultHistory
import org.jline.terminal.{Terminal, TerminalBuilder}

import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, TimeoutException}
import scala.util.{Failure, Success, Try}

class Repl(
  executor: StreamingReplExecutor,
  config: ReplConfig = ReplConfig.default
)(implicit system: ActorSystem, ec: ExecutionContext) {

  private lazy val version: String = SoftClient4esCoreBuildInfo.version

  private val terminal: Terminal = TerminalBuilder
    .builder()
    .system(true)
    .jansi(true) // Force l'utilisation de Jansi
    .jna(true) // Force l'utilisation de JNA
    .build()

  private val completer = new ReplCompleter()

  private val reader: LineReader = LineReaderBuilder
    .builder()
    .terminal(terminal)
    .appName("SoftClient4ES CLI")
    .parser(new ReplParser())
    .history(new DefaultHistory())
    .completer(completer)
    .option(LineReader.Option.CASE_INSENSITIVE, true)
    .option(LineReader.Option.AUTO_LIST, true) // Liste automatique
    .option(LineReader.Option.AUTO_MENU, true) // Menu automatique
    .option(LineReader.Option.LIST_AMBIGUOUS, true) // Liste si ambigu
    .highlighter(new ReplHighlighter())
    .build()

  private var running = true
  private var multilineBuffer = new StringBuilder()

  def start(): Unit = {
    printWelcomeBanner()

    while (running) {
      try {
        val prompt = if (multilineBuffer.isEmpty) {
          config.primaryPrompt
        } else {
          config.continuationPrompt
        }

        val line = reader.readLine(prompt)

        if (line == null) {
          running = false
        } else {
          handleLine(line.trim)
        }

      } catch {
        case _: UserInterruptException =>
          // Ctrl+C pressed
          multilineBuffer.clear()
          println("^C")

        case _: EndOfFileException =>
          // Ctrl+D pressed
          running = false

        case ex: Throwable =>
          printError(s"Unexpected error: ${ex.getMessage}")
      }
    }

    printGoodbyeBanner()
    terminal.close()
  }

  private def handleLine(line: String): Unit = {
    line match {
      // Empty line
      case "" if multilineBuffer.isEmpty =>
      // Do nothing

      // Meta commands (execute immediately, clear buffer first)
      case cmd if metaCommands.exists(meta => cmd.startsWith(meta)) || cmd.startsWith("\\") =>
        multilineBuffer.clear()
        handleMetaCommand(cmd)

      // SQL statement (complete or partial)
      case sql =>
        multilineBuffer.append(sql).append(" ")

        if (isCompleteStatement(multilineBuffer.toString())) {
          val fullSql = multilineBuffer.toString().trim
          multilineBuffer.clear()
          executeStatement(fullSql)
        }
    }
  }

  private def isCompleteStatement(sql: String): Boolean = {
    sql.trim.endsWith(";")
  }

  // Execute SQL from multiline buffer (semicolon already removed)
  private def executeStatement(sql: String): Unit = {
    val cleanSql = sql.stripSuffix(";").trim
    if (cleanSql.isEmpty) return
    executeStatementDirect(cleanSql)
  }

  // Execute SQL directly (for meta commands)
  private def executeStatementDirect(sql: String): Unit = {
    try {
      val future = executor.execute(sql)
      val result = Await.result(future, config.timeout)

      result match {
        case ExecutionSuccess(queryResult, execTime) =>
          val output = ResultRenderer.render(queryResult, execTime, config.format)
          println(output)

        case ExecutionFailure(error, execTime) =>
          printError(s"Error: $error (${execTime.toMillis}ms)")
      }

    } catch {
      case ex: TimeoutException =>
        printError(s"Query timeout after ${config.timeout.toSeconds}s")

      case ex: Throwable =>
        printError(s"Execution failed: ${ex.getMessage}")
    }
  }

  /** Execute SQL from file (batch mode) */
  def executeFile(filePath: String): Int = {
    Try {
      val source = scala.io.Source.fromFile(filePath)
      val content = source.mkString
      source.close()
      content
    } match {
      case Success(sql) =>
        executeBatch(sql)
        0 // Success

      case Failure(ex) =>
        printError(s"Failed to read file: ${ex.getMessage}")
        1 // Error
    }
  }

  /** Execute single SQL command (batch mode) */
  def executeCommand(sql: String): Int = {
    executeBatch(sql)
    0
  }

  private def executeBatch(sql: String): Unit = {
    val statements = sql.split(";").map(_.trim).filter(_.nonEmpty)

    statements.foreach { stmt =>
      println(s"\n${cyan("=>")} ${gray(stmt)}")
      executeStatementDirect(stmt)
    }
  }

  // ==================== Meta Commands ====================

  private lazy val metaCommands: Set[String] = Set(
    "help", "quit", "exit",
    "tables", "pipelines", "watchers", "policies",
    "history", "clear", "timing", "format", "timeout",
    // Stream commands
    "consume", "stream", "cancel"
  )

  private def handleMetaCommand(cmd: String): Unit = {
    val parts = cmd.split("\\s+", 2)
    val command = parts(0)
    val args = if (parts.length > 1) parts(1) else ""

    command match {
      case "help" | "\\h" =>
        handleHelp(args)

      case "quit" | "exit" | "\\q" =>
        running = false

      case "tables" | "\\t" =>
        executeStatementDirect("SHOW TABLES")

      case "\\st" =>
        if (args.nonEmpty) {
          executeStatementDirect(s"SHOW TABLE $args")
        } else {
          printError("Usage: \\st <table_name>")
        }

      case "\\ct" =>
        if (args.nonEmpty) {
          executeStatementDirect(s"SHOW CREATE TABLE $args")
        } else {
          printError("Usage: \\ct <table_name>")
        }

      case "\\dt" =>
        if (args.nonEmpty) {
          executeStatementDirect(s"DESCRIBE TABLE $args")
        } else {
          printError("Usage: describe <table_name>")
        }

      case "pipelines" | "\\p" =>
        executeStatementDirect("SHOW PIPELINES")

      case "\\sp" =>
        if (args.nonEmpty) {
          executeStatementDirect(s"SHOW PIPELINE $args")
        } else {
          printError("Usage: \\sp <pipeline_name>")
        }

      case "\\cp" =>
        if (args.nonEmpty) {
          executeStatementDirect(s"SHOW CREATE PIPELINE $args")
        } else {
          printError("Usage: \\cp <pipeline_name>")
        }

      case "\\dp" =>
        if (args.nonEmpty) {
          executeStatementDirect(s"DESCRIBE PIPELINE $args")
        } else {
          printError("Usage: \\dp <pipeline_name>")
        }

      case "watchers" | "\\w" =>
        executeStatementDirect("SHOW WATCHERS")

      case "\\sw" =>
        if (args.nonEmpty) {
          executeStatementDirect(s"SHOW WATCHER STATUS $args")
        } else {
          printError("Usage: \\sw <watcher_name>")
        }

      case "policies" | "\\pol" =>
        executeStatementDirect("SHOW ENRICH POLICIES")

      case "\\spol" =>
        if (args.nonEmpty) {
          executeStatementDirect(s"SHOW ENRICH POLICY $args")
        } else {
          printError("Usage: \\spol <policy_name>")
        }

      case "history" =>
        printHistory()

      case "clear" =>
        terminal.puts(org.jline.utils.InfoCmp.Capability.clear_screen)
        terminal.flush()

      case "timing" =>
        config.showTiming = !config.showTiming
        println(s"Timing: ${if (config.showTiming) "ON" else "OFF"}")

      case "format" =>
        args.toLowerCase match {
          case "ascii" => config.format = OutputFormat.Ascii
          case "json"  => config.format = OutputFormat.Json
          case "csv"   => config.format = OutputFormat.Csv
          case ""      => println(s"Current format: ${config.format}")
          case other   => printError(s"Unknown format: $other")
        }

      case "timeout" =>
        Try(args.toInt) match {
          case Success(seconds) if seconds > 0 =>
            config.timeout = seconds.seconds
            println(s"Timeout set to ${seconds}s")
          case _ =>
            println(s"Current timeout: ${config.timeout.toSeconds}s")
        }

      // ==================== Stream Commands ====================

      case "consume" | "\\c" =>
        handleConsumeStream(args)

      case "stream" | "\\s" =>
        handleStreamStatus()

      case "cancel" | "\\x" =>
        handleCancelStream()

      case unknown =>
        printError(s"Unknown command: $unknown (type help for available commands)")
    }
  }

  private def handleHelp(args: String): Unit = {
    if (args.isEmpty) {
      // Show general help with topic list
      println(HelpRenderer.renderTopicList())
    } else if (args.startsWith("search ") || args.startsWith("find ")) {
      // Search mode
      val query = args.stripPrefix("search ").stripPrefix("find ").trim
      val results = HelpRegistry.search(query)
      println(HelpRenderer.renderSearchResults(query, results))
    } else if (args.equalsIgnoreCase("ddl")) {
      println(HelpRenderer.renderTopicList(Some(HelpCategory.DDL)))
    } else if (args.equalsIgnoreCase("dml")) {
      println(HelpRenderer.renderTopicList(Some(HelpCategory.DML)))
    } else if (args.equalsIgnoreCase("dql")) {
      println(HelpRenderer.renderTopicList(Some(HelpCategory.DQL)))
    } else if (args.equalsIgnoreCase("functions")) {
      println(HelpRenderer.renderTopicList(Some(HelpCategory.Functions)))
    } else {
      // Specific topic help
      HelpRegistry.getHelp(args) match {
        case Some(entry) =>
          println(HelpRenderer.render(entry))
        case None =>
          // Try search as fallback
          val results = HelpRegistry.search(args)
          if (results.nonEmpty) {
            println(s"${yellow("Topic not found.")} Did you mean one of these?\n")
            println(HelpRenderer.renderSearchResults(args, results.take(5)))
          } else {
            printError(s"No help available for '$args'")
            println(s"Type ${yellow(".help")} to see available topics.")
          }
      }
    }
  }

  // Update completer for help topics
  private val helpTopics: Set[String] = HelpRegistry.allTopics.toSet ++
    Set("ddl", "dml", "dql", "functions", "search")

  private def handleConsumeStream(args: String): Unit = {
    if (!executor.hasActiveStream) {
      printError("No active stream. Execute a streaming query first.")
      return
    }

    // Parse arguments: .consume [batch_size] [max_rows]
    val argParts = args.split("\\s+").filter(_.nonEmpty)
    val batchSize = argParts.headOption.flatMap(s => Try(s.toInt).toOption).getOrElse(100)
    val maxRows = argParts.lift(1).flatMap(s => Try(s.toInt).toOption)

    println(
      s"${emoji("🌊")} ${cyan("Consuming stream...")} (batch size: $batchSize${maxRows.map(m => s", max: $m").getOrElse("")})"
    )

    var displayedRows = 0
    val allRows = scala.collection.mutable.ListBuffer[ListMap[String, Any]]()

    val resultFuture = executor.consumeStream(
      batchSize = batchSize,
      maxRows = maxRows,
      onBatch = { batch =>
        // Accumulate rows for display
        allRows ++= batch
        displayedRows += batch.size

        // Show progress
        print(s"\r${emoji("📊")} Received $displayedRows rows...")
      }
    )

    try {
      val result = Await.result(resultFuture, config.timeout)
      println() // New line after progress

      result.error match {
        case Some(err) =>
          printError(s"Stream error: $err")
        case None =>
          // Render accumulated rows
          if (allRows.nonEmpty) {
            val output = ResultRenderer.render(
              QueryRows(allRows.toSeq),
              result.duration,
              config.format
            )
            println(output)
          }
          println(
            s"${emoji("✅")} ${green(s"Stream completed:")} ${result.totalRows} rows in ${result.batches} batches ${gray(
              s"(${result.duration.toMillis}ms)"
            )}"
          )
      }
    } catch {
      case _: TimeoutException =>
        executor.cancelStream()
        printError(s"Stream timeout after ${config.timeout.toSeconds}s")
    }
  }

  private def handleStreamStatus(): Unit = {
    if (executor.hasActiveStream) {
      println(s"${emoji("🌊")} ${cyan("Active stream available")}")
      println(s"  Use ${yellow(".consume [batch_size] [max_rows]")} to fetch results")
      println(s"  Use ${yellow(".cancel")} to cancel the stream")
    } else {
      println(s"${emoji("ℹ️")} ${gray("No active stream")}")
    }
  }

  private def handleCancelStream(): Unit = {
    if (executor.hasActiveStream) {
      executor.cancelStream()
      println(s"${emoji("✅")} ${green("Stream cancelled")}")
    } else {
      println(s"${emoji("ℹ️")} ${gray("No active stream to cancel")}")
    }
  }

  // ==================== Help & Banners ====================

  private def printWelcomeBanner(): Unit = {
    val name = s"║  ${formatLigne(bold(cyan("SoftClient4ES CLI")), 56)} ║"
    val ver = s"║  ${formatLigne(gray(s"Version $version"), 56)} ║"
    val help = s"║  ${formatLigne(s"Type ${yellow(".help")} for available commands", 56)} ║"
    val quit = s"║  ${formatLigne(s"Type ${yellow(".quit")} to exit", 56)} ║"
    println(
      s"""
         |╔═══════════════════════════════════════════════════════════╗
         |$name
         |$ver
         |║                                                           ║
         |$help
         |$quit
         |╚═══════════════════════════════════════════════════════════╝
         |""".stripMargin
    )
  }

  private def formatLigne(value: String, size: Int): String = {
    // Supprime les espaces en début et fin de ligne
    val trimmedValue = value.trim

    // Vérifie la longueur du texte
    if (trimmedValue.length > size) {
      // Si le texte est trop long, le tronquer avec '...'
      trimmedValue.take(size - 3) + "..."
    } else {
      // Sinon, on complète avec des espaces
      trimmedValue + " " * (size - trimmedValue.length)
    }
  }

  private def printGoodbyeBanner(): Unit = {
    println(s"\n${emoji("👋")} ${cyan("Goodbye!")}\n")
  }

  private def printHelp(): Unit = {
    println(
      s"""
         |${bold(cyan("Meta Commands:"))}
         |  ${yellow(".help")}              Show this help
         |  ${yellow(".help <topic>")}      Show help for SQL command or function
         |  ${yellow(".quit")}              Exit the REPL
         |  ${yellow(".tables")}            List all tables
         |  ${yellow(".describe <table>")}  Show table schema
         |  ${yellow(".pipelines")}         List all pipelines
         |  ${yellow(".watchers")}          List all watchers
         |  ${yellow(".policies")}          List all enrich policies
         |  ${yellow(".history")}           Show command history
         |  ${yellow(".clear")}             Clear screen
         |  ${yellow(".timing")}            Toggle timing display
         |  ${yellow(".format <type>")}     Set output format (ascii|json|csv)
         |  ${yellow(".timeout <seconds>")} Set query timeout
         |
         |${bold(cyan("Stream Commands:"))}
         |  ${yellow(".stream")}            Show stream status
         |  ${yellow(".consume [n] [max]")} Consume stream (batch size n, max rows)
         |  ${yellow(".cancel")}            Cancel active stream
         |
         |${bold(cyan("SQL Commands:"))}
         |  ${green("SELECT")} * FROM table WHERE ...
         |  ${green("INSERT")} INTO table VALUES ...
         |  ${green("UPDATE")} table SET ... WHERE ...
         |  ${green("DELETE")} FROM table WHERE ...
         |  ${green("CREATE TABLE")} table (...)
         |  ${green("CREATE ENRICH POLICY")} ...
         |  ${green("CREATE WATCHER")} ...
         |  ${green("SHOW")} TABLES | PIPELINES | WATCHERS
         |
         |${bold(cyan("Tips:"))}
         |  • Type ${yellow(".help SELECT")} for SELECT syntax help
         |  • Type ${yellow(".help CURRENT_DATE")} for function help
         |  • Streaming queries return a stream handle
         |  • Use ${yellow(".consume")} to fetch stream results
         |  • Press ${yellow("Ctrl+C")} to cancel current operation
         |""".stripMargin
    )
  }

  private def printHistory(): Unit = {
    val history = reader.getHistory
    if (history.size() == 0) {
      println(gray("No history"))
    } else {
      println(bold(cyan("Command History:")))
      history.forEach { entry =>
        println(s"  ${gray(entry.index().toString.padTo(4, ' '))} ${entry.line()}")
      }
    }
  }

  private def printError(message: String): Unit = {
    println(s"${emoji("❌")} ${red(message)}")
  }

  // ==================== Helpers ====================

  private def emoji(s: String): String = s
  private def bold(s: String): String = s
  private def red(s: String): String = s
  private def green(s: String): String = s
  private def yellow(s: String): String = s
  private def cyan(s: String): String = s
  private def gray(s: String): String = s
}
