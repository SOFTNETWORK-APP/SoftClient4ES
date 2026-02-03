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
import app.softnetwork.elastic.client.result.{OutputFormat, ResultRenderer}
import org.jline.reader._
import org.jline.reader.impl.history.DefaultHistory
import org.jline.terminal.{Terminal, TerminalBuilder}

import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, TimeoutException}
import scala.util.{Failure, Success, Try}

class SqlRepl(
  executor: SqlExecutor,
  config: SqlReplConfig = SqlReplConfig.default
)(implicit system: ActorSystem, ec: ExecutionContext) {

  private val terminal: Terminal = TerminalBuilder
    .builder()
    .system(true)
    .build()

  private val reader: LineReader = LineReaderBuilder
    .builder()
    .terminal(terminal)
    .appName("SoftClient4ES SQL Gateway")
    .parser(new SqlParser())
    .history(new DefaultHistory())
    .completer(new SqlCompleter())
    .highlighter(new SqlHighlighter())
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
          // Ctrl+D pressed
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

      // Meta commands
      case cmd if cmd.startsWith(".") =>
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
    // Simple heuristic: statement ends with semicolon
    sql.trim.endsWith(";")
  }

  private def executeStatement(sql: String): Unit = {
    val cleanSql = sql.stripSuffix(";").trim

    if (cleanSql.isEmpty) return

    try {
      val future = executor.execute(cleanSql)
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
    // Split into statements (simple split by semicolon)
    val statements = sql.split(";").map(_.trim).filter(_.nonEmpty)

    statements.foreach { stmt =>
      println(s"\n${cyan("=>")} ${gray(stmt)}")
      executeStatement(stmt)
    }
  }

  // ==================== Meta Commands ====================

  private def handleMetaCommand(cmd: String): Unit = {
    val parts = cmd.split("\\s+", 2)
    val command = parts(0)
    val args = if (parts.length > 1) parts(1) else ""

    command match {
      case ".help" | ".h" =>
        printHelp()

      case ".quit" | ".exit" | ".q" =>
        running = false

      case ".tables" | ".t" =>
        executeStatement("SHOW TABLES")

      case ".describe" | ".desc" | ".d" =>
        if (args.nonEmpty) {
          executeStatement(s"DESCRIBE TABLE $args")
        } else {
          printError("Usage: .describe <table_name>")
        }

      case ".pipelines" | ".p" =>
        executeStatement("SHOW PIPELINES")

      case ".watchers" | ".w" =>
        executeStatement("SHOW WATCHERS")

      case ".policies" | ".pol" =>
        executeStatement("SHOW ENRICH POLICIES")

      case ".history" =>
        printHistory()

      case ".clear" =>
        terminal.puts(org.jline.utils.InfoCmp.Capability.clear_screen)
        terminal.flush()

      case ".timing" =>
        config.showTiming = !config.showTiming
        println(s"Timing: ${if (config.showTiming) "ON" else "OFF"}")

      case ".format" =>
        args.toLowerCase match {
          case "ascii" => config.format = OutputFormat.Ascii
          case "json"  => config.format = OutputFormat.Json
          case "csv"   => config.format = OutputFormat.Csv
          case ""      => println(s"Current format: ${config.format}")
          case other   => printError(s"Unknown format: $other")
        }

      case ".timeout" =>
        Try(args.toInt) match {
          case Success(seconds) if seconds > 0 =>
            config.timeout = seconds.seconds
            println(s"Timeout set to ${seconds}s")
          case _ =>
            println(s"Current timeout: ${config.timeout.toSeconds}s")
        }

      case unknown =>
        printError(s"Unknown command: $unknown (type .help for available commands)")
    }
  }

  // ==================== Help & Banners ====================

  private def printWelcomeBanner(): Unit = {
    println(
      s"""
         |╔═══════════════════════════════════════════════════════════╗
         |║  ${bold(cyan("SoftClient4ES SQL Gateway"))}                                ║
         |║  ${gray("Version 1.0.0")}                                            ║
         |║                                                           ║
         |║  Type ${yellow(".help")} for available commands                        ║
         |║  Type ${yellow(".quit")} to exit                                       ║
         |╚═══════════════════════════════════════════════════════════╝
         |""".stripMargin
    )
  }

  private def printGoodbyeBanner(): Unit = {
    println(s"\n${emoji("👋")} ${cyan("Goodbye!")}\n")
  }

  private def printHelp(): Unit = {
    println(
      s"""
         |${bold(cyan("Meta Commands:"))}
         |  ${yellow(".help")}              Show this help
         |  ${yellow(".quit")}              Exit the REPL
         |  ${yellow(".tables")}            List all tables
         |  ${yellow(".describe <table>")} Show table schema
         |  ${yellow(".pipelines")}         List all pipelines
         |  ${yellow(".watchers")}          List all watchers
         |  ${yellow(".policies")}          List all enrich policies
         |  ${yellow(".history")}           Show command history
         |  ${yellow(".clear")}             Clear screen
         |  ${yellow(".timing")}            Toggle timing display
         |  ${yellow(".format <type>")}    Set output format (table|json|csv)
         |  ${yellow(".timeout <seconds>")} Set query timeout
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
         |  • Statements can span multiple lines
         |  • End statements with ${yellow(";")} to execute
         |  • Press ${yellow("Ctrl+C")} to cancel current statement
         |  • Press ${yellow("Ctrl+D")} or type ${yellow(".quit")} to exit
         |  • Use ${yellow("↑")} and ${yellow("↓")} to navigate history
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
  private def bold(s: String): String = s // Simplified
  private def red(s: String): String = s
  private def green(s: String): String = s
  private def yellow(s: String): String = s
  private def cyan(s: String): String = s
  private def gray(s: String): String = s
}
