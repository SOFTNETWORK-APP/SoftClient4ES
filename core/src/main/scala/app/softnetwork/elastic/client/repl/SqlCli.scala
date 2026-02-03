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
import app.softnetwork.elastic.client.spi.ElasticClientFactory

import scala.concurrent.ExecutionContext

object SqlCli extends App {

  implicit val system: ActorSystem = ActorSystem("softclient4es-sql-cli")
  implicit val ec: ExecutionContext = system.dispatcher

  // Parse command line arguments
  val config = parseArgs(args)

  try {
    val gateway = ElasticClientFactory.createWithMonitoring(config.elasticConfig)

    val executor = new SqlExecutor(gateway)
    val repl = new SqlRepl(executor, config.replConfig)

    // Batch mode or interactive mode
    val exitCode = (config.executeFile, config.executeCommand) match {
      case (Some(file), _) =>
        repl.executeFile(file)

      case (_, Some(command)) =>
        repl.executeCommand(command)

      case (None, None) =>
        repl.start()
        0
    }

    system.terminate()

    System.exit(exitCode)

  } catch {
    case ex: Throwable =>
      System.err.println(s"Failed to start CLI: ${ex.getMessage}")
      ex.printStackTrace()
      System.exit(1)
  } finally {
    // Cleanup
    system.terminate()
  }

  // ==================== Argument Parsing ====================

  private def parseArgs(args: Array[String]): SqlCliConfig = {
    var scheme = "http"
    var host = "localhost"
    var port = 9200
    var username: Option[String] = None
    var password: Option[String] = None
    var apiKey: Option[String] = None
    var bearerToken: Option[String] = None
    var executeFile: Option[String] = None
    var executeCommand: Option[String] = None

    var i = 0
    while (i < args.length) {
      args(i) match {
        case "-s" | "--scheme" =>
          scheme = args(i + 1)
          i += 2

        case "-h" | "--host" =>
          host = args(i + 1)
          i += 2

        case "-p" | "--port" =>
          port = args(i + 1).toInt
          i += 2

        case "-u" | "--username" =>
          username = Some(args(i + 1))
          i += 2

        case "-P" | "--password" =>
          password = Some(args(i + 1))
          i += 2

        case "--api-key" =>
          apiKey = Some(args(i + 1))
          i += 2

        case "--bearer-token" =>
          bearerToken = Some(args(i + 1))
          i += 2

        case "-f" | "--file" =>
          executeFile = Some(args(i + 1))
          i += 2

        case "-c" | "--command" =>
          executeCommand = Some(args(i + 1))
          i += 2

        case "--help" =>
          printUsage()
          System.exit(0)

        case unknown =>
          System.err.println(s"Unknown argument: $unknown")
          printUsage()
          System.exit(1)
      }
    }

    SqlCliConfig(
      scheme,
      host,
      port,
      username,
      password,
      apiKey,
      bearerToken,
      executeFile,
      executeCommand
    )
  }

  private def printUsage(): Unit = {
    println(
      """
        |Elasticsearch SQL CLI
        |
        |Usage:
        |  elasticsearch-sql [OPTIONS]
        |
        |Options:
        |  -h, --host <host>         Elasticsearch host (default: localhost)
        |  -p, --port <port>         Elasticsearch port (default: 9200)
        |  -u, --username <user>     Username for authentication
        |  -P, --password <pass>     Password for authentication
        |  -f, --file <path>         Execute SQL from file and exit
        |  -c, --command <sql>       Execute SQL command and exit
        |  --help                    Show this help message
        |
        |Examples:
        |  # Start interactive REPL
        |  elasticsearch-sql
        |
        |  # Connect to remote host
        |  elasticsearch-sql -h prod-es.example.com -p 9200
        |
        |  # Execute SQL file
        |  elasticsearch-sql -f queries.sql
        |
        |  # Execute single command
        |  elasticsearch-sql -c "SELECT * FROM users LIMIT 10"
        |
        |Interactive Commands:
        |  .help                     Show available commands
        |  .tables                   List all tables
        |  .describe <table>         Show table schema
        |  .quit                     Exit
        |""".stripMargin
    )
  }
}
