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

package app.softnetwork.elastic.client

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.repl.{Repl, StreamingReplExecutor}
import app.softnetwork.elastic.client.spi.ElasticClientFactory

import scala.concurrent.ExecutionContext

object Cli extends App {

  implicit val system: ActorSystem = ActorSystem("softclient4es-sql-cli")
  implicit val ec: ExecutionContext = system.dispatcher

  // Parse command line arguments
  val config = CliConfig.parseArgs(args)

  try {
    val gateway = ElasticClientFactory.createWithMonitoring(config.elasticConfig)

    val executor = new StreamingReplExecutor(gateway)
    val repl = new Repl(executor, config.replConfig)

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
}
