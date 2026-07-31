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

import app.softnetwork.elastic.client.repl.ReplConfig
import com.typesafe.config.{Config, ConfigFactory, ConfigValueFactory}
import com.typesafe.scalalogging.LazyLogging

/** CLI configuration.
  *
  * Connection settings are `Option`s: `Some` ONLY when the user actually passed the corresponding
  * flag. Unset flags are OMITTED from the primary config layer so the standard Typesafe fallback
  * chain can supply them:
  *
  * CLI flag > ELASTIC_* env var > -Dconfig.file (conf/application.conf) > built-in
  * softnetwork-elastic.conf (hard defaults http / localhost / 9200)
  */
case class CliConfig(
  scheme: Option[String] = None,
  host: Option[String] = None,
  port: Option[Int] = None,
  username: Option[String] = None,
  password: Option[String] = None,
  apiKey: Option[String] = None,
  bearerToken: Option[String] = None,
  executeFile: Option[String] = None,
  executeCommand: Option[String] = None,
  replConfig: ReplConfig = ReplConfig.default
) {

  import CliConfig._

  lazy val elasticConfig: Config = buildElasticConfig(sys.env.get)

  /** Test seam. `env` abstracts `sys.env.get` (JVM env vars are immutable in-process); `external`
    * abstracts the -Dconfig.file layer (unit tests inject a parsed Config instead of mutating the
    * JVM-global `config.file` system property - see CliConfigSpec).
    *
    * The `external` default MUST stay the no-arg `ConfigFactory.load()`: it is the only load
    * variant that honours -Dconfig.file, which the shipped launcher passes. Replacing it with
    * `load("softnetwork-elastic.conf")` silently reintroduces issue #162(b) - the unit matrix
    * cannot catch that (it injects `external`); only the published-style manual smoke exercises
    * this default end-to-end.
    */
  private[client] def buildElasticConfig(
    env: String => Option[String],
    external: Config = ConfigFactory.load()
  ): Config = {
    val cliLayer: Config = layerOf(
      Seq(
        scheme.map(SchemePath -> _),
        host.map(HostPath     -> _),
        port.map(p => PortPath -> (Int.box(p): AnyRef)),
        username.map(UsernamePath       -> _),
        password.map(PasswordPath       -> _),
        apiKey.map(ApiKeyPath           -> _),
        bearerToken.map(BearerTokenPath -> _)
      ).flatten
    )
    // Built-in defaults tail: http / localhost / 9200 + metrics/discovery/watcher blocks.
    val builtinLayer: Config = ConfigFactory.load("softnetwork-elastic.conf")
    sanitize(
      cliLayer
        .withFallback(envLayer(env))
        .withFallback(external)
        .withFallback(builtinLayer)
    )
  }
}

object CliConfig extends LazyLogging {

  private[client] val SchemePath = "elastic.credentials.scheme"
  private[client] val HostPath = "elastic.credentials.host"
  private[client] val PortPath = "elastic.credentials.port"
  private[client] val UsernamePath = "elastic.credentials.username"
  private[client] val PasswordPath = "elastic.credentials.password"
  private[client] val ApiKeyPath = "elastic.credentials.api-key"
  private[client] val BearerTokenPath = "elastic.credentials.bearer-token"

  // Watcher block (issue #172): inherits elastic.credentials.* in the builtin conf,
  // overridable per key through the ELASTIC_WATCHER_* env family.
  private[client] val WatcherSchemePath = "elastic.watcher.scheme"
  private[client] val WatcherHostPath = "elastic.watcher.host"
  private[client] val WatcherPortPath = "elastic.watcher.port"
  private[client] val WatcherUsernamePath = "elastic.watcher.username"
  private[client] val WatcherPasswordPath = "elastic.watcher.password"
  private[client] val WatcherApiKeyPath = "elastic.watcher.api-key"
  private[client] val WatcherBearerTokenPath = "elastic.watcher.bearer-token"

  private def layerOf(entries: Seq[(String, AnyRef)]): Config =
    entries.foldLeft(ConfigFactory.empty()) { case (config, (path, value)) =>
      config.withValue(path, ConfigValueFactory.fromAnyRef(value))
    }

  /** Explicit env layer. An env var set to the EMPTY (or whitespace-only) string is treated as
    * UNSET - HOCON `${?VAR}` substitution would consider it present and override every fallback.
    *
    * Value handling: the PRESENCE check trims, but credential VALUES pass through verbatim - a
    * password/token may legitimately contain leading or trailing whitespace, and mutating it breaks
    * authentication with correct credentials. Connection coordinates (scheme/host/port) ARE
    * trimmed: whitespace there is always accidental and would corrupt the URL / crash the port
    * reader.
    *
    * Alias precedence (first match wins):
    *   - host: ELASTIC_IP beats ELASTIC_HOST (parity with softnetwork-elastic.conf, where the later
    *     `host = ${?ELASTIC_IP}` line wins)
    *   - auth settings: installer-documented ELASTIC_<X> beats library-internal
    *     ELASTIC_CREDENTIALS_<X>
    */
  private[client] def envLayer(env: String => Option[String]): Config = {
    def firstNonBlank(names: String*): Option[String] =
      names.flatMap(name => env(name).filter(_.trim.nonEmpty)).headOption
    def firstCoordinate(names: String*): Option[String] =
      firstNonBlank(names: _*).map(_.trim)
    layerOf(
      Seq(
        firstCoordinate("ELASTIC_SCHEME").map(SchemePath           -> _),
        firstCoordinate("ELASTIC_IP", "ELASTIC_HOST").map(HostPath -> _),
        firstCoordinate("ELASTIC_PORT").map(PortPath               -> _),
        firstNonBlank("ELASTIC_USERNAME", "ELASTIC_CREDENTIALS_USERNAME")
          .map(UsernamePath -> _),
        firstNonBlank("ELASTIC_PASSWORD", "ELASTIC_CREDENTIALS_PASSWORD")
          .map(PasswordPath -> _),
        firstNonBlank("ELASTIC_API_KEY", "ELASTIC_CREDENTIALS_API_KEY")
          .map(ApiKeyPath -> _),
        firstNonBlank("ELASTIC_BEARER_TOKEN", "ELASTIC_CREDENTIALS_BEARER_TOKEN")
          .map(BearerTokenPath -> _),
        // ELASTIC_WATCHER_* family (issue #172): same empty-env-is-unset and trimming
        // treatment as the main connection family. ELASTIC_WATCHER_AUTH_METHOD is NOT
        // in this layer - parity with ELASTIC_AUTH_METHOD, which only enters through
        // the builtin conf tail.
        firstCoordinate("ELASTIC_WATCHER_SCHEME").map(WatcherSchemePath          -> _),
        firstCoordinate("ELASTIC_WATCHER_HOST").map(WatcherHostPath              -> _),
        firstCoordinate("ELASTIC_WATCHER_PORT").map(WatcherPortPath              -> _),
        firstNonBlank("ELASTIC_WATCHER_USERNAME").map(WatcherUsernamePath        -> _),
        firstNonBlank("ELASTIC_WATCHER_PASSWORD").map(WatcherPasswordPath        -> _),
        firstNonBlank("ELASTIC_WATCHER_API_KEY").map(WatcherApiKeyPath           -> _),
        firstNonBlank("ELASTIC_WATCHER_BEARER_TOKEN").map(WatcherBearerTokenPath -> _)
      ).flatten
    )
  }

  /** Last-resort guard: a connection-critical setting that resolved EMPTY (e.g. an empty env var
    * leaking through a `${?VAR}` substitution inside a conf file, bypassing the launcher filter)
    * falls back to the hard default instead of producing `http://:9200` or a ConfigReader crash on
    * `port = ""`. `hasPath` is false for HOCON `null`, so `key = null` in a user file is also
    * repaired.
    */
  private[client] def sanitize(resolved: Config): Config = {
    val repaired = repairEmpty(
      resolved,
      Seq[(String, AnyRef)](
        SchemePath -> "http",
        HostPath   -> "localhost",
        PortPath   -> Int.box(9200)
      )
    )
    // Watcher coordinates (issue #172): the watcher targets the main cluster unless
    // explicitly overridden, so an empty/missing watcher coordinate falls back to the
    // (already repaired) main connection coordinate rather than a hard default.
    repairEmpty(
      repaired,
      Seq[(String, AnyRef)](
        WatcherSchemePath -> repaired.getAnyRef(SchemePath),
        WatcherHostPath   -> repaired.getAnyRef(HostPath),
        WatcherPortPath   -> repaired.getAnyRef(PortPath)
      )
    )
  }

  private def repairEmpty(resolved: Config, defaults: Seq[(String, AnyRef)]): Config =
    defaults.foldLeft(resolved) { case (config, (path, default)) =>
      val missingOrEmpty =
        !config.hasPath(path) || config.getString(path).trim.isEmpty
      if (missingOrEmpty) {
        logger.warn(
          s"Connection setting '$path' resolved to an empty value - falling back to default '$default'"
        )
        config.withValue(path, ConfigValueFactory.fromAnyRef(default))
      } else {
        config
      }
    }

  // ==================== Argument Parsing (moved from Cli — Cli extends App and is
  // therefore untestable without triggering its delayedInit main body) ====================

  def parseArgs(args: Array[String]): CliConfig = {
    var scheme: Option[String] = None
    var host: Option[String] = None
    var port: Option[Int] = None
    var username: Option[String] = None
    var password: Option[String] = None
    var apiKey: Option[String] = None
    var bearerToken: Option[String] = None
    var executeFile: Option[String] = None
    var executeCommand: Option[String] = None
    var promptPassword = false

    var i = 0
    while (i < args.length) {
      args(i) match {
        case "-s" | "--scheme" =>
          scheme = Some(args(i + 1))
          i += 2

        case "-h" | "--host" =>
          host = Some(args(i + 1))
          i += 2

        case "-p" | "--port" =>
          port = Some(args(i + 1).toInt)
          i += 2

        case "-u" | "--username" =>
          username = Some(args(i + 1))
          i += 2

        case "-P" | "--password" =>
          password = Some(args(i + 1))
          i += 2

        case "-W" =>
          promptPassword = true
          i += 1

        case "-k" | "--api-key" =>
          apiKey = Some(args(i + 1))
          i += 2

        case "-b" | "--bearer-token" =>
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

    if (promptPassword) {
      val console = System.console()
      if (console == null) {
        System.err.println("Error: -W requires an interactive terminal")
        System.exit(1)
      }
      System.err.print("Enter password: ")
      System.err.flush()
      password = Some(new String(console.readPassword()))
    }

    CliConfig(
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

  private[client] def printUsage(): Unit = {
    println(
      """
        |Elasticsearch SQL CLI
        |
        |Usage:
        |  softclient4es [OPTIONS]
        |
        |Options:
        |  -s, --scheme <scheme>      Connection scheme (http or https, default: http)
        |  -h, --host <host>          Elasticsearch host (default: localhost)
        |  -p, --port <port>          Elasticsearch port (default: 9200)
        |  -u, --username <user>      Username for authentication
        |  -P, --password <pass>      Password for authentication
        |  -W                         Prompt for password interactively (input not echoed)
        |  -k, --api-key <key>        API key for authentication
        |  -b, --bearer-token <token> Bearer token for authentication
        |  -f, --file <path>          Execute SQL from file and exit
        |  -c, --command <sql>        Execute SQL command and exit
        |  --help                     Show this help message
        |
        |Configuration precedence:
        |  CLI flag > ELASTIC_* environment variable > conf/application.conf (-Dconfig.file)
        |  > built-in defaults (http://localhost:9200)
        |
        |Examples:
        |  # Start interactive REPL
        |  softclient4es
        |
        |  # Connect to remote host
        |  softclient4es -h prod-es.example.com -p 9200
        |
        |  # Execute SQL file
        |  softclient4es -f queries.sql
        |
        |  # Execute single command
        |  softclient4es -c "SELECT * FROM users LIMIT 10"
        |
        |Interactive Commands:
        | help (\h)       Display help information
        | quit (\q)       Exit the REPL
        | exit (\q)       Exit the REPL
        | history         Display command history
        | clear           Clear the screen
        | timing          Toggle timing display ON/OFF
        | format          Set or show output format
        | timeout         Set or show query timeout
        |
        |
        |Table Commands:
        | tables (\t)     List all tables
        | \st <table>     Show table details
        | \ct <table>     Show table ddl
        | \dt <table>     Describe table schema
        |
        |Pipeline Commands:
        | pipelines (\p)  List all pipelines
        | \sp <pipeline>  Show pipeline details
        | \cp <pipeline>  Show pipeline ddl
        | \dp <pipeline>  Describe pipeline schema
        |
        |Watcher Commands:
        | watchers (\w)   List all watchers
        | \sw <watcher>   Show watcher status
        |
        |Policy Commands:
        | policies (\pol)   List all enrich policies
        | \sl <policy>    Show enrich policy details
        |
        |Stream Commands:
        | consume (\c)    Consume streaming results from last query
        | stream (\s)     Show stream status
        | cancel (\x)     Cancel active stream
        |""".stripMargin
    )
  }
}
