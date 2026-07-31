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

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Precedence matrix for the REPL/CLI connection settings (issue #162):
  *
  * CLI flag > ELASTIC_* env var > -Dconfig.file > built-in defaults
  *
  * Env vars are stubbed through the `env` seam parameter (JVM env is immutable in-process). The
  * config-file layer is stubbed through the `external` seam parameter - NEVER via the `config.file`
  * system property: that sysprop plus `ConfigFactory.invalidateCaches()` are JVM-global, `Test /
  * parallelExecution := false` only covers the ROOT project, and other suites (licensing) call
  * no-arg `ConfigFactory.load()` concurrently. The production default (`external =
  * ConfigFactory.load()`) is exercised end-to-end by the manual smoke on a published-style install.
  *
  * PRECONDITION: the test JVM must not itself run with ELASTIC_* connection env vars exported - the
  * built-in softnetwork-elastic.conf resolves them for real. Guarded by `assume` below (skips, not
  * fails, on a polluted environment).
  */
class CliConfigSpec extends AnyFlatSpec with Matchers {

  private val noEnv: String => Option[String] = _ => None

  private def envOf(pairs: (String, String)*): String => Option[String] =
    pairs.toMap.get

  /** The `-Dconfig.file` layer, injected as a parsed Config (literals only - no substitutions
    * needed for the matrix).
    */
  private def fileOf(content: String): Config = ConfigFactory.parseString(content)

  private def credentials(config: Config, key: String): String =
    config.getString(s"elastic.credentials.$key")

  private def assumeCleanEnvironment(): Unit =
    Seq(
      "ELASTIC_SCHEME",
      "ELASTIC_HOST",
      "ELASTIC_IP",
      "ELASTIC_PORT",
      "ELASTIC_AUTH_METHOD",
      "ELASTIC_USERNAME",
      "ELASTIC_PASSWORD",
      "ELASTIC_API_KEY",
      "ELASTIC_BEARER_TOKEN",
      "ELASTIC_CREDENTIALS_USERNAME",
      "ELASTIC_CREDENTIALS_PASSWORD",
      "ELASTIC_CREDENTIALS_API_KEY",
      "ELASTIC_CREDENTIALS_BEARER_TOKEN",
      "ELASTIC_WATCHER_SCHEME",
      "ELASTIC_WATCHER_HOST",
      "ELASTIC_WATCHER_PORT",
      "ELASTIC_WATCHER_AUTH_METHOD",
      "ELASTIC_WATCHER_USERNAME",
      "ELASTIC_WATCHER_PASSWORD",
      "ELASTIC_WATCHER_API_KEY",
      "ELASTIC_WATCHER_BEARER_TOKEN"
    ).foreach(name => assume(sys.env.get(name).forall(_.trim.isEmpty)))

  behavior of "CliConfig connection precedence"

  it should "fall back to http/localhost/9200 when nothing is set anywhere (AC 4)" in {
    assumeCleanEnvironment()
    // production path: default external layer (real no-arg ConfigFactory.load())
    val config = CliConfig().buildElasticConfig(noEnv)
    credentials(config, "scheme") shouldBe "http"
    credentials(config, "host") shouldBe "localhost"
    config.getInt("elastic.credentials.port") shouldBe 9200
    credentials(config, "username") shouldBe ""
    credentials(config, "password") shouldBe ""
    credentials(config, "api-key") shouldBe ""
    credentials(config, "bearer-token") shouldBe ""
  }

  it should "let an env var configure the connection when no flag is passed (AC 1)" in {
    assumeCleanEnvironment()
    val config = CliConfig().buildElasticConfig(envOf("ELASTIC_PORT" -> "19200"))
    config.getInt("elastic.credentials.port") shouldBe 19200
    credentials(config, "host") shouldBe "localhost" // untouched settings keep defaults
  }

  it should "let the config-file layer configure the connection (AC 2)" in {
    assumeCleanEnvironment()
    val config = CliConfig().buildElasticConfig(
      noEnv,
      external = fileOf("elastic.credentials.port = 19200")
    )
    config.getInt("elastic.credentials.port") shouldBe 19200
    credentials(config, "host") shouldBe "localhost"
  }

  it should "rank flag over env var over config file over default for port (AC 3)" in {
    assumeCleanEnvironment()
    val file = fileOf("elastic.credentials.port = 29200")
    // file beats default
    CliConfig()
      .buildElasticConfig(noEnv, external = file)
      .getInt("elastic.credentials.port") shouldBe 29200
    // env beats file
    CliConfig()
      .buildElasticConfig(envOf("ELASTIC_PORT" -> "19200"), external = file)
      .getInt("elastic.credentials.port") shouldBe 19200
    // flag beats env and file
    CliConfig(port = Some(1234))
      .buildElasticConfig(envOf("ELASTIC_PORT" -> "19200"), external = file)
      .getInt("elastic.credentials.port") shouldBe 1234
  }

  it should "apply the same precedence to every connection setting (AC 5)" in {
    assumeCleanEnvironment()
    case class Case(
      key: String,
      withFlag: CliConfig,
      envName: String,
      flagValue: String
    )
    val cases = Seq(
      Case("scheme", CliConfig(scheme = Some("https")), "ELASTIC_SCHEME", "https"),
      Case("host", CliConfig(host = Some("flag-host")), "ELASTIC_HOST", "flag-host"),
      Case(
        "username",
        CliConfig(username = Some("flag-user")),
        "ELASTIC_USERNAME",
        "flag-user"
      ),
      Case(
        "password",
        CliConfig(password = Some("flag-pass")),
        "ELASTIC_PASSWORD",
        "flag-pass"
      ),
      Case("api-key", CliConfig(apiKey = Some("flag-key")), "ELASTIC_API_KEY", "flag-key"),
      Case(
        "bearer-token",
        CliConfig(bearerToken = Some("flag-token")),
        "ELASTIC_BEARER_TOKEN",
        "flag-token"
      )
    )
    cases.foreach { c =>
      val file = fileOf(s"""elastic.credentials.${c.key} = "from-file"""")
      // file beats default
      credentials(
        CliConfig().buildElasticConfig(noEnv, external = file),
        c.key
      ) shouldBe "from-file"
      // env beats file
      credentials(
        CliConfig().buildElasticConfig(envOf(c.envName -> "from-env"), external = file),
        c.key
      ) shouldBe "from-env"
      // flag beats env and file
      credentials(
        c.withFlag.buildElasticConfig(envOf(c.envName -> "from-env"), external = file),
        c.key
      ) shouldBe c.flagValue
    }
  }

  it should "treat an env var set to the empty string as unset (AC 8)" in {
    assumeCleanEnvironment()
    // empty env must NOT mask the file value
    credentials(
      CliConfig().buildElasticConfig(
        envOf("ELASTIC_HOST" -> ""),
        external = fileOf("""elastic.credentials.host = "file-host"""")
      ),
      "host"
    ) shouldBe "file-host"
    // empty env with no file: default survives, no crash on port
    val config = CliConfig().buildElasticConfig(
      envOf("ELASTIC_HOST" -> "", "ELASTIC_PORT" -> "", "ELASTIC_SCHEME" -> "  ")
    )
    credentials(config, "host") shouldBe "localhost"
    credentials(config, "scheme") shouldBe "http"
    config.getInt("elastic.credentials.port") shouldBe 9200
  }

  it should "keep credential values verbatim while trimming connection coordinates (AC 12)" in {
    assumeCleanEnvironment()
    val config = CliConfig().buildElasticConfig(
      envOf(
        "ELASTIC_HOST"     -> " remote-host ",
        "ELASTIC_USERNAME" -> "user",
        "ELASTIC_PASSWORD" -> " spacey pass "
      )
    )
    credentials(config, "host") shouldBe "remote-host" // coordinates trimmed
    credentials(config, "password") shouldBe " spacey pass " // credentials verbatim
  }

  it should "repair an empty value leaking from a config file (sanitize guard)" in {
    assumeCleanEnvironment()
    val config = CliConfig().buildElasticConfig(
      noEnv,
      external = fileOf("""
        elastic.credentials.host = ""
        elastic.credentials.scheme = ""
        elastic.credentials.port = ""
      """)
    )
    credentials(config, "host") shouldBe "localhost"
    credentials(config, "scheme") shouldBe "http"
    config.getInt("elastic.credentials.port") shouldBe 9200
  }

  it should "honour env alias precedence (ELASTIC_IP > ELASTIC_HOST, ELASTIC_USERNAME > ELASTIC_CREDENTIALS_USERNAME)" in {
    assumeCleanEnvironment()
    credentials(
      CliConfig().buildElasticConfig(
        envOf("ELASTIC_IP" -> "10.0.0.1", "ELASTIC_HOST" -> "other-host")
      ),
      "host"
    ) shouldBe "10.0.0.1"
    credentials(
      CliConfig().buildElasticConfig(envOf("ELASTIC_CREDENTIALS_USERNAME" -> "legacy")),
      "username"
    ) shouldBe "legacy"
    credentials(
      CliConfig().buildElasticConfig(
        envOf(
          "ELASTIC_USERNAME"             -> "modern",
          "ELASTIC_CREDENTIALS_USERNAME" -> "legacy"
        )
      ),
      "username"
    ) shouldBe "modern"
  }

  behavior of "CliConfig auth method auto-detection (AC 11)"

  it should "auto-detect the auth method from supplied credentials once method has no builtin default" in {
    assumeCleanEnvironment()
    ElasticConfig(
      CliConfig(username = Some("elastic"), password = Some("changeme"))
        .buildElasticConfig(noEnv)
    ).credentials.authMethod shouldBe Some(BasicAuth)
    ElasticConfig(
      CliConfig().buildElasticConfig(
        envOf("ELASTIC_USERNAME" -> "elastic", "ELASTIC_PASSWORD" -> "changeme")
      )
    ).credentials.authMethod shouldBe Some(BasicAuth)
    ElasticConfig(
      CliConfig(apiKey = Some("key")).buildElasticConfig(noEnv)
    ).credentials.authMethod shouldBe Some(ApiKeyAuth)
    ElasticConfig(
      CliConfig(bearerToken = Some("token")).buildElasticConfig(noEnv)
    ).credentials.authMethod shouldBe Some(BearerTokenAuth)
    // no credentials anywhere: no auth, exactly as before the fix
    ElasticConfig(
      CliConfig().buildElasticConfig(noEnv)
    ).credentials.authMethod shouldBe None
    // an explicit method still wins over auto-detection
    ElasticConfig(
      CliConfig(username = Some("elastic"), password = Some("changeme"))
        .buildElasticConfig(
          noEnv,
          external = fileOf("""elastic.credentials.method = "noauth"""")
        )
    ).credentials.authMethod shouldBe Some(NoAuth)
  }

  behavior of "CliConfig watcher block inheritance (issue #172)"

  private def watcher(config: Config, key: String): String =
    config.getString(s"elastic.watcher.$key")

  it should "inherit the resolved credentials values when nothing watcher-specific is set" in {
    assumeCleanEnvironment()
    val config = CliConfig().buildElasticConfig(noEnv)
    watcher(config, "scheme") shouldBe "http"
    watcher(config, "host") shouldBe "localhost"
    config.getInt("elastic.watcher.port") shouldBe 9200
    watcher(config, "username") shouldBe ""
    watcher(config, "password") shouldBe ""
    watcher(config, "api-key") shouldBe ""
    watcher(config, "bearer-token") shouldBe ""
    // method inherits the (absent) credentials.method - it must stay absent so
    // auth auto-detection engages
    config.hasPath("elastic.watcher.method") shouldBe false
    // regression: no key may carry the old literal dotted-path garbage
    Seq("method", "username", "password", "api-key", "bearer-token").foreach { key =>
      if (config.hasPath(s"elastic.watcher.$key")) {
        watcher(config, key) should not startWith "elastic.credentials"
      }
    }
  }

  it should "inherit CUSTOM credentials values through the builtin conf substitutions" in {
    // Exercises the builtin resource's ${elastic.credentials.*} substitutions directly:
    // overriding a credentials key before resolution (what ELASTIC_CREDENTIALS_* env
    // vars do at load time) must propagate into the watcher block.
    assumeCleanEnvironment()
    val resolved = ConfigFactory
      .parseString(
        """
          |elastic.credentials.host = "main-host"
          |elastic.credentials.username = "bob"
          |elastic.credentials.api-key = "secret-key"
        """.stripMargin
      )
      .withFallback(ConfigFactory.parseResources("softnetwork-elastic.conf"))
      .resolve()
    resolved.getString("elastic.watcher.host") shouldBe "main-host"
    resolved.getString("elastic.watcher.username") shouldBe "bob"
    resolved.getString("elastic.watcher.api-key") shouldBe "secret-key"
  }

  it should "let ELASTIC_WATCHER_* env vars override inherited and file values" in {
    assumeCleanEnvironment()
    val file = fileOf("""elastic.watcher.host = "file-watcher-host"""")
    // env beats file
    watcher(
      CliConfig().buildElasticConfig(
        envOf("ELASTIC_WATCHER_HOST" -> "watcher-host"),
        external = file
      ),
      "host"
    ) shouldBe "watcher-host"
    // env beats the inherited credentials value, without touching the main connection
    val config = CliConfig().buildElasticConfig(
      envOf(
        "ELASTIC_WATCHER_HOST"     -> " watcher-host ",
        "ELASTIC_WATCHER_PASSWORD" -> " spacey pass "
      )
    )
    watcher(config, "host") shouldBe "watcher-host" // coordinates trimmed
    watcher(config, "password") shouldBe " spacey pass " // credentials verbatim
    credentials(config, "host") shouldBe "localhost"
  }

  it should "treat an empty ELASTIC_WATCHER_* env var as unset" in {
    assumeCleanEnvironment()
    // empty env must NOT mask the file value
    watcher(
      CliConfig().buildElasticConfig(
        envOf("ELASTIC_WATCHER_HOST" -> ""),
        external = fileOf("""elastic.watcher.host = "file-watcher-host"""")
      ),
      "host"
    ) shouldBe "file-watcher-host"
    // empty env with no file: the inherited value survives
    val config = CliConfig().buildElasticConfig(
      envOf("ELASTIC_WATCHER_HOST" -> "", "ELASTIC_WATCHER_PORT" -> " ")
    )
    watcher(config, "host") shouldBe "localhost"
    config.getInt("elastic.watcher.port") shouldBe 9200
  }

  it should "repair an empty watcher coordinate to the main connection coordinate (sanitize guard)" in {
    assumeCleanEnvironment()
    val config = CliConfig().buildElasticConfig(
      noEnv,
      external = fileOf("""
        elastic.credentials.host = "main-host"
        elastic.watcher.host = ""
        elastic.watcher.port = ""
      """)
    )
    watcher(config, "host") shouldBe "main-host"
    config.getInt("elastic.watcher.port") shouldBe 9200
  }

  it should "auto-detect NO auth on the watcher block when nothing is set (garbage-ApiKeyAuth regression)" in {
    assumeCleanEnvironment()
    // Before the fix, watcher.api-key resolved to the literal string
    // "elastic.credentials.api-key", which auto-detection picked up as ApiKeyAuth.
    val watcherCredentials =
      ElasticConfig(CliConfig().buildElasticConfig(noEnv)).watcher
    watcherCredentials.authMethod shouldBe None
    watcherCredentials.apiKey.getOrElse("") shouldBe ""
    watcherCredentials.bearerToken.getOrElse("") shouldBe ""
    watcherCredentials.username shouldBe ""
    watcherCredentials.password shouldBe ""
  }

  behavior of "CliConfig.parseArgs"

  it should "leave every connection setting unset when no flag is passed (AC 10)" in {
    val parsed = CliConfig.parseArgs(Array.empty)
    parsed.scheme shouldBe None
    parsed.host shouldBe None
    parsed.port shouldBe None
    parsed.username shouldBe None
    parsed.password shouldBe None
    parsed.apiKey shouldBe None
    parsed.bearerToken shouldBe None
    parsed.executeFile shouldBe None
    parsed.executeCommand shouldBe None
  }

  it should "capture exactly the flags that were passed (AC 3, 10)" in {
    val parsed = CliConfig.parseArgs(
      Array("-s", "https", "-h", "example.com", "-p", "19200", "-c", "SHOW TABLES")
    )
    parsed.scheme shouldBe Some("https")
    parsed.host shouldBe Some("example.com")
    parsed.port shouldBe Some(19200)
    parsed.executeCommand shouldBe Some("SHOW TABLES")
    parsed.username shouldBe None

    val auth = CliConfig.parseArgs(
      Array(
        "--username",
        "user",
        "--password",
        "pass",
        "--api-key",
        "key",
        "--bearer-token",
        "token",
        "--file",
        "queries.sql"
      )
    )
    auth.username shouldBe Some("user")
    auth.password shouldBe Some("pass")
    auth.apiKey shouldBe Some("key")
    auth.bearerToken shouldBe Some("token")
    auth.executeFile shouldBe Some("queries.sql")
  }
}
