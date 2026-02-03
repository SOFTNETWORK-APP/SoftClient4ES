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

import com.typesafe.config.{Config, ConfigFactory}

case class SqlCliConfig(
  scheme: String,
  host: String,
  port: Int,
  username: Option[String],
  password: Option[String],
  apiKey: Option[String],
  bearerToken: Option[String],
  executeFile: Option[String],
  executeCommand: Option[String],
  replConfig: SqlReplConfig = SqlReplConfig.default
) {
  private lazy val elasticConfigAsString: String =
    s"""
       |elastic {
       |  credentials {
       |    scheme       = "$scheme"
       |    host         = "$host"
       |    port         = $port
       |    username     = ${username.map(u => s""""$u"""").getOrElse("null")}
       |    password     = ${password.map(p => s""""$p"""").getOrElse("null")}
       |    api-key      = ${apiKey.map(k => s""""$k"""").getOrElse("null")}
       |    bearer-token = ${bearerToken.map(t => s""""$t"""").getOrElse("null")}
       |  }
       |}
       |""".stripMargin

  lazy val elasticConfig: Config = ConfigFactory
    .parseString(elasticConfigAsString)
    .withFallback(ConfigFactory.load("softnetwork-elastic.conf"))

}
