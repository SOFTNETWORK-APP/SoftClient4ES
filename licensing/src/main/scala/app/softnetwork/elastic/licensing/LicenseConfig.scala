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

package app.softnetwork.elastic.licensing

import com.typesafe.config.{Config, ConfigFactory}

import scala.concurrent.duration._

case class LicenseConfig(
  key: Option[String],
  apiKey: Option[String],
  apiUrl: String,
  connectTimeout: FiniteDuration,
  readTimeout: FiniteDuration,
  refreshEnabled: Boolean,
  refreshInterval: FiniteDuration,
  telemetryEnabled: Boolean,
  gracePeriod: FiniteDuration,
  cacheDir: String
)

object LicenseConfig {

  private def nonBlank(s: String): Option[String] =
    Option(s).map(_.trim).filter(_.nonEmpty)

  def load(): LicenseConfig = load(ConfigFactory.load())

  def load(config: Config): LicenseConfig = {
    val license = config.getConfig("softclient4es.license")

    val key = nonBlank(license.getString("key"))
    val apiKey = nonBlank(license.getString("api-key"))

    val refreshEnabled = license.getBoolean("refresh.enabled")
    val refreshInterval = license.getDuration("refresh.interval").toMillis.millis

    val telemetryEnabled = license.getBoolean("telemetry.enabled")

    val gracePeriod = license.getDuration("grace-period").toMillis.millis

    val apiUrl = license.getString("api-url")
    val connectTimeout = license.getDuration("connect-timeout").toMillis.millis
    val readTimeout = license.getDuration("read-timeout").toMillis.millis

    val cacheDir = license.getString("cache-dir")

    LicenseConfig(
      key = key,
      apiKey = apiKey,
      apiUrl = apiUrl,
      connectTimeout = connectTimeout,
      readTimeout = readTimeout,
      refreshEnabled = refreshEnabled,
      refreshInterval = refreshInterval,
      telemetryEnabled = telemetryEnabled,
      gracePeriod = gracePeriod,
      cacheDir = cacheDir
    )
  }
}
