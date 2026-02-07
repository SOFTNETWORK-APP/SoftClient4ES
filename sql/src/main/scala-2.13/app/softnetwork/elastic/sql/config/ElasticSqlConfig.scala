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

package app.softnetwork.elastic.sql.config

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import configs.ConfigReader

case class ElasticSqlConfig(
  compositeKeySeparator: String,
  artificialPrimaryKeyColumnName: String,
  transformLastUpdatedColumnName: String
)

object ElasticSqlConfig extends StrictLogging {
  def apply(config: Config): ElasticSqlConfig = {
    ConfigReader[ElasticSqlConfig]
      .read(config.withFallback(ConfigFactory.load("softnetwork-sql.conf")), "sql")
      .toEither match {
      case Left(configError) =>
        logger.error(s"Something went wrong with the provided arguments $configError")
        throw configError.configException
      case Right(r) => r
    }
  }

  def apply(): ElasticSqlConfig = apply(com.typesafe.config.ConfigFactory.load())
}
