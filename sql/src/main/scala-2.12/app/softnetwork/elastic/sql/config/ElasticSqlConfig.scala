package app.softnetwork.elastic.sql.config

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import configs.Configs

case class ElasticSqlConfig(
  compositeKeySeparator: String,
  artificialPrimaryKeyColumnName: String,
  transformLastUpdatedColumnName: String
)

object ElasticSqlConfig extends StrictLogging {
  def apply(config: Config): ElasticSqlConfig = {
    Configs[ElasticSqlConfig]
      .get(config.withFallback(ConfigFactory.load("softnetwork-sql.conf")), "sql")
      .toEither match {
      case Left(configError) =>
        logger.error(s"Invalid sql configuration: ${ConfigErrorKeys(configError)}")
        throw configError.configException
      case Right(r) => r
    }
  }

  def apply(): ElasticSqlConfig = apply(com.typesafe.config.ConfigFactory.load())
}
