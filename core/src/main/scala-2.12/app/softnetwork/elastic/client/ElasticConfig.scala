package app.softnetwork.elastic.client

import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import configs.Configs

case class ElasticConfig(
  credentials: ElasticCredentials = ElasticCredentials(),
  multithreaded: Boolean = true,
  discoveryEnabled: Boolean = false
)

object ElasticConfig extends StrictLogging {
  def apply(config: Config): ElasticConfig = {
    Configs[ElasticConfig]
      .get(config.withFallback(ConfigFactory.load("softnetwork-elastic.conf")), "elastic")
      .toEither match {
      case Left(configError) =>
        logger.error(s"Something went wrong with the provided arguments $configError")
        throw configError.configException
      case Right(r) => r
    }
  }
}
