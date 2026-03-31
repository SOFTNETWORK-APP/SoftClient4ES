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

import com.typesafe.scalalogging.LazyLogging

class LicenseResolver(
  config: LicenseConfig,
  jwtLicenseManager: JwtLicenseManager,
  apiKeyFetcher: Option[String => Either[LicenseError, String]] = None,
  cacheReader: Option[() => Option[String]] = None
) extends LazyLogging {

  private val gracePeriod: java.time.Duration =
    java.time.Duration.ofMillis(config.gracePeriod.toMillis)

  def resolve(): LicenseKey = {
    // Step 1: Static JWT
    config.key.foreach { jwt =>
      jwtLicenseManager.validateWithGracePeriod(jwt, gracePeriod) match {
        case Right(key) =>
          return key
        case Left(ExpiredLicense(exp)) =>
          logger.warn(s"Static JWT expired at $exp (beyond ${config.gracePeriod} grace period)")
        case Left(err) =>
          logger.error(s"Static JWT invalid: ${err.message}")
      }
    }

    // Step 2: API key fetch
    config.apiKey.foreach { apiKey =>
      apiKeyFetcher.foreach { fetcher =>
        fetcher(apiKey) match {
          case Right(jwt) =>
            jwtLicenseManager.validate(jwt) match {
              case Right(key) =>
                return key
              case Left(err) =>
                logger.error(s"Fetched JWT is invalid: ${err.message}")
            }
          case Left(err) =>
            logger.warn(s"Failed to fetch license: ${err.message}")
        }
      }
    }

    // Step 3: Disk cache
    cacheReader.foreach { reader =>
      reader().foreach { jwt =>
        jwtLicenseManager.validateWithGracePeriod(jwt, gracePeriod) match {
          case Right(key) =>
            logger.warn("Using cached license")
            return key
          case Left(_) => // fall through
        }
      }
    }

    // Step 4: Community default
    if (config.apiKey.isDefined) {
      logger.error("Could not fetch license — falling back to Community mode")
    } else if (config.key.isDefined) {
      logger.error("License invalid or expired — falling back to Community mode")
    } else {
      logger.info("Running in Community mode")
    }
    jwtLicenseManager.resetToCommunity()
    LicenseKey.Community
  }
}
