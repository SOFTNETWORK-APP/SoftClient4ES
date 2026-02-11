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

import app.softnetwork.elastic.client.result.{ElasticResult, ElasticSuccess}

trait LicenseApi extends ElasticClientHelpers {

  /** Get license information.
    *
    * @return
    *   an Option containing the license information if available, None otherwise
    */
  def licenseInfo: ElasticResult[Option[String]] = {
    logger.info(s"Fetching license info...")
    executeLicenseInfo match {
      case success @ ElasticSuccess(Some(licenseInfo)) =>
        logger.info(s"✅ License info fetched successfully.")
        success
      case success @ ElasticSuccess(None) =>
        logger.warn(s"⚠️ No license info found.")
        success
      case failure =>
        logger.error(s"❌ Failed to fetch license info.")
        failure
    }
  }

  /** Enable basic license.
    *
    * @return
    *   true if the basic license was enabled successfully, false otherwise
    */
  def enableBasicLicense(): ElasticResult[Boolean] = {
    licenseInfo match {
      case ElasticSuccess(Some(licenseInfo)) =>
        logger.info(s"License info: \n$licenseInfo")
        if (
          licenseInfo
            .contains("\"type\":\"trial\"") || licenseInfo.contains("\"status\":\"expired\"")
        ) {
          logger.warn("⚠️ Trial or expired license detected, activating Basic license...")
          executeEnableBasicLicense() match {
            case success @ ElasticSuccess(true) =>
              logger.info("✅ Basic license activated successfully")
              success
            case failure =>
              logger.error("❌ Failed to activate Basic license")
              failure
          }
        } else {
          // License already present
          logger.info("✅ Basic license already active")
          ElasticSuccess(true)
        }
      case _ => executeEnableBasicLicense()
    }
  }

  /** Enable trial license.
    *
    * @return
    *   true if the trial license was enabled successfully, false otherwise
    */
  def enableTrialLicense(): ElasticResult[Boolean] = {
    licenseInfo match {
      case ElasticSuccess(Some(licenseInfo)) =>
        logger.info(s"License info: \n$licenseInfo")
        if (
          licenseInfo
            .contains("\"type\":\"basic\"") || licenseInfo.contains("\"status\":\"expired\"")
        ) {
          logger.warn("⚠️ Basic or expired license detected, activating Trial license...")
          executeEnableTrialLicense() match {
            case success @ ElasticSuccess(true) =>
              logger.info("✅ Trial license activated successfully")
              success
            case failure =>
              logger.error("❌ Failed to activate Trial license")
              failure
          }
        } else {
          // License already present
          logger.info("✅ Trial license already active")
          ElasticSuccess(true)
        }
      case _ => executeEnableTrialLicense()
    }
  }

  private[client] def executeLicenseInfo: ElasticResult[Option[String]]

  private[client] def executeEnableBasicLicense(): ElasticResult[Boolean]

  private[client] def executeEnableTrialLicense(): ElasticResult[Boolean]
}
