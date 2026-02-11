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

package app.softnetwork.elastic.client.jest

import app.softnetwork.elastic.client.LicenseApi
import app.softnetwork.elastic.client.jest.actions.{ActivateLicense, GetLicense}
import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess
}

trait JestLicenseApi extends LicenseApi with JestClientHelpers {
  _: JestVersionApi with JestClientCompanion =>

  override private[client] def executeLicenseInfo: ElasticResult[Option[String]] = {
    apply().execute(new GetLicense.Builder().build) match {
      case jestResult if jestResult.isSucceeded =>
        val jsonString = jestResult.getJsonString
        ElasticSuccess(Some(jsonString))
      case jestResult =>
        val errorMessage = jestResult.getErrorMessage
        ElasticFailure(
          ElasticError(
            s"Failed to fetch license info: $errorMessage"
          )
        )
    }
  }

  override private[client] def executeEnableBasicLicense(): ElasticResult[Boolean] = {
    apply().execute(
      new ActivateLicense.Builder(license = "basic", acknowledge = true).build
    ) match {
      case jestResult if jestResult.isSucceeded =>
        ElasticSuccess(true)
      case jestResult =>
        val errorMessage = jestResult.getErrorMessage
        ElasticFailure(
          ElasticError(
            s"Failed to enable basic license: $errorMessage"
          )
        )
    }
  }

  override private[client] def executeEnableTrialLicense(): ElasticResult[Boolean] = {
    apply().execute(
      new ActivateLicense.Builder(license = "trial", acknowledge = true).build
    ) match {
      case jestResult if jestResult.isSucceeded =>
        ElasticSuccess(true)
      case jestResult =>
        val errorMessage = jestResult.getErrorMessage
        ElasticFailure(
          ElasticError(
            s"Failed to enable trial license: $errorMessage"
          )
        )
    }
  }

}
