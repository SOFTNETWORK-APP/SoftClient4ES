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
import app.softnetwork.elastic.client.result.ElasticResult
import io.searchbox.client.JestResult

/** All three operations route through [[JestClientHelpers.executeJestAction]] like the rest of the
  * module. They used to call `apply().execute(...)` directly, so a network fault threw
  * (`IOException` / `CouldNotConnectException`) instead of returning an `ElasticFailure` —
  * `LicenseApi` documents no exception contract and consumers reasonably treat `ElasticResult` as
  * total (SoftClient4ES#204). The wrapper also carries the HTTP status through and names the
  * failing operation, which the hand-rolled message prefix could not.
  */
trait JestLicenseApi extends LicenseApi with JestClientHelpers {
  _: JestVersionApi with JestClientCompanion =>

  override private[client] def executeLicenseInfo: ElasticResult[Option[String]] =
    executeJestAction[JestResult, Option[String]](
      operation = "licenseInfo",
      retryable = true
    )(
      new GetLicense.Builder().build
    )(
      // `Option`, not `Some`: a succeeded JestResult can carry a null body, and callers — core's
      // `enableBasicLicense` chains straight on to inspect the string — would NPE on `Some(null)`.
      jestResult => Option(jestResult.getJsonString)
    )

  override private[client] def executeEnableBasicLicense(): ElasticResult[Boolean] =
    executeActivateLicense("basic")

  override private[client] def executeEnableTrialLicense(): ElasticResult[Boolean] =
    executeActivateLicense("trial")

  /** Activating a licence is not idempotent — `retryable = false` so a transient fault is reported
    * rather than replayed.
    *
    * The boolean is read from `<license>_was_started` in the body rather than from the HTTP status
    * (SoftClient4ES#216). `executeJestBooleanAction` maps `_.isSucceeded`, but the transformer only
    * runs when the result already succeeded, so it could return nothing but a constant `true`.
    * Measured against Elasticsearch 8.18.3: a *state* refusal ("Current license is basic", "Trial
    * was already activated") is an HTTP **403** and is therefore already a failure, but a refusal
    * for want of acknowledgement is an HTTP **200** carrying `"basic_was_started": false` — and
    * that is the response listing what the downgrade would disable, Watcher included. This client
    * always sends `acknowledge=true`, so that case is not reachable today; reading the field costs
    * nothing and stops it being one dropped parameter away from reporting a licence it never got.
    */
  private[this] def executeActivateLicense(license: String): ElasticResult[Boolean] =
    executeJestAction[JestResult, Boolean](
      operation = s"enable${license.capitalize}License",
      retryable = false
    )(
      new ActivateLicense.Builder(license = license, acknowledge = true).build
    )(jestResult =>
      Option(jestResult.getJsonObject)
        .flatMap(json => Option(json.get(s"${license}_was_started")))
        .exists(_.getAsBoolean)
    )

}
