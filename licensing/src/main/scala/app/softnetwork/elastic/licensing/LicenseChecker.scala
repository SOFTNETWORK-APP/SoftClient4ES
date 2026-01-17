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

import scala.concurrent.Future

/** Generic abstraction for license checking.
  *
  * Type parameters:
  *   - REQUEST: The request type to check (e.g., SQL statement, query)
  *   - RESULT: The result type to return (e.g., query result, DDL result)
  */
trait LicenseChecker[REQUEST, RESULT] {

  /** Check license for a request.
    *
    * @param request
    *   The request to check
    * @param execute
    *   The actual execution function (if license allows)
    * @return
    *   Either a license error or the execution result
    */
  def check(
    request: REQUEST,
    execute: REQUEST => Future[RESULT]
  ): Future[Either[LicenseError, RESULT]]
}

/** Helper for creating checkers
  */
object LicenseChecker {

  /** No-op checker (always allows)
    */
  def noOp[REQUEST, RESULT]: LicenseChecker[REQUEST, RESULT] =
    new LicenseChecker[REQUEST, RESULT] {
      override def check(
        request: REQUEST,
        execute: REQUEST => Future[RESULT]
      ): Future[Either[LicenseError, RESULT]] = {
        import scala.concurrent.ExecutionContext.Implicits.global
        execute(request).map(Right(_))
      }
    }
}
