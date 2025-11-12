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

package app.softnetwork.elastic.client.macros

import app.softnetwork.elastic.sql.macros.SQLQueryMacros
import app.softnetwork.elastic.sql.query.SQLQuery
import org.json4s.{DefaultFormats, Formats}

import scala.language.experimental.macros

/** Test trait that uses macros for compile-time validation.
  */
trait TestElasticClientApi {

  /** Search with compile-time SQL validation (macro).
    */
  def searchAs[T](query: String)(implicit m: Manifest[T], formats: Formats): Seq[T] =
    macro SQLQueryMacros.searchAsImpl[T]

  /** Search without compile-time validation (runtime).
    */
  def searchAsUnchecked[T](
    sqlQuery: SQLQuery
  )(implicit m: Manifest[T], formats: Formats): Seq[T] = {
    // Dummy implementation for tests
    Seq.empty[T]
  }
}

object TestElasticClientApi extends TestElasticClientApi {
  // default implicit for the tests
  implicit val defaultFormats: Formats = DefaultFormats
}
