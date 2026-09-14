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

import app.softnetwork.elastic.client.result.ElasticError
import app.softnetwork.elastic.sql.query.{derivedTablesPresent, Statement}

/** The ONE rejection every venue WITHOUT the relational engine emits for a closure-shaped statement
  * — a cross-index JOIN or a derived table (epic 22 AD-5; #157's discipline, widened).
  *
  * Two readers, one message: `CoreDqlExtension.execute` (the gateway path, where a join-capable
  * extension would have claimed the statement first) and `SearchApi.resolveWithSchema` (the direct
  * client API, which no extension sees). A second message would drift; a second PREDICATE would
  * drift faster, which is why both read
  * `app.softnetwork.elastic.sql.query.relationalClosureRequired`.
  */
object RelationalClosureGuard {

  val ExtensionJar = "softclient4es-arrow-extensions"

  /** Which construct triggered the refusal — NAMED because the remedy differs: a cross-index JOIN
    * can be rewritten by hand, a derived table is usually emitted by a BI tool the user does not
    * control. When BOTH are present the derived table is reported, because it is the one the user
    * did not choose.
    */
  def shapeOf(statement: Statement): String =
    if (derivedTablesPresent(statement)) "A derived table (subquery in FROM/JOIN)"
    else "A cross-index JOIN"

  /** `operation` stays `"join"` on the gateway path for BOTH shapes: nothing downstream
    * distinguishes them (the JDBC driver relays `message` verbatim), so changing it would be a
    * second behaviour change with no consumer.
    *
    * 🔴 The wording is deliberately VENUE-NEUTRAL, which #157's original text was not. Since this
    * story widened `SearchApi.resolveWithSchema` (lead ruling on OQ-1) the same message is returned
    * to a Scala embedder calling `client.search(...)` and — through `gateway.run` — to the JDBC
    * driver's `handleFailure` and the Flight sidecar, verbatim. "Re-run the installer" and a
    * `repl.md` anchor are not actionable there, so the remedy names the JAR and the REPL flag is
    * given as the REPL's spelling of it rather than as the only one.
    */
  def rejection(statement: Statement, operation: String = "join"): ElasticError =
    ElasticError(
      message =
        s"${shapeOf(statement)} requires the relational engine shipped in the $ExtensionJar jar " +
        "(Java 11+); this venue has none, so the statement is refused rather than executed " +
        s"against the first index it names. Put $ExtensionJar on the classpath (at the REPL: " +
        "re-run the installer, or drop --no-extensions). See " +
        "documentation/client/repl.md#extensions-cross-index-joins-materialized-views.",
      statusCode = Some(400),
      operation = Some(operation)
    )
}
