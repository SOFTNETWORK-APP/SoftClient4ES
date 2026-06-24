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

import scala.util.DynamicVariable

/** Execution-scoped carrier for the per-call "suppress the single-index result cap" signal (Story
  * P0.5, ADR §D4.1 / Architecture Decision AD-1).
  *
  * This is a license-agnostic '''mechanism''' that lives in the Apache core. It carries NO quota
  * value and makes NO enforcement decision — it only records, for the duration of one
  * `gateway.run(...)` call on the current thread, whether that call is a JOIN '''leg''' (whose
  * input must NOT be capped, or the join silently returns wrong results) versus a direct
  * single-index query (which IS capped at the licensed `maxQueryResults`).
  *
  * ==Why a thread-scoped carrier and not a method/implicit parameter==
  *
  * Every read path — direct query AND join leg — funnels through `GatewayApi.run(sql: String)` →
  * `run(statement)` → `ExtensionRegistry.findHandler(statement).execute(statement, client)`. None
  * of those signatures has a slot for a per-call flag, and widening them would ripple across all ES
  * clients, every `Executor`, and the arrow streaming seam. The cap '''decision''' (which lives in
  * the closed `EnforcedDqlExtension`) is taken '''synchronously on the caller thread''', before the
  * first `Future`/Elasticsearch boundary, so a thread-scoped read observes the value the leg
  * executor (local joins) or the inbound-header middleware (federation legs) set around the `run`
  * call. The boolean rests on the single-license-per-deployment invariant (the sidecar already
  * knows its own cap, so only a boolean — not an explicit row count — needs to ride the wire).
  *
  * Default = `false` (do NOT suppress → the cap applies). Join-leg executors flip it to `true` only
  * for the scope of the leg.
  */
object ResultCapContext {

  private val suppress: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)

  /** `true` when the current call is a JOIN leg and the single-index result cap must be skipped. */
  def isSuppressed: Boolean = suppress.value

  /** Run `body` with cap suppression enabled for the current thread (and any synchronous callee on
    * it), restoring the previous value on exit. Used by the arrow join-leg executors
    * (`LocalConnection.execute` for local joins; the inbound-header middleware for federation legs)
    * to wrap the `gateway.run(...)` that materializes a leg.
    */
  def suppressed[T](body: => T): T = suppress.withValue(true)(body)

  /** Run `body` with an explicit suppression value (rarely needed; prefer [[suppressed]]). */
  def withSuppressed[T](value: Boolean)(body: => T): T = suppress.withValue(value)(body)
}
