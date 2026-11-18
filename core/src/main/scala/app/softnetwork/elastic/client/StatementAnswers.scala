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

import app.softnetwork.elastic.sql.schema.Schema

import java.util.concurrent.ConcurrentHashMap

/** SCHEMA-17 (R-159, A-191) -- the answer computed for a statement over a schema, remembered per
  * client: the SAME statement instance over the SAME schema instance answers from memory.
  *
  * Keyed by identity on both, never by value: two different statements may compare equal
  * (`Identifier.equals` leaves out the function chain), and a schema instance is replaced whenever
  * the schema cache's entry is -- a TTL expiry, a DDL invalidation, an explicit path's reload -- so
  * a stale answer can never be handed out, only recomputed. One entry per statement, replaced in
  * place by the answer over its new schema.
  *
  * Bounded: an insert of a new statement at `cap` entries clears the map first. Dead statements --
  * parsed, answered, dropped by their caller -- would otherwise keep their entries for good, and a
  * memo of the statements a consumer holds needs no ranking to be rebuilt.
  */
private[client] final class StatementAnswers[A](val cap: Int) {

  private[this] val answers = new ConcurrentHashMap[StatementAnswers.Ref, (Schema, A)]()

  /** The answer stored for `statement` over `schema` -- the very instances -- if any. */
  def get(statement: AnyRef, schema: Schema): Option[A] =
    answers.get(new StatementAnswers.Ref(statement)) match {
      case null                                 => None
      case (stored, answer) if stored eq schema => Some(answer)
      case _                                    => None
    }

  /** Remember `answer` for `statement` over `schema`, replacing the statement's previous one. */
  def put(statement: AnyRef, schema: Schema, answer: A): Unit = {
    val key = new StatementAnswers.Ref(statement)
    if (answers.size() >= cap && !answers.containsKey(key)) answers.clear()
    answers.put(key, (schema, answer))
  }

  /** How many statements are remembered. */
  def size: Int = answers.size()
}

private[client] object StatementAnswers {

  /** A statement compared by identity. */
  private final class Ref(val statement: AnyRef) {
    override def hashCode(): Int = System.identityHashCode(statement)
    override def equals(other: Any): Boolean =
      other match {
        case ref: Ref => ref.statement eq statement
        case _        => false
      }
  }
}
