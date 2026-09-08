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

import java.time.Duration

/** Schema-cache settings (`elastic.schema-cache` in HOCON, story 21.8 Part D).
  *
  * @param ttl
  *   how long a table's schema may be cached by this client before it is read from Elasticsearch
  *   again (`elastic.schema-cache.ttl`, `ELASTIC_SCHEMA_CACHE_TTL`). The DEFAULT: an index that
  *   declares its own (`ALTER TABLE … SET SCHEMA CACHE TTL = '10m'`) overrides it for itself. Since
  *   #306 every executed query reads the cached schema, so this is a correctness-latency knob:
  *   shorten it for a mapping that changes under a running client, lengthen it to spare the
  *   re-parse of a wide mapping. Must be positive.
  */
case class SchemaCacheSettings(
  ttl: Duration = Duration.ofMillis(SchemaCacheSettings.DefaultTtlMs)
) {
  require(
    !ttl.isZero && !ttl.isNegative,
    s"elastic.schema-cache.ttl must be positive (ELASTIC_SCHEMA_CACHE_TTL), got $ttl"
  )

  def ttlMs: Long = ttl.toMillis
}

object SchemaCacheSettings {

  /** Five minutes -- what `IndicesApi`, `ScrollApi` and `SearchApi` each hard-coded before this
    * setting existed.
    */
  val DefaultTtlMs: Long = 5 * 60 * 1000L
}
