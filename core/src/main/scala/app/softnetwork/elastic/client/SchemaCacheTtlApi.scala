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

import app.softnetwork.elastic.client.result.ElasticResult
import app.softnetwork.elastic.sql.schema.Schema

/** The ONE schema-cache TTL derivation, shared by every cache that keys on an index (story 21.8
  * Part D).
  *
  * Precedence, matching the house `flag > env > file > default` convention: the index's own
  * metadata (`ALTER TABLE … SET SCHEMA CACHE TTL`, read from the mapping when the schema is
  * fetched) beats `elastic.schema-cache.ttl` (HOCON / `ELASTIC_SCHEMA_CACHE_TTL`), which beats the
  * built-in five minutes.
  *
  * 🔴 Three caches used to hard-code five minutes in three files -- the schema cache
  * ([[IndicesApi]]), the primary-shard-count cache ([[ScrollApi]], #238) and the 404 negative cache
  * ([[SearchApi]], #276). They are already coupled (every schema-cache write or invalidation drops
  * the shard counts naming that index), and a coupled pair that drifts is the failure nobody
  * debugs, because each half reads as correct in isolation. Hence one trait, mixed into all three.
  *
  * The two members are deliberately different questions:
  *   - [[schemaCacheTtlMs]] — the DEFAULT, which is all a cache keyed by anything other than a
  *     concrete index can use;
  *   - [[schemaCacheTtlMsFor]] — the resolved TTL for one index, overridden by [[IndicesApi]] (the
  *     only trait that holds schemas) to consult what the index itself declares.
  */
trait SchemaCacheTtlApi {

  /** The default TTL for every cache keyed by an index. Overridden from `elastic.schema-cache.ttl`
    * by [[ElasticClientApi]]; still a `protected def` so a subclass can pin it in a test.
    */
  protected def schemaCacheTtlMs: Long = SchemaCacheSettings.DefaultTtlMs

  /** The resolved TTL for one index: what it declares, else [[schemaCacheTtlMs]].
    *
    * 🔴 Answering must never cost a round trip — this is called on the paging path. [[IndicesApi]]
    * answers it from the cache entry it already holds, so an index whose schema is not cached (or
    * an expression that is not one index, such as `orders*`) resolves to the default.
    */
  protected def schemaCacheTtlMsFor(index: String): Long = schemaCacheTtlMs
}

/** One expiry rule, for every cache that stamps an entry with the TTL that governed it.
  *
  * 🔴 The rule is `final` and the entries carry their own `ttlMs` on purpose: the TTL is per index
  * now, so re-spelling `now - cachedAt < ttl` at each call site would let two caches disagree about
  * when the same index goes stale (story 21.8 D.2).
  */
private[client] trait CacheEntry {
  def cachedAt: Long
  def ttlMs: Long
  final def isExpired(now: Long): Boolean = now - cachedAt >= ttlMs
}

/** A cached schema and the TTL resolved for it when it was fetched — see
  * [[SchemaCacheTtlApi.schemaCacheTtlMsFor]].
  */
private[client] case class CachedSchema(schema: Schema, cachedAt: Long, ttlMs: Long)
    extends CacheEntry

/** A cached primary-shard count (#238) on the schema's clock: the shortest TTL among the indices
  * the cache key names.
  */
private[client] case class CachedShardCount(
  count: ElasticResult[Int],
  cachedAt: Long,
  ttlMs: Long
) extends CacheEntry
