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

package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.sql.{ObjectValue, Value}
import com.typesafe.config.{ConfigFactory, ConfigValueFactory}

import scala.util.{Failure, Success, Try}

/** How long a table's schema may be cached by a client -- where it is written, how it is spelled,
  * and the ONE parse of it (story 21.8 Part D).
  *
  * The value lives in the index's own mapping metadata, at [[MetadataPath]], because the volatility
  * of a mapping is a property of the index. Three surfaces write and read it, and all three go
  * through this object so they cannot disagree:
  *   - `ALTER TABLE <t> SET SCHEMA CACHE TTL = '10m'` (and `CREATE TABLE … OPTIONS (mappings =
  *     (_meta = (schema_cache_ttl = '10m')))`), which the parser validates with [[parse]] so a
  *     misspelled duration is refused at parse time rather than silently ignored a page later;
  *   - `IndicesApi`, which stamps the resolved TTL on the cache entry when the schema is fetched;
  *   - `ScrollApi`'s shard-count cache, which follows the same clock (a schema cached for an hour
  *     and a shard count re-probed every five minutes is incoherent).
  *
  * 🔴 The TTL is a CORRECTNESS-latency knob, not a performance one. Since #306 every executed query
  * reads the cached schema, so a stale entry no longer means a stale column list -- it means wrong
  * emitted Painless (retype a column `keyword` -> `long` and the cached `keyword` schema keeps
  * emitting `Long.parseLong(doc['x'].value)` against a numeric field for the rest of the TTL).
  */
object SchemaCacheTtl {

  /** The `_meta` key. Snake case, like every other key this project writes into `_meta`
    * (`primary_key`, `partition_by`, `materialized_views`).
    */
  val MetadataKey: String = "schema_cache_ttl"

  /** The dotted path an `ALTER TABLE … SET MAPPING` writes -- `ObjectValue.set` splits on `.`. */
  val MetadataPath: String = s"_meta.$MetadataKey"

  /** The spelling of the dedicated DDL sugar, for error messages. */
  val Ddl: String = "SET SCHEMA CACHE TTL"

  /** Parse a written TTL into milliseconds.
    *
    * HOCON duration syntax (`10m`, `30 seconds`, `1h`, a bare number of milliseconds) through
    * Typesafe Config itself, so the per-index spelling and the `elastic.schema-cache.ttl` default
    * accept exactly the same forms -- two duration parsers would be two sets of accepted spellings.
    * The raw text is passed as a config VALUE, never interpolated into parsed text, so a `"` or a
    * newline in it cannot become syntax.
    *
    * @return
    *   the TTL in milliseconds, or the reason it is not one
    */
  def parse(raw: String): Either[String, Long] = {
    val trimmed = Option(raw).map(_.trim).getOrElse("")
    if (trimmed.isEmpty) Left("a schema cache TTL cannot be empty")
    else
      Try(
        ConfigFactory.empty
          .withValue("ttl", ConfigValueFactory.fromAnyRef(trimmed))
          .getDuration("ttl")
          .toMillis
      ) match {
        case Success(ms) if ms > 0 => Right(ms)
        case Success(ms) =>
          Left(s"a schema cache TTL must be positive, got '$trimmed' ($ms ms)")
        case Failure(_) =>
          Left(
            s"'$trimmed' is not a duration — write it as '30s', '10m', '1h' or a number of milliseconds"
          )
      }
  }

  /** The TTL written on this table, if any: `None` when the table declares none (the caller's
    * default governs), `Some(Left(reason))` when it declares one that is not a duration.
    *
    * A table read back from Elasticsearch carries `_meta` verbatim in `Table.mappings`
    * (`IndexMappings.options`), and `Table.update()` rebuilds only the five keys it owns, so this
    * key survives every round trip.
    */
  def of(table: Table): Option[Either[String, Long]] =
    ObjectValue(table.mappings)
      .find(MetadataPath)
      .map((v: Value[_]) => parse(String.valueOf(v.value)))
}
