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

import app.softnetwork.elastic.client.metrics.MetricsConfig
import com.typesafe.config.{Config, ConfigFactory}
import com.typesafe.scalalogging.StrictLogging
import configs.ConfigReader

import java.time.Duration

/** Complete Elasticsearch client configuration.
  *
  * @param credentials
  *   Connection credentials (url, username, password)
  * @param multithreaded
  *   Enables multi-threaded mode for parallel operations
  * @param discovery
  *   Automatic cluster node discovery configuration
  * @param connectionTimeout
  *   Connection timeout to the cluster
  * @param socketTimeout
  *   Socket operation timeout
  * @param metrics
  *   Metrics and monitoring configuration
  * @param watcher
  *   Credentials for the watcher component (if applicable)
  * @param includeDocumentId
  *   When enabled, result rows surface the Elasticsearch document id as an `_id` column (disabled
  *   by default)
  * @param scroll
  *   Paged row extraction settings (`elastic.scroll`: page size and the ceiling on concurrent PIT
  *   slices, #238)
  * @param schemaCache
  *   How long a table's schema (and the shard count that follows it) may be cached by this client
  *   (`elastic.schema-cache.ttl`, story 21.8 Part D). An index may shorten or lengthen its own
  *   through `ALTER TABLE … SET SCHEMA CACHE TTL`
  */
case class ElasticConfig(
  credentials: ElasticCredentials = ElasticCredentials(),
  multithreaded: Boolean = true,
  discovery: DiscoveryConfig,
  connectionTimeout: Duration,
  socketTimeout: Duration,
  metrics: MetricsConfig,
  watcher: ElasticCredentials,
  includeDocumentId: Boolean = false,
  scroll: ScrollSettings = ScrollSettings(),
  schemaCache: SchemaCacheSettings = SchemaCacheSettings()
)

object ElasticConfig extends StrictLogging {

  /** The `elastic.*` defaults shipped in this jar (`softnetwork-elastic.conf`), resolved against
    * the classloader that loaded this class rather than the thread context classloader: under a
    * host-owned blind TCCL `ConfigFactory.load(name)` finds nothing and every client creation fails
    * on configuration before it can even look for a provider (#258).
    */
  private def defaults: Config =
    ConfigFactory.load(classOf[ElasticConfig].getClassLoader, "softnetwork-elastic.conf")

  def apply(config: Config): ElasticConfig = {
    ConfigReader[ElasticConfig]
      .read(config.withFallback(defaults), "elastic")
      .toEither match {
      case Left(configError) =>
        logger.error(s"Something went wrong with the provided arguments $configError")
        throw configError.configException
      case Right(r) => r
    }
  }
}
