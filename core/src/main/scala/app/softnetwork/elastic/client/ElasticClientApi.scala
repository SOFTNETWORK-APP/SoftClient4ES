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

import app.softnetwork.common.ClientCompanion
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import com.fasterxml.jackson.databind.ObjectMapper
import com.typesafe.config.{Config, ConfigFactory}
import org.json4s.jackson
import org.json4s.jackson.Serialization
import org.slf4j.Logger

import java.io.Closeable
import scala.language.{implicitConversions, postfixOps, reflectiveCalls}

/** Created by smanciot on 28/06/2018.
  */
trait ElasticClientApi
    extends IndicesApi
    with SettingsApi
    with AliasApi
    with MappingApi
    with CountApi
    with SearchApi
    with SingleValueAggregateApi
    with ScrollApi
    with IndexApi
    with UpdateApi
    with GetApi
    with BulkApi
    with DeleteApi
    with RefreshApi
    with FlushApi
    with VersionApi
    with SerializationApi
    with PipelineApi
    with TemplateApi
    with ClientCompanion {

  protected def logger: Logger

  def config: Config = ConfigFactory.load()

  final lazy val elasticConfig: ElasticConfig = ElasticConfig(config)
}

trait SerializationApi {
  implicit val serialization: Serialization.type = jackson.Serialization
  val mapper: ObjectMapper = JacksonConfig.objectMapper

}
