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

package app.softnetwork.elastic.client.spi

import app.softnetwork.elastic.client.ElasticClientApi
import com.typesafe.config.Config

/** Service Provider Interface for creating Elasticsearch client instances.
  */
trait ElasticClientSpi {

  //format:off
  /** Creates an Elasticsearch client instance.
    *
    * @param conf
    *   Typesafe configuration containing Elasticsearch parameters
    * @return
    *   Configured ElasticClientApi instance
    *
    * @example
    * {{{
    * class MyElasticClientProvider extends ElasticClientSpi {
    *   override def client(config: Config): ElasticClientApi = {
    *     new MyElasticClientImpl(config)
    *   }
    * }
    * }}}
    */
  //format:on
  def client(conf: Config): ElasticClientApi
}
