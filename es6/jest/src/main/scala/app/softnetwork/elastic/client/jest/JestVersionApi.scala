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

package app.softnetwork.elastic.client.jest

import app.softnetwork.elastic.client.{result, ElasticsearchVersion, VersionApi}
import io.searchbox.core.Cat
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

trait JestVersionApi extends VersionApi with JestClientHelpers {
  _: JestClientCompanion =>

  /** Whether a request that carries a mapping must say `include_type_name=false`: on Elasticsearch
    * 6.8, the one version that receives the mapping TYPELESS yet reads a mapping as TYPED by
    * default.
    *
    * The mapping's shape is decided upstream, per version, by
    * [[ElasticsearchVersion.requiresDocTypeWrapper]] (`MappingConverter`, `TemplateConverter`):
    * wrapped in `_doc` before 6.8, typeless from 6.8. Every 6.x defaults `include_type_name` to
    * `true`, so 6.8 read the typeless mapping's top-level keys as TYPE names and refused the
    * `_meta` every table writes (`Failed to parse mapping [_meta]: Root mapping definition has
    * unsupported parameters`): no `CREATE TABLE` ran through this client on 6.8, partitioned or
    * not. The ES 6 REST client sends the same parameter on the same requests (its typeless
    * `CreateIndexRequest` and `PutIndexTemplateRequest`). Not before 6.8, which refuses the
    * parameter with a `_doc`-wrapped mapping; not from 7.0, where a mapping is typeless by default.
    *
    * A put mapping needs none: it names the type in its path (`PUT /<index>/_doc/_mapping`), which
    * 6.8 takes with a typeless body, and with the parameter set it would refuse the type.
    */
  private[client] def sendsTypelessMappings: Boolean =
    version match {
      case result.ElasticSuccess(v) =>
        ElasticsearchVersion.isEs6(v) && !ElasticsearchVersion.requiresDocTypeWrapper(v)
      case _ => false
    }

  override private[client] def executeVersion(): result.ElasticResult[String] =
    executeJestAction(
      "version",
      retryable = true
    )(
      new Cat.NodesBuilder()
        .setParameter("h", "version")
        .build()
    ) { result =>
      val jsonString = result.getJsonString
      implicit val formats: DefaultFormats.type = DefaultFormats
      val json = JsonMethods.parse(jsonString)
      (json \\ "version").extract[String]
    }
}
