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

/** Pinned to 6.7.2 like every other jest spec that runs DDL (`JestGatewayApiSpec`,
  * `JestClientTemplateApiSpec`, …): on 6.8 this client sends a typed mapping in which `_meta` is
  * read as the TYPE name, so any `CREATE TABLE` — which always writes `_meta.columns` — is rejected
  * with `Root mapping definition has unsupported parameters`. Pre-existing and unrelated to the
  * TTL; the ES 6 rest client on 6.8 runs the same DDL fine.
  */
class JestClientPerIndexSchemaCacheTtlSpec extends PerIndexSchemaCacheTtlSpec {
  override def elasticVersion: String = "6.7.2"
}
