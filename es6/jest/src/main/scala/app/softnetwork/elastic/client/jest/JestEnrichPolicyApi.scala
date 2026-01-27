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

import app.softnetwork.elastic.client.result.ElasticFailure
import app.softnetwork.elastic.client.{result, EnrichPolicyApi}
import app.softnetwork.elastic.sql.schema

trait JestEnrichPolicyApi extends EnrichPolicyApi with JestClientHelpers {
  _: JestVersionApi with JestClientCompanion =>

  override private[client] def executeCreateEnrichPolicy(
    policy: schema.EnrichPolicy
  ): result.ElasticResult[Boolean] =
    ElasticFailure(
      result.ElasticError(
        message = "Enrich policy creation not implemented for Jest client",
        operation = Some("CreateEnrichPolicy"),
        statusCode = Some(501)
      )
    )

  override private[client] def executeDeleteEnrichPolicy(
    policyName: String
  ): result.ElasticResult[Boolean] =
    ElasticFailure(
      result.ElasticError(
        message = "Enrich policy deletion not implemented for Jest client",
        operation = Some("CreateEnrichPolicy"),
        statusCode = Some(501)
      )
    )

  override private[client] def executeExecuteEnrichPolicy(
    policyName: String
  ): result.ElasticResult[String] =
    ElasticFailure(
      result.ElasticError(
        message = "Enrich policy execution not implemented for Jest client",
        operation = Some("ExecuteEnrichPolicy"),
        statusCode = Some(501)
      )
    )
}
