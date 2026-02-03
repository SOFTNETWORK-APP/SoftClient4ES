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

import app.softnetwork.elastic.client.result.{
  ElasticError,
  ElasticFailure,
  ElasticResult,
  ElasticSuccess
}
import app.softnetwork.elastic.sql.schema.{EnrichPolicy, EnrichPolicyTask}

trait EnrichPolicyApi extends ElasticClientHelpers { _: VersionApi =>

  private def checkVersionForEnrichPolicy(): ElasticResult[Boolean] = {
    val elasticVersion = version match {
      case ElasticSuccess(value) => value
      case ElasticFailure(error) =>
        val failure =
          s"Cannot retrieve elastic version to check enrich policy support: ${error.getMessage}"
        logger.error(s"❌ $failure")
        return ElasticFailure(
          ElasticError(
            message = failure,
            operation = Some("CheckVersionForEnrichPolicy"),
            statusCode = Some(400)
          )
        )
    }
    if (!ElasticsearchVersion.supportsEnrich(elasticVersion)) {
      val failure = s"Enrich policies are not supported in elastic version $elasticVersion"
      logger.error(s"❌ $failure")
      return ElasticFailure(
        ElasticError(
          message = failure,
          operation = Some("CheckVersionForEnrichPolicy"),
          statusCode = Some(400)
        )
      )
    }
    ElasticSuccess(true)
  }

  def createEnrichPolicy(policy: EnrichPolicy): ElasticResult[Boolean] = {
    checkVersionForEnrichPolicy() match {
      case ElasticSuccess(_)           => // continue
      case failure @ ElasticFailure(_) => return failure
    }
    logger.info(s"🔧 Creating enrich policy: ${policy.name}")
    executeCreateEnrichPolicy(policy)
  }

  def deleteEnrichPolicy(policyName: String): ElasticResult[Boolean] = {
    checkVersionForEnrichPolicy() match {
      case ElasticSuccess(_)           => // continue
      case failure @ ElasticFailure(_) => return failure
    }
    logger.info(s"🔧 Deleting enrich policy: $policyName")
    executeDeleteEnrichPolicy(policyName)
  }

  def executeEnrichPolicy(policyName: String): ElasticResult[EnrichPolicyTask] = {
    checkVersionForEnrichPolicy() match {
      case ElasticSuccess(_)           => // continue
      case failure @ ElasticFailure(_) => return failure
    }
    logger.info(s"🔧 Executing enrich policy: $policyName")
    executeExecuteEnrichPolicy(policyName)
  }

  private[client] def executeCreateEnrichPolicy(policy: EnrichPolicy): ElasticResult[Boolean]

  private[client] def executeDeleteEnrichPolicy(policyName: String): ElasticResult[Boolean]

  private[client] def executeExecuteEnrichPolicy(
    policyName: String
  ): ElasticResult[EnrichPolicyTask]
}
