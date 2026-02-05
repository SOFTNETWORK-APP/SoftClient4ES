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

package app.softnetwork.elastic.sql.policy

import app.softnetwork.elastic.sql.DdlToken
import app.softnetwork.elastic.sql.query.{Criteria, Where}
import app.softnetwork.elastic.sql.schema.mapper
import com.fasterxml.jackson.databind.JsonNode

case class EnrichPolicy(
  name: String,
  policyType: EnrichPolicyType = EnrichPolicyType.Match,
  indices: Seq[String],
  matchField: String,
  enrichFields: List[String],
  criteria: Option[Criteria] = None
) extends DdlToken {
  def sql: String =
    s"CREATE OR REPLACE ENRICH POLICY $name TYPE $policyType FROM ${indices
      .mkString(",")} ON $matchField ENRICH ${enrichFields.mkString(", ")}${Where(criteria)}"

  def node(implicit criteriaToNode: Criteria => JsonNode): JsonNode = {
    val node = mapper.createObjectNode()
    val policy = mapper.createObjectNode()
    val indicesNode = mapper.createArrayNode()
    indices.foreach { index =>
      indicesNode.add(index)
      ()
    }
    policy.set("indices", indicesNode)
    policy.put("match_field", matchField)
    val enrichFieldsNode = mapper.createArrayNode()
    enrichFields.foreach { field =>
      enrichFieldsNode.add(field)
      ()
    }
    policy.set("enrich_fields", enrichFieldsNode)
    criteria.foreach { c =>
      policy.set("query", implicitly[JsonNode](c))
      ()
    }
    node.set(policyType.name.toLowerCase, policy)
    node
  }
}
