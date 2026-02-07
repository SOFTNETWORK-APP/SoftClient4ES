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

sealed trait EnrichPolicyType {
  def name: String
  override def toString: String = name
}

object EnrichPolicyType {
  case object Match extends EnrichPolicyType {
    val name: String = "MATCH"
  }
  case object GeoMatch extends EnrichPolicyType {
    val name: String = "GEO_MATCH"
  }
  case object Range extends EnrichPolicyType {
    val name: String = "RANGE"
  }

  def apply(name: String): EnrichPolicyType = name.toUpperCase() match {
    case "MATCH"     => Match
    case "GEO_MATCH" => GeoMatch
    case "RANGE"     => Range
    case _           => Match
  }
}
