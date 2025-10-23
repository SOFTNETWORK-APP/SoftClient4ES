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

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.Token

case class SQLMultiSearchRequest(requests: Seq[SQLSearchRequest]) extends Token {
  override def sql: String = s"${requests.map(_.sql).mkString(" UNION ALL ")}"

  def update(): SQLMultiSearchRequest = this.copy(requests = requests.map(_.update()))

  override def validate(): Either[String, Unit] = {
    requests.map(_.validate()).filter(_.isLeft) match {
      case Nil    => Right(())
      case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
    }
  }

  lazy val sqlAggregations: Map[String, SQLAggregation] =
    requests.flatMap(_.sqlAggregations).distinct.toMap

  lazy val fieldAliases: Map[String, String] =
    requests.flatMap(_.fieldAliases).distinct.toMap
}
