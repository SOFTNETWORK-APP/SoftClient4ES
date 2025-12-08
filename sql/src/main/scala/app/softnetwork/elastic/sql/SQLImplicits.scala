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

package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{Criteria, MultiSearch, SingleSearch, Statement}

import scala.util.matching.Regex

/** Created by smanciot on 27/06/2018.
  */
object SQLImplicits {
  import scala.language.implicitConversions

  implicit def queryToSQLCriteria(query: String): Option[Criteria] = {
    val maybeQuery: Option[Statement] = query
    maybeQuery match {
      case Some(statement) =>
        statement match {
          case single: SingleSearch =>
            single.where.flatMap(_.criteria)
          case _ => None
        }
      case _ => None
    }
  }

  implicit def queryToStatement(
    query: String
  ): Option[Statement] = {
    Parser(query) match {
      case Left(_)  => None
      case Right(r) => Some(r)
    }
  }

  implicit def sqllikeToRegex(value: String): Regex = toRegex(value).r

}
