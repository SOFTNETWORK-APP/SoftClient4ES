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

import app.softnetwork.elastic.sql.{Expr, TokenRegex}

case object Limit extends Expr("LIMIT") with TokenRegex

case class Limit(limit: Int, offset: Option[Offset])
    extends Expr(s" LIMIT $limit${offset.map(_.sql).getOrElse("")}")

case object Offset extends Expr("OFFSET") with TokenRegex

case class Offset(offset: Int) extends Expr(s" OFFSET $offset")
