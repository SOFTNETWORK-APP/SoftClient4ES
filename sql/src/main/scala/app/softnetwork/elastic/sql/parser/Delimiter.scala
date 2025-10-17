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

package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.{Expr, Token}

sealed trait Delimiter extends Token

sealed trait StartDelimiter extends Delimiter

case object StartPredicate extends Expr("(") with StartDelimiter
case object StartCase extends Expr("case") with StartDelimiter
case object WhenCase extends Expr("when") with StartDelimiter

sealed trait EndDelimiter extends Delimiter

case object EndPredicate extends Expr(")") with EndDelimiter
case object Separator extends Expr(",") with EndDelimiter
case object EndCase extends Expr("end") with EndDelimiter
case object ThenCase extends Expr("then") with EndDelimiter
