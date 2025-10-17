/*
 * Copyright 2015 SOFTNETWORK
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

import app.softnetwork.elastic.sql.query.{Limit, Offset}

trait LimitParser {
  self: Parser =>

  def offset: PackratParser[Offset] = Offset.regex ~ long ^^ { case _ ~ i =>
    Offset(i.value.toInt)
  }

  def limit: PackratParser[Limit] = Limit.regex ~ long ~ offset.? ^^ { case _ ~ i ~ o =>
    Limit(i.value.toInt, o)
  }

}
