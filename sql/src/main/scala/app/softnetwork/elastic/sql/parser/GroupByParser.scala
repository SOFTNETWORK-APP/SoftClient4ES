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

import app.softnetwork.elastic.sql.query.{Bucket, GroupBy}

trait GroupByParser {
  self: Parser with WhereParser =>

  def bucket: PackratParser[Bucket] = (long | identifier) ^^ { i =>
    Bucket(i)
  }

  def groupBy: PackratParser[GroupBy] =
    GroupBy.regex ~ rep1sep(bucket, separator) ^^ { case _ ~ buckets =>
      GroupBy(buckets)
    }

}
