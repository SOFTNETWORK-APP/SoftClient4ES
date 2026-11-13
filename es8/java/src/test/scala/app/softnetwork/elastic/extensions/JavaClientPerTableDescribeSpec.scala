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

package app.softnetwork.elastic.extensions

import app.softnetwork.elastic.client.{ElasticClientApi, PerTableDescribeSpec}
import app.softnetwork.elastic.sql.query.CreateMaterializedView

/** SCHEMA-15 (AC5) -- the ES 8 runner of [[PerTableDescribeSpec]], in the materialized-view
  * consumer's package on purpose: `describePerTable` is reached from that package exactly as the
  * view extension calls it, `client.describePerTable(create.search)`, over a client of the
  * consumer's static type (`ElasticClientApi`, from `ElasticClientFactory`, i.e. a delegator) and a
  * parsed `CREATE MATERIALIZED VIEW … AS <body>`.
  */
class JavaClientPerTableDescribeSpec extends PerTableDescribeSpec {

  "A view body" should "be typed from describePerTable(create.search) (AC5)" in {
    val consumer: ElasticClientApi = client
    val (b8Items, b8Keys) = g3.head
    val bodies = Seq(
      // B2: a joined object field and a function over a joined column
      s"SELECT a.aid, b.label, b.addr.zip AS customer_zip, UPPER(b.name) AS uname " +
      s"FROM $ptA a JOIN $ptB b $onBid" ->
      s"SELECT aid, label, addr.zip AS customer_zip, UPPER(name) AS uname FROM $ptAB",
      // B8: the aggregates
      s"SELECT $b8Items FROM $ptA a JOIN $ptB b $onBid GROUP BY $b8Keys" ->
      s"SELECT ${unqualified(b8Items)} FROM $ptAB GROUP BY ${unqualified(b8Keys)}",
      // B9: every B1 type of the main table
      s"SELECT a.aid, a.t, a.n, a.big_a, a.x, a.de, a.ts, a.b FROM $ptA a JOIN $ptB b $onBid" ->
      s"SELECT aid, t, n, big_a, x, de, ts, b FROM $ptA"
    )
    bodies.foreach { case (body, oracle) =>
      withClue(s"[$body] ") {
        val create = parsed(s"CREATE MATERIALIZED VIEW v AS $body") match {
          case view: CreateMaterializedView => view
          case other                        => fail(s"not a view: $other")
        }
        val typed = consumer.describePerTable(create.search)
        typed.toOption.flatten should not be empty
        sameAnswer(typed, consumer.describe(oracle))
      }
    }
  }
}
