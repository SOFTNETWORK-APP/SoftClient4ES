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

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.result._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration._

/** The STATUS a materialized view carrying an unmaterializable `HAVING` answers with, OBSERVED
  * through `GatewayApi.run(sql)` rather than assumed from the `Parser` verdict.
  *
  * 🔴 `operation` is `"sql"`, not `"mv"`, and that is forced rather than chosen: the refusal is a
  * parse-time rejection, and `GatewayApi.run`'s rejection branch cannot classify a statement it has
  * by construction failed to parse. MEASURED: the three materialized-view arms that already ship
  * (set operation / derived table / WHERE subquery) answer exactly the same way, so this pins
  * CONSISTENCY with them -- and the part that matters is the negative, asserted below: never a 500,
  * never a 404, never a 402 (the two defects extensions had to fix for reporting a user error as a
  * server fault and a permissions problem as a quota upsell).
  */
class MaterializedViewHavingGatewaySpec
    extends AnyFlatSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem("mv-having-gateway")
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(10.seconds))

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private class Nope extends NopeClientApi {
    override protected def logger: Logger = LoggerFactory.getLogger(getClass)
  }

  private val client = new Nope

  private def failureOf(sql: String): ElasticError =
    client.run(sql).futureValue match {
      case ElasticFailure(error) => error
      case other                 => fail(s"expected a failure for [$sql], got $other")
    }

  private val refused: Seq[(String, String)] = Seq(
    "a GROUP BY key condition" ->
    "CREATE MATERIALIZED VIEW mv AS SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city = 'Paris'",
    "a partial key condition beside a metric" ->
    "CREATE MATERIALIZED VIEW mv AS SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING COUNT(*) > 1 AND city = 'Paris'",
    "a HAVING with no GROUP BY" ->
    "CREATE MATERIALIZED VIEW mv AS SELECT COUNT(*) AS c FROM customers HAVING COUNT(*) > 1",
    "an aggregate the view does not publish" ->
    "CREATE MATERIALIZED VIEW mv AS SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING MAX(amount) > 1",
    "an aggregate no transform can compute" ->
    "CREATE MATERIALIZED VIEW mv AS SELECT city, STDDEV(amount) AS sd FROM customers GROUP BY city HAVING STDDEV(amount) > 1",
    "an expression over aggregates" ->
    "CREATE MATERIALIZED VIEW mv AS SELECT city, MAX(amount) - MIN(amount) AS d FROM customers GROUP BY city HAVING d > 3",
    "two aggregates compared" ->
    "CREATE MATERIALIZED VIEW mv AS SELECT city, MAX(amount) AS mx, MIN(amount) AS mn FROM customers GROUP BY city HAVING MAX(amount) > MIN(amount)"
  )

  "an unmaterializable HAVING" should "answer 400, naming the clause" in {
    refused.foreach { case (label, sql) =>
      withClue(s"[$label] ") {
        val error = failureOf(sql)
        error.statusCode shouldBe Some(400)
        error.message should include("MATERIALIZED VIEW")
        error.message should include("HAVING")
      }
    }
  }

  it should "never answer 500, 404 or 402" in {
    refused.foreach { case (label, sql) =>
      withClue(s"[$label] ") {
        val code = failureOf(sql).statusCode
        code should not be Some(500)
        code should not be Some(404)
        code should not be Some(402)
      }
    }
  }

  it should "answer exactly as the materialized-view arms that already ship" in {
    // The contract asserted is SAMENESS with the sibling refusals, not a literal `"sql"`: the three
    // below are the set-operation, derived-table and WHERE-subquery arms, all decided in the same
    // `CreateMaterializedView.validate()` and all surfaced through the same rejection branch.
    val siblings = Seq(
      "CREATE MATERIALIZED VIEW mv AS SELECT city FROM customers UNION ALL SELECT city FROM prospects",
      "CREATE MATERIALIZED VIEW mv AS SELECT d.city FROM (SELECT city FROM customers) AS d",
      "CREATE MATERIALIZED VIEW mv AS SELECT city FROM customers WHERE id IN (SELECT customer_id FROM orders)"
    ).map(failureOf)
    siblings.map(_.statusCode).distinct shouldBe Seq(Some(400))
    val siblingOperation = siblings.map(_.operation).distinct
    siblingOperation should have size 1
    refused.foreach { case (label, sql) =>
      withClue(s"[$label] ") {
        failureOf(sql).operation shouldBe siblingOperation.head
      }
    }
  }

  "a materialized view whose HAVING a transform CAN express" should "not be refused at parse" in {
    // No materialized-view extension is registered on any elasticsql classpath, so the furthest a
    // valid CREATE gets here is the DDL router's "unsupported" answer. That it reaches the ROUTER
    // at all is the assertion: the new rules did not refuse it.
    Seq(
      "CREATE MATERIALIZED VIEW mv AS SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING COUNT(*) > 1",
      "CREATE MATERIALIZED VIEW mv AS SELECT city, MAX(amount) AS mx FROM customers GROUP BY city HAVING MAX(amount) > 1",
      "CREATE OR REPLACE MATERIALIZED VIEW mv AS SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING COUNT(*) > 1",
      "CREATE MATERIALIZED VIEW mv AS SELECT city, COUNT(*) AS c FROM customers GROUP BY city"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val error = failureOf(sql)
        error.message should not include "MATERIALIZED VIEW cannot"
        error.message should not include "MATERIALIZED VIEW with a HAVING"
        error.operation shouldBe Some("table")
      }
    }
  }

  "the same HAVING outside a view" should "still be routed to the search executor" in {
    // The MV-only separation, at the gateway: the identical body as a plain SELECT is never
    // refused by these rules -- it reaches DQL routing and fails only for want of a real cluster.
    Seq(
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING city = 'Paris'",
      "SELECT COUNT(*) AS c FROM customers HAVING COUNT(*) > 1",
      "SELECT city, COUNT(*) AS c FROM customers GROUP BY city HAVING MAX(amount) > 1"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val error = failureOf(sql)
        error.operation shouldBe Some("dql")
        error.message should not include "MATERIALIZED VIEW"
      }
    }
  }
}
