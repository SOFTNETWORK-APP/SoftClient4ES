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

package app.softnetwork.elastic.arrow

import app.softnetwork.elastic.client.{GatewayApi, VersionApi, VersionDouble}
import app.softnetwork.elastic.client.SetOperationValues
import app.softnetwork.elastic.client.SetOperationValues.{LegSchema, LegSlot}
import app.softnetwork.elastic.client.metrics.{MetricsCollector, MetricsElasticClient}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The leg schema API as the relational engine (`app.softnetwork.elastic.arrow`) reaches it: from a
  * `GatewayApi`-typed client wrapped as its factory wraps it, the version read through
  * `VersionApi.dateDialectVersion`, each leg's slots read through `LegSchema` and `LegSlot`.
  */
class LegSchemasConsumerSmokeSpec extends AnyFlatSpec with Matchers {

  /** A client denied `GET /`, built for Elasticsearch 8, behind the metrics wrapper. */
  private val gateway: GatewayApi =
    new MetricsElasticClient(new VersionDouble(Some(8)), new MetricsCollector)

  private def legs(sql: String): Seq[Option[LegSchema]] = {
    val (requests, operators) = VersionDouble.resolved(sql)
    SetOperationValues.legSchemas(
      requests,
      operators,
      gateway.asInstanceOf[VersionApi].dateDialectVersion
    ) match {
      case Right(schemas) => schemas
      case Left(refusal)  => fail(s"[$sql] refused: $refusal")
    }
  }

  /** Each leg's slot types, `None` where a position is typed as its values come. */
  private def slotTypes(sql: String): Seq[Seq[Option[String]]] =
    legs(sql).map { leg =>
      leg.toSeq.flatMap(_.slots.map((slot: Option[LegSlot]) => slot.map(_.sqlType.sqlType.typeId)))
    }

  "The leg schemas" should "be read by the relational engine on every form it runs" in {
    // plain
    slotTypes("SELECT c_es AS k, n FROM t UNION SELECT tsz AS k, x AS n FROM t") shouldBe
    Seq(Seq(Some("TIMESTAMP"), Some("INT")), Seq(Some("VARCHAR"), Some("DOUBLE")))
    // multi-step
    slotTypes(
      "SELECT c_es AS k FROM t UNION SELECT ts AS k FROM t INTERSECT SELECT tsz AS k FROM t " +
      "EXCEPT SELECT c_c1 AS k FROM t"
    ) shouldBe Seq(
      Seq(Some("TIMESTAMP")),
      Seq(Some("TIMESTAMP")),
      Seq(Some("VARCHAR")),
      Seq(Some("TIMESTAMP"))
    )
    // a WITH passing its column through: the plain column's slot
    slotTypes(
      "WITH a AS (SELECT tsz FROM t) SELECT tsz AS k FROM a UNION SELECT ts AS k FROM t"
    ) shouldBe Seq(Seq(Some("VARCHAR")), Seq(Some("TIMESTAMP")))
    // a WITH aliasing in its CTE, a derived table: the column is ANY, no slot in any leg
    Seq(
      "WITH a AS (SELECT tsz AS k FROM t) SELECT k FROM a UNION SELECT ts AS k FROM t",
      "SELECT k FROM (SELECT tsz AS k FROM t) dd UNION SELECT ts AS k FROM t"
    ).foreach(sql => slotTypes(sql) shouldBe Seq(Seq(None), Seq(None)))
    // the as-stored positions and the reader, as the engine reads them
    val tsz: LegSlot = legs("SELECT tsz AS k FROM t UNION SELECT ts AS k FROM t").head
      .flatMap(_.slots.head)
      .getOrElse(fail("no slot"))
    tsz.asStored.map((p: LegSlot.AsStored) => p.path) shouldBe Seq("tsz")
    tsz.read("2024-01-31 10:30 CET") shouldBe "2024-01-31 10:30 CET"
  }
}
