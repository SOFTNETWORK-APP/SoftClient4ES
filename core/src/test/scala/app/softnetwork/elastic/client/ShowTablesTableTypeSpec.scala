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
import app.softnetwork.elastic.sql.schema.TableType
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.immutable.ListMap
import scala.concurrent.duration._

/** Story BIDC-10a Part D (AC 10) — the `type` column of `SHOW TABLES`, end to end, Docker-free.
  *
  * `TableTypeVocabularySpec` (sql) pins `TableType.sqlName` itself. This one pins the only thing
  * that makes it matter: that the projection in `TableExecutor` actually publishes it, starting
  * from a mapping whose `_meta.type` carries the STORED name. The same seam serves the JDBC
  * `getTables`, the Flight SQL `GET_TABLES` and the REPL, all three of which execute `SHOW TABLES`
  * through the gateway.
  */
class ShowTablesTableTypeSpec
    extends AnyFlatSpec
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {

  implicit private val system: ActorSystem = ActorSystem("show-tables-table-type")
  override implicit val patienceConfig: PatienceConfig =
    PatienceConfig(timeout = scaled(5.seconds))

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  /** A mapping exactly as this engine writes it: the STORED `_meta.type` name, never the display
    * value. If `TableType.apply` ever stopped reading these, every one of these rows would fall
    * back to `Regular` and the expectations below would redden.
    */
  private def mappingOf(t: TableType): String =
    s"""{"_meta":{"type":"${t.name}"},"properties":{"name":{"type":"keyword"}}}"""

  private class StubMappings(mappings: Map[String, String]) extends NopeClientApi {
    override protected def logger: Logger = LoggerFactory.getLogger(getClass)
    override private[client] def executeGetAllMappings(
      indices: Seq[String]
    ): ElasticResult[Map[String, String]] = ElasticResult.success(mappings)
  }

  private def showTables(mappings: Map[String, String]): Seq[ListMap[String, Any]] =
    new StubMappings(mappings).run("SHOW TABLES").futureValue match {
      case ElasticSuccess(QueryRows(rows, _)) => rows
      case other                              => fail(s"expected QueryRows, got $other")
    }

  private val allTypes: Seq[TableType] = Seq(
    TableType.Regular,
    TableType.View,
    TableType.MaterializedView,
    TableType.External,
    TableType.Changelog,
    TableType.Enrichment
  )

  "SHOW TABLES" should "report the client vocabulary for every table type" in {
    val indices = allTypes.map(t => s"idx_${t.name}" -> mappingOf(t)).toMap
    val byName = showTables(indices).map { r =>
      r("name").toString -> r("type").toString
    }.toMap

    byName shouldBe allTypes.map(t => s"idx_${t.name}" -> t.sqlName).toMap
  }

  it should "report a plain index as TABLE, never REGULAR" in {
    val rows = showTables(Map("orders" -> mappingOf(TableType.Regular)))
    rows.map(_("type").toString) shouldBe Seq("TABLE")
  }

  /** A mapping with no `_meta.type` at all — every index created before table types existed. It
    * defaults to `Regular` and must therefore also read as `TABLE`.
    */
  it should "report an index with no _meta as TABLE" in {
    val rows = showTables(Map("legacy" -> """{"properties":{"name":{"type":"keyword"}}}"""))
    rows.map(_("type").toString) shouldBe Seq("TABLE")
  }

  it should "keep a materialized view distinct from a view" in {
    val rows = showTables(
      Map(
        "orders_mv" -> mappingOf(TableType.MaterializedView),
        "orders_v"  -> mappingOf(TableType.View)
      )
    ).map(r => r("name").toString -> r("type").toString).toMap

    rows("orders_mv") shouldBe "MATERIALIZED_VIEW"
    rows("orders_v") shouldBe "VIEW"
  }
}
