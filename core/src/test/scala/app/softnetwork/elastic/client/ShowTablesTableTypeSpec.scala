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
import app.softnetwork.elastic.sql.schema.{Table, TableType, TableTypeEnumeration}
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

  /** Derived from the SAME sealed-hierarchy walk `TableTypeVocabularySpec` uses (reachable here
    * because `core -> macros -> sql` carries `test->test`), never hand-listed.
    *
    * A hand-written list would leave THIS spec green when a seventh table type is added while the
    * sql-side vocabulary spec goes red - i.e. the projection guard would silently stop covering the
    * projection, which is the exact shape of the defect this story closes.
    */
  private val allTypes: Seq[TableType] = TableTypeEnumeration.declaredTableTypes

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

  // -- The SECOND display projection: the `SHOW TABLE <t>` header (REPL `\st <table>`) ----------
  //
  // `ResultRenderer.renderTableDefinition` interpolated the case object itself and printed
  // `[Regular]` - the Scala type name. Both projections now read `TableType.sqlName`, so a REPL
  // session cannot show one vocabulary in `SHOW TABLES` and another in `SHOW TABLE`.

  private def showTableHeader(t: TableType): String =
    ResultRenderer.renderAscii(TableResult(Table("users", Nil, tableType = t)), 1.milli)

  "SHOW TABLE" should "head its definition with the same vocabulary for every type" in {
    allTypes.foreach { t =>
      withClue(s"`SHOW TABLE` header for $t: ") {
        showTableHeader(t) should include(s"[${t.sqlName}]")
      }
    }
  }

  it should "never head a plain table with the Scala type name" in {
    val header = showTableHeader(TableType.Regular)
    header should include("[TABLE]")
    header should not include "[Regular]"
  }

  /** The divergence this closes was measurable in the repo's own walkthrough: `SHOW TABLES` and
    * `SHOW TABLE` appear 35 lines apart in `documentation/sql/dql_statements.md` and used to
    * disagree.
    */
  it should "agree with the SHOW TABLES type column" in {
    allTypes.foreach { t =>
      val listed = showTables(Map("users" -> mappingOf(t))).head("type").toString
      withClue(s"SHOW TABLES says `$listed` for $t; SHOW TABLE header: ") {
        showTableHeader(t) should include(s"[$listed]")
      }
    }
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
