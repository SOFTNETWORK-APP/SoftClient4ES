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
import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.client.scroll.ScrollMetrics
import app.softnetwork.elastic.scalatest.ElasticTestKit
import app.softnetwork.elastic.sql.schema.{IngestPipeline, Table}
import app.softnetwork.persistence.generateUUID
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import scala.collection.immutable.ListMap
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContextExecutor}

// ---------------------------------------------------------------------------
// Base test trait — to be mixed with ElasticDockerTestKit
// ---------------------------------------------------------------------------

trait GatewayIntegrationTestKit extends AnyFlatSpecLike with Matchers with ScalaFutures {
  self: ElasticTestKit =>

  lazy val log: Logger = LoggerFactory getLogger getClass.getName

  implicit val system: ActorSystem = ActorSystem(generateUUID())
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(30, Seconds))

  // Provided by concrete test class
  def client: GatewayApi

  override def beforeAll(): Unit = {
    self.beforeAll()
  }

  override def afterAll(): Unit = {
    Await.result(system.terminate(), Duration(30, TimeUnit.SECONDS))
    self.afterAll()
  }

  def supportsEnrichPolicies: Boolean = {
    client.asInstanceOf[VersionApi].version match {
      case ElasticSuccess(v) => ElasticsearchVersion.supportsEnrich(v)
      case ElasticFailure(error) =>
        log.error(s"❌ Failed to retrieve Elasticsearch version: ${error.message}")
        false
    }
  }

  def supportsQueryWatchers: Boolean = {
    client.asInstanceOf[VersionApi].version match {
      case ElasticSuccess(v) => ElasticsearchVersion.supportsQueryWatchers(v)
      case ElasticFailure(error) =>
        log.error(s"❌ Failed to retrieve Elasticsearch version: ${error.message}")
        false
    }
  }

  def renderResults(stratTime: Long, res: ElasticResult[QueryResult]): Unit = {
    val duration = (System.nanoTime() - stratTime).nanos
    res match {
      case ElasticSuccess(result) =>
        log.info(s"\n${ResultRenderer.render(result, duration)}")
      case ElasticFailure(error) =>
        log.error(s"❌ Execution failed after ${duration.toMillis} ms: ${error.message}")
    }
  }

  // -------------------------------------------------------------------------
  // Helper: assert SELECT result type
  // -------------------------------------------------------------------------

  private def normalizeRow(row: ListMap[String, Any]): ListMap[String, Any] = {
    val updated = row - "_id" - "_index" - "_score" - "_version" - "_sort"
    updated.map(entry =>
      entry._2 match {
        case m: ListMap[_, _] =>
          entry._1 -> normalizeRow(m.asInstanceOf[ListMap[String, Any]])
        case seq: Seq[_] if seq.nonEmpty && seq.head.isInstanceOf[Map[_, _]] =>
          entry._1 -> seq
            .asInstanceOf[Seq[ListMap[String, Any]]]
            .map(m => normalizeRow(m))
        case other => entry._1 -> other
      }
    )
  }

  def assertSelectResult(
    startTime: Long,
    res: ElasticResult[QueryResult],
    rows: Seq[Map[String, Any]] = Seq.empty,
    nbResults: Option[Int] = None
  ): Unit = {
    if (!res.isSuccess) {
      renderResults(startTime, res)
    }
    res.isSuccess shouldBe true
    res.toOption.get match {
      case QueryStream(stream) =>
        val sink =
          Sink.fold[Seq[ListMap[String, Any]], (ListMap[String, Any], ScrollMetrics)](Seq.empty) {
            case (acc, (row, _)) =>
              acc :+ normalizeRow(row)
          }
        val results = stream.runWith(sink).futureValue
        renderResults(startTime, ElasticSuccess(QueryRows(results)))
        if (rows.nonEmpty) {
          results.size shouldBe rows.size
          results should contain theSameElementsAs rows
        } else if (nbResults.isDefined) {
          results.size shouldBe nbResults.get
        } else {
          log.info(s"Rows: $results")
        }
      case QueryStructured(response) =>
        renderResults(startTime, res)
        val results =
          response.results.map(normalizeRow)
        if (rows.nonEmpty) {
          results.size shouldBe rows.size
          results should contain theSameElementsAs rows
        } else if (nbResults.isDefined) {
          results.size shouldBe nbResults.get
        } else {
          log.info(s"Rows: $results")
        }
      case q: QueryRows =>
        renderResults(startTime, res)
        val results = q.rows.map(normalizeRow)
        if (rows.nonEmpty) {
          results.size shouldBe rows.size
          results should contain theSameElementsAs rows
        } else if (nbResults.isDefined) {
          results.size shouldBe nbResults.get
        } else {
          log.info(s"Rows: $results")
        }
      case other =>
        renderResults(startTime, res)
        fail(s"Unexpected QueryResult type for SELECT: $other")
    }
  }

  // -------------------------------------------------------------------------
  // Helper: assert DDL result type
  // -------------------------------------------------------------------------

  def assertDdl(startTime: Long, res: ElasticResult[QueryResult]): Unit = {
    renderResults(startTime, res)
    res.isSuccess shouldBe true
    res.toOption.get shouldBe a[DdlResult]
  }

  // -------------------------------------------------------------------------
  // Helper: assert DML result type
  // -------------------------------------------------------------------------

  def assertDml(
    startTime: Long,
    res: ElasticResult[QueryResult],
    result: Option[DmlResult] = None
  ): Unit = {
    renderResults(startTime, res)
    res.isSuccess shouldBe true
    res.toOption.get shouldBe a[DmlResult]
    result match {
      case Some(expected) =>
        val dml = res.toOption.get.asInstanceOf[DmlResult]
        dml.inserted shouldBe expected.inserted
        dml.updated shouldBe expected.updated
        dml.deleted shouldBe expected.deleted
      case None => // do nothing
    }
  }

  // -------------------------------------------------------------------------
  // Helper: assert Query Rows result type
  // -------------------------------------------------------------------------

  def assertQueryRows(startTime: Long, res: ElasticResult[QueryResult]): Seq[Map[String, Any]] = {
    renderResults(startTime, res)
    res.isSuccess shouldBe true
    res.toOption.get shouldBe a[QueryRows]
    res.toOption.get.asInstanceOf[QueryRows].rows
  }

  // -------------------------------------------------------------------------
  // Helper: assert SHOW TABLE result type
  // -------------------------------------------------------------------------

  def assertShowTable(startTime: Long, res: ElasticResult[QueryResult]): Table = {
    renderResults(startTime, res)
    res.isSuccess shouldBe true
    res.toOption.get shouldBe a[TableResult]
    res.toOption.get.asInstanceOf[TableResult].table
  }

  // -------------------------------------------------------------------------
  // Helper: assert SHOW PIPELINE result type
  // -------------------------------------------------------------------------

  def assertShowPipeline(startTime: Long, res: ElasticResult[QueryResult]): IngestPipeline = {
    renderResults(startTime, res)
    res.isSuccess shouldBe true
    res.toOption.get shouldBe a[PipelineResult]
    res.toOption.get.asInstanceOf[PipelineResult].pipeline
  }

  // -------------------------------------------------------------------------
  // Helper: assert SHOW CREATE result type
  // -------------------------------------------------------------------------

  def assertShowCreate(startTime: Long, res: ElasticResult[QueryResult]): String = {
    renderResults(startTime, res)
    res.isSuccess shouldBe true
    res.toOption.get shouldBe a[SQLResult]
    res.toOption.get.asInstanceOf[SQLResult].sql
  }

}
