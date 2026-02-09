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

package app.softnetwork.elastic.client.repl

import akka.actor.ActorSystem
import akka.stream.Materializer
import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.client._
import app.softnetwork.elastic.client.result.{
  DdlResult,
  DmlResult,
  ElasticFailure,
  ElasticSuccess,
  PipelineResult,
  QueryRows,
  QueryStream,
  QueryStructured,
  ResultRenderer,
  SQLResult,
  StreamResult,
  TableResult
}
import app.softnetwork.elastic.client.scroll.ScrollMetrics
import app.softnetwork.elastic.scalatest.ElasticTestKit
import app.softnetwork.elastic.sql.schema.{IngestPipeline, Schema}
import app.softnetwork.persistence.generateUUID
import org.scalatest.BeforeAndAfterAll
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import scala.concurrent.{Await, ExecutionContextExecutor}
import scala.concurrent.duration._

/** Base trait for REPL integration tests Mirrors GatewayApiIntegrationSpec but uses the REPL
  * executor
  */
trait ReplIntegrationTestKit
    extends AnyFlatSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll {
  self: ElasticTestKit =>

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher
  implicit val materializer: Materializer = Materializer(system)
  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(30, Seconds))

  // Provided by concrete test class
  def gateway: GatewayApi

  // REPL components
  protected lazy val executor: StreamingReplExecutor = new StreamingReplExecutor(gateway)
  protected lazy val testRepl: TestableRepl = new TestableRepl(executor)

  override def beforeAll(): Unit = {
    self.beforeAll()
  }

  override def afterAll(): Unit = {
    Await.result(system.terminate(), Duration(30, TimeUnit.SECONDS))
    self.afterAll()
  }

  def supportsEnrichPolicies: Boolean = {
    gateway.asInstanceOf[VersionApi].version match {
      case ElasticSuccess(v) => ElasticsearchVersion.supportsEnrich(v)
      case ElasticFailure(error) =>
        log.error(s"❌ Failed to retrieve Elasticsearch version: ${error.message}")
        false
    }
  }

  def supportsQueryWatchers: Boolean = {
    gateway.asInstanceOf[VersionApi].version match {
      case ElasticSuccess(v) => ElasticsearchVersion.supportsQueryWatchers(v)
      case ElasticFailure(error) =>
        log.error(s"❌ Failed to retrieve Elasticsearch version: ${error.message}")
        false
    }
  }

  // -------------------------------------------------------------------------
  // Helper: Execute and render results
  // -------------------------------------------------------------------------

  def renderResults(startTime: Long, res: ExecutionResult): Unit = {
    val duration = (System.nanoTime() - startTime).nanos
    res match {
      case ExecutionSuccess(result, _) =>
        result match {
          case StreamResult(_, _) =>
            log.info(s"✅ Query executed successfully in ${duration.toMillis} ms (streaming result)")
            log.info(
              s"\n${ResultRenderer.render(QueryRows(testRepl.consumeStreamSync()), duration)}"
            )
          case _ =>
            log.info(s"\n${ResultRenderer.render(result, duration)}")
        }
      case ExecutionFailure(error, _) =>
        log.error(s"❌ Execution failed after ${duration.toMillis} ms: ${error.message}")
    }
  }

  // -------------------------------------------------------------------------
  // Helper: Normalize rows (remove metadata fields)
  // -------------------------------------------------------------------------

  private def normalizeRow(row: Map[String, Any]): Map[String, Any] = {
    val updated = row - "_id" - "_index" - "_score" - "_version" - "_sort"
    updated.map { entry =>
      entry._2 match {
        case m: Map[_, _] =>
          entry._1 -> normalizeRow(m.asInstanceOf[Map[String, Any]])
        case seq: Seq[_] if seq.nonEmpty && seq.head.isInstanceOf[Map[_, _]] =>
          entry._1 -> seq.asInstanceOf[Seq[Map[String, Any]]].map(m => normalizeRow(m))
        case other => entry._1 -> other
      }
    }
  }

  // -------------------------------------------------------------------------
  // Helper: Assert SELECT result type
  // -------------------------------------------------------------------------

  def assertSelectResult(
    startTime: Long,
    res: ExecutionResult,
    rows: Seq[Map[String, Any]] = Seq.empty,
    nbResults: Option[Int] = None
  ): Unit = {
    renderResults(startTime, res)
    res shouldBe a[ExecutionSuccess]

    val success = res.asInstanceOf[ExecutionSuccess]
    success.result match {
      case QueryStream(stream) =>
        val sink = Sink.fold[Seq[Map[String, Any]], (Map[String, Any], ScrollMetrics)](Seq.empty) {
          case (acc, (row, _)) => acc :+ normalizeRow(row)
        }
        val results = stream.runWith(sink).futureValue
        if (rows.nonEmpty) {
          results.size shouldBe rows.size
          results should contain theSameElementsAs rows
        } else if (nbResults.isDefined) {
          results.size shouldBe nbResults.get
        } else {
          log.info(s"Rows: $results")
        }

      case QueryStructured(response) =>
        val results = response.results.map(normalizeRow)
        if (rows.nonEmpty) {
          results.size shouldBe rows.size
          results should contain theSameElementsAs rows
        } else if (nbResults.isDefined) {
          results.size shouldBe nbResults.get
        } else {
          log.info(s"Rows: $results")
        }

      case q: QueryRows =>
        val results = q.rows.map(normalizeRow)
        if (rows.nonEmpty) {
          results.size shouldBe rows.size
          results should contain theSameElementsAs rows
        } else if (nbResults.isDefined) {
          results.size shouldBe nbResults.get
        } else {
          log.info(s"Rows: $results")
        }

      case StreamResult(estimatedSize, isActive) =>
        log.info(
          s"Stream result - Estimated size: ${estimatedSize.getOrElse("N/A")}, Active: $isActive"
        )

      case other => fail(s"Unexpected QueryResult type for SELECT: $other")
    }
  }

  // -------------------------------------------------------------------------
  // Helper: Assert DDL result type
  // -------------------------------------------------------------------------

  def assertDdl(startTime: Long, res: ExecutionResult): Unit = {
    renderResults(startTime, res)
    res shouldBe a[ExecutionSuccess]
    res.asInstanceOf[ExecutionSuccess].result shouldBe a[DdlResult]
  }

  // -------------------------------------------------------------------------
  // Helper: Assert DML result type
  // -------------------------------------------------------------------------

  def assertDml(
    startTime: Long,
    res: ExecutionResult,
    result: Option[DmlResult] = None
  ): Unit = {
    renderResults(startTime, res)
    res shouldBe a[ExecutionSuccess]
    res.asInstanceOf[ExecutionSuccess].result shouldBe a[DmlResult]
    result match {
      case Some(expected) =>
        val dml = res.asInstanceOf[ExecutionSuccess].result.asInstanceOf[DmlResult]
        dml.inserted shouldBe expected.inserted
        dml.updated shouldBe expected.updated
        dml.deleted shouldBe expected.deleted
      case None => // do nothing
    }
  }

  // -------------------------------------------------------------------------
  // Helper: Assert Query Rows result type
  // -------------------------------------------------------------------------

  def assertQueryRows(startTime: Long, res: ExecutionResult): Seq[Map[String, Any]] = {
    renderResults(startTime, res)
    res shouldBe a[ExecutionSuccess]
    res.asInstanceOf[ExecutionSuccess].result shouldBe a[QueryRows]
    res.asInstanceOf[ExecutionSuccess].result.asInstanceOf[QueryRows].rows
  }

  // -------------------------------------------------------------------------
  // Helper: Assert SHOW TABLE result type
  // -------------------------------------------------------------------------

  def assertShowTable(startTime: Long, res: ExecutionResult): Schema = {
    renderResults(startTime, res)
    res shouldBe a[ExecutionSuccess]
    res.asInstanceOf[ExecutionSuccess].result shouldBe a[TableResult]
    res.asInstanceOf[ExecutionSuccess].result.asInstanceOf[TableResult].table
  }

  // -------------------------------------------------------------------------
  // Helper: Assert SHOW PIPELINE result type
  // -------------------------------------------------------------------------

  def assertShowPipeline(startTime: Long, res: ExecutionResult): IngestPipeline = {
    renderResults(startTime, res)
    res shouldBe a[ExecutionSuccess]
    res.asInstanceOf[ExecutionSuccess].result shouldBe a[PipelineResult]
    res.asInstanceOf[ExecutionSuccess].result.asInstanceOf[PipelineResult].pipeline
  }

  // -------------------------------------------------------------------------
  // Helper: Assert SHOW CREATE result type
  // -------------------------------------------------------------------------

  def assertShowCreate(startTime: Long, res: ExecutionResult): String = {
    renderResults(startTime, res)
    res shouldBe a[ExecutionSuccess]
    res.asInstanceOf[ExecutionSuccess].result shouldBe a[SQLResult]
    res.asInstanceOf[ExecutionSuccess].result.asInstanceOf[SQLResult].sql
  }

  // -------------------------------------------------------------------------
  // Execute SQL via REPL
  // -------------------------------------------------------------------------

  def executeSync(sql: String): ExecutionResult = {
    testRepl.executeSync(sql)
  }
}
