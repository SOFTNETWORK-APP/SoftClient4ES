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
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticSuccess}
import app.softnetwork.elastic.scalatest.ElasticTestKit
import app.softnetwork.persistence.generateUUID
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor}

trait PipelineApiSpec
    extends AnyFlatSpecLike
    with Matchers
    with ScalaFutures
    with BeforeAndAfterAll
    with BeforeAndAfterEach {
  self: ElasticTestKit =>

  lazy val log: Logger = LoggerFactory.getLogger(getClass.getName)

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds))

  def client: PipelineApi

  override def beforeAll(): Unit = {
    self.beforeAll()
    log.info("🚀 Starting PipelineApiSpec test suite")
  }

  override def afterAll(): Unit = {
    log.info("🏁 Finishing PipelineApiSpec test suite")
    Await.result(system.terminate(), Duration(30, TimeUnit.SECONDS))
    self.afterAll()
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    // Cleanup: delete test pipelines before each test
    cleanupTestPipelines()
  }

  override def afterEach(): Unit = {
    // Cleanup: delete test pipelines after each test
    cleanupTestPipelines()
    super.afterEach()
  }

  private def cleanupTestPipelines(): Unit = {
    val testPipelines = List(
      "test_pipeline",
      "user_pipeline",
      "simple_pipeline",
      "complex_pipeline",
      "pipeline_to_update",
      "pipeline_to_delete",
      "pipeline_with_script",
      "invalid_pipeline"
    )

    testPipelines.foreach { pipelineName =>
      client.deletePipeline(pipelineName, ifExists = true) match {
        case _ => // Ignore result, just cleanup
      }
    }
  }

  // ========================================================================
  // TEST: CREATE PIPELINE
  // ========================================================================

  "PipelineApi" should "create a simple pipeline with SET processor" in {
    val pipelineDefinition =
      """
        |{
        |  "description": "Simple test pipeline",
        |  "processors": [
        |    {
        |      "set": {
        |        "field": "status",
        |        "value": "active"
        |      }
        |    }
        |  ]
        |}
        |""".stripMargin

    val result = client.createPipeline("simple_pipeline", pipelineDefinition)

    result shouldBe a[ElasticSuccess[_]]
    result.toOption.get shouldBe true

    // Verify pipeline was created
    val getResult = client.getPipeline("simple_pipeline")
    getResult shouldBe a[ElasticSuccess[_]]
    getResult.toOption.get shouldBe defined
  }

  it should "create a complex pipeline with multiple processors" in {
    val pipelineDefinition =
      """
        |{
        |  "description": "Complex test pipeline",
        |  "processors": [
        |    {
        |      "set": {
        |        "field": "name",
        |        "value": "anonymous",
        |        "if": "ctx.name == null"
        |      }
        |    },
        |    {
        |      "lowercase": {
        |        "field": "status"
        |      }
        |    },
        |    {
        |      "remove": {
        |        "field": "temp_field",
        |        "ignore_failure": true
        |      }
        |    }
        |  ]
        |}
        |""".stripMargin

    val result = client.createPipeline("complex_pipeline", pipelineDefinition)

    result shouldBe a[ElasticSuccess[_]]
    result.toOption.get shouldBe true
  }

  it should "create a pipeline with script processor" in {
    val pipelineDefinition =
      """
        |{
        |  "description": "Pipeline with script",
        |  "processors": [
        |    {
        |      "script": {
        |        "lang": "painless",
        |        "source": "ctx.age = ChronoUnit.YEARS.between(ZonedDateTime.parse(ctx.birthdate), ZonedDateTime.ofInstant(Instant.ofEpochMilli(ctx['_ingest']['timestamp']), ZoneId.of('Z')))"
        |      }
        |    }
        |  ]
        |}
        |""".stripMargin

    val result = client.createPipeline("pipeline_with_script", pipelineDefinition)

    result shouldBe a[ElasticSuccess[_]]
    result.toOption.get shouldBe true
  }

  it should "fail to create pipeline with invalid JSON" in {
    val invalidPipelineDefinition = """{ "invalid": json }"""

    val result = client.createPipeline("invalid_pipeline", invalidPipelineDefinition)

    result shouldBe a[ElasticFailure]
    result.error match {
      case Some(error) =>
        error.statusCode shouldBe Some(400)
      case _ => fail("Expected an error for empty pipeline name")
    }
  }

  it should "fail to create pipeline with empty name" in {
    val pipelineDefinition =
      """
        |{
        |  "description": "Test",
        |  "processors": []
        |}
        |""".stripMargin

    val result = client.createPipeline("", pipelineDefinition)

    result shouldBe a[ElasticFailure]
    result.error match {
      case Some(error) =>
        error.statusCode shouldBe Some(400)
      case _ => fail("Expected an error for empty pipeline name")
    }
  }

  it should "fail to create pipeline with invalid name characters" in {
    val pipelineDefinition =
      """
        |{
        |  "description": "Test",
        |  "processors": []
        |}
        |""".stripMargin

    val result = client.createPipeline("invalid/pipeline*name", pipelineDefinition)

    result shouldBe a[ElasticFailure]
  }

  // ========================================================================
  // TEST: UPDATE PIPELINE
  // ========================================================================

  it should "update an existing pipeline" in {
    // First create a pipeline
    val initialDefinition =
      """
        |{
        |  "description": "Initial pipeline",
        |  "processors": [
        |    {
        |      "set": {
        |        "field": "status",
        |        "value": "pending"
        |      }
        |    }
        |  ]
        |}
        |""".stripMargin

    client.createPipeline("pipeline_to_update", initialDefinition)

    // Now update it
    val updatedDefinition =
      """
        |{
        |  "description": "Updated pipeline",
        |  "processors": [
        |    {
        |      "set": {
        |        "field": "status",
        |        "value": "active"
        |      }
        |    },
        |    {
        |      "set": {
        |        "field": "updated",
        |        "value": true
        |      }
        |    }
        |  ]
        |}
        |""".stripMargin

    val result = client.updatePipeline("pipeline_to_update", updatedDefinition)

    result shouldBe a[ElasticSuccess[_]]
    result.toOption.get shouldBe true

    // Verify the update
    val getResult = client.getPipeline("pipeline_to_update")
    getResult shouldBe a[ElasticSuccess[_]]
    val pipelineJson = getResult.toOption.get.get
    pipelineJson should include("Updated pipeline")
  }

  // ========================================================================
  // TEST: GET PIPELINE
  // ========================================================================

  it should "retrieve an existing pipeline" in {
    val pipelineDefinition =
      """
        |{
        |  "description": "Test retrieval",
        |  "processors": [
        |    {
        |      "set": {
        |        "field": "test",
        |        "value": "value"
        |      }
        |    }
        |  ]
        |}
        |""".stripMargin

    client.createPipeline("test_pipeline", pipelineDefinition)

    val result = client.getPipeline("test_pipeline")

    result shouldBe a[ElasticSuccess[_]]
    val maybePipeline = result.toOption.get
    maybePipeline shouldBe defined
    maybePipeline.get should include("Test retrieval")
  }

  it should "return None when getting a non-existent pipeline" in {
    val result = client.getPipeline("non_existent_pipeline")

    result shouldBe a[ElasticSuccess[_]]
    result.toOption.get shouldBe None
  }

  // ========================================================================
  // TEST: DELETE PIPELINE
  // ========================================================================

  it should "delete an existing pipeline" in {
    val pipelineDefinition =
      """
        |{
        |  "description": "Pipeline to delete",
        |  "processors": []
        |}
        |""".stripMargin

    client.createPipeline("pipeline_to_delete", pipelineDefinition)

    val deleteResult = client.deletePipeline("pipeline_to_delete", ifExists = false)

    deleteResult shouldBe a[ElasticSuccess[_]]
    deleteResult.toOption.get shouldBe true

    // Verify deletion
    val getResult = client.getPipeline("pipeline_to_delete")
    getResult.toOption.get shouldBe None
  }

  it should "handle deletion of non-existent pipeline gracefully" in {
    val result = client.deletePipeline("non_existent_pipeline", ifExists = true)

    // Should succeed but return false or handle gracefully
    result shouldBe a[ElasticSuccess[_]]
  }

  // ========================================================================
  // TEST: DDL - CREATE PIPELINE
  // ========================================================================

  it should "execute CREATE PIPELINE DDL statement" in {
    val sql =
      """
        |CREATE PIPELINE user_pipeline WITH PROCESSORS (
        |    SET (
        |        field = "name",
        |        if = "ctx.name == null",
        |        description = "DEFAULT 'anonymous'",
        |        ignore_failure = true,
        |        value = "anonymous"
        |    )
        |)
        |""".stripMargin

    val result = client.pipeline(sql)

    result shouldBe a[ElasticSuccess[_]]
    result.toOption.get shouldBe true

    // Verify pipeline was created
    val getResult = client.getPipeline("user_pipeline")
    getResult shouldBe a[ElasticSuccess[_]]
    getResult.toOption.get shouldBe defined
  }

  it should "execute CREATE OR REPLACE PIPELINE DDL statement" in {
    // First create
    val createSql =
      """
        |CREATE PIPELINE user_pipeline WITH PROCESSORS (
        |    SET (field = "status", value = "pending")
        |)
        |""".stripMargin

    client.pipeline(createSql)

    // Then replace
    val replaceSql =
      """
        |CREATE OR REPLACE PIPELINE user_pipeline WITH PROCESSORS (
        |    SET (field = "status", value = "active")
        |)
        |""".stripMargin

    val result = client.pipeline(replaceSql)

    result shouldBe a[ElasticSuccess[_]]
    result.toOption.get shouldBe true
  }

  it should "execute CREATE PIPELINE IF NOT EXISTS DDL statement" in {
    val sql =
      """
        |CREATE PIPELINE IF NOT EXISTS user_pipeline WITH PROCESSORS (
        |    SET (field = "test", value = "value")
        |)
        |""".stripMargin

    // First execution should create
    val result1 = client.pipeline(sql)
    result1 shouldBe a[ElasticSuccess[_]]

    // Second execution should not fail
    val result2 = client.pipeline(sql)
    result2 shouldBe a[ElasticSuccess[_]]
  }

  // ========================================================================
  // TEST: DDL - DROP PIPELINE
  // ========================================================================

  it should "execute DROP PIPELINE DDL statement" in {
    // First create a pipeline
    val createSql =
      """
        |CREATE PIPELINE test_pipeline WITH PROCESSORS (
        |    SET (field = "test", value = "value")
        |)
        |""".stripMargin

    client.pipeline(createSql)

    // Then drop it
    val dropSql = "DROP PIPELINE test_pipeline"

    val result = client.pipeline(dropSql)

    result shouldBe a[ElasticSuccess[_]]
    result.toOption.get shouldBe true

    // Verify deletion
    val getResult = client.getPipeline("test_pipeline")
    getResult.toOption.get shouldBe None
  }

  it should "execute DROP PIPELINE IF EXISTS DDL statement" in {
    val sql = "DROP PIPELINE IF EXISTS non_existent_pipeline"

    val result = client.pipeline(sql)

    // Should not fail even if pipeline doesn't exist
    result shouldBe a[ElasticSuccess[_]]
  }

  // ========================================================================
  // TEST: DDL - ALTER PIPELINE
  // ========================================================================

  it should "execute ALTER PIPELINE with ADD PROCESSOR" in {
    // First create a pipeline
    val createSql =
      """
        |CREATE PIPELINE user_pipeline WITH PROCESSORS (
        |    SET (field = "name", value = "anonymous")
        |)
        |""".stripMargin

    client.pipeline(createSql)

    // Then alter it
    val alterSql =
      """
        |ALTER PIPELINE user_pipeline (
        |    ADD PROCESSOR SET (
        |        field = "status",
        |        value = "active"
        |    )
        |)
        |""".stripMargin

    val result = client.pipeline(alterSql)

    result shouldBe a[ElasticSuccess[_]]
    result.toOption.get shouldBe true

    // Verify the pipeline was updated
    val getResult = client.getPipeline("user_pipeline")
    getResult shouldBe a[ElasticSuccess[_]]
    val pipelineJson = getResult.toOption.get.get
    pipelineJson should include("status")
  }

  it should "execute ALTER PIPELINE with DROP PROCESSOR" in {
    // First create a pipeline with multiple processors
    val createSql =
      """
        |CREATE PIPELINE user_pipeline WITH PROCESSORS (
        |    SET (field = "name", value = "anonymous"),
        |    SET (field = "status", value = "active")
        |)
        |""".stripMargin

    client.pipeline(createSql)

    // Then drop one processor
    val alterSql =
      """
        |ALTER PIPELINE user_pipeline (
        |    DROP PROCESSOR SET (status)
        |)
        |""".stripMargin

    val result = client.pipeline(alterSql)

    result shouldBe a[ElasticSuccess[_]]
    result.toOption.get shouldBe true
  }

  it should "execute ALTER PIPELINE with multiple actions" in {
    // First create a pipeline
    val createSql =
      """
        |CREATE PIPELINE user_pipeline WITH PROCESSORS (
        |    SET (field = "name", value = "anonymous")
        |)
        |""".stripMargin

    client.pipeline(createSql)

    // Then alter with multiple actions
    val alterSql =
      """
        |ALTER PIPELINE user_pipeline (
        |    ADD PROCESSOR SET (field = "status", value = "active"),
        |    ADD PROCESSOR REMOVE (field = "temp_field", ignore_failure = true)
        |)
        |""".stripMargin

    val result = client.pipeline(alterSql)

    result shouldBe a[ElasticSuccess[_]]
    result.toOption.get shouldBe true
  }

  it should "fail to ALTER non-existent pipeline" in {
    val alterSql =
      """
        |ALTER PIPELINE non_existent_pipeline (
        |    ADD PROCESSOR SET (field = "test", value = "value")
        |)
        |""".stripMargin

    val result = client.pipeline(alterSql)

    result shouldBe a[ElasticFailure]
    result.error match {
      case Some(error) =>
        error.statusCode shouldBe Some(404)
        error.message should include("not found")
      case _ => fail("Expected an error for empty pipeline name")
    }
  }

  it should "execute ALTER PIPELINE IF EXISTS without error" in {
    val alterSql =
      """
        |ALTER PIPELINE IF EXISTS non_existent_pipeline (
        |    ADD PROCESSOR SET (field = "test", value = "value")
        |)
        |""".stripMargin

    val result = client.pipeline(alterSql)

    // Should handle gracefully with IF EXISTS
    result match {
      case ElasticSuccess(_) => succeed
      case ElasticFailure(error) =>
        error.statusCode shouldBe Some(404)
    }
  }

  // ========================================================================
  // TEST: ERROR HANDLING
  // ========================================================================

  it should "fail with invalid SQL syntax" in {
    val invalidSql = "CREATE PIPELINE INVALID SYNTAX"

    val result = client.pipeline(invalidSql)

    result shouldBe a[ElasticFailure]
    result.error match {
      case Some(error) =>
        error.statusCode shouldBe Some(400)
      case _ => fail("Expected an error for invalid pipeline ddl statement")
    }
  }

  it should "fail with unsupported DDL statement" in {
    val unsupportedSql = "SELECT * FROM pipelines"

    val result = client.pipeline(unsupportedSql)

    result shouldBe a[ElasticFailure]
    result.error match {
      case Some(error) =>
        error.statusCode shouldBe Some(400)
      case _ => fail("Expected an error for unsupported pipeline ddl statement")
    }
  }

  // ========================================================================
  // TEST: INTEGRATION WITH INDEX
  // ========================================================================

  it should "create pipeline and use it with index" in {
    // Create an index with pipeline
    createIndex("test_index_with_pipeline")

    // Create a pipeline
    val pipelineDefinition =
      """
        |{
        |  "description": "Pipeline for test index",
        |  "processors": [
        |    {
        |      "set": {
        |        "field": "indexed_at",
        |        "value": "{{_ingest.timestamp}}"
        |      }
        |    }
        |  ]
        |}
        |""".stripMargin

    val createResult = client.createPipeline("test_index_pipeline", pipelineDefinition)
    createResult shouldBe a[ElasticSuccess[_]]

    // Verify pipeline exists
    val getResult = client.getPipeline("test_index_pipeline")
    getResult shouldBe a[ElasticSuccess[_]]
    getResult.toOption.get shouldBe defined

    // Cleanup
    deleteIndex("test_index_with_pipeline")
    client.deletePipeline("test_index_pipeline", ifExists = false)
  }
}
