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
import app.softnetwork.elastic.client.bulk.BulkOptions
import app.softnetwork.elastic.client.file._
import app.softnetwork.elastic.scalatest.ElasticTestKit
import app.softnetwork.persistence.generateUUID
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.scalatest.time.{Seconds, Span}
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.TimeUnit
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, ExecutionContextExecutor}

trait BulkApiSpec extends AnyFlatSpecLike with Matchers with ScalaFutures {
  self: ElasticTestKit =>

  lazy val log: Logger = LoggerFactory getLogger getClass.getName

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  implicit val patience: PatienceConfig = PatienceConfig(timeout = Span(10, Seconds))

  def client: BulkApi

  override def beforeAll(): Unit = {
    self.beforeAll()
    createIndex("person")
  }

  override def afterAll(): Unit = {
    Await.result(system.terminate(), Duration(30, TimeUnit.SECONDS))
    self.afterAll()
  }

  private val persons: List[String] = List(
    """ { "uuid": "A12", "name": "Homer Simpson", "birthDate": "1967-11-21", "childrenCount": 0} """,
    """ { "uuid": "A14", "name": "Moe Szyslak",   "birthDate": "1967-11-21", "childrenCount": 0} """,
    """ { "uuid": "A16", "name": "Barney Gumble2", "birthDate": "1969-05-09", "children": [{ "parentId": "A16", "name": "Steve Gumble", "birthDate": "1999-05-09"}, { "parentId": "A16", "name": "Josh Gumble", "birthDate": "2002-05-09"}], "childrenCount": 2 } """
  )

  "BulkClient" should "handle Json file format" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person")

    val tempFile = java.io.File.createTempFile("persons", ".jsonl")
    tempFile.deleteOnExit()

    val writer = new java.io.PrintWriter(tempFile)
    persons.foreach(writer.println)
    writer.close()

    val response =
      client
        .bulkFromFile(
          tempFile.getAbsolutePath,
          idKey = Some(Set("uuid"))
        )
        .futureValue

    log.info(s"Bulk index response: $response")
    response.failedCount shouldBe 0
    response.successCount shouldBe persons.size
    response.indices should contain("person")
  }

  it should "handle multiple elements JSON array" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person")

    val tempFile = java.io.File.createTempFile("array_persons", ".jsonl")
    tempFile.deleteOnExit()

    val writer = new java.io.PrintWriter(tempFile)
    writer.println("[")
    writer.println(persons.mkString(",\n"))
    writer.println("]")
    writer.close()

    val response =
      client
        .bulkFromFile(
          tempFile.getAbsolutePath,
          idKey = Some(Set("uuid")),
          format = JsonArray
        )
        .futureValue

    log.info(s"Bulk index response: ${response}")
    response.failedCount shouldBe 0
    response.successCount shouldBe persons.size
    response.indices should contain("person")
  }

  it should "handle empty JSON array" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person")

    val tempFile = createTempJsonArrayFile("empty_array", List.empty)

    val response = client
      .bulkFromFile(
        tempFile.getAbsolutePath,
        idKey = Some(Set("uuid")),
        format = JsonArray
      )
      .futureValue

    response.successCount shouldBe 0
    response.failedCount shouldBe 0
    response.totalCount shouldBe 0
  }

  it should "handle single element JSON array" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person")

    val singlePerson = List(
      """{"uuid": "A99", "name": "Single Person", "birthDate": "1990-01-01", "childrenCount": 0}"""
    )

    val tempFile = createTempJsonArrayFile("single_person", singlePerson)

    val response = client
      .bulkFromFile(
        tempFile.getAbsolutePath,
        idKey = Some(Set("uuid")),
        format = JsonArray
      )
      .futureValue

    response.successCount shouldBe 1
    response.failedCount shouldBe 0
  }

  it should "handle JSON array with nested objects" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person")

    val personsWithChildren = List(
      """{"uuid": "A16", "name": "Barney Gumble", "birthDate": "1969-05-09", "children": [{"name": "Steve", "birthDate": "1999-05-09"}, {"name": "Josh", "birthDate": "2002-05-09"}], "childrenCount": 2}"""
    )

    val tempFile = createTempJsonArrayFile("persons_with_children", personsWithChildren)

    val response = client
      .bulkFromFile(
        tempFile.getAbsolutePath,
        idKey = Some(Set("uuid")),
        format = JsonArray
      )
      .futureValue

    response.successCount shouldBe 1
    response.failedCount shouldBe 0

    /*// Verify the document was indexed with nested structure
    Thread.sleep(1000) // Wait for indexing

    val searchResult =
      client.search("person", """{"query": {"term": {"uuid": "A16"}}}""").futureValue
    searchResult.hits.total.value shouldBe 1

    val doc = searchResult.hits.hits.head.source
    doc should include("children")
    doc should include("Steve")
    doc should include("Josh")*/
  }

  it should "handle malformed JSON array gracefully" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person")

    val tempFile = java.io.File.createTempFile("malformed_array", ".json")
    tempFile.deleteOnExit()

    val writer = new java.io.PrintWriter(tempFile)
    try {
      writer.println("[{invalid json}]")
    } finally {
      writer.close()
    }

    val result = client
      .bulkFromFile(
        tempFile.getAbsolutePath,
        idKey = Some(Set("uuid")),
        format = JsonArray
      )
      .futureValue

    result.successCount shouldBe 0
    result.failedCount shouldBe 0
    result.totalCount shouldBe 0
  }

  it should "handle large JSON array efficiently" in {
    implicit val bulkOptions: BulkOptions = BulkOptions("person")

    // Generate 1000 persons
    val largePersonsList = (1 to 1000).map { i =>
      s"""{"uuid": "P$i", "name": "Person $i", "birthDate": "1990-01-01", "childrenCount": 0}"""
    }.toList

    val tempFile = createTempJsonArrayFile("large_array", largePersonsList)

    val startTime = System.currentTimeMillis()

    val response = client
      .bulkFromFile(
        tempFile.getAbsolutePath,
        idKey = Some(Set("uuid")),
        format = JsonArray
      )
      .futureValue

    val duration = System.currentTimeMillis() - startTime

    log.info(s"✅ Indexed ${response.successCount} documents in ${duration}ms")

    response.successCount shouldBe 1000
    response.failedCount shouldBe 0

    // Performance check (adjust threshold as needed)
    duration should be < 10000L // Should complete in less than 10 seconds
  }

  def createTempJsonArrayFile(
    prefix: String,
    jsonStrings: List[String]
  ): java.io.File = {
    val tempFile = java.io.File.createTempFile(prefix, ".json")
    tempFile.deleteOnExit()
    val writer = new java.io.PrintWriter(tempFile)
    writer.println("[")
    writer.println(jsonStrings.mkString(",\n"))
    writer.println("]")
    writer.close()
    tempFile
  }
}
