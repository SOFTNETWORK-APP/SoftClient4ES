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

import app.softnetwork.elastic.client.result.DmlResult
import app.softnetwork.elastic.scalatest.{ElasticTestKit, MinioTestKit}

import java.time.LocalDate

/** Integration tests for `COPY INTO ... FROM 's3a://...'` backed by a MinIO container.
  *
  * Mix this trait into a concrete test class together with
  * [[app.softnetwork.elastic.scalatest.ElasticDockerTestKit]] and [[MinioTestKit]]:
  *
  * {{{
  * class JavaClientCopyIntoS3Spec
  *   extends CopyIntoS3IntegrationSpec
  *   with ElasticDockerTestKit
  *   with MinioTestKit {
  *   override lazy val client: GatewayApi = new JavaClientSpi().client(elasticConfig)
  * }
  * }}}
  *
  * The concrete module must declare `hadoop-aws` as a `% Test` dependency so that the S3A
  * filesystem implementation is available at test runtime.
  */
trait CopyIntoS3IntegrationSpec extends GatewayIntegrationTestKit {
  self: ElasticTestKit with MinioTestKit =>

  // ---------------------------------------------------------------------------
  // COPY INTO from S3 (MinIO) integration tests
  // ---------------------------------------------------------------------------

  behavior of "COPY INTO from S3"

  it should "create the target table for S3 COPY INTO tests" in {
    val create =
      """CREATE TABLE IF NOT EXISTS copy_into_s3_test (
        |  uuid     KEYWORD NOT NULL,
        |  name     VARCHAR,
        |  birthDate DATE,
        |  score    DOUBLE,
        |  PRIMARY KEY (uuid)
        |);""".stripMargin

    assertDdl(System.nanoTime(), client.run(create).futureValue)
  }

  it should "support COPY INTO from S3 with JSONL format (auto-detected)" in {
    val persons = List(
      """{"uuid": "S01", "name": "Leia Organa",    "birthDate": "1956-10-21", "score": 9.5}""",
      """{"uuid": "S02", "name": "Han Solo",        "birthDate": "1942-07-13", "score": 8.0}""",
      """{"uuid": "S03", "name": "Luke Skywalker",  "birthDate": "1951-09-25", "score": 9.0}"""
    )

    uploadToMinio(persons.mkString("\n"), "persons/data.jsonl")

    val copyInto =
      s"""COPY INTO copy_into_s3_test
         |FROM 's3a://$minioBucket/persons/data.jsonl';""".stripMargin

    assertDml(
      System.nanoTime(),
      client.run(copyInto).futureValue,
      Some(DmlResult(inserted = persons.size))
    )

    assertSelectResult(
      System.nanoTime(),
      client.run("SELECT * FROM copy_into_s3_test ORDER BY uuid ASC").futureValue,
      Seq(
        Map(
          "uuid"      -> "S01",
          "name"      -> "Leia Organa",
          "birthDate" -> LocalDate.parse("1956-10-21"),
          "score"     -> 9.5
        ),
        Map(
          "uuid"      -> "S02",
          "name"      -> "Han Solo",
          "birthDate" -> LocalDate.parse("1942-07-13"),
          "score"     -> 8.0
        ),
        Map(
          "uuid"      -> "S03",
          "name"      -> "Luke Skywalker",
          "birthDate" -> LocalDate.parse("1951-09-25"),
          "score"     -> 9.0
        )
      )
    )
  }

  it should "support COPY INTO from S3 with FILE_FORMAT = JSON_ARRAY and ON CONFLICT DO UPDATE" in {
    // Updated scores — ON CONFLICT DO UPDATE should replace the existing documents.
    val updatedPersons = List(
      """{"uuid": "S01", "name": "Leia Organa",    "birthDate": "1956-10-21", "score": 9.8}""",
      """{"uuid": "S02", "name": "Han Solo",        "birthDate": "1942-07-13", "score": 8.5}""",
      """{"uuid": "S03", "name": "Luke Skywalker",  "birthDate": "1951-09-25", "score": 9.2}"""
    )
    val jsonArray = s"[\n${updatedPersons.mkString(",\n")}\n]"

    uploadToMinio(jsonArray, "persons/data_updated.json")

    val copyInto =
      s"""COPY INTO copy_into_s3_test
         |FROM 's3a://$minioBucket/persons/data_updated.json'
         |FILE_FORMAT = JSON_ARRAY
         |ON CONFLICT (uuid) DO UPDATE;""".stripMargin

    assertDml(
      System.nanoTime(),
      client.run(copyInto).futureValue,
      Some(DmlResult(inserted = updatedPersons.size))
    )

    assertSelectResult(
      System.nanoTime(),
      client.run("SELECT * FROM copy_into_s3_test ORDER BY uuid ASC").futureValue,
      Seq(
        Map(
          "uuid"      -> "S01",
          "name"      -> "Leia Organa",
          "birthDate" -> LocalDate.parse("1956-10-21"),
          "score"     -> 9.8
        ),
        Map(
          "uuid"      -> "S02",
          "name"      -> "Han Solo",
          "birthDate" -> LocalDate.parse("1942-07-13"),
          "score"     -> 8.5
        ),
        Map(
          "uuid"      -> "S03",
          "name"      -> "Luke Skywalker",
          "birthDate" -> LocalDate.parse("1951-09-25"),
          "score"     -> 9.2
        )
      )
    )
  }
}
