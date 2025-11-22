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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticSuccess}
import app.softnetwork.elastic.model.window.Employee
import app.softnetwork.persistence.generateUUID
import org.json4s.Formats
import org.scalatest.Suite

import scala.language.implicitConversions

trait EmployeeData { _: Suite =>

  implicit val system: ActorSystem = ActorSystem(generateUUID())

  implicit def formats: Formats

  def client: ElasticClientApi

  /** Load employees
    */
  def loadEmployees(): Unit = {

    implicit val bulkOptions: BulkOptions = BulkOptions(
      defaultIndex = "emp",
      logEvery = 5
    )

    val employees = getEmployees.zipWithIndex.map { case (emp, idx) =>
      serialization.write(emp.copy(id = s"emp_${idx + 1}"))
    }.toList

    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)

    client.bulk[String](employees, identity, idKey = Some("id")) match {
      case ElasticSuccess(response) =>
        println(s"✅ Bulk indexing completed:")
        println(s"   - Total items: ${response.metrics.totalDocuments}")
        println(s"   - Successful: ${response.successCount}")
        println(s"   - Failed: ${response.failedCount}")
        println(s"   - Took: ${response.metrics.durationMs}ms")

        // Afficher les erreurs éventuelles
        val failures = response.failedDocuments
        if (failures.nonEmpty) {
          println(s"   ⚠️ ${failures.size} documents failed:")
          failures.foreach { item =>
            println(s"      - Document ${item.id}: ${item.error.message}")
          }
        }

      case ElasticFailure(error) =>
        error.cause.foreach(t => t.printStackTrace())
        fail(s"❌ Bulk indexing failed: ${error.message}")
    }
  }

  // ========================================================================
  // HELPER METHODS
  // ========================================================================

  private def getEmployees: Seq[Employee] = Seq(
    Employee(
      "Alice Johnson",
      "Engineering",
      "New York",
      95000,
      "2019-03-15",
      "Senior",
      List("Java", "Python", "Scala")
    ),
    Employee(
      "Bob Smith",
      "Engineering",
      "New York",
      120000,
      "2018-01-10",
      "Lead",
      List("Scala", "Spark", "Kafka")
    ),
    Employee(
      "Charlie Brown",
      "Engineering",
      "San Francisco",
      85000,
      "2020-06-20",
      "Mid",
      List("Python", "Django")
    ),
    Employee(
      "Diana Prince",
      "Engineering",
      "San Francisco",
      110000,
      "2017-09-05",
      "Senior",
      List("Go", "Kubernetes", "Docker")
    ),
    Employee(
      "Eve Davis",
      "Engineering",
      "New York",
      75000,
      "2021-02-12",
      "Junior",
      List("JavaScript", "React")
    ),
    Employee(
      "Frank Miller",
      "Sales",
      "New York",
      80000,
      "2019-07-22",
      "Mid",
      List("Salesforce", "CRM")
    ),
    Employee(
      "Grace Lee",
      "Sales",
      "Chicago",
      90000,
      "2018-11-30",
      "Senior",
      List("Negotiation", "B2B")
    ),
    Employee(
      "Henry Wilson",
      "Sales",
      "Chicago",
      70000,
      "2020-04-18",
      "Junior",
      List("Cold Calling")
    ),
    Employee(
      "Iris Chen",
      "Sales",
      "New York",
      95000,
      "2017-03-08",
      "Lead",
      List("Strategy", "Analytics")
    ),
    Employee(
      "Jack Taylor",
      "Marketing",
      "San Francisco",
      78000,
      "2019-10-01",
      "Mid",
      List("SEO", "Content")
    ),
    Employee(
      "Karen White",
      "Marketing",
      "San Francisco",
      88000,
      "2018-05-15",
      "Senior",
      List("Brand", "Digital")
    ),
    Employee(
      "Leo Martinez",
      "Marketing",
      "Chicago",
      65000,
      "2021-01-20",
      "Junior",
      List("Social Media")
    ),
    Employee(
      "Maria Garcia",
      "HR",
      "New York",
      72000,
      "2019-08-12",
      "Mid",
      List("Recruiting", "Onboarding")
    ),
    Employee("Nathan King", "HR", "Chicago", 68000, "2020-11-05", "Junior", List("Payroll")),
    Employee(
      "Olivia Scott",
      "HR",
      "New York",
      85000,
      "2017-12-01",
      "Senior",
      List("Policy", "Compliance")
    ),
    Employee(
      "Paul Anderson",
      "Engineering",
      "Remote",
      105000,
      "2016-04-10",
      "Senior",
      List("Rust", "Systems")
    ),
    Employee("Quinn Roberts", "Sales", "Remote", 92000, "2019-02-28", "Senior", List("Enterprise")),
    Employee(
      "Rachel Green",
      "Marketing",
      "Remote",
      81000,
      "2020-09-10",
      "Mid",
      List("Analytics", "PPC")
    ),
    Employee(
      "Sam Turner",
      "Engineering",
      "San Francisco",
      130000,
      "2015-06-01",
      "Principal",
      List("Architecture", "Leadership")
    ),
    Employee("Tina Brooks", "Sales", "Chicago", 75000, "2021-03-15", "Junior", List("B2C"))
  )
}
