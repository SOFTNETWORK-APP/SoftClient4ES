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

import akka.stream.scaladsl.Sink
import app.softnetwork.elastic.client.result.{ElasticFailure, ElasticSuccess}
import app.softnetwork.elastic.client.scroll.ScrollConfig
import app.softnetwork.elastic.client.spi.ElasticClientFactory
import app.softnetwork.elastic.model.window._
import app.softnetwork.elastic.scalatest.ElasticDockerTestKit
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.time.LocalDate
import scala.concurrent.Await
import scala.concurrent.duration._

trait WindowFunctionSpec
    extends AnyFlatSpecLike
    with ElasticDockerTestKit
    with Matchers
    with EmployeeData {

  lazy val log: Logger = LoggerFactory getLogger getClass.getName

  override def client: ElasticClientApi = ElasticClientFactory.create(elasticConfig)

  override def beforeAll(): Unit = {
    super.beforeAll()

    val mapping =
      """{
        |  "properties": {
        |    "name": {
        |      "type": "text",
        |      "fields": {
        |        "keyword": {
        |          "type": "keyword"
        |        }
        |      }
        |    },
        |    "department": {
        |      "type": "keyword"
        |    },
        |    "location": {
        |      "type": "keyword"
        |    },
        |    "salary": {
        |      "type": "integer"
        |    },
        |    "hire_date": {
        |      "type": "date",
        |      "format": "yyyy-MM-dd"
        |    },
        |    "level": {
        |      "type": "keyword"
        |    },
        |    "skills": {
        |      "type": "keyword"
        |    }
        |  }
        |}""".stripMargin

    client.createIndex("emp").get shouldBe true

    client.setMapping("emp", mapping).get shouldBe true

    loadEmployees()
  }

  override def afterAll(): Unit = {
    client.deleteIndex("emp")
    // system.terminate()
    super.afterAll()
  }

  "Index mapping" should "have correct field types" in {
    client.getMapping("emp") match {
      case ElasticSuccess(mapping) =>
        log.info(s"📋 Mapping: $mapping")

        mapping should include("hire_date")
        mapping should include("\"type\":\"date\"")
        mapping should include("\"format\":\"yyyy-MM-dd\"")

      case ElasticFailure(error) => fail(s"Failed to get mapping: ${error.message}")
    }
  }

  "Sample document" should "have hire_date as string" in {
    val results = client.searchAs[Employee]("""
      SELECT
        name,
        department,
        location,
        salary,
        hire_date,
        level,
        skills,
        id
      FROM emp
      WHERE name.keyword = 'Sam Turner'
    """)

    results match {
      case ElasticSuccess(employees) =>
        employees should have size 1
        val sam = employees.head

        sam.name shouldBe "Sam Turner"
        sam.hire_date shouldBe "2015-06-01"

        log.info(s"✅ Sam Turner hire_date: ${sam.hire_date}")

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  // ========================================================================
  // BASIC WINDOW FUNCTION TESTS
  // ========================================================================

  "FIRST_VALUE window function" should "return first salary per department" in {
    val results = client.searchAs[EmployeeWithWindow](
      """
     SELECT
          department,
          name,
          salary,
          hire_date,
          location,
          level,
          FIRST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS first_salary
        FROM emp
        ORDER BY department, hire_date
     """
    )

    results match {
      case ElasticSuccess(employees) =>
        employees should not be empty

        // Engineering: first hire = Sam Turner (2015-06-01, $130k)
        val engineering = employees.filter(_.department == "Engineering")
        engineering.foreach { emp =>
          emp.first_salary shouldBe Some(130000)
        }

        // Sales: first hire = Iris Chen (2017-03-08, $95k)
        val sales = employees.filter(_.department == "Sales")
        sales.foreach { emp =>
          emp.first_salary shouldBe Some(95000)
        }

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "LAST_VALUE window function" should "return last salary per department" in {
    val results = client.searchAs[EmployeeWithWindow]("""
        SELECT
          department,
          name,
          salary,
          hire_date,
          location,
          level,
          LAST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS last_salary
        FROM emp
        ORDER BY department, hire_date
      """)

    results match {
      case ElasticSuccess(employees) =>
        employees should not be empty

        // Engineering: last hire = Eve Davis (2021-02-12, $75k)
        val engineering = employees.filter(_.department == "Engineering")
        engineering.foreach { emp =>
          emp.last_salary shouldBe Some(75000)
        }

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  /*"ROW_NUMBER window function" should "assign sequential numbers per partition" in {
      val results = client.searchAs[EmployeeWithWindow]("""
        SELECT
          department,
          name,
          salary,
          hire_date,
          ROW_NUMBER() OVER (
            PARTITION BY department
            ORDER BY salary DESC
          ) AS row_number
        FROM emp
        ORDER BY department, row_number
      """)

      results match {
        case ElasticSuccess(employees) =>
          employees.groupBy(_.department).foreach { case (dept, emps) =>
            val rowNumbers = emps.flatMap(_.row_number).sorted
            rowNumbers shouldBe (1 to emps.size).toList

            info(s"$dept: ${emps.size} employees numbered 1 to ${emps.size}")
          }

        case ElasticFailure(error) =>
          fail(s"Query failed: ${error.message}")
      }
    }

    "RANK window function" should "handle ties correctly" in {
      val results = client.searchAs[EmployeeWithWindow]("""
        SELECT
          department,
          name,
          salary,
          hire_date,
          RANK() OVER (
            PARTITION BY department
            ORDER BY salary DESC
          ) AS rank
        FROM emp
        ORDER BY department, rank
      """)

      results match {
        case ElasticSuccess(employees) =>
          employees.groupBy(_.department).foreach { case (dept, emps) =>
            val ranks = emps.flatMap(_.rank)
            ranks.head shouldBe 1 // Top earner always rank 1

            info(s"$dept top earner: ${emps.head.name} (${emps.head.salary})")
          }

        case ElasticFailure(error) =>
          fail(s"Query failed: ${error.message}")
      }
    }*/

  // ========================================================================
  // TESTS WITH FILTERS
  // ========================================================================

  "Window function with WHERE clause" should "apply filters before computation" in {
    val results = client.searchAs[EmployeeWithWindow]("""
        SELECT
          department,
          name,
          salary,
          hire_date,
          FIRST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS first_salary
        FROM emp
        WHERE salary > 80000
        ORDER BY department, hire_date
      """)

    results match {
      case ElasticSuccess(employees) =>
        employees.foreach { emp =>
          emp.salary should be > 80000
        }

        // Engineering avec filtre: first = Paul Anderson (2016-04-10, $105k)
        val engineering = employees.filter(_.department == "Engineering")
        engineering should not be empty
        engineering.foreach { emp =>
          emp.first_salary shouldBe Some(130000)
        }

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "Window function with department filter" should "compute only for filtered data" in {
    val results = client.searchAs[EmployeeWithWindow]("""
        SELECT
          department,
          name,
          salary,
          hire_date,
          location,
          FIRST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS first_salary
        FROM emp
        WHERE department IN ('Engineering', 'Sales')
        ORDER BY department, hire_date
      """)

    results match {
      case ElasticSuccess(employees) =>
        employees.foreach { emp =>
          emp.department should (be("Engineering") or be("Sales"))
          emp.first_salary shouldBe defined
        }

        val departments = employees.map(_.department).distinct
        departments should contain only ("Engineering", "Sales")

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  // ========================================================================
  // TESTS WITH GLOBAL WINDOW
  // ========================================================================

  "Global window function" should "use same value for all rows" in {
    val results = client.searchAs[EmployeeWithGlobalWindow]("""
        SELECT
          name,
          salary,
          hire_date,
          FIRST_VALUE(salary) OVER (ORDER BY hire_date ASC) AS first_ever_salary,
          LAST_VALUE(salary) OVER (
            ORDER BY hire_date ASC
          ) AS last_ever_salary
        FROM emp
        ORDER BY hire_date
        LIMIT 20
      """)

    results match {
      case ElasticSuccess(employees) =>
        employees should have size 20

        // Premier embauché: Sam Turner (2015-06-01, $130k)
        employees.foreach { emp =>
          emp.first_ever_salary shouldBe Some(130000)
        }

        // Dernier embauché: Tina Brooks (2021-03-15, $75k)
        employees.foreach { emp =>
          emp.last_ever_salary shouldBe Some(75000)
        }

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  // ========================================================================
  // TESTS WITH MULTIPLE PARTITIONS
  // ========================================================================

  "Multiple partition keys" should "compute independently" in {
    val results = client.searchAs[EmployeeMultiPartition]("""
        SELECT
          department,
          location,
          name,
          salary,
          hire_date,
          FIRST_VALUE(salary) OVER (
            PARTITION BY department, location
            ORDER BY hire_date ASC
          ) AS first_in_dept_loc
        FROM emp
        WHERE department IN ('Engineering', 'Sales')
        ORDER BY department, location, hire_date
      """)

    results match {
      case ElasticSuccess(employees) =>
        employees
          .groupBy(e => (e.department, e.location))
          .foreach { case ((dept, loc), emps) =>
            val firstValues = emps.flatMap(_.first_in_dept_loc).distinct
            firstValues should have size 1

            info(s"$dept @ $loc: first_salary = ${firstValues.head}")
          }

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  // ========================================================================
  // TESTS WITH LIMIT
  // ========================================================================

  "Window function with LIMIT" should "return correct number of results" in {
    val results = client.searchAs[EmployeeWithWindow]("""
        SELECT
          department,
          name,
          salary,
          hire_date,
          FIRST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS first_salary
        FROM emp
        ORDER BY salary DESC
        LIMIT 10
      """)

    results match {
      case ElasticSuccess(employees) =>
        employees should have size 10

        // Top salary: Sam Turner ($130k)
        employees.head.name shouldBe "Sam Turner"
        employees.head.salary shouldBe 130000

        // Tous les salaires >= $80k
        employees.foreach { emp =>
          emp.salary should be >= 80000
        }

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  // ========================================================================
  // TESTS WITH AGGREGATIONS
  // ========================================================================

  "Window function with aggregations" should "combine GROUP BY and OVER" in {
    val results = client.searchAs[DepartmentStats]("""
        SELECT
          department,
          AVG(salary) AS avg_salary,
          MAX(salary) AS max_salary,
          MIN(salary) AS min_salary,
          COUNT(*) AS employee_count
        FROM emp
        GROUP BY department
      """)

    results match {
      case ElasticSuccess(departments) =>
        departments should not be empty

        val engineering = departments.find(_.department == "Engineering")
        engineering shouldBe defined
        engineering.get.max_salary shouldBe 130000 // Sam Turner
        engineering.get.min_salary shouldBe 75000 // Eve Davis
        engineering.get.employee_count shouldBe 7

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  // ========================================================================
  // SCROLL TESTS
  // ========================================================================

  "Scroll with FIRST_VALUE" should "stream all employees with window enrichment" in {
    val config = ScrollConfig(scrollSize = 5)

    val futureResults = client
      .scrollAs[EmployeeWithWindow](
        """SELECT
            department,
            name,
            salary,
            hire_date,
            location,
            level,
            FIRST_VALUE(salary) OVER (
              PARTITION BY department
              ORDER BY hire_date ASC
            ) AS first_salary
          FROM emp
          ORDER BY department, hire_date
          """,
        config
      )
      .runWith(Sink.seq)

    val results = Await.result(futureResults, 30.seconds)

    results should have size 20

    // Vérifier la cohérence par département
    results.map(_._1).groupBy(_.department).foreach { case (dept, emps) =>
      val firstSalaries = emps.flatMap(_.first_salary).distinct
      firstSalaries should have size 1

      info(s"$dept: first_salary = ${firstSalaries.head}")
    }
  }

  "Scroll with multiple window functions" should "enrich with multiple columns" in {
    val config = ScrollConfig(scrollSize = 3, logEvery = 5)

    val futureResults = client
      .scrollAs[EmployeeWithWindow](
        """
          SELECT
            department,
            name,
            salary,
            hire_date,
            location,
            level,
            FIRST_VALUE(salary) OVER (
              PARTITION BY department
              ORDER BY hire_date ASC
            ) AS first_salary,
            LAST_VALUE(salary) OVER (
              PARTITION BY department
              ORDER BY hire_date ASC
            ) AS last_salary
          FROM emp
          ORDER BY department, hire_date
          """,
        config
      )
      .runWith(Sink.seq)

    val results = Await.result(futureResults, 30.seconds)

    results should have size 20

    results.foreach { case (emp, _) =>
      emp.first_salary shouldBe defined
      emp.last_salary shouldBe defined
    }
  }

  // ========================================================================
  // TESTS DE PERFORMANCE
  // ========================================================================

  "Window functions performance" should "maintain good throughput" in {
    val config = ScrollConfig(scrollSize = 5, logEvery = 10)

    val startTime = System.currentTimeMillis()

    val futureResults = client
      .scrollAs[EmployeeWithWindow](
        """
        SELECT
          department,
          name,
          salary,
          hire_date,
          FIRST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS first_salary,
          LAST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS last_salary
        FROM emp
        """,
        config
      )
      .runWith(Sink.seq)

    val results = Await.result(futureResults, 30.seconds)
    val duration = System.currentTimeMillis() - startTime

    results should have size 20
    duration should be < 5000L

    info(s"Scrolled ${results.size} documents with 3 window functions in ${duration}ms")
  }

  // ========================================================================
  // TEST WITH MINIMAL CASE CLASS
  // ========================================================================

  "Minimal case class" should "work with partial SELECT" in {
    val results = client.searchAs[EmployeeMinimal]("""
        SELECT
          name,
          department,
          salary
        FROM emp
        WHERE salary > 100000
        ORDER BY salary DESC
      """)

    results match {
      case ElasticSuccess(employees) =>
        employees should not be empty
        employees.foreach { emp =>
          emp.salary should be > 100000
        }

        // Top 3: Sam Turner, Bob Smith, Diana Prince
        employees.take(3).map(_.name) should contain allOf (
          "Sam Turner", "Bob Smith", "Diana Prince"
        )

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  // ========================================================================
  // SCROLL: FIRST_VALUE
  // ========================================================================

  "Scroll with FIRST_VALUE" should "stream all employees with first salary per department" in {
    val config = ScrollConfig(scrollSize = 5, logEvery = 10)

    val futureResults = client
      .scrollAs[EmployeeWithWindow](
        """
        SELECT
          department,
          name,
          salary,
          hire_date,
          location,
          level,
          FIRST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS first_salary
        FROM emp
        ORDER BY department, hire_date
        """,
        config
      )
      .runWith(Sink.seq)

    val results = Await.result(futureResults, 30.seconds)

    results should have size 20

    // Vérifier la cohérence par département
    val employees = results.map(_._1)

    val engineering = employees.filter(_.department == "Engineering")
    engineering.foreach { emp =>
      emp.first_salary shouldBe Some(130000) // Sam Turner (2015-06-01)
    }

    val sales = employees.filter(_.department == "Sales")
    sales.foreach { emp =>
      emp.first_salary shouldBe Some(95000) // Iris Chen (2017-03-08)
    }

    val marketing = employees.filter(_.department == "Marketing")
    marketing.foreach { emp =>
      emp.first_salary shouldBe Some(88000) // Karen White (2018-05-15)
    }

    info(s"✅ Scrolled ${results.size} employees with FIRST_VALUE")
  }

  "Scroll with FIRST_VALUE and small batches" should "handle pagination correctly" in {
    val config = ScrollConfig(scrollSize = 2, logEvery = 5)

    val futureResults = client
      .scrollAs[EmployeeWithWindow](
        """
        SELECT
          department,
          name,
          salary,
          hire_date,
          FIRST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS first_salary
        FROM emp
        WHERE department = 'Engineering'
        ORDER BY hire_date
        """,
        config
      )
      .runWith(Sink.seq)

    val results = Await.result(futureResults, 30.seconds)

    results should have size 7 // 7 engineers

    val employees = results.map(_._1)
    employees.foreach { emp =>
      emp.department shouldBe "Engineering"
      emp.first_salary shouldBe Some(130000)
    }

    // Vérifier l'ordre chronologique
    val hireDates = employees.map(_.hire_date)
    hireDates shouldBe hireDates.sorted

    info(s"✅ Scrolled ${results.size} engineers in batches of 2")
  }

  // ========================================================================
  // SCROLL: LAST_VALUE
  // ========================================================================

  "Scroll with LAST_VALUE" should "stream all employees with last salary per department" in {
    val config = ScrollConfig(scrollSize = 4, logEvery = 8)

    val futureResults = client
      .scrollAs[EmployeeWithWindow](
        """
        SELECT
          department,
          name,
          salary,
          hire_date,
          location,
          LAST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS last_salary
        FROM emp
        ORDER BY department, hire_date
        """,
        config
      )
      .runWith(Sink.seq)

    val results = Await.result(futureResults, 30.seconds)

    results should have size 20

    val employees = results.map(_._1)

    val engineering = employees.filter(_.department == "Engineering")
    engineering.foreach { emp =>
      emp.last_salary shouldBe Some(75000) // Eve Davis (2021-02-12)
    }

    val sales = employees.filter(_.department == "Sales")
    sales.foreach { emp =>
      emp.last_salary shouldBe Some(75000) // Tina Brooks (2021-03-15)
    }

    info(s"✅ Scrolled ${results.size} employees with LAST_VALUE")
  }

  "Scroll with LAST_VALUE and filter" should "apply WHERE before window computation" in {
    val config = ScrollConfig(scrollSize = 3)

    val futureResults = client
      .scrollAs[EmployeeWithWindow](
        """
        SELECT
          department,
          name,
          salary,
          hire_date,
          LAST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS last_salary
        FROM emp
        WHERE salary > 80000
        ORDER BY department, hire_date
        """,
        config
      )
      .runWith(Sink.seq)

    val results = Await.result(futureResults, 30.seconds)

    val employees = results.map(_._1)

    employees.foreach { emp =>
      emp.salary should be > 80000
    }

    // Engineering avec filtre: last = Diana Prince (2017-09-05, $110k)
    val engineering = employees.filter(_.department == "Engineering")
    engineering.foreach { emp =>
      emp.last_salary shouldBe Some(85000)
    }

    info(s"✅ Scrolled ${employees.size} high-salary employees with LAST_VALUE")
  }

  // ========================================================================
  // SCROLL: ROW_NUMBER
  // ========================================================================

  /*"Scroll with ROW_NUMBER" should "assign sequential numbers per partition" in {
      val config = ScrollConfig(scrollSize = 5)

      val futureResults = client
        .scrollAs[EmployeeWithWindow](
          """
        SELECT
          department,
          name,
          salary,
          hire_date,
          ROW_NUMBER() OVER (
            PARTITION BY department
            ORDER BY salary DESC
          ) AS row_number
        FROM emp
        ORDER BY department, row_number
        """,
          config
        )
        .runWith(Sink.seq)

      val results = Await.result(futureResults, 30.seconds)

      results should have size 20

      val employees = results.map(_._1)

      employees.groupBy(_.department).foreach { case (dept, emps) =>
        val rowNumbers = emps.flatMap(_.row_number).sorted
        rowNumbers shouldBe (1 to emps.size).toList

        // Top earner (row_number = 1)
        val topEarner = emps.find(_.row_number.contains(1)).get

        dept match {
          case "Engineering" => topEarner.name shouldBe "Sam Turner"
          case "Sales"       => topEarner.name shouldBe "Iris Chen"
          case "Marketing"   => topEarner.name shouldBe "Karen White"
          case "HR"          => topEarner.name shouldBe "Olivia Scott"
          case _             => // OK
        }

        info(s"$dept: ${emps.size} employees, top earner = ${topEarner.name}")
      }
    }

    "Scroll with ROW_NUMBER and LIMIT simulation" should "get top N per department" in {
      val config = ScrollConfig(scrollSize = 10)

      val futureResults = client
        .scrollAs[EmployeeWithWindow](
          """
        SELECT
          department,
          name,
          salary,
          hire_date,
          ROW_NUMBER() OVER (
            PARTITION BY department
            ORDER BY salary DESC
          ) AS row_number
        FROM emp
        ORDER BY department, row_number
        """,
          config
        )
        .runWith(Sink.seq)

      val results = Await.result(futureResults, 30.seconds)

      val employees = results.map(_._1)

      // Filtrer les top 2 par département
      val top2PerDept = employees
        .filter(_.row_number.exists(_ <= 2))
        .groupBy(_.department)

      top2PerDept.foreach { case (dept, emps) =>
        emps should have size 2
        info(s"$dept top 2: ${emps.map(e => s"${e.name} ($${e.salary})").mkString(", ")}")
      }
    }

    // ========================================================================
    // SCROLL: RANK
    // ========================================================================

    "Scroll with RANK" should "handle ties correctly" in {
      val config = ScrollConfig(scrollSize = 5)

      val futureResults = client
        .scrollAs[EmployeeWithWindow](
          """
        SELECT
          department,
          name,
          salary,
          hire_date,
          RANK() OVER (
            PARTITION BY department
            ORDER BY salary DESC
          ) AS rank
        FROM emp
        ORDER BY department, rank
        """,
          config
        )
        .runWith(Sink.seq)

      val results = Await.result(futureResults, 30.seconds)

      results should have size 20

      val employees = results.map(_._1)

      employees.groupBy(_.department).foreach { case (dept, emps) =>
        val ranks = emps.flatMap(_.rank)
        ranks.head shouldBe 1 // Top earner always rank 1

        val topEarner = emps.head
        info(s"$dept rank 1: ${topEarner.name} ($${topEarner.salary})")
      }
    }*/

  // ========================================================================
  // SCROLL: GLOBAL WINDOW
  // ========================================================================

  "Scroll with global window" should "use same value for all rows" in {
    val config = ScrollConfig(scrollSize = 7)

    val futureResults = client
      .scrollAs[EmployeeWithGlobalWindow](
        """
        SELECT
          name,
          salary,
          hire_date,
          FIRST_VALUE(salary) OVER (ORDER BY hire_date ASC) AS first_ever_salary,
          LAST_VALUE(salary) OVER (
            ORDER BY hire_date ASC
          ) AS last_ever_salary
        FROM emp
        ORDER BY hire_date
        """,
        config
      )
      .runWith(Sink.seq)

    val results = Await.result(futureResults, 30.seconds)

    results should have size 20

    val employees = results.map(_._1)

    // Premier embauché: Sam Turner (2015-06-01, $130k)
    employees.foreach { emp =>
      emp.first_ever_salary shouldBe Some(130000)
    }

    // Dernier embauché: Tina Brooks (2021-03-15, $75k)
    employees.foreach { emp =>
      emp.last_ever_salary shouldBe Some(75000)
    }

    // Vérifier l'ordre chronologique
    val hireDates = employees.map(_.hire_date)
    hireDates shouldBe hireDates.sorted

    info(s"✅ Global window: first = $$130k, last = $$75k")
  }

  // ========================================================================
  // SCROLL: MULTIPLE PARTITIONS
  // ========================================================================

  "Scroll with multiple partition keys" should "compute independently" in {
    val config = ScrollConfig(scrollSize = 4)

    val futureResults = client
      .scrollAs[EmployeeMultiPartition](
        """
        SELECT
          department,
          location,
          name,
          salary,
          hire_date,
          FIRST_VALUE(salary) OVER (
            PARTITION BY department, location
            ORDER BY hire_date ASC
          ) AS first_in_dept_loc
        FROM emp
        WHERE department IN ('Engineering', 'Sales')
        ORDER BY department, location, hire_date
        """,
        config
      )
      .runWith(Sink.seq)

    val results = Await.result(futureResults, 30.seconds)

    val employees = results.map(_._1)

    employees
      .groupBy(e => (e.department, e.location))
      .foreach { case ((dept, loc), emps) =>
        val firstValues = emps.flatMap(_.first_in_dept_loc).distinct
        firstValues should have size 1

        info(s"$dept @ $loc: first_salary = ${firstValues.head}, ${emps.size} employees")
      }

    // Engineering @ New York: Alice (2019-03-15, $95k) ou Bob (2018-01-10, $120k)
    val engNY = employees.filter(e => e.department == "Engineering" && e.location == "New York")
    engNY.foreach { emp =>
      emp.first_in_dept_loc shouldBe Some(120000) // Bob Smith
    }
  }

  // ========================================================================
  // SCROLL WITH COMPLEX FILTERS
  // ========================================================================

  "Scroll with complex WHERE clause" should "apply all filters before window" in {
    val config = ScrollConfig(scrollSize = 5)

    val futureResults = client
      .scrollAs[EmployeeWithWindow](
        """
        SELECT
          department,
          name,
          salary,
          hire_date,
          location,
          level,
          FIRST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS first_salary
        FROM emp
        WHERE salary > 80000
          AND hire_date >= '2018-01-01'
          AND department IN ('Engineering', 'Sales')
        ORDER BY department, hire_date
        """,
        config
      )
      .runWith(Sink.seq)

    val results = Await.result(futureResults, 30.seconds)

    val employees = results.map(_._1)

    employees.foreach { emp =>
      emp.salary should be > 80000
      emp.hire_date should be >= LocalDate.of(2018, 1, 1)
      emp.department should (be("Engineering") or be("Sales"))
    }

    info(s"✅ Filtered scroll: ${employees.size} employees matching criteria")
  }

  // ========================================================================
  // SCROLL: PERFORMANCE AND MONITORING
  // ========================================================================

  "Scroll with performance monitoring" should "track progress and timing" in {
    val config = ScrollConfig(
      scrollSize = 5,
      logEvery = 5
    )

    val startTime = System.currentTimeMillis()

    val futureResults = client
      .scrollAs[EmployeeWithWindow](
        """
        SELECT
          department,
          name,
          salary,
          hire_date,
          FIRST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS first_salary,
          LAST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS last_salary
        FROM emp
        """,
        config
      )
      .runWith(Sink.seq)

    val results = Await.result(futureResults, 30.seconds)
    val duration = System.currentTimeMillis() - startTime

    results should have size 20
    duration should be < 5000L

    val employees = results.map(_._1)

    // Vérifier que toutes les colonnes window sont présentes
    employees.foreach { emp =>
      emp.first_salary shouldBe defined
      emp.last_salary shouldBe defined
    }

    info(s"✅ Scrolled ${results.size} docs with 4 window functions in ${duration}ms")
    info(s"   Throughput: ${results.size * 1000 / duration} docs/sec")
  }

  // ========================================================================
  // SCROLL: STREAMING WITH TRANSFORMATION
  // ========================================================================

  "Scroll with stream transformation" should "process results on-the-fly" in {
    val config = ScrollConfig(scrollSize = 3)

    val futureResults = client
      .scrollAs[EmployeeWithWindow](
        """
        SELECT
          department,
          name,
          salary,
          hire_date,
          FIRST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS first_salary
        FROM emp
        ORDER BY department, salary DESC
        """,
        config
      )
      .map { case (emp, scrollId) =>
        // Transformation: calculer le % vs premier salaire
        val pctVsFirst = emp.first_salary.map { first =>
          (emp.salary.toDouble / first * 100).round.toInt
        }

        (emp, pctVsFirst, scrollId)
      }
      .filter { case (emp, pct, _) =>
        // Ne garder que les top earners (row_number <= 3)
        emp.row_number.exists(_ <= 3)
      }
      .runWith(Sink.seq)

    val results = Await.result(futureResults, 30.seconds)

    results.foreach { case (emp, pctVsFirst, _) =>
      info(
        s"${emp.department} - ${emp.name}: $${emp.salary} (${pctVsFirst.getOrElse("N/A")}% vs first)"
      )
    }

    // Chaque département devrait avoir max 3 employés
    val countPerDept = results.map(_._1).groupBy(_.department).mapValues(_.size)
    countPerDept.values.foreach { count =>
      count should be <= 3
    }
  }

  // ========================================================================
  // SCROLL WITH DOWNSTREAM AGGREGATION
  // ========================================================================

  "Scroll with downstream aggregation" should "compute stats from stream" in {
    val config = ScrollConfig(scrollSize = 4)

    val futureStats = client
      .scrollAs[EmployeeWithWindow](
        """
        SELECT
          department,
          name,
          salary,
          hire_date,
          FIRST_VALUE(salary) OVER (
            PARTITION BY department
            ORDER BY hire_date ASC
          ) AS first_salary
        FROM emp
        """,
        config
      )
      .map(_._1)
      .runFold(Map.empty[String, (Int, Int, Int)]) { case (acc, emp) =>
        // Calculer min/max/count par département
        val (min, max, count) = acc.getOrElse(emp.department, (Int.MaxValue, 0, 0))

        acc + (emp.department -> (
          math.min(min, emp.salary),
          math.max(max, emp.salary),
          count + 1
        ))
      }

    val stats = Await.result(futureStats, 30.seconds)

    stats should not be empty

    stats.foreach { case (dept, (min, max, count)) =>
      info(s"$dept: $count employees, salary range: $$${min} - $$${max}")
    }

    // Engineering: 7 employees, $75k - $130k
    stats("Engineering") shouldBe (75000, 130000, 7)

    // Sales: 6 employees, $70k - $95k
    stats("Sales") shouldBe (70000, 95000, 6)
  }

  // ========================================================================
  // SCROLL: ERROR HANDLING
  // ========================================================================

  "Scroll with error handling" should "handle failures gracefully" in {
    val config = ScrollConfig(scrollSize = 5)

    val futureResults = client
      .scrollAs[EmployeeWithWindow](
        """
          SELECT
            department,
            name,
            salary,
            hire_date,
            FIRST_VALUE(salary) OVER (
              PARTITION BY department
              ORDER BY hire_date ASC
            ) AS first_salary
          FROM emp
          """,
        config
      )
      .recover { case ex =>
        info(s"⚠️ Scroll error: ${ex.getMessage}")
        (EmployeeWithWindow("", "", 0, LocalDate.now), config.metrics)
      }
      .filter(_._1.department.nonEmpty) // Filtrer les erreurs
      .runWith(Sink.seq)

    val results = Await.result(futureResults, 30.seconds)

    results should not be empty
    results.foreach { case (emp, _) =>
      emp.department should not be empty
    }
  }

  // ========================================================================
  // Multi partitioning integration tests
  // ========================================================================

  private def checkEmployeesWithDistinctPartitions(
    employees: Seq[EmployeeDistinctPartitions]
  ): Unit = {
    employees should not be empty
    log.info(s"\n=== Testing ${employees.size} employees with 2 distinct window partitions ===")

    // Verify that all values are present
    employees.foreach { emp =>
      emp.first_in_dept_loc shouldBe defined
      emp.last_in_dept_loc shouldBe defined
      emp.first_in_dept shouldBe defined
      emp.last_in_dept shouldBe defined
    }

    // Display data for debugging
    log.info("\n--- Raw Data ---")
    employees.sortBy(e => (e.department, e.location, e.hire_date)).foreach { emp =>
      log.info(
        f"${emp.department}%-12s ${emp.location}%-15s ${emp.name}%-20s " +
        f"salary=${emp.salary}%6d hire=${emp.hire_date} " +
        f"| first_loc=${emp.first_in_dept_loc.get}%6d last_loc=${emp.last_in_dept_loc.get}%6d " +
        f"first_dept=${emp.first_in_dept.get}%6d last_dept=${emp.last_in_dept.get}%6d"
      )
    }

    // ============================================================
    // Test Window 1: PARTITION BY (department, location)
    // ============================================================
    log.info("\n--- Window 1: PARTITION BY (department, location) ---")
    val byDeptLoc = employees.groupBy(e => (e.department, e.location))

    byDeptLoc.foreach { case ((dept, loc), emps) =>
      val sortedByHireDate = emps.sortBy(_.hire_date)

      // Tous les employés de cette partition doivent avoir le même FIRST_VALUE et LAST_VALUE
      val firstValuesLoc = emps.flatMap(_.first_in_dept_loc).distinct
      val lastValuesLoc = emps.flatMap(_.last_in_dept_loc).distinct

      withClue(
        s"Partition ($dept, $loc) should have exactly 1 distinct first_in_dept_loc value\n"
      ) {
        firstValuesLoc should have size 1
      }

      withClue(s"Partition ($dept, $loc) should have exactly 1 distinct last_in_dept_loc value\n") {
        lastValuesLoc should have size 1
      }

      val expectedFirstLoc = sortedByHireDate.head.salary
      val expectedLastLoc = sortedByHireDate.last.salary

      withClue(
        s"Partition ($dept, $loc)\n" +
        s"  Expected first: ${sortedByHireDate.head.name} hired on ${sortedByHireDate.head.hire_date} with salary $expectedFirstLoc\n" +
        s"  Actual first_in_dept_loc: ${firstValuesLoc.head}\n"
      ) {
        firstValuesLoc.head shouldBe expectedFirstLoc
      }

      withClue(
        s"Partition ($dept, $loc)\n" +
        s"  Expected last: ${sortedByHireDate.last.name} hired on ${sortedByHireDate.last.hire_date} with salary $expectedLastLoc\n" +
        s"  Actual last_in_dept_loc: ${lastValuesLoc.head}\n"
      ) {
        lastValuesLoc.head shouldBe expectedLastLoc
      }

      log.info(f"  ✓ ($dept%-12s, $loc%-15s): ${emps.size} emps")
      log.info(
        f"      FIRST_LOC=$expectedFirstLoc%6d (${sortedByHireDate.head.name}%-20s ${sortedByHireDate.head.hire_date})"
      )
      log.info(
        f"      LAST_LOC =$expectedLastLoc%6d (${sortedByHireDate.last.name}%-20s ${sortedByHireDate.last.hire_date})"
      )
    }

    // ============================================================
    // Test Window 2: PARTITION BY (department)
    // ============================================================
    log.info("\n--- Window 2: PARTITION BY (department) ---")
    val byDept = employees.groupBy(_.department)

    byDept.foreach { case (dept, emps) =>
      val sortedByHireDate = emps.sortBy(_.hire_date)

      val firstValuesDept = emps.flatMap(_.first_in_dept).distinct
      val lastValuesDept = emps.flatMap(_.last_in_dept).distinct

      withClue(s"Department $dept should have exactly 1 distinct first_in_dept value\n") {
        firstValuesDept should have size 1
      }

      withClue(s"Department $dept should have exactly 1 distinct last_in_dept value\n") {
        lastValuesDept should have size 1
      }

      val expectedFirstDept = sortedByHireDate.head.salary
      val expectedLastDept = sortedByHireDate.last.salary

      withClue(
        s"Department $dept\n" +
        s"  Expected first: ${sortedByHireDate.head.name} hired on ${sortedByHireDate.head.hire_date} with salary $expectedFirstDept\n" +
        s"  Actual first_in_dept: ${firstValuesDept.head}\n"
      ) {
        firstValuesDept.head shouldBe expectedFirstDept
      }

      withClue(
        s"Department $dept\n" +
        s"  Expected last: ${sortedByHireDate.last.name} hired on ${sortedByHireDate.last.hire_date} with salary $expectedLastDept\n" +
        s"  Actual last_in_dept: ${lastValuesDept.head}\n"
      ) {
        lastValuesDept.head shouldBe expectedLastDept
      }

      log.info(f"  ✓ $dept%-12s: ${emps.size} emps")
      log.info(
        f"      FIRST_DEPT=$expectedFirstDept%6d (${sortedByHireDate.head.name}%-20s ${sortedByHireDate.head.hire_date})"
      )
      log.info(
        f"      LAST_DEPT =$expectedLastDept%6d (${sortedByHireDate.last.name}%-20s ${sortedByHireDate.last.hire_date})"
      )
    }

    // ============================================================
    // Checking the consistency between the two partitions
    // ============================================================
    log.info("\n--- Verifying distinct partition results ---")

    byDept.foreach { case (dept, deptEmps) =>
      val deptLocations = deptEmps.map(_.location).distinct.sorted

      log.info(s"\n$dept has ${deptLocations.size} locations: ${deptLocations.mkString(", ")}")

      val deptFirstSalary = deptEmps.flatMap(_.first_in_dept).distinct.head
      val deptLastSalary = deptEmps.flatMap(_.last_in_dept).distinct.head
      val deptFirstHire = deptEmps.minBy(_.hire_date)
      val deptLastHire = deptEmps.maxBy(_.hire_date)

      log.info(f"  Department-level window:")
      log.info(
        f"    FIRST = $deptFirstSalary%6d (${deptFirstHire.name} @ ${deptFirstHire.location}, ${deptFirstHire.hire_date})"
      )
      log.info(
        f"    LAST  = $deptLastSalary%6d (${deptLastHire.name} @ ${deptLastHire.location}, ${deptLastHire.hire_date})"
      )

      deptLocations.foreach { loc =>
        val locEmps = deptEmps.filter(_.location == loc)
        val locFirstSalary = locEmps.flatMap(_.first_in_dept_loc).distinct.head
        val locLastSalary = locEmps.flatMap(_.last_in_dept_loc).distinct.head
        val locFirstHire = locEmps.minBy(_.hire_date)
        val locLastHire = locEmps.maxBy(_.hire_date)

        log.info(f"  Location-level window ($loc):")
        log.info(
          f"    FIRST = $locFirstSalary%6d (${locFirstHire.name}, ${locFirstHire.hire_date})"
        )
        log.info(f"    LAST  = $locLastSalary%6d (${locLastHire.name}, ${locLastHire.hire_date})")

        // Logical check : the hiring date at the departmental level
        // must be <= on all dates at the level (department, location)
        withClue(
          s"$dept: First hire date at dept level (${deptFirstHire.hire_date}) " +
          s"should be <= first hire at ($dept, $loc) level (${locFirstHire.hire_date})\n"
        ) {
          deptFirstHire.hire_date should be <= locFirstHire.hire_date
        }

        withClue(
          s"$dept: Last hire date at dept level (${deptLastHire.hire_date}) " +
          s"should be >= last hire at ($dept, $loc) level (${locLastHire.hire_date})\n"
        ) {
          deptLastHire.hire_date should be >= locLastHire.hire_date
        }
      }

      if (deptLocations.size > 1) {
        log.info(s"  ✓ Different partitions produce different results (as expected)")
      } else {
        log.info(s"  ℹ Single location - partition results are identical")
      }
    }
  }

  "Search API with distinct window partitions" should "compute FIRST_VALUE and LAST_VALUE on different partitions" in {
    val results = client.searchAs[EmployeeDistinctPartitions]("""
      SELECT
        department,
        location,
        name,
        salary,
        hire_date,
        FIRST_VALUE(salary) OVER (
          PARTITION BY department, location
          ORDER BY hire_date ASC
        ) AS first_in_dept_loc,
        LAST_VALUE(salary) OVER (
          PARTITION BY department, location
          ORDER BY hire_date ASC
        ) AS last_in_dept_loc,
        FIRST_VALUE(salary) OVER (
          PARTITION BY department
          ORDER BY hire_date ASC
        ) AS first_in_dept,
        LAST_VALUE(salary) OVER (
          PARTITION BY department
          ORDER BY hire_date ASC
        ) AS last_in_dept
      FROM emp
      WHERE department IN ('Engineering', 'Sales')
      ORDER BY department, location, hire_date
      LIMIT 20
    """)

    results match {
      case ElasticSuccess(employees) =>
        checkEmployeesWithDistinctPartitions(employees)

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  it should "handle 3 different window partitions simultaneously" in {
    val results = client.searchAs[EmployeeMultiWindowPartitions]("""
      SELECT
        department,
        location,
        level,
        name,
        salary,
        hire_date,
        FIRST_VALUE(salary) OVER (
          PARTITION BY department, location
          ORDER BY hire_date ASC
        ) AS first_salary_dept_loc,
        FIRST_VALUE(salary) OVER (
          PARTITION BY department
          ORDER BY hire_date ASC
        ) AS first_salary_dept,
        AVG(salary) OVER (
          PARTITION BY level
        ) AS avg_salary_level
      FROM emp
      ORDER BY department, location, hire_date
      LIMIT 20
    """)

    results match {
      case ElasticSuccess(employees) =>
        employees should have size 20

        log.info(s"\n=== Testing 3 distinct window partitions ===")

        // Window 1: PARTITION BY department, location
        val byDeptLoc = employees.groupBy(e => (e.department, e.location))
        log.info(s"\nWindow 1: ${byDeptLoc.size} partitions (department, location)")
        byDeptLoc.foreach { case ((dept, loc), emps) =>
          val firstValues = emps.flatMap(_.first_salary_dept_loc).distinct
          firstValues should have size 1
          log.info(s"  ✓ ($dept, $loc): FIRST=${firstValues.head}")
        }

        // Window 2: PARTITION BY department
        val byDept = employees.groupBy(_.department)
        log.info(s"\nWindow 2: ${byDept.size} partitions (department)")
        byDept.foreach { case (dept, emps) =>
          val firstValues = emps.flatMap(_.first_salary_dept).distinct
          firstValues should have size 1
          log.info(s"  ✓ $dept: FIRST=${firstValues.head}")
        }

        // Window 3: PARTITION BY level
        val byLevel = employees.groupBy(_.level)
        log.info(s"\nWindow 3: ${byLevel.size} partitions (level)")
        byLevel.foreach { case (level, emps) =>
          val avgValues = emps.flatMap(_.avg_salary_level).distinct
          avgValues should have size 1
          val expectedAvg = emps.map(_.salary).sum.toDouble / emps.size
          avgValues.head shouldBe expectedAvg +- 0.01
          log.info(s"  ✓ $level: AVG=${avgValues.head} (${emps.size} employees)")
        }

      case ElasticFailure(error) =>
        fail(s"Query failed: ${error.message}")
    }
  }

  "Scroll API with distinct window partitions" should "compute correctly with streaming and different partitions" in {
    val config = ScrollConfig(scrollSize = 5, logEvery = 5)
    val startTime = System.currentTimeMillis()

    val futureResults = client
      .scrollAs[EmployeeDistinctPartitions](
        """
      SELECT
        department,
        location,
        name,
        salary,
        hire_date,
        FIRST_VALUE(salary) OVER (
          PARTITION BY department, location
          ORDER BY hire_date ASC
        ) AS first_in_dept_loc,
        LAST_VALUE(salary) OVER (
          PARTITION BY department, location
          ORDER BY hire_date ASC
        ) AS last_in_dept_loc,
        FIRST_VALUE(salary) OVER (
          PARTITION BY department
          ORDER BY hire_date ASC
        ) AS first_in_dept,
        LAST_VALUE(salary) OVER (
          PARTITION BY department
          ORDER BY hire_date ASC
        ) AS last_in_dept
      FROM emp
      ORDER BY department, location, hire_date
      LIMIT 20
    """,
        config
      )
      .runWith(Sink.seq)

    futureResults await { value =>
      val duration = System.currentTimeMillis() - startTime
      val results = value.map(_._1)
      checkEmployeesWithDistinctPartitions(results)
      duration should be < 1000L
    } match {
      case scala.util.Success(_)  => // OK
      case scala.util.Failure(ex) => fail(s"Scroll failed: ${ex.getMessage}")
    }
  }

  it should "handle 3 distinct partitions with small scroll size" in {
    val config = ScrollConfig(scrollSize = 3, logEvery = 3)
    val startTime = System.currentTimeMillis()

    var batchCount = 0
    val futureResults = client
      .scrollAs[EmployeeMultiWindowPartitions](
        """
      SELECT
        department,
        location,
        level,
        name,
        salary,
        hire_date,
        FIRST_VALUE(salary) OVER (
          PARTITION BY department, location
          ORDER BY hire_date ASC
        ) AS first_salary_dept_loc,
        FIRST_VALUE(salary) OVER (
          PARTITION BY department
          ORDER BY hire_date ASC
        ) AS first_salary_dept,
        AVG(salary) OVER (
          PARTITION BY level
        ) AS avg_salary_level
      FROM emp
      LIMIT 20
    """,
        config
      )
      .map { batch =>
        batchCount += 1
        log.info(s"  Batch $batchCount: ${batch._2.totalDocuments} documents")
        batch
      }
      .runWith(Sink.seq)

    val results = Await.result(futureResults, 30.seconds).map(_._1)
    val duration = System.currentTimeMillis() - startTime

    results should have size 20
    log.info(s"\n✓ Scrolled ${results.size} documents in $batchCount batches (${duration}ms)")

    // Check all 3 partitions
    val byDeptLoc = results.groupBy(e => (e.department, e.location))
    val byDept = results.groupBy(_.department)
    val byLevel = results.groupBy(_.level)

    log.info(s"\nPartition counts:")
    log.info(s"  (department, location): ${byDeptLoc.size} partitions")
    log.info(s"  (department): ${byDept.size} partitions")
    log.info(s"  (level): ${byLevel.size} partitions")

    // Check each partition
    byDeptLoc.foreach { case ((dept, loc), emps) =>
      val firstValues = emps.flatMap(_.first_salary_dept_loc).distinct
      firstValues should have size 1
    }

    byDept.foreach { case (dept, emps) =>
      val firstValues = emps.flatMap(_.first_salary_dept).distinct
      firstValues should have size 1
    }

    byLevel.foreach { case (level, emps) =>
      val avgValues = emps.flatMap(_.avg_salary_level).distinct
      avgValues should have size 1
    }

    log.info("✓ All 3 partitions computed correctly")
  }

  "Search and Scroll APIs with distinct window partitions" should "maintain consistency between them" in {
    // Search
    val searchResults = client.searchAs[EmployeeDistinctPartitions]("""
      SELECT
        department,
        location,
        name,
        salary,
        hire_date,
        FIRST_VALUE(salary) OVER (
          PARTITION BY department, location
          ORDER BY hire_date ASC
        ) AS first_in_dept_loc,
        LAST_VALUE(salary) OVER (
          PARTITION BY department, location
          ORDER BY hire_date ASC
        ) AS last_in_dept_loc,
        FIRST_VALUE(salary) OVER (
          PARTITION BY department
          ORDER BY hire_date ASC
        ) AS first_in_dept,
        LAST_VALUE(salary) OVER (
          PARTITION BY department
          ORDER BY hire_date ASC
        ) AS last_in_dept
      FROM emp
      WHERE department IN ('Engineering', 'Sales')
      LIMIT 20
    """) match {
      case ElasticSuccess(emps)  => emps
      case ElasticFailure(error) => fail(s"Search failed: ${error.message}")
    }

    // Scroll
    val config = ScrollConfig(scrollSize = 4)
    val futureScrollResults = client
      .scrollAs[EmployeeDistinctPartitions](
        """
      SELECT
        department,
        location,
        name,
        salary,
        hire_date,
        FIRST_VALUE(salary) OVER (
          PARTITION BY department, location
          ORDER BY hire_date ASC
        ) AS first_in_dept_loc,
        LAST_VALUE(salary) OVER (
          PARTITION BY department, location
          ORDER BY hire_date ASC
        ) AS last_in_dept_loc,
        FIRST_VALUE(salary) OVER (
          PARTITION BY department
          ORDER BY hire_date ASC
        ) AS first_in_dept,
        LAST_VALUE(salary) OVER (
          PARTITION BY department
          ORDER BY hire_date ASC
        ) AS last_in_dept
      FROM emp
      WHERE department IN ('Engineering', 'Sales')
      LIMIT 20
    """,
        config
      )
      .runWith(Sink.seq)

    val scrollResults = Await.result(futureScrollResults, 30.seconds).map(_._1)

    log.info(s"\n=== Comparing Search vs Scroll for distinct partitions ===")
    log.info(s"  Search: ${searchResults.size} results")
    log.info(s"  Scroll: ${scrollResults.size} results")

    searchResults.size shouldBe scrollResults.size

    // Compare Window 1: (dept, loc)
    val searchByDeptLoc = searchResults.groupBy(e => (e.department, e.location))
    val scrollByDeptLoc = scrollResults.groupBy(e => (e.department, e.location))

    searchByDeptLoc.keys shouldBe scrollByDeptLoc.keys

    log.info("\n--- Window 1: PARTITION BY (department, location) ---")
    searchByDeptLoc.foreach { case (key @ (dept, loc), searchEmps) =>
      val scrollEmps = scrollByDeptLoc(key)

      val searchFirst = searchEmps.flatMap(_.first_in_dept_loc).distinct.head
      val scrollFirst = scrollEmps.flatMap(_.first_in_dept_loc).distinct.head
      val searchLast = searchEmps.flatMap(_.last_in_dept_loc).distinct.head
      val scrollLast = scrollEmps.flatMap(_.last_in_dept_loc).distinct.head

      searchFirst shouldBe scrollFirst
      searchLast shouldBe scrollLast

      log.info(s"  ✓ ($dept, $loc): FIRST=$searchFirst, LAST=$searchLast (consistent)")
    }

    // Compare Window 2: (dept)
    val searchByDept = searchResults.groupBy(_.department)
    val scrollByDept = scrollResults.groupBy(_.department)

    log.info("\n--- Window 2: PARTITION BY (department) ---")
    searchByDept.foreach { case (dept, searchEmps) =>
      val scrollEmps = scrollByDept(dept)

      val searchFirst = searchEmps.flatMap(_.first_in_dept).distinct.head
      val scrollFirst = scrollEmps.flatMap(_.first_in_dept).distinct.head
      val searchLast = searchEmps.flatMap(_.last_in_dept).distinct.head
      val scrollLast = scrollEmps.flatMap(_.last_in_dept).distinct.head

      searchFirst shouldBe scrollFirst
      searchLast shouldBe scrollLast

      log.info(s"  ✓ $dept: FIRST=$searchFirst, LAST=$searchLast (consistent)")
    }
  }

}
