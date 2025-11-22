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

package app.softnetwork.elastic.model

import java.time.LocalDate

package object window {

  case class Employee(
    name: String,
    department: String,
    location: String,
    salary: Int,
    hire_date: String,
    level: String,
    skills: List[String],
    id: String = ""
  )

  case class EmployeeWithWindow(
    department: String,
    name: String,
    salary: Int,
    hire_date: LocalDate,
    location: Option[String] = None,
    level: Option[String] = None,
    skills: Option[List[String]] = None,
    first_salary: Option[Int] = None,
    last_salary: Option[Int] = None,
    rank: Option[Int] = None,
    row_number: Option[Int] = None
  )

  case class DepartmentStats(
    department: String,
    avg_salary: Double,
    max_salary: Int,
    min_salary: Int,
    employee_count: Long
  )

  case class DepartmentWithWindow(
    department: String,
    location: Option[String] = None,
    avg_salary: Option[Double] = None,
    top_earners: Option[List[String]] = None,
    first_hire_date: Option[String] = None
  )

  case class EmployeeMinimal(
    name: String,
    department: String,
    salary: Int
  )

  case class EmployeeWithGlobalWindow(
    name: String,
    salary: Int,
    hire_date: String,
    first_ever_salary: Option[Int] = None,
    last_ever_salary: Option[Int] = None
  )

  case class EmployeeMultiPartition(
    department: String,
    location: String,
    name: String,
    salary: Int,
    hire_date: String,
    first_in_dept_loc: Option[Int] = None
  )
}
