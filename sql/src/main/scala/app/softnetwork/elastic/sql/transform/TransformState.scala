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

package app.softnetwork.elastic.sql.transform

import app.softnetwork.elastic.sql.DdlToken
import app.softnetwork.elastic.sql.health.HealthStatus

/** Transform state
  */
sealed trait TransformState extends DdlToken {
  def name: String
  def sql: String = name
  def health: HealthStatus
}

object TransformState {
  case object Started extends TransformState {
    val name: String = "STARTED"
    override def health: HealthStatus = HealthStatus.Green
  }
  case object Indexing extends TransformState {
    val name: String = "INDEXING"
    override def health: HealthStatus = HealthStatus.Yellow
  }
  case object Aborting extends TransformState {
    val name: String = "ABORTING"
    override def health: HealthStatus = HealthStatus.Yellow
  }
  case object Stopping extends TransformState {
    val name: String = "STOPPING"
    override def health: HealthStatus = HealthStatus.Yellow
  }
  case object Stopped extends TransformState {
    val name: String = "STOPPED"
    override def health: HealthStatus = HealthStatus.Green
  }
  case object Failed extends TransformState {
    val name: String = "FAILED"
    override def health: HealthStatus = HealthStatus.Red
  }
  case object Waiting extends TransformState {
    val name: String = "WAITING"
    override def health: HealthStatus = HealthStatus.Green
  }
  case class Other(name: String) extends TransformState {
    override def health: HealthStatus = HealthStatus.Other(name)
  }

  def apply(name: String): TransformState = name.toUpperCase() match {
    case "STARTED"  => Started
    case "INDEXING" => Indexing
    case "ABORTING" => Aborting
    case "STOPPING" => Stopping
    case "STOPPED"  => Stopped
    case "FAILED"   => Failed
    case "WAITING"  => Waiting
    case other      => Other(other)
  }
}
