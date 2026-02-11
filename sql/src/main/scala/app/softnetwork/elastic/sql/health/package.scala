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

package app.softnetwork.elastic.sql

package object health {

  implicit class HealthStatusOps(val status: HealthStatus) extends AnyVal {

    /** Convert to boolean (true = operational) */
    def toBoolean: Boolean = status.isOperational

    /** Get color code for terminal output */
    def colorCode: String = status match {
      case HealthStatus.Green    => Console.GREEN
      case HealthStatus.Yellow   => Console.YELLOW
      case HealthStatus.Red      => Console.RED
      case HealthStatus.Other(_) => Console.WHITE
    }

    /** Format for terminal with color */
    def colorDisplay: String = {
      s"$colorCode${status.emoji} ${status.name}${Console.RESET}"
    }
  }

}
