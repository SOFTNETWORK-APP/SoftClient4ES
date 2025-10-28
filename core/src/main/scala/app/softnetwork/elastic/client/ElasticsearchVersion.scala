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

/** Elasticsearch version comparison utilities
  */
object ElasticsearchVersion {

  /** Parse Elasticsearch version string (e.g., "7.10.2", "8.11.0")
    * @return
    *   (major, minor, patch)
    */
  def parse(versionString: String): (Int, Int, Int) = {
    try {
      val parts = versionString.split('.').take(3)
      val major = parts.headOption.map(_.toInt).getOrElse(0)
      val minor = parts.lift(1).map(_.toInt).getOrElse(0)
      val patch = parts.lift(2).map(_.toInt).getOrElse(0)
      (major, minor, patch)
    } catch {
      case _: NumberFormatException =>
        throw new IllegalArgumentException(s"Invalid version format: $versionString")
    }
  }

  /** Check if version is >= target version
    */
  def isAtLeast(
    version: String,
    targetMajor: Int,
    targetMinor: Int = 0,
    targetPatch: Int = 0
  ): Boolean = {
    val (major, minor, patch) = parse(version)

    if (major > targetMajor) true
    else if (major < targetMajor) false
    else { // major == targetMajor
      if (minor > targetMinor) true
      else if (minor < targetMinor) false
      else { // minor == targetMinor
        patch >= targetPatch
      }
    }
  }

  /** Check if PIT is supported (ES >= 7.10)
    */
  def supportsPit(version: String): Boolean = {
    isAtLeast(version, 7, 10)
  }

  /** Check if version is ES 8+
    */
  def isEs8OrHigher(version: String): Boolean = {
    isAtLeast(version, 8, 0)
  }
}
