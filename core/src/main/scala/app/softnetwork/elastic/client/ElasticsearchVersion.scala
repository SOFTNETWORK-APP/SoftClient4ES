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
    *
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

  /** Check if version is <= target version
    */
  def isAtMost(
    version: String,
    targetMajor: Int,
    targetMinor: Int = 0,
    targetPatch: Int = 0
  ): Boolean = {
    val (major, minor, patch) = parse(version)

    if (major < targetMajor) true
    else if (major > targetMajor) false
    else { // major == targetMajor
      if (minor < targetMinor) true
      else if (minor > targetMinor) false
      else { // minor == targetMinor
        patch <= targetPatch
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
    isAtLeast(version, 8)
  }

  /** Check if version is ES 7+
    */
  def isEs7OrHigher(version: String): Boolean = {
    isAtLeast(version, 7)
  }

  /** Check if version is ES 6
    */
  def isEs6(version: String): Boolean = {
    val (major, _, _) = parse(version)
    major == 6
  }

  /** Check if version is ES 7
    */
  def isEs7(version: String): Boolean = {
    val (major, _, _) = parse(version)
    major == 7
  }

  /** Check if Data Streams are supported (ES >= 7.9)
    */
  def supportsDataStreams(version: String): Boolean = {
    isAtLeast(version, 7, 9)
  }

  /** Check if Composable Templates are supported (ES >= 7.8)
    */
  def supportsComposableTemplates(version: String): Boolean = {
    isAtLeast(version, 7, 8)
  }

  /** Check if Searchable Snapshots are supported (ES >= 7.10)}
    */
  def supportsSearchableSnapshots(version: String): Boolean = {
    isAtLeast(version, 7, 10)
  }

  /** Check if Elasticsearch version requires _doc type wrapper in mappings
    *
    * ES 6.x requires mappings to be wrapped in a type name (e.g., "_doc") ES 7.x removed mapping
    * types
    *
    * @param version
    *   the Elasticsearch version (e.g., "6.8.0")
    * @return
    *   true if _doc wrapper is required
    */
  def requiresDocTypeWrapper(version: String): Boolean = {
    !isAtLeast(version, 6, 8)
  }

  /** Check if deletion by query on closed indices is supported (ES >= 7.5)
    */
  def supportsDeletionByQueryOnClosedIndices(version: String): Boolean = {
    isAtLeast(version, 7, 5)
  }

  /** Check if enrich processor is supported (ES >= 7.5)
    */
  def supportsEnrich(version: String): Boolean = {
    isAtLeast(version, 7, 5)
  }

  /** Check if latest transform features are supported (ES >= 7.8)
    */
  def supportsLatestTransform(version: String): Boolean = {
    isAtLeast(version, 7, 8)
  }

  /** Check if materialized views are supported (enrich + latest transform)
    */
  def supportsMaterializedView(version: String): Boolean = {
    supportsEnrich(version) && supportsLatestTransform(version)
  }
}
