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

package app.softnetwork.elastic.client.repl

import java.util.Properties

import scala.util.Try

/** Bundle provenance disclosure (REPL.4 / #163 fix 4).
  *
  * The self-contained `-all` assemblies built by the softclient4es-repl packaging repo carry a
  * `softclient4es-bundle-info.properties` resource at the jar root, stamping the bundle version and
  * the exact pinned engine / extension versions. When that resource is present on the classpath,
  * the REPL banner and the `version` meta-command disclose the bundle provenance. Plain installs
  * and sbt runs have no such resource — the surface stays silent.
  */
object BundleInfo {

  val ResourceName: String = "softclient4es-bundle-info.properties"

  final case class Bundle(
    bundleVersion: String,
    engineVersion: String,
    communityExtensionsVersion: String,
    arrowExtensionsVersion: String,
    gitSha: Option[String],
    javaFloor: Option[String]
  ) {

    /** Full disclosure line (spec wording — `version` meta-command). */
    def summary: String =
      s"Bundle $bundleVersion (engine $engineVersion, " +
      s"community $communityExtensionsVersion, arrow-ext $arrowExtensionsVersion)"

    /** Compact form for the fixed-width welcome banner (must stay within 56 visible chars). */
    def bannerLine: String =
      s"Bundle $bundleVersion (engine $engineVersion, " +
      s"ext $communityExtensionsVersion + $arrowExtensionsVersion)"
  }

  /** Bundle info from the runtime classpath, if any. */
  lazy val fromClasspath: Option[Bundle] = load(getClass.getClassLoader)

  /** Test seam: load the bundle-info resource from an explicit ClassLoader. */
  private[repl] def load(classLoader: ClassLoader): Option[Bundle] =
    Option(classLoader.getResourceAsStream(ResourceName)).flatMap { is =>
      try {
        val props = new Properties()
        props.load(is)
        parse(props)
      } catch {
        case _: Exception => None
      } finally {
        Try(is.close())
      }
    }

  private[repl] def parse(props: Properties): Option[Bundle] = {
    def get(key: String): Option[String] =
      Option(props.getProperty(key)).map(_.trim).filter(_.nonEmpty)
    for {
      bundle    <- get("bundle.version")
      engine    <- get("engine.version")
      community <- get("community.extensions.version")
      arrowExt  <- get("arrow.extensions.version")
    } yield Bundle(
      bundleVersion = bundle,
      engineVersion = engine,
      communityExtensionsVersion = community,
      arrowExtensionsVersion = arrowExt,
      gitSha = get("bundle.git.sha"),
      javaFloor = get("java.floor")
    )
  }
}
