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

import app.softnetwork.elastic.licensing.LicenseManager
import app.softnetwork.elastic.sql.query.Statement
import com.typesafe.config.Config

/** Registry for managing loaded extensions.
  */
class ExtensionRegistry(
  config: Config,
  licenseManager: LicenseManager
) {

  import scala.jdk.CollectionConverters._
  import java.util.ServiceLoader

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /** All loaded extensions.
    */
  lazy val extensions: Seq[ExtensionSpi] = {
    val loader = ServiceLoader.load(classOf[ExtensionSpi])

    val loaded = loader.iterator().asScala.toSeq.flatMap { ext =>
      logger.info(
        s"🔌 Discovered extension: ${ext.extensionName} v${ext.version} (priority: ${ext.priority})"
      )

      ext.initialize(config, licenseManager) match {
        case Right(_) =>
          logger.info(s"✅ Extension ${ext.extensionName} initialized successfully")
          Some(ext)

        case Left(error) =>
          logger.warn(s"⚠️ Failed to initialize extension ${ext.extensionName}: $error")
          None
      }
    }

    // ✅ Sort by priority (lower = higher priority)
    loaded.sortBy(_.priority)
  }

  /** Find extension that can handle a statement.
    */
  def findHandler(statement: Statement): Option[ExtensionSpi] = {
    extensions.find(_.canHandle(statement))
  }

  /** Get all supported syntax.
    */
  def allSupportedSyntax: Seq[String] = {
    extensions.flatMap(_.supportedSyntax).distinct
  }

  /** Shutdown all extensions.
    */
  def shutdown(): Unit = {
    extensions.foreach { ext =>
      logger.info(s"🔌 Shutting down extension: ${ext.extensionName}")
      ext.shutdown()
    }
  }
}
