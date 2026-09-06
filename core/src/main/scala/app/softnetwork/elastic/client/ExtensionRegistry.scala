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

import app.softnetwork.elastic.licensing.LicenseRefreshStrategy
import app.softnetwork.elastic.sql.query.Statement
import com.typesafe.config.Config

/** Registry for managing loaded extensions.
  */
class ExtensionRegistry(
  config: Config,
  licenseRefreshStrategy: LicenseRefreshStrategy
) {

  import scala.jdk.CollectionConverters._
  import java.util.ServiceLoader

  private val logger = org.slf4j.LoggerFactory.getLogger(getClass)

  /** All loaded extensions.
    */
  lazy val extensions: Seq[ExtensionSpi] = {
    val scanStart = System.nanoTime()
    // #258: resolve providers against the classloader that loaded this library, never the thread
    // context classloader - under a host-owned blind loader a single-arg load found nothing and the
    // registry silently ran with no extension at all (#157's silent-wrong-answer mode).
    val spiClass = classOf[ExtensionSpi]
    val discovered = ServiceLoader.load(spiClass, spiClass.getClassLoader).iterator().asScala.toList
    if (discovered.isEmpty) {
      logger.warn(
        s"No ${spiClass.getName} provider found through ${spiClass.getClassLoader}: no SQL " +
        "extension will be available - not even the core DDL/DQL extensions shipped in " +
        "softclient4es-core, let alone cross-index JOIN or materialized views. Providers are " +
        "resolved against the classloader that loaded softclient4es-core, never the thread context " +
        "classloader: the extension jars must be on that same classpath (#258)."
      )
    }

    val loaded = discovered.flatMap { ext =>
      logger.info(
        s"🔌 Discovered extension: ${ext.extensionName} v${ext.version} (priority: ${ext.priority})"
      )

      val initStart = System.nanoTime()
      ext.initialize(config, licenseRefreshStrategy) match {
        case Right(_) =>
          logger.info(
            s"✅ Extension ${ext.extensionName} initialized successfully in ${elapsedMs(initStart)}ms"
          )
          Some(ext)

        case Left(error) =>
          logger.warn(
            s"⚠️ Failed to initialize extension ${ext.extensionName} after ${elapsedMs(initStart)}ms: $error"
          )
          None
      }
    }

    // ✅ Sort by priority (lower = higher priority)
    val sorted = loaded.sortBy(_.priority)
    logger.info(
      s"🔌 Extension discovery + initialization completed in ${elapsedMs(scanStart)}ms " +
      s"(${sorted.size} extension(s) active)"
    )
    sorted
  }

  private def elapsedMs(t0: Long): Long = (System.nanoTime() - t0) / 1000000L

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
