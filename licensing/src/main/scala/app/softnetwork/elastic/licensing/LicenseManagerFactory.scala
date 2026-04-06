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

package app.softnetwork.elastic.licensing

import com.typesafe.config.Config
import com.typesafe.scalalogging.LazyLogging

import java.util.ServiceLoader
import java.util.concurrent.atomic.AtomicReference
import scala.jdk.CollectionConverters._

/** Factory for creating and caching LicenseRefreshStrategy instances.
  *
  * Owns the full strategy lifecycle: SPI discovery -> build -> initialize -> cache. The LicenseMode
  * is derived from configuration (`refreshEnabled=true` -> LongRunning, else -> Driver).
  *
  * Thread-safe: all AtomicReference mutations use compareAndSet.
  */
object LicenseManagerFactory extends LazyLogging {

  private val _strategy: AtomicReference[Option[LicenseRefreshStrategy]] =
    new AtomicReference(None)

  /** Create a LicenseManager from config. Derives LicenseMode from config. Resolves the best SPI,
    * builds the strategy, initializes it, and caches it.
    */
  def create(config: Config): LicenseManager =
    resolveStrategy(config).licenseManager

  /** Get the cached strategy (resolved during create()). Falls back to NopRefreshStrategy. */
  def currentStrategy: LicenseRefreshStrategy =
    _strategy.get().getOrElse(new NopRefreshStrategy())

  /** Replace the cached strategy. Initializes the new strategy before caching. Use for license
    * upgrade/downgrade at runtime. Shuts down the old strategy before replacing.
    */
  def setStrategy(strategy: LicenseRefreshStrategy): Unit = {
    strategy.initialize()
    var old: Option[LicenseRefreshStrategy] = None
    var updated = false
    while (!updated) {
      val current = _strategy.get()
      if (_strategy.compareAndSet(current, Some(strategy))) {
        old = current
        updated = true
      }
    }
    old.foreach(_.shutdown())
  }

  /** Shutdown the cached strategy's background resources (if any) and clear the cache. Called
    * during process shutdown to stop the refresh scheduler.
    */
  def shutdown(): Unit = {
    var old: Option[LicenseRefreshStrategy] = None
    var updated = false
    while (!updated) {
      val current = _strategy.get()
      if (_strategy.compareAndSet(current, None)) {
        old = current
        updated = true
      }
    }
    old.foreach(_.shutdown())
  }

  /** Reset cached strategy (for testing). Uses CAS to ensure atomic clear. */
  def reset(): Unit = {
    var updated = false
    while (!updated) {
      val current = _strategy.get()
      updated = _strategy.compareAndSet(current, None)
    }
  }

  /** Resolve LicenseMode from config. refreshEnabled=true -> LongRunning, else -> Driver. */
  private def resolveMode(config: Config): Option[LicenseMode] = {
    val licenseConfig = LicenseConfig.load(config)
    if (licenseConfig.refreshEnabled) Some(LicenseMode.LongRunning)
    else Some(LicenseMode.Driver)
  }

  /** Resolve strategy via SPI, initialize it, and cache it. Synchronized to prevent concurrent
    * creation of duplicate strategies (which would leak resources like Akka schedulers).
    */
  private def resolveStrategy(config: Config): LicenseRefreshStrategy =
    _strategy.get() match {
      case Some(s) => s
      case None =>
        synchronized {
          // Double-check after acquiring lock
          _strategy.get() match {
            case Some(s) => s
            case None =>
              val mode = resolveMode(config)
              val loader = ServiceLoader.load(classOf[LicenseManagerSpi])
              val spis = loader.iterator().asScala.toSeq.sortBy(_.priority)
              val strategy = spis.headOption
                .map { spi =>
                  try {
                    val s = spi.createStrategy(config, mode)
                    s.initialize()
                    logger.info(
                      s"License strategy initialized: ${s.getClass.getSimpleName} " +
                      s"(mode=${mode.getOrElse("default")}, type=${s.licenseManager.licenseType})"
                    )
                    s
                  } catch {
                    case e: Exception =>
                      logger.error(
                        s"Failed to create license strategy from ${spi.getClass.getName}: ${e.getMessage}",
                        e
                      )
                      val fallback = new NopRefreshStrategy()
                      fallback.initialize()
                      fallback
                  }
                }
                .getOrElse {
                  val fallback = new NopRefreshStrategy()
                  fallback.initialize()
                  fallback
                }
              _strategy.set(Some(strategy))
              strategy
          }
        }
    }
}
