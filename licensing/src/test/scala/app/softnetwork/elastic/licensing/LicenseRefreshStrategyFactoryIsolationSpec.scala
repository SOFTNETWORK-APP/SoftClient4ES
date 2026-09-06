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

import app.softnetwork.elastic.licensing.ClassLoaderIsolation.RedefiningClassLoader
import app.softnetwork.elastic.licensing.metrics.MetricsApi
import ch.qos.logback.classic.Level
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URL

/** #258 - `LicenseRefreshStrategyFactory` resolves its `LicenseManagerSpi` providers against the
  * interface's own classloader, never the thread context classloader, and WARNs when it finds none.
  *
  * Both specs drive a FRESH copy of the factory object (its CAS cache included) defined by a
  * [[RedefiningClassLoader]], so the discovery under test is the object's first touch whatever
  * other suites in this JVM did before.
  */
class LicenseRefreshStrategyFactoryIsolationSpec extends AnyFlatSpec with Matchers {

  private val factoryObject = "app.softnetwork.elastic.licensing.LicenseRefreshStrategyFactory"

  // LazyLogging names the logger after the module class: "...LicenseRefreshStrategyFactory$"
  private val factoryLogger = LicenseRefreshStrategyFactory.getClass.getName
  private val spiName = classOf[LicenseManagerSpi].getName
  private val servicesResource = "META-INF/services/" + spiName
  private val initializedLine = "License strategy initialized"

  // Built BEFORE any blind context classloader is installed: ConfigFactory.load() is TCCL-bound.
  private val config: Config = ConfigFactory.load()

  private def createVia(loader: ClassLoader): LicenseRefreshStrategy = {
    val factory = ClassLoaderIsolation.moduleOf(loader, factoryObject)
    // a distinct Class => a distinct, empty CAS cache: this IS the object's first touch
    factory.getClass should not be theSameInstanceAs(LicenseRefreshStrategyFactory.getClass)
    factory.getClass
      .getMethod("create", classOf[Config], classOf[MetricsApi])
      .invoke(factory, config, MetricsApi.Noop)
      .asInstanceOf[LicenseRefreshStrategy]
  }

  behavior of "LicenseRefreshStrategyFactory provider discovery (#258)"

  it should "discover the shipped CommunityLicenseManagerSpi on first touch under a blind context classloader" in {
    val parent = getClass.getClassLoader
    // precondition, asserted not assumed: the licensing jar registers CommunityLicenseManagerSpi
    parent.getResources(servicesResource).hasMoreElements shouldBe true
    val blind = ClassLoaderIsolation.blindLoader()
    blind.getResources(servicesResource).hasMoreElements shouldBe false

    val isolating =
      new RedefiningClassLoader(parent, Seq(factoryObject), Set.empty, Array.empty[URL])
    val (strategy, events) = LogbackCapture.capture(factoryLogger, Level.INFO) {
      ClassLoaderIsolation.withContextClassLoader(blind)(createVia(isolating))
    }
    try {
      // CommunityLicenseManagerSpi builds a plain NopRefreshStrategy - the SAME class as the empty
      // list fallback - so the strategy class is no oracle. The SPI path is the one that logs the
      // initialisation line; the fallback path is the one that WARNs.
      events.map(_.message).filter(_.startsWith(initializedLine)) should have size 1
      events.filter(_.level == "WARN") shouldBe empty
      strategy.licenseManager.licenseType shouldBe LicenseType.Community
    } finally strategy.shutdown()
  }

  it should "WARN naming LicenseManagerSpi and fall back to NopRefreshStrategy when no provider is visible to the interface's own classloader" in {
    val parent = getClass.getClassLoader
    val isolating = new RedefiningClassLoader(
      parent,
      Seq(factoryObject, spiName),
      Set(servicesResource),
      Array.empty[URL]
    )
    // the interface now belongs to the isolating loader, which hides its registration
    isolating.loadClass(spiName).getClassLoader shouldBe theSameInstanceAs(isolating)
    isolating.getResources(servicesResource).hasMoreElements shouldBe false

    val (strategy, events) = LogbackCapture.capture(factoryLogger, Level.INFO)(createVia(isolating))
    try {
      strategy shouldBe a[NopRefreshStrategy]
      val warnings = events.filter(_.level == "WARN")
      warnings should have size 1
      warnings.head.message should include(spiName)
      warnings.head.message should include(classOf[NopRefreshStrategy].getSimpleName)
      events.map(_.message).exists(_.startsWith(initializedLine)) shouldBe false
    } finally strategy.shutdown()
  }
}
