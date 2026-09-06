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

import app.softnetwork.elastic.client.extensions.{CoreDdlExtension, CoreDqlExtension}
import app.softnetwork.elastic.licensing.ClassLoaderIsolation.RedefiningClassLoader
import app.softnetwork.elastic.licensing.{
  ClassLoaderIsolation,
  LicenseRefreshStrategy,
  LogbackCapture,
  NopRefreshStrategy
}
import ch.qos.logback.classic.Level
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.net.URL

/** #258 - `ExtensionRegistry` resolves its `ExtensionSpi` providers against the interface's own
  * classloader, never the thread context classloader, and WARNs when it finds none (the silent "no
  * JOIN / MV extension" mode of #157).
  *
  * Each spec instantiates a FRESH copy of the registry class defined by a
  * [[RedefiningClassLoader]], so the `lazy val extensions` it forces is that class's first touch
  * whatever ran before.
  */
class ExtensionRegistryIsolationSpec extends AnyFlatSpec with Matchers {

  private val registryClass = classOf[ExtensionRegistry].getName
  private val spiName = classOf[ExtensionSpi].getName
  private val servicesResource = "META-INF/services/" + spiName

  // ExtensionRegistry logs through LoggerFactory.getLogger(getClass): the logger is its class name
  private val registryLogger = registryClass

  // Built BEFORE any blind context classloader is installed: ConfigFactory.load() is TCCL-bound.
  private val config: Config = ConfigFactory.load()
  private val strategy: LicenseRefreshStrategy = new NopRefreshStrategy()

  private def extensionsOfFreshRegistry(loader: ClassLoader): Seq[_] = {
    val cls = loader.loadClass(registryClass)
    // a distinct Class => a distinct, unforced lazy val: this IS the registry's first touch
    cls should not be theSameInstanceAs(classOf[ExtensionRegistry])
    val registry = cls
      .getConstructor(classOf[Config], classOf[LicenseRefreshStrategy])
      .newInstance(config, strategy)
      .asInstanceOf[AnyRef]
    cls.getMethod("extensions").invoke(registry).asInstanceOf[Seq[_]]
  }

  behavior of "ExtensionRegistry provider discovery (#258)"

  it should "discover the extensions shipped in softclient4es-core on first touch under a blind context classloader" in {
    val parent = getClass.getClassLoader
    // precondition, asserted not assumed: core's own jar registers CoreDdlExtension/CoreDqlExtension
    parent.getResources(servicesResource).hasMoreElements shouldBe true
    val blind = ClassLoaderIsolation.blindLoader()
    blind.getResources(servicesResource).hasMoreElements shouldBe false

    val isolating =
      new RedefiningClassLoader(parent, Seq(registryClass), Set.empty, Array.empty[URL])
    val (extensions, events) = LogbackCapture.capture(registryLogger, Level.INFO) {
      ClassLoaderIsolation.withContextClassLoader(blind)(extensionsOfFreshRegistry(isolating))
    }
    val ids = extensions.collect { case e: ExtensionSpi => e.extensionId }
    ids should contain allOf (new CoreDdlExtension().extensionId, new CoreDqlExtension().extensionId)
    events.filter(_.level == "WARN") shouldBe empty
  }

  it should "WARN naming ExtensionSpi and load nothing when no provider is visible to the interface's own classloader" in {
    val parent = getClass.getClassLoader
    // The context classloader is deliberately left as it is - one that CAN see core's registration.
    // Post-fix that must not matter (AD-S5-1: a provider visible only through the TCCL is no longer
    // honoured), and this WARN is what makes that narrowing visible. Pre-fix the single-arg load
    // consults the TCCL and dies on `CoreDdlExtension not a subtype` of the redefined interface.
    val isolating = new RedefiningClassLoader(
      parent,
      Seq(registryClass, spiName),
      Set(servicesResource),
      Array.empty[URL]
    )
    // the interface now belongs to the isolating loader, which hides its registration
    isolating.loadClass(spiName).getClassLoader shouldBe theSameInstanceAs(isolating)
    isolating.getResources(servicesResource).hasMoreElements shouldBe false

    val (extensions, events) =
      LogbackCapture.capture(registryLogger, Level.INFO)(extensionsOfFreshRegistry(isolating))
    extensions shouldBe empty
    val warnings = events.filter(_.level == "WARN")
    warnings should have size 1
    warnings.head.message should include(spiName)
  }
}
