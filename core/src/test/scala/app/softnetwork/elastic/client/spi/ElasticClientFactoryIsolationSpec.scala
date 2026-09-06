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

package app.softnetwork.elastic.client.spi

import app.softnetwork.elastic.client.ElasticClientApi
import app.softnetwork.elastic.licensing.ClassLoaderIsolation
import app.softnetwork.elastic.licensing.ClassLoaderIsolation.RedefiningClassLoader
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.lang.reflect.InvocationTargetException
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters._
import scala.util.{Failure, Try}

/** Thrown by [[IsolationStubClientSpi]]: proof that the provider was discovered AND invoked. */
final class IsolationStubReached
    extends RuntimeException("IsolationStubClientSpi.client was invoked")

/** Test-only `ElasticClientSpi` provider. It is registered NOWHERE on the real test classpath: the
  * spec serves its `META-INF/services` entry from a synthetic directory that only the isolating
  * loader can see. Every es{N} client module inherits `core % "test->test"` and
  * `ElasticClientFactory` takes `headOption` of ALL providers, so a registration under
  * `core/src/test/resources` would race the real client providers in the integration suites.
  */
final class IsolationStubClientSpi extends ElasticClientSpi {
  override def client(conf: Config): ElasticClientApi = throw new IsolationStubReached
}

/** #258 - `ElasticClientFactory` resolves its `ElasticClientSpi` providers against the interface's
  * own classloader, never the thread context classloader.
  *
  * The factory's `ServiceLoader` is a `val` on an `object`, latched at class initialisation. The
  * spec defines a FRESH copy of the object (and of the SPI interface, so that the interface's
  * loader is the isolating one) with a [[RedefiningClassLoader]] and forces its initialiser while a
  * blind context classloader is installed. Two-sided oracle: before the fix the blind loader yields
  * `IllegalStateException("No ElasticClientSpi implementation found")`; after it the ONLY
  * registration anywhere - the synthetic one - is found and its provider is invoked.
  */
class ElasticClientFactoryIsolationSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val factoryObject = "app.softnetwork.elastic.client.spi.ElasticClientFactory"
  private val spiName = classOf[ElasticClientSpi].getName
  private val stubName = classOf[IsolationStubClientSpi].getName
  private val servicesResource = "META-INF/services/" + spiName

  // Built BEFORE any blind context classloader is installed (ConfigFactory.load is TCCL-bound), and
  // from the resource that carries every `elastic.*` default itself, so that this spec isolates
  // DISCOVERY: ElasticConfig(config) runs before the ServiceLoader and must not be what fails here.
  // ElasticConfigIsolationSpec covers ElasticConfig's own fallback under a blind TCCL.
  private val config: Config = ConfigFactory.load("softnetwork-elastic.conf")

  private var servicesDir: Path = _
  private def servicesFile: Path = servicesDir.resolve(servicesResource)

  override def beforeAll(): Unit = {
    servicesDir = Files.createTempDirectory("bidc5-elastic-client-spi")
    Files.createDirectories(servicesFile.getParent)
    Files.write(servicesFile, (stubName + "\n").getBytes(StandardCharsets.UTF_8))
  }

  override def afterAll(): Unit =
    if (servicesDir != null) {
      Files.deleteIfExists(servicesFile)
      Files.deleteIfExists(servicesFile.getParent) // META-INF/services
      Files.deleteIfExists(servicesFile.getParent.getParent) // META-INF
      Files.deleteIfExists(servicesDir)
    }

  behavior of "ElasticClientFactory provider discovery (#258)"

  it should "find a provider registered beside ElasticClientSpi on first touch under a blind context classloader" in {
    val parent = getClass.getClassLoader
    // Precondition, asserted not assumed: the REAL test classpath carries no ElasticClientSpi
    // provider (the four registrations live in es6/7/8/9). That is what makes the oracle
    // two-sided - the only registration anywhere is the one the isolating loader serves.
    parent.getResources(servicesResource).hasMoreElements shouldBe false

    val blind = ClassLoaderIsolation.blindLoader()
    blind.getResources(servicesResource).hasMoreElements shouldBe false
    a[ClassNotFoundException] should be thrownBy Class.forName(spiName, false, blind)

    val isolating = new RedefiningClassLoader(
      parent,
      Seq(factoryObject, spiName, stubName),
      Set.empty,
      Array[URL](servicesDir.toUri.toURL)
    )
    val spiInIsolation = isolating.loadClass(spiName)
    spiInIsolation.getClassLoader shouldBe theSameInstanceAs(isolating)
    spiInIsolation should not be theSameInstanceAs(classOf[ElasticClientSpi])
    isolating.getResources(servicesResource).asScala.toList should have size 1

    val outcome = ClassLoaderIsolation.withContextClassLoader(blind) {
      // the object's initialiser - and its ServiceLoader.load - runs HERE, under the blind TCCL
      val factory = ClassLoaderIsolation.moduleOf(isolating, factoryObject)
      try Try(factory.getClass.getMethod("create", classOf[Config]).invoke(factory, config))
      finally removeShutdownHook(factory)
    }

    outcome match {
      case Failure(e: InvocationTargetException) if e.getCause.isInstanceOf[IsolationStubReached] =>
        succeed
      case Failure(e: InvocationTargetException) =>
        fail(s"provider NOT discovered: create() failed with ${e.getCause}", e.getCause)
      case other =>
        fail(s"expected create() to reach the stub provider, got $other")
    }
  }

  /** Every fresh copy of the factory registers its own JVM shutdown hook in its initialiser; remove
    * it so a test run does not accumulate hooks in the long-lived sbt JVM.
    */
  private def removeShutdownHook(factory: AnyRef): Unit =
    factory.getClass
      .getMethod("shutdownHook")
      .invoke(factory)
      .asInstanceOf[scala.sys.ShutdownHookThread]
      .remove()
}
