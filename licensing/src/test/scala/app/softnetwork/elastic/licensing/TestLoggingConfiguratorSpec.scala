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
import ch.qos.logback.classic.{ClassicConstants, Level, LoggerContext}
import ch.qos.logback.classic.spi.Configurator
import ch.qos.logback.core.Context
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.Logger

import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Path}

/** Pins the ONE property that makes [[TestLoggingConfigurator]] safe to ship in a test tree that
  * every es{N} test classpath inherits: it must step aside whenever a logback XML configuration is
  * visible to its own classloader (the integration suites' `logback.xml`), and configure only when
  * none is. Logback 1.5 invokes service configurators BEFORE its XML search, so a configurator that
  * forgot to defer would silently re-configure every integration suite's logging.
  */
class TestLoggingConfiguratorSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  private val configuratorClass = classOf[TestLoggingConfigurator].getName

  private var xmlDir: Path = _
  private def xmlFile: Path = xmlDir.resolve(ClassicConstants.AUTOCONFIG_FILE)

  override def beforeAll(): Unit = {
    xmlDir = Files.createTempDirectory("bidc5-logback-xml")
    Files.write(
      xmlFile,
      "<configuration><root level=\"ERROR\"/></configuration>\n".getBytes(StandardCharsets.UTF_8)
    )
  }

  override def afterAll(): Unit =
    if (xmlDir != null) {
      Files.deleteIfExists(xmlFile)
      Files.deleteIfExists(xmlDir)
    }

  private def assertNoXmlConfigurationOnThisClasspath(): Unit = {
    // preconditions, asserted not assumed: nothing on the licensing test classpath configures logback
    System.getProperty(ClassicConstants.CONFIG_FILE_PROPERTY) shouldBe null
    getClass.getClassLoader.getResource(ClassicConstants.TEST_AUTOCONFIG_FILE) shouldBe null
    getClass.getClassLoader.getResource(ClassicConstants.AUTOCONFIG_FILE) shouldBe null
  }

  behavior of "TestLoggingConfigurator"

  it should "configure the root logger at WARN with one console appender when no XML configuration is visible" in {
    assertNoXmlConfigurationOnThisClasspath()
    val context = new LoggerContext()
    val configurator = new TestLoggingConfigurator()
    configurator.setContext(context)

    configurator.xmlConfigurationPresent shouldBe false
    configurator.configure(context) shouldBe Configurator.ExecutionStatus.DO_NOT_INVOKE_NEXT_IF_ANY

    val root = context.getLogger(Logger.ROOT_LOGGER_NAME)
    root.getLevel shouldBe Level.WARN
    root.getAppender(TestLoggingConfigurator.ConsoleAppenderName) should not be null
    root.iteratorForAppenders().next().getName shouldBe TestLoggingConfigurator.ConsoleAppenderName
  }

  it should "defer to DefaultJoranConfigurator when a logback.xml is visible to its own classloader" in {
    assertNoXmlConfigurationOnThisClasspath()
    // A fresh copy of the configurator whose OWN classloader sees a logback.xml - the shape of every
    // es{N} test classpath, where persistence-core-testkit's logback.xml is on the classpath.
    val withXml = new RedefiningClassLoader(
      getClass.getClassLoader,
      Seq(configuratorClass),
      Set.empty,
      Array[URL](xmlDir.toUri.toURL)
    )
    val cls = withXml.loadClass(configuratorClass)
    cls.getClassLoader shouldBe theSameInstanceAs(withXml)
    withXml.getResource(ClassicConstants.AUTOCONFIG_FILE) should not be null

    val context = new LoggerContext()
    val configurator = cls.getConstructor().newInstance().asInstanceOf[AnyRef]
    cls.getMethod("setContext", classOf[Context]).invoke(configurator, context)
    cls.getMethod("xmlConfigurationPresent").invoke(configurator) shouldBe true

    val status = cls.getMethod("configure", classOf[LoggerContext]).invoke(configurator, context)
    status shouldBe Configurator.ExecutionStatus.INVOKE_NEXT_IF_ANY

    // and it touched nothing: a fresh context's root keeps logback's default level and no appender
    val root = context.getLogger(Logger.ROOT_LOGGER_NAME)
    root.getLevel shouldBe Level.DEBUG
    root.iteratorForAppenders().hasNext shouldBe false
  }
}
