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

import ch.qos.logback.classic.{ClassicConstants, Level, LoggerContext}
import ch.qos.logback.classic.encoder.PatternLayoutEncoder
import ch.qos.logback.classic.spi.{Configurator, ILoggingEvent}
import ch.qos.logback.core.ConsoleAppender
import ch.qos.logback.core.spi.ContextAwareBase
import ch.qos.logback.core.util.{Loader, OptionHelper}
import org.slf4j.Logger

/** Test-only logback fallback for the `licensing` and `core` unit-test classpaths: root at WARN,
  * one console appender - applied ONLY where no logback XML configuration exists.
  *
  * Why a `ch.qos.logback.classic.spi.Configurator` service and not a `logback-test.xml`: this test
  * tree reaches every es{N} test classpath through `core % "test->test"`, where
  * `persistence-core-testkit` ships the `logback.xml` the integration suites are read by (INFO to
  * STDOUT). A `logback-test.xml` here would OVERRIDE it. Measured on logback 1.5.32
  * (`ContextInitializer.autoConfig`): ServiceLoader configurators are invoked BEFORE the internal
  * `DefaultJoranConfigurator` (the XML search) and `BasicConfigurator`, so a service configurator
  * would override the XML just the same - unless it defers. Hence [[xmlConfigurationPresent]]: this
  * configurator repeats `DefaultJoranConfigurator`'s own search and steps aside
  * (`INVOKE_NEXT_IF_ANY`) whenever an XML configuration is visible, so the ES suites keep their
  * `logback.xml` and the two unit-test modules (which had no logging backend before #258) get a
  * quiet root instead of `BasicConfigurator`'s DEBUG console.
  */
class TestLoggingConfigurator extends ContextAwareBase with Configurator {

  override def configure(context: LoggerContext): Configurator.ExecutionStatus =
    if (xmlConfigurationPresent) {
      addInfo(
        "TestLoggingConfigurator: a logback XML configuration is present, deferring to DefaultJoranConfigurator"
      )
      Configurator.ExecutionStatus.INVOKE_NEXT_IF_ANY
    } else {
      val encoder = new PatternLayoutEncoder()
      encoder.setContext(context)
      encoder.setPattern("%d{HH:mm:ss.SSS} %-5level %logger{36} - %msg%n")
      encoder.start()

      val console = new ConsoleAppender[ILoggingEvent]()
      console.setContext(context)
      console.setName(TestLoggingConfigurator.ConsoleAppenderName)
      console.setEncoder(encoder)
      console.start()

      val root = context.getLogger(Logger.ROOT_LOGGER_NAME)
      root.setLevel(Level.WARN)
      root.addAppender(console)

      addInfo(
        "TestLoggingConfigurator: no logback XML configuration found, root logger at WARN with a console appender"
      )
      Configurator.ExecutionStatus.DO_NOT_INVOKE_NEXT_IF_ANY
    }

  /** Mirrors `DefaultJoranConfigurator.performMultiStepConfigurationFileSearch` (logback 1.5.32):
    * the `logback.configurationFile` system property, then `logback-test.xml`, then `logback.xml`,
    * resolved through this class's own classloader.
    */
  private[licensing] def xmlConfigurationPresent: Boolean = {
    val loader = Loader.getClassLoaderOfObject(this)
    OptionHelper.getSystemProperty(ClassicConstants.CONFIG_FILE_PROPERTY) != null ||
    Loader.getResource(ClassicConstants.TEST_AUTOCONFIG_FILE, loader) != null ||
    Loader.getResource(ClassicConstants.AUTOCONFIG_FILE, loader) != null
  }
}

object TestLoggingConfigurator {
  val ConsoleAppenderName = "console"
}
