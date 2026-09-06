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

import ch.qos.logback.classic.{Level, Logger => LogbackLogger, LoggerContext}
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

/** Test-only logback capture (house pattern: `SlicedScrollCompletenessSpec.captureClientLog`).
  *
  * `logback-classic` is a `Test`-scoped dependency of `licensing` and `core` for exactly this: the
  * #258 specs assert the WARN a factory emits when an SPI interface has no provider. Reading the
  * captured list only after the appender is detached, and under the appender's lock, is what keeps
  * a late log line from another thread from throwing `ConcurrentModificationException`.
  */
object LogbackCapture {

  final case class Captured(level: String, message: String)

  /** Run `body` while capturing every event logged through `loggerName` at `level` or above BY THE
    * CALLING THREAD; the logger's level is raised for the window and restored afterwards. Returns
    * the body's result and the captured events in order.
    *
    * The thread filter is what keeps an exact-count assertion honest in a module whose suites run
    * in parallel (`licensing` does): the redefined objects under test log on the spec's own thread,
    * while the REAL object of the same name - hence the same logger - may log from another suite's
    * thread at any moment. Events from other threads are dropped. A `body` that throws propagates
    * its exception and the events are discarded with the appender - wrap a throwing body in `Try`
    * INSIDE `body` if its log matters.
    */
  def capture[A](loggerName: String, level: Level)(body: => A): (A, Seq[Captured]) = {
    val logger = logbackLogger(loggerName)
    val capturingThread = Thread.currentThread().getName
    // logback 1.5 stamps an event's thread name LAZILY (on the first getThreadName()), and a plain
    // ListAppender only stores the event - so the thread filter below must not depend on some
    // other appender (a root console appender) having forced the stamp. Prepare the event on the
    // logging thread ourselves, whatever the root configuration is.
    val appender = new ListAppender[ILoggingEvent]() {
      override protected def append(e: ILoggingEvent): Unit = {
        e.prepareForDeferredProcessing()
        super.append(e)
      }
    }
    appender.start()
    val previousLevel = logger.getLevel
    logger.setLevel(level)
    logger.addAppender(appender)
    try {
      val result = body
      logger.detachAppender(appender)
      val events = appender.synchronized(
        appender.list.asScala
          .filter(_.getThreadName == capturingThread)
          .map(e => Captured(e.getLevel.toString, e.getFormattedMessage))
          .toVector
      )
      (result, events)
    } finally {
      logger.detachAppender(appender) // idempotent
      appender.stop()
      logger.setLevel(previousLevel)
    }
  }

  private val bindingWaitNanos = 10000000000L // 10 s

  /** The logback logger named `loggerName`, taken from the REAL `LoggerContext`.
    *
    * slf4j 2 performs its one-time binding on the first thread that asks and, while that runs,
    * hands every OTHER thread a `SubstituteLogger` (measured: the licensing suites run in parallel,
    * and a plain `LoggerFactory.getLogger` here returned `org.slf4j.helpers.SubstituteLogger` under
    * `+ licensing/test` although it returned a logback logger when the spec ran alone). Waiting,
    * bounded, for `getILoggerFactory` to become the `LoggerContext` makes the capture
    * deterministic.
    */
  private def logbackLogger(loggerName: String): LogbackLogger = {
    val deadline = System.nanoTime() + bindingWaitNanos
    var factory = LoggerFactory.getILoggerFactory
    while (!factory.isInstanceOf[LoggerContext] && System.nanoTime() < deadline) {
      Thread.sleep(20)
      factory = LoggerFactory.getILoggerFactory
    }
    factory match {
      case context: LoggerContext => context.getLogger(loggerName)
      case other =>
        throw new IllegalStateException(
          s"slf4j is not bound to logback (${other.getClass.getName}): " +
          "logback-classic must be on the test classpath"
        )
    }
  }
}
