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

import ch.qos.logback.classic.{Level, Logger => LogbackLogger, LoggerContext}
import ch.qos.logback.classic.spi.{ILoggingEvent, IThrowableProxy}
import ch.qos.logback.core.read.ListAppender
import org.slf4j.LoggerFactory

import scala.jdk.CollectionConverters._

/** The ONE log capture of the client-cache specs, unit and integration (house pattern:
  * `LogbackCapture`).
  *
  * The client lines are INFO while the test root level is WARN (licensing's
  * `TestLoggingConfigurator`), so a capture that left the level alone would see nothing and every
  * "no secret in the logs" assertion would pass vacuously: the `app.softnetwork.elastic.client`
  * logger is opened to every level (TRACE, the lowest; logback deprecates `Level.ALL`) for the
  * window and restored afterwards. Every event of every thread is kept, with its formatted message,
  * its arguments and its throwable chain.
  */
object ClientLogCapture {

  val LoggerName = "app.softnetwork.elastic.client"

  final case class Event(
    thread: String,
    level: String,
    logger: String,
    message: String,
    arguments: Seq[String],
    throwable: String
  ) {

    /** Everything the event carries, for a leak search. */
    def text: String = (message +: arguments :+ throwable).mkString("\n")
  }

  /** Runs `body` while capturing every event logged under [[LoggerName]]. A `body` that throws
    * propagates its exception and loses the events: wrap a throwing call in `Try` INSIDE `body`.
    */
  def capture[A](body: => A): (A, Vector[Event]) = {
    val logger = logbackLogger(LoggerName)
    val appender = new ListAppender[ILoggingEvent]() {
      override protected def append(e: ILoggingEvent): Unit = {
        e.prepareForDeferredProcessing()
        super.append(e)
      }
    }
    appender.start()
    val previousLevel = logger.getLevel
    val previousAdditive = logger.isAdditive
    logger.setLevel(Level.TRACE)
    // the root appenders would print every DEBUG line of the window
    logger.setAdditive(false)
    logger.addAppender(appender)
    try {
      val result = body
      logger.detachAppender(appender)
      val events = appender.synchronized(appender.list.asScala.map(event).toVector)
      (result, events)
    } finally {
      logger.detachAppender(appender) // idempotent
      appender.stop()
      logger.setAdditive(previousAdditive)
      logger.setLevel(previousLevel)
    }
  }

  private[this] def event(e: ILoggingEvent): Event =
    Event(
      thread = e.getThreadName,
      level = e.getLevel.toString,
      logger = e.getLoggerName,
      message = String.valueOf(e.getFormattedMessage),
      arguments = Option(e.getArgumentArray).toSeq.flatten.map(String.valueOf),
      throwable = throwable(e.getThrowableProxy)
    )

  private[this] def throwable(proxy: IThrowableProxy): String =
    if (proxy == null) ""
    else {
      val suppressed = Option(proxy.getSuppressed).toSeq.flatten.map(throwable)
      (Seq(s"${proxy.getClassName}: ${proxy.getMessage}") ++
      Option(proxy.getStackTraceElementProxyArray).toSeq.flatten.map(_.getSTEAsString) ++
      suppressed :+ throwable(proxy.getCause)).mkString("\n")
    }

  private[this] val bindingWaitNanos = 10000000000L // 10 s

  /** slf4j 2 hands a `SubstituteLogger` to other threads while its one-time binding runs: wait,
    * bounded, for the real `LoggerContext` (see `LogbackCapture`).
    */
  private[this] def logbackLogger(name: String): LogbackLogger = {
    val deadline = System.nanoTime() + bindingWaitNanos
    var factory = LoggerFactory.getILoggerFactory
    while (!factory.isInstanceOf[LoggerContext] && System.nanoTime() < deadline) {
      Thread.sleep(20)
      factory = LoggerFactory.getILoggerFactory
    }
    factory match {
      case context: LoggerContext => context.getLogger(name)
      case other =>
        throw new IllegalStateException(
          s"slf4j is not bound to logback (${other.getClass.getName}): " +
          "logback-classic must be on the test classpath"
        )
    }
  }
}
