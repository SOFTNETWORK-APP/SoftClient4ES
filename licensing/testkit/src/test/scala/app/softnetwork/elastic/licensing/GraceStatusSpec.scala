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

import ch.qos.logback.classic.{Level, Logger}
import ch.qos.logback.classic.spi.ILoggingEvent
import ch.qos.logback.core.read.ListAppender
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.LoggerFactory

import java.util.Date
import scala.jdk.CollectionConverters._

class GraceStatusSpec extends AnyFlatSpec with Matchers {

  private def manager = new JwtLicenseManager(
    publicKeyOverride = Some(JwtTestHelper.publicKey)
  )

  private val gracePeriod14d = java.time.Duration.ofDays(14)

  private def validProJwt: String =
    JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder().build())

  private def expiredJwt(daysAgo: Int): String = {
    val expDate = new Date(System.currentTimeMillis() - daysAgo.toLong * 24 * 3600000L)
    JwtTestHelper.signJwt(JwtTestHelper.proClaimsBuilder(expDate).build())
  }

  private def withLogCapture[T](f: ListAppender[ILoggingEvent] => T): T = {
    val loggerName = classOf[JwtLicenseManager].getName
    val logger = LoggerFactory.getLogger(loggerName).asInstanceOf[Logger]
    val appender = new ListAppender[ILoggingEvent]()
    appender.start()
    logger.addAppender(appender)
    try {
      f(appender)
    } finally {
      logger.detachAppender(appender)
      appender.stop()
    }
  }

  // --- GraceStatus computation (AC #1, #2, #5) ---

  "JwtLicenseManager with valid non-expired JWT" should "have NotInGrace status" in {
    val m = manager
    val jwt = validProJwt
    m.validateWithGracePeriod(jwt, gracePeriod14d)
    m.graceStatus shouldBe GraceStatus.NotInGrace
  }

  "JwtLicenseManager with JWT expired 1 day ago" should "have EarlyGrace status" in {
    val m = manager
    val jwt = expiredJwt(1)
    m.validateWithGracePeriod(jwt, gracePeriod14d)
    m.graceStatus shouldBe a[GraceStatus.EarlyGrace]
    m.graceStatus.asInstanceOf[GraceStatus.EarlyGrace].daysExpired shouldBe 1L
  }

  "JwtLicenseManager with JWT expired 6 days ago" should "have EarlyGrace status" in {
    val m = manager
    val jwt = expiredJwt(6)
    m.validateWithGracePeriod(jwt, gracePeriod14d)
    m.graceStatus shouldBe a[GraceStatus.EarlyGrace]
    m.graceStatus.asInstanceOf[GraceStatus.EarlyGrace].daysExpired shouldBe 6L
  }

  "JwtLicenseManager with JWT expired 7 days ago" should "have MidGrace status" in {
    val m = manager
    val jwt = expiredJwt(7)
    m.validateWithGracePeriod(jwt, gracePeriod14d)
    m.graceStatus shouldBe a[GraceStatus.MidGrace]
    val mg = m.graceStatus.asInstanceOf[GraceStatus.MidGrace]
    mg.daysExpired shouldBe 7L
    mg.daysRemaining shouldBe 7L
  }

  "JwtLicenseManager with JWT expired 13 days ago" should "have MidGrace status" in {
    val m = manager
    val jwt = expiredJwt(13)
    m.validateWithGracePeriod(jwt, gracePeriod14d)
    m.graceStatus shouldBe a[GraceStatus.MidGrace]
    val mg = m.graceStatus.asInstanceOf[GraceStatus.MidGrace]
    mg.daysExpired shouldBe 13L
    mg.daysRemaining shouldBe 1L
  }

  "JwtLicenseManager after validate() (no grace)" should "have NotInGrace status" in {
    val m = manager
    val jwt = validProJwt
    m.validate(jwt)
    m.graceStatus shouldBe GraceStatus.NotInGrace
  }

  "JwtLicenseManager after resetToCommunity" should "have NotInGrace status" in {
    val m = manager
    val jwt = expiredJwt(7)
    m.validateWithGracePeriod(jwt, gracePeriod14d)
    m.graceStatus shouldBe a[GraceStatus.MidGrace]
    m.resetToCommunity()
    m.graceStatus shouldBe GraceStatus.NotInGrace
  }

  // --- Custom grace period scaling (AC #5) ---

  "JwtLicenseManager with 30-day grace period" should "have earlyThreshold at 15 days" in {
    val m = manager
    val jwt = expiredJwt(10)
    val gracePeriod30d = java.time.Duration.ofDays(30)
    m.validateWithGracePeriod(jwt, gracePeriod30d)
    m.graceStatus shouldBe a[GraceStatus.EarlyGrace] // 10 < 15 (30/2)
  }

  "JwtLicenseManager with 30-day grace period, expired 20 days" should "have MidGrace" in {
    val m = manager
    val jwt = expiredJwt(20)
    val gracePeriod30d = java.time.Duration.ofDays(30)
    m.validateWithGracePeriod(jwt, gracePeriod30d)
    m.graceStatus shouldBe a[GraceStatus.MidGrace]
    val mg = m.graceStatus.asInstanceOf[GraceStatus.MidGrace]
    mg.daysExpired shouldBe 20L
    mg.daysRemaining shouldBe 10L
  }

  // --- wasDegraded (AC #4) ---

  "JwtLicenseManager.wasDegraded" should "be false initially" in {
    val m = manager
    m.wasDegraded shouldBe false
  }

  "JwtLicenseManager.wasDegraded" should "be true after resetToCommunity" in {
    val m = manager
    m.resetToCommunity()
    m.wasDegraded shouldBe true
  }

  "JwtLicenseManager.wasDegraded" should "be false after successful validate" in {
    val m = manager
    m.resetToCommunity()
    m.wasDegraded shouldBe true
    m.validate(validProJwt)
    m.wasDegraded shouldBe false
  }

  "JwtLicenseManager.wasDegraded" should "be false after successful validateWithGracePeriod" in {
    val m = manager
    m.resetToCommunity()
    m.wasDegraded shouldBe true
    val jwt = expiredJwt(5) // within grace
    m.validateWithGracePeriod(jwt, gracePeriod14d)
    m.wasDegraded shouldBe false
  }

  // --- warnIfInGrace (AC #2) ---

  private val degradationMessagePattern = "degrade to Community"

  "JwtLicenseManager.warnIfInGrace with non-expired JWT" should "not emit a degradation warning" in {
    val m = manager
    withLogCapture { appender =>
      m.validate(validProJwt)
      appender.list.clear()
      m.warnIfInGrace()
      m.graceStatus shouldBe GraceStatus.NotInGrace
      val degradationWarnings = appender.list.asScala
        .filter(e =>
          e.getLevel == Level.WARN && e.getFormattedMessage.contains(degradationMessagePattern)
        )
      degradationWarnings shouldBe empty
    }
  }

  "JwtLicenseManager.warnIfInGrace with early grace JWT" should "not emit a degradation warning" in {
    val m = manager
    withLogCapture { appender =>
      m.validateWithGracePeriod(expiredJwt(3), gracePeriod14d)
      appender.list.clear()
      m.warnIfInGrace()
      m.graceStatus shouldBe a[GraceStatus.EarlyGrace]
      val degradationWarnings = appender.list.asScala
        .filter(e =>
          e.getLevel == Level.WARN && e.getFormattedMessage.contains(degradationMessagePattern)
        )
      degradationWarnings shouldBe empty
    }
  }

  "JwtLicenseManager.warnIfInGrace with mid-grace JWT" should "emit a degradation warning" in {
    val m = manager
    withLogCapture { appender =>
      m.validateWithGracePeriod(expiredJwt(10), gracePeriod14d)
      appender.list.clear()
      m.warnIfInGrace()
      m.graceStatus shouldBe a[GraceStatus.MidGrace]
      val degradationWarnings = appender.list.asScala
        .filter(e =>
          e.getLevel == Level.WARN && e.getFormattedMessage.contains(degradationMessagePattern)
        )
      degradationWarnings should not be empty
    }
  }
}
