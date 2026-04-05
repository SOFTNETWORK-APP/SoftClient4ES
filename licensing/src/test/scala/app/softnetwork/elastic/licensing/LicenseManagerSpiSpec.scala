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

import java.util.ServiceLoader

import scala.jdk.CollectionConverters._

import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class LicenseManagerSpiSpec extends AnyFlatSpec with Matchers {

  "ServiceLoader" should "discover CommunityLicenseManagerSpi" in {
    val loader = ServiceLoader.load(classOf[LicenseManagerSpi])
    val spis = loader.iterator().asScala.toSeq
    spis should not be empty
    spis.exists(_.isInstanceOf[CommunityLicenseManagerSpi]) shouldBe true
  }

  "CommunityLicenseManagerSpi" should "have Int.MaxValue priority" in {
    new CommunityLicenseManagerSpi().priority shouldBe Int.MaxValue
  }

  it should "create a CommunityLicenseManager regardless of mode" in {
    val config = ConfigFactory.load()
    val spi = new CommunityLicenseManagerSpi()

    // No mode
    val mgr1 = spi.create(config)
    mgr1.licenseType shouldBe LicenseType.Community
    mgr1.refresh() shouldBe Left(RefreshNotSupported)

    // LongRunning mode — still Community
    val mgr2 = spi.create(config, Some(LicenseMode.LongRunning))
    mgr2.licenseType shouldBe LicenseType.Community

    // Driver mode — still Community
    val mgr3 = spi.create(config, Some(LicenseMode.Driver))
    mgr3.licenseType shouldBe LicenseType.Community
  }

  /** Mirrors the SPI resolution logic in ExtensionApi.licenseManager */
  private def resolveManager(
    spis: Seq[LicenseManagerSpi],
    mode: Option[LicenseMode] = None
  ): LicenseManager = {
    val config = ConfigFactory.load()
    spis
      .sortBy(_.priority)
      .headOption
      .flatMap { spi =>
        try { Some(spi.create(config, mode)) }
        catch { case _: Exception => None }
      }
      .getOrElse(new CommunityLicenseManager())
  }

  "SPI resolution" should "pick the lowest-priority SPI" in {
    val lowPriority = new LicenseManagerSpi {
      override def priority: Int = 10
      override def create(
        config: Config,
        mode: Option[LicenseMode]
      ): LicenseManager = new CommunityLicenseManager() {
        override def licenseType: LicenseType = LicenseType.Pro
      }
    }
    val community = new CommunityLicenseManagerSpi()

    val mgr = resolveManager(Seq(community, lowPriority))
    mgr.licenseType shouldBe LicenseType.Pro
  }

  it should "fall back to CommunityLicenseManager when SPI list is empty" in {
    val mgr = resolveManager(Seq.empty)
    mgr shouldBe a[CommunityLicenseManager]
    mgr.licenseType shouldBe LicenseType.Community
  }

  it should "fall back to CommunityLicenseManager when winning SPI throws" in {
    val broken = new LicenseManagerSpi {
      override def priority: Int = 1
      override def create(
        config: Config,
        mode: Option[LicenseMode]
      ): LicenseManager = throw new RuntimeException("boom")
    }

    val mgr = resolveManager(Seq(broken))
    mgr shouldBe a[CommunityLicenseManager]
    mgr.licenseType shouldBe LicenseType.Community
  }

  it should "pass licenseMode to the winning SPI" in {
    var receivedMode: Option[LicenseMode] = None
    val spy = new LicenseManagerSpi {
      override def priority: Int = 1
      override def create(
        config: Config,
        mode: Option[LicenseMode]
      ): LicenseManager = {
        receivedMode = mode
        new CommunityLicenseManager()
      }
    }

    resolveManager(Seq(spy), Some(LicenseMode.LongRunning))
    receivedMode shouldBe Some(LicenseMode.LongRunning)

    resolveManager(Seq(spy), Some(LicenseMode.Driver))
    receivedMode shouldBe Some(LicenseMode.Driver)

    resolveManager(Seq(spy), None)
    receivedMode shouldBe None
  }
}
