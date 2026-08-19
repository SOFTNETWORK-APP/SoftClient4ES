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

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.scaladsl.Source
import app.softnetwork.elastic.client.scroll.{ScrollConfig, ScrollMetrics}
import app.softnetwork.elastic.sql.query.{SearchStatement, SelectStatement}
import com.typesafe.config.{Config, ConfigFactory}
import org.json4s.{DefaultFormats, Formats}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import java.util.concurrent.atomic.AtomicReference
import scala.collection.immutable.ListMap

/** #238 — the `elastic.scroll { size, max-slices }` surface and how it reaches every scroll call
  * site: the HOCON block (run under `sbt "+ core/test"` — kxbmap 0.4.4 on 2.12, 0.6.1 on 2.13),
  * `ScrollApi.defaultScrollConfig` (a `def`), the client overrides, and the `scrollRows` routing
  * clamp (`maxSlices = Some(1)` on an explicit LIMIT, `None` otherwise).
  */
class ScrollSettingsSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("scroll-settings-spec")
  implicit val context: ConversionContext = NativeContext

  private val testLogger: Logger = LoggerFactory.getLogger(getClass)

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  // ---- HOCON -------------------------------------------------------------------------------

  "ElasticConfig" should "read elastic.scroll.size and elastic.scroll.max-slices" in {
    val cfg =
      ElasticConfig(ConfigFactory.parseString("elastic.scroll { size = 5000, max-slices = 2 }"))
    cfg.scroll.size shouldBe 5000
    cfg.scroll.maxSlices shouldBe 2
  }

  it should "default elastic.scroll to 1000 rows per page and a ceiling of 8 slices" in {
    val cfg = ElasticConfig(ConfigFactory.parseString("elastic { credentials { host = \"x\" } }"))
    cfg.scroll.size shouldBe 1000
    cfg.scroll.maxSlices shouldBe ScrollConfig.DefaultMaxSlices
    ScrollConfig.DefaultMaxSlices shouldBe 8
  }

  // ---- defaultScrollConfig ------------------------------------------------------------------

  /** A client whose HOCON comes from a string — exposes the protected ceiling for assertion. */
  private class ConfiguredClient(hocon: String) extends NopeClientApi {
    override protected def logger: Logger = testLogger
    override def config: Config = ConfigFactory.parseString(hocon)
    def ceiling: Int = configuredMaxSlices
  }

  "ScrollApi.defaultScrollConfig" should "be a def: every call carries fresh metrics" in {
    val client = new ConfiguredClient("")
    val a = client.defaultScrollConfig
    Thread.sleep(2)
    val b = client.defaultScrollConfig
    (a.metrics ne b.metrics) shouldBe true
    b.metrics.startTime should be > a.metrics.startTime
  }

  it should "apply the HOCON page size and leave the ceiling inherited (maxSlices = None)" in {
    val client = new ConfiguredClient("elastic.scroll { size = 250, max-slices = 3 }")
    val cfg = client.defaultScrollConfig
    cfg.scrollSize shouldBe 250
    cfg.maxSlices shouldBe None
    cfg.slices shouldBe 1
    client.ceiling shouldBe 3
  }

  it should "fall back to the reference defaults when nothing is configured" in {
    val client = new ConfiguredClient("")
    client.defaultScrollConfig.scrollSize shouldBe 1000
    client.ceiling shouldBe ScrollConfig.DefaultMaxSlices
  }

  "ScrollSettings" should "reject a non-positive page size or a ceiling below 1 at config load" in {
    an[IllegalArgumentException] should be thrownBy ScrollSettings(size = 0)
    an[IllegalArgumentException] should be thrownBy ScrollSettings(maxSlices = 0)
    the[IllegalArgumentException] thrownBy ScrollSettings(size = -5) should have message
    "requirement failed: elastic.scroll.size must be positive (ELASTIC_SCROLL_SIZE), got -5"
    // through the HOCON reader the load itself fails (kxbmap 0.6.1 surfaces the requirement
    // message; 0.4.4 on 2.12 falls back to the companion apply and reports a generic error)
    an[Exception] should be thrownBy ElasticConfig(
      ConfigFactory.parseString("elastic.scroll { size = 0 }")
    )
  }

  it should "derive the REST pool sizing from the slice ceiling with the Apache defaults as floor" in {
    ScrollSettings().restPoolPerRoute shouldBe 10
    ScrollSettings().restPoolTotal shouldBe 30
    ScrollSettings(maxSlices = 16).restPoolPerRoute shouldBe 18
    ScrollSettings(maxSlices = 16).restPoolTotal shouldBe 54
  }

  "ScrollConfig" should "default maxSlices to None and slices to 1" in {
    val cfg = ScrollConfig()
    cfg.maxSlices shouldBe None
    cfg.slices shouldBe 1
    ScrollMetrics().slices shouldBe 1
  }

  // ---- scrollRows routing ---------------------------------------------------------------------

  /** Records the config that reaches `scroll`. The override deliberately does NOT redeclare a
    * default argument: a redeclared default would shadow `defaultScrollConfig` for no-argument
    * calls through this subclass and hide what the production path actually sends.
    */
  private class RecordingClient extends NopeClientApi {
    override protected def logger: Logger = testLogger
    val scrolledConfig = new AtomicReference[ScrollConfig]()
    val scrolledStatement = new AtomicReference[SearchStatement]()

    override def scroll(
      statement: SearchStatement,
      config: ScrollConfig
    )(implicit
      system: ActorSystem,
      context: ConversionContext
    ): Source[(ListMap[String, Any], ScrollMetrics), NotUsed] = {
      scrolledStatement.set(statement)
      scrolledConfig.set(config)
      Source.empty[(ListMap[String, Any], ScrollMetrics)]
    }
  }

  "SearchApi.scrollRows" should "clamp maxSlices to Some(1) on an explicit LIMIT above the one-shot window" in {
    val client = new RecordingClient
    client.search(SelectStatement("SELECT id FROM idx LIMIT 11000"))
    val cfg = client.scrolledConfig.get()
    cfg should not be null
    cfg.maxSlices shouldBe Some(1)
    cfg.maxDocuments shouldBe Some(11000L)
    cfg.scrollSize shouldBe client.defaultScrollConfig.scrollSize
  }

  it should "leave maxSlices = None on a no-LIMIT row query (the configured ceiling applies)" in {
    val client = new RecordingClient
    client.search(SelectStatement("SELECT id FROM idx"))
    val cfg = client.scrolledConfig.get()
    cfg should not be null
    cfg.maxSlices shouldBe None
    cfg.maxDocuments shouldBe None
  }

  it should "reach scroll with the inherited defaultScrollConfig on the no-argument gateway path" in {
    val client = new RecordingClient
    client.scroll(SelectStatement("SELECT id FROM idx"))
    val cfg = client.scrolledConfig.get()
    cfg should not be null
    cfg.maxSlices shouldBe None
    cfg.scrollSize shouldBe 1000
  }

  // Macro applications do not support default arguments (scalac rejects an omitted `config` on
  // scrollAs outright), so the typed path reaches the HOCON defaults only through an explicit
  // `defaultScrollConfig`; the macro re-emits that argument verbatim into scrollAsUnchecked → scroll.
  // macros-tests cannot host this case (it does not depend on core, where the real macro lives).
  "ScrollApi.scrollAs" should "forward an explicit defaultScrollConfig through the macro expansion" in {
    implicit val formats: Formats = DefaultFormats
    val client = new RecordingClient
    client.scrollAs[ScrollSettingsSpec.IdRow]("SELECT id FROM idx", client.defaultScrollConfig)
    val cfg = client.scrolledConfig.get()
    cfg should not be null
    cfg.maxSlices shouldBe None
    cfg.scrollSize shouldBe client.defaultScrollConfig.scrollSize
  }

  it should "apply defaultScrollConfig on scrollAsUnchecked when the config is omitted" in {
    implicit val formats: Formats = DefaultFormats
    val client = new RecordingClient
    client.scrollAsUnchecked[ScrollSettingsSpec.IdRow](SelectStatement("SELECT id FROM idx"))
    val cfg = client.scrolledConfig.get()
    cfg should not be null
    cfg.maxSlices shouldBe None
    cfg.scrollSize shouldBe 1000
  }
}

object ScrollSettingsSpec {
  case class IdRow(id: String)
}
