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

import app.softnetwork.elastic.licensing.ClassLoaderIsolation
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** #258 (found by the ElasticClientFactory isolation spec): `ElasticConfig.apply` supplies the
  * `elastic.*` defaults by loading `softnetwork-elastic.conf` itself, and
  * `ConfigFactory.load(name)` resolves that resource through the thread context classloader. Under
  * a host-owned blind TCCL the factory therefore failed on configuration BEFORE its `ServiceLoader`
  * ever ran. The resource must resolve against the classloader that loaded softclient4es-core,
  * whatever the TCCL.
  */
class ElasticConfigIsolationSpec extends AnyFlatSpec with Matchers {

  private val defaultsResource = "softnetwork-elastic.conf"

  behavior of "ElasticConfig defaults resolution (#258)"

  it should "resolve the softnetwork-elastic.conf defaults under a blind context classloader" in {
    // Built BEFORE the swap (ConfigFactory.load() is itself TCCL-bound). Precondition, asserted not
    // assumed: the application config carries NO `elastic.*` key of its own, so every value below
    // has to come from the defaults resource.
    val application: Config = ConfigFactory.load()
    application.hasPath("elastic") shouldBe false

    val blind = ClassLoaderIsolation.blindLoader()
    blind.getResource(defaultsResource) shouldBe null

    val expected = ElasticConfig(application)
    val underBlindTccl =
      ClassLoaderIsolation.withContextClassLoader(blind)(ElasticConfig(application))

    underBlindTccl shouldBe expected
  }
}
