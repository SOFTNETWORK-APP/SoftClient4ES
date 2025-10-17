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

package app.softnetwork.elastic.scalatest

import org.scalatest.Suite
import pl.allegro.tech.embeddedelasticsearch.EmbeddedElastic
import pl.allegro.tech.embeddedelasticsearch.PopularProperties._

import java.net.ServerSocket
import java.util.UUID
import java.util.concurrent.TimeUnit
import scala.reflect.io.Path

trait EmbeddedElasticTestKit extends ElasticTestKit { _: Suite =>

  override lazy val elasticURL: String = s"http://127.0.0.1:${embeddedElastic.getHttpPort}"

  override def stop(): Unit = embeddedElastic.stop()

  private[this] def dynamicPort: Int = {
    val socket = new ServerSocket(0)
    val port = socket.getLocalPort
    socket.close()
    port
  }

  private[this] val embeddedElastic: EmbeddedElastic = EmbeddedElastic
    .builder()
    .withElasticVersion(elasticVersion)
    .withSetting(HTTP_PORT, dynamicPort)
    .withSetting(CLUSTER_NAME, clusterName)
    .withInstallationDirectory(Path(s"target/embedded-elastic-${UUID.randomUUID.toString}").jfile)
    .withCleanInstallationDirectoryOnStop(true)
    .withEsJavaOpts("-Xms128m -Xmx512m")
    .withStartTimeout(2, TimeUnit.MINUTES)
    .build()
    .start()

}
