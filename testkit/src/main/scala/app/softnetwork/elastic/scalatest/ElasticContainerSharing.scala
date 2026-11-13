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

/** Whether a suite may run on its test JVM's shared Elasticsearch container.
  *
  * When the system property `softclient4es.testkit.shared-container` is `true`,
  * [[ElasticDockerTestKit]] runs every suite whose `sharedContainer` is `true` on one container per
  * forked test JVM and per configuration, reset before each suite (see
  * [[SharedElasticContainers]]). A suite that asserts on the WHOLE cluster (every index, every
  * watcher) overrides it to `false` and keeps a container of its own.
  *
  * 🔴 The member lives HERE and nowhere else. A suite trait whose self-type is `ElasticTestKit`
  * cannot override a member of `ElasticTestKit`, but it can extend this trait and override this
  * member; `ElasticDockerTestKit` extends it too and only READS it, so by linearization the suite
  * trait's override wins in both mixin orders (`X extends Spec with ElasticDockerTestKit` and the
  * reverse). A concrete `sharedContainer` added to `ElasticDockerTestKit` would win silently.
  */
trait ElasticContainerSharing {

  def sharedContainer: Boolean = true

}
