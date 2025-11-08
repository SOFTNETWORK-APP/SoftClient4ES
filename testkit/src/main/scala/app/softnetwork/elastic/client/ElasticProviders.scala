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

import app.softnetwork.elastic.model.{Binary, Parent, Sample}
import app.softnetwork.elastic.persistence.query.ElasticProvider
import app.softnetwork.persistence.ManifestWrapper
import app.softnetwork.persistence.person.model.Person
import com.typesafe.config.Config

object ElasticProviders {

  class PersonProvider(conf: Config) extends ElasticProvider[Person] with ManifestWrapper[Person] {
    override protected val manifestWrapper: ManifestW = ManifestW()

    override lazy val config: Config = conf

  }

  class SampleProvider(conf: Config) extends ElasticProvider[Sample] with ManifestWrapper[Sample] {
    override protected val manifestWrapper: ManifestW = ManifestW()

    override lazy val config: Config = conf

  }

  class BinaryProvider(conf: Config) extends ElasticProvider[Binary] with ManifestWrapper[Binary] {
    override protected val manifestWrapper: ManifestW = ManifestW()

    override lazy val config: Config = conf

  }

  class ParentProvider(conf: Config) extends ElasticProvider[Parent] with ManifestWrapper[Parent] {
    override protected val manifestWrapper: ManifestW = ManifestW()

    override lazy val config: Config = conf

  }
}
