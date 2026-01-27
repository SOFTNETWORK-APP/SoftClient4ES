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

package app.softnetwork.elastic.client.jest

import app.softnetwork.elastic.client._
import app.softnetwork.elastic.sql.bridge._
import io.searchbox.action.BulkableAction
import io.searchbox.core._
import org.json4s.Formats

import scala.jdk.CollectionConverters._
import scala.language.implicitConversions

/** Created by smanciot on 20/05/2021.
  */
trait JestClientApi
    extends ElasticClientApi
    with JestIndicesApi
    with JestAliasApi
    with JestSettingsApi
    with JestMappingApi
    with JestRefreshApi
    with JestFlushApi
    with JestCountApi
    with JestIndexApi
    with JestUpdateApi
    with JestDeleteApi
    with JestGetApi
    with JestSearchApi
    with JestScrollApi
    with JestBulkApi
    with JestVersionApi
    with JestPipelineApi
    with JestTemplateApi
    with JestEnrichPolicyApi
    with JestTransformApi
    with JestClientCompanion
