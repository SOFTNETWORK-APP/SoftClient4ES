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

package app.softnetwork.elastic.persistence.query

import app.softnetwork.persistence.person.message.PersonEvent
import app.softnetwork.persistence.person.model.Person
import app.softnetwork.persistence.person.query.PersonToExternalProcessorStream
import app.softnetwork.persistence.query.{InMemoryJournalProvider, InMemoryOffsetProvider}

trait PersonToElasticProcessorStream
    extends State2ElasticProcessorStream[Person, PersonEvent]
    with PersonToExternalProcessorStream
    with InMemoryJournalProvider
    with InMemoryOffsetProvider
    with ElasticProvider[Person]
