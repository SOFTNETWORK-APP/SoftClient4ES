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

import app.softnetwork.elastic.sql.transform.TransformState
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import com.fasterxml.jackson.databind.ObjectMapper
import org.json4s.ext.{JavaTimeSerializers, JavaTypesSerializers, JodaTimeSerializers}
import org.json4s.{jackson, CustomSerializer, Formats, JNull, JString, NoTypeHints}
import org.json4s.jackson.Serialization

trait SerializationApi {
  implicit val serialization: Serialization.type = jackson.Serialization

  val mapper: ObjectMapper = JacksonConfig.objectMapper

  def defaultFormats: Formats = Serialization.formats(NoTypeHints) ++
    JodaTimeSerializers.all ++
    JavaTypesSerializers.all ++
    JavaTimeSerializers.all

  def customSerializers: Seq[SerializationApi.TransformStateSerializer] =
    Seq(new SerializationApi.TransformStateSerializer)

  implicit val formats: Formats = defaultFormats ++ customSerializers
}

object SerializationApi extends SerializationApi {
  class TransformStateSerializer
      extends CustomSerializer[TransformState](_ =>
        (
          {
            // Deserializer
            case JString(s) => TransformState(s)
            case JNull      => TransformState.Stopped
          },
          {
            // Serializer
            case state: TransformState => JString(state.name)
          }
        )
      )

}
