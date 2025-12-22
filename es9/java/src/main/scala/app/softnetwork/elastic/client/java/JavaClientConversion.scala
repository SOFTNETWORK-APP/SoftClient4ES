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

package app.softnetwork.elastic.client.java

import app.softnetwork.elastic.client.SerializationApi
import co.elastic.clients.json.JsonpSerializable
import co.elastic.clients.json.jackson.JacksonJsonpMapper

import java.io.{IOException, StringWriter}
import scala.util.Try

trait JavaClientConversion { _: JavaClientCompanion with SerializationApi =>
  private[this] val jsonpMapper = new JacksonJsonpMapper(mapper)

  /** Convert any Elasticsearch response to JSON string */
  protected def convertToJson[T <: JsonpSerializable](response: T): String = {
    val stringWriter = new StringWriter()
    val generator = jsonpMapper.jsonProvider().createGenerator(stringWriter)
    try {
      response.serialize(generator, jsonpMapper)
      generator.flush()
      stringWriter.toString
    } catch {
      case ex: Exception =>
        logger.error(s"Failed to convert response to JSON: ${ex.getMessage}", ex)
        throw new IOException("Failed to serialize Elasticsearch response", ex)
    } finally {
      Try(generator.close()).failed.foreach { ex =>
        logger.warn(s"Failed to close JSON generator: ${ex.getMessage}")
      }
    }
  }
}
