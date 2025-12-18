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

package app.softnetwork.elastic.sql

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.node.ObjectNode
import com.fasterxml.jackson.databind.{
  DeserializationFeature,
  JsonNode,
  ObjectMapper,
  SerializationFeature
}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.language.implicitConversions
import scala.jdk.CollectionConverters._

package object serialization {

  /** Jackson ObjectMapper configuration */
  object JacksonConfig {
    lazy val objectMapper: ObjectMapper = {
      val mapper = new ObjectMapper()

      // Scala module for native support of Scala types
      mapper.registerModule(DefaultScalaModule)

      // Java Time module for java.time.Instant, LocalDateTime, etc.
      mapper.registerModule(new JavaTimeModule())

      // Setup for performance
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
      mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false)
      mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)

      // Ignores null values in serialization
      mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL)

      // Optimizations
      mapper.configure(SerializationFeature.INDENT_OUTPUT, false) // No pretty print
      mapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, false)

      mapper
    }
  }

  implicit def objectValueToObjectNode(value: ObjectValue): ObjectNode = {
    import JacksonConfig.{objectMapper => mapper}
    val node = mapper.createObjectNode()
    updateNode(node, value.value)
    node
  }

  private[sql] def updateNode(node: ObjectNode, updates: Map[String, Value[_]]): ObjectNode = {
    import JacksonConfig.{objectMapper => mapper}

    updates.foreach { case (k, v) =>
      v match {
        case Null                 => node.putNull(k)
        case BooleanValue(b)      => node.put(k, b)
        case StringValue(s)       => node.put(k, s)
        case ByteValue(b)         => node.put(k, b)
        case ShortValue(s)        => node.put(k, s)
        case IntValue(i)          => node.put(k, i)
        case LongValue(l)         => node.put(k, l)
        case DoubleValue(d)       => node.put(k, d)
        case FloatValue(f)        => node.put(k, f)
        case IdValue              => node.put(k, s"${v.value}")
        case IngestTimestampValue => node.put(k, s"${v.value}")
        case v: Values[_, _] =>
          val arrayNode = mapper.createArrayNode()
          v.values.foreach {
            case Null            => arrayNode.addNull()
            case BooleanValue(b) => arrayNode.add(b)
            case StringValue(s)  => arrayNode.add(s)
            case ByteValue(b)    => arrayNode.add(b)
            case ShortValue(s)   => arrayNode.add(s)
            case IntValue(i)     => arrayNode.add(i)
            case LongValue(l)    => arrayNode.add(l)
            case DoubleValue(d)  => arrayNode.add(d)
            case FloatValue(f)   => arrayNode.add(f)
            case ObjectValue(o)  => arrayNode.add(updateNode(mapper.createObjectNode(), o))
            case _               => // do nothing
          }
          node.set(k, arrayNode)
        case ObjectValue(value) =>
          if (value.nonEmpty)
            node.set(k, updateNode(mapper.createObjectNode(), value))
        case _ => // do nothing
      }
    }
    node
  }

  implicit def jsonNodeToObjectValue(
    node: JsonNode
  )(implicit ignoredKeys: Set[String] = Set.empty): ObjectValue = {
    ObjectValue(extractObject(node, ignoredKeys))
  }

  private[elastic] def extractObject(
    node: JsonNode,
    ignoredKeys: Set[String] = Set.empty
  ): Map[String, Value[_]] = {
    node
      .properties()
      .asScala
      .flatMap { entry =>
        val key = entry.getKey
        val value = entry.getValue

        if (ignoredKeys.contains(key)) {
          None
        } else {
          Value(value).map(key -> Value(_))
        }
      }
      .toMap
  }

}
