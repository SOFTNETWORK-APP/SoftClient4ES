package app.softnetwork.elastic.sql

import com.fasterxml.jackson.annotation.JsonInclude
import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper, SerializationFeature}
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule
import com.fasterxml.jackson.module.scala.DefaultScalaModule

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

}
