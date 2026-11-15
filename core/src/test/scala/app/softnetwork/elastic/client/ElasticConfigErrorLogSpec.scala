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

import app.softnetwork.elastic.client.spi.ClientLogCapture
import com.typesafe.config.{ConfigException, ConfigFactory}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A configuration that cannot be read is logged by key, never by value: the value may be a secret
  * set under the wrong key, and a log line travels further than the exception its caller receives.
  */
class ElasticConfigErrorLogSpec extends AnyFlatSpec with Matchers {

  // letters only: Typesafe finds no number in it and quotes the whole value back
  // ("No number in duration value '…'"); a value with digits would be split into number and unit
  private val secret = "hunterTwoSecretValue"

  "ElasticConfig" should "log the key it could not read, never the value" in {
    val hocon = s"""elastic.connection-timeout = "$secret""""
    val (thrown, events) = ClientLogCapture.capture(
      intercept[ConfigException](ElasticConfig(ConfigFactory.parseString(hocon)))
    )

    // the premise: this failure's message quotes the value (the exception the caller receives is
    // unchanged), so a log line built from the message would carry it too
    thrown.getMessage should include(secret)

    val logged = events.filter(_.logger.startsWith(classOf[ElasticConfig].getName))
    logged should not be empty
    val text = logged.map(_.text).mkString("\n")
    text should include("connection-timeout")
    text should not include secret
  }
}
