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

package app.softnetwork.elastic.sql.config

import configs.ConfigError

/** What a configuration failure may write to the log: each key that could not be read and the kind
  * of failure, never the failure's message. A Typesafe "bad value" message quotes the value it
  * could not read, and that value may be a secret set under the wrong key.
  *
  * The exception thrown to the caller is not this summary: it keeps the library's message, which
  * names what is wrong (a `require` of the settings, a missing key) for the caller who supplied the
  * configuration.
  *
  * The kind is the throwable's full class name: `getSimpleName` can throw on JDK 8 for a Scala
  * class nested in an object, and is empty for an anonymous class.
  */
private[elastic] object ConfigErrorKeys {

  def apply(error: ConfigError): String =
    error.entries
      .map(entry => s"${entry.paths.mkString(".")} (${entry.throwable.getClass.getName})")
      .distinct
      .mkString(", ")
}
