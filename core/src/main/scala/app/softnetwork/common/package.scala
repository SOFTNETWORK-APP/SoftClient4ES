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

package app.softnetwork

import org.slf4j.Logger

import java.io.Closeable
import java.net.URI
import scala.util.Try

package object common {

  trait ClientCompanion extends Closeable with UrlValidator { _: { def logger: Logger } =>

    /** Check if client is initialized and connected
      */
    def isInitialized: Boolean

    /** Test connection
      * @return
      *   true if connection is successful
      */
    def testConnection(): Boolean
  }

  trait UrlValidator {

    /** Validate URL format using java.net.URI
      */
    def validateUrl(url: String): Try[URI] = {
      Try {
        if (url == null || url.trim.isEmpty) {
          throw new IllegalArgumentException("URL cannot be null or empty")
        }

        val uri = new URI(url)

        // Vérifier le schéma
        if (uri.getScheme == null) {
          throw new IllegalArgumentException(
            s"URL must have a scheme (http:// or https://): $url"
          )
        }

        val scheme = uri.getScheme.toLowerCase
        if (scheme != "http" && scheme != "https") {
          throw new IllegalArgumentException(
            s"URL scheme must be http or https, got: $scheme"
          )
        }

        // Vérifier l'hôte
        if (uri.getHost == null || uri.getHost.trim.isEmpty) {
          throw new IllegalArgumentException(
            s"URL must have a valid hostname: $url"
          )
        }

        // Vérifier le port si présent
        if (uri.getPort != -1) {
          if (uri.getPort < 0 || uri.getPort > 65535) {
            throw new IllegalArgumentException(
              s"Invalid port number: ${uri.getPort} (must be between 0 and 65535)"
            )
          }
        }

        uri
      }
    }
  }
}
