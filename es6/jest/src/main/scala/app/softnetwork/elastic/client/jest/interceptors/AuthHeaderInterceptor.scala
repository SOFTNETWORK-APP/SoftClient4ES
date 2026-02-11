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

package app.softnetwork.elastic.client.jest.interceptors

import app.softnetwork.elastic.client.{ApiKeyAuth, BearerTokenAuth, ElasticCredentials}
import com.typesafe.scalalogging.LazyLogging
import org.apache.http.{HttpRequest, HttpRequestInterceptor}
import org.apache.http.protocol.HttpContext

/** HTTP Request Interceptor for adding authentication headers */
class AuthHeaderInterceptor(credentials: ElasticCredentials)
    extends HttpRequestInterceptor
    with LazyLogging {

  override def process(request: HttpRequest, context: HttpContext): Unit = {
    credentials.authMethod match {
      case Some(ApiKeyAuth) if credentials.apiKey.exists(_.nonEmpty) =>
        request.addHeader("Authorization", ApiKeyAuth.createAuthHeader(credentials))
        logger.debug("Added API Key header to request")

      case Some(BearerTokenAuth) if credentials.bearerToken.exists(_.nonEmpty) =>
        request.addHeader("Authorization", BearerTokenAuth.createAuthHeader(credentials))
        logger.debug("Added Bearer Token header to request")

      case _ =>
        // Basic Auth handled by defaultCredentials
        logger.debug("No custom auth header added (using Basic Auth or no auth)")
    }
  }
}
