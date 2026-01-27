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

import app.softnetwork.elastic.client.{result, TransformApi}
import app.softnetwork.elastic.sql.schema

trait JestTransformApi extends TransformApi with JestClientHelpers {
  _: JestVersionApi with JestClientCompanion =>

  override private[client] def executeCreateTransform(
    config: schema.TransformConfig,
    start: Boolean
  ): result.ElasticResult[Boolean] =
    result.ElasticFailure(
      result.ElasticError(
        message = "Transform creation not implemented for Jest client",
        operation = Some("CreateTransform"),
        statusCode = Some(501)
      )
    )

  override private[client] def executeDeleteTransform(
    transformId: String,
    force: Boolean
  ): result.ElasticResult[Boolean] =
    result.ElasticFailure(
      result.ElasticError(
        message = "Transform deletion not implemented for Jest client",
        operation = Some("DeleteTransform"),
        statusCode = Some(501)
      )
    )

  override private[client] def executeStartTransform(
    transformId: String
  ): result.ElasticResult[Boolean] =
    result.ElasticFailure(
      result.ElasticError(
        message = "Transform start not implemented for Jest client",
        operation = Some("StartTransform"),
        statusCode = Some(501)
      )
    )

  override private[client] def executeStopTransform(
    transformId: String,
    force: Boolean,
    waitForCompletion: Boolean
  ): result.ElasticResult[Boolean] =
    result.ElasticFailure(
      result.ElasticError(
        message = "Transform stop not implemented for Jest client",
        operation = Some("StopTransform"),
        statusCode = Some(501)
      )
    )

  override private[client] def executeGetTransformStats(
    transformId: String
  ): result.ElasticResult[Option[schema.TransformStats]] =
    result.ElasticFailure(
      result.ElasticError(
        message = "Get Transform stats not implemented for Jest client",
        operation = Some("GetTransformStats"),
        statusCode = Some(501)
      )
    )

  override private[client] def executeScheduleTransformNow(
    transformId: String
  ): result.ElasticResult[Boolean] =
    result.ElasticFailure(
      result.ElasticError(
        message = "Schedule Transform now not implemented for Jest client",
        operation = Some("ScheduleNow"),
        statusCode = Some(501)
      )
    )
}
