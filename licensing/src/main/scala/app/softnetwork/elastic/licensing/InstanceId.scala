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

package app.softnetwork.elastic.licensing

import com.typesafe.scalalogging.LazyLogging

import java.nio.file.{Files, Path, Paths}
import java.util.UUID

object InstanceId extends LazyLogging {

  def getOrCreate(cacheDir: String): String = {
    try {
      val dir = Paths.get(cacheDir)
      if (!Files.exists(dir)) {
        Files.createDirectories(dir)
      }
      val file: Path = dir.resolve("instance-id")
      if (Files.exists(file)) {
        val content = new String(Files.readAllBytes(file), "UTF-8").trim
        if (content.nonEmpty) {
          return content
        }
      }
      val id = UUID.randomUUID().toString
      Files.write(file, id.getBytes("UTF-8"))
      id
    } catch {
      case e: Exception =>
        logger.warn(s"Could not persist instance ID: ${e.getMessage}")
        UUID.randomUUID().toString
    }
  }
}
