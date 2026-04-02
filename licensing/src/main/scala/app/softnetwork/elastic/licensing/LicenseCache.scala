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

import java.nio.file.{Files, Paths, StandardCopyOption}
import java.nio.file.attribute.PosixFilePermissions

import com.typesafe.scalalogging.LazyLogging

class LicenseCache(cacheDir: String) extends LazyLogging {

  import LicenseCache._

  def read(): Option[String] = {
    try {
      val file = Paths.get(cacheDir).resolve(CacheFileName)
      if (!Files.exists(file)) return None
      if (Files.isSymbolicLink(file)) {
        logger.warn("License cache file is a symbolic link, ignoring")
        return None
      }
      val size = Files.size(file)
      if (size > MaxCacheFileSizeBytes) {
        logger.warn(s"License cache file is unexpectedly large ($size bytes), ignoring")
        return None
      }
      val content = new String(Files.readAllBytes(file), "UTF-8").trim
      if (content.isEmpty) None else Some(content)
    } catch {
      case e: Exception =>
        logger.warn(s"Could not read license cache: ${e.getMessage}")
        None
    }
  }

  def write(jwt: String): Unit = {
    try {
      val dir = Paths.get(cacheDir)
      if (!Files.exists(dir)) {
        try {
          Files.createDirectories(
            dir,
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------"))
          )
        } catch {
          case _: UnsupportedOperationException =>
            Files.createDirectories(dir)
        }
      }
      val target = dir.resolve(CacheFileName)
      val tmp =
        try {
          Files.createTempFile(
            dir,
            ".license-cache",
            ".tmp",
            PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------"))
          )
        } catch {
          case _: UnsupportedOperationException =>
            Files.createTempFile(dir, ".license-cache", ".tmp")
        }
      try {
        Files.write(tmp, jwt.getBytes("UTF-8"))
        try {
          Files.move(
            tmp,
            target,
            StandardCopyOption.REPLACE_EXISTING,
            StandardCopyOption.ATOMIC_MOVE
          )
        } catch {
          case _: java.nio.file.AtomicMoveNotSupportedException =>
            logger.warn(
              "Filesystem does not support atomic move — using non-atomic overwrite"
            )
            Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING)
        }
      } catch {
        case e: Exception =>
          try { Files.deleteIfExists(tmp) }
          catch { case _: Exception => }
          throw e
      }
    } catch {
      case e: Exception =>
        logger.warn(s"Could not write license cache: ${e.getMessage}")
    }
  }

  def delete(): Unit = {
    try {
      val file = Paths.get(cacheDir).resolve(CacheFileName)
      Files.deleteIfExists(file)
    } catch {
      case e: Exception =>
        logger.warn(s"Could not delete license cache: ${e.getMessage}")
    }
  }
}

object LicenseCache {
  private[licensing] val CacheFileName: String = "license-cache.jwt"
  private val MaxCacheFileSizeBytes: Long = 65536L
}
