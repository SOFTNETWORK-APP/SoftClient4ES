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

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfterEach

import java.nio.file.{Files, Path}
import java.util.Comparator

class LicenseCacheSpec extends AnyFlatSpec with Matchers with BeforeAndAfterEach {

  private var tempDir: Path = _

  override def beforeEach(): Unit = {
    tempDir = Files.createTempDirectory("license-cache-test")
  }

  override def afterEach(): Unit = {
    if (tempDir != null && Files.exists(tempDir)) {
      // Walk in reverse order (deepest first) to delete nested directories
      val stream = Files.walk(tempDir)
      try {
        stream.sorted(Comparator.reverseOrder[Path]()).forEach(f => Files.deleteIfExists(f))
      } finally { stream.close() }
    }
  }

  "LicenseCache write + read" should "round-trip a JWT string" in {
    val cache = new LicenseCache(tempDir.toString)
    val jwt = "eyJhbGciOiJFZERTQSJ9.eyJzdWIiOiJ0ZXN0In0.signature"
    cache.write(jwt)
    cache.read() shouldBe Some(jwt)
    Files.exists(tempDir.resolve(LicenseCache.CacheFileName)) shouldBe true
  }

  "LicenseCache read with no cache file" should "return None" in {
    val cache = new LicenseCache(tempDir.toString)
    cache.read() shouldBe None
  }

  "LicenseCache read with empty cache file" should "return None" in {
    Files.write(tempDir.resolve(LicenseCache.CacheFileName), Array.emptyByteArray)
    val cache = new LicenseCache(tempDir.toString)
    cache.read() shouldBe None
  }

  "LicenseCache write with missing directory" should "create directory and write" in {
    val nested = tempDir.resolve("subdir").resolve("nested")
    val cache = new LicenseCache(nested.toString)
    cache.write("jwt-string")
    cache.read() shouldBe Some("jwt-string")
  }

  "LicenseCache write" should "overwrite existing cache" in {
    val cache = new LicenseCache(tempDir.toString)
    cache.write("jwt-1")
    cache.read() shouldBe Some("jwt-1")
    cache.write("jwt-2")
    cache.read() shouldBe Some("jwt-2")
  }

  "LicenseCache delete" should "remove cache file" in {
    val cache = new LicenseCache(tempDir.toString)
    cache.write("jwt-to-delete")
    Files.exists(tempDir.resolve(LicenseCache.CacheFileName)) shouldBe true
    cache.delete()
    Files.exists(tempDir.resolve(LicenseCache.CacheFileName)) shouldBe false
  }

  "LicenseCache delete on missing file" should "not throw" in {
    val cache = new LicenseCache(tempDir.toString)
    noException should be thrownBy cache.delete()
  }

  "LicenseCache read with I/O error" should "return None" in {
    // Create a directory named license-cache.jwt — readAllBytes throws IOException on a directory
    Files.createDirectory(tempDir.resolve(LicenseCache.CacheFileName))
    val cache = new LicenseCache(tempDir.toString)
    cache.read() shouldBe None
  }

  "LicenseCache write with I/O error" should "not throw" in {
    // Use a regular file as the cacheDir — createDirectories will fail
    val blockerFile = Files.createTempFile("license-cache-blocker", ".tmp")
    try {
      val cache = new LicenseCache(blockerFile.toString)
      noException should be thrownBy cache.write("jwt")
    } finally {
      Files.deleteIfExists(blockerFile)
    }
  }

  "LicenseCache read with oversized file" should "return None" in {
    val bigContent = new Array[Byte](65537)
    java.util.Arrays.fill(bigContent, 'x'.toByte)
    Files.write(tempDir.resolve(LicenseCache.CacheFileName), bigContent)
    val cache = new LicenseCache(tempDir.toString)
    cache.read() shouldBe None
  }

  "LicenseCache read with symbolic link" should "return None" in {
    val target = Files.createFile(tempDir.resolve("target.txt"))
    Files.write(target, "jwt-content".getBytes("UTF-8"))
    Files.createSymbolicLink(tempDir.resolve(LicenseCache.CacheFileName), target)
    val cache = new LicenseCache(tempDir.toString)
    cache.read() shouldBe None
  }
}
