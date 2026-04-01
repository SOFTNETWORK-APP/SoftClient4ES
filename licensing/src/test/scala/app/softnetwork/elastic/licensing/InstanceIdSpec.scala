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

import java.nio.file.Files

class InstanceIdSpec extends AnyFlatSpec with Matchers {

  "InstanceId.getOrCreate with empty dir" should "generate UUID and write file" in {
    val dir = Files.createTempDirectory("instance-id-test").toFile
    try {
      val id = InstanceId.getOrCreate(dir.getAbsolutePath)
      id should not be empty
      // UUID format check
      id should fullyMatch regex "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
      // File should exist
      val file = new java.io.File(dir, "instance-id")
      file.exists() shouldBe true
      new String(Files.readAllBytes(file.toPath), "UTF-8") shouldBe id
    } finally {
      deleteDir(dir)
    }
  }

  "InstanceId.getOrCreate with existing file" should "return same UUID" in {
    val dir = Files.createTempDirectory("instance-id-test").toFile
    try {
      val id1 = InstanceId.getOrCreate(dir.getAbsolutePath)
      val id2 = InstanceId.getOrCreate(dir.getAbsolutePath)
      id2 shouldBe id1
    } finally {
      deleteDir(dir)
    }
  }

  "InstanceId.getOrCreate with non-existent dir" should "create dir and generate UUID" in {
    val parent = Files.createTempDirectory("instance-id-test").toFile
    val dir = new java.io.File(parent, "sub/deep")
    try {
      dir.exists() shouldBe false
      val id = InstanceId.getOrCreate(dir.getAbsolutePath)
      id should not be empty
      dir.exists() shouldBe true
      new java.io.File(dir, "instance-id").exists() shouldBe true
    } finally {
      deleteDir(parent)
    }
  }

  "InstanceId.getOrCreate with unwritable dir" should "return fresh UUID without persisting" in {
    val dir = Files.createTempDirectory("instance-id-test").toFile
    try {
      dir.setWritable(false)
      val id = InstanceId.getOrCreate(dir.getAbsolutePath)
      id should not be empty
      // File should NOT exist since dir is not writable
      new java.io.File(dir, "instance-id").exists() shouldBe false
    } finally {
      dir.setWritable(true)
      deleteDir(dir)
    }
  }

  private def deleteDir(dir: java.io.File): Unit = {
    if (dir.isDirectory) {
      Option(dir.listFiles()).foreach(_.foreach(deleteDir))
    }
    dir.delete()
  }
}
