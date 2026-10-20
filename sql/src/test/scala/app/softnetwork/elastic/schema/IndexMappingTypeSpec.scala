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

package app.softnetwork.elastic.schema

import app.softnetwork.elastic.sql.`type`.SQLTypes
import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The mapping of an Elasticsearch 6 index is wrapped in its single type. `_doc` was the only name
  * unwrapped, so an index typed `doc` -- what Logstash and Beats 6.x create -- loaded no column at
  * all, and every statement over it ran untyped. A typeless root (7+) must be left alone.
  */
class IndexMappingTypeSpec extends AnyFlatSpec with Matchers {

  private val mapper = new ObjectMapper()

  "an Elasticsearch 6 mapping whose type is not named _doc" should "load its columns" in {
    val schema = Index(
      "logs",
      """{"mappings":{"doc":{"properties":{"event_ts":{"type":"date"},"id":{"type":"keyword"}}}},
        |"settings":{"index":{"number_of_shards":"1"}}}""".stripMargin
    ).schema
    schema.columns.map(_.name).sorted shouldBe List("event_ts", "id")
    schema.find("event_ts").map(_.dataType) shouldBe Some(SQLTypes.Timestamp)
    schema.find("id").map(_.dataType) shouldBe Some(SQLTypes.Keyword)
  }

  "a typeless root holding only _meta and dynamic" should "be read as the mapping itself" in {
    val mappings = IndexMappings(
      mapper.readTree("""{"dynamic":"strict","_meta":{"primary_key":["id"]}}""")
    )
    mappings.fields shouldBe Nil
    mappings.primaryKey shouldBe List("id")
    mappings.options.keySet should contain allOf ("dynamic", "_meta")
  }

  "a typeless root whose fields are named properties and doc" should "be read as the mapping itself" in {
    IndexMappings(
      mapper.readTree(
        """{"properties":{"properties":{"type":"keyword"},"doc":{"properties":{"x":{"type":"long"}}}}}"""
      )
    ).fields.map(_.name).sorted shouldBe List("doc", "properties")
  }

  "a root with two mapping types" should "not be unwrapped" in {
    IndexMappings(
      mapper.readTree(
        """{"a":{"properties":{"x":{"type":"long"}}},"b":{"properties":{"y":{"type":"long"}}}}"""
      )
    ).fields shouldBe Nil
  }

  "a typeless root holding only a runtime section" should "not be unwrapped" in {
    IndexMappings(
      mapper.readTree("""{"runtime":{"properties":{"type":"keyword"}}}""")
    ).fields shouldBe Nil
  }
}
