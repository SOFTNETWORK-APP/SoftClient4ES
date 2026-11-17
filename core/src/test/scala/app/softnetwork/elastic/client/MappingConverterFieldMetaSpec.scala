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

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.serialization.JacksonConfig
import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/** SCHEMA-17 (A-170) -- below Elasticsearch 7.6 no field takes a `meta`: the per-major seam every
  * mapping writer passes (`MappingConverter`) leaves core's keys out, and a `meta` they leave
  * empty; a user's own keys still reach Elasticsearch. From 7.6 the mapping is sent as written.
  */
class MappingConverterFieldMetaSpec extends AnyFlatSpec with Matchers {

  private val mapper = JacksonConfig.objectMapper

  /** The mapping core writes for a table read back from `properties` and `columns` (`_meta`): a
    * date carrying a user's own `meta` key, a multi-field, a list of objects over `object`.
    */
  private def written(): ObjectNode =
    Index(
      "t",
      """{"t": {"aliases": {}, "settings": {"index": {"number_of_shards": "1"}}, "mappings": {
        | "_meta": {"columns": {"d": {"data_type": "DATE"}, "lst": {"data_type": "ARRAY<STRUCT>"}}},
        | "properties": {
        |  "id": {"type": "keyword"},
        |  "d": {"type": "date", "format": "dd/MM/yyyy", "meta": {"unit": "day"}},
        |  "k": {"type": "keyword", "fields": {"raw": {"type": "text"}}},
        |  "lst": {"type": "object", "properties": {"p": {"type": "integer"}}}
        |}}}}""".stripMargin
    ).schema.indexMappings

  private def metaKeys(mapping: ObjectNode, path: String*): Set[String] = {
    val field = path.foldLeft(mapping.get("properties"): JsonNode) { (node, name) =>
      Option(node.get("properties")).orElse(Option(node.get("fields"))).getOrElse(node).get(name)
    }
    Option(field.get("meta")).map(_.fieldNames().asScala.toSet).getOrElse(Set.empty[String])
  }

  "MappingConverter.convert" should "leave core's field meta out below 7.6, and the user's keys in" in {
    val mapping = written()
    val before = mapper.writeValueAsString(mapping)
    val es6 = MappingConverter.convert(mapping, "6.8.23")
    metaKeys(es6, "id") shouldBe empty
    es6.get("properties").get("id").has("meta") shouldBe false
    metaKeys(es6, "d") shouldBe Set("unit")
    metaKeys(es6, "k", "raw") shouldBe empty
    metaKeys(es6, "lst", "p") shouldBe empty
    // the table's own mapping is never touched
    mapper.writeValueAsString(mapping) shouldBe before
    // the String overload (`MappingApi.setMapping`) answers the same
    mapper.readTree(MappingConverter.withoutCoreFieldMeta(before, "7.5.2")) shouldBe es6
  }

  it should "send the mapping as written from 7.6" in {
    val mapping = written()
    Seq("7.6.0", "8.18.3").foreach { version =>
      val converted = MappingConverter.convert(mapping, version)
      metaKeys(converted, "id") should contain("sql_type")
      metaKeys(converted, "d") shouldBe Set("unit", "sql_type", "sql_format")
      metaKeys(converted, "k", "raw") should contain("sql_type")
      metaKeys(converted, "lst", "p") should contain("sql_list_depths")
    }
  }
}
