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

package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.schema.Index
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The settings a copy of an index must not carry: ONE list, two faces. The ListMap face serves the
  * table paths (ALTER TABLE reindex, CREATE TABLE … AS SELECT) with the bytes they had; the JSON
  * face serves the restore of a failed migration, whose input is a table's `{"index": {…}}` or the
  * raw `index` object of `GET <index>/_settings`.
  */
class CopyableSettingsSpec extends AnyFlatSpec with Matchers {

  /** What `GET /t` answers on Elasticsearch 8 (the es8 client renders `creation_date` as a number).
    */
  private val indexDocument =
    """{"t":{"aliases":{},"mappings":{"properties":{"id":{"type":"keyword"},"n":{"type":"integer"}}},
      |"settings":{"index":{"routing":{"allocation":{"include":{"_tier_preference":"data_content"}}},
      |"number_of_replicas":"0","provided_name":"t","creation_date":1793871490958,
      |"version":{"created":"8525000"},"default_pipeline":"t_ddl_default_pipeline",
      |"final_pipeline":"t_ddl_final_pipeline","uuid":"ILOBO5FNQHaVqqSkFIvWNw",
      |"number_of_shards":"1","refresh_interval":"2s"}}}}""".stripMargin.replaceAll("\n", "")

  private lazy val loaded: Table = Index("t", indexDocument).schema

  behavior of "generatedIndexSettings"

  it should "be the four settings Elasticsearch generates" in {
    generatedIndexSettings shouldBe Set("uuid", "creation_date", "provided_name", "version")
  }

  behavior of "copyableSettings (ListMap face)"

  it should "equal main's chained removal for a loaded table, byte for byte" in {
    // the control: the loaded table does carry the generated settings
    loaded.settings.keySet should contain allOf ("uuid", "creation_date", "provided_name", "version")
    val chained = loaded.settings - "uuid" - "creation_date" - "provided_name" - "version"
    copyableSettings(loaded.settings) shouldBe chained
    // the ALTER TABLE reindex: the bytes of the settings it creates the indices with
    loaded.copy(settings = copyableSettings(loaded.settings)).indexSettings.toString shouldBe
    loaded.copy(settings = chained).indexSettings.toString
    // CREATE TABLE … AS SELECT: the same, without the pipelines
    val ctas = copyableSettings(loaded.settings) - "default_pipeline" - "final_pipeline"
    val mainCtas = loaded.settings - "uuid" - "creation_date" - "provided_name" - "version" -
      "default_pipeline" - "final_pipeline"
    ctas shouldBe mainCtas
    loaded.copy(settings = ctas).indexSettings.toString shouldBe
    loaded.copy(settings = mainCtas).indexSettings.toString
  }

  behavior of "copyableSettings (JSON face)"

  private val rest =
    """"number_of_shards":"1","analysis":{"filter":{"version":{"type":"stop","stopwords":["a"]}},"analyzer":{"uuid":{"type":"custom","tokenizer":"standard","filter":["lowercase","asciifolding"]}}},"max_ngram_diff":20,"ratio":0.5,"routing":{"allocation":{"include":{"_tier_preference":"data_content"}}}"""

  private val generated =
    """"uuid":"ILOBO5FNQHaVqqSkFIvWNw","creation_date":1793871490958,"provided_name":"t","version":{"created":"8525000"}"""

  it should "remove the set from the object under index, and leave the rest byte-equal" in {
    copyableSettings(s"""{"index":{$generated,$rest}}""") shouldBe s"""{"index":{$rest}}"""
    // a key beside `index` is not under it, and stays
    copyableSettings(s"""{"index":{$rest,$generated},"uuid":"beside"}""") shouldBe
    s"""{"index":{$rest},"uuid":"beside"}"""
  }

  it should "remove the set from flat JSON, and leave the rest byte-equal" in {
    copyableSettings(s"""{$rest,$generated}""") shouldBe s"""{$rest}"""
    copyableSettings(s"""{"provided_name":"t"}""") shouldBe "{}"
  }

  it should "keep a nested key that is named like a generated one" in {
    // `analysis.filter.version` and `analysis.analyzer.uuid` are not generated settings
    copyableSettings(s"""{$rest}""") shouldBe s"""{$rest}"""
  }

  it should "leave a document that is not an object unchanged" in {
    copyableSettings("[]") shouldBe "[]"
    copyableSettings("\"index\"") shouldBe "\"index\""
  }

  behavior of "copyableSettings (both faces)"

  it should "agree on the settings of a table loaded from an index document" in {
    val viaJson = mapper.readTree(copyableSettings(loaded.indexSettings.toString))
    val viaListMap = loaded.copy(settings = copyableSettings(loaded.settings)).indexSettings
    viaJson shouldBe viaListMap
    viaJson.path("index").has("uuid") shouldBe false
    viaJson.path("index").path("refresh_interval").asText() shouldBe "2s"
  }

  it should "agree on the raw settings Elasticsearch answers" in {
    val raw = mapper.readTree(indexDocument).path("t").path("settings").path("index").toString
    val viaJson = mapper.readTree(copyableSettings(raw))
    val viaListMap = loaded.copy(settings = copyableSettings(loaded.settings)).indexSettings
    viaJson shouldBe viaListMap.path("index")
  }
}
