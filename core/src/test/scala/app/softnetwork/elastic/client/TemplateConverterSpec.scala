package app.softnetwork.elastic.client

import com.fasterxml.jackson.databind.ObjectMapper
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class TemplateConverterSpec extends AnyFlatSpec with Matchers {

  "isLegacyFormat" should "detect legacy format" in {
    val legacy = """{"index_patterns":["logs-*"],"order":1,"settings":{}}"""
    TemplateConverter.isLegacyFormat(legacy) shouldBe true
  }

  it should "detect composable format" in {
    val composable = """{"index_patterns":["logs-*"],"priority":1,"template":{"settings":{}}}"""
    TemplateConverter.isLegacyFormat(composable) shouldBe false
  }

  "convertLegacyToComposable" should "convert legacy to composable" in {
    val legacy =
      """
        |{
        |  "index_patterns": ["logs-*"],
        |  "order": 1,
        |  "settings": {"number_of_shards": 1},
        |  "mappings": {"properties": {"timestamp": {"type": "date"}}}
        |}
        |""".stripMargin

    val result = TemplateConverter.convertLegacyToComposable(legacy)
    result.isSuccess shouldBe true

    val composable = new ObjectMapper().readTree(result.get)
    composable.has("priority") shouldBe true
    composable.get("priority").asInt() shouldBe 1
    composable.has("template") shouldBe true
    composable.get("template").has("settings") shouldBe true
    composable.get("template").has("mappings") shouldBe true
  }

  "normalizeTemplate" should "convert legacy to composable for ES 7.8+" in {
    val legacy = """{"index_patterns":["logs-*"],"order":1,"settings":{}}"""
    val result = TemplateConverter.normalizeTemplate(legacy, "7.10.0")

    result.isSuccess shouldBe true
    val normalized = new ObjectMapper().readTree(result.get)
    normalized.has("priority") shouldBe true
    normalized.has("template") shouldBe true
  }

  it should "convert composable to legacy for ES < 7.8" in {
    val composable = """{"index_patterns":["logs-*"],"priority":1,"template":{"settings":{}}}"""
    val result = TemplateConverter.normalizeTemplate(composable, "7.5.0")

    result.isSuccess shouldBe true
    val normalized = new ObjectMapper().readTree(result.get)
    normalized.has("order") shouldBe true
    normalized.has("settings") shouldBe true
    normalized.has("template") shouldBe false
  }

  it should "keep legacy format for ES < 7.8" in {
    val legacy = """{"index_patterns":["logs-*"],"order":1,"settings":{}}"""
    val result = TemplateConverter.normalizeTemplate(legacy, "7.5.0")

    result.isSuccess shouldBe true
    result.get should include("order")
    result.get should not include "priority"
  }

  it should "keep composable format for ES 7.8+" in {
    val composable = """{"index_patterns":["logs-*"],"priority":1,"template":{"settings":{}}}"""
    val result = TemplateConverter.normalizeTemplate(composable, "7.10.0")

    result.isSuccess shouldBe true
    result.get should include("priority")
    result.get should include("template")
  }
}
