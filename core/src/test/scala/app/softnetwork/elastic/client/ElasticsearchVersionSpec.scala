package app.softnetwork.elastic.client

import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers

class ElasticsearchVersionSpec extends AnyWordSpec with Matchers {

  "ElasticsearchVersion.parse" should {
    "parse valid version strings" in {
      ElasticsearchVersion.parse("7.10.2") shouldBe (7, 10, 2)
      ElasticsearchVersion.parse("8.11.0") shouldBe (8, 11, 0)
      ElasticsearchVersion.parse("6.8.23") shouldBe (6, 8, 23)
      ElasticsearchVersion.parse("7.0") shouldBe (7, 0, 0)
      ElasticsearchVersion.parse("8") shouldBe (8, 0, 0)
    }

    "throw exception for invalid versions" in {
      an[IllegalArgumentException] should be thrownBy {
        ElasticsearchVersion.parse("invalid")
      }
    }
  }

  "ElasticsearchVersion.isAtLeast" should {
    "correctly compare versions" in {
      // ES 7.10.2
      ElasticsearchVersion.isAtLeast("7.10.2", 7, 10) shouldBe true
      ElasticsearchVersion.isAtLeast("7.10.2", 7, 9) shouldBe true
      ElasticsearchVersion.isAtLeast("7.10.2", 7, 11) shouldBe false
      ElasticsearchVersion.isAtLeast("7.10.2", 8, 0) shouldBe false
      ElasticsearchVersion.isAtLeast("7.10.2", 6, 0) shouldBe true

      // ES 8.11.0
      ElasticsearchVersion.isAtLeast("8.11.0", 7, 10) shouldBe true
      ElasticsearchVersion.isAtLeast("8.11.0", 8, 0) shouldBe true
      ElasticsearchVersion.isAtLeast("8.11.0", 8, 11) shouldBe true
      ElasticsearchVersion.isAtLeast("8.11.0", 8, 12) shouldBe false

      // ES 6.8.23
      ElasticsearchVersion.isAtLeast("6.8.23", 7, 10) shouldBe false
      ElasticsearchVersion.isAtLeast("6.8.23", 6, 8) shouldBe true
      ElasticsearchVersion.isAtLeast("6.8.23", 6, 9) shouldBe false
    }
  }

  "ElasticsearchVersion.supportsPit" should {
    "return true for ES >= 7.10" in {
      ElasticsearchVersion.supportsPit("7.10.0") shouldBe true
      ElasticsearchVersion.supportsPit("7.10.2") shouldBe true
      ElasticsearchVersion.supportsPit("7.17.0") shouldBe true
      ElasticsearchVersion.supportsPit("8.0.0") shouldBe true
      ElasticsearchVersion.supportsPit("8.11.0") shouldBe true
    }

    "return false for ES < 7.10" in {
      ElasticsearchVersion.supportsPit("7.9.3") shouldBe false
      ElasticsearchVersion.supportsPit("7.0.0") shouldBe false
      ElasticsearchVersion.supportsPit("6.8.23") shouldBe false
    }
  }

  "ElasticsearchVersion.isEs8OrHigher" should {
    "return true for ES >= 8.0" in {
      ElasticsearchVersion.isEs8OrHigher("8.0.0") shouldBe true
      ElasticsearchVersion.isEs8OrHigher("8.11.0") shouldBe true
      ElasticsearchVersion.isEs8OrHigher("9.0.0") shouldBe true
    }

    "return false for ES < 8.0" in {
      ElasticsearchVersion.isEs8OrHigher("7.17.0") shouldBe false
      ElasticsearchVersion.isEs8OrHigher("7.10.2") shouldBe false
      ElasticsearchVersion.isEs8OrHigher("6.8.23") shouldBe false
    }
  }
}
