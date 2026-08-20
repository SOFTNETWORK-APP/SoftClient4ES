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
    "return true for ES >= 7.12" in {
      ElasticsearchVersion.supportsPit("7.12.0") shouldBe true
      ElasticsearchVersion.supportsPit("7.17.0") shouldBe true
      ElasticsearchVersion.supportsPit("8.0.0") shouldBe true
      ElasticsearchVersion.supportsPit("8.11.0") shouldBe true
      ElasticsearchVersion.supportsPit("9.0.0") shouldBe true
    }

    // 7.10/7.11 have the PIT API but no _shard_doc sort field and no automatic PIT tiebreaker:
    // a _doc-sorted PIT search on those versions silently drops rows across shards (#197), so
    // they must take the classic _id-sorted search_after path instead.
    "return false for ES < 7.12" in {
      ElasticsearchVersion.supportsPit("7.11.2") shouldBe false
      ElasticsearchVersion.supportsPit("7.10.0") shouldBe false
      ElasticsearchVersion.supportsPit("7.10.2") shouldBe false
      ElasticsearchVersion.supportsPit("7.9.3") shouldBe false
      ElasticsearchVersion.supportsPit("7.0.0") shouldBe false
      ElasticsearchVersion.supportsPit("6.8.23") shouldBe false
    }
  }

  "ElasticsearchVersion.supportsPitSlicing" should {
    // slice + pit in ONE search request exists from 7.15 (elastic/elasticsearch#74457), not 7.10
    // as the #238 issue text said; 7.12–7.14 keep PIT but page sequentially.
    "return true for ES >= 7.15" in {
      ElasticsearchVersion.supportsPitSlicing("7.15.0") shouldBe true
      ElasticsearchVersion.supportsPitSlicing("7.17.29") shouldBe true
      ElasticsearchVersion.supportsPitSlicing("8.18.3") shouldBe true
      ElasticsearchVersion.supportsPitSlicing("9.0.3") shouldBe true
    }

    "return false for ES < 7.15 (PIT without slicing on 7.12-7.14)" in {
      ElasticsearchVersion.supportsPitSlicing("7.14.2") shouldBe false
      ElasticsearchVersion.supportsPitSlicing("7.12.0") shouldBe false
      ElasticsearchVersion.supportsPitSlicing("7.10.0") shouldBe false
      ElasticsearchVersion.supportsPitSlicing("6.8.23") shouldBe false
    }

    "keep supportsPit at 7.12" in {
      ElasticsearchVersion.supportsPit("7.12.0") shouldBe true
      ElasticsearchVersion.supportsPit("7.11.2") shouldBe false
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
