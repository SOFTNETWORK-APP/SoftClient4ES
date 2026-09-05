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

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

/** SoftClient4ES#241 / #217 — the end-of-stream decision of every classic-scroll and `search_after`
  * paging loop.
  *
  * Deciding on the CONVERTED rows turns a page whose rows were all dropped into a clean
  * end-of-stream, i.e. a silently truncated result set presented as a success (the
  * #205/#207/#209/#224/#238 defect class). The decision belongs to the RAW page.
  */
class ScrollPageEndOfStreamSpec extends AnyWordSpec with Matchers {

  "endOfScrollPage" should {

    "end the stream on a genuinely empty page" in {
      endOfScrollPage(rows = 0, rawHits = 0, "Scroll page [abc]") shouldBe true
    }

    "continue while the page produced rows" in {
      endOfScrollPage(rows = 10, rawHits = 10, "Scroll page [abc]") shouldBe false
    }

    "continue on an aggregation-shaped page that carries no raw hits" in {
      // GROUP BY / COUNT pages legitimately produce rows out of `aggregations` alone.
      endOfScrollPage(rows = 3, rawHits = 0, "Scroll page [abc]") shouldBe false
    }

    "FAIL — never end — when a non-empty raw page produced no rows" in {
      val ex = intercept[IllegalStateException](
        endOfScrollPage(rows = 0, rawHits = 500, "Scroll page [abc]")
      )
      ex.getMessage should include("Scroll page [abc]")
      ex.getMessage should include("500")
    }

    "raise a NON-retriable failure (AD-S1-1)" in {
      // `retryWithBackoff` retries IOException / SocketTimeoutException. A retried page failure
      // re-polls a spent scroll cursor, which SKIPS rows — so the throw must not be retriable.
      val ex = intercept[IllegalStateException](
        endOfScrollPage(rows = 0, rawHits = 1, "Scroll page [abc]")
      )
      ex shouldBe a[IllegalStateException]
      ex should not be a[java.io.IOException]
      isRetriableError(ex) shouldBe false
    }

    "build the page description only when it fails" in {
      var built = 0
      def page: String = { built += 1; "Scroll page [abc]" }
      endOfScrollPage(rows = 1, rawHits = 1, page) shouldBe false
      endOfScrollPage(rows = 0, rawHits = 0, page) shouldBe true
      built shouldBe 0
      intercept[IllegalStateException](endOfScrollPage(rows = 0, rawHits = 1, page))
      built shouldBe 1
    }
  }
}
