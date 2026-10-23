package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `NOT BETWEEN` whose temporal bounds render one as Elasticsearch date math and the other as a
  * script (`LAST_DAY(…)`) is `NOT (a AND b)`, as SQL reads it: the two halves are negated as ONE
  * condition. It rendered `must_not: [a, b]`, i.e. `NOT a AND NOT b`, which no value satisfies
  * when the bounds are in order: an empty answer, HTTP 200.
  */
class NotBetweenMixedBoundSpec extends AnyFlatSpec with Matchers {

  implicit val timestamp: Long = 0L

  private val mapper = new ObjectMapper()

  private def queryOf(sql: String): JsonNode = Parser(sql) match {
    case Right(s: SingleSearch) =>
      val request: ElasticSearchRequest = s
      mapper.readTree(request.query).get("query")
    case other => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private val dateMath = "CAST('2024-02-01' AS DATE)"
  private val script = "LAST_DAY(CAST('2024-03-01' AS DATE))"

  "NOT BETWEEN with a date-math and a script bound" should "negate the whole range" in {
    Seq(s"$dateMath AND $script", s"$script AND $dateMath").foreach { bounds =>
      val q = queryOf(s"SELECT id FROM t WHERE d NOT BETWEEN $bounds")
      withClue(q) {
        val negated = q.findValue("must_not")
        negated.size() shouldBe 1
        val halves = negated.get(0).get("bool").get("must")
        halves.size() shouldBe 2
        halves.findValues("range").size() shouldBe 1
        q.toString should include(""""exists":{"field":"d"}""")
      }
    }
  }

  it should "keep both halves required without NOT" in {
    val q = queryOf(s"SELECT id FROM t WHERE d BETWEEN $dateMath AND $script")
    withClue(q) {
      q.findValue("must_not") shouldBe null
      q.findValue("must").size() shouldBe 2
    }
  }

  "NOT BETWEEN over a distance" should "negate its two halves as one condition" in {
    val q = queryOf(
      "SELECT id FROM t WHERE ST_DISTANCE(POINT(-70.0, 40.0), toLocation) NOT BETWEEN 4000 km AND 5000 km"
    )
    withClue(q) {
      val negated = q.findValue("must_not")
      negated.size() shouldBe 1
      negated.get(0).get("bool").get("must").size() shouldBe 2
    }
  }
}
