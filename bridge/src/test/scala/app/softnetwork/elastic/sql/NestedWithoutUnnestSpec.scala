package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `WHERE NESTED(…)` with no JOIN UNNEST filters by its condition: the documents one of whose
  * elements satisfies it, as DuckDB answers `EXISTS (SELECT 1 FROM unnest(items) AS e WHERE …)`.
  * Its columns carry no nested element (no UNNEST declares one), and the bridge rendered
  * `match_all` for it: every document, HTTP 200.
  */
class NestedWithoutUnnestSpec extends AnyFlatSpec with Matchers {

  implicit val timestamp: Long = 0L

  private def queryOf(sql: String): String = Parser(sql) match {
    case Right(s: SingleSearch) =>
      val request: ElasticSearchRequest = s
      request.query
    case other => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  "a NESTED condition with no JOIN UNNEST" should "be the nested query over its columns' path" in {
    queryOf("SELECT id FROM t WHERE NESTED(items.sku = 's1')") shouldBe
    """{"query":{"bool":{"filter":[{"nested":{"path":"items","query":{"term":{"items.sku":""" +
    """{"value":"s1"}}},"ignore_unmapped":true}}]}},"_source":{"includes":["id"]}}"""
  }

  it should "test every condition on the same element" in {
    val q = queryOf("SELECT id FROM t WHERE NESTED(items.sku = 's2' AND items.qty > 2)")
    withClue(q) {
      "\"nested\"".r.findAllMatchIn(q).size shouldBe 1
      q should include(""""term":{"items.sku":{"value":"s2"}}""")
      q should include(""""range":{"items.qty":{"gt":2}}""")
      q should not include "match_all"
    }
  }

  it should "stand beside a condition on the document" in {
    val q = queryOf("SELECT id FROM t WHERE NESTED(items.sku = 's1') AND g1 = 'a'")
    withClue(q) {
      q should include(""""term":{"g1":{"value":"a"}}""")
      q should include(""""nested":{"path":"items"""")
      q should not include "match_all"
    }
  }

  it should "test each operand on its own path over several nested paths" in {
    // no element belongs to two paths: each operand is the nested query over its own. It was
    // refused, and so were two NESTED(…) of two paths, which the resolution merges into one
    Seq(
      "SELECT id FROM t WHERE NESTED(items.sku = 's1' AND tags.v = 'x')",
      "SELECT id FROM t WHERE NESTED(items.sku = 's1') AND NESTED(tags.v = 'x')"
    ).foreach { sql =>
      val q = queryOf(sql)
      withClue(q) {
        "\"nested\"".r.findAllMatchIn(q).size shouldBe 2
        q should include(""""path":"items"""")
        q should include(""""path":"tags"""")
      }
    }
  }

  "a JOIN UNNEST" should "keep its nested query and inner hits" in {
    val q = queryOf("SELECT t.id FROM t JOIN UNNEST(t.items) AS i WHERE i.sku = 's1'")
    withClue(q) {
      q should include(""""nested":{"path":"items"""")
      q should include(""""inner_hits":{"name":"i"""")
    }
  }
}
