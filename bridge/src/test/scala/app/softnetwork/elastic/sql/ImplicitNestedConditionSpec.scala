package app.softnetwork.elastic.sql

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.Schema
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `WHERE items.sku = 's1'` over a `nested` field, with no NESTED and no JOIN UNNEST, keeps the
  * documents one of whose elements satisfies it, as `NESTED(items.sku = 's1')` does and as DuckDB
  * answers `EXISTS (SELECT 1 FROM unnest(items) AS e WHERE e.sku = 's1')`. It rendered a query on
  * the ROOT document, which holds no field of a nested path: no document, HTTP 200.
  */
class ImplicitNestedConditionSpec extends AnyFlatSpec with Matchers {

  implicit val timestamp: Long = 0L

  private val schema: Schema = Index(
    "t",
    """{"t": {"aliases": {}, "mappings": {"properties": {"id": {"type": "keyword"},
      | "g1": {"type": "keyword"}, "n": {"type": "integer"},
      | "items": {"type": "nested", "properties": {"sku": {"type": "keyword"},
      | "qty": {"type": "integer"}}},
      | "tags": {"type": "nested", "properties": {"v": {"type": "keyword"}}},
      | "o": {"properties": {"a": {"type": "keyword"}}}}},
      | "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
  ).schema

  private def resolved(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(schema))
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def queryOf(statement: SingleSearch): String = {
    val request: ElasticSearchRequest = statement
    request.query
  }

  private def queryOf(where: String): String = queryOf(resolved(s"SELECT id FROM t WHERE $where"))

  private def nestedCount(q: String): Int = "\"nested\"".r.findAllMatchIn(q).size

  "a condition over a nested column" should "be the nested query over its path" in {
    val q = queryOf("items.sku = 's1'")
    q shouldBe
    """{"query":{"bool":{"filter":[{"nested":{"path":"items","query":{"term":{"items.sku":""" +
    """{"value":"s1"}}},"ignore_unmapped":true}}]}},"_source":{"includes":["id"]}}"""
    Seq(
      "items.sku <> 's1'",
      "NOT items.sku = 's1'",
      "items.sku IN ('s1', 's3')",
      "items.qty BETWEEN 2 AND 4",
      "UPPER(items.sku) = 'S1'",
      "MATCH(items.sku) AGAINST ('s1')"
    ).foreach { where =>
      val q = queryOf(where)
      withClue(s"[$where] $q ") {
        nestedCount(q) shouldBe 1
        q should include(""""path":"items"""")
      }
    }
  }

  it should "test the conditions over one path on one element" in {
    val q = queryOf("items.sku = 's2' AND items.qty > 2")
    withClue(q)(nestedCount(q) shouldBe 1)
  }

  it should "test the conditions over two paths each on its own" in {
    val q = queryOf("items.sku = 's1' AND tags.v = 'x'")
    withClue(q) {
      nestedCount(q) shouldBe 2
      q should include(""""path":"tags"""")
    }
  }

  it should "stand beside a condition on the document" in {
    val q = queryOf("items.sku = 's1' OR g1 = 'e'")
    withClue(q) {
      nestedCount(q) shouldBe 1
      q should include(""""should"""")
      q should include(""""term":{"g1":{"value":"e"}}""")
    }
  }

  it should "render the same once resolved twice, as core resolves a scrolled search" in {
    Seq(
      "items.sku = 's1'",
      "items.sku = 's2' AND items.qty > 2",
      "items.sku = 's1' AND tags.v = 'x'"
    )
      .foreach { where =>
        val once = resolved(s"SELECT id FROM t WHERE $where")
        withClue(where)(queryOf(once.update(Some(schema))) shouldBe queryOf(once))
      }
  }

  "a condition that no nested query can test" should "be refused by name" in {
    Seq(
      "items.qty > n" -> "a column of the nested path items and a column of the document"
    ).foreach { case (where, reason) =>
      withClue(where) {
        resolved(s"SELECT id FROM t WHERE $where")
          .validateResolved()
          .left
          .toOption
          .getOrElse("") should
        include(reason)
      }
    }
  }

  "a column of an object, a JOIN UNNEST and a statement with no schema" should "render as before" in {
    nestedCount(queryOf("o.a = 'x'")) shouldBe 0
    val noSchema = Parser("SELECT id FROM t WHERE items.sku = 's1'") match {
      case Right(s: SingleSearch) => queryOf(s)
      case other                  => fail(s"$other")
    }
    nestedCount(noSchema) shouldBe 0
    val unnest =
      queryOf(resolved("SELECT t.id FROM t AS t JOIN UNNEST(t.items) AS i WHERE i.sku = 's1'"))
    unnest should include(""""inner_hits":{"name":"i"""")
    // a condition reading a JOIN UNNEST column beside a column of the document is the UNNEST's:
    // neither wrapped nor refused here (the UNNEST scope rules judge it)
    val mixed = resolved(
      "SELECT t.id, i.qty AS q FROM t AS t JOIN UNNEST(t.items) AS i WHERE COALESCE(t.g1, 'x', i.sku) = 'a' LIMIT 100"
    )
    mixed.validateResolved() shouldBe Right(())
    nestedCount(queryOf(mixed)) shouldBe nestedCount(
      queryOf(
        Parser(
          "SELECT t.id, i.qty AS q FROM t AS t JOIN UNNEST(t.items) AS i WHERE COALESCE(t.g1, 'x', i.sku) = 'a' LIMIT 100"
        ) match {
          case Right(s: SingleSearch) => s
          case other                  => fail(s"$other")
        }
      )
    )
  }
}
