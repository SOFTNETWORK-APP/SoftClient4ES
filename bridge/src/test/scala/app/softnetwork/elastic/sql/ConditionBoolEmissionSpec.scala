package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.parser.ConditionPopulation._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The Elasticsearch query a WHERE emits evaluates SQL's reading of the condition.
  *
  * 🔴 A correct condition TREE is not enough: an unparenthesised sub-condition used to be written
  * into its parent's `bool`, where an OR under an AND put its `should` clauses next to `filter`
  * clauses -- optional for Elasticsearch. The query is evaluated here with Elasticsearch's own
  * `bool` rules (`BoolQueryModel`), for Elasticsearch 7+ and for the Elasticsearch 6 filter-context
  * rule, against the oracle of the generated structure.
  */
class ConditionBoolEmissionSpec extends AnyFlatSpec with Matchers {

  implicit val timestamp: Long = 0L

  private val mapper = new ObjectMapper()

  /** P2: the WHERE statements of P1 whose leaves are all `ci = 1` (every class but the kinds). */
  private lazy val wherePopulation: List[Gen] =
    population().filter(g => g.clause == WhereC && g.cls != "K")

  private def queryOf(search: SingleSearch): JsonNode = {
    val request: ElasticSearchRequest = search
    mapper.readTree(request.query).get("query")
  }

  private def searchOf(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"[$sql] $other")
  }

  "a WHERE that combines AND and OR" should "emit a query that evaluates SQL's reading" in {
    val wrong = wherePopulation.flatMap { g =>
      val q = queryOf(searchOf(g.sql))
      val bits = 0 until (1 << g.n)
      val es7 = bits.map(BoolQueryModel.matches(q, _, es6 = false)).toVector
      val es6 = bits.map(BoolQueryModel.matches(q, _, es6 = true)).toVector
      if (es7 == g.ansiTable && es6 == g.ansiTable) None
      else Some(s"[${g.sql}] es7=${es7 == g.ansiTable} es6=${es6 == g.ansiTable}: $q")
    }
    withClue(
      s"${wrong.size} of ${wherePopulation.size} wrong, first 10:\n${wrong.take(10).mkString("\n")}\n"
    ) {
      wrong shouldBe empty
    }
  }

  it should "never leave should clauses beside filter or must clauses" in {
    val offending = wherePopulation
      .map(g => g.sql -> BoolQueryModel.optionalShoulds(queryOf(searchOf(g.sql))))
      .filter(_._2 > 0)
    withClue(s"first 10: ${offending.take(10).mkString("\n")}\n") { offending shouldBe empty }
  }

  "a WHERE with ONE operator" should "stay one flat bool" in {
    queryOf(
      searchOf("SELECT id FROM t WHERE c1 = 1 AND c2 = 1 AND c3 = 1 AND c4 = 1")
    ).toString shouldBe
    """{"bool":{"filter":[{"term":{"c1":{"value":1}}},{"term":{"c2":{"value":1}}},{"term":{"c3":{"value":1}}},{"term":{"c4":{"value":1}}}]}}"""
    queryOf(searchOf("SELECT id FROM t WHERE c1 = 1 OR c2 = 1 OR c3 = 1")).toString shouldBe
    """{"bool":{"should":[{"term":{"c1":{"value":1}}},{"term":{"c2":{"value":1}}},{"term":{"c3":{"value":1}}}]}}"""
  }

  "DELETE and UPDATE" should "send the query the same WHERE sends in a SELECT" in {
    val wrong = wherePopulation.filter(_.mixedLevel).flatMap { g =>
      val select = searchOf(g.sql)
      val expected = queryOf(select)
      Seq(
        "DELETE" -> select.copy(deleteByQuery = true),
        "UPDATE" -> select.copy(updateByQuery = true)
      )
        .collect { case (kind, s) if queryOf(s) != expected => s"$kind [${g.sql}]: ${queryOf(s)}" }
    }
    wrong shouldBe empty
  }

  "an OR group holding a MATCH, under an AND" should "stay ONE condition of the AND" in {
    // Spread into the root bool beside its `filter` clauses, the group's `should` clauses turned
    // optional: `c1 = 1 AND (MATCH ... OR c3 = 1)` selected every document with c1 = 1. A MATCH
    // over several columns is such a group too. The group that IS the whole condition is still
    // spread, which is exact (the flat `should` of a lone MATCH).
    Seq(
      "c1 = 1 AND (MATCH (c2) AGAINST ('1') OR c3 = 1)" -> (3, A(L(1), O(L(2), L(3)))),
      "(MATCH (c1) AGAINST ('1') OR c2 = 1) AND c3 = 1" -> (3, A(O(L(1), L(2)), L(3))),
      "c1 = 1 AND MATCH (c2, c3) AGAINST ('1')"         -> (3, A(L(1), O(L(2), L(3)))),
      "(c1 = 1 OR MATCH (c2) AGAINST ('1')) AND (c3 = 1 OR MATCH (c4) AGAINST ('1'))" ->
      (4, A(O(L(1), L(2)), O(L(3), L(4)))),
      "MATCH (c1, c2) AGAINST ('1')"                  -> (2, O(L(1), L(2))),
      "MATCH (c1) AGAINST ('1') OR c2 = 1 AND c3 = 1" -> (3, O(L(1), A(L(2), L(3))))
    ).foreach { case (cond, (n, expected)) =>
      val q = queryOf(searchOf(s"SELECT id FROM t WHERE $cond"))
      val bits = 0 until (1 << n)
      withClue(s"[$cond] $q ") {
        bits.map(BoolQueryModel.matches(q, _, es6 = false)).toVector shouldBe table(expected, n)
        bits.map(BoolQueryModel.matches(q, _, es6 = true)).toVector shouldBe table(expected, n)
        BoolQueryModel.optionalShoulds(q) shouldBe 0
      }
    }
  }

  "a WHERE over an UNNEST column" should "emit the nested query SQL's reading needs" in {
    // one child per document: a nested query is its own query
    Seq(
      "inner_items.c1 = 1 OR inner_items.c2 = 1 AND inner_items.c3 = 1" -> O(L(1), A(L(2), L(3))),
      "c1 = 1 OR inner_items.c2 = 1 AND inner_items.c3 = 1"             -> O(L(1), A(L(2), L(3))),
      "inner_items.c1 = 1 AND inner_items.c2 = 1 OR c3 = 1"             -> O(A(L(1), L(2)), L(3))
    ).foreach { case (cond, expected) =>
      val q = queryOf(searchOf(s"SELECT id FROM t JOIN UNNEST(t.items) AS inner_items WHERE $cond"))
      withClue(s"[$cond] $q ") {
        (0 until 8).map(BoolQueryModel.matches(q, _, es6 = false)).toVector shouldBe table(
          expected,
          3
        )
        (0 until 8).map(BoolQueryModel.matches(q, _, es6 = true)).toVector shouldBe table(
          expected,
          3
        )
      }
    }
  }
}
