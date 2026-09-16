package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.MultiSearch
import com.sksamuel.elastic4s.searches.MultiSearchRequest
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 22.6 — the bridge half of the set-operator guard.
  *
  * Two claims, and only two: a `UNION ALL` still converts per leg exactly as before, and anything
  * else reaching this conversion throws a NAMED `IllegalArgumentException` rather than rendering as
  * its cheaper cousin. The EMITTED `_msearch` body is NOT built here —
  * `ElasticMultiSearchRequest.query` is called by no production path (its own scaladoc says so) and
  * Elasticsearch receives `ElasticQueries.multiQuery` — so the byte pin lives in core.
  */
class SetOperatorEmissionSpec extends AnyFlatSpec with Matchers {

  implicit val timestamp: Long = 0L

  private def multi(sql: String): MultiSearch = Parser(sql) match {
    case Right(m: MultiSearch) => m
    case other                 => fail(s"[$sql] expected MultiSearch, got $other")
  }

  /** Forces the IMPLICIT conversion at a real parameter position.
    *
    * 🔴 Do NOT write `val _: MultiSearchRequest = m` for this: `val _: T = e` is a PATTERN
    * DEFINITION in Scala, not an ascription, so it type-TESTS `e` against `T` and never applies an
    * implicit conversion at all. Written that way this suite failed with a `scala.MatchError` on
    * the `MultiSearch` — which reads exactly like a bridge defect and is in fact a test that
    * exercised nothing.
    */
  private def asRequest(request: MultiSearchRequest): MultiSearchRequest = request

  "A UNION ALL of bounded legs" should
  "still convert to one MultiSearchRequest of per-leg SearchRequests" in {
    val m = multi(
      "SELECT id, name FROM dql_users WHERE age > 30 LIMIT 100 " +
      "UNION ALL SELECT id, name FROM dql_users WHERE age <= 30 LIMIT 100"
    )
    m.isUnionAllOnly shouldBe true
    requestToMultiSearchRequest(m).searches should have size 2
    // ...and through the IMPLICIT conversion, which is how every production caller reaches it
    asRequest(m).searches should have size 2
  }

  "A non-UNION ALL set operation reaching the bridge" should
  "throw the NAMED backstop, never render as UNION ALL" in {
    Seq("UNION", "UNION DISTINCT", "INTERSECT", "INTERSECT ALL", "EXCEPT", "EXCEPT ALL").foreach {
      op =>
        val m = multi(s"SELECT id FROM dql_users $op SELECT id FROM dql_users")
        withClue(s"[$op] ") {
          val direct = intercept[IllegalArgumentException] { requestToMultiSearchRequest(m) }
          direct.getMessage should include("cannot be rendered as an Elasticsearch _msearch")
          direct.getMessage should include("softclient4es-arrow-extensions")
          // the IMPLICIT path is the one production reaches, and it must be guarded too
          val implicitly = intercept[IllegalArgumentException] { asRequest(m) }
          implicitly.getMessage should include("cannot be rendered as an Elasticsearch _msearch")
        }
    }
  }
}
