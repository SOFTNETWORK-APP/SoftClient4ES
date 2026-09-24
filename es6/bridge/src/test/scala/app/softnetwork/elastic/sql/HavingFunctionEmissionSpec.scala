package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime
import scala.jdk.CollectionConverters._

/** Issue #389 -- the EMITTED Elasticsearch query for a `HAVING` over a FUNCTION of an aggregate,
  * measured against the HAND-MAINTAINED es6 bridge (AC-7).
  *
  * 🔴 The whole fix lives in the `sql` module: no line of either `ElasticAggregation.scala` moved.
  * This twin is what PROVES that -- it is the same matrix, asserted against a different elastic4s
  * major, and the only expectation that differs is elastic4s 6's bare-string `terms` `exclude`.
  *
  * The `sql` half (detection, the representability gate, the refusals) is pinned in
  * `HavingOverAggregateFunctionSpec`. What is measured HERE is what Elasticsearch is actually sent:
  * the `bucket_selector`, its `buckets_path`, and the metric aggregation the script reads. Those
  * are the three things that used to disagree -- the defect was invisible to a parse, because every
  * one of these statements parsed, ran, and returned EVERY group with HTTP 200.
  *
  * 🔴 Every shape named in `emitted` below was EXECUTED against a real Elasticsearch 8.18.3 before
  * it was written here, and every shape in `refused` was measured to FAIL there. That is not
  * ceremony: the spec asserted that `Double.valueOf(Math.abs(params.c)) > 1` was a legal
  * `bucket_selector` source, and Elasticsearch rejects it at COMPILE time. A byte pin would have
  * certified it (`feedback_assert_the_mechanism_not_a_proxy` -- PARSES != RENDERS != RUNS).
  *
  * The file closes with a structural invariant over every shape it names: a `bucket_selector` may
  * read no `params.<x>` its own `buckets_path` does not declare, and must null-test every metric it
  * declares. `metricSelectorForBucket`'s own comment asserts the first half; it is now load-bearing
  * for a new population, so it is asserted rather than assumed.
  */
class HavingFunctionEmissionSpec extends AnyFlatSpec with Matchers {

  import scala.language.implicitConversions

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  implicit def sqlQueryToRequest(sqlQuery: SelectStatement): ElasticSearchRequest =
    sqlQuery.statement match {
      case Some(value: SingleSearch) => value.copy(score = sqlQuery.score)
      case other => throw new IllegalArgumentException(s"Not a single search: $other")
    }

  private val mapper = new ObjectMapper()

  private def queryOf(sql: String): String = {
    val select: ElasticSearchRequest = SelectStatement(sql)
    select.query
  }

  private val terms = """"terms":{"field":"status","size":65536,"min_doc_count":1}"""
  private val group = "SELECT status, COUNT(*) AS c FROM t GROUP BY status HAVING "

  /** Every shape this file emits -- the input of the structural invariant at the bottom, so the
    * guard is computed over the material it guards rather than over a hand-picked subset.
    */
  private val emitted: Seq[String] = Seq(
    group + "COUNT(*) > 1",
    group + "COALESCE(COUNT(*), 0) > 1",
    group + "GREATEST(COUNT(*), 0) > 1",
    group + "LEAST(COUNT(*), 99) > 1",
    group + "SIGN(COUNT(*)) > 0",
    group + "COUNT(*) > 1 AND GREATEST(COUNT(*), 0) > 2",
    group + "GREATEST(COUNT(*), 0) > 1 OR COUNT(*) > 5",
    group + "GREATEST(COUNT(*), 0) BETWEEN 1 AND 5",
    group + "GREATEST(COUNT(*), 0) IN (1, 2)",
    group + "NOT GREATEST(COUNT(*), 0) > 1",
    group + "1 < GREATEST(COUNT(*), 0)",
    group + "COALESCE(COUNT(*), 0) > 1 AND status <> 'x'",
    "SELECT status, COUNT(*) AS c FROM t GROUP BY status HAVING COUNT(*) > GREATEST(MAX(x), 0)",
    "SELECT status FROM t GROUP BY status HAVING GREATEST(COUNT(*), 0) > 1",
    "SELECT COUNT(*) AS c FROM t HAVING GREATEST(COUNT(*), 0) > 1",
    "SELECT COUNT(*) AS c FROM t HAVING COALESCE(COUNT(*), 0) > 1",
    "SELECT status, MAX(x) - MIN(x) AS d FROM t GROUP BY status HAVING d > 3",
    "SELECT e.name FROM t JOIN UNNEST(t.emails) AS e GROUP BY e.name " +
    "HAVING GREATEST(COUNT(e.address), 0) > 1"
  )

  // ---------------------------------------------------------------------------------------------
  // S1 / S2 -- the shapes the engine CAN express now emit a selector (AC-1)
  // ---------------------------------------------------------------------------------------------

  "COALESCE over an aggregate" should "emit a bucket_selector reading the published metric" in {
    queryOf(group + "COALESCE(COUNT(*), 0) > 1") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"status":{""",
      terms,
      ""","aggs":{"c":{"value_count":{"field":"_index"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"c":"c"},""",
      """"script":{"source":"(params.c != null ? params.c : 0) > 1"}}}}}}}"""
    ).mkString
  }

  "GREATEST over an aggregate" should "emit a bucket_selector, null-guarded" in {
    queryOf(group + "GREATEST(COUNT(*), 0) > 1") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"status":{""",
      terms,
      ""","aggs":{"c":{"value_count":{"field":"_index"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"c":"c"},""",
      """"script":{"source":"(params.c == null ? false : """,
      """(Math.max(params.c, 0) > 1))"}}}}}}}"""
    ).mkString
  }

  // ---------------------------------------------------------------------------------------------
  // S7 -- a conjunction emits BOTH halves (AC-3). On `main` the second one VANISHED.
  // ---------------------------------------------------------------------------------------------

  "a conjunction of a bare and a wrapped aggregate" should "emit both conditions" in {
    queryOf(group + "COUNT(*) > 1 AND GREATEST(COUNT(*), 0) > 2") should include(
      """"script":{"source":"(params.c == null ? false : (params.c > 1)) && """ +
      """(params.c == null ? false : (Math.max(params.c, 0) > 2))"}"""
    )
  }

  "a disjunction" should "emit both conditions" in {
    // Dropping a disjunct makes the filter STRICTER than written: rows disappear silently.
    queryOf(group + "GREATEST(COUNT(*), 0) > 1 OR COUNT(*) > 5") should include(
      """(params.c == null ? false : (Math.max(params.c, 0) > 1)) || """ +
      """(params.c == null ? false : (params.c > 5))"""
    )
  }

  "a wrapped aggregate beside a bucket-key predicate" should "honour BOTH mechanisms" in {
    // The key predicate is a `terms` exclude, the aggregate one a `bucket_selector`. Neither may
    // cost the other.
    val q = queryOf(group + "COALESCE(COUNT(*), 0) > 1 AND status <> 'x'")
    // elastic4s 6 renders a single-value `exclude` as a bare string, 7+ as an array -- the ONE
    // documented emission difference this matrix touches.
    q should include(""""exclude":"x"""")
    q should include("""(params.c != null ? params.c : 0) > 1""")
  }

  // ---------------------------------------------------------------------------------------------
  // S9 -- the aggregation reached ONLY through the HAVING function must be CREATED (AC-5)
  // ---------------------------------------------------------------------------------------------

  "an aggregate reached only through a HAVING function" should "create its aggregation" in {
    queryOf("SELECT status FROM t GROUP BY status HAVING GREATEST(COUNT(*), 0) > 1") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"status":{""",
      terms,
      ""","aggs":{"count_all":{"value_count":{"field":"_index"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"count_all":"count_all"},""",
      """"script":{"source":"(params.count_all == null ? false : """,
      """(Math.max(params.count_all, 0) > 1))"}}}}}}}"""
    ).mkString
  }

  it should "create it inside a NESTED relation too" in {
    // 🔴 MEASURED on `main`: this statement emitted `{"query":{"match_all":{}},"_source":true}` --
    // no aggregation AT ALL. The GROUP BY itself vanished and raw documents came back.
    queryOf(
      "SELECT e.name FROM t JOIN UNNEST(t.emails) AS e GROUP BY e.name " +
      "HAVING GREATEST(COUNT(e.address), 0) > 1"
    ) shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"e":{"nested":{"path":"emails"},""",
      """"aggs":{"e.name":{"terms":{"field":"emails.name","size":65536,"min_doc_count":1},""",
      """"aggs":{"count_e_address":{"value_count":{"field":"emails.address"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"count_e_address":"count_e_address"},""",
      """"script":{"source":"(params.count_e_address == null ? false : """,
      """(Math.max(params.count_e_address, 0) > 1))"}}}}}}}}}"""
    ).mkString
  }

  "an aggregate reached only through the RIGHT operand" should "create and guard its aggregation" in {
    // 🔴 MEASURED on `main`: the script read `params.max_x`, `buckets_path` declared only `c`, and
    // no `max_x` aggregation existed -- so Elasticsearch ran `Math.abs(null)`.
    queryOf(
      "SELECT status, COUNT(*) AS c FROM t GROUP BY status HAVING COUNT(*) > GREATEST(MAX(x), 0)"
    ) shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"status":{""",
      terms,
      ""","aggs":{"c":{"value_count":{"field":"_index"}},"max_x":{"max":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"c":"c","max_x":"max_x"},""",
      """"script":{"source":"(params.c == null || params.max_x == null ? false : """,
      """(params.c > Math.max(params.max_x, 0)))"}}}}}}}"""
    ).mkString
  }

  // ---------------------------------------------------------------------------------------------
  // S8 -- the whole-table HAVING follows the same rule (AC-4)
  // ---------------------------------------------------------------------------------------------

  "a whole-table HAVING over a function of an aggregate" should "hang the selector off the synthetic bucket" in {
    queryOf("SELECT COUNT(*) AS c FROM t HAVING GREATEST(COUNT(*), 0) > 1") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,""",
      """"aggs":{"__whole_table_having__":{"filters":{"filters":{"_all":{"match_all":{}}}},""",
      """"aggs":{"c":{"value_count":{"field":"_index"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"c":"c"},""",
      """"script":{"source":"(params.c == null ? false : """,
      """(Math.max(params.c, 0) > 1))"}}}}}}}"""
    ).mkString
  }

  // ---------------------------------------------------------------------------------------------
  // AC-2 -- nothing in the matrix emits a query with the predicate missing
  // ---------------------------------------------------------------------------------------------

  private val refused: Seq[String] = Seq(
    group + "ABS(COUNT(*)) > 1",
    group + "FLOOR(COUNT(*)) > 1",
    group + "NULLIF(COUNT(*), 0) > 1",
    group + "NULLIF(c, 0) > 1",
    group + "COALESCE(NULLIF(COUNT(*), 0), 5) > 1",
    group + "CASE WHEN COUNT(*) > 1 THEN 1 ELSE 0 END = 1",
    group + "COUNT(*) > 1 AND NULLIF(COUNT(*), 0) > 2",
    "SELECT status, MAX(amount) AS m FROM t GROUP BY status HAVING ROUND(MAX(amount), 2) > 1",
    "SELECT COUNT(*) AS c FROM t HAVING NULLIF(COUNT(*), 0) > 1"
  )

  "a HAVING the engine cannot express" should "never reach emission" in {
    refused.foreach { sql =>
      withClue(s"[$sql] ") {
        Parser(sql) match {
          case Left(e) => e.msg should startWith("HAVING cannot")
          case Right(s) =>
            fail(s"expected a rejection; it emitted ${queryOf(sql)} from $s")
        }
      }
    }
  }

  "every statement that DOES emit" should "carry a having_filter" in {
    // Non-vacuity: the invariant below would pass on an empty set, which is exactly the shape the
    // defect produced.
    emitted.foreach { sql =>
      withClue(s"[$sql] ") {
        valuesOf(mapper.readTree(queryOf(sql)), "bucket_selector") should not be empty
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The structural invariant (AD-2 item 3)
  // ---------------------------------------------------------------------------------------------

  private val paramRef = "params\\.([A-Za-z_][A-Za-z0-9_]*)".r

  private def valuesOf(node: JsonNode, key: String): Seq[JsonNode] = {
    val here = Option(node.get(key)).toSeq
    val below = node.elements().asScala.toSeq.flatMap(valuesOf(_, key))
    here ++ below
  }

  "every emitted bucket pipeline" should "read exactly the metrics its buckets_path declares" in {
    emitted.foreach { sql =>
      withClue(s"[$sql] ") {
        val root = mapper.readTree(queryOf(sql))
        val pipelines = valuesOf(root, "bucket_selector") ++ valuesOf(root, "bucket_script")
        pipelines should not be empty
        pipelines.foreach { pipeline =>
          val declared = pipeline.get("buckets_path").fieldNames().asScala.toSet
          val source = Option(pipeline.get("script"))
            .map { s =>
              if (s.isTextual) s.asText() else s.get("source").asText()
            }
            .getOrElse(fail("a bucket pipeline with no script"))
          val read = paramRef.findAllMatchIn(source).map(_.group(1)).toSet - "__now__"
          read shouldBe declared
        }
      }
    }
  }

  it should "null-test every metric a bucket_selector declares" in {
    // The AC-4b contract, restated for the #389 population: a selector either guards the metric
    // (`params.x == null ? false : ...`) or handles the null itself (`params.x != null ? ... : k`,
    // which is what COALESCE means). Dereferencing it unguarded fails the whole search.
    emitted.foreach { sql =>
      withClue(s"[$sql] ") {
        val root = mapper.readTree(queryOf(sql))
        valuesOf(root, "bucket_selector").foreach { pipeline =>
          val declared = pipeline.get("buckets_path").fieldNames().asScala.toSet
          val source = pipeline.get("script").get("source").asText()
          declared.foreach { key =>
            withClue(s"metric '$key' in [$source] ") {
              assert(
                source.contains(s"params.$key == null") || source.contains(s"params.$key != null"),
                "is never null-tested"
              )
            }
          }
        }
      }
    }
  }
}
