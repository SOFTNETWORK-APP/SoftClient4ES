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
  * 🔴 NO bridge file changed in round 3 — the whole rule lives in `sql`. This twin is what PROVES
  * it: the same derived matrix and the same pins against a different elastic4s major, with two
  * expectations differing (elastic4s 6 renders a single-value `terms` `include` / `exclude` as a
  * bare string).
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

  private val paramRef = "params\\.([A-Za-z_][A-Za-z0-9_]*)".r

  private def valuesOf(node: JsonNode, key: String): Seq[JsonNode] = {
    val here = Option(node.get(key)).toSeq
    val below = node.elements().asScala.toSeq.flatMap(valuesOf(_, key))
    here ++ below
  }

  private val terms = """"terms":{"field":"status","size":65536,"min_doc_count":1}"""
  private val group = "SELECT status, COUNT(*) AS c FROM t GROUP BY status HAVING "

  // ---------------------------------------------------------------------------------------------
  // The matrix is DERIVED, not enumerated
  //
  // 🔴 The first version of this file carried two hand-written literal lists, and BOTH excluded the
  // whole family in which the left operand is a bare aggregate and the RIGHT one carries the
  // function -- which is exactly the family that emitted four HTTP-400 scripts and one
  // `Internal parser error`. A count over a hand-written list is a literal asserting itself
  // (`feedback_assert_the_mechanism_not_a_proxy`): the population has to be derived, and every cell
  // has to get a VERDICT.
  // ---------------------------------------------------------------------------------------------

  private val wrappers: Seq[String => String] = Seq(
    a => s"COALESCE($a, 0)",
    a => s"GREATEST($a, 0)",
    a => s"LEAST($a, 99)",
    a => s"SIGN($a)",
    a => s"ABS($a)",
    a => s"FLOOR($a)",
    a => s"ROUND($a, 2)",
    a => s"NULLIF($a, 0)",
    a => s"CASE WHEN $a > 1 THEN 1 ELSE 0 END"
  )

  private val aggregates = Seq("COUNT(*)", "MAX(amount)", "SUM(amount)")

  /** Every operand POSITION a wrapped aggregate can occupy. The second and third are the ones the
    * hand-written lists missed.
    */
  private val positions: Seq[String => String] = Seq(
    w => s"$w > 1",
    w => s"1 < $w",
    w => s"COUNT(*) > $w",
    w => s"NOT $w > 1",
    w => s"$w BETWEEN 1 AND 5",
    w => s"$w IN (1, 2)",
    w => s"COUNT(*) > 1 AND $w > 2",
    w => s"COUNT(*) > 1 OR $w > 2"
  )

  private val matrix: Seq[String] =
    for {
      wrap <- wrappers
      agg  <- aggregates
      pos  <- positions
    } yield group + pos(wrap(agg))

  "every cell of the derived matrix" should "either emit a filter or be refused by name -- never both, never neither" in {
    // 🔴 The verdict, not the shape. On `main` 149 of the 247 statements of this family answered
    // HTTP 200 with the predicate SILENTLY DROPPED; the contract is that NONE does.
    val silent = matrix.filter { sql =>
      Parser(sql) match {
        case Left(_) => false
        case Right(_) =>
          val root = mapper.readTree(queryOf(sql))
          valuesOf(root, "bucket_selector").isEmpty
      }
    }
    withClue(
      s"${silent.size} statements emit NO filter and no refusal:\n${silent.take(8).mkString("\n")}\n"
    )(
      silent shouldBe empty
    )
  }

  it should "name the HAVING clause in every refusal" in {
    val refusals = matrix.flatMap(sql => Parser(sql).left.toOption.map(sql -> _.msg))
    // Non-vacuity computed over the material: the matrix MUST contain refusals, or the assertion
    // above could pass on an all-emitting set.
    refusals.size should be >= 100
    refusals.foreach { case (sql, msg) =>
      withClue(s"[$sql] ") {
        msg should startWith("HAVING cannot")
        msg should not startWith Parser.InternalParseFailure
      }
    }
  }

  it should "cover both operand sides, so the right-hand family cannot go missing again" in {
    // The guard on the guard: if `positions` ever loses the right-operand rows, this reddens.
    val rightHand = matrix.filter(_.contains("COUNT(*) > COALESCE"))
    rightHand should not be empty
    val bounds = matrix.filter(_.contains("BETWEEN 1 AND 5"))
    bounds should not be empty
  }

  /** The shapes whose EXACT emission is pinned below, each executed against a real cluster. The
    * structural invariants at the bottom run over these AND over the derived matrix above.
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
  // The GROUP BY key population -- BUCKET level only (round 3)
  //
  // 🔴 The round-2 document push-down is GONE. Its licence ("a function of the key is constant
  // within a bucket") is false when the KEY is a function of the column and the predicate reads the
  // column, and false again on a MULTI-VALUED field where one document belongs to several buckets.
  // Both were measured as HTTP 200 wrong answers on Elasticsearch 8.18.3.
  // ---------------------------------------------------------------------------------------------

  "a key predicate the terms filter expresses" should "stay in the terms filter and touch nothing else" in {
    val q = queryOf(group + "status = 'a'")
    q should include(""""include":"a"""")
    // 🔴 No query filter: the round-2 push-down put one here, and that is what made a multi-valued
    // grouping wrong.
    q should include(""""query":{"match_all":{}}""")
    q should not include "having_filter"
  }

  it should "leave the WHERE clause exactly as written" in {
    val q = queryOf(
      "SELECT status, COUNT(*) AS c FROM t WHERE amount > 1 GROUP BY status HAVING status = 'a'"
    )
    q should include(""""include":"a"""")
    q should not include "toUpperCase"
  }

  "a key predicate the terms filter cannot express" should "never reach emission at all" in {
    Seq("UPPER(status) = 'A'", "LENGTH(status) = 1").foreach { p =>
      withClue(s"[$p] ") {
        Parser(group + p) match {
          case Left(e)  => e.msg should startWith("HAVING cannot")
          case Right(_) => fail(s"accepted; it emitted ${queryOf(group + p)}")
        }
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // The include/exclude CHANNEL -- it unions, it never intersects (rule b2)
  //
  // 🔴 `excludes` IS `includes(bucket, !not, …)`. Every assertion above this point is include-side,
  // which is how an inverted rule once shipped: it reasoned about the include sense and was applied
  // by a method that is also the exclude sense. These cells are the EXCLUDE side, derived.
  // ---------------------------------------------------------------------------------------------

  "an AND of inequalities" should "union into ONE exclude list, exactly as on 455433ae" in {
    queryOf(group + "status <> 'a' AND status <> 'b'") should include(
      """"exclude":["a","b"]"""
    )
    queryOf(group + "status NOT IN ('a','b') AND status <> 'c'") should include(
      """"exclude":["a","b","c"]"""
    )
  }

  it should "keep working beside a metric, which is a different mechanism" in {
    val q = queryOf(group + "status <> 'a' AND COUNT(*) > 1")
    // ⚠️ ES6 RENDERING, and it is a RESIDUAL, not a choice: this bridge renders a SINGLE-element
    // value list as a bare string, which Elasticsearch 6.8 reads as a REGEX. It happens to select
    // the right terms here (anchored, no metacharacters) but it is why a set-based include beside
    // a single-valued exclude is rejected outright by 6.8 -- recorded, pre-existing, not fixed.
    q should include(""""exclude":"a"""")
    q should include("bucket_selector")
  }

  "a combination the channel cannot express" should "never reach emission" in {
    // Each of these emitted a SILENT WRONG ANSWER on `455433ae` -- measured:
    //   `<> a OR <> b`  -> exclude:["a","b"], though the disjunction is true for EVERY bucket
    //   `= a AND = b`   -> include:["a","b"], though the SQL means NO bucket
    //   `= a OR <> b`   -> include:["a"] AND exclude:["b"], a conjunction where SQL says OR
    //   `LIKE a% AND LIKE b%` -> include:"a.*" only, the second pattern dropped
    Seq(
      "status <> 'a' OR status <> 'b'",
      "status NOT IN ('a','b') OR status <> 'c'",
      "status = 'a' AND status = 'b'",
      "status = 'a' OR status <> 'b'",
      "status LIKE 'a%' AND status LIKE 'b%'"
    ).foreach { p =>
      withClue(s"[$p] ") {
        Parser(group + p) match {
          case Left(e)  => e.msg should startWith("HAVING cannot")
          case Right(_) => fail(s"accepted; it emitted ${queryOf(group + p)}")
        }
      }
    }
  }

  it should "still allow one include and one exclude under a conjunction" in {
    // Both lists MULTI-valued, so this bridge renders both as arrays -- see the residual note
    // above; ES 6.8 rejects a set-based include beside a regex-rendered exclude.
    val q = queryOf(group + "status IN ('a','b') AND status NOT IN ('c','d')")
    q should include(""""include":["a","b"]""")
    q should include(""""exclude":["c","d"]""")
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
    // elastic4s 6 renders a single-value `exclude` as a bare string, 7+ as an array.
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
