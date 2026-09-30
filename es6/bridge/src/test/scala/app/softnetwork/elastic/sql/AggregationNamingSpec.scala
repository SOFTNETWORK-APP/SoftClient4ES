package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.query._
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.sksamuel.elastic4s.searches.aggs.{AbstractAggregation, Aggregation}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime
import scala.jdk.CollectionConverters._

/** Issues #54 / #223 (story BIDC-2): `buckets_path` and aggregation naming, and the Painless a
  * bucket pipeline emits.
  *
  * Every assertion here is on the GENERATED Elasticsearch query, never on the `.sql` render: the
  * defects this pins were invisible to a parse (the statements parsed, ran, and returned wrong
  * buckets). Two structural guards close the file: no emitted aggregation or `order` key is `""`
  * (AC 5), and no emitted Painless compares or dereferences a possibly-null value unguarded (AC 4b,
  * lead directive 2026-09-06).
  */
class AggregationNamingSpec extends AnyFlatSpec with Matchers {

  import scala.language.implicitConversions

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  implicit def sqlQueryToRequest(sqlQuery: SelectStatement): ElasticSearchRequest =
    sqlQuery.statement match {
      case Some(value: SingleSearch) => value.copy(score = sqlQuery.score)
      case other => throw new IllegalArgumentException(s"Not a single search: $other")
    }

  private def queryOf(sql: String): String = {
    val select: ElasticSearchRequest = SelectStatement(sql)
    select.query
  }

  private val terms = """"terms":{"field":"id","size":65536,"min_doc_count":1}"""

  // ---------------------------------------------------------------------------------------------
  // #54 -- an un-aliased aggregate is named after the WHOLE expression
  // ---------------------------------------------------------------------------------------------

  "a HAVING-only COUNT(field)" should "be named after the aggregate and resolve its buckets_path (issue #54)" in {
    queryOf("SELECT id FROM t GROUP BY id HAVING COUNT(x) > 5") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"count_x":{"value_count":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"count_x":"count_x"},""",
      """"script":{"source":"(params.count_x == null ? false : (params.count_x > 5))"}}}}}}}"""
    ).mkString
  }

  "a HAVING that references a SELECT aggregate by its alias" should "filter on that aggregate (issue #54)" in {
    // `cnt` has no aggregate function of its own: the selector saw no metric and the whole HAVING
    // was dropped -- every group came back. The alias now resolves to the SELECT item's aggregate.
    queryOf("SELECT id, COUNT(x) AS cnt FROM t GROUP BY id HAVING cnt > 5") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"cnt":{"value_count":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"cnt":"cnt"},""",
      """"script":{"source":"(params.cnt == null ? false : (params.cnt > 5))"}}}}}}}"""
    ).mkString
  }

  it should "resolve the alias of a transformed aggregate too (issue #54)" in {
    queryOf(
      "SELECT id, MAX(YEAR(createdAt)) AS y FROM t GROUP BY id HAVING y > 2020 AND COUNT(x) > 1"
    ) shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"y":{"max":{"field":"createdAt","script":{"lang":"painless",""",
      """"source":"def param1 = (doc['createdAt'].size() == 0 ? null : """,
      """doc['createdAt'].value.toInstant().atZone(ZoneId.of('Z')).get(ChronoField.YEAR)); param1"}}},""",
      """"count_x":{"value_count":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"y":"y","count_x":"count_x"},""",
      """"script":{"source":"(params.y == null ? false : (params.y > 2020)) && """,
      """(params.count_x == null ? false : (params.count_x > 1))"}}}}}}}"""
    ).mkString
  }

  "two aggregates over one field" should "stay two aggregations, both in buckets_path (issue #54)" in {
    // Both used to be named `x`; the second was dropped and every term compared the count.
    queryOf("SELECT id FROM t GROUP BY id HAVING COUNT(x) > 5 AND MAX(x) > 3") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"count_x":{"value_count":{"field":"x"}},"max_x":{"max":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"count_x":"count_x","max_x":"max_x"},""",
      """"script":{"source":"(params.count_x == null ? false : (params.count_x > 5)) && """,
      """(params.max_x == null ? false : (params.max_x > 3))"}}}}}}}"""
    ).mkString
  }

  it should "keep an ORDER BY aggregate beside a HAVING aggregate over the same field (issue #54)" in {
    // `ORDER BY MIN(x)` used to vanish entirely -- no `order` key at all.
    queryOf(
      "SELECT id, COUNT(x) AS c FROM t GROUP BY id HAVING MAX(x) > 3 ORDER BY MIN(x) DESC"
    ) shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      """"terms":{"field":"id","size":65536,"min_doc_count":1,"order":{"min_x":"desc"}},""",
      """"aggs":{"c":{"value_count":{"field":"x"}},"max_x":{"max":{"field":"x"}},""",
      """"min_x":{"min":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"max_x":"max_x"},""",
      """"script":{"source":"(params.max_x == null ? false : (params.max_x > 3))"}}}}}}}"""
    ).mkString
  }

  it should "tell COUNT from COUNT(DISTINCT) (issue #54)" in {
    queryOf("SELECT id FROM t GROUP BY id HAVING COUNT(x) > 5 AND COUNT(DISTINCT x) > 2") shouldBe
    Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"count_x":{"value_count":{"field":"x"}},""",
      """"count_distinct_x":{"cardinality":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"count_x":"count_x",""",
      """"count_distinct_x":"count_distinct_x"},""",
      """"script":{"source":"(params.count_x == null ? false : (params.count_x > 5)) && """,
      """(params.count_distinct_x == null ? false : (params.count_distinct_x > 2))"}}}}}}}"""
    ).mkString
  }

  "a dotted field" should "never leak into a buckets_path key (issue #54)" in {
    // `params.profile.age` is a nested-property read Painless rejects.
    queryOf("SELECT id FROM t GROUP BY id HAVING MAX(profile.age) > 30") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"max_profile_age":{"max":{"field":"profile.age"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"max_profile_age":"max_profile_age"},""",
      """"script":{"source":"(params.max_profile_age == null ? false : (params.max_profile_age > 30))"}}}}}}}"""
    ).mkString
  }

  it should "let a HAVING inside a nested level keep its selector (issue #54)" in {
    // The selector's name scanner saw `params.emails` (no such aggregation, not root level) and
    // dropped the condition: the HAVING was silently ignored.
    queryOf(
      """SELECT e.domain FROM customers c JOIN UNNEST(c.emails) AS e
        |GROUP BY e.domain HAVING COUNT(e.address) > 1""".stripMargin
    ) shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"e":{"nested":{"path":"emails"},""",
      """"aggs":{"e.domain":{"terms":{"field":"emails.domain","size":65536,"min_doc_count":1},""",
      """"aggs":{"count_e_address":{"value_count":{"field":"emails.address"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"count_e_address":"count_e_address"},""",
      """"script":{"source":"(params.count_e_address == null ? false : (params.count_e_address > 1))"}}}}}}}}}"""
    ).mkString
  }

  "a global metric of a direct nested child" should "resolve through its local name (AC 3)" in {
    // The `>`-joined child path: the only place `buckets_path` value and key legitimately differ.
    queryOf(
      """SELECT c.id, COUNT(DISTINCT e.address) AS nb
        |FROM customers c JOIN UNNEST(c.emails) AS e
        |GROUP BY c.id HAVING COUNT(DISTINCT e.address) > 1""".stripMargin
    ) shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"having_filter":{"bucket_selector":{"buckets_path":{"nb":"e>nb"},""",
      """"script":{"source":"(params.nb == null ? false : (params.nb > 1))"}}},""",
      """"e":{"nested":{"path":"emails"},"aggs":{"nb":{"cardinality":{"field":"emails.address"}}}}}}}}"""
    ).mkString
  }

  // ---------------------------------------------------------------------------------------------
  // #223 -- HAVING / ORDER BY over a transformed aggregate
  // ---------------------------------------------------------------------------------------------

  private val maxYearCreatedAt =
    """"max_year_createdat":{"max":{"field":"createdAt","script":{"lang":"painless",""" +
    """"source":"def param1 = (doc['createdAt'].size() == 0 ? null : """ +
    """doc['createdAt'].value.toInstant().atZone(ZoneId.of('Z')).get(ChronoField.YEAR)); param1"}}}"""

  "HAVING over a transformed aggregate" should "read the metric, guarded, with the transform applied once (issue #223)" in {
    // Was: `def left = (doc[..] ? null : ..get(YEAR)).get(YEAR); left == null ? false : (left > 2020)`
    // -- a `doc[]` read where there is no document, the transform twice, a method on a nullable.
    queryOf(
      "SELECT id, COUNT(x) AS c FROM t GROUP BY id HAVING MAX(YEAR(createdAt)) > 2020"
    ) shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"c":{"value_count":{"field":"x"}},""",
      maxYearCreatedAt,
      ""","having_filter":{"bucket_selector":{"buckets_path":{"max_year_createdat":"max_year_createdat"},""",
      """"script":{"source":"(params.max_year_createdat == null ? false : (params.max_year_createdat > 2020))"}}}}}}}"""
    ).mkString
  }

  it should "compose under OR as one guarded expression per term (issue #223)" in {
    // The statement form `def left = ...; left == null ? false : (left > 2020) || params.c > 5`
    // parsed as `left == null ? false : ((left > 2020) || params.c > 5)`: a null left swallowed B.
    queryOf(
      "SELECT id, COUNT(x) AS c FROM t GROUP BY id HAVING MAX(YEAR(createdAt)) > 2020 OR COUNT(x) > 5"
    ) shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"c":{"value_count":{"field":"x"}},""",
      maxYearCreatedAt,
      ""","having_filter":{"bucket_selector":{"buckets_path":{"max_year_createdat":"max_year_createdat","c":"c"},""",
      """"script":{"source":"(params.max_year_createdat == null ? false : (params.max_year_createdat > 2020)) || """,
      """(params.c == null ? false : (params.c > 5))"}}}}}}}"""
    ).mkString
  }

  it should "guard an aggregate on the right-hand side too (AC 4b)" in {
    queryOf("SELECT id FROM t GROUP BY id HAVING MAX(a) > MIN(b)") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"max_a":{"max":{"field":"a"}},"min_b":{"min":{"field":"b"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"max_a":"max_a","min_b":"min_b"},""",
      """"script":{"source":"(params.max_a == null || params.min_b == null ? false : (params.max_a > params.min_b))"}}}}}}}"""
    ).mkString
  }

  it should "convert a temporal literal to epoch millis inside the guard (issue #223)" in {
    // The conversion used to be appended to the WHOLE predicate; guarded, it would land on the boolean.
    queryOf(
      "SELECT id FROM t GROUP BY id HAVING MAX(DATE_TRUNC(createdAt, DAY)) > now - interval 7 day"
    ) shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"max_date_trunc_createdat_day":{"max":{"field":"createdAt","script":{"lang":"painless",""",
      """"source":"def param1 = (doc['createdAt'].size() == 0 ? null : """,
      """doc['createdAt'].value.toInstant().atZone(ZoneId.of('Z')).truncatedTo(ChronoUnit.DAYS)); param1"}}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"max_date_trunc_createdat_day":"max_date_trunc_createdat_day"},""",
      """"script":{"source":"(params.max_date_trunc_createdat_day == null ? false : """,
      """(params.max_date_trunc_createdat_day > ZonedDateTime.ofInstant(Instant.ofEpochMilli(params.__now__), ZoneId.of('Z'))""",
      """.minus(7, ChronoUnit.DAYS).toInstant().toEpochMilli()))","params":{"__now__":1767139200000}}}}}}}}"""
    ).mkString
  }

  "BETWEEN over an aggregate" should "read the guarded metric as two comparisons (AC 4b)" in {
    // The document form rendered the chained `1 <= p <= 5`, which Painless rejects.
    queryOf("SELECT id FROM t GROUP BY id HAVING COUNT(x) BETWEEN 1 AND 5") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"count_x":{"value_count":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"count_x":"count_x"},""",
      """"script":{"source":"(params.count_x == null ? false : (params.count_x >= 1 && params.count_x <= 5))"}}}}}}}"""
    ).mkString
  }

  it should "keep its NOT (AC 4b)" in {
    queryOf("SELECT id FROM t GROUP BY id HAVING COUNT(x) NOT BETWEEN 1 AND 5") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"count_x":{"value_count":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"count_x":"count_x"},""",
      """"script":{"source":"(params.count_x == null ? false : !(params.count_x >= 1 && params.count_x <= 5))"}}}}}}}"""
    ).mkString
  }

  "IN over an aggregate" should "read the guarded metric through a list membership (AC 4b)" in {
    queryOf("SELECT id FROM t GROUP BY id HAVING MAX(x) IN (1, 2)") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"max_x":{"max":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"max_x":"max_x"},""",
      """"script":{"source":"(params.max_x == null ? false : (params.max_x == 1 || params.max_x == 2))"}}}}}}}"""
    ).mkString
  }

  it should "keep its NOT (AC 4b)" in {
    queryOf("SELECT id FROM t GROUP BY id HAVING MAX(x) NOT IN (1, 2)") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"max_x":{"max":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"max_x":"max_x"},""",
      """"script":{"source":"(params.max_x == null ? false : !(params.max_x == 1 || params.max_x == 2))"}}}}}}}"""
    ).mkString
  }

  // `COUNT(x) IN (…)` was REJECTED by the shared Expression.validate (BIGINT vs ARRAY<BIGINT>) until
  // the third look, so it never reached the emission: the MAX pins above could not cover it. A
  // `value_count` metric arrives on the buckets_path as a number too, so the guarded `==` chain is
  // the same shape -- the S2-1 defect (Integer literals vs a boxed Double) was exactly a typing
  // mismatch that a parse-level pin cannot see.
  it should "emit the same guarded chain for a COUNT metric, newly accepted (T3-3)" in {
    queryOf("SELECT id FROM t GROUP BY id HAVING COUNT(x) IN (1, 2)") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"count_x":{"value_count":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"count_x":"count_x"},""",
      """"script":{"source":"(params.count_x == null ? false : (params.count_x == 1 || params.count_x == 2))"}}}}}}}"""
    ).mkString
  }

  it should "keep the NOT of a COUNT metric (T3-3)" in {
    queryOf("SELECT id FROM t GROUP BY id HAVING COUNT(x) NOT IN (1, 2)") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"count_x":{"value_count":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"count_x":"count_x"},""",
      """"script":{"source":"(params.count_x == null ? false : !(params.count_x == 1 || params.count_x == 2))"}}}}}}}"""
    ).mkString
  }

  "A AND NOT B" should "negate the RIGHT operand, inside its guard (R2-2)" in {
    // The selector used to prefix `!` to the LEFT operand -- the exact complement of what was asked.
    // The NOT is pushed into the right-hand comparison (`> 3` becomes `<= 3`) so a bucket whose
    // metric is missing still fails it, as SQL's three-valued NOT would.
    queryOf("SELECT id FROM t GROUP BY id HAVING COUNT(x) > 5 AND NOT MAX(x) > 3") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"count_x":{"value_count":{"field":"x"}},"max_x":{"max":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"count_x":"count_x","max_x":"max_x"},""",
      """"script":{"source":"((params.count_x == null ? false : (params.count_x > 5))) && """,
      """(params.max_x == null ? false : (params.max_x <= 3))"}}}}}}}"""
    ).mkString
  }

  it should "negate a leading NOT the same way" in {
    queryOf("SELECT id FROM t GROUP BY id HAVING NOT MAX(x) > 3") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"max_x":{"max":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"max_x":"max_x"},""",
      """"script":{"source":"(params.max_x == null ? false : (params.max_x <= 3))"}}}}}}}"""
    ).mkString
  }

  "a HAVING that references a bucket_script by its alias" should "read the sibling pipeline aggregation (R2-1)" in {
    // `d` used to stay a bare identifier: no metric, `1 == 1`, no having_filter -- every group back.
    queryOf("SELECT id, MAX(x) - MIN(x) AS d FROM t GROUP BY id HAVING d > 3") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"d":{"bucket_script":{"buckets_path":{"max_x":"max_x","min_x":"min_x"},""",
      """"script":"params.max_x - params.min_x"}},""",
      """"max_x":{"max":{"field":"x"}},"min_x":{"min":{"field":"x"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"d":"d"},""",
      """"script":{"source":"(params.d == null ? false : (params.d > 3))"}}}}}}}"""
    ).mkString
  }

  "a nested-level HAVING with now - interval" should "keep its condition (R2-6)" in {
    // `params.__now__` is the request clock, not a metric; resolved as Unknown it dropped the whole
    // condition inside a nested bucket (Unknown is only tolerated at root level).
    queryOf(
      """SELECT e.domain FROM customers c JOIN UNNEST(c.emails) AS e
        |GROUP BY e.domain HAVING MAX(e.sent) > now - interval 7 day""".stripMargin
    ) shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"e":{"nested":{"path":"emails"},""",
      """"aggs":{"e.domain":{"terms":{"field":"emails.domain","size":65536,"min_doc_count":1},""",
      """"aggs":{"max_e_sent":{"max":{"field":"emails.sent"}},""",
      """"having_filter":{"bucket_selector":{"buckets_path":{"max_e_sent":"max_e_sent"},""",
      """"script":{"source":"(params.max_e_sent == null ? false : (params.max_e_sent > """,
      """ZonedDateTime.ofInstant(Instant.ofEpochMilli(params.__now__), ZoneId.of('Z')).minus(7, ChronoUnit.DAYS)""",
      """.toInstant().toEpochMilli()))","params":{"__now__":1767139200000}}}}}}}}}}"""
    ).mkString
  }

  "the validator" should "reject the shapes the pipeline cannot express, loudly" in {
    Seq(
      "SELECT id FROM t GROUP BY id HAVING MAX(x) - MIN(x) > 3" ->
      "HAVING cannot combine aggregates arithmetically inline",
      "SELECT id FROM t WHERE COUNT(x) > 5 GROUP BY id" ->
      "Aggregate functions are not allowed in WHERE",
      "SELECT id, MIN(x) AS max_x FROM t GROUP BY id HAVING MAX(x) > 3" ->
      "Alias 'max_x' names a SELECT aggregate and a different aggregate"
    ).foreach { case (sql, reason) =>
      withClue(s"[$sql] ") {
        val rejected = app.softnetwork.elastic.sql.parser.Parser(sql).swap.toOption.map(_.msg)
        rejected shouldBe defined
        rejected.get should include(reason)
        // A boundary catch would ALSO yield a Left carrying the reason -- assert it is the grammar's.
        rejected.get should not startWith app.softnetwork.elastic.sql.parser.Parser.InternalParseFailure
      }
    }
  }

  "ORDER BY over a transformed aggregate" should "name its aggregation (issue #223)" in {
    // The sub-aggregation key and the `order` key were both `""`.
    queryOf(
      "SELECT id, COUNT(x) AS c FROM t GROUP BY id ORDER BY MAX(ABS(salary)) DESC"
    ) shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      """"terms":{"field":"id","size":65536,"min_doc_count":1,"order":{"max_abs_salary":"desc"}},""",
      """"aggs":{"c":{"value_count":{"field":"x"}},"max_abs_salary":{"max":{"script":{"lang":"painless",""",
      """"source":"def param1 = (doc['salary'].size() == 0 ? null : doc['salary'].value); """,
      """(param1 == null) ? null : Double.valueOf(Math.abs(param1))"}}}}}}}"""
    ).mkString
  }

  // ---------------------------------------------------------------------------------------------
  // #54 -- arithmetic over aggregates (bucket_script)
  // ---------------------------------------------------------------------------------------------

  "arithmetic over aggregates" should "create its operands and read them as params (issue #54)" in {
    // Was: no `max`/`min` at all, `buckets_path {"d":"d"}` (itself) and `params.x - params.x`.
    queryOf("SELECT id, MAX(x) - MIN(x) AS d FROM t GROUP BY id") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"d":{"bucket_script":{"buckets_path":{"max_x":"max_x","min_x":"min_x"},""",
      """"script":"params.max_x - params.min_x"}},""",
      """"max_x":{"max":{"field":"x"}},"min_x":{"min":{"field":"x"}}}}}}"""
    ).mkString
  }

  it should "reuse an operand that is also a SELECT item (issue #54)" in {
    queryOf("SELECT id, MAX(x) AS m, MAX(x) - MIN(x) AS d FROM t GROUP BY id") shouldBe Seq(
      """{"query":{"match_all":{}},"size":0,"_source":false,"aggs":{"id":{""",
      terms,
      ""","aggs":{"d":{"bucket_script":{"buckets_path":{"m":"m","min_x":"min_x"},""",
      """"script":"params.m - params.min_x"}},"m":{"max":{"field":"x"}},""",
      """"min_x":{"min":{"field":"x"}}}}}}"""
    ).mkString
  }

  // ---------------------------------------------------------------------------------------------
  // Structural guards over every shape above (and the ones the pins do not spell out)
  // ---------------------------------------------------------------------------------------------

  private val shapes: Seq[String] = Seq(
    "SELECT id FROM t GROUP BY id HAVING COUNT(x) > 5",
    "SELECT id, COUNT(x) FROM t GROUP BY id HAVING COUNT(x) > 5",
    "SELECT id, COUNT(x) AS cnt FROM t GROUP BY id HAVING cnt > 5",
    "SELECT id, MAX(YEAR(createdAt)) AS y FROM t GROUP BY id HAVING y > 2020 AND COUNT(x) > 1",
    "SELECT id FROM t GROUP BY id HAVING COUNT(x) BETWEEN 1 AND 5",
    "SELECT id FROM t GROUP BY id HAVING COUNT(x) NOT BETWEEN 1 AND 5",
    "SELECT id FROM t GROUP BY id HAVING MAX(x) IN (1, 2)",
    "SELECT id FROM t GROUP BY id HAVING MAX(x) NOT IN (1, 2)",
    "SELECT id FROM t GROUP BY id HAVING COUNT(x) IN (1, 2)",
    "SELECT id FROM t GROUP BY id HAVING COUNT(x) NOT IN (1, 2)",
    "SELECT id FROM t GROUP BY id HAVING COUNT(x) > 5 AND NOT MAX(x) > 3",
    "SELECT id FROM t GROUP BY id HAVING COUNT(x) > 5 AND NOT MAX(x) IN (1, 2)",
    "SELECT id FROM t GROUP BY id HAVING NOT MAX(x) > 3",
    "SELECT id, MAX(x) - MIN(x) AS d FROM t GROUP BY id HAVING d > 3",
    "SELECT id, COUNT(x) AS cnt FROM t GROUP BY id HAVING COUNT(x) > 1 ORDER BY COUNT(x) DESC",
    """SELECT e.domain FROM customers c JOIN UNNEST(c.emails) AS e
      |GROUP BY e.domain HAVING MAX(e.sent) > now - interval 7 day""".stripMargin,
    "SELECT id FROM t GROUP BY id HAVING COUNT(x) > 5 AND MAX(x) > 3",
    "SELECT id, COUNT(x) AS c FROM t GROUP BY id HAVING MAX(x) > 3 ORDER BY MIN(x) DESC",
    "SELECT id FROM t GROUP BY id HAVING COUNT(x) > 5 AND COUNT(DISTINCT x) > 2",
    "SELECT id FROM t GROUP BY id HAVING COUNT(*) > 2",
    "SELECT id FROM t GROUP BY id HAVING MAX(a) > MIN(b)",
    "SELECT id FROM t GROUP BY id HAVING MAX(profile.age) > 30",
    "SELECT id, COUNT(x) AS c FROM t GROUP BY id HAVING MAX(YEAR(createdAt)) > 2020",
    "SELECT id, COUNT(x) AS c FROM t GROUP BY id HAVING MAX(DATE_TRUNC(createdAt, MINUTE)) > 2020",
    "SELECT id, COUNT(x) AS c FROM t GROUP BY id HAVING MAX(ABS(salary)) > 10",
    "SELECT id, COUNT(x) AS c FROM t GROUP BY id HAVING MAX(YEAR(createdAt)) > 2020 OR COUNT(x) > 5",
    "SELECT id, COUNT(x) AS c FROM t GROUP BY id ORDER BY MAX(ABS(salary)) DESC",
    "SELECT id, COUNT(x) AS c FROM t GROUP BY id ORDER BY MAX(YEAR(createdAt)) DESC",
    "SELECT id, MAX(ABS(salary)) FROM t GROUP BY id ORDER BY MAX(ABS(salary)) DESC",
    "SELECT id, MAX(ABS(salary)) AS m FROM t GROUP BY id HAVING MAX(ABS(salary)) > 10",
    "SELECT id FROM t GROUP BY id HAVING MAX(DATE_TRUNC(createdAt, DAY)) > now - interval 7 day",
    "SELECT id, MAX(x) - MIN(x) AS d FROM t GROUP BY id",
    "SELECT id, MAX(ABS(x)) - MIN(x) AS d FROM t GROUP BY id",
    """SELECT c.id, COUNT(DISTINCT e.address) AS nb FROM customers c JOIN UNNEST(c.emails) AS e
      |GROUP BY c.id HAVING COUNT(DISTINCT e.address) > 1""".stripMargin,
    """SELECT c.id FROM customers c JOIN UNNEST(c.emails) AS e
      |GROUP BY c.id HAVING COUNT(DISTINCT e.address) > 1""".stripMargin,
    """SELECT e.domain, COUNT(e.address) AS nb FROM customers c JOIN UNNEST(c.emails) AS e
      |GROUP BY e.domain HAVING COUNT(e.address) > 1""".stripMargin,
    """SELECT e.domain FROM customers c JOIN UNNEST(c.emails) AS e
      |GROUP BY e.domain HAVING COUNT(e.address) > 1""".stripMargin
  ) ++ percentileShapes.map(_._1)

  private val mapper = new ObjectMapper()

  /** Every value of a field named `name`, anywhere in the tree. */
  private def valuesOf(node: JsonNode, name: String): Seq[JsonNode] = {
    val here = Option(node.get(name)).toSeq
    val below = node.elements().asScala.toSeq.flatMap(child => valuesOf(child, name))
    here ++ below
  }

  /** Every Painless script the query carries: `"script":{"source":...}` objects and the bare
    * `"script":"..."` form a bucket_script uses.
    */
  private def scriptsOf(root: JsonNode): Seq[String] =
    valuesOf(root, "script").flatMap { s =>
      if (s.isTextual) Some(s.asText())
      else Option(s.get("source")).filter(_.isTextual).map(_.asText())
    }

  private val paramRef = "params\\.([A-Za-z_][A-Za-z0-9_]*)".r

  "no emitted aggregation or order key" should "be the empty string (AC 5)" in {
    shapes.foreach { sql =>
      withClue(s"[$sql] ") {
        val root = mapper.readTree(queryOf(sql))
        valuesOf(root, "aggs").foreach { aggs =>
          aggs.fieldNames().asScala.toSeq.foreach(_ should not be empty)
        }
        valuesOf(root, "order").foreach { order =>
          order.fieldNames().asScala.toSeq.foreach(_ should not be empty)
        }
      }
    }
  }

  "a bucket pipeline script" should "read only declared, null-guarded params and never a document (AC 4, AC 4b)" in {
    shapes.foreach { sql =>
      withClue(s"[$sql] ") {
        val root = mapper.readTree(queryOf(sql))
        // A HAVING whose selector was dropped (the nested-level class this story fixed) would leave
        // nothing below to check -- the guard must not pass vacuously.
        if (sql.toUpperCase.contains("HAVING"))
          valuesOf(root, "bucket_selector") should not be empty
        val pipelines = valuesOf(root, "bucket_selector") ++ valuesOf(root, "bucket_script")
        pipelines.foreach { pipeline =>
          val declared = pipeline.get("buckets_path").fieldNames().asScala.toSet
          scriptsOf(pipeline).foreach { script =>
            script should not include "doc["
            val read = paramRef.findAllMatchIn(script).map(_.group(1)).toSet - "__now__"
            read shouldBe declared
            // The selector null-guards every metric it compares; a bucket_script computes a value
            // (a null operand yields a null result, which Elasticsearch treats as a gap).
            if (pipeline.has("script") && Option(pipeline.get("script")).exists(_.isObject))
              declared.foreach(key => script should include(s"params.$key == null"))
          }
        }
      }
    }
  }

  "no emitted Painless" should "dereference or compare a possibly-null value unguarded (AC 4b)" in {
    shapes.foreach { sql =>
      withClue(s"[$sql] ") {
        scriptsOf(mapper.readTree(queryOf(sql))).foreach(assertNullSafe)
      }
    }
  }

  /** The null-safety rule (lead directive 2026-09-06) on ONE Painless script. */
  private def assertNullSafe(script: String): Unit =
    withClue(s"script [$script] ") {
      // (a) A `( ... ? null : ... )` group must not be dereferenced: `(cond ? null : v).m()`
      //     invokes `m` on null whenever `cond` holds -- the doubled `.get(ChronoField.YEAR)`.
      nullTernaryGroupEnds(script).foreach { end =>
        if (end + 1 < script.length) script.charAt(end + 1) should not be '.'
      }
      // (b) A `def` bound to such a group is nullable: any later `name.` dereference must be
      //     preceded by a `name == null` / `name != null` test.
      nullableDefs(script).foreach { name =>
        val afterDef = script.substring(script.indexOf(s"def $name") + 4 + name.length)
        if (afterDef.contains(s"$name."))
          assert(
            afterDef.contains(s"$name == null") || afterDef.contains(s"$name != null"),
            s"'$name.' is dereferenced without a null test"
          )
      }
    }

  // ---------------------------------------------------------------------------------------------
  // Issue #222 (story BIDC-3) -- extended_stats over a transform. The metric scripts these shapes
  // make reach Elasticsearch 8 / 9 for the first time are held to the same rule. The Default
  // serializer refuses to render them (it would drop the script), so they are read off the built
  // request's aggregation tree -- the ScriptedExtendedStatsAggregation marker -- not off JSON.
  // ---------------------------------------------------------------------------------------------

  private val transformExtendedStatsShapes: Seq[String] = Seq(
    "SELECT id, STDDEV(YEAR(createdAt)) AS s FROM t GROUP BY id",
    "SELECT id, STDDEV(ABS(salary)) AS s FROM t GROUP BY id",
    "SELECT id, VARIANCE(ABS(salary)) AS v FROM t GROUP BY id",
    "SELECT id, STDDEV_POP(DATE_TRUNC(createdAt, MINUTE)) AS s FROM t GROUP BY id",
    "SELECT id, VAR_SAMP(YEAR(createdAt)) AS a, VAR_POP(YEAR(createdAt)) AS b FROM t GROUP BY id",
    "SELECT id, STDDEV(YEAR(createdAt)) AS s FROM t GROUP BY id HAVING COUNT(x) > 1",
    "SELECT id, name, STDDEV(YEAR(createdAt)) OVER (PARTITION BY id) AS s FROM t",
    "SELECT id, name, VARIANCE(ABS(salary)) OVER (PARTITION BY id) AS v FROM t",
    "SELECT id, name, STDDEV_SAMP(YEAR(createdAt)) OVER (PARTITION BY id) AS s FROM t"
  )

  private def markerScriptsOf(aggs: Iterable[AbstractAggregation]): Seq[String] =
    aggs.toSeq.flatMap {
      case m: ScriptedExtendedStatsAggregation => m.inner.script.map(_.script).toSeq
      case a: Aggregation                      => markerScriptsOf(a.subaggs)
      case _                                   => Seq.empty
    }

  "an extended_stats over a transform" should "carry a null-safe metric script, plain and windowed (issue #222)" in {
    transformExtendedStatsShapes.foreach { sql =>
      withClue(s"[$sql] ") {
        val request: ElasticSearchRequest = SelectStatement(sql)
        request.hasTransformExtendedStats shouldBe true
        val scripts = markerScriptsOf(request.search.aggs)
        scripts should not be empty
        scripts.foreach(assertNullSafe)
      }
    }
  }

  /** Index of the `)` closing each parenthesised group whose body carries a `? null :`. */
  private def nullTernaryGroupEnds(script: String): Seq[Int] = {
    val marker = "? null :"
    Iterator
      .iterate(script.indexOf(marker))(from => script.indexOf(marker, from + 1))
      .takeWhile(_ >= 0)
      .toList // strict on both Scala legs (2.12's Iterator.toSeq is a lazy Stream)
      .flatMap { at =>
        // Walk back to the unmatched `(` that opens the group holding this ternary...
        var depth = 0
        var open = at - 1
        while (open >= 0 && !(script(open) == '(' && depth == 0)) {
          script(open) match {
            case ')' => depth += 1
            case '(' => depth -= 1
            case _   =>
          }
          open -= 1
        }
        // ...then forward to its matching `)`.
        if (open < 0) None
        else {
          depth = 0
          var close = open
          var found = -1
          while (found < 0 && close < script.length) {
            script(close) match {
              case '(' => depth += 1
              case ')' =>
                depth -= 1
                if (depth == 0) found = close
              case _ =>
            }
            close += 1
          }
          if (found >= 0) Some(found) else None
        }
      }
  }

  /** Names bound by `def <name> = <initializer>;` whose initializer carries a `? null :`. */
  private def nullableDefs(script: String): Seq[String] =
    "def ([A-Za-z_][A-Za-z0-9_]*) = ([^;]*);".r
      .findAllMatchIn(script)
      .filter(_.group(2).contains("? null :"))
      .map(_.group(1))
      .toSeq

  // ---------------------------------------------------------------------------------------------
  // Story IDENT-1 -- a percentile a bucket pipeline or a terms `order` reads is read BY KEY
  //
  // Percentiles over one value column share one `percentiles` aggregation, and only the RESPONSE
  // side used to redirect a merged column to its owner's `values`. A `buckets_path` or a terms
  // `order` read the percentile by its OWN name, so under a merge it named no aggregation (the
  // selector read nothing: zero groups at HTTP 200; the `order` was dropped), and on its owner it
  // named a node carrying several percents (refused: "contains multiple values"). Found repeating a
  // SELECT percentile with another spelling of its column (the render now keeps the qualifier and
  // the quoting); the same holes existed for another percent of the column and for the alias of a
  // merged SELECT percentile. Every read is now `<node>[<percent>]` on the node that carries the
  // percent -- the one form Elasticsearch accepts on every major, for a merged node and a
  // single-percent one alike (MEASURED on 6.8.23, 7.17.29, 8.18.3 and 9.0.3). Asserted at both
  // resolutions: R1 is `Parser.apply`'s single one (an index pattern is sent after it), R2 the
  // schema-attaching second one a search over one concrete index gets.
  // ---------------------------------------------------------------------------------------------

  private val percentileSchema: SchemaTable = SchemaTable(
    "t",
    columns = List(Column("k", SQLTypes.Keyword), Column("x", SQLTypes.Double))
  )

  private def bodiesAt(sql: String): Seq[(String, String)] = {
    val r1 = SelectStatement(sql).statement match {
      case Some(single: SingleSearch) => single
      case other                      => fail(s"[$sql] is not a single search: $other")
    }
    Seq("R1" -> r1, "R2" -> r1.update(Some(percentileSchema))).map { case (res, single) =>
      res -> (single: ElasticSearchRequest).query
    }
  }

  private val keyedPath = """([^\[\]>]+)\[([^\[\]]+)\]""".r

  /** Every value a `bucket_selector` or a `bucket_script` reads (its `buckets_path`) and every key
    * a terms `order` sorts on, with the node it reads and, for a percentile, the percent its key
    * asks for -- failing on a name no sibling aggregation carries, on a percentile read without a
    * key, and on a key its node does not carry.
    */
  private def pipelineReads(json: String): Seq[(String, JsonNode, Option[Double])] = {
    def entries(order: JsonNode): Seq[String] =
      if (order.isArray) order.elements().asScala.toSeq.flatMap(_.fieldNames().asScala)
      else order.fieldNames().asScala.toSeq
    def read(path: String, siblings: JsonNode): (String, JsonNode, Option[Double]) = {
      val (name, key) = path match {
        case keyedPath(node, percent) => (node, Some(percent))
        case node                     => (node, None)
      }
      val node = Option(siblings)
        .flatMap(aggs => Option(aggs.get(name)))
        .getOrElse(fail(s"`$path` names no aggregation"))
      if (node.has("percentiles")) {
        val percent =
          key.getOrElse(fail(s"`$path` reads a percentiles node without a key")).toDouble
        withClue(s"`$path` ")(percentsOf(node) should contain(percent))
        (path, node, Some(percent))
      } else {
        withClue(s"`$path` ")(key shouldBe None)
        (path, node, None)
      }
    }
    def walk(node: JsonNode): Seq[(String, JsonNode, Option[Double])] =
      Option(node.get("aggs")).toSeq.flatMap { aggs =>
        aggs.fields().asScala.toSeq.flatMap { entry =>
          val agg = entry.getValue
          val pipelines = Seq("bucket_selector", "bucket_script")
            .flatMap(kind => Option(agg.get(kind)))
            .flatMap(_.get("buckets_path").fields().asScala.toSeq)
            .map(path => read(path.getValue.asText(), aggs))
          val ordered =
            Option(agg.get("terms")).flatMap(t => Option(t.get("order"))).toSeq.flatMap { order =>
              entries(order).filterNot(_.startsWith("_")).map(read(_, agg.get("aggs")))
            }
          pipelines ++ ordered ++ walk(agg)
        }
      }
    walk(mapper.readTree(json))
  }

  private def percentsOf(node: JsonNode): Seq[Double] =
    Option(node.get("percentiles"))
      .map(_.get("percents").elements().asScala.toSeq.map(_.asDouble()))
      .getOrElse(fail(s"not a percentiles aggregation: $node"))

  /** The percents of every emitted `percentiles` aggregation, one entry per aggregation. */
  private def percentileNodes(json: String): Seq[Seq[Double]] =
    valuesOf(mapper.readTree(json), "percentiles").map(
      _.get("percents").elements().asScala.toSeq.map(_.asDouble())
    )

  // lazy: `shapes`, initialised above, reads them through `percentileShapes`
  private lazy val p50 = "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY %s)"
  private lazy val px = "PERCENTILE_CONT(%s) WITHIN GROUP (ORDER BY x)"
  private lazy val pair =
    s"SELECT k, ${px.format("0.5")} AS a, ${px.format("0.9")} AS b FROM t GROUP BY k"

  /** (statement, the percent every percentile read must key) */
  private lazy val percentileShapes: Seq[(String, Double)] =
    (for {
      (selected, repeated) <- Seq(
        "t.x"   -> "x",
        "x"     -> "t.x",
        "\"x\"" -> "x",
        "x"     -> "\"x\"",
        "t.x"   -> "\"x\""
      )
      clause <- Seq(s"HAVING ${p50.format(repeated)} > 1", s"ORDER BY ${p50.format(repeated)} DESC")
    } yield s"SELECT k, ${p50.format(selected)} AS p FROM t GROUP BY k $clause" -> 50.0) ++ Seq(
      s"SELECT k, ${px.format("0.5")} AS a FROM t GROUP BY k HAVING ${px.format("0.9")} > 1" -> 90.0,
      s"SELECT k, ${px.format("0.5")} AS a FROM t GROUP BY k ORDER BY ${px.format("0.9")} DESC" -> 90.0,
      s"$pair HAVING b > 1"                   -> 90.0,
      s"$pair ORDER BY b DESC"                -> 90.0,
      s"$pair HAVING a > 1"                   -> 50.0,
      s"$pair HAVING ${px.format("0.9")} > 1" -> 90.0,
      // no GROUP BY: the one implicit group's `filters` bucket carries the selector
      s"SELECT ${px.format("0.5")} AS a, ${px.format("0.9")} AS b FROM t HAVING b > 1" -> 90.0
    )

  "a percentile a pipeline or an order reads" should "be read by its key on the node that carries it, at both resolutions" in {
    percentileShapes.foreach { case (sql, percent) =>
      bodiesAt(sql).foreach { case (res, json) =>
        withClue(s"[$res] $sql\n$json\n") {
          val read = pipelineReads(json).flatMap(_._3)
          read should not be empty // a dropped selector or order reads nothing -- never vacuous
          read.foreach(_ shouldBe percent)
        }
      }
    }
  }

  it should "leave every percentile over the column in ONE aggregation, whatever reads it" in {
    // every spelling of `x` is one value column: `t.x`, `x` and `"x"` share one node at R2, the
    // resolution a search over one concrete index sends
    percentileShapes.foreach { case (sql, _) =>
      val (_, json) = bodiesAt(sql).last
      withClue(s"[R2] $sql\n$json\n")(percentileNodes(json) should have size 1)
    }
    bodiesAt(s"${pair.replace(" FROM", s", ${px.format("0.99")} AS c FROM")} HAVING b > 1")
      .foreach { case (res, json) =>
        withClue(s"[$res] $json\n") {
          percentileNodes(json) shouldBe Seq(Seq(50.0, 90.0, 99.0))
          pipelineReads(json).map(_._1) shouldBe Seq("a[90.0]")
        }
      }
  }

  it should "be read by key by a bucket_script too, merged or not" in {
    val difference = s"${px.format("0.9")} - ${px.format("0.5")}"
    Seq(
      s"SELECT k, $difference AS d FROM t GROUP BY k",
      s"SELECT k, ${px.format("0.5")} AS a, ${px.format("0.9")} AS b, $difference AS d " +
      "FROM t GROUP BY k"
    ).foreach { sql =>
      bodiesAt(sql).foreach { case (res, json) =>
        withClue(s"[$res] $sql\n$json\n") {
          val read = pipelineReads(json)
          read.flatMap(_._3).sorted shouldBe Seq(50.0, 90.0)
          percentileNodes(json) shouldBe Seq(Seq(50.0, 90.0))
        }
      }
    }
  }

  it should "be read by key under an UNNEST too, where its aggregation name is a path" in {
    val p = "PERCENTILE_CONT(%s) WITHIN GROUP (ORDER BY e.size)"
    val select = s"SELECT e.domain, ${p.format("0.5")} AS a, ${p.format("0.9")} AS b " +
      "FROM customers c JOIN UNNEST(c.emails) AS e GROUP BY e.domain"
    Seq(s"$select HAVING b > 1", s"$select ORDER BY b DESC").foreach { sql =>
      val json = queryOf(sql)
      withClue(s"$sql\n$json\n") {
        pipelineReads(json).map(r => r._1 -> r._3) shouldBe Seq("a[90.0]" -> Some(90.0))
        percentileNodes(json) shouldBe Seq(Seq(50.0, 90.0))
      }
    }
  }

  /** The repeat over the SAME qualified spelling used to be refused as "a different aggregate"
    * under the SELECT alias (one copy resolved, one not); it now reads exactly what the alias does.
    */
  "a percentile repeated in HAVING over its qualified spelling" should "emit exactly what HAVING on its alias emits" in {
    val select = s"SELECT k, ${p50.format("t.x")} AS p FROM t GROUP BY k"
    bodiesAt(s"$select HAVING ${p50.format("t.x")} > 1")
      .zip(bodiesAt(s"$select HAVING p > 1"))
      .foreach { case ((res, repeated), (_, alias)) =>
        withClue(s"[$res] ")(repeated shouldBe alias)
      }
  }
}
