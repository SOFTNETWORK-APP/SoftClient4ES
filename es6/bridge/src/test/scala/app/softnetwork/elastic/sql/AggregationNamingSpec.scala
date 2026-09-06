package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.query._
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
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
      """"script":{"source":"(params.count_x == null ? false : (!(params.count_x >= 1 && params.count_x <= 5)))"}}}}}}}"""
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
      """"script":{"source":"(params.max_x == null ? false : (!(params.max_x == 1 || params.max_x == 2)))"}}}}}}}"""
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
  )

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
        scriptsOf(mapper.readTree(queryOf(sql))).foreach { script =>
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
        }
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
}
