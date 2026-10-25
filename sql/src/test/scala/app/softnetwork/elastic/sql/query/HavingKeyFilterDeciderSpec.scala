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

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.toRegex
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Rule (b2)'s exact decision (`SingleSearch.keyFilterExactly`): a HAVING combination on one GROUP
  * BY key is accepted when the two lists both bridges send keep exactly the groups SQL keeps, and
  * refused when they keep others -- decided on the lists, never on the combination's shape.
  *
  * Each accepted row below was answered by 0.23.0 with the same lists (RUN, and equal to DuckDB's
  * answer on the gate's fixture), then refused by the shape rules alone.
  */
class HavingKeyFilterDeciderSpec extends AnyFlatSpec with Matchers {

  private val group = "SELECT sval, COUNT(*) AS c FROM ident3_pop_keys GROUP BY sval HAVING "

  private def parsed(sql: String): Either[String, SingleSearch] =
    Parser(sql) match {
      case Right(ss: SingleSearch) => Right(ss)
      case Right(other)            => fail(s"[$sql] expected a SingleSearch, got $other")
      case Left(err)               => Left(err.msg)
    }

  private def unvalidated(sql: String): SingleSearch =
    Parser.parseUnvalidated(sql) match {
      case Right(ss: SingleSearch) => ss
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  /** The (kept, removed) lists the statement sends on its first level, as both bridges emit them: a
    * list holding a pattern is sent as the pattern alone.
    */
  private def lists(ss: SingleSearch): (BucketIncludesExcludes, BucketIncludesExcludes) = {
    val empty = BucketIncludesExcludes()
    val criteria = ss.having.flatMap(_.criteria).getOrElse(fail("no HAVING"))
    def emitted(list: BucketIncludesExcludes): BucketIncludesExcludes =
      if (list.regex.isDefined) list.copy(values = Set.empty) else list
    (
      emitted(criteria.includes(ss.buckets.head, not = false, empty)),
      emitted(criteria.excludes(ss.buckets.head, not = false, empty))
    )
  }

  private def decided(sql: String, rlikeFullMatch: Boolean = false): Option[Boolean] = {
    val ss = unvalidated(sql)
    ss.keyFilterExactly(ss.having.flatMap(_.criteria).get, ss.buckets.head, rlikeFullMatch)
  }

  private def pattern(p: String) = BucketIncludesExcludes(regex = Some(p))
  private def values(vs: String*) = BucketIncludesExcludes(values = vs.toSet)
  private val none = BucketIncludesExcludes()

  "a combination whose lists keep exactly SQL's groups" should "be accepted, whatever its shape" in {
    Seq(
      // the conjunction holds for no key, and the pattern the list keeps is SQL's answer
      "sval = 'axb' AND sval IN ('a','b') OR sval LIKE 'a%'" -> (pattern("a.*") -> none),
      // two kept values that are one value: the union is the intersection
      "sval = 'axb' AND sval NOT IN ('x','b') AND sval = 'axb'" -> (values("axb") -> values(
        "x",
        "b"
      )),
      // both values match the pattern that replaces them
      "(sval = 'axb' OR sval LIKE 'a%') OR sval = 'a.b'" -> (pattern("a.*") -> none),
      // no removed value matches the pattern, so the AND changes nothing
      "sval LIKE 'a%' OR sval LIKE 'a%' AND sval NOT IN ('x','b')" -> (pattern("a.*") -> values(
        "x",
        "b"
      )),
      // equalities joined by AND and OR together
      "sval = 'v1' OR sval = 'v2' AND NOT sval = 'v3'" -> (values("v1", "v2") -> values("v3"))
    ).foreach { case (having, expected) =>
      val sql = group + having
      withClue(sql) {
        decided(sql) shouldBe Some(true)
        val ss = parsed(sql).fold(reason => fail(s"refused: $reason"), identity)
        lists(ss) shouldBe expected
      }
    }
    // beside an aggregate, which is another stage of the request
    parsed(
      group + "(sval = 'axb' AND sval IN ('a','b') OR sval LIKE 'a%') AND COUNT(*) > 1"
    ).isRight shouldBe true
  }

  "a combination whose lists keep other groups" should "be refused, with the reason its shape gives" in {
    // `include:"b.*"` drops the value `a`, which SQL keeps
    decided(group + "sval = 'a' OR sval LIKE 'b%'") shouldBe Some(false)
    parsed(group + "sval = 'a' OR sval LIKE 'b%'").left.getOrElse("") should startWith(
      "HAVING cannot combine the conditions in sval = 'a' OR sval LIKE 'b%' on one GROUP BY key: " +
      "a key filter carries one list of values and one pattern"
    )
    // the lists drop `v3`, which SQL keeps
    decided(group + "sval = 'v3' OR sval = 'v2' AND NOT sval = 'v3'") shouldBe Some(false)
    parsed(group + "sval = 'v3' OR sval = 'v2' AND NOT sval = 'v3'").left.getOrElse("") should
    include("each is a union")
    // the conjunction holds for no key, and the include list keeps `a` and `b`
    decided(group + "sval = 'a' AND sval = 'b'") shouldBe Some(false)
  }

  "the four ident3-C shapes 0.23.0 answered wrong" should "stay refused" in {
    Seq(
      "SELECT sval, lval, COUNT(*) AS c FROM ident3_pop_keys GROUP BY sval, lval " +
      "HAVING sval IN ('a.b','axb') OR lval IN (1, 2)",
      "SELECT sval, lval, COUNT(*) AS c FROM ident3_pop_keys GROUP BY sval, lval " +
      "HAVING sval LIKE 'a%' OR lval IN (1, 2)",
      "SELECT sval, lval, COUNT(*) AS c FROM ident3_pop_keys GROUP BY sval, lval " +
      "HAVING sval = 'a*' OR lval = 1.5",
      group + "sval NOT RLIKE 'a\\.b|axb' AND sval NOT LIKE 'a.%'"
    ).foreach { sql => withClue(sql) { parsed(sql).isLeft shouldBe true } }
  }

  "an RLIKE" should "not be decided while its reading of the key is not settled" in {
    // exact under a full match of the key (the engine's reading), not under a partial one
    val sql = group + "sval = 'a.b' OR sval RLIKE 'a.*'"
    decided(sql) shouldBe None
    decided(sql, rlikeFullMatch = true) shouldBe Some(true)
    parsed(sql).isLeft shouldBe true
  }

  it should "not be decided over a pattern Lucene and Java read differently" in {
    val sql = group + "sval RLIKE 'a.*&~(ab)' OR sval = 'x'"
    decided(sql, rlikeFullMatch = true) shouldBe None
    parsed(sql).isLeft shouldBe true
    Seq("a.*&~(ab)", "a@", "#", "<1-5>", "\"a\"", "^a", "a$", "\\d+", "(?i)a", "a*?", "a+?", "a*+")
      .foreach { p => withClue(p) { SingleSearch.keyPatternAsJava(p) shouldBe None } }
    Seq("a.*", "a|b", "[ab]+", "a\\.b", toRegex("a.b%"), toRegex("it's 100%_^$&@~#<>\""))
      .foreach { p => withClue(p) { SingleSearch.keyPatternAsJava(p).isDefined shouldBe true } }
  }

  // --------------------------------------------------------------------- a key IS NOT NULL

  private val keyed = "SELECT k AS g, COUNT(*) AS c FROM t GROUP BY k HAVING "

  "a key IS NOT NULL" should "keep every group, as a level builds no NULL group" in {
    // DuckDB keeps every non-NULL group; Elasticsearch builds no bucket for a NULL key
    Seq(
      keyed + "k IS NOT NULL",
      keyed + "ISNOTNULL(k)",
      "SELECT k AS g, COUNT(*) AS c FROM t GROUP BY g HAVING g IS NOT NULL",
      "SELECT UPPER(k) AS g, COUNT(*) AS c FROM t GROUP BY UPPER(k) HAVING ISNOTNULL(UPPER(k))"
    ).foreach { sql =>
      withClue(sql) {
        val ss = parsed(sql).fold(reason => fail(s"refused: $reason"), identity)
        lists(ss) shouldBe ((none, none))
      }
    }
  }

  it should "combine as TRUE, and be refused where the lists cannot keep every group" in {
    // TRUE AND k = 'a' is k = 'a'
    lists(parsed(keyed + "k IS NOT NULL AND k = 'a'").fold(fail(_), identity)) shouldBe
    ((values("a"), none))
    // NOT ISNULL(k) is k IS NOT NULL, where the grammar reads it (after an operator)
    lists(parsed(keyed + "k = 'a' AND NOT ISNULL(k)").fold(fail(_), identity)) shouldBe
    ((values("a"), none))
    lists(parsed(keyed + "k = 'a' AND NOT k IS NULL").fold(fail(_), identity)) shouldBe
    ((values("a"), none))
    // TRUE OR k = 'a' keeps every group, and the list would keep `a` alone
    decided(keyed + "k IS NOT NULL OR k = 'a'") shouldBe Some(false)
    parsed(keyed + "k IS NOT NULL OR k = 'a'").left.getOrElse("") should startWith(
      "HAVING cannot combine the conditions in k IS NOT NULL OR k = 'a' on one GROUP BY key"
    )
    // undecided beside it, it is refused: the pattern alone would be sent
    parsed(keyed + "k IS NOT NULL OR k RLIKE 'a.*'").isLeft shouldBe true
  }

  it should "leave the aggregate to the group filter, and an OR with it refused" in {
    val and = parsed(keyed + "k IS NOT NULL AND COUNT(*) > 1").fold(fail(_), identity)
    lists(and) shouldBe ((none, none))
    parsed(keyed + "k IS NOT NULL OR COUNT(*) > 1").left.getOrElse("") should startWith(
      "HAVING cannot OR conditions that Elasticsearch applies with different mechanisms"
    )
  }

  "a key IS NULL" should "stay refused: it keeps only the NULL group, which no level builds" in {
    Seq(
      keyed + "k IS NULL",
      keyed + "ISNULL(k)",
      // NOT ISNOTNULL(k) is k IS NULL
      keyed + "k <> 'a' OR NOT ISNOTNULL(k)",
      // a function of the key is no test of the key
      "SELECT k AS g, COUNT(*) AS c FROM t GROUP BY k HAVING ISNOTNULL(UPPER(k))"
    ).foreach { sql =>
      withClue(sql) {
        parsed(sql).left.getOrElse("") should startWith("HAVING cannot filter on ")
      }
    }
    // a NOT at the start of a HAVING is not read before ISNULL
    parsed(keyed + "NOT ISNULL(k)").isLeft shouldBe true
  }

  // ------------------------------------------- a numeric FIELD level: values name numbers

  private val numeric = app.softnetwork.elastic.sql.schema.Table(
    "t",
    columns = List(
      app.softnetwork.elastic.sql.schema
        .Column("n", app.softnetwork.elastic.sql.`type`.SQLTypes.Int),
      app.softnetwork.elastic.sql.schema
        .Column("x", app.softnetwork.elastic.sql.`type`.SQLTypes.Double),
      app.softnetwork.elastic.sql.schema
        .Column("k", app.softnetwork.elastic.sql.`type`.SQLTypes.Keyword)
    )
  )

  private def resolved(sql: String): Either[String, Unit] =
    parsed(sql).flatMap(_.update(Some(numeric)).validateResolved())

  "a numeric field level" should "read 5 and 5.0 as ONE key, as Elasticsearch parses them" in {
    // include ["5"], exclude ["5.0"]: Elasticsearch removes the group 5, and DuckDB 1.5.5 keeps
    // (5, n) for both statements. Refused once the mapping says n is a number.
    Seq(
      "SELECT n AS g, COUNT(*) AS c FROM t GROUP BY n HAVING n = 5 AND (n IS NOT NULL OR NOT n = 5.0)",
      "SELECT n AS g, COUNT(*) AS c FROM t GROUP BY n HAVING n = 5 AND (n <> 7 OR NOT n = 5.0)",
      "SELECT x AS g, COUNT(*) AS c FROM t GROUP BY x HAVING x IN (1.0, 5.0) AND (x NOT IN (1, 5) OR x IS NOT NULL)"
    ).foreach { sql =>
      withClue(sql) {
        resolved(sql).left.getOrElse("") should startWith("HAVING cannot combine the conditions in")
      }
    }
    // the same spellings are distinct keys on a keyword level, where the lists are exact
    resolved(
      "SELECT k AS g, COUNT(*) AS c FROM t GROUP BY k HAVING k = '5' AND (k <> '7' OR NOT k = '5.0')"
    ) shouldBe Right(())
    // exact numerically: still answered
    resolved(
      "SELECT n AS g, COUNT(*) AS c FROM t GROUP BY n HAVING n = 5 OR n = 7 AND NOT n = 3.0"
    ) shouldBe Right(())
    resolved(
      "SELECT n AS g, COUNT(*) AS c FROM t GROUP BY n HAVING n IS NOT NULL AND n = 5.0"
    ) shouldBe
    Right(())
  }

  "an undecided or inexact combination beside a key IS NOT NULL" should
  "be refused with a reason that names its leaf" in {
    parsed(keyed + "k IS NOT NULL AND k RLIKE 'a.*'").left.getOrElse("") should include(
      "k RLIKE 'a.*' is a regular expression the key filter matches against the whole key"
    )
    parsed(keyed + "k IS NOT NULL OR k = 'a'").left.getOrElse("") should include(
      "k IS NOT NULL holds for every group"
    )
  }

  // ------------------------------------- the key kinds whose key filter is proven, and the others

  /** A mapping as Elasticsearch returns it, with one field of every kind. */
  private lazy val kinds = app.softnetwork.elastic.schema
    .Index(
      "hk",
      """{"hk":{"mappings":{"properties":{"k":{"type":"keyword"},"n":{"type":"integer"},""" +
      """"x":{"type":"double"},"r":{"type":"float"},"hf":{"type":"half_float"},""" +
      """"sf":{"type":"scaled_float","scaling_factor":100},"ul":{"type":"unsigned_long"},""" +
      """"d":{"type":"date"},"ip":{"type":"ip"}}},"settings":{"index":{}}}}"""
    )
    .schema

  private def onKinds(sql: String): Either[String, Unit] =
    parsed(sql).flatMap(_.update(Some(kinds)).validateResolved())

  private def unmapped(sql: String): Either[String, Unit] =
    parsed(sql).flatMap(_.validateUnresolved())

  private def ruleB(leaf: String): String =
    s"HAVING cannot filter on $leaf: a condition over the GROUP BY key is applied by the terms " +
    "filter, which can only express a direct comparison of the key (=, <>, LIKE, IN). Compare " +
    "the key itself, or filter in WHERE."

  private def grouped(key: String, having: String): String =
    s"SELECT $key AS g, COUNT(*) AS c FROM hk GROUP BY $key HAVING $having"

  "a key IS NOT NULL beside values of a level whose kind is not proven" should
  "be refused as before, with rule (b)'s reason" in {
    // Elasticsearch parses these levels' values its own way: 5 and 5.0 (or two spellings of one
    // date, one address) name one key, two in the lists. MEASURED on 8.18.3 for the 26124b9b
    // emission: no group where DuckDB 1.5.5 keeps (5, 1), and an error on unsigned_long.
    Seq("hf", "sf", "ul", "r").foreach { c =>
      val sql = grouped(c, s"$c = 5 AND ($c IS NOT NULL OR NOT $c = 5.0)")
      withClue(sql) { onKinds(sql) shouldBe Left(ruleB(s"$c IS NOT NULL")) }
    }
    val date =
      grouped("d", "d = '2024-01-01' AND (d IS NOT NULL OR NOT d = '2024-01-01T00:00:00Z')")
    onKinds(date) shouldBe Left(ruleB("d IS NOT NULL"))
    val ip = grouped("ip", "ip = '10.0.0.1' AND (ip IS NOT NULL OR NOT ip = '::ffff:10.0.0.1')")
    onKinds(ip) shouldBe Left(ruleB("ip IS NOT NULL"))
    onKinds(grouped("r", "r = 0.1 AND r IS NOT NULL")) shouldBe Left(ruleB("r IS NOT NULL"))
    // with no mapping no kind is proven: refused at the seam that sends the statement unresolved
    val noMapping =
      "SELECT n AS g, COUNT(*) AS c FROM t GROUP BY n HAVING n = 5 AND (n IS NOT NULL OR NOT n = 5.0)"
    parsed(noMapping).isRight shouldBe true
    unmapped(noMapping) shouldBe Left(ruleB("n IS NOT NULL"))
  }

  "a key IS NOT NULL alone" should "keep every group on a level of any kind, as it sends no list" in {
    // DuckDB 1.5.5 keeps every non-NULL group, and a level builds no bucket for a NULL key
    Seq(
      grouped("d", "d IS NOT NULL"),
      grouped("d", "ISNOTNULL(d)"),
      grouped("hf", "hf IS NOT NULL AND COUNT(*) > 1"),
      grouped("ip", "ip IS NOT NULL"),
      grouped("r", "r IS NOT NULL")
    ).foreach { sql => withClue(sql) { onKinds(sql) shouldBe Right(()) } }
    unmapped("SELECT n AS g, COUNT(*) AS c FROM t GROUP BY n HAVING n IS NOT NULL") shouldBe Right(
      ()
    )
  }

  "a combination on a level whose kind is not proven" should "be decided as before" in {
    // the equalities of the bare key joined by AND and OR: decided on their texts, as before
    // (distinct dates: DuckDB keeps the 2024-01-01 and 2024-01-02 groups)
    onKinds(
      grouped("d", "d = '2024-01-01' OR d = '2024-01-02' AND NOT d = '2024-01-03'")
    ) shouldBe Right(())
    // the shape rules otherwise: two kept lists under an AND are refused, as before
    onKinds(grouped("ip", "ip = '10.0.0.1' AND ip = '10.0.0.1'")).left.getOrElse("") should
    include("each is a union")
    // ... where a level of a proven kind is decided on its lists
    onKinds(grouped("k", "k = 'a' AND k = 'a'")) shouldBe Right(())
  }

  "a string computed from strings alone" should "be decided on its lists, as a keyword is" in {
    // the script's text goes through the same include / exclude as a keyword's terms
    onKinds(
      grouped("UPPER(k)", "UPPER(k) = 'AXB' AND UPPER(k) IN ('A', 'B') OR UPPER(k) LIKE 'A%'")
    ) shouldBe Right(())
    onKinds(grouped("LOWER(k)", "LOWER(k) = 'a' OR LOWER(k) LIKE 'a%'")) shouldBe Right(())
    onKinds(grouped("CONCAT(k, 'x')", "CONCAT(k, 'x') = 'ax' AND CONCAT(k, 'x') = 'ax'")) shouldBe
    Right(())
    // a text converted from a number is not proven: the shape rules judge it, as before
    onKinds(
      grouped("CAST(n AS VARCHAR)", "CAST(n AS VARCHAR) = '5' AND CAST(n AS VARCHAR) = '5'")
    ).left.getOrElse("") should include("each is a union")
    onKinds(
      grouped("CONCAT(k, 1)", "CONCAT(k, 1) = 'a1' AND CONCAT(k, 1) = 'a1'")
    ).left.getOrElse("") should include("each is a union")
  }
}
