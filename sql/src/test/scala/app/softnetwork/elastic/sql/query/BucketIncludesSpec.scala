package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.parser.Parser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The `terms` aggregation's `include` / `exclude` (and their regex form) are derived from a HAVING
  * criterion's value. They must come from the VALUE, never from its SQL rendering: `.sql` carries
  * the quote delimiters and, since string literals are escaped, a value holding one backslash would
  * reach Elasticsearch holding two — an include that matches nothing.
  */
class BucketIncludesSpec extends AnyFlatSpec with Matchers {

  private def includesOf(sql: String): BucketIncludesExcludes =
    Parser(sql) match {
      case Right(s: SingleSearch) =>
        val bucket = s.buckets.headOption.getOrElse(fail(s"no bucket in [$sql]"))
        s.having
          .flatMap(_.criteria)
          .map(_.includes(bucket, not = false, BucketIncludesExcludes()))
          .getOrElse(fail(s"no HAVING criteria in [$sql]"))
      case other => fail(s"Expected a SingleSearch, got $other")
    }

  "a HAVING equality on the grouped field" should "include the value itself" in {
    includesOf(
      "SELECT category, COUNT(*) AS c FROM t GROUP BY category HAVING category = 'books'"
    ).values shouldBe Set("books")
  }

  it should "not escape a backslash the value carries" in {
    // SQL source `'a\\b'` is the two-character value `a\b`.
    includesOf(
      "SELECT category, COUNT(*) AS c FROM t GROUP BY category HAVING category = 'a\\\\b'"
    ).values shouldBe Set("a\\b")
  }

  it should "keep an apostrophe the value carries" in {
    includesOf(
      "SELECT category, COUNT(*) AS c FROM t GROUP BY category HAVING category = 'it\\'s'"
    ).values shouldBe Set("it's")
  }

  "a HAVING LIKE on the grouped field" should "build a regex without the quote delimiters" in {
    includesOf(
      "SELECT category, COUNT(*) AS c FROM t GROUP BY category HAVING category LIKE 'book%'"
    ).regex shouldBe Some("book.*")
  }

  "a HAVING RLIKE on the grouped field" should "pass the pattern through verbatim" in {
    includesOf(
      "SELECT category, COUNT(*) AS c FROM t GROUP BY category HAVING category RLIKE 'boo.+'"
    ).regex shouldBe Some("boo.+")
  }
}
