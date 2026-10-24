package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.operator.AND
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
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

  private val group = "SELECT category, COUNT(*) AS c FROM t GROUP BY category HAVING "

  private def excludesOf(sql: String): BucketIncludesExcludes =
    Parser(sql) match {
      case Right(s: SingleSearch) =>
        val bucket = s.buckets.headOption.getOrElse(fail(s"no bucket in [$sql]"))
        s.having
          .flatMap(_.criteria)
          .map(_.excludes(bucket, not = false, BucketIncludesExcludes()))
          .getOrElse(fail(s"no HAVING criteria in [$sql]"))
      case other => fail(s"Expected a SingleSearch, got $other")
    }

  "a HAVING IN on the grouped field" should "include each value itself, never its SQL rendering" in {
    // SQL `'a\b'` keeps an unknown escape verbatim: the three characters a, backslash, b.
    includesOf(group + "category IN ('a\\b')").values shouldBe Set("a\\b")
    includesOf(group + "category IN ('it''s')").values shouldBe Set("it's")
    includesOf(group + "category IN ('a\\b', 'it''s', 'x')").values shouldBe
    Set("a\\b", "it's", "x")
  }

  it should "exclude each value itself under NOT IN" in {
    excludesOf(group + "category NOT IN ('a\\b')").values shouldBe Set("a\\b")
    excludesOf(group + "category NOT IN ('it''s')").values shouldBe Set("it's")
  }

  "IN and =" should "derive the same key-filter value for EVERY printable character" in {
    // A differential between the two forms over a DERIVED alphabet, not the two characters the
    // defect happened to involve. `=` has always read the value; IN read its `.sql`.
    val alphabet: Seq[Char] = (0x20 to 0x7e).map(_.toChar) :+ '\u00e9'
    val differing = alphabet.filter { c =>
      val literal = "'" + s"x${c}y".replace("'", "''") + "'"
      includesOf(group + s"category = $literal").values !=
        includesOf(group + s"category IN ($literal)").values
    }
    differing shouldBe empty
  }

  private val keySchema: SchemaTable = SchemaTable(
    "t",
    columns = List(
      Column("n", SQLTypes.BigInt),
      Column("i", SQLTypes.Int),
      Column("dbl", SQLTypes.Double),
      Column("category", SQLTypes.Keyword),
      Column(
        "items",
        SQLTypes.Array(SQLTypes.Struct),
        multiFields = List(Column("qty", SQLTypes.BigInt))
      )
    )
  )

  /** The query venue as core runs it: parse, attach the mapping, then `validateResolved` -- once,
    * and a second time (the engine resolves a statement again on the scrolling path).
    */
  private def verdicts(sql: String): Seq[(String, Either[String, Unit])] =
    Parser(sql) match {
      case Right(s: SingleSearch) =>
        val once = s.update(Some(keySchema))
        Seq(
          "once"  -> once.validateResolved(),
          "twice" -> once.update(Some(keySchema)).validateResolved()
        )
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  private def unnested(predicate: String): String =
    "SELECT inner_items.qty AS q, MIN(inner_items.qty) AS m FROM t " +
    s"JOIN UNNEST(t.items) AS inner_items GROUP BY inner_items.qty HAVING $predicate"

  private def keyed(key: String, predicate: String): String =
    s"SELECT $key, COUNT(*) AS c FROM t GROUP BY $key HAVING $predicate"

  private val fractionalRefusal = "HAVING cannot compare the integer GROUP BY key"

  /** The statement as core runs it, resolved once and a second time (the fold is idempotent). */
  private def resolvedTwice(sql: String): Seq[SingleSearch] =
    Parser(sql) match {
      case Right(s: SingleSearch) =>
        val once = s.update(Some(keySchema))
        Seq(once, once.update(Some(keySchema)))
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  /** What a resolved HAVING over the key answers: `None` for no group (a WHERE that filters no
    * document in, the HAVING gone), `Some(None)` for every group (no HAVING), `Some(Some(sql))` for
    * the HAVING left.
    */
  private def answered(s: SingleSearch): Option[Option[String]] = {
    def filtersNothingIn(c: Criteria): Boolean = c match {
      case _: MatchNoneCriteria          => true
      case Predicate(l, AND, r, None, _) => filtersNothingIn(l) || filtersNothingIn(r)
      case _                             => false
    }
    if (s.where.flatMap(_.criteria).exists(filtersNothingIn)) None
    else Some(s.having.flatMap(_.criteria).map(_.sql))
  }

  "a key filter comparing an INTEGER key with a fractional number" should
  "answer as SQL does once the mapping is known: no integer is equal to it" in {
    // (statement, DuckDB 1.5.5's answer as `answered` reads it) -- collected, so a red run lists
    // EVERY cell
    val none: Option[Option[String]] = None
    val every: Option[Option[String]] = Some(None)
    val cases = Seq(
      keyed("n", "n = 1.5")         -> none,
      keyed("n", "n <> 1.5")        -> every,
      keyed("n", "n IN (1.5, 2.5)") -> none,
      keyed("n", "n NOT IN (0.5)")  -> every,
      // a list holding a whole number too keeps it alone
      keyed("n", "n IN (1.0, 2.5)") -> Some(Some("n IN (1.0)")),
      keyed("i", "i = -1.5")        -> none,
      // `PI` is a Value[Double] the equality grammar admits -- not a DoubleValue
      keyed("n", "n = PI") -> none,
      // the SELECT alias: the leaf `k` carries no column, its BUCKET does
      "SELECT n AS k, COUNT(*) AS c FROM t GROUP BY k HAVING k = 1.5" -> none,
      // a STRING reaches the filter as the same text, which Elasticsearch reads as a number
      keyed("n", "n = '1.5'")                                                -> none,
      keyed("n", "n <> '1.5'")                                               -> every,
      keyed("n", "n IN ('1.5')")                                             -> none,
      "SELECT n AS k, COUNT(*) AS c FROM t GROUP BY k HAVING k = '1.5'"      -> none,
      "SELECT n AS k, COUNT(*) AS c FROM t GROUP BY k HAVING k <> 'NaN'"     -> every,
      "SELECT n AS k, COUNT(*) AS c FROM t GROUP BY k HAVING k = 'Infinity'" -> none,
      // the GROUP BY position
      "SELECT n, COUNT(*) AS c FROM t GROUP BY 1 HAVING n = 1.5" -> none,
      // beside a metric, and under a NOT: the constant folds into the tree
      keyed("n", "n <> 1.5 AND COUNT(*) > 1")    -> Some(Some("COUNT(*) > 1")),
      keyed("n", "n = 1.5 AND COUNT(*) > 1")     -> none,
      keyed("n", "COUNT(*) > 1 AND n <> 1.5")    -> Some(Some("COUNT(*) > 1")),
      keyed("n", "n = 2 OR n = 1.5")             -> Some(Some("n = 2")),
      keyed("n", "COUNT(*) > 1 AND NOT n = 1.5") -> Some(Some("COUNT(*) > 1")),
      keyed("n", "NOT n = 1.5")                  -> every
    )
    val wrong = for {
      (sql, expected) <- cases
      resolved        <- resolvedTwice(sql)
      verdict = resolved.validateResolved()
      if verdict != Right(()) || answered(resolved) != expected
    } yield s"[$sql] expected $expected, got ${answered(resolved)} ($verdict)"
    withClue(s"${wrong.size} of ${cases.size * 2} wrong:\n${wrong.mkString("\n")}\n") {
      wrong shouldBe empty
    }
  }

  it should "stay refused where the fold does not reach: a key inside a nested relation" in {
    // an UNNEST key: its predicate sits inside the nested relation the resolution wraps it in
    val refused = Seq(
      unnested("inner_items.qty <> 1.5")       -> "inner_items.qty",
      unnested("inner_items.qty NOT IN (1.5)") -> "inner_items.qty",
      unnested("inner_items.qty = 1.5")        -> "inner_items.qty"
    )
    val wrong = for {
      (sql, key)            <- refused
      (resolution, verdict) <- verdicts(sql)
      if !verdict.left.exists(_.startsWith(s"$fractionalRefusal $key with "))
    } yield s"[$sql] resolved $resolution: expected the refusal naming $key, got $verdict"
    withClue(s"${wrong.size} of ${refused.size * 2} wrong:\n${wrong.mkString("\n")}\n") {
      wrong shouldBe empty
    }
    verdicts(unnested("inner_items.qty = 1.5")).head._2 shouldBe Left(
      "HAVING cannot compare the integer GROUP BY key inner_items.qty with 1.5: no integer is " +
      "equal to it, and Elasticsearch would compare the key with 1 instead. Compare it with a " +
      "whole number."
    )
  }

  it should "accept a whole number, an integer, a fraction on a DOUBLE key, and a keyword key" in {
    val accepted = Seq(
      keyed("n", "n = 2.0"),
      keyed("n", "n = 2"),
      keyed("n", "n IN (1, 2)"),
      keyed("n", "n IN (1.0, 2.0)"),
      keyed("dbl", "dbl = 1.5"),
      keyed("dbl", "dbl IN (1.5, 2.5)"),
      keyed("category", "category = '1.5'"),
      keyed("category", "category IN ('1.5', 'NaN')"),
      keyed("category", "category LIKE '1.5%'"),
      "SELECT n AS k, COUNT(*) AS c FROM t GROUP BY k HAVING k = 2.0",
      "SELECT n AS k, COUNT(*) AS c FROM t GROUP BY k HAVING k = '2'",
      // a metric compared with a fraction is not a key filter
      keyed("n", "COUNT(*) > 1.5"),
      unnested("inner_items.qty = 2.0"),
      unnested("inner_items.qty <> 2")
    )
    val wrong = for {
      sql                   <- accepted
      (resolution, verdict) <- verdicts(sql)
      if verdict != Right(())
    } yield s"[$sql] resolved $resolution: expected Right(()), got $verdict"
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
  }

  it should "leave a string that is not a number to Elasticsearch (CHARACTERIZATION)" in {
    // through the alias spelling, whose leaf carries no column -- so no type rule reads it either
    verdicts("SELECT n AS k, COUNT(*) AS c FROM t GROUP BY k HAVING k = 'abc'").map(_._2) shouldBe
    Seq(Right(()), Right(()))
  }

  it should "leave a statement with no mapping alone -- its key's type is unknown (CHARACTERIZATION)" in {
    Parser(keyed("n", "n = 1.5")).isRight shouldBe true
  }

  /** Story IDENT-4 -- the query venue's verdict once the mapping is attached (`resolveWithSchema`):
    * IDENT-3's [[verdicts]], which must agree at both resolutions.
    */
  private def resolvedVerdict(sql: String): Either[String, Unit] =
    verdicts(sql).map(_._2).distinct match {
      case Seq(verdict) => verdict
      case several      => Left(s"the two resolutions disagree: $several")
    }

  "a QUALIFIED integer key compared with a fractional number (IDENT-4)" should
  "answer no group once the mapping is known, in every spelling, and be left alone before it" in {
    // R1 has no mapping (every column is ANY), so nothing folds there -- the boundary IDENT-3
    // states, inherited by the qualified spelling. R2 is `SearchApi.resolveWithSchema`.
    val noGroupAfterResolution = Seq(
      "SELECT tb.n, COUNT(*) AS c FROM t tb GROUP BY tb.n HAVING tb.n = 1.5",
      "SELECT tb.n, COUNT(*) AS c FROM t tb GROUP BY tb.n HAVING n = 1.5",
      "SELECT n, COUNT(*) AS c FROM t tb GROUP BY n HAVING tb.n = 1.5",
      "SELECT tb.n, COUNT(*) AS c FROM t tb GROUP BY tb.n HAVING tb.n IN (1.5, 2.5)",
      "SELECT tb.n AS k, COUNT(*) AS c FROM t tb GROUP BY k HAVING k = 1.5"
    )
    val wrong = noGroupAfterResolution.flatMap { sql =>
      val atParse = Parser(sql) match {
        case Right(s: SingleSearch) if s.having.isDefined => None
        case other => Some(s"[$sql] R1: expected accepted (no mapping), got $other")
      }
      val afterResolution =
        if (atParse.isDefined) None // reported by atParse
        else
          resolvedTwice(sql).flatMap { resolved =>
            (resolved.validateResolved(), answered(resolved)) match {
              case (Right(()), None) => None
              case other             => Some(s"[$sql] R2: expected no group, got $other")
            }
          }.headOption
      atParse.toSeq ++ afterResolution.toSeq
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  it should "accept a whole number with a decimal point in every spelling" in {
    Seq(
      "SELECT tb.n, COUNT(*) AS c FROM t tb GROUP BY tb.n HAVING tb.n = 2.0",
      "SELECT tb.n, COUNT(*) AS c FROM t tb GROUP BY tb.n HAVING n = 2.0",
      "SELECT n, COUNT(*) AS c FROM t tb GROUP BY n HAVING tb.n = 2.0"
    ).flatMap { sql =>
      resolvedVerdict(sql) match {
        case Right(()) => None
        case other     => Some(s"[$sql] $other")
      }
    } shouldBe empty
  }
}
