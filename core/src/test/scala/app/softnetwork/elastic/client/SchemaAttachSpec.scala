package app.softnetwork.elastic.client

import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Schema, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

/** Issue #306 — the schema now reaches the EXECUTION path.
  *
  * `SQLTypeUtils.coerce` is indexed by the operand's `baseType`, which for an identifier is
  * `col.map(_.dataType)`, and `col` is populated only by `SingleSearch.update(Some(schema))`. Until
  * this issue that method had exactly ONE production call site — `Table.mergeWithSearch`, which
  * infers a CTAS target table's COLUMNS and returns a `Table` rather than a statement to execute —
  * so every executing query reached `coerce` with `baseType = SQLTypes.Any` and **no cast over a
  * column ever emitted a conversion, for any source type**.
  *
  * `SearchApi.resolveTemporalLiterals` now attaches the schema it was already loading. This spec
  * pins the two properties the fix rests on, both of which were only asserted in a source COMMENT
  * before it existed:
  *
  *   1. the seam ATTACHES — a statement that comes back out of `resolveTemporalLiterals` carries
  *      the mapped column types, so the conversion is emitted;
  *   2. the attach is IDEMPOTENT — `SearchApi.search` resolves and then routes a row query through
  *      `scrollRows` -> `ScrollApi.scroll`, which resolves AGAIN, so it genuinely runs twice on
  *      that path. Story 21.3 lost a round to a resolution that was not idempotent (a substituted
  *      node whose SHAPE the resolver also matched), so this is pinned rather than assumed.
  */
class SchemaAttachSpec extends AnyFlatSpec with Matchers {

  private val testLogger: Logger = LoggerFactory.getLogger(getClass)

  private val schema: Schema = Table(
    "events",
    columns = List(
      Column("id", SQLTypes.Keyword),
      Column("code", SQLTypes.Keyword),
      Column("descr", SQLTypes.Text),
      Column("amount", SQLTypes.Double),
      Column("event_ts", SQLTypes.Date)
    )
  )

  private class SchemaClient extends NopeClientApi {
    override protected def logger: Logger = testLogger
  }

  private def seeded(): SchemaClient = {
    val client = new SchemaClient
    client.updateSchema("events", schema)
    client
  }

  private def parse(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  private def painlessOf(s: SingleSearch): String = s.select.fields.head.painless(None)

  // -- 1. the seam attaches -----------------------------------------------------
  "resolveTemporalLiterals" should "attach the schema, so a cast over a COLUMN converts" in {
    val client = seeded()
    val parsed = parse("SELECT CAST(code AS BIGINT) AS n FROM events LIMIT 5")

    // BEFORE the attach this is the whole story: no schema, `baseType` is Any, no arm fires.
    painlessOf(parsed) should not include "parseLong"

    client.resolveTemporalLiterals(parsed) match {
      case ElasticSuccess(resolved) =>
        painlessOf(resolved) should include("Long.parseLong")
      case ElasticFailure(error) => fail(s"unexpected failure: ${error.message}")
    }
  }

  it should "attach it for a statement with NO temporal literal at all" in {
    // The early return this method used to make (`if (!TemporalLiterals.hasCandidates(single))`)
    // would skip the lookup here — and almost no statement carries a temporal WHERE literal, so
    // the attach would have been dead for nearly every query.
    val client = seeded()
    val parsed = parse("SELECT CAST(amount AS BIGINT) AS m FROM events WHERE amount > 10 LIMIT 5")
    client.resolveTemporalLiterals(parsed) match {
      case ElasticSuccess(resolved) => painlessOf(resolved) should include("(long)")
      case ElasticFailure(error)    => fail(s"unexpected failure: ${error.message}")
    }
  }

  it should "leave a multi-source or wildcard FROM untouched (the precondition still holds)" in {
    val client = seeded()
    val wildcard = parse("SELECT CAST(code AS BIGINT) AS n FROM events* LIMIT 5")
    client.resolveTemporalLiterals(wildcard) match {
      case ElasticSuccess(resolved) => painlessOf(resolved) should not include "parseLong"
      case ElasticFailure(error)    => fail(s"unexpected failure: ${error.message}")
    }
  }

  // -- 2. idempotency, which the scrollRows path REQUIRES ------------------------
  "the attach" should "be IDEMPOTENT — `search` -> `scrollRows` -> `scroll` resolves twice" in {
    // Not a hypothetical: `SearchApi.search` resolves, builds the ElasticQuery, and hands the
    // ALREADY-resolved statement to `scrollRows`, which calls `ScrollApi.scroll`, which resolves
    // again. Both emissions must be byte-identical or the routing decides the answer.
    Seq(
      "SELECT CAST(code AS BIGINT) AS n FROM events LIMIT 5",
      "SELECT CAST(amount AS BIGINT) AS m FROM events LIMIT 5",
      "SELECT CAST(descr AS DATE) AS d FROM events LIMIT 5",
      "SELECT UPPER(code) AS u FROM events LIMIT 5"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val once = parse(sql).update(Some(schema))
        val twice = once.update(Some(schema))
        painlessOf(twice) shouldBe painlessOf(once)
        twice.sql shouldBe once.sql
      }
    }
  }

  it should "be idempotent through the SEAM applied twice, not only through update()" in {
    val client = seeded()
    val parsed = parse("SELECT CAST(code AS BIGINT) AS n FROM events LIMIT 5")
    val first = client.resolveTemporalLiterals(parsed) match {
      case ElasticSuccess(r)     => r
      case ElasticFailure(error) => fail(error.message)
    }
    val second = client.resolveTemporalLiterals(first) match {
      case ElasticSuccess(r)     => r
      case ElasticFailure(error) => fail(error.message)
    }
    painlessOf(second) shouldBe painlessOf(first)
    painlessOf(second) should include("Long.parseLong")
  }

  // -- 3. the nullable ternary must box, or the script does not COMPILE ----------
  "a nullable conversion" should "box its result so the ternary branches unify" in {
    // 🔴 Measured on real Elasticsearch 8.18 while landing #306:
    //   class_cast_exception: Cannot cast from [long] to [java.lang.Object].
    // Painless unifies a ternary's branches, and a PRIMITIVE cannot unify with `null`. Every
    // conversion arm that returns a primitive — `((long) x)`, `Long.parseLong(x).longValue()` —
    // therefore has to be boxed before the null-guard wraps it.
    //
    // This was LATENT and unreachable before #306: `nullable` is true only for an identifier
    // operand, and no identifier ever reached an arm while `baseType` was Any. A literal operand
    // is `nullable = false` and skips the ternary entirely, which is why every literal case passed
    // before AND after.
    val attached = parse("SELECT CAST(amount AS BIGINT) AS m FROM events LIMIT 5").update(Some(schema))
    val script = painlessOf(attached)
    script should include("(def)")
    script should include("(long)")
    // the guard shape itself: a null operand still yields null, never a primitive default
    script should include("!= null")
  }
}
