package app.softnetwork.elastic.sql.parser

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 21.2 — AC-5. `Parser(stmt.sql) == Right(stmt)` over the whole table-name matrix.
  *
  * Two rows carry the design and must never be deleted as "redundant":
  *
  *   - `"logs-2025.03"` is the ONLY test that catches a render which quotes the index name PER
  *     dot-separated part. `"logs-2025"."03"` is a perfectly valid fixed point for a *different*
  *     statement — it re-parses to qualifier `logs-2025`, index `03` — so equality of the re-parsed
  *     AST against the ORIGINAL is the only assertion that sees it.
  *   - `"elastic".bi_events` is the row that proves the render is no longer lossy. Before this
  *     story it rendered `FROM bi_events` and the fixed point STILL HELD, because the qualifier was
  *     destroyed at parse time. A round-trip test alone never catches information loss; that is why
  *     every structural row below is PAIRED with an explicit rendered-TEXT assertion.
  */
class QuotedTableRoundTripSpec extends AnyFlatSpec with Matchers {

  private val fixedPoints = Seq(
    "SELECT category FROM bi_events LIMIT 1",
    "SELECT category FROM elastic.bi_events LIMIT 1",
    "SELECT category FROM logs-2025.03",
    "SELECT category FROM a.b.bi_events",
    """SELECT category FROM "bi_events" LIMIT 1""",
    "SELECT category FROM `bi_events` LIMIT 1",
    """SELECT category FROM "logs-2025.03"""",
    "SELECT category FROM `logs-2025.03`",
    "SELECT category FROM `logs-2025`.`03`",
    """SELECT category FROM "elastic".bi_events LIMIT 1""",
    "SELECT category FROM `elastic`.bi_events LIMIT 1",
    """SELECT category FROM "elastic"."bi_events" LIMIT 1""",
    "SELECT category FROM `elastic`.`bi_events` LIMIT 1",
    """SELECT category FROM "elastic"."bi_events" "bi_events"""",
    "SELECT category FROM `elastic`.`bi_events` `bi_events`",
    """SELECT a FROM "t1", "t2"""",
    """SELECT a FROM "elasticsearch"."prod-cluster"."bi_events"""",
    """SELECT category FROM elastic."bi_events"""", // W7 - mixed quoting, ONE joined name
    """SELECT a FROM "a""b".orders""", // W8 - doubled quote in the qualifier
    """SELECT a FROM "a"."b".t""", // multi-level - re-emits "a"."b".t faithfully
    """SELECT a FROM "sch".a.b""",
    """SELECT a FROM "sch".logs-2025.03""",
    "SELECT o.id FROM `elastic`.`orders` o JOIN `elastic`.`customers` c ON o.cid = c.id",
    """SELECT o.id FROM orders o LEFT OUTER JOIN "prod_us"."customers" c ON o.cid = c.id""",
    "SELECT o.id FROM orders o CROSS JOIN customers c", // AD-7 fix - ON-less CROSS JOIN
    "SELECT a FROM t JOIN UNNEST(t.items) i",
    """SELECT a FROM "prod_us".orders o, "prod_eu".orders p""",
    """DELETE FROM "elastic".orders WHERE id = 1""",
    "DELETE FROM `orders` WHERE id = 1",
    """CREATE TABLE dest AS SELECT a FROM "elastic".src""",
    "CREATE MATERIALIZED VIEW v AS SELECT a FROM `elastic`.`src`"
  )

  fixedPoints.foreach { sql =>
    it should s"be an AST fixed point: [$sql]" in {
      val parsed = Parser(sql)
      withClue(s"[$sql] ") { parsed.isRight shouldBe true }
      val stmt = parsed.toOption.get
      Parser(stmt.sql) shouldBe Right(stmt)
    }
  }

  // The render itself - the half a fixed point cannot see.
  it should "re-emit the qualifier it used to delete" in {
    Parser("""SELECT category FROM "elastic".bi_events""").toOption.get.sql shouldBe
    """SELECT category FROM "elastic".bi_events"""
  }

  it should "canonicalise backticks to ANSI double quotes without changing the AST" in {
    val bt = Parser("SELECT category FROM `elastic`.`bi_events`").toOption.get
    val dq = Parser("""SELECT category FROM "elastic"."bi_events"""").toOption.get
    bt shouldBe dq
    bt.sql shouldBe """SELECT category FROM "elastic"."bi_events""""
  }

  it should "quote a dotted index name as ONE lexeme, never per part" in {
    // A per-part render would emit `"logs-2025"."03"`, which re-parses to index `03`.
    Parser("""SELECT category FROM "logs-2025.03"""").toOption.get.sql shouldBe
    """SELECT category FROM "logs-2025.03""""
    Parser("SELECT category FROM `logs-2025.03`").toOption.get.sql shouldBe
    """SELECT category FROM "logs-2025.03""""
  }

  it should "leave an unquoted name unquoted" in {
    Parser("SELECT category FROM elastic.bi_events").toOption.get.sql shouldBe
    "SELECT category FROM elastic.bi_events"
    Parser("SELECT category FROM logs-2025.03").toOption.get.sql shouldBe
    "SELECT category FROM logs-2025.03"
  }

  it should "re-emit a multi-level qualifier FAITHFULLY, level by level" in {
    // Under the parts render nothing is joined: each level stays its own part and re-emits as
    // written (modulo backtick -> ANSI canonicalisation). A joined-and-requoted `"a.b".t` here
    // would mean someone re-introduced the superseded Table.catalog design - keep this loud.
    Parser("""SELECT a FROM "a"."b".t""").toOption.get.sql shouldBe """SELECT a FROM "a"."b".t"""
  }

  it should "re-emit MIXED quoting as one quoted joined name (W7)" in {
    Parser("""SELECT category FROM elastic."bi_events"""").toOption.get.sql shouldBe
    """SELECT category FROM "elastic.bi_events""""
  }

  it should "render a qualified JOIN source as ONE lexeme per part, like the FROM side" in {
    // `Join.sql` renders `$source` through `Identifier.sql`, which quotes PER dot-separated part.
    // `StandardJoin` overrides it (AD-5) precisely so a dotted index name on a JOIN leg is not
    // split - a `"logs-2025"."03"` here would re-parse to index `03`.
    Parser(
      "SELECT o.id FROM orders o JOIN `logs-2025.03` c ON o.cid = c.id"
    ).toOption.get.sql shouldBe
    """SELECT o.id FROM orders AS o   JOIN "logs-2025.03" AS c ON o.cid = c.id"""
    Parser(
      """SELECT o.id FROM orders o JOIN "prod_us".customers c ON o.cid = c.id"""
    ).toOption.get.sql shouldBe
    """SELECT o.id FROM orders AS o   JOIN "prod_us".customers AS c ON o.cid = c.id"""
  }

  it should "keep the MATERIALIZED VIEW render honest - it is PERSISTED" in {
    // MaterializedViewExtension stores this exact string as metadata.query and compares it on
    // CREATE OR REPLACE; the ALTER render is re-run through client.run.
    Parser(
      """CREATE MATERIALIZED VIEW v AS SELECT a FROM "elastic".src"""
    ).toOption.get.sql shouldBe
    """CREATE MATERIALIZED VIEW v AS SELECT a FROM "elastic".src"""
  }

  it should "keep the DELETE render honest - a lost qualifier deletes from another index" in {
    Parser("""DELETE FROM "elastic".orders WHERE id = 1""").toOption.get.sql shouldBe
    """DELETE FROM "elastic".orders WHERE id = 1"""
  }
}
