package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.NamePart
import app.softnetwork.elastic.sql.query.{Insert, SingleSearch, StandardJoin, Update}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

/** Story 21.2 / issues #252 (part 2) and #85 — the FROM/JOIN table-name surface.
  *
  * The invariant this file exists to protect is NOT "these statements parse". It is **which index
  * they read**. A verdict-only assertion cannot see the failure mode that matters: joining the
  * dot-separated parts of `"elastic"."bi_events"` into one name makes the statement parse AND read
  * an index that does not exist — measured on 48 of the 99 captured BI statements. Every accepting
  * row therefore asserts `name`, `parts` and `sources`.
  */
class QuotedTableNameSpec extends AnyFlatSpec with Matchers {

  private def single(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss
      case Right(other)            => fail(s"Expected SingleSearch for [$sql], got $other")
      case Left(err)               => fail(s"Expected [$sql] to parse, got: ${err.msg}")
    }

  /** A GRAMMAR rejection, as opposed to the `NonFatal` boundary catch story 21.4 installed in
    * `Parser.apply`. The third assertion is what makes the test falsifiable at all: with the
    * boundary catch in place `noException` holds unconditionally and `isLeft` holds for an internal
    * crash too, so without it a restored `throw` would keep every row green.
    */
  private def rejected(sql: String): Unit = {
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    withClue(s"[$sql] ") { Parser(sql).isLeft shouldBe true }
    val msg = Parser(sql).swap.toOption.map(_.msg).getOrElse("")
    withClue(s"[$sql] msg=[$msg] - must be a grammar rejection, not the boundary catch. ") {
      msg should not startWith Parser.InternalParseFailure
    }
    ()
  }

  private def q(v: String) = NamePart(v, quoted = true)
  private def b(v: String) = NamePart(v, quoted = false)

  private def joinParts(ss: SingleSearch): Seq[Seq[NamePart]] =
    ss.from.joins.collect { case sj: StandardJoin => sj.parts }

  // -- AC-1 - the #252 acceptance table, in the issue's own row order -------------------------
  //
  // (name, parts) for every accepting row. Row 8 is story 21.1's and is listed so the matrix is
  // the whole table; if it is red, 21.1 has not landed.
  private val acceptanceTable: Seq[(Int, String, String, Seq[NamePart])] = Seq(
    (1, "SELECT category FROM bi_events LIMIT 1", "bi_events", Seq(b("bi_events"))),
    (2, "SELECT category FROM `bi_events` LIMIT 1", "bi_events", Seq(q("bi_events"))),
    (3, """SELECT category FROM "bi_events" LIMIT 1""", "bi_events", Seq(q("bi_events"))),
    (
      4,
      "SELECT category FROM elastic.bi_events LIMIT 1",
      "elastic.bi_events",
      Seq(b("elastic.bi_events"))
    ),
    (
      5,
      "SELECT category FROM `elastic`.`bi_events` LIMIT 1",
      "bi_events",
      Seq(q("elastic"), q("bi_events"))
    ),
    (
      6,
      """SELECT category FROM "elastic".bi_events LIMIT 1""",
      "bi_events",
      Seq(q("elastic"), b("bi_events"))
    ),
    (
      7,
      """SELECT category FROM "elastic"."bi_events" LIMIT 1""",
      "bi_events",
      Seq(q("elastic"), q("bi_events"))
    ),
    (8, "SELECT `category` FROM bi_events LIMIT 1", "bi_events", Seq(b("bi_events"))),
    (9, """SELECT "category" FROM bi_events LIMIT 1""", "bi_events", Seq(b("bi_events"))),
    (
      10,
      "SELECT e.category FROM elastic.bi_events e LIMIT 1",
      "elastic.bi_events",
      Seq(b("elastic.bi_events"))
    )
  )

  acceptanceTable.foreach { case (row, sql, expectedName, expectedParts) =>
    it should s"resolve #252 acceptance-table row $row to index [$expectedName]" in {
      val ss = single(sql)
      ss.from.mainTable.name shouldBe expectedName
      ss.from.mainTable.parts shouldBe expectedParts
      ss.sources shouldBe Seq(expectedName) // AC-2 - the index actually read
    }
  }

  // -- AC-2 - the shapes whose resolved index must be BYTE-IDENTICAL to the pre-story behaviour
  //
  // Stated exactly, because AC-2's own wording ("the index a statement reads NEVER changes for any
  // statement that parses today") is measurably too strong against the CURRENT main: two shapes do
  // move, and both move from a wrong index to the right one, because story 21.1 made them parse
  // where this story's older baseline had them rejected --
  //   `FROM elastic."bi_events"` : main reads index `elastic.` -> branch reads `elastic.bi_events`
  //   `JOIN "prod_us".customers` : main reads `prod_us.customers` -> branch reads `customers`
  // Both are pinned below, in their own tests. What this one protects is AC-2's SUBSTANCE: no
  // statement resolves to a JOINED qualifier+name, the outcome AD-1 option (c) was rejected for.
  it should "not move the index of any statement whose reading was already correct" in {
    single("SELECT category FROM bi_events").sources shouldBe Seq("bi_events")
    single("SELECT category FROM elastic.bi_events").sources shouldBe Seq("elastic.bi_events")
    single("""SELECT category FROM "elastic".bi_events""").sources shouldBe Seq("bi_events")
    single("SELECT a FROM t1, t2").sources shouldBe Seq("t1", "t2")
    single("""SELECT a FROM "sch".a.b""").sources shouldBe Seq("a.b")
    single("""SELECT a FROM "sch".logs-2025.03""").sources shouldBe Seq("logs-2025.03")
  }

  // The FROM qualifier separator is whitespace-tolerant, and must STAY so (story 21.2 AD-8).
  // All three spacings parse today and all three read `bi_events`. Making the separator
  // adjacency-strict — symmetric with `nameTailPart`, which owns its dot since #285 — would send
  // `"elastic" .bi_events` down `qualifiedName`, whose tail DOES match `.bi_events`, and the
  // statement would silently start reading index `elastic.bi_events`.
  it should "keep reading a whitespace-separated qualifier dot exactly as before (AD-8)" in {
    Seq(
      """SELECT a FROM "elastic" . bi_events""",
      """SELECT a FROM "elastic" .bi_events""",
      """SELECT a FROM "elastic". bi_events"""
    ).foreach { s =>
      withClue(s"[$s] ") {
        single(s).from.mainTable.name shouldBe "bi_events"
        single(s).sources shouldBe Seq("bi_events")
      }
    }
  }

  // -- AC-4 - dotted UNQUOTED index names stay ONE name ---------------------------------------
  it should "keep an unquoted dotted index name whole" in {
    single("SELECT category FROM logs-2025.03").from.mainTable.name shouldBe "logs-2025.03"
    single("SELECT category FROM logs-2025.03 l").from.mainTable.name shouldBe "logs-2025.03"
    single("SELECT category FROM a.b.bi_events").from.mainTable.name shouldBe "a.b.bi_events"
    single("SELECT category FROM a.b.bi_events").from.mainTable.parts shouldBe
    Seq(b("a.b.bi_events"))
  }

  it should "read a QUOTED dotted index name as one name, not as a qualifier" in {
    Seq(
      """SELECT category FROM "logs-2025.03"""",
      "SELECT category FROM `logs-2025.03`"
    ).foreach { s =>
      val t = single(s).from.mainTable
      withClue(s"[$s] ") {
        t.name shouldBe "logs-2025.03"
        t.parts shouldBe Seq(q("logs-2025.03")) // ONE part - a quoted lexeme is never split
      }
    }
  }

  // The documented cost of the leading-run rule (AD-1): quoting each PART of a dotted index name
  // makes the first part a qualifier. Rejected before this story, so nothing regresses.
  it should "read a per-part-quoted dotted name as qualifier + index (known limitation, N1)" in {
    val t = single("SELECT category FROM `logs-2025`.`03`").from.mainTable
    t.name shouldBe "03"
    t.parts shouldBe Seq(q("logs-2025"), q("03"))
  }

  // -- AC-3 - Tableau's own shape, both dialects ----------------------------------------------
  it should "parse Tableau's qualified-table-plus-alias shape in both dialects" in {
    Seq(
      "SELECT category FROM `elastic`.`bi_events` `bi_events`",
      """SELECT category FROM "elastic"."bi_events" "bi_events""""
    ).foreach { s =>
      val t = single(s).from.mainTable
      withClue(s"[$s] ") {
        t.name shouldBe "bi_events"
        t.parts shouldBe Seq(q("elastic"), q("bi_events"))
        t.tableAlias.map(_.alias) shouldBe Some("bi_events")
      }
    }
  }

  it should "parse a quoted multi-table FROM" in {
    single("""SELECT a FROM "t1", "t2"""").sources shouldBe Seq("t1", "t2")
  }

  it should "accept a multi-level quoted qualifier (JDBC catalog + schema, story 20.3)" in {
    // Each level stays its OWN part - nothing is joined, no role is assigned; the resolver splits.
    val t = single("""SELECT a FROM "elasticsearch"."prod-cluster"."bi_events"""").from.mainTable
    t.name shouldBe "bi_events"
    t.parts shouldBe Seq(q("elasticsearch"), q("prod-cluster"), q("bi_events"))
  }

  // -- AC-7 - the FROM/JOIN asymmetry is closed -----------------------------------------------
  it should "resolve a qualified JOIN source to the bare index, like the FROM side" in {
    val ss = single(
      "SELECT o.id FROM `elastic`.`bi_events` o JOIN `elastic`.`bi_category_dim` d ON o.cid = d.id"
    )
    ss.from.mainTable.name shouldBe "bi_events"
    ss.from.mainTable.parts shouldBe Seq(q("elastic"), q("bi_events"))
    ss.from.mainTable.joinedTables shouldBe Seq("bi_category_dim") // NOT elastic.bi_category_dim
    ss.from.tableAliases shouldBe ListMap("bi_events" -> "o", "bi_category_dim" -> "d")
    joinParts(ss) shouldBe Seq(Seq(q("elastic"), q("bi_category_dim")))
  }

  it should "resolve a double-quoted JOIN qualifier to the bare index too" in {
    // MEASURED on origin/main BEFORE this story: this statement PARSED and read index
    // `prod_us.customers` on the JOIN leg while the FROM leg read `orders` — one statement, two
    // rules, no error. Story 21.1 made `FromParser.source` quoting-aware through `identifier`,
    // which JOINS the parts; this story gives the JOIN side the table rule.
    val ss = single("""SELECT o.id FROM orders o JOIN "prod_us".customers c ON o.cid = c.id""")
    ss.from.mainTable.joinedTables shouldBe Seq("customers")
    joinParts(ss) shouldBe Seq(Seq(q("prod_us"), b("customers")))
  }

  it should "keep an unquoted dotted JOIN source whole" in {
    single(
      "SELECT o.id FROM orders o JOIN prod_us.customers c ON o.cid = c.id"
    ).from.mainTable.joinedTables shouldBe
    Seq("prod_us.customers")
    single(
      "SELECT o.id FROM orders o JOIN logs-2025.03 c ON o.cid = c.id"
    ).from.mainTable.joinedTables shouldBe
    Seq("logs-2025.03")
  }

  it should "accept a quoted JOIN source in every join type" in {
    Seq(
      "INNER JOIN",
      "LEFT JOIN",
      "LEFT OUTER JOIN",
      "RIGHT JOIN",
      "RIGHT OUTER JOIN",
      "FULL JOIN",
      "FULL OUTER JOIN",
      "CROSS JOIN",
      "JOIN"
    ).foreach { jt =>
      withClue(s"[$jt] ") {
        single(
          s"SELECT o.id FROM orders o $jt `customers` c ON o.cid = c.id"
        ).from.mainTable.joinedTables shouldBe
        Seq("customers")
      }
    }
  }

  it should "close the FROM/JOIN asymmetry from the FROM side too" in {
    // The FROM half used to block the whole statement: `FROM `orders` o JOIN customers c` was a
    // parse error even after 21.1 flipped the JOIN half.
    val ss = single("SELECT o.id FROM `orders` o JOIN customers c ON o.cid = c.id")
    ss.sources shouldBe Seq("orders")
    ss.from.mainTable.parts shouldBe Seq(q("orders"))
  }

  // -- AC-13 - CROSS JOIN without ON parses and validates (AD-7 fix) ---------------------------
  it should "accept CROSS JOIN without an ON clause (AD-7 fix)" in {
    // REJECTED before this story: `StandardJoin.validate` demanded ON unconditionally and returned
    // before `Join.validate`'s CrossJoin exemption could run, so the exemption was dead code.
    single("SELECT o.id FROM orders o CROSS JOIN customers c").from.mainTable.joinedTables shouldBe
    Seq("customers")
    single(
      "SELECT o.id FROM orders o CROSS JOIN `customers` c"
    ).from.mainTable.joinedTables shouldBe
    Seq("customers")
  }

  it should "keep parsing CROSS JOIN WITH an ON clause" in {
    // The fix widens; it must not narrow.
    single(
      "SELECT o.id FROM orders o CROSS JOIN customers c ON o.cid = c.id"
    ).from.mainTable.joinedTables shouldBe
    Seq("customers")
  }

  it should "still reject a bare JOIN without an ON clause" in {
    // AD-7 exempts CROSS JOIN only. `StandardJoin.validate`'s arm is the ONLY rejection for the
    // bare join (`Join.validate`'s guard skips joinType = None), so this pin guards the fix's scope.
    rejected("SELECT o.id FROM orders o JOIN customers c")
    rejected("SELECT o.id FROM orders o LEFT JOIN customers c")
  }

  // -- W7 / W8 - the shapes the AD-1 rule-4 table exists to pin --------------------------------
  it should "read MIXED quoting as ONE joined name, not as a qualifier (W7, known limitation)" in {
    // The qualifier is the maximal LEADING run of quoted parts each followed by a dot. Here the
    // first part is bare, so the run is empty and the whole operand is one index name (21.1's
    // `qualifiedName` joins the mixed tail into one quoted lexeme).
    //
    // MEASURED on origin/main BEFORE this story this statement did NOT reject: `identifierRegex`
    // ate the trailing dot and `quotedAlias` claimed `"bi_events"` as the ALIAS, so it read index
    // `elastic.` — an index that cannot exist. This row therefore FIXES a silent corruption; it
    // does not merely widen.
    val t = single("""SELECT category FROM elastic."bi_events"""").from.mainTable
    t.name shouldBe "elastic.bi_events"
    t.parts shouldBe Seq(q("elastic.bi_events"))
    t.tableAlias shouldBe None
  }

  it should "accept a doubled quote inside the qualifier (W8)" in {
    single("""SELECT a FROM "a""b".orders""").from.mainTable.parts.head shouldBe q("a\"b")
  }

  it should "still reject a leading digit on the FIRST name part" in {
    // `bareFirstPart` forbids it; `bareNextPart` (which is what makes `logs-2025.03` work) does not.
    rejected("""SELECT a FROM "elastic".1x""")
  }

  // -- AC-12 - the same-name cross-qualifier collapse is FIXED (AD-6) --------------------------
  it should "keep both aliases when two tables differ only by qualifier (AD-6 fix)" in {
    // MEASURED before the fix: `tableAliases` was keyed by `table.name`, both stripped to `orders`,
    // and the ListMap kept the LAST — alias `o` was silently gone, so `o.a` resolved against
    // nothing and queried a field literally named `o.a`. `sources` (the indices actually read) is
    // deliberately unchanged.
    val ss = single("""SELECT a FROM "prod_us".orders o, "prod_eu".orders p""")
    ss.sources shouldBe Seq("orders", "orders")
    ss.from.tableAliases shouldBe ListMap("prod_us.orders" -> "o", "prod_eu.orders" -> "p")
    single("SELECT a FROM `prod_us`.orders o, `prod_eu`.orders p").from.tableAliases shouldBe
    ListMap("prod_us.orders" -> "o", "prod_eu.orders" -> "p")
  }

  it should "disambiguate a same-name JOIN leg by its qualifier too (AD-6 fix)" in {
    val ss = single(
      """SELECT o.id FROM "prod_us".orders o JOIN "prod_eu".orders p ON o.cid = p.id"""
    )
    ss.from.tableAliases shouldBe ListMap("prod_us.orders" -> "o", "prod_eu.orders" -> "p")
    ss.from.mainTable.joinedTables shouldBe Seq("orders")
  }

  it should "leave the alias-map keys BARE whenever no bare name is ambiguous (AD-6 blast radius)" in {
    // 🔴 The half of AD-6 that is easy to lose. `tableAliases` is read by KEY and by REVERSE
    // lookup outside this file — `Identifier.update` returns the key into `Identifier.table`, and
    // softclient4es-extensions' JoinDependencyGraph/FieldAnalyzer look tables up in a `schemas`
    // map keyed by the BARE index name. Keying every qualified table by its qualified name would
    // have made a MATERIALIZED VIEW over `FROM "elastic".orders o JOIN …` — which parses today —
    // silently plan with no fields. The qualified key is used ONLY where the bare one is ambiguous.
    single("""SELECT a FROM "elastic".orders o""").from.tableAliases shouldBe
    ListMap("orders" -> "o")
    single(
      """SELECT o.id FROM "elastic".orders o JOIN "elastic".customers c ON o.cid = c.id"""
    ).from.tableAliases shouldBe
    ListMap("orders" -> "o", "customers" -> "c")
    single("""SELECT a FROM "elastic".orders""").from.aliasesToTable shouldBe
    ListMap("orders" -> "orders")
  }

  it should "keep BOTH aliases of two IDENTICALLY qualified same-name tables in aliasesToTable (BIDC-8)" in {
    // Story 21.2 (AD-6) pinned this shape as "collapsing, as before": two references to the SAME
    // qualified table are not ambiguous, so `tableAliases` — keyed by TABLE — kept `orders -> p`
    // and alias `o` was lost from BOTH maps (`aliasesToTable` was its `.swap`). Story BIDC-8
    // (softclient4es-arrow#144) changes that pin ON PURPOSE: `tableAliases` still holds one alias
    // per key (its contract is unchanged — extensions read it forward), while `aliasesToTable` is
    // now built directly and keeps every alias. The COMMA spelling is rejected by `From.validate()`
    // (a multi-index search cannot join a table to itself — BIDC-8 tripwire 2), so the JOIN
    // spelling carries the assertion.
    val qualified = single(
      """SELECT o.id FROM "elastic".orders o JOIN "elastic".orders p ON o.id = p.parent_id"""
    )
    qualified.from.tableAliases shouldBe ListMap("orders" -> "p")
    qualified.from.aliasesToTable shouldBe ListMap("o" -> "orders", "p" -> "orders")
    val bare = single("SELECT o.id FROM orders o JOIN orders p ON o.id = p.parent_id")
    bare.from.tableAliases shouldBe ListMap("orders" -> "p")
    bare.from.aliasesToTable shouldBe ListMap("o" -> "orders", "p" -> "orders")
    rejected("""SELECT a FROM "elastic".orders o, "elastic".orders p""")
    rejected("SELECT a FROM orders o, orders p")
  }

  it should "disambiguate only the ambiguous half of a MIXED qualified/bare FROM (AD-6)" in {
    // One leg qualified, one bare, same index name: the bare name IS ambiguous (two distinct
    // qualified references, `orders` and `prod_eu.orders`), so both keys survive and the bare leg
    // keeps its own bare key.
    single(
      """SELECT o.id FROM orders o JOIN "prod_eu".orders p ON o.cid = p.id"""
    ).from.tableAliases shouldBe
    ListMap("orders" -> "o", "prod_eu.orders" -> "p")
  }

  it should "key an ambiguous name qualified and every other name bare, in one statement (AD-6)" in {
    // `orders` is ambiguous and gets qualified keys; `customers` is not, and stays bare. The two
    // rules coexist inside a single FROM.
    single(
      """SELECT o.id FROM "prod_us".orders o, "prod_eu".orders p, customers c"""
    ).from.tableAliases shouldBe
    ListMap("prod_us.orders" -> "o", "prod_eu.orders" -> "p", "customers" -> "c")
  }

  it should "keep From.joinSourceKeys in step with Identifier.table (AD-6 companion)" in {
    // 🔴 `Identifier.table` IS a `tableAliases` key, so in the ambiguous branch it is the QUALIFIED
    // reference. Anything in `sql` that compares it against an index name must use the same key
    // language: `TemporalLiterals`' cross-index join-leg guard compared it against the bare
    // `joinAliases` source names and stopped firing for exactly this statement until it was routed
    // through `From.joinSourceKeys`.
    val ss = single("""SELECT o.a, p.b FROM orders o JOIN "prod_eu".orders p ON o.cid = p.id""")
    ss.from.joinSourceKeys shouldBe Set("prod_eu.orders")
    val pTable =
      ss.select.fields.map(_.identifier).find(_.tableAlias.contains("p")).flatMap(_.table)
    pTable shouldBe Some("prod_eu.orders")
    pTable.exists(ss.from.joinSourceKeys.contains) shouldBe true
    // ... and the unambiguous case keeps BOTH sides bare, exactly as before this story.
    val plain = single("SELECT o.a, c.b FROM orders o JOIN customers c ON o.cid = c.id")
    plain.from.joinSourceKeys shouldBe Set("customers")
    plain.select.fields
      .map(_.identifier)
      .find(_.tableAlias.contains("c"))
      .flatMap(_.table) shouldBe Some("customers")
  }

  it should "reject an expression where a JOIN source is required (N2)" in {
    // MEASURED on origin/main BEFORE this story: both of these PARSED, because `FromParser.source`
    // took the full `identifier` production — so a JOIN source could carry a `::` cast or a
    // DISTINCT. Neither reached anything: `Table.joinedTables` and `joinAliases` read
    // `source.name`, so the cast was silently discarded — while the RENDER kept it
    // (`JOIN customers::BIGINT AS c`), advertising a conversion the engine never applied. A JOIN
    // source is a table name, so `source` now takes `tableParts` and these are loud rejections.
    rejected("SELECT o.id FROM orders o JOIN customers::BIGINT c ON o.cid = c.id")
    rejected("SELECT o.id FROM orders o JOIN DISTINCT customers c ON o.cid = c.id")
    // The FROM side always rejected them — the two sides now agree.
    rejected("SELECT a FROM orders::BIGINT")
    rejected("SELECT a FROM DISTINCT orders")
  }

  it should "leave UNNEST alone - its operand is a column path, not a table" in {
    val ss = single("SELECT a FROM t JOIN UNNEST(t.items) i")
    ss.from.unnests.map(_.name) shouldBe Seq("items")
    ss.from.mainTable.joinedTables shouldBe empty
  }

  // -- AC-8 - every statement kind that routes through FromParser.table ------------------------
  it should "resolve a quoted qualifier in DELETE (which uses FromParser.table, not ident)" in {
    Parser("""DELETE FROM "elastic".orders WHERE id = 1""").toOption.get.sql shouldBe
    """DELETE FROM "elastic".orders WHERE id = 1"""
    Parser("DELETE FROM `orders` WHERE id = 1").toOption.get.sql shouldBe
    """DELETE FROM "orders" WHERE id = 1"""
  }

  it should "resolve a quoted qualifier inside CTAS and MATERIALIZED VIEW bodies" in {
    // The RENDERED TEXT is asserted, not just `isRight`. An AST fixed point cannot see a lossy
    // render (this story measured that on main), and the MV render is the one that is PERSISTED:
    // MaterializedViewExtension stores `create.search.sql` as metadata.query and compares it on
    // CREATE OR REPLACE. Before this story both of these rendered `... FROM src`.
    Parser("""CREATE TABLE dest AS SELECT a FROM "elastic".src""").toOption.get.sql shouldBe
    """CREATE TABLE dest AS SELECT a FROM "elastic".src"""
    Parser("CREATE MATERIALIZED VIEW v AS SELECT a FROM `elastic`.`src`").toOption.get.sql shouldBe
    """CREATE MATERIALIZED VIEW v AS SELECT a FROM "elastic"."src""""
  }

  // -- AC-9 - the rejections that stay rejections ---------------------------------------------
  it should "still reject a bare quote where a table name is required" in {
    rejected("""SELECT category FROM "" """) // empty lexeme is not an identifier (21.1 AD-5)
    rejected("SELECT category FROM ``")
    rejected("""SELECT category FROM "elastic".""")
  }

  it should "let #191's multi-index watcher guard fire on a cross-qualifier FROM (AD-6)" in {
    // UNDECLARED BEHAVIOUR CHANGE, now pinned. `Parser.qualifiedOverManyIndices` rejects a
    // table-qualified predicate over a multi-index watcher FROM, and it tests `_.table.isDefined`
    // — so it was defeated whenever the alias map dropped an alias. AD-6 keeps both aliases for two
    // tables differing only by qualifier, so `o` now resolves and the guard fires. This statement
    // is ACCEPTED before this story and REJECTED after: #191's guard finally catching a statement
    // it always meant to catch.
    rejected(
      """CREATE OR REPLACE WATCHER my_watcher AS EVERY 5 MINUTES FROM "a".orders o, """ +
      """"b".orders p WHERE o.x = 1 WITHIN 2 MINUTES ALWAYS DO LOG_ACTION LOG 'x' END"""
    )
    // 21.2 pinned the WHOLLY unqualified self-join as still defeating the guard, "exactly as
    // before" (the alias map had nothing to disambiguate, so `o` did not resolve). Story BIDC-8
    // flips that pin ON PURPOSE: qualifiers resolve through the lossless `From.aliasesToTable`, so
    // `o` resolves here too and the guard fires — a qualifier over a doubled single index is still
    // a qualifier a multi-index search cannot scope.
    val watcherSelfJoin =
      "CREATE OR REPLACE WATCHER my_watcher AS EVERY 5 MINUTES FROM orders o, orders p " +
      "WHERE o.x = 1 WITHIN 2 MINUTES ALWAYS DO LOG_ACTION LOG 'x' END"
    rejected(watcherSelfJoin)
    // It must be #191's guard that fires, not just any grammar rejection (review M-1).
    Parser(watcherSelfJoin).swap.toOption.map(_.msg).getOrElse("") should include(
      "cannot qualify a column by table when it searches several indices"
    )
  }

  /** RETARGETED by story 21.7, not deleted: these rows pinned a PENDING CAPABILITY ("the DML and
    * DDL name surface story 21.7 owns"), not a defect, so the contract they record is now the
    * ACCEPTANCE. `QuotedDmlDdlNameSpec` owns the full matrix; these three stay here because they
    * are the exact statements 21.2 handed over, and their red is what tells a reader that 21.7's
    * conversion has been undone.
    */
  it should "have handed the DML and DDL name surface to story 21.7, which accepts it" in {
    Parser("INSERT INTO `prod_eu`.dest SELECT a FROM src") match {
      case Right(i: Insert) =>
        i.table shouldBe "dest"; i.parts shouldBe Seq(q("prod_eu"), b("dest"))
      case other => fail(s"expected an Insert, got $other")
    }
    Parser("""INSERT INTO "prod_eu".dest SELECT a FROM src""") match {
      case Right(i: Insert) =>
        i.table shouldBe "dest"; i.parts shouldBe Seq(q("prod_eu"), b("dest"))
      case other => fail(s"expected an Insert, got $other")
    }
    Parser("UPDATE `orders` SET a = 1 WHERE id = 1") match {
      case Right(u: Update) => u.table shouldBe "orders"; u.parts shouldBe Seq(q("orders"))
      case other            => fail(s"expected an Update, got $other")
    }
    // Rejected on its own merits — there is no LOCAL TEMPORARY production, quoted or not.
    rejected("CREATE LOCAL TEMPORARY TABLE dest (COL INTEGER)")
  }
}
