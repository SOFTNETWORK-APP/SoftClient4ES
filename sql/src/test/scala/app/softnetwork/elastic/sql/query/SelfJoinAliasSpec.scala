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
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

/** Story BIDC-8 — a self-join (`FROM idx a JOIN idx b`) keeps BOTH aliases
  * (softclient4es-arrow#144).
  *
  * MEASURED at the baseline (`origin/main` b37ef940), and pinned in the BEFORE form of this spec
  * before the re-key landed (T4):
  *
  * {{{
  * SELECT a.id, b.amount FROM idx a JOIN idx b ON a.id = b.id
  *   tableAliases   = ListMap(idx -> b)
  *   aliasesToTable = ListMap(b -> idx)              -- alias `a` gone from BOTH maps
  *   a.id   -> name=a.id   table=None      tableAlias=None    -- never resolved
  *   b.amount -> name=amount table=Some(idx) tableAlias=Some(b)
  *   joinKeyMatches = Some(List())                   -- the ON clause lost its join key
  * SELECT a.x, b.y FROM t a, t b                     -- tripwire 2 (comma-FROM, core path)
  *   a.x -> name=a.x table=None ; b.y -> name=y table=Some(t)   -- a silent wrong answer
  * }}}
  *
  * `tableAliases` is keyed by TABLE and therefore holds ONE alias per table by construction — that
  * map's contract is UNCHANGED here (softclient4es-extensions reads it forward). The lossless
  * direction is alias -> table: `From.aliasesToTable` is now built directly, and the two resolvers
  * (`Identifier.update`, `FieldSort.update`) read it.
  */
class SelfJoinAliasSpec extends AnyFlatSpec with Matchers {

  private def single(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss
      case Right(other)            => fail(s"Expected SingleSearch for [$sql], got $other")
      case Left(err)               => fail(s"Expected [$sql] to parse, got: ${err.msg}")
    }

  /** A GRAMMAR / validation rejection — not story 21.4's boundary catch, which would make a bare
    * `isLeft` unfalsifiable.
    */
  private def rejectedWith(sql: String, fragment: String): Unit = {
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    val msg = Parser(sql).swap.toOption.map(_.msg).getOrElse(fail(s"[$sql] was accepted"))
    withClue(s"[$sql] msg=[$msg] ") {
      msg should not startWith Parser.InternalParseFailure
      msg should include(fragment)
    }
    ()
  }

  private val selfJoin = "SELECT a.id, b.amount FROM idx a JOIN idx b ON a.id = b.id"

  behavior of "From.aliasesToTable on a self-join"

  it should "keep BOTH aliases while tableAliases keeps its one-alias-per-table contract" in {
    val from = single(selfJoin).from
    from.aliasesToTable shouldBe ListMap("a" -> "idx", "b" -> "idx")
    // UNCHANGED, on purpose: keyed by table, one alias per key, the forward-lookup contract every
    // consumer outside `sql` speaks. It is `aliasesToTable` that is lossless, not this.
    from.tableAliases shouldBe ListMap("idx" -> "b")
    from.mainTableKey shouldBe "idx"
    from.joinSourceKeys shouldBe Set("idx")
  }

  it should "resolve BOTH legs' columns, each tagged with its own alias and the shared index" in {
    val ss = single(selfJoin)
    val a = ss.select.fields.head.identifier
    a.name shouldBe "id"
    a.table shouldBe Some("idx")
    a.tableAlias shouldBe Some("a")
    val b = ss.select.fields(1).identifier
    b.name shouldBe "amount"
    b.table shouldBe Some("idx")
    b.tableAlias shouldBe Some("b")
  }

  it should "carry the join key on BOTH sides of the ON clause" in {
    val sj = single(selfJoin).from.mainTable.joins.collect { case s: StandardJoin => s }.head
    sj.on.map(_.joinKeyMatches) shouldBe Some(
      Seq(JoinKeyMatch(JoinKey("idx", "a", "id"), JoinKey("idx", "b", "id")))
    )
  }

  it should "render to SQL that re-parses to the same statement" in {
    val ss = single(selfJoin)
    Parser(ss.sql) shouldBe Right(ss)
  }

  it should "equal the old .swap wherever no two aliases share a key (no regression)" in {
    Seq(
      "SELECT o.id FROM orders o JOIN customers c ON o.customer_id = c.id",
      "SELECT * FROM orders LEFT JOIN customers ON orders.customer_id = customers.id",
      "SELECT o.id, i.qty FROM orders o JOIN UNNEST(o.items) AS i",
      """SELECT o.id FROM "prod_us".orders o JOIN "prod_eu".orders p ON o.cid = p.id""",
      "SELECT a FROM t"
    ).foreach { sql =>
      val from = single(sql).from
      withClue(sql)(from.aliasesToTable shouldBe from.tableAliases.map(_.swap))
    }
  }

  behavior of "#159's bare-alias canary on a self-join"

  it should "reject ORDER BY on EITHER bare table alias" in {
    // Before BIDC-8 `tableAliases` was `ListMap(idx -> b)`, so `ORDER BY a` was not recognised as
    // a bare table alias and went through as a sort on a column named `a`.
    rejectedWith(s"$selfJoin ORDER BY a", "Column name expected after table alias 'a'")
    rejectedWith(s"$selfJoin ORDER BY b", "Column name expected after table alias 'b'")
  }

  behavior of "the comma-FROM duplicate shape (tripwire 2)"

  it should "be REJECTED loudly under different aliases, naming the JOIN spelling" in {
    // Lead ruling: keep the (silent, wrong) baseline behaviour or reject loudly — never change it
    // silently. With the alias map lossless both `a.x` and `b.y` would have resolved against ONE
    // doubled index; a comma-separated FROM has no join engine behind it.
    rejectedWith(
      "SELECT a.x, b.y FROM t a, t b",
      "Table t is listed more than once in FROM under different aliases"
    )
    rejectedWith("SELECT x FROM t, t a", "FROM t t JOIN t a ON t.<key> = a.<key>")
    rejectedWith(
      """SELECT a FROM "elastic".orders o, "elastic".orders p""",
      """Table "elastic".orders is listed more than once"""
    )
  }

  it should "be REJECTED inside every statement kind that embeds a query (INSERT, CTAS, MV)" in {
    // `From.validate()` is reached only through the embedding statement's own `validate()`; a
    // kind that did not delegate would keep the silently-changed resolution the lead ruled out.
    val comma = "SELECT a.x, b.y FROM t a, t b"
    rejectedWith(s"INSERT INTO target $comma", "listed more than once in FROM")
    rejectedWith(s"CREATE TABLE target AS $comma", "listed more than once in FROM")
    rejectedWith(
      s"CREATE MATERIALIZED VIEW target REFRESH EVERY 30 SECONDS AS $comma",
      "listed more than once in FROM"
    )
  }

  it should "leave the same-alias duplicate and the cross-qualifier shape untouched" in {
    // `FROM t, t`: key and value identical, nothing was ever lost, nothing changes.
    single("SELECT x FROM t, t").from.tableAliases shouldBe ListMap("t" -> "t")
    // Story 21.2's shape: two DIFFERENT qualified references, i.e. two tables, not a duplicate.
    single("""SELECT o.a FROM "prod_us".orders o, "prod_eu".orders p""").from.tableAliases shouldBe
    ListMap("prod_us.orders" -> "o", "prod_eu.orders" -> "p")
  }
}
