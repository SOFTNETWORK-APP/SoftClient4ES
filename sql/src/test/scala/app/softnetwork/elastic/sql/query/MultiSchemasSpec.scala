/*
 * Copyright 2026 SOFTNETWORK
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

import app.softnetwork.elastic.sql.GenericIdentifier
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => Schema}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `SingleSearch.schemas` — one schema per table, so a JOIN resolves every leg's columns.
  *
  * `SQLTypeUtils.coerce` is indexed by an identifier's `baseType`, which is `col.map(_.dataType)`.
  * With a single `schema` only one table could ever populate `col`, so a materialized view over a
  * JOIN converted nothing (issue #306's remaining half).
  *
  * 🔴 **The map is keyed the way `From.tableAliases` keys it — the INDEX NAME (via `aliasKey`,
  * story 21.2 AD-6) — never the SQL alias.** That is also how softclient4es-extensions builds its
  * `schemas` map. Both spellings are pinned below, because keying by alias silently resolves
  * nothing rather than failing, and silence is this family's whole failure mode.
  *
  * 🔴 **`update` is not the first pass.** The parser normalises `c.zip` to `name = "zip"`,
  * `table = Some("customers")` before these tests run, so the qualified branch of
  * `GenericIdentifier.update` is NOT the one that resolves a JOIN leg here — `this.table` is. A
  * lookup keyed on the alias-bearing `name` is therefore unreachable on the production path; that
  * is exactly the defect these tests were written against.
  */
class MultiSchemasSpec extends AnyFlatSpec with Matchers {

  private val orders: Schema = Schema(
    name = "orders",
    columns = List(Column("id", SQLTypes.Int), Column("cid", SQLTypes.Int))
  )

  private val customers: Schema = Schema(
    name = "customers",
    columns = List(Column("id", SQLTypes.Int), Column("zip", SQLTypes.Keyword))
  )

  private val byName: Map[String, Schema] = Map("orders" -> orders, "customers" -> customers)

  private val joinSql = "SELECT o.id, c.zip FROM orders o JOIN customers c ON o.cid = c.id"

  private def parsed(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"[$sql] rejected: $other")
    }

  /** The resolved column behind a SELECT field, by output position. */
  private def colOf(ss: SingleSearch, index: Int): Option[Column] =
    ss.select.fields(index).identifier match {
      case g: GenericIdentifier => g.col
      case other                => fail(s"expected a GenericIdentifier, got $other")
    }

  "a JOIN over several schemas" should "resolve the MAIN table's column" in {
    colOf(parsed(joinSql).updateAll(byName), 0).map(_.dataType) shouldBe Some(SQLTypes.Int)
  }

  it should "resolve the JOINED leg's column" in {
    // The half that was broken: every column arrives already normalised, so it fell to the
    // main-table branch and `orders.find("zip")` returned nothing — silently.
    colOf(parsed(joinSql).updateAll(byName), 1).map(_.dataType) shouldBe Some(SQLTypes.Keyword)
  }

  it should "resolve both legs when the tables carry no alias" in {
    val bare =
      "SELECT orders.id, customers.zip FROM orders JOIN customers ON orders.cid = customers.id"
    val ss = parsed(bare).updateAll(byName)
    colOf(ss, 0).map(_.dataType) shouldBe Some(SQLTypes.Int)
    colOf(ss, 1).map(_.dataType) shouldBe Some(SQLTypes.Keyword)
  }

  it should "NOT resolve when the map is keyed by SQL alias" in {
    // Pinned deliberately. The alias is not the key language; a map built that way resolves
    // nothing, and this test is what makes that visible instead of mysterious.
    val byAlias: Map[String, Schema] = Map("o" -> orders, "c" -> customers)
    val ss = parsed(joinSql).updateAll(byAlias)
    colOf(ss, 0) shouldBe None
    colOf(ss, 1) shouldBe None
  }

  "a leg missing from the map" should "resolve to NOTHING, never to the main table's schema" in {
    // 🔴 The important negative. Falling back to the main table would resolve `zip` against
    // `orders`; if `orders` had a same-named column of another type the cast would silently
    // convert the WRONG way. No answer beats a wrong answer.
    val ss = parsed(joinSql).updateAll(Map("orders" -> orders))
    colOf(ss, 0).map(_.dataType) shouldBe Some(SQLTypes.Int)
    colOf(ss, 1) shouldBe None
  }

  "an unqualified column" should "resolve against the main table" in {
    colOf(parsed("SELECT id FROM orders o").updateAll(Map("orders" -> orders)), 0)
      .map(_.dataType) shouldBe Some(SQLTypes.Int)
  }

  "the single-schema path (#306)" should "be untouched by the schemas map" in {
    // `SearchApi.resolveWithSchema` still calls `update(Some(schema))` with an EMPTY `schemas`,
    // so the legacy fallback has to keep working or every executing SELECT stops converting.
    val ss = parsed("SELECT id FROM orders").update(Some(orders))
    ss.schemas shouldBe empty
    colOf(ss, 0).map(_.dataType) shouldBe Some(SQLTypes.Int)
  }
}
