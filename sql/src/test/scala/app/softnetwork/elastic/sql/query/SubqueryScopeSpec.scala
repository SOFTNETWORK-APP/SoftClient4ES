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

/** Story 22.2 — the correlation detector itself, on parsed scopes.
  *
  * `WhereSubquerySpec` pins what the PARSER does with a correlated statement; this pins the walk
  * story 22.3 consumes as THE detector, so a change that keeps the rejections but loses the
  * shadowing rule or the nested walk fails here.
  */
class SubqueryScopeSpec extends AnyFlatSpec with Matchers {

  private def single(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"[$sql] $other")
  }

  /** The correlated statements cannot be obtained through `Parser` (they are rejected at
    * `validate()`), so the body and the outer scope are taken apart the way `update` sees them: the
    * body is parsed on its own — where its qualifiers resolve against NOTHING — and measured
    * against the outer statement's names.
    */
  private def refs(body: String, outer: String): Seq[String] =
    SubqueryScope.correlatedReferences(single(body), single(outer)).map(_.name)

  "correlationNames" should "carry every alias, index name and UNNEST alias of the FROM" in {
    val s = single("SELECT o.id FROM orders o")
    SubqueryScope.correlationNames(s) should contain allOf ("orders", "o")
  }

  "A qualified reference to an outer name" should "be reported" in {
    refs(
      "SELECT c.id FROM customers c WHERE c.region = o.region",
      "SELECT id FROM orders o"
    ) shouldBe
    Seq("o.region")
    // the outer INDEX name is a correlation name too
    refs(
      "SELECT id FROM customers WHERE region = orders.region",
      "SELECT id FROM orders"
    ) shouldBe Seq("orders.region")
  }

  it should "be silent when the inner statement declares the same name (innermost wins)" in {
    refs("SELECT o.id FROM customers o WHERE o.region = 'EU'", "SELECT id FROM orders o") shouldBe
    Nil
  }

  it should "be silent for a bare name (SQL resolves it innermost-first — PD-2)" in {
    refs("SELECT id FROM customers WHERE region = 'EU'", "SELECT id FROM orders o") shouldBe Nil
  }

  it should "be silent for a dotted path whose head is in NEITHER scope (an object field)" in {
    refs("SELECT id FROM customers WHERE address.city = 'X'", "SELECT id FROM orders o") shouldBe
    Nil
  }

  "A body nested one level deeper" should "still see the OUTERMOST scope" in {
    refs(
      "SELECT c.id FROM customers c WHERE c.k IN (SELECT k FROM z WHERE z.r = o.region)",
      "SELECT id FROM orders o"
    ) shouldBe Seq("o.region")
  }

  "The messages" should "name the offender, the scope and the story that will execute it" in {
    val node = single("SELECT id FROM t WHERE a IN (SELECT a FROM u)").whereSubqueries.head
    val id = single("SELECT id FROM customers WHERE region = orders.region").referencedIdentifiers
      .find(_.name.contains("."))
      .getOrElse(fail("no qualified identifier"))
    val qualified = SubqueryScope.correlatedMessage(id, node)
    qualified should include("Correlated subquery")
    qualified should include("orders.region")
    qualified should include("story 22.3")
    val bare = SubqueryScope.bareCorrelatedMessage("vip", "orders", "customers")
    bare should include("'vip' is not a column of 'orders'")
    bare should include("is a column of 'customers'")
    bare should include("story 22.3")
  }
}
