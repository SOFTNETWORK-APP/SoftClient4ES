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

package app.softnetwork.elastic.sql.function.string

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `CONCAT` skips a NULL argument, as DuckDB 1.5.5 does: `concat(NULL, 'x')` is `'x'`, `concat(k,
  * 'x')` over a NULL `k` is `'x'`, and `concat(NULL, NULL)` is `''` (measured), in a query, an
  * ingest processor and a transform alike.
  *
  * Before, an argument whose NULL nothing flagged -- the `NULL` literal, a `COALESCE`, a `CASE` or
  * a `GREATEST` answering NULL -- reached `String.valueOf(null)` and the row read `"nullx"`, and a
  * NULL column made the whole call NULL.
  */
class ConcatNullArgumentSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("k", SQLTypes.Keyword),
      Column("g1", SQLTypes.Keyword),
      Column("n", SQLTypes.Int),
      Column("d", SQLTypes.Date)
    )
  )

  private def scriptOf(
    sql: String,
    contextType: PainlessContextType = PainlessContextType.Query
  ): String = Parser(sql) match {
    case Right(ss: SingleSearch) =>
      val ctx = PainlessContext(contextType)
      val body = ss.update(Some(schema)).select.fields.head.painless(Some(ctx))
      s"$ctx$body"
    case other => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  "a NULL literal argument of CONCAT" should "be skipped, not printed as the text null" in {
    scriptOf("SELECT CONCAT(NULL, 'x') FROM t") shouldBe "(\"\" + \"x\")"
    scriptOf("SELECT CONCAT(GREATEST(NULL, NULL), 'x') FROM t") shouldBe "(\"\" + \"x\")"
  }

  "an argument that can be NULL" should "answer the empty text when it is, and the call no NULL" in {
    scriptOf("SELECT CONCAT(k, 'x') FROM t") shouldBe
    "def param1 = (doc['k'].size() == 0 ? null : doc['k'].value); " +
    "((param1 == null ? \"\" : String.valueOf(param1)) + \"x\")"
    scriptOf("SELECT CONCAT(COALESCE(NULL, CAST(d AS DATE)), 'x') FROM t") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param2 = (param1 != null ? param1.toInstant().atZone(ZoneId.of('Z')).toLocalDate() : null); " +
    "((param2 == null ? \"\" : String.valueOf(param2)) + \"x\")"
  }

  "a function over CONCAT" should "apply to the whole text" in {
    scriptOf("SELECT UPPER(CONCAT(k, 'x')) FROM t") shouldBe
    "def param1 = (doc['k'].size() == 0 ? null : doc['k'].value); " +
    "((param1 == null ? \"\" : String.valueOf(param1)) + \"x\").toUpperCase()"
  }

  /** One rule in every context (the lead's ruling of 2026-10-23, overturning #382 Ruling B): an
    * ingest processor -- a computed column, a view's -- and a transform skip a NULL argument as a
    * query does. The processor made the whole call NULL, printed the NULL literal as `null`, and
    * applied `UPPER` to its last argument only.
    */
  "an ingest processor and a transform" should "skip a NULL argument, as a query does" in {
    scriptOf("SELECT CONCAT(k, 'x') FROM t", PainlessContextType.Processor) shouldBe
    "def param1 = ctx.k; ((param1 == null ? \"\" : String.valueOf(param1)) + \"x\")"
    scriptOf("SELECT CONCAT(NULL, 'x') FROM t", PainlessContextType.Processor) shouldBe
    "(\"\" + \"x\")"
    scriptOf("SELECT UPPER(CONCAT(k, 'x')) FROM t", PainlessContextType.Processor) shouldBe
    "def param1 = ctx.k; ((param1 == null ? \"\" : String.valueOf(param1)) + \"x\").toUpperCase()"
    scriptOf("SELECT CONCAT(k, 'x') FROM t", PainlessContextType.Transform) shouldBe
    "def param1 = (doc['k'].size() == 0 ? null : doc['k'].value); " +
    "((param1 == null ? \"\" : String.valueOf(param1)) + \"x\")"
  }
}
