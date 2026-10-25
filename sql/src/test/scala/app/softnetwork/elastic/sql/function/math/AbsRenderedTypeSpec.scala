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

package app.softnetwork.elastic.sql.function.math

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import app.softnetwork.elastic.sql.{renderedTypeOf, Identifier, PainlessContext}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `ABS` renders and reports its argument's type, as DuckDB 1.5.5 types it: `abs(INTEGER)` is an
  * INTEGER, `abs(BIGINT)` a BIGINT, `abs(SMALLINT)` a SMALLINT, `abs(TINYINT)` a TINYINT and
  * `abs(DOUBLE)` a DOUBLE (measured). Over a whole number the script answers the argument's own
  * whole value; over a DOUBLE or a REAL, Painless's `Math.abs` answers a `Double`.
  *
  * It rendered NUMERIC whatever the argument: a GROUP BY `ABS(n)` key was a `double` terms level,
  * and its keys came back as doubles (`3.0` for the INT `3`).
  */
class AbsRenderedTypeSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("n", SQLTypes.Int),
      Column("l", SQLTypes.BigInt),
      Column("sh", SQLTypes.SmallInt),
      Column("tb", SQLTypes.TinyInt),
      Column("x", SQLTypes.Double),
      Column("r", SQLTypes.Real)
    )
  )

  private def statement(sql: String): SingleSearch = Parser(sql) match {
    case Right(ss: SingleSearch) => ss.update(Some(schema))
    case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def abs(column: String): Identifier =
    statement(s"SELECT ABS($column) AS v FROM t").select.fields.head.identifier

  "ABS" should "render and report a whole argument's own type" in {
    Seq[(String, SQLType)](
      "n"  -> SQLTypes.Int,
      "l"  -> SQLTypes.BigInt,
      "sh" -> SQLTypes.SmallInt,
      "tb" -> SQLTypes.TinyInt
    ).foreach { case (column, t) =>
      withClue(column) {
        renderedTypeOf(abs(column)) shouldBe t
        abs(column).reportedType shouldBe t
        // the script answers the argument's own whole value
        abs(column).painless(Some(PainlessContext())) should endWith(
          "((param1) < 0 ? -(param1) : (param1))"
        )
      }
    }
  }

  it should "render a DOUBLE over a DOUBLE or a REAL, the Double Math.abs answers" in {
    renderedTypeOf(abs("x")) shouldBe SQLTypes.Double
    abs("x").reportedType shouldBe SQLTypes.Double
    renderedTypeOf(abs("r")) shouldBe SQLTypes.Double
    // DuckDB reports `abs(FLOAT)` a FLOAT: the reported type stays the argument's
    abs("r").reportedType shouldBe SQLTypes.Real
    abs("x").painless(Some(PainlessContext())) should endWith("Double.valueOf(Math.abs(param1))")
  }

  "a GROUP BY ABS(n) key" should "be a whole-number terms level, its keys whole numbers" in {
    statement("SELECT ABS(n) AS v, COUNT(*) AS c FROM t GROUP BY ABS(n)").groupBy
      .map(_.buckets.map(_.scriptedKeyValueType)) shouldBe Some(Seq(Some("long")))
    // control: over a DOUBLE, a double level
    statement("SELECT ABS(x) AS v, COUNT(*) AS c FROM t GROUP BY ABS(x)").groupBy
      .map(_.buckets.map(_.scriptedKeyValueType)) shouldBe Some(Seq(Some("double")))
  }
}
