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

package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.schema.IndexField
import app.softnetwork.elastic.sql.ObjectValue
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.CreateTable
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** `ScriptProcessor.materialized` — the column's value is ALREADY materialized in this index, so
  * the script is provenance and must not be executed here.
  *
  * The pair of properties this pins is the whole point, and neither half is sufficient alone:
  * `DESCRIBE` must still report the derivation, and the ingest pipeline must not carry it.
  *
  * 🔴 The flag must survive TWO reconstruction paths, and they read different things:
  * `Parser.column` rebuilds the column (and hence the ingest processor) from the DDL TEXT, so the
  * text carries the `STORED` marker; `IndexField` rebuilds it from `_meta`, which is the path
  * `DESCRIBE` / `SHOW CREATE` take. `_meta` ALONE is not enough — `Table.update()` regenerates
  * `_meta.columns` from the parsed columns, so a text-less flag is erased on the first round-trip
  * and the suppressed processor silently returns. That was measured, not assumed.
  */
class MaterializedScriptSpec extends AnyFlatSpec with Matchers {

  private val derivation = "UPPER(customer_name)"
  private val painless =
    "def param1 = ctx.customer_name; ctx.customer_name_upper = (param1 == null) ? null : param1.toUpperCase()"

  private def scriptOf(materialized: Boolean): ScriptProcessor = ScriptProcessor(
    script = derivation,
    column = "customer_name_upper",
    dataType = SQLTypes.Varchar,
    source = painless,
    materialized = materialized
  )

  private def column(materialized: Boolean): Column = Column(
    name = "customer_name_upper",
    dataType = SQLTypes.Varchar,
    script = Some(scriptOf(materialized)),
    comment = Some(s"Computed: $derivation")
  )

  private def columnsOf(sql: String): List[Column] =
    Parser(sql) match {
      case Right(ct: CreateTable) =>
        ct.ddl match {
          case Right(cs) => cs
          case other     => fail(s"expected a column list, got $other")
        }
      case other => fail(s"[$sql] rejected: $other")
    }

  "a materialized script" should "emit NO ingest processor" in {
    column(materialized = true).processors shouldBe empty
  }

  it should "still publish the derivation to DESCRIBE" in {
    // `Column.asMap`'s `Script` cell is what `DESCRIBE [MATERIALIZED VIEW]` renders. Suppressing the
    // processor must not cost the provenance — that is the difference between this and simply
    // dropping `script`.
    val table = Table(name = "v", columns = List(column(materialized = true)))
    val row = column(materialized = true).asMap(table).head
    row("Script") shouldBe derivation
  }

  it should "record the flag in _meta too, for the schema-load path" in {
    ObjectValue(column(materialized = true)._meta).ddl should include("materialized")
  }

  it should "render the STORED marker into the DDL text" in {
    column(materialized = true).sql should include(s"SCRIPT AS ($derivation) STORED")
  }

  it should "survive the DDL round-trip, keeping the script and still emitting no processor" in {
    // 🔴 THE property. The deployed index is created by rendering the schema to DDL and running the
    // text (`client.run(schema.sql)`), and `Table.update()` then REBUILDS `_meta.columns` from the
    // parsed columns — so the DDL text is the flag's only carrier. Losing it here means the
    // suppressed processor silently returns, which is exactly what a `_meta`-only design did.
    val rendered = column(materialized = true).sql
    val reparsed = columnsOf(s"CREATE TABLE t (customer_name VARCHAR, $rendered)")
    val restored = reparsed.find(_.name == "customer_name_upper").getOrElse(fail("column lost"))

    restored.script.map(_.script) shouldBe Some(derivation)
    restored.script.map(_.materialized) shouldBe Some(true)
    restored.processors shouldBe empty
  }

  it should "survive Table.update(), which rebuilds _meta from the columns" in {
    val rendered = column(materialized = true).sql
    val reparsed = columnsOf(s"CREATE TABLE t (customer_name VARCHAR, $rendered)")
    val updated = Table(name = "v", columns = reparsed).update()

    ObjectValue(updated.mappings).ddl should include("materialized")
    updated.defaultPipeline.processors.map(_.column) should not contain "customer_name_upper"
  }

  it should "survive the _meta round-trip a schema load performs" in {
    // The load path (`DESCRIBE` / `SHOW CREATE`) reads `_meta`, not the DDL text, so the flag has to
    // be readable from BOTH.
    val original = column(materialized = true)
    val reloaded =
      IndexField("customer_name_upper", original.node, Some(ObjectValue(original._meta))).ddlColumn

    reloaded.script.map(_.script) shouldBe Some(derivation)
    reloaded.script.map(_.materialized) shouldBe Some(true)
    reloaded.processors shouldBe empty
  }

  "an ordinary (non-materialized) script" should "be completely unaffected" in {
    val ordinary = column(materialized = false)
    ordinary.processors.map(_.column) shouldBe Seq("customer_name_upper")
    ordinary.sql should include(s"SCRIPT AS ($derivation)")
    ordinary.sql should not include "STORED"
    ObjectValue(ordinary._meta).ddl should not include "materialized"

    val reloaded =
      IndexField("customer_name_upper", ordinary.node, Some(ObjectValue(ordinary._meta))).ddlColumn
    reloaded.script.map(_.materialized) shouldBe Some(false)
    reloaded.processors.map(_.column) shouldBe Seq("customer_name_upper")
  }
}
