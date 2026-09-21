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

package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.sql.PainlessScript
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{AlterTable, CreateTable, SingleSearch}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.prop.TableDrivenPropertyChecks

import scala.collection.immutable.ListMap

/** Issue #385 — the computed-column reference rule (#376) reaches the two paths that carry a
  * computed column WITHOUT a statement-level column list.
  *
  *   1. **CTAS.** `CreateTable.validate()` takes `case Left(select) => select.validate()`: it
  *      validates the QUERY, never the scripts the query's columns carry, and
  *      `Table.mergeWithSearch` copied the whole source `Column`, `script` included. `CREATE TABLE
  *      t2 AS SELECT c FROM t1` left `t2` with a processor reading `ctx.d` on an index with no `d`
  *      — it throws on every document, `ignore_failure: true` swallows it, and `SHOW CREATE TABLE
  *      t2` advertises a derivation the index cannot perform. The script is now DROPPED (the values
  *      come from the `INSERT INTO … AS SELECT` that populates the table), and `GatewayApi`
  *      validates the merged table as the backstop.
  *
  * 2. **A live table.** `ScriptProcessor.expr` is `None` for anything read back out of
  * Elasticsearch, and the rule walked `expr`, so `ALTER TABLE t DROP COLUMN n` was caught on a
  * PARSED table and silently accepted on the LOADED one — the only shape production ever sees. The
  * SQL text survives the round-trip (`_meta.script.sql`), so the reference set is recovered by
  * re-parsing it.
  */
class ComputedColumnReachSpec extends AnyFlatSpec with Matchers with TableDrivenPropertyChecks {

  private def create(sql: String): Table = Parser(sql) match {
    case Right(ct: CreateTable) => ct.schema
    case other                  => fail(s"[$sql] expected a CreateTable, got $other")
  }

  private def search(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def alter(sql: String): AlterTable = Parser(sql) match {
    case Right(a: AlterTable) => a
    case other                => fail(s"[$sql] expected an AlterTable, got $other")
  }

  private def columnOf(table: Table, name: String): Column =
    table.find(name).getOrElse(fail(s"table '${table.name}' has no column '$name'"))

  private def scriptOf(table: Table, name: String): ScriptProcessor =
    columnOf(table, name).script.getOrElse(
      fail(s"column '$name' of table '${table.name}' carries no script")
    )

  /** The shape EVERY production CTAS starts from: a source table read back out of Elasticsearch,
    * whose processors therefore have no parsed expression. Built by copying, so the two shapes
    * differ in exactly one field and nothing else can explain a divergence.
    */
  private def asLoadedFromElasticsearch(table: Table): Table = {
    def strip(column: Column): Column =
      column.copy(
        script = column.script.map(_.copy(expr = None)),
        multiFields = column.multiFields.map(strip)
      )
    table.copy(columns = table.columns.map(strip))
  }

  // 🔴 The two WARNs this module emits are asserted in `core`
  // (`CtasComputedColumnSpec`), not here. `sql` carries NO logging backend, so an slf4j NOP logger
  // would make a capture here pass VACUOUSLY -- and adding one would put a `logback-test.xml` on
  // the test classpath of every downstream module through `test->test`, overriding
  // `persistence-core-testkit`'s own `logback.xml` on all five ES client modules. `licensing`
  // records that trap verbatim in its own build. `core` already has the backend, the
  // `TestLoggingConfigurator` SPI and `LogbackCapture`.

  private val t1 = create("CREATE TABLE t1 (d DATE, c INTEGER SCRIPT AS (YEAR(d)))")

  // ── 1. CTAS: a script the projection cannot satisfy is dropped ────────────────────────────────

  "a projection that does not carry a script's operands" should "copy the column WITHOUT its script" in {
    val merged = t1.mergeWithSearch(search("SELECT c FROM t1")).copy(name = "t2")

    columnOf(merged, "c").dataType shouldBe columnOf(t1, "c").dataType
    columnOf(merged, "c").script shouldBe None
    // 🔴 The PIPELINE is what reaches Elasticsearch — a column with no script that still emits a
    // processor would be the same defect one layer down.
    merged.defaultPipeline.processors shouldBe empty
    // and the backstop `GatewayApi` runs on the merged table agrees
    validateScriptReferences(merged) shouldBe Right(())
  }

  it should "do so when the SOURCE table was read back out of Elasticsearch (expr = None)" in {
    // The production shape, and the one a parsed fixture cannot reach: `loadSchema` builds every
    // ScriptProcessor from `_meta` with no AST, so a rule that walks `expr` alone sees nothing here.
    val loaded = asLoadedFromElasticsearch(t1)
    scriptOf(loaded, "c").expr shouldBe None

    val merged = loaded.mergeWithSearch(search("SELECT c FROM t1")).copy(name = "t2")
    columnOf(merged, "c").script shouldBe None
    merged.defaultPipeline.processors shouldBe empty
  }

  // ── 2. ...and a projection that DOES carry them changes nothing ───────────────────────────────

  "a projection that carries every operand" should "keep the script, byte-identical" in {
    // 🔴 The no-regression pin, and the one that fails if the drop is too eager. `source` is the
    // Painless Elasticsearch executes: comparing it byte-for-byte is the only assertion that says
    // the emission did not move.
    forAll(
      Table(
        ("dql", "why"),
        ("SELECT d, c FROM t1", "the operand precedes the computed column"),
        (
          "SELECT c, d FROM t1",
          "🔴 the operand FOLLOWS it — the fold-order trap: deciding inside the fold sees a " +
          "PARTIAL column list and would drop a script the projection does satisfy"
        )
      )
    ) { (dql, why) =>
      withClue(s"[$dql] — $why: ") {
        val merged = t1.mergeWithSearch(search(dql)).copy(name = "t3")
        scriptOf(merged, "c").source shouldBe scriptOf(t1, "c").source
        scriptOf(merged, "c").script shouldBe "YEAR(d)"
        merged.defaultPipeline.processors.map(_.column) should contain("c")
        validateScriptReferences(merged) shouldBe Right(())
      }
    }
  }

  // ── 3. the carve-outs and the paths that resolve the same way as the validator ────────────────

  "a STORED script" should "be kept even when its operands do not survive the projection" in {
    // A `materialized` script emits NOTHING (`Column.processors` filters it out) and carrying the
    // derivation forward into an index whose source operands deliberately did not survive is the
    // documented POINT of STORED. Same carve-out `validateScriptReferences` makes.
    val source = create(
      "CREATE TABLE v (d DATE, c INTEGER SCRIPT AS (YEAR(d)) STORED)"
    )
    val merged = source.mergeWithSearch(search("SELECT c FROM v")).copy(name = "v2")
    scriptOf(merged, "c").materialized shouldBe true
    scriptOf(merged, "c").script shouldBe "YEAR(d)"
    merged.defaultPipeline.processors shouldBe empty
  }

  "a STRUCT-path operand" should "decide the same way the validator's lookup does" in {
    val source = create(
      "CREATE TABLE s (meta STRUCT FIELDS (dt DATE), c TIMESTAMP SCRIPT AS (DATE_TRUNC(meta.dt, MONTH)))"
    )
    val kept = source.mergeWithSearch(search("SELECT meta, c FROM s")).copy(name = "s2")
    withClue("the STRUCT survives, so the script must: ") {
      scriptOf(kept, "c").source shouldBe scriptOf(source, "c").source
    }
    val dropped = source.mergeWithSearch(search("SELECT c FROM s")).copy(name = "s3")
    withClue("the STRUCT does not survive: ") {
      columnOf(dropped, "c").script shouldBe None
      dropped.defaultPipeline.processors shouldBe empty
    }
  }

  /** CHARACTERISATION, measured, not a design choice made here: an aliased projection is looked up
    * by the ALIAS, which the source table does not declare, so the whole source column — script
    * included — is never found and a fresh plain column is built instead. Pinned so the behaviour
    * is visible; if the lookup ever learns about aliases, the drop rule above is what must then
    * decide the script's fate.
    */
  "an aliased projection" should "not carry the source column at all" in {
    val merged = t1.mergeWithSearch(search("SELECT c AS x FROM t1")).copy(name = "t5")
    merged.columns.map(_.name) shouldBe List("x")
    columnOf(merged, "x").script shouldBe None
  }

  /** L-2, MEASURED rather than assumed. Inside a `STRUCT`, `y INTEGER SCRIPT AS (YEAR(dt))`
    * resolves `dt` to the TOP-LEVEL column of that name — the emission itself reads `ctx.dt`, not
    * `ctx.meta.dt` — so there is no struct-relative resolution in this engine for a top-level
    * lookup to be wrong about. That is also why the shape cannot be CREATED today: the same reading
    * makes `validateScriptReferences` reject it at parse time. Pinned so the recursion's lookup is
    * a recorded fact rather than an assumption.
    */
  "a computed column INSIDE a STRUCT" should "resolve its operand at the TOP level, in the emission and in both rules" in {
    val ddl = "CREATE TABLE s (meta STRUCT FIELDS (dt DATE, y INTEGER SCRIPT AS (YEAR(dt))))"
    withClue("the DDL is rejected at parse time by the same reading: ") {
      Parser(ddl) match {
        case Left(err) =>
          err.msg should include("'meta.y'")
          err.msg should include("reads 'dt'")
        case Right(_) => fail("ACCEPTED — the reference rule no longer reads it top-level")
      }
    }
    // ...and the AST behind that rejection says why: `ctx.dt`, a top-level read.
    val table = Parser.parseUnvalidated(ddl) match {
      case Right(ct: CreateTable) => ct.schema
      case other                  => fail(s"[$ddl] expected a CreateTable, got $other")
    }
    val nested = columnOf(table, "meta.y")
    nested.path shouldBe "meta.y"
    nested.script.map(_.source).getOrElse("") should include("ctx.dt")
    nested.script.map(_.source).getOrElse("") should not include "ctx.meta.dt"
    nested.script.toSeq
      .flatMap(_.validationExpr)
      .flatMap(ScriptReferences.of)
      .map(_._1.path) shouldBe Seq("dt")
  }

  /** The recursion into `multiFields`, on the shape that REACHES it: a computed column declared
    * inside a `STRUCT` whose operand is a TOP-LEVEL column (which is the only kind of operand this
    * engine resolves, per the test above). `SELECT meta FROM s` carries the struct and drops `d`,
    * so the nested script must go with it — a rule applied to top-level columns only would ship a
    * pipeline entry for `meta.y` reading `ctx.d`.
    */
  "a computed column nested in a projected STRUCT" should "lose its script when its operand does not survive" in {
    val source =
      create("CREATE TABLE s (d DATE, meta STRUCT FIELDS (y INTEGER SCRIPT AS (YEAR(d))))")
    withClue("the source really does emit it: ") {
      source.defaultPipeline.processors.map(_.column) should contain("meta.y")
    }
    val merged = source.mergeWithSearch(search("SELECT meta FROM s")).copy(name = "s2")
    merged.columns.map(_.name) shouldBe List("meta")
    columnOf(merged, "meta.y").script shouldBe None
    merged.defaultPipeline.processors shouldBe empty
  }

  it should "keep it when the operand IS projected" in {
    val source =
      create("CREATE TABLE s (d DATE, meta STRUCT FIELDS (y INTEGER SCRIPT AS (YEAR(d))))")
    val merged = source.mergeWithSearch(search("SELECT d, meta FROM s")).copy(name = "s2")
    columnOf(merged, "meta.y").script.map(_.script) shouldBe Some("YEAR(d)")
    merged.defaultPipeline.processors.map(_.column) should contain("meta.y")
  }

  // ── 3b. the table-level processors a projection inherits ──────────────────────────────────────

  /** 🔴 B-2. `defaultPipeline` admits a `Table.processors` entry exactly when no COLUMN contributes
    * a processor for the same name, and `mergeWithSearch` inherited that list WHOLESALE — so a
    * projection shipped a pipeline reading a column the target has no mapping for. The rule the
    * columns get and the rule the pipeline gets must be the same rule.
    */
  private def copyFromD =
    SetProcessor(column = "c", value = app.softnetwork.elastic.sql.Null, copyFrom = Some("d"))

  "a table-level processor whose operand the projection drops" should "not be carried" in {
    // No script on `c` here, so the processor is in the SOURCE's pipeline already: the inheritance
    // is wholesale and predates the script-drop rule entirely.
    val source = create("CREATE TABLE t1 (d DATE, c INTEGER)").copy(processors = Seq(copyFromD))
    withClue("the source really does emit it: ") {
      source.defaultPipeline.processors.map(_.sql) should contain("c COPY FROM d")
    }
    val merged = source.mergeWithSearch(search("SELECT c FROM t1")).copy(name = "t2")
    withClue(s"merged pipeline = ${merged.defaultPipeline.processors.map(_.sql)} ") {
      merged.processors shouldBe empty
      merged.defaultPipeline.processors shouldBe empty
    }
    // 🔴 and the WRITE target alone does not decide it: `c` IS projected here, the lost operand is
    // `copy_from: d`. A rule keyed on `p.column` would have kept this processor.
    columnOf(merged, "c").name shouldBe "c"
  }

  it should "not be UN-GATED by the script drop either" in {
    // 🔴 The second route to the same defect, and the one this change created: `defaultPipeline`
    // admits a table-level processor exactly when no COLUMN contributes one for that name, so
    // while `c` carried its script the `set` was filtered OUT of the source's pipeline — and
    // dropping the script un-gates it in the TARGET. Both routes, one rule.
    val source = t1.copy(processors = Seq(copyFromD))
    withClue("gated out of the SOURCE pipeline by the column's own script: ") {
      source.defaultPipeline.processors.map(_.sql) should not contain "c COPY FROM d"
    }
    val merged = source.mergeWithSearch(search("SELECT c FROM t1")).copy(name = "t2")
    columnOf(merged, "c").script shouldBe None // the script IS dropped, so the gate IS lifted
    withClue(s"merged pipeline = ${merged.defaultPipeline.processors.map(_.sql)} ") {
      merged.defaultPipeline.processors shouldBe empty
    }
  }

  it should "be carried when the projection keeps every column it depends on" in {
    val source = create("CREATE TABLE t1 (d DATE, c INTEGER)").copy(processors = Seq(copyFromD))
    val merged = source.mergeWithSearch(search("SELECT d, c FROM t1")).copy(name = "t3")
    merged.processors.map(_.sql) shouldBe Seq("c COPY FROM d")
    merged.defaultPipeline.processors.map(_.sql) should contain("c COPY FROM d")
  }

  it should "ignore a dependency the SOURCE table never declared" in {
    // `_id` is a metadata field, not a column: the projection has nothing to say about it, and a
    // rule that treated every unknown name as missing would drop every primary-key processor.
    val source = t1.copy(processors = Seq(PrimaryKeyProcessor(column = "_id", value = Set("c"))))
    val merged = source.mergeWithSearch(search("SELECT c FROM t1")).copy(name = "t2")
    merged.processors.map(_.column) shouldBe Seq("_id")
  }

  it should "drop a table-level SCRIPT processor whose expression reads a dropped column" in {
    // The same walk the column rule uses, reached through `Table.processors` instead of a column:
    // a table-level script processor is the shape an `ALTER PIPELINE … ADD PROCESSOR SCRIPT(…)`
    // leaves behind, and it has no column entry to be gated by.
    val source = create("CREATE TABLE t1 (d DATE, c INTEGER)").copy(processors =
      Seq(
        ScriptProcessor(
          script = "YEAR(d)",
          column = "c",
          dataType = SQLTypes.Int,
          source = "def param1 = ctx.d.get(ChronoField.YEAR); ctx.c = param1"
        )
      )
    )
    source.defaultPipeline.processors.map(_.column) should contain("c")
    val merged = source.mergeWithSearch(search("SELECT c FROM t1")).copy(name = "t2")
    merged.defaultPipeline.processors shouldBe empty
  }

  /** ⚠️ The stated LIMIT, pinned so it cannot be mistaken for coverage: a `GenericProcessor` is an
    * arbitrary JSON body read back from Elasticsearch. Its WRITE target is determinable and is
    * checked; its own operands are not, and one of those pointing at a dropped column is CARRIED —
    * exactly as before this change.
    */
  "a GenericProcessor" should "be filtered on its write target only" in {
    val readsDropped = GenericProcessor(
      processorType = IngestProcessorType.Set,
      properties = ListMap[String, Any]("field" -> "c", "copy_from" -> "d")
    )
    val writesDropped = GenericProcessor(
      processorType = IngestProcessorType.Set,
      properties = ListMap[String, Any]("field" -> "d", "value" -> "x")
    )
    val merged = t1
      .copy(processors = Seq(readsDropped, writesDropped))
      .mergeWithSearch(search("SELECT c FROM t1"))
      .copy(name = "t2")
    withClue("the undeterminable READ is carried, knowingly: ") {
      merged.processors.map(_.column) shouldBe Seq("c")
    }
  }

  // ── 4. a live table: the rule must see a script it did not parse ──────────────────────────────

  "DROP COLUMN on a column a script reads" should "be rejected on a PARSED and on a LOADED table alike" in {
    // 🔴 Pinned TOGETHER on purpose: the parsed half passed before this issue and the loaded half
    // did not, and only the pair says the two paths now answer the same question.
    val parsed = create("CREATE TABLE t (n INTEGER, c INTEGER SCRIPT AS (n + 1))")
    val statements = alter("ALTER TABLE t DROP COLUMN n").statements

    forAll(
      Table(
        ("table", "shape"),
        (parsed, "parsed from DDL (expr defined)"),
        (asLoadedFromElasticsearch(parsed), "read back from Elasticsearch (expr = None)")
      )
    ) { (table, shape) =>
      withClue(s"$shape: ") {
        validateScriptReferences(table.merge(statements)) match {
          case Left(reason) =>
            reason should include("'c'")
            reason should include("'n'")
            reason should include("does not exist")
          case Right(_) => fail("the orphaned processor was ACCEPTED")
        }
      }
    }
  }

  it should "keep the type half of the rule working on a LOADED table too" in {
    // The consuming function comes from the re-parsed chain, so the second half of #376's rule
    // (a column handed to a function that cannot take its declared type) rides along for free.
    //
    // 🔴 Built by hand rather than parsed, and that is the POINT: `CREATE TABLE t (k KEYWORD, c
    // INTEGER SCRIPT AS (YEAR(k)))` is REJECTED by #376's rule today, so the only way this shape
    // exists in a cluster is as an index created before it — i.e. exactly a table read back out of
    // Elasticsearch, with `expr = None` and the SQL text in `_meta.script.sql`.
    val loaded = app.softnetwork.elastic.sql.schema.Table(
      name = "t",
      columns = List(
        Column("k", SQLTypes.Keyword),
        Column(
          "c",
          SQLTypes.Int,
          script = Some(
            ScriptProcessor(
              script = "YEAR(k)",
              column = "c",
              dataType = SQLTypes.Int,
              source = "def param1 = ctx.k.get(ChronoField.YEAR); ctx.c = param1"
            )
          )
        )
      )
    )
    scriptOf(loaded, "c").expr shouldBe None
    validateScriptReferences(loaded) match {
      case Left(reason) =>
        reason should include("'k'")
        reason should include("KEYWORD")
      case Right(_) => fail("a KEYWORD handed to YEAR was ACCEPTED")
    }
  }

  /** 🔴 The consequence of making a LOADED table's scripts visible, and the reason the docs say so:
    * a table is re-validated AS A WHOLE, so one invalid computed column refuses EVERY alteration of
    * that table, naming a column the user did not touch. Pinned here because it is the user-visible
    * behaviour a release note has to describe — and so is the escape hatch: dropping the offending
    * column is always accepted, because removing it removes the reference.
    */
  "a loaded table carrying ONE invalid computed column" should "refuse every ALTER until that column goes" in {
    val loaded = asLoadedFromElasticsearch(
      create("CREATE TABLE t (n INTEGER, c INTEGER SCRIPT AS (n + 1))")
    ).merge(alter("ALTER TABLE t DROP COLUMN n").statements)

    forAll(
      Table(
        "unrelated alteration",
        "ALTER TABLE t ADD COLUMN x INTEGER",
        "ALTER TABLE t SET SCHEMA CACHE TTL '10m'"
      )
    ) { statement =>
      withClue(s"[$statement] ") {
        validateScriptReferences(loaded.merge(alter(statement).statements)) match {
          case Left(reason) => reason should include("'c'")
          case Right(_)     => fail("ACCEPTED - the whole-table re-validation no longer happens")
        }
      }
    }

    withClue("the escape hatch: ") {
      validateScriptReferences(
        loaded.merge(alter("ALTER TABLE t DROP COLUMN c").statements)
      ) shouldBe Right(())
    }
  }

  /** 🔴 ...and the escape hatch is per COLUMN, not per statement, because the validator collects
    * the errors of EVERY column. Dropping one offender leaves the other one refusing. Pinned
    * because the documentation sentence a stuck user acts on says exactly this, and an earlier
    * draft of it said "always accepted", which is false.
    */
  it should "still refuse while a SECOND invalid computed column remains" in {
    val two = app.softnetwork.elastic.sql.schema.Table(
      name = "t",
      columns = List(
        Column("k", SQLTypes.Keyword),
        Column("n", SQLTypes.Int),
        loadedScript("c1", "YEAR(k)"),
        loadedScript("c2", "YEAR(k)")
      )
    )
    withClue("dropping ONE of the two: ") {
      validateScriptReferences(two.merge(alter("ALTER TABLE t DROP COLUMN c1").statements)) match {
        case Left(reason) =>
          reason should include("'c2'")
          reason should not include "'c1'"
        case Right(_) => fail("ACCEPTED while a second invalid computed column remains")
      }
    }
    withClue("dropping BOTH: ") {
      validateScriptReferences(
        two
          .merge(alter("ALTER TABLE t DROP COLUMN c1").statements)
          .merge(alter("ALTER TABLE t DROP COLUMN c2").statements)
      ) shouldBe Right(())
    }
  }

  /** A computed column as Elasticsearch hands it back: the SQL text survives, the parsed expression
    * does not.
    */
  private def loadedScript(name: String, expression: String): Column =
    Column(
      name,
      SQLTypes.Int,
      script = Some(
        ScriptProcessor(
          script = expression,
          column = name,
          dataType = SQLTypes.Int,
          source = s"ctx.$name = 1"
        )
      )
    )

  // ── 5. the fail-open, stated and asserted ─────────────────────────────────────────────────────

  /** The fail-open. Its WARN half — the receipt — is asserted in `core` (`CtasComputedColumnSpec`),
    * where a logging backend exists; see the note at the top of this class for why it cannot live
    * here.
    */
  "a stored expression this grammar does not accept" should "be skipped, never rejected" in {
    // `Table` here must be the SCHEMA one -- `TableDrivenPropertyChecks` shadows the name.
    val legacy = app.softnetwork.elastic.sql.schema.Table(
      name = "legacy",
      columns = List(
        Column("n", SQLTypes.Int),
        Column(
          "c",
          SQLTypes.Int,
          script = Some(
            ScriptProcessor(
              script = "### not an expression this grammar accepts ###",
              column = "c",
              dataType = SQLTypes.Int,
              source = "ctx.c = 1"
            )
          )
        )
      )
    )
    scriptOf(legacy, "c").validationExpr shouldBe None
    validateScriptReferences(legacy) shouldBe Right(())
  }

  // ── 6. completeness: the recovered walk is the SAME walk ──────────────────────────────────────

  /** 🔴 The guard that says the fallback is not a second, weaker answer. For each shape the
    * references recovered from the STORED TEXT (expr = None) must equal those the PARSED expression
    * yields — derived independently, one from `ScriptProcessor.script`, one from
    * `ScriptProcessor.expr`. A fallback that quietly found fewer references would make every rule
    * above fail OPEN, which is precisely the defect this issue is about.
    */
  "the references recovered from the stored text" should "be exactly those the parsed expression yields" in {
    val shapes = Table(
      "ddl",
      "CREATE TABLE t (d DATE, c INTEGER SCRIPT AS (YEAR(d)))",
      "CREATE TABLE t (d DATE, n INTEGER, c INTEGER SCRIPT AS (YEAR(d) + n))",
      "CREATE TABLE t (a KEYWORD, b KEYWORD, c KEYWORD SCRIPT AS (CONCAT(a, b)))",
      "CREATE TABLE t (d DATE, n INTEGER, c INTEGER SCRIPT AS (CASE WHEN n > 0 THEN YEAR(d) ELSE 0 END))",
      "CREATE TABLE t (d DATE, c INTEGER SCRIPT AS (YEAR(DATE_TRUNC(d, MONTH))))",
      "CREATE TABLE t (meta STRUCT FIELDS (dt DATE), c TIMESTAMP SCRIPT AS (DATE_TRUNC(meta.dt, MONTH)))",
      "CREATE TABLE t (a INTEGER, b INTEGER, e INTEGER, c INTEGER SCRIPT AS ((a + b) * e))",
      "CREATE TABLE t (k KEYWORD, c KEYWORD SCRIPT AS (COALESCE(k, 'x')))",
      "CREATE TABLE t (n INTEGER, lo INTEGER, hi INTEGER, c INTEGER SCRIPT AS (CASE WHEN n BETWEEN lo AND hi THEN 1 ELSE 0 END))",
      "CREATE TABLE t (d DATE, c KEYWORD SCRIPT AS (DATE_PARSE(d, 'yyyy-MM-dd')))",
      "CREATE TABLE t (a KEYWORD, n INTEGER, c KEYWORD SCRIPT AS (CONCAT(a, n)))"
    )
    forAll(shapes) { ddl =>
      val parsed = scriptOf(create(ddl), "c")
      def references(script: ScriptProcessor): Seq[String] =
        script.validationExpr.toSeq
          .flatMap(ScriptReferences.of)
          .map(_._1.path)

      val fromExpr = references(parsed)
      val fromStoredText = references(parsed.copy(expr = None))
      withClue(s"[$ddl] stored text = ${parsed.script} ") {
        fromExpr should not be empty
        fromStoredText shouldBe fromExpr
      }
    }
  }

  /** The unresolved identifier a re-parse yields must address a column by the SAME name the
    * RESOLVED one does, or `schema.find(id.path)` answers a different question on the two paths.
    *
    * 🔴 Both sides are DERIVED: the resolved side from a real `CREATE TABLE`'s own
    * `ScriptProcessor.expr` (built by `Table.update()` against the settled column list), the
    * unresolved side from `parseScriptExpression` of the SAME text. An expectation list would only
    * have asserted the second one against my reading of it.
    */
  "a re-parsed identifier" should "carry the same path as the RESOLVED one, derived on both sides" in {
    forAll(
      Table(
        ("ddl", "expression"),
        ("CREATE TABLE t (n INTEGER, c INTEGER SCRIPT AS (n + 1))", "n + 1"),
        (
          "CREATE TABLE t (meta STRUCT FIELDS (dt DATE), c TIMESTAMP SCRIPT AS (DATE_TRUNC(meta.dt, MONTH)))",
          "DATE_TRUNC(meta.dt, MONTH)"
        ),
        (
          "CREATE TABLE t (a KEYWORD, b KEYWORD, c KEYWORD SCRIPT AS (CONCAT(a, b)))",
          "CONCAT(a, b)"
        ),
        (
          "CREATE TABLE t (`odd name` INTEGER, c INTEGER SCRIPT AS (`odd name` + 1))",
          "`odd name` + 1"
        )
      )
    ) { (ddl, expression) =>
      val resolved: PainlessScript =
        scriptOf(create(ddl), "c").expr.getOrElse(fail(s"[$ddl] the parsed script has no expr"))
      val unresolved: PainlessScript =
        Parser.parseScriptExpression(expression).getOrElse(fail(s"[$expression] was not parsed"))
      val resolvedPaths = ScriptReferences.of(resolved).map(_._1.path)
      withClue(s"[$ddl] resolved = $resolvedPaths ") {
        resolvedPaths should not be empty
        ScriptReferences.of(unresolved).map(_._1.path) shouldBe resolvedPaths
      }
    }
  }

  // ── 7. a CTAS target is a plain index, whatever it was projected FROM ──────────────────────────

  /** 🔴 B-4. `mergeWithSearch` copied `tableType`, so `CREATE TABLE t AS SELECT … FROM <a
    * materialized view>` produced a table typed `materialized_view` — which makes
    * `validateScriptReferences` short-circuit on its `isRegular` carve-out (the backstop inert on
    * exactly that target), makes every later `ALTER` on that ordinary index THROW, and publishes
    * `_meta.type = materialized_view`. This also PINS the intent behind the deliberate asymmetry
    * documented on `withSatisfiableScripts`: the projection rule has no `isRegular` carve-out, and
    * this is the case that would otherwise have hidden it.
    */
  "a CTAS target projected from a non-regular table" should "be a REGULAR table, with the script rule applied" in {
    val view = create("CREATE TABLE v (d DATE, c INTEGER SCRIPT AS (YEAR(d)))")
      .copy(tableType = TableType.MaterializedView, materializedViews = List("some_view"))
    view.isRegular shouldBe false

    val merged = view.mergeWithSearch(search("SELECT c FROM v")).copy(name = "t2")
    merged.tableType shouldBe TableType.Regular
    merged.materializedViews shouldBe empty
    withClue("...so the projection rule really ran on it: ") {
      columnOf(merged, "c").script shouldBe None
      merged.defaultPipeline.processors shouldBe empty
    }
    withClue("...and the backstop is no longer short-circuited: ") {
      validateScriptReferences(merged) shouldBe Right(())
    }
  }
}
