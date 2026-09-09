package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.IngestTimestampValue
import app.softnetwork.elastic.sql.query.{CreatePipeline, CreateTable}
import com.fasterxml.jackson.databind.node.ObjectNode
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.jdk.CollectionConverters._

/** Story 21.8 Part F.1 — the ALTER pipeline churn.
  *
  * The production comparison is `GatewayApi.loadTablePipelineDiff`: the pipeline READ BACK out of
  * Elasticsearch on one side, the one the DDL DECLARES on the other. For a processor nobody touched
  * that diff must be empty, and until this file nothing asserted it — which is why the churn
  * survived story 21.5's fix of the other half of the same root cause.
  *
  * Three independent causes made it non-empty, and only the first was in the issue record. Each is
  * reachable from a DIFFERENT processor kind, which is why the assertions below run over a corpus:
  * one statement can only ever exhibit one of them.
  *
  * ==the anonymous script processor — Elasticsearch 6.8 only==
  * Processor `description` is an ES 7.9+ field; 6.8 drops it, and `PipelineApi` strips it before
  * sending on 6.x besides. `IngestProcessor.apply` re-types a script processor from that
  * description (`ScriptDescRegex`), so without it the read-back is an anonymous `GenericProcessor`.
  * `IngestPipeline.diff` keys processors by `"<pipeline>-<type>-<column>"`, and an anonymous
  * processor has no column, so it could never match its declared counterpart: every ALTER reported
  * it REMOVED and re-ADDED.
  *
  * ==the retyped stored value — EVERY version==
  * The read was `Value(v.asText())`, so `reputation DOUBLE DEFAULT 0.0` was declared as the number
  * `0.0` and read back as the string `"0.0"`: a permanent `ProcessorChanged` that does not depend
  * on `description` at all. Both sides render as `0.0` in the warning the operator sees, so it read
  * as a diff that is not a diff, and that — not rarity — is why it survived every version run.
  *
  * ==the final pipeline compared against the default one — RECORDED, NOT FIXED==
  * `loadTablePipelineDiff` takes the pipeline type, uses it to read the right pipeline out of
  * Elasticsearch, and then compares it against `table.defaultPipeline` regardless. 🔴 The obvious
  * fix is MEASURABLY WORSE and was reverted: a table never DECLARES a final processor, so diffing
  * against `table.finalPipeline` turns churn into an order to DELETE processors nobody touched. The
  * test below is that measurement.
  */
class PipelineRoundTripIdentitySpec extends AnyFlatSpec with Matchers {

  /** Elasticsearch 6.8 stores a pipeline WITHOUT the processor descriptions it was given. */
  private def asStoredOnEs68(pipeline: IngestPipeline): String = {
    val node = pipeline.node
    node.get("processors").elements().asScala.foreach { p =>
      val processorType = p.fieldNames().next()
      p.get(processorType).asInstanceOf[ObjectNode].remove("description")
      ()
    }
    mapper.writeValueAsString(node)
  }

  /** Elasticsearch 7.9+ stores it whole. */
  private def asStoredOnEs79(pipeline: IngestPipeline): String = pipeline.json

  private def declared(sql: String): IngestPipeline =
    Parser(sql) match {
      case Right(ct: CreateTable)    => ct.schema.defaultPipeline
      case Right(cp: CreatePipeline) => cp.ddlPipeline
      case other                     => fail(s"[$sql] expected a pipeline-bearing DDL, got $other")
    }

  private def readBack(
    declaredPipeline: IngestPipeline,
    stored: String,
    as: Option[IngestPipelineType] = None
  ): IngestPipeline =
    IngestPipeline(
      name = declaredPipeline.name,
      json = stored,
      pipelineType = Some(as.getOrElse(declaredPipeline.pipelineType))
    )

  // -- the corpus ------------------------------------------------------------------------------
  // Between them these cover every processor kind a CREATE TABLE deploys: `script` (top level and
  // nested under a STRUCT, whose target is a dotted PATH), `set` as a textual / numeric / boolean /
  // `_ingest.timestamp` default, `date_index_name` from PARTITION BY, and the `_id` `set` from
  // PRIMARY KEY. `rename`, `remove` and `enrich` reach a pipeline only through CREATE PIPELINE and
  // are covered by `handWritten` below, which is asserted separately because its declared
  // processors carry a pipeline type its own pipeline does not (see the divergence recorded after
  // it).

  private val everyProcessorKind =
    """CREATE TABLE users (
      |  id INT NOT NULL,
      |  name VARCHAR DEFAULT 'anonymous',
      |  birthdate DATE,
      |  age INT SCRIPT AS (DATE_DIFF(birthdate, CURRENT_DATE, YEAR)),
      |  ingested_at TIMESTAMP DEFAULT _ingest.timestamp,
      |  profile STRUCT FIELDS(
      |    join_date DATE,
      |    seniority INT SCRIPT AS (DATE_DIFF(profile.join_date, CURRENT_DATE, DAY))
      |  ),
      |  PRIMARY KEY (id)
      |) PARTITION BY birthdate (MONTH)""".stripMargin

  private val numericDefault =
    "CREATE TABLE users (id KEYWORD, reputation DOUBLE DEFAULT 0.0)"

  private val integerDefault =
    "CREATE TABLE users (id KEYWORD, visits INT DEFAULT 0)"

  private val booleanDefault =
    "CREATE TABLE users (id KEYWORD, active BOOLEAN DEFAULT true)"

  private val scriptOnly =
    """CREATE TABLE users (
      |  join_date DATE,
      |  seniority INT SCRIPT AS (DATE_DIFF(join_date, CURRENT_DATE, DAY))
      |)""".stripMargin

  private val handWritten =
    """CREATE OR REPLACE PIPELINE user_pipeline WITH PROCESSORS (
      |    SET (
      |        field = "name",
      |        if = "ctx.name == null",
      |        ignore_failure = true,
      |        value = "anonymous"
      |    ),
      |    SCRIPT (
      |        lang = "painless",
      |        source = "def param1 = ctx.birthdate; ctx.age = (param1 == null) ? null : 1",
      |        ignore_failure = true
      |    ),
      |    RENAME (
      |        field = "old_name",
      |        target_field = "new_name",
      |        ignore_failure = true
      |    ),
      |    REMOVE (
      |        field = "obsolete",
      |        ignore_failure = true
      |    ),
      |    ENRICH (
      |        field = "zip",
      |        policy_name = "zip_policy",
      |        target_field = "location",
      |        max_matches = 1,
      |        ignore_failure = true
      |    )
      |)""".stripMargin

  private val corpus: Seq[(String, String)] = Seq(
    "every processor kind" -> everyProcessorKind,
    "a numeric default"    -> numericDefault,
    "an integer default"   -> integerDefault,
    "a boolean default"    -> booleanDefault,
    "a computed column"    -> scriptOnly,
    // The value read is now TYPE-sensitive, so the shapes where a JSON scalar and a SQL literal
    // could disagree are guarded rather than assumed: an integer literal on a floating column, a
    // magnitude past what a Double holds exactly, and a scale that renders differently.
    "an integer literal on a DOUBLE column" ->
    "CREATE TABLE users (id KEYWORD, x DOUBLE DEFAULT 0)",
    "a value beyond 2^53" ->
    "CREATE TABLE users (id KEYWORD, x BIGINT DEFAULT 9007199254740993)",
    "a trailing-zero scale" ->
    "CREATE TABLE users (id KEYWORD, x DOUBLE DEFAULT 0.10)"
  )

  // -- the property ----------------------------------------------------------------------------

  for ((label, sql) <- corpus) {
    s"a pipeline declaring $label" should "diff EMPTY after an Elasticsearch 6.8 round trip" in {
      val desired = declared(sql)
      val actual = readBack(desired, asStoredOnEs68(desired))
      withClue(s"read back as ${actual.processors.map(p => p.processorType.name -> p.column)}: ") {
        actual.diff(desired) shouldBe Nil
      }
    }

    it should "diff EMPTY after an Elasticsearch 7.9+ round trip" in {
      val desired = declared(sql)
      val actual = readBack(desired, asStoredOnEs79(desired))
      withClue(s"read back as ${actual.processors.map(p => p.processorType.name -> p.column)}: ") {
        actual.diff(desired) shouldBe Nil
      }
    }
  }

  "a pipeline declared directly, with hand-written processors" should "diff EMPTY on both versions" in {
    // The other DDL surface. Its `source` was NOT written by `ScriptProcessor`, so the script
    // target is recovered from a script this codebase did not emit.
    //
    // Read back under the pipeline type its processors DECLARE -- see the divergence recorded
    // immediately below, which this case deliberately holds still so it tests one thing.
    val desired = declared(handWritten)
    val asDeclared = Some(IngestPipelineType.Default)
    readBack(desired, asStoredOnEs68(desired), asDeclared).diff(desired) shouldBe Nil
    readBack(desired, asStoredOnEs79(desired), asDeclared).diff(desired) shouldBe Nil

    // 🔴 The empty diff ALONE proves little here: both sides are anonymous processors over
    // IDENTICAL content, so any deterministic identity satisfies it. What matters is WHERE the
    // recovery lives -- these processors keep an anonymous `column`, because the recovery is
    // confined to the diff key and must not reach `Table.defaultPipeline`'s `filterNot`,
    // `IngestPipeline.merge`, `ProcessorRemoved.stmt` or `describe`.
    desired.processors
      .filter(_.processorType == IngestProcessorType.Script)
      .map(_.column)
      .foreach(_ should startWith("anonymous_"))
  }

  "a CUSTOM pipeline read back under its OWN name" should
  "diverge on `pipelineType` -- RECORDED, not fixed here" in {
    // 🔴 A THIRD divergence of the same class, found by this file and deliberately left standing.
    //
    // `IngestPipeline(name, json, …)` stamps the PIPELINE's type onto every processor it parses;
    // the case-class constructor `IngestPipeline(name, type, processors)` does not, and the DDL
    // parse hard-codes `Default` because the pipeline type is not known yet when a processor is
    // read. So a custom pipeline's processors say `DEFAULT` on the declared side and `CUSTOM` on
    // the read-back side, and `pipelineType` is part of `IngestPipeline.diff`'s key: EVERY
    // processor comes back added AND removed.
    //
    // It is not reached by shipped code -- `PipelineApi` merges custom pipelines rather than
    // diffing them, and `GatewayApi` only ever diffs the default and final ones, both of which are
    // consistent on both sides once the FINAL-vs-DEFAULT comparison is fixed. Closing it means
    // re-typing the declared processors through the read-back derivation, which changes the JSON
    // `PipelineApi` sends to Elasticsearch; that is a lead call, not a drive-by.
    //
    // This assertion is the record. It FAILS the day someone fixes it, which is the point.
    // 🔴 Pinned on the CAUSE — the two pipeline-type stamps — and NOT on the size of the resulting
    // diff. A magnitude assertion inside a file whose thesis is that the round trip IS an identity
    // would certify both, and the cheapest way past it for a future fixer is to edit the number.
    // Naming the stamps means the day they agree, this test says so.
    val desired = declared(handWritten)
    val actual = IngestPipeline(name = desired.name, json = asStoredOnEs79(desired))
    desired.processors.map(_.pipelineType.name).distinct shouldBe List("DEFAULT")
    actual.processors.map(_.pipelineType.name).distinct shouldBe List("CUSTOM")
    actual.diff(desired) should not be Nil
  }

  // -- the counter-property: an unchanged processor is not bought by matching everything ---------

  "a script processor whose expression GENUINELY changed" should "be reported as CHANGED" in {
    // 🔴 The SHAPE is the assertion. `should not be Nil` is satisfied by the DEFECT too -- the
    // churn made every diff non-empty -- so it proves nothing. Recovering the identity is what
    // turns a remove-plus-add into a change, and only naming `ProcessorChanged` tells them apart.
    val desired = declared(
      """CREATE TABLE users (
        |  join_date DATE,
        |  seniority INT SCRIPT AS (DATE_DIFF(join_date, CURRENT_DATE, MONTH))
        |)""".stripMargin
    )
    val actual = readBack(declared(scriptOnly), asStoredOnEs68(declared(scriptOnly)))
    actual.diff(desired).map(_.getClass.getSimpleName) shouldBe List("ProcessorChanged")
  }

  "a default value that GENUINELY changed" should "be reported as CHANGED" in {
    val desired = declared("CREATE TABLE users (id KEYWORD, reputation DOUBLE DEFAULT 1.0)")
    val actual = readBack(declared(numericDefault), asStoredOnEs79(declared(numericDefault)))
    actual.diff(desired).map(_.getClass.getSimpleName) shouldBe List("ProcessorChanged")
  }

  "a computed column that was DROPPED" should "still be reported as removed" in {
    val desired = declared("CREATE TABLE users (join_date DATE)")
    val actual = readBack(declared(scriptOnly), asStoredOnEs68(declared(scriptOnly)))
    actual.diff(desired).map(_.getClass.getSimpleName) shouldBe List("ProcessorRemoved")
  }

  // -- which pipeline a diff is against ---------------------------------------------------------

  "the pipeline a table declares for a type" should "be the FINAL one for FINAL" in {
    // 🔴 This assertion replaced one that recorded the opposite, and the correction is the point.
    //
    // `GatewayApi.loadTablePipelineDiff` compared what it had read back under the FINAL name
    // against `table.defaultPipeline`. The first reading of that was "the obvious fix is worse,
    // because a table declares no final processor, so diffing against the empty `finalPipeline`
    // orders a DELETE". Tracing it through `TableDiff.finalPipeline` -- which is what the caller
    // actually applies -- refutes it: BOTH comparisons order the same `ProcessorRemoved`. The
    // deletion is already there. What the old comparison adds is Default-typed `ProcessorAdded`
    // entries that get spliced in and applied to the DEFAULT pipeline.
    //
    // A diff has to be read through the filter its caller applies. Read raw, it says the opposite.
    val table = Parser(scriptOnly) match {
      case Right(ct: CreateTable) => ct.schema
      case other                  => fail(s"expected a CreateTable, got $other")
    }
    val storedFinal = IngestPipeline(
      name = "final-users",
      pipelineType = IngestPipelineType.Final,
      processors = Seq(
        table.columns
          .find(_.name == "seniority")
          .flatMap(_.script)
          .getOrElse(fail("no script processor for seniority"))
          .copy(pipelineType = IngestPipelineType.Final)
      )
    )

    def finalEntries(d: IngestPipeline): List[String] =
      storedFinal
        .diff(d)
        .filter(_.pipelineType == IngestPipelineType.Final)
        .map(_.getClass.getSimpleName)

    // the same order either way -- the claim that the fix introduces a deletion is false
    finalEntries(table.defaultPipeline) shouldBe List("ProcessorRemoved")
    finalEntries(table.declaredPipeline(IngestPipelineType.Final)) shouldBe List("ProcessorRemoved")

    // what actually differs: comparing like with like stops producing DEFAULT-typed additions
    storedFinal
      .diff(table.defaultPipeline)
      .count(
        _.pipelineType == IngestPipelineType.Default
      ) should be > 0
    storedFinal
      .diff(table.declaredPipeline(IngestPipelineType.Final))
      .count(_.pipelineType == IngestPipelineType.Default) shouldBe 0

    // and the selection is exhaustive, so no type silently answers with the default pipeline
    table.declaredPipeline(IngestPipelineType.Default) shouldBe table.defaultPipeline
    table.declaredPipeline(IngestPipelineType.Custom) should not be table.defaultPipeline
  }

  "a stored final SCRIPT processor" should
  "still diff as removed -- RECORDED, it needs the load path, not this choice" in {
    // The residual, stated so it is not mistaken for something this story closed. A table never
    // DECLARES a final script processor: the parser has no syntax for one, and the
    // `Index -> Table` load attaches a final pipeline's `ScriptProcessor` to its COLUMN, which puts
    // it in `tableProcessors` and so in the DEFAULT pipeline. Only `rename`, `remove` and a
    // non-default `set` survive the load as Final-typed.
    val table = Parser(scriptOnly) match {
      case Right(ct: CreateTable) => ct.schema
      case other                  => fail(s"expected a CreateTable, got $other")
    }
    table.processors shouldBe empty
    table.declaredPipeline(IngestPipelineType.Final).processors shouldBe empty
  }

  // -- a stored value must survive its own round trip, container or not ------------------------

  private def storedSet(value: String): IngestPipeline =
    IngestPipeline(
      name = "p",
      json = s"""{"processors":[{"set":{"field":"tags","value":$value}}]}""",
      pipelineType = Some(IngestPipelineType.Default)
    )

  /** What the next `ALTER PIPELINE` would write back to Elasticsearch for that processor. */
  private def writtenBack(pipeline: IngestPipeline): String =
    mapper.writeValueAsString(pipeline.processors.head.node.get("set").get("value"))

  "a stored LIST value" should "be written back as the same list" in {
    // 🔴 Two defects met here, and the second is why the first could not simply be waved through.
    //
    // The read was `Value(node.asText())`, and Jackson's `asText()` on a container node is the
    // EMPTY STRING, so a list-valued `set` processor came back as `""` and was rewritten as `""`.
    //
    // Reading it properly is not enough: `Values.value` is the WRAPPED `Seq[Value[_]]`, and
    // `properties` feeds that straight to `mapToJsonNode`, which serialises each element as a
    // Jackson BEAN — `{"value":"a","regex":{…},"expr":{…}}`. `ALTER PIPELINE` rewrites every
    // processor of the merged pipeline, so an UNTOUCHED processor would have been written back to
    // the cluster in that shape. `Value.unwrap` is the other half of the fix.
    writtenBack(storedSet("""["a","b"]""")) shouldBe """["a","b"]"""
  }

  it should "be written back as the same list for every scalar element type" in {
    writtenBack(storedSet("[1,2]")) shouldBe "[1,2]"
    writtenBack(storedSet("[1.5,2.5]")) shouldBe "[1.5,2.5]"
    writtenBack(storedSet("[true,false]")) shouldBe "[true,false]"
  }

  "a stored OBJECT value" should "be written back as the same object" in {
    writtenBack(storedSet("""{"lat":48.8,"lon":2.3}""")) shouldBe """{"lat":48.8,"lon":2.3}"""
  }

  it should "survive NESTING, which one level of unwrapping does not" in {
    // 🔴 Measured: unwrapping only the outer map left `tags` as beans. The unwrap has to recurse.
    writtenBack(storedSet("""{"tags":["a","b"],"n":1}""")) shouldBe """{"tags":["a","b"],"n":1}"""
  }

  "a stored value the typed read cannot type" should "fall back, never throw" in {
    // 🔴 A list whose head is `Null` has no arm in the sealed `Values` hierarchy, so
    // `Value.apply(Any)` THROWS — and this read runs inside `PipelineApi`'s pipeline load and
    // `IndicesApi`'s schema cache, neither of which catches it. Reading a value more precisely must
    // not turn a wrong ALTER into one that dies; such a value keeps the old text read.
    val pipeline = storedSet("""[null,"a"]""")
    pipeline.processors.map(_.column) shouldBe List("tags")
    writtenBack(pipeline) shouldBe "\"\"" // the JSON empty string, i.e. today's text read
  }

  "a container-valued processor" should "diff EMPTY against itself across the round trip" in {
    // The property this file exists for, applied to the shapes the corpus above cannot reach: a
    // container value can only enter a pipeline through CREATE PIPELINE or an externally authored
    // one, never through a CREATE TABLE default.
    Seq("""["a","b"]""", "[1,2]", """{"lat":48.8,"lon":2.3}""", """{"tags":["a","b"],"n":1}""")
      .foreach { value =>
        val declared = storedSet(value)
        val actual = readBack(declared, asStoredOnEs79(declared))
        withClue(s"value $value: ") { actual.diff(declared) shouldBe Nil }
      }
  }

  // -- the identity itself ----------------------------------------------------------------------

  "the script target" should "round-trip through the one derivation that writes it" in {
    ScriptTarget.of(ScriptTarget.assign("age", "1")) shouldBe Some("age")
    ScriptTarget.of(ScriptTarget.assign("profile.seniority", "param2")) shouldBe
    Some("profile.seniority")
  }

  it should "read the LAST assignment, which is the one `assign` appends" in {
    // 🔴 Added because a falsification pass found this untested: switching the match from greedy to
    // lazy left every other case green. A generated preamble holds `def paramN = …` only, so the
    // sources this codebase writes have exactly one assignment and cannot tell the two apart. A
    // hand-written SCRIPT processor can have several, and the target is the last one — that is
    // where `assign` puts it.
    ScriptTarget.of("ctx.tmp = 1; ctx.age = 2") shouldBe Some("age")
    ScriptTarget.of("def p = ctx.a; ctx.tmp = p; ctx.profile.seniority = p") shouldBe
    Some("profile.seniority")
  }

  it should "read the ASSIGNMENT, not a `ctx.` reference in the preamble" in {
    // Every generated preamble statement mentions `ctx.<path>` on the RIGHT of its `=`. Picking one
    // of those up would key the processor by an INPUT column instead of the output one -- which
    // still matches nothing, and does it silently.
    ScriptTarget.of(
      "def param1 = ctx.join_date; def param2 = ctx.hired_date; ctx.seniority = param1"
    ) shouldBe Some("seniority")
  }

  it should "read a source whose statements are newline-separated" in {
    // Painless requires the `;`, so the separator is `;\n` rather than a bare newline. `(?s)` plus
    // `\s*` already covers it; asserted because a multi-line operator-authored pipeline is the
    // realistic hand-written shape and nothing else in this file has one.
    ScriptTarget.of("def p = ctx.a;\nctx.age = p") shouldBe Some("age")
  }

  it should "decline a source it did not write, so the caller keeps its fallback" in {
    ScriptTarget.of("ctx['age'] = 1") shouldBe None
    ScriptTarget.of("def x = 1") shouldBe None
    ScriptTarget.of("if (ctx.age == null) { return }") shouldBe None
  }
}
