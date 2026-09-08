package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.NamePart
import app.softnetwork.elastic.sql.query._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 21.7 - `Parser.ident` uniformity: DQL, DML and DDL respect the same name rules.
  *
  * `Parser.ident` was the THIRD name surface of this dialect (stories 21.1 and 21.2 rewrote the
  * other two) and the reason `INSERT`, `UPDATE`, `CREATE`, `DROP`, `ALTER`, `COPY INTO` and every
  * SHOW/DESCRIBE disagreed with `SELECT` - and with each other - about what a name may look like.
  *
  * 🔴 What this file protects is NOT "these statements parse". Three things can go wrong once they
  * do, and a verdict-only assertion sees none of them:
  *
  *   1. the QUALIFIER is interpreted, joined or dropped, so the statement names a different object
  *      (story 21.2 AD-1; measured on 48 of the 99 captured BI statements for the FROM surface); 2.
  *      the RENDER loses the quoting, so `stmt.sql` no longer re-parses to `stmt` - the
  *      lossy-render defect story 21.2 exists to remove, re-created on the DDL surface.
  *      `MaterializedViewExtension` persists a render and runs `client.run(alter.sql)`, so that is
  *      a live corruption, not cosmetics; 3. a BARE spelling that parses today stops parsing, or
  *      parses to a different AST.
  *
  * Every accepting row therefore asserts the resolved name, the parts, AND the rendered text.
  */
class QuotedDmlDdlNameSpec extends AnyFlatSpec with Matchers {

  private def q(v: String) = NamePart(v, quoted = true)
  private def b(v: String) = NamePart(v, quoted = false)

  private def parsed(sql: String): Statement =
    Parser(sql) match {
      case Right(st) => st
      case Left(err) => fail(s"Expected [$sql] to parse, got: ${err.msg}")
    }

  /** A GRAMMAR rejection, as opposed to story 21.4's `NonFatal` boundary catch in `Parser.apply`.
    * The third assertion is what makes the row falsifiable at all: with the boundary catch in place
    * `noException` holds unconditionally and `isLeft` holds for an internal crash too.
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

  /** AC-3: the render is a fixed point in BOTH senses - the exact text, and the AST it re-parses
    * to. Asserting only the AST is blind to information the parse already destroyed (story 21.2
    * measured `FROM "elastic".bi_events` rendering `FROM bi_events` while `Parser(stmt.sql) ==
    * Right(stmt)` still held).
    */
  private def rendersAndReparses(sql: String, expectedRender: String): Statement = {
    val st = parsed(sql)
    withClue(s"[$sql] render ") { st.sql shouldBe expectedRender }
    withClue(s"[$sql] re-parse of [$expectedRender] ") { Parser(st.sql) shouldBe Right(st) }
    st
  }

  // ===========================================================================================
  // AC-1 / AC-2 - TABLE-NAME positions take the qualifier/parts rule, identically to FROM
  // ===========================================================================================

  it should "resolve a qualified INSERT target the way FROM does, in both quote styles" in {
    Seq(
      "INSERT INTO `prod_eu`.dest SELECT a FROM src",
      """INSERT INTO "prod_eu".dest SELECT a FROM src"""
    ).foreach { sql =>
      val insert = rendersAndReparses(sql, """INSERT INTO "prod_eu".dest SELECT a FROM src""")
        .asInstanceOf[Insert]
      withClue(s"[$sql] ") {
        insert.table shouldBe "dest" // the LAST part, structurally
        insert.parts shouldBe Seq(q("prod_eu"), b("dest"))
      }
    }
  }

  it should "resolve a qualified UPDATE target the way FROM does" in {
    val u = rendersAndReparses(
      """UPDATE "sch".orders SET a = 1 WHERE id = 1""",
      """UPDATE "sch".orders SET a = 1 WHERE id = 1"""
    ).asInstanceOf[Update]
    u.table shouldBe "orders"
    u.parts shouldBe Seq(q("sch"), b("orders"))
  }

  it should "accept a quoted table name in every statement kind that names one" in {
    val rows: Seq[(String, String, String, Seq[NamePart])] = Seq(
      (
        """CREATE TABLE "sch".dest (c INTEGER)""",
        "CREATE TABLE \"sch\".dest (\n c INT\n)",
        "dest",
        Seq(q("sch"), b("dest"))
      ),
      ("""DROP TABLE "dest"""", """DROP TABLE "dest"""", "dest", Seq(q("dest"))),
      ("DROP TABLE `dest`", """DROP TABLE "dest"""", "dest", Seq(q("dest"))),
      ("""TRUNCATE TABLE "dest"""", """TRUNCATE TABLE "dest"""", "dest", Seq(q("dest"))),
      ("""SHOW TABLE "dest"""", """SHOW TABLE "dest"""", "dest", Seq(q("dest"))),
      (
        """SHOW CREATE TABLE "dest"""",
        """SHOW CREATE TABLE "dest"""",
        "dest",
        Seq(q("dest"))
      ),
      ("""DESCRIBE TABLE "dest"""", """DESCRIBE TABLE "dest"""", "dest", Seq(q("dest"))),
      (
        """ALTER TABLE "dest" ADD COLUMN c INTEGER""",
        """ALTER TABLE "dest" ADD COLUMN c INT""",
        "dest",
        Seq(q("dest"))
      ),
      (
        """COPY INTO "dest" FROM 's3://b/f.json' FILE_FORMAT = JSON""",
        """COPY INTO "dest" FROM 's3://b/f.json' FILE_FORMAT = JSON""",
        "dest",
        Seq(q("dest"))
      ),
      (
        """DROP MATERIALIZED VIEW "mv1"""",
        """DROP MATERIALIZED VIEW "mv1"""",
        "mv1",
        Seq(q("mv1"))
      ),
      (
        """REFRESH MATERIALIZED VIEW "mv1"""",
        """REFRESH MATERIALIZED VIEW "mv1"""",
        "mv1",
        Seq(q("mv1"))
      ),
      (
        """SHOW MATERIALIZED VIEW "mv1"""",
        """SHOW MATERIALIZED VIEW "mv1"""",
        "mv1",
        Seq(q("mv1"))
      ),
      (
        """SHOW MATERIALIZED VIEW STATUS "mv1"""",
        """SHOW MATERIALIZED VIEW STATUS "mv1"""",
        "mv1",
        Seq(q("mv1"))
      ),
      (
        """SHOW CREATE MATERIALIZED VIEW "mv1"""",
        """SHOW CREATE MATERIALIZED VIEW "mv1"""",
        "mv1",
        Seq(q("mv1"))
      ),
      ("""DROP PIPELINE "p1"""", """DROP PIPELINE "p1"""", "p1", Seq(q("p1"))),
      ("""SHOW PIPELINE "p1"""", """SHOW PIPELINE "p1"""", "p1", Seq(q("p1"))),
      (
        """SHOW CREATE PIPELINE "p1"""",
        """SHOW CREATE PIPELINE "p1"""",
        "p1",
        Seq(q("p1"))
      ),
      ("""DESCRIBE PIPELINE "p1"""", """DESCRIBE PIPELINE "p1"""", "p1", Seq(q("p1"))),
      ("""DROP WATCHER "w1"""", """DROP WATCHER "w1"""", "w1", Seq(q("w1"))),
      (
        """SHOW WATCHER STATUS "w1"""",
        """SHOW WATCHER STATUS "w1"""",
        "w1",
        Seq(q("w1"))
      ),
      (
        """DROP ENRICH POLICY "pol"""",
        """DROP ENRICH POLICY "pol"""",
        "pol",
        Seq(q("pol"))
      ),
      (
        """SHOW ENRICH POLICY "pol"""",
        """SHOW ENRICH POLICY "pol"""",
        "pol",
        Seq(q("pol"))
      ),
      (
        """EXECUTE ENRICH POLICY "pol"""",
        """EXECUTE ENRICH POLICY "pol"""",
        "pol",
        Seq(q("pol"))
      )
    )
    rows.foreach { case (sql, render, name, parts) =>
      val st = rendersAndReparses(sql, render)
      withClue(s"[$sql] ") {
        nameOf(st) shouldBe name
        partsOf(st) shouldBe parts
      }
    }
  }

  /** The `name` / `parts` accessors are per-node, so read them through ONE place rather than
    * repeating a 23-arm match in every assertion.
    */
  private def nameOf(st: Statement): String = st match {
    case s: CreateTable                => s.table
    case s: DropTable                  => s.table
    case s: TruncateTable              => s.table
    case s: ShowTable                  => s.table
    case s: ShowCreateTable            => s.table
    case s: DescribeTable              => s.table
    case s: AlterTable                 => s.table
    case s: Insert                     => s.table
    case s: Update                     => s.table
    case s: CopyInto                   => s.targetTable
    case s: CreateMaterializedView     => s.view
    case s: DropMaterializedView       => s.name
    case s: RefreshMaterializedView    => s.name
    case s: ShowMaterializedView       => s.name
    case s: ShowMaterializedViewStatus => s.name
    case s: ShowCreateMaterializedView => s.name
    case s: DescribeMaterializedView   => s.name
    case s: CreatePipeline             => s.name
    case s: AlterPipeline              => s.name
    case s: DropPipeline               => s.name
    case s: ShowPipeline               => s.name
    case s: ShowCreatePipeline         => s.name
    case s: DescribePipeline           => s.name
    case s: DropWatcher                => s.name
    case s: ShowWatcherStatus          => s.name
    case s: CreateEnrichPolicy         => s.name
    case s: DropEnrichPolicy           => s.name
    case s: ShowEnrichPolicy           => s.name
    case s: ExecuteEnrichPolicy        => s.name
    case other                         => fail(s"no name accessor recorded for ${other.getClass}")
  }

  private def partsOf(st: Statement): Seq[NamePart] = st match {
    case s: CreateTable                => s.parts
    case s: DropTable                  => s.parts
    case s: TruncateTable              => s.parts
    case s: ShowTable                  => s.parts
    case s: ShowCreateTable            => s.parts
    case s: DescribeTable              => s.parts
    case s: AlterTable                 => s.parts
    case s: Insert                     => s.parts
    case s: Update                     => s.parts
    case s: CopyInto                   => s.parts
    case s: CreateMaterializedView     => s.parts
    case s: DropMaterializedView       => s.parts
    case s: RefreshMaterializedView    => s.parts
    case s: ShowMaterializedView       => s.parts
    case s: ShowMaterializedViewStatus => s.parts
    case s: ShowCreateMaterializedView => s.parts
    case s: DescribeMaterializedView   => s.parts
    case s: CreatePipeline             => s.parts
    case s: AlterPipeline              => s.parts
    case s: DropPipeline               => s.parts
    case s: ShowPipeline               => s.parts
    case s: ShowCreatePipeline         => s.parts
    case s: DescribePipeline           => s.parts
    case s: DropWatcher                => s.parts
    case s: ShowWatcherStatus          => s.parts
    case s: CreateEnrichPolicy         => s.parts
    case s: DropEnrichPolicy           => s.parts
    case s: ShowEnrichPolicy           => s.parts
    case s: ExecuteEnrichPolicy        => s.parts
    case other                         => fail(s"no parts accessor recorded for ${other.getClass}")
  }

  // An UNQUOTED dot belongs to the name. This is the rule that protects real Elasticsearch index
  // names, and it must read the same in DDL/DML as it does in FROM (story 21.2 AD-1).
  it should "keep an unquoted dotted name whole, exactly as FROM does" in {
    Seq(
      "DROP TABLE logs-2025.03",
      "TRUNCATE TABLE logs-2025.03",
      "SHOW TABLE logs-2025.03"
    ).foreach { sql =>
      withClue(s"[$sql] ") {
        val st = parsed(sql)
        nameOf(st) shouldBe "logs-2025.03"
        partsOf(st) shouldBe Nil // a single BARE part IS the name - nothing to record
        st.sql shouldBe sql
      }
    }
    // ... and DELETE, whose FROM-routed surface story 21.2 already fixed, agrees with them.
    parsed("DELETE FROM logs-2025.03 WHERE id = 1").sql shouldBe
    "DELETE FROM logs-2025.03 WHERE id = 1"
  }

  // ===========================================================================================
  // AC-4 - COLUMN-NAME positions take story 21.1's quoted single part
  // ===========================================================================================

  it should "accept a quoted column name wherever a column is named" in {
    rendersAndReparses(
      """INSERT INTO tbl ("c") VALUES ('a')""",
      "INSERT INTO tbl (c) VALUES (\"a\")"
    ).asInstanceOf[Insert].cols shouldBe Seq("c")
    rendersAndReparses(
      "INSERT INTO tbl (`c`) VALUES ('a')",
      "INSERT INTO tbl (c) VALUES (\"a\")"
    ).asInstanceOf[Insert].cols shouldBe Seq("c")
    rendersAndReparses(
      """UPDATE tbl SET "a" = 1 WHERE id = 1""",
      "UPDATE tbl SET a = 1 WHERE id = 1"
    ).asInstanceOf[Update].values.keys.toSeq shouldBe Seq("a")
    rendersAndReparses(
      "UPDATE tbl SET `a` = 1 WHERE id = 1",
      "UPDATE tbl SET a = 1 WHERE id = 1"
    ).asInstanceOf[Update].values.keys.toSeq shouldBe Seq("a")
    rendersAndReparses(
      """CREATE TABLE dest ("c" INTEGER, "d" KEYWORD)""",
      "CREATE TABLE dest (\n c INT,\n d KEYWORD\n)"
    ).asInstanceOf[CreateTable].ddl.toOption.map(_.map(_.name)) shouldBe Some(List("c", "d"))
    rendersAndReparses(
      """ALTER TABLE dest DROP COLUMN "c"""",
      "ALTER TABLE dest DROP COLUMN c"
    )
    rendersAndReparses(
      """ALTER TABLE dest RENAME COLUMN "c" TO "d"""",
      "ALTER TABLE dest RENAME COLUMN c TO d"
    )
    rendersAndReparses(
      """ALTER TABLE dest ALTER COLUMN "c" SET DATA TYPE BIGINT""",
      "ALTER TABLE dest ALTER COLUMN c SET DATA TYPE BIGINT"
    )
    rendersAndReparses(
      """CREATE TABLE dest ("c" INTEGER, PRIMARY KEY ("c"))""",
      "CREATE TABLE dest (\n c INT,\n PRIMARY KEY (c)\n)"
    )
  }

  it should "preserve the case a quoted column name was written in" in {
    val ct = parsed("""CREATE TABLE "Dest" ("Col" INTEGER)""").asInstanceOf[CreateTable]
    ct.table shouldBe "Dest"
    ct.ddl.toOption.map(_.map(_.name)) shouldBe Some(List("Col"))
  }

  // A column list is not a table reference: there is no qualifier RUN here, so every part is
  // JOINED into one key and none can be dropped - exactly as `SELECT "e"."c"` names `e.c`.
  it should "join a dotted column name instead of treating its head as a qualifier" in {
    parsed("""INSERT INTO tbl ("sch".c) VALUES (1)""").asInstanceOf[Insert].cols shouldBe
    Seq("sch.c")
    parsed("""ALTER TABLE dest SET MAPPING "_meta.columns.c.default_value" = 'x'""").sql shouldBe
    parsed("ALTER TABLE dest SET MAPPING _meta.columns.c.default_value = 'x'").sql
  }

  // AC-3, option 2 with its pin: a column name lives in a `List[String]` / `ListMap[String, _]`
  // with nowhere to carry a per-name bit, so `renderColumnName` decides by SHAPE - bare when the
  // bare name surface can spell it, ANSI-quoted otherwise. Without it, the row below renders
  // `INSERT INTO tbl (my col) VALUES (1)`, which does not re-parse.
  it should "re-quote a column name the bare name surface cannot spell" in {
    rendersAndReparses(
      """INSERT INTO tbl ("my col") VALUES (1)""",
      """INSERT INTO tbl ("my col") VALUES (1)"""
    ).asInstanceOf[Insert].cols shouldBe Seq("my col")
    rendersAndReparses(
      """UPDATE tbl SET "my col" = 1 WHERE id = 1""",
      """UPDATE tbl SET "my col" = 1 WHERE id = 1"""
    )
    rendersAndReparses(
      """CREATE TABLE dest ("my col" INTEGER)""",
      "CREATE TABLE dest (\n \"my col\" INT\n)"
    )
    rendersAndReparses(
      """ALTER TABLE dest DROP COLUMN "my col"""",
      """ALTER TABLE dest DROP COLUMN "my col""""
    )
  }

  // ===========================================================================================
  // OQ-1 - option keys and struct-entry keys are IN SCOPE: accept the quoted spelling, key = the
  // unescaped content, NO qualifier run, zero semantic change.
  //
  // Scope note, measured on the unmodified tree: the DOUBLE-QUOTED spelling of an OPTION key was
  // ALREADY accepted, through `TypeParser.literal`, and already produced the content as the key -
  // so for `option` this story is a BACKTICK-only widening and the AST is byte-identical for the
  // double-quoted spelling. `struct_entry` had no `literal` alternative, so BOTH spellings are new
  // there. Neither key kind renders quoted: their bare render is what it has always been (see
  // AD-6), which is why the double-quoted rows below assert the SAME text as the bare ones.
  // ===========================================================================================

  it should "accept a quoted key in each key kind, with the content as the key" in {
    val bare = parsed("CREATE TABLE dest (c INTEGER) OPTIONS (number_of_shards = 1)")
    parsed("""CREATE TABLE dest (c INTEGER) OPTIONS ("number_of_shards" = 1)""") shouldBe bare
    parsed("CREATE TABLE dest (c INTEGER) OPTIONS (`number_of_shards` = 1)") shouldBe bare
    parsed("CREATE TABLE dest (c INTEGER) OPTIONS ('number_of_shards' = 1)") shouldBe bare

    val struct = parsed("INSERT INTO tbl (c) VALUES ({a = 1, b = 2})")
    parsed("""INSERT INTO tbl (c) VALUES ({"a" = 1, b = 2})""") shouldBe struct
    parsed("INSERT INTO tbl (c) VALUES ({`a` = 1, b = 2})") shouldBe struct

    val mapping = parsed("ALTER TABLE dest SET MAPPING dynamic = 'strict'")
    parsed("""ALTER TABLE dest SET MAPPING "dynamic" = 'strict'""") shouldBe mapping
    parsed("ALTER TABLE dest SET MAPPING `dynamic` = 'strict'") shouldBe mapping
    parsed("""ALTER TABLE dest DROP MAPPING "dynamic"""") shouldBe
    parsed("ALTER TABLE dest DROP MAPPING dynamic")
  }

  // ===========================================================================================
  // AC-1 consequences pinned (story 21.2 AD-4)
  // ===========================================================================================

  it should "parse Tableau's MySQL capability probe pair" in {
    val create = rendersAndReparses(
      "CREATE TABLE `#Tableau_sid_1_Connect_Chec` (`COL` INTEGER)",
      "CREATE TABLE \"#Tableau_sid_1_Connect_Chec\" (\n COL INT\n)"
    ).asInstanceOf[CreateTable]
    create.table shouldBe "#Tableau_sid_1_Connect_Chec"
    create.parts shouldBe Seq(q("#Tableau_sid_1_Connect_Chec"))

    val drop = rendersAndReparses(
      "DROP TABLE IF EXISTS `#Tableau_sid_1_Connect_Chec`",
      """DROP TABLE IF EXISTS "#Tableau_sid_1_Connect_Chec""""
    ).asInstanceOf[DropTable]
    drop.table shouldBe "#Tableau_sid_1_Connect_Chec"
    drop.ifExists shouldBe true

    // The SQL-92 half of the same probe family.
    rendersAndReparses("""DROP TABLE "XT__1234567890"""", """DROP TABLE "XT__1234567890"""")
  }

  it should "leave CREATE LOCAL TEMPORARY TABLE rejected, quoted or not" in {
    rejected("CREATE LOCAL TEMPORARY TABLE dest (COL INTEGER)")
    rejected("""CREATE LOCAL TEMPORARY TABLE "XT__1234567890" ("COL" INTEGER)""")
  }

  it should "leave an empty lexeme a rejection in every converted position (21.1 AD-5)" in {
    rejected("""CREATE TABLE "" (c INTEGER)""")
    rejected("CREATE TABLE `` (c INTEGER)")
    rejected("""DROP TABLE """"")
    rejected("DROP TABLE ``")
    rejected("""INSERT INTO tbl ("") VALUES ('a')""")
    rejected("""UPDATE tbl SET "" = 1 WHERE id = 1""")
  }

  it should "leave a bare INSERT without a column list rejected for its own reason" in {
    // Not a quoting rejection: the row arity is checked against an empty column list.
    val msg = Parser("INSERT INTO tbl VALUES ('a')").swap.toOption.map(_.msg).getOrElse("")
    msg should include("invalid number of values")
  }

  // ===========================================================================================
  // AC-5 - the bare path is byte-identical, and the ONE narrowing is loud
  // ===========================================================================================

  // 🔴 AD-1. `tableParts` reaches its name through `bareFirstPart`, which carries the reserved-word
  // negative lookahead `identifier` needs; `ident` never had it. Replacing `ident` outright would
  // newly reject ~130 bare names in every DDL/DML position. `identParts` / `identName` keep `ident`
  // as their LAST alternative for exactly this, and these rows are the guard.
  it should "keep a reserved word usable as a bare name, as it has always been" in {
    Seq(
      "DROP TABLE count",
      "DROP TABLE order",
      "TRUNCATE TABLE match",
      "CREATE TABLE dest (count INTEGER, min INTEGER, first INTEGER, last INTEGER)",
      "INSERT INTO tbl (count) VALUES (1)",
      "UPDATE tbl SET count = 1 WHERE id = 1",
      "ALTER TABLE dest DROP COLUMN count",
      "ALTER TABLE dest SET MAPPING order = 1"
    ).foreach { sql =>
      withClue(s"[$sql] ") { Parser(sql).isRight shouldBe true }
    }
    // ... and the quoted spelling is the escape hatch the same names now have everywhere.
    parsed("""DROP TABLE "count"""").asInstanceOf[DropTable].table shouldBe "count"
  }

  it should "leave every bare DML and DDL statement byte-identical" in {
    val bare = Seq(
      "INSERT INTO dest SELECT a FROM src",
      "INSERT INTO tbl (c) VALUES ('a')",
      "UPDATE tbl SET a = 1 WHERE id = 1",
      "DROP TABLE IF EXISTS dest",
      "DROP TABLE dest",
      "TRUNCATE TABLE dest",
      "SHOW TABLE dest",
      "SHOW CREATE TABLE dest",
      "DESCRIBE TABLE dest",
      "COPY INTO dest FROM 's3://b/f.json' FILE_FORMAT = JSON",
      "CREATE TABLE dest (c INTEGER, d KEYWORD) OPTIONS (number_of_shards = 1)",
      "CREATE TABLE dest (c INTEGER, PRIMARY KEY (c))",
      "CREATE TABLE dest AS SELECT a FROM src",
      "ALTER TABLE dest ADD COLUMN c INTEGER",
      "ALTER TABLE dest ALTER COLUMN c SET NOT NULL",
      "DROP MATERIALIZED VIEW mv1",
      "CREATE MATERIALIZED VIEW mv1 AS SELECT a FROM src",
      "DROP PIPELINE p1",
      "DROP WATCHER w1",
      "DROP ENRICH POLICY pol",
      "EXECUTE ENRICH POLICY pol"
    )
    bare.foreach { sql =>
      withClue(s"[$sql] ") {
        val st = parsed(sql)
        // `parts` stays EMPTY for a single bare part, so the AST is what it was before 21.7 and a
        // programmatically built node still compares equal to a parsed one (TableDiff, IndicesApi,
        // AlterTableRoundTripSpec all rely on that).
        partsOf(st) shouldBe Nil
        Parser(st.sql) shouldBe Right(st)
      }
    }
  }

  // The one shape `ident` used to swallow whole that `identParts` does not: `|` commits to the
  // first SUCCEEDING alternative, and `tableParts` succeeds on the leading `a`, leaving the rest
  // unconsumed. A loud rejection of a name nobody meant, never a silent reading change.
  it should "reject a malformed dotted name instead of inventing one" in {
    rejected("DROP TABLE a..b")
    rejected("DROP TABLE a.")
    rejected("SHOW TABLE a.")
  }

  // A watcher's name, its chain-input names and its action names became quotable here, and their
  // render belongs to the `Watcher` MODEL rather than to the statement — so they take the same
  // shape-based re-quote every collection-held name takes. Without it, the row below renders
  // `CREATE OR REPLACE WATCHER my watch AS ...`, which does not re-parse.
  it should "keep a quoted watcher, input and action name re-parseable" in {
    val quoted =
      """CREATE OR REPLACE WATCHER "my watch" AS EVERY 5 MINUTES """ +
      """WITH INPUTS "my input" AS FROM t WITHIN 2 MINUTES ALWAYS """ +
      """DO "my action" AS LOG 'x' END"""
    val st = parsed(quoted)
    st.sql should include("\"my watch\"")
    st.sql should include("\"my input\"")
    st.sql should include("\"my action\"")
    Parser(st.sql).isRight shouldBe true
    // ... while a bare-spellable watcher renders exactly as it always did.
    val bare =
      "CREATE OR REPLACE WATCHER my_watch AS EVERY 5 MINUTES " +
      "WITH INPUTS my_input AS FROM t WITHIN 2 MINUTES ALWAYS " +
      "DO my_action AS LOG 'x' END"
    val bareSt = parsed(bare)
    bareSt.sql should include(" my_watch ")
    Parser(bareSt.sql) shouldBe Right(bareSt)
  }

  // ===========================================================================================
  // Task 4.1 - the DDL examples `documentation/sql/joins.md` publishes must now be TRUE
  // ===========================================================================================

  it should "parse the qualified DDL examples documentation/sql/joins.md publishes" in {
    val insert = parsed("INSERT INTO `prod_eu`.dest SELECT o.id FROM orders o").asInstanceOf[Insert]
    insert.table shouldBe "dest"
    insert.parts shouldBe Seq(q("prod_eu"), b("dest"))
    val create =
      parsed("CREATE TABLE `prod_eu`.dest AS SELECT o.id FROM orders o").asInstanceOf[CreateTable]
    create.table shouldBe "dest"
    create.parts shouldBe Seq(q("prod_eu"), b("dest"))
  }
}
