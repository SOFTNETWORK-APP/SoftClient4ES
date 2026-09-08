package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.sql.StringValue
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{
  AlterTable,
  AlterTableMapping,
  AlterTableStatement,
  CreateTable,
  DropTableMapping
}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The per-index schema-cache TTL (story 21.8 Part D).
  *
  * Two spellings write ONE thing: the dedicated `SET SCHEMA CACHE TTL` is sugar that desugars to
  * the `_meta` write `SET MAPPING` performs, so there is a single AST, a single diff and a single
  * read. These tests pin that identity — sugar that produced a second representation would be a
  * second place for the value to be missing.
  */
class SchemaCacheTtlSpec extends AnyFlatSpec with Matchers {

  private def statementsOf(sql: String): Seq[AlterTableStatement] =
    Parser(sql) match {
      case Right(AlterTable(_, _, statements, _)) => statements
      case other                                  => fail(s"Expected an AlterTable, got $other")
    }

  private val users: Table =
    Table(
      name = "users",
      columns = List(Column("id", SQLTypes.Int), Column("name", SQLTypes.Varchar)),
      primaryKey = List("id")
    ).update()

  "parse" should "accept the HOCON duration spellings the configuration default accepts" in {
    SchemaCacheTtl.parse("10m") shouldBe Right(600000L)
    SchemaCacheTtl.parse("30s") shouldBe Right(30000L)
    SchemaCacheTtl.parse("1h") shouldBe Right(3600000L)
    SchemaCacheTtl.parse("500 milliseconds") shouldBe Right(500L)
    SchemaCacheTtl.parse("750") shouldBe Right(750L) // bare number = milliseconds
    SchemaCacheTtl.parse("  10m  ") shouldBe Right(600000L)
  }

  it should "reject what is not a positive duration, naming the accepted forms" in {
    SchemaCacheTtl.parse("soon").left.map(_.contains("not a duration")) shouldBe Left(true)
    SchemaCacheTtl.parse("").left.map(_.contains("cannot be empty")) shouldBe Left(true)
    SchemaCacheTtl.parse("0s").left.map(_.contains("must be positive")) shouldBe Left(true)
    SchemaCacheTtl.parse("-5m").left.map(_.contains("must be positive")) shouldBe Left(true)
  }

  "SET SCHEMA CACHE TTL" should "desugar to the same _meta write SET MAPPING performs" in {
    val sugar = statementsOf("ALTER TABLE users SET SCHEMA CACHE TTL = '10m'")
    val explicit =
      statementsOf("ALTER TABLE users SET MAPPING _meta.schema_cache_ttl = '10m'")
    sugar shouldBe explicit
    sugar shouldBe Seq(AlterTableMapping(SchemaCacheTtl.MetadataPath, StringValue("10m")))
  }

  it should "accept the equals sign as optional" in {
    statementsOf("ALTER TABLE users SET SCHEMA CACHE TTL '10m'") shouldBe
    statementsOf("ALTER TABLE users SET SCHEMA CACHE TTL = '10m'")
  }

  it should "be case-insensitive like every other statement keyword" in {
    statementsOf("alter table users set schema cache ttl = '10m'") shouldBe
    Seq(AlterTableMapping(SchemaCacheTtl.MetadataPath, StringValue("10m")))
  }

  it should "re-parse from its own render" in {
    // The render is the `SET MAPPING` form (that IS the AST); what matters is that running the
    // rendered DDL back through the parser yields the same statement — MV deployment renders a
    // schema to DDL and executes the text.
    val stmts = statementsOf("ALTER TABLE users SET SCHEMA CACHE TTL = '10m'")
    val rendered = s"ALTER TABLE users ${stmts.map(_.sql).mkString}"
    statementsOf(rendered) shouldBe stmts
  }

  it should "reject a TTL that is not a duration, at parse time" in {
    val rejection = Parser("ALTER TABLE users SET SCHEMA CACHE TTL = 'soon'")
    rejection.isLeft shouldBe true
    val reason = rejection.swap.map(_.msg).getOrElse("")
    reason should include("SET SCHEMA CACHE TTL")
    reason should include("not a duration")
    // 21.4: a rejection assertion is unfalsifiable unless it also excludes the boundary catch.
    reason should not startWith Parser.InternalParseFailure
  }

  it should "reject a non-positive TTL at parse time" in {
    val rejection = Parser("ALTER TABLE users SET SCHEMA CACHE TTL = '0s'")
    rejection.isLeft shouldBe true
    val reason = rejection.swap.map(_.msg).getOrElse("")
    reason should include("must be positive")
    reason should not startWith Parser.InternalParseFailure
  }

  "DROP SCHEMA CACHE TTL" should "desugar to the matching _meta removal" in {
    statementsOf("ALTER TABLE users DROP SCHEMA CACHE TTL") shouldBe
    statementsOf("ALTER TABLE users DROP MAPPING _meta.schema_cache_ttl")
  }

  "merge" should "store the TTL where SchemaCacheTtl.of reads it, and survive update()" in {
    val merged = users.merge(statementsOf("ALTER TABLE users SET SCHEMA CACHE TTL = '10m'"))
    SchemaCacheTtl.of(merged) shouldBe Some(Right(600000L))
    // `Table.update()` runs on every schema and REBUILDS the five `_meta` keys it owns; a key it
    // does not own must survive, or the TTL would vanish on the first round trip.
    SchemaCacheTtl.of(merged.update()) shouldBe Some(Right(600000L))
    users.diff(merged).mappings should not be empty
  }

  it should "remove it on DROP" in {
    val withTtl = users.merge(statementsOf("ALTER TABLE users SET SCHEMA CACHE TTL = '10m'"))
    val dropped = withTtl.merge(statementsOf("ALTER TABLE users DROP SCHEMA CACHE TTL"))
    SchemaCacheTtl.of(dropped) shouldBe None
    withTtl.diff(dropped).mappings should not be empty
  }

  it should "report a hand-written TTL that is not a duration rather than throw" in {
    // `SET MAPPING` and a hand-edited `_meta` bypass the sugar's parse-time validation.
    val merged =
      users.merge(statementsOf("ALTER TABLE users SET MAPPING _meta.schema_cache_ttl = 'soon'"))
    SchemaCacheTtl.of(merged).map(_.isLeft) shouldBe Some(true)
  }

  "a table declaring no TTL" should "read as None, not as a default" in {
    SchemaCacheTtl.of(users) shouldBe None
  }

  // The three spellings documented in `documentation/sql/ddl_statements.md` are asserted to be
  // equivalent, not merely to parse: a doc example that parses can still mean something else.
  "CREATE TABLE ... OPTIONS" should "be able to declare the TTL, identically" in {
    val created = Parser(
      """CREATE TABLE IF NOT EXISTS orders (
        |  id INT NOT NULL
        |) OPTIONS (mappings = (_meta = (schema_cache_ttl = '1h')))""".stripMargin
    ) match {
      case Right(create: CreateTable) => create.schema
      case other                      => fail(s"Expected a CreateTable, got $other")
    }
    SchemaCacheTtl.of(created) shouldBe Some(Right(3600000L))
    // …and identical to what the ALTER sugar writes on the same table.
    val altered = created
      .copy(mappings = created.mappings - "_meta")
      .merge(statementsOf("ALTER TABLE orders SET SCHEMA CACHE TTL = '1h'"))
    SchemaCacheTtl.of(altered) shouldBe SchemaCacheTtl.of(created)
  }

  "the documented examples" should "parse and mean what the documentation says" in {
    statementsOf("ALTER TABLE orders SET SCHEMA CACHE TTL = '1h'") shouldBe
    Seq(AlterTableMapping(SchemaCacheTtl.MetadataPath, StringValue("1h")))
    statementsOf("ALTER TABLE feature_flags SET SCHEMA CACHE TTL = '30s'") shouldBe
    Seq(AlterTableMapping(SchemaCacheTtl.MetadataPath, StringValue("30s")))
    statementsOf("ALTER TABLE orders SET SCHEMA CACHE TTL '1h'") shouldBe
    Seq(AlterTableMapping(SchemaCacheTtl.MetadataPath, StringValue("1h")))
    statementsOf("ALTER TABLE orders DROP SCHEMA CACHE TTL") shouldBe
    Seq(DropTableMapping(SchemaCacheTtl.MetadataPath))
    statementsOf("ALTER TABLE orders SET MAPPING _meta.schema_cache_ttl = '1h'") shouldBe
    Seq(AlterTableMapping(SchemaCacheTtl.MetadataPath, StringValue("1h")))
    // The single-line OPTIONS spelling published on the documentation site, and the
    // semicolon-terminated forms every site example uses.
    Parser(
      "CREATE TABLE orders (id INT NOT NULL) OPTIONS (mappings = (_meta = (schema_cache_ttl = '1h')));"
    ).isRight shouldBe true
    Parser("ALTER TABLE orders SET SCHEMA CACHE TTL = '1h';").isRight shouldBe true
    Parser("ALTER TABLE orders DROP SCHEMA CACHE TTL;").isRight shouldBe true
  }

  // The three new keywords (SCHEMA, CACHE, TTL) are matched only after `SET`/`DROP` at the start of
  // an ALTER TABLE statement. They must not become reserved words anywhere else — adding a keyword
  // that swallows an identifier is how `COUNT(*) AS count` became a parse error (#302).
  "the new keywords" should "not be reserved as identifiers elsewhere" in {
    statementsOf("ALTER TABLE users SET MAPPING schema = 'analytics'") shouldBe
    Seq(AlterTableMapping("schema", StringValue("analytics")))
    statementsOf("ALTER TABLE users SET SETTING cache = 'x'") should have size 1
    statementsOf("ALTER TABLE users ADD COLUMN cache INT") should have size 1
    statementsOf("ALTER TABLE users ADD COLUMN ttl INT") should have size 1
    Parser("SELECT schema FROM users").isRight shouldBe true
  }

  it should "reject an unquoted duration rather than read it as something else" in {
    val rejection = Parser("ALTER TABLE users SET SCHEMA CACHE TTL = 10m")
    rejection.isLeft shouldBe true
    rejection.swap.map(_.msg).getOrElse("") should not startWith Parser.InternalParseFailure
  }
}
