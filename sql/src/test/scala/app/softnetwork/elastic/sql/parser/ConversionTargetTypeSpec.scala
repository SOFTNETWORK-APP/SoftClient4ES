package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.Identifier
import app.softnetwork.elastic.sql.function.convert.Conversion
import app.softnetwork.elastic.sql.query.{CreateTable, SingleSearch, Statement}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypeUtils, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Issue #275: the CAST/CONVERT TARGET-TYPE surface. `SIGNED`/`UNSIGNED` (Tableau's MySQL dialect),
  * `DECIMAL(p,s)`, `NUMERIC`, `CHAR(n)`, `TEXT`, `KEYWORD` and `CONVERT(x USING cs)` were all
  * rejected by the type regex — the operand was never the gap (#267, closed as not-reproducible).
  *
  * Every alias maps onto an EXISTING `SQLType`, so the render normalises to a name the grammar
  * already accepts and the AST fixed point holds. That is also how the discarded precision is made
  * VISIBLE: `DECIMAL(10,2)` re-renders `DOUBLE`. 🔴 The round-trip block below is what makes that
  * honest — if a normalised render ever stopped re-parsing to an equal AST, AD-5 would collapse and
  * the type would have to be rejected instead. It is a tripwire, not a formality.
  *
  * Parameter acceptance follows a RULE, not a list (lead ruling 2026-09-07, OQ-7): every type SQL
  * gives a length, precision or scale accepts `(p[,s])`, and every parameter is parsed and
  * discarded. Both halves are pinned — the accepting rows below and the rejecting rows at the end.
  *
  * Discipline: Painless is asserted DIRECTLY over literal operands (never via `.sql` alone); over a
  * SCHEMA-LESS identifier operand only the chain and the target type are asserted, because
  * `SQLTypeUtils.coerce` has no `(Any, _)` arm and the Painless there is the raw doc value —
  * asserting a conversion string would be asserting a fiction. The schema-CARRYING half is
  * `CastConversionSpec` (part C).
  */
class ConversionTargetTypeSpec extends AnyFlatSpec with Matchers {

  private def statementOf(sql: String): Statement =
    Parser(sql) match {
      case Right(s)  => s
      case Left(err) => fail(s"[$sql] rejected: ${err.msg}")
    }

  private def identifierOf(sql: String): Identifier =
    Parser(sql) match {
      case Right(s: SingleSearch) => s.select.fields.head.identifier
      case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  private def targetOf(sql: String): SQLType =
    identifierOf(sql).functions
      .collectFirst { case c: Conversion => c.targetType }
      .getOrElse(fail(s"[$sql] no Conversion in the chain"))

  private def chainOf(sql: String): List[String] =
    identifierOf(sql).functions.map(_.getClass.getSimpleName)

  /** A rejection that is a real grammar rejection, not story 21.4's `NonFatal` boundary catch —
    * without which a crashing parser still returns a `Left` and every rejection row below would be
    * unfalsifiable.
    */
  private def rejects(sql: String): Unit = {
    withClue(s"[$sql] ") {
      noException should be thrownBy Parser(sql)
      Parser(sql) match {
        case Left(err) => err.msg should not startWith Parser.InternalParseFailure
        case Right(s)  => fail(s"expected a rejection, parsed as [${s.sql}]")
      }
    }
    ()
  }

  // -- A - the decision table ------------------------------------------------
  "the target-type surface" should "map every accepted alias onto the decided SQLType" in {
    val rows: Seq[(String, SQLType, String)] = Seq(
      ("SELECT CAST(amount AS SIGNED) FROM t", SQLTypes.BigInt, "CAST(amount AS BIGINT)"),
      ("SELECT CAST(amount AS UNSIGNED) FROM t", SQLTypes.BigInt, "CAST(amount AS BIGINT)"),
      ("SELECT CAST(amount AS SIGNED INTEGER) FROM t", SQLTypes.BigInt, "CAST(amount AS BIGINT)"),
      ("SELECT CAST(amount AS UNSIGNED INT) FROM t", SQLTypes.BigInt, "CAST(amount AS BIGINT)"),
      ("SELECT CAST(amount AS DECIMAL) FROM t", SQLTypes.Double, "CAST(amount AS DOUBLE)"),
      ("SELECT CAST(amount AS DECIMAL(10,2)) FROM t", SQLTypes.Double, "CAST(amount AS DOUBLE)"),
      ("SELECT CAST(amount AS DECIMAL(10)) FROM t", SQLTypes.Double, "CAST(amount AS DOUBLE)"),
      ("SELECT CAST(amount AS NUMERIC) FROM t", SQLTypes.Double, "CAST(amount AS DOUBLE)"),
      ("SELECT CAST(amount AS NUMERIC(10,2)) FROM t", SQLTypes.Double, "CAST(amount AS DOUBLE)"),
      ("SELECT CAST(amount AS DEC) FROM t", SQLTypes.Double, "CAST(amount AS DOUBLE)"),
      ("SELECT CAST(name AS CHAR(10)) FROM t", SQLTypes.Char, "CAST(name AS CHAR)"),
      ("SELECT CAST(name AS VARCHAR(255)) FROM t", SQLTypes.Varchar, "CAST(name AS VARCHAR)"),
      ("SELECT CAST(name AS TEXT) FROM t", SQLTypes.Text, "CAST(name AS TEXT)"),
      ("SELECT CAST(name AS KEYWORD) FROM t", SQLTypes.Keyword, "CAST(name AS KEYWORD)"),
      ("SELECT CONVERT(name USING utf8) FROM t", SQLTypes.Varchar, "CONVERT(name, VARCHAR)")
    )
    rows.foreach { case (sql, expected, renderFragment) =>
      withClue(s"[$sql] ") {
        targetOf(sql) shouldBe expected
        statementOf(sql).sql should include(renderFragment)
      }
    }
  }

  // -- A2 - OQ-7: the UNIFORM parameter rule ---------------------------------
  // "Every type SQL allows to parameterise accepts a parameter list, and every parameter is parsed
  // and discarded." Stated as a rule so it cannot drift back into a list of three shapes.
  it should "accept a discarded parameter list on EVERY parameterisable type" in {
    val rows: Seq[(String, SQLType, String)] = Seq(
      ("SELECT CAST(x AS INT(11)) FROM t", SQLTypes.Int, "CAST(x AS INT)"),
      ("SELECT CAST(x AS INTEGER(11)) FROM t", SQLTypes.Int, "CAST(x AS INT)"),
      ("SELECT CAST(x AS BIGINT(20)) FROM t", SQLTypes.BigInt, "CAST(x AS BIGINT)"),
      ("SELECT CAST(x AS LONG(20)) FROM t", SQLTypes.BigInt, "CAST(x AS BIGINT)"),
      ("SELECT CAST(x AS SMALLINT(6)) FROM t", SQLTypes.SmallInt, "CAST(x AS SMALLINT)"),
      ("SELECT CAST(x AS TINYINT(4)) FROM t", SQLTypes.TinyInt, "CAST(x AS TINYINT)"),
      // FLOAT and REAL are the same SQLType and it renders REAL — a pre-existing normalisation,
      // and one the grammar reads back (`float_type` accepts both spellings).
      ("SELECT CAST(x AS FLOAT(53)) FROM t", SQLTypes.Real, "CAST(x AS REAL)"),
      ("SELECT CAST(x AS REAL(10,2)) FROM t", SQLTypes.Real, "CAST(x AS REAL)"),
      ("SELECT CAST(x AS DOUBLE(10,2)) FROM t", SQLTypes.Double, "CAST(x AS DOUBLE)"),
      ("SELECT CAST(x AS TIMESTAMP(3)) FROM t", SQLTypes.Timestamp, "CAST(x AS TIMESTAMP)"),
      ("SELECT CAST(x AS TIME(6)) FROM t", SQLTypes.Time, "CAST(x AS TIME)"),
      ("SELECT CAST(x AS DATETIME(3)) FROM t", SQLTypes.DateTime, "CAST(x AS DATETIME)"),
      // BINARY and VARBINARY are likewise one SQLType, rendering VARBINARY.
      ("SELECT CAST(x AS BINARY(16)) FROM t", SQLTypes.VarBinary, "CAST(x AS VARBINARY)"),
      ("SELECT CAST(x AS VARBINARY(255)) FROM t", SQLTypes.VarBinary, "CAST(x AS VARBINARY)"),
      ("SELECT CAST(x AS TEXT(255)) FROM t", SQLTypes.Text, "CAST(x AS TEXT)")
    )
    rows.foreach { case (sql, expected, renderFragment) =>
      withClue(s"[$sql] ") {
        targetOf(sql) shouldBe expected
        statementOf(sql).sql should include(renderFragment)
      }
    }
  }

  it should "reject a parameter list on a type SQL does not parameterise" in {
    // The other half of the rule. `DATE`, `BOOLEAN`, `KEYWORD`, `STRUCT` and the MySQL cast targets
    // `SIGNED`/`UNSIGNED` carry no length, precision or scale in any dialect; `ARRAY` carries its
    // own `<...>`. Accepting `(n)` there would be inventing syntax, not admitting a dialect.
    Seq(
      "SELECT CAST(x AS DATE(3)) FROM t",
      "SELECT CAST(x AS BOOLEAN(1)) FROM t",
      "SELECT CAST(x AS KEYWORD(255)) FROM t",
      "SELECT CAST(x AS SIGNED(11)) FROM t",
      "SELECT CAST(x AS UNSIGNED(11)) FROM t"
    ).foreach(rejects)
  }

  it should "reject a malformed parameter list on a type that IS parameterisable" in {
    // I14, made mandatory by the uniform rule: widening the rule multiplies the over-permissive
    // surface, and the parameters are discarded anyway so strictness costs nothing behaviourally.
    // SQL has at most `(p,s)` and no negative precision.
    Seq(
      "SELECT CAST(x AS DECIMAL(10,2,3)) FROM t",
      "SELECT CAST(x AS CHAR(-1)) FROM t",
      "SELECT CAST(x AS VARCHAR(-255)) FROM t",
      "SELECT CAST(x AS CHAR()) FROM t",
      "SELECT CAST(x AS DECIMAL(10,)) FROM t",
      "SELECT CAST(x AS CHAR(1.5)) FROM t",
      "SELECT CAST(x AS CHAR(n)) FROM t"
    ).foreach(rejects)
  }

  it should "round-trip every alias to an EQUAL AST (the render normalises, so the fixed point holds)" in {
    Seq(
      "SELECT CAST(amount AS SIGNED) FROM t",
      "SELECT CAST(amount AS UNSIGNED) FROM t",
      "SELECT CAST(amount AS DECIMAL(10,2)) FROM t",
      "SELECT CAST(amount AS NUMERIC) FROM t",
      "SELECT CAST(name AS CHAR(10)) FROM t",
      "SELECT CAST(name AS VARCHAR(255)) FROM t",
      "SELECT CAST(name AS TEXT) FROM t",
      "SELECT CAST(name AS KEYWORD) FROM t",
      "SELECT CONVERT(name USING utf8) FROM t",
      "SELECT TRY_CAST(name AS DECIMAL(10,2)) FROM t",
      "SELECT CAST(x AS INT(11)) FROM t",
      "SELECT CAST(x AS TIMESTAMP(3)) FROM t",
      "SELECT CAST(x AS BINARY(16)) FROM t"
    ).foreach { sql =>
      val stmt = statementOf(sql)
      withClue(s"[$sql] rendered as [${stmt.sql}] ") { Parser(stmt.sql) shouldBe Right(stmt) }
    }
  }

  // -- B - Painless, asserted directly, over LITERAL operands ----------------
  it should "emit the real conversion in Painless when the operand's type is known" in {
    identifierOf("SELECT CAST('12' AS SIGNED) FROM t").painless(None) shouldBe
    """Long.parseLong("12").longValue()"""
    identifierOf("SELECT CAST('1.5' AS DECIMAL(10,2)) FROM t").painless(None) shouldBe
    """Double.parseDouble("1.5").doubleValue()"""
    identifierOf("SELECT CAST('1.5' AS NUMERIC) FROM t").painless(None) shouldBe
    """Double.parseDouble("1.5").doubleValue()"""
    // The CHAR silent no-op this story fixes: it used to emit the bare `1`.
    identifierOf("SELECT CAST(1 AS CHAR) FROM t").painless(None) shouldBe "String.valueOf(1)"
    identifierOf("SELECT CAST(1 AS TEXT) FROM t").painless(None) shouldBe "String.valueOf(1)"
    identifierOf("SELECT CAST(1 AS KEYWORD) FROM t").painless(None) shouldBe "String.valueOf(1)"
    // TRY_CAST keeps its one-branch safe wrapper.
    identifierOf("SELECT TRY_CAST('12' AS SIGNED) FROM t").painless(None) should startWith("try {")
  }

  it should "assert the CHAIN, not the Painless, when the operand type is unknown" in {
    // Schema-less, `coerce` has no (Any, BigInt) arm, so an identifier operand's Painless is the
    // raw doc value — identical to what CAST(x AS BIGINT) has always produced. Asserting a
    // conversion string here would be asserting a fiction; assert what the AST carries instead.
    chainOf("SELECT CAST(amount AS SIGNED) FROM t") shouldBe List("Cast")
    targetOf("SELECT CAST(amount AS SIGNED) FROM t") shouldBe SQLTypes.BigInt
  }

  // -- C - nesting / paren balance (#219/#220 must stay closed) ---------------
  it should "survive nesting inside another function and inside SCRIPT AS" in {
    val nested = statementOf("SELECT CONCAT(CAST(name AS CHAR(10)), 'x') FROM t")
    Parser(nested.sql) shouldBe Right(nested)
    identifierOf("SELECT CONCAT(CAST('1' AS CHAR(10)), 'x') FROM t")
      .painless(None) should include("String.valueOf")
    val script = statementOf("CREATE TABLE t (c VARCHAR SCRIPT AS (UPPER(CAST(a AS CHAR(3)))))")
    Parser(script.sql) shouldBe Right(script)
  }

  it should "not let a parameterised type swallow a following parenthesised construct" in {
    // The #219/#220 paren-greed shape, which is why `typeParams` is attached to each individual
    // type production and NEVER hoisted onto `sql_type` (AD-6). `opt` recovers a Failure, so when
    // `(` matches but the parameter does not, the position is restored.
    Parser("CREATE TABLE t (c VARCHAR FIELDS (raw KEYWORD))").isRight shouldBe true
    Parser("CREATE TABLE t (c VARCHAR(255) FIELDS (raw KEYWORD))").isRight shouldBe true
    Parser("CREATE TABLE t (c VARCHAR SCRIPT AS (UPPER(a)))").isRight shouldBe true
    Parser("CREATE TABLE t (c VARCHAR(255) SCRIPT AS (UPPER(a)))").isRight shouldBe true
    Parser("SELECT amount::BIGINT FROM t WHERE id IN (1,2)").isRight shouldBe true
    Parser("SELECT (amount::INT) + 1 FROM t").isRight shouldBe true
  }

  // -- D - no regression ------------------------------------------------------
  it should "leave every shape that parsed before unchanged" in {
    Seq(
      "SELECT CAST(x AS DATETIME) FROM t",
      "SELECT CAST(x AS TIMESTAMP) FROM t",
      "SELECT CAST(x AS DATE) FROM t",
      "SELECT CAST(x AS TIME) FROM t",
      "SELECT CAST(x AS VARCHAR) FROM t",
      "SELECT CAST(amount AS ARRAY<INT>) FROM t",
      "SELECT CONVERT(VARCHAR, name) FROM t",
      "SELECT CONVERT(name, VARCHAR) FROM t",
      "SELECT amount::BIGINT FROM t",
      "SELECT CONCAT(a::CHAR, 'x') FROM t"
    ).foreach { sql =>
      val stmt = statementOf(sql)
      withClue(s"[$sql] ") { Parser(stmt.sql) shouldBe Right(stmt) }
    }
  }

  it should "not turn the new type words into reserved words" in {
    identifierOf("SELECT signed FROM t").name shouldBe "signed"
    identifierOf("SELECT unsigned FROM t").name shouldBe "unsigned"
    identifierOf("SELECT decimal FROM t").name shouldBe "decimal"
    identifierOf("SELECT numeric FROM t").name shouldBe "numeric"
    // `dec` is the riskiest of the five — the shortest word, and the one whose regex carries the
    // word boundary that keeps it from eating a prefix.
    identifierOf("SELECT dec FROM t").name shouldBe "dec"
  }

  it should "accept the new parameterised forms in DDL too, rendering the type actually applied" in {
    statementOf("CREATE TABLE t (c VARCHAR(255))").sql should include("c VARCHAR")
    statementOf("CREATE TABLE t (c DECIMAL(10,2))").sql should include("c DOUBLE")
    statementOf("CREATE TABLE t (c BIGINT(20))").sql should include("c BIGINT")
    statementOf("ALTER TABLE t ALTER COLUMN c SET DATA TYPE VARCHAR(64)").sql should
    include("SET DATA TYPE VARCHAR")
    // Unparameterised DDL is untouched.
    Parser("CREATE TABLE t (c STRUCT)").isRight shouldBe true
    Parser("CREATE TABLE t (c VARCHAR FIELDS (raw KEYWORD))").isRight shouldBe true
  }

  // -- D2 - the DDL half of the CHAR fix (AD-9) -------------------------------
  it should "map a CHAR column to an Elasticsearch text field, not an object field" in {
    // `elasticType` had arms for Varchar|Text and for Keyword and NONE for Char, so `Char` fell to
    // `case _ => "object"` and a CHAR column was created as an OBJECT field. Nearly unreachable
    // while bare `CHAR` was the only spelling; this story accepts `CHAR(n)`, which is the spelling
    // everybody writes.
    SQLTypeUtils.elasticType(SQLTypes.Char) shouldBe "text"
    SQLTypeUtils.elasticType(SQLTypes.Array(SQLTypes.Char)) shouldBe "text"

    // The unit assertion alone would stay green if the DDL path ever stopped going through
    // `elasticType`, so assert the mapping node the CREATE TABLE actually produces.
    Parser("CREATE TABLE t (c CHAR(1))") match {
      case Right(ct: CreateTable) =>
        val column = ct.ddl match {
          case Right(cols) => cols.head
          case other       => fail(s"expected column DDL, got $other")
        }
        column.node.toString should include(""""type":"text"""")
      case other => fail(s"unexpected $other")
    }

    // A text-mapped column takes no `null_value` — Elasticsearch rejects it on a `text` field — so
    // CHAR must inherit that rule too now that it maps to text.
    Parser("CREATE TABLE t (c CHAR(1) DEFAULT 'x')") match {
      case Right(ct: CreateTable) =>
        val column = ct.ddl match {
          case Right(cols) => cols.head
          case other       => fail(s"expected column DDL, got $other")
        }
        column.node.toString should not include "null_value"
      case other => fail(s"unexpected $other")
    }
  }

  it should "still reject the target types this story deliberately did not add" in {
    Seq(
      "SELECT CAST(x AS BLOB) FROM t",
      "SELECT CAST(x AS CLOB) FROM t",
      "SELECT CAST(x AS BIT) FROM t",
      "SELECT CAST(x AS MONEY) FROM t",
      "SELECT CAST(x AS UUID) FROM t",
      "SELECT CAST(x AS INTERVAL) FROM t"
    ).foreach(rejects)
  }

  // -- E - the operand regression matrix that never existed (T11) -------------
  // #267 alleged these were all broken. They were not, and NOTHING at sql-module level protected
  // them — the only coverage was two Docker-bound testkit specs asserting `res.isSuccess`. This
  // block is that guard.
  "a bare literal operand" should "keep working in every conversion spelling" in {
    val rows: Seq[(String, List[String], String)] = Seq(
      (
        "SELECT CAST('125' AS BIGINT) AS c FROM t",
        List("Cast", "StringValue"),
        """Long.parseLong("125").longValue()"""
      ),
      (
        "SELECT CAST('125' BIGINT) AS c FROM t",
        List("Cast", "StringValue"),
        """Long.parseLong("125").longValue()"""
      ),
      (
        "SELECT CONVERT('125', BIGINT) AS c FROM t",
        List("Convert", "StringValue"),
        """Long.parseLong("125").longValue()"""
      ),
      (
        "SELECT CONVERT(BIGINT, '125') AS c FROM t",
        List("Convert", "StringValue"),
        """Long.parseLong("125").longValue()"""
      ),
      (
        "SELECT '125'::BIGINT AS c FROM t",
        List("CastOperator", "StringValue"),
        """Long.parseLong("125").longValue()"""
      ),
      (
        "SELECT CAST('12.5' AS DOUBLE) AS c FROM t",
        List("Cast", "StringValue"),
        """Double.parseDouble("12.5").doubleValue()"""
      )
    )
    rows.foreach { case (sql, chain, painless) =>
      withClue(s"[$sql] ") {
        chainOf(sql) shouldBe chain
        identifierOf(sql).painless(None) shouldBe painless
      }
    }
    identifierOf("SELECT TRY_CAST('123' AS INT) AS c FROM t").painless(None) shouldBe
    """try { Integer.parseInt("123").intValue() } catch (Exception e) { return null; }"""
  }

  it should "keep the interval operand working - the shape that forbids widening the operand list" in {
    // Inserting `identifierWithValue` BEFORE `identifierWithIntervalFunction` in the four
    // conversion operand lists breaks exactly this: `|` commits to the first SUCCEEDING
    // alternative and the later `~` failure never backtracks. Measured; do not "clean up" there.
    val sql = "SELECT CAST('2025-01-01' + INTERVAL 1 DAY AS DATE) AS c FROM t"
    chainOf(sql) shouldBe List("Cast", "SQLAddInterval", "StringValue")
    identifierOf(sql).painless(None) shouldBe
    """LocalDate.parse("2025-01-01", DateTimeFormatter.ofPattern("yyyy-MM-dd")).plus(1, ChronoUnit.DAYS)"""
  }
}
