package app.softnetwork.elastic.sql.macros

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SQLQueryValidatorSpec extends AnyFlatSpec with Matchers {

  // ============================================================
  // Story 22.1 — relational-closure statements cannot be typed at compile time
  // ============================================================

  // A derived table is a `SingleSearch`, NOT a new statement KIND, so story 20.9's
  // `case Right(other)` abort does NOT catch it: without the new guarded arm the macro would
  // ACCEPT this and type `Row` against nothing.
  it should "REJECT a derived table at compile time (one index mapping cannot type it)" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.query.SelectStatement

      case class Row(COL: Int)

      TestElasticClientApi.searchAs[Row](
        "SELECT COL FROM (SELECT 1 AS COL) AS d"
      )""")
  }

  // ⚠️ BEHAVIOUR CHANGE, release-noted: this used to COMPILE and then run the FIRST index alone
  // (the #157 silent-wrong-answer mode). It is a compile error now.
  it should "REJECT a cross-index JOIN at compile time (this used to compile and run one table)" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.query.SelectStatement

      case class Row(id: Int, name: String)

      TestElasticClientApi.searchAs[Row](
        "SELECT o.id, c.name FROM orders o JOIN customers c ON o.cid = c.id"
      )""")
  }

  // Story 22.5 — a CTE statement is also a `SingleSearch`, so it reaches the SAME guarded arm with
  // no new macro branch.
  //
  // 🔴 The SQL is an UNREFERENCED CTE, and that choice is the whole test. A statement that
  // REFERENCES its CTE carries a derived table, so story 22.1's `from.relationalClosureRequired`
  // arm already aborts it — MEASURED: with such a statement, deleting `|| ctes.nonEmpty` from
  // `SingleSearch.relationalClosureRequired` left this suite 25/25 GREEN, i.e. the row could not
  // fail for the reason its title named. `WITH u AS (...) SELECT a FROM t` has NO derived table,
  // so only the new disjunct can reject it.
  it should "REJECT a CTE at compile time, even when no CTE is referenced" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.query.SelectStatement

      case class Row(a: Int)

      TestElasticClientApi.searchAs[Row](
        "WITH u AS (SELECT a FROM t) SELECT a FROM t"
      )""")
  }

  // …and the REFERENCED form too, which 22.1's arm would also catch — kept as the neighbour, not
  // as the guard.
  it should "REJECT a referenced CTE at compile time" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.query.SelectStatement

      case class Row(a: Int)

      TestElasticClientApi.searchAs[Row](
        "WITH m AS (SELECT a FROM t) SELECT a FROM m"
      )""")
  }

  // Story 22.6 — a set operation is a `MultiSearch`, so it reaches the GUARDED `MultiSearch` arm
  // (`relationalClosureRequired(multi)`), which now sees the operator list. `UNION ALL` must keep
  // compiling: it is typed from the first leg exactly as before.
  it should "REJECT a UNION / INTERSECT / EXCEPT at compile time" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.query.SelectStatement

      case class Row(a: Int)

      TestElasticClientApi.searchAs[Row](
        "SELECT a FROM x UNION SELECT a FROM y"
      )""")
  }

  it should "REJECT an INTERSECT at compile time" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.query.SelectStatement

      case class Row(a: Int)

      TestElasticClientApi.searchAs[Row](
        "SELECT a FROM x INTERSECT SELECT a FROM y"
      )""")
  }

  /** 🔴 `assertDoesNotCompile` reports only THAT a snippet failed, never WHY — so the two rows
    * above cannot see the shape naming at all: disabling the `ctesPresent` branch in
    * `closureAbortMessage` left them GREEN. The message is asserted DIRECTLY, the way
    * `RelationalClosureGuardSpec` asserts `RelationalClosureGuard.shapeOf` for the runtime guard.
    */
  it should "name the WITH clause in the abort message, not the derived table it becomes" in {
    def parsed(sql: String): app.softnetwork.elastic.sql.query.Statement =
      app.softnetwork.elastic.sql.parser.Parser(sql) match {
        case Right(s) => s
        case Left(e)  => fail(s"[$sql] rejected: ${e.msg}")
      }
    val cte = "WITH m AS (SELECT a FROM t) SELECT a FROM m"
    val msg = SQLQueryValidator.closureAbortMessage(parsed(cte), cte)
    msg should include("WITH clauses (common table expressions)")
    // The falsifiable half: it must NOT fall through to the construct the author never wrote.
    msg should not include "Derived tables"
    // …and the neighbours must keep their own names (the arm did not swallow them).
    val derived = "SELECT COL FROM (SELECT 1 AS COL) AS d"
    SQLQueryValidator.closureAbortMessage(parsed(derived), derived) should include(
      "Derived tables (subqueries in FROM/JOIN)"
    )
    val join = "SELECT o.id, c.name FROM orders o JOIN customers c ON o.cid = c.id"
    SQLQueryValidator.closureAbortMessage(parsed(join), join) should include("Cross-index JOINs")
    // Story 22.6 — the set operation outranks every branch shape, for the same reason.
    val setOp = "SELECT a FROM t UNION SELECT a FROM u"
    val setOpMsg = SQLQueryValidator.closureAbortMessage(parsed(setOp), setOp)
    setOpMsg should include("UNION / UNION DISTINCT / INTERSECT / EXCEPT set operations")
    setOpMsg should not include "Cross-index JOINs"
    // ...and a plain UNION ALL is not a closure shape at all, so it never reaches this message
    app.softnetwork.elastic.sql.query.relationalClosureRequired(
      parsed("SELECT a FROM t UNION ALL SELECT a FROM u")
    ) shouldBe false
  }

  // ============================================================
  // Positive Tests (Should Compile)
  // ============================================================

  // Story 22.2 — an uncorrelated WHERE subquery IS typeable at compile time: the statement is a
  // `SingleSearch` over the OUTER index, which is exactly what the row type binds against, and the
  // inner query is an execution step the macro never has to type. No new macro arm was needed; this
  // row is the guard that none is silently added.
  it should "ACCEPT an uncorrelated WHERE subquery at compile time (story 22.2)" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Strings
      import app.softnetwork.elastic.sql.query.SelectStatement

      TestElasticClientApi.searchAs[Strings](
        "SELECT vchar::VARCHAR, c::CHAR, text FROM strings WHERE c IN (SELECT c FROM others)"
      )""")
  }

  /** Story 22.6 — the CONTROL for the two set-operator rejections above. `UNION ALL` is typed from
    * the first leg, exactly as before this story; without this row a macro arm that rejected every
    * `MultiSearch` would look identical.
    */
  it should "ACCEPT a plain UNION ALL at compile time (typed from the first leg, as before)" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Strings
      import app.softnetwork.elastic.sql.query.SelectStatement

      TestElasticClientApi.searchAs[Strings](
        "SELECT vchar::VARCHAR, c::CHAR, text FROM strings UNION ALL SELECT vchar::VARCHAR, c::CHAR, text FROM strings"
      )""")
  }

  "SQLQueryValidator" should "VALIDATE all numeric types" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Numbers
      import app.softnetwork.elastic.sql.query.SelectStatement

      TestElasticClientApi.searchAs[Numbers](
        "SELECT tiny::TINYINT as tiny, small::SMALLINT as small, normal::INT as normal, big::BIGINT as big, huge::BIGINT as huge, decimal::DOUBLE as decimal, r::REAL as r FROM numbers"
      )""")
  }

  it should "VALIDATE string types" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Strings
      import app.softnetwork.elastic.sql.query.SelectStatement

      TestElasticClientApi.searchAs[Strings](
        "SELECT vchar::VARCHAR, c::CHAR, text FROM strings"
      )""")
  }

  // Story 21.5 (#275) makes TEXT and KEYWORD reachable as DQL cast targets. They must land on the
  // macro's String arm, not in its permissive `case _ => true` fallback — a newly reachable target
  // that nothing validates is a hole this story would otherwise have opened.
  it should "VALIDATE the newly reachable TEXT / KEYWORD cast targets as String" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Strings
      import app.softnetwork.elastic.sql.query.SelectStatement

      TestElasticClientApi.searchAs[Strings](
        "SELECT vchar::TEXT, c::KEYWORD, text FROM strings"
      )""")
  }

  it should "REJECT a TEXT cast bound to a non-String field" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.query.SelectStatement

      case class WrongText(vchar: Int)

      TestElasticClientApi.searchAs[WrongText](
        "SELECT vchar::TEXT FROM strings"
      )""")
  }

  it should "VALIDATE temporal types" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Temporal
      import app.softnetwork.elastic.sql.query.SelectStatement

      TestElasticClientApi.searchAs[Temporal](
        "SELECT d::DATE, t::TIME, dt::DATETIME, ts::TIMESTAMP FROM temporal"
      )""")
  }

  it should "VALIDATE with all fields" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
      import app.softnetwork.elastic.sql.query.SelectStatement

      TestElasticClientApi.searchAs[Product](
        "SELECT id, name, price::DOUBLE, stock::INT, active::BOOLEAN, createdAt::DATETIME FROM products"
      )""")
  }

  it should "VALIDATE with aliases" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
      import app.softnetwork.elastic.sql.query.SelectStatement

      TestElasticClientApi.searchAs[Product](
        "SELECT product_id AS id, product_name AS name, product_price::DOUBLE AS price, product_stock::INT AS stock, is_active::BOOLEAN AS active, created_at::TIMESTAMP AS createdAt FROM products"
      )""")
  }

  it should "ACCEPT query with missing Option fields" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.ProductWithOptional
      import app.softnetwork.elastic.sql.query.SelectStatement

      TestElasticClientApi.searchAs[ProductWithOptional](
        "SELECT id, name FROM products"
      )
    """)
  }

  it should "ACCEPT query with missing fields that have defaults" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.ProductWithDefaults
      import app.softnetwork.elastic.sql.query.SelectStatement

      TestElasticClientApi.searchAs[ProductWithDefaults](
        "SELECT id, name FROM products"
      )
    """)
  }

  it should "ACCEPT SELECT * with Unchecked variant" in {
    assertCompiles("""
    import app.softnetwork.elastic.client.macros.TestElasticClientApi
    import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
    import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
    import app.softnetwork.elastic.sql.query.SelectStatement

    TestElasticClientApi.searchAsUnchecked[Product](
      SelectStatement("SELECT * FROM products")
    )
  """)
  }

  it should "ACCEPT nested object with complete selection" in {
    assertCompiles("""
    import app.softnetwork.elastic.client.macros.TestElasticClientApi
    import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
    import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.{User, Address}

    TestElasticClientApi.searchAs[User](
      "SELECT id, name, address FROM users"
    )""")
  }

  it should "ACCEPT nested object with UNNEST" in {
    assertCompiles("""
    import app.softnetwork.elastic.client.macros.TestElasticClientApi
    import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
    import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.{User, Address}

    TestElasticClientApi.searchAs[User](
      "SELECT id, name, address.street, address.city, address.country FROM users JOIN UNNEST(users.address) AS address"
    )""")
  }

  // ============================================================
  // Negative Tests (Should NOT Compile)
  // ============================================================

  it should "REJECT query with missing required fields" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
      import app.softnetwork.elastic.sql.query.SelectStatement

      TestElasticClientApi.searchAs[Product](
        "SELECT id, name FROM products"
      )""")
  }

  it should "REJECT a FROM-less SELECT (issue #251 — the handshake is not a search)" in {
    // Since story 20.9, `SELECT 1` PARSES (FromlessSelect). searchAs must still refuse it —
    // with a clean c.abort naming the statement kind, never a macro MatchError.
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product

      TestElasticClientApi.searchAs[Product](
        "SELECT 1"
      )""")
  }

  it should "REJECT query with invalid field names" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
      import app.softnetwork.elastic.sql.query.SelectStatement

      TestElasticClientApi.searchAs[Product](
        "SELECT id, invalid_name, price, stock, active, createdAt FROM products"
      )""")
  }

  it should "REJECT query with type mismatches" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.query.SelectStatement

      case class WrongTypes(id: Int, name: Int)

      TestElasticClientApi.searchAs[WrongTypes](
        "SELECT id::BIGINT, name FROM products"
      )""")
  }

  it should "SUGGEST closest field names" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
      import app.softnetwork.elastic.sql.query.SelectStatement

      TestElasticClientApi.searchAs[Product](
        "SELECT id, nam, price, stock, active, createdAt FROM products"
      )""")
  }

  it should "REJECT dynamic queries (non-literals)" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
      import app.softnetwork.elastic.sql.query.SelectStatement

      val dynamicField = "name"
      TestElasticClientApi.searchAs[Product](
        s"SELECT id, $dynamicField FROM products"
      )""")
  }

  it should "REJECT SELECT * queries" in {
    assertDoesNotCompile("""
    import app.softnetwork.elastic.client.macros.TestElasticClientApi
    import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
    import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
    import app.softnetwork.elastic.sql.query.SelectStatement

    TestElasticClientApi.searchAs[Product](
      "SELECT * FROM products"
    )
  """)
  }

  it should "REJECT SELECT * even with WHERE clause" in {
    assertDoesNotCompile("""
    import app.softnetwork.elastic.client.macros.TestElasticClientApi
    import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
    import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
    import app.softnetwork.elastic.sql.query.SelectStatement

    TestElasticClientApi.searchAs[Product](
      "SELECT * FROM products WHERE active = true"
    )
  """)
  }

  it should "REJECT nested object with individual field selection without UNNEST" in {
    assertDoesNotCompile("""
    import app.softnetwork.elastic.client.macros.TestElasticClientApi
    import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
    import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.{User, Address}

    TestElasticClientApi.searchAs[User](
      "SELECT id, name, address.street, address.city, address.country FROM users"
    )""")
  }

}

object SQLQueryValidatorSpec {
  case class Product(
    id: String,
    name: String,
    price: Double,
    stock: Int,
    active: Boolean,
    createdAt: java.time.LocalDateTime
  )

  case class ProductWithOptional(
    id: String,
    name: String,
    price: Option[Double], // ✅ OK if missing
    stock: Option[Int] // ✅ OK if missing
  )

  // Case class with default values
  case class ProductWithDefaults(
    id: String,
    name: String,
    price: Double = 0.0, // ✅ OK if missing
    stock: Int = 0 // ✅ OK if missing
  )

  case class Numbers(
    tiny: Byte,
    small: Short,
    normal: Int,
    big: Long,
    huge: BigInt,
    decimal: Double,
    r: Float
  )

  case class Strings(
    vchar: String,
    c: String,
    text: String
  )

  case class Temporal(
    d: java.time.LocalDate,
    t: java.time.LocalTime,
    dt: java.time.LocalDateTime,
    ts: java.time.Instant
  )

  case class Address(
    street: String,
    city: String,
    country: String
  )

  case class User(
    id: String,
    name: String,
    address: Address
  )
}
