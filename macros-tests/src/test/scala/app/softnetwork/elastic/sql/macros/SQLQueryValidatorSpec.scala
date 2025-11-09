package app.softnetwork.elastic.sql.macros

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class SQLQueryValidatorSpec extends AnyFlatSpec with Matchers {

  // ============================================================
  // Positive Tests (Should Compile)
  // ============================================================

  "SQLQueryValidator" should "validate all numeric types" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Numbers
      import app.softnetwork.elastic.sql.query.SQLQuery

      TestElasticClientApi.searchAs[Numbers](
        "SELECT tiny::TINYINT as tiny, small::SMALLINT as small, normal::INT as normal, big::BIGINT as big, huge::BIGINT as huge, decimal::DOUBLE as decimal, r::REAL as r FROM numbers"
      )""")
  }

  it should "validate string types" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Strings
      import app.softnetwork.elastic.sql.query.SQLQuery

      TestElasticClientApi.searchAs[Strings](
        "SELECT vchar::VARCHAR, c::CHAR, text FROM strings"
      )""")
  }

  it should "validate temporal types" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Temporal
      import app.softnetwork.elastic.sql.query.SQLQuery

      TestElasticClientApi.searchAs[Temporal](
        "SELECT d::DATE, t::TIME, dt::DATETIME, ts::TIMESTAMP FROM temporal"
      )""")
  }

  it should "validate Product with all fields" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
      import app.softnetwork.elastic.sql.query.SQLQuery

      TestElasticClientApi.searchAs[Product](
        "SELECT id, name, price::DOUBLE, stock::INT, active::BOOLEAN, createdAt::DATETIME FROM products"
      )""")
  }

  it should "validate with aliases" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
      import app.softnetwork.elastic.sql.query.SQLQuery

      TestElasticClientApi.searchAs[Product](
        "SELECT product_id AS id, product_name AS name, product_price::DOUBLE AS price, product_stock::INT AS stock, is_active::BOOLEAN AS active, created_at::TIMESTAMP AS createdAt FROM products"
      )""")
  }

  it should "accept query with missing Option fields" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.ProductWithOptional
      import app.softnetwork.elastic.sql.query.SQLQuery

      TestElasticClientApi.searchAs[ProductWithOptional](
        "SELECT id, name FROM products"
      )
    """)
  }

  it should "accept query with missing fields that have defaults" in {
    assertCompiles("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.ProductWithDefaults
      import app.softnetwork.elastic.sql.query.SQLQuery

      TestElasticClientApi.searchAs[ProductWithDefaults](
        "SELECT id, name FROM products"
      )
    """)
  }

  it should "accept SELECT * with Unchecked variant" in {
    assertCompiles("""
    import app.softnetwork.elastic.client.macros.TestElasticClientApi
    import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
    import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
    import app.softnetwork.elastic.sql.query.SQLQuery

    TestElasticClientApi.searchAsUnchecked[Product](
      SQLQuery("SELECT * FROM products")
    )
  """)
  }

  // ============================================================
  // Negative Tests (Should NOT Compile)
  // ============================================================

  it should "REJECT query with missing required fields" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
      import app.softnetwork.elastic.sql.query.SQLQuery

      TestElasticClientApi.searchAs[Product](
        "SELECT id, name FROM products"
      )""")
  }

  it should "REJECT query with invalid field names" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
      import app.softnetwork.elastic.sql.query.SQLQuery

      TestElasticClientApi.searchAs[Product](
        "SELECT id, invalid_name, price, stock, active, createdAt FROM products"
      )""")
  }

  it should "REJECT query with type mismatches" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.query.SQLQuery

      case class WrongTypes(id: Int, name: Int)

      TestElasticClientApi.searchAs[WrongTypes](
        "SELECT id::LONG, name FROM products"
      )""")
  }

  it should "suggest closest field names" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
      import app.softnetwork.elastic.sql.query.SQLQuery

      TestElasticClientApi.searchAs[Product](
        "SELECT id, nam, price, stock, active, createdAt FROM products"
      )""")
  }

  it should "REJECT dynamic queries (non-literals)" in {
    assertDoesNotCompile("""
      import app.softnetwork.elastic.client.macros.TestElasticClientApi
      import app.softnetwork.elastic.client.macros.TestElasticClientApi.defaultFormats
      import app.softnetwork.elastic.sql.macros.SQLQueryValidatorSpec.Product
      import app.softnetwork.elastic.sql.query.SQLQuery

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
    import app.softnetwork.elastic.sql.query.SQLQuery

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
    import app.softnetwork.elastic.sql.query.SQLQuery

    TestElasticClientApi.searchAs[Product](
      "SELECT * FROM products WHERE active = true"
    )
  """)
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
}
