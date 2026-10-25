package app.softnetwork.elastic.sql.function.math

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A value outside a function's domain fails the statement with DuckDB 1.5.5's reason (measured),
  * where `Math` answered `NaN` / `-Infinity` for the document.
  */
class MathDomainSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table("t", columns = List(Column("x", SQLTypes.Double)))

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

  "a function outside its domain" should "throw DuckDB's reason" in {
    scriptOf("SELECT ASIN(x) FROM t") shouldBe
    "def param1 = (doc['x'].size() == 0 ? null : doc['x'].value); (param1 == null) ? null : " +
    "(param1 != null && (param1 < -1 || param1 > 1) ? " +
    "Double.valueOf(Optional.empty().orElseThrow(() -> " +
    "new IllegalArgumentException('ASIN is undefined outside [-1,1]')).toString()) : " +
    "Double.valueOf(Math.asin(param1)))"
    scriptOf("SELECT ACOS(x) FROM t") should include("'ACOS is undefined outside [-1,1]'")
    scriptOf("SELECT SQRT(x) FROM t") should include(
      "(param1 != null && param1 < 0 ? Double.valueOf(Optional.empty().orElseThrow(() -> new IllegalArgumentException('cannot take square root of a negative number'))"
    )
    Seq("LOG(x)", "LOG10(x)").foreach { expr =>
      val script = scriptOf(s"SELECT $expr FROM t")
      script should include(
        "(param1 != null && param1 == 0 ? Double.valueOf(Optional.empty().orElseThrow(() -> new IllegalArgumentException('cannot take logarithm of zero'))"
      )
      script should include("'cannot take logarithm of a negative number'")
    }
  }

  it should "leave a function with no domain to check as it was" in {
    scriptOf("SELECT EXP(x) FROM t") should not include "IllegalArgumentException"
    scriptOf("SELECT SIN(x) FROM t") should not include "IllegalArgumentException"
  }

  "an ingest processor" should "render it as it did" in {
    scriptOf(
      "SELECT ASIN(x) FROM t",
      PainlessContextType.Processor
    ) should not include "IllegalArgumentException"
  }
}
