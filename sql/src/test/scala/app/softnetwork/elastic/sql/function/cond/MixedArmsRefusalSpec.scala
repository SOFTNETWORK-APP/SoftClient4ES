package app.softnetwork.elastic.sql.function.cond

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A CASE or a COALESCE whose arms have no common type is refused once the types are read, with
  * DuckDB 1.5.5's reason (measured: `Cannot mix values of type INTEGER and DATE in CASE expression
  *   - an explicit cast is required`).
  */
class MixedArmsRefusalSpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("kd", SQLTypes.Keyword),
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("n", SQLTypes.Int),
      Column("x", SQLTypes.Double),
      Column("b", SQLTypes.Boolean),
      Column("dn", SQLTypes.Any)
    )
  )

  private def refusals(sql: String): Seq[String] = Parser(sql) match {
    case Right(ss: SingleSearch) => ss.update(Some(schema)).typeErrors
    case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  "arms with no common type" should "be refused with DuckDB's reason" in {
    Seq(
      "CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE n END" ->
      "Cannot mix values of type INT and DATE in CASE expression - an explicit cast is required",
      "CASE WHEN n > 0 THEN ts ELSE x END" ->
      "Cannot mix values of type DOUBLE and TIMESTAMP in CASE expression - an explicit cast is required",
      "CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE CAST(ts AS TIME) END" ->
      "Cannot mix values of type TIME and DATE in CASE expression - an explicit cast is required",
      "CASE WHEN n > 0 THEN kd ELSE n END" ->
      "Cannot mix values of type INT and KEYWORD in CASE expression - an explicit cast is required",
      "CASE WHEN n > 0 THEN b ELSE CAST(d AS DATE) END" ->
      "Cannot mix values of type DATE and BOOLEAN in CASE expression - an explicit cast is required",
      "COALESCE(CAST(d AS DATE), n)" ->
      "Cannot mix values of type DATE and INT in COALESCE operator - an explicit cast is required",
      "COALESCE(ts, 1.5)" ->
      "Cannot mix values of type TIMESTAMP and DOUBLE in COALESCE operator - an explicit cast is required"
    ).foreach { case (expr, reason) =>
      withClue(s"[$expr] ")(refusals(s"SELECT $expr AS v FROM t") should contain(reason))
    }
  }

  "arms DuckDB reads together" should "be accepted" in {
    Seq(
      "CASE WHEN n > 0 THEN b ELSE n END", // INTEGER
      "CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE ts END", // TIMESTAMP
      "CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE '2024-01-01' END", // the literal is read as a DATE
      "CASE WHEN n > 0 THEN CAST(d AS DATE) ELSE NULL END",
      "COALESCE(n, x)",
      "COALESCE(dn, n)", // a type still unknown: Elasticsearch answers
      "CASE WHEN n > 0 THEN 1 ELSE 2.5 END"
    ).foreach { expr =>
      withClue(s"[$expr] ")(
        refusals(s"SELECT $expr AS v FROM t").filter(_.contains("Cannot mix")) shouldBe empty
      )
    }
  }
}
