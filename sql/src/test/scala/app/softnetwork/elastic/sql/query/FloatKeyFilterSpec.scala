package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A HAVING key filter on a `float` (REAL) grouping key, compared with a number a float does not
  * hold exactly, is refused by name: Elasticsearch compares the key as its float widened to a
  * double (`0.10000000149011612`), so `r = 0.1` kept no group and `r <> 0.1` removed none, HTTP
  * 200, where DuckDB 1.5.5 keeps the group 0.1. A value a float holds exactly is answered.
  */
class FloatKeyFilterSpec extends AnyFlatSpec with Matchers {

  private val schema: SchemaTable = SchemaTable(
    "t",
    columns = List(
      Column("r", SQLTypes.Real),
      Column("x", SQLTypes.Double),
      Column("hf", SQLTypes.Any)
    )
  )

  private def verdict(sql: String): Either[String, Unit] = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(schema)).validateResolved()
    case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
  }

  private def keyed(key: String, predicate: String): String =
    s"SELECT $key, COUNT(*) AS c FROM t GROUP BY $key HAVING $predicate"

  private val lead = "HAVING cannot compare the float GROUP BY key"

  "a float key compared with a value a float does not hold" should "be refused by name" in {
    Seq(
      keyed("r", "r = 0.1")                                             -> "r",
      keyed("r", "r <> 0.1")                                            -> "r",
      keyed("r", "r IN (0.5, 0.1)")                                     -> "r",
      keyed("r", "r NOT IN (0.1)")                                      -> "r",
      keyed("r", "r = '0.1'")                                           -> "r",
      "SELECT r AS rr, COUNT(*) AS c FROM t GROUP BY r HAVING rr = 0.1" -> "rr",
      keyed("r", "r = 0.1 AND COUNT(*) > 1")                            -> "r"
    ).foreach { case (sql, key) =>
      withClue(sql)(verdict(sql).left.toOption.getOrElse("") should startWith(s"$lead $key with "))
    }
    verdict(keyed("r", "r = 0.1")) shouldBe Left(
      "HAVING cannot compare the float GROUP BY key r with 0.1: Elasticsearch compares the key as " +
      "the double its float widens to (0.10000000149011612 for the float nearest 0.1), so the " +
      "condition would not compare the value SQL compares. Filter the rows in WHERE, where " +
      "Elasticsearch reads the value as a float, or compare the key with a value a float holds " +
      "exactly."
    )
  }

  "a float key compared with a value a float holds exactly" should "be answered" in {
    Seq(
      keyed("r", "r = 0.5"),
      keyed("r", "r IN (0.25, 0.5)"),
      keyed("r", "r = 3"),
      // a DOUBLE key compares exactly; a metric is no key filter
      keyed("x", "x = 0.1"),
      keyed("r", "COUNT(*) > 0.1"),
      // a key of no SQL type (half_float, scaled_float) is not judged here
      keyed("hf", "hf = 0.1")
    ).foreach(sql => withClue(sql)(verdict(sql) shouldBe Right(())))
  }
}
