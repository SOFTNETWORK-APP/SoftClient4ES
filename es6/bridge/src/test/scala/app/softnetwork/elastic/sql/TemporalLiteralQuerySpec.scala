package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{SingleSearch, TemporalLiterals}
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime
import scala.collection.immutable.ListMap

/** Issue #276 -- the GENERATED Elasticsearch query once temporal literals have been resolved
  * against the mapped `date` columns. A parse-level assertion cannot see what reaches `rangeQuery`
  * / `termQuery`; this spec does. The keyword negative control (AC 3) is a byte-identical
  * comparison of the emitted JSON with and without the schema.
  */
class TemporalLiteralQuerySpec extends AnyFlatSpec with Matchers {

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  private val schema: SchemaTable = SchemaTable(
    "events",
    columns = List(
      Column("id", SQLTypes.Keyword),
      Column("event_ts", SQLTypes.Date),
      Column("label", SQLTypes.Keyword),
      Column(
        "fmt_ts",
        SQLTypes.Date,
        options = ListMap("format" -> StringValue("yyyy-MM-dd HH:mm:ss"))
      )
    )
  )

  private def single(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s
    case other                  => fail(s"$sql did not parse as a SingleSearch: $other")
  }

  private def json(search: SingleSearch): String =
    SearchBodyBuilderFn(requestToElasticSearchRequest(search).search).string()

  private def resolved(sql: String): SingleSearch =
    TemporalLiterals(single(sql), schema) match {
      case Right(s)     => s
      case Left(reason) => fail(s"$sql was rejected: $reason")
    }

  "a range comparison against a date column" should "emit the T-separated literal" in {
    json(resolved("SELECT id FROM events WHERE event_ts >= '2026-06-04 00:00:00.000000'")) shouldBe
    """{"query":{"bool":{"filter":[{"range":{"event_ts":{"gte":"2026-06-04T00:00:00.000000"}}}]}},"_source":{"includes":["id"]}}"""
  }

  "equality, BETWEEN and IN against a date column" should "emit normalised term, range and terms values" in {
    json(resolved("SELECT id FROM events WHERE event_ts = '2026-06-04 00:00:00'")) shouldBe
    """{"query":{"bool":{"filter":[{"term":{"event_ts":{"value":"2026-06-04T00:00:00"}}}]}},"_source":{"includes":["id"]}}"""

    json(
      resolved(
        "SELECT id FROM events WHERE event_ts BETWEEN '2026-06-01 00:00:00' AND '2026-06-30 23:59:59'"
      )
    ) shouldBe
    """{"query":{"bool":{"filter":[{"range":{"event_ts":{"gte":"2026-06-01T00:00:00","lte":"2026-06-30T23:59:59"}}}]}},"_source":{"includes":["id"]}}"""

    json(
      resolved(
        "SELECT id FROM events WHERE event_ts IN ('2026-06-04 00:00:00', '2026-07-01 00:00:00')"
      )
    ) shouldBe
    """{"query":{"bool":{"filter":[{"terms":{"event_ts":["2026-06-04T00:00:00","2026-07-01T00:00:00"]}}]}},"_source":{"includes":["id"]}}"""
  }

  "a keyword column compared to a date-shaped string" should "emit byte-identical JSON with and without the schema" in {
    val parsed = single("SELECT id FROM events WHERE label = '2026-06-04 00:00:00'")
    val withSchema = TemporalLiterals(parsed, schema) match {
      case Right(s)     => s
      case Left(reason) => fail(reason)
    }
    withSchema should be theSameInstanceAs parsed
    json(withSchema) shouldBe json(parsed)
    json(parsed) shouldBe
    """{"query":{"bool":{"filter":[{"term":{"label":{"value":"2026-06-04 00:00:00"}}}]}},"_source":{"includes":["id"]}}"""
  }

  "an epoch-millis literal against a date column" should "be forwarded untouched" in {
    json(resolved("SELECT id FROM events WHERE event_ts >= '1780531200000'")) shouldBe
    """{"query":{"bool":{"filter":[{"range":{"event_ts":{"gte":"1780531200000"}}}]}},"_source":{"includes":["id"]}}"""
  }

  "a custom-format date column" should "keep the space form its format already parses" in {
    json(resolved("SELECT id FROM events WHERE fmt_ts >= '2026-06-04 00:00:00'")) shouldBe
    """{"query":{"bool":{"filter":[{"range":{"fmt_ts":{"gte":"2026-06-04 00:00:00"}}}]}},"_source":{"includes":["id"]}}"""
  }

  "an already ISO literal" should "emit the same JSON as before" in {
    val parsed =
      single("SELECT id FROM events WHERE event_ts < '2026-06-04T00:00:00' AND label = 'x'")
    json(resolved(parsed.sql)) shouldBe json(parsed)
  }
}
