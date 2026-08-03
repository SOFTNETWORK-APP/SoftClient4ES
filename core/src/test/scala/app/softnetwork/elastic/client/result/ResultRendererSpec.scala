package app.softnetwork.elastic.client.result

import org.scalatest.flatspec.AsyncFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap
import scala.concurrent.duration._

class ResultRendererSpec extends AsyncFlatSpec with Matchers {

  behavior of "ResultRenderer"

  it should "render empty result" in {
    val output = ResultRenderer.render(EmptyResult, 10.millis)
    println(output)

    output should include("Empty result")
  }

  it should "render query rows as table" in {
    val rows = Seq(
      ListMap("id" -> 1, "name" -> "Alice", "email" -> "alice@example.com"),
      ListMap("id" -> 2, "name" -> "Bob", "email"   -> "bob@example.com")
    )

    val output = ResultRenderer.render(QueryRows(rows), 50.millis)
    println(output)

    output should include("id")
    output should include("name")
    output should include("email")
    output should include("Alice")
    output should include("Bob")
    output should include("2 row(s)")
  }

  it should "render DML result" in {
    val result = DmlResult(inserted = 10, updated = 5, deleted = 2, rejected = 1)

    val output = ResultRenderer.render(result, 100.millis)
    println(output)

    output should include("10 inserted")
    output should include("5 updated")
    output should include("2 deleted")
    output should include("1 rejected")
  }

  it should "render DDL success" in {
    val output = ResultRenderer.render(DdlResult(success = true), 20.millis)
    println(output)

    output should include("Success")
  }

  // Story R1FIX.8 — a DDL statement may succeed in a degraded mode and say so.
  // NB: the renderer is fansi-coloured unconditionally, so an escape sequence sits between the
  // emoji and the word. Assert on words, never on "✅ Success" / "⚠️ …" as a single literal.

  it should "render DDL success without a warning line when there are no warnings" in {
    val output = ResultRenderer.render(DdlResult(success = true), 20.millis)
    println(output)

    output should include("Success")
    output should not include "⚠️"
    output.linesIterator.size shouldBe 1
  }

  it should "render a DDL warning below the success line" in {
    val warning =
      "Materialized view 'orders_mv' was created, but automatic refresh is unavailable: " +
      "run 'REFRESH MATERIALIZED VIEW orders_mv' whenever the joined tables change."
    val output =
      ResultRenderer.render(DdlResult(success = true, warnings = Seq(warning)), 20.millis)
    println(output)

    // The statement still reports success — a caveat must not read as a failure.
    output should include("Success")
    output should include("⚠️")
    output should include("REFRESH MATERIALIZED VIEW orders_mv")

    val lines = output.linesIterator.toSeq
    lines.size shouldBe 2
    lines.head should include("Success")
    lines(1) should include("automatic refresh is unavailable")
  }

  it should "render one line per DDL warning" in {
    val output = ResultRenderer.render(
      DdlResult(success = true, warnings = Seq("first caveat", "second caveat")),
      20.millis
    )
    println(output)

    val lines = output.linesIterator.toSeq
    lines.size shouldBe 3
    lines(1) should include("first caveat")
    lines(2) should include("second caveat")
  }

  it should "render a DDL warning on a no-op result too" in {
    val output =
      ResultRenderer.render(DdlResult(success = false, warnings = Seq("a caveat")), 20.millis)
    println(output)

    output should include("No changes")
    output should include("a caveat")
  }

  it should "format values correctly" in {
    val rows = Seq(
      ListMap(
        "string"  -> "test",
        "number"  -> 42,
        "boolean" -> true,
        "null"    -> null,
        "array"   -> Seq(1, 2, 3),
        "map"     -> Map("key" -> "value")
      )
    )

    val output = ResultRenderer.render(QueryRows(rows), 10.millis)
    println(output)

    output should include("test")
    output should include("42")
    output should include("true")
    output should include("NULL")
  }
}
