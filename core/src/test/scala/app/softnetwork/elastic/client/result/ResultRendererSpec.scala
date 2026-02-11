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
