package app.softnetwork.elastic.client.result

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.concurrent.duration._

class JsonFormatterSpec extends AnyFlatSpec with Matchers {

  behavior of "JsonFormatter"

  it should "emit no warnings key for a plain DDL result" in {
    val output = JsonFormatter.format(DdlResult(success = true), 20.millis)
    println(output)

    output should include("\"success\"")
    // Story R1FIX.8 — existing consumers must see exactly the keys they saw before.
    output should not include "warnings"
  }

  it should "emit DDL warnings as a JSON array when present" in {
    val output = JsonFormatter.format(
      DdlResult(success = true, warnings = Seq("first caveat", "second caveat")),
      20.millis
    )
    println(output)

    output should include("\"warnings\"")
    output should include("first caveat")
    output should include("second caveat")
  }
}
