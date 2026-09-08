package app.softnetwork.elastic.sql.schema

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.AlterPipeline
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

/** Story 21.5 — `GenericProcessor.column` was `UUID.randomUUID().toString`, and that ONE expression
  * produced two distinct defects, because two different consumers read it:
  *
  *   - `Table.diff` keys processors by `"<pipeline>-<type>-<column>"`, so an anonymous processor
  *     never matched its counterpart and every diff reported it REMOVED and re-ADDED — a processor
  *     nobody had touched (21.8 Part F.1, the churn);
  *   - `ProcessorRemoved.stmt` renders `DROP PROCESSOR <TYPE>(<column>)`, so the generated DDL read
  *     `DROP PROCESSOR SCRIPT(b18b1864-5e89-44fd-b4f8-191faa9e9e0c)`, whose hyphens are not an
  *     `ident` — the statement did not parse (Part F.2, fixed here).
  *
  * MEASURED before the fix: three consecutive reads of one processor's `column` returned three
  * different UUIDs, and the third is the one that reached the DDL.
  *
  * On ES 7/8/9 this was unreachable: their processors keep the `description` that re-types them
  * into a `ScriptProcessor` (processor `description` is an ES 7.9+ field), so they never become
  * anonymous. ES 6.8 drops it, which is why only 6.8 failed.
  */
class AlterPipelineRenderSpec extends AnyFlatSpec with Matchers {

  private def anonymous(source: String): GenericProcessor =
    GenericProcessor(
      processorType = IngestProcessorType.Script,
      properties = ListMap[String, Any](
        "lang"           -> "painless",
        "source"         -> source,
        "ignore_failure" -> true
      )
    )

  "an anonymous processor's column" should "be stable across reads" in {
    val p = anonymous("ctx.a = 1")
    // the defect was that these three differ; `ProcessorRemoved` reads it a second time, so a
    // random value also meant the key and the rendered statement disagreed with each other.
    p.column shouldBe p.column
    p.column shouldBe p.column
  }

  it should "be a function of the processor's CONTENT, not of the call" in {
    anonymous("ctx.a = 1").column shouldBe anonymous("ctx.a = 1").column
    anonymous("ctx.a = 1").column should not be anonymous("ctx.b = 2").column
  }

  it should "not depend on the order the properties were parsed in" in {
    // `properties` is a ListMap -- INSERTION ordered. Hashing its `toString` directly would make
    // the id depend on the order the JSON happened to be parsed in, and the diff KEYS on this
    // string. Canonicalising (render pairs, sort) makes equality a property rather than an
    // observation, and `String.hashCode` is JLS-specified, so it holds across JVM runs too.
    val a = GenericProcessor(
      processorType = IngestProcessorType.Script,
      properties = ListMap[String, Any](
        "lang"           -> "painless",
        "source"         -> "ctx.a = 1",
        "ignore_failure" -> true
      )
    )
    val b = GenericProcessor(
      processorType = IngestProcessorType.Script,
      properties = ListMap[String, Any](
        "ignore_failure" -> true,
        "source"         -> "ctx.a = 1",
        "lang"           -> "painless"
      )
    )
    a.properties.toString should not be b.properties.toString // the orders really do differ
    a.column shouldBe b.column
  }

  it should "be identifier-safe, so the generated DDL round-trips" in {
    // The actual failure mode: a UUID's hyphens are not an `ident`, so the rendered ALTER PIPELINE
    // was rejected by our own parser with "Mismatched closing parentheses in ALTER PIPELINE
    // statement" -- a message that names the wrong cause, which is why this asserts the ROUND TRIP
    // rather than the message.
    val stmt = ProcessorRemoved(anonymous("ctx.a = 1")).stmt
    val sql = AlterPipeline("my_pipeline", ifExists = false, List(stmt)).sql
    withClue(s"generated [$sql]: ") {
      sql should include("DROP PROCESSOR")
      Parser(sql) match {
        case Right(_: AlterPipeline) => succeed
        case other                   => fail(s"generated DDL did not re-parse: $other")
      }
    }
  }

  "a processor that HAS a field" should "still use it, unchanged" in {
    // The compatibility case. `column` is computed at diff time and NEVER persisted, so no stored
    // pipeline carries the old value and nothing has to migrate. What an existing installation can
    // carry is a processor WITH a `field`, and that path is byte-identical to before the fix.
    val p = GenericProcessor(
      processorType = IngestProcessorType.Set,
      properties = ListMap[String, Any]("field" -> "profile.city", "value" -> "Paris")
    )
    p.column shouldBe "profile.city"
    ProcessorRemoved(p).stmt.sql should include("(profile.city)")
  }
}
