/*
 * Copyright 2025 SOFTNETWORK
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app.softnetwork.elastic.client

import akka.actor.ActorSystem
import app.softnetwork.elastic.client.result.{ElasticError, ElasticFailure}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.Await
import scala.concurrent.duration._

/** jdbc#35 — `GatewayApi.run(sql)` labelled EVERY parse rejection, DQL included, as `"Error parsing
  * schema DDL statement: ..."` with `operation = Some("schema")`. A BI tool shows that text
  * verbatim in its error dialog (the JDBC driver puts `error.message` straight into the
  * `SQLException`), so an analyst who mistyped a SELECT was sent hunting through DDL docs.
  *
  * These tests assert the SHAPE of the message, never the parser's own reason text: `l.msg` names
  * whichever internal production failed last (`string matching regex '(?i)COPY\b' expected but 'C'
  * found` for a bad leading keyword, because `copy` is the last alternative of `dmlStatement`) and
  * moves with any grammar edit.
  */
class ParseRejectionMessageSpec extends AnyFlatSpec with Matchers with BeforeAndAfterAll {

  implicit val system: ActorSystem = ActorSystem("parse-rejection-message-spec")

  override def afterAll(): Unit = {
    system.terminate()
    super.afterAll()
  }

  private val testLogger: Logger = LoggerFactory.getLogger(getClass)

  // `protected def logger` is NopeClientApi's only abstract member; a parse rejection never reaches
  // the client, so no ES, no Docker and no extension lookup is involved.
  private val client: ElasticClientApi = new NopeClientApi {
    override protected def logger: Logger = testLogger
  }

  private def rejectionOf(sql: String): ElasticError =
    Await.result(client.run(sql), 10.seconds) match {
      case ElasticFailure(error) => error
      case other                 => fail(s"Expected a failure for [$sql], got: $other")
    }

  private def assertRejection(sql: String, error: ElasticError): Unit = {
    error.message should startWith("Error parsing SQL statement")
    error.message should include(sql)
    error.message should not include "schema DDL"
    error.message should not include "Operation failed"
    error.operation shouldBe Some("sql")
    // AC-2 has TWO halves and the second one is the easy one to lose: a builder that dropped
    // `reason` would satisfy every other assertion in this file. AC-8 forbids pinning the reason's
    // TEXT (it is `l.msg` and moves with the grammar), so pin its PRESENCE instead — something
    // non-blank must follow the `]: `.
    val head = s"Error parsing SQL statement [$sql]: "
    error.message should startWith(head)
    error.message.substring(head.length).trim should not be empty
    ()
  }

  behavior of "GatewayApi.run parse rejections"

  // THE bug's actual shape: a DQL statement, reported as a schema DDL error.
  // `SELECT * FRM users` is chosen because no roadmap item makes it parse — unlike `SELECT 1`
  // (#251), backticks or `FROM "t"` (#252), which Epic 21 is expected to accept.
  it should "not describe a rejected SELECT as a schema DDL error" in {
    val sql = "SELECT * FRM users"
    val error = rejectionOf(sql)
    assertRejection(sql, error)
    error.statusCode shouldBe Some(400)
  }

  it should "report a rejected DDL statement with the same neutral wording" in {
    val sql = "CREAT TABL missing_keyword"
    val error = rejectionOf(sql)
    assertRejection(sql, error)
    error.statusCode shouldBe Some(400)
  }

  it should "report an unsupported statement with the same neutral wording" in {
    val sql = "GRANT SELECT ON users TO user1"
    val error = rejectionOf(sql)
    assertRejection(sql, error)
    error.statusCode shouldBe Some(400)
  }

  // jOOQ's rendering of RLIKE, which is the measured #250 repro: `Parser.apply` used to THROW
  // `ValidationError("Unbalanced parentheses")` on this input instead of returning `Left`, and the
  // rejection reached a caller with NO status at all. Since #250 it takes the ordinary `Left`
  // route, so the improvement is pinned here where the defect used to be.
  //
  // The reason TEXT is deliberately not asserted: `Unbalanced parentheses` is the `sql` module's
  // string and ParserTotalitySpec owns it; `core` must not couple to a parser literal.
  it should "report a WHERE-clause rejection with the same neutral wording" in {
    val sql = "select id from emp where (name like_regex 'Jo.*')"
    val error = rejectionOf(sql)
    assertRejection(sql, error)
    error.statusCode shouldBe Some(400)
  }

  // #250 AD-10 — this replaces the old "leave statusCode and cause exactly as attempt produced
  // them on the thrown route" pin, which went red (as its own comment predicted) once the throw
  // became a `Left`. The CAPABILITY it pinned must not vanish with the route that carried it:
  // `ElasticResult.attempt` used to supply the `Throwable` cause, and `ElasticError extends
  // Throwable(message, cause.orNull)`, so an internal parser fault is the only thing that ever
  // produces a stack trace here. `ParserError.cause` now carries it.
  //
  // `SELECT a, b FROM t GROUP BY 0` is measured: `SingleSearch.bucketNames` indexes
  // `select.fields(n - 1)` with no bounds check, from inside `single`'s combinator action.
  // Ordinal-bucket SEMANTICS belong to story 21.3 / #253 — when 21.3 rejects this in `validate()`
  // the `Internal parser error` label legitimately changes; RETARGET this assertion, and keep the
  // status/operation/cause ones, which are #250's contract.
  it should "report an internal parser fault with an honest status and a preserved cause" in {
    val error = rejectionOf("SELECT a, b FROM t GROUP BY 0")
    error.message should include("Internal parser error")
    error.statusCode shouldBe Some(400)
    error.operation shouldBe Some("sql")
    error.cause shouldBe defined
  }

  // AD-4 — `PipelineApi.pipeline(sql)` carried the identical dead `ElasticFailure` branch and had
  // ZERO coverage. Its fold changes BOTH the message (`Operation failed: ...` ->
  // `Error parsing pipeline DDL statement: ...`) and the status (`None` -> `Some(400)`), so it is
  // exercised here. `NopeClientApi` is an `ElasticClientApi`, so this stays Docker-free.
  it should "report a pipeline DDL parse rejection with an honest status" in {
    client.pipeline("CREATE PIPELINE p AS SELECT a FROM t WHERE (b = 1") match {
      case ElasticFailure(error) =>
        error.message should startWith("Error parsing pipeline DDL statement")
        error.message should not include "Operation failed"
        error.statusCode shouldBe Some(400)
        error.operation shouldBe Some("pipeline")
      case other => fail(s"Expected a pipeline parse failure, got: $other")
    }
  }

  // The pipeline route's `cause = l.cause` is only exercised by an INTERNAL fault - the test above
  // feeds a grammar rejection, where the cause is `None` by design, so it cannot see the field at
  // all. `pipeline(sql)` parses any statement before checking it is a `PipelineStatement`, so the
  // measured AST crasher reaches it. Also pins that the reason is BOUNDED here as it is on the SQL
  // route: `excerpt` collapses control characters and line separators.
  it should "preserve the cause and bound the reason on the pipeline internal-fault route" in {
    client.pipeline("SELECT a, b FROM t GROUP BY 0") match {
      case ElasticFailure(error) =>
        error.message should startWith("Error parsing pipeline DDL statement")
        error.message should include("Internal parser error")
        error.message.linesIterator.size shouldBe 1
        error.statusCode shouldBe Some(400)
        error.operation shouldBe Some("pipeline")
        error.cause shouldBe defined
      case other => fail(s"Expected a pipeline internal-fault failure, got: $other")
    }
  }

  // The multi-statement branch has no message of its own — `run(statement)` inside the fold is the
  // String overload, so it re-enters the single-statement path. The batch is ordered so the FIRST
  // statement is the bad one: the fold short-circuits, so the second never runs and no ES call is
  // attempted, which is what keeps this spec Docker-free.
  //
  // HONEST LIMIT, do not oversell this test: with the failure at position 1, "names the FAILING
  // statement" and "names the FIRST statement" are the same assertion. What it does prove is that
  // the batch path re-enters the two message sites and echoes ONE member, not the whole input.
  // AC-4's real property — a rejection at position 2 is reported, with the earlier statement having
  // succeeded — needs a statement that actually executes, so it lives in the Docker suite
  // (GatewayApiIntegrationSpec, batch position-2 case).
  it should "echo only the failing member of a multi-statement batch" in {
    val error = rejectionOf("SELECT * FRM users; SELECT * FROM t")
    error.message should startWith("Error parsing SQL statement")
    error.message should include("SELECT * FRM users")
    error.message should not include "SELECT * FROM t"
    error.operation shouldBe Some("sql")
  }

  // The raw statement carries newlines AND two-space indents; finding the exact single-spaced form
  // in the message IS the proof that the collapse happened. The whole-message newline check is
  // legitimate too, and worth having: BOTH halves go through `excerpt`, so a parse-rejection
  // message is single-line by construction — which is what a log record depends on.
  it should "collapse a multi-line statement to one line in the excerpt" in {
    val error = rejectionOf("SELECT *\n  FRM\n  users")
    error.message should include("SELECT * FRM users")
    error.message should not include "\n"
  }

  // Head AND tail, not a prefix: the tail is what tells a Tableau user WHICH of their identically
  // prefixed queries failed. `FRM users` is the discriminating suffix here and must survive.
  it should "bound the excerpt, mark it truncated, and keep the tail" in {
    val padding = "x" * 400
    val error = rejectionOf(s"SELECT $padding FRM users")
    error.message should include(GatewayApi.ExcerptEllipsis)
    error.message should include("FRM users")
    // The 400-char run cannot survive a 200-char excerpt. The whole-message ceiling is exact now
    // that BOTH halves are bounded: prefix + brackets + MaxExcerpt + ": " + MaxExcerpt.
    error.message should not include padding
    error.message.length should be < (2 * GatewayApi.MaxExcerpt + 100)
  }

  // Pins the branch this story does NOT touch.
  it should "leave the empty-query rejection unchanged" in {
    val error = rejectionOf("   ")
    error.message shouldBe "Empty SQL query."
    error.statusCode shouldBe Some(400)
    error.operation shouldBe Some("sql")
  }

  behavior of "GatewayApi.excerpt"

  it should "return a short statement unchanged" in {
    GatewayApi.excerpt("SELECT 1") shouldBe "SELECT 1"
  }

  it should "keep head and tail, with the ASCII ellipsis between them" in {
    val long = "H" * 300 + "TAIL"
    val excerpt = GatewayApi.excerpt(long)
    excerpt should startWith("H" * GatewayApi.ExcerptHead)
    excerpt should endWith("TAIL")
    excerpt should include(GatewayApi.ExcerptEllipsis)
  }

  // The post-condition, stated as an invariant rather than an example: the ellipsis lives INSIDE
  // the budget. A `take(n) + "..."` implementation returns n + 3 and fails this.
  it should "never exceed MaxExcerpt, at any length around the boundary" in {
    for (n <- Seq(0, 1, 199, 200, 201, 202, 500, 5000)) {
      val excerpt = GatewayApi.excerpt("a" * n)
      withClue(s"n=$n: ") {
        excerpt.length should be <= GatewayApi.MaxExcerpt
      }
    }
    GatewayApi.excerpt("a" * 200).length shouldBe 200 // exactly at the cap: no ellipsis
    GatewayApi.excerpt("a" * 200) should not include GatewayApi.ExcerptEllipsis
  }

  // A cut that lands mid-surrogate emits a lone surrogate — not valid UTF-16, and the excerpt is
  // re-encoded at least twice before the analyst sees it. The property is asserted as a UTF-8
  // ROUND TRIP rather than by walking the chars: it is exact (a lone surrogate becomes U+FFFD and
  // the strings differ), it cannot itself throw, and it is the failure the analyst would actually
  // see. Do NOT assert an exact length here — a guarded cut may hand a character back.
  it should "never split a surrogate pair" in {
    // U+1F600, one code point, two UTF-16 units. Written as backslash-u escapes so the source
    // stays ASCII (the encoding rule in the story's build facts); Scala resolves them before
    // lexing. (A bare backslash-u in this very comment would break the 2.12 leg, which still
    // processes unicode escapes inside comments.)
    val pair = "\uD83D\uDE00"
    // Sweep both cuts across a pair boundary: pad 119/120 straddle the head cut, trailing 75/76
    // straddle the tail cut.
    for (pad <- 118 to 122; trailing <- 74 to 80) {
      val excerpt = GatewayApi.excerpt("a" * pad + pair + "b" * 300 + pair + "c" * trailing)
      withClue(s"pad=$pad trailing=$trailing: ") {
        val utf8 = java.nio.charset.StandardCharsets.UTF_8
        new String(excerpt.getBytes(utf8), utf8) shouldBe excerpt
        excerpt.length should be <= GatewayApi.MaxExcerpt
      }
    }
  }

  // The security control, asserted as a control and not as cosmetics: a crafted statement must not
  // be able to forge a log line or emit an ANSI sequence. `\s` alone does NOT cover these.
  it should "collapse control characters, not just whitespace" in {
    // Written as backslash-u escapes on purpose: a literal ESC / NUL / BEL in a source file is
    // invisible to review and to `git diff`. Scala resolves the escapes before lexing, so these
    // ARE the characters.
    val hostile =
      "SELECT" + "\u001b" + "[2J" + "\u0000" + "\u0007" + " 1\nFROM" + "\u0085" + "t"
    val excerpt = GatewayApi.excerpt(hostile)
    excerpt.exists(c => Character.isISOControl(c)) shouldBe false
    excerpt should not include "\u0085"
    excerpt shouldBe "SELECT [2J 1 FROM t"
  }

  behavior of "GatewayApi.parseRejectionMessage"

  // The reason half is bounded by the SAME constant as the statement half — otherwise a
  // `validate()` failure that embeds a whole AST (or a whole SCRIPT body, `Parser.scala:312`)
  // buries the diagnosis it was meant to deliver. This assertion is what makes the whole-message
  // length bound in the `run` tests meaningful rather than lucky.
  it should "bound the parser reason as well as the statement" in {
    val message = GatewayApi.parseRejectionMessage("SELECT 1", "R" * 4000)
    message should include(GatewayApi.ExcerptEllipsis)
    message.length should be < (2 * GatewayApi.MaxExcerpt + 100)
    message should startWith("Error parsing SQL statement [SELECT 1]: ")
  }

  // `ParserError(msg)` carries no non-empty invariant, so the parser can hand over a blank reason.
  // The message must never end in `]: ` with nothing after the colon (AC-2).
  it should "never emit an empty reason, whichever route supplied it" in {
    GatewayApi.parseRejectionMessage("SELECT 1", "   ") shouldBe
    s"Error parsing SQL statement [SELECT 1]: ${GatewayApi.NoParserReason}"
    GatewayApi.parseRejectionMessage("SELECT 1", "") should not endWith "]: "
  }

}
