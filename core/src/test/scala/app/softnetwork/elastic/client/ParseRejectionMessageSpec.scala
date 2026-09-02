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

  // The OTHER rejection route: SoftClient4ES#250 — `Parser.apply` THROWS `ValidationError` here
  // instead of returning `Left` (measured on this exact input; it is jOOQ's rendering of RLIKE).
  // The MESSAGE assertions are route-agnostic by design: both routes must produce the same shape.
  it should "report a THROWN parse failure with the same neutral wording" in {
    val sql = "select id from emp where (name like_regex 'Jo.*')"
    assertRejection(sql, rejectionOf(sql))
  }

  // ...but shape assertions alone cannot FAIL if site 2 regresses, because site 1 satisfies them
  // too. These two are the route's only discriminators — site 1 always yields `statusCode =
  // Some(400)` and no cause — and they pin the two decisions AD-5 and Task 1.3 state explicitly:
  // `statusCode` is left exactly as `ElasticResult.attempt` set it, and `copy` preserves the cause.
  // Without them a dev can rebuild the error from scratch at site 2, break three recorded decisions,
  // and watch every test stay green.
  //
  // TRAP — this test is EXPECTED to go red when #250 lands and the throw becomes a `Left`. That is
  // the signal, not a defect: delete it then (site 1 already covers the shape). Do not weaken it now
  // to make it survive a change that has not happened.
  it should "leave statusCode and cause exactly as attempt produced them on the thrown route" in {
    val error = rejectionOf("select id from emp where (name like_regex 'Jo.*')")
    error.statusCode shouldBe None
    error.cause shouldBe defined
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

  // `ParserError(msg)` has no non-empty invariant either (`Parser.scala:1365`), so the `Left` route
  // can hand over a blank reason exactly as the thrown route can. The message must never end in
  // `]: ` with nothing after the colon (AC-2).
  it should "never emit an empty reason, whichever route supplied it" in {
    GatewayApi.parseRejectionMessage("SELECT 1", "   ") shouldBe
    s"Error parsing SQL statement [SELECT 1]: ${GatewayApi.NoParserReason}"
    GatewayApi.parseRejectionMessage("SELECT 1", "") should not endWith "]: "
  }

  behavior of "GatewayApi.parseFailureReason"

  // Branch 1 — the live one: `ElasticResult.attempt` wrapped a thrown parser exception.
  it should "prefer the cause's own message over attempt's wrapper" in {
    val wrapped = new IllegalStateException("Unbalanced parentheses")
    GatewayApi.parseFailureReason(
      ElasticError("Operation failed: Unbalanced parentheses", cause = Some(wrapped))
    ) shouldBe "Unbalanced parentheses"
  }

  // Branch 2 — unreachable through `run` today, which is exactly why it is tested here. Note what
  // it must NOT return: `attempt` interpolates a null message into its own wrapper, so relaying
  // `error.message` would put the literal "Operation failed: null" in front of the analyst.
  it should "use the cause's class name when its message is null, never attempt's wrapper" in {
    val reason = GatewayApi.parseFailureReason(
      ElasticError("Operation failed: null", cause = Some(new RuntimeException()))
    )
    reason shouldBe classOf[RuntimeException].getName
    reason should not include "Operation failed"
  }

  // Branch 3 — a blank cause message would render as `[...]: ` with nothing after the colon.
  it should "use the cause's class name when its message is blank" in {
    GatewayApi.parseFailureReason(
      ElasticError("Operation failed:", cause = Some(new IllegalArgumentException("   ")))
    ) shouldBe classOf[IllegalArgumentException].getName
  }

  // Branch 4 — defensive: `ElasticResult.attempt` always sets a cause, so this cannot arise at
  // site 2. It exists so the function is total and can never return an empty reason.
  it should "never return an empty reason when there is no cause at all" in {
    GatewayApi.parseFailureReason(ElasticError("whatever")) shouldBe GatewayApi.NoParserReason
  }
}
