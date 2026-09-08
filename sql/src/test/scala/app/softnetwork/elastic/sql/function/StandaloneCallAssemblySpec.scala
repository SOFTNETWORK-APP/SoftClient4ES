package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.io.Source

/** Story 21.8 Part C — the chain assembly concatenated the operand in front of a STANDALONE call.
  *
  * `TransformFunction.toPainless`'s context-free branch appended the function's own rendering to
  * the operand on the assumption that the rendering is a method CHAINED onto it, i.e. that it
  * begins with `.`. True of most of the date family — `.plus(…)`, `.truncatedTo(…)`,
  * `.get(ChronoField.YEAR)` — and false of exactly the four whose call is standalone, because a
  * standalone call already carries the operand in its own ARGUMENTS. MEASURED on `origin/main`
  * `e6891135`:
  * {{{
  * DATE_PARSE('2025-01-10','yyyy-MM-dd')
  *   -> "2025-01-10"LocalDate.parse("2025-01-10", DateTimeFormatter.ofPattern("yyyy-MM-dd"))
  * DATE_PARSE(name,'yyyy-MM-dd')
  *   -> (def e0 = (doc['name']…); e0 != null ? e0def arg0 = (doc['name']…
  * }}}
  * `e0def` — two tokens fused. Painless that cannot compile, for a literal operand and a column
  * operand alike, and it PROPAGATED: `YEAR(DATE_PARSE(name,'yyyy-MM-dd'))` and
  * `DATE_ADD(DATE_PARSE(name,…), INTERVAL 1 DAY)` carried the fusion outward, which the defect
  * record did not have.
  *
  * 🔴 It is fixed at the ASSEMBLY and guarded over the FAMILY (the story's AD-1 / AC-5), not
  * patched in the four `toPainlessCall`s. Each of those is correct in isolation; encoding the
  * assembly's assumption into them would leave the next standalone-call function broken on arrival.
  * The `case Some(ctx)` branch beside it already made exactly this decision with its `else p`, so
  * the fix also collapses two renderings onto one rule instead of letting them disagree.
  *
  * 🔴 Blast radius, measured and NOT what the record assumed. `painless(None)` is the CONTEXT-FREE
  * rendering; every shipped surface that reaches these four functions renders them with a
  * `PainlessContext` (script fields, script sorts, the materialized-view projection), and that
  * branch was always right. The context-free sites in shipped code — `bucket_selector`,
  * `bucket_script`, geo distance, top_hits script fields — either carry aggregates or drop the
  * field. So this is a public-API correctness fix with no live query failure behind it, which is
  * why the lead deferred it out of 21.8's first pass; it is NOT a claim that some query was
  * failing.
  *
  * 🔴 Every emission pinned below was EXECUTED on a real Elasticsearch 8.18.3 as the `source` of a
  * `script_fields` entry over a real index, because "this is syntactically valid Painless" is a
  * claim only Elasticsearch settles — this project's standing rule, and the confirmation the defect
  * record itself flagged as OWED and inferred. Discharged: the two pre-fix strings come back
  * `script_exception :: compile error`, and the post-fix ones return `2025-01-10` / `2025` and
  * `null` for a document missing the field. The pre-fix strings are deliberately NOT pinned
  * anywhere — they are the corruption this fixes.
  *
  * ⚠️ One pinned emission below is pinned as FUSION-FREE and explicitly NOT as executable; the
  * reason is at that test, and it is a pre-existing defect of a different kind.
  */
class StandaloneCallAssemblySpec extends AnyFlatSpec with Matchers {

  private val schema: Table = Table(
    "t",
    columns = List(
      Column("name", SQLTypes.Keyword),
      Column("created", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp)
    )
  )

  /** A schema-CARRYING parse. Without it every identifier's `baseType` stays `Any`, no conversion
    * arm is reachable and a broken assembly looks identical to a fixed one (#306 / story 21.5).
    */
  private def field(sql: String) =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss.update(Some(schema)).select.fields.head
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  private def painlessOf(sql: String): String = field(sql).painless(None)

  private def scriptOf(sql: String): String = {
    val ctx = PainlessContext(PainlessContextType.Query)
    val body = field(sql).painless(Some(ctx))
    s"$ctx$body"
  }

  // -- the four standalone-call functions, both operand kinds ------------------------------------

  "DATE_PARSE over a literal" should "emit the parse alone, not the literal in front of it" in {
    // Executed on ES 8.18.3: returns 2025-01-10.
    painlessOf("SELECT DATE_PARSE('2025-01-10', 'yyyy-MM-dd') FROM t") shouldBe
    """LocalDate.parse("2025-01-10", DateTimeFormatter.ofPattern("yyyy-MM-dd"))"""
  }

  "DATE_PARSE over a column" should "emit the null-guarded parse alone" in {
    // Executed on ES 8.18.3 with the doc access substituted by a bound value: returns 2025-01-10,
    // and null for a missing field rather than failing the shard.
    painlessOf("SELECT DATE_PARSE(name, 'yyyy-MM-dd') FROM t") shouldBe
    "def arg0 = (doc['name'].size() == 0 ? null : doc['name'].value); " +
    """(arg0 == null) ? null : LocalDate.parse(arg0, DateTimeFormatter.ofPattern("yyyy-MM-dd"))"""
  }

  "DATETIME_PARSE" should "emit the parse alone for both operand kinds" in {
    painlessOf("SELECT DATETIME_PARSE('2025-01-10 10:00:00', 'yyyy-MM-dd HH:mm:ss') FROM t") shouldBe
    """ZonedDateTime.parse("2025-01-10 10:00:00", """ +
    """DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of('Z')))"""
    painlessOf("SELECT DATETIME_PARSE(name, 'yyyy-MM-dd HH:mm:ss') FROM t") shouldBe
    "def arg0 = (doc['name'].size() == 0 ? null : doc['name'].value); (arg0 == null) ? null : " +
    """ZonedDateTime.parse(arg0, """ +
    """DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of('Z')))"""
  }

  "DATE_FORMAT and DATETIME_FORMAT" should "emit the format call alone for both operand kinds" in {
    // A TEMPORAL operand is formatted directly; these two are byte-identical to `origin/main`.
    painlessOf("SELECT DATE_FORMAT(created, 'yyyy') FROM t") shouldBe
    "def arg0 = (doc['created'].size() == 0 ? null : doc['created'].value); " +
    """(arg0 == null) ? null : DateTimeFormatter.ofPattern("yyyy").format(arg0)"""
    painlessOf("SELECT DATETIME_FORMAT(ts, 'yyyy') FROM t") shouldBe
    "def arg0 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    """(arg0 == null) ? null : DateTimeFormatter.ofPattern("yyyy").format(arg0)"""
  }

  it should "PARSE a string operand instead of handing a String to format()" in {
    // 🔴 Found by executing the assembly's own output, which is what AC-4's real-index rule is
    // for. `DateTimeFormatter.format` takes a `TemporalAccessor`; a `String` is not one, so
    // MEASURED on ES 8.18.3 both of these were a script COMPILE error before this story:
    //
    //   Cannot cast from [java.lang.String] to [java.time.temporal.TemporalAccessor]
    //
    // Live on the production path too, and for a KEYWORD COLUMN as well as a literal. The
    // documented spelling `DATE_FORMAT('2025-01-10'::DATE, '%Y-%m-%d')` escaped it only because
    // the CAST inserted the parse — which is also why the operand is parsed here through the SAME
    // `coerce` arms, so the two spellings cannot accept different format sets.
    //
    // Executed on ES 8.18.3: both return "2025".
    painlessOf("SELECT DATE_FORMAT('2025-01-10', 'yyyy') FROM t") shouldBe
    """DateTimeFormatter.ofPattern("yyyy").format(""" +
    """LocalDate.parse(("2025-01-10").replace("/", "-"), """ +
    """DateTimeFormatter.ofPattern("yyyy-MM-dd")))"""
    scriptOf("SELECT DATE_FORMAT(name, 'yyyy') FROM t") shouldBe
    "def param1 = (doc['name'].size() == 0 ? null : doc['name'].value); " +
    """def param2 = (param1 != null ? LocalDate.parse((param1).replace("/", "-"), """ +
    """DateTimeFormatter.ofPattern("yyyy-MM-dd")) : null); """ +
    """def param3 = DateTimeFormatter.ofPattern("yyyy"); """ +
    "(param1 == null) ? null : param3.format(param2)"
  }

  it should "keep that parse INSIDE a null guard" in {
    // The parse is bound as its own parameter, so it is EVALUATED before the guard around the
    // format call — a document merely MISSING the field would run `LocalDate.parse(null, …)` and
    // fail the shard. Executed on ES 8.18.3 against a document without the field: returns null.
    // (The first draft passed `nullable = false` here and did exactly that.)
    scriptOf("SELECT DATE_FORMAT(name, 'yyyy') FROM t") should include("param1 != null ?")
  }

  // -- a call CHAINED onto a guarded standalone call ---------------------------------------------

  "a chained call after a guarded parse" should "land inside the guard, boxed" in {
    // 🔴 The second defect the real-index pass found, and the one that was live on every client.
    // `.get(ChronoField.YEAR)` returns a PRIMITIVE, and appending it after the parse's own guard
    // gave `(param1 == null) ? null : LocalDate.parse(…).get(…)`, which ES 8.18.3 rejects with
    // `class_cast_exception: Cannot cast from [int] to [java.lang.Object]` — Painless cannot unify
    // a primitive with `null`. So `YEAR(DATE_PARSE(col, fmt))` and `MONTH(...)` did not run at all.
    //
    // ⚠️ Neither boxing the whole expression nor the null-safe `?.` fixes it: measured, Painless
    // types `cond ? null : X` as `Object`, so `(…)?.get(…)` fails with `member method
    // [java.lang.Object, get/1] not found`. The method must land INSIDE the guard, which is why
    // the guarded expression is bound to a name first.
    //
    // Executed on ES 8.18.3: 2025 for a document that has the field, null for one that does not.
    scriptOf("SELECT YEAR(DATE_PARSE(name, 'yyyy-MM-dd')) FROM t") shouldBe
    "def param1 = (doc['name'].size() == 0 ? null : doc['name'].value); " +
    """def param2 = (param1 == null) ? null : LocalDate.parse(param1, """ +
    """DateTimeFormatter.ofPattern("yyyy-MM-dd")); """ +
    "(param2 == null) ? null : (def)(param2.get(ChronoField.YEAR))"
  }

  "DATE_TRUNC over a date-only operand" should "not try to truncate a time it does not have" in {
    // 🔴 The third: `truncatedTo` is not a member of `java.time.LocalDate`, so
    // `DATE_TRUNC(DATE_PARSE(col, fmt), MONTH)` was `member method [java.time.LocalDate,
    // truncatedTo/1] not found`. `withDayOfMonth` exists on `LocalDate` and a date-only value has
    // no time to truncate, so omitting the truncation is the same value, not an approximation.
    // Executed on ES 8.18.3: 2025-01-01, and null for a document without the field.
    scriptOf("SELECT DATE_TRUNC(DATE_PARSE(name, 'yyyy-MM-dd'), MONTH) FROM t") shouldBe
    "def param1 = (doc['name'].size() == 0 ? null : doc['name'].value); " +
    """def param2 = (param1 == null) ? null : LocalDate.parse(param1, """ +
    """DateTimeFormatter.ofPattern("yyyy-MM-dd")); """ +
    "(param2 == null) ? null : (def)(param2.withDayOfMonth(1))"
  }

  "a THREE-function chain" should "guard at every step, not just the first" in {
    // 🔴 The case that caught a mistake in the first version of this fix. Binding the guarded
    // expression as a PARAMETER made it findable, so the next function appended its method to a
    // parameter whose value is a ternary -- outside the guard again, one level up:
    //
    //   (param2 == null) ? null : (def)(param2.truncatedTo(…)).get(ChronoField.YEAR)
    //
    // It also proves the bridge fixture was pinning a script Elasticsearch REJECTS: the
    // `origin/main` emission for this exact statement returns `class_cast_exception: Cannot cast
    // from [int] to [java.lang.Object]` on ES 8.18.3, and no test ever ran it. Executed after the
    // fix: 2025 for a document with the field, null for one without.
    scriptOf(
      "SELECT YEAR(DATE_TRUNC(DATETIME_PARSE(name, 'yyyy-MM-dd HH:mm:ss'), MINUTE)) FROM t"
    ) shouldBe
    "def param1 = (doc['name'].size() == 0 ? null : doc['name'].value); " +
    """def param2 = (param1 == null) ? null : ZonedDateTime.parse(param1, """ +
    """DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneId.of('Z'))); """ +
    "def param3 = (param2 == null) ? null : (def)(param2.truncatedTo(ChronoUnit.MINUTES)); " +
    "(param3 == null) ? null : (def)(param3.get(ChronoField.YEAR))"
  }

  "a NON-nullable operand" should "keep appending its method, with no extra binding" in {
    // The scope of the re-binding rule: it exists only to get a method inside a guard, so where
    // there is no guard it must not fire. `LAST_DAY(...)` over a non-nullable operand is
    // byte-identical to `origin/main`, and it is what caught the first, unscoped version.
    scriptOf("SELECT LAST_DAY(created) FROM t") should not include "(def)("
  }

  it should "still truncate an operand that HAS a time" in {
    // The other half of the rule, byte-identical to `origin/main`: an Elasticsearch `date` field
    // resolves to a ZonedDateTime, which does carry a time.
    scriptOf("SELECT DATE_TRUNC(ts, MONTH) FROM t") should include(
      ".withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS)"
    )
  }

  "the fusion" should "no longer propagate outward through a wrapping function" in {
    // 🔴 Not in the defect record, found re-measuring: the malformed operand+call was embedded in
    // whatever wrapped it, so ONE broken assembly corrupted every chain that contained it.
    //
    // ⚠️ This emission is pinned because the FUSION is gone, and deliberately NOT claimed to run:
    // it is still the unboxed `(cond) ? null : <int>` shape the test above fixes ON THE
    // CONTEXT-BEARING PATH. It cannot be fixed here, and the reason is structural rather than an
    // oversight: the context-free renderer guards by emitting STATEMENTS (`def arg0 = …;`), so
    // there is no bound name to hang the method on and no way to reach inside the guard. Making
    // that path executable means changing how it guards, which is a different change with no
    // production surface behind it — every shipped surface that reaches these functions renders
    // WITH a context. Pinned so the day someone does change it, this test says so.
    painlessOf("SELECT YEAR(DATE_PARSE(name, 'yyyy-MM-dd')) FROM t") shouldBe
    "def arg0 = (doc['name'].size() == 0 ? null : doc['name'].value); (arg0 == null) ? null : " +
    """LocalDate.parse(arg0, DateTimeFormatter.ofPattern("yyyy-MM-dd")).get(ChronoField.YEAR)"""
  }

  // -- the production rendering is untouched -----------------------------------------------------

  "the context-bearing rendering" should "be byte-identical to before the fix" in {
    // The branch that every shipped surface takes was already correct, and the whole point of
    // taking the decision at the assembly is that the two renderings now agree BY CONSTRUCTION.
    // These bytes are the pre-fix ones, captured on the baseline.
    scriptOf("SELECT DATE_PARSE(name, 'yyyy-MM-dd') FROM t") shouldBe
    "def param1 = (doc['name'].size() == 0 ? null : doc['name'].value); (param1 == null) ? null : " +
    """LocalDate.parse(param1, DateTimeFormatter.ofPattern("yyyy-MM-dd"))"""
    scriptOf("SELECT DATE_FORMAT(created, 'yyyy') FROM t") shouldBe
    "def param1 = (doc['created'].size() == 0 ? null : doc['created'].value); " +
    """def param2 = DateTimeFormatter.ofPattern("yyyy"); """ +
    "(param1 == null) ? null : param2.format(param1)"
  }

  "a CHAINED call" should "still take the operand as its prefix" in {
    // The other half of the rule: `.get(…)`, `.truncatedTo(…)`, `.plus(…)` are methods ON the
    // operand, so dropping the base there would emit a call with no receiver. Byte-identical.
    painlessOf("SELECT YEAR(created) FROM t") shouldBe
    "(doc['created'].size() == 0 ? null : doc['created'].value).get(ChronoField.YEAR)"
    painlessOf("SELECT DATE_TRUNC(ts, MONTH) FROM t") shouldBe
    "(doc['ts'].size() == 0 ? null : doc['ts'].value).withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS)"
  }

  // -- AC-5: the invariant, over the whole family ------------------------------------------------

  /** One statement per date/time `TransformFunction`. The list is not the guard — the source scan
    * at the bottom is: it fails when a new function joins the family without joining this list.
    *
    * `DATE_DIFF` is deliberately absent and its absence is CHECKED rather than assumed: `DateDiff`
    * is a `BinaryFunction`, so it binds its own arguments and is never handed to
    * `TransformFunction.toPainless` at all. The scan below derives the family from the source, and
    * `DateDiff`'s declaration does not reach `TransformFunction` — so if that ever changes, the
    * coverage test reds and this list has to grow.
    */
  private val family: List[String] = List(
    "SELECT DATE_TRUNC(ts, MONTH) FROM t",
    "SELECT YEAR(created) FROM t",
    "SELECT EXTRACT(YEAR FROM ts) FROM t",
    "SELECT LAST_DAY(created) FROM t",
    "SELECT DATE_ADD(created, INTERVAL 1 DAY) FROM t",
    "SELECT DATE_SUB(created, INTERVAL 1 DAY) FROM t",
    "SELECT DATE_PARSE(name, 'yyyy-MM-dd') FROM t",
    "SELECT DATE_FORMAT(created, 'yyyy') FROM t",
    "SELECT DATETIME_ADD(ts, INTERVAL 1 HOUR) FROM t",
    "SELECT DATETIME_SUB(ts, INTERVAL 1 HOUR) FROM t",
    "SELECT DATETIME_PARSE(name, 'yyyy-MM-dd HH:mm:ss') FROM t",
    "SELECT DATETIME_FORMAT(ts, 'yyyy') FROM t",
    "SELECT created + INTERVAL 1 DAY FROM t",
    "SELECT created - INTERVAL 1 DAY FROM t"
  )

  private def transformsOf(sql: String): List[TransformFunction[_, _]] =
    FunctionUtils.transformFunctions(field(sql).identifier).collect {
      case tf: TransformFunction[_, _] => tf
    }

  private val Sentinel = "__OPERAND__"

  behavior of "every date and time transform function"

  it should "never append a standalone call to the operand (AC-5)" in {
    // The invariant, asserted on the ASSEMBLY rather than inferred from a rendered string: if the
    // function's own rendering does not start with `.`, it is a complete expression that already
    // carries the operand, so it must be emitted at position 0 — never appended to anything.
    //
    // Falsified: reverting the one-line assembly fix reds this for DATE_PARSE, DATE_FORMAT,
    // DATETIME_PARSE and DATETIME_FORMAT and leaves the ten chained members green.
    family.foreach { sql =>
      transformsOf(sql).foreach { fn =>
        val call = fn.painless(None)
        if (!call.startsWith(".")) {
          val assembled = fn.toPainless(Sentinel, 7, None)
          withClue(s"[$sql] ${fn.getClass.getSimpleName} assembled=[$assembled] call=[$call] ") {
            assembled.indexOf(call) should (be(-1) or be(0))
          }
        }
      }
    }
  }

  it should "leave no operand fused to the token that follows it" in {
    // The same property read off the emission rather than off the assembly, and it needs BOTH
    // names to be honest. The sentinel stands in for the operand where it is inlined; where the
    // assembly binds it first (`def e7 = <operand>;`) the operand is thereafter called `e7`, and
    // THAT is where the original corruption showed — `e0def arg0 = …`, two tokens fused into one
    // identifier that no lexer can flag. A first draft of this test scanned the sentinel only and
    // stayed GREEN under the mutation it claims to catch; a gate that its own falsification cannot
    // red is not a gate.
    //
    // The follow set is Painless's own: an operand may be followed by an operator, a separator, a
    // closing bracket, a member access or nothing at all — never by a bare word.
    val mayFollow = ".,;)]}?: \t=!<>+-*/%&|^".toSet
    def isIdentChar(c: Char): Boolean = c.isLetterOrDigit || c == '_' || c == '$'
    def fusions(text: String, token: String): List[String] = {
      var out = List.empty[String]
      var i = text.indexOf(token)
      while (i >= 0) {
        val before = i == 0 || !isIdentChar(text.charAt(i - 1))
        val next = i + token.length
        if (before && next < text.length && !mayFollow.contains(text.charAt(next)))
          out = out :+ text.substring(i, scala.math.min(next + 12, text.length))
        i = text.indexOf(token, next)
      }
      out
    }
    family.foreach { sql =>
      transformsOf(sql).foreach { fn =>
        val assembled = fn.toPainless(Sentinel, 7, None)
        val found = fusions(assembled, Sentinel) ++ fusions(assembled, "e7")
        withClue(s"[$sql] ${fn.getClass.getSimpleName} in [$assembled] ") {
          found shouldBe empty
        }
      }
    }
  }

  it should "have exercised at least one transform per family statement" in {
    // Guards the guard: a statement that yields no `TransformFunction` makes both loops above
    // iterate zero times and pass vacuously.
    family.foreach { sql =>
      withClue(s"[$sql] ") { transformsOf(sql) should not be empty }
    }
  }

  // -- anti-drift: the family list may not fall behind the source --------------------------------

  private val timeSourceCandidates = Seq(
    new File("sql/src/main/scala/app/softnetwork/elastic/sql/function/time/package.scala"),
    new File("src/main/scala/app/softnetwork/elastic/sql/function/time/package.scala")
  )

  private def timeSource: Option[File] = timeSourceCandidates.find(_.isFile)

  private def read(f: File): String = {
    val src = Source.fromFile(f, "UTF-8")
    try src.mkString
    finally src.close()
  }

  /** Every `case class` in the time package that is a `TransformFunction`, resolved through the
    * traits in the same file rather than hard-coded: a declaration counts when its `extends` text
    * names `TransformFunction[` directly, or names a trait already known to be one. Iterated to a
    * fixpoint so `SQLAddInterval extends AddInterval extends IntervalFunction extends
    * TransformFunction` is reached.
    */
  private def transformCaseClasses: Set[String] = {
    val text = timeSource.map(read).getOrElse("")
    // A declaration is its header: from `case class X` / `sealed trait X` up to the body's `{`.
    val decl = """(?s)\b(case class|sealed trait|trait)\s+(\w+)[^{]*""".r
    val decls = decl
      .findAllMatchIn(text)
      .map(m => (m.group(1), m.group(2), m.matched))
      .toList
    var known = Set("TransformFunction")
    var changed = true
    while (changed) {
      val next = known ++ decls.collect {
        case (_, name, header) if known.exists(k => header.contains(s"$k[")) => name
      }
      changed = next != known
      known = next
    }
    decls.collect { case ("case class", name, _) if known.contains(name) => name }.toSet
  }

  /** The runtime class AND every class it inherits from.
    *
    * 🔴 Not the simple name alone: `YEAR(x)` instantiates `Year`, which extends
    * `TimeFieldExtract(YEAR)`, which extends the declared `case class Extract`. Comparing leaf
    * names only would have reported `Extract` as uncovered while the very statement that exercises
    * its assembly was in the list — a guard failing on its own bookkeeping rather than on a real
    * gap.
    */
  private def exercisedNames: Set[String] =
    family.flatMap(transformsOf).flatMap { fn =>
      Iterator
        .iterate[Class[_]](fn.getClass)(_.getSuperclass)
        .takeWhile(_ != null)
        .map(_.getSimpleName)
    }.toSet

  behavior of "the family list"

  // NOT `assume`: a cancelled test is not a failure and `sbt test` still exits 0, so the scan
  // would pass while having scanned nothing.
  it should "have the time package on disk to scan" in {
    withClue(s"working directory = ${new File(".").getAbsolutePath} - ") {
      timeSource shouldBe defined
    }
  }

  it should "have found more than one transform case class to compare against" in {
    transformCaseClasses.size should be > 5
  }

  it should "cover every transform function declared in the time package" in {
    // The reason this file can claim to guard a FAMILY rather than four cases: a new date function
    // reds this test on the day it is added, until it is exercised above.
    (transformCaseClasses -- exercisedNames) shouldBe empty
  }
}
