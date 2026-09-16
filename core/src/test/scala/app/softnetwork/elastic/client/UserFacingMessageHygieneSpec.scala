package app.softnetwork.elastic.client

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.collection.mutable
import scala.io.Source
import scala.util.matching.Regex

/** Anti-drift guard: no INTERNAL work-item identifier may reach a customer.
  *
  * A message built in a shipped module travels verbatim: an `ElasticError.message` becomes the JDBC
  * driver's `SQLException` text and the Flight sidecar's error, a `Left(...)` reason is echoed by
  * the REPL, a macro abort IS the user's compile error, and a log line is read by whoever operates
  * the cluster. None of those readers can resolve a story number, a sprint decision id, a branch
  * name or a bare `#123` -- the product publishes none of them -- so such a reference is noise at
  * best and, when it promises future work ("wait for X, story N.M"), an unkeepable commitment.
  *
  * WHAT THIS DOES NOT COVER, recorded so nobody mistakes it for a proof:
  *   - Comments and scaladoc are deliberately EXEMPT. Recording why a line exists, by story or by
  *     issue, is how this codebase carries its own history; only text that can leave the process is
  *     in scope. That is the whole reason this scan lexes string LITERALS instead of grepping
  *     lines.
  *   - A message assembled from a value computed at runtime (a constant read from a config, an
  *     exception's own `getMessage`) is invisible here: only literal text is examined.
  *   - It matches the RECORDED SPELLINGS, not the idea. Drop the word and the same leak walks
  *     through: `"(22.6)"` and `"tracked as SC4ES-258"` match nothing below, and no regex that
  *     caught them would leave version numbers and ordinary numeric prose alone. The patterns make
  *     the habit expensive, not impossible.
  *   - `name#123` is allowed by the lookbehind -- ANY word will do, the regex cannot tell a tracker
  *     from a prefix. The intent is to permit a QUALIFIED upstream coordinate such as
  *     `elastic4s#4100`, which a reader can actually look up, while refusing a BARE `#123`, which
  *     names no tracker at all.
  *   - Only `.scala` under the trees in `scannedTrees`. Shipped **resources** are not scanned --
  *     notably the `help` corpus under `core/src/main/resources`, which the REPL's `HELP` prints
  *     verbatim, and the reference HOCON. Both were swept by hand while this was written and were
  *     clean; the help corpus has a guard of its own in `HelpCorpusSpec`.
  *   - `testkit` is excluded, and so are its four `copyTestkit` copies: their strings are ScalaTest
  *     test NAMES, and a case that pins an issue legitimately says so.
  *
  * Scanning whole directories does reach `core/src/main/scala-2.12` and `scala-2.13`, which
  * `scalafmtAll` on the default Scala version silently skips.
  */
class UserFacingMessageHygieneSpec extends AnyFlatSpec with Matchers {

  /** Each pattern paired with the name its failure clue should use. */
  private val internalReferences: Seq[(String, Regex)] = Seq(
    "a story number"       -> """(?i)\bstor(?:y|ies)\b[^.\n]{0,12}?\d""".r,
    "an epic number"       -> """(?i)\bepics?\b[^.\n]{0,12}?\d""".r,
    "a bare issue number"  -> """(?<![A-Za-z0-9_])#\d+""".r,
    "a sprint decision id" -> """\b(?:AD|PD|OQ|AC|NFR|FR)-\d+""".r,
    "an internal path"     -> """_bmad|docs/issues|/Users/""".r,
    "a branch name"        -> """\b(?:feature|fix|chore|release)/[A-Za-z0-9._-]""".r
  )

  private def offendersIn(text: String): Seq[String] =
    internalReferences.flatMap { case (what, rx) =>
      rx.findFirstIn(text).map(hit => s"$what ($hit)")
    }

  // --- what gets scanned ----------------------------------------------------------------------

  /** Walks up from the working directory to the directory holding `build.sbt` AND `sql/src/main`.
    * Unforked modules run with the repo root as CWD; a forked one gets its own base directory, and
    * `core/build.sbt` makes forking conditional on a system property, so neither can be assumed.
    * Both conditions are needed: `core/` has a `build.sbt` of its own.
    */
  private lazy val repoRoot: Option[File] = {
    def looksLikeRoot(d: File): Boolean =
      new File(d, "build.sbt").isFile && new File(d, "sql/src/main").isDirectory
    var candidate: File = new File(".").getAbsoluteFile
    var found: Option[File] = None
    while (candidate != null && found.isEmpty) {
      if (looksLikeRoot(candidate)) found = Some(candidate)
      else candidate = candidate.getParentFile
    }
    found
  }

  /** Every `src/main` tree whose messages can reach a customer: the eight library modules plus the
    * five ES client modules, minus the two kinds that are not message surfaces.
    *
    *   - `testkit/` and its four `copyTestkit` copies are OUT: their strings are ScalaTest test
    *     NAMES, and a case that pins an issue legitimately says so.
    *   - `es7|es8|es9/bridge` are OUT: they are gitignored `copyBridge` OUTPUT, present only after
    *     a build. The TEMPLATE `bridge/src/main` is scanned instead and is the only copy anyone may
    *     edit; `es6/bridge` is hand-maintained and tracked, so it is scanned in its own right.
    *
    * MEASURED, and the reason this is a written list rather than a directory walk: discovering
    * every `src/main` from the filesystem escapes the build. In a working checkout it finds an
    * untracked `benchmark/`, a leftover EMPTY `cli/src/main/scala/app` (git tracks no empty
    * directory, so `git status` never mentions it) and a vendored scratch copy of this very
    * codebase under `tmp/` -- 27 stale Scala files whose old messages would redden a guard about
    * SHIPPED text. A walk is green here and red in the author's own checkout, which is worse than
    * the drift it was meant to catch. The cost, stated: a module added later is not scanned until
    * someone adds it here, and the coverage test below is what makes that an explicit decision.
    */
  private val scannedTrees: Seq[String] = Seq(
    "bridge/src/main",
    "core/src/main",
    "es6/bridge/src/main",
    "es6/jest/src/main",
    "es6/rest/src/main",
    "es7/rest/src/main",
    "es8/java/src/main",
    "es9/java/src/main",
    "licensing/src/main",
    "macros/src/main",
    "persistence/src/main",
    "sql/src/main"
  )

  private lazy val sourceRoots: Seq[File] =
    repoRoot.toSeq.flatMap(root => scannedTrees.map(tree => new File(root, tree)))

  private def scalaFiles(dir: File): Seq[File] = {
    val (dirs, files) =
      Option(dir.listFiles).getOrElse(Array.empty[File]).toSeq.partition(_.isDirectory)
    files.filter(_.getName.endsWith(".scala")) ++ dirs.flatMap(scalaFiles)
  }

  private def read(f: File): String = {
    val src = Source.fromFile(f, "UTF-8")
    try src.mkString
    finally src.close()
  }

  /** Lexed once: the corpus is read twice otherwise, by the harvest floor and by the scan. */
  private lazy val allScalaFiles: Seq[File] = sourceRoots.flatMap(scalaFiles)

  private lazy val allLiterals: Seq[(File, ScalaStringLiterals.Literal)] =
    allScalaFiles.flatMap(f => ScalaStringLiterals.of(read(f)).map(lit => f -> lit))

  behavior of "user-facing message hygiene"

  // NOT `assume`: a cancelled ScalaTest is not a failure and `sbt test` still exits 0, so an
  // `assume`-guarded scan would let "the guard passes" and "the guard scanned nothing" both hold.
  it should "find the scanned source trees on disk" in {
    withClue(s"working directory = ${new File(".").getAbsolutePath} - ") {
      repoRoot shouldBe defined
    }
    sourceRoots.filterNot(_.isDirectory) shouldBe empty
    // Without this, an empty `sourceRoots` makes the per-tree loop below iterate zero times and
    // pass. Measured: it is the one mutation the coverage test could not see.
    sourceRoots should have size scannedTrees.size.toLong
  }

  it should "cover every tree whose messages reach a customer" in {
    // The expected set is spelled out a SECOND time on purpose. `scannedTrees` is the scan's only
    // scope, so without a copy to compare it against, deleting an entry -- the very tree this
    // guard was written for, say -- makes the offender assertion go green having stopped looking.
    withClue(s"scanned: ${scannedTrees.mkString(", ")} - ") {
      scannedTrees should contain theSameElementsAs Seq(
        "bridge/src/main",
        "core/src/main",
        "es6/bridge/src/main",
        "es6/jest/src/main",
        "es6/rest/src/main",
        "es7/rest/src/main",
        "es8/java/src/main",
        "es9/java/src/main",
        "licensing/src/main",
        "macros/src/main",
        "persistence/src/main",
        "sql/src/main"
      )
    }
    // And each one must really yield sources AND literals. The global floors below are sums, so
    // `sql` alone clears them: without this loop the extractor could return nothing for the other
    // eleven trees -- every shipped ES client among them -- and the scan would still pass.
    sourceRoots.foreach { root =>
      val prefix = root.getAbsolutePath
      withClue(s"${root.getPath} - ") {
        scalaFiles(root) should not be empty
        allLiterals.count { case (f, lit) =>
          f.getAbsolutePath.startsWith(prefix) && lit.text.trim.nonEmpty
        } should be > 0
      }
    }
  }

  it should "actually harvest message strings from those trees" in {
    // Guards the guard, at the level that matters. A file-count floor alone leaves the real hole
    // open: every assertion below still passes if the extractor returns NOTHING for every real
    // file, because the canary runs against its own synthetic sample. Tie the lexer to the corpus.
    allScalaFiles.size should be > 100
    allLiterals.count(_._2.text.trim.nonEmpty) should be > 1000
  }

  it should "tell a message string apart from a comment that says the same thing" in {
    // The canary. Everything below rests on the claim that the extractor reads STRINGS and skips
    // COMMENTS; a sample with a known answer is what makes that claim falsifiable, and it is what
    // stops a future simplification of the extractor from quietly turning the whole scan green.
    //
    // Every line carries a planted marker, and every construct here is live in the scanned trees:
    // a quote inside a comment, a char literal holding a quote, an ESCAPED char literal, an
    // escaped quote inside a message, a `//` inside a message, an `f` interpolator, an `s`
    // interpolator with a nested literal, the four-quote close (`s""" ... "$x""""`, as in
    // WatcherAction) and a plain triple-quoted string.
    val q3 = "\"\"\"" // a triple quote, spelled without one
    val sample = Seq(
      "object Sample {",
      "  // fixed by \"story 21.4\", recorded in docs/issues/250-x.md on feature/21.4",
      "  /* \"AD-7\" says: reject, never recover (#250) */",
      "  /** Scaladoc naming epic 22 and OQ-5. */",
      "  val ok     = \"UNION ALL is not supported yet. Write one subquery per branch.\"",
      "  val leak   = \"not supported yet (story 22.6)\"",
      "  val ch     = '\"'; val after = \"and then (story 9.9)\"",
      "  val esc    = '\\''; val afterEsc = \"and also (story 7.7)\"",
      "  val quoted = \"he said \\\"see story 6.6\\\" loudly\"",
      "  val slashy = \"http://x/y -- not a // comment (story 5.5)\"",
      "  val fmt    = f\"pad $n%08x then (story 4.4)\"",
      "  val nested = s\"count=${List(\"inner\").size} done\"",
      "  val run    = s" + q3 + " FOREACH \"$f\"" + q3 + "; val tail = \"end (story 3.3)\"",
      "  val tri    = " + q3 + "a doc mentioning issue #99" + q3,
      "}"
    ).mkString("\n")

    val literals = ScalaStringLiterals.of(sample).map(_.text)
    val flagged = literals.filter(text => offendersIn(text).nonEmpty)

    // Every planted leak is seen. A construct the lexer loses its place on takes the marker on its
    // OWN line with it, and usually the next one too.
    val planted = Seq(
      "story 22.6",
      "story 9.9",
      "story 7.7",
      "story 6.6",
      "story 5.5",
      "story 4.4",
      "story 3.3",
      "#99"
    )
    planted.foreach(marker => withClue(s"[$marker] ")(flagged.mkString(" ") should include(marker)))
    withClue(s"flagged = ${flagged.mkString(" | ")} - ")(
      flagged should have size planted.size.toLong
    )

    // And nothing a COMMENT said is anywhere in the harvest -- not merely unflagged. The first two
    // comment lines carry their reference INSIDE QUOTES, so a lexer that stopped skipping comments
    // would harvest them as literals; the scaladoc line and the block comment cover the other two
    // comment forms.
    val harvest = literals.mkString(" ")
    Seq("story 21.4", "AD-7", "epic 22", "OQ-5", "#250").foreach { fromAComment =>
      withClue(s"[$fromAComment] ")(harvest should not include fromAComment)
    }

    // The clean message survives intact and the literal nested inside `${ ... }` was collected in
    // its own right.
    literals should contain("UNION ALL is not supported yet. Write one subquery per branch.")
    literals should contain("inner")
  }

  it should "let a QUALIFIED upstream reference through" in {
    // `elastic4s#4100` is a real, public, lookup-able coordinate and is used verbatim in a refusal
    // message today. The rule is about a BARE `#123`; if this ever reddens, the lookbehind is gone
    // and the scan has started rejecting references a reader CAN resolve.
    offendersIn("the elastic4s builder drops the script (elastic4s#4100)") shouldBe empty
    offendersIn("resolved against the classloader (#258)") should not be empty
  }

  it should "carry no internal work-item reference in any user-facing message" in {
    val found = mutable.ListBuffer.empty[String]
    allLiterals.foreach { case (f, lit) =>
      val what = offendersIn(lit.text)
      if (what.nonEmpty)
        found += s"${f.getPath}:${lit.line}: ${what.mkString(", ")} in [${lit.text.take(160)}]"
    }
    withClue(
      "A string literal in a shipped module carries an internal reference. These strings reach " +
      "customers verbatim - an ElasticError message becomes the JDBC SQLException text and the " +
      "Flight sidecar's error, a Left(...) reason is echoed by the REPL, a macro abort IS the " +
      "user's compile error, a log line is read by the operator - and none of those readers can " +
      "resolve a story number, a decision id, a branch name or a bare issue number.\n" +
      "Keep the construct, the offending input and the actionable remedy; drop the reference. Do " +
      "NOT replace it with a date or a version promise. If the reference belongs to a public " +
      "upstream tracker, qualify it (elastic4s#4100) so a reader can find it.\n" +
      "If the text is EXPLANATION rather than a message it belongs in a comment or scaladoc, " +
      "which this scan deliberately exempts.\n" +
      "The bridge refusals live in TWO tracked copies - bridge/src/main (the copyBridge template, " +
      "scanned) and es6/bridge/src/main (hand-maintained, scanned). Fix both; never edit the " +
      "generated es7|es8|es9/bridge trees, which this scan deliberately skips.\n" +
      "SCOPE, so this clue is not over-trusted: `.scala` string LITERALS under the trees in " +
      "`scannedTrees`, matching the spellings in `internalReferences`. A message assembled at " +
      "runtime, a paraphrase that drops the keyword, shipped `resources` (the REPL help corpus, " +
      "the reference HOCON) and testkit's test names are NOT covered.\n" +
      found.mkString("\n") + "\n"
    ) {
      found.toList shouldBe empty
    }
  }
}

/** A string-literal extractor for Scala sources, interpolation-aware.
  *
  * Comments and string literals cannot be told apart by two independent passes -- a `//` inside a
  * string is not a comment, and a `"` inside a comment is not a string -- so one lexer owns both.
  * Interpolation matters for the same reason: in `s"...${options.map(o =>
  * s""","$o"""").getOrElse("")}..."` (a real line in `query/Where.scala`) the quotes inside
  * `${...}` are code, and a lexer that does not recurse there loses its place for the rest of the
  * file and reports garbage -- which is exactly what a first attempt at this scan did, on that very
  * line.
  *
  * Kept deliberately small: it recognises line comments, nested block comments, char literals,
  * simple and triple-quoted strings, and `$ident` / `${ expr }` interpolation. It is not a Scala
  * parser and does not need to be: an interpolated hole contributes a space and an escape keeps its
  * backslash, because the consumer matches words rather than reconstructing the runtime value.
  */
private[client] object ScalaStringLiterals {

  final case class Literal(line: Int, text: String)

  def of(src: String): Seq[Literal] = {
    val scan = new Scan(src)
    scan.code(stopOnCloseBrace = false)
    scan.literals.toList
  }

  private final class Scan(s: String) {
    private val n = s.length
    private var i = 0
    private var line = 1
    val literals: mutable.ListBuffer[Literal] = mutable.ListBuffer.empty

    private def at(idx: Int): Char = if (idx >= 0 && idx < n) s.charAt(idx) else ' '
    private def startsAt(idx: Int, t: String): Boolean = s.startsWith(t, idx)
    private def isIdChar(c: Char): Boolean = Character.isLetterOrDigit(c) || c == '_'

    private def bump(): Unit = {
      if (at(i) == '\n') line += 1
      i += 1
    }

    def code(stopOnCloseBrace: Boolean): Unit = {
      var depth = 0
      var done = false
      while (i < n && !done) {
        val c = s.charAt(i)
        if (stopOnCloseBrace && c == '}' && depth == 0) done = true
        else if (c == '{') { depth += 1; bump() }
        else if (c == '}') { depth -= 1; bump() }
        else if (startsAt(i, "//")) while (i < n && s.charAt(i) != '\n') i += 1
        else if (startsAt(i, "/*")) blockComment()
        else if (c == '\'') charLiteral()
        else if (c == '"') string(interpolated = i > 0 && isIdChar(at(i - 1)))
        else bump()
      }
    }

    /** Scala block comments nest. */
    private def blockComment(): Unit = {
      var depth = 1
      i += 2
      while (i < n && depth > 0) {
        if (startsAt(i, "/*")) { depth += 1; i += 2 }
        else if (startsAt(i, "*/")) { depth -= 1; i += 2 }
        else bump()
      }
    }

    /** `'a'`, `'\n'`, `'\''` and -- the one that matters most here -- `'"'`, which must not be read
      * as a string opener. Anything else (a symbol literal) advances a single character.
      *
      * The terminator search must start PAST an escaped quote, or `'\''` gives up its own closing
      * quote to the next token.
      *
      * MEASURED: reverting that `from` to `i + 2` leaves the canary GREEN, so the `'\''` line below
      * PINS the construct rather than proving this branch. The reason is that on every live
      * occurrence the stray quote is followed by `,`/space/`)`, where the next call advances one
      * character and the lexer self-heals; it mis-lexes only when the residue's `i + 2` is another
      * quote, as in `Seq('\'','x')`, which occurs nowhere in the scanned trees.
      */
    private def charLiteral(): Unit = {
      if (at(i + 1) == '\\') {
        val from = if (at(i + 2) == '\'') i + 3 else i + 2
        val close = s.indexOf('\'', from)
        if (close >= 0 && close - i <= 8) i = close + 1 else i += 1
      } else if (at(i + 2) == '\'' && at(i + 1) != '\n') i += 3
      else i += 1
    }

    private def string(interpolated: Boolean): Unit = {
      val startLine = line
      val buf = new StringBuilder
      val triple = startsAt(i, "\"\"\"")
      i += (if (triple) 3 else 1)
      var done = false
      while (i < n && !done) {
        val c = s.charAt(i)
        if (triple && startsAt(i, "\"\"\"")) {
          // A run of more than three quotes closes on the LAST three; the extras are content.
          i += 3
          while (i < n && s.charAt(i) == '"') { buf.append('"'); i += 1 }
          done = true
        } else if (!triple && c == '"') { i += 1; done = true }
        else if (!triple && c == '\\') { buf.append(s.substring(i, math.min(i + 2, n))); i += 2 }
        else if (!triple && c == '\n') done = true // unterminated: bail, never swallow the file
        else if (interpolated && c == '$') interpolation(buf)
        else {
          if (c == '\n') line += 1
          buf.append(c)
          i += 1
        }
      }
      literals += Literal(startLine, buf.toString)
    }

    /** An interpolated hole contributes no literal text of its own -- a space stands in for it so
      * the words either side do not run together. A literal nested inside `${ ... }` is collected
      * by the recursive `code` call, exactly as if it stood alone.
      */
    private def interpolation(buf: StringBuilder): Unit = {
      val next = at(i + 1)
      if (next == '{') {
        i += 2
        code(stopOnCloseBrace = true)
        if (at(i) == '}') i += 1
        buf.append(' ')
      } else if (next == '$') { buf.append('$'); i += 2 }
      else if (Character.isLetter(next) || next == '_') {
        i += 1
        while (i < n && isIdChar(s.charAt(i))) i += 1
        buf.append(' ')
      } else { buf.append('$'); i += 1 }
    }
  }
}
