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

package app.softnetwork.elastic.client.help

import app.softnetwork.elastic.sql.SQLKeywords
import app.softnetwork.elastic.sql.parser.Parser
import org.json4s.{DefaultFormats, Formats, JString, JValue}
import org.json4s.native.JsonMethods.parse
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.io.File
import scala.io.Source
import scala.util.{Failure, Success, Try}

/** Anti-drift guard for the shipped help corpus (Story 20.7, Epic 19 OQ-3).
  *
  * The corpus is walked, never enumerated: a hard-coded list of known-bad entries is precisely the
  * artefact that let phantoms #11-13 (the three `help/functions/json` docs) appear unnoticed for
  * months. Every help document on disk must name either a function spelling the parser accepts, or
  * one of the four parser words that own a help page without being functions.
  *
  * This spec IS the deliverable: it never uses `assume`, because a cancelled ScalaTest is not a
  * failure and `sbt test` still exits 0 (`project_elasticsql_build_test_facts`).
  */
class HelpCorpusSpec extends AnyFlatSpec with Matchers {

  private implicit val formats: Formats = DefaultFormats

  /** Help topics that are real parser words but are NOT functions, so they can never appear in
    * `SQLKeywords.functionWords`:
    *   - `CASE` - clause form, `function/cond/package.scala:55`, in `clauseTokens`
    *   - `INTERVAL` - clause form, `time/package.scala:162`, in `clauseTokens`
    *   - `PI` - bare-word literal, `package.scala:643`, in `literalTokens` (`SELECT PI`, not
    *     `PI()`)
    *   - `RANDOM` - bare-word literal, `package.scala:649`, in `literalTokens`
    *
    * Escape hatch for CLAUSE and LITERAL tokens only, and the escape-hatch assertion checks
    * membership against the tokens' CANONICAL `sql` spellings - never `flatMap(wordsOf)`, whose
    * split of `"LEFT\\s+OUTER"` (`query/From.scala:45`) would also admit the fragment `OUTER`,
    * which is not valid syntax alone.
    */
  private val NonFunctionTopics: Set[String] = Set("CASE", "INTERVAL", "PI", "RANDOM")

  /** Canonical single-token spellings that own a help page without being functions. */
  private val ClauseAndLiteralSql: Set[String] =
    (SQLKeywords.clauseTokens ++ SQLKeywords.literalTokens).map(_.sql.toUpperCase).toSet

  // --- corpus location ---------------------------------------------------------------
  //
  // Two views, both walked, results unioned:
  //   1. the classpath copy under `<module>/target/.../classes` - what actually ships and what
  //      `HelpJsonLoader` reads;
  //   2. the source tree - what is on disk in the repo.
  // Unioning is monotone-safe: a phantom in EITHER view fails the walk, so a stale `target/` can
  // never mask a source deletion and an uncopied resource can never mask a source addition. A
  // STALE `target/` therefore makes the walk RED, not green - remedy: `sbt core/clean`.
  //
  // AD-2b (story HELP-1a) - THE UNION IS MONOTONE-SAFE IN ONE DIRECTION ONLY. It holds for every
  // doc -> parser assertion, where an extra document can only ADD failures. It INVERTS for a
  // parser -> doc assertion, where an extra document REMOVES one: delete `pi.json` from the source
  // tree while a stale `target/` still holds it and a union-based reverse assertion stays GREEN.
  // "Every canonical function has a help document" therefore reads `sourceDocsUnder`, the SOURCE
  // view alone. Do not "restore consistency" by switching it to `docsUnder` - that reopens the hole.

  private def classpathRoot(sub: String): Option[File] =
    Option(getClass.getClassLoader.getResource(s"help/$sub"))
      .filter(
        _.getProtocol == "file"
      ) // a `jar:` URL means the corpus was packaged; the source view covers it
      .map(url => new File(url.toURI))
      .filter(_.isDirectory)

  private def sourceRoot(sub: String): Option[File] =
    Seq(
      new File(s"core/src/main/resources/help/$sub"), // sbt runs from the repo root (no `fork`)
      new File(s"src/main/resources/help/$sub") // module-local invocation
    ).find(_.isDirectory)

  private def roots(sub: String): Seq[File] =
    (classpathRoot(sub).toSeq ++ sourceRoot(sub).toSeq).distinct

  private def entries(dir: File): Seq[File] =
    Option(dir.listFiles).getOrElse(Array.empty[File]).toSeq.sortBy(_.getName)

  /** Directories that can hold a document. A directory with no `.json` file ANYWHERE beneath it is
    * skipped.
    *
    * Story HELP-1a AC 7 / reference D. sbt's resource sync leaves an EMPTY directory behind under
    * `<module>/target/scala-2.13/classes` when a category is deleted from source - that is exactly
    * what `help/functions/json` became when story 20.7 removed the four phantom documents - and the
    * classpath view then reported a category `HelpJsonLoader` does not declare. The suite went RED
    * on a normal working copy for a reason that had nothing to do with the corpus, and a red that a
    * developer learns to dismiss as noise is a guard that has stopped guarding.
    *
    * An empty directory holds no document, so it can hide nothing: filtering it leaves 20.7's
    * monotone-safe union INTACT. The two alternatives were both refused - "walk the source tree
    * instead" reverses the union decision recorded above and reopens the hole it closed, and "a
    * better failure message naming `sbt core/clean`" leaves the suite failing.
    *
    * The test is `.json` ANYWHERE beneath, not directly inside: a category holding only a
    * sub-directory of documents must stay visible to the "exactly two levels deep" assertion. And
    * `_index.json` does not count - it is not a document, so a stale category left holding only its
    * index is just as empty as one holding nothing, and would otherwise walk straight back into the
    * failure this filter exists to remove.
    */
  private def hasJsonBeneath(dir: File): Boolean =
    entries(dir).exists(f =>
      (f.isFile && f.getName.endsWith(".json") && f.getName != "_index.json") ||
      (f.isDirectory && hasJsonBeneath(f))
    )

  private def dirsOf(root: File): Seq[File] =
    entries(root).filter(_.isDirectory).filter(hasJsonBeneath)

  private def docsOf(category: File): Seq[File] =
    entries(category).filter(f =>
      f.isFile && f.getName.endsWith(".json") && f.getName != "_index.json"
    )

  private def read(f: File): String = {
    val src = Source.fromFile(f, "UTF-8")
    try src.mkString
    finally src.close()
  }

  /** Parse or fail NAMING THE FILE - a raw json4s stack trace would abort the walk with no path. */
  private def json(f: File): JValue =
    Try(parse(read(f))) match {
      case Success(v) => v
      case Failure(e) =>
        fail(s"help document is not parseable JSON: ${f.getPath} (${e.getMessage})")
    }

  private def indexEntries(category: File): List[String] = {
    val idx = new File(category, "_index.json")
    if (!idx.isFile) Nil
    else
      Try(json(idx).extract[List[String]]).getOrElse(
        fail(s"_index.json is not a JSON array of file names: ${idx.getPath}")
      )
  }

  /** (relative path, document file) for every help document under `sub`, in the given views. */
  private def docFiles(sub: String, views: Seq[File]): Seq[(String, File)] =
    for {
      root <- views
      cat  <- dirsOf(root)
      doc  <- docsOf(cat)
    } yield (s"$sub/${cat.getName}/${doc.getName}", doc)

  /** The topic a document claims: its JSON `name` field - the key `HelpJsonLoader` registers -
    * never the filename: `MONTH_OF_YEAR.sql == "MONTH"`, so filenames and tokens disagree by
    * design. (`DialectCensusSpec` keys on the FILENAME from the `sql` module; the two guards
    * deliberately watch different things.)
    */
  private def topicOfJson(jdoc: JValue, where: String): String =
    jdoc \ "name" match {
      case JString(n) => n.toUpperCase
      case other      => fail(s"help document has no string `name` field: $where ($other)")
    }

  private def topicOf(doc: File): String = topicOfJson(json(doc), doc.getPath)

  /** (relative path, uppercase topic) for every help document under `sub`, in EVERY view. */
  private def docsUnder(sub: String): Seq[(String, String)] =
    docFiles(sub, roots(sub)).map { case (rel, doc) => (rel, topicOf(doc)) }

  /** The same, restricted to the SOURCE view - the only sound basis for a parser -> doc assertion
    * (AD-2b, and the reason recorded on the union comment above).
    */
  private def sourceDocsUnder(sub: String): Seq[(String, String)] =
    docFiles(sub, sourceRoot(sub).toSeq).map { case (rel, doc) => (rel, topicOf(doc)) }

  private def functionDocs: Seq[(String, String)] = docsUnder("functions")

  /** A JSON string field, or a failure NAMING the document - a `MappingException` from json4s
    * carries no path.
    */
  private def stringAt(v: JValue, field: String, where: String): String = v \ field match {
    case JString(s) => s
    case other      => fail(s"$where: `$field` is not a JSON string ($other)")
  }

  private def stringOf(v: JValue, where: String): String = v match {
    case JString(s) => s
    case other      => fail(s"$where: expected a JSON string ($other)")
  }

  // --- assertions --------------------------------------------------------------------

  "The help corpus" should "be locatable (this spec is the deliverable, never cancelled)" in {
    withClue(
      s"help/functions found neither on the test classpath nor under the working directory " +
      s"(${new File(".").getAbsolutePath}); run from the repo root with: sbt core/testOnly *HelpCorpusSpec\n"
    ) { roots("functions") should not be empty }
    withClue("help/commands not found; same remedy\n") { roots("commands") should not be empty }
    functionDocs should not be empty
    docsUnder("commands") should not be empty
  }

  it should "be exactly two levels deep, so no document can hide from the walk" in {
    for {
      sub  <- Seq("commands", "functions")
      root <- roots(sub)
    } {
      withClue(
        s"[${root.getPath}] loose files beside the category directories - HelpJsonLoader never reads " +
        s"them and the walk never sees them: ${entries(root).filter(_.isFile).map(_.getName)}\n"
      ) {
        entries(root).filter(_.isFile) shouldBe empty
      }
      dirsOf(root).foreach { cat =>
        withClue(
          s"[${cat.getPath}] sub-directories inside a category - a document one level deeper is " +
          s"invisible to BOTH the loader and this walk: ${dirsOf(cat).map(_.getName)}\n"
        ) {
          dirsOf(cat) shouldBe empty
        }
      }
    }
  }

  "Every function help document" should "publish only syntax the parser accepts" in {
    val unexplained = functionDocs.filterNot { case (_, topic) =>
      SQLKeywords.functionWords.contains(topic) || NonFunctionTopics.contains(topic)
    }.distinct
    withClue(
      "these help documents publish syntax the parser REJECTS. Fix one of three ways: implement the " +
      "function, delete the document AND its `_index.json` entry in the same commit, or - only for a " +
      "clause/literal token - record it in NonFunctionTopics with a source citation.\n" +
      unexplained.map { case (p, t) => s"  $p -> $t" }.mkString("\n") + "\n"
    ) { unexplained shouldBe empty }
  }

  it should "map each topic to exactly one document" in {
    // `loadFunctions` ends in `.toMap` over `name.toUpperCase -> ...` and `List[(K, V)].toMap`
    // keeps the LAST pair, silently shadowing the earlier document.
    val shadowed = functionDocs.distinct.groupBy(_._2).filter(_._2.size > 1)
    withClue(
      s"topics claimed by more than one document - the last one loaded wins SILENTLY: " +
      s"${shadowed.map { case (t, ds) => s"$t <- ${ds.map(_._1).mkString(", ")}" }.mkString("; ")}\n"
    ) {
      shadowed shouldBe empty
    }
  }

  it should "keep the non-function escape hatch honest" in {
    withClue("a NonFunctionTopics entry that IS a function - drop it from the set\n") {
      NonFunctionTopics.intersect(SQLKeywords.functionWords) shouldBe empty
    }
    // Canonical clause/literal spellings only. `IF`, `TRUNCATE` and `JSON_ARRAY` (statementWords)
    // and compound fragments such as `OUTER` are all rejected, so a phantom cannot be laundered
    // here.
    withClue(
      "a NonFunctionTopics entry that is not the canonical spelling of a clause or literal token\n"
    ) {
      NonFunctionTopics.diff(ClauseAndLiteralSql) shouldBe empty
    }
    // AC-6 demonstration: the laundering candidates are rejected by the canonical-spelling gate
    // itself - `IF`, `TRUNCATE`, `JSON_ARRAY` are statementWords only, and `OUTER` exists only as
    // a fragment of `"LEFT\\s+OUTER"`-style `words` entries, never as a token's canonical `sql`.
    withClue(
      "a statementWord or compound fragment leaked into the canonical clause/literal set\n"
    ) {
      ClauseAndLiteralSql.intersect(Set("IF", "TRUNCATE", "JSON_ARRAY", "OUTER")) shouldBe empty
    }
  }

  "Every command help document" should "name a statement phrase the parser knows" in {
    // Command topics are multi-word statement phrases (`CREATE ENRICH POLICY`, `SHOW WATCHER
    // STATUS`). `allWords` is the RIGHT surface here - it unions statementWords with the clause
    // tokens that supply `SELECT`. (The "never use allWords" rule is about FUNCTION claims only.)
    val unknown = docsUnder("commands").distinct
      .map { case (path, topic) =>
        path -> topic.split("\\s+").filterNot(SQLKeywords.allWords.contains).toList
      }
      .filter(_._2.nonEmpty)
    withClue(
      "these command help documents contain words the parser does not know:\n" +
      unknown.map { case (p, ws) => s"  $p -> ${ws.mkString(", ")}" }.mkString("\n") + "\n"
    ) {
      unknown shouldBe empty
    }
  }

  "Every help category index" should "name exactly the documents present beside it" in {
    for {
      sub  <- Seq("commands", "functions")
      root <- roots(sub)
      cat  <- dirsOf(root)
    } {
      val onDisk = docsOf(cat).map(_.getName).toSet
      val indexed = indexEntries(cat).toSet
      val where = s"${root.getPath}/${cat.getName}"
      withClue(
        s"[$where] indexed but absent - HelpJsonLoader skips these SILENTLY: ${indexed.diff(onDisk)}\n"
      ) {
        indexed.diff(onDisk) shouldBe empty
      }
      withClue(
        s"[$where] present but unindexed - invisible in `.help`; this is how 10 phantoms hid: ${onDisk
          .diff(indexed)}\n"
      ) {
        onDisk.diff(indexed) shouldBe empty
      }
    }
  }

  "HelpJsonLoader" should "enumerate every category directory that exists" in {
    val checks = Seq(
      ("functions", HelpJsonLoader.functionCategories.toSet),
      ("commands", HelpJsonLoader.commandCategories.toSet)
    )
    checks.foreach { case (sub, declared) =>
      val present = roots(sub).flatMap(dirsOf).map(_.getName).toSet
      withClue(
        s"[$sub] category directories the loader never reads - silently invisible: ${present.diff(declared)}\n"
      ) {
        present.diff(declared) shouldBe empty
      }
      withClue(
        s"[$sub] categories the loader names but which do not exist: ${declared.diff(present)}\n"
      ) {
        declared.diff(present) shouldBe empty
      }
    }
  }

  // --- HELP-1a: the two probes, and the parser -> doc direction ------------------------
  //
  // Every assertion above runs doc -> parser (does this document NAME something the parser
  // accepts). None of them reads `examples[]` or `syntax[]`, and none runs parser -> doc. That is
  // why 32 of 262 published examples could be rejected by the engine shipping in the same jar -
  // including the corpus's most-copied `GROUP BY` + `COUNT(*)` example - while this suite stayed
  // green, and why `LAST_DAY`/`EPOCHDAY` could parse with no help page at all.

  private case class Snippet(path: String, where: String, sql: String)

  private def examplesUnder(sub: String): Seq[Snippet] =
    (for {
      (rel, doc) <- docFiles(sub, roots(sub))
      (ex, i)    <- (json(doc) \ "examples").children.zipWithIndex
    } yield {
      val where = s"$rel examples[$i]"
      Snippet(rel, where, stringAt(ex, "sql", where))
    }).distinct

  "Every published example" should "parse against the real Parser" in {
    val all = examplesUnder("commands") ++ examplesUnder("functions")
    // Vacuity guard: every document publishes at least one example today, so a walk that lost a
    // view - or a `docFiles` that silently returned nothing - cannot look like a pass.
    val documented =
      docFiles("commands", roots("commands")) ++ docFiles("functions", roots("functions"))
    withClue(
      "documents that published no probed example - the probe walked less than the corpus\n"
    ) {
      documented.map(_._1).toSet.diff(all.map(_.path).toSet) shouldBe empty
    }
    val rejected = all.flatMap { s =>
      Parser(s.sql) match {
        case Left(e)  => Some(s"  ${s.where}: ${s.sql}\n    -> ${e.msg}")
        case Right(_) => None
      }
    }.distinct
    withClue(
      "these PUBLISHED examples are REJECTED by the parser shipping in the same jar. A help corpus " +
      "is a reference, not a source of failing SQL: rewrite the example into a form the engine " +
      "accepts today (never delete it - a function whose only example is removed is undocumented " +
      "in practice while this suite goes green). Two contradictory lines under ONE name mean the " +
      "classpath copy is stale, not that the corpus is inconsistent - remedy: `sbt core/clean`.\n" +
      rejected.mkString("\n") + "\n"
    ) {
      rejected shouldBe empty
    }
  }

  /** Placeholder substitutions for the `syntax[]` probe.
    *
    * A syntax template is not SQL - it is a template - so the probe fills its declared parameters
    * in and wraps the result as `SELECT <line> FROM t`. NAME-keyed overrides beat the type rule:
    *   - `data_type` is typed `KEYWORD`, and the type rule would emit `CAST(a AS MONTH)`;
    *   - `DISTINCT column` is a parameter literally named with a space (see above);
    *   - `*` is a parameter literally named `*`, and the template is already valid.
    *
    * An unmapped type FAILS rather than skipping: a new parameter type must extend this table
    * deliberately, not disappear from the probe silently.
    *
    * This is a probeability compromise, not a clean bill of health, in two directions. Substituting
    * `1` for `exponent` certifies `POW(base, exponent)` while what a user actually writes -
    * `POW(rate, years)` - is rejected, because `POW`'s exponent slot accepts only an integer
    * literal; substituting a literal can therefore never catch a "must be a column" restriction
    * either. And every template is probed in PROJECTION position (`SELECT <line> FROM t`), so a
    * predicate-shaped function is certified there while its documented `WHERE` usage may still be
    * rejected - measured in this very corpus, where both `REGEXP_LIKE` examples needed `= true`
    * appended to survive a bare `WHERE`. The templates are right; the engine gaps are recorded
    * against the examples, and in the documents' own `notes`, instead.
    */
  /** Replace a placeholder on WORD BOUNDARIES where the name is word-shaped.
    *
    * `sortBy(-length)` only protects one parameter name from another; it does nothing about a name
    * occurring inside surrounding template text. `date_add` and friends declare a parameter named
    * `n` and publish a line containing `unit` - which survives today only because `unit` is longer
    * and its replacement happens to contain no lowercase `n`. A bare `String.replace` there would
    * corrupt the template and then blame the CORPUS for the probe's own defect.
    */
  private def substituteName(line: String, name: String, sub: String): String =
    if (name.matches("[A-Za-z_][A-Za-z0-9_]*"))
      line.replaceAll(
        "\\b" + java.util.regex.Pattern.quote(name) + "\\b",
        java.util.regex.Matcher.quoteReplacement(sub)
      )
    else line.replace(name, sub)

  private def substituteFor(name: String, tpe: String, where: String): Option[String] =
    name match {
      case "*"               => None
      case "data_type"       => Some("INT")
      case "DISTINCT column" => Some("DISTINCT a")
      // A placeholder NAMED `column` is an identifier slot whatever type it declares. Eight
      // aggregates type it `NUMERIC`, and the type rule would probe `AVG(column)` as `AVG(1)` -
      // green on an input nobody writes, with the identifier path never exercised. `percentile_*`
      // is worse: `WITHIN GROUP (ORDER BY 1)` is an ORDINAL since #298, so the template would be
      // certified by a positional reference to the aggregate itself.
      case "column" => Some("a")
      case _ =>
        tpe.toUpperCase match {
          case "NUMERIC" | "INT" => Some("1")
          // NOT `1`: the `double` production requires a decimal point
          // (`parser/type/package.scala:58-59`), so an integer literal in a DOUBLE slot is
          // rejected and the probe would MANUFACTURE a failure - `POINT(1, 1)` is rejected
          // where `POINT(1.0, 1.0)` parses.
          case "DOUBLE"                                => Some("1.0")
          case "VARCHAR"                               => Some("'x'")
          case "DATE" | "TIMESTAMP" | "DATE/TIMESTAMP" => Some("a")
          case "GEO_POINT" | "WILDCARD" | "ANY"        => Some("a")
          case "KEYWORD"                               => Some("MONTH")
          case other =>
            fail(
              s"$where: parameter `$name` has type `$other`, which the syntax probe's substitution " +
              s"table does not map. Add it to `substituteFor` - do not let it skip the probe."
            )
        }
    }

  /** BNF metacharacters. A template carrying any of these describes optionality or repetition and
    * is not probeable as written - which is a statement about probeability, NOT about correctness:
    * `create_table.json` `syntax[8]` was metachar-bearing AND wrong (the keyword is `PARTITION BY`,
    * `Parser.scala:367`). Such a line is fixed by inspection; this probe can never see it.
    */
  private val BnfMetachars: Set[Char] = Set('[', ']', '{', '}', '<', '>')

  /** `NonFunctionTopics` that are CLAUSE forms. Their `syntax` is block pseudo-code and the form is
    * not projectable (`SELECT a + INTERVAL 1 MONTH FROM t` parses; the bare form does not). `PI`
    * and `RANDOM` - the LITERAL non-function topics - are deliberately NOT excluded: two of the
    * three templates this probe exists to catch are theirs.
    */
  private val ClauseNonFunctionTopics: Set[String] = Set("CASE", "INTERVAL")

  "Every function syntax template" should "parse once its declared parameters are filled in" in {
    // FUNCTION syntax only. A command `syntax` array is one multi-line pseudo-code block, one line
    // per element - elements include "", ")", "alteration1," and "-- Alteration types:" - so it is
    // not SQL at any granularity and is excluded WHOLESALE. That exclusion is why
    // `create_table.json`'s `PARTITIONED BY` survived this guard and had to be fixed by inspection.
    val functionDocFiles = docFiles("functions", roots("functions"))

    // Exercise EVERY declared parameter type, whether or not its document has a probeable syntax
    // line. `substituteFor` is otherwise only reached from inside the comprehension below, so a
    // document whose every line is metachar-bearing would let a new unmapped type disappear from
    // the probe SILENTLY - which is precisely what `substituteFor`'s clue promises cannot happen.
    functionDocFiles.foreach { case (rel, doc) =>
      (json(doc) \ "parameters").children.foreach { p =>
        substituteFor(stringAt(p, "name", rel), stringAt(p, "type", rel), rel)
      }
    }

    def probeable(line: String): Boolean =
      !line.exists(BnfMetachars.contains) && !line.contains("...")

    def syntaxLines(jdoc: JValue, rel: String): Seq[(String, Int)] =
      (jdoc \ "syntax").children.zipWithIndex.map { case (l, i) =>
        (stringOf(l, s"$rel syntax[$i]"), i)
      }

    val probed = for {
      (rel, doc) <- functionDocFiles
      jdoc = json(doc)
      if !ClauseNonFunctionTopics.contains(topicOfJson(jdoc, rel))
      params = (jdoc \ "parameters").children
        .map { p =>
          (stringAt(p, "name", rel), stringAt(p, "type", rel))
        }
        .sortBy(p =>
          -p._1.length
        ) // longest first, or one name that is a substring of another corrupts it
      (line, i) <- syntaxLines(jdoc, rel)
      if probeable(line)
    } yield {
      val where = s"$rel syntax[$i]"
      val filled = params.foldLeft(line) { case (acc, (n, t)) =>
        substituteFor(n, t, where).fold(acc)(sub => substituteName(acc, n, sub))
      }
      (rel, where, line, s"SELECT $filled FROM t")
    }
    // Coverage, to the same standard as the example probe: `probed should not be empty` would stay
    // green on ONE surviving template, and between the two silent skips above (BNF metacharacters,
    // clause topics) coverage could collapse from dozens of templates to a handful with no signal.
    val shouldContribute = functionDocFiles.collect {
      case (rel, doc)
          if !ClauseNonFunctionTopics.contains(topicOf(doc)) &&
            syntaxLines(json(doc), rel).exists(l => probeable(l._1)) =>
        rel
    }.toSet
    withClue(
      "function documents that publish a probeable syntax template which the probe did not " +
      "reach - the walk covered less than the corpus\n"
    ) {
      shouldContribute.diff(probed.map(_._1).toSet) shouldBe empty
    }
    withClue("the syntax probe walked nothing\n") { probed should not be empty }
    val rejected = probed.flatMap { case (_, where, line, sql) =>
      Parser(sql) match {
        case Left(e)  => Some(s"  $where: $line\n    probed as: $sql\n    -> ${e.msg}")
        case Right(_) => None
      }
    }.distinct
    withClue(
      "these published syntax templates describe a form the parser REJECTS:\n" +
      rejected.mkString("\n") + "\n"
    ) {
      rejected shouldBe empty
    }
  }

  "Every canonical function the parser accepts" should "have a help document" in {
    // The FIRST parser -> doc assertion in this file, and the direction that makes the corpus a
    // contract rather than a snapshot. Canonical spellings only (`functionTokens.map(_.sql)`),
    // NEVER `functionWords`: ~54 of those are aliases and an alias needs no document of its own.
    // One-way, never a bijection: docs - canon is `{CASE, INTERVAL, PI, RANDOM, SAFE_CAST}`, the
    // first four `NonFunctionTopics` and the fifth an ALIAS document that legitimately exists
    // (`TRY_CAST` has its own) and which could not be laundered through `NonFunctionTopics`
    // anyway, being neither a clause nor a literal token.
    withClue(
      "the help SOURCE tree was not found; a parser -> doc assertion cannot run against the " +
      s"classpath view alone (AD-2b). Working directory: ${new File(".").getAbsolutePath}\n"
    ) {
      sourceRoot("functions") should not be empty
    }
    withClue("SQLKeywords.functionTokens is empty - this assertion would pass vacuously\n") {
      SQLKeywords.functionTokens should not be empty
    }
    val documented = sourceDocsUnder("functions").map(_._2).toSet
    val undocumented =
      SQLKeywords.functionTokens.map(_.sql.toUpperCase).toSet.diff(documented)
    withClue(
      "the parser accepts these canonical function spellings and the corpus documents none of " +
      "them - `HELP <name>` returns nothing. Add a document under `help/functions/<category>/` " +
      "AND its `_index.json` entry (`loadResourceDirectory` reads only what the index names, so " +
      "the index update is load-bearing for the SHIPPED JAR, not merely for this guard): " +
      s"${undocumented.toSeq.sorted.mkString(", ")}\n"
    ) {
      undocumented shouldBe empty
    }
  }

  "HelpJsonLoader.loadAll()" should "return a topic for every document on disk" in {
    // Every other assertion here reads the raw JSON `name` itself and never calls the loader.
    // `loadCommands`/`loadFunctions` wrap extraction in `Try { ... }.toOption`, so a missing
    // `description`, `shortDescription` or `returnType`, a `syntax` written as a String, an
    // example carrying `description` but no `title`, or a parameter with no `description` all
    // throw `MappingException` and are SILENTLY swallowed - the document satisfies every other
    // assertion in this file and still produces NOTHING at the REPL.
    val db = HelpJsonLoader.loadAll()
    val loaded = db.commands.keySet ++ db.functions.keySet
    val dropped = (docsUnder("commands") ++ docsUnder("functions")).distinct.collect {
      case (rel, topic) if !loaded.contains(topic) => s"  $rel -> $topic"
    }
    withClue(
      "these documents are on disk but `loadAll()` returns no topic for them - the loader's " +
      "`Try { ... }.toOption` swallowed a MappingException, or the document is missing from its " +
      "`_index.json`. Check every required field of `SqlCommandJson`/`FunctionJson`, including " +
      "each example's `title` and each parameter's `description`.\n" +
      dropped.mkString("\n") + "\n"
    ) {
      dropped shouldBe empty
    }
  }

  "Every command help document" should "map each topic to exactly one document" in {
    // `loadCommands` shadows exactly as `loadFunctions` does - `List[(K, V)].toMap` keeps the LAST
    // pair - and the pre-HELP-1a duplicate-topic guard covered functions only.
    val shadowed = docsUnder("commands").distinct.groupBy(_._2).filter(_._2.size > 1)
    withClue(
      s"topics claimed by more than one document - the last one loaded wins SILENTLY: " +
      s"${shadowed.map { case (t, ds) => s"$t <- ${ds.map(_._1).mkString(", ")}" }.mkString("; ")}\n"
    ) {
      shadowed shouldBe empty
    }
  }

  "Every seeAlso pointer" should "name a documented topic" in {
    // `HelpDatabase.getHelp` can only resolve a topic that has a document (the `aliases` map is
    // built empty today), so a `seeAlso` naming an accepted ALIAS - `TO_DATE`, `TO_TIMESTAMP` -
    // is as dead at the REPL as one naming something that does not exist at all. Point at the
    // canonical documented spelling instead of dropping the pointer.
    //
    // AD-2b applies to the TARGET set, exactly as it does to the parser -> doc assertion: a
    // pointer resolves against documents, so an extra document REMOVES a failure and a stale
    // `target/` holding a deleted document would keep a now-dangling pointer green. Targets come
    // from the SOURCE view; the pointers themselves keep the union, where an extra document can
    // only ADD a failure.
    for (sub <- Seq("commands", "functions"))
      withClue(
        s"the help SOURCE tree for `$sub` was not found; `seeAlso` targets cannot be resolved " +
        s"against the classpath view alone (AD-2b). Working directory: ${new File(".").getAbsolutePath}\n"
      ) {
        sourceRoot(sub) should not be empty
      }
    val documented = (sourceDocsUnder("commands") ++ sourceDocsUnder("functions")).map(_._2).toSet
    val dangling = (for {
      sub        <- Seq("commands", "functions")
      (rel, doc) <- docFiles(sub, roots(sub))
      (target, i) <- (json(doc) \ "seeAlso").children.zipWithIndex.map { case (t, i) =>
        (stringOf(t, s"$rel seeAlso[$i]"), i)
      }
      if !documented.contains(target.toUpperCase)
    } yield s"  $rel seeAlso[$i] -> $target").distinct
    withClue(
      "these `seeAlso` pointers name no documented topic, so `HELP <target>` returns nothing:\n" +
      dangling.mkString("\n") + "\n"
    ) {
      dangling shouldBe empty
    }
  }
}
