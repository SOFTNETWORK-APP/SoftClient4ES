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
import org.json4s.{DefaultFormats, Formats, JValue}
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

  private def dirsOf(root: File): Seq[File] = entries(root).filter(_.isDirectory)

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

  /** (relative path, uppercase topic) for every help document under `sub`, in every view. The topic
    * is the JSON `name` field - the key `HelpJsonLoader` registers - never the filename:
    * `MONTH_OF_YEAR.sql == "MONTH"`, so filenames and tokens disagree by design.
    */
  private def docsUnder(sub: String): Seq[(String, String)] =
    for {
      root <- roots(sub)
      cat  <- dirsOf(root)
      doc  <- docsOf(cat)
    } yield {
      val topic = json(doc) \ "name" match {
        case org.json4s.JString(n) => n.toUpperCase
        case other => fail(s"help document has no string `name` field: ${doc.getPath} ($other)")
      }
      (s"${cat.getName}/${doc.getName}", topic)
    }

  private def functionDocs: Seq[(String, String)] = docsUnder("functions")

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
}
