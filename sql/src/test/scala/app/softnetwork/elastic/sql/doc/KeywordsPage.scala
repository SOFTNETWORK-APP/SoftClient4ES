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

package app.softnetwork.elastic.sql.doc

import app.softnetwork.elastic.sql.{SQLKeywords, TokenRegex}
import app.softnetwork.elastic.sql.function.convert.CastOperator
import app.softnetwork.elastic.sql.parser.Parser

import java.io.File
import java.nio.charset.{CodingErrorAction, StandardCharsets}
import java.nio.file.{Files, StandardCopyOption}
import java.util.Locale

/** Generator for the keyword tables of `documentation/sql/keywords.md`.
  *
  * WHY THIS EXISTS. The page used to be hand-maintained, and it had drifted in BOTH directions:
  * words the parser reserves were missing from it, and words it listed as "reserved words" are
  * merely recognised. Worse, its one-dimensional list could not express the distinction at all -
  * and since story 22.2 the distinction is load-bearing (`EXISTS` is reserved, so a column named
  * `exists` needs quoting; `ANY`/`SOME` were deliberately left unreserved so a column named `any`
  * keeps parsing). A generated table carrying a per-word reserved flag is correct by construction.
  *
  * WHAT IS GENERATED, AND WHAT IS NOT. Only the region between [[KeywordsPage.BeginMarker]] and
  * [[KeywordsPage.EndMarker]] is generated. Everything outside those two markers is hand-written
  * prose, is read back verbatim by [[render]], and survives regeneration untouched - the
  * explanation of what "reserved" means belongs to a human, the lists do not.
  *
  * SOURCES OF TRUTH, none of them copied here:
  *   - `SQLKeywords` supplies the words (its own scaladoc lists what it deliberately does not
  *     enumerate; `SQLKeywordsSpec` guards it against the parser sources, though note that its
  *     source-scan tests `assume` their input and therefore cancel rather than fail when the source
  *     tree is absent);
  *   - `Parser.reservedKeywords` supplies the `reserved` flag;
  *   - the PARSER ITSELF supplies the `shadowed` flag - see [[BareReading]]. Measuring it rather
  *     than asserting it is what caught `CURDATE`, `CURTIME`, `NULL` and `RANDOM`, four words that
  *     are not reserved and still cannot be used as a bare column name.
  *
  * DETERMINISM. Every input is a `Set` or a `List` that may contain duplicates, so nothing is
  * emitted in iteration order: rows are `sorted` by keyword with `String`'s natural ordering
  * (UTF-16 code units - locale-independent), and the keywords are `[A-Z0-9_]` only. `_` (0x5F)
  * sorts after `Z` (0x5A), so `DATEADD` precedes `DATE_ADD`; that is stable, which is all the guard
  * needs. Every `toUpperCase`/`toLowerCase` here passes `Locale.ROOT`.
  *
  * Known boundary, not fixed here: `SQLKeywords.wordsOf` uppercases with the DEFAULT locale and
  * then keeps only `[A-Z][A-Z0-9_]*`, so under a Turkish default locale every word containing `i`
  * would be dropped from the registry itself - which would move this page and the REPL highlighter
  * together. Fixing it means changing `SQLKeywords`, which is out of this page's scope; the guard
  * would redden rather than ship a short page.
  */
object KeywordsPage {

  /** Path of the generated page, relative to the repository root (= the CWD of the unforked `sql`
    * tests).
    */
  val PagePath = "documentation/sql/keywords.md"

  val BeginMarker = "<!-- BEGIN GENERATED KEYWORDS -->"
  val EndMarker = "<!-- END GENERATED KEYWORDS -->"

  /** Printed by the guard when the committed page is stale, and by the page itself. A failure that
    * does not say how to fix it gets fixed by hand, which is the defect this replaces.
    * `KeywordsPageSpec` pins this string against the alias actually declared in `build.sbt`, so the
    * two cannot drift.
    */
  val RegenerateCommand = "sbt regenerateKeywordsPage"

  /** How a BARE occurrence of a word reads in `SELECT <word> FROM t`. */
  sealed trait BareReading
  object BareReading {

    /** It is a column of that name - the word is usable as a bare identifier. */
    case object Column extends BareReading

    /** It parses, but as something else (a literal, a zero-argument function, ...), so a column of
      * that name is unreachable without quoting even though the word is not reserved.
      */
    case object Shadowed extends BareReading

    /** It does not parse at all - what every reserved word does. */
    case object Rejected extends BareReading
  }

  /** A keyword and everything the page says about it. */
  final case class Entry(
    word: String,
    reserved: Boolean,
    shadowed: Boolean,
    kinds: Seq[String]
  ) {
    def kind: String = kinds.mkString(", ")

    /** The `Reserved` cell. Three values, because two are not enough: a word can be unreserved and
      * still not usable as a bare column name.
      */
    def reservedCell: String =
      if (reserved) "yes" else if (shadowed) "no (shadowed)" else "no"
  }

  /** Kind label for a function token, keyed on the leaf of its PACKAGE - never on the word, so a
    * new function needs no edit here. `function.time` and the temporal extractors' own `sql.time`
    * both land on `time`. Note the flip side: moving a token to another package relabels its row.
    */
  private val functionKindByPackage: Map[String, String] = Map(
    "aggregate" -> "aggregate function",
    "cond"      -> "conditional function",
    "convert"   -> "conversion function",
    "geo"       -> "geo function",
    "math"      -> "math function",
    "string"    -> "string function",
    "time"      -> "temporal function"
  )

  /** Leaf of the package a token object is declared in. A nested case object renders as
    * `a.b.c.Outer$INNER$`, so taking everything before the last `.` yields the package either way.
    */
  private def packageLeaf(token: TokenRegex): String = {
    val name = token.getClass.getName
    val pkg = name.lastIndexOf('.') match {
      case -1 => ""
      case i  => name.substring(0, i)
    }
    pkg.lastIndexOf('.') match {
      case -1 => pkg
      case i  => pkg.substring(i + 1)
    }
  }

  private def functionKind(token: TokenRegex): String =
    functionKindByPackage.getOrElse(
      packageLeaf(token),
      throw new IllegalStateException(
        s"No kind label for function token ${token.getClass.getName} (package leaf " +
        s"'${packageLeaf(token)}'). Add it to KeywordsPage.functionKindByPackage."
      )
    )

  /** Token spellings made of punctuation rather than letters.
    *
    * These CANNOT come from `SQLKeywords`: `wordsOf` filters any spelling that is not
    * `[A-Z][A-Z0-9_]*`, deliberately, and neither `CastOperator` nor its siblings are in the
    * registry's token lists at all. So this is a curated list - but of token OBJECTS, which is the
    * same compile-checked pattern `SQLKeywords` itself uses, not of strings.
    *
    * `||` (`function.string.Pipe`) is deliberately ABSENT: the token exists but no production
    * accepts it - `SELECT a || b FROM t` is a parse error, measured. Publishing it would be a wrong
    * claim. `KeywordsPageSpec` pins that rejection, so if `||` ever starts working the guard
    * reddens instead of the page staying quietly incomplete.
    */
  private val symbolicTokens: List[(String, String)] = List(
    unescape(CastOperator.sql) -> "cast operator, e.g. `'125'::BIGINT`"
  )

  /** `TokenRegex` spellings are regex source, so punctuation arrives backslash-escaped. */
  private def unescape(regexSource: String): String = regexSource.replaceAll("""\\(.)""", "$1")

  /** Every word the page lists, with the kinds it owes its presence to. A word may have several
    * (`LIMIT` is both a clause token and a `keyword("LIMIT")` statement word).
    */
  private def kindsByWord: Map[String, Seq[String]] = {
    val pairs: Seq[(String, String)] =
      SQLKeywords.clauseTokens.flatMap(t =>
        SQLKeywords
          .wordsOf(t)
          .map(w => w -> (if (packageLeaf(t) == "operator") "operator" else "clause"))
      ) ++
      SQLKeywords.functionTokens.flatMap(t =>
        SQLKeywords.wordsOf(t).map(w => w -> functionKind(t))
      ) ++
      SQLKeywords.literalTokens.flatMap(t => SQLKeywords.wordsOf(t).map(w => w -> "literal")) ++
      SQLKeywords.literalWords.toSeq.map(w => w -> "literal") ++
      SQLKeywords.statementWords.toSeq.map(w => w -> "statement") ++
      SQLKeywords.typeWords.toSeq.map(w => w -> "type") ++
      SQLKeywords.timeUnitPluralWords.toSeq.map(w => w -> "time unit")

    val grouped = pairs.groupBy(_._1).map { case (w, ps) => w -> ps.map(_._2).distinct.sorted }

    // A word that is reserved but backed by no grammar surface at all is still a word you cannot
    // use bare, so the page must list it. At this baseline these are FORMAT_DATE, FORMAT_DATETIME
    // and CURRENT_DATETIME (SQLKeywordsSpec pins the same three). Naming the kind "reserved only"
    // makes them visible instead of silently absent.
    val orphans = reservedWords.diff(grouped.keySet).map(w => w -> Seq("reserved only")).toMap
    grouped ++ orphans
  }

  /** `Parser.reservedKeywords`, uppercased. The list is matched case-insensitively by the parser.
    */
  def reservedWords: Set[String] = Parser.reservedKeywords.map(_.toUpperCase(Locale.ROOT)).toSet

  /** Runs the word through the parser in the one position the `Reserved` column is about.
    *
    * This is the oracle the column MEANS, rather than a restatement of the list it is derived from,
    * which is why it is computed and not assumed.
    */
  def bareReading(word: String): BareReading = {
    val bare = word.toLowerCase(Locale.ROOT)
    val statement = s"SELECT $bare FROM t"
    Parser(statement) match {
      case Right(parsed) if parsed.sql == statement => BareReading.Column
      case Right(_)                                 => BareReading.Shadowed
      case Left(_)                                  => BareReading.Rejected
    }
  }

  def entries: Seq[Entry] =
    kindsByWord.toSeq
      .map { case (w, kinds) =>
        val reserved = reservedWords.contains(w)
        Entry(w, reserved, shadowed = !reserved && bareReading(w) != BareReading.Column, kinds)
      }
      .sortBy(_.word)

  /** Multi-word keyword phrases carried by a SINGLE token, e.g. `ORDER BY`, `IS NOT NULL`, `UNION
    * ALL`, `LEFT OUTER`. Constructions assembled from two tokens (`NOT IN`, `MATCH ... AGAINST`,
    * `WITHIN GROUP`) are structurally invisible here and live in the page's hand-written prose
    * instead.
    */
  def phrases: Seq[String] = SQLKeywords.compoundPhrases.toSeq.sorted

  /** The generated region, markers included. */
  def generatedSection: String = {
    val rows = entries
    val reservedCount = rows.count(_.reserved)
    val sb = new StringBuilder
    sb.append(BeginMarker).append('\n')
    sb.append("<!--\n")
    sb.append("  GENERATED - DO NOT EDIT THIS SECTION BY HAND.\n")
    sb.append(s"  Regenerate with: $RegenerateCommand\n")
    sb.append("  Source: sql/src/test/scala/app/softnetwork/elastic/sql/doc/KeywordsPage.scala,\n")
    sb.append("  derived from SQLKeywords (words), Parser.reservedKeywords (reserved flag) and\n")
    sb.append("  the parser itself (shadowed flag).\n")
    sb.append("  Text OUTSIDE the two markers is hand-written and is preserved by regeneration.\n")
    sb.append("-->\n\n")

    sb.append("## Keyword index\n\n")
    sb.append(s"${rows.size} keywords, of which $reservedCount are reserved.\n\n")
    sb.append("| Keyword | Reserved | Kind |\n")
    sb.append("| --- | --- | --- |\n")
    rows.foreach(e => sb.append(s"| `${e.word}` | ${e.reservedCell} | ${e.kind} |\n"))

    sb.append("\n## Compound phrases\n\n")
    sb.append(
      s"${phrases.size} multi-word phrases carried by a single keyword token. Each component word " +
      "has its own row above. Constructions assembled from two separate keywords are listed under " +
      "[Multi-keyword constructions](#multi-keyword-constructions).\n\n"
    )
    sb.append("| Phrase |\n")
    sb.append("| --- |\n")
    phrases.foreach(p => sb.append(s"| `$p` |\n"))

    sb.append("\n## Symbolic operators\n\n")
    sb.append("| Operator | Meaning |\n")
    sb.append("| --- | --- |\n")
    symbolicTokens
      .sortBy(_._1)
      .foreach { case (op, meaning) => sb.append(s"| `$op` | $meaning |\n") }

    sb.append('\n').append(EndMarker)
    sb.toString
  }

  /** Splits a page into the prose before the generated region and the prose after it. */
  def split(page: String): (String, String) = {
    val begin = page.indexOf(BeginMarker)
    val end = page.indexOf(EndMarker)
    require(
      begin >= 0 && end > begin,
      s"$PagePath must contain '$BeginMarker' then '$EndMarker'"
    )
    require(
      page.indexOf(BeginMarker, begin + 1) < 0 && page.indexOf(EndMarker, end + 1) < 0,
      s"$PagePath must contain each marker exactly once"
    )
    (page.substring(0, begin), page.substring(end + EndMarker.length))
  }

  /** The region between the markers of an existing page, markers included. */
  def committedSection(page: String): String = {
    val (before, after) = split(page)
    page.substring(before.length, page.length - after.length)
  }

  /** The page as it should be on disk: the committed prose with a freshly generated middle. */
  def render(page: String): String = {
    val (before, after) = split(page)
    before + generatedSection + after
  }

  /** The repository root, found by walking up from the working directory until [[PagePath]] is
    * there. A bare `new File(".")` assumes the unforked-test CWD, which is true today and is a
    * comment away from not being: a later `Test / fork := true` on `sql`, or an IDE run
    * configuration defaulting to the module directory, both relocate the CWD and would fail half
    * this suite on a diff that has nothing to do with the page. Falls back to the working directory
    * so the "not found" message still names it.
    */
  def repoRoot: File = {
    var dir = new File(".").getAbsoluteFile
    while (dir != null && !new File(dir, PagePath).isFile) dir = dir.getParentFile
    if (dir == null) new File(".").getAbsoluteFile else dir
  }

  def pageFile(root: File = repoRoot): File = new File(root, PagePath)

  /** Decodes STRICTLY. The lenient `new String(bytes, UTF_8)` replaces every malformed byte with
    * U+FFFD, and since the page is written back whole, a lenient read followed by a write would
    * silently destroy hand-written prose this tool promises not to touch.
    */
  def read(file: File): String = {
    val decoder = StandardCharsets.UTF_8
      .newDecoder()
      .onMalformedInput(CodingErrorAction.REPORT)
      .onUnmappableCharacter(CodingErrorAction.REPORT)
    val bytes = java.nio.ByteBuffer.wrap(Files.readAllBytes(file.toPath))
    try decoder.decode(bytes).toString
    catch {
      case e: java.nio.charset.CharacterCodingException =>
        throw new IllegalStateException(
          s"${file.getPath} is not valid UTF-8; refusing to rewrite it, which would replace the " +
          s"offending bytes with U+FFFD and lose content: ${e.getMessage}",
          e
        )
    }
  }

  /** Writes via a sibling temporary file and a move, so an interrupted run cannot leave the page
    * truncated.
    */
  def write(file: File, content: String): Unit = {
    val tmp = new File(file.getParentFile, file.getName + ".tmp")
    Files.write(tmp.toPath, content.getBytes(StandardCharsets.UTF_8))
    Files.move(tmp.toPath, file.toPath, StandardCopyOption.REPLACE_EXISTING)
    ()
  }
}

/** `sbt regenerateKeywordsPage` - rewrites the generated region of `documentation/sql/keywords.md`
  * in place, leaving the hand-written prose alone.
  *
  * Deliberately throws rather than calling `sys.exit`: the `sql` project is not forked, so
  * `sys.exit` would go through sbt's `TrapExit` SecurityManager (deprecated for removal on JDK 17).
  */
object KeywordsPageGenerator {
  def main(args: Array[String]): Unit = {
    val file = KeywordsPage.pageFile()
    if (!file.isFile) {
      throw new IllegalStateException(
        s"${KeywordsPage.PagePath} not found. Run this from the repository root " +
        s"(working directory was ${new File(".").getAbsolutePath})."
      )
    }
    val current = KeywordsPage.read(file)
    val rendered = KeywordsPage.render(current)
    if (rendered == current) {
      println(s"[keywords] ${KeywordsPage.PagePath} already up to date")
    } else {
      KeywordsPage.write(file, rendered)
      println(s"[keywords] regenerated ${KeywordsPage.PagePath}")
    }
  }
}
