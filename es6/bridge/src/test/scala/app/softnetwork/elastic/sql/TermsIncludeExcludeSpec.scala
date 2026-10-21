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

package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.Es68TermsFilterOracle._
import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.{BucketIncludesExcludes, SingleSearch}
import com.sksamuel.elastic4s.http.search.SearchBodyBuilderFn
import com.sksamuel.elastic4s.searches.aggs.{NestedAggregation, TermsAggregation}
import org.apache.lucene.util.BytesRef
import org.elasticsearch.search.DocValueFormat
import org.elasticsearch.search.aggregations.bucket.terms.{IncludeExclude => Es68IncludeExclude}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.util.{Success, Try}

/** What Elasticsearch 6.8 KEEPS for every GROUP BY key filter the es6 bridge emits, decided by
  * [[Es68TermsFilterOracle]] (Elasticsearch 6.8.23's own code, in process).
  *
  * The oracle is the SQL meaning -- never the engine. Every test that feeds the calibration against
  * a real Elasticsearch 6.8.23 run COLLECTS its wrong cells and asserts the list once, so a red run
  * is a per-cell map, not the first failure. The alphabet is swept at RUN level by
  * `GroupByCompletenessSpec` (testkit).
  */
class TermsIncludeExcludeSpec extends AnyFlatSpec with Matchers {

  implicit private val timestamp: Long = 1767139200000L // 2025-12-31T00:00:00Z

  private def single(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  /** What the ES 6 bridge sends, through the DEFAULT serializer (which carries the refusal). */
  private def queryOf(sql: String): String = requestToElasticSearchRequest(single(sql)).query

  /** The same request rendered AROUND the serializer: what 6.8 would receive without a refusal. */
  private def rawQueryOf(sql: String): String =
    SearchBodyBuilderFn(requestToElasticSearchRequest(single(sql)).search).string()

  // ------------------------------------------------------------------ the population (alphabet)

  private val valuesUnderTest: Seq[String] =
    Seq("a.b", "a*", "a+b", "(x)", "[a]", "a|b", "a@b", "a\"b", "a\\b", "it's", "^a$", "\u00e9", "")

  /** Keys the REGEX reading of some value also keeps -- they make the two readings disagree. */
  private val decoys: Seq[String] = Seq("axb", "a", "aa", "ab", "aab", "x", "b", "its")

  private val stringKeys: Seq[String] = valuesUnderTest ++ decoys

  private final case class KeyOp(sql: String, include: Boolean, listForm: Boolean) {
    def apply(key: String, literal: String): String =
      if (listForm) s"$key $sql ($literal)" else s"$key $sql $literal"
  }

  private val keyOps: Seq[KeyOp] = Seq(
    KeyOp("=", include = true, listForm = false),
    KeyOp("IN", include = true, listForm = true),
    KeyOp("<>", include = false, listForm = false),
    KeyOp("NOT IN", include = false, listForm = true)
  )

  private def sqlString(v: String): String = "'" + v.replace("'", "''") + "'"

  private def keyed(field: String, predicate: String): String =
    s"SELECT $field, COUNT(*) AS c FROM t GROUP BY $field HAVING $predicate"

  // ============================================== PART A: the emission, through 6.8's own code

  "a ONE-value key filter on a keyword key" should "keep EXACTLY the value on Elasticsearch 6.8" in {
    val cells = for {
      v  <- valuesUnderTest
      op <- keyOps
      if !(v.isEmpty && !op.listForm) // `= ''` / `<> ''`: refused before emission, see below
    } yield (v, op)
    cells should have size (valuesUnderTest.size * keyOps.size - 2).toLong
    val wrong = cells.flatMap { case (v, op) =>
      val sql = keyed("sval", op("sval", sqlString(v)))
      val expected = Kept(if (op.include) Set(v) else stringKeys.toSet - v)
      val actual = Try(termsBody(queryOf(sql), "sval")).map(keptBy(_, KeywordKey, stringKeys))
      if (actual == Success(expected)) None else Some(s"[$sql] expected $expected, got $actual")
    }
    withClue(s"${wrong.size} of ${cells.size} cells wrong:\n${wrong.mkString("\n")}\n") {
      wrong shouldBe empty
    }
  }

  it should "reach the channel for every cell but the two the parser refuses on every major" in {
    // A POPULATION guard, not a contract about the empty string: if `= ''` ever reaches the key
    // filter, move those two cells into the sweep above instead of editing this line.
    val refused = for {
      v  <- valuesUnderTest
      op <- keyOps
      if Parser(keyed("sval", op("sval", sqlString(v)))).isLeft
    } yield op("sval", sqlString(v))
    refused shouldBe Seq("sval = ''", "sval <> ''")
  }

  "a ONE-value key filter on a numeric or boolean key" should "be exact, never a failed search, on 6.8" in {
    val cases: Seq[(String, KeyType, Seq[String], String, Seq[KeyOp])] = Seq(
      ("lval", LongKey, Seq("1", "2", "10", "21"), "1", keyOps),
      ("dval", DoubleKey, Seq("1.5", "2.5", "10.5"), "1.5", keyOps),
      // the IN grammar takes string, long and double lists only
      ("bval", BooleanKey, Seq("true", "false"), "true", keyOps.filterNot(_.listForm))
    )
    val wrong = for {
      (field, keyType, keys, v, ops) <- cases
      op                             <- ops
      sql = keyed(field, op(field, v))
      expected = Kept(if (op.include) Set(v) else keys.toSet - v)
      actual = Try(termsBody(queryOf(sql), field)).map(keptBy(_, keyType, keys))
      if actual != Success(expected)
    } yield s"[$sql] expected $expected, got $actual"
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  private val aKeys: Set[String] = stringKeys.filter(_.startsWith("a")).toSet

  /** {one value, several values, pattern}^2 over (kept, removed): the 3 x 3 of what one `terms`
    * level can carry, written out because each cell needs a hand-computed oracle -- and GUARDED by
    * the kind-classification test below, which derives every cell's kinds from the real
    * `Criteria.includes` / `excludes`. Two of the nine are the pairs 6.8 cannot apply together.
    */
  private val channelCells: Seq[(String, String, Set[String])] = Seq(
    ("V1+V1", "sval = 'a.b' AND sval <> 'axb'", Set("a.b")),
    ("V1+Vn", "sval = 'a.b' AND sval NOT IN ('axb','b')", Set("a.b")),
    ("V1+P", "sval = 'a.b' AND sval NOT LIKE 'x%'", Set("a.b")),
    ("Vn+V1", "sval IN ('a.b','axb') AND sval <> 'axb'", Set("a.b")),
    ("Vn+Vn", "sval IN ('a.b','axb','b') AND sval NOT IN ('axb','x')", Set("a.b", "b")),
    ("Vn+P", "sval IN ('a.b','axb') AND sval NOT LIKE 'x%'", Set("a.b", "axb")),
    ("P+V1", "sval LIKE 'a%' AND sval <> 'a.b'", aKeys - "a.b"),
    ("P+Vn", "sval LIKE 'a%' AND sval NOT IN ('a.b','axb')", aKeys -- Set("a.b", "axb")),
    ("P+P", "sval LIKE 'a%' AND sval NOT LIKE 'a.%'", aKeys - "a.b")
  )

  private val unmixableOnSix: Set[String] = Set("Vn+P", "P+Vn")

  "the 3 x 3 channel population" should "be exactly {V1, Vn, P}^2, every cell carrying the kinds its label names" in {
    // A control (green on main too): the labels must cover the DERIVED product, and each cell's
    // kinds are read from the mechanism, so a mislabelled or a missing cell fails here.
    def kind(side: BucketIncludesExcludes): String =
      if (side.regex.exists(_.nonEmpty)) "P"
      else if (side.values.size == 1) "V1"
      else if (side.values.size > 1) "Vn"
      else "-"
    val kinds = Seq("V1", "Vn", "P")
    channelCells.map(_._1).toSet shouldBe (for { k <- kinds; e <- kinds } yield s"$k+$e").toSet
    channelCells should have size (kinds.size * kinds.size).toLong
    val mislabelled = channelCells.flatMap { case (label, predicate, _) =>
      val s = single(keyed("sval", predicate))
      val bucket = s.buckets.headOption.getOrElse(fail(s"no bucket in [$predicate]"))
      val criteria = s.having.flatMap(_.criteria).getOrElse(fail(s"no HAVING in [$predicate]"))
      val actual =
        s"${kind(criteria.includes(bucket, not = false, BucketIncludesExcludes()))}+" +
        s"${kind(criteria.excludes(bucket, not = false, BucketIncludesExcludes()))}"
      if (actual == label) None else Some(s"[$label] $predicate carries $actual")
    }
    withClue(mislabelled.mkString("\n")) { mislabelled shouldBe empty }
  }

  "the kept and removed lists of one key filter" should "combine exactly, as Elasticsearch 6.8 reads them" in {
    // Collected: on the previous spelling this listed V1+V1, V1+Vn, V1+P, Vn+V1 and P+V1.
    val wrong =
      channelCells.filterNot(c => unmixableOnSix(c._1)).flatMap { case (label, predicate, kept) =>
        val sql = keyed("sval", predicate)
        val actual = Try(termsBody(queryOf(sql), "sval")).map(keptBy(_, KeywordKey, stringKeys))
        if (actual == Success(Kept(kept))) None
        else Some(s"[$label] $sql expected ${Kept(kept)}, got $actual")
      }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  it should "leave to 6.8's parser exactly the two pairs it rejects (the refusal bypassed)" in {
    // On the previous spelling, the same five cells as above failed (V1+Vn / Vn+V1 `Cannot mix`,
    // V1+V1 / V1+P / P+V1 regex-wrong); now only Vn+P and P+Vn fail, in 6.8's own parser.
    val wrong = channelCells.flatMap { case (label, predicate, kept) =>
      val raw = Try(termsBody(rawQueryOf(keyed("sval", predicate)), "sval"))
        .map(keptBy(_, KeywordKey, stringKeys))
      val ok =
        if (unmixableOnSix(label)) raw match {
          case Success(Failed(400, reason)) => reason.contains("Cannot mix")
          case _                            => false
        }
        else raw == Success(Kept(kept))
      if (ok) None else Some(s"[$label] raw: $raw")
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  "a key filter on a scripted numeric key" should
  "keep on Elasticsearch 6.8 exactly the texts its script value prints for the number" in {
    // A script key is a STRING terms: 6.8 compares each value with the text the script value
    // prints, a Long `5` or a Double `5.0` depending on the column's mapping.
    val in = termsBody(
      queryOf(
        "SELECT CAST(n AS DOUBLE) AS k, COUNT(*) AS c FROM t GROUP BY CAST(n AS DOUBLE) " +
        "HAVING CAST(n AS DOUBLE) IN (5, 1)"
      ),
      "k"
    )
    keptBy(in, KeywordKey, Seq("5", "5.0", "1", "1.0", "2", "2.0", "15", "5.5")) shouldBe
    Kept(Set("5", "5.0", "1", "1.0"))
    // one literal: its two texts are one exact set, never a pattern
    val one = termsBody(
      queryOf(
        "SELECT CAST(n AS DOUBLE) AS k, COUNT(*) AS c FROM t GROUP BY CAST(n AS DOUBLE) " +
        "HAVING CAST(n AS DOUBLE) <> 3"
      ),
      "k"
    )
    keptBy(one, KeywordKey, Seq("3", "3.0", "30", "3x0", "4")) shouldBe Kept(Set("30", "3x0", "4"))
  }

  // ============================================ PART B: the refusal and the one writing site

  "the Default serializer" should "refuse, by name, exactly the pairs 6.8 rejects -- at any grouping level" in {
    // The second-level row needs the recursion into `terms.subaggs`: a single-level cell
    // cannot see it.
    val secondLevel =
      "SELECT lval, sval, COUNT(*) AS c FROM t GROUP BY lval, sval " +
      "HAVING sval LIKE 'a%' AND sval NOT IN ('a.b','axb')"
    val refused =
      channelCells.filter(c => unmixableOnSix(c._1)).map { case (label, predicate, _) =>
        label -> keyed("sval", predicate)
      } :+ ("second grouping level" -> secondLevel)
    val wrong = refused.flatMap { case (label, sql) =>
      Try(queryOf(sql)) match {
        case scala.util.Failure(e: UnsupportedOperationException)
            if e.getMessage == SearchBodySerializer.unmixableTermsFilterUnsupported("sval") =>
          None
        case other => Some(s"[$label] expected the named refusal, got $other")
      }
    }
    // ... and every OTHER cell of the 3 x 3 is emitted, never refused (0 over-refused)
    val overRefused =
      channelCells.filterNot(c => unmixableOnSix(c._1)).flatMap { case (label, predicate, _) =>
        Try(queryOf(keyed("sval", predicate))) match {
          case Success(_) => None
          case other      => Some(s"[$label] must be emitted, got $other")
        }
      }
    withClue(s"${(wrong ++ overRefused).mkString("\n")}\n") {
      (wrong ++ overRefused) shouldBe empty
    }
  }

  "TermsIncludeExclude.unmixableIn" should "find the pair under a terms AND under any other bucket aggregation" in {
    // Hand-built trees: an input only the recursion can classify. A nested key's `terms` is built
    // under a `nested` / `filter` aggregation, which no SQL cell here
    // reaches.
    val mixed = TermsAggregation("inner").includeExclude(Seq("x.*"), Seq("a", "b"))
    val clean = TermsAggregation("outer").includeExclude(Seq("a", "a"), Nil)
    TermsIncludeExclude.unmixableIn(Seq(clean.subaggs(Seq(mixed)))) shouldBe Some("inner")
    TermsIncludeExclude.unmixableIn(Seq(NestedAggregation("n", "p", subaggs = Seq(mixed)))) shouldBe
    Some("inner")
    TermsIncludeExclude.unmixableIn(
      Seq(clean, NestedAggregation("n", "p", subaggs = Seq(clean)))
    ) shouldBe None
  }

  "TermsIncludeExclude.entries" should "write each kind of side the way 6.8 reads it as meant" in {
    val none = BucketIncludesExcludes()
    val one = BucketIncludesExcludes(values = Set("a.b"))
    val two = BucketIncludesExcludes(values = Set("a", "b"))
    val pattern = BucketIncludesExcludes(regex = Some("x.*"))
    TermsIncludeExclude.entries(one, none) shouldBe Seq("a.b", "a.b") // a SET of one
    TermsIncludeExclude.entries(one, two) shouldBe Seq("a.b", "a.b")
    TermsIncludeExclude.entries(one, pattern) shouldBe Seq("a\\.b") // an EXACT pattern
    TermsIncludeExclude.entries(two, pattern) shouldBe two.values.toSeq // unchanged
    TermsIncludeExclude.entries(pattern, one) shouldBe Seq("x.*") // unchanged
    TermsIncludeExclude.entries(none, one) shouldBe Nil
    TermsIncludeExclude.unmixable(Seq("x.*"), Seq("a", "b")) shouldBe true
    TermsIncludeExclude.unmixable(Seq("a", "b"), Seq("x.*")) shouldBe true
    TermsIncludeExclude.unmixable(Seq("a.b", "a.b"), Seq("c", "d")) shouldBe false
    TermsIncludeExclude.unmixable(Seq("a\\.b"), Seq("x.*")) shouldBe false
  }

  "the ES 6 key-filter wire rule" should "be written in ONE place: TermsIncludeExclude" in {
    // Any other `terms` include / exclude write is elastic4s 6's count rule again -- a one-entry
    // list is a bare string, i.e. a REGEX on Elasticsearch 6.8 -- so a second write site (a new
    // grouping path, a qualified key) must go through `TermsIncludeExclude.apply`. A source scan,
    // unforked module, so the path is relative to the repository root. Every spelling that sets
    // the field is scanned: the builder methods, a `copy(includeExclude = ...)`, and a hand-built
    // `IncludeExclude(...)` -- the three ways elastic4s 6 lets a caller write it.
    val root = new java.io.File("es6/bridge/src/main/scala")
    withClue(s"es6 bridge sources not found under ${root.getAbsolutePath}: ") {
      root.isDirectory shouldBe true
    }
    def scalaFiles(f: java.io.File): Seq[java.io.File] =
      if (f.isDirectory) Option(f.listFiles).toSeq.flatten.flatMap(scalaFiles)
      else if (f.getName.endsWith(".scala")) Seq(f)
      else Nil
    val write =
      """\.include\(|\.exclude\(|\bincludeExclude\(|includeExclude\s*=|\bIncludeExclude\(""".r
    val writes = for {
      file <- scalaFiles(root)
      (line, n) <- java.nio.file.Files
        .readAllLines(file.toPath, java.nio.charset.StandardCharsets.UTF_8)
        .toArray(Array.empty[String])
        .toSeq
        .zipWithIndex
      code = line.trim
      if !code.startsWith("//") && !code.startsWith("*") && write.findFirstIn(code).isDefined
    } yield s"${file.getName}:${n + 1}: $code"
    // the canary is the one legitimate write: a scan that finds nothing proves nothing
    withClue(writes.mkString("\n")) {
      writes should have size 1
      writes.head should startWith("TermsIncludeExclude.scala:")
    }
  }

  "an EMPTY one-value side beside a pattern" should "keep or remove exactly the empty key on 6.8" in {
    // `literalRegex("")` is "" -- a one-entry side, i.e. a bare JSON string, which 6.8 compiles as
    // `RegExp("")`, whose language is exactly the empty key. elastic4s's String overload of
    // `includeExclude` would DROP an empty string (`StringOption`), which is why `apply` passes the
    // Iterable one. A regression guard of the new spelling (the empty value was already a
    // one-entry string on the previous spelling, so this row is green there too).
    val cells = Seq(
      "sval IN ('') AND sval NOT LIKE 'x%'" -> Set(""),
      "sval LIKE '%' AND sval NOT IN ('')"  -> (stringKeys.toSet - "")
    )
    val wrong = cells.flatMap { case (predicate, kept) =>
      val sql = keyed("sval", predicate)
      val actual = Try(termsBody(queryOf(sql), "sval")).map(keptBy(_, KeywordKey, stringKeys))
      if (actual == Success(Kept(kept))) None
      else Some(s"[$sql] expected ${Kept(kept)}, got $actual")
    }
    withClue(s"${wrong.size} wrong:\n${wrong.mkString("\n")}\n") { wrong shouldBe empty }
  }

  "literalRegex" should "make Elasticsearch 6.8 keep exactly the value, for every character" in {
    // every printable ASCII character, plus a Latin-1 letter and a character outside the BMP (a
    // surrogate pair, handed to `toRegex` one half at a time)
    val alphabet: Seq[String] =
      (0x20 to 0x7e).map(_.toChar.toString) ++ Seq("\u00e9", "\ud83d\ude00")
    val broken = alphabet.flatMap { c =>
      val v = s"x${c}y"
      val neighbours = Seq(v, "x", "y", "xy", "xzy", s"x$c${c}y", s"x$c", s"${c}y", s"x${c}yy")
      Try {
        val filter = new Es68IncludeExclude(TermsIncludeExclude.literalRegex(v), null: String)
          .convertToStringFilter(DocValueFormat.RAW)
        neighbours.filter(w => filter.accept(new BytesRef(w)) != (w == v))
      } match {
        case Success(Seq()) => None
        case other          => Some(s"[$c] $other")
      }
    }
    withClue(s"${broken.size} of ${alphabet.size} characters broken:\n${broken.mkString("\n")}\n") {
      broken shouldBe empty
    }
  }
}
