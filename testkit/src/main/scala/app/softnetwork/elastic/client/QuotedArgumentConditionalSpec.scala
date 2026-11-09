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

import akka.NotUsed
import akka.stream.scaladsl.Source
import app.softnetwork.elastic.client.bulk._
import app.softnetwork.elastic.client.result.{
  ElasticFailure,
  ElasticResult,
  ElasticSuccess,
  QueryResult
}
import app.softnetwork.elastic.scalatest.ElasticTestKit
import app.softnetwork.elastic.sql.{ComparisonRule, StringValue => SqlStringValue}
import app.softnetwork.elastic.sql.`type`.SQLTypes

import java.time.{LocalDate, ZonedDateTime}
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.language.implicitConversions
import scala.util.Try

/** A quoted argument of `COALESCE`, `GREATEST`, `LEAST` and a `CASE` result is read as the type
  * beside it, and a comparison between a text and a number or a boolean -- or a boolean and a
  * number -- answers as DuckDB 1.5.5 casts it (SCHEMA-5b): each row is the DuckDB twin's value and
  * Java class, on a real cluster. A text that converts to nothing fails the query, as DuckDB fails
  * it; the runtime conversions answer what the literal rule (`ComparisonRule`) reads (the twin
  * pin). Fixtures `s5b_*`.
  */
trait QuotedArgumentConditionalSpec extends GatewayIntegrationTestKit {
  self: ElasticTestKit =>

  private lazy val api: ElasticClientApi = client.asInstanceOf[ElasticClientApi]

  private def exec(sql: String): ElasticResult[QueryResult] =
    Await.result(client.run(sql), 60.seconds)

  /** A statement over the fixtures, loaded on first use: a runner mixes `ElasticDockerTestKit` in
    * last, and its `beforeAll` does not call this trait's.
    */
  private def run(sql: String): ElasticResult[QueryResult] = {
    fixtures
    exec(sql)
  }

  private def ddl(sql: String): Unit = exec(sql) match {
    case ElasticSuccess(_)     =>
    case ElasticFailure(error) => fail(s"[$sql] failed: ${error.message}")
  }

  private def rows(sql: String): Seq[Map[String, Any]] = run(sql) match {
    case ElasticFailure(error) => fail(s"[$sql] failed: ${error.message}")
    case ok                    => collectRows(System.nanoTime(), ok)
  }

  /** The failure of `sql` -- refused, or failed by Elasticsearch -- if it fails. */
  private def failure(sql: String): Option[String] =
    Try(run(sql)).toEither match {
      case Left(e)                      => Some(String.valueOf(e.getMessage))
      case Right(ElasticFailure(error)) => Some(error.message)
      case Right(ok) =>
        Try(collectRows(System.nanoTime(), ok)).failed.toOption.map(e =>
          String.valueOf(e.getMessage)
        )
    }

  private def bulk(index: String, docs: List[String]): Unit = {
    implicit val bulkOptions: BulkOptions = BulkOptions(defaultIndex = index, logEvery = 100000)
    implicit def listToSource[T](list: List[T]): Source[T, NotUsed] =
      Source.fromIterator(() => list.iterator)
    api.bulk[String](docs, identity, idKey = Some(Set("id"))) match {
      case ElasticSuccess(_)     =>
      case ElasticFailure(error) => fail(s"bulk into $index failed: ${error.message}")
    }
    api.refresh(index)
  }

  private def json(s: String): String =
    "\"" + s.replace("\\", "\\\\").replace("\"", "\\\"") + "\""

  /** `id -> column` over the rows of `sql`. */
  private def byId(sql: String, column: String = "r"): Map[String, Any] =
    rows(sql).map(r => String.valueOf(r("id")) -> r.getOrElse(column, null)).toMap

  private def ids(sql: String): Set[String] = rows(sql).map(r => String.valueOf(r("id"))).toSet

  /** Does `actual` hold the value `expected` names -- a number by value, a date by its day, a
    * timestamp by its instant (`2024-01-15T00:00:00Z`), anything else by its text?
    */
  private def same(actual: Any, expected: Any): Boolean = (actual, expected) match {
    case (null, null)           => true
    case (null, _) | (_, null)  => false
    case (a: Number, e: Number) => BigDecimal(a.toString) == BigDecimal(e.toString)
    case (a: ZonedDateTime, e)  => a.toInstant.toString == String.valueOf(e)
    case (a: LocalDate, e)      => a.toString == String.valueOf(e)
    case (a, e)                 => String.valueOf(a) == String.valueOf(e)
  }

  /** Each `id -> value` of `SELECT id, <expr> AS r FROM <index>`, and the Java class of every
    * non-NULL value, the class the DuckDB twin's type reads as.
    */
  private def answers(
    index: String,
    expr: String,
    cls: Class[_],
    expected: (String, Any)*
  ): Unit = {
    val sql = s"SELECT id, $expr AS r FROM $index"
    val got = byId(sql)
    withClue(s"[$sql] -> $got: ") {
      expected.foreach { case (id, value) =>
        withClue(s"$id: ") {
          same(got.getOrElse(id, null), value) shouldBe true
          Option(got.getOrElse(id, null)).foreach(v => v.getClass shouldBe cls)
        }
      }
    }
  }

  private val IntC = classOf[java.lang.Integer]
  private val LongC = classOf[java.lang.Long]
  private val DoubleC = classOf[java.lang.Double]
  private val BoolC = classOf[java.lang.Boolean]
  private val TextC = classOf[String]
  private val DateC = classOf[LocalDate]
  private val TimestampC = classOf[ZonedDateTime]

  private lazy val fixtures: Unit = {
    ddl(
      "CREATE TABLE s5b_t (id KEYWORD, g KEYWORD, ti TINYINT, n INT, l BIGINT, x DOUBLE, d DATE, " +
      "ts TIMESTAMP, b BOOLEAN, k KEYWORD, PRIMARY KEY (id))"
    )
    bulk(
      "s5b_t",
      List(
        """{"id":"t1","g":"g1","ti":1,"n":1,"l":1,"x":1.5,"d":"2024-01-10","ts":"2024-01-10T08:00:00Z","b":true,"k":"1"}""",
        """{"id":"t2","g":"g2","k":"3"}""",
        """{"id":"t3","g":"g3","ti":5,"n":3,"l":5,"x":0.5,"d":"2024-02-01","ts":"2024-02-01T00:00:00Z","b":false,"k":"3"}"""
      )
    )
    ddl(
      "CREATE TABLE s5b_c (id KEYWORD, g KEYWORD, k KEYWORD, n INT, l BIGINT, x DOUBLE, b BOOLEAN, " +
      "tx TEXT, ak ARRAY<KEYWORD>, ab ARRAY<BOOLEAN>, PRIMARY KEY (id))"
    )
    bulk(
      "s5b_c",
      List(
        """{"id":"c1","g":"g1","k":"1","n":1,"l":1,"x":1.5,"b":true,"tx":"1","ak":["1"],"ab":[true]}""",
        """{"id":"c2","g":"g2","k":"0","n":0,"l":0,"x":0.5,"b":false,"tx":"0","ak":["0"],"ab":[false]}"""
      )
    )
    ddl(
      "CREATE TABLE s5b_k (id KEYWORD, k KEYWORD, l BIGINT, x DOUBLE, b BOOLEAN, PRIMARY KEY (id))"
    )
    bulk(
      "s5b_k",
      wholeTexts.zipWithIndex.map { case (t, i) =>
        s"""{"id":"w$i","k":${json(t)},"l":${ComparisonRule.roundedWhole(t).getOrElse(0L)}}"""
      }.toList ++
      booleanTexts.zipWithIndex.map { case (t, i) =>
        s"""{"id":"b$i","k":${json(t)},"b":${booleanOf(t).getOrElse(true)}}"""
      } ++
      doubleTexts.zipWithIndex.map { case (t, i) =>
        s"""{"id":"x$i","k":${json(t)},"x":${doubleOf(t).getOrElse(0.0)}}"""
      }
    )
    ddl(
      "CREATE TABLE s5b_p (id KEYWORD, n INT, b BOOLEAN, d DATE, ts TIMESTAMP, " +
      "c1 INT SCRIPT AS (COALESCE(n, '3')), c2 INT SCRIPT AS (GREATEST(n, '1.5')), " +
      "c3 BIGINT SCRIPT AS (GREATEST(b, 1)), c4 BIGINT SCRIPT AS (LEAST(b, 1)), " +
      "c5 DATE SCRIPT AS (COALESCE(d, '2024-01-15')), " +
      "c6 INT SCRIPT AS (CASE WHEN b = 1 THEN 1 ELSE 0 END), " +
      "c7 BOOLEAN SCRIPT AS (NULLIF(b, 0)), " +
      "c8 DATE SCRIPT AS (DATE_ADD(COALESCE(d, '2024-01-15'), INTERVAL 1 DAY)), " +
      "c9 DATE SCRIPT AS (DATE_ADD(COALESCE(ts, '2024-01-15'), INTERVAL 1 DAY)), " +
      "c10 INT SCRIPT AS (CASE WHEN b BETWEEN 1 AND 1 THEN 1 ELSE 0 END), PRIMARY KEY (id))"
    )
    bulk(
      "s5b_p",
      List(
        """{"id":"p1","n":1,"b":true,"d":"2024-01-10"}""",
        """{"id":"p2","n":"5","b":"true"}""",
        """{"id":"p3","b":""}""",
        """{"id":"p4","b":false}""",
        """{"id":"p5","b":"false"}""",
        """{"id":"p6"}"""
      )
    )
  }

  // ---- the texts of the twin pins (D7): what the literal rule reads each as ----

  private val wholeTexts: Seq[String] = Seq(
    "3",
    "1.5",
    "-2.5",
    "0.5",
    "1e3",
    " 2 ",
    "200",
    "9223372036854775807.4",
    "01",
    ".5",
    "-0.5",
    "0.4999",
    "5.",
    "1e+2",
    "1e-30000000",
    "1e30000000",
    "9223372036854775807.5",
    "-9223372036854775808.5",
    "abc",
    "",
    "1,5",
    "1d",
    "0x10",
    "1_000",
    "NaN",
    "Infinity",
    "inf",
    "+.",
    "1e",
    "2024-01-15",
    "true",
    "yes"
  )

  // U+017F ('ſ') upper-cases to 'S': a case-insensitive comparison read 'yeſ' as true (R0-2),
  // where the literal rule -- the text in lower case -- reads nothing
  private val booleanTexts: Seq[String] = Seq(
    "t",
    "TRUE",
    " true ",
    "on",
    "OFF",
    "yes",
    "Y",
    "1",
    "0",
    "f",
    "no",
    "n",
    "false",
    "2",
    "",
    "maybe",
    "nan",
    "ye\u017F",
    "YE\u017F",
    "fal\u017Fe"
  )

  private val doubleTexts: Seq[String] =
    Seq("1.5", "1e3", " 2 ", ".5", "abc", "1d", "0x1p4", "NaN", "1e400", "-1e400")

  private def booleanOf(text: String): Option[Boolean] =
    ComparisonRule.readingOf(SqlStringValue(text), SQLTypes.Boolean).collect {
      case ComparisonRule.BooleanReading(value) => value
    }

  private def doubleOf(text: String): Option[Double] =
    ComparisonRule.numberOf(text).map(_.painless.stripSuffix("L").toDouble)

  /** What a stored text beside a DOUBLE is compared as (A-93): the number the literal rule reads,
    * or +/-Infinity for a number past DOUBLE's range -- `Double.parseDouble`'s overflow, DuckDB
    * 1.5.5's value -- where the literal rule reads nothing (D16); `None`, a failure, otherwise.
    */
  private def doubleTwinOf(text: String): Option[Double] =
    doubleOf(text).orElse {
      val t = text.trim
      if (t.matches("[+-]?(\\d+\\.?\\d*|\\.\\d+)([eE][+-]?\\d+)?"))
        Some(t.toDouble).filter(_.isInfinite)
      else None
    }

  // ---- AC1: a quoted argument beside a typed one ----

  "a quoted argument beside a number" should "answer the number DuckDB reads, in its type" in {
    answers("s5b_t", "COALESCE(n, '3')", IntC, "t1"         -> 1, "t2"    -> 3, "t3"    -> 3)
    answers("s5b_t", "COALESCE('3', n)", IntC, "t1"         -> 3, "t2"    -> 3, "t3"    -> 3)
    answers("s5b_t", "GREATEST(n, '1.5')", IntC, "t1"       -> 2, "t2"    -> 2, "t3"    -> 3)
    answers("s5b_t", "LEAST('-2.5', n)", IntC, "t1"         -> -3, "t2"   -> -3, "t3"   -> -3)
    answers("s5b_t", "GREATEST(l, '1e3')", LongC, "t1"      -> 1000, "t2" -> 1000, "t3" -> 1000)
    answers("s5b_t", "COALESCE(x, '0.5')", DoubleC, "t1"    -> 1.5, "t2"  -> 0.5, "t3"  -> 0.5)
    answers("s5b_t", "COALESCE(n, ' 2 ')", IntC, "t1"       -> 1, "t2"    -> 2, "t3"    -> 3)
    answers("s5b_t", "COALESCE(n, l, '3')", LongC, "t1"     -> 1, "t2"    -> 3, "t3"    -> 3)
    answers("s5b_t", "GREATEST(n, '1.5', NULL)", IntC, "t1" -> 2, "t2"    -> 2, "t3"    -> 3)
    answers(
      "s5b_t",
      "CASE WHEN n > 0 THEN '1.5' ELSE n END",
      IntC,
      "t1" -> 2,
      "t2" -> null,
      "t3" -> 2
    )
  }

  it should "widen past the operand's whole range to BIGINT, never wrap (D2)" in {
    answers("s5b_t", "GREATEST(ti, '200')", LongC, "t1" -> 200, "t2" -> 200, "t3" -> 200)
  }

  it should "keep a text that reads as nothing as main answered it (D11)" in {
    answers("s5b_t", "COALESCE(n, 'abc')", TextC, "t1" -> "1", "t2" -> "abc", "t3" -> "3")
  }

  it should "answer a constant-only conditional (6.8 refuses some constant scripts)" in {
    answers("s5b_t", "GREATEST(3, '1.5')", LongC, "t1"     -> 3, "t2"   -> 3, "t3"   -> 3)
    answers("s5b_t", "GREATEST(1.5, '2.5')", DoubleC, "t1" -> 2.5, "t2" -> 2.5, "t3" -> 2.5)
  }

  "a quoted argument beside a date, a timestamp, a boolean" should "answer its value" in {
    answers(
      "s5b_t",
      "COALESCE(d, '2024-01-15')",
      DateC,
      "t1" -> "2024-01-10",
      "t2" -> "2024-01-15",
      "t3" -> "2024-02-01"
    )
    answers(
      "s5b_t",
      "GREATEST(d, '2024-01-15 10:30:00')",
      DateC,
      "t1" -> "2024-01-15",
      "t2" -> "2024-01-15",
      "t3" -> "2024-02-01"
    )
    answers(
      "s5b_t",
      "LEAST(ts, '2024-01-15')",
      TimestampC,
      "t1" -> "2024-01-10T08:00:00Z",
      "t2" -> "2024-01-15T00:00:00Z",
      "t3" -> "2024-01-15T00:00:00Z"
    )
    answers("s5b_t", "COALESCE(b, 'true')", BoolC, "t1" -> true, "t2" -> true, "t3" -> false)
  }

  "a BOOLEAN beside whole numbers (D5, R-107)" should "answer 1 / 0 in their type" in {
    answers("s5b_t", "GREATEST(b, 1)", LongC, "t1" -> 1, "t2" -> 1, "t3" -> 1)
    answers("s5b_t", "LEAST(b, 1)", LongC, "t1"    -> 1, "t2" -> 1, "t3" -> 0)
    answers("s5b_t", "COALESCE(b, 1)", LongC, "t1" -> 1, "t2" -> 1, "t3" -> 0)
  }

  "a read conditional" should "be filtered, grouped and filtered per group by its value" in {
    ids("SELECT id FROM s5b_t WHERE COALESCE(n, '3') = 3") shouldBe Set("t2", "t3")
    ids("SELECT id FROM s5b_t WHERE GREATEST(x, '1') > 1") shouldBe Set("t1")
    // main failed on every row: Cannot cast java.lang.String to java.time.chrono.ChronoZonedDateTime
    ids("SELECT id FROM s5b_t WHERE COALESCE(d, '2024-01-15') = '2024-01-15'") shouldBe Set("t2")
    ids("SELECT id FROM s5b_t WHERE COALESCE(b, 'false') = 'false'") shouldBe Set("t2", "t3")
    rows("SELECT g, MAX(n) AS m FROM s5b_t GROUP BY g HAVING COALESCE(MAX(n), '0') > 1")
      .map(r => String.valueOf(r("g"))) shouldBe Seq("g3")
    val groups = rows(
      "SELECT COALESCE(n, '3') AS r, COUNT(*) AS c FROM s5b_t GROUP BY COALESCE(n, '3')"
    ).map(r => r("r") -> r("c"))
    // main grouped the TEXTS "1" and "3"
    groups.foreach { case (key, _) => key shouldBe a[Number] }
    groups.map { case (key, c) =>
      BigDecimal(key.toString) -> BigDecimal(c.toString)
    }.toMap shouldBe
    Map(BigDecimal(1) -> BigDecimal(1), BigDecimal(3) -> BigDecimal(2))
  }

  it should "compare with a text column as DuckDB casts it (A-84, D6)" in {
    // main answered t2 only: an Integer is never == a String
    ids("SELECT id FROM s5b_t WHERE COALESCE(n, '3') = k") shouldBe Set("t1", "t2", "t3")
    ids("SELECT id FROM s5b_t WHERE CASE WHEN n > 0 THEN n ELSE '3' END = k") shouldBe
    Set("t1", "t2", "t3")
    ids("SELECT id FROM s5b_t WHERE COALESCE(n, '3') IN ('3', '1')") shouldBe Set("t1", "t2", "t3")
    // a literal that reads as nothing: refused, as its unquoted twin is
    failure("SELECT id FROM s5b_t WHERE COALESCE(n, '3') IN ('3', 'abc')") shouldBe defined
  }

  it should "be read as its text by a string function (R-109)" in {
    answers(
      "s5b_t",
      "UPPER(CASE WHEN n > 0 THEN n ELSE '3' END)",
      TextC,
      "t1" -> "1",
      "t2" -> "3",
      "t3" -> "3"
    )
    answers("s5b_t", "UPPER(n)", TextC, "t1"                -> "1", "t2" -> null, "t3" -> "3")
    answers("s5b_t", "LENGTH(n)", LongC, "t1"               -> 1, "t2"   -> null, "t3" -> 1)
    answers("s5b_t", "UPPER(COALESCE(n, '3'))", TextC, "t1" -> "1", "t2" -> "3", "t3"  -> "3")
    answers(
      "s5b_t",
      "LOWER(CASE WHEN n > 0 THEN 'A' ELSE 'B' END)",
      TextC,
      "t1" -> "a",
      "t2" -> "b",
      "t3" -> "a"
    )
  }

  "a BOOLEAN cast to a temporal (R-110)" should "be refused" in {
    failure("SELECT id, CAST(b AS DATE) AS r FROM s5b_t").getOrElse("") should include(
      "cannot cast BOOLEAN to DATE"
    )
  }

  "a read conditional nested beside text (A-97)" should "answer its value's text, 0.24.0's type" in {
    // nested beside a text argument, the read conditional answers the text of the value it is read
    // as -- the same row's CAST(<inner> AS VARCHAR) -- where the nested reading refused the
    // statement ('Cannot mix values of type INT and KEYWORD') that 0.24.0 answered as text
    def text(expr: String, inner: String, expected: (String, Any)*): Unit = {
      answers("s5b_t", expr, TextC, expected: _*)
      answers("s5b_t", s"CAST($inner AS VARCHAR)", TextC, expected: _*)
    }
    // 0.24.0 answered '1.5' on the NULL row; the inner COALESCE reads it as 2 beside the INT n
    text(
      "COALESCE(COALESCE(n, '1.5'), k)",
      "COALESCE(n, '1.5')",
      "t1" -> "1",
      "t2" -> "2",
      "t3" -> "3"
    )
    text(
      "COALESCE(CASE WHEN n > 0 THEN n ELSE '3' END, k)",
      "CASE WHEN n > 0 THEN n ELSE '3' END",
      "t1" -> "1",
      "t2" -> "3",
      "t3" -> "3"
    )
    text(
      "COALESCE(COALESCE(n, '3'), COALESCE(d, '2024-01-15'))",
      "COALESCE(n, '3')",
      "t1" -> "1",
      "t2" -> "3",
      "t3" -> "3"
    )
    // main refused the inner GREATEST (a text beside a number): refused -> answered
    text(
      "COALESCE(GREATEST(n, '2'), k)",
      "GREATEST(n, '2')",
      "t1" -> "2",
      "t2" -> "2",
      "t3" -> "3"
    )
    // the date arm answers on the rows n > 0 (its ISO date); the NULL row takes k
    answers(
      "s5b_t",
      "CASE WHEN n > 0 THEN COALESCE(d, '2024-01-15') ELSE k END",
      TextC,
      "t1" -> "2024-01-10",
      "t2" -> "3",
      "t3" -> "2024-02-01"
    )
    answers(
      "s5b_t",
      "CAST(COALESCE(d, '2024-01-15') AS VARCHAR)",
      TextC,
      "t1" -> "2024-01-10",
      "t3" -> "2024-02-01"
    )
    // beside a typed arm the read CASE keeps its INT, its NULL test enclosed (r4: `3 != null`)
    answers(
      "s5b_t",
      "COALESCE(CASE WHEN n > 0 THEN n ELSE '3' END, n)",
      IntC,
      "t1" -> 1,
      "t2" -> 3,
      "t3" -> 3
    )
  }

  "a literal-first COALESCE beside text (A-98)" should "compare its literal's text, as main does" in {
    // main answers both right on every row (RUN); DuckDB refuses them at bind time
    answers(
      "s5b_t",
      "CASE WHEN COALESCE('2024-01-15', d) = k THEN 1 ELSE 0 END",
      LongC,
      "t1" -> 0,
      "t2" -> 0,
      "t3" -> 0
    )
    answers(
      "s5b_t",
      "NULLIF(COALESCE('2024-01-15', d), k)",
      TextC,
      "t1" -> "2024-01-15",
      "t2" -> "2024-01-15",
      "t3" -> "2024-01-15"
    )
  }

  it should "answer COALESCE(NULLIF(F, k), k) as COALESCE(F, k) (β)" in {
    Seq(
      "CASE WHEN n > 0 THEN d ELSE '2024-01-15' END",
      "CASE WHEN n > 0 THEN ts ELSE '2024-01-15 10:30:00' END"
    ).foreach { f =>
      // the oracle: COALESCE(F, k) on the same tree, its date or instant's text (A-97)
      val oracle = byId(s"SELECT id, COALESCE($f, k) AS r FROM s5b_t")
      answers("s5b_t", s"COALESCE(NULLIF($f, k), k)", TextC, oracle.toSeq: _*)
    }
    answers(
      "s5b_t",
      "COALESCE(NULLIF(CASE WHEN n > 0 THEN d ELSE '2024-01-15' END, k), k)",
      TextC,
      "t1" -> "2024-01-10",
      "t2" -> "2024-01-15",
      "t3" -> "2024-02-01"
    )
  }

  "a COALESCE over a GREATEST / LEAST holding a literal (A-99)" should "answer, never NULL" in {
    // main and r4 tested the reducer's primitive for NULL: Painless refused to compile it
    answers("s5b_t", "COALESCE(GREATEST(n, 3), l)", LongC, "t1"  -> 3, "t2" -> 3, "t3" -> 3)
    answers("s5b_t", "COALESCE(GREATEST(n, '3'), n)", IntC, "t1" -> 3, "t2" -> 3, "t3" -> 3)
  }

  it should "store its value in a computed column" in {
    // one column per table: one failing pipeline hides nothing
    ddl(
      "CREATE TABLE s5b_u1 (id KEYWORD, n INT, l BIGINT, k KEYWORD, " +
      "c BIGINT SCRIPT AS (COALESCE(GREATEST(n, 3), l)), PRIMARY KEY (id))"
    )
    ddl(
      "CREATE TABLE s5b_u2 (id KEYWORD, n INT, l BIGINT, k KEYWORD, " +
      "c VARCHAR SCRIPT AS (COALESCE(GREATEST(n, '3'), k)), PRIMARY KEY (id))"
    )
    val docs = List(
      """{"id":"q1","n":1,"l":5,"k":"a"}""",
      """{"id":"q2","k":"b"}""",
      """{"id":"q3","n":7,"l":2,"k":"c"}"""
    )
    bulk("s5b_u1", docs)
    bulk("s5b_u2", docs)
    Seq(
      "s5b_u1" -> Seq("q1" -> 3, "q2" -> 3, "q3" -> 7),
      "s5b_u2" -> Seq("q1" -> "3", "q2" -> "3", "q3" -> "7")
    )
      .foreach { case (index, expected) =>
        val got = byId(s"SELECT id, c FROM $index", "c")
        expected.foreach { case (id, value) =>
          withClue(s"$index $id $got: ")(same(got.getOrElse(id, null), value) shouldBe true)
        }
      }
  }

  "COALESCE over a CASE (R-113)" should "test the whole CASE for NULL" in {
    // main tested the ELSE alone (`c ? a : b != null ? … : k`): where the condition held and the
    // THEN was NULL it answered NULL, not the next argument (t2: k = '3', n missing)
    answers(
      "s5b_t",
      "COALESCE(CASE WHEN k = '3' THEN CAST(n AS VARCHAR) ELSE g END, g)",
      TextC,
      "t1" -> "g1",
      "t2" -> "g2",
      "t3" -> "3"
    )
    // a NULL ELSE
    answers(
      "s5b_t",
      "COALESCE(CASE WHEN k = '3' THEN CAST(n AS VARCHAR) ELSE NULL END, g)",
      TextC,
      "t1" -> "g1",
      "t2" -> "g2",
      "t3" -> "3"
    )
    // a CASE nested in the fold, as a middle argument
    answers(
      "s5b_t",
      "COALESCE(CAST(ti AS VARCHAR), CASE WHEN k = '3' THEN CAST(n AS VARCHAR) ELSE g END, g)",
      TextC,
      "t1" -> "1",
      "t2" -> "g2",
      "t3" -> "5"
    )
  }

  it should "test the whole CASE for NULL in a computed column too" in {
    ddl(
      "CREATE TABLE s5b_n (id KEYWORD, k KEYWORD, n INT, g KEYWORD, " +
      "c KEYWORD SCRIPT AS (COALESCE(CASE WHEN k = '3' THEN CAST(n AS VARCHAR) ELSE g END, g)), " +
      "PRIMARY KEY (id))"
    )
    bulk(
      "s5b_n",
      List(
        """{"id":"n1","k":"3","g":"g1"}""",
        """{"id":"n2","k":"3","n":5,"g":"g2"}""",
        """{"id":"n3","k":"1","g":"g3"}"""
      )
    )
    byId("SELECT id, c FROM s5b_n", "c") shouldBe Map("n1" -> "g1", "n2" -> "5", "n3" -> "g3")
  }

  // ---- an ingest processor (D3 (5), R-107, D6 / D7): string and native _source ----

  "a computed column" should "store the value its quoted argument or its BOOLEAN is read as" in {
    val got = rows("SELECT id, c1, c2, c3, c4, c5, c6, c7 FROM s5b_p")
      .map(r => String.valueOf(r("id")) -> r)
      .toMap
    def column(c: String, expected: (String, Any)*): Unit =
      expected.foreach { case (id, value) =>
        withClue(s"$c $id ${got.get(id)}: ")(same(got(id).getOrElse(c, null), value) shouldBe true)
      }
    column("c1", "p1" -> 1, "p2" -> 5, "p3" -> 3, "p4" -> 3, "p5" -> 3, "p6" -> 3)
    column("c2", "p1" -> 2, "p3" -> 2, "p4" -> 2, "p5" -> 2, "p6" -> 2)
    // main stored nothing for a document with a boolean: Math.max over a Boolean threw
    column("c3", "p1" -> 1, "p2" -> 1, "p3" -> 1, "p4" -> 1, "p5" -> 1, "p6" -> 1)
    column("c4", "p1" -> 1, "p2" -> 1, "p3" -> 0, "p4" -> 0, "p5" -> 0, "p6" -> 1)
    column(
      "c5",
      "p1" -> "2024-01-10",
      "p2" -> "2024-01-15",
      "p3" -> "2024-01-15",
      "p6" -> "2024-01-15"
    )
    // main compared a Boolean with 1: never equal, 0 on every document
    column("c6", "p1" -> 1, "p2"    -> 1, "p3"    -> 0, "p4"    -> 0, "p5"    -> 0, "p6" -> 0)
    column("c7", "p1" -> true, "p2" -> true, "p3" -> null, "p4" -> null, "p6" -> null)
  }

  it should "keep a quoted date's text type outside a query, so DATE_ADD parses it (A-92)" in {
    // before A-92 the conditional was a DATE in the processor while its value was text, and
    // DATE_ADD called `.plus` on a String: every document failed and the column was absent
    val got = rows("SELECT id, c8, c9 FROM s5b_p").map(r => String.valueOf(r("id")) -> r).toMap
    def stored(id: String, c: String): Any = got.get(id).flatMap(_.get(c)).orNull
    withClue(s"c8 p1 $got: ")(same(stored("p1", "c8"), "2024-01-11") shouldBe true)
    Seq("p2", "p3", "p4", "p5", "p6").foreach { id =>
      withClue(s"c8 $id $got: ")(same(stored(id, "c8"), "2024-01-16") shouldBe true)
      // a timestamp read in a processor is the literal as written, which DATE_ADD's date parse reads
      withClue(s"c9 $id $got: ")(same(stored(id, "c9"), "2024-01-16") shouldBe true)
    }
  }

  it should "compare a BOOLEAN BETWEEN two numbers as 1 / 0 (A-91)" in {
    // main compared the Boolean itself with the bounds: the processor failed on every document
    val got = byId("SELECT id, c10 FROM s5b_p", "c10")
    Seq("p1" -> 1, "p2" -> 1, "p3" -> 0, "p4" -> 0, "p5" -> 0, "p6" -> 0).foreach { case (id, v) =>
      withClue(s"c10 $id $got: ")(same(got.getOrElse(id, null), v) shouldBe true)
    }
  }

  it should "store a date read in a CASE result as its ISO text (A-89)" in {
    // before A-89 the processor called a method on the string literal ("2024-01-15".atStartOfDay):
    // Elasticsearch refused the pipeline, so this CREATE failed where main and 0.24.0 accept it
    ddl(
      "CREATE TABLE s5b_q (id KEYWORD, n INT, d DATE, " +
      "c DATE SCRIPT AS (CASE WHEN n > 0 THEN d ELSE '2024-01-15' END), " +
      "c2 DATE SCRIPT AS (CASE WHEN n > 0 THEN '2024-01-15' ELSE d END), PRIMARY KEY (id))"
    )
    bulk(
      "s5b_q",
      List(
        """{"id":"q1","n":1,"d":"2024-01-10"}""",
        """{"id":"q2","n":0,"d":"2024-01-10"}""",
        """{"id":"q3","n":-1,"d":"2024-01-20"}""",
        """{"id":"q4","d":"2024-01-10"}"""
      )
    )
    val got = rows("SELECT id, c, c2 FROM s5b_q").map(r => String.valueOf(r("id")) -> r).toMap
    def stored(id: String, c: String): Any = got.get(id).flatMap(_.get(c)).orNull
    Seq("q2", "q3", "q4").foreach { id =>
      withClue(s"c $id $got: ")(same(stored(id, "c"), "2024-01-15") shouldBe true)
    }
    withClue(s"c2 q1 $got: ")(same(stored("q1", "c2"), "2024-01-15") shouldBe true)
    // the date column's arm renders as on main (A-89 moves only the literal): recorded, not pinned
    info(s"c q1 = ${stored("q1", "c")}; c2 q2 = ${stored("q2", "c2")}")
  }

  it should "compare a conditional holding a quoted date as its text, as 0.24.0 does (R1-1)" in {
    // before R1-1 the processor read the conditional as a DATE over the text it holds and called
    // `isEqual` / `isBefore` on a String: every document failed and the column was absent
    ddl(
      "CREATE TABLE s5b_r (id KEYWORD, n INT, d DATE, ts TIMESTAMP, " +
      "c1 INT SCRIPT AS (CASE WHEN COALESCE(d, '2024-01-15') = '2024-01-15' THEN 1 ELSE 0 END), " +
      "c2 INT SCRIPT AS (CASE WHEN COALESCE(d, '2024-01-15') BETWEEN '2024-01-01' AND " +
      "'2024-01-31' THEN 1 ELSE 0 END), " +
      "c3 INT SCRIPT AS (CASE WHEN CASE WHEN n > 0 THEN d ELSE '2024-01-15' END " +
      "IN ('2024-01-15', '2024-03-01') THEN 1 ELSE 0 END), " +
      "c4 INT SCRIPT AS (CASE WHEN COALESCE(ts, '2024-01-15 10:30:00') < '2024-01-12' " +
      "THEN 1 ELSE 0 END), " +
      "c5 DATE SCRIPT AS (NULLIF(COALESCE(d, '2024-01-15'), '2024-01-15')), PRIMARY KEY (id))"
    )
    bulk(
      "s5b_r",
      List(
        """{"id":"r1","n":1,"d":"2024-01-15","ts":"2024-01-10T08:00:00Z"}""",
        """{"id":"r2","n":1,"d":"2024-02-01","ts":"2024-01-20T08:00:00Z"}""",
        """{"id":"r3","n":0,"d":"2024-01-10"}""",
        """{"id":"r4"}"""
      )
    )
    val got = rows("SELECT id, c1, c2, c3, c4, c5 FROM s5b_r")
      .map(r => String.valueOf(r("id")) -> r)
      .toMap
    def column(c: String, expected: (String, Any)*): Unit =
      expected.foreach { case (id, value) =>
        withClue(s"$c $id ${got.get(id)}: ") {
          same(got.get(id).flatMap(_.get(c)).orNull, value) shouldBe true
        }
      }
    // DuckDB's values on these documents (a missing d is the literal)
    column("c1", "r1" -> 1, "r2" -> 0, "r3" -> 0, "r4" -> 1)
    column("c2", "r1" -> 1, "r2" -> 0, "r3" -> 1, "r4" -> 1)
    // the literal's rows; the date column's arm renders as on main (recorded, not pinned)
    column("c3", "r3" -> 1, "r4"    -> 1)
    column("c4", "r1" -> 1, "r2"    -> 0, "r3"            -> 0, "r4"            -> 0)
    column("c5", "r1" -> null, "r2" -> "2024-02-01", "r3" -> "2024-01-10", "r4" -> null)
    def stored(id: String): Any = got.get(id).flatMap(_.get("c3")).orNull
    info(s"c3 r1 = ${stored("r1")}; c3 r2 = ${stored("r2")}")
  }

  // ---- AC3: R-60's cells (R-102: 17 delivered), the twin fixture and its boolean discriminator ----

  "R-60's comparison cells" should "answer DuckDB's rows" in {
    Seq(
      "n = b" -> Set("c1", "c2"),
      "l = b" -> Set("c1", "c2"),
      "x = b" -> Set.empty[String],
      "k = n" -> Set("c1", "c2"),
      "k = x" -> Set.empty[String],
      "k = b" -> Set("c1", "c2"),
      "b = n" -> Set("c1", "c2"),
      "b = x" -> Set.empty[String],
      // over an ARRAY<X> column, the element (D8)
      "ak = n" -> Set("c1", "c2"),
      "ab = n" -> Set("c1", "c2")
    ).foreach { case (cond, expected) =>
      withClue(cond)(ids(s"SELECT id FROM s5b_c WHERE $cond") shouldBe expected)
    }
    answers("s5b_c", "NULLIF(k, 0)", TextC, "c1"                      -> "1", "c2" -> null)
    answers("s5b_c", "CASE WHEN k = 1 THEN 1 ELSE 0 END", LongC, "c1" -> 1, "c2"   -> 0)
    answers(
      "s5b_c",
      "CASE k WHEN 1 THEN 'one' ELSE 'other' END",
      TextC,
      "c1" -> "one",
      "c2" -> "other"
    )
    answers("s5b_c", "NULLIF(b, 0)", BoolC, "c1"                      -> true, "c2" -> null)
    answers("s5b_c", "CASE WHEN b = 1 THEN 1 ELSE 0 END", LongC, "c1" -> 1, "c2"    -> 0)
    answers(
      "s5b_c",
      "CASE b WHEN 1 THEN 'one' ELSE 'other' END",
      TextC,
      "c1" -> "one",
      "c2" -> "other"
    )
    answers("s5b_c", "GREATEST(b, 1)", LongC, "c1"                     -> 1, "c2" -> 1)
    answers("s5b_c", "LEAST(b, 1)", LongC, "c1"                        -> 1, "c2" -> 0)
    answers("s5b_c", "CASE WHEN ak = 1 THEN 1 ELSE 0 END", LongC, "c1" -> 1, "c2" -> 0)
    answers("s5b_c", "LEAST(ab, 1)", LongC, "c1"                       -> 1, "c2" -> 0)
  }

  it should "filter groups on a BOOLEAN metric as 1 / 0, with no conversion (R10)" in {
    rows("SELECT g, MAX(b) AS m FROM s5b_c GROUP BY g HAVING MAX(b) > 1") shouldBe empty
    rows("SELECT g, MAX(b) AS m FROM s5b_c GROUP BY g HAVING MAX(b) > 0")
      .map(r => String.valueOf(r("g"))) shouldBe Seq("g1")
  }

  it should "leave a text column to Elasticsearch, and keep R-102's exclusions (A-78, A-79)" in {
    // a script over a `text` field fails in Elasticsearch (fielddata), never in core's type rule
    val text = failure("SELECT id FROM s5b_c WHERE tx = n")
    text shouldBe defined
    text.get should not include "Type mismatch"
    failure("SELECT g, MAX(k) AS m FROM s5b_c GROUP BY g HAVING MAX(k) > '2024-01-01'")
      .getOrElse("") should include("compares a metric as text")
  }

  // ---- D7's twin pins: the runtime conversion answers what the literal rule reads ----

  "a stored text beside a number" should "be read as roundedWhole reads it, or fail the query" in {
    wholeTexts.zipWithIndex.foreach { case (t, i) =>
      val sql =
        s"SELECT id, CASE WHEN k = l THEN 'eq' ELSE 'ne' END AS r FROM s5b_k WHERE id = 'w$i'"
      withClue(s"[$t] $sql: ") {
        ComparisonRule.roundedWhole(t) match {
          case Some(_) => byId(sql) shouldBe Map(s"w$i" -> "eq")
          case None    => failure(sql) shouldBe defined
        }
      }
    }
  }

  "a stored text beside a boolean" should "be read as the literal rule reads it, or fail (A-81)" in {
    booleanTexts.zipWithIndex.foreach { case (t, i) =>
      val sql =
        s"SELECT id, CASE WHEN k = b THEN 'eq' ELSE 'ne' END AS r FROM s5b_k WHERE id = 'b$i'"
      withClue(s"[$t] $sql: ") {
        booleanOf(t) match {
          case Some(_) => byId(sql) shouldBe Map(s"b$i" -> "eq")
          case None    => failure(sql) shouldBe defined
        }
      }
    }
  }

  "a stored text beside a DOUBLE" should "be read as the number it spells, or fail" in {
    doubleTexts.zipWithIndex.foreach { case (t, i) =>
      val sql =
        s"SELECT id, CASE WHEN k = x THEN 'eq' ELSE 'ne' END AS r FROM s5b_k WHERE id = 'x$i'"
      withClue(s"[$t] $sql: ") {
        doubleTwinOf(t) match {
          // past DOUBLE's range: +/-Infinity, which equals no stored (finite) x (A-93)
          case Some(v) if v.isInfinite => byId(sql) shouldBe Map(s"x$i" -> "ne")
          case Some(_)                 => byId(sql) shouldBe Map(s"x$i" -> "eq")
          case None                    => failure(sql) shouldBe defined
        }
      }
    }
  }
}
