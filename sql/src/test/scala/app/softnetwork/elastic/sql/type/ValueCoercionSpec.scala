package app.softnetwork.elastic.sql.`type`

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.time.{
  Instant,
  LocalDate,
  LocalDateTime,
  LocalTime,
  OffsetDateTime,
  ZoneOffset,
  ZonedDateTime
}
import scala.collection.immutable.{HashMap, ListMap}

/** softclient4es-arrow#168 — `coerceToDate` had no `Number` arm while `coerceToTimestamp` did, so
  * an Elasticsearch `date` field stored as epoch millis threw `Cannot convert java.lang.Long to
  * DATE` and took down every cross-index JOIN leg that projected it (and `ResultSet.getDate` with
  * it).
  *
  * SCHEMA-17 fix R-171 — `coerceToString` (JDBC `getString`, arrow's VARCHAR writer) renders a
  * nested list, map or bytes through itself, in today's style; a flat value renders as before.
  */
class ValueCoercionSpec extends AnyFlatSpec with Matchers {

  // 2026-01-01T00:00:00Z plus 13h 45m — a value with a real time-of-day, so a naive
  // implementation that truncated (or failed to) is visible.
  private val EpochMillis: Long = 1767225600000L + (13L * 3600 + 45L * 60) * 1000L
  private val ExpectedDate: LocalDate =
    Instant.ofEpochMilli(EpochMillis).atZone(ZoneOffset.UTC).toLocalDate

  "coerceToDate" should "read a java.lang.Long as epoch milliseconds (arrow#168)" in {
    val d = ValueCoercion.coerceToDate(java.lang.Long.valueOf(EpochMillis))
    d should not be null
    d.getTime shouldBe EpochMillis
  }

  it should "read every other Number shape as epoch milliseconds" in {
    ValueCoercion.coerceToDate(java.lang.Integer.valueOf(0)).getTime shouldBe 0L
    ValueCoercion
      .coerceToDate(java.math.BigDecimal.valueOf(EpochMillis))
      .getTime shouldBe EpochMillis
    ValueCoercion.coerceToDate(EpochMillis.toDouble).getTime shouldBe EpochMillis
  }

  it should "keep every pre-existing conversion working" in {
    ValueCoercion.coerceToDate(null) shouldBe null
    val ld = LocalDate.of(2026, 1, 2)
    ValueCoercion.coerceToDate(ld) shouldBe java.sql.Date.valueOf(ld)
    ValueCoercion.coerceToDate("2026-01-02") shouldBe java.sql.Date.valueOf("2026-01-02")
    ValueCoercion.coerceToDate(Instant.ofEpochMilli(EpochMillis)).getTime shouldBe EpochMillis
  }

  it should "still reject a value with no date meaning" in {
    a[java.sql.SQLException] should be thrownBy ValueCoercion.coerceToDate(new Object)
  }

  // Negative control for the DELIBERATE asymmetry recorded in coerceToDate's scaladoc: a bare
  // number in a TIME position is ambiguous, so coerceToTime must NOT grow a Number arm by
  // symmetry. If someone "fixes" that for consistency, this test tells them it was a decision.
  "coerceToTime" should "deliberately reject a Number" in {
    a[java.sql.SQLException] should be thrownBy ValueCoercion.coerceToTime(
      java.lang.Long.valueOf(EpochMillis)
    )
  }

  "coerceToTimestamp" should "remain the reference for the Number arm" in {
    ValueCoercion
      .coerceToTimestamp(java.lang.Long.valueOf(EpochMillis))
      .getTime shouldBe EpochMillis
  }

  // Documents the ES-mapping consequence that made arrow#168 reachable at all: an ES `date`
  // mapping resolves to SQLTypes.Date (typeId "DATE"), which arrow-core turns into a Date64
  // vector — while the _source value may be a plain number.
  "SQLTypes" should "resolve the ES 'date' mapping token to SQLTypes.Date" in {
    SQLTypes("date") shouldBe SQLTypes.Date
    SQLTypes.Date.typeId shouldBe "DATE"
  }

  // Subject restated on purpose — this is about coerceToDate, not about SQLTypes.
  "coerceToDate" should "map an epoch-millis Long to the same UTC calendar date in every JVM zone" in {
    // Derived from Instant + ZoneOffset.UTC, so the expectation carries no dependency on the
    // machine's default zone — the assertion holds in every CI timezone.
    Instant
      .ofEpochMilli(ValueCoercion.coerceToDate(java.lang.Long.valueOf(EpochMillis)).getTime)
      .atZone(ZoneOffset.UTC)
      .toLocalDate shouldBe ExpectedDate
  }

  // ─── SCHEMA-17 fix R-171: coerceToString at every depth ──────────────────────────────────────

  private val Hello: Array[Byte] = "hello".getBytes(StandardCharsets.UTF_8)

  /** `coerceToString` as it was before R-171, verbatim: the oracle of every FLAT value (AC2). */
  private def before(value: Any): String = value match {
    case null           => null
    case s: String      => s
    case b: Array[Byte] => java.util.Base64.getEncoder.encodeToString(b)
    case seq: Seq[_]    => seq.mkString("[", ", ", "]")
    case map: Map[_, _] => map.map { case (k, v) => s"$k: $v" }.mkString("{", ", ", "}")
    case other          => other.toString
  }

  "coerceToString" should "render a nested object, list or bytes through itself, in today's style (R-171 AC1)" in {
    ValueCoercion.coerceToString(ListMap("x" -> 1, "y" -> ListMap("z" -> 2))) shouldBe
    "{x: 1, y: {z: 2}}"
    ValueCoercion.coerceToString(List(1, List(2, 3))) shouldBe "[1, [2, 3]]"
    ValueCoercion.coerceToString(List(ListMap("a" -> 1), ListMap("a" -> 2))) shouldBe
    "[{a: 1}, {a: 2}]"
    ValueCoercion.coerceToString(ListMap("k" -> List(1, 2))) shouldBe "{k: [1, 2]}"
    // nested bytes: their base64, as at top level (was `[B@<hash>`)
    ValueCoercion.coerceToString(ListMap("b" -> Hello)) shouldBe "{b: aGVsbG8=}"
    ValueCoercion.coerceToString(List(Hello, null)) shouldBe "[aGVsbG8=, null]"
    // a nested null is `null`, a text unquoted
    ValueCoercion.coerceToString(ListMap("n" -> null, "s" -> "a b")) shouldBe "{n: null, s: a b}"
    ValueCoercion.coerceToString(List("x", ListMap("s" -> "y", "t" -> null))) shouldBe
    "[x, {s: y, t: null}]"
  }

  it should "write a map's entries in its own iteration order at every depth, never sorted (R-171 AC1)" in {
    ValueCoercion.coerceToString(ListMap("z" -> 1, "a" -> ListMap("y" -> 2, "b" -> 3))) shouldBe
    "{z: 1, a: {y: 2, b: 3}}"
    // a HashMap's order is its own, whatever it is: its entries as it iterates them
    val hash = HashMap((1 to 8).map(i => s"k$i" -> List(i, ListMap("v" -> i))): _*)
    ValueCoercion.coerceToString(hash) shouldBe
    hash.iterator.map { case (k, l) => s"$k: [${l.head}, {v: ${l.head}}]" }.mkString("{", ", ", "}")
  }

  it should "render {map, list, bytes, null} nested in every container class at depth 1, 2 and 3 (R-171 AC1, AC3)" in {
    // the leaves, each with the text AC1 gives it
    val leaves: Seq[(String, Any, String)] = Seq(
      ("map", ListMap("a" -> 1, "b" -> "x"), "{a: 1, b: x}"),
      ("list", List(1, 2), "[1, 2]"),
      ("bytes", Hello, "aGVsbG8="),
      ("null", null, "null")
    )
    // the container classes core hands to a consumer (T0-1), each with the text it wraps its
    // element in: a map with one entry or with two (`Map2` keeps its insertion order), a list
    val wrappers: Seq[(String, Any => Any, String => String)] = Seq(
      ("ListMap", (v: Any) => ListMap("k" -> v), (t: String) => s"{k: $t}"),
      ("Map", (v: Any) => Map("k" -> v, "j" -> 0), (t: String) => s"{k: $t, j: 0}"),
      ("List", (v: Any) => List(v), (t: String) => s"[$t]"),
      ("Vector", (v: Any) => Vector(0, v), (t: String) => s"[0, $t]")
    )
    def paths(depth: Int): Seq[List[(String, Any => Any, String => String)]] =
      if (depth == 0) Seq(Nil)
      else for { w <- wrappers; rest <- paths(depth - 1) } yield w :: rest
    val cases: Seq[(String, Any, String)] =
      for {
        depth <- 1 to 3
        path  <- paths(depth)
        leaf  <- leaves
      } yield {
        val (leafName, leafValue, leafText) = leaf
        val value = path.foldRight(leafValue)((w, inner) => w._2(inner))
        val text = path.foldRight(leafText)((w, inner) => w._3(inner))
        (s"${path.map(_._1).mkString(" > ")} > $leafName", value, text)
      }
    cases.size shouldBe (4 + 16 + 64) * 4
    val wrong = cases.flatMap { case (name, value, text) =>
      val rendered = ValueCoercion.coerceToString(value)
      if (rendered == text) None else Some(s"$name: <$rendered> instead of <$text>")
    }
    wrong shouldBe empty
  }

  it should "render every flat value byte-identically to before (R-171 AC2)" in {
    val day = LocalDate.of(2024, 1, 31)
    val flat: Seq[Any] = Seq(
      "hello",
      "",
      Int.box(42),
      Long.box(42L),
      Double.box(1.5),
      Float.box(2.5f),
      Short.box(7.toShort),
      Byte.box(8.toByte),
      new java.math.BigDecimal("1.50"),
      new java.math.BigInteger("18446744073709551615"),
      BigDecimal("2.50"),
      Boolean.box(true),
      Boolean.box(false),
      day,
      LocalTime.of(10, 30),
      LocalTime.of(10, 30, 15, 123000000),
      LocalDateTime.of(2024, 1, 31, 10, 30),
      ZonedDateTime.of(2024, 1, 31, 10, 30, 0, 0, ZoneOffset.UTC),
      ZonedDateTime.of(2024, 1, 31, 10, 30, 0, 123456789, ZoneOffset.UTC),
      OffsetDateTime.of(2024, 1, 31, 10, 30, 0, 0, ZoneOffset.ofHours(1)),
      Instant.parse("2024-01-31T10:30:00Z"),
      java.sql.Date.valueOf("2024-01-31"),
      java.sql.Timestamp.valueOf("2024-01-31 10:30:00"),
      Hello,
      List(1, "a", null, 2.5, true, day),
      Vector(3L, "b"),
      List(),
      ListMap("a" -> 1, "b" -> "x", "c" -> null, "d" -> day),
      Map("only"  -> 1.5),
      HashMap((1 to 8).map(i => s"k$i" -> i): _*),
      Map.empty[String, Any],
      ListMap("lat" -> 1.5, "lon" -> 2.5),
      // a top hit's `bucket_root` is an Option inside a map: its `toString`, as before
      ListMap("v" -> 1, "bucket_root" -> Some("root"))
    )
    flat.foreach { value =>
      withClue(s"$value (${value.getClass.getName}): ") {
        ValueCoercion.coerceToString(value) shouldBe before(value)
      }
    }
    // the literal pins: top-level bytes (ResultSchemaSpec), a flat list, a flat map, null
    ValueCoercion.coerceToString(Hello) shouldBe "aGVsbG8="
    ValueCoercion.coerceToString(List(1, "a", null, 2.5)) shouldBe "[1, a, null, 2.5]"
    ValueCoercion.coerceToString(ListMap("a" -> 1, "b" -> "x", "c" -> null)) shouldBe
    "{a: 1, b: x, c: null}"
    ValueCoercion.coerceToString(List()) shouldBe "[]"
    ValueCoercion.coerceToString(Map.empty[String, Any]) shouldBe "{}"
    ValueCoercion.coerceToString(null) shouldBe null
  }

  it should "hand a text back as the same instance, its fast arm (R-171 AC5)" in {
    val text = new String("abc")
    ValueCoercion.coerceToString(text) should be theSameInstanceAs text
  }
}
