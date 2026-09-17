package app.softnetwork.elastic.client

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

/** Pins the single-pass [[ElasticConversion.rowNormalizer]] (#229) against the legacy
  * [[ElasticConversion.normalizeRow]] contract: requested fields first, in SQL SELECT order —
  * missing ones null-filled (or skipped under [[EntityContext]]) — then the row's extra entries in
  * their original order. Order is asserted on `.toList` (ListMap equality ignores order).
  */
class RowNormalizerSpec extends AnyFlatSpec with Matchers with ElasticConversion {

  implicit val context: ConversionContext = NativeContext

  private val fields = Seq("a", "b", "c")

  private def normalize(
    row: ListMap[String, Any],
    requestedFields: Seq[String] = fields
  )(implicit ctx: ConversionContext): ListMap[String, Any] =
    rowNormalizer(requestedFields)(ctx)(row)

  private def legacy(
    row: ListMap[String, Any],
    requestedFields: Seq[String] = fields
  )(implicit ctx: ConversionContext): ListMap[String, Any] =
    normalizeRow(row, requestedFields)(ctx)

  "rowNormalizer" should "return an already-shaped row as the same instance" in {
    val row = ListMap[String, Any]("a" -> 1, "b" -> 2, "c" -> 3)
    normalize(row) should be theSameInstanceAs row
  }

  it should "return a row with in-order fields followed by extras as the same instance" in {
    val row = ListMap[String, Any]("a" -> 1, "b" -> 2, "c" -> 3, "_id" -> "42", "extra" -> true)
    normalize(row) should be theSameInstanceAs row
  }

  it should "reorder fields to the SQL SELECT order" in {
    val row = ListMap[String, Any]("c" -> 3, "a" -> 1, "b" -> 2)
    normalize(row).toList shouldBe List("a" -> 1, "b" -> 2, "c" -> 3)
  }

  it should "null-fill missing fields in native context" in {
    val row = ListMap[String, Any]("b" -> 2)
    normalize(row).toList shouldBe List("a" -> null, "b" -> 2, "c" -> null)
  }

  it should "append extras after the requested fields, preserving their original order" in {
    val row = ListMap[String, Any]("x" -> 0, "c" -> 3, "y" -> 9, "a" -> 1)
    normalize(row).toList shouldBe List("a" -> 1, "b" -> null, "c" -> 3, "x" -> 0, "y" -> 9)
  }

  it should "resume positional matching after an extra breaks a non-empty in-order prefix" in {
    val row = ListMap[String, Any]("a" -> 1, "x" -> 0, "b" -> 2, "c" -> 3)
    normalize(row).toList shouldBe List("a" -> 1, "b" -> 2, "c" -> 3, "x" -> 0)
  }

  it should "keep a present-but-null value as present" in {
    val row = ListMap[String, Any]("a" -> null, "b" -> 2, "c" -> 3)
    normalize(row) should be theSameInstanceAs row
    val reordered = ListMap[String, Any]("b" -> 2, "a" -> null, "c" -> 3)
    normalize(reordered).toList shouldBe List("a" -> null, "b" -> 2, "c" -> 3)
  }

  it should "normalize an empty row to all-null fields in native context" in {
    normalize(ListMap.empty[String, Any]).toList shouldBe
    List("a" -> null, "b" -> null, "c" -> null)
  }

  it should "return the row unchanged when no fields are requested" in {
    val row = ListMap[String, Any]("z" -> 26, "a" -> 1)
    normalize(row, Seq.empty) should be theSameInstanceAs row
  }

  it should "skip missing fields in entity context" in {
    val row = ListMap[String, Any]("c" -> 3, "a" -> 1)
    normalize(row)(EntityContext).toList shouldBe List("a" -> 1, "c" -> 3)
  }

  it should "return an in-order strict prefix as the same instance in entity context" in {
    val row = ListMap[String, Any]("a" -> 1, "b" -> 2)
    normalize(row)(EntityContext) should be theSameInstanceAs row
  }

  it should "keep a present-but-null value in entity context" in {
    val row = ListMap[String, Any]("b" -> null, "a" -> 1)
    normalize(row)(EntityContext).toList shouldBe List("a" -> 1, "b" -> null)
  }

  it should "match the legacy normalizeRow output on every shape, in both contexts" in {
    val rows = Seq(
      ListMap[String, Any]("a" -> 1, "b"    -> 2, "c" -> 3),
      ListMap[String, Any]("c" -> 3, "b"    -> 2, "a" -> 1),
      ListMap[String, Any]("b" -> 2),
      ListMap[String, Any]("a" -> 1, "b"    -> 2, "c" -> 3, "_id" -> "42"),
      ListMap[String, Any]("x" -> 0, "c"    -> 3, "y" -> 9, "a"   -> 1),
      ListMap[String, Any]("a" -> 1, "x"    -> 0, "b" -> 2, "c"   -> 3),
      ListMap[String, Any]("a" -> null, "c" -> 3),
      ListMap[String, Any]("x" -> 0, "y"    -> 9),
      ListMap.empty[String, Any]
    )
    for (row <- rows) {
      normalize(row)(NativeContext).toList shouldBe legacy(row)(NativeContext).toList
      normalize(row)(EntityContext).toList shouldBe legacy(row)(EntityContext).toList
      normalize(row, Seq.empty)(NativeContext).toList shouldBe
      legacy(row, Seq.empty)(NativeContext).toList
    }
  }

  it should "normalize a heterogeneous stream of rows through ONE normalizer instance" in {
    // The production pattern: one closure built per stream, applied to every row — each
    // invocation must be independent (no state may leak between rows)
    val normalizer = rowNormalizer(fields)(NativeContext)
    val shaped = ListMap[String, Any]("a" -> 1, "b" -> 2, "c" -> 3)
    val stream = Seq(
      shaped,
      ListMap[String, Any]("c" -> 30, "a" -> 10),
      ListMap[String, Any]("x" -> 0, "b"  -> 200),
      ListMap.empty[String, Any],
      ListMap[String, Any]("a" -> 1000, "b" -> 2000, "c" -> 3000, "_id" -> "42"),
      shaped
    )
    val normalized = stream.map(normalizer)
    normalized.map(_.toList) shouldBe Seq(
      List("a" -> 1, "b"    -> 2, "c"    -> 3),
      List("a" -> 10, "b"   -> null, "c" -> 30),
      List("a" -> null, "b" -> 200, "c"  -> null, "x"   -> 0),
      List("a" -> null, "b" -> null, "c" -> null),
      List("a" -> 1000, "b" -> 2000, "c" -> 3000, "_id" -> "42"),
      List("a" -> 1, "b"    -> 2, "c"    -> 3)
    )
    normalized.head should be theSameInstanceAs shaped
    normalized.last should be theSameInstanceAs shaped
  }

  it should "fall back to the legacy semantics when requested fields contain duplicates" in {
    val duplicated = Seq("a", "b", "a")
    val rows = Seq(
      ListMap[String, Any]("a" -> 1, "b"     -> 2),
      ListMap[String, Any]("b" -> 2, "extra" -> true),
      ListMap.empty[String, Any]
    )
    for (row <- rows) {
      normalize(row, duplicated)(NativeContext).toList shouldBe
      legacy(row, duplicated)(NativeContext).toList
      normalize(row, duplicated)(EntityContext).toList shouldBe
      legacy(row, duplicated)(EntityContext).toList
    }
  }

  // ── rowProjector: the same walk with the OUTPUT names decoupled (issue #354) ───────────────

  private def project(
    row: ListMap[String, Any],
    source: Seq[String],
    target: Seq[String]
  )(implicit ctx: ConversionContext): ListMap[String, Any] =
    rowProjector(source, target)(ctx)(row)

  "rowProjector" should "be rowNormalizer exactly when the two name lists are equal" in {
    val row = ListMap[String, Any]("a" -> 1, "b" -> 2, "c" -> 3, "_id" -> "42")
    // including the fast path: an already-shaped row comes back as the SAME instance
    project(row, fields, fields)(NativeContext) should be theSameInstanceAs row
    project(ListMap[String, Any]("c" -> 3), fields, fields)(NativeContext).toList shouldBe
    normalize(ListMap[String, Any]("c" -> 3))(NativeContext).toList
  }

  it should "rename column i to target i, whatever either side calls it" in {
    // the reordered-branch shape: the row is keyed by the BRANCH's names, in the BRANCH's order
    val row = ListMap[String, Any]("tag" -> "t", "category" -> "c")
    project(row, Seq("tag", "category"), Seq("category", "tag"))(NativeContext).toList shouldBe
    List("category" -> "t", "tag" -> "c")
  }

  it should "rebuild even when the row is already in source order" in {
    // 🔴 the fast path MUST be off under a rename: the row IS in order under its own names, and
    // returning it unchanged would answer with the branch's names instead of the result's.
    val row = ListMap[String, Any]("y" -> 9)
    project(row, Seq("y"), Seq("x"))(NativeContext).toList shouldBe List("x" -> 9)
  }

  it should "null-fill a target whose source the row does not carry, and append extras" in {
    val row = ListMap[String, Any]("q" -> 1, "extra" -> true)
    project(row, Seq("p", "q"), Seq("a", "b"))(NativeContext).toList shouldBe
    List("a" -> null, "b" -> 1, "extra" -> true)
    // …and EntityContext skips the missing one rather than null-filling it, as it always has
    project(row, Seq("p", "q"), Seq("a", "b"))(EntityContext).toList shouldBe
    List("b" -> 1, "extra" -> true)
  }

  /** 🔴 A hazard that exists ONLY once the two lists differ: before, a row key equal to a requested
    * name was always found by the name index and could never become an "extra". Now it can, and
    * appending it CLOBBERED the column the projection had just filled — the declared column read
    * back as a raw nested object, or as Elasticsearch's `_id` instead of the branch's own.
    *
    * Reachable shapes, all measured on this engine: the PARENT of a dotted path (`parseSimpleHits`
    * re-adds `addr.city` as `city` while `addr` survives from `_source`), `_id` when the
    * document-id column is on, and the internal aggregation key a metric leaves behind.
    */
  it should "not let a stray row entry clobber a result column that shares its name" in {
    val row = ListMap[String, Any]("profileId" -> "P", "profiles" -> ListMap("city" -> "Paris"))
    project(row, Seq("profileId", "profiles.city"), Seq("profileId", "profiles"))(
      NativeContext
    ).toList shouldBe List("profileId" -> "P", "profiles" -> null)

    val withCity = ListMap[String, Any](
      "profileId" -> "P",
      "profiles"  -> ListMap("city" -> "Paris"),
      "city"      -> "Paris"
    )
    project(withCity, Seq("profileId", "city"), Seq("profileId", "profiles"))(
      NativeContext
    ).toList shouldBe List("profileId" -> "P", "profiles" -> "Paris")

    // the `_id` shape: the branch's own `id` is what column 1 holds, not the document id
    val withDocId = ListMap[String, Any]("id" -> "biz-7", "_id" -> "esdoc-123")
    project(withDocId, Seq("id"), Seq("_id"))(NativeContext).toList shouldBe
    List("_id" -> "biz-7")
  }

  /** …and the same guard in the duplicate-source arm, which builds by walking the TARGET list. */
  it should "not let a stray row entry clobber a result column under a duplicate projection" in {
    val row = ListMap[String, Any]("a" -> 7, "e" -> 1)
    project(row, Seq("a", "a"), Seq("x", "e"))(NativeContext).toList shouldBe
    List("x" -> 7, "e" -> 7)
  }

  /** 🔴 A result column name repeated at two positions: a row MAP cannot hold it twice, so the
    * FIRST position is the one that survives. Emitting both let the SECOND win, so column 1
    * displayed column 2's value — and it diverged across cross-builds, because 2.13's `ListMap`
    * builder replaces a duplicate key in place while 2.12's removes and re-appends it.
    */
  it should "keep the FIRST position when the target names repeat" in {
    val row = ListMap[String, Any]("amount" -> "A", "m" -> "M")
    project(row, Seq("amount", "m"), Seq("amount", "amount"))(NativeContext).toList shouldBe
    List("amount" -> "A")
    // …including when the first position has no value to supply
    project(ListMap[String, Any]("m" -> "M"), Seq("amount", "m"), Seq("amount", "amount"))(
      NativeContext
    ).toList shouldBe List("amount" -> null)
  }

  it should "feed every target that names the same source under a duplicate projection" in {
    // `SELECT a, a` renamed onto `(x, y)`: a row map holds ONE `a`, and both columns read it
    val row = ListMap[String, Any]("a" -> 7, "extra" -> true)
    project(row, Seq("a", "a"), Seq("x", "y"))(NativeContext).toList shouldBe
    List("x" -> 7, "y" -> 7, "extra" -> true)
  }

  it should "degrade to a plain normalization when the target list has a different length" in {
    // no positional alignment exists, so nothing is guessed
    val row = ListMap[String, Any]("b" -> 2)
    project(row, fields, Seq("x"))(NativeContext).toList shouldBe
    normalize(row)(NativeContext).toList
  }

  it should "be identity when there is no source projection to match" in {
    val row = ListMap[String, Any]("whatever" -> 1)
    project(row, Seq.empty, Seq("a"))(NativeContext) should be theSameInstanceAs row
  }
}
