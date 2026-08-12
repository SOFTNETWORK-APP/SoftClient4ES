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
}
