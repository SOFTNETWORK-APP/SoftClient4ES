package app.softnetwork.elastic.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

/** `ObjectValue.set`/`remove` carry the table's `_meta`, whose paths are three and four levels deep
  * (`_meta.columns.<column>.default_value`). Descending to the leaf's parent and re-attaching it
  * under the FIRST key collapsed everything in between — the deeper the path, the more siblings
  * disappeared — so a metadata update silently replaced `_meta` with its innermost object.
  */
class ObjectValuePathSpec extends AnyFlatSpec with Matchers {

  private def meta: ObjectValue =
    ObjectValue(
      ListMap(
        "type" -> StringValue("regular"),
        "columns" -> ObjectValue(
          ListMap(
            "id" -> ObjectValue(ListMap("data_type" -> StringValue("INT"))),
            "name" -> ObjectValue(ListMap("data_type" -> StringValue("VARCHAR")))
          )
        )
      )
    )

  "set" should "reach a depth-3 path without dropping its siblings" in {
    val updated = meta.set("columns.id.default_value", IngestTimestampValue)
    updated.find("columns.id.default_value") shouldBe Some(IngestTimestampValue)
    updated.find("columns.id.data_type") shouldBe Some(StringValue("INT"))
    updated.find("columns.name.data_type") shouldBe Some(StringValue("VARCHAR"))
    updated.find("type") shouldBe Some(StringValue("regular"))
  }

  it should "create the intermediate levels of a path that does not exist yet" in {
    val updated = meta.set("columns.age.data_type", StringValue("INT"))
    updated.find("columns.age.data_type") shouldBe Some(StringValue("INT"))
    updated.find("columns.id.data_type") shouldBe Some(StringValue("INT"))
    updated.find("type") shouldBe Some(StringValue("regular"))
  }

  it should "still handle depth 1 and 2" in {
    meta.set("type", StringValue("view")).find("type") shouldBe Some(StringValue("view"))
    val d2 = meta.set("columns.extra", StringValue("x"))
    d2.find("columns.extra") shouldBe Some(StringValue("x"))
    d2.find("columns.id.data_type") shouldBe Some(StringValue("INT"))
  }

  "remove" should "reach a depth-3 path without dropping its siblings" in {
    val updated = meta.remove("columns.id.data_type")
    updated.find("columns.id.data_type") shouldBe None
    updated.find("columns.name.data_type") shouldBe Some(StringValue("VARCHAR"))
    updated.find("type") shouldBe Some(StringValue("regular"))
  }

  it should "leave the object untouched when the path is absent" in {
    // Used to overwrite the head key with an empty object, deleting everything under it.
    meta.remove("columns.absent.data_type") shouldBe meta
    meta.remove("absent.deeper.still") shouldBe meta
  }
}
