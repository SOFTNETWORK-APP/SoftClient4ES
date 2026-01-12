package app.softnetwork.elastic.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class ObjectValueSpec extends AnyFlatSpec with Matchers {

  "ObjectValue" should "correctly store and retrieve nested objects" in {
    val nestedObject = Map(
      "level1" -> ObjectValue(
        Map(
          "level2" -> ObjectValue(
            Map(
              "level3" -> StringValue("value")
            )
          )
        )
      )
    )

    val objectValue = ObjectValue(nestedObject)

    val retrievedObject = objectValue.value
    val level1 = retrievedObject("level1").asInstanceOf[ObjectValue].value
    val level2 = level1("level2").asInstanceOf[ObjectValue].value
    val level3Value = level2("level3").asInstanceOf[StringValue]

    level3Value.value shouldEqual "value"

    objectValue.find("level1.level2.level3").getOrElse(Null).value shouldBe "value"
  }

  it should "return Null for non-existing paths" in {
    val objectValue = ObjectValue(
      Map(
        "key1" -> StringValue("value1"),
        "key2" -> ObjectValue(
          Map(
            "key3" -> StringValue("value3")
          )
        )
      )
    )

    objectValue.find("key2.key4") shouldBe None
    objectValue.find("key5") shouldBe None
  }

  it should "set values at specified paths" in {
    val objectValue = ObjectValue(
      Map(
        "key1" -> StringValue("value1"),
        "key2" -> ObjectValue(
          Map(
            "key3" -> StringValue("value3")
          )
        )
      )
    )

    val updatedObject = objectValue.set("key2.key4", StringValue("newValue"))

    val key2 = updatedObject.value("key2").asInstanceOf[ObjectValue].value
    val key4Value = key2("key4").asInstanceOf[StringValue]

    key4Value.value shouldEqual "newValue"
  }

  it should "overwrite existing values when setting at a path" in {
    val objectValue = ObjectValue(
      Map(
        "key1" -> StringValue("value1"),
        "key2" -> ObjectValue(
          Map(
            "key3" -> StringValue("value3")
          )
        )
      )
    )

    val updatedObject = objectValue.set("key2.key3", StringValue("updatedValue"))

    val key2 = updatedObject.value("key2").asInstanceOf[ObjectValue].value
    val key3Value = key2("key3").asInstanceOf[StringValue]

    key3Value.value shouldEqual "updatedValue"
  }

  it should "remove values at specified paths" in {
    val objectValue = ObjectValue(
      Map(
        "key1" -> StringValue("value1"),
        "key2" -> ObjectValue(
          Map(
            "key3" -> StringValue("value3"),
            "key4" -> StringValue("value4")
          )
        )
      )
    )

    val updatedObject = objectValue.remove("key2.key3")

    val key2 = updatedObject.value("key2").asInstanceOf[ObjectValue].value

    key2.contains("key3") shouldBe false
    key2.contains("key4") shouldBe true
  }

  it should "handle removal of non-existing paths gracefully" in {
    val objectValue = ObjectValue(
      Map(
        "key1" -> StringValue("value1"),
        "key2" -> ObjectValue(
          Map(
            "key3" -> StringValue("value3")
          )
        )
      )
    )

    val updatedObject = objectValue.remove("key2.key4")

    updatedObject shouldEqual objectValue
  }

  it should "correctly serialize and deserialize to/from JSON" in {
    val objectValue = ObjectValue(
      Map(
        "key1" -> StringValue("value1"),
        "key2" -> ObjectValue(
          Map(
            "key3" -> StringValue("value3")
          )
        )
      )
    )

    val jsonString = objectValue.toJson
    val deserializedObject = ObjectValue.fromJson(jsonString)

    deserializedObject shouldEqual objectValue
  }
}
