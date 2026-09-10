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

package app.softnetwork.elastic.client.metadata

import app.softnetwork.elastic.client.ElasticConversion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.immutable.ListMap

/** Story BIDC-10a — one Scala-to-Java value conversion, in core, for the `getObject` contract of
  * every driver.
  *
  * Core's read path hands out Scala `Map`s and `List`s as CELL VALUES. A JDBC or ADBC consumer
  * calling `getObject` must receive `java.util.Map` / `java.util.List`; without this, a host that
  * casts gets a `ClassCastException` and one that calls `toString` gets Scala syntax. Each driver
  * writing its own is the duplication the type catalogue just removed one layer up.
  */
class JavaValueConversionSpec extends AnyFlatSpec with Matchers {

  import JavaValueConversion.toJavaValue

  /** 🔴 THE test, written first and for a reason: it is the one a SHALLOW implementation passes
    * everything else on.
    *
    * A nested field yields `List[Map[String, Any]]` — three of core's producers make exactly this
    * shape (`jsonNodeToAny`'s array branch, and `extractInnerHits`, which builds a
    * `List[ListMap[String, Any]]` by hand). A shallow `asJava` on the outer list returns a
    * `java.util.List` whose ELEMENTS are still Scala maps, so the defect survives one level down
    * and every single-level test still passes.
    */
  "toJavaValue" should "convert a Map nested inside a List (the shallow-conversion trap)" in {
    val value = List(ListMap("city" -> "Paris", "zip" -> 75001))

    val converted = toJavaValue(value)

    converted shouldBe a[java.util.List[_]]
    val list = converted.asInstanceOf[java.util.List[Any]]
    list.size shouldBe 1
    withClue("the ELEMENT of the list is still a Scala Map: ") {
      list.get(0) shouldBe a[java.util.Map[_, _]]
    }
    val inner = list.get(0).asInstanceOf[java.util.Map[String, Any]]
    inner.get("city") shouldBe "Paris"
    inner.get("zip") shouldBe 75001
  }

  it should "convert a List nested inside a Map, the other way round" in {
    val value = ListMap("tags" -> List("a", "b"))

    val inner = toJavaValue(value).asInstanceOf[java.util.Map[String, Any]].get("tags")

    inner shouldBe a[java.util.List[_]]
    inner.asInstanceOf[java.util.List[Any]].size shouldBe 2
  }

  it should "convert arbitrarily deep alternations" in {
    val value = List(ListMap("inner" -> List(ListMap("leaf" -> 1))))

    val leaf = toJavaValue(value)
      .asInstanceOf[java.util.List[Any]]
      .get(0)
      .asInstanceOf[java.util.Map[String, Any]]
      .get("inner")
      .asInstanceOf[java.util.List[Any]]
      .get(0)

    leaf shouldBe a[java.util.Map[_, _]]
    leaf.asInstanceOf[java.util.Map[String, Any]].get("leaf") shouldBe 1
  }

  it should "convert a top-level Map" in {
    toJavaValue(ListMap("a" -> 1)) shouldBe a[java.util.Map[_, _]]
  }

  it should "convert a top-level List" in {
    toJavaValue(List(1, 2)) shouldBe a[java.util.List[_]]
  }

  /** Column order is meaningful to a consumer walking a struct, and `ListMap` is ordered, so the
    * conversion must not hand back a hash-ordered map.
    */
  it should "preserve key order" in {
    val value = ListMap("z" -> 1, "a" -> 2, "m" -> 3)
    val keys = toJavaValue(value).asInstanceOf[java.util.Map[String, Any]].keySet()
    keys.toArray.toSeq shouldBe Seq("z", "a", "m")
  }

  /** 🔴 The hot-path property. Every scalar cell on an extraction path passes through here, so a
    * non-collection must come back as the SAME reference — one type test, zero allocation.
    */
  it should "return non-collections by identity, allocating nothing" in {
    val s = "text"
    val i = java.lang.Integer.valueOf(7)
    val d = java.lang.Double.valueOf(1.5)
    val t = java.time.LocalDate.of(2026, 1, 1)
    // `.asInstanceOf[AnyRef]` only to satisfy `theSameInstanceAs`, which needs an AnyRef; the
    // assertion is still reference identity. (`Cannot prove that Any <:< AnyRef` otherwise.)
    toJavaValue(s).asInstanceOf[AnyRef] should be theSameInstanceAs s
    toJavaValue(i).asInstanceOf[AnyRef] should be theSameInstanceAs i
    toJavaValue(d).asInstanceOf[AnyRef] should be theSameInstanceAs d
    toJavaValue(t).asInstanceOf[AnyRef] should be theSameInstanceAs t
  }

  it should "pass null through" in {
    // `shouldBe null` does not compile on an `Any`; this is the same assertion.
    Option(toJavaValue(null)) shouldBe None
  }

  /** A `VARBINARY` cell is a `byte[]`. An Array is not a Scala `Seq`, so it must survive untouched
    * — converting it to a `java.util.List[Byte]` would break every consumer that casts to `[B`.
    */
  it should "leave a byte array alone" in {
    val bytes = Array[Byte](1, 2, 3)
    toJavaValue(bytes).asInstanceOf[AnyRef] should be theSameInstanceAs bytes
  }

  it should "convert an empty collection rather than passing it through" in {
    toJavaValue(List.empty) shouldBe a[java.util.List[_]]
    toJavaValue(ListMap.empty) shouldBe a[java.util.Map[_, _]]
  }

  /** 🔴 The census, pinned against REAL core code rather than a hand-built shape.
    *
    * `toJavaValue` is only worth anything if core actually hands out Scala collections. This drives
    * `ElasticConversion.jsonNodeToAny` — producer 1 — with a document whose field is an array of
    * objects, and asserts BOTH halves: that what comes back is a Scala `List` of Scala `Map`s (so
    * the problem is real), and that the conversion resolves it at every level (so the fix is).
    *
    * If a producer is ever changed to emit Java collections directly, this test says so instead of
    * `toJavaValue` silently becoming dead code.
    */
  it should "convert what ElasticConversion actually produces for a nested field" in {
    val node = new com.fasterxml.jackson.databind.ObjectMapper()
      .readTree("""{"addresses":[{"city":"Paris","zip":75001},{"city":"Lyon","zip":69001}]}""")

    val produced = ElasticConversion.jsonNodeToAny(node.get("addresses"), ListMap.empty)

    withClue("core no longer produces a Scala List here: ") {
      produced shouldBe a[scala.collection.immutable.List[_]]
    }
    withClue("core no longer produces a Scala Map inside it: ") {
      produced.asInstanceOf[List[Any]].head shouldBe a[scala.collection.Map[_, _]]
    }

    val converted = toJavaValue(produced).asInstanceOf[java.util.List[Any]]
    converted.size shouldBe 2
    converted.get(0) shouldBe a[java.util.Map[_, _]]
    converted.get(0).asInstanceOf[java.util.Map[String, Any]].get("city") shouldBe "Paris"
    converted.get(1).asInstanceOf[java.util.Map[String, Any]].get("zip") shouldBe 69001
  }

  /** The shapes core's three producers actually emit, asserted as one. If a fourth producer appears
    * with a shape not covered here, this is where it should be added.
    */
  it should "handle every shape core's read path produces" in {
    // 1. jsonNodeToAny: an array of scalars, and a nested object
    toJavaValue(List("a", "b")) shouldBe a[java.util.List[_]]
    toJavaValue(ListMap("k" -> "v")) shouldBe a[java.util.Map[_, _]]
    // 2. the aggregation path: a stats/percentiles ListMap as a CELL value
    val stats = ListMap("count" -> 3L, "sum" -> 1.5, "avg" -> 0.5)
    val javaStats = toJavaValue(stats).asInstanceOf[java.util.Map[String, Any]]
    javaStats.get("count") shouldBe 3L
    javaStats.size shouldBe 3
    // 3. extractInnerHits: a List of row-shaped ListMaps
    val innerHits = List(ListMap("id" -> 1), ListMap("id" -> 2))
    val javaHits = toJavaValue(innerHits).asInstanceOf[java.util.List[Any]]
    javaHits.size shouldBe 2
    javaHits.get(1) shouldBe a[java.util.Map[_, _]]
  }
}
