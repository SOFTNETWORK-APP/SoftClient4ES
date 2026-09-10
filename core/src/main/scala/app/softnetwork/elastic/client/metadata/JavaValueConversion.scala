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

/** Converts a core row VALUE into the Java collections a `getObject` consumer expects.
  *
  * Story BIDC-10a. Core's read path hands out Scala `Map`s and `List`s as cell values; a JDBC or
  * ADBC consumer that casts gets a `ClassCastException`, and one that calls `toString` gets Scala
  * syntax. Both drivers need the same conversion, so it lives here once rather than twice.
  *
  * ==Why this is NOT at the producer==
  *
  * The obvious placement is `ElasticConversion.jsonNodeToAny`, which builds the `List` and the
  * nested `Map` from `_source`. A census of the read path (2026-09-10) says that would be honest
  * for one path and a lie for two others. **Three** producers make these shapes, and only the first
  * goes through `jsonNodeToAny`:
  *
  *   1. `ElasticConversion.jsonNodeToAny` — the array branch (a Scala `List`) and the object branch
  *      (`jsonNodeToMap`, a `ListMap`). 2. The aggregation path — a stats aggregation emits `name
  * -> ListMap("count" -> ..., "sum" -> ...)` and `percentiles` emits `name -> ListMap(<key> ->
  * <double>)`, both as CELL values, neither built from a `JsonNode` through `jsonNodeToAny`. 3.
  * `ElasticConversion.extractInnerHits` — `innerHitName -> List[ListMap[String, Any]]`, hand
  * assembled. That is precisely the `List`-of-`Map` shape a shallow conversion gets wrong.
  *
  * ⇒ The conversion belongs at the CONSUMER boundary, where it is producer-agnostic by
  * construction: it converts whatever value it is handed, so a fourth producer needs no change
  * here. Placing it beside one producer would have needed a change for every new one.
  *
  * It lives in `metadata`, with the type catalogue, for the same reason: nothing inside core calls
  * it. It exists solely to satisfy a driver-facing contract.
  *
  * ==Eager or lazy: lazy, at the accessor, with an identity fast path==
  *
  * Converting every row at extraction time would put allocation on the extraction hot path for
  * EVERY row of EVERY query — including the overwhelming majority where no cell is a collection and
  * no consumer ever calls `getObject`. That is the path #238 and arrow#139 exist to keep cheap, and
  * the standing rule is that per-row work on an extraction path must not be re-derived per cell.
  *
  * So a driver calls this from `getObject`, on demand, per accessed cell. A non-collection returns
  * the SAME REFERENCE — one type test, zero allocation — so the common case costs effectively
  * nothing and is paid only for cells someone actually asks for.
  *
  * Deliberately NOT memoised here: core does not own the row's lifetime. A driver that measures
  * repeated `getObject` calls on the same wide struct can cache per row at its own seam, where the
  * lifetime is known.
  *
  * ==Boundaries==
  *
  *   - Call it on a VALUE, never on the row itself. A row is a `ListMap[String, Any]` and so is a
  *     nested struct; this function cannot tell them apart, and converting the row would break
  *     every by-column accessor.
  *   - A `byte[]` (a `VARBINARY` cell) is a Java array, not a Scala `Seq`, so it survives
  *     untouched. Converting it to a `java.util.List[Byte]` would break every consumer that casts
  *     to `[B`.
  *   - `Option` is deliberately not unwrapped. No producer emits one today (the stats aggregation
  *     unwraps its own via `collect`), so unwrapping here would hide a real defect rather than fix
  *     one.
  */
object JavaValueConversion {

  /** Deep-converts Scala collections to their Java counterparts; returns everything else by
    * identity.
    *
    * 🔴 DEEP is the whole point. A nested field yields `List[Map[String, Any]]`, so a shallow
    * `asJava` on the outer list hands back a `java.util.List` whose elements are still Scala maps
    * and the defect survives one level down — while every single-level test still passes. Measured:
    * a shallow implementation passes 7 of this object's 11 tests, including key order, identity,
    * null, byte arrays and empty collections, and fails only the four depth-sensitive ones.
    */
  def toJavaValue(value: Any): Any = value match {
    case null => null

    // `LinkedHashMap`, not `HashMap`: core's maps are `ListMap`s and their key order is meaningful
    // to a consumer walking a struct or a stats aggregation.
    case m: scala.collection.Map[_, _] =>
      val out = new java.util.LinkedHashMap[Any, Any](Math.max(4, m.size * 2))
      m.foreach { case (k, v) => out.put(toJavaValue(k), toJavaValue(v)) }
      out

    case s: scala.collection.Seq[_] =>
      val out = new java.util.ArrayList[Any](s.size)
      s.foreach(v => out.add(toJavaValue(v)))
      out

    case s: scala.collection.Set[_] =>
      val out = new java.util.LinkedHashSet[Any](Math.max(4, s.size * 2))
      s.foreach(v => out.add(toJavaValue(v)))
      out

    // The hot path: one type test, same reference back, nothing allocated.
    case other => other
  }
}
