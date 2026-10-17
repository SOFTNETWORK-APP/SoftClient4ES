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

package app.softnetwork.elastic.client.perf

import app.softnetwork.elastic.client.{
  mapper,
  ClientAggregation,
  ConversionContext,
  ElasticConversion,
  NativeContext
}
import com.fasterxml.jackson.databind.JsonNode

import scala.collection.immutable.ListMap

/** SCHEMA-12 cost probe: what building rows from one 1,000-hit page costs through
  * `parseResponseTree` -- the public row path, which the nested-path reader joins only for a
  * projection with an un-aliased dotted column.
  *
  * Public API only, so the SAME file runs on main and on the branch: the comparison is main vs
  * final, run as separate JVMs interleaved (control, branch, control, branch) by the caller.
  *
  * {{{
  * sbt "core/Test/runMain app.softnetwork.elastic.client.perf.NestedPathCostProbe 31 1000"
  * }}}
  *
  * Arguments, optional and positional: `blocks` (default 31) and `hits` per page (default 1,000).
  * Each block parses the page [[RepsPerBlock]] times; block 1 is reported as COLD (no warm-up
  * before it), the median of the remaining blocks as WARM.
  *
  * Shapes: s1 = five plain columns in `_source` order (`id, n, name, d, flag`); s2 = the aliased
  * control `obj.a AS k, n`; s3 = a GROUP BY response of `hits` buckets read as `obj.a, c`; and, for
  * information only, the changed shape `id, obj.a` (S2), which has no main equivalent.
  *
  * This probe NEVER asserts: it prints and exits (a timing assertion in a suite is a CI-flake
  * liability). It is a `main`-bearing object, not a ScalaTest suite.
  */
object NestedPathCostProbe extends ElasticConversion {

  val DefaultBlocks: Int = 31
  val DefaultHits: Int = 1000
  val RepsPerBlock: Int = 50

  private implicit val ctx: ConversionContext = NativeContext

  private final case class Shape(
    label: String,
    page: JsonNode,
    fieldAliases: ListMap[String, String],
    fields: Seq[String],
    aggregations: ListMap[String, ClientAggregation] = ListMap.empty
  )

  private def hitsPage(n: Int)(source: Int => String): JsonNode =
    mapper.readTree(
      (0 until n)
        .map(i => s"""{"_id":"d$i","_source":${source(i)}}""")
        .mkString(s"""{"took":1,"hits":{"total":{"value":$n},"hits":[""", ",", "]}}")
    )

  private def bucketsPage(n: Int): JsonNode =
    mapper.readTree(
      (0 until n)
        .map(i => s"""{"key":$i,"doc_count":${i % 7 + 1},"c":{"value":${i % 7 + 1}}}""")
        .mkString(
          s"""{"took":1,"hits":{"total":{"value":$n},"hits":[]},"aggregations":{"obj.a":{"buckets":[""",
          ",",
          "]}}}"
        )
    )

  private def shapes(n: Int): Seq[Shape] = Seq(
    Shape(
      "s1 id, n, name, d, flag",
      hitsPage(n)(i =>
        s"""{"id":"d$i","n":$i,"name":"name-$i","d":"2024-01-${f"${i % 28 + 1}%02d"}",""" +
        s""""flag":${i % 2 == 0}}"""
      ),
      ListMap.empty,
      Seq("id", "n", "name", "d", "flag")
    ),
    Shape(
      "s2 obj.a AS k, n",
      hitsPage(n)(i => s"""{"obj":{"a":$i},"n":$i}"""),
      ListMap("obj.a" -> "k"),
      Seq("k", "n")
    ),
    Shape("s3 GROUP BY obj.a (buckets)", bucketsPage(n), ListMap.empty, Seq("obj.a", "c")),
    Shape(
      "S2 id, obj.a (info only)",
      hitsPage(n)(i => s"""{"id":"d$i","obj":{"a":$i}}"""),
      ListMap.empty,
      Seq("id", "obj.a")
    )
  )

  private def block(s: Shape): Long = {
    var sink = 0
    val start = System.nanoTime()
    var r = 0
    while (r < RepsPerBlock) {
      sink += parseResponseTree(s.page, s.fieldAliases, s.aggregations, s.fields).get.size
      r += 1
    }
    val elapsed = System.nanoTime() - start
    if (sink == Int.MinValue) println("")
    elapsed / RepsPerBlock
  }

  private def median(xs: Seq[Long]): Long = {
    val sorted = xs.sorted
    sorted(sorted.length / 2)
  }

  def main(args: Array[String]): Unit = {
    val blocks = math.max(2, args.lift(0).map(_.toInt).getOrElse(DefaultBlocks))
    val hits = args.lift(1).map(_.toInt).getOrElse(DefaultHits)
    println(s"NestedPathCostProbe — $blocks blocks x $RepsPerBlock pages of $hits hits")
    println(
      s"JVM ${System.getProperty("java.version")} · Scala ${util.Properties.versionNumberString}"
    )
    val all = shapes(hits)
    // block 1 of every shape is COLD: no warm-up before it
    val timings: Map[String, Array[Long]] = all.map(s => s.label -> Array.ofDim[Long](blocks)).toMap
    var b = 0
    while (b < blocks) {
      // alternate the order so drift cannot favour one shape
      val ordered = if (b % 2 == 0) all else all.reverse
      ordered.foreach(s => timings(s.label)(b) = block(s))
      b += 1
    }
    println()
    println(f"${"shape"}%-32s ${"cold µs"}%10s ${"warm median µs"}%15s ${"warm best µs"}%13s")
    all.foreach { s =>
      val t = timings(s.label)
      val warm = t.drop(1).toSeq
      println(
        f"${s.label}%-32s ${t(0) / 1000.0}%10.1f ${median(warm) / 1000.0}%15.1f ${warm.min / 1000.0}%13.1f"
      )
    }
  }
}
