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
  ConversionContext,
  ElasticConversion,
  NativeContext,
  NopeClientApi
}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.SingleSearch

import scala.collection.immutable.ListMap
import scala.collection.mutable.ListBuffer

/** Row-shaping cost probe for issue #354's `rowNormalizer` -> `rowProjector` generalisation.
  *
  * Row shaping is THE per-row hot path: every row of every query passes through it, and an
  * un-LIMITed extraction can mean millions of them, so a regression here is a regression in
  * end-to-end query time. The parse path is untouched by #354/#355 (no grammar change), which is
  * why this probe measures rows rather than statements.
  *
  * ==The control is in this file, and the two run interleaved==
  * `baseline` is the PRE-#354 `rowNormalizer`, transcribed verbatim from `git show
  * <merge-base>:core/.../ElasticConversion.scala`. `current` is the shipped one. They are measured
  * A/B/A/B in ONE JVM on the SAME rows, because a difference measured against a remembered number
  * from another process is noise, not attribution.
  *
  * Run it with:
  *
  * {{{
  * sbt "core/Test/runMain app.softnetwork.elastic.client.perf.RowCostProbe"
  * sbt "core/Test/runMain app.softnetwork.elastic.client.perf.RowCostProbe 20 200000"
  * }}}
  *
  * Arguments, optional and positional: `blocks` (interleaved A/B pairs, default [[DefaultBlocks]])
  * and `rowsPerBlock` (default [[DefaultRowsPerBlock]]).
  *
  * ==This probe NEVER asserts==
  * It prints and exits, like `ParseCostProbe`. A timing assertion in the suite is a CI-flake
  * liability (issues #269/#270). Nothing here is collected by `sbt test`: this is a `main`-bearing
  * object, not a ScalaTest suite.
  */
object RowCostProbe extends ElasticConversion {

  val DefaultBlocks: Int = 31
  val DefaultRowsPerBlock: Int = 200000

  private implicit val ctx: ConversionContext = NativeContext

  // ── the control: the pre-#354 implementation, verbatim ────────────────────────────────────
  //
  // Its duplicate-requested-name arm delegated to a trait-private helper; it is replaced here by
  // the equivalent public `normalizeRow`, and no measured shape reaches that arm.
  private def baseline(
    requestedFields: Seq[String]
  ): ListMap[String, Any] => ListMap[String, Any] = {
    if (requestedFields.isEmpty) identity
    else {
      val fieldArr: Array[String] = requestedFields.toArray
      val len = fieldArr.length
      val fieldIndex = new java.util.HashMap[String, Integer](len * 2)
      var i = 0
      while (i < len) {
        fieldIndex.putIfAbsent(fieldArr(i), i)
        i += 1
      }
      if (fieldIndex.size() != len) { row => normalizeRow(row, requestedFields) }
      else {
        val nullFillMissing = true
        row => {
          val values = new Array[Any](len)
          val seen = new Array[Boolean](len)
          var extras: ListBuffer[(String, Any)] = null
          var inOrder = true
          var passthrough = false
          var p = 0
          val it = row.iterator
          while (!passthrough && it.hasNext) {
            val entry = it.next()
            if (inOrder && fieldArr(p) == entry._1) {
              values(p) = entry._2
              seen(p) = true
              p += 1
              if (p == len) passthrough = true
            } else {
              inOrder = false
              val idx = fieldIndex.get(entry._1)
              if (idx ne null) {
                values(idx.intValue) = entry._2
                seen(idx.intValue) = true
              } else {
                if (extras eq null) extras = new ListBuffer[(String, Any)]
                extras += entry
              }
            }
          }
          if (passthrough || (inOrder && !nullFillMissing)) row
          else {
            val builder = ListMap.newBuilder[String, Any]
            var j = 0
            while (j < len) {
              if (seen(j)) builder += fieldArr(j) -> values(j)
              else if (nullFillMissing) builder += fieldArr(j) -> null
              j += 1
            }
            if (extras ne null) extras.foreach(builder += _)
            builder.result()
          }
        }
      }
    }
  }

  // ── row shapes, each one a real thing Elasticsearch hands back ────────────────────────────
  private def cols(n: Int): Seq[String] = (1 to n).map(i => s"col$i")

  private def row(names: Seq[String]): ListMap[String, Any] =
    ListMap(names.map(k => k -> (s"$k-value": Any)): _*)

  private case class Shape(
    label: String,
    fields: Seq[String],
    rows: Array[ListMap[String, Any]]
  )

  private def shapes: Seq[Shape] = {
    val f5 = cols(5)
    val f20 = cols(20)
    Seq(
      // the dominant case by far: `SELECT a, b, c, d, e` over a flat index — `_source` order
      // already IS the SELECT order, so the row is returned as the same instance
      Shape("5 cols, already shaped (fast path)", f5, Array(row(f5))),
      Shape("20 cols, already shaped (fast path)", f20, Array(row(f20))),
      // a projection Elasticsearch answers out of order — the full rebuild
      Shape("5 cols, reordered (rebuild)", f5, Array(row(f5.reverse))),
      // the document-id column / an inner-hits leftover trailing the projection
      Shape(
        "5 cols + 2 extras (fast path)",
        f5,
        Array(row(f5) ++ ListMap[String, Any]("_id" -> "42", "_score" -> 1.0))
      ),
      // a column the document does not carry: null-filled, so a rebuild
      Shape("5 cols, one missing (rebuild)", f5, Array(row(f5.drop(1))))
    )
  }

  private def median(xs: Array[Long]): Long = {
    val sorted = xs.sorted
    sorted(sorted.length / 2)
  }

  /** The MIN of many interleaved blocks, not the mean: a microbenchmark's noise is one-sided
    * (scheduling, GC, another core) and the fastest observed block is the one least polluted by it.
    * Both sides get the same treatment, and the median is printed beside it so a disagreement
    * between the two is visible rather than hidden.
    */
  private def best(xs: Array[Long]): Long = xs.min

  private def timeRows(
    f: ListMap[String, Any] => ListMap[String, Any],
    rows: Array[ListMap[String, Any]],
    iterations: Int
  ): Long = {
    var sink = 0
    val n = rows.length
    val start = System.nanoTime()
    var i = 0
    while (i < iterations) {
      sink += f(rows(i % n)).size
      i += 1
    }
    val elapsed = System.nanoTime() - start
    if (sink == Int.MinValue) println("")
    elapsed
  }

  def main(args: Array[String]): Unit = {
    val blocks = args.lift(0).map(_.toInt).getOrElse(DefaultBlocks)
    val rows = args.lift(1).map(_.toInt).getOrElse(DefaultRowsPerBlock)

    println(s"RowCostProbe — $blocks interleaved A/B blocks x $rows rows each")
    println(
      s"JVM ${System.getProperty("java.version")} · Scala ${util.Properties.versionNumberString}"
    )

    // global warm-up: both implementations, every shape, before any timing
    shapes.foreach { s =>
      timeRows(baseline(s.fields), s.rows, 50000)
      timeRows(rowNormalizer(s.fields), s.rows, 50000)
    }

    println()
    println(
      f"${"shape"}%-38s ${"base"}%9s ${"current"}%9s ${"delta"}%8s   (ns/row, best of blocks)"
    )
    shapes.foreach { s =>
      val b = Array.ofDim[Long](blocks)
      val c = Array.ofDim[Long](blocks)
      var k = 0
      while (k < blocks) {
        // INTERLEAVED, same rows, same JVM, alternating order so drift cannot favour one side
        if (k % 2 == 0) {
          b(k) = timeRows(baseline(s.fields), s.rows, rows)
          c(k) = timeRows(rowNormalizer(s.fields), s.rows, rows)
        } else {
          c(k) = timeRows(rowNormalizer(s.fields), s.rows, rows)
          b(k) = timeRows(baseline(s.fields), s.rows, rows)
        }
        k += 1
      }
      val bm = best(b).toDouble / rows
      val cm = best(c).toDouble / rows
      val bmed = median(b).toDouble / rows
      val cmed = median(c).toDouble / rows
      println(
        f"${s.label}%-38s $bm%9.2f $cm%9.2f ${(cm - bm) / bm * 100}%+7.1f%%" +
        f"   (median $bmed%6.2f / $cmed%6.2f)"
      )
    }

    // ── what the FEATURE itself costs: a renaming leg has no pre-#354 equivalent ─────────────
    println()
    println("UNION ALL positional rename (new work — no baseline equivalent):")
    println(f"${"shape"}%-38s ${"no rename"}%9s ${"renaming"}%9s ${"delta"}%8s   (ns/row)")
    val f5 = cols(5)
    val renamed = Seq("a", "b", "c", "d", "e")
    Seq(
      ("5 cols, already shaped", Array(row(f5))),
      ("5 cols, reordered", Array(row(f5.reverse)))
    ).foreach { case (label, rs) =>
      val n = Array.ofDim[Long](blocks)
      val r = Array.ofDim[Long](blocks)
      var k = 0
      while (k < blocks) {
        if (k % 2 == 0) {
          n(k) = timeRows(rowProjector(f5, f5), rs, rows)
          r(k) = timeRows(rowProjector(f5, renamed), rs, rows)
        } else {
          r(k) = timeRows(rowProjector(f5, renamed), rs, rows)
          n(k) = timeRows(rowProjector(f5, f5), rs, rows)
        }
        k += 1
      }
      val nm = best(n).toDouble / rows
      val rm = best(r).toDouble / rows
      println(f"$label%-38s $nm%9.2f $rm%9.2f ${(rm - nm) / nm * 100}%+7.1f%%")
    }

    // ── issue #355's OTHER half: the resolution the per-leg route no longer repeats ─────────
    //
    // `unionAllByLeg` used to re-enter `search(leg)`, which re-ran `resolveWithSchema` on a leg the
    // `MultiSearch` seam had already resolved — so an N-leg `UNION ALL` paid 2N resolutions where
    // N is enough. This is what ONE of them costs on a client whose schema lookup is a miss (the
    // shape every `NopeClientApi`-style client and every un-mapped index takes), so the saving per
    // dropped call is at least this.
    val resolver = new NopeClientApi {
      override protected def logger: org.slf4j.Logger =
        org.slf4j.LoggerFactory.getLogger("RowCostProbe")
    }
    val leg = Parser("SELECT category, amount FROM union_left WHERE amount > 10") match {
      case Right(single: SingleSearch) => single
      case other => throw new IllegalStateException(s"probe statement: $other")
    }
    var w = 0
    while (w < 20000) { resolver.resolveWithSchema(leg); w += 1 }
    val resolveBlocks = Array.ofDim[Long](blocks)
    var rb = 0
    while (rb < blocks) {
      val n = 50000
      val t = System.nanoTime()
      var i = 0
      while (i < n) { resolver.resolveWithSchema(leg); i += 1 }
      resolveBlocks(rb) = (System.nanoTime() - t) / n
      rb += 1
    }
    println()
    println(
      f"per-leg resolveWithSchema, DROPPED once per leg by #355: ${best(resolveBlocks)}%d ns" +
      f" (median ${median(resolveBlocks)}%d ns)"
    )

    // ── the per-STREAM half: what building one normalizer costs ──────────────────────────────
    println()
    println("per-stream construction (paid ONCE per statement/leg, not per row):")
    Seq(5, 20).foreach { width =>
      val f = cols(width)
      val n = 200000
      val b = Array.ofDim[Long](blocks)
      val c = Array.ofDim[Long](blocks)
      var k = 0
      while (k < blocks) {
        var t = System.nanoTime()
        var i = 0
        while (i < n) { baseline(f); i += 1 }
        b(k) = System.nanoTime() - t
        t = System.nanoTime()
        i = 0
        while (i < n) { rowNormalizer(f); i += 1 }
        c(k) = System.nanoTime() - t
        k += 1
      }
      val bm = best(b).toDouble / n
      val cm = best(c).toDouble / n
      println(
        f"  $width%2d columns: baseline $bm%7.1f ns   current $cm%7.1f ns   ${(cm - bm)}%+7.1f ns"
      )
    }
  }
}
