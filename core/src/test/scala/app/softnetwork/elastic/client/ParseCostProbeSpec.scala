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

import org.json4s.jackson.JsonMethods.parse
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** #238 — cost of the ONE parse of the request body that `ScrollApi.scrollWithMetrics` performs per
  * stream and shares between the strategy decision (`aggs` / `aggregations`) and the slice decision
  * (`sort`). It replaced two separate parses of the same string, so the net change was `-1` parse
  * per extraction; this spec records what that single parse costs and guards the order of
  * magnitude.
  *
  * Measured 2026-08-20 (Apple silicon, JDK 17, json4s-jackson): p50 0.003 ms at 76 B, 0.005 ms at
  * 350 B, 0.028 ms at 3.2 KB, 0.067 ms at 32 KB; p99 ≤ 0.15 ms everywhere — versus a budget of 1 ms
  * and an extraction that then runs for seconds.
  *
  * The assertion is on the MEDIAN, not a tail: a p99/max on a shared CI box measures GC pauses, not
  * parsing (a 9 ms `max` was observed on the 3.2 KB body while the 10× larger body maxed at 0.12
  * ms). The ceiling catches a change that makes body inspection categorically expensive, never a
  * slow machine.
  *
  * Measured 2026-09-02 (loaded shared GitHub runners, SoftClient4ES#269): p50 1.35-1.42 ms at 32 KB
  * — >20× the Apple-silicon median, so the original 1 ms ceiling (~15× that local median) flunked
  * runs whose diff never touched the parse. The ceiling is therefore calibrated against the slowest
  * machine that runs this suite: ~7× the worst CI-observed median, still ~150× the local one — a
  * categorical regression (a parser swap, an accidental re-parse loop) blows past it on any
  * machine; a slow runner does not.
  */
class ParseCostProbeSpec extends AnyFlatSpec with Matchers {

  private val Warmup = 500
  private val Runs = 500

  /** ~7× the worst CI-observed median (1.42 ms on a shared GitHub runner, #269), in nanoseconds. */
  private val MedianCeilingNanos = 10000000L // 10 ms

  private def sourceFields(n: Int): String =
    (1 to n).map(i => s""""field_$i"""").mkString(",")

  private def filters(n: Int): String =
    (1 to n).map(i => s"""{"term":{"field_$i":{"value":"v$i"}}}""").mkString(",")

  private val small =
    """{"query":{"match_all":{}},"_source":{"includes":["id","value"]},"size":1000}"""

  private val typical =
    s"""{"query":{"bool":{"filter":[${filters(3)}],"must":[{"range":{"ts":{"gte":"2026-01-01"}}}]}},
       |"_source":{"includes":[${sourceFields(
      10
    )}]},"size":1000,"track_total_hits":false}""".stripMargin

  private val large =
    s"""{"query":{"bool":{"filter":[${filters(50)}]}},
       |"_source":{"includes":[${sourceFields(100)}]},
       |"script_fields":{"s1":{"script":{"lang":"painless","source":"def x = doc['a'].value; return x != null ? x * 2 : null"}}},
       |"size":5000,"track_total_hits":false}""".stripMargin

  private val pathological =
    s"""{"query":{"bool":{"filter":[${filters(500)}]}},
       |"_source":{"includes":[${sourceFields(1000)}]},"size":10000}""".stripMargin

  /** Median parse time in nanoseconds, reported on stdout with the tail for context. */
  private def medianParseNanos(label: String, body: String): Long = {
    var w = 0
    while (w < Warmup) { parse(body); w += 1 }
    val timings = Array.fill(Runs)(0L)
    var i = 0
    while (i < Runs) {
      val t0 = System.nanoTime()
      parse(body)
      timings(i) = System.nanoTime() - t0
      i += 1
    }
    val sorted = timings.sorted
    val p50 = sorted(Runs / 2)
    def ms(n: Long): String = f"${n / 1000000.0}%.4f"
    info(
      s"$label: ${body.length} bytes — p50 ${ms(p50)} ms, p95 ${ms(sorted((Runs * 95) / 100))} ms, " +
      s"max ${ms(sorted(Runs - 1))} ms"
    )
    p50
  }

  "the per-stream request-body parse" should "stay below the median budget on every realistic body" in {
    Seq(
      "small"        -> small,
      "typical"      -> typical,
      "large"        -> large,
      "pathological" -> pathological
    ).foreach { case (label, body) =>
      val p50 = medianParseNanos(label, body)
      withClue(s"median parse of the $label body (${body.length} bytes) — ") {
        p50 should be < MedianCeilingNanos
      }
    }
  }
}
