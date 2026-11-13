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

import app.softnetwork.elastic.client.{NopeClientApi, ResultSchema}
import app.softnetwork.elastic.client.result._
import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query.Statement
import app.softnetwork.elastic.sql.schema.Schema
import org.slf4j.{Logger, LoggerFactory}

import scala.util.Try

/** SCHEMA-15 (AC7) -- what `describePerTable` costs on a JOIN body, against what `describe` costs
  * on its two single-table halves, and what single-table `describe` costs (a path the story leaves
  * unchanged: equal within noise between a control tree and this one).
  *
  * Each sample parses the statement OUTSIDE the timed region (a fresh AST, as a request has: the
  * per-node type memos of a re-used AST would answer from the previous sample), then times the one
  * call. The schemas are seeded in the client's cache: the timed work is the typing, never a
  * mapping read. Medians in microseconds, after a warm-up.
  *
  * `describePerTable` is reached reflectively (a `private[elastic]` member is public in bytecode),
  * so the SAME file compiles in a control tree, where that row reads "absent".
  *
  * ==No assertion==
  * A `main`, never a test with a ceiling (#269/#270). Compare two trees by running it in each,
  * INTERLEAVED, in one session.
  *
  * {{{
  * sbt "core/Test/runMain app.softnetwork.elastic.client.perf.PerTableDescribeCostProbe"
  * sbt "core/Test/runMain app.softnetwork.elastic.client.perf.PerTableDescribeCostProbe 300 1000"
  * }}}
  */
object PerTableDescribeCostProbe {

  private val DefaultWarmup = 300
  private val DefaultRuns = 1000

  private val testLogger: Logger = LoggerFactory.getLogger(getClass)

  private def schemaOf(index: String, properties: String, meta: String = "{}"): Schema =
    Index(
      index,
      s"""{"$index": {"aliases": {}, "mappings": {"_meta": {"columns": $meta},
         | "properties": $properties}, "settings": {"index": {"number_of_shards": "1"}}}}""".stripMargin
    ).schema

  private val ptA: Schema = schemaOf(
    "pt_a",
    """{"aid": {"type": "keyword"}, "bid": {"type": "keyword"}, "k": {"type": "keyword"},
      | "t": {"type": "text"}, "n": {"type": "integer"}, "big_a": {"type": "long"},
      | "x": {"type": "double"}, "r": {"type": "float"}, "ts": {"type": "date"},
      | "dc": {"type": "date", "format": "yyyy/MM/dd"},
      | "de": {"type": "date", "format": "epoch_second"}, "b": {"type": "boolean"},
      | "obj": {"properties": {"a": {"type": "integer"}}}, "tags": {"type": "integer"}}""".stripMargin,
    """{"de": {"data_type": "DATE"}, "tags": {"data_type": "ARRAY<INT>"}}"""
  )

  private val ptB: Schema = schemaOf(
    "pt_b",
    """{"bid": {"type": "keyword"}, "label": {"type": "keyword"}, "name": {"type": "text"},
      | "m": {"type": "integer"}, "big": {"type": "long"}, "d2": {"type": "date"},
      | "addr": {"properties": {"zip": {"type": "keyword"}}}, "k": {"type": "keyword"},
      | "n": {"type": "long"}}""".stripMargin
  )

  private class ProbeClient extends NopeClientApi {
    override protected def logger: Logger = testLogger
    override private[client] def executeVersion(): ElasticResult[String] =
      ElasticResult.success("8.18.3")
  }

  /** B9 (every B1 type of `pt_a`) widened by two columns of the joined table, and its halves. */
  private val joinBody =
    "SELECT a.aid, a.t, a.n, a.big_a, a.x, a.de, a.ts, a.b, b.label, b.m " +
    "FROM pt_a a JOIN pt_b b ON a.bid = b.bid"
  private val mainHalf = "SELECT aid, t, n, big_a, x, de, ts, b FROM pt_a"
  private val joinedHalf = "SELECT label, m FROM pt_b"

  /** Single-table shapes: the path the story leaves unchanged. */
  private val singleTable: List[(String, String)] = List(
    "B9 half (pt_a)" -> mainHalf,
    "joined half"    -> joinedHalf,
    "computed items" -> "SELECT UPPER(k) AS u, n + 1 AS v, CAST(ts AS VARCHAR) AS s FROM pt_a",
    "group by"       -> "SELECT k, COUNT(*) AS c, SUM(n) AS s, MAX(dc) AS ma FROM pt_a GROUP BY k",
    "star"           -> "SELECT * FROM pt_a",
    "union all"      -> "SELECT n AS v FROM pt_a UNION ALL SELECT m AS v FROM pt_b",
    "join (refused, 400)" -> joinBody
  )

  private def parse(sql: String): Statement =
    Parser(sql) match {
      case Right(statement) => statement
      case Left(e)          => throw new IllegalArgumentException(s"[$sql] ${e.msg}")
    }

  private def median(xs: Array[Long]): Double = {
    val sorted = xs.sorted
    val n = sorted.length
    if (n == 0) 0d
    else if (n % 2 == 1) sorted(n / 2).toDouble
    else (sorted(n / 2 - 1) + sorted(n / 2)) / 2.0
  }

  /** The median of `body` over a fresh `setup` per sample, `setup` untimed. */
  private def timeMedian[S](warmup: Int, runs: Int)(setup: => S)(body: S => Any): Double = {
    var i = 0
    while (i < warmup) { body(setup); i += 1 }
    val samples = new Array[Long](runs)
    i = 0
    while (i < runs) {
      val s = setup
      val t0 = System.nanoTime()
      body(s)
      samples(i) = System.nanoTime() - t0
      i += 1
    }
    median(samples) / 1000.0
  }

  def main(args: Array[String]): Unit = {
    val warmup = Try(args(0).toInt).getOrElse(DefaultWarmup)
    val runs = Try(args(1).toInt).getOrElse(DefaultRuns)
    val client = new ProbeClient
    client.updateSchema("pt_a", ptA)
    client.updateSchema("pt_b", ptB)
    val perTable: Option[java.lang.reflect.Method] =
      Try(client.getClass.getMethod("describePerTable", classOf[Statement])).toOption
    def describePerTable(s: Statement): ElasticResult[Option[ResultSchema]] =
      perTable.get.invoke(client, s).asInstanceOf[ElasticResult[Option[ResultSchema]]]

    // global warm-up over every shape
    (singleTable.map(_._2) :+ joinBody).foreach { sql =>
      var i = 0
      while (i < warmup) { client.describe(parse(sql)); i += 1 }
    }

    println(s"PerTableDescribeCostProbe warmup=$warmup runs=$runs (medians, microseconds)")
    println("shape\tdescribe")
    val medians = singleTable.map { case (name, sql) =>
      val m = timeMedian(warmup, runs)(parse(sql))((s: Statement) => client.describe(s))
      println(f"$name\t$m%.1f")
      name -> m
    }.toMap

    perTable match {
      case None => println("describePerTable\tabsent (control tree)")
      case Some(_) =>
        println(s"[answer] ${describePerTable(parse(joinBody))}")
        val joined =
          timeMedian(warmup, runs)(parse(joinBody))((s: Statement) => describePerTable(s))
        val halves = medians("B9 half (pt_a)") + medians("joined half")
        println("join body\tdescribePerTable\tsum of describe over its halves\tratio")
        println(f"B9 + 2 joined columns\t$joined%.1f\t$halves%.1f\t${joined / halves}%.2f")
    }
  }
}
