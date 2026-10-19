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

package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.bridge._
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.query._
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.time.ZonedDateTime
import scala.jdk.CollectionConverters._

/** Story IDENT-4 -- what Elasticsearch is SENT for a qualified GROUP BY key filter.
  *
  * RENDER level, at both resolutions: a single-table statement is resolved once at an index-pattern
  * venue (and in a materialized view) and twice at a concrete-index one; a JOIN only by extensions
  * and CTAS. The RUN-level truth is `GroupByCompletenessSpec`'s IDENT-4 rows.
  */
class QualifiedGroupKeyEmissionSpec extends AnyFlatSpec with Matchers {

  implicit def timestamp: Long = ZonedDateTime.parse("2025-12-31T00:00:00Z").toInstant.toEpochMilli

  private val mapper = new ObjectMapper()

  private val orders: SchemaTable = SchemaTable(
    "orders",
    columns = List(
      Column("id", SQLTypes.Keyword),
      Column("did", SQLTypes.Keyword),
      Column("category", SQLTypes.Keyword),
      Column("amount", SQLTypes.Int),
      Column("d", SQLTypes.Date)
    )
  )

  private def accepted(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"[$sql] expected an ACCEPTED SingleSearch, got $other")
    }

  /** The COLLECTED form of [[accepted]]: a refusal is a wrong CELL, never an abort -- these tests
    * read as a per-cell map.
    */
  private def parsed(sql: String): Either[String, SingleSearch] =
    Parser(sql) match {
      case Right(s: SingleSearch) => Right(s)
      case Right(other)           => Left(s"not a SingleSearch: $other")
      case Left(err)              => Left(err.msg)
    }

  private def query(s: SingleSearch): String = {
    val request: ElasticSearchRequest = s
    request.query
  }

  private def walk(aggs: JsonNode, prefix: String): Seq[(String, JsonNode)] =
    Option(aggs).toSeq.flatMap(_.fields().asScala.toSeq).flatMap { e =>
      val path = if (prefix.isEmpty) e.getKey else s"$prefix>${e.getKey}"
      (path -> e.getValue) +: walk(e.getValue.get("aggs"), path)
    }

  /** Every aggregation, keyed by its PATH (a self-join's two levels share a name). */
  private def aggregations(json: String): Seq[(String, JsonNode)] =
    walk(mapper.readTree(json).get("aggs"), "")

  /** Every `terms` level's include, by path. */
  private def includes(json: String): Seq[(String, Option[String])] =
    aggregations(json).collect {
      case (path, body) if body.has("terms") =>
        path -> Option(body.get("terms").get("include")).map(_.toString)
    }

  private val spellings = Seq(
    "category"   -> "category",
    "category"   -> "a.category",
    "a.category" -> "category",
    "a.category" -> "a.category"
  )

  /** How this bridge writes a ONE-value include (the es6 copy: `["x","x"]`, IDENT-3's wire rule).
    */
  private val oneValueInclude = """["x"]"""

  private def keyed(groupBy: String, having: String): String =
    s"SELECT COUNT(*) AS c, MAX(amount) AS mx FROM orders a GROUP BY $groupBy HAVING $having = 'x'"

  "IDENT-4: a key filter" should "reach Elasticsearch byte-identical to the bare spelling's, at both resolutions" in {
    val control = accepted(keyed("category", "category"))
    val r1Bare = query(control)
    val r2Bare = query(control.update(Some(orders)))
    // non-vacuity: the control carries the filter AND the metric on its level
    includes(r1Bare) shouldBe Seq("category" -> Some(oneValueInclude))
    aggregations(r1Bare).map(_._1) should contain("category>mx")
    val wrong = spellings.flatMap { case (k, h) =>
      parsed(keyed(k, h)) match {
        case Left(reason) => Seq(s"[$k / $h] refused: $reason")
        case Right(s) =>
          Seq(
            Some(query(s)).filter(_ != r1Bare).map(q => s"[$k / $h] R1:\n  $q\n  bare $r1Bare"),
            Some(query(s.update(Some(orders))))
              .filter(_ != r2Bare)
              .map(q => s"[$k / $h] R2:\n  $q\n  bare $r2Bare")
          ).flatten
      }
    }
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
  }

  "a self-join's key filter" should "sit on the ONE level its correlation names, at both resolutions" in {
    val sql = (alias: String) =>
      "SELECT COUNT(*) AS c, MAX(o.amount) AS mx FROM orders o JOIN orders p ON o.did = p.id " +
      s"GROUP BY o.category, p.category HAVING $alias.category = 'x'"
    val wrong = for {
      (alias, filtered) <- Seq("o" -> "category", "p" -> "category>category")
      (label, statement) <- Seq(
        "R1" -> parsed(sql(alias)),
        "R2" -> parsed(sql(alias)).map(_.updateAll(Map("orders" -> orders)))
      )
      problem <- statement match {
        case Left(reason) => Seq(s"[$alias.category, $label] refused: $reason")
        case Right(s) =>
          val actual = includes(query(s))
          val expected = Seq("category", "category>category").map(p =>
            p -> (if (p == filtered) Some(oneValueInclude) else None)
          )
          if (actual != expected) Seq(s"[$alias.category, $label] $actual, expected $expected")
          else Nil
      }
    } yield problem
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
  }

  "GROUP BY <select alias> over a qualified column" should
  "emit byte-identically to its unqualified twin, at both resolutions" in {
    // The twin at BOTH resolutions: at one resolution the qualified body used to lack the metric
    // sub-aggregation (a qualified key's metrics staying on its level, through the alias-substituted copy). The metric makes it observable.
    val wrong = Seq(
      "SELECT o.category AS cat, COUNT(*) AS c FROM orders o GROUP BY cat HAVING cat = 'x'" ->
      "SELECT category AS cat, COUNT(*) AS c FROM orders GROUP BY cat HAVING cat = 'x'",
      "SELECT o.category AS cat, COUNT(*) AS c, MAX(amount) AS mx FROM orders o GROUP BY cat HAVING cat = 'x'" ->
      "SELECT category AS cat, COUNT(*) AS c, MAX(amount) AS mx FROM orders GROUP BY cat HAVING cat = 'x'"
    ).flatMap { case (q, b) =>
      val (qualified, twin) = (accepted(q), accepted(b))
      Seq(
        Some(query(qualified))
          .filter(_ != query(twin))
          .map(x => s"[$q] R1:\n  $x\n  twin ${query(twin)}"),
        Some(query(qualified.update(Some(orders))))
          .filter(_ != query(twin.update(Some(orders))))
          .map(x => s"[$q] R2:\n  $x\n  twin ${query(twin.update(Some(orders)))}")
      ).flatten
    }
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
  }

  "a qualified GROUP BY key with a metric" should
  "keep the metric on its level after ONE resolution -- the body an index pattern sends" in {
    // The metric's path used to be the parsed spelling `a.category`, its level's `category`, and a
    // metric no level claims is dropped under a GROUP BY.
    val wrong = Seq(
      "SELECT a.category, MAX(amount) AS mx FROM orders a GROUP BY a.category",
      "SELECT category, MAX(amount) AS mx FROM orders a GROUP BY a.category",
      "SELECT a.category AS k, MAX(amount) AS mx FROM orders a GROUP BY 1"
    ).flatMap { sql =>
      val paths =
        aggregations(query(accepted(sql))).map(
          _._1
        ) // accepted before too: a qualified key's metrics staying on its level is a RENDER defect
      if (paths.exists(_.endsWith(">mx"))) None
      else Some(s"[$sql] no metric under its level: $paths")
    }
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
  }

  // ---------------------------------------------------------------------------------------------
  // The chain of a qualified column resolving with it, the date-histogram key-filter refusal's parse arm and the WHERE temporal comparison, at R1 on this bridge
  // ---------------------------------------------------------------------------------------------

  /** Every `field` and script text under the aggregations -- what a key reads. */
  private def keyReads(json: String): Seq[String] = {
    def texts(node: JsonNode): Seq[String] =
      if (node == null) Nil
      else if (node.isObject)
        node.fields().asScala.toSeq.flatMap { e =>
          if (Set("field", "source", "inline", "script")(e.getKey) && e.getValue.isTextual)
            Seq(e.getValue.asText)
          else texts(e.getValue)
        }
      else if (node.isArray) node.elements().asScala.toSeq.flatMap(texts)
      else Nil
    texts(mapper.readTree(json).get("aggs"))
  }

  private val dateSchema: SchemaTable =
    SchemaTable(
      "t",
      columns =
        List(Column("d", SQLTypes.Date), Column("e", SQLTypes.Date), Column("s", SQLTypes.Keyword))
    )

  "a function of a qualified column as the GROUP BY key" should
  "read the RESOLVED field after ONE resolution -- every registry function key, projected or not" in {
    // A key script naming the PARSED column (`doc['q.d']`) reads a field no mapping has: wrong or
    // failed for every mapping. Judged over every function key of the registry (story IDENT-1's
    // population), over `FROM t q`, the key projected and not (the GROUPING, not the metrics).
    val rows = for {
      (spelling, key, _) <- QualifiedKeyPopulation.functionKeys
      k = key.replace("t.d", "q.d")
      sql <- Seq(
        s"SELECT COUNT(*) AS c FROM t q GROUP BY $k",
        s"SELECT $k AS k, COUNT(*) AS c FROM t q GROUP BY $k"
      )
      s <- parsed(sql).toOption.toSeq
    } yield (spelling, sql, keyReads(query(s)))
    val stale = rows.filter(r => r._3.exists(read => read.contains("'q.d'") || read == "q.d"))
    val resolved = rows.filter(_._3.exists(_.contains("doc['d']")))
    info(
      s"${rows.size} R1 renders · ${resolved.size} key scripts read doc['d'] · ${stale.size} read the parsed q.d"
    )
    resolved should not be empty // non-vacuity: the stale-argument members render a key script
    withClue(stale.map(r => s"[${r._1}] ${r._2}: ${r._3.mkString(" ;; ")}").mkString("\n")) {
      stale shouldBe empty
    }
  }

  "a function key whose output is temporal without a mapping" should
  "be emitted at R1 as a date histogram -- the premise of the date-histogram key-filter refusal parse arm" in {
    // The population reaching the parse arm: every registry function key (bare `d`, depth 1) whose
    // level is temporal with no mapping. Each must reach Elasticsearch as a date histogram, the
    // level that has no key filter -- else the arm would refuse a filter that could be applied.
    val rows = QualifiedKeyPopulation.functionKeys.flatMap { case (spelling, _, bare) =>
      parsed(s"SELECT COUNT(*) AS c FROM t GROUP BY $bare").toOption.flatMap { s =>
        s.buckets.headOption.filter(_.isDateHistogram).map { _ =>
          val level = Option(mapper.readTree(query(s)).get("aggs"))
            .flatMap(a => Option(a.elements()).filter(_.hasNext).map(_.next()))
          (
            spelling,
            bare,
            level.exists(_.has("date_histogram")),
            parsed(
              s"SELECT COUNT(*) AS c FROM t GROUP BY $bare HAVING $bare = '2024-01-01'"
            )
          )
        }
      }
    }
    info(s"${rows.size} temporal-output keys reach the parse arm")
    rows should not be empty
    val wrong = rows.flatMap { case (spelling, bare, histogram, filtered) =>
      Seq(
        if (histogram) None
        else Some(s"[$spelling] GROUP BY $bare is not emitted as a date histogram"),
        filtered match {
          case Left(reason) if reason.startsWith("HAVING cannot filter a DATE grouping level") =>
            None
          case other => Some(s"[$spelling] GROUP BY $bare HAVING $bare = '2024-01-01': $other")
        }
      ).flatten
    }
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
  }

  "a temporal WHERE comparison of a GROUP BY key column" should
  "compare the two dates as dates, qualified or bare, whether or not the column is grouped" in {
    // A WHERE comparison reads documents, never a bucket key: whether its column is also a GROUP BY
    // key must not decide its Painless. It used to take the numeric fallback -- a raw `>` on two
    // dates, which Painless refuses -- exactly when the column was attached to its key. So each
    // statement emits the WHERE its ungrouped twin emits, and a comparison with a date the statement
    // computes (`DATE_ADD`) -- or, once the mapping is known, with a date column -- is a date one.
    val statements = Seq(
      "SELECT COUNT(*) AS c FROM t q WHERE q.d > DATE_ADD(q.e, INTERVAL 1 DAY) GROUP BY q.d",
      "SELECT COUNT(*) AS c FROM t WHERE d > DATE_ADD(e, INTERVAL 1 DAY) GROUP BY d",
      "SELECT COUNT(*) AS c FROM t q WHERE q.d > q.e GROUP BY q.d",
      "SELECT COUNT(*) AS c FROM t WHERE d > e GROUP BY d"
    )
    val wrong = for {
      sql <- statements
      (label, statement) <- Seq(
        "R1" -> parsed(sql),
        "R2" -> parsed(sql).map(_.update(Some(dateSchema)))
      )
      ungrouped = sql.substring(0, sql.indexOf(" GROUP BY"))
      problem <- (statement, parsed(ungrouped)) match {
        case (Right(s), Right(control)) =>
          val where = Option(mapper.readTree(query(s)).get("query")).map(_.toString).getOrElse("")
          val twin =
            Option(mapper.readTree(query(control)).get("query")).map(_.toString).getOrElse("")
          val dated = sql.contains("DATE_ADD") || label == "R2"
          Seq(
            if (!dated || where.contains("isAfter")) None
            else Some(s"[$label] $sql: no date comparison in $where"),
            // the ungrouped twin, at the same resolution: the same comparison, grouped or not
            if (label != "R1" || where == twin) None
            else Some(s"[$label] $sql: $where, ungrouped $twin")
          ).flatten
        case other => Seq(s"[$label] $sql: $other")
      }
    } yield problem
    withClue(wrong.mkString("\n")) { wrong shouldBe empty }
  }

  // The date-histogram key-filter refusal -- the ONE derivation both bridges and the classifier read, tied in BOTH directions: the
  // levels the date-histogram key-filter refusal refuses and the ones it ACCEPTS (a key filter that reaches a `terms` level).
  "Bucket.isDateHistogram" should "be true exactly for the levels the bridge emits as a date_histogram" in {
    val observations = for {
      sql <- Seq(
        "SELECT COUNT(*) AS c FROM orders GROUP BY DATE_TRUNC(d, MONTH)",
        "SELECT COUNT(*) AS c FROM orders GROUP BY YEAR(d)",
        "SELECT COUNT(*) AS c FROM orders GROUP BY category",
        "SELECT COUNT(*) AS c FROM orders GROUP BY d",
        "SELECT COUNT(*) AS c FROM orders GROUP BY YEAR(d) HAVING YEAR(d) = 2024",
        "SELECT COUNT(*) AS c FROM orders a GROUP BY a.d HAVING a.d = '2024-01-03'"
      )
      s <- Seq(accepted(sql), accepted(sql).update(Some(orders)))
      level = aggregations(query(s)).head._2
    } yield (sql, s.buckets.head.isDateHistogram, level.has("date_histogram"))
    observations.filter(o => o._2 != o._3) shouldBe empty
    observations.map(_._2).distinct.sorted shouldBe Seq(false, true) // both kinds observed
  }
}
