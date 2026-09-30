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

import scala.collection.mutable

/** A fixture on which every boolean reading of a condition selects a DIFFERENT set of documents,
  * and the conditions to run on it.
  *
  * 32 documents `d0` … `d31`: document `dN` carries `ci` = bit `i - 1` of `N` (`c1` … `c5`, 0 or
  * 1), its own group key `g` = its id, its id again as full text in `t` (for MATCH), and `m` = 0.
  * So `ci = 1` holds exactly on the documents whose bit `i - 1` is set, and the documents a
  * condition selects spell its truth table. The expected answer is computed HERE, from the
  * structure of the condition -- never from the engine: SQL's reading, `NOT` before `AND` before
  * `OR`, left-associative, parentheses respected.
  */
object ConditionTruthTable {

  val Documents: Int = 32

  def bit(doc: Int, i: Int): Boolean = ((doc >> (i - 1)) & 1) == 1

  def id(doc: Int): String = s"d$doc"

  /** The bulk documents, `id` as the key. */
  def documents: List[String] = (0 until Documents).toList.map { n =>
    val cs = (1 to 5).map(i => s""""c$i":${if (bit(n, i)) 1 else 0}""").mkString(",")
    s"""{"id":"${id(n)}","g":"${id(n)}","t":"${id(n)}",$cs,"m":0}"""
  }

  val Mapping: String =
    """{
      |  "properties": {
      |    "id": { "type": "keyword" },
      |    "g":  { "type": "keyword" },
      |    "t":  { "type": "text" },
      |    "c1": { "type": "integer" }, "c2": { "type": "integer" }, "c3": { "type": "integer" },
      |    "c4": { "type": "integer" }, "c5": { "type": "integer" },
      |    "m":  { "type": "integer" }
      |  }
      |}""".stripMargin

  /** A written condition: a leaf `i` (negated or not), or a group of items joined by AND (`true`)
    * or OR (`false`), written in parentheses or not.
    */
  sealed trait Cond {
    def render(leaf: (Int, Boolean) => String): String
    def holds(doc: Int): Boolean
  }

  final case class Leaf(i: Int, negated: Boolean = false) extends Cond {
    def render(leaf: (Int, Boolean) => String): String = leaf(i, negated)
    def holds(doc: Int): Boolean = bit(doc, i) != negated
  }

  final case class Group(items: List[Cond], ands: List[Boolean], paren: Boolean) extends Cond {
    def render(leaf: (Int, Boolean) => String): String = {
      val body = items.head.render(leaf) + items.tail
        .zip(ands)
        .map { case (item, isAnd) =>
          (if (isAnd) " AND " else " OR ") + item.render(leaf)
        }
        .mkString
      if (paren) s"($body)" else body
    }

    /** SQL's reading: runs joined by AND, the runs joined by OR. */
    def holds(doc: Int): Boolean = {
      val runs = mutable.ListBuffer[List[Cond]]()
      var current = List(items.head)
      items.tail.zip(ands).foreach { case (item, isAnd) =>
        if (isAnd) current = current :+ item
        else { runs += current; current = List(item) }
      }
      runs += current
      runs.exists(_.forall(_.holds(doc)))
    }
  }

  /** The documents a condition selects, by id. */
  def expected(c: Cond): Set[String] = (0 until Documents).filter(c.holds).map(id).toSet

  def whereLeaf(i: Int, negated: Boolean): String = if (negated) s"NOT c$i = 1" else s"c$i = 1"

  def havingLeaf(i: Int, negated: Boolean): String =
    if (negated) s"NOT MAX(c$i) > 0" else s"MAX(c$i) > 0"

  private def build(
    lo: Int,
    hi: Int,
    ands: Vector[Boolean],
    fam: Set[(Int, Int)],
    negs: Set[Int],
    paren: Boolean
  ): Group = {
    val inside = fam.filter { case (a, b) => a >= lo && b <= hi && (a, b) != ((lo, hi)) }
    val maximal = inside.filter { case (a, b) =>
      !inside.exists { case (c, d) => (c, d) != ((a, b)) && c <= a && b <= d }
    }
    val items = mutable.ListBuffer[Cond]()
    val itemAnds = mutable.ListBuffer[Boolean]()
    var p = lo
    while (p <= hi) {
      val (item, end) = maximal.find(_._1 == p) match {
        case Some((a, b)) => (build(a, b, ands, fam, negs, paren = true), b)
        case None         => (Leaf(p, negs.contains(p)), p)
      }
      if (items.nonEmpty) itemAnds += ands(p - 2)
      items += item
      p = end + 1
    }
    Group(items.toList, itemAnds.toList, paren)
  }

  /** Every laminar family of parenthesised intervals of 2+ conditions, whole clause excluded. */
  private def families(n: Int): List[Set[(Int, Int)]] = {
    val intervals = for {
      a <- (1 to n).toList; b <- (a + 1 to n).toList if (a, b) != ((1, n))
    } yield (a, b)
    def laminar(s: List[(Int, Int)]): Boolean = s.forall { case (a, b) =>
      s.forall { case (c, d) =>
        (a, b) == ((c, d)) || b < c || d < a || (a <= c && d <= b) || (c <= a && b <= d)
      }
    }
    (0 until (1 << intervals.size)).toList
      .map(m => intervals.zipWithIndex.collect { case (x, k) if ((m >> k) & 1) == 1 => x })
      .filter(laminar)
      .map(_.toSet)
  }

  private def operators(n: Int): List[Vector[Boolean]] =
    (0 until (1 << (n - 1))).toList.map(m => (0 until n - 1).map(k => ((m >> k) & 1) == 1).toVector)

  /** 3 and 4 conditions: every parenthesisation x every operator sequence (12 + 88). */
  def parenthesised: List[Cond] =
    for { n <- List(3, 4); fam <- families(n); ands <- operators(n) } yield build(
      1,
      n,
      ands,
      fam,
      Set.empty,
      paren = false
    )

  /** 3 conditions, one of them negated, flat or with one group (36). */
  def negated: List[Cond] =
    for { fam <- families(3); ands <- operators(3); neg <- List(1, 2, 3) } yield build(
      1,
      3,
      ands,
      fam,
      Set(neg),
      paren = false
    )
}
