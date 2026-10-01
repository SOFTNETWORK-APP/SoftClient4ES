package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.function.cond.Case
import app.softnetwork.elastic.sql.operator.{AND, OR}
import app.softnetwork.elastic.sql.query._

import scala.collection.mutable

/** The derived population of conditions that combine AND and OR, and its oracles.
  *
  * Every statement is GENERATED from a structure -- an operator sequence, a laminar family of
  * parenthesised intervals, the negated leaves, the single-leaf parentheses and the leaf kinds --
  * and its meaning is computed from that structure ONLY, never from the engine: [[ansi]] is SQL's
  * reading (`NOT` > `AND` > `OR`, left-associative, parentheses respected) and [[leftFold]] is kept
  * to report what moved. A tree built by the engine is compared by its FULL truth table: a leaf is
  * one boolean variable, true iff its bit is set.
  */
object ConditionPopulation {

  // ------------------------------------------------------------ formulas over leaves 1..n

  sealed trait F
  final case class L(i: Int) extends F
  final case class N(f: F) extends F
  final case class A(l: F, r: F) extends F
  final case class O(l: F, r: F) extends F

  def eval(f: F, bits: Int): Boolean = f match {
    case L(i)    => ((bits >> (i - 1)) & 1) == 1
    case N(g)    => !eval(g, bits)
    case A(l, r) => eval(l, bits) && eval(r, bits)
    case O(l, r) => eval(l, bits) || eval(r, bits)
  }

  def table(f: F, n: Int): Vector[Boolean] = (0 until (1 << n)).map(eval(f, _)).toVector

  // ------------------------------------------------------------ the WRITTEN form

  sealed trait W
  final case class WLeaf(i: Int, neg: Boolean) extends W
  final case class WGroup(items: List[W], ands: List[Boolean], paren: Boolean) extends W

  /** SQL's reading: split at every OR into runs joined by AND, fold each run, fold the runs. */
  def ansi(w: W): F = w match {
    case WLeaf(i, neg) => if (neg) N(L(i)) else L(i)
    case WGroup(items, ands, _) =>
      val fs = items.map(ansi)
      val runs = mutable.ListBuffer[List[F]]()
      var current = List(fs.head)
      fs.tail.zip(ands).foreach { case (f, isAnd) =>
        if (isAnd) current = current :+ f
        else { runs += current; current = List(f) }
      }
      runs += current
      runs.toList.map(_.reduceLeft((x, y) => A(x, y): F)).reduceLeft((x, y) => O(x, y): F)
  }

  def leftFold(w: W): F = w match {
    case WLeaf(i, neg) => if (neg) N(L(i)) else L(i)
    case WGroup(items, ands, _) =>
      val fs = items.map(leftFold)
      fs.tail.zip(ands).foldLeft(fs.head) { case (acc, (f, isAnd)) =>
        if (isAnd) A(acc, f) else O(acc, f)
      }
  }

  def render(w: W, leaf: (Int, Boolean) => String): String = w match {
    case WLeaf(i, neg) => leaf(i, neg)
    case WGroup(items, ands, paren) =>
      val sb = new StringBuilder(render(items.head, leaf))
      items.tail.zip(ands).foreach { case (item, isAnd) =>
        sb.append(if (isAnd) " AND " else " OR ").append(render(item, leaf))
      }
      if (paren) s"(${sb.toString})" else sb.toString
  }

  /** Leaves `lo..hi`; `ands(k - 1)` joins leaf k and leaf k + 1; `fam` holds the parenthesised
    * intervals (laminar), `singles` the leaves written in their own parentheses.
    */
  def build(
    lo: Int,
    hi: Int,
    ands: Vector[Boolean],
    fam: Set[(Int, Int)],
    singles: Set[Int],
    negs: Set[Int],
    paren: Boolean
  ): WGroup = {
    val inside = fam.filter { case (a, b) => a >= lo && b <= hi && (a, b) != ((lo, hi)) }
    val maximal = inside.filter { case (a, b) =>
      !inside.exists { case (c, d) => (c, d) != ((a, b)) && c <= a && b <= d }
    }
    val items = mutable.ListBuffer[W]()
    val itemAnds = mutable.ListBuffer[Boolean]()
    var p = lo
    while (p <= hi) {
      val (item, end) = maximal.find(_._1 == p) match {
        case Some((a, b)) => (build(a, b, ands, fam, singles, negs, paren = true), b)
        case None =>
          val leaf = WLeaf(p, negs.contains(p))
          (if (singles.contains(p)) WGroup(List(leaf), Nil, paren = true) else leaf, p)
      }
      if (items.nonEmpty) itemAnds += ands(p - 2)
      items += item
      p = end + 1
    }
    WGroup(items.toList, itemAnds.toList, paren)
  }

  def intervals(n: Int): List[(Int, Int)] =
    for { a <- (1 to n).toList; b <- (a + 1 to n).toList } yield (a, b)

  def laminar(s: List[(Int, Int)]): Boolean = s.forall { case (a, b) =>
    s.forall { case (c, d) =>
      (a, b) == ((c, d)) || b < c || d < a || (a <= c && d <= b) || (c <= a && b <= d)
    }
  }

  /** Every laminar family of parenthesised intervals of 2+ leaves, the whole clause included. */
  def families(n: Int): List[Set[(Int, Int)]] = {
    val iv = intervals(n)
    (0 until (1 << iv.size)).toList
      .map(m => iv.zipWithIndex.collect { case (x, k) if ((m >> k) & 1) == 1 => x })
      .filter(laminar)
      .map(_.toSet)
  }

  def opSeqs(n: Int): List[Vector[Boolean]] =
    (0 until (1 << (n - 1))).toList.map(m => (0 until n - 1).map(k => ((m >> k) & 1) == 1).toVector)

  def subsets(n: Int): List[Set[Int]] =
    (0 until (1 << n)).toList.map(m => (1 to n).filter(i => ((m >> (i - 1)) & 1) == 1).toSet)

  // ------------------------------------------------------------ clauses

  sealed abstract class Clause(val name: String)
  case object WhereC extends Clause("WHERE")
  case object HavingKeysC extends Clause("HAVING on several keys")
  case object HavingKeyC extends Clause("HAVING on one key")
  case object HavingAggC extends Clause("HAVING on aggregates")
  case object OnC extends Clause("JOIN ON")
  case object CaseC extends Clause("CASE WHEN")

  val Clauses: List[Clause] = List(WhereC, HavingKeysC, HavingKeyC, HavingAggC, OnC, CaseC)

  /** Leaf kinds of WHERE / CASE WHEN: 0 `=`, 1 LIKE, 2 IN, 3 BETWEEN, 4 IS NULL, 5 `>`; each with
    * its own spelling of NOT.
    */
  def leafText(clause: Clause, kindOf: Int => Int)(i: Int, neg: Boolean): String = clause match {
    case WhereC | CaseC =>
      kindOf(i) match {
        case 0 => if (neg) s"NOT c$i = 1" else s"c$i = 1"
        case 1 => if (neg) s"c$i NOT LIKE 'x%'" else s"c$i LIKE 'x%'"
        case 2 => if (neg) s"c$i NOT IN (1, 2)" else s"c$i IN (1, 2)"
        case 3 => if (neg) s"c$i NOT BETWEEN 1 AND 2" else s"c$i BETWEEN 1 AND 2"
        case 4 => if (neg) s"c$i IS NOT NULL" else s"c$i IS NULL"
        case _ => if (neg) s"NOT c$i > 1" else s"c$i > 1"
      }
    case HavingKeysC => if (neg) s"NOT c$i = 'v$i'" else s"c$i = 'v$i'"
    case HavingKeyC  => if (neg) s"NOT k = 'v$i'" else s"k = 'v$i'"
    case HavingAggC  => if (neg) s"NOT MAX(c$i) > 1" else s"MAX(c$i) > 1"
    case OnC         => if (neg) s"NOT t.c$i = u.c$i" else s"t.c$i = u.c$i"
  }

  def statement(clause: Clause, n: Int, cond: String): String = clause match {
    case WhereC => s"SELECT id FROM t WHERE $cond"
    case HavingKeysC =>
      val keys = (1 to n).map(i => s"c$i").mkString(", ")
      s"SELECT $keys, COUNT(*) AS cnt FROM t GROUP BY $keys HAVING $cond"
    case HavingKeyC => s"SELECT k, COUNT(*) AS cnt FROM t GROUP BY k HAVING $cond"
    case HavingAggC => s"SELECT g, COUNT(*) AS cnt FROM t GROUP BY g HAVING $cond"
    case OnC        => s"SELECT t.id FROM t JOIN u ON $cond"
    case CaseC      => s"SELECT CASE WHEN $cond THEN 1 ELSE 0 END AS x FROM t"
  }

  def criteriaOf(clause: Clause, st: Statement): Option[Criteria] = st match {
    case s: SingleSearch =>
      clause match {
        case WhereC                                => s.where.flatMap(_.criteria)
        case HavingKeysC | HavingKeyC | HavingAggC => s.having.flatMap(_.criteria)
        case OnC =>
          s.from.joins.collectFirst { case j: StandardJoin => j.on }.flatten.map(_.criteria)
        case CaseC =>
          s.select.fields.headOption
            .flatMap(_.identifier.functions.collectFirst { case c: Case => c })
            .flatMap(_.conditions.headOption)
            .map(_._1)
            .collect { case c: Criteria => c }
      }
    case _ => None
  }

  // ------------------------------------------------------------ one generated statement

  final case class Gen(clause: Clause, cls: String, n: Int, w: W, kindOf: Int => Int) {
    lazy val cond: String = render(w, leafText(clause, kindOf))
    lazy val sql: String = statement(clause, n, cond)
    lazy val ansiTable: Vector[Boolean] = table(ansi(w), n)
    lazy val leftTable: Vector[Boolean] = table(leftFold(w), n)

    /** Some parenthesis level holds both AND and OR, unparenthesised. */
    lazy val mixedLevel: Boolean = {
      def any(x: W): Boolean = x match {
        case WGroup(items, ands, _) =>
          (ands.contains(true) && ands.contains(false)) || items.exists(any)
        case _ => false
      }
      any(w)
    }

    /** One operator in the whole condition. */
    lazy val oneOperator: Boolean = {
      def ops(x: W): List[Boolean] = x match {
        case WGroup(items, ands, _) => ands ++ items.flatMap(ops)
        case _                      => Nil
      }
      ops(w).distinct.size <= 1
    }
  }

  /** P1. Counts are asserted from this generator by the specs, never typed by hand. */
  def population(): List[Gen] = {
    val out = mutable.ListBuffer[Gen]()
    val k0: Int => Int = _ => 0
    // P: every parenthesisation x every operator sequence, NOT-free, `=` leaves
    for {
      clause <- Clauses
      n      <- if (clause == WhereC) List(3, 4, 5) else List(3, 4)
      fam    <- families(n)
      ands   <- opSeqs(n)
    } out += Gen(
      clause,
      "P",
      n,
      build(1, n, ands, fam, Set.empty, Set.empty, fam.contains((1, n))),
      k0
    )
    // N: NOT on any non-empty leaf subset; 3 leaves every parenthesisation, 4 leaves flat
    for {
      clause <- Clauses
      n      <- List(3, 4)
      fam    <- if (n == 3) families(3) else List(Set.empty[(Int, Int)])
      ands   <- opSeqs(n)
      negs   <- subsets(n) if negs.nonEmpty
    } out += Gen(clause, "N", n, build(1, n, ands, fam, Set.empty, negs, fam.contains((1, n))), k0)
    // S: single-leaf parentheses, flat otherwise
    for {
      clause  <- List(WhereC, HavingAggC, CaseC)
      n       <- List(3, 4)
      singles <- subsets(n) if singles.nonEmpty
      ands    <- opSeqs(n)
    } out += Gen(
      clause,
      "S",
      n,
      build(1, n, ands, Set.empty, singles, Set.empty, paren = false),
      k0
    )
    // K: the six leaf kinds, rotated, every parenthesisation, with and without the kind's NOT
    for {
      n      <- List(3, 4)
      fam    <- families(n)
      ands   <- opSeqs(n)
      rot    <- 0 until 6
      negAll <- List(false, true)
    } {
      val negs = if (negAll) Set(2) else Set.empty[Int]
      out += Gen(
        WhereC,
        "K",
        n,
        build(1, n, ands, fam, Set.empty, negs, fam.contains((1, n))),
        i => (i + rot) % 6
      )
    }
    out.toList
  }

  // ------------------------------------------------------------ the engine's tree as a formula

  private val ColumnIndex = """\bc(\d+)\b""".r
  private val ValueIndex = """'v(\d+)'""".r

  /** A leaf's index: its column (`c3`), or -- HAVING on one key, where every leaf names `k` -- its
    * value (`'v3'`).
    */
  def leafIndex(clause: Clause, e: Expression): Option[Int] =
    if (clause == HavingKeyC) ValueIndex.findFirstMatchIn(e.sql).map(_.group(1).toInt)
    else ColumnIndex.findFirstMatchIn(e.identifier.sql).map(_.group(1).toInt)

  /** The engine's tree as a formula. `Predicate.not` negates the RIGHT operand. */
  def formulaOf(c: Criteria, clause: Clause): Either[String, F] = c match {
    case p: Predicate =>
      for {
        l <- formulaOf(p.leftCriteria, clause)
        r <- formulaOf(p.rightCriteria, clause)
      } yield {
        val right = if (p.not.isDefined) N(r) else r
        p.operator match {
          case AND => A(l, right)
          case OR  => O(l, right)
        }
      }
    case r: ElasticRelation => formulaOf(r.criteria, clause)
    case e: Expression =>
      leafIndex(clause, e) match {
        case Some(i) =>
          val negated = e match {
            case _: IsNotNullExpr | _: IsNotNullCriteria => true
            case _: IsNullExpr | _: IsNullCriteria       => false
            case other                                   => other.maybeNot.isDefined
          }
          Right(if (negated) N(L(i)) else L(i))
        case None => Left(s"leaf not indexable: ${e.sql}")
      }
    case other => Left(s"unhandled ${other.getClass.getSimpleName}: ${other.sql}")
  }

  // ------------------------------------------------------------ Painless precedence

  /** Evaluates a `bucket_selector` script of `MAX(ci) > 0` leaves (a negated leaf renders `<= 0`)
    * with Painless's own precedence: `!` > `&&` > `||`, parentheses respected.
    */
  def scriptTruth(script: String, bits: Int): Boolean = {
    val leaf = """\((?:[^()]|\([^()]*\))*?params\.[A-Za-z_]*?c(\d+)(?:[^()]|\([^()]*\))*?\)""".r
    var s = script
    var more = true
    while (more) {
      leaf.findFirstMatchIn(s) match {
        case Some(m) =>
          val bit = ((bits >> (m.group(1).toInt - 1)) & 1) == 1
          val truth = if (m.matched.contains("<= 0")) !bit else bit
          s = s.substring(0, m.start) + (if (truth) "T" else "F") + s.substring(m.end)
        case None => more = false
      }
    }
    val tokens = mutable.Queue[String]()
    var k = 0
    while (k < s.length) {
      s.charAt(k) match {
        case ' '                          => k += 1
        case 'T' | 'F' | '(' | ')' | '!'  => tokens += s.charAt(k).toString; k += 1
        case '&' if s.startsWith("&&", k) => tokens += "&&"; k += 2
        case '|' if s.startsWith("||", k) => tokens += "||"; k += 2
        case other => throw new IllegalArgumentException(s"'$other' in reduced script $s ($script)")
      }
    }
    def or(): Boolean = {
      var v = and()
      while (tokens.headOption.contains("||")) { tokens.dequeue(); val r = and(); v = v || r }
      v
    }
    def and(): Boolean = {
      var v = not()
      while (tokens.headOption.contains("&&")) { tokens.dequeue(); val r = not(); v = v && r }
      v
    }
    def not(): Boolean =
      if (tokens.headOption.contains("!")) { tokens.dequeue(); !not() }
      else atom()
    def atom(): Boolean = tokens.dequeue() match {
      case "T" => true
      case "F" => false
      case "(" =>
        val v = or()
        require(tokens.dequeue() == ")", s"unbalanced: $s")
        v
      case t => throw new IllegalArgumentException(s"token $t in $s")
    }
    val result = or()
    require(tokens.isEmpty, s"trailing tokens in $s")
    result
  }
}
