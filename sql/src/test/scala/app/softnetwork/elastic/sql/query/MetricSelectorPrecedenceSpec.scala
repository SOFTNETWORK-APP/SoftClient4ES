package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.parser.ConditionPopulation._
import app.softnetwork.elastic.sql.parser.Parser
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import scala.collection.mutable

/** The `bucket_selector` script of a HAVING over aggregates evaluates the condition's TREE.
  *
  * The script is evaluated here with Painless's own precedence (`!` > `&&` > `||`), which is
  * exactly how Elasticsearch runs it, and compared with SQL's reading of the written condition.
  *
  * 🔴 Why both halves must hold together: a written group used to render its operands in
  * parentheses but not itself, so Painless re-associated it -- and the unparenthesised mixes that
  * came out right were right only because that flattening met a wrong tree. Correcting either half
  * alone breaks the statements the other half was hiding.
  */
class MetricSelectorPrecedenceSpec extends AnyFlatSpec with Matchers {

  private def leaf(i: Int, neg: Boolean): String = if (neg) s"NOT MAX(c$i) > 0" else s"MAX(c$i) > 0"

  private final case class Row(label: String, n: Int, w: W) {
    lazy val sql: String = s"SELECT g, COUNT(*) AS cnt FROM t GROUP BY g HAVING ${render(w, leaf)}"
    lazy val expected: Vector[Boolean] = table(ansi(w), n)
    lazy val mixedLevel: Boolean = {
      def any(x: W): Boolean = x match {
        case WGroup(items, ands, _) =>
          (ands.contains(true) && ands.contains(false)) || items.exists(any)
        case _ => false
      }
      any(w)
    }
    lazy val flat: Boolean = w match {
      case WGroup(items, _, false) => items.forall(_.isInstanceOf[WLeaf])
      case _                       => false
    }
  }

  /** P3: derived. */
  private lazy val rows: List[Row] = {
    val out = mutable.ListBuffer[Row]()
    for { fam <- families(3); ands <- opSeqs(3) } out += Row(
      "3, parenthesised",
      3,
      build(1, 3, ands, fam, Set.empty, Set.empty, fam.contains((1, 3)))
    )
    for { fam <- families(4); ands <- opSeqs(4) } out += Row(
      "4, parenthesised",
      4,
      build(1, 4, ands, fam, Set.empty, Set.empty, fam.contains((1, 4)))
    )
    for { singles <- subsets(3) if singles.nonEmpty; ands <- opSeqs(3) } out += Row(
      "3, single-leaf parentheses",
      3,
      build(1, 3, ands, Set.empty, singles, Set.empty, paren = false)
    )
    for { ands <- opSeqs(3); negs <- subsets(3) if negs.nonEmpty } out += Row(
      "3, NOT",
      3,
      build(1, 3, ands, Set.empty, Set.empty, negs, paren = false)
    )
    out.toList
  }

  // The rendering BEFORE the null-aware read (`metricSelector` reads through it): the read replaces
  // each metric in place and leaves the tree this spec evaluates as it is.
  private def scriptOf(sql: String): String = Parser(sql) match {
    case Right(s: SingleSearch) =>
      MetricSelectorScript
        .selectorScript(s.having.flatMap(_.criteria).getOrElse(fail(s"[$sql] no HAVING")))
        .getOrElse("1 == 1")
    case other => fail(s"[$sql] $other")
  }

  private def wrongAmong(subset: Seq[Row]): Seq[String] = subset.flatMap { r =>
    val script = scriptOf(r.sql)
    val evaluated = (0 until (1 << r.n)).map(scriptTruth(script, _)).toVector
    if (evaluated == r.expected) None else Some(s"[${r.sql}] -> $script")
  }

  "a HAVING over aggregates" should "emit a script that evaluates SQL's reading, for every shape" in {
    val wrong = wrongAmong(rows)
    withClue(
      s"${wrong.size} of ${rows.size} wrong, first 20:\n${wrong.take(20).mkString("\n")}\n"
    ) {
      wrong shouldBe empty
    }
  }

  it should "answer every unparenthesised mix -- those that used to be right by accident included" in {
    val mixes = rows.filter(r => r.mixedLevel && r.flat)
    mixes should not be empty
    wrongAmong(mixes) shouldBe empty
  }

  it should "parenthesise a written OR group under an AND" in {
    // its own arm renders `(a) || (b)` -- the operands, not the group
    val row = Row(
      "written group",
      3,
      build(1, 3, Vector(false, true), Set((1, 2)), Set.empty, Set.empty, paren = false)
    )
    row.sql should endWith("HAVING (MAX(c1) > 0 OR MAX(c2) > 0) AND MAX(c3) > 0")
    wrongAmong(Seq(row)) shouldBe empty
  }
}
