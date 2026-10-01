package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.operator.{AND, OR}
import app.softnetwork.elastic.sql.parser.ConditionPopulation._
import app.softnetwork.elastic.sql.query._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A condition that combines AND and OR is reduced with SQL precedence -- `NOT` > `AND` > `OR`,
  * left-associative, parentheses respected -- in every clause that takes one, and the SQL the
  * engine renders back re-parses to the very same tree.
  *
  * 🔴 The evidence is the DERIVED population of `ConditionPopulation` against an oracle computed
  * from the generated structure only. Every test COLLECTS its wrong statements and asserts the list
  * once: a failing `foreach` would stop at the first one and hide how many there are.
  */
class ConditionPrecedenceSpec extends AnyFlatSpec with Matchers {

  private lazy val population: List[Gen] = ConditionPopulation.population()

  private def treeOf(clause: Clause, sql: String): Criteria =
    Parser.parseUnvalidated(sql) match {
      case Right(st) => criteriaOf(clause, st).getOrElse(fail(s"[$sql] carries no condition"))
      case Left(e)   => fail(s"[$sql] does not parse: ${e.msg}")
    }

  private def whereOf(sql: String): Criteria = treeOf(WhereC, sql)

  private def formula(clause: Clause, c: Criteria): F =
    formulaOf(c, clause).fold(why => fail(why), identity)

  private def assertNoneWrong(wrong: Seq[String], of: Int): Unit =
    withClue(s"${wrong.size} of $of wrong, first 20:\n${wrong.take(20).mkString("\n")}\n") {
      wrong shouldBe empty
    }

  "the oracle" should "read a OR b AND c as a OR (b AND c), a written group as written" in {
    val w = build(1, 3, Vector(false, true), Set.empty, Set.empty, Set.empty, paren = false)
    render(w, leafText(WhereC, _ => 0)) shouldBe "c1 = 1 OR c2 = 1 AND c3 = 1"
    ansi(w) shouldBe O(L(1), A(L(2), L(3)))
    leftFold(w) shouldBe A(O(L(1), L(2)), L(3))
    val g = build(1, 3, Vector(false, true), Set((1, 2)), Set.empty, Set.empty, paren = false)
    render(g, leafText(WhereC, _ => 0)) shouldBe "(c1 = 1 OR c2 = 1) AND c3 = 1"
    ansi(g) shouldBe A(O(L(1), L(2)), L(3))
  }

  "the population" should "cover every clause with mixes at one level and parenthesised mixes" in {
    val empty = for {
      clause <- Clauses
      cell   <- List("mixed level", "parenthesised mix", "one operator")
      if !population.exists { g =>
        g.clause == clause && (cell match {
          case "mixed level"       => g.mixedLevel
          case "parenthesised mix" => !g.mixedLevel && !g.oneOperator
          case _                   => g.oneOperator
        })
      }
    } yield s"${clause.name} / $cell"
    empty shouldBe empty
  }

  "every condition, in every clause" should "reduce to the tree SQL precedence means" in {
    val wrong = population.flatMap { g =>
      Parser.parseUnvalidated(g.sql) match {
        case Left(e) => Some(s"[${g.sql}] grammar rejected: ${e.msg}")
        case Right(st) =>
          criteriaOf(g.clause, st) match {
            case None => Some(s"[${g.sql}] carries no condition")
            case Some(c) =>
              formulaOf(c, g.clause) match {
                case Left(why) => Some(s"[${g.sql}] $why")
                case Right(f) =>
                  val built = table(f, g.n)
                  if (built == g.ansiTable) None
                  else if (built == g.leftTable) Some(s"[${g.sql}] built the LEFT FOLD")
                  else Some(s"[${g.sql}] built neither SQL's reading nor the left fold")
              }
          }
      }
    }
    assertNoneWrong(wrong, population.size)
  }

  "the SQL rendered back" should "re-parse to the same statement and render identically" in {
    val wrong = population.flatMap { g =>
      Parser.parseUnvalidated(g.sql).toOption.flatMap { st =>
        val rendered = st.sql
        Parser.parseUnvalidated(rendered) match {
          case Left(e)                     => Some(s"[$rendered] does not re-parse: ${e.msg}")
          case Right(again) if again != st => Some(s"[$rendered] re-parses to another AST")
          case Right(again) if again.sql != rendered => Some(s"[$rendered] renders ${again.sql}")
          case _                                     => None
        }
      }
    }
    assertNoneWrong(wrong, population.size)
  }

  "a level after a parenthesised group" should "keep its operators where they were written" in {
    // the reduction used to push the group without folding the pending `c1 OR`, then built the next
    // predicate with the NEW operator and re-pushed the OLD one: (c1 AND (g)) OR c4
    formula(
      WhereC,
      whereOf("SELECT id FROM t WHERE c1 = 1 OR (c2 = 1 OR c3 = 1) AND c4 = 1")
    ) shouldBe
    O(L(1), A(O(L(2), L(3)), L(4)))
  }

  "four conditions" should "reduce with precedence although the scanner pairs them left to right" in {
    // the scanner hands `[c1 AND c2], AND, [c3 OR c4]` for the first one
    Seq(
      "c1 = 1 AND c2 = 1 AND c3 = 1 OR c4 = 1" -> O(A(A(L(1), L(2)), L(3)), L(4)),
      "c1 = 1 OR c2 = 1 AND c3 = 1 OR c4 = 1"  -> O(O(L(1), A(L(2), L(3))), L(4)),
      "c1 = 1 AND c2 = 1 OR c3 = 1 AND c4 = 1" -> O(A(L(1), L(2)), A(L(3), L(4))),
      "c1 = 1 OR c2 = 1 OR c3 = 1 AND c4 = 1"  -> O(O(L(1), L(2)), A(L(3), L(4)))
    ).foreach { case (cond, expected) =>
      withClue(s"[$cond] ") {
        formula(WhereC, whereOf(s"SELECT id FROM t WHERE $cond")) shouldBe expected
      }
    }
  }

  "a NOT heading conditions joined by AND after an OR" should
  "be folded into its condition -- the criterion the grammar builds for the same text" in {
    // (the NOT as written after OR, the spelling the grammar reads at the start of a condition)
    val kinds = Seq(
      "NOT c2 = 1"                   -> "NOT c2 = 1",
      "NOT c2 > 1"                   -> "NOT c2 > 1",
      "NOT c2 LIKE 'x%'"             -> "c2 NOT LIKE 'x%'",
      "NOT c2 RLIKE 'x.*'"           -> "c2 NOT RLIKE 'x.*'",
      "NOT c2 IN (1, 2)"             -> "c2 NOT IN (1, 2)",
      "NOT c2 BETWEEN 1 AND 2"       -> "c2 NOT BETWEEN 1 AND 2",
      "NOT c2 IS NULL"               -> "c2 IS NOT NULL",
      "NOT c2 IS NOT NULL"           -> "c2 IS NULL",
      "NOT EXISTS (SELECT 1 FROM u)" -> "NOT EXISTS (SELECT 1 FROM u)",
      "NOT c2 IN (SELECT a FROM u)"  -> "c2 NOT IN (SELECT a FROM u)",
      "NOT ISNULL(c2)"               -> "ISNOTNULL(c2)",
      "NOT ISNOTNULL(c2)"            -> "ISNULL(c2)"
    )
    val wrong = kinds.flatMap { case (written, spelled) =>
      val sql = s"SELECT id FROM t WHERE c1 = 1 OR $written AND c3 = 1"
      val expectedRun = whereOf(s"SELECT id FROM t WHERE $spelled AND c3 = 1")
      Parser(sql) match {
        case Left(e) => Some(s"[$sql] refused: ${e.msg}")
        case Right(st) =>
          whereOf(sql) match {
            case Predicate(_, OR, run, None, false) if run == expectedRun =>
              if (Parser(st.sql) == Right(st)) None
              else Some(s"[$sql] renders ${st.sql}, which does not re-parse to the same statement")
            case other => Some(s"[$sql] built $other, expected c1 OR ($expectedRun)")
          }
      }
    }
    assertNoneWrong(wrong, kinds.size)
  }

  it should "be refused by name where the condition cannot carry its own NOT" in {
    // (written as the message renders them: a MATCH over several columns renders `(c2,c4)`)
    val refused = Seq("MATCH (c2) AGAINST ('q')", "MATCH (c2,c4) AGAINST ('q')")
    val wrong = refused.flatMap { c =>
      val sql = s"SELECT id FROM t WHERE c1 = 1 OR NOT $c AND c3 = 1"
      // the remedy the message gives, written as it says
      val remedy = s"SELECT id FROM t WHERE c1 = 1 OR (c3 = 1 AND NOT $c)"
      (Parser(sql), Parser(remedy)) match {
        case (Left(e), Right(_))
            if e.msg.startsWith(s"NOT $c cannot start conditions joined by AND after an OR") &&
              e.msg.contains(s"for example A OR (B AND NOT $c)") =>
          None
        case (verdict, remedyVerdict) =>
          Some(s"[$sql] got $verdict; its remedy [$remedy] got $remedyVerdict")
      }
    }
    assertNoneWrong(wrong, refused.size)
  }

  "a condition the reducer cannot use" should "keep the message it had" in {
    Seq(
      "SELECT id FROM t WHERE c1 = 1 AND"           -> "WHERE clause requires criteria",
      "SELECT id FROM t WHERE c1 = 1 OR c2 = 1 AND" -> "WHERE clause requires criteria",
      "SELECT id FROM t WHERE AND c1 = 1"           -> "Invalid stack state for predicate creation",
      "SELECT id FROM t WHERE (c1 = 1"              -> "Unbalanced parentheses",
      "SELECT id FROM t WHERE c1 = 1 AND ()"        -> "Empty sub-expression",
      "SELECT g, COUNT(*) AS n FROM t GROUP BY g HAVING MAX(c1) > 1 OR" -> "HAVING clause requires criteria",
      "SELECT t.id FROM t JOIN u ON t.a = u.a AND"    -> "ON clause requires criteria",
      "SELECT id FROM t WHERE child(child.a = 1 AND)" -> "CHILD clause requires criteria"
    ).foreach { case (sql, message) =>
      withClue(s"[$sql] ") {
        Parser(sql) match {
          case Left(e)  => e.msg should include(message)
          case Right(s) => fail(s"accepted as ${s.sql}")
        }
      }
    }
  }

  "a relation body that mixes AND and OR" should "reduce to the tree the same condition has at top level" in {
    Seq(
      "child.a = 1 OR child.b = 2 AND child.c = 3",
      "child.a = 1 AND child.b = 2 OR child.c = 3 AND child.d = 4",
      "child.a = 1 OR NOT child.b = 2 AND child.c = 3",
      "child.a = 1 OR (child.b = 2 OR child.c = 3) AND child.d = 4"
    ).foreach { shape =>
      val top = whereOf(s"SELECT * FROM t WHERE $shape")
      whereOf(s"SELECT * FROM t WHERE child($shape)") match {
        case r: ElasticRelation => withClue(s"[$shape] ")(r.criteria shouldBe top)
        case other              => fail(s"[$shape] built $other")
      }
    }
  }

  "a tree rebuilt in code" should "render parentheses wherever an OR sits under an AND" in {
    val a = whereOf("SELECT id FROM t WHERE a = 1")
    val b = whereOf("SELECT id FROM t WHERE b = 1")
    val c = whereOf("SELECT id FROM t WHERE c = 1")
    val rows = Seq[(Criteria, String)](
      Predicate(a, AND, Predicate(b, OR, c)) -> "a = 1 AND (b = 1 OR c = 1)",
      Predicate(Predicate(a, OR, b), AND, c) -> "(a = 1 OR b = 1) AND c = 1",
      Predicate(a, OR, Predicate(b, AND, c)) -> "a = 1 OR b = 1 AND c = 1",
      // the transparent wrapper `update` puts around an all-nested operand renders bare
      Predicate(a, AND, ElasticNested(Predicate(b, OR, c), None)) -> "a = 1 AND (b = 1 OR c = 1)",
      // a written relation renders its own parentheses
      Predicate(a, AND, ElasticNested(Predicate(b, OR, c), None, fromCriteria = false)) ->
      "a = 1 AND NESTED(b = 1 OR c = 1)"
    )
    val wrong = rows.flatMap { case (tree, expected) =>
      if (tree.sql != expected) Some(s"rendered [${tree.sql}], expected [$expected]")
      else {
        // same meaning once parsed again (the leaves a, b, c are 3 variables)
        def f(x: Criteria): F = x match {
          case p: Predicate =>
            val r = if (p.not.isDefined) N(f(p.rightCriteria)) else f(p.rightCriteria)
            if (p.operator == AND) A(f(p.leftCriteria), r) else O(f(p.leftCriteria), r)
          case r: ElasticRelation => f(r.criteria)
          case e: Expression      => L(Seq("a", "b", "c").indexOf(e.identifier.name) + 1)
          case other              => fail(s"unexpected $other")
        }
        val again = whereOf(s"SELECT id FROM t WHERE ${tree.sql}")
        if (table(f(again), 3) == table(f(tree), 3)) None
        else Some(s"[${tree.sql}] re-parses to another meaning")
      }
    }
    assertNoneWrong(wrong, rows.size)
  }
}
