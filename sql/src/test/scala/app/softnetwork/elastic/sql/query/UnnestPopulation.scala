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

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.{SQLKeywords, TokenRegex}
import app.softnetwork.elastic.sql.function.convert.CastOperator
import app.softnetwork.elastic.sql.operator.math.{ADD, DIVIDE, MODULO, MULTIPLY, SUBTRACT}
import app.softnetwork.elastic.sql.parser.Parser

/** The population the UNNEST function rule is asserted over -- DERIVED, never a hand list of
  * functions: every function SPELLING the registry accepts (`SQLKeywords.functionTokens`), crossed
  * with an argument-shape alphabet and the five column types of the nested object, plus the
  * operator forms the grammar declares as symbol tokens (read off their objects), `CASE`, depth-2
  * chains and the mixed parent / nested operand pairs in both orders. A cell is kept where its FLAT
  * form parses
  * -- the spelling and arity are then valid, so what the nested form answers is the rule's doing.
  *
  * A spelling added to the registry joins on arrival; a spelling that no shape exercises is
  * reported by [[notExercised]] and pinned by a floor.
  */
object UnnestPopulation {

  /** The nested object's columns (`d` date, `s` keyword, `n` long, `x` double, `b` boolean). */
  val nestedCols: Seq[String] = Seq("d", "s", "n", "x", "b")

  /** A parent column of a compatible type, for the mixed pairs. */
  val parentTwin: Map[String, String] =
    Map("d" -> "created", "s" -> "name", "n" -> "m", "x" -> "m")

  /** The two ways a statement names an UNNEST element: an alias that differs from the field, and
    * the self-named alias -- the spelling under which `GenericIdentifier.update`'s rename is a
    * no-op.
    */
  val aliases: Seq[String] = Seq("i", "items")

  def familyOf(t: TokenRegex): String = {
    val cls = t.getClass.getName
    if (cls.contains(".function.time.") || cls.contains(".sql.time.")) "temporal"
    else if (cls.contains(".function.string.")) "string"
    else if (cls.contains(".function.math.")) "math"
    else if (cls.contains(".function.convert.")) "conversion"
    else if (cls.contains(".function.cond.")) "conditional"
    else if (cls.contains(".function.geo.")) "geo"
    else if (cls.contains(".function.aggregate.")) "aggregate"
    else "other"
  }

  lazy val spellings: Seq[(String, String)] =
    SQLKeywords.functionTokens
      .flatMap(t => SQLKeywords.wordsOf(t).map(w => w -> familyOf(t)))
      .distinct
      .sortBy(_._1)

  /** The argument-shape ALPHABET (`%1$s` = spelling, `%2$s` = operand). Extend it when a spelling
    * is exercised by no shape; never add an exclusion list.
    */
  val templates: Seq[String] = Seq(
    "%1$s(%2$s)",
    "%1$s(%2$s, MONTH)",
    "%1$s(MONTH, %2$s)",
    "%1$s(%2$s, INTERVAL 1 DAY)",
    "%1$s(DAY, 1, %2$s)",
    "%1$s(%2$s, 'yyyy-MM-dd')",
    "%1$s(%2$s, %2$s, DAY)",
    "%1$s(DAY, %2$s, %2$s)",
    "%1$s(%2$s, %2$s)",
    "%1$s(YEAR FROM %2$s)",
    "%1$s(%2$s, 1)",
    "%1$s(%2$s, 1, 2)",
    "%1$s(%2$s, 'x')",
    "%1$s('x', %2$s)",
    "%1$s(%2$s, 'x', 'y')",
    "%1$s(%2$s AS VARCHAR)",
    "%1$s(%2$s AS BIGINT)",
    "%1$s(%2$s AS DOUBLE)",
    "%1$s(%2$s AS DATE)",
    "%1$s(%2$s, VARCHAR)",
    "%1$s('x' IN %2$s)",
    "%1$s(%2$s FROM 1 FOR 2)",
    "DATE_DIFF(%2$s, %1$s, DAY)"
  )

  /** The operator forms -- the grammar's symbol tokens, read off their objects. */
  lazy val operatorTemplates: Seq[(String, String, String)] = {
    val arithmetic = Seq(ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO).flatMap { op =>
      val escaped = op.sql.replace("%", "%%") // a String.format template
      Seq(
        ("arithmetic", op.sql, s"%2$$s $escaped 2"),
        ("arithmetic", op.sql, s"2 $escaped %2$$s")
      )
    }
    val castOp = CastOperator.sql.replace("\\", "")
    val casts =
      Seq("VARCHAR", "BIGINT", "DOUBLE", "DATE").map(t => ("conversion", castOp, s"%2$$s$castOp$t"))
    val cases = Seq(
      ("conditional", "CASE", "CASE WHEN %2$s IS NULL THEN 'n' ELSE 'v' END"),
      ("conditional", "CASE", "CASE WHEN 1 = 1 THEN %2$s ELSE %2$s END"),
      ("conditional", "CASE", "CASE %2$s WHEN %2$s THEN 1 ELSE 0 END")
    )
    arithmetic ++ casts ++ cases
  }

  val pairTemplates: Seq[String] =
    Seq("%1$s(%2$s, %3$s)", "%1$s(%2$s, %3$s, DAY)", "%1$s(DAY, %2$s, %3$s)")

  /** One cell: `expr(operandOf)` renders the expression for a given operand spelling function. */
  final case class Cell(
    family: String,
    spelling: String,
    shape: String,
    expr: (String => String, String => String) => String
  ) {
    def flat: String = expr(identity, identity)
    def nested(alias: String): String = expr(col => s"$alias.$col", p => s"t.$p")
  }

  private def fmt(t: String, args: String*): String = t.format(args: _*)

  lazy val candidates: Seq[Cell] = {
    val fn = for {
      (spelling, family) <- spellings
      template           <- templates
      col                <- nestedCols
    } yield Cell(family, spelling, s"fn:$template:$col", (o, _) => fmt(template, spelling, o(col)))
    val chain2 = for {
      (spelling, family) <- spellings
      template           <- templates
      col                <- nestedCols
    } yield Cell(
      family,
      spelling,
      s"chain2:$template:$col",
      (o, _) => fmt(template, spelling, "(" + fmt(template, spelling, o(col)) + ")")
    )
    val ops = for {
      (family, spelling, template) <- operatorTemplates
      col                          <- nestedCols
    } yield Cell(family, spelling, s"op:$template:$col", (o, _) => fmt(template, "", o(col)))
    val pairs = for {
      (spelling, family) <- spellings
      template           <- pairTemplates
      (col, pcol)        <- parentTwin.toSeq
      nestedFirst        <- Seq(true, false)
    } yield Cell(
      family,
      spelling,
      s"pair:$template:$col:$nestedFirst",
      (o, p) =>
        if (nestedFirst) fmt(template, spelling, o(col), p(pcol))
        else fmt(template, spelling, p(pcol), o(col))
    )
    // arithmetic between an UNNEST column and a PARENT column, both orders (the operator objects)
    val opPairs = for {
      op          <- Seq(ADD, SUBTRACT, MULTIPLY, DIVIDE, MODULO)
      (col, pcol) <- parentTwin.toSeq if col == "n" || col == "x"
      nestedFirst <- Seq(true, false)
    } yield Cell(
      "arithmetic",
      op.sql,
      s"op-pair:${op.sql}:$col:$nestedFirst",
      (o, p) =>
        if (nestedFirst) s"${o(col)} ${op.sql} ${p(pcol)}"
        else s"${p(pcol)} ${op.sql} ${o(col)}"
    )
    fn ++ chain2 ++ ops ++ pairs ++ opPairs
  }

  private def flatParses(expr: String): Boolean = Parser(s"SELECT $expr AS c FROM t").isRight

  /** The kept cells: those whose FLAT form parses.
    *
    * Decided ONCE per shape, over the column `d`: without a schema every column is `ANY`, so the
    * grammar's verdict does not depend on which column fills the slot (MEASURED: every
    * single-column family count is a multiple of the five columns). Parsing all 38,967 candidates
    * costs ~20 s; this costs one parse per shape. The floor in the spec pins the resulting count to
    * the full parse's, so a shape the shortcut would lose is caught.
    */
  lazy val exercised: Seq[Cell] = {
    val verdicts = scala.collection.mutable.Map.empty[String, Boolean]
    def parsesOnce(probe: String): Boolean = verdicts.getOrElseUpdate(probe, flatParses(probe))
    candidates.filter { c =>
      val probe = c.expr(_ => "d", p => p)
      // a depth-2 chain is tried only where its depth-1 shape parses (the same spelling and
      // template around the bare column) -- a chain of an unparseable call cannot parse
      if (c.shape.startsWith("chain2:")) {
        val parts = c.shape.split(':')
        val depth1 = fmt(parts(1), c.spelling, "d")
        parsesOnce(depth1) && parsesOnce(probe)
      } else parsesOnce(probe)
    }
  }

  lazy val notExercised: Seq[(String, String)] = {
    val seen = exercised.map(_.spelling).toSet
    spellings.filterNot { case (s, _) => seen.contains(s) }
  }

  def from(alias: String): String = s"FROM t JOIN UNNEST(t.items) AS $alias"

  def selectSql(c: Cell, alias: String): String =
    s"SELECT id, ${c.nested(alias)} AS c ${from(alias)}"

  /** The same projection BESIDE a window function: a window-enriched row query, whose rows come
    * from the window-free statement core executes (`SingleSearch.withoutWindows`).
    */
  val window: String = "COUNT(*) OVER (PARTITION BY id) AS w"

  def selectBesideWindowSql(c: Cell, alias: String): String =
    s"SELECT id, ${c.nested(alias)} AS c, $window ${from(alias)}"

  /** The parent twin of a single-column cell: the SAME expression over the parent's column. */
  def parentSelectSql(c: Cell, alias: String, beside: Option[String] = None): Option[String] =
    if (c.shape.startsWith("pair:")) None
    else {
      val col = c.shape.split(':').last
      parentTwin.get(col).map { pcol =>
        val e = c.expr(_ => s"t.$pcol", p => s"t.$p")
        s"SELECT id, $alias.k AS k, $e AS c${beside.map(w => s", $w").getOrElse("")} ${from(alias)}"
      }
    }

  /** Literals tried, in order, until the WHERE form clears every rule that runs before this one. */
  val whereLiterals: Seq[String] = Seq("'x'", "1", "1.5", "TRUE", "'2025-01-01'")

  def whereSql(c: Cell, alias: String, literal: String): String =
    s"SELECT id, $alias.k AS k ${from(alias)} WHERE ${c.nested(alias)} = $literal"
}
