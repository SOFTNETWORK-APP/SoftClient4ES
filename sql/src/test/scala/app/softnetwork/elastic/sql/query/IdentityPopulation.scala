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

import app.softnetwork.elastic.sql.function.{
  Function,
  FunctionN,
  FunctionUtils,
  FunctionWithIdentifier
}
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{Identifier, PainlessContext, PainlessContextType, SQLKeywords}

import scala.util.Try

/** The population story IDENT-1's lemma runs over (and IDENT-4 may reuse): every function SPELLING
  * the registry accepts, crossed with an argument-shape alphabet, four operand spellings and two
  * nestings -- kept where the grammar accepts the statement, REPORTED where it does not. Never a
  * hand list of functions: a spelling added to `SQLKeywords.functionTokens` joins on arrival.
  */
object IdentityPopulation {

  val schema: SchemaTable = SchemaTable(
    "t",
    columns = List(Column("d", SQLTypes.Date), Column("s", SQLTypes.Keyword))
  )

  /** The argument-shape ALPHABET (`%1$s` = spelling, `%2$s` = operand) -- the arities and keyword
    * forms the grammar's function productions take. Extend it when a temporal or string spelling is
    * exercised by no shape; never add an exclusion list.
    *
    * The last shape is the one a ZERO-arity temporal spelling (`CURRENT_DATE`, `NOW`, `TODAY`, ...)
    * takes beside a column -- the sibling argument of a function holding the column, as in the
    * documented `DATE_DIFF(birthdate, CURRENT_DATE, YEAR)`. Without it those seven spellings were
    * exercised by no shape at all (MEASURED on the baseline): they have no argument that could hold
    * a copy of the column, so this is the only way they reach a chain beside one.
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
    "DATE_DIFF(%2$s, %1$s, DAY)"
  )

  /** (kind, operand, FROM) -- the ways a column reaches a function after `update`.
    *
    * 🔴 `unnested-as-field` is not a variant of `unnested`: with the UNNEST aliased by its own
    * field name, `update` "renames" `items.d` to `items.d` -- a resolution that does NOT move the
    * doc access
    * -- which is the only way a copy `update` never resolves can agree with the column on `param`
    * while disagreeing on the correlation (story IDENT-1, AD-5 / AD-8). An axis that cannot spell
    * it proves nothing about it. The spelling is common (`UNNEST(o.items) AS items`).
    */
  val operands: Seq[(String, String, String)] = Seq(
    ("bare", "d", "FROM t"),
    ("qualified", "t.d", "FROM t"),
    ("unnested", "i.d", "FROM t JOIN UNNEST(t.items) AS i"),
    ("unnested-as-field", "items.d", "FROM t JOIN UNNEST(t.items) AS items")
  )

  case class Cell(spelling: String, template: String, operandKind: String, depth: Int, sql: String)

  lazy val spellings: List[String] =
    SQLKeywords.functionTokens.flatMap(SQLKeywords.wordsOf).distinct.sorted

  /** Reporting / non-vacuity ONLY (never selection): the family a spelling's token belongs to, read
    * off the token's package -- `function.time` and `sql.time` (the extractors) are temporal,
    * `function.string` is string.
    */
  lazy val familyOf: Map[String, String] =
    SQLKeywords.functionTokens.flatMap { t =>
      val cls = t.getClass.getName
      val family =
        if (cls.contains(".function.time.") || cls.contains(".sql.time.")) "temporal"
        else if (cls.contains(".function.string.")) "string"
        else "other"
      SQLKeywords.wordsOf(t).map(_ -> family)
    }.toMap

  private def nest(template: String, spelling: String, operand: String, depth: Int): String =
    (1 to depth).foldLeft(operand)((inner, _) => template.format(spelling, inner))

  private def verdict(sql: String): Either[String, SingleSearch] =
    Parser(sql) match {
      case Right(ss: SingleSearch) => Right(ss)
      case Right(other)            => Left(s"not a SingleSearch: ${other.getClass.getSimpleName}")
      case Left(err)               => Left(err.msg)
    }

  /** Every candidate with the grammar's verdict. Depth 3 is tried only where depth 1 parsed. */
  lazy val candidates: Seq[(Cell, Either[String, SingleSearch])] =
    for {
      spelling              <- spellings
      template              <- templates
      (kind, operand, from) <- operands
      depth1 = Cell(
        spelling,
        template,
        kind,
        1,
        s"SELECT ${nest(template, spelling, operand, 1)} AS c $from"
      )
      first = depth1 -> verdict(depth1.sql)
      cell <- first +: (if (first._2.isRight) {
                          val depth3 = Cell(
                            spelling,
                            template,
                            kind,
                            3,
                            s"SELECT ${nest(template, spelling, operand, 3)} AS c $from"
                          )
                          Seq(depth3 -> verdict(depth3.sql))
                        } else Nil)
    } yield cell

  lazy val exercised: Seq[(Cell, SingleSearch)] = candidates.collect { case (c, Right(ss)) =>
    c -> ss
  }

  lazy val exercisedSpellingsPerFamily: Map[String, Int] =
    exercised
      .map(_._1.spelling)
      .distinct
      .groupBy(s => familyOf.getOrElse(s, "other"))
      .map { case (family, ss) =>
        family -> ss.size
      }
      .withDefaultValue(0)

  /** Exercised-spelling floors MEASURED on the baseline (story IDENT-1) -- they guard against the
    * population silently shrinking (a template or registry change), not the verdict.
    */
  val TemporalFloor: Int = 73
  val StringFloor: Int = 24

  def carrierOf(ss: SingleSearch): Identifier = ss.select.fields.head.identifier

  /** Every copy of the column a function chain carries: the carrier, each function's receiver
    * (`FunctionWithIdentifier.identifier`) and each identifier ARGUMENT -- the pairs
    * `TransformFunction.checkIfNullable` (site 2) and `processorBase` (site 3) compare. 🔴 NOT
    * `FunctionUtils.funIdentifiers`: its `FunctionN` arm is matched first and skips a
    * `FunctionWithIdentifier`'s receiver (`DateTrunc.args` is `List(unit)`).
    */
  def copies(id: Identifier): Seq[Identifier] = id +: id.functions.flatMap(fnCopies)

  private def fnCopies(f: Function): Seq[Identifier] = f match {
    case i: Identifier => copies(i)
    case other =>
      val receiver = other match {
        case fwi: FunctionWithIdentifier => copies(fwi.identifier)
        case _                           => Nil
      }
      val arguments = other match {
        case fn: FunctionN[_, _] =>
          fn.args.flatMap {
            case i: Identifier => copies(i)
            case g: Function   => fnCopies(g)
            case _             => Nil
          }
        case _ => Nil
      }
      receiver ++ arguments
  }

  /** Pairs that AGREE on the doc access and DISAGREE on the correlation -- the only pairs the new
    * equality answers differently from the old. Empty for every cell is the lemma.
    */
  def lemmaViolations(carrier: Identifier): Seq[String] = {
    val cs = copies(carrier)
    for {
      a <- cs
      b <- cs
      if a.param == b.param && a.tableAlias != b.tableAlias
    } yield s"${a.param}: ${a.tableAlias} vs ${b.tableAlias}"
  }

  /** Distinct copies that agree on the doc access AND carry a correlation -- the case the lemma
    * must actually be checked on. A population with none of these proved nothing.
    */
  def correlatedAgreeingPairs(carrier: Identifier): Int = {
    val cs = copies(carrier)
    (for {
      a <- cs
      b <- cs
      if (a ne b) && a.param == b.param && a.tableAlias.isDefined
    } yield 1).size
  }

  /** Site 3's discriminator, read off the AST: does the INNERMOST transform list the column's own
    * copy among its arguments? (`functions` is stored OUTERMOST first.)
    */
  def stratum(carrier: Identifier): String =
    FunctionUtils.transformFunctions(carrier).lastOption match {
      case None                            => "no-function"
      case Some(_) if carrier.name.isEmpty => "nameless"
      case Some(f) =>
        f match {
          case fwi: FunctionWithIdentifier =>
            fwi match {
              case fn: FunctionN[_, _] if fn.args.exists(_ eq fwi.identifier) => "argument"
              case _                                                          => "chained"
            }
          case _ => "chained"
        }
    }

  /** Whether every identifier ARGUMENT of the innermost transform reads the same doc access as the
    * carrier. False where the single-update lag leaves a stale dotted copy: a qualified column
    * after ONE `update()`, because the qualified branch of `GenericIdentifier.update` does not
    * update the chain. Pre-existing and equality-invariant, so the site-3 row reports those rows
    * instead of judging them. ⚠️ It is a statement about `param` ONLY: a copy that is never
    * resolved but reads the SAME doc access as the carrier -- `WEEKDAY`'s receiver under an UNNEST
    * aliased by its own field, before `DayOfWeek` learned to resolve it (story IDENT-1) -- answers
    * `true` here; the site-3 row's DEAD-parse assertion is what sees that class.
    */
  def receiverResolved(carrier: Identifier): Boolean =
    FunctionUtils
      .transformFunctions(carrier)
      .lastOption
      .toSeq
      .flatMap {
        case fn: FunctionN[_, _] => fn.args.collect { case i: Identifier => i }
        case _                   => Nil
      }
      .forall(_.param == carrier.param)

  def strataOf(cells: Seq[(Cell, SingleSearch)]): Map[String, Int] =
    cells.groupBy { case (_, ss) => stratum(carrierOf(ss)) }.map { case (k, v) => k -> v.size }

  /** The parse declarations of a processor script that nothing READS: `def paramN = <expr>;` whose
    * expression parses a temporal (`instanceof String`) while `paramN` never occurs again. That is
    * the shape site 3 exists to prevent -- "a parse nobody reads" -- and the only form in which an
    * equality that re-answers site 3 shows up: the operand is still read once, from another
    * parameter, so a parse COUNT stays at one while a dead declaration appears (MEASURED: the
    * `unnested-as-field` `WEEKDAY` rows went from 0 dead parses to 1 under the correlation-aware
    * equality alone).
    */
  def deadParses(script: String): Seq[String] =
    "def (param[0-9]+) = ".r.findAllMatchIn(script).toSeq.flatMap { m =>
      val name = m.group(1)
      val end = script.indexOf("; ", m.end)
      val expr = script.substring(m.end, if (end < 0) script.length else end)
      val uses = ("\\b" + name + "\\b").r.findAllMatchIn(script).size
      if (uses == 1 && expr.contains("instanceof String")) Seq(s"$name = $expr") else Nil
    }

  /** The processor script of the SELECT item, rendered as softclient4es-extensions' `FieldAnalyzer`
    * renders a scripted field (`PainlessContext(Processor)`, prologue + body). `None` if it throws.
    */
  def processorScript(ss: SingleSearch): Option[String] =
    Try {
      val ctx = PainlessContext(PainlessContextType.Processor)
      val body = ss.select.fields.head.painless(Some(ctx))
      s"$ctx$body"
    }.toOption
}
