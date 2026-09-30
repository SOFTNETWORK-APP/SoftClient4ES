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

import app.softnetwork.elastic.sql.function.math.PowParam
import app.softnetwork.elastic.sql.function.time.{DayOfWeek, LastDayOfMonth}
import app.softnetwork.elastic.sql.function.TransformFunction
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.{GenericIdentifier, Identifier, LiteralParam, PainlessParam}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import scala.collection.immutable.{HashMap, HashSet}
import scala.util.{Failure, Success, Try}

/** Story IDENT-1 — two identifiers are equal iff they denote the same column THROUGH THE SAME
  * CORRELATION.
  *
  * MEASURED at the baseline (`origin/main` 9bf185f1) on the statement below: both buckets read
  * `doc['category'].value`, their table aliases are `Some(o)` and `Some(p)`, and yet `buckets(0) ==
  * buckets(1)` was TRUE and `buckets.distinct.size` was 1 -- `Identifier` inherited
  * `PainlessParam.equals` (the doc access alone) and every case class carrying an identifier
  * inherited the collapse through its synthesised equality.
  *
  * In order: the measured collapse at both resolutions (AC-1); the identity cross product with an
  * oracle that reads only the SPELLING (AC-2); the equals contract across every `PainlessParam`
  * family (AC-3); the lemma the three collapse-dependent sites rest on, over a population derived
  * from the function registry, and the sites' own decisions (AC-4); `WEEKDAY` / `DAYOFWEEK`
  * resolving their receiver, the one copy the lemma found `update` never resolved; and a
  * percentile's render keeping the correlation it was written with, which the correlation-aware AST
  * oracle is what exposed. R1 = `Parser.apply` (one `update()`); R2 = `updateAll`, the
  * softclient4es-extensions venue that resolves a JOIN a second time, one schema per leg
  * (`SearchApi.resolveWithSchema` refuses a closure-shaped statement before its own update; CTAS
  * column inference, `Table.mergeWithSearch`, re-updates with the first table's schema only).
  */
class IdentifierEqualitySpec extends AnyFlatSpec with Matchers {

  private val schemas: Map[String, SchemaTable] = Map(
    "orders" -> SchemaTable(
      "orders",
      columns = List(
        Column("id", SQLTypes.Keyword),
        Column("did", SQLTypes.Keyword),
        Column("cid", SQLTypes.Keyword),
        Column("category", SQLTypes.Keyword),
        Column("amount", SQLTypes.Double),
        Column("createdAt", SQLTypes.Timestamp)
      )
    ),
    "customers" -> SchemaTable(
      "customers",
      columns = List(
        Column("id", SQLTypes.Keyword),
        Column("category", SQLTypes.Keyword),
        Column("city", SQLTypes.Keyword)
      )
    )
  )

  private def single(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss
      case Right(other) =>
        fail(s"[$sql] expected a SingleSearch, got ${other.getClass.getSimpleName}")
      case Left(err) => fail(s"[$sql] did not parse: ${err.msg}")
    }

  private def resolutions(sql: String): Seq[(String, SingleSearch)] = {
    val r1 = single(sql)
    Seq("R1" -> r1, "R2" -> r1.updateAll(schemas))
  }

  /** The grammar and its single `update()` WITHOUT `validate()` -- the AST `single` returns, for a
    * row about RESOLUTION whose statement a validation rule may refuse.
    */
  private def unvalidated(sql: String): SingleSearch =
    Parser.parseUnvalidated(sql) match {
      case Right(ss: SingleSearch) => ss
      case Right(other) =>
        fail(s"[$sql] expected a SingleSearch, got ${other.getClass.getSimpleName}")
      case Left(err) => fail(s"[$sql] did not parse: ${err.msg}")
    }

  private def describe(p: PainlessParam): String = p match {
    case i: Identifier =>
      s"Identifier(name=${i.name}, tableAlias=${i.tableAlias}, quoted=${i.quoted}, param=${i.param})"
    case other => s"${other.getClass.getSimpleName}(${other.param})"
  }

  // -- AC-1 ---------------------------------------------------------------------------------------

  private val selfJoinGroupBy =
    "SELECT COUNT(*) AS c FROM orders o INNER JOIN orders p ON o.did = p.id " +
    "GROUP BY o.category, p.category"

  "Two correlations of one column" should "stay TWO buckets at both resolutions (AC-1)" in {
    resolutions(selfJoinGroupBy).foreach { case (res, ss) =>
      val buckets = ss.buckets
      withClue(s"[$res] ${buckets.map(b => describe(b.identifier))} ") {
        buckets.map(_.identifier.tableAlias) shouldBe Seq(Some("o"), Some("p"))
        // the premise, asserted: both read the SAME doc value -- all the old equality compared
        buckets.map(_.identifier.param).distinct shouldBe Seq("doc['category'].value")
        buckets.head should not be buckets(1)
        buckets.distinct.size shouldBe 2
        buckets.toSet.size shouldBe 2
      }
    }
  }

  it should "still equal its own re-parse -- the fix is not reference identity (AC-1)" in {
    val r1 = single(selfJoinGroupBy)
    Parser(r1.sql) shouldBe Right(r1)
    single(r1.sql).buckets shouldBe r1.buckets
  }

  it should "not force two correlations into one hash (hash QUALITY -- not the contract)" in {
    val ids = single(selfJoinGroupBy).buckets.map(_.identifier)
    ids.head.## should not be ids(1).##
  }

  // -- AC-2 ---------------------------------------------------------------------------------------

  private val shapes: Seq[(String, String)] = Seq(
    "single table" -> "FROM orders a",
    "two indices"  -> "FROM orders a JOIN customers b ON a.cid = b.id",
    "self-join"    -> "FROM orders a JOIN orders b ON a.did = b.id"
  )

  /** The alphabet: unqualified, alias A, alias B. In the single-table shape `b` is UNDECLARED, so
    * `b.category` stays an object path -- a different column, which both equalities keep apart.
    * (Named `aliasSpellings`, not `spellings`: `IdentityPopulation` has a `spellings` too.)
    */
  private val aliasSpellings: Seq[String] = Seq("category", "a.category", "b.category")

  "Identifier equality" should "hold iff the two identifiers are SPELLED alike, at both resolutions (AC-2)" in {
    val cells =
      for {
        (shape, from) <- shapes
        x             <- aliasSpellings
        y             <- aliasSpellings
        (res, ss)     <- resolutions(s"SELECT $x AS x, $y AS y $from")
      } yield (shape, x, y, res, ss)

    // DERIVED, and counted so a generator that silently produced fewer cells cannot pass
    cells.size shouldBe shapes.size * aliasSpellings.size * aliasSpellings.size * 2

    val mergedByTheOldEquality = cells.count { case (shape, x, y, res, ss) =>
      val idX = ss.select.fields.head.identifier
      val idY = ss.select.fields(1).identifier
      val expected = x == y // the ORACLE: the spelling, independent of the code under test
      withClue(s"[$shape][$res] $x vs $y: ${describe(idX)} / ${describe(idY)} ") {
        (idX == idY) shouldBe expected
        (idY == idX) shouldBe expected
        (Field(idX) == Field(idY)) shouldBe expected
        (Bucket(idX) == Bucket(idY)) shouldBe expected
        if (expected) idX.## shouldBe idY.##
        Set[Identifier](idX, idY).size shouldBe (if (expected) 1 else 2)
      }
      !expected && idX.param == idY.param
    }
    // non-vacuity: the population holds exactly the cells this story changes -- single table 2
    // (`category` / `a.category` both ways; `b.category` is an object path there), two indices 6,
    // self-join 6, per resolution
    mergedByTheOldEquality shouldBe 28
  }

  it should "survive render -> re-parse with the correlation intact -- the AST oracle now sees it (AC-2)" in {
    for {
      (_, from) <- shapes
      x         <- aliasSpellings
      y         <- aliasSpellings
    } {
      val r1 = single(s"SELECT $x AS x, $y AS y $from")
      withClue(s"[${r1.sql}] ") {
        Parser(r1.sql) shouldBe Right(r1)
        Parser(r1.sql).map(_.sql) shouldBe Right(r1.sql)
      }
    }
  }

  // -- AC-3 ---------------------------------------------------------------------------------------

  /** The rule under test, stated independently of the implementation: identifiers compare by doc
    * access and correlation; an identifier never equals a non-identifier; non-identifiers compare
    * by their rendered string.
    */
  private def contractEqual(p: PainlessParam, q: PainlessParam): Boolean = (p, q) match {
    case (a: Identifier, b: Identifier) => a.param == b.param && a.tableAlias == b.tableAlias
    case (_: Identifier, _) | (_, _: Identifier) => false
    case _                                       => p.param == q.param
  }

  "Every PainlessParam pair" should "honour the equals contract, across families (AC-3)" in {
    val joinIds = single(
      "SELECT category AS x, a.category AS y, b.category AS z " +
      "FROM orders a JOIN customers b ON a.cid = b.id"
    ).select.fields.map(_.identifier)
    val quotedPair = single("""SELECT category AS x, "category" AS y FROM orders a""").select.fields
      .map(_.identifier)
    val bare = joinIds.head
    withClue(s"${quotedPair.map(describe)} ") { quotedPair(1).quoted shouldBe true }

    val literal = LiteralParam(bare.param)
    literal.param shouldBe bare.param // the SAME rendered string: the case the symmetry arm exists for
    (literal == bare) shouldBe false
    (bare == literal) shouldBe false

    val all: Seq[PainlessParam] = joinIds ++ quotedPair ++ Seq(
      literal,
      LiteralParam(bare.param, None, Some("keyed")),
      LiteralParam("ctx.category"),
      PowParam(2),
      LiteralParam(PowParam(2).param)
    )
    for (p <- all; q <- all) withClue(s"${describe(p)} vs ${describe(q)} ") {
      (p == q) shouldBe (q == p)
      (p == q) shouldBe contractEqual(p, q)
      if (p == q) p.## shouldBe q.##
    }

    val set = HashSet(all: _*)
    val map = HashMap(all.map(p => p -> p.param): _*)
    all.foreach { p =>
      withClue(s"${describe(p)} ") {
        set.contains(p) shouldBe true
        map.contains(p) shouldBe true
      }
    }
    val classes = all.foldLeft(Seq.empty[PainlessParam]) { (acc, p) =>
      if (acc.exists(contractEqual(_, p))) acc else acc :+ p
    }
    set.size shouldBe classes.size
  }

  /** The family list above is complete only while these are the implementors -- DERIVED from the
    * sources, so a fourth one reddens this instead of silently escaping the contract.
    */
  "The PainlessParam implementors" should "all be covered by the contract test (AC-3)" in {
    val roots = Seq(
      "sql/src/main/scala",
      "core/src/main/scala",
      "bridge/src/main/scala",
      "es6/bridge/src/main/scala"
    ).map(new java.io.File(_))
    roots.foreach { r =>
      withClue(s"cwd=${new java.io.File(".").getAbsolutePath} root=$r ") {
        r.isDirectory shouldBe true
      }
    }
    def sources(f: java.io.File): Seq[java.io.File] =
      if (f.isDirectory) Option(f.listFiles()).toSeq.flatten.flatMap(sources)
      else if (f.getName.endsWith(".scala")) Seq(f)
      else Nil
    val withoutComments =
      (s: String) => s.replaceAll("(?s)/\\*.*?\\*/", " ").replaceAll("//[^\n]*", " ")
    val declaration =
      """\b(?:class|trait|object)\s+(\w+)(?:(?!\b(?:class|trait|object)\b)[^{}])*?\b(?:extends|with)\s+PainlessParam\b""".r
    val found = roots
      .flatMap(sources)
      .flatMap { f =>
        val src = withoutComments(new String(Files.readAllBytes(f.toPath), StandardCharsets.UTF_8))
        declaration.findAllMatchIn(src).map(_.group(1))
      }
      .toSet
    withClue("a new PainlessParam family must join the contract test above ") {
      found shouldBe Set("LiteralParam", "Identifier", "PowParam")
    }
  }

  // -- AC-4 ---------------------------------------------------------------------------------------

  // SELECTIVE on purpose: a wildcard import would make any later reference to a name this class
  // also defines ambiguous ("defined in class ... and imported subsequently").
  import IdentityPopulation.{
    candidates,
    carrierOf,
    correlatedAgreeingPairs,
    deadParses,
    exercised,
    exercisedSpellingsPerFamily,
    lemmaViolations,
    operands,
    processorScript,
    receiverResolved,
    schema,
    strataOf,
    stratum,
    StringFloor,
    TemporalFloor
  }

  "The copies of one column inside a function chain" should
  "never agree on the doc access while disagreeing on the correlation (AC-4, the lemma)" in {
    val notExercised = candidates.collect { case (cell, Left(reason)) => cell -> reason }
    val resolved = exercised.flatMap { case (cell, r1) =>
      Seq(
        (cell, "R1", Success(r1): Try[SingleSearch]),
        (cell, "R2", Try(r1.update(Some(schema))))
      )
    }
    val threw = resolved.collect { case (cell, res, Failure(e)) => s"[$res] ${cell.sql}: $e" }
    val ok = resolved.collect { case (cell, res, Success(ss)) => (cell, res, carrierOf(ss)) }
    val violations = ok.flatMap { case (cell, res, carrier) =>
      lemmaViolations(carrier).map(v => s"[$res] ${cell.sql}: $v")
    }

    info(
      s"candidates=${candidates.size} exercised=${exercised.size} " +
      s"not-exercised=${notExercised.size} threw=${threw.size} violations=${violations.size}"
    )
    info(s"strata=${strataOf(exercised)} families=$exercisedSpellingsPerFamily")

    withClue(s"a resolution THREW -- an outcome, not a pass: ${threw.take(10)} ") {
      threw shouldBe empty
    }
    // Over the WHOLE population: `WEEKDAY` / `DAYOFWEEK` resolve their receiver since story
    // IDENT-1, so no copy of the column is left that `update` never reaches (the lemma's one
    // exception, an UNNEST aliased by its own field, is gone by construction -- never excluded).
    withClue(s"${violations.take(10)} ") { violations shouldBe empty }

    // non-vacuity: the lemma was checked where it CAN fail
    strataOf(exercised).getOrElse("argument", 0) should be > 0
    strataOf(exercised).getOrElse("chained", 0) should be > 0
    operands.foreach { case (kind, _, _) =>
      withClue(s"[$kind] ")(exercised.count(_._1.operandKind == kind) should be > 0)
    }
    ok.collect { case (_, "R2", carrier) => correlatedAgreeingPairs(carrier) }.sum should be > 0
    // each family really reached, and not below the floors MEASURED on the baseline -- the floors
    // guard the POPULATION (a template or registry change shrinking it), not the verdict
    withClue("the floors are the counts measured on the baseline, never a placeholder ") {
      TemporalFloor should be > 0
      StringFloor should be > 0
    }
    exercisedSpellingsPerFamily("temporal") should be >= TemporalFloor
    exercisedSpellingsPerFamily("string") should be >= StringFloor
  }

  it should "flag a hand-built violation -- the checker is not vacuous (AC-4)" in {
    val raw = GenericIdentifier("d")
    val carrier =
      GenericIdentifier("d", tableAlias = Some("t"), functions = List(LastDayOfMonth(raw)))
    lemmaViolations(carrier) should not be empty
  }

  /** Site 2's DECISION, asserted directly (not its bytes). Expected values MEASURED on the baseline
    * -- a CHARACTERIZATION of `checkIfNullable`, labelled as one. The repeated chain is the shape
    * that separates structural equality from reference identity (MEASURED: `eq` turns every
    * repeated row false); the qualified spelling at R2 is deliberately NOT pinned -- the
    * single-update lag leaves the innermost copy stale there, a pre-existing asymmetry.
    */
  "TransformFunction.checkIfNullable (site 2)" should "keep its decision on the repeated and the mixed chain (AC-4)" in {
    val repeated = "SELECT LAST_DAY(LAST_DAY(LAST_DAY(%s))) AS c FROM t"
    val mixed = "SELECT LAST_DAY(DATE_TRUNC(DATE_TRUNC(%s, MONTH), MONTH)) AS c FROM t"
    def outermost(sql: String, res: String, operand: String): Boolean = {
      val r1 = single(sql.format(operand))
      val ss = if (res == "R1") r1 else r1.update(Some(schema))
      carrierOf(ss).functions.head match {
        case f: TransformFunction[_, _] => f.checkIfNullable
        case other                      => fail(s"outermost is not a TransformFunction: $other")
      }
    }
    Seq(("R1", "d"), ("R2", "d"), ("R1", "t.d")).foreach { case (res, op) =>
      withClue(s"[$res][$op] repeated ")(outermost(repeated, res, op) shouldBe true)
      withClue(s"[$res][$op] mixed ")(outermost(mixed, res, op) shouldBe false)
    }
  }

  /** Site 3, at the mechanism: under a function that takes the column as an ARGUMENT, the processor
    * operand is parsed at most once, and no parse is left that nothing reads.
    *
    * The two assertions see two different failures. An identity that also compared the rendered SQL
    * makes `processorBase` fire where the argument path already parses: TWO live parses (the
    * `instanceof String` count). An identity that re-answered site 3 for a copy `update` never
    * resolved -- `WEEKDAY`'s receiver under an UNNEST aliased by its own field, before `DayOfWeek`
    * resolved it -- leaves the operand read ONCE, through the argument path, beside a parse that
    * nothing reads: a count of one, and a DEAD declaration (MEASURED: 0 on the baseline, 2 rows
    * with the correlation-aware equality alone, 0 with the receiver resolved). Only the second
    * assertion sees that one.
    *
    * Scoped by the MECHANISM, not by a list: a row is judged only where the innermost function's
    * argument copy is RESOLVED like the carrier (`receiverResolved`). Where it is not -- the
    * single-update lag: a qualified column at R1 -- the stale dotted copy parses a second operand
    * for a PRE-EXISTING reason both equalities share. Those rows are counted and printed, never
    * judged here. A resolution or a rendering that THROWS is a third outcome, counted and printed
    * -- never a silent skip. Depth 1 only.
    */
  "processorBase (site 3)" should "parse a processor operand at most once over the ARGUMENT stratum, and leave no parse unread (AC-4)" in {
    val attempts = for {
      (cell, r1) <- exercised if cell.depth == 1
      (res, tried) <- Seq(
        "R1" -> (Success(r1): Try[SingleSearch]),
        "R2" -> Try(r1.update(Some(schema)))
      )
    } yield (cell, res, tried.flatMap(ss => Try(ss -> processorScript(ss))))
    val threw = attempts.collect { case (cell, res, Failure(e)) => s"[$res] ${cell.sql}: $e" }
    val unrendered = attempts.collect { case (cell, res, Success((_, None))) =>
      s"[$res] ${cell.sql}"
    }
    val rows = for {
      (cell, res, Success((ss, Some(script)))) <- attempts
      carrier = carrierOf(ss)
      if stratum(carrier) == "argument"
    } yield (
      cell,
      res,
      receiverResolved(carrier),
      "instanceof String".r.findAllMatchIn(script).size,
      script
    )
    val (inScope, lagged) = rows.partition(_._3)
    info(
      s"argument-stratum rows=${rows.size} judged=${inScope.size} lagged (single update)=" +
      s"${lagged.size} threw=${threw.size} unrendered=${unrendered.size}"
    )
    info(s"unrendered in the Processor context: ${unrendered.take(10)}")
    withClue(s"threw: ${threw.take(5)} ") { threw shouldBe empty }
    withClue(
      s"${inScope.filter(_._4 > 1).take(5).map(r => s"[${r._2}] ${r._1.sql} -> ${r._5}")} "
    ) {
      inScope.filter(_._4 > 1) shouldBe empty
    }
    val dead = inScope.flatMap(r => deadParses(r._5).map(d => s"[${r._2}] ${r._1.sql}: $d"))
    withClue(s"a parse nothing reads: ${dead.take(5)} ") { dead shouldBe empty }
    inScope.count(_._4 == 1) should be > 0 // non-vacuity: something really parses
  }

  it should "recognise a dead parse when it sees one -- the detector is not vacuous (AC-4)" in {
    // The measured shape of the defect: a parse declared first and never read, beside the one read.
    val script =
      "def param1 = (ctx.d instanceof String ? LocalDate.parse(ctx.d) : null); def param2 = " +
      "ctx.d; def param3 = (param2 instanceof String ? LocalDate.parse(param2) : null); " +
      "(param2 == null) ? null : param3.getYear()"
    deadParses(script).map(_.takeWhile(_ != ' ')) shouldBe Seq("param1")
    deadParses(script.replace("param1", "param9") + " + param9") shouldBe empty
  }

  // -- WEEKDAY / DAYOFWEEK: the one copy of a column `update` never resolved ---------------------

  /** `DayOfWeek` inherited `Extract.update = this`, so its receiver kept the spelling it was parsed
    * with at every resolution -- the lemma's only counterexample, and the reason `WEEKDAY(t.d)`
    * read the literal field `t.d`. It now resolves its receiver like every other transform function
    * holding a copy of the column, so the receiver EQUALS the column it reads.
    *
    * RESOLVE-level: this asserts identity, never what Elasticsearch answers. In particular the RUN
    * answer of a SELECT projection over an UNNEST column belongs to the rule that refuses it by
    * name
    * -- it is deliberately not asserted here, and the statements are parsed WITHOUT `validate()`,
    * so the row holds whether or not that rule refuses them. Only the resolutions that REACH the
    * receiver are listed: a table-qualified column after ONE `update()` still holds its dotted
    * copy, because the qualified branch of `GenericIdentifier.update` does not update the chain --
    * a pre-existing lag this story does not touch, so it is not pinned either.
    */
  "WEEKDAY and DAYOFWEEK" should "resolve their receiver to the column they read" in {
    val cases: Seq[(String, String => SingleSearch => SingleSearch, Seq[String])] = Seq(
      ("SELECT %s(t.d) AS c FROM t", _ => r1 => r1.update(Some(schema)), Seq("R2")),
      (
        "SELECT %s(i.d) AS c FROM t JOIN UNNEST(t.items) AS i",
        res => r1 => if (res == "R1") r1 else r1.update(Some(schema)),
        Seq("R1", "R2")
      ),
      (
        "SELECT %s(items.d) AS c FROM t JOIN UNNEST(t.items) AS items",
        res => r1 => if (res == "R1") r1 else r1.update(Some(schema)),
        Seq("R1", "R2")
      ),
      (
        "SELECT o.id, %s(o.createdAt) AS dw FROM orders o JOIN customers c " +
        "ON o.cid = c.id WHERE o.id > 0",
        _ => r1 => r1.updateAll(schemas),
        Seq("R2")
      )
    )
    val checked = for {
      spelling                <- Seq("WEEKDAY", "DAYOFWEEK")
      (template, resolve, rs) <- cases
      res                     <- rs
    } yield {
      val sql = template.format(spelling)
      val ss = resolve(res)(unvalidated(sql))
      val carrier = ss.select.fields
        .map(_.identifier)
        .find(_.functions.exists {
          case _: DayOfWeek => true
          case _            => false
        }) match {
        case Some(c) => c
        case None    => fail(s"[$sql] no DayOfWeek in the SELECT list")
      }
      val receivers = carrier.functions.collect { case dw: DayOfWeek => dw.identifier }
      withClue(s"[$res] $sql carrier=${describe(carrier)} receivers=${receivers.map(describe)} ") {
        receivers should have size 1
        receivers.head.tableAlias shouldBe carrier.tableAlias
        receivers.head shouldBe carrier // identifier equality: same doc access, same correlation
      }
      sql
    }
    checked.distinct.size shouldBe 8
  }

  // -- A percentile's render keeps the correlation and the quoting it was written with -----------

  /** `PercentileAgg.sql` rendered its value column and partitions through `identifierName`, which
    * drops the table qualifier AND the quoting. MEASURED on the baseline: a column that needs
    * quoting rendered SQL the parser rejects, and `PARTITION BY c.city` in a JOIN rendered
    * `PARTITION BY city` -- the MAIN table's column on re-parse. The render fixed point sees the
    * first; the AST oracle sees the second only since identifier equality compares the correlation.
    * Both reach a PERSISTED text: a materialized view stores this render.
    */
  "A percentile's render" should "keep the qualifier, the quoting and the UNNEST alias it was written with" in {
    Seq(
      "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.x) OVER (PARTITION BY t.k) AS p FROM t",
      "SELECT PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY t.x) AS p FROM t",
      "SELECT PERCENTILE_CONT(t.x, 0.5) AS p FROM t",
      "SELECT PERCENTILE_CONT(0.5) OVER (PARTITION BY t.k ORDER BY t.x) AS p FROM t",
      """SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "my col") AS p FROM t""",
      """SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x) OVER (PARTITION BY "order") AS p FROM t""",
      "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY `my col`) AS p FROM t",
      "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.amount) OVER (PARTITION BY c.city) " +
      "AS p FROM orders o JOIN customers c ON o.cid = c.id",
      "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY i.d) AS p FROM t JOIN UNNEST(t.items) AS i"
    ).foreach { sql =>
      val r1 = single(sql)
      withClue(s"[$sql] rendered as [${r1.sql}] ") {
        Parser(r1.sql).map(_.sql) shouldBe Right(r1.sql) // the render fixed point
        Parser(r1.sql) shouldBe Right(r1) // the AST oracle -- it compares the correlation now
      }
    }
  }

  /** CHARACTERIZATION, not a feature pin: `DISTINCT` now reaches the render, which this parser
    * accepts and reads back to the same statement. It means nothing downstream -- the Elasticsearch
    * `percentiles` emission ignores `DISTINCT` (before and after), and DuckDB rejects `WITHIN GROUP
    * (ORDER BY DISTINCT x)` where it used to ignore the dropped `DISTINCT`, so a relational engine
    * fed this render fails loudly on it. Pinned so that change is seen.
    */
  it should "carry DISTINCT into the render (CHARACTERIZATION)" in {
    val r1 = single("SELECT PERCENTILE_CONT(DISTINCT x, 0.5) AS p FROM t")
    r1.sql shouldBe "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY DISTINCT x) AS p FROM t"
    Parser(r1.sql) shouldBe Right(r1)
  }

  it should "leave an unqualified, unquoted percentile rendering exactly as before" in {
    Map(
      "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x) OVER (PARTITION BY k) AS p FROM t" ->
      "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x) OVER (PARTITION BY k) AS p FROM t",
      "SELECT PERCENTILE_CONT(x, 0.5) AS p FROM t" ->
      "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x) AS p FROM t",
      "SELECT PERCENTILE_CONT(0.5) OVER (PARTITION BY k ORDER BY x) AS p FROM t" ->
      "SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x) OVER (PARTITION BY k) AS p FROM t"
    ).foreach { case (sql, rendered) =>
      withClue(s"[$sql] ")(single(sql).sql shouldBe rendered)
    }
  }

  // -- A percentile repeated in HAVING over its own qualified spelling -----------------------------

  private val percentileSchema: SchemaTable = SchemaTable(
    "t",
    columns = List(Column("k", SQLTypes.Keyword), Column("x", SQLTypes.Double))
  )

  private def percentileResolutions(sql: String): Seq[(String, SingleSearch)] = {
    val r1 = single(sql)
    Seq("R1" -> r1, "R2" -> r1.update(Some(percentileSchema)))
  }

  /** A percentile repeated in `HAVING` over the SAME qualified spelling used to be refused as "a
    * different aggregate" under the SELECT alias: the SELECT copy is resolved by the single
    * `update()` of `Parser.apply` and the `HAVING` copy is not, and the old render dropped the
    * qualifier from one of them only. The render now prints what was written, so the two are one
    * aggregate, and the statement reads exactly what `HAVING p > 1` reads (RUN: the testkit's
    * `WindowFunctionSpec`).
    */
  "A percentile repeated in HAVING over its qualified spelling" should "be accepted and read the SELECT item, at both resolutions" in {
    val repeated =
      "SELECT k, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.x) AS p FROM t GROUP BY k " +
      "HAVING PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.x) > 1"
    val byAlias =
      "SELECT k, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY t.x) AS p FROM t GROUP BY k HAVING p > 1"
    percentileResolutions(repeated).zip(percentileResolutions(byAlias)).foreach {
      case ((res, ss), (_, alias)) =>
        withClue(s"[$res] ") {
          ss.validate() shouldBe Right(())
          ss.sqlAggregations shouldBe alias.sqlAggregations
        }
    }
    val join =
      "SELECT c.city, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.amount) AS p " +
      "FROM orders o JOIN customers c ON o.cid = c.id GROUP BY c.city " +
      "HAVING PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY o.amount) > 1"
    resolutions(join).foreach { case (res, ss) =>
      withClue(s"[$res] JOIN ")(ss.validate() shouldBe Right(()))
    }
  }
}
