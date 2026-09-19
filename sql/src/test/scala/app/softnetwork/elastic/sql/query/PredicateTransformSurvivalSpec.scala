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

import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.{SQLTemporal, SQLTypes}
import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType, SQLKeywords}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Issue #367 — a predicate over a function-wrapped column must emit the FUNCTION's value, not the
  * column's.
  *
  * `Expression.painless` had a shortcut: where the operand is registered as a Painless parameter it
  * compared the PARAMETER NAME. That is the whole operand only when the chain folded into the
  * parameter's own declaration as a method (`YEAR(d)` becomes `def param1 = doc['d']…
  * .get(ChronoField.YEAR)`). A chain that renders as an EXPRESSION cannot fold — `WEEKDAY(d)` is
  * arithmetic — so the parameter held the untransformed doc-value and the function VANISHED: `WHERE
  * WEEKDAY(d) = 0` emitted `param1 == null ? false : (param1 == 0)`.
  *
  * 🔴 The population is not a list, it is a PROPERTY, so this spec derives it: every function
  * spelling the grammar accepts (`SQLKeywords.functionWords`) × every operand shape × every
  * right-hand side that typechecks. The issue named six date functions; the census also convicted
  * every `CAST` / `CONVERT` / `TRY_CAST` / `SAFE_CAST` and `ISNULL` / `ISNOTNULL`, which is why the
  * population is generated rather than enumerated.
  *
  * 🔴 TWO detectors, because each is blind where the other sees. A SIGNATURE detector (every method
  * and static the operand's own rendering needs must appear in the predicate) misses `DATE_PARSE`,
  * whose `LocalDate.parse` also appears in the right-hand side's rendering, and `ISNULL`, whose
  * transform contains no call at all. A VERBATIM detector (a non-trivial operand rendering must
  * appear in the predicate literally) catches both. Neither has an allow-list.
  *
  * ⚠️ What this spec cannot see, stated rather than implied: whether Elasticsearch ACCEPTS the
  * emission and AGREES with it. Comparing a `ZonedDateTime` to an `int` is not an error in
  * Painless, it is `false` — the reason the defect returned zero rows with HTTP 200 for four
  * releases. That is settled by `PredicateFunctionResultSpec`, which executes these predicates over
  * a real index on ES 6.8 / 7.17 / 8.18 / 9.0 and asserts the ROW SETS.
  */
class PredicateTransformSurvivalSpec extends AnyFlatSpec with Matchers {

  private val schema: SchemaTable = SchemaTable(
    "t",
    columns = List(
      Column("id", SQLTypes.Keyword),
      Column("d", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("name", SQLTypes.Keyword),
      Column("amount", SQLTypes.Double),
      Column("n", SQLTypes.Int),
      Column("num", SQLTypes.Keyword),
      Column("other", SQLTypes.Keyword)
    )
  )

  /** Operand shapes tried for every function word. A function is never skipped for the shape of its
    * argument list; one that fits none of these is REPORTED, not silently dropped.
    */
  private def operands(f: String): Seq[String] = Seq(
    s"$f(name)",
    s"$f(amount)",
    s"$f(d)",
    s"$f(ts)",
    s"$f(n)",
    s"$f(amount, 2)",
    s"$f(name, 'x')",
    s"$f(d, 'yyyy')",
    s"$f(name, 'yyyy-MM-dd')",
    s"$f(d, MONTH)",
    s"$f(d, ts, DAY)",
    s"$f(amount, n)",
    s"$f(name, 1, 2)",
    s"$f(d, INTERVAL 1 DAY)",
    s"$f(name AS INT)",
    s"$f(name AS DATE)",
    s"$f(name, INT)",
    s"$f(YEAR FROM d)",
    s"$f()"
  )

  /** 🔴 Every right-hand side, not one: a sweep built on `f(x) = 1` TYPE-REJECTS every function
    * that does not return a number, and the filing of this issue counted such a rejection as
    * "clean" — which is how five of the six date functions it was about were reported as
    * unaffected. A type mismatch is NOT a clean result.
    */
  private val rightHandSides: Seq[String] = Seq(
    "= 1",
    "= 1.5",
    "= 'x'",
    "= true",
    "= CAST('2025-01-31' AS DATE)",
    "= CAST('2025-01-31T00:00:00Z' AS TIMESTAMP)",
    // 🔴 A COLUMN on the right, and a FUNCTION on the right — added after review, and not for
    // completeness' sake: with literals only, this corpus could not see `WHERE LAST_DAY(d) = ts`,
    // where the operand is coerced DATE -> TIMESTAMP and `.atStartOfDay(…)` lands on a
    // `ZonedDateTime`. That was a live regression, and every detector here was green on it purely
    // because no row of the corpus had a column as the right-hand side.
    "= d",
    "= ts",
    "= n",
    "= name",
    "> d",
    "> n",
    "= YEAR(ts)",
    "= UPPER(name)",
    "= DATE_FORMAT(ts, 'yyyy')",
    "= LAST_DAY(ts)",
    "= CAST(name AS INT)"
  )

  private val venues: Seq[String => String] = Seq(
    op => s"SELECT id FROM t WHERE $op RHS",
    op => s"SELECT id FROM t WHERE $op IN (RHSV)",
    op => s"SELECT id FROM t WHERE NOT $op RHS",
    op => s"SELECT COUNT(*) AS c FROM t GROUP BY name HAVING $op RHS"
  )

  private def statements: Seq[String] =
    for {
      f     <- SQLKeywords.functionWords.toSeq.sorted
      venue <- venues
      op    <- operands(f)
      rhs   <- rightHandSides
    } yield venue(op).replace("RHSV", rhs.stripPrefix("= ")).replace("RHS", rhs)

  /** Every method call and every static reference a rendering depends on. */
  private def signatures(rendered: String): Set[String] = {
    val methods = """\.([A-Za-z_]\w*)\(""".r.findAllMatchIn(rendered).map(_.group(1)).toSet
    val statics =
      """\b([A-Z]\w*(?:\.[A-Za-z_]\w*)+)\b""".r.findAllMatchIn(rendered).map(_.group(1)).toSet
    methods ++ statics
  }

  private case class Rendered(operand: String, operandBody: String, predicate: String)

  private def expressionOf(sql: String): Option[Expression] =
    Parser(sql) match {
      case Right(ss: SingleSearch) =>
        val updated = ss.update(Some(schema))
        updated.where
          .flatMap(_.criteria)
          .orElse(updated.having.flatMap(_.criteria))
          .collect { case e: Expression if e.identifier.functions.nonEmpty => e }
      case _ => None
    }

  /** 🔴 An AGGREGATE predicate is out of this spec's population BY DESIGN, not by accident: it is
    * rendered context-free as a bucket-pipeline read of the metric Elasticsearch already computed
    * (`MetricSelectorScript.metricSelector` calls `painless(None)`, which returns from
    * `bucketPipelinePainless` before any operand is coerced). It is counted and reported separately
    * so "not exercised" keeps meaning "this spec never reached it".
    */
  private def criteriaOf(sql: String): Option[Expression] =
    expressionOf(sql).filterNot(_.identifier.isAggregation)

  /** The operand's own rendering and the predicate's, with their script prologues.
    *
    * 🔴 Each comes from its OWN parse of the same SQL, and that is not fussiness: a rendering
    * MUTATES the tree it renders (`PainlessParam.addPainlessMethod`) and the context it renders
    * into, so asking one AST for both answers changes the second one — measured here as a
    * `TRY_CAST` binding `safe2` for the predicate after the operand had taken `safe1`, and as
    * `DATE_ADD` emitting its methods in the other order. Two parses share no mutable state.
    */
  private def rendered(sql: String): Option[Rendered] =
    for {
      operandExpr   <- criteriaOf(sql)
      predicateExpr <- criteriaOf(sql)
    } yield {
      val operandContext = PainlessContext(context = PainlessContextType.Query)
      val operandBody = operandExpr.identifier.painless(Some(operandContext))
      val predicateContext = PainlessContext(context = PainlessContextType.Query)
      val predicateBody = predicateExpr.painless(Some(predicateContext))
      Rendered(
        operand = operandContext.toString + operandBody,
        operandBody = operandBody,
        predicate = predicateContext.toString + predicateBody
      )
    }

  /** The bare-NAME case is exempt from the VERBATIM rule and only from that one: where the chain
    * folded into a declaration — a parameter whose method list carries it (`YEAR`), or a local the
    * safe cast hoisted (`TRY_CAST`) — the operand's rendering IS that name and the transform lives
    * in the prologue, which the signature rule still checks. Anything carrying an operator is not a
    * name and gets no exemption.
    */
  private def isBoundName(body: String): Boolean = body.matches("[A-Za-z_]\\w*")

  "no predicate over a function-wrapped column" should "lose the function's transform" in {
    var exercised = 0
    val aggregateOnly = collection.mutable.Set.empty[String]
    val notExercised = collection.mutable.Set.empty[String]
    val offenders = collection.mutable.ArrayBuffer.empty[String]

    SQLKeywords.functionWords.toSeq.sorted.foreach { f =>
      var any = false
      var aggregate = false
      for {
        venue <- venues
        op    <- operands(f)
        rhs   <- rightHandSides
      } {
        val sql = venue(op).replace("RHSV", rhs.stripPrefix("= ")).replace("RHS", rhs)
        if (expressionOf(sql).exists(_.identifier.isAggregation)) aggregate = true
        rendered(sql).foreach { r =>
          any = true
          exercised += 1
          val missing = signatures(r.operand).diff(signatures(r.predicate))
          if (missing.nonEmpty)
            offenders += s"$sql\n    operand  : ${r.operand}\n    predicate: ${r.predicate}\n    missing  : ${missing.toSeq.sorted
              .mkString(", ")}"
          else {
            val body = r.operandBody
            if (!isBoundName(body) && !r.predicate.contains(body))
              offenders += s"$sql\n    operand  : ${r.operand}\n    predicate: ${r.predicate}\n    missing  : the operand rendering itself"
          }
        }
      }
      if (!any) { if (aggregate) aggregateOnly += f else notExercised += f }
    }

    // Three outcomes, and the third is printed with its count: a population a probe never reached is
    // not a population it vouched for.
    info(
      s"exercised $exercised predicates over ${SQLKeywords.functionWords.size} function spellings"
    )
    info(
      s"aggregate only, rendered context-free (${aggregateOnly.size}): " +
      aggregateOnly.toSeq.sorted.mkString(", ")
    )
    info(s"not exercised (${notExercised.size}): ${notExercised.toSeq.sorted.mkString(", ")}")

    withClue(
      s"${offenders.size} predicate(s) emit an operand that is not the function's value:\n" +
      offenders.take(12).mkString("\n")
    ) { offenders shouldBe empty }

    // 🔴 And the cheapest structural fact of all, which nothing asserted: the emission PARSES as far
    // as delimiters go. `COALESCE(d) = 'x'` emitted `String.valueOf( : param1))` — a paren that closed
    // nothing — because a one-argument COALESCE folded over zero arguments. Degenerate SQL, legal SQL,
    // and unreachable until this issue stopped predicates from dropping the transform.
    val unbalanced = statements.flatMap(sql => rendered(sql).map(sql -> _.predicate)).collect {
      case (sql, emitted)
          if emitted.count(_ == '(') != emitted.count(_ == ')') ||
            emitted.count(_ == '[') != emitted.count(_ == ']') =>
        s"$sql\n    => $emitted"
    }
    withClue(
      s"${unbalanced.size} predicate(s) emit unbalanced delimiters:\n" +
      unbalanced.take(8).mkString("\n")
    ) { unbalanced shouldBe empty }

    // A silent empty walk must not pass. The counts are floors, not equalities: adding a function
    // or an operand shape may only raise them.
    exercised should be > 4000
    // The ones this spec genuinely cannot reach: the window functions, the two percentile forms, the
    // geo constructor and the two no-argument time functions. Anything more is a population that
    // quietly stopped being tested.
    withClue(s"not exercised: ${notExercised.toSeq.sorted.mkString(", ")}") {
      notExercised.size should be <= 8
    }
  }

  /** 🔴 A SECOND mechanism, because the two detectors above are structurally blind to the other way
    * a predicate can be wrong: they see a transform that is MISSING, never one that was ADDED. A
    * spurious conversion is an addition, and review found exactly that — `WHERE LAST_DAY(d) = ts`
    * emitted `.atStartOfDay(…)` on a `ZonedDateTime`, which is declared on `LocalDate` alone, so a
    * query that WORKED before this fix answered `dynamic method [java.time.ZonedDateTime,
    * atStartOfDay/1] not found` on ES 8.18. The census above was green with that live.
    *
    * So this asks the JDK, which is the only authority on it (#368's rule): starting from the
    * receiver the PROLOGUE declares — a `date` doc-value is a `ZonedDateTime`, unless a narrowing
    * `.toLocalDate()` / `.toLocalTime()` was injected, which the declaration shows — walk the
    * method CHAIN and re-type it at every step from the method's own return type. A method that
    * does not exist on the receiver it is applied to is the defect, and walking is what sees it:
    * `atStartOfDay` is applied to `param1.with(…)`, not to `param1`, so a check that only looked at
    * bare receivers passed with B-1 live (measured — that version of this test was green under the
    * mutation).
    *
    * ⚠️ SCOPED to a chain whose OUTPUT is temporal, the family whose Java type its SQL type can
    * disagree about. This is not a general type-checker: `CHARACTER_LENGTH(d)` emits `.length()` on
    * a `ZonedDateTime` — nonsense SQL the GRAMMAR accepts, 128 shapes of it in this corpus, a
    * validation gap that predates this issue and is none of its business.
    */
  "every method a predicate emits" should "exist on the receiver the pipeline hands it" in {
    val zoned = classOf[java.time.ZonedDateTime]
    val localDate = classOf[java.time.LocalDate]
    val localTime = classOf[java.time.LocalTime]

    /** name -> its declaration body, from the script prologue. */
    def declarations(emitted: String): Seq[(String, String)] =
      """def (\w+) = ([^;]*);""".r.findAllMatchIn(emitted).map(m => m.group(1) -> m.group(2)).toSeq

    /** What Elasticsearch hands Painless for each column of the fixture schema: a `date` doc-value
      * is a `ZonedDateTime`, a `keyword` a `String`, a numeric a boxed number. Reading the COLUMN
      * is the point — inferring "any `doc[...]` is temporal" made this check accuse
      * `param.toUpperCase()` on a keyword column, 69 times.
      */
    val columnReceiver: Map[String, Class[_]] = Map(
      "d"      -> zoned,
      "ts"     -> zoned,
      "id"     -> classOf[String],
      "name"   -> classOf[String],
      "amount" -> classOf[java.lang.Double],
      "n"      -> classOf[java.lang.Long]
    )

    val columnRead = """doc\['(\w+)'\]""".r

    /** The receiver a declaration establishes, when it can be read off it with certainty. */
    def receiverOf(declaration: String): Option[Class[_]] = {
      val column = columnRead.findFirstMatchIn(declaration).map(_.group(1))
      if (column.isEmpty) None
      else if (!column.exists(columnReceiver.contains)) None // not a column of the fixture schema
      else if (declaration.endsWith(".toLocalDate()")) Some(localDate)
      else if (declaration.endsWith(".toLocalTime()")) Some(localTime)
      else if (declaration.contains(".toLocalDate()") || declaration.contains(".toLocalTime()"))
        None // a narrowing somewhere in the middle: too weak a reading to assert on
      else column.flatMap(columnReceiver.get)
    }

    /** The method names applied, in order, to the occurrence of `name` starting at `from`. */
    def chainAt(text: String, from: Int): Seq[String] = {
      val methods = collection.mutable.ArrayBuffer.empty[String]
      var i = from
      var continue = true
      while (continue && i < text.length && text.charAt(i) == '.') {
        val nameEnd = text.indexOf('(', i)
        if (nameEnd < 0) continue = false
        else {
          val method = text.substring(i + 1, nameEnd)
          if (!method.matches("[A-Za-z_]\\w*")) continue = false
          else {
            methods += method
            // skip the argument list, honouring nesting and string literals
            var depth = 0
            var j = nameEnd
            var inString = false
            var closed = -1
            while (closed < 0 && j < text.length) {
              val c = text.charAt(j)
              if (inString) { if (c == '"') inString = false }
              else
                c match {
                  case '"' => inString = true
                  case '(' => depth += 1
                  case ')' => depth -= 1; if (depth == 0) closed = j
                  case _   =>
                }
              j += 1
            }
            if (closed < 0) continue = false else i = closed + 1
          }
        }
      }
      methods.toSeq
    }

    var checked = 0
    var untyped = 0
    var outOfScope = 0
    val offenders = collection.mutable.ArrayBuffer.empty[String]

    var temporalPredicates = 0
    statements.foreach { sql =>
      // The population is a temporal chain over a TEMPORAL COLUMN — where the chain's SQL type and the
      // Java type of the doc-value can disagree. `LAST_DAY(name)` over a keyword is nonsense SQL the
      // GRAMMAR accepts (180 shapes of it here, emitting `String.with(TemporalAdjusters…)`), and
      // `CHARACTER_LENGTH(d)` is its mirror image: both are a validation gap that predates this issue
      // and neither is its business.
      if (
        !criteriaOf(sql).exists(e =>
          e.identifier.chainType.isInstanceOf[SQLTemporal] &&
          e.identifier.baseType.isInstanceOf[SQLTemporal]
        )
      ) outOfScope += 1
      else {
        temporalPredicates += 1
        rendered(sql).foreach { r =>
          declarations(r.predicate).foreach { case (name, declaration) =>
            receiverOf(declaration) match {
              case None       => untyped += 1
              case Some(root) =>
                // every place this name is USED, in the rest of the script
                val uses = s"""\\b$name(?=\\.)""".r.findAllMatchIn(r.predicate).map(_.end).toSeq
                uses.foreach { at =>
                  var receiver: Option[Class[_]] = Some(root)
                  chainAt(r.predicate, at).foreach { method =>
                    receiver.foreach { current =>
                      // 🔴 ALL overloads, not the first: `getMethods` also returns the BRIDGE methods
                      // (`ZonedDateTime.with(TemporalAdjuster)` is declared three times, returning
                      // `Temporal` and `ChronoZonedDateTime` as well), and picking one of those made
                      // the return type unknown, stopped the walk, and let the very defect this test
                      // exists for pass — measured, by restoring it.
                      val overloads = current.getMethods.filter(_.getName == method)
                      if (overloads.isEmpty) {
                        offenders +=
                          s"$sql\n    $name is a ${current.getSimpleName}, which has no " +
                          s"$method: ${r.predicate}"
                        receiver = None
                      } else {
                        checked += 1
                        receiver = overloads
                          .flatMap(m => Seq(zoned, localDate, localTime).find(_ == m.getReturnType))
                          .headOption
                      }
                    }
                  }
                }
            }
          }
        }
      }
    }

    info(
      s"$temporalPredicates predicates with a temporal chain output; walked $checked method calls " +
      s"from a typed receiver; $untyped declarations this spec cannot type; $outOfScope inputs " +
      s"either rejected by the grammar or not temporal"
    )
    withClue(
      s"${offenders.size} predicate(s) call a method the receiver does not have:\n" +
      offenders.distinct.take(8).mkString("\n")
    ) { offenders shouldBe empty }
    // Floors, so a silent empty walk cannot pass. 270 temporal predicates and 42 typed chain calls
    // today; both may only grow.
    temporalPredicates should be > 100
    checked should be > 20
  }

  // -- byte pins on the shapes the issue names ----------------------------------------------------

  private def predicateOf(sql: String): String =
    rendered(sql).map(_.predicate).getOrElse(fail(s"[$sql] has no function-bearing criteria"))

  "WEEKDAY in a predicate" should "compare the extracted weekday" in {
    predicateOf("SELECT id FROM t WHERE WEEKDAY(d) = 0") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value.toInstant().atZone(ZoneId.of('Z'))); " +
    "def left1 = (param1 == null) ? null : (param1.get(ChronoField.DAY_OF_WEEK) + 6) % 7; " +
    "(left1 == null ? false : ((left1 == 0)))"
  }

  "DATE_FORMAT in a predicate" should "compare the formatted value" in {
    predicateOf("SELECT id FROM t WHERE DATE_FORMAT(d, 'yyyy') = '2025'") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param2 = DateTimeFormatter.ofPattern(\"yyyy\"); " +
    "def left1 = (param1 == null) ? null : param2.format(param1); " +
    "(left1 == null ? false : ((left1.compareTo(\"2025\") == 0)))"
  }

  "LAST_DAY in a predicate" should "compare the adjusted date" in {
    predicateOf("SELECT id FROM t WHERE LAST_DAY(d) = CAST('2025-01-31' AS DATE)") should include(
      "def left1 = (param1 == null) ? null : param1.with(TemporalAdjusters.lastDayOfMonth());"
    )
  }

  "a CAST in a predicate" should "compare the converted value, with no numeric widening" in {
    // 🔴 The widening is what must NOT be here: it renders as the PRIMITIVE cast `((long) …)` over a
    // null-guarded operand, which Painless types as `Object` — measured as `script_exception:
    // compile error` on ES 8.18. Painless compares boxed numbers numerically without it.
    val emitted = predicateOf("SELECT id FROM t WHERE CAST(name AS INT) = 1")
    emitted shouldBe
    "def param1 = (doc['name'].size() == 0 ? null : doc['name'].value); " +
    "def left1 = (param1 != null ? (def)(Integer.parseInt(param1).intValue()) : null); " +
    "(left1 == null ? false : ((left1 == 1)))"
    emitted should not include "(long)"
  }

  /** Also a mutation-verified pin: it reddens both if the safe cast stops being hoisted (`def left1
    * = try { … };`, which Elasticsearch refuses at compile time) and if the operand is rendered
    * twice (a second `safe` local, with the predicate reading the wrong one).
    */
  "a TRY_CAST in a predicate" should "hoist its try/catch into the prologue" in {
    // Painless has no expression-level try/catch, so the safe cast is a STATEMENT: valid as the tail
    // of a whole script (a projection), never as an operand. Hoisted, the operand is a name.
    val emitted = predicateOf("SELECT id FROM t WHERE TRY_CAST(name AS INT) = 1")
    emitted shouldBe
    "def param1 = (doc['name'].size() == 0 ? null : doc['name'].value); " +
    "def safe1 = null; try { safe1 = (param1 != null ? (def)(Integer.parseInt(param1).intValue()) : null); } catch (Exception e) {} " +
    "def left2 = safe1; (left2 == null ? false : ((left2 == 1)))"
    emitted should not include "def left1 = try"
  }

  "ISNULL in a predicate" should "compare the null test, not the value" in {
    predicateOf("SELECT id FROM t WHERE ISNULL(name) = true") shouldBe
    "def param1 = (doc['name'].size() == 0 ? null : doc['name'].value); " +
    "def left1 = param1 == null; (left1 == null ? false : ((left1 == true)))"
  }

  /** 🔴 B-1, found by review and MEASURED on ES 8.18 as a REGRESSION of an earlier draft of this
    * fix: with a date COLUMN on the right the operand was coerced DATE -> TIMESTAMP, which renders
    * `.atStartOfDay(…)` — a `LocalDate` method — onto a `ZonedDateTime`, and the query went from
    * working to `dynamic method [java.time.ZonedDateTime, atStartOfDay/1] not found`. The rule that
    * fixes it: a temporal chain over a temporal column keeps the COLUMN's Java type.
    */
  "a temporal function compared with a date column" should "not be coerced at all" in {
    val emitted = predicateOf("SELECT id FROM t WHERE LAST_DAY(d) = ts")
    emitted shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value); " +
    "def param2 = (doc['ts'].size() == 0 ? null : doc['ts'].value); " +
    "def left1 = (param1 == null) ? null : param1.with(TemporalAdjusters.lastDayOfMonth()); " +
    "(left1 == null || param2 == null ? false : ((left1.isEqual(param2))))"
    emitted should not include "atStartOfDay"
  }

  /** 🔴 `List.contains` is Java `equals`, which is FALSE across boxed numeric types. MEASURED on ES
    * 8.18 over a keyword column holding "6"/"9"/"12": `[9,12].contains(<Integer>)` matched, while
    * `<Long>` and `<Double>` both returned ZERO rows — and `== 9` matched in all three. The
    * bucket-pipeline form of IN already knew this (`bucketPipelineCheck` renders `==` for the same
    * measured reason); the document form did not, and this fix is what lets these operands reach a
    * document at all.
    */
  "IN over a numeric operand" should "compare with == rather than List.contains" in {
    val emitted = predicateOf("SELECT id FROM t WHERE CAST(num AS BIGINT) IN (9, 12)")
    emitted shouldBe
    "def param1 = (doc['num'].size() == 0 ? null : doc['num'].value); " +
    "def left1 = (param1 != null ? (def)(Long.parseLong(param1).longValue()) : null); " +
    "(left1 == null ? false : (((left1 == 9 || left1 == 12))))"
    emitted should not include ".contains("
  }

  it should "keep List.contains for a list of strings, where equals is what IN means" in {
    predicateOf("SELECT id FROM t WHERE UPPER(name) IN ('A','B')") should include(
      """["A","B"].contains(left1)"""
    )
  }

  "a one-argument COALESCE" should "render as its argument" in {
    // It folded over zero arguments and emitted `String.valueOf( : param1))`.
    val emitted = predicateOf("SELECT id FROM t WHERE COALESCE(d) = 'x'")
    emitted should include("def left1 = String.valueOf(param1);")
    emitted.count(_ == '(') shouldBe emitted.count(_ == ')')
  }

  /** 🔴 The SECOND HALF of the same lie, on the RIGHT operand. `out` on a function-wrapped
    * identifier is its COLUMN's type, and `check` picks the comparison spelling from it, so a
    * function on both sides chose the spelling of the wrong type. MEASURED on ES 8.18 before this
    * half landed: `YEAR(d) = YEAR(ts)` gave `dynamic method [java.lang.Integer, isEqual/1] not
    * found`, `DATE_FORMAT(d,'yyyy') = DATE_FORMAT(ts,'yyyy')` the same over `String`, and `CAST(num
    * AS INT) = CAST(other AS INT)` stringified the left and answered ZERO rows for documents that
    * match. All five shapes below are now executed on real Elasticsearch by
    * `PredicateFunctionResultSpec`.
    */
  "a function on BOTH sides" should "compare in the spelling of the chain's type" in {
    predicateOf("SELECT id FROM t WHERE YEAR(d) = YEAR(ts)") should endWith(
      "(left1 == null || param2 == null ? false : ((left1 == param2)))"
    )
    predicateOf("SELECT id FROM t WHERE YEAR(d) > YEAR(ts)") should endWith("((left1 > param2)))")

    predicateOf("SELECT id FROM t WHERE DATE_FORMAT(d, 'yyyy') = DATE_FORMAT(ts, 'yyyy')") should
    endWith("((left1.compareTo(right2) == 0)))")

    // No `String.valueOf` on either side: the common supertype of INT and the right-hand COLUMN's
    // KEYWORD is a string, which is what used to stringify the left and lose every match.
    val casts = predicateOf("SELECT id FROM t WHERE CAST(num AS INT) = CAST(other AS INT)")
    casts should not include "String.valueOf"
    casts should include("Integer.parseInt(param1)")
    casts should include("Integer.parseInt(param2)")

    // 🔴 And the narrowing must fire on BOTH sides or NEITHER: injected on the left alone it made the
    // sides different Java types — `class_cast_exception: Cannot cast java.time.ZonedDateTime to
    // java.time.chrono.ChronoLocalDate` (MEASURED). A document field on the right means neither.
    val temporal = predicateOf("SELECT id FROM t WHERE LAST_DAY(d) = LAST_DAY(ts)")
    temporal should not include ".toLocalDate()"
    temporal should endWith("((left1.isEqual(right2))))")
  }

  it should "keep narrowing against a DATE LITERAL, where the literal IS a LocalDate" in {
    predicateOf("SELECT id FROM t WHERE LAST_DAY(d) = CAST('2025-01-31' AS DATE)") should startWith(
      "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value.toLocalDate());"
    )
  }

  // -- the shapes that must NOT move --------------------------------------------------------------

  /** 🔴 The parameter shortcut is KEPT, and these are the emissions that prove it: a chain that
    * folds into its parameter still renders through it, byte-for-byte as before the fix. Widening
    * the fix to "always bind a local" would have moved every one of them.
    */
  "a function that folds into its parameter" should "still take the parameter shortcut" in {
    predicateOf("SELECT id FROM t WHERE YEAR(d) = 2025") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value.toInstant().atZone(ZoneId.of('Z')).get(ChronoField.YEAR)); " +
    "param1 == null ? false : (param1 == 2025)"

    predicateOf("SELECT id FROM t WHERE EPOCHDAY(d) = 20129") shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value.toInstant().atZone(ZoneId.of('Z')).getLong(ChronoField.EPOCH_DAY)); " +
    "param1 == null ? false : (param1 == 20129)"

    predicateOf("SELECT id FROM t WHERE DATE_TRUNC(d, MONTH) = CAST('2025-01-01' AS DATE)") should
    startWith(
      "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value.toLocalDate().toInstant()" +
      ".atZone(ZoneId.of('Z')).withDayOfMonth(1).truncatedTo(ChronoUnit.DAYS));"
    )
  }

  /** 🔴 An INTERVAL must land on the operand only, and this is a REGRESSION pin with a measured
    * cause: RE-RENDERING the operand after the right-hand side has registered its own parameter
    * re-resolves onto that parameter. An intermediate version of this fix did exactly that and
    * appended `.plus(1, ChronoUnit.DAYS)` to the LITERAL's declaration as well — a day added to
    * BOTH sides of the comparison, which is a wrong answer, not an error.
    *
    * Verified by MUTATION, not by hope: adding a second `identifier.painless(context)` below
    * `innerRight` in `Expression.painless` reddens this test and the `TRY_CAST` pin below (which
    * gains a second, dead `safe` local). Neither is caught by the census above — it sees a
    * transform that is MISSING, not one applied twice — which is why both pins are spelled out.
    */
  "an INTERVAL in a predicate" should "be applied to the operand and not to the literal" in {
    val emitted = predicateOf(
      "SELECT id FROM t WHERE DATE_ADD(d, INTERVAL 1 DAY) = CAST('2025-01-31' AS DATE)"
    )
    emitted shouldBe
    "def param1 = (doc['d'].size() == 0 ? null : doc['d'].value.toLocalDate().plus(1, ChronoUnit.DAYS)); " +
    "def param2 = LocalDate.parse((\"2025-01-31\").replace(\"/\", \"-\"), DateTimeFormatter.ofPattern(\"yyyy-MM-dd\")); " +
    "param1 == null ? false : (param1.isEqual(param2))"
    """\.plus\(1, ChronoUnit\.DAYS\)""".r.findAllMatchIn(emitted).size shouldBe 1
  }

  /** A plain column carries no chain, so nothing about it may change — including the absence of the
    * numeric widening the operand path skips.
    */
  "a plain column predicate" should "be untouched" in {
    Parser("SELECT id FROM t WHERE n > 1") match {
      case Right(ss: SingleSearch) =>
        val ctx = PainlessContext(context = PainlessContextType.Query)
        val criteria = ss.update(Some(schema)).where.flatMap(_.criteria).getOrElse(fail("no WHERE"))
        val body = criteria.painless(Some(ctx))
        s"$ctx$body" shouldBe
        "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); param1 == null ? false : (param1 > 1)"
      case other => fail(s"unexpected $other")
    }
  }
}
