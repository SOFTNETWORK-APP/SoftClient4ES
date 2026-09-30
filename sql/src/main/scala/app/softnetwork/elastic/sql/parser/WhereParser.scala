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

package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.function.geo.Meters
import app.softnetwork.elastic.sql.{
  DoubleFromTo,
  DoubleValues,
  GeoDistance,
  GeoDistanceFromTo,
  Identifier,
  IdentifierFromTo,
  LiteralFromTo,
  LongFromTo,
  LongValue,
  LongValues,
  StringValues,
  Token
}
import app.softnetwork.elastic.sql.operator.{
  AGAINST,
  ALL,
  AND,
  ANY,
  BETWEEN,
  Child,
  ComparisonOperator,
  DIFF,
  EQ,
  EXISTS,
  ExpressionOperator,
  GE,
  GT,
  IN,
  IS_NOT_NULL,
  IS_NULL,
  LE,
  LIKE,
  LT,
  MATCH,
  NE,
  NOT,
  Nested,
  OR,
  Parent,
  PredicateOperator,
  Quantifier,
  RLIKE,
  SOME
}
import app.softnetwork.elastic.sql.query.{
  BetweenExpr,
  ConditionalFunctionAsCriteria,
  Criteria,
  DistanceCriteria,
  DqlStatement,
  ElasticChild,
  ElasticNested,
  ElasticParent,
  ElasticRelation,
  ExistsSubquery,
  GenericExpression,
  InExpr,
  InSubquery,
  IsNotNullExpr,
  IsNullExpr,
  MultiMatchCriteria,
  Predicate,
  QuantifiedSubquery,
  ScalarSubquery,
  Where
}

/** One element of a condition once the scanner's tokens are taken apart (`WhereParser.flatten`): an
  * operand, or the operator joining two operands together with the `NOT` the scanner attached to
  * the operand AFTER it. Top-level, not inside the trait: an inner case class of a trait cannot be
  * type-tested without an unchecked outer reference.
  */
private[parser] sealed trait ChainItem
private[parser] final case class ChainOperand(criteria: Criteria) extends ChainItem
private[parser] final case class ChainJunction(operator: PredicateOperator, negatesNext: Boolean)
    extends ChainItem

trait WhereParser {
  self: Parser with GroupByParser with OrderByParser =>

  lazy val isNull: PackratParser[Criteria] = (quotedIdentifier | identifier) ~ IS_NULL.regex ^^ {
    case i ~ _ =>
      IsNullExpr(i)
  }

  lazy val isNotNull: PackratParser[Criteria] =
    (quotedIdentifier | identifier) ~ IS_NOT_NULL.regex ^^ { case i ~ _ =>
      IsNotNullExpr(i)
    }

  lazy val eq: PackratParser[ComparisonOperator] = EQ.sql ^^ (_ => EQ)

  lazy val ne: PackratParser[ComparisonOperator] = NE.sql ^^ (_ => NE)

  lazy val diff: PackratParser[ComparisonOperator] = DIFF.sql ^^ (_ => DIFF)

  /** 🔴 `lazy val`, NOT `def` — and it is a PERFORMANCE contract, not a style choice.
    *
    * `PackratParsers` memoises on *(parser INSTANCE, position)*: `recall(p, in)` and
    * `updateCacheAndGet(p, …)` key the cache on `p` itself (scala-parser-combinators 1.1.2,
    * `PackratParsers.scala:237-289`), and the library's own scaladoc states the rule outright —
    * *"each grammar production previously declared as a `def` without formal parameters becomes a
    * `lazy val`"* (`:34-38`). A `def` builds a FRESH instance on every reference, so each
    * alternative of [[criteria]] that begins with `any_identifier` got its OWN memo entry at the
    * same position and re-did the identical 9-way alternation from scratch.
    *
    * MEASURED (`ParserSpec`, 10 timed runs, median): **3.490 s -> 1.228 s (-64 %) on `main` with
    * this one word changed**, and 4.927 s -> 1.240 s on this branch. The cost was therefore
    * GRAMMAR-WIDE and pre-existing — every statement the engine has ever parsed paid it — not
    * something story 22.2's four new alternatives created; what those alternatives did was make it
    * visible, because they multiplied the number of times the same `any_identifier` was re-parsed.
    *
    * With memoisation actually firing, the four extra alternatives cost +0.95 % (1.228 -> 1.240,
    * ranges fully overlapping): below this suite's measurement resolution.
    */
  private lazy val any_identifier: PackratParser[Identifier] =
    // #284 - see quotedIdentifierUnlessArithmetic.
    quotedIdentifierUnlessArithmetic |
    identifierWithArithmeticExpression |
    identifierWithTransformation |
    identifierWithWindowFunction |
    identifierWithAggregation |
    identifierWithIntervalFunction |
    identifierWithFunction |
    identifierWithValue |
    identifier

  private lazy val equality: PackratParser[GenericExpression] =
    not.? ~ any_identifier ~ (eq | ne | diff) ~ (boolean | quotedQualifiedIdentifier | literal | double | pi | geo_distance | long | any_identifier) ^^ {
      case n ~ i ~ o ~ v => GenericExpression(i, o, v, n)
    }

  lazy val like: PackratParser[GenericExpression] =
    any_identifier ~ not.? ~ LIKE.regex ~ literal ^^ { case i ~ n ~ _ ~ v =>
      GenericExpression(i, LIKE, v, n)
    }

  lazy val rlike: PackratParser[GenericExpression] =
    any_identifier ~ not.? ~ RLIKE.regex ~ literal ^^ { case i ~ n ~ _ ~ v =>
      GenericExpression(i, RLIKE, v, n)
    }

  lazy val ge: PackratParser[ComparisonOperator] = GE.sql ^^ (_ => GE)

  lazy val gt: PackratParser[ComparisonOperator] = GT.sql ^^ (_ => GT)

  lazy val le: PackratParser[ComparisonOperator] = LE.sql ^^ (_ => LE)

  lazy val lt: PackratParser[ComparisonOperator] = LT.sql ^^ (_ => LT)

  private lazy val comparison: PackratParser[GenericExpression] =
    not.? ~ any_identifier ~ (ge | gt | le | lt) ~ (double | pi | random | geo_distance | long | quotedQualifiedIdentifier | literal | any_identifier) ^^ {
      case n ~ i ~ o ~ v => GenericExpression(i, o, v, n)
    }

  lazy val in: PackratParser[ExpressionOperator] = IN.regex ^^ (_ => IN)

  private lazy val inLiteral: PackratParser[Criteria] =
    any_identifier ~ not.? ~ in ~ start ~ rep1sep(literal, separator) ~ end ^^ {
      case i ~ n ~ _ ~ _ ~ v ~ _ =>
        InExpr(
          i,
          StringValues(v),
          n
        )
    }

  private lazy val inDoubles: PackratParser[Criteria] =
    any_identifier ~ not.? ~ in ~ start ~ rep1sep(
      double,
      separator
    ) ~ end ^^ { case i ~ n ~ _ ~ _ ~ v ~ _ =>
      InExpr(
        i,
        DoubleValues(v),
        n
      )
    }

  private lazy val inLongs: PackratParser[Criteria] =
    any_identifier ~ not.? ~ in ~ start ~ rep1sep(
      long,
      separator
    ) ~ end ^^ { case i ~ n ~ _ ~ _ ~ v ~ _ =>
      InExpr(
        i,
        LongValues(v),
        n
      )
    }

  lazy val between: PackratParser[Criteria] =
    any_identifier ~ not.? ~ BETWEEN.regex ~ literal ~ and ~ literal ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => BetweenExpr(i, LiteralFromTo(from, to), n)
    }

  lazy val betweenLongs: PackratParser[Criteria] =
    any_identifier ~ not.? ~ BETWEEN.regex ~ long ~ and ~ long ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => BetweenExpr(i, LongFromTo(from, to), n)
    }

  lazy val betweenDoubles: PackratParser[Criteria] =
    any_identifier ~ not.? ~ BETWEEN.regex ~ double ~ and ~ double ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => BetweenExpr(i, DoubleFromTo(from, to), n)
    }

  lazy val betweenIdentifiers: PackratParser[Criteria] =
    any_identifier ~ not.? ~ BETWEEN.regex ~ any_identifier ~ and ~ any_identifier ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => BetweenExpr(i, IdentifierFromTo(from, to), n)
    }

  lazy val betweenDistances: PackratParser[Criteria] =
    distance_identifier ~ not.? ~ BETWEEN.regex ~ (geo_distance | long) ~ and ~ (geo_distance | long) ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to =>
        BetweenExpr(
          i,
          GeoDistanceFromTo(
            from match {
              case gd: GeoDistance => gd
              case l: LongValue    => GeoDistance(l, Meters)
            },
            to match {
              case gd: GeoDistance => gd
              case l: LongValue    => GeoDistance(l, Meters)
            }
          ),
          n
        )
    }

  /*def distanceCriteria: PackratParser[Criteria] =
    distance ~ (ge | gt | le | lt) ~ geo_distance ^^ { case d ~ o ~ g =>
      DistanceCriteria(d, o, g)
    }*/

  lazy val matchCriteria: PackratParser[MultiMatchCriteria] =
    MATCH.regex ~ start ~ rep1sep(
      any_identifier,
      separator
    ) ~ end ~ AGAINST.regex ~ start ~ literal ~ end ^^ { case _ ~ _ ~ i ~ _ ~ _ ~ _ ~ l ~ _ =>
      MultiMatchCriteria(i, l)
    }

  lazy val and: PackratParser[PredicateOperator] = AND.regex ^^ (_ => AND)

  lazy val or: PackratParser[PredicateOperator] = OR.regex ^^ (_ => OR)

  lazy val not: PackratParser[NOT.type] = NOT.regex ^^ (_ => NOT)

  lazy val logical_criteria: PackratParser[Criteria] =
    (is_null | is_notnull) ^^ { case ConditionalFunctionAsCriteria(c) =>
      c
    }

  /** Story 22.2 — the parenthesised body of a WHERE subquery.
    *
    * It is 22.1's `derivedTableBodyInner` (`searchStatement | fromlessSelect`, in that order, for
    * the same `|`-commit reason) with its OWN parentheses. 🔴 NOT `derivedTable`, whose `err("A
    * derived table body must be a SELECT …")` alternative would fire on `WHERE a = (b + 1)` — an
    * `Error` does not backtrack, so the alternation below could never fall back to `equality` and a
    * statement that parses today (MEASURED at Task 0: `WHERE a = (b + 1)` is `Right`) would be
    * rejected. With `derivedTableBodyInner` the failure inside the parentheses is a plain `Failure`
    * and the fall-through keeps the parenthesised-expression reading. Pinned by the neighbour test.
    */
  private lazy val subqueryBody: PackratParser[DqlStatement] = start ~> derivedTableBodyInner <~ end

  private lazy val comparisonOp: PackratParser[ComparisonOperator] =
    eq | ne | diff | ge | gt | le | lt

  /** `SOME` is canonicalised to `ANY` here (ANSI synonyms), so the AST carries one spelling and `x
    * > SOME (S)` renders — and re-parses — as `x > ANY (S)`.
    */
  private lazy val quantifier: PackratParser[Quantifier] =
    ANY.regex ^^ (_ => ANY) | SOME.regex ^^ (_ => ANY) | ALL.regex ^^ (_ => ALL)

  private lazy val existsSubquery: PackratParser[Criteria] =
    not.? ~ (EXISTS.regex ~> subqueryBody) ^^ { case n ~ q => ExistsSubquery(q, n) }

  private lazy val inSubquery: PackratParser[Criteria] =
    any_identifier ~ not.? ~ in ~ subqueryBody ^^ { case i ~ n ~ _ ~ q => InSubquery(i, q, n) }

  private lazy val scalarSubquery: PackratParser[Criteria] =
    not.? ~ any_identifier ~ comparisonOp ~ subqueryBody ^^ { case n ~ i ~ o ~ q =>
      ScalarSubquery(i, o, q, n)
    }

  /** `<id> <op> ANY|SOME|ALL (<subquery>)`.
    *
    * The two combinations that ARE the `IN` machinery collapse onto [[InSubquery]] here (PD-3): `=
    * ANY` / `= SOME` is `IN`, `<> ALL` / `!= ALL` is `NOT IN`. Every OTHER combination becomes a
    * [[QuantifiedSubquery]], reduced from the resolved value list at execution time (lead ruling
    * OQ-4, 2026-09-14 — this REPLACES the spec's `err` naming a MIN/MAX rewrite).
    *
    * 🔴 MUST precede `equality` / `comparison` in [[criteria]]. `ANY` and `SOME` are NOT reserved
    * words and this story reserves nothing (`WHERE any = 1` and `SELECT some FROM t` parse today —
    * measured — and must keep parsing), so `x = ANY (…)` otherwise SUCCEEDS as `x = <column any>`
    * and the `(SELECT …)` is left to `whereCriteria`'s bare delimiters. What makes the quantified
    * form win is the ORDER of the alternation, never a new reserved word: on `x = any` this
    * production fails at `subqueryBody` and the alternation falls through to `equality` with the
    * column reading intact.
    */
  private lazy val quantifiedSubquery: PackratParser[Criteria] =
    not.? ~ any_identifier ~ comparisonOp ~ quantifier ~ subqueryBody ^^ {
      case n ~ i ~ EQ ~ ANY ~ q          => InSubquery(i, q, n)
      case n ~ i ~ (NE | DIFF) ~ ALL ~ q =>
        // `NOT x <> ALL (S)` is `x IN (S)`: the two negations cancel, and the canonical render is
        // the positive `IN`.
        InSubquery(i, q, if (n.isDefined) None else Some(NOT))
      case n ~ i ~ o ~ qf ~ q => QuantifiedSubquery(i, o, qf, q, n)
    }

  /** `lazy val` for the same reason as [[any_identifier]]: this is the hottest production in the
    * grammar (every WHERE, HAVING, JOIN-ON and CASE-WHEN condition reaches it) and it is referenced
    * from several places, each of which would otherwise rebuild the whole alternation and defeat
    * the cache at the same position.
    */
  lazy val criteria: PackratParser[Criteria] =
    // Story 22.2 — the four subquery productions come FIRST, and the order is load-bearing:
    //   - `existsSubquery`: `EXISTS` is a reserved word, so no identifier-headed production can
    //     match its prefix and nothing else can start with it;
    //   - `quantifiedSubquery`: MUST precede `equality` / `comparison` — see its scaladoc;
    //   - `inSubquery` vs `inLiteral`/`inLongs`/`inDoubles`: disjoint at the character after `(`
    //     (`SELECT` is neither a quote nor a digit), so either order is correct; the subquery form
    //     is first only because it fails fastest;
    //   - `scalarSubquery` before `equality` / `comparison`: those FAIL (they do not partially
    //     succeed) on `(SELECT`, and on `(b + 1)` it is `scalarSubquery` that FAILS — at
    //     `derivedTableBodyInner` — so `WHERE a = (b + 1)` keeps its parenthesised-expression
    //     reading. Every neighbour is pinned byte-identical in `WhereSubquerySpec`.
    (existsSubquery |
    quantifiedSubquery |
    inSubquery |
    scalarSubquery |
    equality |
    like |
    rlike |
    comparison |
    inLiteral |
    inLongs |
    inDoubles |
    between |
    betweenDistances |
    betweenLongs |
    betweenDoubles |
    betweenIdentifiers |
    isNotNull |
    isNull | /*coalesce | nullif | distanceCriteria | */
    matchCriteria |
    logical_criteria) ^^ (c => c)

  lazy val predicate: PackratParser[Predicate] = criteria ~ (and | or) ~ not.? ~ criteria ^^ {
    case l ~ o ~ n ~ r => Predicate(l, o, r, n)
  }

  /** One balanced parenthesised group INSIDE a relation predicate, re-emitted as tokens.
    *
    * It matches a `(` together with its OWN `)` and hands both back, so `processTokens` sees
    * exactly the token shape it sees at top level and builds the same tree (including
    * `Predicate.group = true`, which is what renders the parentheses back).
    */
  private lazy val relationGroup: PackratParser[List[Token]] =
    start ~ relationTokens ~ end ^^ { case s ~ ts ~ e => (s :: ts) :+ e }

  /** The token stream inside a relation predicate's parentheses.
    *
    * `whereCriteria` scans the alternation `allPredicate | allCriteria | start | or | and | end |
    * then_case` (since story 22.1 with a depth rule, but the ITEM alternation is unchanged). This
    * is that alternation minus THREE of those, and the differences are ENUMERATED rather than
    * summarised, because the correctness claim rests on exactly which ones are missing:
    *
    *   - a bare `end` is omitted - the closing parenthesis belongs to the relation predicate, and
    *     the scan does NOT backtrack, so leaving it in would let the repetition swallow that `)`
    *     and everything after it, after which the enclosing end could never match. (Story 22.1's
    *     depth rule solves the SAME problem one construct up, for a parenthesised derived-table
    *     body; it is not a substitute here, because a relation predicate's `)` is at depth 0 of a
    *     group this production opened itself through `relationGroup`.);
    *   - a bare `start` is omitted for the mirror reason: an opening parenthesis must only ever be
    *     consumed by `relationGroup`, together with its OWN closing one, which is what keeps the
    *     two balanced and lets sub-groups nest;
    *   - `then_case` is omitted because a bare `THEN` cannot appear in a relation body (a `CASE`
    *     expression inside one is consumed whole by `criteria` - measured).
    *
    * Every criteria-bearing alternative is shared, so a shape added to `whereCriteria` is reachable
    * here too. What KEEPS them in step is the property test *"reduce a relation body to exactly the
    * tree the same expression produces at top level"* (`ParserTotalitySpec`): if one alternation
    * gains a shape the other cannot reach, the two trees stop being equal and it fails.
    */
  private lazy val relationTokens: PackratParser[List[Token]] =
    rep1(
      relationGroup |
      allPredicate ^^ (c => List(c: Token)) |
      allCriteria ^^ (t => List(t)) |
      (or | and) ^^ (o => List(o: Token))
    ) ^^ (_.flatten)

  /** `( <N criteria joined by AND / OR, with optional sub-groups> )` for a relation predicate.
    *
    * #250 / story 21.4. This REPLACES `start ~ predicate ~ end`. `predicate` is strictly BINARY
    * (`criteria ~ (and|or) ~ not.? ~ criteria`), so a relation predicate with THREE OR MORE
    * criteria could not match it and fell through to the old `X.regex ~ start.? ~ criteria ~ end.?`
    * form - whose `start.?` swallowed the `(` while its `end.?` never fired. The measured
    * consequence was a SILENT WRONG ANSWER, not a syntax error: `WHERE id = 1 AND child(a = 2 AND b
    * = 3 AND c = 4)` parsed as `WHERE id = 1 AND CHILD(a = 2) AND b = 3 AND c = 4`
    * i.e. the relation scope collapsed to the FIRST criterion and the other two escaped onto the
    * PARENT document; with `OR` the operator itself spanned the relation boundary.
    *
    * Reducing the tokens with `processTokens` - the same function `where`, `having`, `on` and
    * `case_condition` use - is the point: `NESTED(X)` now produces exactly the criteria tree `WHERE
    * X` produces, so associativity, grouping and NOT handling cannot drift between the two.
    */
  /** ⚠️ The `err`s below are non-backtracking, and that is deliberate: they fire only AFTER `start
    * ~> relationTokens <~ end` has committed, i.e. after `CHILD(` has actually matched. A column
    * literally NAMED `child` / `nested` / `parent` is therefore untouched - `Child.regex` matches,
    * the following `start` fails with a `Failure` (not an `Error`), and the alternation falls
    * through to `criteria` exactly as before. Measured, and pinned by *"still treat a column named
    * like a relation as an ordinary column"*.
    *
    * `relation` names the relation in the `Right(None)` message, which IS reachable: `child(child.a
    * = 1 AND)` yields *"CHILD clause requires criteria"* (pinned).
    */
  private def relationCriteria(relation: String): PackratParser[Criteria] =
    start ~> relationTokens <~ end >> { tokens =>
      processTokens(tokens) match {
        case Right(Some(c)) => success(c)
        case Right(None)    => err(s"$relation clause requires criteria")
        case Left(reason)   => err(reason)
      }
    }

  /** The PAREN-LESS form, e.g. `NESTED a = 1`. The parenthesised form - any arity - belongs to
    * `nestedPredicate` below.
    *
    * The optional `start.?` / `end.?` this used to carry are GONE (#250): they let a `(` be
    * consumed with no matching `)` and nothing notice, so `WHERE child(a = 1 AND b = 2` parsed
    * happily as `CHILD(a = 1) AND b = 2`. An opening parenthesis now commits to `childPredicate`,
    * which requires the closing one.
    */
  lazy val nestedCriteria: PackratParser[ElasticRelation] =
    Nested.regex ~> criteria ^^ { c =>
      ElasticNested(c, None, fromCriteria = false)
    }

  lazy val nestedPredicate: PackratParser[ElasticRelation] =
    Nested.regex ~> relationCriteria("NESTED") ^^ { c =>
      ElasticNested(c, None, fromCriteria = false)
    }

  lazy val childCriteria: PackratParser[ElasticRelation] = Child.regex ~> criteria ^^ { c =>
    ElasticChild(c)
  }

  lazy val childPredicate: PackratParser[ElasticRelation] =
    Child.regex ~> relationCriteria("CHILD") ^^ { c =>
      ElasticChild(c)
    }

  lazy val parentCriteria: PackratParser[ElasticRelation] =
    Parent.regex ~> criteria ^^ { c =>
      ElasticParent(c)
    }

  lazy val parentPredicate: PackratParser[ElasticRelation] =
    Parent.regex ~> relationCriteria("PARENT") ^^ { c =>
      ElasticParent(c)
    }

  private lazy val allPredicate: PackratParser[Criteria] =
    nestedPredicate | childPredicate | parentPredicate | predicate

  private lazy val allCriteria: PackratParser[Token] =
    nestedCriteria | childCriteria | parentCriteria | criteria

  /** The token stream of a WHERE / HAVING / CASE-WHEN / JOIN-ON condition.
    *
    * Story 22.1 (AD-2b, specified with story 22.2): same alternatives as the `rep1` this replaces,
    * ONE rule added — a `)` is consumed only while an unmatched `(` is open in THIS clause. At
    * depth 0 the scan stops and leaves the `)` to whoever opened it.
    *
    * 🔴 Why it had to change. `rep1` offers a bare `end` and never backtracks, so for every
    * PARENTHESISED body whose last clause is a WHERE or a HAVING — story 22.1's `FROM (SELECT a
    * FROM t WHERE x = 1) d`, story 22.2's `IN (SELECT id FROM c WHERE r = 'EU')` — the inner clause
    * swallowed the subquery's own `)`, the reducer's (`processTokens`) top-level `EndDelimiter` arm
    * answered `Left("Unbalanced parentheses")` and `where` raised it as a NON-backtracking `err`
    * that killed the whole statement. It is the mechanism story 21.4 met inside relation predicates
    * and fixed by giving them `relationTokens`, which omits `end` (`:264-293`) — the same defect,
    * one construct up.
    *
    * Depth is counted on `StartPredicate` / `EndPredicate` ONLY, because those are the only
    * delimiters this alternation can emit: `start` produces `StartPredicate`, `end` produces
    * `EndPredicate`, and `then_case` produces `ThenCase` — which IS an `EndDelimiter` but must keep
    * being consumed, since the reducer (`processTokens`) reads it as end-of-tokens for a CASE-WHEN
    * condition. A parenthesis that an ITEM consumes (a function call, a relation predicate's own
    * group, `IN (1, 2)`) never reaches this counter: items are consumed atomically.
    *
    * What moves, and it is pinned: an unmatched OPENING paren is still consumed and still reaches
    * `processTokens`, so `WHERE (b = 1` keeps its `"Unbalanced parentheses"` rejection. A STRAY `)`
    * at depth 0 is no longer eaten — the clause ends before it and `phrase` rejects the statement
    * as trailing input, so `SELECT a FROM t WHERE a = 1)` stays a rejection but changes its wording
    * (`ParserTotalitySpec` retargets those three contract pins).
    *
    * Written as a plain `Parser[List[Token]]` over the existing item parser: PackratParser memoises
    * the items exactly as before, and the depth is a local of one scan, never parser state.
    */
  lazy val whereCriteria: PackratParser[List[Token]] = new self.Parser[List[Token]] {

    // The SAME alternation, in the SAME order, as the `rep1` this replaces.
    private val item: self.Parser[Token] =
      allPredicate | allCriteria | start | or | and | end | then_case

    @scala.annotation.tailrec
    private def scan(rest: Input, depth: Int, acc: List[Token]): ParseResult[List[Token]] =
      item(rest) match {
        case Success(EndPredicate, _) if depth == 0 =>
          if (acc.isEmpty) Failure("criteria expected", rest) else Success(acc.reverse, rest)
        case Success(EndPredicate, next)   => scan(next, depth - 1, EndPredicate :: acc)
        case Success(StartPredicate, next) => scan(next, depth + 1, StartPredicate :: acc)
        case Success(t, next)              => scan(next, depth, t :: acc)
        // An item's own `err` (a relation predicate's, #250 / story 21.4) propagates unchanged.
        case e: Error => e
        case f: Failure =>
          if (acc.isEmpty) f else Success(acc.reverse, rest)
      }

    override def apply(in: Input): ParseResult[List[Token]] = scan(in, 0, Nil)
  }

  lazy val where: PackratParser[Where] =
    Where.regex ~ whereCriteria >> { case _ ~ rawTokens =>
      // `err`, not `throw` and not `failure` (#250, same reasoning as `alterTable`,
      // Parser.scala:713-729). `Error.append` returns `this` (scala-parser-combinators 1.1.2,
      // Parsers.scala:211), so an `Error` short-circuits `where.?` / `opt(where)` at all six call
      // sites instead of collapsing into a `None` and silently dropping the WHERE - the #213
      // failure mode. Nothing that parses today is lost: every input that reaches a `Left` here
      // used to throw, which aborted the whole parse.
      processTokens(rawTokens) match {
        case Right(Some(criteria)) => success(Where(Some(criteria)))
        // A dangling `AND` / `OR` used to leave `Where(None)`, which renders as NO CLAUSE AT ALL:
        // `DELETE FROM orders WHERE id = 1 AND` parsed as `DELETE FROM orders` and emptied the
        // index (the #213 data-loss family, measured 2026-09-04). `where` runs only once the
        // literal WHERE has matched, and `whereCriteria` yields at least one token or FAILS (story
        // 22.1's scanner returns the item's own `Failure` when its accumulator is empty, exactly as
        // `rep1` did), so `None` here always means "a WHERE was written and nothing usable came of
        // it".
        case Right(None)  => err("WHERE clause requires criteria")
        case Left(reason) => err(reason)
      }
    }

  import scala.annotation.tailrec

  /** The tokens of ONE parenthesis level as an alternating operand / junction list.
    *
    * 🔴 `whereCriteria` scans `allPredicate` first, and `allPredicate` holds the binary
    * [[predicate]] production (`criteria (AND|OR) [NOT] criteria`). So the scanner PRE-PAIRS two
    * adjacent conditions into one `Predicate` token, left to right and whatever their operators: `a
    * OR b AND c` arrives as `[a OR b], AND, c`. A precedence reduction must take the pairs apart
    * before it can apply AND before OR. A pair is taken apart WITHOUT moving its `NOT`: the scanner
    * put it on the pair's right operand, and [[reduceChain]] puts it back there whenever that
    * operand stays a right operand. `predicate` itself must stay in the scanner: it is the only
    * home of a `NOT` in front of a LIKE, IN, BETWEEN or IS condition after an operator (`a AND NOT
    * b LIKE 'x%'`) -- those carry their own NOT only after the column.
    *
    * A parenthesised group is reduced on its own ([[processSubTokens]]) and enters the chain as ONE
    * operand, marked `group = true`: the mark `Predicate.sql` renders the parentheses from. `THEN`
    * ends the scan: `case_condition` hands this reducer its tokens up to and including it.
    *
    * Rejections are returned, never thrown (#250).
    */
  @tailrec
  private def flatten(
    tokens: List[Token],
    acc: List[ChainItem]
  ): Either[String, List[ChainItem]] =
    tokens match {
      case Nil | (ThenCase :: _) => Right(acc.reverse)
      case (_: StartDelimiter) :: rest =>
        extractSubTokens(rest, 1) match {
          case Left(reason) => Left(reason)
          case Right((subTokens, remainingTokens)) =>
            processSubTokens(subTokens) match {
              case Left(reason) => Left(reason)
              case Right(p: Predicate) =>
                flatten(remainingTokens, ChainOperand(p.copy(group = true)) :: acc)
              case Right(c) => flatten(remainingTokens, ChainOperand(c) :: acc)
            }
        }
      case (p: Predicate) :: rest =>
        flatten(
          rest,
          ChainOperand(p.rightCriteria) :: ChainJunction(p.operator, p.not.isDefined) ::
          ChainOperand(p.leftCriteria) :: acc
        )
      case (c: Criteria) :: rest => flatten(rest, ChainOperand(c) :: acc)
      case (op: PredicateOperator) :: rest =>
        flatten(rest, ChainJunction(op, negatesNext = false) :: acc)
      case (_: EndDelimiter) :: _ =>
        // A closing delimiter reaching the scan of a level means no `StartDelimiter` arm ever took
        // ownership of it: a stray `)` (`WHERE a = 1)`), or -- before story 21.4 -- the `)` of a
        // relation predicate whose `(` the paren-less form swallowed. Pinned in ParserTotalitySpec.
        Left("Unbalanced parentheses")
      case unexpected :: _ =>
        // Believed unreachable (`whereCriteria` emits no other token kind) -- and must not
        // silently truncate the clause if a grammar change makes it reachable (#250).
        Left(s"Unexpected token in predicate: ${unexpected.getClass.getSimpleName}")
    }

  /** Reduces one level with SQL precedence -- `NOT` > `AND` > `OR`, left-associative, parentheses
    * respected (a group is one operand).
    *
    * The operands are split at every `OR` into runs joined by `AND`; each run is folded left, and
    * the runs are folded left with `OR`. So `a OR b AND c` is `a OR (b AND c)`, `a AND b OR c AND
    * d` is `(a AND b) OR (c AND d)`, and a one-operator chain is the left fold.
    *
    * The scanner's `NOT` qualifies the operand after its operator, and that operand is the RIGHT
    * operand of the predicate built for it -- `Predicate.not`, as the scanner built it -- except in
    * ONE position: the first operand of a run that follows an `OR` becomes the LEFT operand of the
    * run's first `AND`, and `Predicate` has no left-hand `NOT`. There the negation is folded into
    * the operand through `Criteria.negated`, which is the criterion the grammar itself builds for
    * that text at the start of a clause (`NOT a = 1 AND b = 2`, `a NOT LIKE 'x%' AND b = 2`,
    * `ISNOTNULL(a) AND b = 2`). A criterion that cannot carry its own `NOT` (`MATCH ... AGAINST`)
    * is refused there by name -- it is refused at the start of a clause too.
    *
    * The shapes of the old reduction are kept where they were right: a dangling operator is still
    * `Right(None)` (the caller names the clause); a leading operator and two consecutive operands
    * or operators are still `Left("Invalid stack state for predicate creation")`.
    */
  private def reduceChain(items: List[ChainItem]): Either[String, Option[Criteria]] = {
    val invalid = "Invalid stack state for predicate creation"
    def not(negated: Boolean): Option[NOT.type] = if (negated) Some(NOT) else None
    // The LEFT operand of a run's first AND.
    def runHead(c: Criteria, negated: Boolean): Either[String, Criteria] =
      if (!negated) Right(c)
      else
        c.negated.toRight(
          s"NOT ${c.sql} cannot start conditions joined by AND after an OR: write it after the " +
          s"AND, for example A OR (B AND NOT ${c.sql})"
        )
    // `runs`: the finished OR operands, newest first, each with the scanner's NOT of a run that
    // stayed ONE operand long; `current` / `currentNot`: the run being folded.
    @tailrec
    def loop(
      rest: List[ChainItem],
      runs: List[(Criteria, Boolean)],
      current: Criteria,
      currentNot: Boolean
    ): Either[String, Option[Criteria]] =
      rest match {
        case Nil =>
          val all = ((current, currentNot) :: runs).reverse
          Right(Some(all.tail.foldLeft(all.head._1) { case (acc, (run, negated)) =>
            Predicate(acc, OR, run, not(negated))
          }))
        case ChainJunction(_, _) :: Nil => Right(None) // a dangling operator
        case ChainJunction(AND, negated) :: ChainOperand(c) :: tail =>
          runHead(current, currentNot) match {
            case Left(reason) => Left(reason)
            case Right(head) =>
              loop(tail, runs, Predicate(head, AND, c, not(negated)), currentNot = false)
          }
        case ChainJunction(OR, negated) :: ChainOperand(c) :: tail =>
          loop(tail, (current, currentNot) :: runs, c, negated)
        case _ => Left(invalid)
      }
    items match {
      case Nil                         => Right(None)
      case ChainOperand(first) :: rest => loop(rest, Nil, first, currentNot = false)
      case _                           => Left(invalid)
    }
  }

  /** Reduces the tokens of a WHERE / HAVING / JOIN ON / CASE WHEN condition, or of a relation body,
    * to one criteria tree: [[flatten]] takes the scanner's pairs apart, then [[reduceChain]]
    * applies SQL precedence.
    *
    * Narrowed to `private[parser]` with #250: its four callers (`where` here,
    * `HavingParser.having`, `FromParser.on` and `parser.function.cond.case_condition`) are all
    * inside this package, and the return type changed from `Option[Criteria]` to `Either[String,
    * Option[Criteria]]`.
    *
    * @param tokens
    *   - list of SQL tokens
    * @return
    *   the criteria built from the tokens, or a Left carrying the reason the tokens are invalid
    */
  private[parser] def processTokens(
    tokens: List[Token]
  ): Either[String, Option[Criteria]] =
    flatten(tokens, Nil).flatMap(reduceChain)

  /** This method is used to process subtokens extracted between delimiters. It reduces them with
    * [[processTokens]] and returns the result as a SQLCriteria, or a `Left` carrying the reason no
    * criteria could be built (#250 - it used to throw).
    *
    * @param tokens
    *   - list of SQL tokens
    * @return
    *   the criteria built from the sub-tokens, or a Left carrying the reason they are invalid
    */
  private def processSubTokens(tokens: List[Token]): Either[String, Criteria] =
    processTokens(tokens) match {
      case Right(Some(criteria)) => Right(criteria)
      case Right(None)           => Left("Empty sub-expression")
      case Left(reason)          => Left(reason)
    }

  /** This method is used to extract subtokens between a start delimiter (StartDelimiter) and its
    * corresponding end delimiter (EndDelimiter). It uses a recursive approach to maintain the count
    * of open and closed delimiters and correctly construct the list of extracted subtokens.
    *
    * @param tokens
    *   - list of SQL tokens
    * @param openCount
    *   - count of open delimiters
    * @param subTokens
    *   - list of extracted subtokens
    * @return
    *   the extracted sub-tokens and the tokens left over, or a Left carrying the reason the
    *   delimiters are unbalanced (#250 - it used to throw)
    */
  @tailrec
  private def extractSubTokens(
    tokens: List[Token],
    openCount: Int,
    subTokens: List[Token] = Nil
  ): Either[String, (List[Token], List[Token])] = {
    tokens match {
      case Nil => Left("Unbalanced parentheses")
      case (start: StartDelimiter) :: rest =>
        extractSubTokens(rest, openCount + 1, start :: subTokens)
      case (end: EndDelimiter) :: rest =>
        if (openCount - 1 == 0) {
          Right((subTokens.reverse, rest))
        } else extractSubTokens(rest, openCount - 1, end :: subTokens)
      case head :: rest => extractSubTokens(rest, openCount, head :: subTokens)
    }
  }
}
