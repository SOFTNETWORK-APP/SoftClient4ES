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

trait WhereParser {
  self: Parser with GroupByParser with OrderByParser =>

  def isNull: PackratParser[Criteria] = (quotedIdentifier | identifier) ~ IS_NULL.regex ^^ {
    case i ~ _ =>
      IsNullExpr(i)
  }

  def isNotNull: PackratParser[Criteria] =
    (quotedIdentifier | identifier) ~ IS_NOT_NULL.regex ^^ { case i ~ _ =>
      IsNotNullExpr(i)
    }

  def eq: PackratParser[ComparisonOperator] = EQ.sql ^^ (_ => EQ)

  def ne: PackratParser[ComparisonOperator] = NE.sql ^^ (_ => NE)

  def diff: PackratParser[ComparisonOperator] = DIFF.sql ^^ (_ => DIFF)

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

  private def equality: PackratParser[GenericExpression] =
    not.? ~ any_identifier ~ (eq | ne | diff) ~ (boolean | quotedQualifiedIdentifier | literal | double | pi | geo_distance | long | any_identifier) ^^ {
      case n ~ i ~ o ~ v => GenericExpression(i, o, v, n)
    }

  def like: PackratParser[GenericExpression] =
    any_identifier ~ not.? ~ LIKE.regex ~ literal ^^ { case i ~ n ~ _ ~ v =>
      GenericExpression(i, LIKE, v, n)
    }

  def rlike: PackratParser[GenericExpression] =
    any_identifier ~ not.? ~ RLIKE.regex ~ literal ^^ { case i ~ n ~ _ ~ v =>
      GenericExpression(i, RLIKE, v, n)
    }

  def ge: PackratParser[ComparisonOperator] = GE.sql ^^ (_ => GE)

  def gt: PackratParser[ComparisonOperator] = GT.sql ^^ (_ => GT)

  def le: PackratParser[ComparisonOperator] = LE.sql ^^ (_ => LE)

  def lt: PackratParser[ComparisonOperator] = LT.sql ^^ (_ => LT)

  private def comparison: PackratParser[GenericExpression] =
    not.? ~ any_identifier ~ (ge | gt | le | lt) ~ (double | pi | random | geo_distance | long | quotedQualifiedIdentifier | literal | any_identifier) ^^ {
      case n ~ i ~ o ~ v => GenericExpression(i, o, v, n)
    }

  def in: PackratParser[ExpressionOperator] = IN.regex ^^ (_ => IN)

  private def inLiteral: PackratParser[Criteria] =
    any_identifier ~ not.? ~ in ~ start ~ rep1sep(literal, separator) ~ end ^^ {
      case i ~ n ~ _ ~ _ ~ v ~ _ =>
        InExpr(
          i,
          StringValues(v),
          n
        )
    }

  private def inDoubles: PackratParser[Criteria] =
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

  private def inLongs: PackratParser[Criteria] =
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

  def between: PackratParser[Criteria] =
    any_identifier ~ not.? ~ BETWEEN.regex ~ literal ~ and ~ literal ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => BetweenExpr(i, LiteralFromTo(from, to), n)
    }

  def betweenLongs: PackratParser[Criteria] =
    any_identifier ~ not.? ~ BETWEEN.regex ~ long ~ and ~ long ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => BetweenExpr(i, LongFromTo(from, to), n)
    }

  def betweenDoubles: PackratParser[Criteria] =
    any_identifier ~ not.? ~ BETWEEN.regex ~ double ~ and ~ double ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => BetweenExpr(i, DoubleFromTo(from, to), n)
    }

  def betweenIdentifiers: PackratParser[Criteria] =
    any_identifier ~ not.? ~ BETWEEN.regex ~ any_identifier ~ and ~ any_identifier ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => BetweenExpr(i, IdentifierFromTo(from, to), n)
    }

  def betweenDistances: PackratParser[Criteria] =
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

  def matchCriteria: PackratParser[MultiMatchCriteria] =
    MATCH.regex ~ start ~ rep1sep(
      any_identifier,
      separator
    ) ~ end ~ AGAINST.regex ~ start ~ literal ~ end ^^ { case _ ~ _ ~ i ~ _ ~ _ ~ _ ~ l ~ _ =>
      MultiMatchCriteria(i, l)
    }

  def and: PackratParser[PredicateOperator] = AND.regex ^^ (_ => AND)

  def or: PackratParser[PredicateOperator] = OR.regex ^^ (_ => OR)

  def not: PackratParser[NOT.type] = NOT.regex ^^ (_ => NOT)

  def logical_criteria: PackratParser[Criteria] =
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
  private def subqueryBody: PackratParser[DqlStatement] = start ~> derivedTableBodyInner <~ end

  private def comparisonOp: PackratParser[ComparisonOperator] = eq | ne | diff | ge | gt | le | lt

  /** `SOME` is canonicalised to `ANY` here (ANSI synonyms), so the AST carries one spelling and `x
    * > SOME (S)` renders — and re-parses — as `x > ANY (S)`.
    */
  private def quantifier: PackratParser[Quantifier] =
    ANY.regex ^^ (_ => ANY) | SOME.regex ^^ (_ => ANY) | ALL.regex ^^ (_ => ALL)

  private def existsSubquery: PackratParser[Criteria] =
    not.? ~ (EXISTS.regex ~> subqueryBody) ^^ { case n ~ q => ExistsSubquery(q, n) }

  private def inSubquery: PackratParser[Criteria] =
    any_identifier ~ not.? ~ in ~ subqueryBody ^^ { case i ~ n ~ _ ~ q => InSubquery(i, q, n) }

  private def scalarSubquery: PackratParser[Criteria] =
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
  private def quantifiedSubquery: PackratParser[Criteria] =
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

  def predicate: PackratParser[Predicate] = criteria ~ (and | or) ~ not.? ~ criteria ^^ {
    case l ~ o ~ n ~ r => Predicate(l, o, r, n)
  }

  /** One balanced parenthesised group INSIDE a relation predicate, re-emitted as tokens.
    *
    * It matches a `(` together with its OWN `)` and hands both back, so `processTokens` sees
    * exactly the token shape it sees at top level and builds the same tree (including
    * `Predicate.group = true`, which is what renders the parentheses back).
    */
  private def relationGroup: PackratParser[List[Token]] =
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
  private def relationTokens: PackratParser[List[Token]] =
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
  def nestedCriteria: PackratParser[ElasticRelation] =
    Nested.regex ~> criteria ^^ { c =>
      ElasticNested(c, None, fromCriteria = false)
    }

  def nestedPredicate: PackratParser[ElasticRelation] =
    Nested.regex ~> relationCriteria("NESTED") ^^ { c =>
      ElasticNested(c, None, fromCriteria = false)
    }

  def childCriteria: PackratParser[ElasticRelation] = Child.regex ~> criteria ^^ { c =>
    ElasticChild(c)
  }

  def childPredicate: PackratParser[ElasticRelation] =
    Child.regex ~> relationCriteria("CHILD") ^^ { c =>
      ElasticChild(c)
    }

  def parentCriteria: PackratParser[ElasticRelation] =
    Parent.regex ~> criteria ^^ { c =>
      ElasticParent(c)
    }

  def parentPredicate: PackratParser[ElasticRelation] =
    Parent.regex ~> relationCriteria("PARENT") ^^ { c =>
      ElasticParent(c)
    }

  private def allPredicate: PackratParser[Criteria] =
    nestedPredicate | childPredicate | parentPredicate | predicate

  private def allCriteria: PackratParser[Token] =
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
    * swallowed the subquery's own `)`, `processTokensHelper`'s top-level `EndDelimiter` arm
    * answered `Left("Unbalanced parentheses")` and `where` raised it as a NON-backtracking `err`
    * that killed the whole statement. It is the mechanism story 21.4 met inside relation predicates
    * and fixed by giving them `relationTokens`, which omits `end` (`:264-293`) — the same defect,
    * one construct up.
    *
    * Depth is counted on `StartPredicate` / `EndPredicate` ONLY, because those are the only
    * delimiters this alternation can emit: `start` produces `StartPredicate`, `end` produces
    * `EndPredicate`, and `then_case` produces `ThenCase` — which IS an `EndDelimiter` but must keep
    * being consumed, since `processTokensHelper` reads it as end-of-tokens for a CASE-WHEN
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
  def whereCriteria: PackratParser[List[Token]] = new self.Parser[List[Token]] {

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

  def where: PackratParser[Where] =
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

  /** This method is used to recursively process a list of SQL tokens and construct SQL criteria and
    * predicates from these tokens. Here are the key points:
    *
    * Base case (Nil): If the list of tokens is empty (Nil), we check the contents of the stack to
    * determine the final result.
    *
    * If the stack contains an operator, a left criterion and a right criterion, we create a
    * SQLPredicate predicate. Otherwise, we return the first criterion (SQLCriteria) of the stack if
    * it exists. Case of criteria (SQLCriteria): If the first token is a criterion, we treat it
    * according to the content of the stack:
    *
    * If the stack contains a predicate operator, we create a predicate with the left and right
    * criteria and update the stack. Otherwise, we simply add the criterion to the stack. Case of
    * operators (SQLPredicateOperator): If the first token is a predicate operator, we treat it
    * according to the contents of the stack:
    *
    * If the stack contains at least two elements, we create a predicate with the left and right
    * criterion and update the stack. If the stack contains only one element (a single operator), we
    * simply add the operator to the stack. Otherwise, it is an invalid stack state. Case of
    * delimiters (StartDelimiter and EndDelimiter): If the first token is a start delimiter
    * (StartDelimiter), we extract the tokens up to the corresponding end delimiter (EndDelimiter),
    * we recursively process the extracted sub-tokens, then we continue with the rest of the tokens.
    * A closing delimiter that reaches this scan is unmatched, because a balanced group is consumed
    * whole by extractSubTokens.
    *
    * Rejections: every failure is returned as a `Left(reason)` and NEVER thrown (#250).
    * `Parser.apply` is typed `Either[ParserError, Statement]` and five production call sites match
    * on that Either with no `try` of their own; the combinator callers of this helper turn a `Left`
    * into `err(reason)`.
    *
    * @param tokens
    *   - list of SQL tokens
    * @param stack
    *   - stack of tokens
    * @return
    *   the criteria built from the tokens, or a Left carrying the reason the tokens are invalid
    */
  @tailrec
  private def processTokensHelper(
    tokens: List[Token],
    stack: List[Token]
  ): Either[String, Option[Criteria]] = {
    tokens match {
      case Nil =>
        stack match {
          case (right: Criteria) :: (op: PredicateOperator) :: (left: Criteria) :: Nil =>
            Right(Option(Predicate(left, op, right)))
          // #250 - a Criteria head with anything still UNDER it means the tokens folded into a
          // stack this function cannot reduce, and returning just the head would SILENTLY DROP the
          // rest: the same defect class as the EndDelimiter arm below, and the #213 family. It used
          // to return `stack.headOption`.
          // MEASURED 2026-09-05 by instrumenting this arm and running every suite that parses SQL
          // (sql 592, core 856, bridge 120, macros-tests 19): NO input reaches it with a Criteria
          // head and a non-empty tail. The shapes that do reach the fallback below all have a
          // PredicateOperator head - a dangling AND/OR - and must keep yielding `Right(None)` so
          // the caller's "WHERE/HAVING clause requires criteria" message wins over this one.
          case (_: Criteria) :: rest if rest.nonEmpty =>
            Left("Invalid stack state for predicate creation")
          case _ =>
            Right(stack.headOption.collect { case c: Criteria => c })
        }
      case (_: StartDelimiter) :: rest =>
        extractSubTokens(rest, 1) match {
          case Left(reason) => Left(reason)
          case Right((subTokens, remainingTokens)) =>
            processSubTokens(subTokens) match {
              case Left(reason) => Left(reason)
              case Right(p: Predicate) =>
                processTokensHelper(remainingTokens, p.copy(group = true) :: stack)
              case Right(c) =>
                processTokensHelper(remainingTokens, c :: stack)
            }
        }
      case (c: Criteria) :: rest =>
        stack match {
          case (op: PredicateOperator) :: (left: Criteria) :: tail =>
            val predicate = Predicate(left, op, c)
            processTokensHelper(rest, predicate :: tail)
          case _ =>
            processTokensHelper(rest, c :: stack)
        }
      case (op: PredicateOperator) :: rest =>
        stack match {
          case (right: Criteria) :: (left: Criteria) :: tail =>
            val predicate = Predicate(left, op, right)
            processTokensHelper(rest, predicate :: tail)
          case (right: Criteria) :: (o: PredicateOperator) :: tail =>
            tail match {
              case (left: Criteria) :: tt =>
                val predicate = Predicate(left, op, right)
                processTokensHelper(rest, o :: predicate :: tt)
              case _ =>
                processTokensHelper(rest, op :: stack)
            }
          case _ :: Nil =>
            processTokensHelper(rest, op :: stack)
          case _ =>
            // #250 - was `throw ValidationError(...)`. `Parser.apply` is typed
            // `Either[ParserError, Statement]`; five production call sites match on that Either
            // with no `try` of their own (SQLImplicits.queryToStatement, IndicesApi x3, the
            // searchAs macro). The caller turns this into `err(...)`, which short-circuits
            // `where.?` instead of silently yielding a None.
            Left("Invalid stack state for predicate creation")
        }
      case ThenCase :: _ =>
        processTokensHelper(Nil, stack) // exit processing on THEN
      case (_: EndDelimiter) :: _ =>
        // A closing delimiter reaching the TOP-LEVEL scan means no `StartDelimiter` arm above ever
        // took ownership of it. This used to "ignore and move on", which silently discarded it.
        // TWO different inputs land here, and BOTH used to be corrupted rather than reported
        // (measured 2026-09-05 by reverting just this arm):
        //
        //   1. A genuinely stray `)`. `SELECT a FROM t WHERE a = 1)` parsed as `... WHERE a = 1`.
        //
        //   2. 🔴 A BALANCED relation predicate with THREE OR MORE criteria - so the reason text
        //      "Unbalanced parentheses" is accurate about the TOKEN STREAM, not about what the
        //      user typed. `nestedPredicate`/`childPredicate`/`parentPredicate` take a `predicate`,
        //      which is strictly BINARY (`criteria ~ (and|or) ~ not.? ~ criteria`), so with a third
        //      criterion they fail and the parser falls back to
        //      `nestedCriteria`/`childCriteria`/`parentCriteria` = `X.regex ~ start.? ~ criteria ~
        //      end.?`. That takes ONE criterion, its `start.?` swallows the `(`, its `end.?` finds
        //      `AND` instead of `)` - and the real `)` arrives here with nothing to close.
        //      Measured before this change:
        //        `WHERE id = 1 AND child(a = 2 AND b = 3 AND c = 4)`
        //          parsed as `WHERE id = 1 AND CHILD(a = 2) AND b = 3 AND c = 4`
        //      i.e. the CHILD scope silently collapsed to the first criterion and the other two
        //      escaped onto the parent document - a wrong answer that executes and returns rows.
        //      `child(x = 1 OR y = 2 OR z = 3)` likewise became `CHILD(x = 1) OR y = 2 OR z = 3`.
        //      Rejecting is strictly better, and is the #213 family this story is closing; the
        //      `start.?`/`end.?` asymmetry that causes it belongs to a later story (local record
        //      docs/issues/local-21.4-relation-predicate-paren-asymmetry.md). Its twin hole - an
        //      unmatched OPENING paren, `child(a = 1 AND b = 2`, still silently accepted - is NOT
        //      reachable from here and is recorded there too.
        Left("Unbalanced parentheses")
      case unexpected :: _ =>
        // #250 - this arm used to be `processTokensHelper(Nil, stack)`, which ABANDONED every
        // remaining token and returned whatever the stack happened to hold: a silent truncation of
        // the clause the user wrote. It is believed unreachable - `whereCriteria` scans
        // `allPredicate | allCriteria | start | or | and | end | then_case` (story 22.1 added a
        // depth rule, not a token kind) and every one of those token kinds is matched by an arm
        // above - and it was NEVER reached while
        // instrumented across the sql, core, bridge and macros-tests suites (2026-09-05). That is
        // exactly why it must not silently truncate: an unreachable arm that loses data is one
        // grammar change away from being reachable. Same reasoning as the defensive arm in
        // `parser/operator/math`.
        Left(s"Unexpected token in predicate: ${unexpected.getClass.getSimpleName}")
    }
  }

  /** This method calls processTokensHelper with an empty stack (Nil) to begin processing primary
    * tokens.
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
  ): Either[String, Option[Criteria]] = {
    processTokensHelper(tokens, Nil)
  }

  /** This method is used to process subtokens extracted between delimiters. It calls
    * processTokensHelper and returns the result as a SQLCriteria, or a `Left` carrying the reason
    * no criteria could be built (#250 - it used to throw).
    *
    * @param tokens
    *   - list of SQL tokens
    * @return
    *   the criteria built from the sub-tokens, or a Left carrying the reason they are invalid
    */
  private def processSubTokens(tokens: List[Token]): Either[String, Criteria] =
    processTokensHelper(tokens, Nil) match {
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
