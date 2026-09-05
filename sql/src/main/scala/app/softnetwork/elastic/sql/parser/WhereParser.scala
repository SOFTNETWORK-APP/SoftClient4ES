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
  AND,
  BETWEEN,
  Child,
  ComparisonOperator,
  DIFF,
  EQ,
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
  RLIKE
}
import app.softnetwork.elastic.sql.query.{
  BetweenExpr,
  ConditionalFunctionAsCriteria,
  Criteria,
  DistanceCriteria,
  ElasticChild,
  ElasticNested,
  ElasticParent,
  ElasticRelation,
  GenericExpression,
  InExpr,
  IsNotNullExpr,
  IsNullExpr,
  MultiMatchCriteria,
  Predicate,
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

  private def any_identifier: PackratParser[Identifier] =
    quotedIdentifier |
    identifierWithArithmeticExpression |
    identifierWithTransformation |
    identifierWithWindowFunction |
    identifierWithAggregation |
    identifierWithIntervalFunction |
    identifierWithFunction |
    identifierWithValue |
    identifier

  private def equality: PackratParser[GenericExpression] =
    not.? ~ any_identifier ~ (eq | ne | diff) ~ (boolean | literal | double | pi | geo_distance | long | any_identifier) ^^ {
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
    not.? ~ any_identifier ~ (ge | gt | le | lt) ~ (double | pi | random | geo_distance | long | literal | any_identifier) ^^ {
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

  def criteria: PackratParser[Criteria] =
    (equality |
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

  def nestedCriteria: PackratParser[ElasticRelation] =
    Nested.regex ~ start.? ~ criteria ~ end.? ^^ { case _ ~ _ ~ c ~ _ =>
      ElasticNested(c, None, fromCriteria = false)
    }

  def nestedPredicate: PackratParser[ElasticRelation] = Nested.regex ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticNested(p, None, fromCriteria = false)
  }

  def childCriteria: PackratParser[ElasticRelation] = Child.regex ~ start.? ~ criteria ~ end.? ^^ {
    case _ ~ _ ~ c ~ _ => ElasticChild(c)
  }

  def childPredicate: PackratParser[ElasticRelation] = Child.regex ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticChild(p)
  }

  def parentCriteria: PackratParser[ElasticRelation] =
    Parent.regex ~ start.? ~ criteria ~ end.? ^^ { case _ ~ _ ~ c ~ _ =>
      ElasticParent(c)
    }

  def parentPredicate: PackratParser[ElasticRelation] = Parent.regex ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticParent(p)
  }

  private def allPredicate: PackratParser[Criteria] =
    nestedPredicate | childPredicate | parentPredicate | predicate

  private def allCriteria: PackratParser[Token] =
    nestedCriteria | childCriteria | parentCriteria | criteria

  def whereCriteria: PackratParser[List[Token]] = rep1(
    allPredicate | allCriteria | start | or | and | end | then_case
  )

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
        // literal WHERE has matched and `whereCriteria` is `rep1`, so `None` here always means
        // "a WHERE was written and nothing usable came of it".
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
        // the clause the user wrote. It is believed unreachable - `whereCriteria` is
        // `rep1(allPredicate | allCriteria | start | or | and | end | then_case)` and every one of
        // those token kinds is matched by an arm above - and it was NEVER reached while
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
