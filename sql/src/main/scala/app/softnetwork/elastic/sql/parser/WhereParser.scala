package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.{
  DoubleFromTo,
  DoubleValues,
  Identifier,
  LiteralFromTo,
  LongFromTo,
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
  ElasticChild,
  ElasticGeoDistance,
  ElasticNested,
  ElasticParent,
  ElasticRelation,
  GenericExpression,
  InExpr,
  IsNotNullExpr,
  IsNullExpr,
  MatchCriteria,
  Predicate,
  Where
}

trait WhereParser {
  self: Parser with GroupByParser with OrderByParser =>

  def isNull: PackratParser[Criteria] = identifier ~ IS_NULL.regex ^^ { case i ~ _ =>
    IsNullExpr(i)
  }

  def isNotNull: PackratParser[Criteria] = identifier ~ IS_NOT_NULL.regex ^^ { case i ~ _ =>
    IsNotNullExpr(i)
  }

  private def eq: PackratParser[ComparisonOperator] = EQ.sql ^^ (_ => EQ)

  private def ne: PackratParser[ComparisonOperator] = NE.sql ^^ (_ => NE)

  private def diff: PackratParser[ComparisonOperator] = DIFF.sql ^^ (_ => DIFF)

  private def any_identifier: PackratParser[Identifier] =
    identifierWithTransformation | identifierWithAggregation | identifierWithSystemFunction | identifierWithIntervalFunction | identifierWithArithmeticExpression | identifierWithFunction | date_diff_identifier | extract_identifier | identifier

  private def equality: PackratParser[GenericExpression] =
    not.? ~ any_identifier ~ (eq | ne | diff) ~ (boolean | literal | double | pi | long | any_identifier) ^^ {
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

  private def ge: PackratParser[ComparisonOperator] = GE.sql ^^ (_ => GE)

  def gt: PackratParser[ComparisonOperator] = GT.sql ^^ (_ => GT)

  private def le: PackratParser[ComparisonOperator] = LE.sql ^^ (_ => LE)

  def lt: PackratParser[ComparisonOperator] = LT.sql ^^ (_ => LT)

  private def comparison: PackratParser[GenericExpression] =
    not.? ~ any_identifier ~ (ge | gt | le | lt) ~ (double | pi | long | literal | any_identifier) ^^ {
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

  def sql_distance: PackratParser[Criteria] =
    distance ~ start ~ identifier ~ separator ~ start ~ double ~ separator ~ double ~ end ~ end ~ le ~ literal ^^ {
      case _ ~ _ ~ i ~ _ ~ _ ~ lat ~ _ ~ lon ~ _ ~ _ ~ _ ~ d => ElasticGeoDistance(i, d, lat, lon)
    }

  def matchCriteria: PackratParser[MatchCriteria] =
    MATCH.regex ~ start ~ rep1sep(
      any_identifier,
      separator
    ) ~ end ~ AGAINST.regex ~ start ~ literal ~ end ^^ { case _ ~ _ ~ i ~ _ ~ _ ~ _ ~ l ~ _ =>
      MatchCriteria(i, l)
    }

  def and: PackratParser[PredicateOperator] = AND.regex ^^ (_ => AND)

  def or: PackratParser[PredicateOperator] = OR.regex ^^ (_ => OR)

  def not: PackratParser[NOT.type] = NOT.regex ^^ (_ => NOT)

  def logical_criteria: PackratParser[Criteria] =
    (is_null | is_notnull) ^^ { case ConditionalFunctionAsCriteria(c) =>
      c
    }

  def criteria: PackratParser[Criteria] =
    (equality | like | rlike | comparison | inLiteral | inLongs | inDoubles | between | betweenLongs | betweenDoubles | isNotNull | isNull | /*coalesce | nullif |*/ sql_distance | matchCriteria | logical_criteria) ^^ (
      c => c
    )

  def predicate: PackratParser[Predicate] = criteria ~ (and | or) ~ not.? ~ criteria ^^ {
    case l ~ o ~ n ~ r => Predicate(l, o, r, n)
  }

  def nestedCriteria: PackratParser[ElasticRelation] =
    Nested.regex ~ start.? ~ criteria ~ end.? ^^ { case _ ~ _ ~ c ~ _ =>
      ElasticNested(c, None)
    }

  def nestedPredicate: PackratParser[ElasticRelation] = Nested.regex ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticNested(p, None)
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
    Where.regex ~ whereCriteria ^^ { case _ ~ rawTokens =>
      Where(processTokens(rawTokens))
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
    * simply add the operator to the stack. Otherwise, it's a battery status error. Case of
    * delimiters (StartDelimiter and EndDelimiter): If the first token is a start delimiter
    * (StartDelimiter), we extract the tokens up to the corresponding end delimiter (EndDelimiter),
    * we recursively process the extracted sub-tokens, then we continue with the rest of the tokens.
    *
    * Other cases: If none of the previous cases match, an IllegalStateException is thrown to
    * indicate an unexpected token type.
    *
    * @param tokens
    *   - liste des tokens SQL
    * @param stack
    *   - stack de tokens
    * @return
    */
  @tailrec
  private def processTokensHelper(
    tokens: List[Token],
    stack: List[Token]
  ): Option[Criteria] = {
    tokens match {
      case Nil =>
        stack match {
          case (right: Criteria) :: (op: PredicateOperator) :: (left: Criteria) :: Nil =>
            Option(
              Predicate(left, op, right)
            )
          case _ =>
            stack.headOption.collect { case c: Criteria => c }
        }
      case (_: StartDelimiter) :: rest =>
        val (subTokens, remainingTokens) = extractSubTokens(rest, 1)
        val subCriteria = processSubTokens(subTokens) match {
          case p: Predicate => p.copy(group = true)
          case c            => c
        }
        processTokensHelper(remainingTokens, subCriteria :: stack)
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
            throw ValidationError("Invalid stack state for predicate creation")
        }
      case ThenCase :: _ =>
        processTokensHelper(Nil, stack) // exit processing on THEN
      case (_: EndDelimiter) :: rest =>
        processTokensHelper(rest, stack) // Ignore and move on
      case _ => processTokensHelper(Nil, stack)
    }
  }

  /** This method calls processTokensHelper with an empty stack (Nil) to begin processing primary
    * tokens.
    *
    * @param tokens
    *   - list of SQL tokens
    * @return
    */
  protected def processTokens(
    tokens: List[Token]
  ): Option[Criteria] = {
    processTokensHelper(tokens, Nil)
  }

  /** This method is used to process subtokens extracted between delimiters. It calls
    * processTokensHelper and returns the result as a SQLCriteria, or throws an exception if no
    * criteria is found.
    *
    * @param tokens
    *   - list of SQL tokens
    * @return
    */
  private def processSubTokens(tokens: List[Token]): Criteria = {
    processTokensHelper(tokens, Nil).getOrElse(
      throw ValidationError("Empty sub-expression")
    )
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
    */
  @tailrec
  private def extractSubTokens(
    tokens: List[Token],
    openCount: Int,
    subTokens: List[Token] = Nil
  ): (List[Token], List[Token]) = {
    tokens match {
      case Nil => throw ValidationError("Unbalanced parentheses")
      case (start: StartDelimiter) :: rest =>
        extractSubTokens(rest, openCount + 1, start :: subTokens)
      case (end: EndDelimiter) :: rest =>
        if (openCount - 1 == 0) {
          (subTokens.reverse, rest)
        } else extractSubTokens(rest, openCount - 1, end :: subTokens)
      case head :: rest => extractSubTokens(rest, openCount, head :: subTokens)
    }
  }
}
