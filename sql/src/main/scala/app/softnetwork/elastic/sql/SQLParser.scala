package app.softnetwork.elastic.sql

import scala.util.parsing.combinator.RegexParsers

/** Created by smanciot on 27/06/2018.
  *
  * SQL Parser for ElasticSearch
  *
  * TODO implements SQL :
  *   - JOIN,
  *   - GROUP BY,
  *   - HAVING, etc.
  */
object SQLParser
    extends SQLParser
    with SQLSelectParser
    with SQLFromParser
    with SQLWhereParser
    with SQLGroupByParser
    with SQLOrderByParser
    with SQLLimitParser {

  def request: Parser[SQLSearchRequest] = {
    phrase(select ~ from ~ where.? ~ groupBy.? ~ orderBy.? ~ limit.?) ^^ {
      case s ~ f ~ w ~ g ~ o ~ l =>
        SQLSearchRequest(s, f, w, g, o, l)
          .update()
    }
  }

  def union: Parser[Union.type] = Union.regex ^^ (_ => Union)

  def requests: Parser[List[SQLSearchRequest]] = rep1sep(request, union) ^^ (s => s)

  def apply(
    query: String
  ): Either[SQLParserError, Either[SQLSearchRequest, SQLMultiSearchRequest]] = {
    parse(requests, query) match {
      case NoSuccess(msg, _) =>
        Console.err.println(msg)
        Left(SQLParserError(msg))
      case Success(result, _) =>
        result match {
          case x :: Nil => Right(Left(x))
          case _        => Right(Right(SQLMultiSearchRequest(result)))
        }
    }
  }

}

trait SQLCompilationError

case class SQLParserError(msg: String) extends SQLCompilationError

trait SQLParser extends RegexParsers {

  def literal: Parser[SQLLiteral] =
    """"[^"]*"|'[^']*'""".r ^^ (str => SQLLiteral(str.substring(1, str.length - 1)))

  def int: Parser[SQLInt] = """(-)?(0|[1-9]\d*)""".r ^^ (str => SQLInt(str.toInt))

  def double: Parser[SQLDouble] = """(-)?(\d+\.\d+)""".r ^^ (str => SQLDouble(str.toDouble))

  def boolean: Parser[SQLBoolean] = """(true|false)""".r ^^ (bool => SQLBoolean(bool.toBoolean))

  def start: Parser[SQLDelimiter] = "(" ^^ (_ => StartPredicate)

  def end: Parser[SQLDelimiter] = ")" ^^ (_ => EndPredicate)

  def separator: Parser[SQLDelimiter] = "," ^^ (_ => Separator)

  def count: Parser[AggregateFunction] = Count.regex ^^ (_ => Count)

  def min: Parser[AggregateFunction] = Min.regex ^^ (_ => Min)

  def max: Parser[AggregateFunction] = Max.regex ^^ (_ => Max)

  def avg: Parser[AggregateFunction] = Avg.regex ^^ (_ => Avg)

  def sum: Parser[AggregateFunction] = Sum.regex ^^ (_ => Sum)

  def aggregateFunction: Parser[AggregateFunction] = count | min | max | avg | sum

  def distanceFunction: Parser[SQLFunction] = Distance.regex ^^ (_ => Distance)

  def sqlFunction: Parser[SQLFunction] = aggregateFunction | distanceFunction

  private val regexIdentifier = """[\*a-zA-Z_\-][a-zA-Z0-9_\-\.\[\]\*]*"""

  def identifierWithFunction: Parser[SQLIdentifier] = sqlFunction ~ start ~ identifier ~ end ^^ {
    case f ~ _ ~ i ~ _ =>
      i.copy(function = Some(f))
  }

  def identifier: Parser[SQLIdentifier] =
    Distinct.regex.? ~ regexIdentifier.r ^^ { case d ~ i =>
      SQLIdentifier(
        i,
        None,
        d.isDefined
      )
    }

  private val regexAlias =
    """\b(?!(?i)except\b)\b(?!(?i)where\b)\b(?!(?i)filter\b)\b(?!(?i)from\b)\b(?!(?i)group\b)\b(?!(?i)having\b)\b(?!(?i)order\b)\b(?!(?i)limit\b)[a-zA-Z0-9_]*"""

  def alias: Parser[SQLAlias] = Alias.regex.? ~ regexAlias.r ^^ { case _ ~ b => SQLAlias(b) }

  def field: Parser[SQLField] = (identifierWithFunction | identifier) ~ alias.? ^^ { case i ~ a =>
    SQLField(i, a)
  }

}

trait SQLSelectParser {
  self: SQLParser with SQLWhereParser =>

  def except: Parser[SQLExcept] = Except.regex ~ start ~ rep1sep(field, separator) ~ end ^^ {
    case _ ~ _ ~ e ~ _ =>
      SQLExcept(e)
  }

  def select: Parser[SQLSelect] =
    Select.regex ~ rep1sep(aggregate | field, separator) ~ except.? ^^ { case _ ~ fields ~ e =>
      SQLSelect(fields, e)
    }

}

trait SQLFromParser {
  self: SQLParser with SQLLimitParser =>

  def unnest: Parser[SQLTable] = Unnest.regex ~ start ~ identifier ~ limit.? ~ end ~ alias ^^ {
    case _ ~ _ ~ i ~ l ~ _ ~ a =>
      SQLTable(SQLUnnest(i, l), Some(a))
  }

  def table: Parser[SQLTable] = identifier ~ alias.? ^^ { case i ~ a => SQLTable(i, a) }

  def from: Parser[SQLFrom] = From.regex ~ rep1sep(unnest | table, separator) ^^ {
    case _ ~ tables =>
      SQLFrom(tables)
  }

}

trait SQLWhereParser {
  self: SQLParser with SQLGroupByParser with SQLOrderByParser =>

  def isNull: Parser[SQLCriteria] = identifier ~ IsNull.regex ^^ { case i ~ _ => SQLIsNull(i) }

  def isNotNull: Parser[SQLCriteria] = identifier ~ IsNotNull.regex ^^ { case i ~ _ =>
    SQLIsNotNull(i)
  }

  private def eq: Parser[SQLExpressionOperator] = Eq.sql ^^ (_ => Eq)

  private def ne: Parser[SQLExpressionOperator] = Ne.sql ^^ (_ => Ne)

  def filter: Parser[SQLFilter] = Filter.regex ~> "[" ~> whereCriteria <~ "]" ^^ { rawTokens =>
    SQLFilter(
      processTokens(rawTokens)
    )
  }

  def aggregate: Parser[SQLAggregate] =
    aggregateFunction ~ start ~ identifier ~ end ~ alias.? ~ filter.? ^^ {
      case agg ~ _ ~ i ~ _ ~ a ~ f => new SQLAggregate(agg, i, a, f)
    }

  private def equality: Parser[SQLExpression] =
    not.? ~ (identifierWithFunction | identifier) ~ (eq | ne) ~ (boolean | literal | double | int) ^^ {
      case n ~ i ~ o ~ v => SQLExpression(i, o, v, n)
    }

  def like: Parser[SQLExpression] =
    (identifierWithFunction | identifier) ~ not.? ~ Like.regex ~ literal ^^ { case i ~ n ~ _ ~ v =>
      SQLExpression(i, Like, v, n)
    }

  private def ge: Parser[SQLExpressionOperator] = Ge.sql ^^ (_ => Ge)

  def gt: Parser[SQLExpressionOperator] = Gt.sql ^^ (_ => Gt)

  private def le: Parser[SQLExpressionOperator] = Le.sql ^^ (_ => Le)

  def lt: Parser[SQLExpressionOperator] = Lt.sql ^^ (_ => Lt)

  private def comparison: Parser[SQLExpression] =
    not.? ~ (identifierWithFunction | identifier) ~ (ge | gt | le | lt) ~ (double | int | literal) ^^ {
      case n ~ i ~ o ~ v => SQLExpression(i, o, v, n)
    }

  def in: Parser[SQLExpressionOperator] = In.regex ^^ (_ => In)

  private def inLiteral: Parser[SQLCriteria] =
    identifier ~ not.? ~ in ~ start ~ rep1(literal ~ separator.?) ~ end ^^ {
      case i ~ n ~ _ ~ _ ~ v ~ _ =>
        SQLIn(
          i,
          SQLLiteralValues(v map {
            _._1
          }),
          n
        )
    }

  private def inNumerical: Parser[SQLCriteria] =
    (identifierWithFunction | identifier) ~ not.? ~ in ~ start ~ rep1(
      (double | int) ~ separator.?
    ) ~ end ^^ { case i ~ n ~ _ ~ _ ~ v ~ _ =>
      SQLIn(
        i,
        SQLNumericValues(v map {
          _._1
        }),
        n
      )
    }

  def between: Parser[SQLCriteria] =
    (identifierWithFunction | identifier) ~ not.? ~ Between.regex ~ literal ~ and ~ literal ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => SQLBetween(i, from, to, n)
    }

  def distance: Parser[SQLCriteria] =
    distanceFunction ~ start ~ identifier ~ separator ~ start ~ double ~ separator ~ double ~ end ~ end ~ le ~ literal ^^ {
      case _ ~ _ ~ i ~ _ ~ _ ~ lat ~ _ ~ lon ~ _ ~ _ ~ _ ~ d => ElasticGeoDistance(i, d, lat, lon)
    }

  def matchCriteria: Parser[ElasticMatch] =
    Match.regex ~ start ~ identifier ~ separator ~ literal ~ separator.? ~ literal.? ~ end ^^ {
      case _ ~ _ ~ i ~ _ ~ l ~ _ ~ o ~ _ => ElasticMatch(i, l, o.map(_.value))
    }

  def and: Parser[SQLPredicateOperator] = And.regex ^^ (_ => And)

  def or: Parser[SQLPredicateOperator] = Or.regex ^^ (_ => Or)

  def not: Parser[Not.type] = Not.regex ^^ (_ => Not)

  def criteria: Parser[SQLCriteria] =
    (equality | like | comparison | inLiteral | inNumerical | between | isNotNull | isNull | distance | matchCriteria) ^^ (
      c => c
    )

  def predicate: Parser[SQLPredicate] = criteria ~ (and | or) ~ not.? ~ criteria ^^ {
    case l ~ o ~ n ~ r => SQLPredicate(l, o, r, n)
  }

  def nestedCriteria: Parser[ElasticRelation] = Nested.regex ~ start.? ~ criteria ~ end.? ^^ {
    case _ ~ _ ~ c ~ _ => ElasticNested(c, None)
  }

  def nestedPredicate: Parser[ElasticRelation] = Nested.regex ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticNested(p, None)
  }

  def childCriteria: Parser[ElasticRelation] = Child.regex ~ start.? ~ criteria ~ end.? ^^ {
    case _ ~ _ ~ c ~ _ => ElasticChild(c)
  }

  def childPredicate: Parser[ElasticRelation] = Child.regex ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticChild(p)
  }

  def parentCriteria: Parser[ElasticRelation] = Parent.regex ~ start.? ~ criteria ~ end.? ^^ {
    case _ ~ _ ~ c ~ _ => ElasticParent(c)
  }

  def parentPredicate: Parser[ElasticRelation] = Parent.regex ~ start ~ predicate ~ end ^^ {
    case _ ~ _ ~ p ~ _ => ElasticParent(p)
  }

  private def allPredicate: Parser[SQLCriteria] =
    nestedPredicate | childPredicate | parentPredicate | predicate

  private def allCriteria: Parser[SQLToken] =
    nestedCriteria | childCriteria | parentCriteria | criteria

  def whereCriteria: Parser[List[SQLToken]] = rep1(
    allPredicate | allCriteria | start | or | and | end
  )

  def where: Parser[SQLWhere] =
    Where.regex ~ whereCriteria ^^ { case _ ~ rawTokens =>
      SQLWhere(processTokens(rawTokens))
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
    tokens: List[SQLToken],
    stack: List[SQLToken]
  ): Option[SQLCriteria] = {
    tokens match {
      case Nil =>
        stack match {
          case (right: SQLCriteria) :: (op: SQLPredicateOperator) :: (left: SQLCriteria) :: Nil =>
            Option(
              SQLPredicate(left, op, right)
            )
          case _ =>
            stack.headOption.collect { case c: SQLCriteria => c }
        }
      case (_: StartDelimiter) :: rest =>
        val (subTokens, remainingTokens) = extractSubTokens(rest, 1)
        val subCriteria = processSubTokens(subTokens) match {
          case p: SQLPredicate => p.copy(group = true)
          case c               => c
        }
        processTokensHelper(remainingTokens, subCriteria :: stack)
      case (c: SQLCriteria) :: rest =>
        stack match {
          case (op: SQLPredicateOperator) :: (left: SQLCriteria) :: tail =>
            val predicate = SQLPredicate(left, op, c)
            processTokensHelper(rest, predicate :: tail)
          case _ =>
            processTokensHelper(rest, c :: stack)
        }
      case (op: SQLPredicateOperator) :: rest =>
        stack match {
          case (right: SQLCriteria) :: (left: SQLCriteria) :: tail =>
            val predicate = SQLPredicate(left, op, right)
            processTokensHelper(rest, predicate :: tail)
          case (right: SQLCriteria) :: (o: SQLPredicateOperator) :: tail =>
            tail match {
              case (left: SQLCriteria) :: tt =>
                val predicate = SQLPredicate(left, op, right)
                processTokensHelper(rest, o :: predicate :: tt)
              case _ =>
                processTokensHelper(rest, op :: stack)
            }
          case _ :: Nil =>
            processTokensHelper(rest, op :: stack)
          case _ =>
            throw new IllegalStateException("Invalid stack state for predicate creation")
        }
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
    tokens: List[SQLToken]
  ): Option[SQLCriteria] = {
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
  private def processSubTokens(tokens: List[SQLToken]): SQLCriteria = {
    processTokensHelper(tokens, Nil).getOrElse(
      throw new IllegalStateException("Empty sub-expression")
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
    tokens: List[SQLToken],
    openCount: Int,
    subTokens: List[SQLToken] = Nil
  ): (List[SQLToken], List[SQLToken]) = {
    tokens match {
      case Nil => throw new IllegalStateException("Unbalanced parentheses")
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

trait SQLGroupByParser {
  self: SQLParser with SQLWhereParser =>

  private def having: Parser[SQLHaving] = Having.regex ~> whereCriteria ^^ { rawTokens =>
    SQLHaving(
      processTokens(rawTokens)
    )
  }

  def bucket: Parser[SQLBucket] = identifier ^^ (i => SQLBucket(i))

  def groupBy: Parser[SQLGroupBy] = GroupBy.regex ~ rep1sep(bucket, separator) ~ having.? ^^ {
    case _ ~ buckets ~ having => SQLGroupBy(buckets, having)
  }

}

trait SQLOrderByParser {
  self: SQLParser =>

  def asc: Parser[Asc.type] = Asc.regex ^^ (_ => Asc)

  def desc: Parser[Desc.type] = Desc.regex ^^ (_ => Desc)

  private def fieldName: Parser[String] =
    """\b(?!(?i)limit\b)[a-zA-Z_][a-zA-Z0-9_]*""".r ^^ (f => f)

  def fieldWithFunction: Parser[(String, SQLFunction)] = sqlFunction ~ start ~ fieldName ~ end ^^ {
    case f ~ _ ~ n ~ _ => (n, f)
  }

  def sort: Parser[SQLFieldSort] =
    (fieldWithFunction | fieldName) ~ (asc | desc).? ^^ { case f ~ o =>
      f match {
        case i: (String, SQLFunction) => SQLFieldSort(i._1, o, Some(i._2))
        case s: String                => SQLFieldSort(s, o, None)
      }
    }

  def orderBy: Parser[SQLOrderBy] = OrderBy.regex ~ rep1sep(sort, separator) ^^ { case _ ~ s =>
    SQLOrderBy(s)
  }

}

trait SQLLimitParser {
  self: SQLParser =>

  def limit: Parser[SQLLimit] = Limit.regex ~ int ^^ { case _ ~ i => SQLLimit(i.value) }

}
