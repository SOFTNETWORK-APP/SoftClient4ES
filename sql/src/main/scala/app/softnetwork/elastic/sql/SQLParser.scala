package app.softnetwork.elastic.sql

import scala.util.parsing.combinator.{PackratParsers, RegexParsers}
import scala.util.parsing.input.CharSequenceReader

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
    with SQLHavingParser
    with SQLOrderByParser
    with SQLLimitParser
    with PackratParsers {

  def request: PackratParser[SQLSearchRequest] = {
    phrase(select ~ from ~ where.? ~ groupBy.? ~ having.? ~ orderBy.? ~ limit.?) ^^ {
      case s ~ f ~ w ~ g ~ h ~ o ~ l =>
        SQLSearchRequest(s, f, w, g, h, o, l)
          .update()
    }
  }

  def union: PackratParser[Union.type] = Union.regex ^^ (_ => Union)

  def requests: PackratParser[List[SQLSearchRequest]] = rep1sep(request, union) ^^ (s => s)

  def apply(
    query: String
  ): Either[SQLParserError, Either[SQLSearchRequest, SQLMultiSearchRequest]] = {
    val reader = new PackratReader(new CharSequenceReader(query))
    parse(requests, reader) match {
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

trait SQLParser extends RegexParsers with PackratParsers {

  def literal: PackratParser[SQLLiteral] =
    """"[^"]*"|'[^']*'""".r ^^ (str => SQLLiteral(str.substring(1, str.length - 1)))

  def long: PackratParser[SQLLong] = """(-)?(0|[1-9]\d*)""".r ^^ (str => SQLLong(str.toLong))

  def double: PackratParser[SQLDouble] = """(-)?(\d+\.\d+)""".r ^^ (str => SQLDouble(str.toDouble))

  def boolean: PackratParser[SQLBoolean] =
    """(true|false)""".r ^^ (bool => SQLBoolean(bool.toBoolean))

  def start: PackratParser[SQLDelimiter] = "(" ^^ (_ => StartPredicate)

  def end: PackratParser[SQLDelimiter] = ")" ^^ (_ => EndPredicate)

  def separator: PackratParser[SQLDelimiter] = "," ^^ (_ => Separator)

  def count: PackratParser[AggregateFunction] = Count.regex ^^ (_ => Count)

  def min: PackratParser[AggregateFunction] = Min.regex ^^ (_ => Min)

  def max: PackratParser[AggregateFunction] = Max.regex ^^ (_ => Max)

  def avg: PackratParser[AggregateFunction] = Avg.regex ^^ (_ => Avg)

  def sum: PackratParser[AggregateFunction] = Sum.regex ^^ (_ => Sum)

  def year: PackratParser[TimeUnit] = Year.regex ^^ (_ => Year)

  def month: PackratParser[TimeUnit] = Month.regex ^^ (_ => Year)

  def quarter: PackratParser[TimeUnit] = Quarter.regex ^^ (_ => Quarter)

  def week: PackratParser[TimeUnit] = Week.regex ^^ (_ => Week)

  def day: PackratParser[TimeUnit] = Day.regex ^^ (_ => Day)

  def hour: PackratParser[TimeUnit] = Hour.regex ^^ (_ => Hour)

  def minute: PackratParser[TimeUnit] = Minute.regex ^^ (_ => Minute)

  def second: PackratParser[TimeUnit] = Second.regex ^^ (_ => Second)

  def interval: PackratParser[TimeInterval] =
    Interval.regex ~ long ~ (year | month | quarter | week | day | hour | minute | second) ^^ {
      case _ ~ l ~ u =>
        TimeInterval(l.value.toInt, u)
    }

  def current_date: PackratParser[DateTimeFunction] = CurrentDate.regex ~ start.? ~ end.? ^^ {
    case _ ~ s ~ t =>
      if (s.isDefined && t.isDefined) CurentDateWithParens else CurrentDate
  }

  def current_time: PackratParser[DateTimeFunction] = CurrentTime.regex ~ start.? ~ end.? ^^ {
    case _ ~ s ~ t =>
      if (s.isDefined && t.isDefined) CurrentTimeWithParens else CurrentTime
  }

  def current_timestamp: PackratParser[DateTimeFunction] =
    CurrentTimestamp.regex ~ start.? ~ end.? ^^ { case _ ~ s ~ t =>
      if (s.isDefined && t.isDefined) CurrentTimestampWithParens else CurrentTimestamp
    }

  def now: PackratParser[DateTimeFunction] = Now.regex ~ start.? ~ end.? ^^ { case _ ~ s ~ t =>
    if (s.isDefined && t.isDefined) NowWithParens else Now
  }

  def plus: PackratParser[ArithmeticOperator] = Plus.sql ^^ (_ => Plus)

  def minus: PackratParser[ArithmeticOperator] = Minus.sql ^^ (_ => Minus)

  def arithmeticOperator: PackratParser[ArithmeticOperator] = plus | minus

  def dateTimeWithInterval: PackratParser[SQLDateTimeField] =
    (current_date | current_time | current_timestamp | now) ~ arithmeticOperator.? ~ interval.? ^^ {
      case f ~ o ~ i =>
        SQLDateTimeField(
          SQLIdentifier(f.sql),
          o,
          i
        )
    }

  def aggregateFunction: PackratParser[AggregateFunction] = count | min | max | avg | sum

  def distanceFunction: PackratParser[SQLFunction] = Distance.regex ^^ (_ => Distance)

  def sqlFunction: PackratParser[SQLFunction] = aggregateFunction | distanceFunction

  private val regexIdentifier = """[\*a-zA-Z_\-][a-zA-Z0-9_\-\.\[\]\*]*"""

  def identifierWithFunction: PackratParser[SQLIdentifier] =
    sqlFunction ~ start ~ identifier ~ end ^^ { case f ~ _ ~ i ~ _ =>
      i.copy(function = Some(f))
    }

  def identifier: PackratParser[SQLIdentifier] =
    Distinct.regex.? ~ regexIdentifier.r ^^ { case d ~ i =>
      SQLIdentifier(
        i,
        None,
        d.isDefined
      )
    }

  def identifierWithInterval: PackratParser[SQLDateTimeField] =
    identifier ~ arithmeticOperator ~ interval ^^ { case f ~ o ~ i =>
      SQLDateTimeField(
        f,
        Some(o),
        Some(i)
      )
    }

  private val regexAlias =
    """\b(?!(?i)as\b)\b(?!(?i)except\b)\b(?!(?i)where\b)\b(?!(?i)filter\b)\b(?!(?i)from\b)\b(?!(?i)group\b)\b(?!(?i)having\b)\b(?!(?i)order\b)\b(?!(?i)limit\b)[a-zA-Z0-9_]*"""

  def alias: PackratParser[SQLAlias] = Alias.regex.? ~ regexAlias.r ^^ { case _ ~ b => SQLAlias(b) }

  def field: PackratParser[Field] = (identifierWithFunction | identifier) ~ alias.? ^^ {
    case i ~ a =>
      SQLField(i, a)
  }

  def scriptField: PackratParser[ScriptField] =
    (dateTimeWithInterval | identifierWithInterval) ~ alias.? ^^ { case d ~ a =>
      d.copy(fieldAlias = a)
    }
}

trait SQLSelectParser {
  self: SQLParser with SQLWhereParser =>

  def except: PackratParser[SQLExcept] = Except.regex ~ start ~ rep1sep(field, separator) ~ end ^^ {
    case _ ~ _ ~ e ~ _ =>
      SQLExcept(e)
  }

  def select: PackratParser[SQLSelect] =
    Select.regex ~ rep1sep(scriptField | field, separator) ~ except.? ^^ { case _ ~ fields ~ e =>
      SQLSelect(fields, e)
    }

}

trait SQLFromParser {
  self: SQLParser with SQLLimitParser =>

  def unnest: PackratParser[SQLTable] =
    Unnest.regex ~ start ~ identifier ~ limit.? ~ end ~ alias ^^ { case _ ~ _ ~ i ~ l ~ _ ~ a =>
      SQLTable(SQLUnnest(i, l), Some(a))
    }

  def table: PackratParser[SQLTable] = identifier ~ alias.? ^^ { case i ~ a => SQLTable(i, a) }

  def from: PackratParser[SQLFrom] = From.regex ~ rep1sep(unnest | table, separator) ^^ {
    case _ ~ tables =>
      SQLFrom(tables)
  }

}

trait SQLWhereParser {
  self: SQLParser with SQLGroupByParser with SQLOrderByParser =>

  def isNull: PackratParser[SQLCriteria] = identifier ~ IsNull.regex ^^ { case i ~ _ =>
    SQLIsNull(i)
  }

  def isNotNull: PackratParser[SQLCriteria] = identifier ~ IsNotNull.regex ^^ { case i ~ _ =>
    SQLIsNotNull(i)
  }

  private def eq: PackratParser[SQLExpressionOperator] = Eq.sql ^^ (_ => Eq)

  private def ne: PackratParser[SQLExpressionOperator] = Ne.sql ^^ (_ => Ne)

  private def equality: PackratParser[SQLExpression] =
    not.? ~ (identifierWithFunction | identifier) ~ (eq | ne) ~ (boolean | literal | double | long) ^^ {
      case n ~ i ~ o ~ v => SQLExpression(i, o, v, n)
    }

  def like: PackratParser[SQLExpression] =
    (identifierWithFunction | identifier) ~ not.? ~ Like.regex ~ literal ^^ { case i ~ n ~ _ ~ v =>
      SQLExpression(i, Like, v, n)
    }

  private def ge: PackratParser[SQLExpressionOperator] = Ge.sql ^^ (_ => Ge)

  def gt: PackratParser[SQLExpressionOperator] = Gt.sql ^^ (_ => Gt)

  private def le: PackratParser[SQLExpressionOperator] = Le.sql ^^ (_ => Le)

  def lt: PackratParser[SQLExpressionOperator] = Lt.sql ^^ (_ => Lt)

  private def comparison: PackratParser[SQLExpression] =
    not.? ~ (identifierWithFunction | identifier) ~ (ge | gt | le | lt) ~ (double | long | literal) ^^ {
      case n ~ i ~ o ~ v => SQLExpression(i, o, v, n)
    }

  def in: PackratParser[SQLExpressionOperator] = In.regex ^^ (_ => In)

  private def inLiteral: PackratParser[SQLCriteria] =
    identifier ~ not.? ~ in ~ start ~ rep1sep(literal, separator) ~ end ^^ {
      case i ~ n ~ _ ~ _ ~ v ~ _ =>
        SQLIn(
          i,
          SQLLiteralValues(v),
          n
        )
    }

  private def inDoubles: PackratParser[SQLCriteria] =
    (identifierWithFunction | identifier) ~ not.? ~ in ~ start ~ rep1sep(
      double,
      separator
    ) ~ end ^^ { case i ~ n ~ _ ~ _ ~ v ~ _ =>
      SQLIn(
        i,
        SQLDoubleValues(v),
        n
      )
    }

  private def inLongs: PackratParser[SQLCriteria] =
    (identifierWithFunction | identifier) ~ not.? ~ in ~ start ~ rep1sep(
      long,
      separator
    ) ~ end ^^ { case i ~ n ~ _ ~ _ ~ v ~ _ =>
      SQLIn(
        i,
        SQLLongValues(v),
        n
      )
    }

  def between: PackratParser[SQLCriteria] =
    (identifierWithFunction | identifier) ~ not.? ~ Between.regex ~ literal ~ and ~ literal ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => SQLBetween(i, SQLLiteralFromTo(from, to), n)
    }

  def betweenLongs: PackratParser[SQLCriteria] =
    (identifierWithFunction | identifier) ~ not.? ~ Between.regex ~ long ~ and ~ long ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => SQLBetween(i, SQLLongFromTo(from, to), n)
    }

  def betweenDoubles: PackratParser[SQLCriteria] =
    (identifierWithFunction | identifier) ~ not.? ~ Between.regex ~ double ~ and ~ double ^^ {
      case i ~ n ~ _ ~ from ~ _ ~ to => SQLBetween(i, SQLDoubleFromTo(from, to), n)
    }

  def distance: PackratParser[SQLCriteria] =
    distanceFunction ~ start ~ identifier ~ separator ~ start ~ double ~ separator ~ double ~ end ~ end ~ le ~ literal ^^ {
      case _ ~ _ ~ i ~ _ ~ _ ~ lat ~ _ ~ lon ~ _ ~ _ ~ _ ~ d => ElasticGeoDistance(i, d, lat, lon)
    }

  def matchCriteria: PackratParser[SQLMatch] =
    Match.regex ~ start ~ rep1sep(
      identifier,
      separator
    ) ~ end ~ Against.regex ~ start ~ literal ~ end ^^ { case _ ~ _ ~ i ~ _ ~ _ ~ _ ~ l ~ _ =>
      SQLMatch(i, l)
    }

  def and: PackratParser[SQLPredicateOperator] = And.regex ^^ (_ => And)

  def or: PackratParser[SQLPredicateOperator] = Or.regex ^^ (_ => Or)

  def not: PackratParser[Not.type] = Not.regex ^^ (_ => Not)

  def criteria: PackratParser[SQLCriteria] =
    (equality | like | comparison | inLiteral | inLongs | inDoubles | between | betweenLongs | betweenDoubles | isNotNull | isNull | distance | matchCriteria) ^^ (
      c => c
    )

  def predicate: PackratParser[SQLPredicate] = criteria ~ (and | or) ~ not.? ~ criteria ^^ {
    case l ~ o ~ n ~ r => SQLPredicate(l, o, r, n)
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

  private def allPredicate: PackratParser[SQLCriteria] =
    nestedPredicate | childPredicate | parentPredicate | predicate

  private def allCriteria: PackratParser[SQLToken] =
    nestedCriteria | childCriteria | parentCriteria | criteria

  def whereCriteria: PackratParser[List[SQLToken]] = rep1(
    allPredicate | allCriteria | start | or | and | end
  )

  def where: PackratParser[SQLWhere] =
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

  private def having: PackratParser[SQLHaving] = Having.regex ~> whereCriteria ^^ { rawTokens =>
    SQLHaving(
      processTokens(rawTokens)
    )
  }

  def bucket: PackratParser[SQLBucket] = identifier ^^ (i => SQLBucket(i))

  def groupBy: PackratParser[SQLGroupBy] =
    GroupBy.regex ~ rep1sep(bucket, separator) ^^ { case _ ~ buckets =>
      SQLGroupBy(buckets)
    }

}

trait SQLHavingParser {
  self: SQLParser with SQLWhereParser =>

  def having: PackratParser[SQLHaving] = Having.regex ~> whereCriteria ^^ { rawTokens =>
    SQLHaving(
      processTokens(rawTokens)
    )
  }

}

trait SQLOrderByParser {
  self: SQLParser =>

  def asc: PackratParser[Asc.type] = Asc.regex ^^ (_ => Asc)

  def desc: PackratParser[Desc.type] = Desc.regex ^^ (_ => Desc)

  private def fieldName: PackratParser[String] =
    """\b(?!(?i)limit\b)[a-zA-Z_][a-zA-Z0-9_]*""".r ^^ (f => f)

  def fieldWithFunction: PackratParser[(String, SQLFunction)] =
    sqlFunction ~ start ~ fieldName ~ end ^^ { case f ~ _ ~ n ~ _ =>
      (n, f)
    }

  def sort: PackratParser[SQLFieldSort] =
    (fieldWithFunction | fieldName) ~ (asc | desc).? ^^ { case f ~ o =>
      f match {
        case i: (String, SQLFunction) => SQLFieldSort(i._1, o, Some(i._2))
        case s: String                => SQLFieldSort(s, o, None)
      }
    }

  def orderBy: PackratParser[SQLOrderBy] = OrderBy.regex ~ rep1sep(sort, separator) ^^ {
    case _ ~ s =>
      SQLOrderBy(s)
  }

}

trait SQLLimitParser {
  self: SQLParser =>

  def limit: PackratParser[SQLLimit] = Limit.regex ~ long ^^ { case _ ~ i =>
    SQLLimit(i.value.toInt)
  }

}
