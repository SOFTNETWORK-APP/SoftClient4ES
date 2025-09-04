package app.softnetwork.elastic.sql

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object Queries {
  val numericalEq = "select t.col1, t.col2 from Table as t where t.identifier = 1.0"
  val numericalLt = "select * from Table where identifier < 1"
  val numericalLe = "select * from Table where identifier <= 1"
  val numericalGt = "select * from Table where identifier > 1"
  val numericalGe = "select * from Table where identifier >= 1"
  val numericalNe = "select * from Table where identifier <> 1"
  val literalEq = """select * from Table where identifier = "un""""
  val literalLt = "select * from Table where createdAt < \"now-35M/M\""
  val literalLe = "select * from Table where createdAt <= \"now-35M/M\""
  val literalGt = "select * from Table where createdAt > \"now-35M/M\""
  val literalGe = "select * from Table where createdAt >= \"now-35M/M\""
  val literalNe = """select * from Table where identifier <> "un""""
  val boolEq = """select * from Table where identifier = true"""
  val boolNe = """select * from Table where identifier <> false"""
  val literalLike = """select * from Table where identifier like "%un%""""
  val literalNotLike = """select * from Table where identifier not like "%un%""""
  val betweenExpression = """select * from Table where identifier between "1" and "2""""
  val andPredicate = "select * from Table where identifier1 = 1 and identifier2 > 2"
  val orPredicate = "select * from Table where identifier1 = 1 or identifier2 > 2"
  val leftPredicate =
    "select * from Table where (identifier1 = 1 and identifier2 > 2) or identifier3 = 3"
  val rightPredicate =
    "select * from Table where identifier1 = 1 and (identifier2 > 2 or identifier3 = 3)"
  val predicates =
    "select * from Table where (identifier1 = 1 and identifier2 > 2) or (identifier3 = 3 and identifier4 = 4)"
  val nestedPredicate =
    "select * from Table where identifier1 = 1 and nested(nested.identifier2 > 2 or nested.identifier3 = 3)"
  val nestedCriteria =
    "select * from Table where identifier1 = 1 and nested(nested.identifier3 = 3)"
  val childPredicate =
    "select * from Table where identifier1 = 1 and child(child.identifier2 > 2 or child.identifier3 = 3)"
  val childCriteria = "select * from Table where identifier1 = 1 and child(child.identifier3 = 3)"
  val parentPredicate =
    "select * from Table where identifier1 = 1 and parent(parent.identifier2 > 2 or parent.identifier3 = 3)"
  val parentCriteria =
    "select * from Table where identifier1 = 1 and parent(parent.identifier3 = 3)"
  val inLiteralExpression = "select * from Table where identifier in (\"val1\",\"val2\",\"val3\")"
  val inNumericalExpressionWithIntValues = "select * from Table where identifier in (1,2,3)"
  val inNumericalExpressionWithDoubleValues =
    "select * from Table where identifier in (1.0,2.1,3.4)"
  val notInLiteralExpression =
    "select * from Table where identifier not in (\"val1\",\"val2\",\"val3\")"
  val notInNumericalExpressionWithIntValues = "select * from Table where identifier not in (1,2,3)"
  val notInNumericalExpressionWithDoubleValues =
    "select * from Table where identifier not in (1.0,2.1,3.4)"
  val nestedWithBetween =
    "select * from Table where nested(ciblage.Archivage_CreationDate between \"now-3M/M\" and \"now\" and ciblage.statutComportement = 1)"
  val count = "select count(t.id) as c1 from Table as t where t.nom = \"Nom\""
  val countDistinct = "select count(distinct t.id) as c2 from Table as t where t.nom = \"Nom\""
  val countNested =
    "select count(email.value) as email from crmgp where profile.postalCode in (\"75001\",\"75002\")"
  val isNull = "select * from Table where identifier is null"
  val isNotNull = "select * from Table where identifier is not null"
  val geoDistanceCriteria =
    "select * from Table where distance(profile.location,(-70.0,40.0)) <= \"5km\""
  val except = "select * except(col1,col2) from Table"
  val matchCriteria =
    "select * from Table where match (identifier1,identifier2,identifier3) against (\"value\")"
  val groupBy =
    "select identifier, count(identifier2) from Table where identifier2 is not null group by identifier"
  val orderBy = "select * from Table order by identifier desc"
  val limit = "select * from Table limit 10"
  val groupByWithOrderByAndLimit: String =
    """select identifier, count(identifier2)
      |from Table
      |where identifier is not null
      |group by identifier
      |order by identifier2 desc
      |limit 10""".stripMargin.replaceAll("\n", " ")
  val groupByWithHaving: String =
    """SELECT COUNT(CustomerID) as cnt, City, Country
      |FROM Customers
      |GROUP BY Country,City
      |HAVING Country <> "USA" AND City <> "Berlin" AND COUNT(CustomerID) > 1
      |ORDER BY COUNT(CustomerID) DESC,Country asc""".stripMargin.replaceAll("\n", " ").toLowerCase
  val dateTimeWithIntervalFields: String =
    "select current_timestamp() - interval 3 day as ct, current_date as cd, current_time as t, now as n from dual"
  val fieldsWithInterval: String =
    "select createdAt - interval 35 minute as ct, identifier from Table"
  val filterWithDateTimeAndInterval: String =
    "select * from Table where createdAt < current_timestamp() and createdAt >= current_timestamp() - interval 10 day"
  val filterWithDateAndInterval: String =
    "select * from Table where createdAt < current_date and createdAt >= current_date() - interval 10 day"
  val filterWithTimeAndInterval: String =
    "select * from Table where createdAt < current_time and createdAt >= current_time() - interval 10 minute"
}

/** Created by smanciot on 15/02/17.
  */
class SQLParserSpec extends AnyFlatSpec with Matchers {

  import Queries._

  "SQLParser" should "parse numerical eq" in {
    val result = SQLParser(numericalEq)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(numericalEq)
  }

  it should "parse numerical ne" in {
    val result = SQLParser(numericalNe)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(numericalNe)
  }

  it should "parse numerical lt" in {
    val result = SQLParser(numericalLt)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(numericalLt)
  }

  it should "parse numerical le" in {
    val result = SQLParser(numericalLe)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(numericalLe)
  }

  it should "parse numerical gt" in {
    val result = SQLParser(numericalGt)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(numericalGt)
  }

  it should "parse numerical ge" in {
    val result = SQLParser(numericalGe)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(numericalGe)
  }

  it should "parse literal eq" in {
    val result = SQLParser(literalEq)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(literalEq)
  }

  it should "parse literal like" in {
    val result = SQLParser(literalLike)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(literalLike)
  }

  it should "parse literal not like" in {
    val result = SQLParser(literalNotLike)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(literalNotLike)
  }

  it should "parse literal ne" in {
    val result = SQLParser(literalNe)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(literalNe)
  }

  it should "parse literal lt" in {
    val result = SQLParser(literalLt)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(literalLt)
  }

  it should "parse literal le" in {
    val result = SQLParser(literalLe)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(literalLe)
  }

  it should "parse literal gt" in {
    val result = SQLParser(literalGt)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(literalGt)
  }

  it should "parse literal ge" in {
    val result = SQLParser(literalGe)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(literalGe)
  }

  it should "parse boolean eq" in {
    val result = SQLParser(boolEq)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(boolEq)
  }

  it should "parse boolean ne" in {
    val result = SQLParser(boolNe)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(boolNe)
  }

  it should "parse between" in {
    val result = SQLParser(betweenExpression)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(betweenExpression)
  }

  it should "parse and predicate" in {
    val result = SQLParser(andPredicate)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(andPredicate)
  }

  it should "parse or predicate" in {
    val result = SQLParser(orPredicate)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(orPredicate)
  }

  it should "parse left predicate with criteria" in {
    val result = SQLParser(leftPredicate)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(leftPredicate)
  }

  it should "parse right predicate with criteria" in {
    val result = SQLParser(rightPredicate)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(rightPredicate)
  }

  it should "parse multiple predicates" in {
    val result = SQLParser(predicates)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(predicates)
  }

  it should "parse nested predicate" in {
    val result = SQLParser(nestedPredicate)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(nestedPredicate)
  }

  it should "parse nested criteria" in {
    val result = SQLParser(nestedCriteria)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(nestedCriteria)
  }

  it should "parse child predicate" in {
    val result = SQLParser(childPredicate)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(childPredicate)
  }

  it should "parse child criteria" in {
    val result = SQLParser(childCriteria)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(childCriteria)
  }

  it should "parse parent predicate" in {
    val result = SQLParser(parentPredicate)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(parentPredicate)
  }

  it should "parse parent criteria" in {
    val result = SQLParser(parentCriteria)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(parentCriteria)
  }

  it should "parse in literal expression" in {
    val result = SQLParser(inLiteralExpression)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      inLiteralExpression
    )
  }

  it should "parse in numerical expression with Int values" in {
    val result = SQLParser(inNumericalExpressionWithIntValues)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      inNumericalExpressionWithIntValues
    )
  }

  it should "parse in numerical expression with Double values" in {
    val result = SQLParser(inNumericalExpressionWithDoubleValues)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      inNumericalExpressionWithDoubleValues
    )
  }

  it should "parse not in literal expression" in {
    val result = SQLParser(notInLiteralExpression)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      notInLiteralExpression
    )
  }

  it should "parse not in numerical expression with Int values" in {
    val result = SQLParser(notInNumericalExpressionWithIntValues)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      notInNumericalExpressionWithIntValues
    )
  }

  it should "parse not in numerical expression with Double values" in {
    val result = SQLParser(notInNumericalExpressionWithDoubleValues)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      notInNumericalExpressionWithDoubleValues
    )
  }

  it should "parse nested with between" in {
    val result = SQLParser(nestedWithBetween)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(nestedWithBetween)
  }

  it should "parse count" in {
    val result = SQLParser(count)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(count)
  }

  it should "parse distinct count" in {
    val result = SQLParser(countDistinct)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(countDistinct)
  }

  it should "parse count with nested criteria" in {
    val result = SQLParser(countNested)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(countNested)
  }

  it should "parse is null" in {
    val result = SQLParser(isNull)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(isNull)
  }

  it should "parse is not null" in {
    val result = SQLParser(isNotNull)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(isNotNull)
  }

  it should "parse geo distance criteria" in {
    val result = SQLParser(geoDistanceCriteria)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      geoDistanceCriteria
    )
  }

  it should "parse except fields" in {
    val result = SQLParser(except)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(except)
  }

  it should "parse match criteria" in {
    val result = SQLParser(matchCriteria)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(matchCriteria)
  }

  it should "parse group by" in {
    val result = SQLParser(groupBy)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(groupBy)
  }

  it should "parse order by" in {
    val result = SQLParser(orderBy)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(orderBy)
  }

  it should "parse limit" in {
    val result = SQLParser(limit)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(limit)
  }

  it should "parse group by with order by and limit" in {
    val result = SQLParser(groupByWithOrderByAndLimit)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      groupByWithOrderByAndLimit
    )
  }

  it should "parse group by with having" in {
    val result = SQLParser(groupByWithHaving)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(groupByWithHaving)
  }

  it should "parse date time fields" in {
    val result = SQLParser(dateTimeWithIntervalFields)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      dateTimeWithIntervalFields
    )
  }

  it should "parse fields with interval" in {
    val result = SQLParser(fieldsWithInterval)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(fieldsWithInterval)
  }

  it should "parse filter with date time and interval" in {
    val result = SQLParser(filterWithDateTimeAndInterval)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      filterWithDateTimeAndInterval
    )
  }

  it should "parse filter with date and interval" in {
    val result = SQLParser(filterWithDateAndInterval)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      filterWithDateAndInterval
    )
  }

  it should "parse filter with time and interval" in {
    val result = SQLParser(filterWithTimeAndInterval)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      filterWithTimeAndInterval
    )
  }
}
