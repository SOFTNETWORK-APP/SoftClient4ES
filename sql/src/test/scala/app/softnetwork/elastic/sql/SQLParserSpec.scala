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
  val literalEq = """select * from Table where identifier = 'un'"""
  val literalLt = "select * from Table where createdAt < 'now-35M/M'"
  val literalLe = "select * from Table where createdAt <= 'now-35M/M'"
  val literalGt = "select * from Table where createdAt > 'now-35M/M'"
  val literalGe = "select * from Table where createdAt >= 'now-35M/M'"
  val literalNe = """select * from Table where identifier <> 'un'"""
  val boolEq = """select * from Table where identifier = true"""
  val boolNe = """select * from Table where identifier <> false"""
  val literalLike = """select * from Table where identifier like '%un%'"""
  val literalNotLike = """select * from Table where identifier not like '%un%'"""
  val betweenExpression = """select * from Table where identifier between '1' and '2'"""
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
  val inLiteralExpression = "select * from Table where identifier in ('val1','val2','val3')"
  val inNumericalExpressionWithIntValues = "select * from Table where identifier in (1,2,3)"
  val inNumericalExpressionWithDoubleValues =
    "select * from Table where identifier in (1.0,2.1,3.4)"
  val notInLiteralExpression =
    "select * from Table where identifier not in ('val1','val2','val3')"
  val notInNumericalExpressionWithIntValues = "select * from Table where identifier not in (1,2,3)"
  val notInNumericalExpressionWithDoubleValues =
    "select * from Table where identifier not in (1.0,2.1,3.4)"
  val nestedWithBetween =
    "select * from Table where nested(ciblage.Archivage_CreationDate between 'now-3M/M' and 'now' and ciblage.statutComportement = 1)"
  val count = "select count(t.id) as c1 from Table as t where t.nom = 'Nom'"
  val countDistinct = "select count(distinct t.id) as c2 from Table as t where t.nom = 'Nom'"
  val countNested =
    "select count(email.value) as email from crmgp where profile.postalCode in ('75001','75002')"
  val isNull = "select * from Table where identifier is null"
  val isNotNull = "select * from Table where identifier is not null"
  val geoDistanceCriteria =
    "select * from Table where distance(profile.location,(-70.0,40.0)) <= '5km'"
  val except = "select * except(col1,col2) from Table"
  val matchCriteria =
    "select * from Table where match (identifier1,identifier2,identifier3) against ('value')"
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
    """select count(CustomerID) as cnt, City, Country
      |from Customers
      |group by Country, City
      |having Country <> 'USA' and City <> 'Berlin' and count(CustomerID) > 1
      |order by count(CustomerID) desc, Country asc""".stripMargin.replaceAll("\n", " ")
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
  val groupByWithHavingAndDateTimeFunctions: String =
    """select count(CustomerID) as cnt, City, Country, max(createdAt) as lastSeen
      |from Table
      |group by Country, City
      |having Country <> 'USA' and City != 'Berlin' and count(CustomerID) > 1 and lastSeen > now - interval 7 day
      |order by Country asc""".stripMargin
      .replaceAll("\n", " ")
  val parseDate =
    "select identifier, count(identifier2) as ct, max(parse_date(createdAt, 'yyyy-MM-dd')) as lastSeen from Table where identifier2 is not null group by identifier order by count(identifier2) desc"
  val parseDateTime: String =
    """select identifier, count(identifier2) as ct,
      |max(
      |year(
      |date_trunc(
      |parse_datetime(
      |createdAt,
      |'yyyy-MM-ddTHH:mm:ssZ'
      |), minute))) as lastSeen
      |from Table
      |where identifier2 is not null
      |group by identifier
      |order by count(identifier2) desc""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\( ", "(")
      .replaceAll(" \\)", ")")

  val dateDiff = "select date_diff(createdAt, updatedAt, day) as diff, identifier from Table"

  val aggregationWithDateDiff =
    "select max(date_diff(parse_datetime(createdAt, 'yyyy-MM-ddTHH:mm:ssZ'), updatedAt, day)) as max_diff from Table group by identifier"

  val formatDate =
    "select identifier, format_date(date_trunc(lastUpdated, month), 'yyyy-MM-dd') as lastSeen from Table where identifier2 is not null"
  val formatDateTime =
    "select identifier, format_datetime(date_trunc(lastUpdated, month), 'yyyy-MM-ddThh:mm:ssZ') as lastSeen from Table where identifier2 is not null"
  val dateAdd =
    "select identifier, date_add(lastUpdated, interval 10 day) as lastSeen from Table where identifier2 is not null"
  val dateSub =
    "select identifier, date_sub(lastUpdated, interval 10 day) as lastSeen from Table where identifier2 is not null"
  val dateTimeAdd =
    "select identifier, datetime_add(lastUpdated, interval 10 day) as lastSeen from Table where identifier2 is not null"
  val dateTimeSub =
    "select identifier, datetime_sub(lastUpdated, interval 10 day) as lastSeen from Table where identifier2 is not null"

  val isnull = "select isnull(identifier) as flag from Table"
  val isnotnull = "select identifier, isnotnull(identifier2) as flag from Table"
  val isNullCriteria = "select * from Table where isnull(identifier)"
  val isNotNullCriteria = "select * from Table where isnotnull(identifier)"
  val coalesce: String =
    "select coalesce(createdAt - interval 35 minute, current_date) as c, identifier from Table"
  val nullif: String =
    "select coalesce(nullif(createdAt, parse_date('2025-09-11', 'yyyy-MM-dd') - interval 2 day), current_date) as c, identifier from Table"
  val cast: String =
    "select cast(coalesce(nullif(createdAt, parse_date('2025-09-11', 'yyyy-MM-dd')), current_date - interval 2 hour) bigint) as c, identifier from Table"
  val allCasts =
    "select cast(identifier as int) as c1, cast(identifier as bigint) as c2, cast(identifier as double) as c3, cast(identifier as real) as c4, cast(identifier as boolean) as c5, cast(identifier as char) as c6, cast(identifier as varchar) as c7, cast(createdAt as date) as c8, cast(createdAt as time) as c9, cast(createdAt as datetime) as c10, cast(createdAt as timestamp) as c11, cast(identifier as smallint) as c12, cast(identifier as tinyint) as c13 from Table"
  val caseWhen: String =
    "select case when lastUpdated > now - interval 7 day then lastUpdated when isnotnull(lastSeen) then lastSeen + interval 2 day else createdAt end as c, identifier from Table"
  val caseWhenExpr: String =
    "select case current_date - interval 7 day when cast(lastUpdated as date) - interval 3 day then lastUpdated when lastSeen then lastSeen + interval 2 day else createdAt end as c, identifier from Table"

  val extract: String =
    "select extract(day from createdAt) as day, extract(month from createdAt) as month, extract(year from createdAt) as year, extract(hour from createdAt) as hour, extract(minute from createdAt) as minute, extract(second from createdAt) as second from Table"
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

  it should "parse group by with having and date time functions" in {
    val result = SQLParser(groupByWithHavingAndDateTimeFunctions)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      groupByWithHavingAndDateTimeFunctions
    )
  }

  it should "parse parse_date function" in {
    val result = SQLParser(parseDate)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      parseDate
    )
  }

  it should "parse parse_date_time function" in {
    val result = SQLParser(parseDateTime)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      parseDateTime
    )
  }

  it should "parse date_diff function" in {
    val result = SQLParser(dateDiff)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      dateDiff
    )
  }

  it should "parse date_diff function with aggregation" in {
    val result = SQLParser(aggregationWithDateDiff)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      aggregationWithDateDiff
    )
  }

  it should "parse format_date function" in {
    val result = SQLParser(formatDate)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      formatDate
    )
  }

  it should "parse format_datetime function" in {
    val result = SQLParser(formatDateTime)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      formatDateTime
    )
  }

  it should "parse date_add function" in {
    val result = SQLParser(dateAdd)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      dateAdd
    )
  }

  it should "parse date_sub function" in {
    val result = SQLParser(dateSub)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      dateSub
    )
  }

  it should "parse datetime_add function" in {
    val result = SQLParser(dateTimeAdd)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      dateTimeAdd
    )
  }

  it should "parse datetime_sub function" in {
    val result = SQLParser(dateTimeSub)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      dateTimeSub
    )
  }

  it should "parse isnull function" in {
    val result = SQLParser(isnull)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      isnull
    )
  }

  it should "parse isnotnull function" in {
    val result = SQLParser(isnotnull)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      isnotnull
    )
  }

  it should "parse isnull criteria" in {
    val result = SQLParser(isNullCriteria)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      isNullCriteria
    )
  }

  it should "parse isnotnull criteria" in {
    val result = SQLParser(isNotNullCriteria)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      isNotNullCriteria
    )
  }

  it should "parse coalesce function" in {
    val result = SQLParser(coalesce)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      coalesce
    )
  }

  it should "parse nullif function" in {
    val result = SQLParser(nullif)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      nullif
    )
  }

  it should "parse cast function" in {
    val result = SQLParser(cast)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      cast
    )
  }

  it should "parse all casts function" in {
    val result = SQLParser(allCasts)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      allCasts
    )
  }

  it should "parse case when expression" in {
    val result = SQLParser(caseWhen)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      caseWhen
    )
  }

  it should "parse case when with expression" in {
    val result = SQLParser(caseWhenExpr)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(caseWhenExpr) shouldBe true
  }

  it should "parse extract function" in {
    val result = SQLParser(extract)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") should ===(
      extract
    )
  }

}
