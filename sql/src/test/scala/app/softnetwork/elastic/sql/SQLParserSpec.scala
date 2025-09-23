package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.parser._

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
  val dateParse =
    "select identifier, count(identifier2) as ct, max(date_parse(createdAt, 'yyyy-MM-dd')) as lastSeen from Table where identifier2 is not null group by identifier order by count(identifier2) desc"
  val dateTimeParse: String =
    """select identifier, count(identifier2) as ct,
      |max(
      |year(
      |date_trunc(
      |datetime_parse(
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
    "select max(date_diff(datetime_parse(createdAt, 'yyyy-MM-ddTHH:mm:ssZ'), updatedAt, day)) as max_diff from Table group by identifier"

  val dateFormat =
    "select identifier, date_format(date_trunc(lastUpdated, month), 'yyyy-MM-dd') as lastSeen from Table where identifier2 is not null"
  val dateTimeFormat =
    "select identifier, datetime_format(date_trunc(lastUpdated, month), 'yyyy-MM-ddThh:mm:ssZ') as lastSeen from Table where identifier2 is not null"
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
    "select coalesce(nullif(createdAt, date_parse('2025-09-11', 'yyyy-MM-dd') - interval 2 day), current_date) as c, identifier from Table"
  val cast: String =
    "select cast(coalesce(nullif(createdAt, date_parse('2025-09-11', 'yyyy-MM-dd')), current_date - interval 2 hour) bigint) as c, identifier from Table"
  val allCasts =
    "select cast(identifier as int) as c1, cast(identifier as bigint) as c2, cast(identifier as double) as c3, cast(identifier as real) as c4, cast(identifier as boolean) as c5, cast(identifier as char) as c6, cast(identifier as varchar) as c7, cast(createdAt as date) as c8, cast(createdAt as time) as c9, cast(createdAt as datetime) as c10, cast(createdAt as timestamp) as c11, cast(identifier as smallint) as c12, cast(identifier as tinyint) as c13 from Table"
  val caseWhen: String =
    "select case when lastUpdated > now - interval 7 day then lastUpdated when isnotnull(lastSeen) then lastSeen + interval 2 day else createdAt end as c, identifier from Table"
  val caseWhenExpr: String =
    "select case current_date - interval 7 day when cast(lastUpdated as date) - interval 3 day then lastUpdated when lastSeen then lastSeen + interval 2 day else createdAt end as c, identifier from Table"

  val extract: String =
    "select extract(day from createdAt) as day, extract(month from createdAt) as month, extract(year from createdAt) as year, extract(hour from createdAt) as hour, extract(minute from createdAt) as minute, extract(second from createdAt) as second from Table"

  val arithmetic: String =
    "select identifier, identifier + 1 as add, identifier - 1 as sub, identifier * 2 as mul, identifier / 2 as div, identifier % 2 as mod, (identifier * identifier2) - 10 as group1 from Table where identifier * (extract(year from current_date) - 10) > 10000"

  val mathematical: String =
    "select identifier, (abs(identifier) + 1.0) * 2, ceil(identifier), floor(identifier), sqrt(identifier), exp(identifier), log(identifier), log10(identifier), pow(identifier, 3), round(identifier), round(identifier, 2), sign(identifier), cos(identifier), acos(identifier), sin(identifier), asin(identifier), tan(identifier), atan(identifier), atan2(identifier, 3.0) from Table where sqrt(identifier) > 100.0"

  val string: String =
    "select identifier, length(identifier2) as len, lower(identifier2) as lower, upper(identifier2) as upper, substring(identifier2, 1, 3) as substr, trim(identifier2) as trim, concat(identifier2, '_test', 1) as concat from Table where length(trim(identifier2)) > 10"
}

/** Created by smanciot on 15/02/17.
  */
class SQLParserSpec extends AnyFlatSpec with Matchers {

  import Queries._

  "SQLParser" should "parse numerical eq" in {
    val result = Parser(numericalEq)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(numericalEq) shouldBe true
  }

  it should "parse numerical ne" in {
    val result = Parser(numericalNe)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(numericalNe) shouldBe true
  }

  it should "parse numerical lt" in {
    val result = Parser(numericalLt)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(numericalLt) shouldBe true
  }

  it should "parse numerical le" in {
    val result = Parser(numericalLe)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(numericalLe) shouldBe true
  }

  it should "parse numerical gt" in {
    val result = Parser(numericalGt)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(numericalGt) shouldBe true
  }

  it should "parse numerical ge" in {
    val result = Parser(numericalGe)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(numericalGe) shouldBe true
  }

  it should "parse literal eq" in {
    val result = Parser(literalEq)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(literalEq) shouldBe true
  }

  it should "parse literal like" in {
    val result = Parser(literalLike)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(literalLike) shouldBe true
  }

  it should "parse literal not like" in {
    val result = Parser(literalNotLike)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(literalNotLike) shouldBe true
  }

  it should "parse literal ne" in {
    val result = Parser(literalNe)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(literalNe) shouldBe true
  }

  it should "parse literal lt" in {
    val result = Parser(literalLt)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(literalLt) shouldBe true
  }

  it should "parse literal le" in {
    val result = Parser(literalLe)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(literalLe) shouldBe true
  }

  it should "parse literal gt" in {
    val result = Parser(literalGt)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(literalGt) shouldBe true
  }

  it should "parse literal ge" in {
    val result = Parser(literalGe)
    result.toOption.flatMap(_.left.toOption.map(_.sql)).getOrElse("") equalsIgnoreCase literalGe
  }

  it should "parse boolean eq" in {
    val result = Parser(boolEq)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(boolEq) shouldBe true
  }

  it should "parse boolean ne" in {
    val result = Parser(boolNe)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(boolNe) shouldBe true
  }

  it should "parse between" in {
    val result = Parser(betweenExpression)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(betweenExpression) shouldBe true
  }

  it should "parse and predicate" in {
    val result = Parser(andPredicate)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(andPredicate) shouldBe true
  }

  it should "parse or predicate" in {
    val result = Parser(orPredicate)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(orPredicate) shouldBe true
  }

  it should "parse left predicate with criteria" in {
    val result = Parser(leftPredicate)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(leftPredicate) shouldBe true
  }

  it should "parse right predicate with criteria" in {
    val result = Parser(rightPredicate)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(rightPredicate) shouldBe true
  }

  it should "parse multiple predicates" in {
    val result = Parser(predicates)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(predicates) shouldBe true
  }

  it should "parse nested predicate" in {
    val result = Parser(nestedPredicate)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(nestedPredicate) shouldBe true
  }

  it should "parse nested criteria" in {
    val result = Parser(nestedCriteria)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(nestedCriteria) shouldBe true
  }

  it should "parse child predicate" in {
    val result = Parser(childPredicate)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(childPredicate) shouldBe true
  }

  it should "parse child criteria" in {
    val result = Parser(childCriteria)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(childCriteria) shouldBe true
  }

  it should "parse parent predicate" in {
    val result = Parser(parentPredicate)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(parentPredicate) shouldBe true
  }

  it should "parse parent criteria" in {
    val result = Parser(parentCriteria)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(parentCriteria) shouldBe true
  }

  it should "parse in literal expression" in {
    val result = Parser(inLiteralExpression)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(inLiteralExpression) shouldBe true
  }

  it should "parse in numerical expression with Int values" in {
    val result = Parser(inNumericalExpressionWithIntValues)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(inNumericalExpressionWithIntValues) shouldBe true
  }

  it should "parse in numerical expression with Double values" in {
    val result = Parser(inNumericalExpressionWithDoubleValues)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(inNumericalExpressionWithDoubleValues) shouldBe true
  }

  it should "parse not in literal expression" in {
    val result = Parser(notInLiteralExpression)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(notInLiteralExpression) shouldBe true
  }

  it should "parse not in numerical expression with Int values" in {
    val result = Parser(notInNumericalExpressionWithIntValues)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(notInNumericalExpressionWithIntValues) shouldBe true
  }

  it should "parse not in numerical expression with Double values" in {
    val result = Parser(notInNumericalExpressionWithDoubleValues)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(notInNumericalExpressionWithDoubleValues) shouldBe true
  }

  it should "parse nested with between" in {
    val result = Parser(nestedWithBetween)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(nestedWithBetween) shouldBe true
  }

  it should "parse count" in {
    val result = Parser(count)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(count) shouldBe true
  }

  it should "parse distinct count" in {
    val result = Parser(countDistinct)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(countDistinct) shouldBe true
  }

  it should "parse count with nested criteria" in {
    val result = Parser(countNested)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(countNested) shouldBe true
  }

  it should "parse is null" in {
    val result = Parser(isNull)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(isNull) shouldBe true
  }

  it should "parse is not null" in {
    val result = Parser(isNotNull)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(isNotNull) shouldBe true
  }

  it should "parse geo distance criteria" in {
    val result = Parser(geoDistanceCriteria)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(geoDistanceCriteria) shouldBe true
  }

  it should "parse except fields" in {
    val result = Parser(except)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(except) shouldBe true
  }

  it should "parse match criteria" in {
    val result = Parser(matchCriteria)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(matchCriteria) shouldBe true
  }

  it should "parse group by" in {
    val result = Parser(groupBy)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(groupBy) shouldBe true
  }

  it should "parse order by" in {
    val result = Parser(orderBy)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(orderBy) shouldBe true
  }

  it should "parse limit" in {
    val result = Parser(limit)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(limit) shouldBe true
  }

  it should "parse group by with order by and limit" in {
    val result = Parser(groupByWithOrderByAndLimit)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(groupByWithOrderByAndLimit) shouldBe true
  }

  it should "parse group by with having" in {
    val result = Parser(groupByWithHaving)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(groupByWithHaving) shouldBe true
  }

  it should "parse date time fields" in {
    val result = Parser(dateTimeWithIntervalFields)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(dateTimeWithIntervalFields) shouldBe true
  }

  it should "parse fields with interval" in {
    val result = Parser(fieldsWithInterval)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(fieldsWithInterval) shouldBe true
  }

  it should "parse filter with date time and interval" in {
    val result = Parser(filterWithDateTimeAndInterval)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(filterWithDateTimeAndInterval) shouldBe true
  }

  it should "parse filter with date and interval" in {
    val result = Parser(filterWithDateAndInterval)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(filterWithDateAndInterval) shouldBe true
  }

  it should "parse filter with time and interval" in {
    val result = Parser(filterWithTimeAndInterval)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(filterWithTimeAndInterval) shouldBe true
  }

  it should "parse group by with having and date time functions" in {
    val result = Parser(groupByWithHavingAndDateTimeFunctions)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(groupByWithHavingAndDateTimeFunctions) shouldBe true
  }

  it should "parse date_parse function" in {
    val result = Parser(dateParse)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(dateParse) shouldBe true
  }

  it should "parse date_parse_time function" in {
    val result = Parser(dateTimeParse)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(dateTimeParse) shouldBe true
  }

  it should "parse date_diff function" in {
    val result = Parser(dateDiff)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(dateDiff) shouldBe true
  }

  it should "parse date_diff function with aggregation" in {
    val result = Parser(aggregationWithDateDiff)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(aggregationWithDateDiff) shouldBe true
  }

  it should "parse format_date function" in {
    val result = Parser(dateFormat)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(dateFormat) shouldBe true
  }

  it should "parse format_datetime function" in {
    val result = Parser(dateTimeFormat)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(dateTimeFormat) shouldBe true
  }

  it should "parse date_add function" in {
    val result = Parser(dateAdd)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(dateAdd) shouldBe true
  }

  it should "parse date_sub function" in {
    val result = Parser(dateSub)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(dateSub) shouldBe true
  }

  it should "parse datetime_add function" in {
    val result = Parser(dateTimeAdd)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(dateTimeAdd) shouldBe true
  }

  it should "parse datetime_sub function" in {
    val result = Parser(dateTimeSub)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(dateTimeSub) shouldBe true
  }

  it should "parse isnull function" in {
    val result = Parser(isnull)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(isnull) shouldBe true
  }

  it should "parse isnotnull function" in {
    val result = Parser(isnotnull)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(isnotnull) shouldBe true
  }

  it should "parse isnull criteria" in {
    val result = Parser(isNullCriteria)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(isNullCriteria) shouldBe true
  }

  it should "parse isnotnull criteria" in {
    val result = Parser(isNotNullCriteria)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(isNotNullCriteria) shouldBe true
  }

  it should "parse coalesce function" in {
    val result = Parser(coalesce)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(coalesce) shouldBe true
  }

  it should "parse nullif function" in {
    val result = Parser(nullif)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(nullif) shouldBe true
  }

  it should "parse cast function" in {
    val result = Parser(cast)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(cast) shouldBe true
  }

  it should "parse all casts function" in {
    val result = Parser(allCasts)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(allCasts) shouldBe true
  }

  it should "parse case when expression" in {
    val result = Parser(caseWhen)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(caseWhen) shouldBe true
  }

  it should "parse case when with expression" in {
    val result = Parser(caseWhenExpr)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(caseWhenExpr) shouldBe true
  }

  it should "parse extract function" in {
    val result = Parser(extract)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(extract) shouldBe true
  }

  it should "parse arithmetic expressions" in {
    val result = Parser(arithmetic)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(arithmetic) shouldBe true
  }

  it should "parse mathematical functions" in {
    val result = Parser(mathematical)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(mathematical) shouldBe true
  }

  it should "parse string functions" in {
    val result = Parser(string)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(string) shouldBe true
  }

}
