package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.parser._

import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object Queries {
  val numericalEq = "SELECT t.col1, t.col2 FROM Table AS t WHERE t.identifier = 1.0"
  val numericalLt = "SELECT * FROM Table WHERE identifier < 1"
  val numericalLe = "SELECT * FROM Table WHERE identifier <= 1"
  val numericalGt = "SELECT * FROM Table WHERE identifier > 1"
  val numericalGe = "SELECT * FROM Table WHERE identifier >= 1"
  val numericalNe = "SELECT * FROM Table WHERE identifier <> 1"
  val literalEq = """SELECT * FROM Table WHERE identifier = 'un'"""
  val literalLt = "SELECT * FROM Table WHERE createdAt < 'NOW-35M/M'"
  val literalLe = "SELECT * FROM Table WHERE createdAt <= 'NOW-35M/M'"
  val literalGt = "SELECT * FROM Table WHERE createdAt > 'NOW-35M/M'"
  val literalGe = "SELECT * FROM Table WHERE createdAt >= 'NOW-35M/M'"
  val literalNe = """SELECT * FROM Table WHERE identifier <> 'un'"""
  val boolEq = """SELECT * FROM Table WHERE identifier = true"""
  val boolNe = """SELECT * FROM Table WHERE identifier <> false"""
  val literalLike = """SELECT * FROM Table WHERE identifier LIKE '%u_n%'"""
  val literalRlike = """SELECT * FROM Table WHERE identifier RLIKE '.*u.n.*'"""
  val literalNotLike = """SELECT * FROM Table WHERE identifier NOT LIKE '%un%'"""
  val betweenExpression = """SELECT * FROM Table WHERE identifier BETWEEN '1' AND '2'"""
  val andPredicate = "SELECT * FROM Table WHERE identifier1 = 1 AND identifier2 > 2"
  val orPredicate = "SELECT * FROM Table WHERE identifier1 = 1 OR identifier2 > 2"
  val leftPredicate =
    "SELECT * FROM Table WHERE (identifier1 = 1 AND identifier2 > 2) OR identifier3 = 3"
  val rightPredicate =
    "SELECT * FROM Table WHERE identifier1 = 1 AND (identifier2 > 2 OR identifier3 = 3)"
  val predicates =
    "SELECT * FROM Table WHERE (identifier1 = 1 AND identifier2 > 2) OR (identifier3 = 3 AND identifier4 = 4)"
  val nestedPredicate =
    "SELECT * FROM Table WHERE identifier1 = 1 AND nested(nested.identifier2 > 2 OR nested.identifier3 = 3)"
  val nestedCriteria =
    "SELECT * FROM Table WHERE identifier1 = 1 AND nested(nested.identifier3 = 3)"
  val childPredicate =
    "SELECT * FROM Table WHERE identifier1 = 1 AND child(child.identifier2 > 2 OR child.identifier3 = 3)"
  val childCriteria = "SELECT * FROM Table WHERE identifier1 = 1 AND child(child.identifier3 = 3)"
  val parentPredicate =
    "SELECT * FROM Table WHERE identifier1 = 1 AND parent(parent.identifier2 > 2 OR parent.identifier3 = 3)"
  val parentCriteria =
    "SELECT * FROM Table WHERE identifier1 = 1 AND parent(parent.identifier3 = 3)"
  val inLiteralExpression = "SELECT * FROM Table WHERE identifier IN ('val1','val2','val3')"
  val inNumericalExpressionWithIntValues = "SELECT * FROM Table WHERE identifier IN (1,2,3)"
  val inNumericalExpressionWithDoubleValues =
    "SELECT * FROM Table WHERE identifier IN (1.0,2.1,3.4)"
  val notInLiteralExpression =
    "SELECT * FROM Table WHERE identifier NOT IN ('val1','val2','val3')"
  val notInNumericalExpressionWithIntValues = "SELECT * FROM Table WHERE identifier NOT IN (1,2,3)"
  val notInNumericalExpressionWithDoubleValues =
    "SELECT * FROM Table WHERE identifier NOT IN (1.0,2.1,3.4)"
  val nestedWithBetween =
    "SELECT * FROM Table WHERE nested(ciblage.Archivage_CreationDate BETWEEN 'NOW-3M/M' AND 'NOW' AND ciblage.statutComportement = 1)"
  val COUNT = "SELECT COUNT(t.id) AS c1 FROM Table AS t WHERE t.nom = 'Nom'"
  val countDistinct = "SELECT COUNT(distinct t.id) AS c2 FROM Table AS t WHERE t.nom = 'Nom'"
  val countNested =
    "SELECT COUNT(email.value) AS email FROM crmgp WHERE profile.postalCode IN ('75001','75002')"
  val isNull = "SELECT * FROM Table WHERE identifier is null"
  val isNotNull = "SELECT * FROM Table WHERE identifier is NOT null"
  val geoDistanceCriteria =
    "SELECT * FROM Table WHERE distance(profile.location,(-70.0,40.0)) <= '5km'"
  val except = "SELECT * except(col1,col2) FROM Table"
  val matchCriteria =
    "SELECT * FROM Table WHERE match (identifier1,identifier2,identifier3) against ('value')"
  val groupBy =
    "SELECT identifier, COUNT(identifier2) FROM Table WHERE identifier2 is NOT null GROUP BY identifier"
  val orderBy = "SELECT * FROM Table ORDER BY identifier DESC"
  val limit = "SELECT * FROM Table limit 10"
  val groupByWithOrderByAndLimit: String =
    """SELECT identifier, COUNT(identifier2)
      |FROM Table
      |WHERE identifier is NOT null
      |GROUP BY identifier
      |ORDER BY identifier2 DESC
      |limit 10""".stripMargin.replaceAll("\n", " ")
  val groupByWithHaving: String =
    """SELECT COUNT(CustomerID) AS cnt, City, Country
      |FROM Customers
      |GROUP BY Country, City
      |HAVING Country <> 'USA' AND City <> 'Berlin' AND COUNT(CustomerID) > 1
      |ORDER BY COUNT(CustomerID) DESC, Country ASC""".stripMargin.replaceAll("\n", " ")
  val dateTimeWithIntervalFields: String =
    "SELECT current_timestamp() - INTERVAL 3 DAY AS ct, CURRENT_DATE AS cd, current_time AS t, NOW AS n FROM dual"
  val fieldsWithInterval: String =
    "SELECT createdAt - INTERVAL 35 MINUTE AS ct, identifier FROM Table"
  val filterWithDateTimeAndInterval: String =
    "SELECT * FROM Table WHERE createdAt < current_timestamp() AND createdAt >= current_timestamp() - INTERVAL 10 DAY"
  val filterWithDateAndInterval: String =
    "SELECT * FROM Table WHERE createdAt < CURRENT_DATE AND createdAt >= CURRENT_DATE() - INTERVAL 10 DAY"
  val filterWithTimeAndInterval: String =
    "SELECT * FROM Table WHERE createdAt < current_time AND createdAt >= current_time() - INTERVAL 10 MINUTE"
  val groupByWithHavingAndDateTimeFunctions: String =
    """SELECT COUNT(CustomerID) AS cnt, City, Country, MAX(createdAt) AS lastSeen
      |FROM Table
      |GROUP BY Country, City
      |HAVING Country <> 'USA' AND City != 'Berlin' AND COUNT(CustomerID) > 1 AND lastSeen > NOW - INTERVAL 7 DAY
      |ORDER BY Country ASC""".stripMargin
      .replaceAll("\n", " ")
  val dateParse =
    "SELECT identifier, COUNT(identifier2) AS ct, MAX(date_parse(createdAt, 'yyyy-MM-dd')) AS lastSeen FROM Table WHERE identifier2 is NOT null GROUP BY identifier ORDER BY COUNT(identifier2) DESC"
  val dateTimeParse: String =
    """SELECT identifier, COUNT(identifier2) AS ct,
      |MAX(
      |year(
      |date_trunc(
      |datetime_parse(
      |createdAt,
      |'yyyy-MM-ddTHH:mm:ssZ'
      |), MINUTE))) AS lastSeen
      |FROM Table
      |WHERE identifier2 is NOT null
      |GROUP BY identifier
      |ORDER BY COUNT(identifier2) DESC""".stripMargin
      .replaceAll("\n", " ")
      .replaceAll("\\( ", "(")
      .replaceAll(" \\)", ")")

  val dateDiff = "SELECT date_diff(createdAt, updatedAt, DAY) AS diff, identifier FROM Table"

  val aggregationWithDateDiff =
    "SELECT MAX(date_diff(datetime_parse(createdAt, 'yyyy-MM-ddTHH:mm:ssZ'), updatedAt, DAY)) AS max_diff FROM Table GROUP BY identifier"

  val dateFormat =
    "SELECT identifier, date_format(date_trunc(lastUpdated, month), 'yyyy-MM-dd') AS lastSeen FROM Table WHERE identifier2 is NOT null"
  val dateTimeFormat =
    "SELECT identifier, datetime_format(date_trunc(lastUpdated, month), 'yyyy-MM-ddThh:mm:ssZ') AS lastSeen FROM Table WHERE identifier2 is NOT null"
  val dateAdd =
    "SELECT identifier, date_add(lastUpdated, INTERVAL 10 DAY) AS lastSeen FROM Table WHERE identifier2 is NOT null"
  val dateSub =
    "SELECT identifier, date_sub(lastUpdated, INTERVAL 10 DAY) AS lastSeen FROM Table WHERE identifier2 is NOT null"
  val dateTimeAdd =
    "SELECT identifier, datetime_add(lastUpdated, INTERVAL 10 DAY) AS lastSeen FROM Table WHERE identifier2 is NOT null"
  val dateTimeSub =
    "SELECT identifier, datetime_sub(lastUpdated, INTERVAL 10 DAY) AS lastSeen FROM Table WHERE identifier2 is NOT null"

  val isnull = "SELECT ISNULL(identifier) AS flag FROM Table"
  val isnotnull = "SELECT identifier, ISNOTNULL(identifier2) AS flag FROM Table"
  val isNullCriteria = "SELECT * FROM Table WHERE ISNULL(identifier)"
  val isNotNullCriteria = "SELECT * FROM Table WHERE ISNOTNULL(identifier)"
  val coalesce: String =
    "SELECT COALESCE(createdAt - INTERVAL 35 MINUTE, CURRENT_DATE) AS c, identifier FROM Table"
  val nullif: String =
    "SELECT COALESCE(nullif(createdAt, date_parse('2025-09-11', 'yyyy-MM-dd') - INTERVAL 2 DAY), CURRENT_DATE) AS c, identifier FROM Table"
  val cast: String =
    "SELECT CAST(COALESCE(nullif(createdAt, date_parse('2025-09-11', 'yyyy-MM-dd')), CURRENT_DATE - INTERVAL 2 hour) bigint) AS c, identifier FROM Table"
  val allCasts =
    "SELECT CAST(identifier AS int) AS c1, CAST(identifier AS bigint) AS c2, CAST(identifier AS double) AS c3, CAST(identifier AS real) AS c4, CAST(identifier AS boolean) AS c5, CAST(identifier AS char) AS c6, CAST(identifier AS varchar) AS c7, CAST(createdAt AS date) AS c8, CAST(createdAt AS time) AS c9, CAST(createdAt AS datetime) AS c10, CAST(createdAt AS timestamp) AS c11, CAST(identifier AS smallint) AS c12, CAST(identifier AS tinyint) AS c13 FROM Table"
  val caseWhen: String =
    "SELECT CASE WHEN lastUpdated > NOW - INTERVAL 7 DAY THEN lastUpdated WHEN ISNOTNULL(lastSeen) THEN lastSeen + INTERVAL 2 DAY ELSE createdAt END AS c, identifier FROM Table"
  val caseWhenExpr: String =
    "SELECT CASE CURRENT_DATE - INTERVAL 7 DAY WHEN CAST(lastUpdated AS date) - INTERVAL 3 DAY THEN lastUpdated WHEN lastSeen THEN lastSeen + INTERVAL 2 DAY ELSE createdAt END AS c, identifier FROM Table"

  val extract: String =
    "SELECT EXTRACT(day_of_month FROM createdAt) AS dom, EXTRACT(day_of_week FROM createdAt) AS dow, EXTRACT(day_of_year FROM createdAt) AS doy, EXTRACT(month_of_year FROM createdAt) AS m, EXTRACT(year FROM createdAt) AS y, EXTRACT(hour_of_day FROM createdAt) AS h, EXTRACT(minute_of_hour FROM createdAt) AS minutes, EXTRACT(second_of_minute FROM createdAt) AS s FROM Table"

  val arithmetic: String =
    "SELECT identifier, identifier + 1 AS add, identifier - 1 AS sub, identifier * 2 AS mul, identifier / 2 AS div, identifier % 2 AS mod, (identifier * identifier2) - 10 FROM Table WHERE identifier * (EXTRACT(year FROM CURRENT_DATE) - 10) > 10000"

  val mathematical: String =
    "SELECT identifier, (ABS(identifier) + 1.0) * 2, CEIL(identifier), FLOOR(identifier), SQRT(identifier), EXP(identifier), LOG(identifier), LOG10(identifier), POW(identifier, 3), ROUND(identifier), ROUND(identifier, 2), SIGN(identifier), COS(identifier), ACOS(identifier), SIN(identifier), ASIN(identifier), TAN(identifier), ATAN(identifier), ATAN2(identifier, 3.0) FROM Table WHERE SQRT(identifier) > 100.0"

  val string: String =
    "SELECT identifier, LENGTH(identifier2) AS l, LOWER(identifier2) AS low, UPPER(identifier2) AS upp, SUBSTRING(identifier2, 1, 3) AS sub, TRIM(identifier2) AS tr, CONCAT(identifier2, '_test', 1) AS con FROM Table WHERE LENGTH(TRIM(identifier2)) > 10"

  val topHits: String =
    "SELECT department AS dept, firstName, CAST(hire_date AS DATE) AS hire_date, COUNT(DISTINCT salary) AS cnt, FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY hire_date ASC) AS first_salary, LAST_VALUE(salary) OVER (PARTITION BY department ORDER BY hire_date ASC) AS last_salary FROM emp"
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

  it should "parse literal LIKE" in {
    val result = Parser(literalLike)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(literalLike) shouldBe true
  }

  it should "parse literal RLIKE" in {
    val result = Parser(literalRlike)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(literalRlike) shouldBe true
  }

  it should "parse literal NOT LIKE" in {
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

  it should "parse BETWEEN" in {
    val result = Parser(betweenExpression)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(betweenExpression) shouldBe true
  }

  it should "parse AND predicate" in {
    val result = Parser(andPredicate)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(andPredicate) shouldBe true
  }

  it should "parse OR predicate" in {
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

  it should "parse NOT in literal expression" in {
    val result = Parser(notInLiteralExpression)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(notInLiteralExpression) shouldBe true
  }

  it should "parse NOT in numerical expression with Int values" in {
    val result = Parser(notInNumericalExpressionWithIntValues)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(notInNumericalExpressionWithIntValues) shouldBe true
  }

  it should "parse NOT in numerical expression with Double values" in {
    val result = Parser(notInNumericalExpressionWithDoubleValues)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(notInNumericalExpressionWithDoubleValues) shouldBe true
  }

  it should "parse nested with BETWEEN" in {
    val result = Parser(nestedWithBetween)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(nestedWithBetween) shouldBe true
  }

  it should "parse COUNT" in {
    val result = Parser(COUNT)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(COUNT) shouldBe true
  }

  it should "parse distinct COUNT" in {
    val result = Parser(countDistinct)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(countDistinct) shouldBe true
  }

  it should "parse COUNT with nested criteria" in {
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

  it should "parse is NOT null" in {
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

  it should "parse GROUP BY" in {
    val result = Parser(groupBy)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(groupBy) shouldBe true
  }

  it should "parse ORDER BY" in {
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

  it should "parse GROUP BY with ORDER BY AND limit" in {
    val result = Parser(groupByWithOrderByAndLimit)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(groupByWithOrderByAndLimit) shouldBe true
  }

  it should "parse GROUP BY with HAVING" in {
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

  it should "parse fields with INTERVAL" in {
    val result = Parser(fieldsWithInterval)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(fieldsWithInterval) shouldBe true
  }

  it should "parse filter with date time AND INTERVAL" in {
    val result = Parser(filterWithDateTimeAndInterval)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(filterWithDateTimeAndInterval) shouldBe true
  }

  it should "parse filter with date AND INTERVAL" in {
    val result = Parser(filterWithDateAndInterval)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(filterWithDateAndInterval) shouldBe true
  }

  it should "parse filter with time AND INTERVAL" in {
    val result = Parser(filterWithTimeAndInterval)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(filterWithTimeAndInterval) shouldBe true
  }

  it should "parse GROUP BY with HAVING AND date time functions" in {
    val result = Parser(groupByWithHavingAndDateTimeFunctions)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("") shouldBe groupByWithHavingAndDateTimeFunctions
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

  it should "parse CASE WHEN expression" in {
    val result = Parser(caseWhen)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("")
      .equalsIgnoreCase(caseWhen) shouldBe true
  }

  it should "parse CASE WHEN with expression" in {
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

  it should "parse top hits functions" in {
    val result = Parser(topHits)
    result.toOption
      .flatMap(_.left.toOption.map(_.sql))
      .getOrElse("") shouldBe topHits
  }

}
