package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.`type`.SQLTypes
import app.softnetwork.elastic.sql.query._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

object Queries {
  val numericalEq = "SELECT t.col1, t.col2 FROM Table AS t WHERE t.identifier = 1.0"
  val numericalLt = "SELECT * FROM Table WHERE identifier < 1"
  val numericalLe = "SELECT * FROM Table WHERE identifier <= 1"
  val numericalGt = "SELECT * FROM Table WHERE identifier > 1"
  val numericalGe = "SELECT * FROM Table WHERE identifier >= 1"
  val numericalNe = "SELECT * FROM Table WHERE identifier <> 1"
  val literalEq = "SELECT * FROM Table WHERE identifier = 'un'"
  val literalLt = "SELECT * FROM Table WHERE createdAt < 'now-35M/M'"
  val literalLe = "SELECT * FROM Table WHERE createdAt <= 'now-35M/M'"
  val literalGt = "SELECT * FROM Table WHERE createdAt > 'now-35M/M'"
  val literalGe = "SELECT * FROM Table WHERE createdAt >= 'now-35M/M'"
  val literalNe = "SELECT * FROM Table WHERE identifier <> 'un'"
  val boolEq = "SELECT * FROM Table WHERE identifier = true"
  val boolNe = "SELECT * FROM Table WHERE identifier <> false"
  val literalLike = "SELECT * FROM Table WHERE identifier LIKE '%u_n%'"
  val literalRlike = "SELECT * FROM Table WHERE identifier RLIKE '.*u.n.*'"
  val literalNotLike = "SELECT * FROM Table WHERE identifier NOT LIKE '%un%'"
  val betweenExpression = "SELECT * FROM Table WHERE identifier BETWEEN '1' AND '2'"
  val andPredicate = "SELECT * FROM Table WHERE identifier1 = 1 AND identifier2 > 2"
  val orPredicate = "SELECT * FROM Table WHERE identifier1 = 1 OR identifier2 > 2"
  val leftPredicate =
    "SELECT * FROM Table WHERE (identifier1 = 1 AND identifier2 > 2) OR identifier3 = 3"
  val rightPredicate =
    "SELECT * FROM Table WHERE identifier1 = 1 AND (identifier2 > 2 OR identifier3 = 3)"
  val predicates =
    "SELECT * FROM Table WHERE (identifier1 = 1 AND identifier2 > 2) OR (identifier3 = 3 AND identifier4 = 4)"
  val nestedPredicate =
    "SELECT * FROM Table JOIN UNNEST(Table.nested) AS nested WHERE identifier1 = 1 AND (nested.identifier2 > 2 OR nested.identifier3 = 3)"
  val nestedCriteria =
    "SELECT * FROM Table JOIN UNNEST(Table.nested) AS nested WHERE identifier1 = 1 AND nested.identifier3 = 3"
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
    "SELECT * FROM Table JOIN UNNEST(Table.ciblage) AS ciblage WHERE ciblage.Archivage_CreationDate BETWEEN 'now-3M/M' AND 'now' AND ciblage.statutComportement = 1"
  val COUNT = "SELECT COUNT(t.id) AS c1 FROM Table AS t WHERE t.nom = 'Nom'"
  val countDistinct = "SELECT COUNT(distinct t.id) AS c2 FROM Table AS t WHERE t.nom = 'Nom'"
  val countNested =
    "SELECT COUNT(email.value) AS email FROM crmgp WHERE profile.postalCode IN ('75001','75002')"
  val isNull = "SELECT * FROM Table WHERE identifier is null"
  val isNotNull = "SELECT * FROM Table WHERE identifier is NOT null"
  val geoDistanceCriteria =
    "SELECT * FROM Table WHERE ST_DISTANCE(profile.location, POINT(-70.0, 40.0)) <= 5 km"
  val except = "SELECT * except(col1,col2) FROM Table"
  val matchCriteria =
    "SELECT * FROM Table WHERE match (identifier1,identifier2,identifier3) against ('value')"
  val groupBy =
    "SELECT identifier, COUNT(identifier2) FROM Table WHERE identifier2 is NOT null GROUP BY identifier"
  val orderBy = "SELECT * FROM Table ORDER BY identifier DESC"
  val limit = "SELECT * FROM Table LIMIT 10 OFFSET 2"
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
    "SELECT CURRENT_TIMESTAMP() - INTERVAL 3 DAY AS ct, CURRENT_DATE AS cd, CURRENT_TIME AS t, NOW AS n, TODAY as td FROM dual"
  val fieldsWithInterval: String =
    "SELECT createdAt - INTERVAL 35 MINUTE AS ct, identifier FROM Table"
  val filterWithDateTimeAndInterval: String =
    "SELECT * FROM Table WHERE createdAt < CURRENT_TIMESTAMP() AND createdAt >= CURRENT_TIMESTAMP() - INTERVAL 10 DAY"
  val filterWithDateAndInterval: String =
    "SELECT * FROM Table WHERE createdAt < CURRENT_DATE AND createdAt >= CURRENT_DATE() - INTERVAL 10 DAY"
  val filterWithTimeAndInterval: String =
    "SELECT * FROM Table WHERE createdAt < CURRENT_TIME AND createdAt >= CURRENT_TIME() - INTERVAL 10 MINUTE"
  val groupByWithHavingAndDateTimeFunctions: String =
    """SELECT COUNT(CustomerID) AS cnt, City, Country, MAX(createdAt) AS lastSeen
      |FROM Table
      |GROUP BY Country, City
      |HAVING Country <> 'USA' AND City != 'Berlin' AND COUNT(CustomerID) > 1 AND MAX(createdAt) > NOW - INTERVAL 7 DAY
      |ORDER BY Country ASC""".stripMargin
      .replaceAll("\n", " ")
  val dateParse =
    "SELECT identifier, COUNT(identifier2) AS ct, MAX(DATE_PARSE(createdAt, '%Y-%m-%d')) AS lastSeen FROM Table WHERE identifier2 is NOT null GROUP BY identifier ORDER BY COUNT(identifier2) DESC"
  val dateTimeParse: String =
    """SELECT identifier, COUNT(identifier2) AS ct,
      |MAX(
      |year(
      |date_trunc(
      |datetime_parse(
      |createdAt,
      |'%Y-%m-%d %H:%i:%s.%f'
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
    "SELECT MAX(date_diff(datetime_parse(createdAt, '%Y-%m-%d %H:%i:%s.%f'), updatedAt, DAY)) AS max_diff FROM Table GROUP BY identifier"

  val dateFormat: String =
    """SELECT
      |identifier,
      |date_format(date_trunc(lastUpdated, YEAR), '%Y-%m-%d') AS y,
      |date_format(date_trunc(lastUpdated, QUARTER), '%Y-%m-%d') AS q,
      |date_format(date_trunc(lastUpdated, MONTH), '%Y-%m-%d') AS m,
      |date_format(date_trunc(lastUpdated, WEEK), '%Y-%m-%d') AS w,
      |date_format(date_trunc(lastUpdated, DAY), '%Y-%m-%d') AS d,
      |date_format(date_trunc(lastUpdated, HOUR), '%Y-%m-%d') AS h,
      |date_format(date_trunc(lastUpdated, MINUTE), '%Y-%m-%d') AS m2,
      |date_format(date_trunc(lastUpdated, SECOND), '%Y-%m-%d') AS lastSeen
      |FROM Table
      |WHERE identifier2 IS NOT NULL""".stripMargin.replaceAll("\n", " ")
  val dateTimeFormat =
    "SELECT identifier, datetime_format(date_trunc(lastUpdated, MONTH), '%Y-%m-%d %H:%i:%s') AS lastSeen FROM Table WHERE identifier2 is NOT null"
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
    "SELECT COALESCE(NULLIF(createdAt, DATE_PARSE('2025-09-11', '%Y-%m-%d') - INTERVAL 2 DAY), CURRENT_DATE) AS c, identifier FROM Table"
  val conversion: String =
    "SELECT TRY_CAST(COALESCE(NULLIF(createdAt, DATE_PARSE('2025-09-11', '%Y-%m-%d')), CURRENT_DATE - INTERVAL 2 HOUR) BIGINT) AS c, CONVERT(CURRENT_TIMESTAMP, BIGINT) AS c2, CURRENT_TIMESTAMP::DATE AS c3, '125'::BIGINT AS c4, '2025-09-11'::DATE AS c5, identifier FROM Table"
  val allCasts =
    "SELECT CAST(identifier AS int) AS c1, CAST(identifier AS bigint) AS c2, CAST(identifier AS double) AS c3, CAST(identifier AS real) AS c4, CAST(identifier AS boolean) AS c5, CAST(identifier AS char) AS c6, CAST(identifier AS varchar) AS c7, CAST(createdAt AS date) AS c8, CAST(createdAt AS time) AS c9, CAST(createdAt AS datetime) AS c10, CAST(createdAt AS timestamp) AS c11, CAST(identifier AS smallint) AS c12, CAST(identifier AS tinyint) AS c13 FROM Table"
  val caseWhen: String =
    "SELECT CASE WHEN lastUpdated > NOW - INTERVAL 7 DAY THEN lastUpdated WHEN ISNOTNULL(lastSeen) THEN lastSeen + INTERVAL 2 DAY ELSE createdAt END AS c, identifier FROM Table"
  val caseWhenExpr: String =
    "SELECT CASE CURRENT_DATE - INTERVAL 7 DAY WHEN CAST(lastUpdated AS date) - INTERVAL 3 DAY THEN lastUpdated WHEN lastSeen THEN lastSeen + INTERVAL 2 DAY ELSE createdAt END AS c, identifier FROM Table"

  val extract: String =
    "SELECT EXTRACT(DAY FROM createdAt) AS dom, EXTRACT(WEEKDAY FROM createdAt) AS dow, EXTRACT(YEARDAY FROM createdAt) AS doy, EXTRACT(MONTH FROM createdAt) AS m, EXTRACT(YEAR FROM createdAt) AS y, EXTRACT(HOUR FROM createdAt) AS h, EXTRACT(MINUTE FROM createdAt) AS minutes, EXTRACT(SECOND FROM createdAt) AS s, EXTRACT(NANOSECOND FROM createdAt) AS nano, EXTRACT(MICROSECOND FROM createdAt) AS micro, EXTRACT(MILLISECOND FROM createdAt) AS milli, EXTRACT(EPOCHDAY FROM createdAt) AS epoch, EXTRACT(OFFSET_SECONDS FROM createdAt) AS off, EXTRACT(WEEK FROM createdAt) AS w, EXTRACT(QUARTER FROM createdAt) AS q FROM Table"

  val arithmetic: String =
    "SELECT identifier, identifier + 1 AS add, identifier - 1 AS sub, identifier * 2 AS mul, identifier / 2 AS div, identifier % 2 AS mod, (identifier * identifier2) - 10 FROM Table WHERE identifier * (EXTRACT(year FROM CURRENT_DATE) - 10) > 10000"

  val mathematical: String =
    "SELECT identifier, (ABS(identifier) + 1.0) * 2, CEIL(identifier), FLOOR(identifier), SQRT(identifier), EXP(identifier), LOG(identifier), LOG10(identifier), POW(identifier, 3), ROUND(identifier), ROUND(identifier, 2), SIGN(identifier), COS(identifier), ACOS(identifier), SIN(identifier), ASIN(identifier), TAN(identifier), ATAN(identifier), ATAN2(identifier, 3.0) FROM Table WHERE SQRT(identifier) > 100.0"

  val string: String =
    "SELECT identifier, LENGTH(identifier2) AS len, LOWER(identifier2) AS low, UPPER(identifier2) AS upp, SUBSTRING(identifier2, 1, 3) AS sub, TRIM(identifier2) AS tr, LTRIM(identifier2) AS ltr, RTRIM(identifier2) AS rtr, CONCAT(identifier2, '_test', 1) AS con, LEFT(identifier2, 5) AS l, RIGHT(identifier2, 3) AS r, REPLACE(identifier2, 'el', 'le') AS rep, REVERSE(identifier2) AS rev, POSITION('soft', identifier2, 1) AS pos, REGEXP_LIKE(identifier2, 'soft', 'im') AS reg FROM Table WHERE LENGTH(TRIM(identifier2)) > 10"

  val topHits: String =
    "SELECT department AS dept, firstName, CAST(hire_date AS DATE) AS hire_date, COUNT(DISTINCT salary) AS cnt, FIRST_VALUE(salary) OVER (PARTITION BY department ORDER BY hire_date ASC) AS first_salary, LAST_VALUE(salary) OVER (PARTITION BY department ORDER BY hire_date ASC) AS last_salary, ARRAY_AGG(name) OVER (PARTITION BY department ORDER BY hire_date ASC, salary DESC) AS employees FROM emp LIMIT 1000"

  val lastDay: String =
    "SELECT LAST_DAY(CAST(createdAt AS DATE)) AS ld, identifier FROM Table WHERE EXTRACT(DAY FROM LAST_DAY(CURRENT_TIMESTAMP)) > 28"

  val extractors: String =
    "SELECT YEAR(createdAt) AS y, MONTH(createdAt) AS m, WEEKDAY(createdAt) AS wd, YEARDAY(createdAt) AS yd, DAY(createdAt) AS d, HOUR(createdAt) AS h, MINUTE(createdAt) AS minutes, SECOND(createdAt) AS s, NANOSECOND(createdAt) AS nano, MICROSECOND(createdAt) AS micro, MILLISECOND(createdAt) AS milli, EPOCHDAY(createdAt) AS epoch, OFFSET_SECONDS(createdAt) AS off, WEEK(createdAt) AS w, QUARTER(createdAt) AS q FROM Table"

  val geoDistance =
    "SELECT ST_DISTANCE(POINT(-70.0, 40.0), toLocation) AS d1, ST_DISTANCE(fromLocation, POINT(-70.0, 40.0)) AS d2, ST_DISTANCE(POINT(-70.0, 40.0), POINT(0.0, 0.0)) AS d3 FROM Table WHERE ST_DISTANCE(POINT(-70.0, 40.0), toLocation) BETWEEN 4000 km AND 5000 km AND ST_DISTANCE(fromLocation, toLocation) < 2000 km AND ST_DISTANCE(POINT(-70.0, 40.0), POINT(-70.0, 40.0)) < 1000 km"

  val betweenTemporal =
    "SELECT * FROM Table WHERE createdAt BETWEEN CURRENT_DATE - INTERVAL 1 MONTH AND CURRENT_DATE AND lastUpdated BETWEEN LAST_DAY('2025-09-11'::DATE) AND DATE_TRUNC(CURRENT_TIMESTAMP, DAY)"

  val nestedOfNested =
    "SELECT matched_comments.author AS comment_authors, matched_comments.comments AS comments, matched_replies.reply_author, matched_replies.reply_text FROM blogs AS blogs JOIN UNNEST(blogs.comments) AS matched_comments JOIN UNNEST(matched_comments.replies) AS matched_replies WHERE MATCH (matched_comments.content) AGAINST ('Nice') AND matched_replies.lastUpdated < LAST_DAY('2025-09-10'::DATE) LIMIT 5" // GROUP BY 1

  val predicateWithDistinctNested =
    "SELECT matched_comments.author AS comment_authors, matched_comments.comments AS comments, matched_replies.reply_author, matched_replies.reply_text FROM blogs AS blogs JOIN UNNEST(blogs.comments) AS matched_comments JOIN UNNEST(blogs.replies) AS matched_replies WHERE MATCH (matched_comments.content) AGAINST ('Nice') AND NOT matched_replies.lastUpdated < LAST_DAY('2025-09-10'::DATE) LIMIT 5" // GROUP BY 1

  val nestedWithoutCriteria =
    "SELECT matched_comments.author AS comment_authors, matched_comments.comments AS comments, matched_replies.reply_author, matched_replies.reply_text FROM blogs AS blogs JOIN UNNEST(blogs.comments) AS matched_comments JOIN UNNEST(matched_comments.replies) AS matched_replies WHERE blogs.lastUpdated::DATE < CURRENT_DATE LIMIT 5" // GROUP BY 1

  val determinationOfTheAggregationContext: String =
    """SELECT AVG(blogs.popularity) AS avg_popularity,
      |AVG(comments.likes) AS avg_comment_likes
      |FROM blogs
      |JOIN UNNEST(blogs.comments) AS comments""".stripMargin.replaceAll("\n", " ")

  val aggregationWithNestedOfNestedContext: String =
    """SELECT AVG(replies.likes) AS avg_reply_likes
      |FROM blogs
      |JOIN UNNEST(blogs.comments) AS comments
      |JOIN UNNEST(comments.replies) AS replies""".stripMargin.replaceAll("\n", " ")

  val whereFiltersAccordingToScope: String =
    """SELECT COUNT(comments.id) AS nb_comments
      |FROM blogs
      |JOIN UNNEST(blogs.comments) AS comments
      |WHERE blogs.status = 'active'
      |AND comments.sentiment = 'positive'""".stripMargin.replaceAll("\n", " ")
}

/** Created by smanciot on 15/02/17.
  */
class ParserSpec extends AnyFlatSpec with Matchers {

  import Queries._

  "Parser" should "parse numerical EQ" in {
    val result = Parser(numericalEq)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(numericalEq) shouldBe true
  }

  it should "parse numerical NE" in {
    val result = Parser(numericalNe)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(numericalNe) shouldBe true
  }

  it should "parse numerical LT" in {
    val result = Parser(numericalLt)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(numericalLt) shouldBe true
  }

  it should "parse numerical LE" in {
    val result = Parser(numericalLe)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(numericalLe) shouldBe true
  }

  it should "parse numerical GT" in {
    val result = Parser(numericalGt)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(numericalGt) shouldBe true
  }

  it should "parse numerical GE" in {
    val result = Parser(numericalGe)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(numericalGe) shouldBe true
  }

  it should "parse literal EQ" in {
    val result = Parser(literalEq)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(literalEq) shouldBe true
  }

  it should "parse literal LIKE" in {
    val result = Parser(literalLike)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(literalLike) shouldBe true
  }

  it should "parse literal RLIKE" in {
    val result = Parser(literalRlike)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(literalRlike) shouldBe true
  }

  it should "parse literal NOT LIKE" in {
    val result = Parser(literalNotLike)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(literalNotLike) shouldBe true
  }

  it should "parse literal NE" in {
    val result = Parser(literalNe)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(literalNe) shouldBe true
  }

  it should "parse literal LT" in {
    val result = Parser(literalLt)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(literalLt) shouldBe true
  }

  it should "parse literal LE" in {
    val result = Parser(literalLe)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(literalLe) shouldBe true
  }

  it should "parse literal GT" in {
    val result = Parser(literalGt)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(literalGt) shouldBe true
  }

  it should "parse literal GE" in {
    val result = Parser(literalGe)
    result.toOption.map(_.sql).getOrElse("") equalsIgnoreCase literalGe
  }

  it should "parse boolean EQ" in {
    val result = Parser(boolEq)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(boolEq) shouldBe true
  }

  it should "parse boolean NE" in {
    val result = Parser(boolNe)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(boolNe) shouldBe true
  }

  it should "parse BETWEEN" in {
    val result = Parser(betweenExpression)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(betweenExpression) shouldBe true
  }

  it should "parse AND predicate" in {
    val result = Parser(andPredicate)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(andPredicate) shouldBe true
  }

  it should "parse OR predicate" in {
    val result = Parser(orPredicate)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(orPredicate) shouldBe true
  }

  it should "parse left predicate with criteria" in {
    val result = Parser(leftPredicate)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(leftPredicate) shouldBe true
  }

  it should "parse right predicate with criteria" in {
    val result = Parser(rightPredicate)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(rightPredicate) shouldBe true
  }

  it should "parse multiple predicates" in {
    val result = Parser(predicates)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(predicates) shouldBe true
  }

  it should "parse nested predicate" in {
    val result = Parser(nestedPredicate)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe nestedPredicate
  }

  it should "parse nested criteria" in {
    val result = Parser(nestedCriteria)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(nestedCriteria) shouldBe true
  }

  it should "parse child predicate" in {
    val result = Parser(childPredicate)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(childPredicate) shouldBe true
  }

  it should "parse child criteria" in {
    val result = Parser(childCriteria)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(childCriteria) shouldBe true
  }

  it should "parse parent predicate" in {
    val result = Parser(parentPredicate)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(parentPredicate) shouldBe true
  }

  it should "parse parent criteria" in {
    val result = Parser(parentCriteria)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(parentCriteria) shouldBe true
  }

  it should "parse IN literal expression" in {
    val result = Parser(inLiteralExpression)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(inLiteralExpression) shouldBe true
  }

  it should "parse IN numerical expression with Int values" in {
    val result = Parser(inNumericalExpressionWithIntValues)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(inNumericalExpressionWithIntValues) shouldBe true
  }

  it should "parse IN numerical expression with Double values" in {
    val result = Parser(inNumericalExpressionWithDoubleValues)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(inNumericalExpressionWithDoubleValues) shouldBe true
  }

  it should "parse NOT IN literal expression" in {
    val result = Parser(notInLiteralExpression)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(notInLiteralExpression) shouldBe true
  }

  it should "parse NOT IN numerical expression with Int values" in {
    val result = Parser(notInNumericalExpressionWithIntValues)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(notInNumericalExpressionWithIntValues) shouldBe true
  }

  it should "parse NOT IN numerical expression with Double values" in {
    val result = Parser(notInNumericalExpressionWithDoubleValues)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(notInNumericalExpressionWithDoubleValues) shouldBe true
  }

  it should "parse nested with BETWEEN" in {
    val result = Parser(nestedWithBetween)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(nestedWithBetween) shouldBe true
  }

  it should "parse COUNT" in {
    val result = Parser(COUNT)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(COUNT) shouldBe true
  }

  it should "parse DISTINCT COUNT" in {
    val result = Parser(countDistinct)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(countDistinct) shouldBe true
  }

  it should "parse COUNT with nested criteria" in {
    val result = Parser(countNested)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(countNested) shouldBe true
  }

  it should "parse IS NULL" in {
    val result = Parser(isNull)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(isNull) shouldBe true
  }

  it should "parse IS NOT NULL" in {
    val result = Parser(isNotNull)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(isNotNull) shouldBe true
  }

  it should "parse geo distance criteria" in {
    val result = Parser(geoDistanceCriteria)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe geoDistanceCriteria
  }

  it should "parse EXCEPT fields" in {
    val result = Parser(except)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(except) shouldBe true
  }

  it should "parse MATCH criteria" in {
    val result = Parser(matchCriteria)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(matchCriteria) shouldBe true
  }

  it should "parse GROUP BY" in {
    val result = Parser(groupBy)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(groupBy) shouldBe true
  }

  it should "parse ORDER BY" in {
    val result = Parser(orderBy)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(orderBy) shouldBe true
  }

  it should "parse LIMIT" in {
    val result = Parser(limit)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe limit
  }

  it should "parse GROUP BY with ORDER BY and LIMIT" in {
    val result = Parser(groupByWithOrderByAndLimit)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(groupByWithOrderByAndLimit) shouldBe true
  }

  it should "parse GROUP BY with HAVING" in {
    val result = Parser(groupByWithHaving)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(groupByWithHaving) shouldBe true
  }

  it should "parse date time fields" in {
    val result = Parser(dateTimeWithIntervalFields)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(dateTimeWithIntervalFields) shouldBe true
  }

  it should "parse fields with INTERVAL" in {
    val result = Parser(fieldsWithInterval)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(fieldsWithInterval) shouldBe true
  }

  it should "parse filter with date time and INTERVAL" in {
    val result = Parser(filterWithDateTimeAndInterval)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(filterWithDateTimeAndInterval) shouldBe true
  }

  it should "parse filter with date and interval" in {
    val result = Parser(filterWithDateAndInterval)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(filterWithDateAndInterval) shouldBe true
  }

  it should "parse filter with time and interval" in {
    val result = Parser(filterWithTimeAndInterval)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(filterWithTimeAndInterval) shouldBe true
  }

  it should "parse GROUP BY with HAVING and date time functions" in {
    val result = Parser(groupByWithHavingAndDateTimeFunctions)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe groupByWithHavingAndDateTimeFunctions
  }

  it should "parse date_parse function" in {
    val result = Parser(dateParse)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(dateParse) shouldBe true
  }

  it should "parse date_parse_time function" in {
    val result = Parser(dateTimeParse)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(dateTimeParse) shouldBe true
  }

  it should "parse date_diff function" in {
    val result = Parser(dateDiff)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(dateDiff) shouldBe true
  }

  it should "parse date_diff function with aggregation" in {
    val result = Parser(aggregationWithDateDiff)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(aggregationWithDateDiff) shouldBe true
  }

  it should "parse format_date function" in {
    val result = Parser(dateFormat)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(dateFormat) shouldBe true
  }

  it should "parse format_datetime function" in {
    val result = Parser(dateTimeFormat)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(dateTimeFormat) shouldBe true
  }

  it should "parse date_add function" in {
    val result = Parser(dateAdd)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(dateAdd) shouldBe true
  }

  it should "parse date_sub function" in {
    val result = Parser(dateSub)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(dateSub) shouldBe true
  }

  it should "parse datetime_add function" in {
    val result = Parser(dateTimeAdd)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(dateTimeAdd) shouldBe true
  }

  it should "parse datetime_sub function" in {
    val result = Parser(dateTimeSub)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(dateTimeSub) shouldBe true
  }

  it should "parse ISNULL function" in {
    val result = Parser(isnull)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(isnull) shouldBe true
  }

  it should "parse ISNOTNULL function" in {
    val result = Parser(isnotnull)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(isnotnull) shouldBe true
  }

  it should "parse ISNULL criteria" in {
    val result = Parser(isNullCriteria)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(isNullCriteria) shouldBe true
  }

  it should "parse ISNOTNULL criteria" in {
    val result = Parser(isNotNullCriteria)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(isNotNullCriteria) shouldBe true
  }

  it should "parse COALESCE function" in {
    val result = Parser(coalesce)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(coalesce) shouldBe true
  }

  it should "parse NULLIF function" in {
    val result = Parser(nullif)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(nullif) shouldBe true
  }

  it should "parse conversion function" in {
    val result = Parser(conversion)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe conversion
  }

  it should "parse all casts function" in {
    val result = Parser(allCasts)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(allCasts) shouldBe true
  }

  it should "parse CASE WHEN expression" in {
    val result = Parser(caseWhen)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(caseWhen) shouldBe true
  }

  it should "parse CASE WHEN with expression" in {
    val result = Parser(caseWhenExpr)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(caseWhenExpr) shouldBe true
  }

  it should "parse EXTRACT function" in {
    val result = Parser(extract)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe extract
  }

  it should "parse arithmetic expressions" in {
    val result = Parser(arithmetic)
    result.toOption
      .map(_.sql)
      .getOrElse("")
      .equalsIgnoreCase(arithmetic) shouldBe true
  }

  it should "parse mathematical functions" in {
    val result = Parser(mathematical)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe mathematical
  }

  it should "parse string functions" in {
    val result = Parser(string)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe string
  }

  it should "parse top hits functions" in {
    val result = Parser(topHits)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe topHits
  }

  it should "parse last_day function" in {
    val result = Parser(lastDay)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe lastDay
  }

  it should "parse all date extractors" in {
    val result = Parser(extractors)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe extractors
  }

  it should "parse geo distance field" in {
    val result = Parser(geoDistance)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe geoDistance
  }

  it should "parse BETWEEN with temporal fields" in {
    val result = Parser(betweenTemporal)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe betweenTemporal
  }

  it should "parse nested of nested" in {
    val result = Parser(nestedOfNested)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe nestedOfNested
  }

  it should "parse predicate with distinct nested" in {
    val result = Parser(predicateWithDistinctNested)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe predicateWithDistinctNested
  }

  it should "parse nested without criteria" in {
    val result = Parser(nestedWithoutCriteria)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe nestedWithoutCriteria
  }

  it should "determine the aggregation context" in {
    val result = Parser(determinationOfTheAggregationContext)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe determinationOfTheAggregationContext
  }

  it should "parse aggregation with nested of nested context" in {
    val result = Parser(aggregationWithNestedOfNestedContext)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe aggregationWithNestedOfNestedContext
  }

  it should "parse where filters according to scope" in {
    val result = Parser(whereFiltersAccordingToScope)
    result.toOption
      .map(_.sql)
      .getOrElse("") shouldBe whereFiltersAccordingToScope
  }

  // --- DDL ---

  it should "parse CREATE TABLE if not exists" in {
    val sql = "CREATE TABLE IF NOT EXISTS users (id INT NOT NULL, name VARCHAR DEFAULT 'anonymous')"
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case CreateTable("users", Right(cols), true, false) =>
        cols.map(_.name) should contain allOf ("id", "name")
        cols.find(_.name == "id").get.notNull shouldBe true
        cols.find(_.name == "name").get.defaultValue.map(_.value) shouldBe Some("anonymous")
      case _ => fail("Expected CreateTable")
    }
  }

  it should "parse CREATE OR REPLACE TABLE as select" in {
    val sql = "CREATE OR REPLACE TABLE users AS SELECT id, name FROM accounts"
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case table: CreateTable =>
        println(table.sql)
        table.table shouldBe "users"
        table.ifNotExists shouldBe false
        table.orReplace shouldBe true
        table.columns.map(_.name) should contain allOf ("id", "name")
      case _ => fail("Expected CreateTable")
    }
  }

  it should "parse DROP TABLE if exists" in {
    val sql = "DROP TABLE IF EXISTS users"
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case DropTable("users", ie, _) if ie =>
      case _                               => fail("Expected DropTable")
    }
  }

  it should "parse TRUNCATE TABLE" in {
    val sql = "TRUNCATE TABLE users"
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case TruncateTable("users") =>
      case _                      => fail("Expected TruncateTable")
    }
  }

  it should "parse ALTER TABLE add column if not exists" in {
    val sql =
      """ALTER TABLE users
        |  ADD COLUMN IF NOT EXISTS age INT DEFAULT 0
        |""".stripMargin
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case AlterTable("users", _, stmt) =>
        stmt match {
          case AddColumn(c, ine) if ine =>
            c.name shouldBe "age"
            c.dataType.typeId should include("INT")
            c.defaultValue.map(_.value) shouldBe Some(0L)
          case _ => fail("Expected AddColumn")
        }
      case _ => fail("Expected AlterTable")
    }
  }

  it should "parse ALTER TABLE rename column" in {
    val sql =
      """ALTER TABLE users
        |  RENAME COLUMN name TO full_name
        |""".stripMargin
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case AlterTable("users", _, stmt) =>
        stmt match {
          case RenameColumn(o, n) =>
            o shouldBe "name"
            n shouldBe "full_name"
          case _ => fail("Expected RenameColumn")
        }
      case _ => fail("Expected AlterTable")
    }
  }

  it should "parse ALTER TABLE set column options if exists" in {
    val sql =
      """ALTER TABLE users
        |  ALTER COLUMN IF EXISTS status SET OPTIONS (description = 'a description')
        |""".stripMargin
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case AlterTable("users", _, stmt) =>
        stmt match {
          case AlterColumnOptions(c, d, ie) if ie =>
            c shouldBe "status"
            d.get("description").map(_.value) shouldBe Some("a description")
          case _ => fail("Expected AlterColumnDefault")
        }
      case _ => fail("Expected AlterTable")
    }
  }

  it should "parse ALTER TABLE set column default" in {
    val sql =
      """ALTER TABLE users
        |  ALTER COLUMN status SET DEFAULT 'active'
        |""".stripMargin
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case AlterTable("users", _, stmt) =>
        stmt match {
          case AlterColumnDefault(c, d, _) =>
            c shouldBe "status"
            d.value shouldBe "active"
          case other => fail(s"Expected AlterColumnDefault, got $other")
        }
      case _ => fail("Expected AlterTable")
    }
  }

  it should "parse ALTER TABLE drop column default" in {
    val sql =
      """ALTER TABLE users
        |  ALTER COLUMN status DROP DEFAULT
        |""".stripMargin
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case AlterTable("users", _, stmt) =>
        stmt match {
          case DropColumnDefault(c, _) =>
            c shouldBe "status"
          case other => fail(s"Expected DropColumnDefault, got $other")
        }
      case _ => fail("Expected AlterTable")
    }
  }

  it should "parse ALTER TABLE set column not null" in {
    val sql =
      """ALTER TABLE users
        |  ALTER COLUMN status SET NOT NULL
        |""".stripMargin
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case AlterTable("users", _, stmt) =>
        stmt match {
          case AlterColumnNotNull(c, _) =>
            c shouldBe "status"
          case other => fail(s"Expected AlterColumnNotNull, got $other")
        }
      case _ => fail("Expected AlterTable")
    }
  }

  it should "parse ALTER TABLE drop column not null" in {
    val sql =
      """ALTER TABLE users
        |  ALTER COLUMN status DROP NOT NULL
        |""".stripMargin
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case AlterTable("users", _, stmt) =>
        stmt match {
          case DropColumnNotNull(c, _) =>
            c shouldBe "status"
          case other => fail(s"Expected DropColumnNotNull, got $other")
        }
      case _ => fail("Expected AlterTable")
    }
  }

  it should "parse ALTER TABLE set column type" in {
    val sql =
      """ALTER TABLE users
        |  ALTER COLUMN status SET DATA TYPE BIGINT
        |""".stripMargin
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case AlterTable("users", _, stmt) =>
        stmt match {
          case AlterColumnType(c, d, _) =>
            c shouldBe "status"
            d shouldBe SQLTypes.BigInt
          case other => fail(s"Expected AlterColumnType, got $other")
        }
      case _ => fail("Expected AlterTable")
    }
  }

  it should "parse ALTER TABLE if exists" in {
    val sql =
      """ALTER TABLE IF EXISTS users
        |  ALTER COLUMN status SET DEFAULT 'active'
        |""".stripMargin
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case AlterTable("users", ifExists, stmt) if ifExists =>
        stmt match {
          case AlterColumnDefault(c, d, _) =>
            c shouldBe "status"
            d.value shouldBe "active"
          case other => fail(s"Expected AlterColumnDefault, got $other")
        }
      case _ => fail("Expected AlterTable")
    }
  }

  // --- DML ---

  it should "parse INSERT INTO ... VALUES" in {
    val sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')"
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case Insert("users", cols, Right(values)) =>
        cols should contain inOrder ("id", "name")
        values.map(_.value) should contain inOrder (1, "Alice")
      case _ => fail("Expected Insert with values")
    }
  }

  it should "parse INSERT INTO ... SELECT" in {
    val sql = "INSERT INTO users SELECT id, name FROM old_users"
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case Insert("users", Nil, Left(sel: DqlStatement)) =>
        sel.sql should include("SELECT id, name FROM old_users")
      case _ => fail("Expected Insert with select")
    }
  }

  it should "parse UPDATE" in {
    val sql = "UPDATE users SET name = 'Bob', age = 42 WHERE id = 1"
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case Update("users", values, Some(where)) =>
        values("name").value shouldBe "Bob"
        values("age").value shouldBe 42
        where.sql should include("id = 1")
      case _ => fail("Expected Update")
    }
  }

  it should "parse DELETE" in {
    val sql = "DELETE FROM users WHERE age > 30"
    val result = Parser(sql)
    result.isRight shouldBe true
    val stmt = result.toOption.get
    stmt match {
      case Delete(table, Some(where)) =>
        table.name shouldBe "users"
        where.sql should include("age > 30")
      case _ => fail("Expected Delete")
    }
  }
}
