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

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.schema.Index
import app.softnetwork.elastic.sql.operator.math.ArithmeticExpression
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{Column, Table => SchemaTable}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** The date-arithmetic rules, as the TYPE every `+ - * / %` expression with a temporal operand
  * gets, or the refusal by name it earns -- asked of a generated population once its schema is
  * attached, and NEVER when the statement is parsed: no type is refused before the column types are
  * known (the lead's ruling of 2026-10-05).
  *
  * The expectation is written HERE, from the ruling, and never read from the implementation:
  *
  * | Expression                                                         | Type                            |
  * |:-------------------------------------------------------------------|:--------------------------------|
  * | DATE - DATE                                                        | BIGINT                          |
  * | TIMESTAMP - TIMESTAMP, a DATE and a TIMESTAMP                      | DOUBLE                          |
  * | DATE +/- a whole number                                            | DATE                            |
  * | DATE +/- a fractional number                                       | TIMESTAMP                       |
  * | TIMESTAMP +/- a number                                             | TIMESTAMP                       |
  * | a number + a temporal                                              | as the temporal + it            |
  * | a temporal and NULL (+ or -)                                       | not refused (the value is NULL) |
  * | temporal + temporal, a temporal under * / %, a number - a temporal | refused by name                 |
  * | a temporal and anything that is neither a number nor a temporal    | refused by name                 |
  *
  * What an operand IS for these rules:
  *   - `NOW()` is a DATETIME, which these rules read as an instant, like a TIMESTAMP;
  *   - an aggregate is the metric it produces: `COUNT` a BIGINT, `AVG` a DOUBLE, `SUM` its column's
  *     number, `MIN` / `MAX` what they aggregate;
  *   - `CASE`, `COALESCE`, `GREATEST` and `LEAST` are the least common supertype of their branches
  *     or arguments, `NULLIF` its first argument -- `GREATEST` / `LEAST` over dates included, a
  *     DATE when every argument is one, a TIMESTAMP when they mix;
  *   - a typed NULL is its type (`CAST(NULL AS DATE)` is a DATE), a bare `NULL` has none;
  *   - a string literal that is a number is that number (`'1'`, `'1.5'`); one that spells a date or
  *     a timestamp is, beside a temporal, that temporal read as the other operand's type (`d -
  *     '2024-01-31'` is DATE minus DATE, as PostgreSQL reads it) and a string beside anything else;
  *     any other is a string;
  *   - a `date` field elasticsql never declared (`u`, an index created by a bulk load) holds
  *     instants: a TIMESTAMP, typed so at the declaration seam -- DESCRIBE shows it so too.
  */
class DateArithmeticTypeSpec extends AnyFlatSpec with Matchers {

  private val schema: SchemaTable = SchemaTable(
    "ev",
    columns = List(
      Column("g", SQLTypes.Keyword),
      Column("d", SQLTypes.Date),
      Column("d2", SQLTypes.Date),
      Column("ts", SQLTypes.Timestamp),
      Column("n", SQLTypes.Int),
      Column("x", SQLTypes.Double),
      // an Elasticsearch `date` with no elasticsql declaration (no `_meta` data type), typed at
      // the declaration seam (`schema.IndexField`)
      Index("raw", """{"mappings":{"properties":{"u":{"type":"date"}}},"settings":{}}""").schema
        .find("u")
        .getOrElse(fail("no undeclared column u"))
    )
  )

  private val ddlColumns = "g KEYWORD, d DATE, d2 DATE, ts TIMESTAMP, n INT, x DOUBLE"

  /** What the ruling sees in an operand. */
  private sealed trait Kind
  private case object DateKind extends Kind
  private case object InstantKind extends Kind
  private case object Whole extends Kind
  private case object Fractional extends Kind
  private case object NullKind extends Kind
  private case object TextKind extends Kind

  /** A string literal that spells a date or a timestamp: what it IS depends on the other operand.
    */
  private case object TemporalText extends Kind

  /** One operand of the population: its SQL, what the ruling sees in it, the type name a refusal
    * names it by, whether it is an aggregate, and whether a computed column can hold it (the DDL
    * venue declares every column, so it has no undeclared one, and no aggregate).
    */
  private final case class Operand(
    sql: String,
    kind: Kind,
    typeName: String,
    aggregate: Boolean = false,
    ddl: Boolean = true
  )

  private val operands: Seq[Operand] = Seq(
    Operand("d", DateKind, "DATE"),
    Operand("ts", InstantKind, "TIMESTAMP"),
    Operand("MAX(d)", DateKind, "DATE", aggregate = true, ddl = false),
    Operand("MAX(ts)", InstantKind, "TIMESTAMP", aggregate = true, ddl = false),
    Operand("2", Whole, "BIGINT"),
    Operand("1.5", Fractional, "DOUBLE"),
    Operand("n", Whole, "INT"),
    Operand("x", Fractional, "DOUBLE"),
    Operand("CURRENT_DATE", DateKind, "DATE"),
    Operand("NOW()", InstantKind, "DATETIME"),
    Operand("NULL", NullKind, "NULL"),
    // ---- the fix round's extension (E1) ----
    Operand("u", InstantKind, "TIMESTAMP", ddl = false),
    Operand("MAX(u)", InstantKind, "TIMESTAMP", aggregate = true, ddl = false),
    Operand("CASE WHEN n > 0 THEN d ELSE d2 END", DateKind, "DATE"),
    Operand("COALESCE(d, d2)", DateKind, "DATE"),
    Operand("COALESCE(ts, d)", InstantKind, "TIMESTAMP"),
    Operand("NULLIF(d, d2)", DateKind, "DATE"),
    Operand("GREATEST(n, 2)", Whole, "BIGINT"),
    Operand("LEAST(x, 1.5)", Fractional, "DOUBLE"),
    Operand("AVG(n)", Fractional, "DOUBLE", aggregate = true, ddl = false),
    Operand("SUM(n)", Whole, "INT", aggregate = true, ddl = false),
    Operand("COUNT(d)", Whole, "BIGINT", aggregate = true, ddl = false),
    Operand("CAST(NULL AS DATE)", DateKind, "DATE"),
    Operand("CAST(NULL AS INT)", Whole, "INT"),
    Operand("'1'", Whole, "VARCHAR"),
    Operand("'1.5'", Fractional, "VARCHAR"),
    Operand("'x'", TextKind, "VARCHAR"),
    // ---- fix round 2 (G1): GREATEST / LEAST over dates and timestamps ----
    Operand("GREATEST(d, d2)", DateKind, "DATE"),
    Operand("LEAST(ts, d)", InstantKind, "TIMESTAMP"),
    Operand("GREATEST(MAX(d), MAX(d2))", DateKind, "DATE", aggregate = true, ddl = false),
    // ---- fix round 3 (T1): a string literal that spells a date or a timestamp ----
    Operand("'2024-01-31'", TemporalText, "VARCHAR"),
    Operand("'2024-01-31T10:00:00Z'", TemporalText, "VARCHAR")
  )

  private val operators: Seq[String] = Seq("+", "-", "*", "/", "%")

  private sealed trait Expected
  private final case class Typed(t: SQLType) extends Expected
  private case object NullValued extends Expected
  private case object Refused extends Expected
  private case object NotDateArithmetic extends Expected

  private def temporal(k: Kind): Boolean = k == DateKind || k == InstantKind
  private def number(k: Kind): Boolean = k == Whole || k == Fractional

  private def shifted(t: Kind, n: Kind): Typed =
    Typed(if (t == DateKind && n == Whole) SQLTypes.Date else SQLTypes.Timestamp)

  /** What an operand IS beside the other: a literal that spells a temporal is, beside a temporal,
    * that temporal read as the other's type -- the comparison rule's reading, as PostgreSQL reads
    * an untyped literal -- and a string beside anything else.
    */
  private def beside(k: Kind, other: Kind): Kind =
    if (k != TemporalText) k else if (temporal(other)) other else TextKind

  /** The ruling, restated. */
  private def expected(op: String, left: Kind, right: Kind): Expected = {
    val l = beside(left, right)
    val r = beside(right, left)
    if (!temporal(l) && !temporal(r)) NotDateArithmetic
    else
      op match {
        case "*" | "/" | "%" => Refused
        case "+" =>
          (l, r) match {
            case (t, n) if temporal(t) && number(n) => shifted(t, n)
            case (n, t) if number(n) && temporal(t) => shifted(t, n)
            case (NullKind, _) | (_, NullKind)      => NullValued
            case _                                  => Refused
          }
        case "-" =>
          (l, r) match {
            case (DateKind, DateKind)                 => Typed(SQLTypes.BigInt)
            case (a, b) if temporal(a) && temporal(b) => Typed(SQLTypes.Double)
            case (t, n) if temporal(t) && number(n)   => shifted(t, n)
            case (NullKind, _) | (_, NullKind)        => NullValued
            case _                                    => Refused
          }
      }
  }

  private final case class Case(op: String, l: Operand, r: Operand) {
    val expression: String = s"${l.sql} $op ${r.sql}"
    val outcome: Expected = expected(op, l.kind, r.kind)
    val aggregate: Boolean = l.aggregate || r.aggregate
    def sql: String =
      if (aggregate) s"SELECT g, $expression AS x FROM ev GROUP BY g"
      else s"SELECT $expression AS x FROM ev"
  }

  private val population: Seq[Case] =
    for {
      op <- operators
      l  <- operands
      r  <- operands
    } yield Case(op, l, r)

  /** The refusal names the operator, both operand types and the remedy, and nothing internal. */
  private def namesTheRefusal(c: Case, message: String): Boolean =
    message.contains(s"operator ${c.op} ") &&
    message.contains(s"${c.l.typeName} and ${c.r.typeName}") &&
    message.contains("subtract two dates for the days between them") &&
    message.contains("add or subtract a number of days") &&
    !message.matches("(?s).*(?<![A-Za-z0-9_])#\\d+.*")

  private def dateArithmeticRefusal(message: String): Boolean =
    message.contains("subtract two dates for the days between them")

  /** The verdict of one statement: parse -- which refuses no TYPE -- attach the schema,
    * `validateResolved`, then the type the SELECT item reports -- or why that is not the ruling's
    * answer.
    */
  private def mismatch(c: Case): Option[String] =
    Parser(c.sql) match {
      case Left(err) => Some(s"refused at parse (${err.msg}): no TYPE is refused at parse")
      case Right(ss: SingleSearch) =>
        val resolved = ss.update(Some(schema))
        val item = resolved.select.fields
          .find(_.fieldAlias.exists(_.alias == "x"))
          .getOrElse(fail(s"[${c.sql}] no x item"))
        (c.outcome, resolved.validateResolved()) match {
          case (Refused, Left(message)) =>
            if (namesTheRefusal(c, message)) None else Some(s"refused as [$message]")
          case (Refused, Right(_)) =>
            Some(s"accepted, reported ${item.identifier.reportedType}, expected a refusal")
          case (Typed(t), Right(_)) =>
            val reported = item.identifier.reportedType
            if (reported == t) None else Some(s"reported $reported, expected $t")
          case (NullValued, Right(_)) => None
          case (NotDateArithmetic, Right(_)) =>
            if (ArithmeticExpression.typeErrorsOf(item.identifier).isEmpty) None
            else Some("a date-arithmetic refusal for an expression with no temporal")
          // a type rule other than the date-arithmetic one (`'x' + 2`), asked once types are known
          case (NotDateArithmetic, Left(message)) if !dateArithmeticRefusal(message) => None
          case (other, Left(message)) => Some(s"refused as [$message], expected $other")
        }
      case Right(other) => Some(s"parsed as ${other.getClass.getSimpleName}")
    }

  "every + - * / % with a DATE, TIMESTAMP or DATETIME operand" should
  "get the ruling's type or be refused by name, once its schema is attached -- never at parse" in {
    // Non-vacuity, over the material: every kind on both sides of every operator, and every
    // outcome of the ruling present.
    population.size shouldBe operators.size * operands.size * operands.size
    val outcomes = population.map(_.outcome)
    outcomes.count(_ == Refused) should be > 100
    outcomes.collect { case Typed(t) => t }.toSet shouldBe
    Set(SQLTypes.BigInt, SQLTypes.Double, SQLTypes.Date, SQLTypes.Timestamp)
    outcomes should contain(NullValued)
    outcomes should contain(NotDateArithmetic)

    val mismatches = population.flatMap(c => mismatch(c).map(m => s"[${c.expression}] $m"))
    info(
      s"date arithmetic, query venue: ${population.size} expressions " +
      s"(${outcomes.count(_ == Refused)} refused, ${outcomes.count(_.isInstanceOf[Typed])} typed, " +
      s"${outcomes.count(_ == NullValued)} NULL, ${outcomes.count(_ == NotDateArithmetic)} " +
      s"without a temporal); mismatches: ${mismatches.size}"
    )
    withClue(mismatches.take(30).mkString("\n", "\n", "\n")) {
      mismatches shouldBe empty
    }
  }

  /** The DDL venue, as `CREATE TABLE` runs it: `Parser` -> `CreateTable.validate()` ->
    * `validateScriptReferences` on the RESOLVED table -- its column types are declared in the
    * statement itself. A computed column cannot hold an aggregate, so the population is the
    * row-level operands, and every column is declared.
    */
  private def ddlVerdict(expression: String, dataType: String): Either[String, String] =
    Parser(s"CREATE TABLE ev ($ddlColumns, c $dataType SCRIPT AS ($expression))") match {
      case Right(ct: CreateTable) =>
        Right(
          ct.schema.columns
            .find(_.name == "c")
            .flatMap(_.script)
            .map(_.source)
            .getOrElse(fail(s"no script processor for [$expression]"))
        )
      case Right(other) => fail(s"[$expression] expected a CreateTable, got $other")
      case Left(err)    => Left(err.msg)
    }

  "a computed column over date arithmetic" should
  "be created with the ruling's type, or refused by name" in {
    val ddlOperands = operands.filter(_.ddl)
    val ddlPopulation = population.filter(c => c.l.ddl && c.r.ddl)
    ddlPopulation.size shouldBe operators.size * ddlOperands.size * ddlOperands.size
    val mismatches = ddlPopulation.flatMap { c =>
      val dataType = c.outcome match {
        case Typed(t) => t.typeId
        case _        => "BIGINT"
      }
      val verdict = ddlVerdict(c.expression, dataType)
      ((c.outcome, verdict) match {
        case (Refused, Left(message)) =>
          if (message.startsWith("Column 'c': ") && namesTheRefusal(c, message)) None
          else Some(s"refused as [$message]")
        case (Refused, Right(source))          => Some(s"created [$source], expected a refusal")
        case (Typed(_) | NullValued, Right(_)) => None
        case (NotDateArithmetic, Right(_))     => None
        case (NotDateArithmetic, Left(message)) if !dateArithmeticRefusal(message) => None
        case (other, Left(message)) => Some(s"refused as [$message], expected $other")
      }).map(m => s"[${c.expression}] $m")
    }
    info(
      s"date arithmetic, DDL venue: ${ddlPopulation.size} computed columns; mismatches: ${mismatches.size}"
    )
    withClue(mismatches.take(30).mkString("\n", "\n", "\n")) {
      mismatches shouldBe empty
    }
  }

  private def resolvedItem(sql: String): (SingleSearch, Field) =
    Parser(sql) match {
      case Right(ss: SingleSearch) =>
        val resolved = ss.update(Some(schema))
        resolved -> resolved.select.fields
          .find(_.fieldAlias.exists(_.alias == "x"))
          .getOrElse(fail(s"[$sql] no x item"))
      case other => fail(s"[$sql] $other")
    }

  "a cast over date arithmetic" should "keep the type it casts to" in {
    Seq(
      "(d - d)::TIMESTAMP"        -> SQLTypes.Timestamp,
      "(d + 1)::TIMESTAMP"        -> SQLTypes.Timestamp,
      "(ts - ts)::BIGINT"         -> SQLTypes.BigInt,
      "(d + 1.5)::DATE"           -> SQLTypes.Date,
      "(MAX(d) - MIN(d))::DOUBLE" -> SQLTypes.Double
    ).foreach { case (expression, t) =>
      val sql =
        if (expression.contains("MAX")) s"SELECT g, $expression AS x FROM ev GROUP BY g"
        else s"SELECT $expression AS x FROM ev"
      val (resolved, item) = resolvedItem(sql)
      withClue(s"[$expression] ") {
        resolved.validateResolved() shouldBe Right(())
        item.identifier.reportedType shouldBe t
      }
    }
  }

  "INTERVAL arithmetic and DATEDIFF" should "keep the types they had" in {
    Seq(
      "d - INTERVAL 1 DAY"        -> SQLTypes.Timestamp,
      "ts + INTERVAL 2 HOUR"      -> SQLTypes.Timestamp,
      "DATEDIFF(d, d)"            -> SQLTypes.Numeric,
      "TIMESTAMPDIFF(DAY, d, ts)" -> SQLTypes.Numeric
    ).foreach { case (expression, t) =>
      Parser(s"SELECT $expression AS x FROM ev") match {
        case Right(ss: SingleSearch) =>
          val resolved = ss.update(Some(schema))
          withClue(s"[$expression] ") {
            resolved.validateResolved() shouldBe Right(())
            resolved.select.fields.head.identifier.reportedType shouldBe t
          }
        case other => fail(s"[$expression] $other")
      }
    }
  }

  "a date field elasticsql never declared" should
  "be a TIMESTAMP everywhere, from the declaration seam on" in {
    // DESCRIBE reads the column's data type: the seam types it TIMESTAMP (the lead's ruling of
    // 2026-10-05), while a declared DATE keeps its declaration
    schema.find("u").map(_.dataType) shouldBe Some(SQLTypes.Timestamp)
    schema.find("d").map(_.dataType) shouldBe Some(SQLTypes.Date)
    resolvedItem("SELECT u AS x FROM ev")._2.identifier.reportedType shouldBe SQLTypes.Timestamp
    resolvedItem("SELECT d + 1 AS x FROM ev")._2.identifier.reportedType shouldBe SQLTypes.Date
    // date arithmetic: a number of days keeps its time of day, a difference is fractional days
    resolvedItem("SELECT u + 1 AS x FROM ev")._2.identifier.reportedType shouldBe
    SQLTypes.Timestamp
    resolvedItem("SELECT u - d AS x FROM ev")._2.identifier.reportedType shouldBe SQLTypes.Double
    resolvedItem(
      "SELECT g, MAX(u) - MIN(u) AS x FROM ev GROUP BY g"
    )._2.identifier.reportedType shouldBe
    SQLTypes.Double
    // and a refusal names it as DESCRIBE does
    resolvedItem("SELECT u * 2 AS x FROM ev")._1
      .validateResolved()
      .swap
      .getOrElse("") should include(
      "cannot be applied to TIMESTAMP and BIGINT"
    )
  }

  "CASE, GREATEST and LEAST" should "report the least common supertype of their branches" in {
    resolvedItem(
      "SELECT CASE WHEN n > 0 THEN d ELSE d2 END AS x FROM ev"
    )._2.identifier.reportedType shouldBe SQLTypes.Date
    resolvedItem(
      "SELECT CASE WHEN n > 0 THEN d ELSE ts END AS x FROM ev"
    )._2.identifier.reportedType shouldBe SQLTypes.Timestamp
    resolvedItem("SELECT GREATEST(n, x) AS x FROM ev")._2.identifier.reportedType shouldBe
    SQLTypes.Double
    resolvedItem("SELECT LEAST(n, 2) AS x FROM ev")._2.identifier.reportedType shouldBe
    SQLTypes.BigInt
    // GREATEST / LEAST over dates and timestamps (the lead's ruling of 2026-10-05): a DATE for
    // dates, a TIMESTAMP when they mix -- and a date difference of one and a date is days
    Seq(
      "SELECT GREATEST(d, d2) AS x FROM ev"                         -> SQLTypes.Date,
      "SELECT LEAST(d, ts) AS x FROM ev"                            -> SQLTypes.Timestamp,
      "SELECT GREATEST(d, CURRENT_DATE) AS x FROM ev"               -> SQLTypes.Date,
      "SELECT g, GREATEST(MAX(d), MAX(d2)) AS x FROM ev GROUP BY g" -> SQLTypes.Date,
      "SELECT GREATEST(d, d2) - d AS x FROM ev"                     -> SQLTypes.BigInt,
      "SELECT LEAST(d, ts) - d AS x FROM ev"                        -> SQLTypes.Double
    ).foreach { case (sql, t) =>
      val (resolved, item) = resolvedItem(sql)
      withClue(s"[$sql] ") {
        resolved.validateResolved() shouldBe Right(())
        item.identifier.reportedType shouldBe t
      }
    }
    // numbers and dates do not mix, as in every SQL engine
    resolvedItem("SELECT GREATEST(d, n) AS x FROM ev")._1
      .validateResolved()
      .swap
      .getOrElse("") should include(
      "GREATEST requires numeric arguments, or date and timestamp arguments, but got DATE for d " +
      "and INT for n"
    )
  }
}
