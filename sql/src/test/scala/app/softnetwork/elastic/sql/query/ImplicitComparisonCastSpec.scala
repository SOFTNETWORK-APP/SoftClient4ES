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

import app.softnetwork.elastic.sql.{
  ComparisonRule,
  PainlessContext,
  PainlessContextType,
  StringValue
}
import app.softnetwork.elastic.sql.function.cond.NullIf
import app.softnetwork.elastic.sql.parser.Parser
import app.softnetwork.elastic.sql.schema.{
  validateScriptReferences,
  Column,
  Schema,
  ScriptProcessor,
  Table => SchemaTable
}
import app.softnetwork.elastic.sql.`type`.{SQLTypeUtils, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** A comparison between a text and a number or a boolean, and between a boolean and a number,
  * answers for the operators DuckDB 1.5.5 binds, as DuckDB casts it (SCHEMA-5b D6 / D7): the
  * binding table both ways, the runtime conversions in every emission context, and the scope rule
  * that keeps a text side outside a query as 0.24.0 judged it (A-82).
  */
class ImplicitComparisonCastSpec extends AnyFlatSpec with Matchers {

  private val table = SchemaTable(
    "t",
    columns = List(
      Column("g", SQLTypes.Keyword),
      Column("k", SQLTypes.Keyword),
      Column("tx", SQLTypes.Text),
      Column("ak", SQLTypes("array<keyword>")),
      Column("n", SQLTypes.Int),
      Column("l", SQLTypes.BigInt),
      Column("x", SQLTypes.Double),
      Column("b", SQLTypes.Boolean),
      Column("ab", SQLTypes("array<boolean>")),
      Column("d", SQLTypes.Date)
    )
  )

  private def resolved(sql: String): SingleSearch = Parser(sql) match {
    case Right(s: SingleSearch) => s.update(Some(table))
    case other                  => fail(s"$sql: not a single search: $other")
  }

  private def verdict(sql: String): Option[String] = resolved(sql).validateResolved().left.toOption

  /** The whole query script of the first SELECT field: its declarations, then its value. */
  private def selectScript(sql: String): String = {
    val ctx = PainlessContext(PainlessContextType.Query)
    val painless = resolved(sql).select.fieldsWithComputedAliases.head.painless(Some(ctx))
    s"$ctx$painless"
  }

  /** The whole WHERE script. */
  private def whereScript(sql: String): String = {
    val ctx = PainlessContext(PainlessContextType.Query)
    val painless = resolved(sql).where.flatMap(_.criteria).get.painless(Some(ctx))
    s"$ctx$painless"
  }

  // ---- P-B2: DuckDB 1.5.5's binding table (`duck_5b_ops.out`), compiled ----

  private sealed trait Family
  private case object Whole extends Family
  private case object Fractional extends Family
  private case object Bool extends Family

  /** Does DuckDB bind a text (`text = true`) or a boolean against `right`, for an equality or an
    * ordering? D6's rows: text x number / boolean, equality only; boolean x whole, every
    * comparison; boolean x fractional, equality only.
    */
  private def binds(text: Boolean, right: Family, equality: Boolean): Boolean =
    if (text) equality
    else right == Whole || equality

  private val textRights: Seq[(String, Family)] =
    Seq(
      "n"   -> Whole,
      "l"   -> Whole,
      "x"   -> Fractional,
      "b"   -> Bool,
      "1"   -> Whole,
      "1.5" -> Fractional
    )

  private val boolRights: Seq[(String, Family)] =
    Seq(
      "n"   -> Whole,
      "l"   -> Whole,
      "x"   -> Fractional,
      "1"   -> Whole,
      "0"   -> Whole,
      "1.5" -> Fractional
    )

  private def literal(r: String): Boolean = r.headOption.exists(_.isDigit)

  /** Every P-B2 statement, with whether DuckDB answers it. */
  private val pb2: Seq[(String, Boolean)] = {
    val lefts = Seq("k" -> true, "ak" -> true, "b" -> false, "ab" -> false)
    for {
      (l, text)  <- lefts
      (r, right) <- if (text) textRights else boolRights
      (sql, equality) <- Seq(
        s"SELECT CASE WHEN $l = $r THEN 1 ELSE 0 END AS v FROM t"              -> true,
        s"SELECT CASE WHEN $l <> $r THEN 1 ELSE 0 END AS v FROM t"             -> true,
        s"SELECT CASE WHEN $l < $r THEN 1 ELSE 0 END AS v FROM t"              -> false,
        s"SELECT CASE WHEN $l >= $r THEN 1 ELSE 0 END AS v FROM t"             -> false,
        s"SELECT CASE WHEN $l BETWEEN $r AND $r THEN 1 ELSE 0 END AS v FROM t" -> false,
        s"SELECT NULLIF($l, $r) AS v FROM t"                                   -> true,
        s"SELECT CASE $l WHEN $r THEN 1 ELSE 0 END AS v FROM t"                -> true
      ) ++ (if (literal(r))
              Seq(s"SELECT CASE WHEN $l IN ($r) THEN 1 ELSE 0 END AS v FROM t" -> true)
            else
              Seq(
                s"SELECT g FROM t WHERE $l = $r"                           -> true,
                s"SELECT g FROM t WHERE $r = $l"                           -> true,
                s"SELECT g FROM t WHERE $r < $l"                           -> false,
                s"SELECT CASE WHEN $r <> $l THEN 1 ELSE 0 END AS v FROM t" -> true,
                s"SELECT NULLIF($r, $l) AS v FROM t"                       -> true
              ))
    } yield sql -> binds(text, right, equality)
  }

  /** main's refusal messages, unchanged where DuckDB refuses. */
  private def mainRefusal(message: String): Boolean =
    message.contains("Type mismatch") || message.contains("comparable types") ||
    message.contains("CASE WHEN conditions must be of the same type")

  behavior of "the implicit-cast table (D6), both ways"

  it should "answer every pair x operator DuckDB binds" in {
    val answered = pb2.filter(_._2).map(_._1)
    answered should not be empty
    answered.foreach(sql => withClue(s"$sql: ")(verdict(sql) shouldBe None))
  }

  /** A BETWEEN whose bounds are COLUMNS: `BetweenExpr.update` resolves the operand only, so the
    * bounds keep an unknown type and main accepts it whatever the operand (pre-existing, recorded)
    * -- the verdict stays main's.
    */
  private val columnBounds = "BETWEEN [a-z]+ AND".r

  it should "keep main's refusal and message for every pair x operator DuckDB refuses" in {
    val refused =
      pb2.filterNot(_._2).map(_._1).filter(sql => columnBounds.findFirstIn(sql).isEmpty)
    refused should not be empty
    refused.foreach { sql =>
      withClue(s"$sql: ") {
        val v = verdict(sql)
        v shouldBe defined
        mainRefusal(v.get) shouldBe true
      }
    }
  }

  it should "keep main's verdict for a BETWEEN over column bounds (pre-existing)" in {
    Seq("k BETWEEN n AND n", "k BETWEEN x AND x", "b BETWEEN x AND x", "b BETWEEN n AND n")
      .foreach { cond =>
        verdict(s"SELECT CASE WHEN $cond THEN 1 ELSE 0 END AS v FROM t") shouldBe None
      }
    // `BetweenExpr.update` resolves its operand only: column bounds keep an unknown type, so the
    // table never applies and both renderings stay main's
    selectScript("SELECT CASE WHEN b BETWEEN n AND n THEN 1 ELSE 0 END AS v FROM t") should include(
      "((param1 >= param2 && param1 <= param2))"
    )
    selectScript(
      "SELECT CASE WHEN k BETWEEN n AND n THEN 1 ELSE 0 END AS v FROM t"
    ) should not include
    "nv1"
  }

  it should "leave a string literal to the comparison rule's reading" in {
    // 'abc' spells no number: refused as text against a number, as on main
    verdict("SELECT CASE WHEN n = 'abc' THEN 1 ELSE 0 END AS v FROM t") shouldBe defined
    verdict("SELECT NULLIF(k, 'abc') AS v FROM t") shouldBe None
  }

  it should "read a group filter's BOOLEAN metric with no conversion (R10)" in {
    Seq(">", "=", ">=", "<").foreach { op =>
      verdict(s"SELECT g, MAX(b) AS m FROM t GROUP BY g HAVING MAX(b) $op 1") shouldBe None
    }
    verdict("SELECT g, MAX(b) AS m FROM t GROUP BY g HAVING MAX(b) = 1.5") shouldBe None
    verdict("SELECT g, MAX(b) AS m FROM t GROUP BY g HAVING MAX(b) < 1.5") shouldBe defined
    val selector = resolved("SELECT g, MAX(b) AS m FROM t GROUP BY g HAVING MAX(b) > 1").having
      .flatMap(_.criteria)
      .map(MetricSelectorScript.metricSelector)
      .get
    selector should include("> 1")
    selector should not include "? 1"
    // a text metric converts nothing per group, and Elasticsearch's max reads no keyword (T0-4)
    verdict("SELECT g, MAX(k) AS m FROM t GROUP BY g HAVING MAX(k) = 2") shouldBe defined
  }

  it should "keep the refusals R-102 excludes (A-78)" in {
    verdict(
      "SELECT g, MAX(k) AS m FROM t GROUP BY g HAVING MAX(k) > '2024-01-01'"
    ).getOrElse("") should include("compares a metric as text")
  }

  behavior of "the runtime conversion (D7)"

  it should "read a text as the whole number it rounds to, inside the operand's NULL guard" in {
    val script = selectScript("SELECT CASE WHEN k = 1 THEN 1 ELSE 0 END AS v FROM t")
    script should include("def nv1 = null; if (param1 != null) {")
    script should include("String.valueOf(param1).trim()")
    script should include("new BigDecimal('9223372036854775807.5')")
    script should include("setScale(0, RoundingMode.HALF_UP).longValueExact()")
    script should include("throw new IllegalArgumentException")
    script should not include "Long.parseLong"
    script should include("(left2 == null ? false : ((left2 == 1)))")
  }

  it should "read a text as a DOUBLE beside a DOUBLE, a BOOLEAN beside a BOOLEAN" in {
    whereScript("SELECT g FROM t WHERE k = x") should include("Double.parseDouble(nv1s)")
    val boolean = whereScript("SELECT g FROM t WHERE k = b")
    boolean should include("String nv1s = String.valueOf(param1).trim().toLowerCase();")
    boolean should include("nv1s.equals('on')")
    boolean should not include "equalsIgnoreCase"
    boolean should not include "Boolean.parseBoolean"
  }

  it should "convert a BOOLEAN beside a number to 1 / 0, and leave the number as it is" in {
    whereScript("SELECT g FROM t WHERE b > n") should include(
      "def left1 = (param1 == null ? null : (def)((param1 ? 1 : 0)))"
    )
    whereScript("SELECT g FROM t WHERE x = b") should include("((left1 == (param2 ? 1.0 : 0.0)))")
  }

  it should "read an ARRAY<X> column as its element in a query (D8)" in {
    whereScript("SELECT g FROM t WHERE ak = n") should include("String.valueOf(param1).trim()")
    selectScript("SELECT CASE WHEN ab = 1 THEN 1 ELSE 0 END AS v FROM t") should include(
      "(param1 ? 1L : 0L)"
    )
  }

  it should "compare a NULLIF's arguments by the table and answer its first argument" in {
    val keyword = selectScript("SELECT NULLIF(k, 0) AS v FROM t")
    keyword should include("param1 == null || nv1 == 0 ? null : param1")
    keyword should not include "0 != null"
    selectScript("SELECT NULLIF(b, 0) AS v FROM t") should include(
      "param1 == null || (param1 ? 1L : 0L) == 0 ? null : param1"
    )
  }

  it should "compare a simple CASE's expression and values by the table" in {
    val keyword = selectScript("SELECT CASE k WHEN 1 THEN 'one' ELSE 'other' END AS v FROM t")
    keyword should include("def param2 = nv1;")
    keyword should include("param2 != null && param3 != null && param2 == param3 ? \"one\"")
    selectScript("SELECT CASE b WHEN 1 THEN 'one' ELSE 'other' END AS v FROM t") should include(
      "(param1 == null ? null : (def)((param1 ? 1L : 0L)))"
    )
  }

  it should "keep a transform's text comparison as main renders it (A-82)" in {
    val field =
      resolved(
        "SELECT CASE WHEN k = 1 THEN 1 ELSE 0 END AS v FROM t"
      ).select.fieldsWithComputedAliases.head
    val transform = field.painless(Some(PainlessContext(PainlessContextType.Transform)))
    transform should not include "nv1"
  }

  it should "spell exactly the literal rule's booleans (A-81 twin pin)" in {
    val script = whereScript("SELECT g FROM t WHERE k = b")
    val spelled = "equals(?:IgnoreCase)?\\('([^']*)'\\)".r
      .findAllMatchIn(script)
      .map(_.group(1))
      .toSet
    // the rendered rule, as the JVM runs it: the methods the script calls, not a Scala restatement
    // (R0-2: `equalsIgnoreCase` folds U+017F 'ſ' to 's', so 'yeſ' read true where `booleanOf` reads
    // nothing; its lower case is still 'yeſ')
    val ignoringCase = script.contains(".equalsIgnoreCase(")
    val lowered = script.contains(".trim().toLowerCase()")
    def rendered(text: String): Boolean = {
      val s = if (lowered) text.trim.toLowerCase else text.trim
      spelled.exists(word => if (ignoringCase) s.equalsIgnoreCase(word) else s == word)
    }
    Seq(
      "t",
      "TRUE",
      " true ",
      "Yes",
      "y",
      "on",
      "OFF",
      "1",
      "0",
      "f",
      "no",
      "n",
      "false",
      "2",
      "",
      "maybe",
      "nan",
      "ye\u017F",
      "YE\u017F",
      "fal\u017Fe"
    ).foreach { text =>
      withClue(s"[$text]: ") {
        ComparisonRule.readingOf(StringValue(text), SQLTypes.Boolean).isDefined shouldBe
        rendered(text)
      }
    }
  }

  behavior of "a text side outside a query (A-82)"

  private val columns =
    "id KEYWORD, n INT, x DOUBLE, b BOOLEAN, k KEYWORD, ak ARRAY<KEYWORD>"

  private def createVerdict(decl: String, expr: String): Either[String, String] =
    Parser(s"CREATE TABLE t ($columns, c $decl SCRIPT AS ($expr))") match {
      case Left(e) => Left(e.msg)
      case Right(ct: CreateTable) =>
        Right(
          ct.schema.defaultPipeline.processors
            .collectFirst { case p: ScriptProcessor if p.column == "c" => p.source }
            .getOrElse("-")
        )
      case other => fail(s"$expr: $other")
    }

  private def alterVerdict(decl: String, expr: String): Either[String, String] = {
    val base: Schema = Parser(s"CREATE TABLE t ($columns)") match {
      case Right(ct: CreateTable) => ct.schema
      case other                  => fail(s"base: $other")
    }
    Parser(s"ALTER TABLE t ADD COLUMN c $decl SCRIPT AS ($expr)") match {
      case Left(e) => Left(e.msg)
      case Right(at: AlterTable) =>
        val merged = base.merge(at.statements)
        validateScriptReferences(merged).map(_ =>
          merged.defaultPipeline.processors
            .collectFirst { case p: ScriptProcessor if p.column == "c" => p.source }
            .getOrElse("-")
        )
      case other => fail(s"$expr: $other")
    }
  }

  it should "refuse NULLIF(k, 0) in a computed column, on CREATE and ALTER (#382)" in {
    createVerdict("KEYWORD", "NULLIF(k, 0)").left.getOrElse("") should include("comparable types")
    alterVerdict("KEYWORD", "NULLIF(k, 0)").left.getOrElse("") should include("comparable types")
  }

  it should "keep main's processor bytes for a text comparison" in {
    createVerdict("INT", "CASE WHEN k = 1 THEN 1 ELSE 0 END") shouldBe
    Right(
      "def param1 = ctx.k; def param2 = param1 == null ? false : (param1 == 1); ctx.c = param2 ? 1 : 0"
    )
  }

  it should "convert a BOOLEAN beside a number in a processor as Elasticsearch reads its _source" in {
    val source = createVerdict("INT", "CASE WHEN b = 1 THEN 1 ELSE 0 END").getOrElse("")
    source should include("param1 instanceof Boolean ? (boolean) param1 : 'true'.equals(param1)")
    source should include("(param1 == null ? null : (def)(")
  }

  it should "judge one NULLIF node as each scope does, in either order (R1, R11)" in {
    def node(): NullIf =
      resolved(
        "SELECT NULLIF(k, 0) AS v FROM t"
      ).select.fieldsWithComputedAliases.head.identifier.functions.collectFirst { case n: NullIf =>
        n
      }.get
    val first = node()
    SQLTypeUtils.validatingComputedColumns(first.typeError) shouldBe defined
    first.typeError shouldBe None
    val second = node()
    second.typeError shouldBe None
    SQLTypeUtils.validatingComputedColumns(second.typeError) shouldBe defined
  }

  /** A scope's answer, or the failure it raises. */
  private def answer[N](n: N, ask: N => Any): Any =
    scala.util.Try(ask(n)).fold(e => s"throws ${e.getMessage}", identity)

  /** Each scope's answer on ONE node, asked in every order, is what that scope answers on a fresh
    * node (A-95): a memo that lets the first scope to ask decide for the others fails an order.
    */
  private def answersEachScopeAsFresh[N](node: () => N, scopes: List[(String, N => Any)]): Unit = {
    val fresh = scopes.map { case (scope, ask) => scope -> answer(node(), ask) }.toMap
    scopes.permutations.foreach { order =>
      val one = node()
      order.foreach { case (scope, ask) =>
        withClue(s"${order.map(_._1).mkString(" > ")}, $scope: ") {
          answer(one, ask) shouldBe fresh(scope)
        }
      }
    }
  }

  /** The whole script `painless` renders from a node in a fresh context of type `t`. */
  private def renderedIn[N](
    t: PainlessContextType
  )(painless: (N, PainlessContext) => String): N => String = { n =>
    val ctx = PainlessContext(t)
    val p = painless(n, ctx)
    s"$ctx$p"
  }

  it should "answer one comparison node as each scope does, in any order (A-95)" in {
    val render = (c: Criteria, ctx: PainlessContext) => c.painless(Some(ctx))
    val scopes: List[(String, Criteria => Any)] = List(
      "query"      -> renderedIn(PainlessContextType.Query)(render),
      "transform"  -> renderedIn(PainlessContextType.Transform)(render),
      "processor"  -> renderedIn(PainlessContextType.Processor)(render),
      "type rule"  -> ((c: Criteria) => c.typeErrors),
      "validation" -> ((c: Criteria) => SQLTypeUtils.validatingComputedColumns(c.typeErrors)),
      "validation render" -> ((c: Criteria) =>
        SQLTypeUtils.validatingComputedColumns(renderedIn(PainlessContextType.Query)(render)(c))
      )
    )
    Seq("k = 1", "k IN (1, 2)", "b BETWEEN 0 AND n").foreach { p =>
      withClue(s"$p: ") {
        val node = () => resolved(s"SELECT g FROM t WHERE $p").where.flatMap(_.criteria).get
        if (p.startsWith("k")) {
          // the text row converts in a query only (A-82): the scopes differ, so the pin bites
          answer(node(), scopes.head._2).toString should include("nv1")
          answer(node(), scopes(1)._2).toString should not include "nv1"
          answer(node(), scopes(2)._2).toString should not include "nv1"
          answer(node(), scopes(5)._2).toString should not include "nv1"
        }
        answersEachScopeAsFresh(node, scopes)
      }
    }
  }

  it should "answer one literal-first NULLIF node as each scope does, in any order (A-95)" in {
    val node = () =>
      resolved(
        "SELECT NULLIF(COALESCE('2024-01-15', d), k) AS v FROM t"
      ).select.fieldsWithComputedAliases.head
    def nullIf(f: Field): NullIf = f.identifier.functions.collectFirst { case n: NullIf => n }.get
    val render = (f: Field, ctx: PainlessContext) => f.painless(Some(ctx))
    val scopes: List[(String, Field => Any)] = List(
      "query"      -> renderedIn(PainlessContextType.Query)(render),
      "processor"  -> renderedIn(PainlessContextType.Processor)(render),
      "type rule"  -> ((f: Field) => nullIf(f).typeError),
      "validation" -> ((f: Field) => SQLTypeUtils.validatingComputedColumns(nullIf(f).typeError)),
      "validation render" -> ((f: Field) =>
        SQLTypeUtils.validatingComputedColumns(renderedIn(PainlessContextType.Query)(render)(f))
      )
    )
    // the literal is read in a query only (A-98 α): a query compares its text, a processor reads
    // the COALESCE whole -- the scopes differ
    val literalCompared = "\"2024-01-15\".compareTo(param"
    answer(node(), scopes.head._2).toString should include(literalCompared)
    val processor = answer(node(), scopes(1)._2).toString
    processor should include("ctx.d")
    processor should not include literalCompared
    answersEachScopeAsFresh(node, scopes)
  }
}
