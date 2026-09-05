package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.query.SingleSearch
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** #252 part 1 — a quoted identifier is accepted in every expression position and in every alias
  * position, in both accepted spellings.
  *
  * The enumeration IS the scope: `identifier` is the production quoting was folded into, so every
  * site that ends in `| identifier` or routes through `valueExpr` inherits it, and the only way to
  * know that is to exercise them. A sampled test would pass while `PARTITION BY`, the four
  * conversion operands and the seventeen `time` operands stayed broken.
  */
class QuotedIdentifierSpec extends AnyFlatSpec with Matchers {

  private def single(sql: String): SingleSearch =
    Parser(sql) match {
      case Right(s: SingleSearch) => s
      case other                  => fail(s"[$sql] did not parse to a SingleSearch: $other")
    }

  private def firstFieldName(sql: String): String = single(sql).select.fields.head.identifier.name

  private def parses(sql: String): Unit =
    withClue(s"[$sql] ") { Parser(sql).isRight shouldBe true }

  /** A GRAMMAR rejection, as opposed to the `NonFatal` boundary catch story 21.4 installed in
    * `Parser.apply`.
    *
    * Copied from `ParserTotalitySpec`'s helper of the same name, and the third assertion is what
    * makes the test falsifiable at all: since 21.4 wraps the whole method body, `noException` holds
    * unconditionally and `isLeft` holds for an internal crash too. Without the prefix assertion a
    * restored `throw` would keep every row green. 21.4 proved that by restoring one and watching
    * seven tests go red.
    */
  private def reasonOf(sql: String): String =
    Parser(sql).swap.toOption.map(_.msg).getOrElse("")

  private def rejected(sql: String): Unit = {
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    withClue(s"[$sql] ") { Parser(sql).isLeft shouldBe true }
    val msg = reasonOf(sql)
    withClue(
      s"[$sql] msg=[$msg] - this rejection must come from the grammar, not from `Parser.apply`'s " +
      "NonFatal boundary catch. "
    ) {
      msg should not startWith Parser.InternalParseFailure
    }
    ()
  }

  /** The mirror of `rejected`: the input DID reach the boundary catch. */
  private def rejectedInternally(sql: String, detail: String): Unit = {
    withClue(s"[$sql] ") { noException should be thrownBy Parser(sql) }
    withClue(s"[$sql] ") { Parser(sql).isLeft shouldBe true }
    val msg = reasonOf(sql)
    withClue(s"[$sql] msg=[$msg] ") { msg should startWith(Parser.InternalParseFailure) }
    withClue(s"[$sql] msg=[$msg] ") { msg should include(detail) }
    ()
  }

  // ---------------------------------------------------------------------------------------------
  // AC-1 — every expression position, both spellings
  // ---------------------------------------------------------------------------------------------

  private val expressionPositions = Seq(
    // select list
    "SELECT `category` FROM bi_events",
    "SELECT \"category\" FROM bi_events",
    // qualified, in all three mixes
    "SELECT `e`.`category` FROM bi_events e",
    "SELECT e.`category` FROM bi_events e",
    "SELECT `e`.category FROM bi_events e",
    // where
    "SELECT id FROM bi_events WHERE `category` = 'a'",
    "SELECT id FROM bi_events WHERE `category` IS NOT NULL",
    "SELECT id FROM bi_events WHERE `amount` > 10",
    "SELECT id FROM bi_events WHERE `amount` BETWEEN 1 AND 2",
    "SELECT id FROM bi_events WHERE `category` IN ('a','b')",
    "SELECT id FROM bi_events WHERE `category` LIKE 'a%'",
    // group by / having / order by
    "SELECT `category`, COUNT(id) AS c FROM bi_events GROUP BY `category`",
    "SELECT `category`, COUNT(id) AS c FROM bi_events GROUP BY `category` HAVING COUNT(`id`) > 1",
    "SELECT id FROM bi_events ORDER BY `event_ts` DESC",
    "SELECT id FROM bi_events ORDER BY `event_ts` DESC NULLS LAST",
    // aggregate and scalar function arguments
    "SELECT MAX(`amount`) AS m FROM bi_events",
    "SELECT UPPER(`category`) AS u FROM bi_events",
    "SELECT ABS(`amount`) AS a FROM bi_events",
    "SELECT CONCAT(`country`, `category`) AS cc FROM bi_events",
    "SELECT COALESCE(`country`, `category`) AS c FROM bi_events",
    "SELECT CASE WHEN `amount` > 1 THEN `country` ELSE `category` END AS c FROM bi_events",
    // conversions — all four spellings, none of which accepted a quoted operand before
    "SELECT CAST(`amount` AS BIGINT) AS a FROM bi_events",
    "SELECT TRY_CAST(`amount` AS BIGINT) AS a FROM bi_events",
    "SELECT CONVERT(`amount`, BIGINT) AS a FROM bi_events",
    "SELECT CONVERT(BIGINT, `amount`) AS a FROM bi_events",
    "SELECT `amount`::BIGINT AS a FROM bi_events",
    // time functions
    "SELECT DATE_TRUNC(`event_ts`, MONTH) AS m FROM bi_events",
    "SELECT EXTRACT(YEAR FROM `event_ts`) AS y FROM bi_events",
    "SELECT DATE_DIFF(`event_ts`, CURRENT_DATE, DAY) AS d FROM bi_events",
    // arithmetic — parenthesised, which is the form a BI tool emits (see the unparenthesised
    // limitation pinned further down)
    "SELECT (`amount` + 1) AS a FROM bi_events",
    "SELECT (`amount` + `qty`) * 2 AS a FROM bi_events",
    "SELECT MAX(`amount` + 1) AS a FROM bi_events",
    // window: PARTITION BY and the window argument
    "SELECT ROW_NUMBER() OVER (PARTITION BY `category` ORDER BY `amount` DESC) AS rn FROM bi_events",
    "SELECT FIRST_VALUE(`amount`) OVER (PARTITION BY `category`) AS f FROM bi_events",
    // unnest
    "SELECT AVG(c.likes) AS a FROM blogs JOIN UNNEST(`blogs`.`comments`) AS c",
    // distinct
    "SELECT COUNT(DISTINCT `category`) AS c FROM bi_events"
  )

  "a backtick-quoted identifier" should "parse in every expression position" in {
    expressionPositions.foreach(parses)
  }

  /** The double-quote matrix is spelled out, NOT produced by mechanically re-spelling the backtick
    * one. Two rows genuinely differ — `SELECT "amount" + 1` is rejected (see the arithmetic pin
    * below) and the value positions keep reading a double-quoted lexeme as a string — so a
    * `.replace` would assert the wrong thing while looking thorough.
    */
  private val doubleQuotedColumnPositions = Seq(
    "SELECT \"category\" FROM bi_events",
    "SELECT \"e\".\"category\" FROM bi_events e",
    "SELECT e.\"category\" FROM bi_events e",
    "SELECT \"e\".category FROM bi_events e",
    "SELECT id FROM bi_events WHERE \"category\" = 'a'",
    "SELECT id FROM bi_events WHERE \"category\" IS NOT NULL",
    "SELECT id FROM bi_events WHERE \"amount\" > 10",
    "SELECT id FROM bi_events WHERE \"amount\" BETWEEN 1 AND 2",
    "SELECT id FROM bi_events WHERE \"category\" IN ('a','b')",
    "SELECT id FROM bi_events WHERE \"category\" LIKE 'a%'",
    "SELECT \"category\", COUNT(id) AS c FROM bi_events GROUP BY \"category\"",
    "SELECT id FROM bi_events ORDER BY \"event_ts\" DESC",
    "SELECT id FROM bi_events ORDER BY \"event_ts\" DESC NULLS LAST",
    "SELECT \"amount\"::BIGINT AS a FROM bi_events",
    "SELECT MAX(\"amount\") AS m FROM bi_events",
    "SELECT CAST(\"amount\" AS BIGINT) AS a FROM bi_events",
    "SELECT DATE_TRUNC(\"event_ts\", MONTH) AS m FROM bi_events",
    "SELECT (\"amount\" + 1) AS a FROM bi_events",
    "SELECT ROW_NUMBER() OVER (PARTITION BY \"category\" ORDER BY \"amount\" DESC) AS rn FROM bi_events",
    "SELECT FIRST_VALUE(\"amount\") OVER (PARTITION BY \"category\") AS f FROM bi_events",
    "SELECT AVG(c.likes) AS a FROM blogs JOIN UNNEST(\"blogs\".\"comments\") AS c",
    "SELECT COUNT(DISTINCT \"category\") AS c FROM bi_events"
  )

  it should "parse in every position where the double-quoted spelling is also a column" in {
    doubleQuotedColumnPositions.foreach(parses)
  }

  /** AD-13, the story's central behaviour change, asserted as a COLUMN rather than merely as
    * "parses".
    *
    * A double-quoted lexeme in an OPERAND position used to be a `StringValue`, because
    * `identifierWithIntervalFunction` reached `TypeParser.literal` before `identifier`. Measured on
    * the unmodified tree: `MAX("salary")` rendered `MAX('salary')`. That made the canonical
    * double-quote render un-round-trippable for a backticked operand — `MAX(`amount`)` rendered
    * `MAX("amount")` and came back as `MAX('amount')` — so the reading had to change for the render
    * contract (AC-5) to hold at all.
    */
  it should "read a double-quoted operand as a COLUMN, in every operand family (AD-13)" in {
    firstFieldName("SELECT MAX(\"salary\") AS m FROM t") shouldBe "salary"
    firstFieldName("SELECT CAST(\"amount\" AS BIGINT) AS a FROM t") shouldBe "amount"
    firstFieldName("SELECT TRY_CAST(\"amount\" AS BIGINT) AS a FROM t") shouldBe "amount"
    firstFieldName("SELECT CONVERT(\"amount\", BIGINT) AS a FROM t") shouldBe "amount"
    firstFieldName("SELECT DATE_TRUNC(\"event_ts\", MONTH) AS m FROM t") shouldBe "event_ts"
    firstFieldName("SELECT COUNT(DISTINCT \"category\") AS c FROM t") shouldBe "category"

    // …and it is the SAME column the backtick spelling names.
    firstFieldName("SELECT MAX(`salary`) AS m FROM t") shouldBe "salary"
    firstFieldName("SELECT CAST(`amount` AS BIGINT) AS a FROM t") shouldBe "amount"

    // The functions a self-contained node embeds do not expose the operand as `identifier.name`,
    // so those are asserted on the render instead — a StringValue would print `'abc'`.
    Parser("SELECT UPPER(\"abc\") AS u FROM t").toOption.get.sql should include("UPPER(\"abc\")")
    Parser("SELECT ABS(\"amount\") AS a FROM t").toOption.get.sql should include("ABS(\"amount\")")
  }

  /** The other half of AD-13, and the half that keeps it from being a blanket change: a VALUE
    * position still reads a double-quoted lexeme as a string, because each of those alternations
    * lists `literal` ahead of the identifier in its OWN alternative list. No alternation order
    * moves in this story, so none of these can have flipped — but they are the rows that would
    * prove it if one ever did.
    */
  it should "keep every VALUE position reading a double-quoted lexeme as a string" in {
    Parser("SELECT id FROM t WHERE a = \"x\"").toOption.get.sql should include("= 'x'")
    Parser("SELECT id FROM t WHERE a > \"x\"").toOption.get.sql should include("> 'x'")
    Parser("SELECT id FROM t WHERE a BETWEEN \"a\" AND \"b\"").toOption.get.sql should
    include("BETWEEN 'a' AND 'b'")
    Parser("SELECT id FROM t WHERE a IN (\"a\",\"b\")").toOption.get.sql should include("('a','b')")
    Parser("SELECT id FROM t WHERE a LIKE \"a%\"").toOption.get.sql should include("LIKE 'a%'")
    // A mixed comparison: the LHS is the column, the RHS is the string.
    Parser("SELECT id FROM t WHERE \"a\" = \"b\"").toOption.get.sql should include("= 'b'")
  }

  /** CORRECTION to the story's ordering audit, found by running it (task 5.2).
    *
    * The audit recorded `date_parse` and `datetime_parse` as the two operand sites that list
    * `literal` BEFORE `identifier`, and concluded their first operand keeps the string reading. The
    * first half is true and the conclusion does not follow: their alternation is
    * `identifierWithTransformation | identifierWithIntervalFunction | identifierWithFunction |
    * literal | identifier`, so `literal` sits at slot FOUR, behind `identifierWithIntervalFunction`
    * at slot two. Before AD-13 that slot reached `literal` anyway (through `identifierWithValue`),
    * which is why the outcome looked like the audit's; after AD-13 it reads the identifier first.
    *
    * So these two sites flip with the rest of the operand family, and the flip is pinned rather
    * than discovered. The single-quoted spelling — the one the documentation uses — is unaffected.
    */
  it should "flip DATE_PARSE's and DATETIME_PARSE's double-quoted first operand too (AD-13)" in {
    firstFieldName("SELECT DATE_PARSE(\"2024-01-01\", 'yyyy-MM-dd') AS d FROM t") shouldBe
    "2024-01-01"
    firstFieldName(
      "SELECT DATETIME_PARSE(\"2024-01-01 10:00:00\", 'yyyy-MM-dd HH:mm:ss') AS d FROM t"
    ) shouldBe
    "2024-01-01 10:00:00"
    // The documented single-quoted spelling keeps meaning the literal it always meant.
    Parser("SELECT DATE_PARSE('2024-01-01', 'yyyy-MM-dd') AS d FROM t").toOption.get.sql should
    include("DATE_PARSE('2024-01-01'")
    // …and a genuine column operand still works in both spellings.
    firstFieldName("SELECT DATE_PARSE(`event_ts`, 'yyyy-MM-dd') AS d FROM t") shouldBe "event_ts"
    firstFieldName("SELECT DATE_PARSE(event_ts, 'yyyy-MM-dd') AS d FROM t") shouldBe "event_ts"
  }

  it should "leave single-quoted literals untouched everywhere" in {
    // The interval production is the one `quotedIdentifier` was inserted into, so this is its own
    // regression gate: hoisting `identifierWithValue` here is what 21.5 measured to break it.
    parses("SELECT CAST('2025-01-01' + INTERVAL 1 DAY AS DATE) AS d FROM t")
    parses("SELECT CAST('125' AS BIGINT) AS a FROM t")
    parses("SELECT TRY_CAST('abc' AS INT) AS a FROM t")
    Parser("SELECT UPPER('abc') AS u FROM t").toOption.get.sql should include("UPPER('abc')")
    Parser("SELECT CONCAT('a', 'b') AS c FROM t").toOption.get.sql should include(
      "CONCAT('a', 'b')"
    )
  }

  /** The ten Tableau SQL-92 corpus shapes: a QUALIFIED double-quoted name inside a function,
    * aggregate or conversion argument. Measured REJECTED on the unmodified tree with `')' expected
    * but '.' found` — the corpus's 9-occurrence error family.
    */
  it should "read a QUALIFIED double-quoted name inside a function argument as a column" in {
    firstFieldName("SELECT SUM(\"bi_events\".\"amount\") AS s FROM bi_events") shouldBe "amount"
    firstFieldName("SELECT AVG(\"bi_events\".\"amount\") AS a FROM bi_events") shouldBe "amount"
    firstFieldName("SELECT MIN(\"bi_events\".\"amount\") AS m FROM bi_events") shouldBe "amount"
    firstFieldName("SELECT MAX(\"bi_events\".\"amount\") AS m FROM bi_events") shouldBe "amount"
    firstFieldName(
      "SELECT CAST(\"bi_events\".\"event_ts\" AS TIMESTAMP) AS t FROM bi_events"
    ) shouldBe
    "event_ts"
    Parser("SELECT UPPER(\"bi_events\".\"country\") AS u FROM bi_events").toOption.get.sql should
    include("UPPER(\"bi_events\".\"country\")")
    parses("SELECT SUBSTRING(\"bi_events\".\"name\" FROM 1 FOR 3) AS s FROM bi_events")
    parses(
      "SELECT CAST(EXTRACT(MONTH FROM \"bi_events\".\"event_ts\") AS INTEGER) AS m FROM bi_events"
    )
    // the mixed spellings of the same qualified shape
    firstFieldName("SELECT SUM(\"bi_events\".amount) AS s FROM bi_events") shouldBe "amount"
    firstFieldName("SELECT SUM(bi_events.\"amount\") AS s FROM bi_events") shouldBe "amount"
    firstFieldName("SELECT SUM(`bi_events`.`amount`) AS s FROM bi_events") shouldBe "amount"
  }

  /** MEASURED LIMITATION, both spellings, unchanged by this story.
    *
    * `quotedIdentifier` is the FIRST alternative of `SelectParser.field` and of
    * `WhereParser.any_identifier`, and `|` commits to the first SUCCEEDING alternative, so a quoted
    * lexeme at the head of an arithmetic expression is consumed alone and the operator is left
    * unconsumed. That ordering is exactly what makes `SELECT "category"` a column rather than a
    * string, so it cannot be relaxed here.
    *
    * `SELECT "amount" + 1` is rejected on the unmodified tree for this same reason (measured, with
    * `end of input expected` — NOT the type error an earlier reading of the code predicted), so
    * this is a pre-existing limitation the backtick spelling inherits, not a regression. The
    * parenthesised form — which is what every BI tool emits for a calculation — works.
    */
  it should "reject an UNPARENTHESISED arithmetic expression headed by a quoted operand" in {
    rejected("SELECT `amount` + 1 AS a FROM t")
    rejected("SELECT \"amount\" + 1 AS a FROM t")
    rejected("SELECT id FROM t WHERE `amount` + 1 > 5")
    // The bare spelling has no such limitation — `quotedIdentifier` cannot match it at all.
    parses("SELECT amount + 1 AS a FROM t")
    parses("SELECT id FROM t WHERE amount + 1 > 5")
    // …and the parenthesised and nested forms work for the quoted spellings.
    parses("SELECT (`amount` + 1) AS a FROM t")
    parses("SELECT (\"amount\" + 1) AS a FROM t")
    parses("SELECT MAX(`amount` + 1) AS a FROM t")
  }

  "a quoted identifier inside SCRIPT AS" should "parse" in {
    parses(
      "ALTER TABLE users ALTER COLUMN age SET SCRIPT AS (DATE_DIFF(`birthdate`, CURRENT_DATE, YEAR))"
    )
  }

  // ---------------------------------------------------------------------------------------------
  // AC-2 — aliases
  // ---------------------------------------------------------------------------------------------

  "a quoted alias" should "parse in every alias position" in {
    Seq(
      "SELECT amount AS `a` FROM bi_events",
      "SELECT amount AS \"a\" FROM bi_events",
      "SELECT amount `a` FROM bi_events",
      "SELECT amount \"a\" FROM bi_events",
      "SELECT id FROM bi_events `e`",
      "SELECT id FROM bi_events AS `e`",
      "SELECT id FROM bi_events \"e\"",
      "SELECT o.id FROM orders o JOIN customers `c` ON o.id = c.id",
      "SELECT AVG(c.likes) AS a FROM blogs JOIN UNNEST(blogs.comments) AS `c`"
    ).foreach(parses)
  }

  it should "carry the alias through unquoted" in {
    single("SELECT amount AS `my alias` FROM bi_events").select.fields.head.fieldAlias
      .map(_.alias) shouldBe Some("my alias")
  }

  // ---------------------------------------------------------------------------------------------
  // AC-3 — the reserved-word bypass
  // ---------------------------------------------------------------------------------------------

  "a quoted identifier" should "bypass the reserved-word lookahead" in {
    val s = single("SELECT `select`, \"order\", `count` FROM t")
    s.select.fields.map(_.identifier.name) shouldBe Seq("select", "order", "count")
  }

  it should "not relax the guard on the unquoted path" in {
    rejected("SELECT count FROM t")
    rejected("SELECT select FROM t")
  }

  it should "leave a reserved word legal as a NON-FIRST part, as it already is" in {
    // The lookahead was only ever anchored at offset 0, so `t.from` is one identifier today.
    //
    // Assert on `.sql`, NOT on `identifier.name`: `Parser.single` runs `.update()` inside its own
    // combinator action, and when the qualifier IS a declared FROM alias `update()` moves it into
    // `tableAlias` and leaves `name = "from"`. Measured on the unmodified tree:
    // `SELECT t.from FROM x t` -> `name == "from"`, `tableAlias == Some("t")`.
    Parser("SELECT t.from FROM x t").toOption.get.sql should include("t.from")
    Parser("SELECT doc.count FROM x doc").toOption.get.sql should include("doc.count")
    Parser("SELECT o.order FROM x o").toOption.get.sql should include("o.order")

    // Without a matching FROM alias the arity check leaves the joined name in place, so `name` is
    // safe to assert directly here.
    firstFieldName("SELECT t.from FROM x") shouldBe "t.from"
    firstFieldName("SELECT logs.count FROM x") shouldBe "logs.count"
  }

  // ---------------------------------------------------------------------------------------------
  // AC-4 — escaping
  // ---------------------------------------------------------------------------------------------

  "the doubled delimiter" should "escape itself in both styles" in {
    firstFieldName("SELECT \"a\"\"b\" FROM t") shouldBe "a\"b"
    firstFieldName("SELECT `a``b` FROM t") shouldBe "a`b"
  }

  "the legacy backslash escape" should "still work in the double-quoted style" in {
    firstFieldName("SELECT \"a\\\"b\" FROM t") shouldBe "a\"b"
    firstFieldName("SELECT \"a\\\\b\" FROM t") shouldBe "a\\b"
  }

  "a backslash in a backticked identifier" should "be an ordinary character" in {
    firstFieldName("SELECT `a\\b` FROM t") shouldBe "a\\b"
  }

  "a backslash before an ordinary character in a DOUBLE-quoted name" should "escape it" in {
    // N5, a recorded behaviour change. The grammar's `([^"\\]|\\.)` alternative has always ACCEPTED
    // a backslash before any character, but the old two-`replace` chain only ever unescaped the
    // quote and the backslash themselves, so the name came out with the backslash still in it.
    // `unquoteName` is consistent with what the regex accepts. Pinned so the change is a decision.
    firstFieldName("SELECT \"a\\b\" FROM t") shouldBe "ab"
  }

  "an empty double-quoted lexeme" should "stay an empty STRING literal, not a nameless column" in {
    // N4. `SELECT ""` must not become an identifier with an empty name — the content quantifier is
    // `+`, so it falls through to `TypeParser.literal` exactly as `SELECT ''` does.
    val s = single("SELECT \"\" AS e FROM t")
    s.select.fields.head.identifier.name shouldBe ""
    s.select.fields.head.identifier.functions should not be empty
    s.sql should include("''")
  }

  "an empty backtick pair" should "be rejected" in {
    rejected("SELECT `` FROM t")
  }

  // ---------------------------------------------------------------------------------------------
  // AC-6 — case
  // ---------------------------------------------------------------------------------------------

  "whitespace inside quotes" should "be part of the name, not trimmed away" in {
    // The bare render trims the joined name (`parts.mkString(".").trim`), which is right for a name
    // that cannot contain edge whitespace. A QUOTED name can, and it means exactly what is between
    // the delimiters — so the quoted render must not trim. Caught in self-review: trimming per part
    // rendered `` `a ` `` as `"a"`, which re-parses to a DIFFERENT field. Silent, and exactly the
    // render/parse asymmetry this story exists to close.
    firstFieldName("SELECT `a ` FROM t") shouldBe "a "
    firstFieldName("SELECT ` a` FROM t") shouldBe " a"
    firstFieldName("SELECT \"a \" FROM t") shouldBe "a "
    Parser("SELECT `a ` FROM t").toOption.get.sql should include("\"a \"")
  }

  "case inside quotes" should "be preserved verbatim" in {
    firstFieldName("SELECT `Category` FROM t") shouldBe "Category"
    firstFieldName("SELECT \"CaTeGoRy\" FROM t") shouldBe "CaTeGoRy"
    single("SELECT a AS `MyAlias` FROM t").select.fields.head.fieldAlias.map(_.alias) shouldBe
    Some("MyAlias")
  }

  // ---------------------------------------------------------------------------------------------
  // AC-7 — the deliberate narrowings and widenings
  // ---------------------------------------------------------------------------------------------

  "names that the single-regex identifier used to swallow" should "now be rejected loudly" in {
    // N1 / N2. Each parsed before as ONE identifier with a nonsense name; `SELECT a. FROM t` even
    // rendered as `SELECT a`, so its round trip was already broken. Neither addresses an ES field.
    rejected("SELECT a..b FROM t")
    rejected("SELECT a. FROM t")
  }

  it should "still accept an array-subscript part, which is NOT a narrowing" in {
    // A non-first part is `[a-zA-Z0-9_\-\[\]\*]+`, so `[0]` is legal before and after. Pinned
    // because an earlier reading of this change listed it as a rejection.
    firstFieldName("SELECT a.[0] FROM t") shouldBe "a.[0]"
  }

  "the quoted second part of a qualified name" should "be part of the NAME, not the alias" in {
    // N3 — a live silent corruption on main that this change fixes. Measured before: `identifierRegex`
    // ate the trailing dot of `e.` and `quotedAlias` claimed `"category"` as the ALIAS, so
    // `SELECT e."category" FROM bi_events e` rendered `SELECT e AS category FROM bi_events AS e`.
    val s = single("SELECT e.\"category\" FROM bi_events e")
    s.select.fields.head.identifier.name shouldBe "category"
    s.select.fields.head.identifier.tableAlias shouldBe Some("e")
    s.select.fields.head.fieldAlias shouldBe None
    s.sql should include("\"e\".\"category\"")
  }

  "whitespace around the dot separator" should "be tolerated" in {
    // A widening: rejected on the unmodified tree with `end of input expected`.
    firstFieldName("SELECT a . b FROM t") shouldBe "a.b"
  }

  /** The cost of that widening, measured in review and pinned so it is a decision.
    *
    * `nameTail` is `rep("." ~> part)` and RegexParsers skips whitespace before every terminal, so a
    * name ending in a DOT swallows whatever word follows it — including a keyword. Both inputs
    * below are malformed SQL whose old parse was equally nonsense (`identifierRegex` matched the
    * trailing dot, giving the field `b.` / `a.`), so this is a change of one broken reading for
    * another, not a regression of any valid query. It is pinned because the ORDER BY case is
    * SILENT: the direction is absorbed into the name and the sort quietly becomes ASC.
    *
    * Closing it means making the dot and the part one whitespace-free regex, which would also
    * revert the widening pinned just above. Recorded, not fixed.
    */
  it should "swallow the following word when a name ends in a dot — malformed input, pinned" in {
    firstFieldName("SELECT a. AS x FROM t") shouldBe "a.AS"
    single("SELECT a FROM t ORDER BY b. DESC").sql should include("ORDER BY b.DESC ASC")
    // A well-formed qualified name is unaffected — the direction survives.
    single("SELECT a FROM t ORDER BY b.c DESC").sql should include("ORDER BY b.c DESC")
  }

  /** The scanners now treat a backtick as a quote opener, so an ODD backtick outside any quoted run
    * consumes to the end of the input. Every position where a backtick could legitimately appear in
    * previously-valid SQL is inside something the scanners already track, and those are unaffected.
    */
  it should "leave a backtick that appears inside a literal or a comment alone" in {
    Parser("SELECT a FROM t WHERE s = 'x`y'").toOption.get.sql should include("'x`y'")
    Parser("SELECT a FROM t WHERE s LIKE '%`%'").toOption.get.sql should include("'%`%'")
    firstFieldName("SELECT \"a`b\" FROM t") shouldBe "a`b"
    parses("SELECT a FROM t -- a ` backtick in a comment\n")
  }

  "a postfix cast on a quoted identifier" should "parse" in {
    // A widening: `quotedIdentifier` gains `>> cast`, which it did not have. Rejected before.
    parses("SELECT `amount`::BIGINT AS a FROM t")
    parses("SELECT \"amount\"::BIGINT AS a FROM t")
  }

  "a quoted FROM table alias" should "parse" in {
    // A widening: `SELECT id FROM bi_events "e"` was measured REJECTED on the unmodified tree. The
    // table NAME is still 21.2's; only the alias moves here.
    single("SELECT id FROM bi_events \"e\"").sql should include("AS \"e\"")
    single("SELECT id FROM bi_events `e`").sql should include("AS \"e\"")
  }

  "a dotted index-shaped name" should "still parse as one name" in {
    firstFieldName("SELECT logs-2025.03 FROM t") shouldBe "logs-2025.03"
    firstFieldName("SELECT items[0].name FROM t") shouldBe "items[0].name"
    firstFieldName("SELECT _ingest.timestamp FROM t") shouldBe "_ingest.timestamp"
  }

  // ---------------------------------------------------------------------------------------------
  // The four load-bearing clause-level orderings (AD-3)
  // ---------------------------------------------------------------------------------------------

  "a double-quoted operand in a CLAUSE position" should "stay an identifier" in {
    // `SelectParser.field`, `GroupByParser.bucketWithFunction`, `OrderByParser.fieldWithFunction`
    // and `WhereParser.any_identifier` all list `quotedIdentifier` FIRST, ahead of an alternative
    // that reaches `literal`. THAT ordering is load-bearing and this pins all four.
    firstFieldName("SELECT \"category\" FROM t") shouldBe "category"
    single("SELECT \"category\", COUNT(id) AS n FROM t GROUP BY \"category\"").sql should
    include("GROUP BY \"category\"")
    single("SELECT id FROM t ORDER BY \"event_ts\" DESC").sql should include("\"event_ts\"")
    single("SELECT id FROM t WHERE \"category\" IS NOT NULL").sql should include("\"category\"")
  }

  it should "never leak the quoting into anything that reaches Elasticsearch" in {
    // AD-1 rule 4. `Field.sourceField` is the ES field path and `Alias.alias` is the script_fields
    // key; both must be the BARE name.
    val f = single("SELECT `category` AS `c` FROM t").select.fields.head
    f.sourceField shouldBe "category"
    f.identifier.name shouldBe "category"
    f.identifier.identifierName shouldBe "category"
    f.fieldAlias.map(_.alias) shouldBe Some("c")
    // …while the RENDER carries the quoting.
    f.sql should include("\"category\"")
    f.sql should include("\"c\"")
  }

  "a qualified quoted name on the LHS of a comparison" should "parse" in {
    // Rejected on the unmodified tree (`end of input expected`) because `quotedIdentifier` had no
    // dotted tail; `quotedQualifiedName` gives it one.
    parses("SELECT id FROM t WHERE \"a\".\"b\" = 'x'")
    parses("SELECT id FROM t WHERE `a`.`b` = 'x'")
  }

  it should "leave the comparison RHS a literal, as it is today" in {
    // Pre-existing and unchanged: `WhereParser.equality`'s RHS lists `literal` before
    // `any_identifier`. The qualified form on the RHS has no reading at all and stays rejected —
    // recorded, not fixed.
    Parser("SELECT id FROM t WHERE \"a\" = \"b\"").toOption.get.sql should include("= 'b'")
    rejected("SELECT id FROM t WHERE a = \"t\".\"c\"")
  }

  // ---------------------------------------------------------------------------------------------
  // AD-12 — the 21.3 interaction
  // ---------------------------------------------------------------------------------------------

  "a name that merely CONTAINS a digit under GROUP BY" should "still fail, as it does today" in {
    // KNOWN, PRE-EXISTING, and 21.3's to fix. `SingleSearch.bucketNames` detects an ordinal with a
    // digit search over the rendered name, so `GROUP BY "city2"` indexes `select.fields(1)` on a
    // one-item SELECT. Measured on the unmodified tree: the same `IndexOutOfBoundsException: 1`,
    // surfaced as a `Left` by the boundary catch story 21.4 installed.
    //
    // This story does not fix it; it adds the backtick spelling as a second door to it, so the
    // crash is pinned here rather than inherited silently. 21.3 replaces `bucketNames`'s
    // discriminator, at which point these two rows MUST be RETARGETED to 21.3's rejection message —
    // never deleted. They pin a contract (the input is rejected, not crashed through), and only the
    // route changes.
    rejectedInternally("SELECT `city2` FROM t GROUP BY `city2`", "IndexOutOfBounds")
    rejectedInternally("SELECT \"city2\" FROM t GROUP BY \"city2\"", "IndexOutOfBounds")
  }

  "a quoted digit" should "be a column NAME, never an ordinal" in {
    // The property 21.3's ordinal predicate preserves: a quoted `1` has `name == "1"` and NO
    // functions, an ordinal has an empty name and a `LongValue`. This is what lets a customer
    // address an ES field whose name is a digit.
    val id = single("SELECT `1` FROM t").select.fields.head.identifier
    id.name shouldBe "1"
    id.functions shouldBe empty
  }

  // ---------------------------------------------------------------------------------------------
  // AC-8 — the scanners (the two that live in `sql`)
  // ---------------------------------------------------------------------------------------------

  "a double-dash inside a backticked identifier" should "not start a comment" in {
    firstFieldName("SELECT `a--b` FROM t") shouldBe "a--b"
  }

  "a newline inside a statement carrying backticks" should "not break the scan" in {
    parses("SELECT `a` FROM t\nWHERE `b` = 1")
  }

  "a parenthesis inside a backticked identifier" should "not close a SCRIPT AS body" in {
    parses("ALTER TABLE t ALTER COLUMN c SET SCRIPT AS (UPPER(`a)b`))")
  }

  // ---------------------------------------------------------------------------------------------
  // AC-9 — the #252 acceptance table
  // ---------------------------------------------------------------------------------------------

  "the #252 acceptance table" should "have the verdicts story 21.1 owns" in {
    // Already parsing — must keep parsing.
    parses("SELECT category FROM bi_events LIMIT 1")
    parses("SELECT category FROM elastic.bi_events LIMIT 1")
    parses("SELECT category FROM \"elastic\".bi_events LIMIT 1")
    parses("SELECT \"category\" FROM bi_events LIMIT 1")
    parses("SELECT e.category FROM elastic.bi_events e LIMIT 1")
    // Row 8 — the one expression-position row, and the one this story flips.
    parses("SELECT `category` FROM bi_events LIMIT 1")
  }

  it should "leave the four FROM-position rows to story 21.2" in {
    // Rows 2, 3, 5 and 7. `FromParser.table` still consumes `identifierRegex` directly, together
    // with `quotedSchemaPrefix`, which DISCARDS the prefix — swapping it here would silently turn
    // `FROM "elastic".bi_events` into index `elastic.bi_events`. 21.2 owns it, with #85.
    rejected("SELECT category FROM `bi_events` LIMIT 1")
    rejected("SELECT category FROM \"bi_events\" LIMIT 1")
    rejected("SELECT category FROM `elastic`.`bi_events` LIMIT 1")
    rejected("SELECT category FROM \"elastic\".\"bi_events\" LIMIT 1")
  }
}
