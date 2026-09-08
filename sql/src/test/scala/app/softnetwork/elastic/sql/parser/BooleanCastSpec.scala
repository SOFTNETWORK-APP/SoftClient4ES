package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.{PainlessContext, PainlessContextType, PainlessScript}
import app.softnetwork.elastic.sql.function.cond.Case
import app.softnetwork.elastic.sql.query.SingleSearch
import app.softnetwork.elastic.sql.schema.{Column, Table}
import app.softnetwork.elastic.sql.`type`.{SQLTypeUtils, SQLTypes}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

/** Story 21.8 part A — `CAST(x AS BOOLEAN)` was a silent no-op.
  *
  * `SQLTypeUtils.coerce` had seven arms FROM boolean and none TO it, so every `(_, Boolean)` pair
  * fell to the identity fallback: `CAST(1 AS BOOLEAN)` emitted `1` and `CAST('true' AS BOOLEAN)`
  * emitted `"true"`. The cast parsed, rendered and round-tripped, and did nothing — the #205
  * silent-wrong-answer family.
  *
  * Semantics are the lead's PD-1 ruling: C-STYLE. Numerics compare against zero; strings go through
  * `Boolean.parseBoolean`, so `'true'` (any case) is true and every other string is false.
  *
  * 🔴 Every emission below was EXECUTED on a real Elasticsearch 8.18.3 via `POST
  * /_scripts/painless/_execute` before being pinned here — `Boolean.parseBoolean` being
  * whitelisted, and `!= 0` accepting a `def`, are Painless claims, and only Elasticsearch settles
  * those (this project's standing rule; story 21.5 paid for it twice).
  *
  * Never pin the BEFORE strings — they are the silent wrong answers this story fixes.
  */
class BooleanCastSpec extends AnyFlatSpec with Matchers {

  /** A schema-CARRYING fixture. A schema-less parse leaves every identifier's `baseType` at `Any`,
    * so no conversion arm is reachable at all and a broken engine looks identical to a fixed one —
    * that is exactly how the C1/C2 defects survived until story 21.5.
    */
  private val schema: Table = Table(
    "t",
    columns = List(
      Column("flag", SQLTypes.Keyword),
      Column("descr", SQLTypes.Text),
      Column("n", SQLTypes.Int),
      Column("big", SQLTypes.BigInt),
      Column("amount", SQLTypes.Double),
      Column("ok", SQLTypes.Boolean),
      Column("ts", SQLTypes.Timestamp)
    )
  )

  private def painlessOf(sql: String): String =
    Parser(sql) match {
      case Right(ss: SingleSearch) => ss.update(Some(schema)).select.fields.head.painless(None)
      case other                   => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  /** 🔴 The PRODUCTION renderer, and the reason this file needs two.
    *
    * `painlessOf` above renders with `painless(None)`. That is faithful for a CAST, whose emission
    * does not branch on the context. It is BLIND to `CASE`, which renders an entirely different way
    * once a `PainlessContext` exists: only that branch hoists each condition into a `def paramN =
    * ...` binding, and only that branch coerces the condition. So a context-free assertion cannot
    * see the defect below no matter what it pins.
    *
    * This is the same distinction story 21.8's T0 refutation turned on — a context-free rendering
    * is not a production one — and it is the reason the whole unit estate (945 sql + 924 core + 2 ×
    * 197 bridge) stayed green while every live client failed.
    */
  private def scriptOf(sql: String): String =
    Parser(sql) match {
      case Right(ss: SingleSearch) =>
        val ctx = PainlessContext(PainlessContextType.Query)
        val body = ss.update(Some(schema)).select.fields.head.painless(Some(ctx))
        s"$ctx$body"
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  private def conditionsOf(sql: String): List[PainlessScript] =
    Parser(sql) match {
      case Right(ss: SingleSearch) =>
        ss.update(Some(schema))
          .select
          .fields
          .head
          .identifier
          .functions
          .collectFirst { case c: Case =>
            c.conditions.map { case (cond, _) => cond }
          }
          .getOrElse(fail(s"[$sql] no CASE in the function chain"))
      case other => fail(s"[$sql] expected a SingleSearch, got $other")
    }

  "a cast of a numeric LITERAL to boolean" should "compare against zero, not vanish" in {
    // Executed on ES 8.18.3: `(1 != 0)` -> true, `(0 != 0)` -> false, `(1.5 != 0)` -> true.
    painlessOf("SELECT CAST(1 AS BOOLEAN) FROM t") shouldBe "(1 != 0)"
    painlessOf("SELECT CAST(0 AS BOOLEAN) FROM t") shouldBe "(0 != 0)"
    painlessOf("SELECT CAST(1.5 AS BOOLEAN) FROM t") shouldBe "(1.5 != 0)"
  }

  "a cast of a string LITERAL to boolean" should "parse it" in {
    // Executed on ES 8.18.3: "true" -> true, "TRUE" -> true (parseBoolean is case-insensitive).
    painlessOf("SELECT CAST('true' AS BOOLEAN) FROM t") shouldBe """Boolean.parseBoolean("true")"""
    painlessOf("SELECT CAST('TRUE' AS BOOLEAN) FROM t") shouldBe """Boolean.parseBoolean("TRUE")"""
  }

  it should "treat a NUMERIC string as false - the documented sharp edge of the C-style rule" in {
    // 🔴 Deliberate, ruled, and written down in functions_type_conversion.md rather than hidden:
    // the numeric rule applies to a numeric OPERAND, not to a string that looks numeric.
    // Executed on ES 8.18.3: `Boolean.parseBoolean("1")` -> false.
    painlessOf("SELECT CAST('1' AS BOOLEAN) FROM t") shouldBe """Boolean.parseBoolean("1")"""
  }

  "a cast of a COLUMN to boolean" should "convert through the mapped type" in {
    // The shape a BI tool generates. Before this story both emitted the raw doc value.
    painlessOf("SELECT CAST(flag AS BOOLEAN) FROM t") should include("Boolean.parseBoolean")
    painlessOf("SELECT CAST(descr AS BOOLEAN) FROM t") should include("Boolean.parseBoolean")
    painlessOf("SELECT CAST(n AS BOOLEAN) FROM t") should include("!= 0")
    painlessOf("SELECT CAST(big AS BOOLEAN) FROM t") should include("!= 0")
    painlessOf("SELECT CAST(amount AS BOOLEAN) FROM t") should include("!= 0")
  }

  it should "stay null-guarded, so a document missing the field does not fail the shard" in {
    // The guard the four temporal arms had to grow for themselves (#306). These arms fall through
    // to the end-of-method wrapper, so it applies for free -- asserted rather than assumed.
    // Executed on ES 8.18.3 with a null operand: the whole expression evaluates to null.
    painlessOf("SELECT CAST(flag AS BOOLEAN) FROM t") should include("!= null")
    painlessOf("SELECT CAST(n AS BOOLEAN) FROM t") should include("!= null")
  }

  "a boolean operand" should "still reach the identity arm, not a new conversion" in {
    // The new arms ENUMERATE their sources precisely so `(Boolean, Boolean)` falls through to the
    // identity arm below them. A wildcard `(_, Boolean)` would have shadowed it.
    val painless = painlessOf("SELECT CAST(ok AS BOOLEAN) FROM t")
    painless should not include "parseBoolean"
    painless should not include "!= 0"
  }

  "a temporal operand" should "keep falling through, not be coerced into a boolean" in {
    val painless = painlessOf("SELECT CAST(ts AS BOOLEAN) FROM t")
    painless should not include "parseBoolean"
    painless should not include "!= 0"
  }

  "a CASE condition" should "reach Painless as the boolean it already is" in {
    // 🔴 The regression this section exists for, and the only defect in story 21.8 that reached a
    // real cluster. A criterion's `baseType` reported its LEFT OPERAND's type (`1 = 1` -> BIGINT,
    // `descr = 'x'` -> the column's), which was inert while `coerce` had no `(_, BOOLEAN)` arm --
    // the pair fell to the identity fallback and the comparison was emitted untouched. Part A added
    // the arms, so `CASE WHEN 1 = 1` coerced an already-boolean comparison FROM BIGINT, emitting
    // `def param1 = (1 == 1 != null ? (def)((1 == 1 != 0)) : null); ...` -- which Elasticsearch
    // refuses to compile (`class_cast_exception: Cannot cast from [boolean] to [java.lang.Object].`
    // under `all shards failed; compile error`, HTTP 400). All five clients failed identically.
    //
    // Every string below is byte-for-byte what `origin/main` emits, so this pins a RESTORATION, not
    // a new shape. All four were executed on a real Elasticsearch 8.18.3.
    scriptOf("SELECT CASE WHEN 1 = 1 THEN 'a' ELSE 'b' END AS c FROM t") shouldBe
    "def param1 = 1 == 1; param1 ? \"a\" : \"b\""
    scriptOf("SELECT CASE WHEN 2 > 1 THEN 1 ELSE 0 END AS c FROM t") shouldBe
    "def param1 = 2 > 1; param1 ? 1 : 0"
    // a COLUMN operand -- ANY before, so no arm ever fired; pinned so it stays that way
    scriptOf("SELECT CASE WHEN n > 1 THEN 1 ELSE 0 END AS c FROM t") shouldBe
    "def param1 = (doc['n'].size() == 0 ? null : doc['n'].value); " +
    "def param2 = param1 == null ? false : (param1 > 1); param2 ? 1 : 0"
    // a VARCHAR column operand -- the `Boolean.parseBoolean` arm's source type, so this is the
    // shape that would have been corrupted into `Boolean.parseBoolean(<a comparison>)`
    scriptOf("SELECT CASE WHEN descr = 'x' THEN 1 ELSE 0 END AS c FROM t") shouldBe
    "def param1 = (doc['descr'].size() == 0 ? null : doc['descr'].value); " +
    "def param2 = param1 == null ? false : (param1.compareTo(\"x\") == 0); param2 ? 1 : 0"
  }

  it should "report BOOLEAN as its own base type, whatever it compares" in {
    // The invariant behind the four pins above, stated where it can be checked without an emission:
    // `baseType` is what EVERY `SQLTypeUtils.coerce` call site reads to learn the type of the
    // string it was handed (`coerce(in, to, ctx)` takes `expr = in.painless(ctx)` and `from =
    // in.baseType`). A criterion emits a comparison, so that type is BOOLEAN and nothing else.
    // Asserting it here means the next `(_, BOOLEAN)` arm cannot re-open this hole silently.
    conditionsOf("SELECT CASE WHEN 1 = 1 THEN 1 END AS c FROM t").map(_.baseType) shouldBe
    List(SQLTypes.Boolean)
    conditionsOf("SELECT CASE WHEN descr = 'x' THEN 1 END AS c FROM t").map(_.baseType) shouldBe
    List(SQLTypes.Boolean)
    conditionsOf("SELECT CASE WHEN ts > '2025-01-01' THEN 1 END AS c FROM t").map(
      _.baseType
    ) shouldBe
    List(SQLTypes.Boolean)
  }

  "canConvert" should "be untouched by this story (AC-6)" in {
    // 🔴 `canConvert` gates SCHEMA EVOLUTION through `schema/TableDiff.scala` -- a different
    // contract with a different blast radius (story 21.5 AD, re-confirmed by 21.8). It is a
    // lookalike of `coerce` and it deliberately DISAGREES with it here: `coerce` now converts a
    // numeric to a boolean, while an existing `long` column may still not be REDEFINED as
    // `boolean`. Pinned so that "make them agree" is a decision someone has to take on purpose.
    SQLTypeUtils.canConvert(SQLTypes.Int, SQLTypes.Boolean) shouldBe false
    SQLTypeUtils.canConvert(SQLTypes.Keyword, SQLTypes.Boolean) shouldBe false
    SQLTypeUtils.canConvert(SQLTypes.Boolean, SQLTypes.Boolean) shouldBe true
  }
}
