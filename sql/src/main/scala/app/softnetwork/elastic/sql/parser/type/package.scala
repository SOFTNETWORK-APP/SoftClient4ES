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

package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.{
  unescapeStringLiteral,
  BooleanValue,
  BooleanValues,
  DoubleValue,
  DoubleValues,
  IdValue,
  Identifier,
  IngestTimestampValue,
  LongValue,
  LongValues,
  Null,
  ParamValue,
  PiValue,
  RandomValue,
  StringValue,
  StringValues,
  Value
}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}

package object `type` {

  trait TypeParser { self: Parser =>

    /** Each branch is ONE atomic regex, quotes included. Splitting it into `"'" ~> content ~> "'"`
      * lets RegexParsers skip whitespace between the opening quote and the content, so `' Bob'`
      * silently parsed as `'Bob'` — the literal's interior must never be subject to whitespace
      * skipping (COPY INTO paths, for one, contractually preserve it — see LocalPath).
      *
      * Both branches accept the SQL-standard DOUBLED delimiter (`''` / `""`, issue #274) beside the
      * grammar's legacy backslash escapes. The three content alternatives are disjoint on their
      * FIRST character — `[^'\\]` excludes both, `\\.` starts with a backslash, `''` with a quote —
      * so the quantifier is deterministic and cannot backtrack catastrophically.
      *
      * Nothing that parsed before can change meaning: the content alternatives cannot cross a LONE
      * quote, so the only place the widened regex reaches further than the old one is across a
      * DOUBLED quote — and two adjacent literals with no separator were never a valid parse in this
      * grammar (`repsep`/`separator` everywhere).
      *
      * The double-quoted branch does NOT make `"a""b"` a string wherever it appears: a quoted
      * lexeme in an identifier position is claimed first by `quotedIdentifier` /
      * `quotedIdentifierUnlessArithmetic`, whose own regex has accepted the doubled escape since
      * story 21.1. Which reading wins is decided by production order, exactly as it already is for
      * a bare `"x"`. An EMPTY `""` stays the empty STRING — `quotedNameRegex`'s content quantifier
      * is `+` on purpose.
      */
    def literal: PackratParser[StringValue] =
      (""""([^"\\]|\\.|"")*"""".r ^^ { str =>
        StringValue(unescapeStringLiteral(str.substring(1, str.length - 1), '"'))
      }) | ("""'([^'\\]|\\.|'')*'""".r ^^ { str =>
        StringValue(unescapeStringLiteral(str.substring(1, str.length - 1), '\''))
      })

    def long: PackratParser[LongValue] =
      """(-)?(0|[1-9]\d*)""".r ^^ (str => LongValue(str.toLong))

    def double: PackratParser[DoubleValue] =
      """(-)?(\d+\.\d+)""".r ^^ (str => DoubleValue(str.toDouble))

    def pi: PackratParser[Value[Double]] =
      PiValue.regex ^^ (_ => PiValue)

    def random: PackratParser[Value[Double]] = """(?i)random\b""".r ^^ (_ => RandomValue)

    def boolean: PackratParser[BooleanValue] =
      """(?i)(true|false)\b""".r ^^ (bool => BooleanValue(bool.toBoolean))

    def param: PackratParser[ParamValue.type] =
      "?" ^^ (_ => ParamValue)

    def nullValue: PackratParser[Null.type] =
      "(?i)NULL\\b".r ^^ (_ => Null)

    def literals: PackratParser[Value[_]] = "[" ~> repsep(literal, ",") <~ "]" ^^ { list =>
      StringValues(list)
    }

    def longs: PackratParser[Value[_]] = "[" ~> repsep(long, ",") <~ "]" ^^ { list =>
      LongValues(list)
    }

    def doubles: PackratParser[Value[_]] = "[" ~> repsep(double, ",") <~ "]" ^^ { list =>
      DoubleValues(list)
    }

    def booleans: PackratParser[BooleanValues] = "[" ~> repsep(boolean, ",") <~ "]" ^^ { list =>
      BooleanValues(list)
    }

    def array: PackratParser[Value[_]] = literals | longs | doubles | booleans

    def value: PackratParser[Value[_]] =
      literal | pi | random | double | long | boolean | nullValue | param | array

    /** The two ingest-time placeholders a column DEFAULT may carry. They live beside `value` so
      * every value position can opt into them — `object Parser`'s `defaultVal` and `trait Parser`'s
      * `option` both need them, and only the former could see them while they were declared in the
      * object.
      */
    def ingest_id: PackratParser[Value[_]] = "_id" ^^ (_ => IdValue)

    def ingest_timestamp: PackratParser[Value[_]] =
      "_ingest.timestamp" ^^ (_ => IngestTimestampValue)

    def identifierWithValue: Parser[Identifier] = (value ^^ functionAsIdentifier) >> cast

    /** An UNSIGNED decimal integer — deliberately NOT `long`, whose regex carries an optional sign
      * and would accept `CHAR(-1)`.
      */
    private def typeParamValue: PackratParser[String] = """\d+""".r ^^ (v => v)

    /** An ANSI/MySQL type parameter list — `CHAR(10)`, `DECIMAL(10,2)`, `INT(11)`, `TIMESTAMP(3)`.
      *
      * Parsed so BI-generated SQL is accepted, then DISCARDED: Elasticsearch has no length- or
      * scale-constrained scalar type. The loss is not hidden — the render normalises the type to
      * its unparameterised name, so every artifact that echoes a statement shows what the engine
      * actually applied.
      *
      * At most `(p[,s])`, and each parameter unsigned. SQL has no third parameter and no negative
      * precision, and since the values are discarded anyway strictness costs nothing behaviourally
      * while keeping the greedy window as small as possible.
      *
      * 🔴 Attached to each INDIVIDUAL type production that SQL parameterises, and NEVER to
      * `sql_type` as a whole. In DDL a type is followed by `FIELDS (…)` / `SCRIPT AS (…)` / `OPTION
      * (…)`, and a `::` cast is followed by arbitrary expression text — an optional `(`-consuming
      * suffix on the shared production is exactly the paren-greed failure mode of #219/#220. It is
      * safe here because `opt(p)` desugars to `p ^^ Some | success(None)` and the inner sequence
      * yields a `Failure` (never an `Error` — none of `start`/`separator`/`end` is `~!`/`err`),
      * which `|` recovers, restoring the input position.
      */
    def typeParams: PackratParser[Unit] =
      opt(start ~ typeParamValue ~ opt(separator ~ typeParamValue) ~ end) ^^ (_ => ())

    def char_type: PackratParser[SQLTypes.Char.type] =
      "(?i)char".r ~ typeParams ^^ (_ => SQLTypes.Char)

    def string_type: PackratParser[SQLTypes.Varchar.type] =
      "(?i)varchar|string".r ~ typeParams ^^ (_ => SQLTypes.Varchar)

    /** `DECIMAL` / `NUMERIC` / `DEC` map to DOUBLE, **not** to `SQLTypes.Numeric`.
      *
      * The `Numeric` case object exists and looks like the right target, but `SQLTypeUtils.coerce`
      * has exactly ONE arm producing it (from a Boolean); everything else falls through to the
      * identity fallback and would emit the operand UNCONVERTED — the #205 silent-wrong-answer
      * shape. DOUBLE is also what Elasticsearch actually stores for these.
      */
    def decimal_type: PackratParser[SQLTypes.Double.type] =
      """(?i)(decimal|numeric|dec)\b""".r ~ typeParams ^^ (_ => SQLTypes.Double)

    /** MySQL's `CAST(x AS SIGNED)` / `AS UNSIGNED` — what Tableau's MySQL dialect emits. Both are
      * 64-bit integer targets; Elasticsearch's `unsigned_long` is not in this type system, so
      * UNSIGNED is an alias of BIGINT and the documentation says so rather than pretending. Neither
      * spelling takes a parameter list in any dialect, so neither gets one.
      */
    def signed_type: PackratParser[SQLTypes.BigInt.type] =
      """(?i)(signed|unsigned)(\s+(integer|int))?\b""".r ^^ (_ => SQLTypes.BigInt)

    /** No dialect gives `DATE` a precision — a fractional-seconds precision belongs to `TIME`,
      * `TIMESTAMP` and `DATETIME`.
      */
    def date_type: PackratParser[SQLTypes.Date.type] = "(?i)date".r ^^ (_ => SQLTypes.Date)

    def time_type: PackratParser[SQLTypes.Time.type] =
      "(?i)time".r ~ typeParams ^^ (_ => SQLTypes.Time)

    def datetime_type: PackratParser[SQLTypes.DateTime.type] =
      "(?i)(datetime)".r ~ typeParams ^^ (_ => SQLTypes.DateTime)

    def timestamp_type: PackratParser[SQLTypes.Timestamp.type] =
      "(?i)(timestamp)".r ~ typeParams ^^ (_ => SQLTypes.Timestamp)

    def boolean_type: PackratParser[SQLTypes.Boolean.type] =
      "(?i)boolean".r ^^ (_ => SQLTypes.Boolean)

    def byte_type: PackratParser[SQLTypes.TinyInt.type] =
      "(?i)(byte|tinyint)".r ~ typeParams ^^ (_ => SQLTypes.TinyInt)

    def short_type: PackratParser[SQLTypes.SmallInt.type] =
      "(?i)(short|smallint)".r ~ typeParams ^^ (_ => SQLTypes.SmallInt)

    def int_type: PackratParser[SQLTypes.Int.type] =
      "(?i)(integer|int)".r ~ typeParams ^^ (_ => SQLTypes.Int)

    def long_type: PackratParser[SQLTypes.BigInt.type] =
      "(?i)long|bigint".r ~ typeParams ^^ (_ => SQLTypes.BigInt)

    def double_type: PackratParser[SQLTypes.Double.type] =
      "(?i)double".r ~ typeParams ^^ (_ => SQLTypes.Double)

    def float_type: PackratParser[SQLTypes.Real.type] =
      "(?i)float|real".r ~ typeParams ^^ (_ => SQLTypes.Real)

    def struct_type: PackratParser[SQLTypes.Struct.type] =
      "(?i)struct".r ^^ (_ => SQLTypes.Struct)

    def array_type: PackratParser[SQLTypes.Array] =
      "(?i)array<".r ~> sql_type <~ ">" ^^ { elementType =>
        SQLTypes.Array(elementType)
      }

    def binary_type: PackratParser[SQLTypes.VarBinary.type] =
      "(?i)(binary|varbinary)".r ~ typeParams ^^ (_ => SQLTypes.VarBinary)

    /** 🔴 The ORDER is load-bearing: none of these regexes carries a `\b`, so a shorter name that
      * prefixes a longer one must come SECOND (`timestamp` before `time`, `datetime` before
      * `date`). The alternatives added for #275 — `decimal_type`, `signed_type`, `text_type`,
      * `keyword_type` — share no prefix with any existing name, which is why they can be inserted
      * without moving anything.
      *
      * `text_type` / `keyword_type` used to live OUTSIDE this production, in `extension_type` (DDL
      * only) — which is precisely why a DQL cast to `TEXT` or `KEYWORD` was rejected while `CREATE
      * TABLE t (c TEXT)` worked.
      */
    def sql_type: PackratParser[SQLType] =
      char_type |
      string_type |
      decimal_type |
      signed_type |
      text_type |
      keyword_type |
      datetime_type |
      timestamp_type |
      date_type |
      time_type |
      boolean_type |
      long_type |
      double_type |
      float_type |
      int_type |
      short_type |
      byte_type |
      struct_type |
      array_type |
      binary_type

    def text_type: PackratParser[SQLTypes.Text.type] =
      "(?i)text".r ~ typeParams ^^ (_ => SQLTypes.Text)

    /** Elasticsearch's `keyword` has no length, in SQL or in ES. */
    def keyword_type: PackratParser[SQLTypes.Keyword.type] =
      "(?i)keyword".r ^^ (_ => SQLTypes.Keyword)

    def geo_point_type: PackratParser[SQLTypes.GeoPoint.type] =
      "(?i)(geo_point|geopoint)".r ^^ (_ => SQLTypes.GeoPoint)

    def extension_type: PackratParser[SQLType] =
      sql_type | geo_point_type
  }
}
