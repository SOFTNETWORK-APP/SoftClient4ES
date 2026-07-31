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

package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.function.aggregate.{
  ARRAY_AGG,
  AVG,
  COUNT,
  DENSE_RANK,
  FIRST_VALUE,
  LAST_VALUE,
  MAX,
  MIN,
  OVER,
  PARTITION_BY,
  PERCENTILE_CONT,
  PERCENTILE_DISC,
  RANK,
  ROW_NUMBER,
  STDDEV,
  STDDEV_POP,
  STDDEV_SAMP,
  SUM,
  VARIANCE,
  VAR_POP,
  VAR_SAMP
}
import app.softnetwork.elastic.sql.function.cond.{
  Case,
  Coalesce,
  ELSE,
  END,
  Greatest,
  IsNotNull,
  IsNull,
  Least,
  NullIf,
  THEN,
  WHEN
}
import app.softnetwork.elastic.sql.function.convert.{Cast, Convert, TryCast}
import app.softnetwork.elastic.sql.function.geo.{Distance, Point}
import app.softnetwork.elastic.sql.function.math.{
  Abs,
  Acos,
  Asin,
  Atan,
  Atan2,
  Ceil,
  Cos,
  Degrees,
  Exp,
  Floor,
  Log,
  Log10,
  Pow,
  Radians,
  Round,
  Sign,
  Sin,
  Sqrt,
  Tan
}
import app.softnetwork.elastic.sql.function.string.{
  Concat,
  For,
  LeftOp,
  Length,
  Lower,
  Ltrim,
  Position,
  RegexpLike,
  Replace,
  Reverse,
  RightOp,
  Rtrim,
  Substring,
  Trim,
  Upper
}
import app.softnetwork.elastic.sql.function.time.{
  CurrentDate,
  CurrentTime,
  CurrentTimestamp,
  DateAdd,
  DateDiff,
  DateFormat,
  DateParse,
  DateSub,
  DateTimeAdd,
  DateTimeFormat,
  DateTimeParse,
  DateTimeSub,
  DateTrunc,
  Extract,
  LastDayOfMonth,
  Now,
  Today
}
import app.softnetwork.elastic.sql.operator.{
  AGAINST,
  AND,
  BETWEEN,
  Child,
  IN,
  IS_NOT_NULL,
  IS_NULL,
  LIKE,
  MATCH,
  NOT,
  Nested,
  OR,
  Parent,
  RLIKE,
  UNION
}
import app.softnetwork.elastic.sql.query.{
  Asc,
  CrossJoin,
  Desc,
  Except,
  From,
  FullJoin,
  GroupBy,
  Having,
  InnerJoin,
  Join,
  LeftJoin,
  Limit,
  NullsFirst,
  NullsLast,
  Offset,
  On,
  OrderBy,
  RightJoin,
  Select,
  Unnest,
  Where
}
import app.softnetwork.elastic.sql.time.{Interval, IsoField, TimeField}

/** Single source of truth for the SQL keywords the parser understands (#161).
  *
  * The parser has four keyword surfaces; this object curates them into one place:
  *   1. `TokenRegex` objects -> `clauseTokens` / `functionTokens` / `literalTokens`
  *   1. `Parser.keyword("…")` literals -> `statementWords`
  *   1. type / literal parser regexes -> `typeWords` / `literalWords`
  *   1. `Parser.reservedKeywords` -> read-only, tied in by `SQLKeywordsSpec`
  *
  * Anti-drift: `SQLKeywordsSpec` (sql) scans the parser sources; `ReplKeywordsSpec` (core) asserts
  * the REPL highlighter/completer consume this registry. When you add a SQL keyword (a new
  * `TokenRegex` object or a new `keyword("…")` literal), add it here or those tests fail.
  *
  * Deliberately NOT listed: pure-symbol tokens (`=`, `::`, `||`, `?`, arithmetic operators) and geo
  * distance units (`km`, `m`, `cm`, `mm`, `mi`, `yd`, `ft`, `in`, `nmi`) — unit codes are
  * 1–3-letter strings that collide with short identifiers and must not be colourised or completed.
  * (The word `IN` still highlights — it arrives via the `operator.IN` token; only the unit *tokens*
  * are unlisted.)
  *
  * Known scan limitation: literal-value tokens declared `extends Value[...] with TokenRegex` (Null,
  * PiValue, RandomValue, EValue, ParamValue, IdValue, IngestTimestampValue) do not match the
  * Expr-scan pattern in `SQLKeywordsSpec` — a NEW token of that shape must be added to
  * `literalTokens` by hand.
  */
object SQLKeywords {

  /** Clause, join, operator and CASE syntax keywords (word-bearing TokenRegex objects). */
  val clauseTokens: List[TokenRegex] = List(
    Select,
    Distinct,
    From,
    Where,
    GroupBy,
    Having,
    OrderBy,
    Asc,
    Desc,
    NullsFirst,
    NullsLast,
    Limit,
    Offset,
    Alias,
    Except,
    UNION,
    InnerJoin,
    LeftJoin,
    RightJoin,
    FullJoin,
    CrossJoin,
    Join,
    On,
    Unnest,
    AND,
    OR,
    NOT,
    IN,
    LIKE,
    RLIKE,
    BETWEEN,
    IS_NULL,
    IS_NOT_NULL,
    MATCH,
    AGAINST,
    Nested,
    Child,
    Parent,
    Case,
    WHEN,
    THEN,
    ELSE,
    END,
    OVER,
    PARTITION_BY,
    Interval,
    For
  )

  /** Function-name keywords (aggregate, window, conditional, conversion, math, string, temporal,
    * geo — all are TokenRegex via the `Function`-derived traits).
    */
  val functionTokens: List[TokenRegex] = List(
    COUNT,
    MIN,
    MAX,
    AVG,
    SUM,
    STDDEV,
    STDDEV_POP,
    STDDEV_SAMP,
    VARIANCE,
    VAR_POP,
    VAR_SAMP,
    PERCENTILE_CONT,
    PERCENTILE_DISC,
    FIRST_VALUE,
    LAST_VALUE,
    ARRAY_AGG,
    ROW_NUMBER,
    RANK,
    DENSE_RANK,
    Coalesce,
    IsNull,
    IsNotNull,
    NullIf,
    Greatest,
    Least,
    Cast,
    TryCast,
    Convert,
    Abs,
    Ceil,
    Floor,
    Round,
    Exp,
    Log,
    Log10,
    Pow,
    Sqrt,
    Sign,
    Sin,
    Asin,
    Cos,
    Acos,
    Tan,
    Atan,
    Atan2,
    Degrees,
    Radians,
    Concat,
    Lower,
    Upper,
    Trim,
    Ltrim,
    Rtrim,
    Substring,
    LeftOp,
    RightOp,
    Length,
    Replace,
    Reverse,
    Position,
    RegexpLike,
    CurrentDate,
    CurrentTime,
    CurrentTimestamp,
    Now,
    Today,
    DateTrunc,
    Extract,
    LastDayOfMonth,
    DateDiff,
    DateAdd,
    DateSub,
    DateParse,
    DateFormat,
    DateTimeAdd,
    DateTimeSub,
    DateTimeParse,
    DateTimeFormat,
    Point,
    Distance,
    TimeField.YEAR,
    TimeField.MONTH_OF_YEAR,
    TimeField.DAY_OF_MONTH,
    TimeField.DAY_OF_WEEK,
    TimeField.DAY_OF_YEAR,
    TimeField.HOUR_OF_DAY,
    TimeField.MINUTE_OF_HOUR,
    TimeField.SECOND_OF_MINUTE,
    TimeField.NANO_OF_SECOND,
    TimeField.MICRO_OF_SECOND,
    TimeField.MILLI_OF_SECOND,
    TimeField.EPOCH_DAY,
    TimeField.OFFSET_SECONDS,
    IsoField.QUARTER_OF_YEAR,
    IsoField.WEEK_OF_WEEK_BASED_YEAR
  )

  /** Word-like literal value tokens (TokenRegex). `EValue` ("E") is listed for completeness but is
    * dropped from `highlightedWords` by the length->=2 rule.
    */
  val literalTokens: List[TokenRegex] = List(Null, PiValue, RandomValue, EValue)

  /** Boolean literals are parsed by `TypeParser.boolean` (type/package.scala:59-60), not via
    * TokenRegex.
    */
  val literalWords: Set[String] = Set("TRUE", "FALSE")

  /** Statement-level keywords matched by `Parser.keyword("…")` (Parser.scala:1118) across
    * Parser.scala / DmlParser.scala. Curated copy — `SQLKeywordsSpec` scans the sources and fails
    * if a `keyword("…")` literal is missing here.
    */
  val statementWords: Set[String] = Set(
    "ADD",
    "ALIAS",
    "ALTER",
    "ALWAYS",
    "AS",
    "AT",
    "BY",
    "CLUSTER",
    "COLUMN",
    "COMMENT",
    "CONFLICT",
    "COPY",
    "CREATE",
    "DATA",
    "DAY",
    "DEFAULT",
    "DELETE",
    "DELTA_LAKE",
    "DESC",
    "DESCRIBE",
    "DO",
    "DROP",
    "END",
    "ENRICH",
    "EVERY",
    "EXECUTE",
    "EXISTS",
    "FIELD",
    "FIELDS",
    "FILE_FORMAT",
    "FOREACH",
    "FROM",
    "GEO_MATCH",
    "HOUR",
    "IF",
    "INDEX",
    "INPUT",
    "INPUTS",
    "INSERT",
    "INTO",
    "JSON",
    "JSON_ARRAY",
    "KEY",
    "LANG",
    "LICENSE",
    "LIKE",
    "LIMIT",
    "LOG",
    "MAPPING",
    "MATCH",
    "MATERIALIZED",
    "MINUTE",
    "MONTH",
    "NAME",
    "NEVER",
    "NOT",
    "NOTHING",
    "NOW",
    "NULL",
    "ON",
    "OPTION",
    "OPTIONS",
    "OR",
    "PARAMS",
    "PARQUET",
    "PARTITION",
    "PIPELINE",
    "PIPELINES",
    "POLICIES",
    "POLICY",
    "PRIMARY",
    "PROCESSOR",
    "PROCESSORS",
    "RANGE",
    "REFRESH",
    "RENAME",
    "REPLACE",
    "RETURNS",
    "SCHEDULE",
    "SCRIPT",
    "SECOND",
    "SET",
    "SETTING",
    "SHOW",
    "STATUS",
    "TABLE",
    "TABLES",
    "TO",
    "TRUE",
    "TRUNCATE",
    "TYPE",
    "UPDATE",
    "USING",
    "VALUES",
    "VIEW",
    "VIEWS",
    "WATCHER",
    "WATCHERS",
    "WEBHOOK",
    "WHEN",
    "WITH",
    "WITHIN",
    "YEAR"
  )

  /** SQL type names accepted by `TypeParser` (parser/type/package.scala:91-164). */
  val typeWords: Set[String] = Set(
    "ARRAY",
    "BIGINT",
    "BINARY",
    "BOOLEAN",
    "BYTE",
    "CHAR",
    "DATE",
    "DATETIME",
    "DOUBLE",
    "FLOAT",
    "GEOPOINT",
    "GEO_POINT",
    "INT",
    "INTEGER",
    "KEYWORD",
    "LONG",
    "REAL",
    "SHORT",
    "SMALLINT",
    "STRING",
    "STRUCT",
    "TEXT",
    "TIME",
    "TIMESTAMP",
    "TINYINT",
    "VARBINARY",
    "VARCHAR"
  )

  /** Plural time-unit forms. `TimeUnit`'s regex accepts an optional trailing `s`
    * (`\\b(?i)$sql(s)?\\b`, time/package.scala:102), so `INTERVAL 2 DAYS` parses — the singular
    * forms arrive via tokens/statementWords, the plurals are enumerated here (they are not
    * derivable from any token's `words`).
    */
  val timeUnitPluralWords: Set[String] = Set(
    "YEARS",
    "MONTHS",
    "QUARTERS",
    "WEEKS",
    "DAYS",
    "HOURS",
    "MINUTES",
    "SECONDS"
  )

  /** All word-bearing tokens, for test/introspection use. */
  val tokens: List[TokenRegex] = clauseTokens ++ functionTokens ++ literalTokens

  /** Normalize a token's `words` into single uppercase words: the literal regex separator `\s+`
    * (e.g. `"LEFT\\s+OUTER"`, From.scala:45) and real whitespace (e.g. `"ORDER BY"`, `"NULLS
    * FIRST"`) both split; non-word entries (`"\\:\\:"`, `"\\|\\|"`, `"?"`) are filtered out.
    * Replace BEFORE uppercasing — uppercasing `"\\s+"` would corrupt the separator to `"\\S+"`.
    */
  private[sql] def wordsOf(token: TokenRegex): List[String] =
    token.words
      .map(_.replaceAll("\\\\s\\+", " "))
      .flatMap(_.split("\\s+").toList)
      .map(_.toUpperCase)
      .filter(_.matches("[A-Z][A-Z0-9_]*"))

  /** Every single-word keyword known to the parser (uppercase). */
  lazy val allWords: Set[String] =
    tokens.flatMap(wordsOf).toSet ++ statementWords ++ typeWords ++ literalWords ++
    timeUnitPluralWords

  /** Words REPL components colourise/complete: all parser keywords, minus 1-letter words (`E` would
    * colourise every `e` table alias).
    */
  lazy val highlightedWords: Set[String] = allWords.filter(_.length >= 2)

  /** Multi-word phrases (compound keywords) for completer use, e.g. "ORDER BY", "LEFT OUTER", "IS
    * NOT NULL", "UNION ALL", "NULLS FIRST", "PARTITION BY".
    */
  lazy val compoundPhrases: Set[String] =
    tokens
      .flatMap(_.words)
      .map(_.replaceAll("\\\\s\\+", " ").trim.replaceAll("\\s+", " ").toUpperCase)
      .filter(p => p.contains(" ") && p.split(" ").forall(_.matches("[A-Z][A-Z0-9_]*")))
      .toSet
}
