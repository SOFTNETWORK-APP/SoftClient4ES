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
      */
    def literal: PackratParser[StringValue] =
      (""""([^"\\]|\\.)*"""".r ^^ { str =>
        StringValue(str.substring(1, str.length - 1).replace("\\\"", "\"").replace("\\\\", "\\"))
      }) | ("""'([^'\\]|\\.)*'""".r ^^ { str =>
        StringValue(str.substring(1, str.length - 1).replace("\\'", "'").replace("\\\\", "\\"))
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

    def char_type: PackratParser[SQLTypes.Char.type] =
      "(?i)char".r ^^ (_ => SQLTypes.Char)

    def string_type: PackratParser[SQLTypes.Varchar.type] =
      "(?i)varchar|string".r ^^ (_ => SQLTypes.Varchar)

    def date_type: PackratParser[SQLTypes.Date.type] = "(?i)date".r ^^ (_ => SQLTypes.Date)

    def time_type: PackratParser[SQLTypes.Time.type] = "(?i)time".r ^^ (_ => SQLTypes.Time)

    def datetime_type: PackratParser[SQLTypes.DateTime.type] =
      "(?i)(datetime)".r ^^ (_ => SQLTypes.DateTime)

    def timestamp_type: PackratParser[SQLTypes.Timestamp.type] =
      "(?i)(timestamp)".r ^^ (_ => SQLTypes.Timestamp)

    def boolean_type: PackratParser[SQLTypes.Boolean.type] =
      "(?i)boolean".r ^^ (_ => SQLTypes.Boolean)

    def byte_type: PackratParser[SQLTypes.TinyInt.type] =
      "(?i)(byte|tinyint)".r ^^ (_ => SQLTypes.TinyInt)

    def short_type: PackratParser[SQLTypes.SmallInt.type] =
      "(?i)(short|smallint)".r ^^ (_ => SQLTypes.SmallInt)

    def int_type: PackratParser[SQLTypes.Int.type] = "(?i)(integer|int)".r ^^ (_ => SQLTypes.Int)

    def long_type: PackratParser[SQLTypes.BigInt.type] =
      "(?i)long|bigint".r ^^ (_ => SQLTypes.BigInt)

    def double_type: PackratParser[SQLTypes.Double.type] = "(?i)double".r ^^ (_ => SQLTypes.Double)

    def float_type: PackratParser[SQLTypes.Real.type] = "(?i)float|real".r ^^ (_ => SQLTypes.Real)

    def struct_type: PackratParser[SQLTypes.Struct.type] =
      "(?i)struct".r ^^ (_ => SQLTypes.Struct)

    def array_type: PackratParser[SQLTypes.Array] =
      "(?i)array<".r ~> sql_type <~ ">" ^^ { elementType =>
        SQLTypes.Array(elementType)
      }

    def binary_type: PackratParser[SQLTypes.VarBinary.type] =
      "(?i)(binary|varbinary)".r ^^ (_ => SQLTypes.VarBinary)

    def sql_type: PackratParser[SQLType] =
      char_type |
      string_type |
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
      "(?i)text".r ^^ (_ => SQLTypes.Text)

    def keyword_type: PackratParser[SQLTypes.Keyword.type] =
      "(?i)keyword".r ^^ (_ => SQLTypes.Keyword)

    def geo_point_type: PackratParser[SQLTypes.GeoPoint.type] =
      "(?i)(geo_point|geopoint)".r ^^ (_ => SQLTypes.GeoPoint)

    def extension_type: PackratParser[SQLType] =
      sql_type | text_type | keyword_type | geo_point_type
  }
}
