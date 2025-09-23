package app.softnetwork.elastic.sql.parser

import app.softnetwork.elastic.sql.{
  BooleanValue,
  DoubleValue,
  Identifier,
  LongValue,
  PiValue,
  StringValue,
  Value
}
import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}

package object `type` {

  trait TypeParser { self: Parser =>

    def literal: PackratParser[StringValue] =
      (("\"" ~> """([^"\\]|\\.)*""".r <~ "\"") | ("'" ~> """([^'\\]|\\.)*""".r <~ "'")) ^^ { str =>
        StringValue(str)
      }

    def long: PackratParser[LongValue] =
      """(-)?(0|[1-9]\d*)""".r ^^ (str => LongValue(str.toLong))

    def double: PackratParser[DoubleValue] =
      """(-)?(\d+\.\d+)""".r ^^ (str => DoubleValue(str.toDouble))

    def pi: PackratParser[Value[Double]] =
      PiValue.regex ^^ (_ => PiValue)

    def boolean: PackratParser[BooleanValue] =
      """(?i)(true|false)\\b""".r ^^ (bool => BooleanValue(bool.toBoolean))

    def value_identifier: PackratParser[Identifier] =
      (literal | long | double | pi | boolean) ^^ { v =>
        Identifier(v)
      }

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

    def sql_type: PackratParser[SQLType] =
      char_type | string_type | datetime_type | timestamp_type | date_type | time_type | boolean_type | long_type | double_type | float_type | int_type | short_type | byte_type

  }
}
