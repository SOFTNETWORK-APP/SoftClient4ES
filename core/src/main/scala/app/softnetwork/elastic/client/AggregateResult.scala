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

package app.softnetwork.elastic.client

import app.softnetwork.elastic.sql.function.aggregate.AggregateFunction

import java.time.temporal.Temporal
import scala.util.{Failure, Success, Try}

sealed trait AggregateResult {
  def field: String
  def error: Option[String]
}

sealed trait MetricAgregateResult extends AggregateResult {
  def function: AggregateFunction
}

sealed trait AggregateValue
case class BooleanValue(value: Boolean) extends AggregateValue
case class NumericValue(value: Number) extends AggregateValue
case class StringValue(value: String) extends AggregateValue
case class TemporalValue(value: Temporal) extends AggregateValue
case class ObjectValue(value: Map[String, Any]) extends AggregateValue

sealed trait ArrayAggregateValue[T] extends AggregateValue {
  def value: Seq[T]
}
case class ArrayOfBooleanValue(value: Seq[Boolean]) extends ArrayAggregateValue[Boolean]
case class ArrayOfNumericValue(value: Seq[Number]) extends ArrayAggregateValue[Number]
case class ArrayOfStringValue(value: Seq[String]) extends ArrayAggregateValue[String]
case class ArrayOfTemporalValue(value: Seq[Temporal]) extends ArrayAggregateValue[Temporal]
case class ArrayOfObjectValue(value: Seq[Map[String, Any]])
    extends ArrayAggregateValue[Map[String, Any]]

case object EmptyValue extends AggregateValue

case class SingleValueAggregateResult(
  field: String,
  function: AggregateFunction,
  value: AggregateValue,
  error: Option[String] = None
) extends MetricAgregateResult {

  def isEmpty: Boolean = value == EmptyValue

  def hasError: Boolean = error.isDefined

  def getOrElse[T](default: T)(extractor: AggregateValue => Option[T]): T =
    extractor(value).getOrElse(default)

  def fold[T](
    onBoolean: Boolean => T,
    onNumeric: Number => T,
    onString: String => T,
    onTemporal: Temporal => T,
    onObject: Map[String, Any] => T,
    onMulti: Seq[Any] => T,
    onEmpty: => T
  ): T = value match {
    case BooleanValue(v)           => onBoolean(v)
    case NumericValue(v)           => onNumeric(v)
    case StringValue(v)            => onString(v)
    case TemporalValue(v)          => onTemporal(v)
    case ObjectValue(v)            => onObject(v)
    case m: ArrayAggregateValue[_] => onMulti(m.value)
    case EmptyValue                => onEmpty
  }

  def asBooleanSafe: Try[Boolean] = value match {
    case BooleanValue(v) => Success(v)
    case _               => Failure(new ClassCastException(s"Cannot cast $value to Boolean"))
  }

  def asNumericSafe: Try[Number] = value match {
    case NumericValue(v) => Success(v)
    case _               => Failure(new ClassCastException(s"Cannot cast $value to Double"))
  }

  def asDoubleSafe: Try[Double] = asNumericSafe.map(_.doubleValue())

  def asIntSafe: Try[Int] = asNumericSafe.map(_.intValue())

  def asLongSafe: Try[Long] = asNumericSafe.map(_.longValue())

  def asByteSafe: Try[Byte] = asNumericSafe.map(_.byteValue())

  def asShortSafe: Try[Short] = asNumericSafe.map(_.shortValue())

  def asStringSafe: Try[String] = value match {
    case StringValue(v) => Success(v)
    case _              => Failure(new ClassCastException(s"Cannot cast $value to String"))
  }

  def asTemporalSafe: Try[Temporal] = value match {
    case TemporalValue(v) => Success(v)
    case _                => Failure(new ClassCastException(s"Cannot cast $value to Temporal"))
  }

  def asMapSafe: Try[Map[String, Any]] = value match {
    case ObjectValue(v) => Success(v)
    case _              => Failure(new ClassCastException(s"Cannot cast $value to Map"))
  }

  def asSeqSafe: Try[Seq[Any]] = value match {
    case ArrayOfBooleanValue(v)  => Success(v)
    case ArrayOfNumericValue(v)  => Success(v)
    case ArrayOfStringValue(v)   => Success(v)
    case ArrayOfTemporalValue(v) => Success(v)
    case ArrayOfObjectValue(v)   => Success(v)
    case _                       => Failure(new ClassCastException(s"Cannot cast $value to Seq"))
  }

  // Pretty print for debugging
  def prettyPrint: String = {
    val errorMsg = error.map(e => s" [ERROR: $e]").getOrElse("")
    s"$function($field) = ${formatValue(value)}$errorMsg"
  }

  private def formatValue(v: AggregateValue): String = v match {
    case BooleanValue(b)         => b.toString
    case NumericValue(n)         => n.toString
    case StringValue(s)          => s""""$s""""
    case TemporalValue(t)        => t.toString
    case ObjectValue(m)          => m.toString
    case ArrayOfBooleanValue(s)  => s.mkString("[", ", ", "]")
    case ArrayOfNumericValue(s)  => s.mkString("[", ", ", "]")
    case ArrayOfStringValue(s)   => s.map(str => s""""$str"""").mkString("[", ", ", "]")
    case ArrayOfTemporalValue(s) => s.mkString("[", ", ", "]")
    case ArrayOfObjectValue(s)   => s.mkString("[", ", ", "]")
    case EmptyValue              => "null"
  }
}
