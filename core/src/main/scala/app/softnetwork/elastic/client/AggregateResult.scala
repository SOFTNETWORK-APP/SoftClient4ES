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

sealed trait AggregateResult {
  def field: String
  def error: Option[String]
}

sealed trait MetricAgregateResult extends AggregateResult {
  def function: AggregateFunction
}

sealed trait AggregateValue
case class NumericValue(value: Double) extends AggregateValue
case class StringValue(value: String) extends AggregateValue
case class TemporalValue(value: Temporal) extends AggregateValue
case class ObjectValue(value: Map[String, Any]) extends AggregateValue
case class MultiBooleanValue(value: Seq[Boolean]) extends AggregateValue
case class MultiNumericValue(value: Seq[Number]) extends AggregateValue
case class MultiStringValue(value: Seq[String]) extends AggregateValue
case class MultiTemporalValue(value: Seq[Temporal]) extends AggregateValue
case class MultiObjectValue(value: Seq[Map[String, Any]]) extends AggregateValue
case object EmptyValue extends AggregateValue

case class SingleValueAggregateResult(
  field: String,
  function: AggregateFunction,
  value: AggregateValue,
  error: Option[String] = None
) extends MetricAgregateResult {
  def asDoubleOption: Option[Double] = value match {
    case NumericValue(v) => Some(v)
    case _               => None
  }
  def asStringOption: Option[String] = value match {
    case StringValue(v) => Some(v)
    case _              => None
  }
  def asMapOption: Option[Map[String, Any]] = value match {
    case ObjectValue(v) => Some(v)
    case _              => None
  }
  def asSeqOption: Option[Seq[Any]] = value match {
    case MultiBooleanValue(v)  => Some(v)
    case MultiNumericValue(v)  => Some(v)
    case MultiStringValue(v)   => Some(v)
    case MultiTemporalValue(v) => Some(v)
    case MultiObjectValue(v)   => Some(v)
    case _                     => None
  }

  def isDouble: Boolean = value match {
    case NumericValue(_) => true
    case _               => false
  }

  def isString: Boolean = value match {
    case StringValue(_) => true
    case _              => false
  }

  def isMap: Boolean = value match {
    case ObjectValue(_) => true
    case _              => false
  }
}
