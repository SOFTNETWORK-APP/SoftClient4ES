package app.softnetwork.elastic.client

import app.softnetwork.elastic.sql.AggregateFunction

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
}
