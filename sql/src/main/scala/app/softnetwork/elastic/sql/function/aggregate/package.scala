package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.Expr

package object aggregate {

  sealed trait AggregateFunction extends Function

  case object Count extends Expr("count") with AggregateFunction

  case object Min extends Expr("min") with AggregateFunction

  case object Max extends Expr("max") with AggregateFunction

  case object Avg extends Expr("avg") with AggregateFunction

  case object Sum extends Expr("sum") with AggregateFunction

}
