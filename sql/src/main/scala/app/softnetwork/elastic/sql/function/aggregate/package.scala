package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.Expr

package object aggregate {

  sealed trait AggregateFunction extends Function

  case object Count extends Expr("COUNT") with AggregateFunction

  case object Min extends Expr("MIN") with AggregateFunction

  case object Max extends Expr("MAX") with AggregateFunction

  case object Avg extends Expr("AVG") with AggregateFunction

  case object Sum extends Expr("SUM") with AggregateFunction

}
