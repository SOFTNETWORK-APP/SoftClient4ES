package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.{Expr, TokenRegex}

package object aggregate {

  sealed trait AggregateFunction extends Function

  case object COUNT extends Expr("COUNT") with AggregateFunction

  case object MIN extends Expr("MIN") with AggregateFunction

  case object MAX extends Expr("MAX") with AggregateFunction

  case object AVG extends Expr("AVG") with AggregateFunction

  case object SUM extends Expr("SUM") with AggregateFunction

  case object Sum extends Expr("SUM") with AggregateFunction

}
