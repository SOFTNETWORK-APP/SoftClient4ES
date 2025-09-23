package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.query.OrderBy
import app.softnetwork.elastic.sql.{Expr, Identifier, TokenRegex}

package object aggregate {

  sealed trait AggregateFunction extends Function

  case object COUNT extends Expr("COUNT") with AggregateFunction

  case object MIN extends Expr("MIN") with AggregateFunction

  case object MAX extends Expr("MAX") with AggregateFunction

  case object AVG extends Expr("AVG") with AggregateFunction

  case object SUM extends Expr("SUM") with AggregateFunction

  sealed trait TopHits extends TokenRegex

  case object FIRST_VALUE extends Expr("FIRST_VALUE") with TopHits

  case object LAST_VALUE extends Expr("LAST_VALUE") with TopHits

  case object OVER extends Expr("OVER") with TokenRegex

  case object PARTITION_BY extends Expr("PARTITION BY") with TokenRegex

  sealed abstract class TopHitsAggregation(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    orderBy: OrderBy
  ) extends AggregateFunction
      with FunctionWithIdentifier {
    def topHits: TopHits
    override def sql: String = {
      val partitionByStr =
        if (partitionBy.nonEmpty) s"$PARTITION_BY ${partitionBy.mkString(", ")}"
        else ""
      s"$topHits($identifier) $OVER ($partitionByStr$orderBy)"
    }

    override def toSQL(base: String): String = sql
  }

  case class FirstValue(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    orderBy: OrderBy
  ) extends TopHitsAggregation(identifier, partitionBy, orderBy) {
    override def topHits: TopHits = FIRST_VALUE
  }

  case class LastValue(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    orderBy: OrderBy
  ) extends TopHitsAggregation(identifier, partitionBy, orderBy) {
    override def topHits: TopHits = LAST_VALUE
  }

}
