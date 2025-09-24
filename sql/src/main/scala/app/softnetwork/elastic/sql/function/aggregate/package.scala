package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.query.{Bucket, Field, OrderBy, SQLSearchRequest}
import app.softnetwork.elastic.sql.{Expr, Identifier, TokenRegex, Updateable}

package object aggregate {

  sealed trait AggregateFunction extends Function

  case object COUNT extends Expr("COUNT") with AggregateFunction

  case object MIN extends Expr("MIN") with AggregateFunction

  case object MAX extends Expr("MAX") with AggregateFunction

  case object AVG extends Expr("AVG") with AggregateFunction

  case object SUM extends Expr("SUM") with AggregateFunction

  sealed trait TopHits extends TokenRegex

  case object FIRST_VALUE extends Expr("FIRST_VALUE") with TopHits {
    override val words: List[String] = List(sql, "FIRST")
  }

  case object LAST_VALUE extends Expr("LAST_VALUE") with TopHits {
    override val words: List[String] = List(sql, "LAST")
  }

  case object OVER extends Expr("OVER") with TokenRegex

  case object PARTITION_BY extends Expr("PARTITION BY") with TokenRegex

  sealed trait TopHitsAggregation
      extends AggregateFunction
      with FunctionWithIdentifier
      with Updateable {
    def partitionBy: Seq[Identifier]
    def orderBy: OrderBy
    def topHits: TopHits

    lazy val buckets: Seq[Bucket] = partitionBy.map(Bucket)

    lazy val bucketNames: Map[String, Bucket] = buckets.map { b =>
      b.identifier.identifierName -> b
    }.toMap

    override def sql: String = {
      val partitionByStr =
        if (partitionBy.nonEmpty) s"$PARTITION_BY ${partitionBy.mkString(", ")}"
        else ""
      s"$topHits($identifier) $OVER ($partitionByStr$orderBy)"
    }

    override def toSQL(base: String): String = sql

    def fields: Seq[Field]

    def update(request: SQLSearchRequest): TopHitsAggregation
  }

  case class FirstValue(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    orderBy: OrderBy,
    fields: Seq[Field] = Seq.empty
  ) extends TopHitsAggregation {
    override def topHits: TopHits = FIRST_VALUE
    override def update(request: SQLSearchRequest): FirstValue = {
      val updated = this.copy(partitionBy = partitionBy.map(_.update(request)))
      updated.copy(
        fields = request.select.fields
          .filterNot(field =>
            field.aggregation || request.bucketNames.keys.toSeq
              .contains(field.identifier.identifierName)
          )
          .filterNot(f => request.excludes.contains(f.sourceField))
      )
    }
  }

  case class LastValue(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    orderBy: OrderBy,
    fields: Seq[Field] = Seq.empty
  ) extends TopHitsAggregation {
    override def topHits: TopHits = LAST_VALUE
    override def update(request: SQLSearchRequest): LastValue = {
      val updated = this.copy(partitionBy = partitionBy.map(_.update(request)))
      updated.copy(
        fields = request.select.fields
          .filterNot(field =>
            field.aggregation || request.bucketNames.keys.toSeq
              .contains(field.identifier.identifierName)
          )
          .filterNot(f => request.excludes.contains(f.sourceField))
      )
    }
  }

}
