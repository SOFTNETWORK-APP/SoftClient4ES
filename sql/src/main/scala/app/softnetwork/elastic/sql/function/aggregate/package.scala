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

package app.softnetwork.elastic.sql.function

import app.softnetwork.elastic.sql.query.{Bucket, Field, Limit, OrderBy, SQLSearchRequest}
import app.softnetwork.elastic.sql.{asString, Expr, Identifier, TokenRegex, Updateable}

package object aggregate {

  sealed trait AggregateFunction extends Function {
    def multivalued: Boolean = false
  }

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

  case object ARRAY_AGG extends Expr("ARRAY_AGG") with TopHits {
    override val words: List[String] = List(sql, "ARRAY")
  }

  case object OVER extends Expr("OVER") with TokenRegex

  case object PARTITION_BY extends Expr("PARTITION BY") with TokenRegex

  sealed trait TopHitsAggregation
      extends AggregateFunction
      with FunctionWithIdentifier
      with Updateable {
    def partitionBy: Seq[Identifier]
    def withPartitionBy(partitionBy: Seq[Identifier]): TopHitsAggregation
    def orderBy: OrderBy
    def topHits: TopHits
    def limit: Option[Limit]

    lazy val buckets: Seq[Bucket] = partitionBy.map(identifier => Bucket(identifier, None))

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

    def withFields(fields: Seq[Field]): TopHitsAggregation

    def update(request: SQLSearchRequest): TopHitsAggregation = {
      val updated = this
        .withPartitionBy(partitionBy = partitionBy.map(_.update(request)))
      updated.withFields(
        fields = request.select.fields
          .filterNot(field =>
            field.aggregation || request.bucketNames.keys.toSeq
              .contains(field.identifier.identifierName)
          )
          .filterNot(f => request.excludes.contains(f.sourceField))
      )
    }
  }

  case class FirstValue(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    orderBy: OrderBy,
    fields: Seq[Field] = Seq.empty
  ) extends TopHitsAggregation {
    override def limit: Option[Limit] = Some(Limit(1, None))
    override def topHits: TopHits = FIRST_VALUE
    override def withPartitionBy(partitionBy: Seq[Identifier]): TopHitsAggregation =
      this.copy(partitionBy = partitionBy)
    override def withFields(fields: Seq[Field]): TopHitsAggregation = this.copy(fields = fields)
    override def update(request: SQLSearchRequest): TopHitsAggregation = super
      .update(request)
      .asInstanceOf[FirstValue]
      .copy(
        identifier = identifier.update(request),
        orderBy = orderBy.update(request)
      )
  }

  case class LastValue(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    orderBy: OrderBy,
    fields: Seq[Field] = Seq.empty
  ) extends TopHitsAggregation {
    override def limit: Option[Limit] = Some(Limit(1, None))
    override def topHits: TopHits = LAST_VALUE
    override def withPartitionBy(partitionBy: Seq[Identifier]): TopHitsAggregation =
      this.copy(partitionBy = partitionBy)
    override def withFields(fields: Seq[Field]): TopHitsAggregation = this.copy(fields = fields)
    override def update(request: SQLSearchRequest): TopHitsAggregation = super
      .update(request)
      .asInstanceOf[LastValue]
      .copy(
        identifier = identifier.update(request),
        orderBy = orderBy.update(request)
      )
  }

  case class ArrayAgg(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    orderBy: OrderBy,
    fields: Seq[Field] = Seq.empty,
    limit: Option[Limit] = None
  ) extends TopHitsAggregation {
    override def topHits: TopHits = ARRAY_AGG
    override def withPartitionBy(partitionBy: Seq[Identifier]): TopHitsAggregation =
      this.copy(partitionBy = partitionBy)
    override def withFields(fields: Seq[Field]): TopHitsAggregation = this
    override def update(request: SQLSearchRequest): TopHitsAggregation = super
      .update(request)
      .asInstanceOf[ArrayAgg]
      .copy(
        identifier = identifier.update(request),
        orderBy = orderBy.update(request),
        limit = limit.orElse(request.limit)
      )
    override def multivalued: Boolean = true
  }

}
