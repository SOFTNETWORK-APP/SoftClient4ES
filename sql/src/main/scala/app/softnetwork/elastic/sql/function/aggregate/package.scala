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

import app.softnetwork.elastic.sql.query.{Bucket, BucketPath, Field, Limit, OrderBy, SingleSearch}
import app.softnetwork.elastic.sql.{Expr, Identifier, TokenRegex, Updateable}

package object aggregate {

  sealed trait AggregateFunction extends Function {
    def multivalued: Boolean = false

    override def isAggregation: Boolean = true

    override def hasAggregation: Boolean = true

    def isBucketScript: Boolean = false

    /** Indicates whether this aggregation is a windowing function with partitioning or not
      */
    def isWindowing: Boolean = false

    def bucketPath: String = ""

  }

  case object COUNT extends Expr("COUNT") with AggregateFunction with Window

  case object MIN extends Expr("MIN") with AggregateFunction with Window

  case object MAX extends Expr("MAX") with AggregateFunction with Window

  case object AVG extends Expr("AVG") with AggregateFunction with Window

  case object SUM extends Expr("SUM") with AggregateFunction with Window

  sealed trait Window extends TokenRegex

  case object FIRST_VALUE extends Expr("FIRST_VALUE") with Window {
    override val words: List[String] = List(sql, "FIRST")
  }

  case object LAST_VALUE extends Expr("LAST_VALUE") with Window {
    override val words: List[String] = List(sql, "LAST")
  }

  case object ARRAY_AGG extends Expr("ARRAY_AGG") with Window {
    override val words: List[String] = List(sql, "ARRAY")
  }

  case object OVER extends Expr("OVER") with TokenRegex

  case object PARTITION_BY extends Expr("PARTITION BY") with TokenRegex

  case class BucketScriptAggregation(
    identifier: Identifier,
    params: Map[String, String] = Map.empty
  ) extends AggregateFunction
      with FunctionWithIdentifier
      with Updateable {
    override def sql: String = identifier.sql

    override def hasAggregation: Boolean = true

    override def shouldBeScripted: Boolean = true

    override def isBucketScript: Boolean = true

    lazy val aggregations: Seq[AggregateFunction] = FunctionUtils.aggregateFunctions(identifier)

    // Get the longest bucket path among the aggregations involved in the bucket script
    // TODO we should check that all bucket paths among the aggregations belong to the same buckets tree
    override lazy val bucketPath: String =
      aggregations.map(_.bucketPath).distinct.sortBy(_.length).reverse.headOption.getOrElse("")

    override def update(request: SingleSearch): BucketScriptAggregation = {
      val identifiers = FunctionUtils.aggregateIdentifiers(identifier)
      val params = identifiers.flatMap {
        case identifier: Identifier =>
          val name = identifier.metricName.getOrElse(identifier.aliasOrName)
          Some(
            name -> request.fieldAliases.getOrElse(identifier.identifierName, name)
          ) // TODO may be be a path
        case _ => None
      }.toMap
      this.copy(params = params)
    }

    override def toString: String = "bucket_script"
  }

  sealed trait WindowFunction
      extends AggregateFunction
      with FunctionWithIdentifier
      with Updateable {
    def partitionBy: Seq[Identifier]
    def withPartitionBy(partitionBy: Seq[Identifier]): WindowFunction
    def orderBy: Option[OrderBy]
    def window: Window
    def limit: Option[Limit]

    override def isWindowing: Boolean = buckets.nonEmpty || orderBy.isDefined

    lazy val buckets: Seq[Bucket] = partitionBy.map(identifier => Bucket(identifier, None))

    override lazy val bucketPath: String = BucketPath(buckets).path

    lazy val bucketNames: Map[String, Bucket] = buckets.map { b =>
      b.identifier.identifierName -> b
    }.toMap

    override def sql: String = {
      (partitionBy, orderBy) match {
        case (Nil, None) => s"$window($identifier)"
        case _ =>
          val orderByStr = orderBy.map(_.sql).getOrElse("")
          val partitionByStr =
            if (partitionBy.nonEmpty) s"$PARTITION_BY ${partitionBy.mkString(", ")}"
            else ""
          s"$window($identifier) $OVER ($partitionByStr$orderByStr)"
      }
    }

    override def toSQL(base: String): String = sql

    def fields: Seq[Field]

    def withFields(fields: Seq[Field]): WindowFunction

    def update(request: SingleSearch): WindowFunction = {
      val updated = this
        .withPartitionBy(partitionBy = partitionBy.map(_.update(request)))
      updated.withFields(
        fields = request.select.fields
          .filterNot(field =>
            field.isAggregation || request.bucketNames.keys.toSeq
              .contains(field.identifier.identifierName)
          )
          .filterNot(field =>
            updated.bucketNames.keys.toSeq
              .contains(field.identifier.identifierName)
          )
          .filterNot(f => request.excludes.contains(f.sourceField))
      )
    }
  }

  case class FirstValue(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    orderBy: Option[OrderBy],
    fields: Seq[Field] = Seq.empty
  ) extends WindowFunction {
    override def limit: Option[Limit] = Some(Limit(1, None))
    override def window: Window = FIRST_VALUE
    override def withPartitionBy(partitionBy: Seq[Identifier]): WindowFunction =
      this.copy(partitionBy = partitionBy)
    override def withFields(fields: Seq[Field]): WindowFunction = this.copy(fields = fields)
    override def update(request: SingleSearch): WindowFunction = super
      .update(request)
      .asInstanceOf[FirstValue]
      .copy(
        identifier = identifier.update(request),
        orderBy = orderBy.map(_.update(request))
      )
  }

  case class LastValue(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    orderBy: Option[OrderBy],
    fields: Seq[Field] = Seq.empty
  ) extends WindowFunction {
    override def limit: Option[Limit] = Some(Limit(1, None))
    override def window: Window = LAST_VALUE
    override def withPartitionBy(partitionBy: Seq[Identifier]): WindowFunction =
      this.copy(partitionBy = partitionBy)
    override def withFields(fields: Seq[Field]): WindowFunction = this.copy(fields = fields)
    override def update(request: SingleSearch): WindowFunction = super
      .update(request)
      .asInstanceOf[LastValue]
      .copy(
        identifier = identifier.update(request),
        orderBy = orderBy.map(_.update(request))
      )
  }

  case class ArrayAgg(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    orderBy: Option[OrderBy],
    fields: Seq[Field] = Seq.empty,
    limit: Option[Limit] = None
  ) extends WindowFunction {
    override def window: Window = ARRAY_AGG
    override def withPartitionBy(partitionBy: Seq[Identifier]): WindowFunction =
      this.copy(partitionBy = partitionBy)
    override def withFields(fields: Seq[Field]): WindowFunction = this
    override def update(request: SingleSearch): WindowFunction = super
      .update(request)
      .asInstanceOf[ArrayAgg]
      .copy(
        identifier = identifier.update(request),
        orderBy = orderBy.map(_.update(request)),
        limit = limit.orElse(request.limit)
      )
    override def multivalued: Boolean = true
  }

  case class CountAgg(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    fields: Seq[Field] = Seq.empty
  ) extends WindowFunction {
    override def limit: Option[Limit] = None

    override def orderBy: Option[OrderBy] = None

    override def window: Window = COUNT

    override def withPartitionBy(partitionBy: Seq[Identifier]): WindowFunction =
      this.copy(partitionBy = partitionBy)

    override def withFields(fields: Seq[Field]): WindowFunction = this.copy(fields = fields)

    override def update(request: SingleSearch): WindowFunction = super
      .update(request)
      .asInstanceOf[CountAgg]
      .copy(
        identifier = identifier.update(request)
      )
  }

  case class MinAgg(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    fields: Seq[Field] = Seq.empty
  ) extends WindowFunction {
    override def limit: Option[Limit] = None

    override def orderBy: Option[OrderBy] = None

    override def window: Window = MIN

    override def withPartitionBy(partitionBy: Seq[Identifier]): WindowFunction =
      this.copy(partitionBy = partitionBy)

    override def withFields(fields: Seq[Field]): WindowFunction = this.copy(fields = fields)

    override def update(request: SingleSearch): WindowFunction = super
      .update(request)
      .asInstanceOf[MinAgg]
      .copy(
        identifier = identifier.update(request)
      )
  }

  case class MaxAgg(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    fields: Seq[Field] = Seq.empty
  ) extends WindowFunction {
    override def limit: Option[Limit] = None

    override def orderBy: Option[OrderBy] = None

    override def window: Window = MAX

    override def withPartitionBy(partitionBy: Seq[Identifier]): WindowFunction =
      this.copy(partitionBy = partitionBy)

    override def withFields(fields: Seq[Field]): WindowFunction = this.copy(fields = fields)

    override def update(request: SingleSearch): WindowFunction = super
      .update(request)
      .asInstanceOf[MaxAgg]
      .copy(
        identifier = identifier.update(request)
      )
  }

  case class AvgAgg(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    fields: Seq[Field] = Seq.empty
  ) extends WindowFunction {
    override def limit: Option[Limit] = None

    override def orderBy: Option[OrderBy] = None

    override def window: Window = AVG

    override def withPartitionBy(partitionBy: Seq[Identifier]): WindowFunction =
      this.copy(partitionBy = partitionBy)

    override def withFields(fields: Seq[Field]): WindowFunction = this.copy(fields = fields)

    override def update(request: SingleSearch): WindowFunction = super
      .update(request)
      .asInstanceOf[AvgAgg]
      .copy(
        identifier = identifier.update(request)
      )
  }

  case class SumAgg(
    identifier: Identifier,
    partitionBy: Seq[Identifier] = Seq.empty,
    fields: Seq[Field] = Seq.empty
  ) extends WindowFunction {
    override def limit: Option[Limit] = None

    override def orderBy: Option[OrderBy] = None

    override def window: Window = SUM

    override def withPartitionBy(partitionBy: Seq[Identifier]): WindowFunction =
      this.copy(partitionBy = partitionBy)

    override def withFields(fields: Seq[Field]): WindowFunction = this.copy(fields = fields)

    override def update(request: SingleSearch): WindowFunction = super
      .update(request)
      .asInstanceOf[SumAgg]
      .copy(
        identifier = identifier.update(request)
      )
  }
}
