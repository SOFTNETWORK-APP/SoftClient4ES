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

import app.softnetwork.elastic.sql.`type`.{SQLType, SQLTypes}
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

  // STDDEV / VARIANCE family — all translate to the same ES `extended_stats`
  // aggregation; the SQL token is preserved for round-trip and used to pick
  // the response key (`std_deviation` vs `std_deviation_population`, etc.).
  case object STDDEV extends Expr("STDDEV") with AggregateFunction with Window
  case object STDDEV_POP extends Expr("STDDEV_POP") with AggregateFunction with Window
  case object STDDEV_SAMP extends Expr("STDDEV_SAMP") with AggregateFunction with Window
  case object VARIANCE extends Expr("VARIANCE") with AggregateFunction with Window
  case object VAR_POP extends Expr("VAR_POP") with AggregateFunction with Window
  case object VAR_SAMP extends Expr("VAR_SAMP") with AggregateFunction with Window

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

  case object ROW_NUMBER extends Expr("ROW_NUMBER") with Window
  case object RANK extends Expr("RANK") with Window
  case object DENSE_RANK extends Expr("DENSE_RANK") with Window

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
      val identifiers = FunctionUtils.funIdentifiers(identifier)
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

    /** Window subclasses that should emit `LIMIT N` inside their OVER clause when round-tripping to
      * SQL. Defaults to false so existing windows (FIRST_VALUE / LAST_VALUE / ARRAY_AGG /
      * aggregate-style) keep the bare round-trip; ranking windows override to true so the push-down
      * syntax is preserved.
      */
    protected def emitsLimitInOver: Boolean = false

    override def sql: String = {
      (partitionBy, orderBy) match {
        case (Nil, None) => s"$window($identifier)"
        case _           =>
          // OrderBy.sql carries a leading space — strip it when there is
          // no PARTITION BY ahead so the OVER clause does not start with
          // `OVER ( ORDER BY ...)`.
          val orderByStr =
            orderBy
              .map(_.sql)
              .map(s => if (partitionBy.isEmpty) s.stripPrefix(" ") else s)
              .getOrElse("")
          val partitionByStr =
            if (partitionBy.nonEmpty) s"$PARTITION_BY ${partitionBy.mkString(", ")}"
            else ""
          val limitStr =
            if (emitsLimitInOver) limit.map(_.sql).getOrElse("") else ""
          s"$window($identifier) $OVER ($partitionByStr$orderByStr$limitStr)"
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
    override def baseType: SQLType = SQLTypes.BigInt

    def isCardinality: Boolean = identifier.distinct

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

  /** STDDEV / VARIANCE family. All six SQL functions translate to a single ES `extended_stats`
    * aggregation; the bridge emits one aggregation per call and the `kind` here drives result-field
    * projection during response extraction (`std_deviation`, `variance`, etc.).
    *
    * `STDDEV` ≡ `STDDEV_SAMP` and `VARIANCE` ≡ `VAR_SAMP` (ANSI defaults to sample — matches
    * PostgreSQL and Snowflake). The two pairs carry distinct kinds so the SQL form round-trips
    * faithfully through `WindowFunction.sql`.
    */
  sealed trait ExtendedStatsKind extends Product with Serializable {
    def window: Window
    def resultField: String
  }

  object ExtendedStatsKind {
    // ES `extended_stats` quirk: the un-suffixed `std_deviation` / `variance`
    // fields are the POPULATION values (kept for backwards compatibility
    // with the pre-7.7 response shape). The explicit sample values live
    // under the `_sampling` keys, which were introduced in ES 7.7 — so SQL
    // SAMP variants (default for ANSI STDDEV / VARIANCE) require ES 7.7+.
    // POP variants work on ES 6+ via the un-suffixed keys.
    case object Stddev extends ExtendedStatsKind {
      val window: Window = STDDEV
      val resultField: String = "std_deviation_sampling"
    }
    case object StddevSamp extends ExtendedStatsKind {
      val window: Window = STDDEV_SAMP
      val resultField: String = "std_deviation_sampling"
    }
    case object StddevPop extends ExtendedStatsKind {
      val window: Window = STDDEV_POP
      val resultField: String = "std_deviation"
    }
    case object Variance extends ExtendedStatsKind {
      val window: Window = VARIANCE
      val resultField: String = "variance_sampling"
    }
    case object VarSamp extends ExtendedStatsKind {
      val window: Window = VAR_SAMP
      val resultField: String = "variance_sampling"
    }
    case object VarPop extends ExtendedStatsKind {
      val window: Window = VAR_POP
      val resultField: String = "variance"
    }
  }

  case class ExtendedStatsAgg(
    identifier: Identifier,
    kind: ExtendedStatsKind,
    partitionBy: Seq[Identifier] = Seq.empty,
    fields: Seq[Field] = Seq.empty
  ) extends WindowFunction {
    override def baseType: SQLType = SQLTypes.Double

    override def limit: Option[Limit] = None

    override def orderBy: Option[OrderBy] = None

    override def window: Window = kind.window

    override def withPartitionBy(partitionBy: Seq[Identifier]): WindowFunction =
      this.copy(partitionBy = partitionBy)

    override def withFields(fields: Seq[Field]): WindowFunction = this.copy(fields = fields)

    override def update(request: SingleSearch): WindowFunction = super
      .update(request)
      .asInstanceOf[ExtendedStatsAgg]
      .copy(
        identifier = identifier.update(request)
      )
  }

  /** ROW_NUMBER / RANK / DENSE_RANK — ranking-style windows.
    *
    * ANSI requires `ORDER BY` inside the `OVER (...)` clause for ranking functions; the parser
    * enforces this so the AST always carries an `Option[OrderBy]` that is `Some(...)`. PARTITION BY
    * is optional — when absent the whole result set is one partition.
    *
    * Distinct from the other `WindowFunction` shapes because the result is one value per ROW within
    * partition, not one value per partition. The `searchWithWindowEnrichment` pipeline branches on
    * this trait via pattern matching: ranking windows produce a per-row ordinal injected by lookup
    * on `(partitionKey, _id)`.
    */
  sealed trait RankingWindow extends WindowFunction {
    override def isWindowing: Boolean = true
    // Ranking windows surface their `LIMIT N` clause in the SQL round-trip
    // so the top-N push-down syntax is preserved through Updateable.update.
    override protected def emitsLimitInOver: Boolean = true

    /** Apply this window's tie rule to an ordered `(rowId, sortKey)` sequence.
      *
      *   - ROW_NUMBER: sequential, no ties (1, 2, 3, 4, …)
      *   - RANK: ties share rank, next rank skips (1, 2, 2, 4, …)
      *   - DENSE_RANK: ties share rank, next rank does not skip (1, 2, 2, 3, …)
      *
      * Tie detection is value-equality on the full OVER ORDER BY tuple.
      */
    def assignOrdinals(ordered: Seq[(String, Seq[Any])]): Seq[(String, Long)]
  }

  case class RowNumber(
    partitionBy: Seq[Identifier] = Seq.empty,
    orderBy: Option[OrderBy],
    fields: Seq[Field] = Seq.empty,
    limit: Option[Limit] = None
  ) extends RankingWindow {
    override def identifier: Identifier = Identifier()
    override def window: Window = ROW_NUMBER
    override def baseType: SQLType = SQLTypes.BigInt

    override def assignOrdinals(ordered: Seq[(String, Seq[Any])]): Seq[(String, Long)] =
      ordered.zipWithIndex.map { case ((rowId, _), i) => rowId -> (i + 1L) }

    override def withPartitionBy(pb: Seq[Identifier]): WindowFunction =
      this.copy(partitionBy = pb)

    override def withFields(fs: Seq[Field]): WindowFunction = this.copy(fields = fs)

    override def update(request: SingleSearch): WindowFunction = super
      .update(request)
      .asInstanceOf[RowNumber]
      .copy(
        orderBy = orderBy.map(_.update(request))
        // NB: ranking windows intentionally do NOT fall back to the outer
        // query LIMIT — top-N push-down comes solely from the inline `LIMIT N`
        // inside OVER. Standard SQL computes window functions before LIMIT, so
        // the outer LIMIT must not shrink the per-partition ranked set.
      )
  }

  case class Ranking(
    partitionBy: Seq[Identifier] = Seq.empty,
    orderBy: Option[OrderBy],
    fields: Seq[Field] = Seq.empty,
    limit: Option[Limit] = None
  ) extends RankingWindow {
    override def identifier: Identifier = Identifier()
    override def window: Window = RANK
    override def baseType: SQLType = SQLTypes.BigInt

    override def assignOrdinals(ordered: Seq[(String, Seq[Any])]): Seq[(String, Long)] = {
      var lastKey: Seq[Any] = null
      var lastRank = 0L
      ordered.zipWithIndex.map { case ((rowId, key), i) =>
        if (key != lastKey) { lastRank = (i + 1).toLong; lastKey = key }
        rowId -> lastRank
      }
    }

    override def withPartitionBy(pb: Seq[Identifier]): WindowFunction =
      this.copy(partitionBy = pb)

    override def withFields(fs: Seq[Field]): WindowFunction = this.copy(fields = fs)

    override def update(request: SingleSearch): WindowFunction = super
      .update(request)
      .asInstanceOf[Ranking]
      .copy(
        orderBy = orderBy.map(_.update(request))
        // NB: ranking windows intentionally do NOT fall back to the outer
        // query LIMIT — top-N push-down comes solely from the inline `LIMIT N`
        // inside OVER. Standard SQL computes window functions before LIMIT, so
        // the outer LIMIT must not shrink the per-partition ranked set.
      )
  }

  case class DenseRank(
    partitionBy: Seq[Identifier] = Seq.empty,
    orderBy: Option[OrderBy],
    fields: Seq[Field] = Seq.empty,
    limit: Option[Limit] = None
  ) extends RankingWindow {
    override def identifier: Identifier = Identifier()
    override def window: Window = DENSE_RANK
    override def baseType: SQLType = SQLTypes.BigInt

    override def assignOrdinals(ordered: Seq[(String, Seq[Any])]): Seq[(String, Long)] = {
      var lastKey: Seq[Any] = null
      var dense = 0L
      ordered.map { case (rowId, key) =>
        if (key != lastKey) { dense += 1; lastKey = key }
        rowId -> dense
      }
    }

    override def withPartitionBy(pb: Seq[Identifier]): WindowFunction =
      this.copy(partitionBy = pb)

    override def withFields(fs: Seq[Field]): WindowFunction = this.copy(fields = fs)

    override def update(request: SingleSearch): WindowFunction = super
      .update(request)
      .asInstanceOf[DenseRank]
      .copy(
        orderBy = orderBy.map(_.update(request))
        // NB: ranking windows intentionally do NOT fall back to the outer
        // query LIMIT — top-N push-down comes solely from the inline `LIMIT N`
        // inside OVER. Standard SQL computes window functions before LIMIT, so
        // the outer LIMIT must not shrink the per-partition ranked set.
      )
  }
}
