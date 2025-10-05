package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.function.aggregate.TopHitsAggregation
import app.softnetwork.elastic.sql.{asString, Token}

case class SQLSearchRequest(
  select: Select = Select(),
  from: From,
  where: Option[Where],
  groupBy: Option[GroupBy] = None,
  having: Option[Having] = None,
  orderBy: Option[OrderBy] = None,
  limit: Option[Limit] = None,
  score: Option[Double] = None
) extends Token {
  override def sql: String =
    s"$select$from${asString(where)}${asString(groupBy)}${asString(having)}${asString(orderBy)}${asString(limit)}"

  lazy val fieldAliases: Map[String, String] = select.fieldAliases
  lazy val tableAliases: Map[String, String] = from.tableAliases
  lazy val unnestAliases: Map[String, (String, Option[Limit])] = from.unnestAliases
  lazy val bucketNames: Map[String, Bucket] = buckets.map { b =>
    b.identifier.identifierName -> b
  }.toMap
  lazy val unnests: Map[String, Unnest] =
    from.unnests.map(u => u.alias.map(_.alias).getOrElse(u.name) -> u).toMap
  lazy val nestedFields: Map[String, Seq[Field]] =
    select.fields
      .filterNot(_.aggregation)
      .filter(_.nested)
      .groupBy(_.identifier.innerHitsName.getOrElse(""))
  lazy val nested: Seq[NestedElement] = {
    // nested roots (i.e., unnests without a parent)
    val roots = from.unnests.filter(_.parent.isEmpty)
    roots.map(toNestedElement)
  }

  def toNestedElement(u: Unnest): NestedElement = {
    NestedElement(
      path = u.path,
      innerHitsName = u.innerHitsName,
      size = limit.map(_.limit),
      children = Nil,
      sources = nestedFields
        .get(u.innerHitsName)
        .map(_.map(_.identifier.name.split('.').tail.mkString(".")))
        .getOrElse(Nil),
      parent = u.parent.map(toNestedElement)
    )
  }

  lazy val sorts: Map[String, SortOrder] =
    orderBy.map { _.sorts.map(s => s.name -> s.direction) }.getOrElse(Map.empty).toMap

  def update(): SQLSearchRequest = {
    (for {
      from    <- Option(this.copy(from = from.update(this)))
      select  <- Option(from.copy(select = select.update(from)))
      where   <- Option(select.copy(where = where.map(_.update(select))))
      groupBy <- Option(where.copy(groupBy = groupBy.map(_.update(where))))
      having  <- Option(groupBy.copy(having = having.map(_.update(groupBy))))
      updated <- Option(having.copy(orderBy = orderBy.map(_.update(having))))
    } yield updated).get
  }

  lazy val scriptFields: Seq[Field] = select.fields.filter(_.isScriptField)

  lazy val fields: Seq[String] = {
    if (aggregates.isEmpty && buckets.isEmpty)
      select.fields
        .filterNot(_.isScriptField)
        .filterNot(_.nested)
        .map(_.sourceField)
        .filterNot(f => excludes.contains(f))
    else
      Seq.empty
  }

  lazy val topHitsFields: Seq[Field] = select.fields.filter(_.topHits.nonEmpty)

  lazy val topHitsAggs: Seq[TopHitsAggregation] = topHitsFields.flatMap(_.topHits)

  lazy val aggregates: Seq[Field] =
    select.fields.filter(_.aggregation).filterNot(_.topHits.isDefined) ++ topHitsFields

  lazy val excludes: Seq[String] = select.except.map(_.fields.map(_.sourceField)).getOrElse(Nil)

  lazy val sources: Seq[String] = from.tables.map(_.name)

  lazy val topHitsBuckets: Seq[Bucket] = topHitsAggs
    .flatMap(_.bucketNames)
    .filterNot(bucket =>
      groupBy.map(_.bucketNames).getOrElse(Map.empty).keys.toSeq.contains(bucket._1)
    )
    .toMap
    .values
    .toSeq

  lazy val buckets: Seq[Bucket] = groupBy.map(_.buckets).getOrElse(Seq.empty) ++ topHitsBuckets

  override def validate(): Either[String, Unit] = {
    for {
      _ <- from.validate()
      _ <- select.validate()
      _ <- where.map(_.validate()).getOrElse(Right(()))
      _ <- groupBy.map(_.validate()).getOrElse(Right(()))
      _ <- having.map(_.validate()).getOrElse(Right(()))
      _ <- orderBy.map(_.validate()).getOrElse(Right(()))
      _ <- limit.map(_.validate()).getOrElse(Right(()))
      /*_ <- {
        // validate that having clauses are only applied when group by is present
        if (having.isDefined && groupBy.isEmpty) {
          Left("HAVING clauses can only be applied when GROUP BY is present")
        } else {
          Right(())
        }
      }*/
      _ <- {
        // validate that non-aggregated fields are not present when group by is present
        if (groupBy.isDefined) {
          val nonAggregatedFields = select.fields.filterNot(f => f.aggregation || f.isScriptField)
          val invalidFields = nonAggregatedFields.filterNot(f =>
            buckets.exists(b =>
              b.name == f.fieldAlias.map(_.alias).getOrElse(f.sourceField.replace(".", "_"))
            )
          )
          if (invalidFields.nonEmpty) {
            Left(
              s"Non-aggregated fields ${invalidFields.map(_.sql).mkString(", ")} cannot be selected when GROUP BY is present"
            )
          } else {
            Right(())
          }
        } else {
          Right(())
        }
      }
    } yield ()
  }
}
