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

package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.function.aggregate.WindowFunction
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
  lazy val bucketNames: Map[String, Bucket] = buckets.flatMap { b =>
    val name = b.identifier.identifierName
    "\\d+".r.findFirstIn(name) match {
      case Some(n) =>
        val identifier = select.fields(n.toInt - 1).identifier
        val updated = b.copy(identifier = select.fields(n.toInt - 1).identifier)
        Map(
          n                         -> updated, // also map numeric bucket to field name
          identifier.identifierName -> updated
        )
      case _ => Map(name -> b)
    }
  }.toMap

  var unnests: scala.collection.mutable.Map[String, Unnest] = {
    val map = from.unnests.map(u => u.alias.map(_.alias).getOrElse(u.name) -> u).toMap
    scala.collection.mutable.Map(map.toSeq: _*)
  }

  lazy val nestedFields: Map[String, Seq[Field]] =
    select.fields
      .filterNot(_.aggregation)
      .filter(_.nested)
      .groupBy(_.identifier.innerHitsName.getOrElse(""))
  lazy val nested: Seq[NestedElement] =
    from.unnests.map(toNestedElement).groupBy(_.path).map(_._2.head).toList
  private[this] lazy val nestedFieldsWithoutCriteria: Map[String, Seq[Field]] = {
    val innerHitsWithCriteria = (where.map(_.nestedElements).getOrElse(Seq.empty) ++
      having.map(_.nestedElements).getOrElse(Seq.empty) ++
      groupBy.map(_.nestedElements).getOrElse(Seq.empty))
      .groupBy(_.path)
      .map(_._2.head)
      .toList
      .map(_.innerHitsName)
    val ret = nestedFields.filterNot { case (innerHitsName, _) =>
      innerHitsWithCriteria.contains(innerHitsName)
    }
    ret
  }
  // nested fields that are not part of where, having or group by clauses
  lazy val nestedElementsWithoutCriteria: Seq[NestedElement] =
    nested.filter(n => nestedFieldsWithoutCriteria.keys.toSeq.contains(n.innerHitsName))

  def toNestedElement(u: Unnest): NestedElement = {
    val updated = unnests.getOrElse(u.alias.map(_.alias).getOrElse(u.name), u)
    val parent = updated.parent.map(toNestedElement)
    NestedElement(
      path = updated.path,
      innerHitsName = updated.innerHitsName,
      size = limit.map(_.limit),
      children = Nil,
      sources = nestedFields
        .get(updated.innerHitsName)
        .map(_.map(_.identifier.name.split('.').tail.mkString(".")))
        .getOrElse(Nil),
      parent = parent
    )
  }

  lazy val sorts: Map[String, SortOrder] =
    orderBy.map { _.sorts.map(s => s.name -> s.direction) }.getOrElse(Map.empty).toMap

  def update(): SQLSearchRequest = {
    (for {
      from <- Option(this.copy(from = from.update(this)))
      select <- Option(
        from.copy(
          select = select.update(from),
          groupBy = groupBy.map(_.update(from)),
          having = having.map(_.update(from))
        )
      )
      where   <- Option(select.copy(where = where.map(_.update(select))))
      updated <- Option(where.copy(orderBy = orderBy.map(_.update(where))))
    } yield updated).getOrElse(
      throw new IllegalStateException("Failed to update SQLSearchRequest")
    )
  }

  lazy val scriptFields: Seq[Field] = select.fields.filter(_.isScriptField)

  lazy val fields: Seq[String] = {
    if (aggregates.isEmpty && buckets.isEmpty)
      select.fields
        .filterNot(_.isScriptField)
        .filterNot(_.nested)
        .map(_.sourceField)
        .filterNot(f => excludes.contains(f))
        .distinct
    else
      Seq.empty
  }

  lazy val windowFields: Seq[Field] = select.fields.filter(_.windows.nonEmpty)

  lazy val windowFunctions: Seq[WindowFunction] = windowFields.flatMap(_.windows)

  lazy val aggregates: Seq[Field] =
    select.fields.filter(_.aggregation).filterNot(_.windows.isDefined) ++ windowFields

  lazy val sqlAggregations: Map[String, SQLAggregation] =
    aggregates.flatMap(f => SQLAggregation.fromField(f, this)).map(a => a.aggName -> a).toMap

  lazy val excludes: Seq[String] = select.except.map(_.fields.map(_.sourceField)).getOrElse(Nil)

  lazy val sources: Seq[String] = from.tables.map(_.name)

  lazy val windowBuckets: Seq[Bucket] = windowFunctions
    .flatMap(_.bucketNames)
    .filterNot(bucket =>
      groupBy.map(_.bucketNames).getOrElse(Map.empty).keys.toSeq.contains(bucket._1)
    )
    .toMap
    .values
    .toSeq

  lazy val buckets: Seq[Bucket] = groupBy.map(_.buckets).getOrElse(Seq.empty) ++ windowBuckets

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
