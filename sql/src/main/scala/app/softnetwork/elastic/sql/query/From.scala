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

import app.softnetwork.elastic.sql.{
  asString,
  Alias,
  Expr,
  Identifier,
  Source,
  TokenRegex,
  Updateable
}

import scala.annotation.tailrec

case object From extends Expr("FROM") with TokenRegex

sealed trait JoinType extends TokenRegex

case object InnerJoin extends Expr("INNER") with JoinType

case object LeftJoin extends Expr("LEFT") with JoinType

case object RightJoin extends Expr("RIGHT") with JoinType

case object FullJoin extends Expr("FULL") with JoinType

case object CrossJoin extends Expr("CROSS") with JoinType

case object On extends Expr("ON") with TokenRegex

case class On(criteria: Criteria) extends Updateable {
  override def sql: String = s" $On $criteria"
  def update(request: SQLSearchRequest): On = this.copy(criteria = criteria.update(request))
}

case object Join extends Expr("JOIN") with TokenRegex

sealed trait Join extends Updateable {
  def source: Source
  def joinType: Option[JoinType]
  def on: Option[On]
  def alias: Option[Alias]
  override def sql: String =
    s" ${asString(joinType)} $Join $source${asString(on)}${asString(alias)}"

  override def update(request: SQLSearchRequest): Join

  override def validate(): Either[String, Unit] =
    for {
      _ <- source.validate()
      _ <- alias match {
        case Some(a) if a.alias.nonEmpty => Right(())
        case _                           => Left(s"JOIN $this requires an alias")
      }
      _ <- this match {
        case j if joinType.isDefined && on.isEmpty && joinType.get != CrossJoin =>
          Left(s"JOIN $j requires an ON clause")
        case j if joinType.isEmpty && on.isDefined =>
          Left(s"JOIN $j requires a JOIN type")
        case j if alias.isEmpty =>
          Left(s"JOIN $j requires an alias")
        case _ => Right(())
      }
    } yield ()
}

case object Unnest extends Expr("UNNEST") with TokenRegex

case class Unnest(
  identifier: Identifier,
  limit: Option[Limit],
  alias: Option[Alias] = None,
  parent: Option[Unnest] = None
) extends Source
    with Join {
  override def sql: String = s"$Join $Unnest($identifier)${asString(alias)}"
  def update(request: SQLSearchRequest): Unnest = {
    val updated = this.copy(
      identifier = identifier.withNested(true).update(request),
      limit = limit.orElse(request.limit)
    )
    updated.identifier.tableAlias match {
      case Some(alias) if updated.identifier.nested =>
        request.unnests.get(alias) match {
          case Some(parent) /*if parent.path != updated.path*/ =>
            val unnest = updated.copy(parent = Some(parent))
            request.unnests += unnest.alias.map(_.alias).getOrElse(unnest.name) -> unnest
            return unnest
          case _ =>
        }
      case _ =>
    }
    updated
  }

  override val name: String = {
    val parts = identifier.name.split('.')
    if (parts.length <= 1) identifier.name
    else parts.tail.mkString(".")
  }

  def innerHitsName: String = alias.map(_.alias).getOrElse(name)

  def path: String = parent match {
    case Some(p) => s"${p.path}.$name"
    case None    => name
  }

  override def source: Source = identifier

  override def joinType: Option[JoinType] = None

  override def on: Option[On] = None

  override def validate(): Either[String, Unit] =
    for {
      _ <- super.validate()
      _ <-
        if (identifier.name.contains('.')) Right(())
        else Left(s"UNNEST identifier $identifier must be a nested field")
    } yield ()

}

case class Table(name: String, tableAlias: Option[Alias] = None, joins: Seq[Join] = Nil)
    extends Source {
  override def sql: String = s"$name${asString(tableAlias)} ${joins.map(_.sql).mkString(" ")}".trim
  def update(request: SQLSearchRequest): Table = this.copy(joins = joins.map(_.update(request)))

  override def validate(): Either[String, Unit] =
    for {
      _ <- tableAlias match {
        case Some(a) if a.alias.isEmpty => Left(s"Table $name alias cannot be empty")
        case _                          => Right(())
      }
      _ <- joins.map(_.validate()).filter(_.isLeft) match {
        case Nil    => Right(())
        case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
      }
    } yield ()
}

case class From(tables: Seq[Table]) extends Updateable {
  override def sql: String = s" $From ${tables.map(_.sql).mkString(",")}"
  lazy val unnests: Seq[Unnest] = tables
    .map(_.joins)
    .collect { case j =>
      j.collect { case u: Unnest => u }
    }
    .flatten

  lazy val tableAliases: Map[String, String] = tables
    .flatMap((table: Table) =>
      table.tableAlias match {
        case Some(alias) if alias.alias.nonEmpty => Some(table.name -> alias.alias)
        case _                                   => Some(table.name -> table.name)
      }
    )
    .toMap ++ unnestAliases.map(unnest => unnest._2._1 -> unnest._1)

  lazy val unnestAliases: Map[String, (String, Option[Limit])] = unnests
    .map(u => // extract unnest info
      (u.alias.map(_.alias).getOrElse(u.name), (u.name, u.limit))
    )
    .toMap
  def update(request: SQLSearchRequest): From =
    this.copy(tables = tables.map(_.update(request)))

  override def validate(): Either[String, Unit] = {
    if (tables.isEmpty) {
      Left("At least one table is required in FROM clause")
    } else {
      for {
        _ <- tables.map(_.validate()).filter(_.isLeft) match {
          case Nil    => Right(())
          case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
        }
      } yield ()
    }
  }
}

case class NestedElement(
  path: String,
  innerHitsName: String,
  size: Option[Int],
  children: Seq[NestedElement] = Nil, // TODO remove and use parent instead
  sources: Seq[String] = Nil,
  parent: Option[NestedElement]
) {
  lazy val root: NestedElement = {
    parent match {
      case Some(p) => p.root
      case None    => this
    }
  }

  lazy val level: Int = {
    parent match {
      case Some(p) => 1 + p.level
      case None    => 0
    }
  }

  lazy val bucketPath: String = {
    parent match {
      case Some(p) => s"${p.bucketPath}>$innerHitsName"
      case None    => innerHitsName
    }
  }
}

object NestedElements {

  def buildNestedTrees(nestedElements: Seq[NestedElement]): Seq[NestedElement] = {
    if (nestedElements.isEmpty) return Nil
    val nestedParentsPath: collection.mutable.Map[String, (NestedElement, Seq[NestedElement])] =
      collection.mutable.Map.empty

    val distinctNestedElements = nestedElements.groupBy(_.path).map(_._2.head).toList

    val distinctNestedElementsByRoot =
      distinctNestedElements
        .groupBy(_.root.path)
        .map(tree => tree._1 -> tree._2.sortBy(_.level).reverse)

    @tailrec
    def getNestedParents(
      n: NestedElement,
      parents: Seq[NestedElement]
    ): NestedElement = {
      n.parent match {
        case Some(p) =>
          if (!nestedParentsPath.contains(p.path)) {
            p.copy(children = Nil)
            nestedParentsPath += p.path -> (p, Seq(n))
            getNestedParents(p, p +: parents)
          } else {
            nestedParentsPath += p.path -> (p, nestedParentsPath(p.path)._2 :+ n)
            p
          }
        case _ => n
      }
    }

    val nestedParents =
      distinctNestedElementsByRoot.values.flatten
        .map(de => getNestedParents(de, Seq.empty))
        .toSeq
        .distinct

    def innerBuildNestedTree(n: NestedElement): NestedElement = {
      val children = nestedParentsPath.get(n.path).map(_._2).getOrElse(Seq.empty)
      if (children.nonEmpty) {
        val updatedChildren = children.map(innerBuildNestedTree)
        n.copy(children = updatedChildren.groupBy(_.path).map(_._2.head).toSeq)
      } else {
        n
      }
    }

    if (nestedParents.nonEmpty) {
      val trees = nestedParents.map(innerBuildNestedTree)
      trees
    } else {
      distinctNestedElements
    }
  }

  def walkNestedTree(n: NestedElement)(f: NestedElement => Unit): Unit = {
    f(n)
    n.children.foreach(child => walkNestedTree(child)(f))
  }
}
