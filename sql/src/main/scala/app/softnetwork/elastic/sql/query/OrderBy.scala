/*
 * Copyright 2015 SOFTNETWORK
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

import app.softnetwork.elastic.sql.function.{Function, FunctionChain}
import app.softnetwork.elastic.sql.{Expr, TokenRegex, Updateable}

case object OrderBy extends Expr("ORDER BY") with TokenRegex

sealed trait SortOrder extends TokenRegex

case object Desc extends Expr("DESC") with SortOrder

case object Asc extends Expr("ASC") with SortOrder

case class FieldSort(
  field: String,
  order: Option[SortOrder],
  functions: List[Function] = List.empty
) extends FunctionChain
    with Updateable {
  lazy val direction: SortOrder = order.getOrElse(Asc)
  lazy val name: String = toSQL(field)
  override def sql: String = s"$name $direction"
  override def update(request: SQLSearchRequest): FieldSort = this // No update logic for now TODO
}

case class OrderBy(sorts: Seq[FieldSort]) extends Updateable {
  override def sql: String = s" $OrderBy ${sorts.mkString(", ")}"

  override def validate(): Either[String, Unit] =
    for {
      _ <-
        if (sorts.isEmpty)
          Left("At least one sort field is required")
        else
          sorts.map(_.validate()).filter(_.isLeft) match {
            case Nil    => Right(())
            case errors => Left(errors.map { case Left(err) => err }.mkString("\n"))
          }
    } yield ()

  def update(request: SQLSearchRequest): OrderBy = this.copy(sorts = sorts.map(_.update(request)))
}
