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
