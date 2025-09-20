package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.function.{Function, FunctionChain}

case object OrderBy extends Expr("order by") with TokenRegex

sealed trait SortOrder extends TokenRegex

case object Desc extends Expr("desc") with SortOrder

case object Asc extends Expr("asc") with SortOrder

case class FieldSort(
  field: String,
  order: Option[SortOrder],
  functions: List[Function] = List.empty
) extends FunctionChain {
  lazy val direction: SortOrder = order.getOrElse(Asc)
  lazy val name: String = toSQL(field)
  override def sql: String = s"$name $direction"
}

case class OrderBy(sorts: Seq[FieldSort]) extends Token {
  override def sql: String = s" $OrderBy ${sorts.mkString(", ")}"
}
