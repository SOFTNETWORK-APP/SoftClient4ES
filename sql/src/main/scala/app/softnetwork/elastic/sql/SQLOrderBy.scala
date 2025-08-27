package app.softnetwork.elastic.sql

case object OrderBy extends SQLExpr("order by") with SQLRegex

sealed trait SortOrder extends SQLRegex

case object Desc extends SQLExpr("desc") with SortOrder

case object Asc extends SQLExpr("asc") with SortOrder

case class SQLFieldSort(
  field: String,
  order: Option[SortOrder],
  function: Option[SQLFunction] = None
) extends SQLTokenWithFunction {
  private[this] lazy val fieldWithFunction: String = function match {
    case Some(f) => s"$f($field)"
    case _       => field
  }
  override def sql: String = s"$fieldWithFunction ${order.getOrElse(Asc)}"
}

case class SQLOrderBy(sorts: Seq[SQLFieldSort]) extends SQLToken {
  override def sql: String = s" $OrderBy ${sorts.mkString(",")}"
}
