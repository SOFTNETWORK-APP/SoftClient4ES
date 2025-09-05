package app.softnetwork.elastic.sql

case object OrderBy extends SQLExpr("order by") with SQLRegex

sealed trait SortOrder extends SQLRegex

case object Desc extends SQLExpr("desc") with SortOrder

case object Asc extends SQLExpr("asc") with SortOrder

case class SQLFieldSort(
  field: String,
  order: Option[SortOrder],
  functions: List[SQLFunction] = List.empty
) extends SQLTokenWithFunction {
  private[this] lazy val fieldWithFunction: String =
    functions.foldLeft(field)((expr, fun) => {
      fun.toSQL(expr)
    })

  lazy val direction: SortOrder = order.getOrElse(Asc)
  lazy val name: String = fieldWithFunction
  override def sql: String = s"$name $direction"
}

case class SQLOrderBy(sorts: Seq[SQLFieldSort]) extends SQLToken {
  override def sql: String = s" $OrderBy ${sorts.mkString(", ")}"
}
