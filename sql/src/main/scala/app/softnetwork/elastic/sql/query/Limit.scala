package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.{Expr, TokenRegex}

case object Limit extends Expr("LIMIT") with TokenRegex

case class Limit(limit: Int, offset: Option[Offset])
    extends Expr(s" LIMIT $limit${offset.map(_.sql).getOrElse("")}")

case object Offset extends Expr("OFFSET") with TokenRegex

case class Offset(offset: Int) extends Expr(s" OFFSET $offset")
