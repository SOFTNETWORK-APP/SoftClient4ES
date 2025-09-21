package app.softnetwork.elastic.sql.query

import app.softnetwork.elastic.sql.{Expr, TokenRegex}

case object Limit extends Expr("limit") with TokenRegex

case class Limit(limit: Int) extends Expr(s" limit $limit")
