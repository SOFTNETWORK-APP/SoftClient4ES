package app.softnetwork.elastic.sql

case object Limit extends Expr("limit") with TokenRegex

case class Limit(limit: Int) extends Expr(s" limit $limit")
