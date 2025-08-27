package app.softnetwork.elastic.sql

case object Limit extends SQLExpr("limit") with SQLRegex

case class SQLLimit(limit: Int) extends SQLExpr(s" limit $limit")
