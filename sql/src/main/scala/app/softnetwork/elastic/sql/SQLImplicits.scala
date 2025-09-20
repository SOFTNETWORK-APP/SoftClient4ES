package app.softnetwork.elastic.sql

import app.softnetwork.elastic.sql.parser.SQLParser

import scala.util.matching.Regex

/** Created by smanciot on 27/06/2018.
  */
object SQLImplicits {
  import scala.language.implicitConversions

  implicit def queryToSQLCriteria(query: String): Option[Criteria] = {
    val maybeQuery: Option[Either[SQLSearchRequest, SQLMultiSearchRequest]] = query
    maybeQuery match {
      case Some(Left(l)) => l.where.flatMap(_.criteria)
      case _             => None
    }
  }

  implicit def queryToSQLQuery(
    query: String
  ): Option[Either[SQLSearchRequest, SQLMultiSearchRequest]] = {
    SQLParser(query) match {
      case Left(_)  => None
      case Right(r) => Some(r)
    }
  }

  implicit def sqllikeToRegex(value: String): Regex = toRegex(value).r

}
