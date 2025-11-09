package app.softnetwork.elastic.client.macros

import app.softnetwork.elastic.sql.macros.SQLQueryMacros
import app.softnetwork.elastic.sql.query.SQLQuery
import org.json4s.{DefaultFormats, Formats}

import scala.language.experimental.macros

/** Test trait that uses macros for compile-time validation.
  */
trait TestElasticClientApi {

  /** Search with compile-time SQL validation (macro).
    */
  def searchAs[T](query: String)(implicit m: Manifest[T], formats: Formats): Seq[T] =
    macro SQLQueryMacros.searchAsImpl[T]

  /** Search without compile-time validation (runtime).
    */
  def searchAsUnchecked[T](
    sqlQuery: SQLQuery
  )(implicit m: Manifest[T], formats: Formats): Seq[T] = {
    // Dummy implementation for tests
    Seq.empty[T]
  }
}

object TestElasticClientApi extends TestElasticClientApi {
  // default implicit for the tests
  implicit val defaultFormats: Formats = DefaultFormats
}
