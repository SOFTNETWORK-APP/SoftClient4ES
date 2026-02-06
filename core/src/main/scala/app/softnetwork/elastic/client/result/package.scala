/*
 * Copyright 2025 SOFTNETWORK
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package app.softnetwork.elastic.client

import akka.NotUsed
import akka.stream.scaladsl.Source
import app.softnetwork.elastic.client.scroll.ScrollMetrics
import app.softnetwork.elastic.sql.schema.{IngestPipeline, Table}

import scala.collection.immutable.ListMap
import scala.util.control.NonFatal

package object result {

  /** Represents the result of an Elasticsearch operation.
    *
    * @tparam T
    *   Value type on success
    */
  sealed trait ElasticResult[+T] {

    /** Indicates whether the operation succeeded */
    def isSuccess: Boolean

    /** Indicates whether the operation failed */
    def isFailure: Boolean = !isSuccess

    /** Transforms the value on success */
    def map[U](f: T => U): ElasticResult[U]

    /** Monadic composition */
    def flatMap[U](f: T => ElasticResult[U]): ElasticResult[U]

    /** Retrieves the value or a default value */
    def getOrElse[U >: T](default: => U): U

    /** Converts to Option */
    def toOption: Option[T]

    /** Converts to Either */
    def toEither: Either[ElasticError, T]

    /** Converts to Future */
    def toFuture(implicit
      ec: scala.concurrent.ExecutionContext
    ): scala.concurrent.Future[T] = {
      scala.concurrent.Future.fromTry(this.toEither.toTry)
    }

    /** Fold pattern matching */
    def fold[U](onFailure: ElasticError => U, onSuccess: T => U): U

    /** Recovers the error if it fails */
    def error: Option[ElasticError]

    /** Performs a side effect if it succeeds */
    def foreach(f: T => Unit): Unit

    /** Filters the result */
    def filter(p: T => Boolean, errorMsg: String = "Filter predicate failed"): ElasticResult[T]

    /** Retrieves the value or throws an exception */
    def get: T
  }

  /** Represents a successful operation.
    */
  case class ElasticSuccess[T](value: T) extends ElasticResult[T] {
    override def isSuccess: Boolean = true

    override def map[U](f: T => U): ElasticResult[U] =
      try {
        ElasticSuccess(f(value))
      } catch {
        case NonFatal(ex) =>
          ElasticFailure(
            ElasticError(
              s"Error during map transformation: ${ex.getMessage}",
              Some(ex)
            )
          )
      }

    override def flatMap[U](f: T => ElasticResult[U]): ElasticResult[U] =
      try {
        f(value)
      } catch {
        case NonFatal(ex) =>
          ElasticFailure(
            ElasticError(
              s"Error during flatMap transformation: ${ex.getMessage}",
              Some(ex)
            )
          )
      }

    override def getOrElse[U >: T](default: => U): U = value

    override def toOption: Option[T] = Some(value)

    override def toEither: Either[ElasticError, T] = Right(value)

    override def fold[U](onFailure: ElasticError => U, onSuccess: T => U): U = onSuccess(value)

    override def error: Option[ElasticError] = None

    override def foreach(f: T => Unit): Unit = f(value)

    override def filter(p: T => Boolean, errorMsg: String): ElasticResult[T] =
      if (p(value)) this
      else ElasticFailure(ElasticError(errorMsg))

    override def get: T = value
  }

  /** Represents a failed operation.
    */
  case class ElasticFailure(elasticError: ElasticError)
      extends Throwable(elasticError.message, elasticError)
      with ElasticResult[Nothing] {
    override def isSuccess: Boolean = false

    override def map[U](f: Nothing => U): ElasticResult[U] = this

    override def flatMap[U](f: Nothing => ElasticResult[U]): ElasticResult[U] = this

    override def getOrElse[U](default: => U): U = default

    override def toOption: Option[Nothing] = None

    override def toEither: Either[ElasticError, Nothing] = Left(elasticError)

    override def fold[U](onFailure: ElasticError => U, onSuccess: Nothing => U): U =
      onFailure(elasticError)

    override def error: Option[ElasticError] = Some(elasticError)

    override def foreach(f: Nothing => Unit): Unit = ()

    override def filter(p: Nothing => Boolean, errorMsg: String): ElasticResult[Nothing] = this

    override def get: Nothing = throw new NoSuchElementException(
      s"ElasticFailure.get: ${elasticError.message}"
    )

    def isNotFound: Boolean = elasticError.statusCode.contains(404)
  }

  /** Represents an Elasticsearch error.
    */
  case class ElasticError(
    message: String,
    cause: Option[Throwable] = None,
    statusCode: Option[Int] = None,
    index: Option[String] = None,
    operation: Option[String] = None
  ) extends Throwable(message, cause.orNull) {

    /** Complete message with context */
    def fullMessage: String = {
      val parts = Seq(
        operation.map(op => s"[$op]"),
        index.map(idx => s"index=$idx"),
        statusCode.map(code => s"status=$code"),
        Some(message)
      ).flatten

      parts.mkString(" ")
    }

    /** Log the error with a logger */
    def log(logger: org.slf4j.Logger): Unit = {
      cause match {
        case Some(ex) => logger.error(fullMessage, ex)
        case None     => logger.error(fullMessage)
      }
    }
  }

  object ElasticError {

    /** Creates an ElasticError from an exception */
    def fromThrowable(
      ex: Throwable,
      statusCode: Option[Int] = None,
      index: Option[String] = None,
      operation: Option[String] = None
    ): ElasticError = {
      ElasticError(
        ex.getMessage,
        Some(ex),
        statusCode,
        index,
        operation
      )
    }

    /** Creates a not found error */
    def notFound(index: String, operation: String): ElasticError = {
      ElasticError(
        s"Resource not found in index '$index' during operation '$operation'",
        statusCode = Some(404),
        index = Some(index),
        operation = Some(operation)
      )
    }

    def notFound(resource: String, name: String, operation: String): ElasticError = {
      ElasticError(
        s"$resource '$name' not found during operation '$operation'",
        statusCode = Some(404),
        operation = Some(operation)
      )
    }
  }

  /** Companion object with utility methods.
    */
  object ElasticResult {

    /** Creates a success.
      */
    def success[T](value: T): ElasticResult[T] = ElasticSuccess(value)

    /** Creates a failure.
      */
    def failure[T](error: ElasticError): ElasticResult[T] = ElasticFailure(error)

    /** Creates a failure with a simple message.
      */
    def failure[T](message: String): ElasticResult[T] =
      ElasticFailure(ElasticError(message))

    /** Creates a failure with a message and an exception.
      */
    def failure[T](message: String, cause: Throwable): ElasticResult[T] =
      ElasticFailure(ElasticError(message, Some(cause)))

    /** Runs a block of code and catches exceptions.
      */
    def attempt[T](block: => T): ElasticResult[T] =
      try {
        ElasticSuccess(block)
      } catch {
        case NonFatal(ex) =>
          ElasticFailure(
            ElasticError(
              s"Operation failed: ${ex.getMessage}",
              Some(ex)
            )
          )
      }

    /** Converts an Option to ElasticResult.
      */
    def fromOption[T](option: Option[T], errorMsg: => String): ElasticResult[T] =
      option match {
        case Some(value) => ElasticSuccess(value)
        case None        => ElasticFailure(ElasticError(errorMsg))
      }

    /** Converts an Either to ElasticResult.
      */
    def fromEither[T](either: Either[String, T]): ElasticResult[T] =
      either match {
        case Right(value) => ElasticSuccess(value)
        case Left(error)  => ElasticFailure(ElasticError(error))
      }

    /** Converts a Try to ElasticResult.
      */
    def fromTry[T](tryValue: scala.util.Try[T]): ElasticResult[T] =
      tryValue match {
        case scala.util.Success(value) => ElasticSuccess(value)
        case scala.util.Failure(ex) =>
          ElasticFailure(
            ElasticError(
              s"Operation failed: ${ex.getMessage}",
              Some(ex)
            )
          )
      }

    def fromFuture[T](future: scala.concurrent.Future[T])(implicit
      ec: scala.concurrent.ExecutionContext
    ): scala.concurrent.Future[ElasticResult[T]] = {
      future
        .map(value => ElasticSuccess(value))
        .recover { case NonFatal(ex) =>
          ElasticFailure(
            ElasticError(
              s"Operation failed: ${ex.getMessage}",
              Some(ex)
            )
          )
        }
    }

    /** Sequences a list of results. Returns success with the list of values if all succeed,
      * otherwise returns the first failure.
      */
    def sequence[T](results: List[ElasticResult[T]]): ElasticResult[List[T]] = {
      results.foldRight(success(List.empty[T]): ElasticResult[List[T]]) { (result, acc) =>
        for {
          value <- result
          list  <- acc
        } yield value :: list
      }
    }

    /** Traverses a list with a function that returns an ElasticResult.
      */
    def traverse[T, U](list: List[T])(f: T => ElasticResult[U]): ElasticResult[List[U]] = {
      sequence(list.map(f))
    }

    /** Implicit class to add methods to Boolean in ElasticResult.
      */
    implicit class BooleanElasticResult(result: ElasticResult[Boolean]) {

      /** Checks if the result is successful AND true */
      def isTrue: Boolean = result match {
        case ElasticSuccess(true) => true
        case _                    => false
      }

      /** Checks if the result is successful AND false */
      def isFalse: Boolean = result match {
        case ElasticSuccess(false) => true
        case _                     => false
      }

      /** Returns true if successful, false otherwise (ignores the value) */
      def succeeded: Boolean = result.isSuccess
    }

    /** Implicit class for logging.
      */
    implicit class LoggableElasticResult[T](result: ElasticResult[T]) {

      /** Log error if failure */
      def logError(logger: org.slf4j.Logger): ElasticResult[T] = {
        result match {
          case ElasticFailure(error) => error.log(logger)
          case _                     => ()
        }
        result
      }

      /** Log success */
      def logSuccess(logger: org.slf4j.Logger, message: T => String): ElasticResult[T] = {
        result match {
          case ElasticSuccess(value) => logger.info(message(value))
          case _                     => ()
        }
        result
      }

      /** Log success or failure */
      def log(
        logger: org.slf4j.Logger,
        onSuccess: T => String,
        onFailure: ElasticError => String
      ): ElasticResult[T] = {
        result match {
          case ElasticSuccess(value) => logger.info(onSuccess(value))
          case ElasticFailure(error) => logger.error(onFailure(error))
        }
        result
      }
    }
  }

  sealed trait QueryResult

  case object EmptyResult extends QueryResult

  object QueryResult {
    def empty: QueryResult = EmptyResult
  }

  // --------------------
  // DQL (SELECT)
  // --------------------
  case class QueryRows(rows: Seq[ListMap[String, Any]]) extends QueryResult

  case class QueryStream(
    stream: Source[(ListMap[String, Any], ScrollMetrics), NotUsed]
  ) extends QueryResult

  case class QueryStructured(response: ElasticResponse) extends QueryResult {
    def asQueryRows: QueryRows = QueryRows(response.results)
  }

  case class StreamResult(
    estimatedSize: Option[Long],
    isActive: Boolean
  ) extends QueryResult

  // --------------------
  // DML (INSERT / UPDATE / DELETE)
  // --------------------
  case class DmlResult(
    inserted: Long = 0L,
    updated: Long = 0L,
    deleted: Long = 0L,
    rejected: Long = 0L
  ) extends QueryResult

  // --------------------
  // DDL (CREATE / ALTER / DROP / TRUNCATE)
  // --------------------
  case class DdlResult(success: Boolean) extends QueryResult

  case class TableResult(table: Table) extends QueryResult

  case class PipelineResult(pipeline: IngestPipeline) extends QueryResult

  case class SQLResult(sql: String) extends QueryResult

}
