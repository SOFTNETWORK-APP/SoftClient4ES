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

package app.softnetwork.elastic.sql.macros

import org.json4s.Formats

import scala.language.experimental.macros
import scala.reflect.macros.blackbox

object SQLQueryMacros extends SQLQueryValidator {

  // ============================================================
  // searchAs
  // ============================================================

  def searchAsImpl[T: c.WeakTypeTag](c: blackbox.Context)(
    query: c.Expr[String]
  )(
    m: c.Expr[Manifest[T]],
    formats: c.Expr[Formats]
  ): c.Tree = {
    import c.universe._

    // 1. Validate the SQL query at compile-time
    val validatedQuery = validateSQLQuery[T](c)(query)

    // 2. Get the type parameter
    val tpe = weakTypeOf[T]

    // 3. Generate the call to searchAsUnchecked
    q"""
      ${c.prefix}.searchAsUnchecked[$tpe](
        _root_.app.softnetwork.elastic.sql.query.SelectStatement($validatedQuery)
      )($m, $formats)
    """
  }

  // ============================================================
  // searchAsyncAs
  // ============================================================

  def searchAsyncAsImpl[U: c.WeakTypeTag](c: blackbox.Context)(
    query: c.Expr[String]
  )(
    m: c.Expr[Any],
    ec: c.Expr[Any],
    formats: c.Expr[Formats]
  ): c.Tree = {
    import c.universe._

    // 1. Validate the SQL query at compile-time
    val validatedQuery = validateSQLQuery[U](c)(query)

    // 2. Get the type parameter
    val tpe = weakTypeOf[U]

    // 3. Generate the call to searchAsUnchecked
    q"""
      ${c.prefix}.searchAsyncAsUnchecked[$tpe](
        _root_.app.softnetwork.elastic.sql.query.SelectStatement($validatedQuery)
      )($m, $ec, $formats)
    """
  }

  // ============================================================
  // scrollAs
  // ============================================================

  def scrollAsImpl[T: c.WeakTypeTag](c: blackbox.Context)(
    sql: c.Expr[String],
    config: c.Expr[Any]
  )(
    system: c.Expr[Any],
    m: c.Expr[Any],
    formats: c.Expr[Formats]
  ): c.Tree = {
    import c.universe._

    // 1. Validate the SQL query at compile-time
    val validatedQuery = validateSQLQuery[T](c)(sql)

    // 2. Get the type parameter
    val tpe = weakTypeOf[T]

    // 3. Generate the call to searchAsUnchecked
    q"""
      ${c.prefix}.scrollAsUnchecked[$tpe](
        _root_.app.softnetwork.elastic.sql.query.SelectStatement($validatedQuery),
        $config
      )($system, $m, $formats)
    """
  }
}
