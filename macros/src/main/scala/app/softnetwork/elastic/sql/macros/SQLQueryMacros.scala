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
  ): c.Expr[Seq[T]] = {
    import c.universe._

    c.echo(c.enclosingPosition, "=" * 60)
    c.echo(c.enclosingPosition, "🚀🚀🚀 MACRO searchAsImpl CALLED 🚀🚀🚀")
    c.echo(c.enclosingPosition, "=" * 60)

    val tpe = weakTypeOf[T]
    val validatedQuery = validateSQLQuery[T](c)(query)

    c.Expr[Seq[T]](q"""
      ${c.prefix}.searchAsUnchecked[$tpe](
        SQLQuery($validatedQuery)
      )($m, $formats)
    """)
  }

  // ============================================================
  // searchAsyncAs
  // ============================================================

  def searchAsyncAsImpl[U: c.WeakTypeTag](c: blackbox.Context)(
    sqlQuery: c.Expr[String]
  )(
    m: c.Expr[Any],
    ec: c.Expr[Any],
    formats: c.Expr[Formats]
  ): c.Expr[Any] = {
    import c.universe._

    c.echo(c.enclosingPosition, "=" * 60)
    c.echo(c.enclosingPosition, "🚀🚀🚀 MACRO searchAsyncAsImpl CALLED 🚀🚀🚀")
    c.echo(c.enclosingPosition, "=" * 60)

    val tpe = weakTypeOf[U]
    val validatedQuery = validateSQLQuery[U](c)(sqlQuery)

    c.Expr[Any](q"""
      ${c.prefix}.searchAsyncAsUnchecked[$tpe](
        SQLQuery($validatedQuery)
      )($m, $ec, $formats)
    """)
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
  ): c.Expr[Any] = {
    import c.universe._

    val tpe = weakTypeOf[T]
    val validatedQuery = validateSQLQuery[T](c)(sql)

    c.echo(c.enclosingPosition, "=" * 60)
    c.echo(c.enclosingPosition, "🚀🚀🚀 MACRO scrollAsImpl CALLED 🚀🚀🚀")
    c.echo(c.enclosingPosition, "=" * 60)

    c.Expr[Any](q"""
      ${c.prefix}.scrollAsUnchecked[$tpe](
        SQLQuery($validatedQuery),
        $config
      )($system, $m, $formats)
    """)
  }
}
